# Pulse At-Scale Conflict / Race Test Suite — Ordered Implementation Plan

> Test lead synthesis. This is the **ordered build plan** for Pulse's at-scale
> conflict and race coverage. Build the steps in the numbered order given:
> each step is sequenced by value-per-new-surface (earliest = highest value,
> least new code), interleaved across all four scenarios — **not** grouped by
> scenario.

---

## 1. Goal + why these tests matter

Pulse's correctness story under concurrency rests on a narrow contract:
**mutations are atomic and serializable end-to-end**, and conflicts must
**fail loud, never silent**. The concrete mechanisms (all verified against the
source):

- Every mutation request spawns a `tx_task`
  (`crates/pulse-jsruntime/src/lib.rs:166`) that owns one connection, runs
  `BEGIN` then `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`
  (`crates/pulse-jsruntime/src/lib.rs:189`), and routes every db op of that
  request onto that single connection — so all writes of one mutation share one
  `BEGIN…COMMIT` (atomic).
- A sticky `serialization_failed` flag fires on SQLSTATE `40001`/`40P01`
  (`is_serialization_failure` / `sql_err_is_serialization`,
  `crates/pulse-jsruntime/src/lib.rs:87-99`); commit failure or any conflicting
  op surfaces `SERIALIZATION_FAILURE`.
- `Worker::execute` (`crates/pulse-jsruntime/src/lib.rs:300`) wraps
  `execute_once` in a retry loop: `MAX_ATTEMPTS=25`, exponential backoff
  `base=(2<<attempt.min(5)).min(40)` plus per-attempt UUID-derived jitter (no
  RNG/clock dependency → handlers stay deterministic); on exhaustion it returns
  code `"CONFLICT"`, mapped to HTTP 409 by `pulse-server` `status_for`.
- Pool isolation is set in `crates/pulse-sql/src/lib.rs` `connect_with`
  (`SET SESSION CHARACTERISTICS … SERIALIZABLE` + `SET statement_timeout` in
  `after_connect`); `pulse-server/src/main.rs` builds an OLTP pool
  (serializable, default 15000ms) and an OLAP pool (non-serializable, 60000ms).

**The trap we are defending against.** This project has already been bitten by
the classic **silent lost update**: `counters.ts` `increment` does
get-then-patch+1 (`const doc = await ctx.db.get(input.id); await
ctx.db.patch(input.id, { value: doc.value + 1 })`). Under hot single-row contention two concurrent
read-modify-writes must collide; one commits, the other's read is now stale and
its UPDATE/COMMIT must raise `40001` and be retried. If anything makes them not
truly contend (pool silently downgrading isolation, in-process routing
accidentally serializing the work, a read that doesn't join the serializable
read-set), the second writer overwrites with a stale `value+1` and an increment
is lost — **with every promise resolved, zero errors, yet `counters.get < K`.**
That failure signature is the whole point: a test that only asserts "all calls
fulfilled" or "no 409s" passes vacuously while the ledger is corrupt.

Every test below is therefore designed around two non-negotiable principles:

1. **Conservation/equality, not inequality** — the row must equal the number of
   committed mutations *exactly* (catches both lost updates `<` and
   double-commits `>`).
2. **Fail loud, never skip** — DB-less CI runs currently pass vacuously
   (`isolation.rs` and integration tests silently SKIP when Postgres is
   unreachable). At-scale tests must **assert the DB is present** or they prove
   nothing.

---

## 2. Tiering model

Two tiers, one env flag, reusing the existing load-config pattern.

| Tier | Runs in | Config | Gate |
|------|---------|--------|------|
| **CI-safe** | normal `vitest` integration/load suite on every PR | existing `vitest.config` / integration config | always |
| **Heavy** | on-demand only | reuse the **`vitest.load.config.ts` pattern** | env flag **`PULSE_HEAVY=1`** |

- CI-safe assertions are **bounded** (N≈40–50 contenders, ≤90s timeouts) and
  must be deterministic enough to run on every PR without flake.
- Heavy assertions **characterize breaking points** (N=100–400, 3-node fan-out,
  R-round repeats, fault windows) and are skipped unless `PULSE_HEAVY` is set,
  via `describe.skipIf(!process.env.PULSE_HEAVY)` (or `it.skip`).
- Heavy invocation: `PULSE_HEAVY=1 vitest run <file>` (or a dedicated
  `vitest.load.config.ts` include glob).
- **Rule:** the *exact same invariants* are asserted in both tiers; heavy only
  scales the inputs and adds the graceful-degradation (409-tolerant) form. The
  CI tier asserts the strict zero-rejection form; the heavy tier asserts
  `committed + 409 == N` and `final == committed`.

---

## 3. New surface to add, consolidated (build the prerequisites first)

Add these *before* the steps that depend on them (each step below names its
prerequisite). Nothing here is speculative — every item is consumed by a numbered
step.

### Product / example surface (`packages/examples-chat`)
- **`accounts` table** in `dev-db.sql`: `id text primary key, balance bigint not
  null default 0`. Distinct from `counters` so the existing 50-increment fixture
  is untouched. *(Step 5)*
- **`transfers` ledger table** (optional but recommended): `id, "from", "to",
  amount`. *(Step 5, reconciliation)*
- **`contract.ts`**: `accounts.create({id, balance})`, `accounts.get({id}) ->
  {id, balance}`, `accounts.list({}) -> [...]`, and the headline
  `accounts.transfer({from, to, amount})` mutation; plus `transfers.countRaw({})`
  mirroring `messages.countRaw`. *(Step 5)*
- **`accounts.ts`** handler: `transfer` as **one mutation** doing both writes on
  the **same request connection** (shared single `BEGIN…COMMIT` per
  `lib.rs:166/189`): read both rows, debit `from`, credit `to`, optionally reject
  with a handler error if `from.balance < amount`. **Critically: do NOT pre-sort
  from/to to canonical lock order** — the deadlock test relies on opposing
  acquisition order to provoke `40P01`. Add a code comment documenting this so a
  well-meaning "fix" doesn't neuter the test. Insert one ledger row in the same
  tx. *(Step 5)*

### Env knob (Rust)
- **`PULSE_MAX_TX_ATTEMPTS`** read in `crates/pulse-jsruntime/src/lib.rs` where
  `MAX_ATTEMPTS` is currently a const used by `Worker::execute` (~line 300).
  Default to 25 when unset/invalid (production unchanged); parse once at worker
  init. This is the **cleanest enabler**: it lets CI force retry exhaustion
  deterministically at low concurrency instead of relying on flaky 200+ load.
  *(Step 6)*

### Harness helpers (`tests/integration/harness.ts`)
- **DB-presence assertion** (`assertDbReachable`, or make `startEngine`/`reset`
  fail loudly): throw if `/health` never comes up or if a `reset()` +
  create/get round-trip fails, so DB-less CI fails loudly instead of skipping
  vacuously. **Build this first (Step 0) — every other step depends on it.**
- **`StartOptions.maxTxAttempts?: number`** wired into the child process env as
  `PULSE_MAX_TX_ATTEMPTS`, sitting next to existing `oltpMaxConns`,
  `oltpStatementTimeoutMs`, `authToken`. *(Step 6)*
- **`reset()` extension** to also `TRUNCATE accounts, transfers` (today it
  truncates only `messages`+`counters` via `docker exec … psql` on container
  `pulse-pg`). Add the two table names to that truncate list, or add
  `resetAccounts()`. *(Step 5)*
- **Typed conflict check** `isPulseConflictError(err)` (`err.code === 'CONFLICT'`,
  HTTP status 409) so heavy tests can cleanly distinguish 409 from
  timeout/INTERNAL without reaching into internals. If `PulseClientError` already
  carries `{code, status}` this is free. *(Steps 1, 5, 6)*
- **Two-node spawn**: no new helper strictly required — `startEngine()` called
  twice is the established `multinode.test.ts` pattern; optionally wrap as a
  `startCluster(n)` convenience for the 3-node heavy variant. *(Steps 1, 7)*

### Client test surface (`packages/client/src/`)
- **`FakeTransport`** test double (~40 lines) implementing the public transport
  interface `LocalFirst` consumes (`transport.ts` shape): `mutate(req)` returns a
  manually-resolvable deferred and records `sentOrder` (seq ids in call order);
  `push(changeSet)` delivers server pushes into the same `LocalStore`
  subscription path; helpers `resolveConfirm(seq, serverValue)`,
  `rejectClient(seq)`, `rejectNetwork(seq)`. *(Step 3)*
- **Read-only observability** on the client if not already exported (check
  `index.ts`): `LocalFirst.pendingCount()` and `LocalStore.confirmedSnapshot()`
  so stuck-overlay and FIFO invariants are assertable through behavior, not
  private fields. *(Step 3)*
- **Transport-override seam** on the client constructor for the heavy variant
  (same seam the CI `FakeTransport` uses) so the live client can be driven
  offline/online. *(Step 8)*

### Load harness (`tests/load/metrics.ts`)
- `runLoad` task contract: let the task **classify a `CONFLICT` (409) as a
  counted-but-not-error outcome** distinct from real errors. Minimal — the heavy
  task closure catches `PulseClientError code==='CONFLICT'`, tallies it locally,
  and rethrows nothing; `runLoad` already never throws and counts errors.
  *(Steps 4, 6 heavy)*

---

## 4. The ORDERED build sequence

Interleaved by difficulty/value, not by scenario. **15 steps.** Steps 0 and 1
are quick-wins that establish the anti-vacuous guard and the highest-value
cross-node convergence test with essentially no new product code. Later steps
add example surface, the env knob, and finally heavy/fault tiers.

Legend for scenarios:
- **S1** = Cross-node hot-row increment convergence (quick-win)
- **S2** = Multi-key deadlock / money-transfer conservation (medium)
- **S3** = Local-first overlay rebase convergence (medium, client-side)
- **S4** = Retry exhaustion + fairness beyond MAX_ATTEMPTS (medium)

---

### Step 0 — Harness: DB-presence guard + typed conflict check *(prerequisite for everything)*
**Scenario:** cross-cutting.
**New surface first:** in `tests/integration/harness.ts` add `assertDbReachable`
(or make `startEngine`/`reset` throw loudly) and `isPulseConflictError(err)`
(`code==='CONFLICT'`, status 409). Reuse `PulseClientError` if it already carries
`{code, status}`.
**CI-safe test:** none of its own — it is the seam. Add a tiny smoke check:
after `startEngine()` + `reset()`, a `counters.create`/`get` round-trip must
succeed or the suite **fails** (not skips).
**Invariant:** *no vacuous pass* — a DB-less CI run fails loudly instead of
silently skipping (closes the `connect().ok()` / early-return SKIP gap in
`isolation.rs` and integration tests).
**Heavy variant:** n/a (shared seam used by all heavy tests).
**Anti-false-positive:** this *is* the anti-false-positive foundation; every
later test calls it before asserting.

---

### Step 1 — S1 CI-safe: cross-node hot-row increment convergence *(quick-win, highest value)*
**Scenario:** S1.
**New surface first:** none beyond Step 0. Reuses `counters.create/get/increment`
(`packages/examples-chat`), `startEngine`, `makeClient`, `reset`, the two-node
`multinode.test.ts` pattern. **Note the real contract** (`contract.ts:88-93`):
`counters.create({ name }) -> id` (returns the new row id), `counters.get({ id })
-> number` (output is a **bare number**, not `{value}`), `counters.increment({ id
}) -> null`. So create once, capture the returned `id`, then drive `increment`/
`get` with that id.
**CI-safe test:** `tests/integration/multinode-conflict.test.ts`.
- `const a = await startEngine(); const b = await startEngine();` (same
  `DATABASE_URL`, distinct random `node_id` UUIDs). Default `oltpMaxConns` so
  neither node is 1-conn-serialized. `afterAll: a.stop(); b.stop()`.
- `await a.reset()` **once**; assert it actually ran (Step 0 guard).
- `const id = await a.client.counters.create.call({ name:'xnode' });` (create
  once on node A; row visible to both via shared PG).
- `const ca = a.makeClient('inc-a'); const cb = b.makeClient('inc-b');`
- `K=40`. Build K calls **alternating nodes**:
  `(i % 2 === 0 ? ca : cb).counters.increment.call({ id })`. Fire all via
  `Promise.allSettled`. 90s timeout (cross-node 40001 retries are slower).
**Invariants:**
1. **No rejections** — `rejected.length === 0` (25-attempt SERIALIZABLE retry
   absorbed every cross-node 40001).
2. **No lost update** — `a.client.counters.get.call({ id }) === K` **exactly**
   (output is a bare number), AND `b.client.counters.get.call({ id }) === K`
   (both nodes agree on shared-PG truth).
3. **Work really crossed the process boundary** — structural check that both
   `ca` and `cb` were used (so a green run can't come from all calls hitting one
   node).
**Heavy variant:** see Step 7.
**Anti-false-positive:** asserts **equality** (`=== K`, not `>=`); asserts both
nodes' reads agree (catches per-process drift); asserts alternation actually
happened (catches the test degenerating to single-node); guarded by Step 0 so a
DB-less run can't pass.

---

### Step 2 — S4 CI-safe TEST 1: retries exhaust into a clean CONFLICT *(needs env knob)*
> Depends on **Step 6** (`PULSE_MAX_TX_ATTEMPTS` + `StartOptions.maxTxAttempts`).
> Listed here in value order; if building strictly top-down, do Step 6 first.
> (Kept distinct because Step 6 is "add the knob"; this step is "use it".)
**Scenario:** S4 (A: clean exhaustion).
**CI-safe test:** `tests/integration/retry-exhaustion.test.ts`.
- `const h = await startEngine({ maxTxAttempts: 2, oltpMaxConns: 16,
  oltpStatementTimeoutMs: 0 });` `await h.reset();`
  `const id = await h.client.counters.create.call({ name:'hot' });` Step-0 guard:
  `counters.get.call({ id }) === 0` after reset.
- `N=40` independent clients; `Promise.allSettled` of one
  `increment.call({ id })` each. With budget=2, most of 40 simultaneous RMWs
  exhaust.
**Invariants:**
1. **Exhaustion actually happened** — `rejected.length > 0` (guards against
   degenerating into the happy-path increment test).
2. **Every rejection is a clean conflict** — each `reason instanceof
   PulseClientError`, `reason.code === 'CONFLICT'`, HTTP **409** (NOT
   INTERNAL/500, NOT a timeout string).
3. **The lost-update trap** — `counters.get.call({ id }) === fulfilled`
   **exactly** (not `>=` → catches double-commit over-count; not `<=` → catches
   silent lost update).
4. `fulfilled > 0` (no-op guard).
**Heavy variant:** Step 11 (breaking-point sweep at real budget=25).
**Anti-false-positive:** the small `maxTxAttempts=2` **forces** the failure path
so the test can't pass by accident on a fast machine; equality between `final`
and `fulfilled` is the exact historical-trap detector; `oltpStatementTimeoutMs:
0` proves a failure is 40001-exhaustion not a timeout.

---

### Step 3 — S3 CI-safe: local-first overlay rebase convergence *(client-side, no engine)*
**Scenario:** S3.
**New surface first:** `FakeTransport` test double + (if missing)
`LocalFirst.pendingCount()` / `LocalStore.confirmedSnapshot()` read-only
accessors. Reuse the in-memory KV already used by `queue.test.ts`.
**CI-safe test:** pure/deterministic, no Postgres, vitest in-memory — mirrors
`packages/client/src/local.test.ts` and `queue.test.ts` style.
- Instantiate public `LocalFirst` with the in-memory KV + `FakeTransport`
  (deferred `mutate()`, `push(changeSet)`). Seed confirmed truth
  `{id:'c', value:0}`.
- Fire `mutate()` seq 1..5 (each optimistic +1) before any transport resolution
  → assert overlay shows 5 immediately.
- Interleave deterministically (via deferreds, **not timers**): confirm seq 1
  (server value=1), then `push` a foreign change setting value=1 on the same row,
  then confirm seq 2..5 (resolve seq 3 before seq 2 to probe FIFO vs completion
  order). Reject seq 4 with `PulseClientError` (handler reject → drop+rollback);
  inject a network error on another seq (stays queued).
**Invariants:**
1. **Convergence** — final `recompute()` === server-confirmed truth, where
   *expected is derived from `FakeTransport`'s recorded confirmations*, not
   hardcoded.
2. **No lost / no duplicate** — final scalar neither below nor above expected.
3. **No stuck overlay** — `OfflineQueue.length()===0` for confirmed+rejected,
   `===1` for the network-failed one; once queue empty,
   `recompute() === confirmedSnapshot()`.
4. **FIFO preserved** — `FakeTransport.sentOrder` deep-equals `[1,2,3,5]`
   (seq 4 rejected, never re-sent; strict order; no dup seq).
5. **Rollback correctness** — the `PulseClientError` rolls back only its own
   updater; a concurrent push to other keys survives.
6. **Network-error durability** — network-failed mutation stays queued, sent
   exactly once on reconnect.
7. **Recompute idempotency** — two consecutive `recompute()` with no change are
   byte-identical (mirrors reactor `record_value` suppression).
**Heavy variant:** Step 8.
**Anti-false-positive:** **does not assert only the final scalar** — also asserts
the recorded confirmation set AND `sentOrder`, because a lost-update plus a
phantom-duplicate can cancel to the right scalar (the project's own
0-errors-but-lost trap in client form). Idempotency check catches a recompute
that mutates pending as a side effect.

---

### Step 4 — S4 CI-safe TEST 2: sustained contention starves no caller *(fairness)*
**Scenario:** S4 (B: fairness / eventual success).
**New surface first:** `metrics.ts` task-classification tweak is *not* needed for
CI (uses `Promise.all` + re-submit), but the conflict-classification helper from
Step 0 is.
**CI-safe test:** same file `tests/integration/retry-exhaustion.test.ts`.
- `startEngine({ maxTxAttempts: 3, oltpMaxConns: 16 })`; reset;
  `const id = await h.client.counters.create.call({ name:'fair' });`.
- Public re-submit wrapper: `commitWithResubmit(c)` loops, calls
  `counters.increment.call({ id })`, on `CONFLICT` (`isPulseConflictError`)
  `continue`, else throw. Add a **per-caller attempt cap** (e.g. 500) that FAILS
  the test with that caller's id if hit — the **starvation detector**.
- `M=30` callers via `Promise.all(...)`. 60s overall timeout.
**Invariants:**
1. `Promise.all` resolves within timeout — every logical mutation eventually
   committed.
2. `counters.get.call({ id }) === 30` — eventual success is lossless and not
   over-counted.
3. Per-caller attempt cap never tripped (no permanent starvation; also probes
   whether UUID-derived deterministic jitter ever produces a permanently
   colliding caller).
**Heavy variant:** Step 12 (M=200 with attempt histogram).
**Anti-false-positive:** the explicit per-caller cap converts a hang into a
**named-caller failure with diagnostic** instead of a silent jest-timeout;
equality `=== 30` catches both loss and over-count.

---

### Step 5 — S2: add accounts/transfers example surface + harness reset *(medium, enables S2)*
**Scenario:** S2 (prerequisite build step).
**New surface first (this whole step is surface):**
- `dev-db.sql`: `accounts` table (+ optional `transfers` ledger).
- `contract.ts`: `accounts.create/get/list`, `accounts.transfer`,
  `transfers.countRaw`.
- `accounts.ts`: `transfer` = one mutation, two writes on the same request
  connection (shared `BEGIN…COMMIT`), optional overdraw reject, ledger insert in
  the same tx. **No canonical from/to sort** (comment why).
- `harness.ts` `reset()` extended to `TRUNCATE accounts, transfers`.
**CI-safe test:** none yet — Step 5 is purely the surface. A trivial smoke test
(`accounts.create` + `get` round-trip equals seeded balance) confirms wiring.
**Invariant (smoke):** seeded balance round-trips; atomic single-write commits.
**Heavy variant:** n/a.
**Anti-false-positive:** the no-canonical-sort comment is itself an
anti-false-positive guard — it prevents a future refactor from neutering the
deadlock provocation in Step 9.

---

### Step 6 — S4 env knob: `PULSE_MAX_TX_ATTEMPTS` + `StartOptions.maxTxAttempts`
**Scenario:** S4 (enabler for Steps 2, 4, 11, 12).
**New surface first (this step is the surface):**
- Rust: read `PULSE_MAX_TX_ATTEMPTS` in `crates/pulse-jsruntime/src/lib.rs`
  replacing the `const MAX_ATTEMPTS: u32 = 25` inside `Worker::execute`
  (`crates/pulse-jsruntime/src/lib.rs:311`, used by the retry loop at lines
  300–326); default 25 on unset/invalid; parse once at worker init.
- `harness.ts`: `StartOptions.maxTxAttempts?: number` → child env
  `PULSE_MAX_TX_ATTEMPTS`, next to `oltpMaxConns`/`oltpStatementTimeoutMs`/
  `authToken`.
**CI-safe test:** a Rust/integration assertion that with `maxTxAttempts=1` a
contended increment surfaces `CONFLICT` while `unset` (=25) absorbs ~40 — proving
the knob is wired and defaults to production behavior.
**Invariant:** default behavior unchanged (25) when knob absent; knob
deterministically shrinks the budget.
**Heavy variant:** n/a (knob is consumed by heavy Steps 11/12).
**Anti-false-positive:** explicit "default 25 when unset" test prevents the knob
from silently changing production retry behavior; the contrast (budget=1 fails,
default passes) proves the knob actually takes effect rather than being a no-op.

---

### Step 7 — S1 heavy: cross-node convergence past the budget + 3-node + repeat
**Scenario:** S1.
**Heavy variant (env-gated `PULSE_HEAVY`):** same
`tests/integration/multinode-conflict.test.ts`,
`describe.skipIf(!process.env.PULSE_HEAVY)`.
- (a) Push **past** the budget: `K=200` across two nodes. Assert the
  **graceful-degradation** contract instead of zero failures:
  `fulfilled + conflicts === K` (every call either committed or surfaced a clean
  409 — none lost) AND `counters.get.call({ id }) === fulfilled` **exactly**
  (strongest no-lost-update form that tolerates 409s). Assert **no** rejection is
  a statement-timeout/INTERNAL.
- (b) 3-node variant (`startEngine() x3`, round-robin clients) to show
  convergence isn't a two-party artifact.
- (c) Repeat the CI K=40 case `R=10` rounds with `reset()` between rounds to
  surface rare/flaky lost updates a single shot hides.
**Invariants:** same as Step 1, plus `committed + 409 == K`, `final ==
committed`, failures are 409-shaped not timeout-shaped, convergence holds across
3 nodes and across R rounds.
**Anti-false-positive:** the R-round repeat surfaces low-probability races; the
"no timeout/INTERNAL among rejections" check closes the timeout-masquerading-as-
conflict gap; equality `final == fulfilled` survives nonzero 409s.

---

### Step 8 — S3 heavy: live-engine overlay convergence with real out-of-order pushes
**Scenario:** S3.
**New surface first:** transport-override seam on the live client constructor
(reuse the FakeTransport seam) so the real client can be toggled offline/online.
**Heavy variant (`PULSE_HEAVY`):** same `LocalFirst`/`LocalStore`/`OfflineQueue`
public API driven against a **live engine** via `startEngine()` (real
HTTP/worker/SERIALIZABLE/LISTEN-NOTIFY stack).
- Two `LocalFirst` clients on the counters surface; `reset()`.
- Client A fires 200 optimistic `counters.increment` via `LocalFirst.mutate`
  while client B fires 200 on the same row — each client's confirmed layer is
  bombarded by real cross-client pushes mid-flush.
- `PULSE_HEAVY_SEED`: inject random micro-delays via interleaved await points;
  randomly toggle a client offline (drop network) for windows, then reconnect and
  flush (offline-queue replay against a moved-forward server).
**Invariants:** after both queues drain and a final push is applied, every
client's rebased view == server truth == `counters.get`; `counters.get === 400`
(no engine-side lost update across the 25-retry budget AND no client-side overlay
loss); each `OfflineQueue.length()===0`; no client ends `!=` server (no stuck
overlay). Run `K=20` randomized seeds — any single non-convergent end-state
fails.
**Anti-false-positive:** real out-of-order pushes (recall `commit_lsn` is always
`Lsn::ZERO` — pushes have **no version** to reconcile against, so the rebase must
converge purely on read-set match + FIFO); 20 seeds expose rare interleavings; a
single non-convergent end-state fails the whole run.

---

### Step 9 — S2 CI-safe: money-transfer deadlock + conservation
**Scenario:** S2.
**New surface first:** Step 5 (accounts/transfers + reset) must be done.
**CI-safe test:** `tests/integration/transfers.test.ts`, modeled on the
increment test. Step-0 DB guard at suite top (explicit
`accounts.list`/`counters.get` that **fails** on ECONNREFUSED, not skips).
- `K=2` accounts, each seeded `balance=1000` (total 2000).
- `clientAB=h.makeClient('ab')`, `clientBA=h.makeClient('ba')`.
- **Symmetric phase:** `Promise.allSettled` of 25
  `clientAB.accounts.transfer({from:A,to:B,amount:1})` interleaved with 25
  `clientBA.accounts.transfer({from:B,to:A,amount:1})` — swapped from/to forces
  **opposing lock order** on rows A and B (provokes `40P01`). 60s timeout.
- **Asymmetric phase:** 30 transfers A→B and 10 B→A (net 20 leaving A).
**Invariants:**
1. **No hang / retry absorbed** — `results.every(fulfilled)` (no deadlock hang,
   no 409 surfaced); print any rejected reasons.
2. **Conservation** — `a.balance + b.balance === 2000` (money neither created nor
   destroyed).
3. **Net-zero symmetric** — `a.balance === 1000 && b.balance === 1000`.
4. **Asymmetric ledger** — after the asymmetric phase `a.balance === 980 &&
   b.balance === 1020` (a silent lost/partial update cannot satisfy an asymmetric
   net while also conserving).
5. **Ledger reconciliation** — `transfers.countRaw` === number of fulfilled
   transfer calls.
**Heavy variant:** Step 10.
**Anti-false-positive:** the **asymmetric phase** is the key anti-vacuous device
— "both halves lost symmetrically" passes the symmetric check but fails the
asymmetric net; conservation under exact-equality catches money creation/
destruction; ledger count catches phantom/dropped entries even if balances
happen to look plausible. This is the *only* test exercising the multi-row write
path (the increment test touches one row and cannot surface `40P01`; the only
other `40P01` coverage is the raw-SQL `isolation.rs` unit, never end-to-end).

---

### Step 10 — S2 heavy: transfer load, cyclic deadlocks, global conservation under partial failure
**Scenario:** S2.
**Heavy variant (`PULSE_HEAVY`):** `tests/load/transfers.load.test.ts`, reuses
`startEngine` + `runLoad(total, concurrency, task)` from `metrics.ts`.
- `startEngine({ oltpMaxConns: 16 })` (force pool queueing, not a 1-conn
  artifact); `K=8` accounts each `balance=10000` (total 80000).
- `runLoad(total=2000, concurrency=64, task=()=>{ pick two distinct random
  accounts i!=j; clients[r].accounts.transfer({from:i,to:j,amount:1}) })` —
  random pairings across 8 rows maximize **cyclic** lock-order deadlocks (not
  just the 2-row cycle) and push past the ~50-contender budget.
**Invariants:**
1. `summarize(latencies)` p99 bounded; errors classified — distinguish 409
   CONFLICT (acceptable, counted) from a hang/timeout (statement_timeout, NOT
   acceptable) by error code.
2. **Global conservation** — sum of all 8 balances `=== 80000` **exactly**,
   *regardless of how many calls errored* (errored transfers must have fully
   rolled back → conservation holds even with nonzero errors). The killer
   invariant: holds under partial failure only if every tx is truly
   all-or-nothing.
3. **Ledger reconcile** — `transfers.countRaw` === confirmed-transfer count
   (catches a created/destroyed unit even if it nets to a plausible per-row
   balance).
- Second variant: concurrency 200, assert conservation **still** holds while
  merely allowing 409s (graceful degradation, not corruption, past the budget).
**Anti-false-positive:** conservation-under-partial-failure is unfalsifiable by a
"all fulfilled / no 409" shortcut because it must hold *with* errors present;
ledger reconciliation catches a balanced-but-corrupt ledger; the 409-vs-timeout
classification closes the timeout-masquerade gap.

---

### Step 11 — S4 heavy: breaking-point sweep at the real budget (=25)
**Scenario:** S4.
**New surface first:** `metrics.ts` task-classification (CONFLICT counted, not an
error).
**Heavy variant (`PULSE_HEAVY`):** `tests/load/retry-exhaustion.load.test.ts`,
reuses `harness.ts` + `metrics.ts`.
- `startEngine({ oltpMaxConns: 16, oltpStatementTimeoutMs: 0 })` — **default
  budget 25**; statement timeout 0 so a failure is provably 40001-exhaustion, not
  a timeout (closes the timeout-vs-conflict ambiguity).
- For `C in [50,100,200,400]`: reset; `const id = await
  h.client.counters.create.call({ name:'hot'+C });`
  `runLoad(C, C, ()=> makeClient(rand).counters.increment.call({ id }))`
  treating CONFLICT as a non-error tally.
**Invariants per C:**
1. Zero **non-CONFLICT** errors (no INTERNAL, no 500, no network) — exhaustion is
   the only failure species.
2. `committed := C - conflicts`; `counters.get.call({ id }) === committed`
   **exactly** at every contention level — the lost-update invariant survives
   scale.
3. At `C=50` conflicts ≈ 0 (25 budget tuned for ~50); at `C=200,400` conflicts >
   0 — documents the breaking point and graceful 409 degradation.
4. `summarize(latencies)` p50/p95/p99 logged so the 409 path is bounded (no
   runaway backoff hang).
**Anti-false-positive:** equality `final == committed` at *every* contention
level; `oltpStatementTimeoutMs: 0` makes a timeout impossible so any non-409
failure is unambiguously a real bug; the C=50-clean / C=400-degrades contrast
proves the test is actually exercising both regimes.

---

### Step 12 — S4 heavy: fairness at scale (M=200, attempt histogram)
**Scenario:** S4.
**Heavy variant (`PULSE_HEAVY`):** same load file as Step 11.
- `M=200` distinct callers each `commitWithResubmit` on one row at default budget;
  record a per-caller attempt histogram.
**Invariants:**
1. All 200 commit; `final value === 200`.
2. Max per-caller attempt count is bounded (e.g. `< 200`) — the attempt
   distribution has a finite tail; no caller in the 99.9th percentile loops
   pathologically.
**Anti-false-positive:** the bounded-tail assertion catches a permanently-
colliding caller that the UUID-derived deterministic jitter could in principle
produce; equality `=== 200` catches loss/over-count at scale.

---

### Step 13 — S1/S2 heavy gap doc-test: Resync (oversized change-set) thundering-herd
**Scenario:** cross-cutting known-gap (S1 family).
**Status:** **documented gap, opt-in heavy probe** — `Resync` (payload > 7800
bytes → `invalidate_all` on every node, `pulse-cdc/src/lib.rs`) is implemented
but **untested**; `invalidate_all` re-execs every subscription (potential
thundering herd at scale).
**Heavy variant (`PULSE_HEAVY`):** force a single change-set over `MAX_PAYLOAD`
(7800 bytes) on node A with many subscriptions live on node B; assert every B
subscription re-runs **exactly once** (coarse invalidation is not amplified into
repeated re-runs) and the system stays bounded (latency p99 logged, no error
storm).
**Invariants:** oversized payload degrades to a single coarse re-eval per sub
(no duplicate re-runs); cluster remains responsive.
**Anti-false-positive:** assert a **count** of re-runs per sub (== 1), not merely
"no error" — a thundering herd would re-run subs many times while still reporting
success.

---

### Step 14 — S1 heavy gap doc-test: bus-loss / listener-reconnect *(requires fault injection — flagged)*
**Scenario:** cross-cutting known-gap (S1 family).
**Status:** **blocked on new harness affordance** — there is currently **no
fault injection** in `harness.ts` (cannot kill/restart a node mid-flight,
partition the bus, or pause Postgres). The cross-node bus is fire-and-forget
`pg_notify` with **no delivery guarantee**; a down/slow listener means Postgres
drops the NOTIFY, and `start_listener` just `break`s on recv error
(`main.rs` logs but never restarts the listener). This is **out of scope for the
public-client tiers above** and should be tracked as a follow-up requiring:
1. a harness affordance to kill/restart a node or pause its listener, and
2. a product decision on listener auto-reconnect (today there is none).
**Recommended action now:** document the gap explicitly in this plan (done here);
add the fault-injection affordance as a separate workstream before attempting the
test. Do **not** write a test that silently passes because the bus happened not
to drop a message.
**Invariant (once buildable):** a dropped/lost bus message or a listener
reconnect does not leave a subscriber permanently stale (requires reconnect +
re-sync, which does not exist yet — so this is a **product gap surfaced by the
test plan**, not just a missing test).
**Anti-false-positive:** until reconnect exists, any "passing" bus-loss test
would be vacuous — hence this step is intentionally a flagged blocker, not a
green checkmark.

---

### Step 15 — S4/cross-cutting: worker-IPC bottleneck characterization *(optional heavy)*
**Scenario:** cross-cutting known-gap.
**Status:** the whole mutation path is mediated by **one JS worker process**: tx
ops flow through a single mpsc channel (capacity 32) and one stdin `Mutex`
(`ChildStdin`). At very high mutation concurrency the worker IPC could become the
bottleneck instead of Postgres — load tests don't isolate this.
**Heavy variant (`PULSE_HEAVY`):** drive high mutation concurrency (e.g. 200+
on distinct rows so Postgres contention is low) and compare throughput against a
read-only baseline; assert mutation throughput does not collapse below a floor
attributable purely to the 32-deep mpsc + stdin Mutex.
**Invariant:** mutation throughput scales with offered load until Postgres (not
the IPC channel) is the bottleneck; if it plateaus early, the IPC seam is
flagged.
**Anti-false-positive:** use **distinct rows** so a low throughput can't be
blamed on row contention — isolating the IPC variable; assert against a
read-baseline ratio, not an absolute number.

---

## 5. Anti-false-positive checklist (summary)

Every test above is built so it **cannot pass while its bug is live**:

1. **Equality, never inequality.** Final row value is asserted `=== committed`
   (not `>=`/`<=`), catching both the silent lost update (`<`) and double-commit
   over-count (`>`). *(Steps 1, 2, 7, 9, 10, 11, 12)*
2. **DB-presence guard.** Step 0 makes a DB-less run **fail loudly** instead of
   the existing silent SKIP — no vacuous green. *(all engine-backed steps)*
3. **Force the failure path.** `PULSE_MAX_TX_ATTEMPTS`=small **provokes**
   exhaustion deterministically; `rejected.length > 0` asserts it actually
   happened so the test can't degenerate into the happy path. *(Steps 2, 6)*
4. **Error taxonomy.** Rejections must be `code==='CONFLICT'` / HTTP 409, **never**
   INTERNAL/500 or timeout; `oltpStatementTimeoutMs: 0` makes a timeout
   impossible so any non-409 failure is unambiguously a bug. *(Steps 2, 7, 10,
   11)*
5. **Conservation under partial failure.** Money sum holds **exactly even with
   nonzero errors** — unfalsifiable by an "all-fulfilled" shortcut. *(Step 10)*
6. **Asymmetric ledger phase.** Defeats the "both halves lost symmetrically"
   alibi that a symmetric-only check would miss. *(Step 9)*
7. **Set + order, not just scalar (client).** Assert the recorded confirmation
   set AND `sentOrder`, because a lost-update plus a phantom-duplicate can cancel
   to a correct scalar. *(Step 3)*
8. **Cross-node agreement + alternation.** Both nodes' reads must agree, and the
   structural check proves both nodes were actually driven. *(Steps 1, 7)*
9. **Count re-runs, not just success.** Resync/herd checks assert exactly-once
   re-execution per sub, not merely "no error". *(Step 13)*
10. **Repeat to surface rare races.** R-round / K-seed repeats expose
    low-probability interleavings a single shot hides; any single non-convergent
    end-state fails the run. *(Steps 7, 8)*
11. **Flag, don't fake.** Bus-loss/reconnect (Step 14) is a flagged blocker
    requiring new fault-injection + a product reconnect path — explicitly **not**
    written as a green test, because it would pass vacuously today.

---

## 6. Build-order rationale (one-glance)

| # | Scenario | Tier | New surface needed first | Difficulty |
|---|----------|------|--------------------------|------------|
| 0 | cross-cut | CI | DB guard + `isPulseConflictError` | quick |
| 1 | S1 | CI | none (reuse counters + 2-node) | quick-win |
| 2 | S4 | CI | (needs Step 6 knob) | medium |
| 3 | S3 | CI | FakeTransport + read accessors | medium |
| 4 | S4 | CI | conflict helper | medium |
| 5 | S2 | surface | accounts/transfers + reset | medium |
| 6 | S4 | enabler | `PULSE_MAX_TX_ATTEMPTS` + `maxTxAttempts` | medium |
| 7 | S1 | heavy | none | quick-win→scale |
| 8 | S3 | heavy | transport-override seam | medium |
| 9 | S2 | CI | Step 5 | medium |
| 10 | S2 | heavy | metrics conflict-classify | medium |
| 11 | S4 | heavy | metrics conflict-classify | medium |
| 12 | S4 | heavy | none | medium |
| 13 | S1 | heavy | none (Resync probe) | gap-probe |
| 14 | S1 | blocked | **fault injection (new)** + product reconnect | hard/blocked |
| 15 | cross-cut | heavy | none (IPC characterization) | optional |

> Note: Step 2 logically depends on the Step 6 knob. They are kept as separate
> numbered steps (6 = "add the knob", 2 = "use it for the clean-CONFLICT test")
> because the knob is a shared enabler also consumed by Steps 11–12. If building
> strictly without forward references, build Step 6 before Step 2.
