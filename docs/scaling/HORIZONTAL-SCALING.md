# Pulse — Horizontal Scaling Design

Status: design doc / lead-architect synthesis
Audience: engine contributors planning M0–M5 so that post-v1 scale-out is not a rewrite
Source of truth for current behavior: `crates/`, `docs/ARCHITECTURE.md` (§4, §7, §10), `packages/client/src/sync.ts`

---

## 1. Executive answer: "Are we designing for horizontal scaling from the start?"

**No — not yet, and we should be honest about it. Pulse as built (M1/M2 thin slice) is single-node by construction.** But it does not have to *stay* a rewrite-to-scale design, and this doc lays out a set of cheap seams to introduce during M0–M5 so that scale-out becomes a deployment topology + transport problem, not a re-architecture.

Two pillars of the reactive system live entirely in the RAM of whichever process happened to handle a given request:

1. **The subscription / SSE registry** — `crates/pulse-server/src/reactor.rs:24-28` holds two process-local `Mutex<HashMap>`s: `clients` (clientId → `mpsc::Sender` for that client's live SSE stream) and `subs` (clientId+sub → `Subscription`). Both are created by `Reactor::new()` at startup (`main.rs:67`) with no shared backing.
2. **The invalidation signal (the write-set)** — a mutation's touched tables are captured *per request, inside the worker that ran it* (`crates/pulse-jsruntime/src/lib.rs:212-226`), surfaced as `res.writes` (`main.rs:143-147`), and fed to `invalidate()` which only iterates *this* process's `subs` map and pushes over *this* process's `mpsc` channels (`main.rs:155-172`, `reactor.rs:61-78`).

Put two `pulse-server` instances behind a load balancer today and reactivity silently breaks: **a write handled by node A never reaches a subscriber whose SSE stream and subscription live on node B**, because the only thing that propagates a write is the in-process write-set, and it cannot cross the process boundary. There is no Redis/NATS/Kafka/broadcast bus anywhere in `crates/`, and `pulse-cdc` / `pulse-reactor` / `pulse-sse` are empty doc-comment stubs.

The good news: the *target* is already described (`ARCHITECTURE.md` §4.3 step 2, §4.4, §7.2), `pulse-core` already ships the correct serde wire types (`Lsn`, `ChangeSet` / `Change` / `TableId` / `PrimaryKey`, `ReadSet` with `matches_change`/`matches`/`referenced_tables`), and the three stub crates are the exact seams where the distribution layer plugs in. The cost of being scale-ready is a handful of interface boundaries, not a distributed system we have to build now.

---

## 2. Current single-node bottlenecks

| # | Component (code) | Issue | Severity |
|---|---|---|---|
| 1 | Reactor registry + per-client SSE channels (`pulse-server/src/reactor.rs:24-28,40-44,61-67`) | `clients` and `subs` are plain in-RAM `Mutex<HashMap>`s with no shared/persistent backing. A client's SSE sender exists only in the process that served its `GET /sync`. `push()` looks up the sender in *this* process's map and returns `false` if absent — any other node literally cannot deliver to that client. | **blocker** |
| 2 | Invalidation via engine-captured write-set (`main.rs:143-172`; capture at `pulse-jsruntime/src/lib.rs:212-226`; match at `reactor.rs:70-78`) | The committed write's *only* signal is the in-process write-set. `invalidate()` matches it against the local `subs` map and pushes to local channels only. A write on node A invalidates only subscriptions on node A; identical subs on B/C go stale indefinitely. The central correctness break for multi-node. | **blocker** |
| 3 | No WAL/CDC consumer (`pulse-cdc` is a stub; `ARCHITECTURE.md` §4.3, §7.2, §10) | The intended node-agnostic change source — a logical-replication consumer decoding `pgoutput` into `ChangeSet`s — is unimplemented. Without it there is no way for every node to learn about every committed write from the shared source of truth (Postgres). Note: a named slot is itself single-consumer, so this must be a single/leader-elected component that fans out. | **blocker** |
| 4 | SSE stickiness not enforced (`main.rs:180-187` `/sync`; `199-239` `/subscribe`/`/unsubscribe`; `sync.ts` control plane) | The control protocol implicitly *requires* one node per client. If the LB routes `GET /sync` to A but `POST /subscribe` to B, B records the sub and `push()` returns `false` (no channel there), while A's stream has no sub — split-brain, silently never delivers. No cookie/header affinity exists in code; stickiness is an unstated hard requirement. | **blocker** |
| 5 | Single Bun worker per engine (`main.rs:57-65`; `pulse-jsruntime/src/lib.rs:87-126,68-74`) | One child worker per engine; all execution serialized through one `Mutex<ChildStdin>` and correlated via process-local `pending`/`captures` maps keyed by a per-process `Uuid`. The correlation + capture state is non-shareable; scales only by adding whole engine+worker pairs. Re-exec on invalidation only ever uses the local worker. | high |
| 6 | `last_mutation_id` / rebase watermark — spec-only, not implemented (`ARCHITECTURE.md` §4.3/§4.4; zero refs in `crates/`/`packages/`) | Rebase relies on `last_mutation_id` advanced in the mutation tx and stored authoritatively. Today mutations are autocommit-per-op (§10) and the client sends only a random `clientId` (`sync.ts makeClientId`). For multi-node this must live in shared Postgres, scoped per clientId; a process-local counter corrupts cross-node rebase. The clientId is ephemeral per page load, so carries no routing affinity. | high |
| 7 | No resumability (`sync.ts runStream` builds `sync?clientId=` only, no `Last-Event-ID`; `catch` just exits; `pulse-sse` is a stub; SSE events carry no id — `main.rs:185`) | No `id:<seq>`, no ring buffer, no reconnect. Any disconnect drops all in-flight invalidations; because the registry was process-local on the dead node, reconnecting to a new node loses every prior subscription. Multi-node breaks SSE far more often (rolling deploys, scale-down, rebalancing). | high |
| 8 | No cross-node ordering / LSN watermark (`main.rs:116-118` pushes carry only `{sub,data}`; no `Lsn` use in `pulse-server`; `ARCHITECTURE.md` §4.4) | The intended model has every push carry `commitLSN` so the client batch-advances all subs to the same point. Today pushes carry no LSN/seq/order token, and each handler re-executes against its own autocommit ops. Across nodes, two subs for the same client (or after failover) can reflect different commit points with no reconciliation. | high |
| 9 | Per-process pool, no OLAP/replica split, no global cap (`main.rs:46-55`; `pulse-sql/src/lib.rs:25-30`; §6/§7.3 vs §10) | One `sqlx PgPool` of `PULSE_OLTP_MAX_CONNS` (default 10) per process, shared by mutations and reactive re-exec. Bounded per node but multiplied per instance with no global cap, so N nodes × max_conns can saturate Postgres. No PgBouncer, no replica routing; analytical scans share the OLTP budget. | medium |

---

## 3. Target architecture

### 3.1 Chosen synthesis

The three candidate designs converge on the same proven topology (ElectricSQL / Rocicorp Zero / LunaDB): **make the change signal come from Postgres, not from the in-process write-set, via a single leader-elected WAL consumer, and fan decoded `ChangeSet`s to all nodes over a shared bus; each stateless node re-executes and pushes only for the subscriptions it locally holds.** The candidates differ on one axis: how subscriptions are distributed across nodes.

- **Candidate A / B** keep the per-node reactor process-local and rely on a broadcast bus + sticky routing + Postgres rehydrate; stickiness is a *performance* optimization because SSE resume + resync make any node able to recover a client.
- **Candidate C** shards the subscription registry by `clientId` on a consistent-hash ring with a gateway that pins all of a client's calls (`/sync` + `/subscribe` + `/unsubscribe` + `/rpc`) to the owning node; stickiness becomes a *correctness invariant* and there is **no cross-node push relay** by construction (a node only ever pushes to clients it owns).

**Decision: adopt Candidate A/B's broadcast-bus + disposable-node model as the v1 scale-out target, with Candidate C's clientId-keyed sticky routing layered on as the affinity mechanism — but treat stickiness as a *warm-state optimization backed by a correctness safety net* (SSE resume + Postgres rehydrate), not a hard invariant we must perfectly enforce on day one.** Rationale:

1. It is the smallest delta from today's code. The per-node reactor (`reactor.rs`) stays process-local *on purpose* (a node tracks only its own clients, like a Zero view-syncer); the only change to its *input* is "ChangeSets off the bus" instead of "the local write-set."
2. It avoids building a distributed registry (Candidate C's `ShardedReactor` + coordinator + rebalancing) before we have proven we need it. Candidate C's hard-stickiness model trades a network registry for a strict routing invariant whose failure mode is *silent non-delivery* — a worse footgun than a brief warm-cache miss.
3. The disposable-node + resume path means a misroute or a failover degrades to "client reconnects and rehydrates," not "subscription silently dropped." That is the safer default while the system is young.
4. Candidate C's clientId hashing is still the right *routing key*; we adopt it for affinity, we just don't make correctness depend on it being perfect. If/when intra-node warm-cache hit rate matters, the gateway's ring lookup tightens affinity without changing the contract.

The full distributed registry (Candidate C) remains the *escape hatch* for extreme fan-out and is deliberately deferred behind the same `Reactor` trait.

### 3.2 Roles

| Role | Crate(s) | Cardinality | Responsibility |
|---|---|---|---|
| **Gateway / ingress** | (LB / reverse proxy; not a crate) | N | clientId-hash or cookie sticky routing of `/sync`+`/subscribe`+`/unsubscribe`+`/rpc` to one engine node, HTTP/2 upstream. Affinity is best-effort; correctness backstopped by resume. |
| **Engine node** (stateless, disposable) | `pulse-server` + `pulse-jsruntime` + `pulse-reactor` + `pulse-sse` + `pulse-sql` | N | Serve `/rpc` (run mutation on local worker, commit to Postgres), `/sync`/`/subscribe` (local reactor only). Subscribe to the bus; on each `ChangeSet`, match against the *local* registry, debounce/dedup, re-execute on the *local* worker, push over *local* SSE. |
| **Change router** (WAL consumer) | `pulse-cdc` + small leader-election wrapper | 1 active (+ hot standby) | The ONLY holder of the named logical-replication slot. Decode `pgoutput` Insert/Update/Delete → `pulse_core::ChangeSet{commit_lsn, changes}`, publish to the bus, advance the slot's confirmed LSN only after the ChangeSet is durably on the bus (crash replays, never drops). |
| **Change bus** | external (NATS / Redis Streams / Kafka) | 1 logical | Carry `ChangeSet`s (already serde-ready) from router to every engine node. |
| **Postgres** | — | 1 logical (primary + optional read replicas) | Single source of truth. Holds the data, the slot, and the `pulse_clients` watermark table. |

### 3.3 Multi-node diagram

```
                            ┌──────────────────────────────────────────┐
   browsers / clients       │  GATEWAY / LOAD BALANCER (HTTP/2)          │
   (durable clientId,       │  sticky-by-clientId: /sync /subscribe       │
    Last-Event-ID)  ───────▶│  /unsubscribe /rpc → same engine node       │
                            └───────┬───────────────┬───────────────┬─────┘
                                    │               │               │
                          ┌─────────▼──────┐ ┌──────▼─────────┐ ┌───▼────────────┐
                          │ ENGINE NODE A  │ │ ENGINE NODE B  │ │ ENGINE NODE C  │
                          │ (pulse-server) │ │                │ │   (stateless,  │
                          │                │ │                │ │   disposable)  │
                          │ pulse-reactor  │ │ pulse-reactor  │ │ pulse-reactor  │
                          │  (local subs)  │ │  (local subs)  │ │  (local subs)  │
                          │ pulse-sse      │ │ pulse-sse      │ │ pulse-sse      │
                          │  (ring buffer) │ │  (ring buffer) │ │  (ring buffer) │
                          │ pulse-jsruntime│ │ pulse-jsruntime│ │ pulse-jsruntime│
                          │  worker + pool │ │  worker + pool │ │  worker + pool │
                          └───▲────┬───────┘ └───▲────┬───────┘ └───▲────┬───────┘
              subscribe       │    │ commit       │    │ commit       │    │ commit
              ChangeSets      │    │ (mutations)  │    │              │    │
                              │    │              │    │              │    │
                       ┌──────┴────┴──────────────┴────┴──────────────┴────┴───────┐
                       │            CHANGE BUS  (NATS / Redis Streams)              │
                       │   subject: pulse.changes  —  carries ChangeSet{lsn,...}    │
                       └───────────────────────────▲────────────────────────────────┘
                                                    │ publish (one ChangeSet / commit,
                                                    │  in commit-LSN order)
                                       ┌────────────┴───────────────┐
                                       │   CHANGE ROUTER (pulse-cdc) │
                                       │   ── leader-elected, x1 ──  │
                                       │   owns the ONE logical slot │
                                       │   pgoutput → ChangeSet       │
                                       │   advance slot AFTER publish │
                                       └────────────▲───────────────┘
                                                    │ logical replication
                                                    │ (START_REPLICATION … pgoutput)
                       ┌────────────────────────────┴────────────────────────────┐
                       │                       POSTGRES (primary)                  │
                       │   data tables · named replication slot · pulse_clients     │
                       │   (last_mutation_id, resume cursor)   [+ read replicas]    │
                       └────────────────────────────────────────────────────────────┘
```

**The key property:** the only thing crossing a process boundary is the committed change, and it travels `node A → Postgres → router → bus → {A,B,C}`. A subscriber on node B is invalidated by node A's write because the signal came from the shared source of truth, never through A's process memory. Mutations still run on the local worker and commit to Postgres; node A is no longer responsible for invalidation (the `main.rs:143-147` write-set→`invalidate()` inline path is bypassed/removed in multi-node mode).

### 3.4 Propagation walk-through

1. Client's `/rpc` lands on its sticky engine node (say A). A's worker runs the mutation and commits to Postgres. A does **not** drive invalidation from the captured write-set.
2. The singleton change router, tailing the slot, decodes the committed rows into `ChangeSet{commit_lsn, changes:[Change{table,key,op}]}` and publishes it to the bus (one message per commit, preserving commit-LSN order).
3. Every engine node receives the identical `ChangeSet`. Each runs `ReadSet::matches`/`matches_change` (already implemented in `pulse-core/src/readset.rs:55-76`) against its *own* registry, debounces ~50–150 ms, dedups by `(path,input)`, re-executes matched subs on its *local* worker at a snapshot ≥ `commit_lsn`, and pushes each result over its *local* SSE channel, stamped `id:<seq>` and carrying `commit_lsn`.

### 3.5 Bus durability

Correctness of delivery is guaranteed by the **SSE ring buffer + `Last-Event-ID`** (in `pulse-sse`) and **slot replay on router crash**, *not* by the bus. This means an at-most-once bus (Redis pub/sub) is acceptable — but only once the SSE resume seam exists. A durable bus (Redis Streams / Kafka) is preferred because it doubles as the cross-node replication log so a restarted node or reconnecting client can replay from an LSN/offset without a second Postgres slot.

---

## 4. Cross-node consistency model

Keep `ARCHITECTURE.md` §4.4's global consistency model, and make **the LSN the single cross-node coordination token** (`pulse_core::Lsn` already exists, is `Ord` + serde, and round-trips `pg_lsn`).

1. **Single global order.** The change router is the one serialization point: it emits `ChangeSet`s in commit-LSN order from the one slot, so every node observes the same monotone stream. There is no per-node clock to reconcile.
2. **Per-query consistency.** Each re-execution reads one Postgres MVCC snapshot **≥ the ChangeSet's `commit_lsn`** (no torn reads). This guarantee depends on M4 transactions; with today's autocommit-per-op it does not yet hold (§10) and must land with M4. *Risk: if a node re-executes at a snapshot < `commit_lsn` (bus delivery racing replica visibility), the client could batch-advance past data it has not actually seen — re-exec MUST read ≥ `commit_lsn`.*
3. **Cross-query / cross-node consistency.** Every SSE push carries `commit_lsn` + a per-stream monotonic `id:seq`. The client batch-advances ALL its subscriptions to the same `commit_lsn` before flushing React, so two subscriptions served from different nodes (or after a failover) reconcile to the same commit point instead of showing divergent state.
4. **Mutation rebase.** `last_mutation_id` is advanced in the same SERIALIZABLE mutation tx (with 40001 retry, M4) and stored in a shared `pulse_clients` table, scoped per clientId. Any node confirming a client's mutation reads/writes the same authoritative counter; a process-local counter would corrupt rebase across nodes/failover. On failover, the client resync-refetches and re-reads `last_mutation_id` from Postgres, so no exactly-once confirmation is lost.
5. **At-least-once delivery** comes from the SSE ring buffer + `Last-Event-ID` and slot replay, not the bus (see §3.5). A long-offline client whose ring buffer rolled past gets a `resync` event and refetches.
6. **Atomic multi-table commits.** One `ChangeSet` = one commit must travel as one bus message (single subject, all changes in one message). If the bus used per-table subjects, a multi-table mutation could be observed table-by-table out of order on a node, breaking the global LSN order the model depends on.

---

## 5. Do now vs defer

The principle: introduce the **interface boundaries** now (cheap, mostly zero runtime change in single-node mode) so the distribution layer is dependency injection / config later, not surgery in `main.rs`. Defer the actual distributed *machinery* until a second node is real.

### 5.1 Do now (M0–M5 seams)

| Seam | When | Cost | What | Why it must be now |
|---|---|---|---|---|
| **S1 — Invalidate via `ChangeSet`, not `HashSet<String>`** | M2 | low | In `main.rs`, instead of turning `res.writes` into a `HashSet<String>` and calling `invalidate()` (`143-147,155`), synthesize a `pulse_core::ChangeSet` (table-level; `commit_lsn` = a local monotonic counter until real LSNs) and feed it through one fn `apply_change_set(ChangeSet)`. Match with `ReadSet::matches` (already exists). | Highest leverage. This is the exact signature the bus consumer and `pulse-cdc` will later call. Adding the bus becomes "spawn a task that calls `apply_change_set`" instead of rewriting the reactor. |
| **S2 — Reactor behind a trait in `pulse-reactor`** | M2 | low | Extract `register_client`/`remove_client`/`add_subscription`/`remove_subscription`/`push`/matching from `reactor.rs` into a `Reactor` trait in the `pulse-reactor` crate (currently a stub). Today's impl becomes `LocalReactor`; `AppState` holds `Box<dyn Reactor>` (or generic). Only invalidation entrypoint is `apply_change_set(ChangeSet)`; only delivery entrypoint is `push(client, payload)`. | Zero behavior change, but every future impl (`ShardedReactor`, bus-backed) slots in without touching `main.rs` call sites. |
| **S3 — Single-writer-shaped change producer** | M2/M3 | low | Have the worker hand the `ChangeSet` to one "invalidation source" object rather than spawning `invalidate()` inline per request (`main.rs:146`). | That object is trivially swapped for "subscribe to the bus" later; it is where the `pulse-cdc` leader plugs in (CDC emits into the same source). |
| **S4 — `id:<seq>` + `commit_lsn` on every SSE event** | M2 | near-zero | Today `push_payload` (`main.rs:116-118`) emits `{sub,data}` and the event has no id (`main.rs:185`). Change payload to `{sub,data,lsn}` and use `Event::default().id(seq)`. Extend `sync.ts handleEvent` (`payload` shape at line ~105) to parse it. | Prerequisite for *both* resumability (S6) and cross-query LSN batch-advance (§4.3). Free single-node; impossible to retrofit cheaply later. |
| **S5 — Durable, stable, server-acknowledgeable `clientId`** | M2/M4 | ~10 lines | Today `makeClientId` is `Math.random` per page load (`sync.ts:5-7`) with no affinity/auth binding. Persist it (localStorage/IndexedDB), send it on `/rpc` as well as `/sync`+`/subscribe`, have the server echo/assign it. | This is the gateway's shard/routing key and the rebase identity. Stabilizing it now avoids a client protocol break later. |
| **S6 — Stub the resume protocol** | M2 (protocol) → M5 (buffer) | low now | Client sends `Last-Event-ID` on `/sync` reconnect (`sync.ts runStream` builds `sync?clientId=` with no resume header); make `runStream`'s `catch` actually reconnect (today it just `markConnected()` and exits, `sync.ts:126-129`). Server accepts and ignores `Last-Event-ID` until the `pulse-sse` ring buffer lands. | The hook that lets a reconnect hit ANY node and recover — the thing that makes engine nodes disposable. |
| **S7 — `last_mutation_id` in shared Postgres** | M4 | low | Create a `pulse_clients` table (per-clientId `last_mutation_id`, optional resume cursor); advance it in the same SERIALIZABLE mutation tx (§4.3). Never a process-local counter. | Spec already wants this in-tx; just ensure the *home* is Postgres so it is node-agnostic from day one. Retrofitting a process-local counter to multi-node corrupts rebase and is expensive. |
| **S8 — Route all of a client's calls through one front door, sticky-by-clientId** | M2 | no-op proxy | Document sticky-by-clientId as a hard routing requirement (not "recommend HTTP/2"). Single-node: a transparent proxy. | Reserves the seam where the gateway's ring lookup drops in, and prevents the split-brain where `/subscribe` lands on a different node than `/sync` (the current `reactor.push` returns-false bug). |
| **S9 — Store read-sets as `pulse_core::ReadSet` keyed by `SubscriptionId`** | M3 | low | When key/range read-sets land, store them as `ReadSet` (the type exists) keyed by a `SubscriptionId = hash(path,input,clientId)`. | Makes the registry shard-partitionable by clientId later with no reshaping. |
| **S10 — Engine nodes stateless w.r.t. auth** | M2+ | low | Keep auth/identity derivation per-request from headers (`collect_headers`, `main.rs:89-97`); do not cache session state in the reactor process. | A disposable node must be able to serve any client's request; node-local auth state would re-introduce stickiness as a correctness requirement. |
| **S11 — Role flag behind config** | M2 | config only | A `PULSE_ROLE` env (`engine` \| `change-router` \| `all-in-one`), default `all-in-one` (one process opens the slot AND serves SSE). | Single-node keeps working, but the code paths are already separated so deploying router separately is config, not refactor (mirrors Zero's replication-manager vs view-syncer split). |

### 5.2 Defer to post-v1 (until a 2nd node is real)

| Item | Why safe to defer |
|---|---|
| The actual change bus (NATS / Redis Streams / Kafka) | Single-node uses the in-process invalidation source (S3). Do NOT defer the `apply_change_set` seam it plugs into. Picking the product waits until a 2nd node exists. |
| Leader election + hot standby for the change router and single-slot enforcement | All-in-one mode owns the slot trivially. Add a leader lease (Postgres advisory lock or the bus's leader primitive) only when the router role deploys separately. |
| Real `pgoutput` decoding in `pulse-cdc` | The in-engine write-set keeps feeding `apply_change_set` through M2–M4. The WAL consumer is needed for (a) out-of-band writes and (b) multi-node, so it lands with scale-out. `ChangeSet` is already the target type. |
| `pulse-sse` ring-buffer sizing/eviction + resync heuristics | Ship a fixed-size buffer first; the `id:seq` stamp (S4) is the only piece needed early. |
| `ShardedReactor` (network registry, cross-node push relay) | Deferred behind the `Reactor` trait; `LocalReactor` serves through v1. Only needed for the extreme-fan-out Candidate-C path. |
| Coordinator / service discovery (etcd/Consul), live hash ring, rebalancing, drain/handoff | Bootstrap later with Postgres advisory rows if needed; nothing to rebalance at N=1. |
| Cross-client / cross-node subscription multiplexing + result diffing → deltas | Throughput optimization (Hasura-style). Single-shape re-exec is correct meanwhile; slots into `pulse-reactor` without changing the bus contract. |
| Targeted cross-node push (deliver to a client whose SSE lives elsewhere) | Sticky routing + resync covers correctness; this only removes the stickiness requirement entirely. Defer until stickiness proves painful. |
| OLAP/replica pool split + PgBouncer transaction pooling + global connection cap (§6/§7.3) | Orthogonal to reactive fan-out. Size per node now; cap globally and add PgBouncer in front when multi-node lands so N × max_conns cannot exhaust Postgres. |

---

## 6. Migration path: single-node → target

Each stage is independently shippable and leaves the system working.

**Stage 0 — today (M1/M2 thin slice).** Single process: in-RAM reactor, write-set invalidation, one Bun worker, one pool. Honest baseline.

**Stage 1 — internalize the seams (M2, no topology change).** Apply S1, S2, S3, S4, S8, S11. After this the reactor consumes `ChangeSet`s through `apply_change_set`, lives behind the `Reactor` trait in `pulse-reactor`, SSE events carry `id:seq` + `commit_lsn`, and the binary has a role flag — all in `all-in-one` mode. Verify: existing M2 reactivity tests still pass; a synthesized `ChangeSet` round-trips through `LocalReactor` and pushes identically to today.

**Stage 2 — client durability + rebase home (M4/M5).** Apply S5, S6 (protocol half), S7, S9, S10. clientId is durable and sent everywhere; `last_mutation_id` lives in `pulse_clients`; read-sets are typed `ReadSet`. Verify: reload preserves clientId; mutation confirmation reads/writes the Postgres watermark; client sends `Last-Event-ID` and reconnects (server may still ignore the header).

**Stage 3 — real change source (M2-full / M7).** Build `pulse-cdc`: a single process holding the slot, decoding `pgoutput` → `ChangeSet`, calling `apply_change_set` in-process (still one node). Replace the synthesized write-set ChangeSet with the real WAL-derived one. Verify: out-of-band SQL writes (not via Pulse) now invalidate subscriptions — impossible in Stage 0.

**Stage 4 — split roles + introduce the bus (first multi-node).** Deploy `change-router` as its own process (leader-elected, sole slot owner) and ≥2 `engine` processes. Router publishes `ChangeSet`s to the bus; each engine subscribes and calls `apply_change_set` from the bus instead of in-process. Put the gateway in front (sticky-by-clientId). Finish `pulse-sse` ring buffer so `Last-Event-ID` actually replays. Verify (the acceptance test for the whole effort): a write via `/rpc` on engine A pushes the updated result to a subscriber whose `/sync` stream lives on engine B — the failure mode that is broken today.

**Stage 5 — harden + optimize (post-v1).** Add hot-standby for the router, OLAP/replica pool split + PgBouncer + global connection cap, cross-client/cross-node multiplexing and result diffing, hot-shard mitigation, and (only if extreme fan-out demands it) the `ShardedReactor` distributed-registry path. Each is additive behind the seams from Stages 1–2.

---

## Key risks (carried from the assessment)

- **Single logical slot is single-consumer (hard invariant).** Two processes opening the slot breaks replication; an abandoned slot pins WAL. The single-owner / leader constraint must be enforced before the router role scales beyond one. This is the #1 footgun.
- **Lossy bus + missing resume = silent staleness.** An at-most-once bus is only safe *after* S4/S6 land. Order of operations: do the SSE resume seam before relying on a lossy bus.
- **Sticky misroute = silent non-delivery.** Until resume + Postgres rehydrate are in place, a `/subscribe` landing on a different node than `/sync` silently never delivers (`reactor.push` returns false). Enforce and monitor affinity; the resume path is the backstop.
- **Re-exec per node multiplies DB load.** A broadcast bus makes every node match every `ChangeSet`, and a hot query shape re-executes once per node. Bounded by deferred multiplexing / interest-routed delivery; a real cost at scale.
- **Determinism (M4) is load-bearing.** Re-execute-anywhere means non-deterministic handlers produce divergent cached/diffed results across nodes. The determinism sandbox is a prerequisite for trustworthy multi-node re-exec.
- **Snapshot ≥ commit_lsn.** Re-exec must read a snapshot at or past the ChangeSet's LSN, or batch-advance can outrun visible data. Requires M4 transactions.

---

### One-paragraph summary

Pulse today is single-node by construction: both pillars of reactivity — the subscription/SSE registry (`pulse-server/src/reactor.rs`) and the invalidation signal (a write-set captured in-process at `pulse-jsruntime/src/lib.rs:212-226` and fed to `invalidate()` at `main.rs:143-172`) — live only in the process that served a request, so behind a load balancer a write on node A never reaches a subscriber on node B. The target is the ElectricSQL/Zero topology: a single leader-elected WAL consumer (`pulse-cdc`) owns the one Postgres logical-replication slot, decodes `pgoutput` into `pulse_core::ChangeSet`s, and fans them over a shared bus to N stateless, disposable engine nodes that each match against their *local* `pulse-reactor` registry, re-execute on their *local* worker, and push over their *local* `pulse-sse` channels — so the only thing crossing a process boundary is the committed change, traveling node→Postgres→router→bus→all-nodes. Consistency stays globally ordered with `pulse_core::Lsn` as the single global ordering token: every push carries `commit_lsn` + a monotonic `id:seq`, the client batch-advances all subs to the same LSN, and `last_mutation_id` lives per-clientId in a shared Postgres `pulse_clients` table advanced in the mutation tx. The whole plan hinges on landing ~11 cheap seams during M0–M5 (route invalidation through `apply_change_set(ChangeSet)` not `HashSet<String>`, put the reactor behind a `Reactor` trait in `pulse-reactor`, stamp `id:seq`+`commit_lsn` on every SSE event, make `clientId` durable and routable, externalize `last_mutation_id`, stub `Last-Event-ID` resume, keep nodes auth-stateless, gate roles behind `PULSE_ROLE`) so that the actual bus, leader election, `pgoutput` decoding, and distributed registry can be deferred to post-v1 as dependency-injection/config changes rather than a rewrite.
