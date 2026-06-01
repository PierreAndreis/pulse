# 05. Reactivity — Engine-Captured Write-Sets + SSE (WAL/CDC Deferred)

- **Status:** Accepted — deviation from `docs/ARCHITECTURE.md` §4 / §7.2, which specify **WAL-driven CDC** (`pgoutput` logical replication on a named slot) as the canonical change source feeding a separate `pulse-reactor`/`pulse-sse`/`pulse-cdc` crate split. This ADR records why the M2 thin slice instead drives invalidation from **write-sets captured directly by the engine**, transports over plain SSE without `commitLSN`/`Last-Event-ID`, and keeps the reactor inside `pulse-server`. The deviation is already noted in `ARCHITECTURE.md` §10 ("Reactivity invalidation is engine-captured write-sets, not WAL/CDC (yet)" and "Reactor lives in `pulse-server`, not the `pulse-reactor`/`pulse-sse` crates (yet)").

## Context & Problem

The roadmap's guiding principle is to **get one reactive query updating one browser end-to-end as early as possible (M2)** — that milestone is the one that proves the reactivity thesis. The spec's target reactivity pipeline (§4.3) is substantial: a dedicated `tokio-postgres` replication connection in `START_REPLICATION ... LOGICAL` mode, a hand-rolled-or-crate `pgoutput` binary decoder, a named slot + publication (with `REPLICA IDENTITY` tuning and slot-lag monitoring), and a `ChangeSet { commitLSN, changes: [(TableId, PrimaryKey, Op)] }` flowing into a Subscription Manager that does key-level matching, debounce, dedup, result diffing, and LSN-tagged delta push.

Standing all of that up before a single push reaches a browser would front-load the heaviest plumbing (WAL decoding, slot liveness) against the least-validated design. But there is a structural shortcut available *for the thin slice specifically*: per ADR 01, **every write already goes through the engine** — handler `ctx.db` ops are proxied back over NDJSON and executed by `pulse-sql` inside Rust. The engine therefore already knows, for each mutation, exactly which tables were written, with no WAL parsing at all.

The forcing question: *what is the smallest invalidation source that still produces the same `read-set → match → re-execute → push` pipeline the WAL design feeds, so M2 is reachable without first building CDC?*

## Decision

Drive invalidation from **write-sets captured by the engine during mutation execution**, match them **table-level** against each live subscription's captured **read-set**, **re-run** the matching subscriptions, and **push** the fresh full result over SSE. No WAL, no `pgoutput`, no slot.

**Capture (in the engine, per ADR 01).** Running a procedure yields an `ExecResult` carrying the tables it touched, classified by `DbOp::access() -> Option<(table, is_write)>`:

```rust
pub struct ExecResult {
    pub value: Value,
    pub reads: Vec<String>,    // tables read    → subscription read-set
    pub writes: Vec<String>,   // tables written → invalidation write-set
}
```

Get/Query → read; Insert/Patch/Replace/Delete → write. Raw analytical SQL (`DbOp::Raw`) returns `None` from `access()` — it is opaque to capture (see Consequences).

**Subscription registry.** The reactor (in `pulse-server`) holds, per client, an SSE sender channel, and a map of subscriptions:

```rust
struct Subscription {
    client_id: String,
    sub: String,                  // client-chosen stable key = path + JSON(input)
    path: Vec<String>,            // procedure path, re-executed on invalidation
    input: Value,
    headers: HashMap<String, String>,
    tables: HashSet<String>,      // the read-set
}
```

**Wire protocol (over plain HTTP, CORS-permissive):**

```
GET  /sync?clientId=<id>     → SSE stream; registers the client's push channel
POST /subscribe  { clientId, sub, path, input }
POST /unsubscribe { clientId, sub }
POST /rpc        { path, input }                  // mutations carry the write trigger
```

Each SSE frame is one JSON object `{ "sub": <key>, "data": <result> }`; the client fans it out to that sub's listeners.

**Lifecycle / behaviors that are part of the decision:**

- **Subscribe = execute-then-register-then-push.** On `POST /subscribe` the engine executes the procedure, takes its `reads` as the subscription's `tables`, registers the `Subscription`, and immediately pushes the first result over SSE. So the initial value arrives on the same channel as updates (no separate fetch path).
- **Invalidate on RPC.** When `POST /rpc` returns a non-empty `writes` set, the server spawns a background task that asks the reactor for every subscription whose read-set intersects the write-set (`!tables.is_disjoint(writes)`), re-executes each with its stored `path/input/headers`, and pushes the new result.
- **Table-level matching only.** A write to `messages` re-runs *every* `messages` subscription regardless of predicate/row. This is the explicit M2 granularity (§4.2 falls back to table-level for anything not statically analyzable; here *everything* is table-level).
- **Re-execution reuses the exact stored request.** Same `path`, `input`, and captured `headers` — so auth/middleware run identically on the re-run.
- **Channel cleanup.** A push that fails to send (receiver gone) drops the client and all its subscriptions.
- **Client ordering.** The SDK registers a subscription only *after* the SSE stream's response headers arrive (`ensureStream()` resolves on headers received), so the server has the push channel in place before the initial push — the first result can't be dropped. Subscriptions are de-duped client-side by the canonical query key `path + JSON(input)` (`queryKeyOf`); the SDK registers/releases at most once per key (`SyncClient.ensure`/`release`).
- **Client delivery.** Each push `{ sub, data }` is written into the client's `LocalStore` confirmed layer (`setConfirmed(sub, data)`); the store recomputes its materialized view (confirmed with pending optimistic updaters replayed on top) and notifies that key's listeners. The reactive SSE result is thus the *confirmed* state the local-first overlay rebases against — the same `sub` key the server pushes is the `queryKeyOf` cache key.

**What is deliberately absent vs the spec** (and currently differs from `ARCHITECTURE.md` §4.3/§5/§7.2):

- No `commitLSN` on pushes and no client **LSN batch-advance** — cross-query consistency (§4.4) is not yet provided; each subscription updates independently.
- No SSE event `id:` / `Last-Event-ID` resume, no per-subscription **ring buffer**, no `resync` event. Delivery is best-effort while connected; the SDK comments note reconnection is M5.
- No **debounce / dedup / result diffing → deltas** — full results are pushed every time (matches the M2 plan: "push results (full results, not yet deltas)").
- WAL writes made **outside** the engine produce no invalidation today — there is no change source for them yet.

## Alternatives Considered

- **WAL CDC now (`pgoutput` logical replication) — the spec target.** The canonical, lossless, replayable change stream, and the *only* source that also catches writes made outside Pulse (psql, other services, triggers). Rejected for the thin slice: it front-loads the heaviest, least-validated plumbing (binary `pgoutput` decode, slot + publication setup, `REPLICA IDENTITY` tuning, slot-lag monitoring to avoid pinning WAL → disk fill) before a single push reaches a browser. It remains the planned upgrade and plugs into the *same* `read-set → match → re-execute → push` pipeline this slice already exercises — only the change *source* swaps.
- **`LISTEN`/`NOTIFY` or triggers as the change source.** Cheaper than WAL to stand up. Rejected for the same reason the spec rejects it (§4.1): lossy, no replay, 8 KB payload cap, connection-per-listener. Engine-captured write-sets are strictly simpler than even NOTIFY for the in-engine-writes case and have none of those caps.
- **Key/range-level read-sets now (precise matching).** Avoids re-running unrelated subscriptions on the same table. Rejected for M2: it needs the instrumented query-builder read-set extraction in `pulse-sql` and is explicitly the M3 milestone. Table-level is coarse but correct, and correctness-first is the milestone's goal.
- **WebSocket transport instead of SSE.** Bidirectional, avoids the separate `POST /subscribe` control channel. Rejected: SSE is the spec's chosen transport (§5), server→client only is sufficient (subscribe/unsubscribe/auth ride normal `POST`), and a fetch-readable SSE stream needs no extra protocol — it also gives free `Last-Event-ID` resume when that lands (M5).
- **Extract `pulse-reactor` / `pulse-sse` / `pulse-cdc` crates now (the spec's crate split).** The eventual structure. Deferred as functionality-first: the reactor is a single module in `pulse-server`. Extraction is a pending refactor, not a behavior change.

## Consequences

Pros:
- Reached the M2 reactive slice with **zero CDC infrastructure** — invalidation is a `HashSet` intersection over data the engine already had.
- The pipeline shape (`read-set → match → re-execute → push`) is exactly the one WAL will feed, so the CDC upgrade swaps the *source* without reworking matching, re-execution, or transport.
- Precise for in-engine writes: the write-set is the literal set of tables the mutation touched — no `REPLICA IDENTITY`/PK-decode subtleties.
- Transport is plain SSE over `fetch`, debuggable and CORS-friendly, with no extra framing.

Cons / costs later:
- **Blind to out-of-band writes.** Any write not made through a Pulse mutation (raw psql, another service, a DB trigger) triggers no invalidation. This is the standing reason WAL CDC is still owed — it is the only lossless source for those.
- **Raw SQL is a capture hole.** `DbOp::Raw` returns `None` from `access()`, so a raw read does not register its tables in the read-set and a raw write would not register its tables in the write-set. Reactive correctness currently depends on handlers using the query builder, not `ctx.sql`. (Consistent with §4.2's "raw SQL trades precision for power," but here it trades *correctness*, not just precision.)
- **Over-invalidation.** Table-level matching re-runs every subscription on a written table; on a hot table this re-executes — and re-pushes full results for — subscriptions whose rows did not change. Key/range matching + diffing (M3) is the fix.
- **No consistency watermark.** Without `commitLSN` batch-advance, two related subscriptions can update on different ticks → the UI can briefly show torn state across components. This is the §4.4 guarantee, owed at M3.
- **No resume / at-least-once.** No ring buffer or `Last-Event-ID`; a push during a dropped connection is lost until M5 adds resume.
- **Re-execution cost is the hot path.** Each invalidation re-runs handlers through the NDJSON worker round-trip (ADR 01) — the same IPC cost the embedded-V8 (M4) work targets.
- **Single-process reactor.** State (`clients`, `subs`) is in-memory in `pulse-server` behind `Mutex`; it does not survive restart and does not span multiple engine instances — horizontal scale is out of scope here.

## Testing Decisions

Test **observable reactive behavior through the public surface** — the client's `subscribe()` (or `POST /subscribe` + the `GET /sync` SSE stream) against a real engine + real Postgres — not the reactor's internal maps, the intersection function, or the SSE frame bytes. Those internals are free to change when WAL/key-level matching land.

A good test here asserts end-to-end pushes:
- **Cross-client push:** client A subscribes to a query; client B issues a mutation that writes the read table; A receives the updated result over SSE without refetching.
- **Initial push:** a fresh subscribe delivers the current result on the SSE channel (not only on later writes).
- **Per-subscription isolation (table-level boundary):** a write to table X must *not* push to a subscription whose read-set is only table Y. (The complementary "write to channel A doesn't re-run channel B's subscription" — i.e. *row*-level isolation — is an M3 assertion, since today both are table `messages`.)
- **Multi-client fan-out:** N subscribers to the same query all receive the push.
- **Unsubscribe stops pushes:** after `POST /unsubscribe`, a subsequent qualifying write produces no further push to that sub.
- **No false invalidation:** a mutation that writes only table X does not re-run a subscription reading only table Y.

Prior art is the M2 end-to-end verification recorded in `ARCHITECTURE.md` §10 and the ADR 01 testing section: example-chat `send`/`list` exercised through `@pulse/client` against real Postgres, plus the M2 reactivity tests (write in one client → pushed to a separate subscriber; multi-client fan-out; per-subscription isolation; unsubscribe stops pushes). New reactivity behavior should be verified at that same end-to-end level rather than by asserting on `Subscription.tables` or SSE frame layout.

## Out of Scope / Deferred

- **WAL CDC (`pulse-cdc`)** — `pgoutput` logical-replication consumer for writes made outside the engine; the planned canonical source, feeding the same pipeline. (§4.3, §7.2)
- **Key/range-level read-sets + result diffing → deltas** — instrumented query-builder read-set extraction in `pulse-sql`; replaces table-level matching and full-result pushes. (M3)
- **Cross-query consistency** — `commitLSN` on every push and client LSN batch-advance so related subscriptions reveal atomically. (§4.4, M3)
- **SSE resume** — event `id:`, per-subscription ring buffer, `Last-Event-ID` replay, and `resync` fallback. (§5, M5)
- **Raw-SQL read/write-set capture** — making `ctx.sql` participate in invalidation instead of being opaque.
- **Reactor/SSE/CDC crate extraction** — splitting the in-`pulse-server` reactor into `pulse-reactor` / `pulse-sse` / `pulse-cdc`; a pending refactor, not a behavior change. (§10)
- **Reconnection, debounce, cross-client dedup, and multi-instance reactor state** — single-process, best-effort-while-connected for now.
