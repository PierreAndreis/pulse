# 00. Pulse: a reactive platform on standard Postgres

- **Status:** Accepted — this is the foundational thesis of `docs/ARCHITECTURE.md` (§1). Not a deviation; the deviations recorded in §10 (e.g. Bun/Node worker instead of embedded V8, engine-captured write-sets instead of WAL CDC) are implementation shortcuts toward this same vision, not departures from it.

## Context & Problem

The best reactive backends give application developers an excellent programming model: you write `query` / `mutation` / `action` functions, define a schema, call them from a fully type-inferred client, and reads update in realtime without manual cache invalidation. The cost of that DX is usually a **proprietary store with custom MVCC**. The developer's data lives inside the platform's engine, reachable only through its APIs.

From the user's (developer's) perspective this forces an uncomfortable decision at adoption time:

- **Take the DX, give up the database.** No `psql`, no `pg_dump`, no BI tools, no other services querying the same data, no Postgres extensions (PostGIS, pgvector), no raw SQL joins/CTEs/window functions. The reactive model is also restricted to a document query builder, so anything Postgres does well that the builder doesn't expose is simply unavailable. Self-hosting is possible but bespoke.
- **Keep Postgres, give up the DX.** Build reactivity yourself — change capture, read-set tracking, invalidation, push transport, an offline-capable client cache — which is a large, error-prone undertaking that most teams get subtly wrong.

The forcing question for Pulse: **can we deliver that reactive model and end-to-end-typed DX while the database of record stays a standard Postgres the developer fully owns?** If yes, the adoption decision above disappears.

## Decision

Build **Pulse**: a reactive, local-first application platform whose **database of record is standard Postgres**, fronted by a **stateless Rust engine** (`pulse-server`) and consumed through a **TypeScript SDK**.

Concretely, the thesis commits us to these properties (all from `docs/ARCHITECTURE.md` §1):

- **Postgres is the record, with no lock-in.** Data lives in real tables. `psql`, `pg_dump`, BI tools, and other applications can read and write the same database directly. Pulse is a service *in front of* your DB, not a replacement for it; bring-your-own-Postgres.
- **Change source = Postgres logical replication (WAL via `pgoutput`).** The canonical, lossless, replayable change stream — chosen specifically because it hands us changed rows' **primary keys**, which is exactly the granularity read-set invalidation needs. (Triggers / `LISTEN`-`NOTIFY` are rejected as the CDC source; see Alternatives.)
- **Two query surfaces, not one.** A typed **document-style query builder** (the path that yields fine-grained read-sets) *and* **raw SQL** via a tagged template (`ctx.sql`) for full Postgres power. Raw SQL trades read-set precision (falls back to table-level) for expressiveness.
- **Contract-first, oRPC-style API with end-to-end type inference, no client codegen.** A dependency-free `contract` declares procedures and their *kind*; the client imports only the contract *type*. Procedure kind drives runtime, determinism rules, and routing:

  ```ts
  oc.reactive()    // subscribable query: read-only, instrumented, deterministic
  oc.mutation()    // read-write, one serializable tx, invalidates subscriptions
  oc.analytical()  // heavy, non-reactive, routed to a read replica
  oc.action()      // side effects, non-deterministic, Node runtime
  ```

- **Reactivity = WAL-driven CDC → automatic read-set tracking → server-side re-execution → SSE delta push**, with client-side LSN batch-advance for cross-query consistency. Reactive queries update live over SSE; the client batch-advances all its subscriptions to the same `commitLSN` before flushing UI updates so siblings never show torn state.
- **First-class heavy/analytical path**, isolated from the reactive hot path: `analytical` procedures route to an OLAP **read replica** with its own connection pool and timeouts, so a long scan can never stall invalidation or exhaust the reactive connection budget.
- **Local-first TypeScript client** that sits under TanStack Query: a normalized store with a confirmed layer + optimistic overlay, a durable offline mutation queue keyed by a per-client monotonic `mutationID`, and rebase-based reconciliation (server re-runs the mutator; the client never resolves conflicts).

One-line pitch: *a reactive programming model and DX, without giving up Postgres, SQL, or your data.*

## Alternatives Considered

1. **Adopt an existing proprietary reactive store as-is.** Best DX out of the box, but it *is* the problem this product exists to solve: it forecloses Postgres, SQL, extensions, direct access, and ownership. Rejected because removing that lock-in is the entire thesis.

2. **Postgres as record, but CDC via triggers / `LISTEN`-`NOTIFY`.** Tempting because it needs no replication slot. Rejected by the spec (§4.1) as the change *source*: lossy, no replay, ~8KB payload cap, and connection-per-listener. Logical replication is lossless and replayable and yields primary keys for free. (`NOTIFY` is kept only as an optional cheap "wake the WAL reader" nudge.)

3. **Full incremental view maintenance / differential dataflow for v1.** Correct and cheap at steady state, but a very large engine that would constrain which SQL we can support. Rejected for v1; the architecture deliberately leaves a seam to add it later for hot query shapes (re-execution is the v1 mechanism). See §4.1 and §9 "Later".

4. **Run all TypeScript in Node worker processes (no embedded V8).** Simpler to build, but pays IPC + process-startup cost on *every* invalidation re-run — and re-execution is the reactive hot path. The spec's long-term answer is an embedded V8 isolate pool for queries/mutations (cheap ~ms isolate start, in-process read/write-set capture), with Node workers reserved for non-deterministic actions. *Note: the current build (§10) ships the simpler Bun/Node worker for Q/M to reach the M2 reactive slice fastest, behind the `pulse-jsruntime` interface, with embedded V8 still targeted for M4.*

5. **Single query surface (document builder only).** Keeps read-set capture uniformly precise, but throws away the Postgres power that justifies choosing Postgres. Rejected in favor of dual surfaces; raw SQL is the explicit escape hatch and accepts coarser (table-level) invalidation as its cost.

## Consequences

**Pros**
- No lock-in: the developer keeps their data, SQL, extensions, tooling, and the ability to self-host on their own Postgres.
- Logical replication gives a canonical, replayable change stream with primary-key granularity — precise, cheap invalidation matching without inventing a custom transaction log.
- One API surface spans reactive, analytical, and side-effecting work; the analytical path is isolated so heavy queries don't degrade reactive latency.
- End-to-end type inference with no client codegen keeps the DX best-in-class.

**Cons / what it costs us later**
- **Operational surface of Postgres logical replication.** Requires `wal_level=logical`, a publication, and a named slot; an abandoned slot pins WAL and can fill disk. Slot liveness/lag must be monitored (§7.2).
- **Embedding V8 is the heaviest single dependency and the trickiest part of the build** (§7.1). The spec accepts this complexity precisely on the hot path; until then the Bun/Node-worker stand-in carries technical debt to repay at M4.
- **Two read-set granularities to reason about.** The document builder yields key/range precision; raw `ctx.sql` falls back to table-level invalidation, so a raw-SQL subscription can be over-invalidated. Developers must understand this tradeoff.
- **Replica freshness for analytics.** Analytical results are at replica-lag freshness and explicitly must not back live OLTP subscriptions (§6).
- **Determinism contract on Q/M.** Reactive queries and mutations must be deterministic (frozen time, seeded RNG, no `fetch`/fs) so re-execution and result caching stay sound; this constrains what authors may do in those functions.

## Testing Decisions

A good test here exercises **external behavior through the public interface** — the TypeScript SDK against a real Postgres — not engine internals. This matches the prior art already in the repo (§10 "Done"):

- **M1 non-reactive slice** is verified end-to-end through `@pulse/client` against real Postgres: a `mutation` persists and a `list` query reflects it; plus stress tests (concurrent load, pool saturation, worker backpressure, a 15s soak).
- **M2 reactive slice** is verified through the client: a write in one client is pushed to a *separate* subscriber over SSE; multi-client fan-out works; per-subscription isolation holds; unsubscribe stops pushes. This is the concrete test that proves the thesis ("two browser tabs, write in A appears in B within ~150ms, no manual refetch" — §9 M2).
- **Type-level** behavior is verified by `tsc` including a `inference.test-d.ts` typecheck test plus vitest, asserting `InferInputs`/`InferOutputs` produce correct types from the contract alone (§9 M0).
- **Analytical raw SQL** is verified through the client: joins/CTEs/`GROUP BY`/aggregates run via `ctx.sql`.

For this vision-level decision specifically, the durable acceptance test is the chat example (`@pulse/examples-chat`) used as the vertical-slice target: it should run reactively against a stock Postgres that you can simultaneously inspect with `psql`/`pg_dump`, demonstrating "Postgres-as-record + reactive DX" in one flow.

## Out of Scope / Deferred

- **Incremental view maintenance / differential dataflow** as the reactivity mechanism — re-execution is the v1 choice; IVM is a post-v1 seam for hot query shapes (§9 "Later").
- **Hasura-style multiplexing** of popular parameterized queries at scale — later.
- **Horizontal scale-out of the engine** (sharded subscription registry) — later.
- **Contract-first OpenAPI emission** — later.
- Detailed designs for individual subsystems (read-set capture precision, the local-first rebase protocol, the CDC pipeline, the analytical path, runtime choices) belong in their own ADRs; this document fixes only the product thesis and the Postgres-as-record stance.

**Current-state caveats** (per §10, current behavior differs from the full spec): the Q/M runtime is a Bun/Node worker rather than embedded V8; invalidation is driven by engine-captured write-sets rather than WAL CDC (the `pulse-cdc` WAL consumer is the planned source for out-of-band writes); read-set matching is table-level (key/range precision pending); analytical queries currently use the OLTP pool rather than a dedicated replica pool; mutations are autocommit per op rather than one serializable tx; and the local-first client is built as of M5 (durable offline mutation queue, optimistic overlay with rollback + rebase, pluggable InMemory/IndexedDB persistence), with SSE `Last-Event-ID` resume the remaining piece. These are sequencing decisions toward the same vision, not reversals of it.
