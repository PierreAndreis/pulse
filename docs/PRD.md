# PRD — Pulse: a reactive, local-first platform on standard Postgres

> Status: master product PRD, synthesized from `docs/ARCHITECTURE.md` and the ADRs in `docs/decisions/`. It describes the full target product and explicitly flags where the current implementation has not yet reached, or deliberately deviates from, the spec. Where an implementation detail encodes a decision more precisely than prose, it is summarized; see the linked ADRs for the full reasoning.

## Problem Statement

A developer building a realtime, collaborative application today faces a forced choice they shouldn't have to make.

- **Take the DX, give up the database.** The best reactive platforms offer an excellent model — write `query` / `mutation` / `action` functions, define a schema, call them from a fully type-inferred client, and reads update in realtime with no manual cache invalidation. The price is usually a proprietary store with custom MVCC. The developer's data lives *inside* the platform, reachable only through its APIs: no `psql`, no `pg_dump`, no BI tools, no other services on the same data, no Postgres extensions (PostGIS, pgvector), no raw SQL joins/CTEs/window functions, and a document query builder that can't express what Postgres does well. Self-hosting is bespoke.
- **Keep Postgres, give up the DX.** Build reactivity yourself — change capture, read-set tracking, invalidation, a push transport, an offline-capable client cache with optimistic updates and conflict reconciliation. This is a large, error-prone undertaking that most teams get subtly wrong.

The developer also wants the same single, typed API surface to serve three very different kinds of work — live reactive reads, transactional writes, and heavy analytical aggregates — without a long analytical scan stalling the realtime path, and without losing type safety or hand-writing DTOs and cache keys.

The forcing question: **can we deliver that reactive programming model and end-to-end-typed DX while the database of record stays a standard Postgres the developer fully owns?**

## Solution

**Pulse** is a reactive, local-first application platform whose **database of record is standard Postgres**, fronted by a **stateless Rust engine** (`pulse-server`) and consumed through a **TypeScript SDK**.

You author a dependency-free *contract* describing your procedures and their *kind* (`reactive` / `mutation` / `analytical` / `action`), define your schema in TypeScript, and implement handlers against the contract with composable, oRPC-style middleware. The client imports only the contract *type* and gets fully-inferred, codegen-free calls that plug into TanStack Query. Reactive queries update live over SSE. Mutations apply optimistically and survive going offline. Heavy analytical queries run on the same API surface but on an isolated execution path. And because the data lives in real Postgres tables, `psql`, `pg_dump`, BI tools, extensions, and other applications all keep working — no lock-in.

The one-line pitch: *a reactive programming model and DX, without giving up Postgres, SQL, or your data.*

The product thesis and the Postgres-as-record stance are fixed in **ADR 00**. The remaining ADRs (01–08) fix the subsystem decisions and record current-state deviations.

## User Stories

### Schema authoring

1. As an app developer, I want to define my tables and their columns in TypeScript with a `v` validator builder, so that I get runtime validation and static types from one source of truth.
2. As an app developer, I want `_id` and `_creationTime` auto-injected on every table, so that I don't hand-roll primary keys and timestamps.
3. As an app developer, I want to declare typed foreign keys with `v.id("otherTable")`, so that references are checked at the type level and resolve to the right table.
4. As an app developer, I want to declare named indexes with `.index(name, [cols])`, so that the engine generates a Postgres B-tree index and (eventually) captures index-range read-sets.
5. As an app developer, I want optional fields via `v.optional(...)` and unions/literals via `v.union`/`v.literal`, so that I can model real-world nullable and enum-like columns.
6. As an app developer, I want `Doc<"table">` and `Id<"table">` types available in my handlers, so that the rows I read and write are precisely typed.
7. As an app developer, I want `Doc<T>` to resolve to the concrete fields of my table without creating a circular type through the validator builder, so that my project type-checks cleanly. *(Decided in ADR 06: `Doc<T>` resolves against an augmentable `PulseDataModel` interface emitted as literal members, not via `DataModelFromSchema<typeof schema>`.)*
8. As an app developer, I want `pulse dev` / `pulse deploy` to diff my schema against the live DB and emit a SQL migration validated against existing rows, so that I can evolve my schema safely (reject lossy changes unless `--allow-data-loss`). *(Target — CLI not yet built; see Out of Scope.)*
9. As an app developer, I want schema→type codegen for `Doc`/`Id` to be distinct from client-call types, so that the client surface needs no codegen at all.

### Queries (reactive reads)

10. As an app developer, I want to mark a query `oc.reactive()` in the contract, so that it becomes subscribable and its result updates live.
11. As an app developer, I want a document query builder (`ctx.db.query("t").withIndex(...).order(...).take(n)` plus `collect`/`first`/`unique`), so that I can express common reads ergonomically without writing SQL.
12. As an app developer, I want `ctx.db.get(id)` to do a point lookup by a self-describing `table:uuid` id with no separate table argument, so that reads are concise and ids are unambiguous on the wire. *(ADR 03.)*
13. As an app developer, I want every read through `ctx.db` to be automatically captured into a read-set by the engine, so that the system knows which subscriptions to re-run when data changes — with no manual invalidation. *(ADR 03 / ADR 05.)*
14. As an app developer, I want my query results to come back as precisely-typed documents (text-cast values decoded back to numbers/booleans/JSON/ids), so that I get correct runtime values and types. *(ADR 03.)*
15. As an app developer, I want `unique()` to error when more than one row matches, so that single-row invariants are enforced at read time.
16. As an app developer, I want predicate filtering (`eq/gt/gte/lt/lte`, AND-combined) and ordering, so that I can scope a list query. *(Current builder supports these; `OR`/`IN`/`LIKE`/`IS NULL`/JSONB ops/arbitrary order/offset are not yet in the document builder — raw `ctx.sql` is the escape hatch. See Out of Scope.)*
17. As an app developer, I want index selection and true range scans behind `.withIndex(...)`, so that reads are efficient and read-sets are fine-grained. *(Partial — `withIndex(name)` now orders by the index's declared columns; true index-range scans and the fine-grained read-set they enable are still future. See Out of Scope.)*

### Mutations (transactional writes)

18. As an app developer, I want to mark a write `oc.mutation()` in the contract, so that it gets a read-write `ctx.db` and invalidates affected subscriptions.
19. As an app developer, I want `ctx.db.insert("t", {...})` to persist a row and return its new id, so that I can create records and reference them immediately.
20. As an app developer, I want `ctx.db.patch(id, {...})` to update only the provided fields, so that partial updates are easy.
21. As an app developer, I want `ctx.db.replace(id, {...})` to fully replace a row — nulling columns absent from the map — so that I get true replace semantics distinct from patch. *(Shipped — `replace` now writes every user column, setting omitted ones to `NULL` while preserving `_id`/`_creationTime`; an omitted `NOT NULL` field surfaces a constraint error. Distinct from `patch`, which touches only provided fields. See ADR 03.)*
22. As an app developer, I want `ctx.db.delete(id)` to remove a row, so that I can delete records.
23. As an app developer, I want each mutation to run inside one `SERIALIZABLE` Postgres transaction with `40001` retry, so that concurrent conflicting writes serialize correctly and a deterministic handler can be safely re-run. *(Target — mutations are currently autocommit per op; one serializable tx + retry is the M4 milestone. See ADR 01 and Out of Scope.)*
24. As an app developer, I want my mutation's write-set captured by the engine, so that exactly the subscriptions reading those tables are re-run. *(ADR 05.)*
25. As an app developer, I want the engine to advance a per-client `last_mutation_id` in the same transaction as my mutation, so that the client has a reliable confirmation watermark for rebase. *(Target — not yet implemented; see ADR 07 and Out of Scope.)*

### Actions (side effects)

26. As an app developer, I want to mark a procedure `oc.action()` and put it in a `"use action"` file, so that it runs in a Node worker with full npm access for external I/O (Stripe, OpenAI, fetch, fs).
27. As an app developer, I want actions to be non-deterministic and non-reactive, so that they can do real-world side effects without violating the determinism contract that reactive re-execution depends on.
28. As an app developer, I want `ctx.runQuery` / `ctx.runMutation` inside an action, so that an action can read and write through the engine after doing external work. *(Target — the action runtime, `pulse-actions` Node worker pool, and re-entry are the M6 milestone; not yet built. See Out of Scope.)*
29. As an operator, I want a wedged action to crash only its own worker, not the engine, so that one bad action can't take down realtime traffic. *(Target — crash isolation is part of the M6 action-pool design.)*

### Reactive subscriptions (realtime delivery)

30. As an end user, I want a list view to update in realtime when another user adds or changes data, so that I see live state without refreshing.
31. As an app developer, I want `useQuery(pulse.x.y.queryOptions({ input }))` on a reactive procedure to auto-subscribe over SSE, so that pushed updates flow straight into the cache with no extra wiring.
32. As an app developer, I want the initial query result to arrive on the same SSE channel as later updates, so that there is no separate fetch/subscribe race. *(ADR 05: subscribe = execute-then-register-then-push.)*
33. As an app developer, I want identical `(path, input)` subscriptions to be de-duplicated client-side, so that two components reading the same query share one stream. *(ADR 05 / ADR 06: keyed by `queryKeyOf(path, input)`.)*
34. As an end user, I want a write in one tab to appear in another tab within ~150ms over SSE, so that collaboration feels instant. *(This is the M2 acceptance test — verified.)*
35. As an app developer, I want a subscription to be re-run only when a write touches a table it read, so that unrelated writes don't churn it. *(Current matching is table-level: any write to a read table re-runs the subscription. Row/key precision is M3 — see below.)*
36. As an app developer, I want unsubscribing to stop further pushes, so that closed views stop consuming work.
37. As an app developer, I want a subscription re-run to never leak another subscriber's data, so that per-subscription isolation holds across clients. *(ADR 05 — verified.)*
38. As an app developer, I want pushes to carry deltas (diffs) rather than full results once data is large, so that the wire stays cheap. *(Target — full results are pushed today; diffing → deltas is M3. See Out of Scope.)*
39. As an end user, I want related components to update together (never one reflecting a write while a sibling lags), so that the UI never shows torn state. *(Target — `commitLSN` on pushes + client LSN batch-advance for cross-query consistency is M3; today each subscription updates independently. See ADR 05 and Out of Scope.)*
40. As an end user, I want my subscriptions to resume after a dropped connection without losing pushes, so that a brief disconnect doesn't desync the UI. *(Target — SSE event `id:` / `Last-Event-ID` resume, per-subscription ring buffer, and `resync` fallback are M5; delivery is best-effort-while-connected today. See ADR 05 and Out of Scope.)*
41. As an end user, I want a write made *outside* Pulse (raw `psql`, another service, a trigger) to still invalidate my subscriptions, so that the UI reflects the true database state. *(Target — this requires WAL CDC; engine-captured write-sets are blind to out-of-band writes today. See ADR 05 and Out of Scope.)*

### Analytical queries (heavy reads)

42. As an app developer, I want to mark a heavy query `oc.analytical()`, so that it is treated as non-reactive and routed to an isolated execution path.
43. As an app developer, I want a raw SQL escape hatch via the `ctx.sql` tagged template, so that I can use joins, CTEs, `GROUP BY`, aggregates, window functions, and Postgres extensions for analytics. *(ADR 04.)*
44. As an app developer, I want to interpolate values (including `Id<"t">`) into `ctx.sql` as bound parameters, so that I avoid SQL injection and ids decode to their bare uuid automatically. *(ADR 04: positional binds, text-based, with a `prefix:uuid` id-decode heuristic.)*
45. As an app developer, I want arbitrary analytical result shapes to decode without a catalog lookup, so that any projection (even columns that exist in no table) round-trips to my `Row` type. *(ADR 04: the engine wraps the query as `SELECT to_jsonb(__pulse_sub) ...`.)*
46. As an app developer, I want analytical queries to never register a read-set or block invalidation, so that a long scan can't stall the realtime path. *(ADR 04: `DbOp::Raw` is opaque to the reactor.)*
47. As an operator, I want analytical procedures routed to a dedicated OLAP read replica with its own connection pool and longer `statement_timeout`, so that a 30-second scan can never exhaust the reactive connection budget or stall invalidation. *(Target — analytical queries currently run on the OLTP pool; replica isolation is M6. See ADR 04 and Out of Scope.)*
48. As an app developer, I want large analytical results streamed as an async iterator / paginated via `infiniteOptions`, so that big result sets don't have to materialize at once. *(Target — M6. See Out of Scope.)*
49. As an app developer, I want raw SQL to be enforced read-only (read-only tx / replica), so that a writing CTE can't silently bypass invalidation. *(Target — read-only is by convention only today; enforcement is tied to the deferred replica work. See ADR 04.)*

### Offline / local-first

50. As an end user, I want a mutation I fire to apply to the UI immediately (optimistically), so that the app feels instant even before the server confirms. *(ADR 07: optimistic overlay.)*
51. As an end user, I want my optimistic write rolled back automatically if the server's *handler* rejects it (a real business error), so that the UI converges to truth without manual `onError`/`onSettled`. *(ADR 07.)*
52. As an end user, I want a write I make while offline to be durably queued and replayed later in order, even across a full page reload, so that no write is ever lost. *(ADR 07: durable FIFO queue persisted before the network send; per-client monotonic mutation id.)*
53. As an app developer, I want a clear distinction between a network failure (keep queued, retry) and a handler rejection (roll back, surface error), so that transient outages don't drop writes and deterministic rejections don't retry forever. *(ADR 07.)*
54. As an end user, I want remaining pending writes to stay correctly applied on top whenever a confirmation lands or fresh confirmed data arrives, so that confirming one write doesn't disturb the others. *(ADR 07: eager replay-on-top rebase via `recompute()`.)*
55. As an end user, I want optimistic rows to keep a stable identity across rebases, so that they don't flicker or duplicate. *(ADR 07: deterministic `tempId` per `(mutation id, call index)`.)*
56. As an app developer, I want persistence to work in the browser (IndexedDB) and degrade gracefully to in-memory in SSR/tests, so that the same code runs everywhere. *(ADR 07: `KVStore` with `IndexedDbKV` / `InMemoryKV` / `defaultKV()`.)*
57. As an end user, I want my reload-restored queue to flush automatically when I come back online or reopen the app, so that queued writes actually replay without me doing anything. *(Target — `flush()` is invoked only from `mutate()`; there is no `online`/startup trigger yet, so the queue is durable but not auto-replayed. See ADR 07 and Out of Scope.)*
58. As an app developer, I want the local-first store wired into `createClient` and the TanStack Query adapter, so that the public client surface actually uses optimism + the queue. *(Target — the modules exist but `createClient` only constructs `SyncClient` today. See ADR 07 and Out of Scope.)*
59. As an end user, I want the client not to momentarily revert a still-pending write when a confirmed pull lands, so that I never see flicker. *(Target — the PowerSync-style write-checkpoint guard / `lastMutationID` watermark is M5. See ADR 07 and Out of Scope.)*
60. As an app developer, I want mutations designed as intent (`insert`, `increment`, `set if version matches`) so that replay against newer server state is correct and the client never resolves conflicts — the server re-runs the mutator. *(Design guidance from the spec; conflict resolution is "server re-runs the mutator.")*

### Middleware / auth

61. As an app developer, I want to compose reusable middleware with an immutable `os` builder, so that I can share an `authedBase` across many procedures without one `.use()` disturbing another. *(ADR 06.)*
62. As an app developer, I want a middleware to *require* values in the incoming context (`os.$context<{ headers }>()`), so that misuse is a type error.
63. As an app developer, I want a middleware to *widen* the downstream context via `next({ context: { user } })`, so that handlers after `.use(authed)` see `ctx.user` typed with no annotation. *(ADR 06: `Useable<TIn, TOut>` tracks the full out-context.)*
64. As an app developer, I want middleware to wrap the handler (pre/post around `next()`), so that I can implement logging, timing, and auth uniformly.
65. As an app developer, I want auth middleware to reject unauthenticated calls with a typed `UNAUTHORIZED` error, so that protected procedures are safe by default.
66. As an app developer, I want re-execution of a subscription on invalidation to run the same middleware with the captured request headers, so that auth/authorization is applied identically on every re-run. *(ADR 05: re-execution reuses the exact stored `path/input/headers`.)*
67. As an app developer, I want calling `next()` twice in one middleware to throw, so that onion-order bugs surface immediately. *(ADR 06.)*

### react-query integration

68. As a React developer, I want `pulse.x.y.queryOptions({ input })` to feed `useQuery`/`useSuspenseQuery`/prefetch, so that reactive procedures slot into my existing TanStack Query app.
69. As a React developer, I want `pulse.x.y.mutationOptions({ onSuccess, optimistic })` to feed `useMutation`, so that mutations integrate with the same patterns.
70. As a React developer, I want `pulse.x.y.infiniteOptions(...)` for paginated/analytical reads, so that I can page through large result sets.
71. As a React developer, I want hierarchical query keys (`.key()` partial for broad `invalidateQueries`, `.queryKey({ input })` exact for `setQueryData`) with client-only context excluded, so that invalidation and dedup behave correctly. *(ADR 06.)*
72. As a React developer, I want the local store to be the source of truth under TanStack Query, so that pushed deltas and optimistic state are reflected without me reconciling caches by hand. *(ADR 05 / ADR 07.)*

### Developer experience (DX)

73. As an app developer, I want to write one dependency-free contract and call it from the client with full type inference and **no codegen**, so that there is no generated client file to drift or rebuild. *(ADR 06: the contract's `typeof` *is* the API; the client ships zero contract bytes via a Proxy.)*
74. As an app developer, I want `InferInput`/`InferOutput`/`InferKind`/`InferErrors` derived from the contract type alone, so that DTOs and handler signatures are never hand-written. *(ADR 06. Note: the spec names these `InferInputs`/`InferOutputs` and the client `RouterClient`; the code exports `Client<C>` and singular inferers — a naming deviation to reconcile.)*
75. As an app developer, I want each handler's `ctx` shaped by its procedure kind (reader for reactive, writer for mutation, reader+`sql` for analytical, runners for action), so that I can only do what the kind allows. *(ADR 06: `KindContext`.)*
76. As an app developer, I want typed error constructors for exactly the errors a procedure declared plus six built-ins (`UNAUTHORIZED`/`FORBIDDEN`/`NOT_FOUND`/`CONFLICT`/`BAD_REQUEST`/`INTERNAL`), so that I throw and catch errors type-safely with structured `data`. *(ADR 06.)*
77. As an app developer, I want my `console.log` in a handler to go somewhere visible (operator stderr) without corrupting the engine↔worker protocol, so that I can debug handlers safely. *(ADR 02: `console.*` redirected to stderr; note direct `process.stdout.write` is not yet caught — a dedicated protocol fd is deferred.)*
78. As an app developer, I want the same procedure-call surface to span reactive, mutation, analytical, and action kinds, so that I learn one API for all server work.
79. As an app developer, I want to run my chat example reactively against a stock Postgres that I can simultaneously inspect with `psql`/`pg_dump`, so that "Postgres-as-record + reactive DX" is demonstrably true in one flow. *(ADR 00: the `@pulse/examples-chat` vertical slice is the durable acceptance test.)*
80. As an operator, I want Pulse to be a stateless Rust service in front of my own Postgres (bring-your-own-DB, self-hostable), so that I keep full ownership and avoid lock-in. *(ADR 00.)*

## Implementation Decisions

The decisions below are recorded in full in `docs/decisions/`; this section summarizes each and links it by number. The product thesis and Postgres-as-record stance are **ADR 00**.

- **Engine / runtime split (ADR 01).** A Rust engine (`pulse-server`) owns Postgres, SQL lowering, and read/write-set capture; user query/mutation handlers run in a supervised Node/Bun worker whose instrumented `ctx.db` proxies every db op back to the engine over a line protocol, returning `ExecResult { value, reads, writes }`. *Deviation:* the spec targets an embedded V8 isolate pool (`deno_core`) with a deterministic sandbox for cheap, in-process re-execution; that is deferred to M4 behind the `pulse-jsruntime` interface. Cost carried in the meantime: IPC on the invalidation hot path, no determinism sandbox, a single shared worker.

- **Engine↔worker wire protocol (ADR 02).** NDJSON (one JSON object per line) over the worker's stdio, with stderr inherited. Two-level correlation: a UUID `requestId` per execute, a monotonic `opId` per db op within a request. Read/write-set capture happens engine-side *before* the db reply, guaranteeing it is complete before the handler can finish. The soak fix: `console.*` is overridden to write to stderr so handler logging cannot corrupt the protocol stream, and all protocol writes are serialized through one promise chain. *Known holes:* direct `process.stdout.write` is not guarded (a dedicated protocol fd is deferred); there is no per-request execution timeout and correlation maps are unbounded on worker-crash/never-completes.

- **Instrumented `ctx.db` + SQL lowering (ADR 03).** `ctx.db` builds no SQL; the engine's `execute_op` is the sole SQL author, which is what keeps read/write-set capture and id encoding in one place. The document builder lowers to parameterized statements (Get/Query/Insert/Patch/Replace/Delete/Raw) with LIMIT-by-mode and `unique → LIMIT 2`. Values cross the SQL boundary uniformly as **text** in both directions (`col::text` on read, `$n::<cast>` on write via a coarse `PgTypeClass`), avoiding per-type plumbing. Ids are self-describing `table:uuid` strings; the catalog merges `information_schema` with worker-supplied validator `describe()` metadata. *Known gaps:* `withIndex(name)` now orders by the index's columns, but there's no index-range scan / index-fine-grained read-set yet; `Raw` captures nothing. (`replace` now has true full-row semantics — it nulls omitted columns — distinct from `patch`.)

- **Analytical raw SQL (ADR 04).** A read-only `DbOp::Raw { sql, params }` op exposed as the `ctx.sql` tagged template. The engine wraps user SQL as `SELECT to_jsonb(__pulse_sub) AS j FROM ( <sql> ) AS __pulse_sub` so any projection decodes as one JSON value per row with no catalog lookup. Params bind as text with a `prefix:uuid` id-decode heuristic. `Raw` returns `None` from `access()`, so it is opaque to the reactor (non-reactive by design). *Deviation:* analytical queries currently run on the OLTP pool, not a dedicated replica with its own pool/timeouts; read-only is by convention, not enforced.

- **Reactivity (ADR 05).** For the M2 slice, invalidation is driven by **write-sets the engine captures during mutation execution**, matched **table-level** (`HashSet` disjointness) against each subscription's captured read-set, then re-run and pushed as full results over SSE. Transport: `GET /sync?clientId` (SSE) plus `POST /subscribe` / `/unsubscribe` control and `POST /rpc` as the write trigger; frames are `{ sub, data }`. Pushes flow into the client's `LocalStore` confirmed layer. *Deviation:* the spec's canonical change source is WAL/`pgoutput` logical replication feeding a `pulse-reactor`/`pulse-sse`/`pulse-cdc` crate split; engine-captured write-sets are simpler but blind to out-of-band writes, and the reactor currently lives inside `pulse-server`. *Not yet:* `commitLSN`/LSN batch-advance, `Last-Event-ID`/ring-buffer/`resync` resume, debounce/dedup/diff-to-deltas, key/range matching. The pipeline shape (`read-set → match → re-execute → push`) is the same one WAL will feed — only the source swaps.

- **Contract + middleware + inference (ADR 06).** A dependency-free `oc` contract builder produces immutable, phantom-typed `ContractProcedure`s carrying `{ kind, input, output, errors }`; the contract is plain data so the client can `import type` it. The `os` middleware builder is immutable and tracks the full out-context, so `next({ context })` widens downstream types and `os.use(a).use(b)` composes cleanly. `implement(contract)` mirrors the contract to a builder tree whose leaf `ctx` is shaped by kind. The client is a runtime Proxy (zero contract bytes shipped) with all call types inferred. `Doc<T>` resolves via an augmentable `PulseDataModel` interface emitted as literal members, breaking the validator→doc type cycle. *Deviations:* code exports `Client<C>`/`InferInput`/`InferOutput`/`InferKind`/`InferErrors` vs the spec's `RouterClient`/`InferInputs`/`InferOutputs`; `implement()` returns an anonymous tree rather than rebinding `os`. *Not yet:* the `pulse gen` CLI that emits the augmentation (the example is hand-written).

- **Local-first client (ADR 07).** Four small modules: `KVStore` (pluggable string KV), `OfflineQueue` (durable FIFO under one JSON array, persisted before the network send), `LocalStore` (confirmed layer + optimistic overlay with eager replay-on-top rebase and stable `tempId`s), and `LocalFirst` (persisted monotonic seq, durable enqueue, reentrancy-guarded flush splitting network vs handler errors). *Deviations:* none of these are wired into `createClient` yet (only `SyncClient` is constructed); there is no auto reconnect/reload flush trigger; there is no `lastMutationID` watermark or write-checkpoint guard; the cache is per-query, not the spec's normalized keyed collections.

## Testing Decisions

The methodology is recorded in **ADR 08** and applies to every milestone. A good test reads like a specification of an externally observable capability and would survive an internal refactor — including the eventual M4 swap of the worker runtime for embedded V8.

- **Test external behavior, not implementation details.** Assertions go through the public surface — `@pulse/client` (`createClient<typeof contract>`) over real HTTP against a real `pulse-server` + worker + Dockerized Postgres — never by reading Postgres directly, never against NDJSON frames, the `reader_loop`, the reactor's internal maps, or private client fields (`view`/`confirmed`/`pending`). Direct DB access is allowed only to *arrange* a test (`harness.reset()` truncates between tests), never to *assert* its outcome.
- **Vertical-slice TDD with tracer bullets.** One failing test for one behavior, minimal code to pass, refactor on green. The first test of a slice proves the whole path end-to-end (the M1 round-trip tracer and the M2 reactive tracer are labelled as such) before breadth is added.
- **Modules tested through their public interface.** The runtime crate is tested via `Worker::spawn`/`execute → ExecResult` (ADR 01/02); `ctx.db`/`ctx.sql` via returned documents (ADR 03/04); reactivity via what a separate subscriber observes over SSE (ADR 05); contract/middleware via the observable onion order, context propagation, and the resolved result through `executeProcedure`, with type-level inference asserted compile-only in `examples-chat` (ADR 06); the local-first modules via their interfaces using a shared `InMemoryKV` to simulate reload (ADR 07).
- **Error surface is covered, not just the happy path.** Validator rejection → `BAD_REQUEST`; missing auth → `UNAUTHORIZED`; a declared error surfaces with code *and* structured `data` (e.g. `RATE_LIMITED { retryAfter: 5 }`).
- **Stress tests as correctness probes, not benchmarks.** Four categories, each pinning a named failure mode and asserting a correctness invariant inside a verifiable envelope: concurrent load (no dropped/duplicated writes, comparing full sets under the list cap), worker backpressure (each result matches its own input — correlation holds under a 200-flood), pool saturation (no deadlock with `oltpMaxConns: 2` and 100 sends), and a soak (zero errors and `ops > 500` over a ~15s window). Loose time bounds serve only as a liveness guard.
- **Prior art to copy** when adding a milestone's tests: `tests/integration/roundtrip.test.ts`, `messages.test.ts`, `analytical.test.ts`, `errors.test.ts`, `reactive.test.ts`, the four `tests/stress/*` files, and the in-package unit tests (`builder.test.ts`, `client.test.ts`, `validators.test.ts`, `naming.rs`, `lsn.rs`, `readset.rs`). The two vitest projects encode the split: fast in-package (`packages/*/src/**`, including `*.test-d.ts`) vs serial integration (`tests/**`, single-fork, 30s/120s timeouts).

## Out of Scope

The following are explicitly deferred. Each is grounded in the spec's roadmap (`ARCHITECTURE.md` §9/§10) and the ADRs; where a milestone is named it indicates the planned phase.

- **Embedded V8 runtime + determinism sandbox (M4).** Replacing the Node/Bun worker with a `deno_core` isolate pool (frozen time, seeded RNG, no net/fs). Until then there is no determinism guarantee, and IPC is paid on the hot path. *(ADR 01/02/03/08.)*
- **Mutation transactions + OCC retry (M4).** One `SERIALIZABLE` tx per mutation with `40001` retry and in-tx `last_mutation_id` advance; today mutations are autocommit per op. *(ADR 01.)*
- **WAL CDC (`pulse-cdc`).** `pgoutput` logical-replication consumer for writes made outside the engine — the canonical, lossless change source. Today engine-captured write-sets are blind to out-of-band writes. *(ADR 05.)*
- **Read-set precision + deltas + cross-query consistency (M3).** Key/range-level read-set capture via the instrumented query builder, result diffing → deltas over SSE, `commitLSN` on pushes, and client LSN batch-advance. Today matching is table-level and full results are pushed. *(ADR 03/05.)*
- **SSE resume (M5).** Event `id:`, per-subscription ring buffer, `Last-Event-ID` replay, and `resync` fallback. *(ADR 05.)*
- **Reactor/SSE/CDC crate extraction.** Splitting the in-`pulse-server` reactor into `pulse-reactor`/`pulse-sse`/`pulse-cdc` — a refactor, not a behavior change. *(ADR 05.)*
- **Actions + analytical replica isolation (M6).** The `pulse-actions` Node worker pool, `"use action"` files, `ctx.runQuery/runMutation` re-entry; routing analytical procedures to a dedicated OLAP replica pool with its own sizing/timeouts; streaming large results / `infiniteOptions`; materialized-view backing; enforcing raw SQL read-only. *(ADR 04.)*
- **Local-first wiring + flush triggers + watermark (M5).** Wiring `LocalFirst`/`LocalStore`/`OfflineQueue` into `createClient` and TanStack Query; `online`/startup flush triggers and retry backoff; server `last_mutation_id` watermark, rebase-by-id, and the write-checkpoint guard; normalized keyed collections and an intent-based `insert`/`increment` API. *(ADR 07.)*
- **Schema, migrations, CLI, hardening (M7).** `pulse-schema` DDL diff + migrations validated against existing rows; the `pulse dev | deploy | migrate | gen` CLI (the `PulseDataModel` augmentation is hand-written until then); slot-lag/WAL-retention monitoring, backpressure, error taxonomy, auth hardening, observability dashboards. *(ADR 03/06.)*
- **Document-builder coverage.** `OR`/`IN`/`LIKE`/`IS NULL`/JSONB ops, arbitrary order, offset, real index selection and range scans; true `replace` semantics (nulling omitted columns); richer Postgres type handling beyond the coarse `PgTypeClass`. Raw `ctx.sql` is the current escape hatch for everything the builder can't express. *(ADR 03.)*
- **Protocol/runtime hardening.** A dedicated protocol fd (to guard direct `process.stdout.write`), per-request execution timeout/cancellation, and bounded/swept correlation maps for worker-crash cases. *(ADR 02.)*
- **Determinism/replay tests, chaos tests, perf SLAs, and browser/React e2e.** Meaningful only once the sandbox / WAL path exists or as separate benchmarking work. *(ADR 08.)*
- **Post-v1 seams (later).** Incremental view maintenance / differential dataflow for hot query shapes, Hasura-style multiplexing, contract-first OpenAPI emission, and horizontal scale-out of the engine (sharded subscription registry). *(ADR 00; `ARCHITECTURE.md` §9 "Later".)*

## Further Notes

- **Living deviation tracking.** `ARCHITECTURE.md` §10 is the living record of built-vs-spec, and each ADR restates its own deviation in its Status line. This PRD inlines those caveats inside the user stories and implementation decisions so a reader can tell, per capability, whether it is shipped, partial, or planned.
- **The deviations are sequencing, not reversals.** Every shortcut (Node/Bun worker over V8, engine write-sets over WAL CDC, table-level over key/range matching, OLTP pool over replica, autocommit over serializable tx, unwired local-first modules) was taken to reach the M2 reactive slice — "one reactive query updating one browser end-to-end" — fastest, and each plugs into the same interface or pipeline its target replacement will use. None changes the ADR 00 thesis.
- **Two artifacts the type system cannot keep in lockstep (ADR 06):** the generated `PulseDataModel` augmentation vs. the actual `defineSchema`, and the spec's `RouterClient`/`InferInputs` names vs. the code's `Client`/`InferInput`. A reader following `ARCHITECTURE.md` verbatim will hit the naming mismatch; reconciling (rename in code or update the spec) is open.
- **Reactive correctness depends on the document builder, not `ctx.sql` (ADR 04/05).** Because `DbOp::Raw` is opaque to the reactor, a raw read registers no read-set and a raw write registers no write-set — so raw SQL in a *reactive* path is a correctness footgun, not just an imprecision. Use `ctx.sql` only on analytical procedures.
- **The chat example is the canonical acceptance vehicle.** `@pulse/examples-chat` is the vertical-slice target across milestones; the durable proof of the thesis is running it reactively against a stock Postgres that `psql`/`pg_dump` can read at the same time.
