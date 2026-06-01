# Architecture Spec — Reactive Postgres Platform (working name: **Pulse**)

> Status: design / pre-implementation. This document is the source of truth used to scaffold the repo.

---

## 1. Product Vision

**Pulse** is a reactive, local-first application platform built on **standard Postgres** as the database of record, a **Rust** reactivity/sync engine, and a **TypeScript** SDK for both authoring server functions and consuming them on the client.

You write server functions in TypeScript (`query` / `mutation` / `action`), define your schema in TypeScript, and call them from the client through a fully type-inferred, oRPC-style API that plugs into TanStack Query. Reactive queries update in realtime over SSE. Heavy analytical queries run on the same API surface but on an isolated execution path. The client cache is local-first with an offline mutation queue and rebase-based reconciliation.

### What it is

- A **server function layer** (oRPC-inspired: procedures + composable middleware, contract-first, no codegen for client types).
- A **reactive engine** that observes Postgres writes and pushes deltas to subscribed clients.
- A **client SDK** that is a local-first cache sitting under TanStack Query, with optimistic mutations and offline support.

### Design choices

The reactive-backend category typically trades the database away for its DX: a proprietary store with custom MVCC, a single document query surface, and a fully managed runtime. Pulse keeps the developer experience but makes the opposite call on storage and access:

| Dimension | Pulse |
|---|---|
| Storage | **Standard Postgres.** Your data is in real tables; you can run psql, pg_dump, BI tools, and other apps against it. No lock-in. |
| Query language | **Raw SQL** (via a typed query builder) *and* a document-style builder. Full Postgres power: joins, CTEs, window functions, JSONB, extensions (PostGIS, pgvector). |
| CDC / change source | **Postgres logical replication (WAL via `pgoutput`)** — the canonical, lossless, replayable change stream. |
| Function runtime | TS functions run in an **embedded V8 isolate pool inside the Rust engine** for deterministic query/mutation re-execution; `actions` run in a separate Node.js worker pool. |
| Reactivity granularity | **Tiered read-set**: table-level → primary-key/range-level, captured automatically by an instrumented query layer. |
| API shape | **oRPC-style procedures + middleware**, contract-first, end-to-end type inference, TanStack Query integration. |
| Analytical queries | **First-class heavy/analytical path**: non-reactive procedures routed to a read replica with its own pool and timeouts. |
| Self-hostable | **Bring-your-own-Postgres.** Pulse is a stateless Rust service in front of your DB. |

**One-line pitch:** a reactive programming model and end-to-end-typed DX, without giving up Postgres, SQL, or your data.

---

## 2. System Component Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              BROWSER / CLIENT                              │
│                                                                            │
│   React app                                                                │
│     │                                                                      │
│     ├── @pulse/client  (RouterClient<typeof contract>, type-inferred)      │
│     │      ├── TanStack Query adapter (.queryOptions/.mutationOptions/…)    │
│     │      ├── Local-first store (normalized keyed collections)            │
│     │      │     • confirmed layer   • optimistic overlay                  │
│     │      └── Offline mutation queue (durable, per-client mutationID)     │
│     │                                                                      │
│     ├──── POST /rpc            (calls: queries, mutations, actions)  ──────┐│
│     └──── GET  /sync  (SSE)    (subscriptions; Last-Event-ID resume) ─────┐││
└───────────────────────────────────────────────────────────────────────┐ │││
                                                                          │ │││
                            HTTP / HTTP-2 (reverse proxy, buffering off)  │ │││
                                                                          ▼ ▼▼▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        RUST ENGINE  (pulse-server)                         │
│                                                                            │
│  ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐ │
│  │  HTTP / RPC layer   │   │  SSE / Sync layer  │   │  Subscription Mgr  │ │
│  │  (axum)            │   │  (axum SSE)        │   │  read-set registry │ │
│  │  • route procedure │   │  • per-client      │   │  • table index     │ │
│  │  • run middleware  │   │    channel + ring  │   │  • key/range index │ │
│  │  • dispatch        │   │    buffer (replay) │   │  • debounce queue  │ │
│  └─────────┬──────────┘   └─────────▲──────────┘   └─────────▲──────────┘ │
│            │                        │                        │            │
│  ┌─────────▼──────────────────────────────────────────────────────────┐  │
│  │  Function execution                                                  │  │
│  │   • Query/Mutation runtime:  embedded V8 isolate pool (deno_core)    │  │
│  │       - deterministic ctx (frozen time, seeded RNG, no net/fs)       │  │
│  │       - instrumented ctx.db  →  emits read-set + write-set           │  │
│  │   • Action runtime:  Node.js worker pool (separate process)          │  │
│  └─────────┬───────────────────────────────────────────┬──────────────┘  │
│            │ SQL (OLTP pool, sqlx)                       │ SQL (OLAP pool) │
│  ┌─────────▼──────────────┐                    ┌─────────▼──────────────┐  │
│  │  WAL Consumer          │                    │  Analytics dispatcher  │  │
│  │  (tokio-postgres,      │                    │  (statement_timeout,   │  │
│  │   logical replication, │                    │   routed to replica)   │  │
│  │   pgoutput) → changes  │                    │                        │  │
│  └─────────┬──────────────┘                    └─────────┬──────────────┘  │
└────────────┼───────────────────────────────────────────┼─────────────────┘
             │ replication slot (LSN cursor)              │ read-only
             ▼                                            ▼
   ┌────────────────────┐  logical replication  ┌────────────────────┐
   │  POSTGRES PRIMARY   │ ────────────────────► │  POSTGRES REPLICA   │
   │  (OLTP, record of   │                       │  (OLAP / analytics) │
   │   record)           │                       │                     │
   └────────────────────┘                       └────────────────────┘
```

**Data flow at a glance**

- **Read (reactive):** client opens SSE `/sync`, subscribes to a query → engine executes the query in V8 against the OLTP pool, records its read-set in the Subscription Manager, returns the first result over SSE.
- **Write:** client `POST /rpc` a mutation → engine runs it in V8 inside one serializable Postgres tx, commits, advances per-client `lastMutationID` in the same tx.
- **Invalidate:** WAL Consumer tails the slot, decodes committed changes, hands the change-set (table + primary keys) to the Subscription Manager, which matches against read-sets, debounces, re-executes affected queries, and pushes deltas over SSE.

---

## 3. Developer-Facing Programming Model (TypeScript)

The model is **contract-first oRPC**: you author a dependency-free *contract* describing procedures, then implement handlers against it. The client imports only the contract type. No codegen for client types — pure TypeScript inference.

### 3.1 Schema

Schema is defined in TypeScript with a `v` validator builder that produces both runtime validation and static types, and is compiled to Postgres DDL + migrations.

```ts
// app/schema.ts
import { defineSchema, defineTable, v } from "@pulse/schema";

export default defineSchema({
  users: defineTable({
    name: v.string(),
    email: v.string(),
    role: v.union(v.literal("admin"), v.literal("member")),
  })
    .index("by_email", ["email"]),

  messages: defineTable({
    authorId: v.id("users"),       // typed FK → users.id
    channelId: v.id("channels"),
    body: v.string(),
    editedAt: v.optional(v.number()),
  })
    .index("by_channel", ["channelId", "_creationTime"]),

  channels: defineTable({
    name: v.string(),
    isPrivate: v.boolean(),
  }),
});
```

- `_id` (uuid/bigint) and `_creationTime` are auto-injected on every table.
- `.index(name, [cols])` generates a Postgres B-tree index *and* registers it with the engine so the query layer can capture index-range read-sets.
- On `pulse dev` / `pulse deploy`, the schema diffs against the live DB and emits a SQL migration (validated against existing rows; reject or `--allow-data-loss`).
- Codegen produces `Doc<"messages">`, `Id<"users">` types for use in handlers. (This is schema→type codegen, distinct from client-call types, which are pure inference.)

### 3.2 Contract (shared, dependency-free)

```ts
// app/contract.ts   — imported by both server and client
import { oc } from "@pulse/contract";
import { v } from "@pulse/schema";

export const contract = {
  messages: {
    list: oc
      .reactive()                                  // marks query as subscribable
      .input(v.object({ channelId: v.id("channels") }))
      .output(v.array(v.doc("messages"))),

    send: oc
      .mutation()
      .input(v.object({ channelId: v.id("channels"), body: v.string() }))
      .output(v.doc("messages"))
      .errors({ RATE_LIMITED: { data: v.object({ retryAfter: v.number() }) } }),

    summarize: oc
      .analytical()                                // heavy, non-reactive, replica
      .input(v.object({ channelId: v.id("channels") }))
      .output(v.object({ summary: v.string() })),
  },
} as const;
```

`oc.reactive()` / `oc.mutation()` / `oc.analytical()` / `oc.action()` set the procedure *kind*, which determines runtime, determinism rules, and routing. The contract is plain data — no server dependencies — so the client can depend on it without pulling in the engine.

### 3.3 Middleware (oRPC-style, immutable builder, `next()`-extended context)

```ts
// app/middleware.ts
import { os } from "@pulse/server";

// Reusable, dependency-declaring middleware: needs `headers` in initial context.
export const authed = os
  .$context<{ headers: Headers }>()
  .middleware(async ({ context, next, errors }) => {
    const user = await verifyJwt(context.headers.get("authorization"));
    if (!user) throw errors.UNAUTHORIZED();
    // shallow-merges { user } at runtime AND widens TS context downstream:
    return next({ context: { user } });
  });

export const logged = os.middleware(async ({ next, path }) => {
  const start = performance.now();
  const result = await next();                     // pre/post wrap the handler
  console.log(path, performance.now() - start);
  return result;
});

// A reusable authed base builder (immutable builder → safe to reuse).
export const authedBase = os.use(logged).use(authed);
```

### 3.4 Implementing handlers

```ts
// app/messages.ts
import { implement } from "@pulse/server";
import { contract } from "./contract";
import { authedBase } from "./middleware";

const os = implement(contract);                    // builder bound to the contract

export const list = os.messages.list
  .use(authedBase)
  .handler(async ({ ctx, input }) => {
    // ctx.db is INSTRUMENTED — every read is captured into the read-set.
    return ctx.db
      .query("messages")
      .withIndex("by_channel", (q) => q.eq("channelId", input.channelId))
      .order("desc")
      .take(100);
  });

export const send = os.messages.send
  .use(authedBase)
  .handler(async ({ ctx, input, errors }) => {
    if (await isRateLimited(ctx, ctx.user.id))
      throw errors.RATE_LIMITED({ retryAfter: 5 });
    // ctx.db here is read-write, running inside ONE serializable tx.
    return ctx.db.insert("messages", {
      channelId: input.channelId,
      authorId: ctx.user.id,
      body: input.body,
    });
  });

export const summarize = os.messages.summarize
  .use(authedBase)
  .handler(async ({ ctx, input }) => {
    // Runs on the OLAP path. ctx.sql allows raw SQL for heavy aggregation.
    const rows = await ctx.sql/*sql*/`
      SELECT body FROM messages WHERE channel_id = ${input.channelId}
      ORDER BY _creation_time DESC LIMIT 5000`;
    return { summary: await summarizeText(rows) };
  });
```

### 3.5 Actions (side effects, non-deterministic, non-reactive)

```ts
// app/notify.ts   — runs in the Node.js worker pool
"use action";                                      // file directive → Node runtime
import { implement } from "@pulse/server";
import { contract } from "./contract";

const os = implement(contract);

export const sendEmail = os.notify.sendEmail.handler(async ({ ctx, input }) => {
  await stripe.charge(input);                       // external I/O allowed here
  await ctx.runMutation(contract.orders.markPaid, { id: input.orderId });
  return { ok: true };
});
```

**Capability matrix**

| Kind | Runtime | DB access | Reactive | Deterministic | Side effects | Retry-safe |
|---|---|---|---|---|---|---|
| `reactive` (query) | V8 isolate | read-only, instrumented | yes | enforced | no | n/a (no writes) |
| `mutation` | V8 isolate | read-write, 1 serializable tx | invalidates | enforced | no | yes (OCC retry) |
| `analytical` | V8 isolate | read-only (replica) | no | not required | no | n/a |
| `action` | Node worker | via `ctx.runQuery/runMutation` | no | no | yes | no |

### 3.6 Client usage (pure inference, no codegen)

```ts
// client.ts
import { createClient } from "@pulse/client";
import type { contract } from "../app/contract";   // type-only import

export const pulse = createClient<typeof contract>({ url: "/", /* auth, etc */ });

// React, via TanStack Query adapter:
import { useQuery, useMutation } from "@tanstack/react-query";

function Channel({ channelId }: { channelId: string }) {
  // reactive query → auto-subscribes over SSE, updates push into the cache
  const { data } = useQuery(pulse.messages.list.queryOptions({ input: { channelId } }));
  const send = useMutation(pulse.messages.send.mutationOptions());
  // ...
}
```

`createClient<typeof contract>` recursively maps the contract to callable, fully-typed procedure clients; `InferInputs`/`InferOutputs` derive DTO types from the contract type alone.

---

## 4. Reactivity — End to End

### 4.1 Chosen mechanism

**WAL-driven CDC + automatic read-set tracking + server-side re-execution + SSE delta push, with batch-advanced client consistency.**

We deliberately reject:
- **LISTEN/NOTIFY / triggers as the CDC source** — lossy, no replay, 8KB cap, connection-per-listener. (We may use NOTIFY only as a cheap "wake the WAL reader" nudge.)
- **Full incremental view maintenance / differential dataflow** for v1 — correct and cheap at steady state but a huge engine that constrains supported SQL. We leave a seam to add it later for hot query shapes.

We adopt **read-set invalidation** because Postgres logical replication already hands us the changed rows' **primary keys**, which is exactly the granularity that makes key-level matching cheap and precise.

### 4.2 Read-set capture

Every reactive query runs through the **instrumented `ctx.db`** in the V8 runtime. As it executes it records a structured read-set:

```
ReadSet {
  tables: Set<TableId>,                       // coarse fallback
  keys:   Map<TableId, Set<PrimaryKey>>,      // exact rows read by .get / point lookups
  ranges: Map<TableId, Vec<IndexRange>>,      // index-range scans from .withIndex
}
```

- Point reads (`ctx.db.get(id)`, `.eq` on a unique index) → exact keys.
- Index range scans (`.withIndex("by_channel", q => q.eq(...).gt(...))`) → `(indexName, lower, upper)` ranges.
- Raw `ctx.sql` queries that can't be statically analyzed fall back to **table-level** read-sets (coarse but correct). The query builder is the path that yields fine granularity; raw SQL trades precision for power.

The read-set is stored in the Subscription Manager keyed by `subscriptionId = hash(procedurePath, input, clientId)`, and the query result is cached keyed by `(procedurePath, input, snapshotLSN)`.

### 4.3 Write → invalidation → push

1. **Write.** A mutation runs in V8 inside one `SERIALIZABLE` Postgres transaction (via the OLTP pool). In the same tx it bumps `pulse_clients.last_mutation_id` for the calling client. On `40001` serialization failure the engine retries the whole handler (handlers are deterministic, so this is safe) up to N times.
2. **CDC.** The **WAL Consumer** (dedicated `tokio-postgres` replication connection on a named slot + publication) decodes `Insert/Update/Delete` messages via `pgoutput`. With `REPLICA IDENTITY` default it gets the changed row's primary key — ideal. It produces `ChangeSet { commitLSN, changes: [(TableId, PrimaryKey, Op)] }`.
3. **Match.** The Subscription Manager intersects the change-set against the read-set registry:
   - table index → candidate subscriptions reading that table;
   - among candidates, refine by key/range membership;
   - matched subscriptions are marked dirty.
4. **Debounce.** Dirty subscriptions are coalesced over a short window (~50–150ms) so a burst of writes triggers one re-run per affected query. Identical `(procedurePath, input)` subscriptions across clients are **de-duplicated**: the query runs once, the result fans out.
5. **Re-execute.** Affected queries re-run in V8 at the **post-commit snapshot** (`AS OF`/repeatable-read snapshot ≥ commitLSN) against the OLTP read pool. New result is diffed against the cached previous result.
6. **Push.** The delta (or full result if small) is pushed to each subscribed client over its SSE channel, tagged with `id: <monotonic seq>` and the `commitLSN` it reflects.
7. **Advance slot.** Only after the change-set has been folded into the in-memory registry does the consumer send a Standby Status Update advancing the slot's confirmed LSN — so a crash replays rather than drops.

### 4.4 Consistency model

- **Per-query:** snapshot-consistent. Each (re)execution reads a single Postgres MVCC snapshot, so no torn reads within a query.
- **Cross-query (client global consistency):** every SSE push carries a logical `commitLSN`. The client **batch-advances** all of its subscriptions to the same `commitLSN` before flushing React updates, so the UI never shows one component reflecting a mutation while a sibling does not. Pushes are buffered client-side and applied at LSN boundaries.
- **Mutations:** serializable (Postgres `SERIALIZABLE`), with OCC-style retry on `40001`. `lastMutationID` is advanced atomically in the same tx, giving the client a reliable confirmation watermark for rebase (see §5).
- **Determinism:** the query/mutation V8 runtime freezes `Date.now()`/`new Date()` to the tx start time, seeds `Math.random()` per invocation, and forbids `fetch`/fs. This is what makes re-execution for invalidation trustworthy and result caching sound.

---

## 5. Client SDK

`@pulse/client` is a **local-first cache that sits under TanStack Query**, modeled on the Replicache/TanStack-DB rebase pattern.

### 5.1 Layers

```
┌─────────────────────────────────────────────┐
│ React components (useQuery / useMutation)     │
├─────────────────────────────────────────────┤
│ TanStack Query adapter                        │
│  pulse.x.y.queryOptions / mutationOptions /   │
│  infiniteOptions ; hierarchical query keys    │
├─────────────────────────────────────────────┤
│ Local store (normalized, keyed collections)   │
│   • confirmed layer  (authoritative)          │
│   • optimistic overlay (speculative)          │
│   completeness tag per result: confirmed/opt  │
├─────────────────────────────────────────────┤
│ Sync client                                   │
│   • SSE connection (Last-Event-ID resume)     │
│   • LSN-aligned batch apply                   │
│   • durable offline mutation queue (FIFO)     │
│   • per-client monotonic mutationID           │
├─────────────────────────────────────────────┤
│ Persistence: IndexedDB                         │
└─────────────────────────────────────────────┘
```

### 5.2 TanStack Query integration

The adapter mirrors the contract and produces, per procedure:
- `.queryOptions({ input })` → `useQuery`/`useSuspenseQuery`/prefetch. For `reactive` procedures it also registers an SSE subscription and writes pushed deltas straight into the cache (TanStack Query becomes a thin view over the local store).
- `.mutationOptions({ onSuccess… })` → `useMutation`.
- `.infiniteOptions({ input: (pageParam) => …, getNextPageParam })` for pagination.
- Hierarchical keys `[[...path], { type, input }]`: `.key()` = **partial** key (broad `invalidateQueries`), `.queryKey({ input })` = **exact** key (`setQueryData`). Client-only context is excluded from keys to avoid dedup bugs.

### 5.3 Optimistic updates & rollback

Two distinct layers (never mutate confirmed state in place):

```ts
const send = useMutation(
  pulse.messages.send.mutationOptions({
    optimistic: (store, input) => {
      store.insert("messages", { ...input, _id: tempId(), _optimistic: true });
    },
  }),
);
```

- On call: assign a temp id, apply to the **optimistic overlay**, enqueue a durable mutation record (`{clientId, mutationId, path, input}`), POST to `/rpc`.
- The mutation's promise does **not** resolve as "synced" until the authoritative change has round-tripped back (server returns the result + its `commitLSN`; the SSE/confirmation watermark catches up).
- A thrown handler error → automatic rollback of that mutation's overlay entry. No manual `onError`/`onSettled`.

### 5.4 Offline mutation queue + rebase

- The mutation queue is a **durable FIFO** in IndexedDB. Each mutation gets a per-client **monotonically increasing `mutationId`** persisted *before* the network send.
- Server stores `last_mutation_id` per client transactionally (same tx as the mutation) and returns it on every sync/push.
- **Rebase on each sync:** rewind optimistic overlay → apply confirmed server patch → **replay only mutations with `id > server.lastMutationId`** on top → reveal atomically. Mutations `<= lastMutationId` are discarded (confirmed).
- **Write-checkpoint guard (PowerSync pattern):** the client does **not** apply newly-pulled confirmed state that would regress an unconfirmed local write until its own queue has round-tripped — avoids flicker where a pull momentarily reverts a pending write before replay.
- Design mutations as **intent-based** (`insert`, `increment`, `set if version matches`) so replay against newer server state is correct. Conflict resolution = "server re-runs the mutator"; the client never resolves conflicts.

### 5.5 Reconnection & resumability

- SSE only (server→client). Subscribe/unsubscribe/auth go over normal `POST /rpc`.
- Every SSE event carries `id: <seq>`. The server keeps a **per-subscription ring buffer**. On reconnect the browser auto-sends `Last-Event-ID`; the server replays the tail after that id → at-least-once delivery, zero client code.
- If the buffer has rolled past `Last-Event-ID` (long offline), the server sends a `resync` event → client refetches affected queries fresh and resets the LSN watermark.
- Recommend HTTP/2 in front (proxy buffering off) to dodge the ~6-connection-per-origin HTTP/1.1 cap when a tab holds many subscriptions.

---

## 6. Heavy / Analytical Query Path

Analytical procedures (`oc.analytical()`) are **non-reactive** and **isolated** from the reactive hot path so a 30-second OLAP scan can never stall invalidation or exhaust the reactive connection budget.

- **Routing:** dispatched to the **OLAP read replica** via a dedicated, separately-sized connection pool (not the OLTP pool). PgBouncer in transaction-pooling mode fronts both pools to bound active connections.
- **Timeouts:** the reactive/OLTP pool runs a low `statement_timeout` (e.g. 15–30s); the analytics pool allows longer. On the replica, `max_standby_streaming_delay` (~30–60s) balances query cancellation vs replication lag.
- **Freshness:** analytical results are at replica-lag freshness — acceptable for analytics, explicitly *not* used for live OLTP subscriptions.
- **Execution:** still runs in the V8 runtime (so middleware/auth/validation apply uniformly) but determinism is **not** enforced and the read-set is **not** registered (no subscription). `ctx.sql` gives full raw SQL for joins/CTEs/window functions/aggregates.
- **Caching/streaming:** large results can be streamed to the client as an async iterator over a normal HTTP response (chunked), and TanStack Query `infiniteOptions` paginate. Optionally back popular analytical queries with a periodically-refreshed Postgres **materialized view**.

---

## 7. Rust Engine Internals

### 7.1 Why these runtime choices

**Q/M runtime = embedded V8 isolate pool (via `deno_core`/`rusty_v8`), in-process.**
Rationale: queries/mutations are re-executed *constantly* for invalidation, must be deterministic, and must be cheap to spin up (~ms isolate start vs hundreds of ms for a process). Embedding V8 lets the Rust engine inject a sandboxed, deterministic `ctx` (frozen time, seeded RNG, no net/fs) and capture read/write-sets directly across the FFI boundary without IPC overhead. One isolate per concurrent execution, pooled and recycled.

**Action runtime = separate Node.js worker pool (out-of-process).**
Rationale: actions need full Node/npm (Stripe, OpenAI, fs, fetch), are not deterministic, and must not share the deterministic sandbox or block the engine. A pool of long-lived Node workers communicates with the engine over a length-prefixed unix-socket/stdio protocol; `ctx.runQuery/runMutation` calls hop back into the engine. Crash isolation: a wedged action kills its worker, not the engine.

> Trade-off acknowledged: embedding V8 is the heaviest single dependency and the trickiest part of the build. The alternative (run *all* TS in Node workers) is simpler but pays IPC + process-startup cost on every invalidation re-run, which is the hot path — unacceptable for a reactive engine. We accept the V8-embedding complexity precisely where it buys the most.

### 7.2 CDC approach

- `wal_level=logical`; one `PUBLICATION` over reactive tables; one named logical replication `SLOT`.
- WAL Consumer = dedicated `tokio-postgres` connection in `START_REPLICATION ... LOGICAL` mode using `pgoutput` (built-in, lowest overhead; we parse the binary relation/tuple protocol — wrap with a crate like `pgwire-replication` or hand-roll the decoder).
- `REPLICA IDENTITY` default (PK only) on most tables → perfect for key-level invalidation; `FULL` only on tables whose subscription predicates need old non-key column values.
- Slot liveness monitored (an abandoned slot pins WAL → disk fill); alert on slot lag.

### 7.3 Connection pooling

- **OLTP/reactive pool** (`sqlx`, compile-time-checked SQL): mutations + reactive query re-execution. Small, low `statement_timeout`.
- **OLAP pool** (`sqlx`, separate config): analytical procedures → replica.
- **Replication connection** (`tokio-postgres`): the slot consumer — never `sqlx` (it doesn't expose replication mode), never starved by the other pools.
- All app pools optionally fronted by PgBouncer (transaction mode).

### 7.4 Crates (Rust)

| Concern | Crate |
|---|---|
| Async runtime | `tokio` |
| HTTP + SSE | `axum`, `tower`, `tower-http` |
| App SQL + pool | `sqlx` (postgres, compile-time checks) |
| Replication stream | `tokio-postgres` (+ `pgwire-replication` or custom `pgoutput` decoder) |
| Embedded JS (Q/M) | `deno_core` / `rusty_v8` |
| Action IPC | `tokio` unix sockets + length-prefixed `serde`/`rmp-serde` framing |
| Serialization | `serde`, `serde_json`, `rmp-serde` |
| LSN/types | custom `Lsn` newtype; `uuid`, `time` |
| Observability | `tracing`, `tracing-subscriber`, `metrics` |
| Errors | `thiserror`, `anyhow` (boundaries only) |

---

## 8. Monorepo Layout

```
pulse/
├── Cargo.toml                      # Rust workspace root
├── pnpm-workspace.yaml             # TS workspace root
├── docs/
│   └── ARCHITECTURE.md             # (this file)
│
├── crates/                         # Rust workspace members
│   ├── pulse-server/               # binary: axum app, wiring, config
│   ├── pulse-core/                 # domain types: Lsn, ChangeSet, ReadSet, ProcedureKind
│   ├── pulse-rpc/                   # procedure routing + middleware execution engine
│   ├── pulse-cdc/                  # WAL consumer (logical replication, pgoutput decode)
│   ├── pulse-reactor/              # subscription mgr: read-set registry, matching, debounce, dedup
│   ├── pulse-sse/                  # SSE transport: per-sub ring buffer, Last-Event-ID replay
│   ├── pulse-jsruntime/            # embedded V8 isolate pool, deterministic ctx, ctx.db bridge
│   ├── pulse-actions/              # Node worker pool manager + IPC protocol
│   ├── pulse-sql/                  # OLTP/OLAP pools, query builder → SQL, read-set extraction
│   └── pulse-schema/               # schema model, migration diffing, DDL gen
│
└── packages/                       # TS workspace members (pnpm)
    ├── @pulse/schema/              # defineSchema/defineTable + v validator (runtime + types)
    ├── @pulse/contract/            # oc contract builder (dependency-free)
    ├── @pulse/server/              # os builder, implement(), middleware, handler ctx types
    ├── @pulse/client/              # RouterClient inference, sync client, local store, offline queue
    ├── @pulse/react/               # TanStack Query adapter (queryOptions/mutationOptions/keys)
    ├── @pulse/runtime-node/        # the Node action worker entrypoint + IPC client
    ├── @pulse/cli/                 # `pulse dev | deploy | migrate | gen`
    └── @pulse/examples-chat/       # end-to-end example app (used as the vertical-slice target)
```

---

## 9. Phased Implementation Roadmap

The guiding principle: **get one reactive query updating one browser end-to-end as early as possible (M2)**, then harden.

### M0 — Skeleton & contracts (foundations)
- Cargo + pnpm workspaces, CI, the crate/package shells above.
- `pulse-core` types: `Lsn`, `ChangeSet`, `ReadSet`, `ProcedureKind`.
- `@pulse/contract` (`oc`) + `@pulse/schema` (`v`, `defineSchema`) with type inference; `@pulse/server` `implement()` + middleware builder (TS only, no engine yet).
- **Verify:** a contract compiles; `InferInputs/Outputs` produce correct types in a typecheck test.

### M1 — Non-reactive RPC vertical slice
- `pulse-server` axum app with `POST /rpc`.
- `pulse-jsruntime`: embed V8, run a TS query/mutation handler with a basic `ctx.db` over `sqlx` (OLTP pool). No determinism sandbox yet.
- `@pulse/client` `createClient` + `@pulse/react` `queryOptions/mutationOptions` (plain request/response, no SSE).
- **Verify:** example-chat can `send` and `list` messages via TanStack Query against real Postgres (non-reactive). Mutation persists; list reflects it on manual refetch.

### M2 — Reactivity thin slice (the milestone that proves the thesis)
- `pulse-cdc`: WAL consumer on a slot/publication, decode `pgoutput`, advance LSN.
- `pulse-reactor`: **table-level** read-set registry + matching + debounce.
- `pulse-sse`: `GET /sync`, per-sub channel, push results (full results, not yet deltas).
- Client: SSE subscription wired so `reactive` `useQuery` updates live.
- **Verify:** two browser tabs open `messages.list`; sending in tab A updates tab B over SSE within ~150ms, no manual refetch.

### M3 — Precision & consistency
- Read-set capture upgraded to **key/range level** via instrumented query builder (`pulse-sql` read-set extraction).
- Result **diffing → deltas** over SSE; cross-query **LSN batch-advance** on the client.
- Subscription **de-duplication** across clients; query result cache keyed by `(path, input, LSN)`.
- **Verify:** a write to one channel does not re-run subscriptions for other channels (assert via metrics); client never renders torn state across two related queries (test).

### M4 — Determinism & mutation correctness
- V8 determinism sandbox: frozen time, seeded RNG, no net/fs in Q/M.
- Mutations in one `SERIALIZABLE` tx + `40001` retry; per-client `last_mutation_id` advanced in-tx.
- **Verify:** concurrent conflicting mutations serialize correctly; replayed mutation produces identical reads (determinism test).

### M5 — Local-first client
- Normalized keyed local store (confirmed + optimistic layers), IndexedDB persistence.
- Optimistic updates with auto-rollback; durable **offline mutation queue**; **rebase** on sync; write-checkpoint guard.
- SSE **Last-Event-ID** resume with per-sub ring buffer + `resync` fallback.
- **Verify:** go offline, mutate, reload, come back online → queued mutations replay, UI converges to server state with no lost or duplicated writes.

### M6 — Actions & analytical path
- `pulse-actions`: Node worker pool + IPC; `"use action"` files; `ctx.runQuery/runMutation` re-entry.
- `oc.analytical()` routed to OLAP replica pool with timeouts; streaming/`infiniteOptions`.
- **Verify:** an action calls Stripe (mock) then a mutation; an analytical query runs on the replica without affecting reactive p99 (load test).

### M7 — Schema, migrations, CLI, hardening
- `pulse-schema` DDL diff + migrations validated against existing rows; `@pulse/cli` `dev/deploy/migrate/gen`.
- Slot-lag/WAL-retention monitoring, backpressure, error taxonomy, auth middleware hardening, observability dashboards.
- **Verify:** schema change generates and applies a reviewed migration; chaos test (kill engine mid-stream) → slot replay, no dropped invalidations.

### Later (post-v1 seams already left in place)
- Incremental view maintenance / differential dataflow for hot query shapes (replace re-execution).
- Hasura-style multiplexing for popular parameterized queries at scale.
- Contract-first OpenAPI emission; horizontal scale-out of the engine (sharded subscription registry).

---

## 10. Implementation Status & Deviations (living)

> Tracks what is actually built vs. this spec. Updated as milestones land.

### Done

- **M0 — skeleton + type-inference core.** Cargo + pnpm workspaces; all crate/package shells; `pulse-core` types (`Lsn`, `ChangeSet`, `ReadSet`, `ProcedureKind`). `@pulse/schema` (`v`, `defineSchema`, `Doc`/`Id`/`Infer`), `@pulse/contract` (`oc`), `@pulse/server` (`os`/`implement`/middleware), `@pulse/client` (proxy client + TanStack options), `@pulse/react`. Verified by `tsc` (incl. `inference.test-d.ts`) + vitest.
- **M1 — non-reactive RPC slice (TDD).** `pulse-server` axum `POST /rpc`; Bun worker runs TS handlers; instrumented `ctx.db` proxies ops over NDJSON to the engine, which lowers the document query builder to SQL (`pulse-sql`) and owns Postgres. Input validation, error mapping (incl. structured `data`), and table-qualified ids (`table:uuid`). Verified end-to-end through `@pulse/client` against real Postgres, plus stress tests (concurrent load, pool saturation, worker backpressure, 15s soak).
- **Analytical raw SQL (pulled forward from M6).** `ctx.sql` tagged template → engine wraps as `SELECT to_jsonb(t) FROM (<user sql>) t`, binds params (table-qualified ids auto-decoded). Joins/CTEs/`GROUP BY`/aggregates work; verified through the client.
- **M2 — reactivity thin slice (TDD).** Client `subscribe()` over a multiplexed fetch-based SSE stream (`GET /sync`) + `POST /subscribe`/`/unsubscribe`. The engine captures each procedure's read-set (queries) and write-set (mutations) from its db ops; a write re-runs every subscription whose read-set intersects the write-set (table-level) and pushes the fresh result over SSE. Verified through the client: write in one client → pushed to a separate subscriber; multi-client fan-out; per-subscription isolation; unsubscribe stops pushes.
- **M5 — local-first client (TDD).** `@pulse/client` gains a `LocalStore` (confirmed + optimistic overlay with rollback and rebase), a durable `OfflineQueue` over a pluggable `KVStore` (`InMemoryKV`/`IndexedDbKV`), and a `LocalFirst` coordinator. `client.x.y.mutate(input, { optimistic })` applies optimism, durably enqueues, and flushes when reachable; subscriptions render the materialized view so optimistic state shows immediately and rebases on confirmed pushes. Verified through the client: offline write is queued and delivered on reconnect (no lost writes); a fresh client over the same storage replays the queue (reload); optimistic update visible before confirmation.
- **M4 — mutation transactions + serializable retry (TDD).** Each mutation runs all its db-ops inside ONE `SERIALIZABLE` Postgres transaction via a per-request transaction task in `pulse-jsruntime` (`execute_op` now takes `&mut PgConnection`; reads stay autocommit on pooled connections and run concurrently). A handler error rolls the whole mutation back; success commits. On a serialization failure (SQLSTATE `40001`/`40P01`, surfaced at an op or at commit) the engine **retries the entire deterministic handler** before returning `CONFLICT`. SERIALIZABLE is enforced as the session default on every pooled connection (`pulse_sql::connect` `after_connect`). Retry uses a generous budget (25 attempts) with exponential backoff + per-attempt jitter, so hot single-row contention converges instead of thundering-herd. Verified through the client: a write-then-throw leaves no row; a successful mutation commits; **50 concurrent read-modify-write increments of one counter row yield exactly 50 — no lost updates** (under the hood ~160 attempts/~167 conflicts resolve to 50 commits). A focused DB test (`pulse-sql/tests/isolation.rs`) independently proves the pool is genuinely SERIALIZABLE and a conflicting RMW raises `40001`. *Pending:* transactional `last_mutation_id` advance (M5-rebase integration) and the V8 determinism sandbox (frozen time / seeded RNG).
- **M6 — actions + OLAP isolation (TDD).** *OLAP isolation:* the engine opens a second `olap_pool` (`pulse_sql::connect_with` + `PoolConfig`) with its own connection budget and a longer `statement_timeout`; analytical procedures route their autocommit ops to it while reactive/mutation ops stay on the OLTP pool (low timeout, serializable). Decisively verified: with OLTP timeout 500 ms and OLAP 10 s, a reactive `pg_sleep(1s)` **times out** while an analytical `pg_sleep(1s)` **succeeds** — a heavy analytical query can't starve the reactive hot path. *Actions:* action-kind procedures get a non-transactional, non-deterministic context with `ctx.runQuery`/`runMutation`/`runAction` that re-enter the engine over `/rpc` (each `runMutation` is its own atomic mutation), with the action's auth header forwarded so identity propagates. Verified through the client: an action runs a mutation then a query, the write commits and is visible; an unauthenticated action is rejected. Targets are addressed by dotted path (typed contract-ref overloads are a follow-up); the spec's separate Node action-pool / `"use action"` directive is deferred — the existing Bun worker already provides Node I/O.
- **M7 — schema codegen + DDL/migrations + CLI (TDD).** `@pulse/cli` gains real `gen` and `migrate` commands over pure, unit-tested generators. `generateDataModel(schema)` walks the schema's `describe()` and emits the literal `PulseDataModel` augmentation (`Doc`/`Id` per table) — `pulse gen` now produces `examples-chat/src/_generated/dataModel.ts`, replacing the hand-written file (regenerated + typechecked identical). `generateDDL(schema)` emits idempotent `CREATE TABLE IF NOT EXISTS` (system columns + camelCase→snake_case user columns, `v.optional` → nullable, union-of-string-literals → `text`) plus `CREATE INDEX IF NOT EXISTS` per declared index. Verified: 9 codegen/DDL unit tests, plus an integration test that applies generated DDL to real Postgres, re-applies it (idempotent), and round-trips it through `information_schema` (columns, types, nullability, index). *Pending:* column-drift detection + `ALTER`/drop migrations (today additive create-and-extend), validating migrations against existing rows, and `pulse dev`/`deploy`.
- **Horizontal scaling — cross-node change bus (TDD).** The engine is now genuinely multi-node. `pulse-cdc` provides a Postgres `LISTEN/NOTIFY` change bus (`publish` + `start_listener` on channel `pulse_changes`): after a local mutation, a node applies invalidation locally **and** publishes the committed `ChangeSet` to the bus; every node's listener receives it and feeds foreign-origin change-sets into its own `apply_change_set` (precise per-read-set matching is preserved across nodes because the bus carries row images). Each node has a `node_id` and drops its own messages on receipt, so the bus is purely additive — single-node behavior is unchanged (verified: all 43 prior integration/stress/soak tests still green). Oversized NOTIFY payloads (>7800 B) degrade to a `Resync` marker → receiver `invalidate_all()` (safe over-approximation). Decisively verified with **two engine processes against one Postgres**: a write on node A pushes to a subscriber on node B, and cross-node matching stays precise (a foreign-channel write on A does *not* re-run a channel-A-only sub on B, while a same-channel write does). (The two-node tests also exposed a test-isolation hazard — leaked engine processes stay attached to the shared bus and pollute other runs — fixed with a process-level cleanup hook in the harness that SIGKILLs every spawned engine on exit. A second subtle bug surfaced too: the two multinode tests shared one node-B client with the same subscription key, so the first test's subscription lingered into the second (an extra cross-node push) — fixed by giving each test its own subscriber client. The integration/stress/soak/multinode suite is fully green with no leaked engines.) This is exactly the seam a WAL/`pgoutput` consumer replaces later (so out-of-Pulse writes also propagate) — the receive side is unchanged. *Pending for full scale-out:* SSE connection stickiness/affinity at the load balancer, sharded subscription registry for very large fan-out, and a non-NOTIFY bus (Redis/NATS or WAL) if NOTIFY throughput becomes the bottleneck.
- **Load testing.** A `tests/load/` harness measures throughput + p50/p95/p99 and asserts parallelism/contention bounds: reads scale ~2.9× from concurrency 1→16 (~780→~2250 ops/s, genuinely parallel); a real 2 s `pg_sleep` query runs fully concurrently with fast reads (fast-read p95 **1.0 ms during the slow query** vs ~1.2 ms baseline — no head-of-line blocking); 500 mutations at concurrency 64 through a 16-connection pool complete with zero deadlocks/errors (p99 ~80 ms) and all writes verified present; a mixed read/write workload sustains throughput with zero errors. **Findings & fixes:** (1) the suite caught an ~800 ms fast-read tail stall caused by synchronous per-call `console.log` on the single worker's hot path — per-call timing logging is now opt-in (`PULSE_LOG_TIMING`), restoring flat tail latency (systemic follow-up below); (2) it caught the slow-query fixture silently *not* sleeping (a bound param is text, and `pg_sleep(text)` doesn't resolve) — fixed with an explicit `::float8` cast, confirming the slow path is genuinely concurrent.

### Deviations from the spec (intentional, with rationale)

- **Q/M runtime is a Node/Bun worker, not embedded V8 (`deno_core`).** Chosen to reach the M2 reactive slice fastest. Rust still owns Postgres, SQL lowering, and read-set capture, so "Rust is the query engine" holds. Embedded-V8 + the deterministic sandbox remains the M4 target, kept behind the `pulse-jsruntime` interface.
- **stdout is the worker↔engine protocol channel.** User `console.*` is redirected to stderr so handler logging can't corrupt the NDJSON stream. A dedicated protocol fd (to also guard direct `process.stdout.write`) is deferred. **Known limit (found via load testing):** the stderr redirect is synchronous, so high-volume per-call logging on the single worker's hot path can stall its event loop (~800 ms fast-read tail under burst). Mitigated by making per-call logging opt-in; the systemic fix (a non-blocking buffered log sink, and/or multiple worker processes) is a scaling follow-up.
- **Reactivity invalidation is engine-captured write-sets, not WAL/CDC (yet).** Because all writes go through the engine, the write-set is known directly — no `pgoutput` parsing needed for the thin slice. The WAL consumer (`pulse-cdc`) remains the planned source for out-of-band writes; it plugs into the same read-set → match → re-execute → push pipeline.
- **Reactor lives in the `pulse-reactor` crate** behind a `Reactor` trait (extracted in M3). The `pulse-sse` crate (ring-buffer replay) is still pending; SSE framing currently lives in `pulse-server`'s `/sync` handler, but pushes already carry the `id`/`seq`/`commitLsn` the ring buffer will key on.

### Not yet implemented

- **Read-set precision deepening (post-M3):** filter/key/full-scan matching and whole-value push-diffing are done. Still pending: true multi-column index-range tuples, row-level array *deltas* (we ship whole-value-equality push-skip), cross-query LSN batch-advance on the client, and `ExecKey` cross-client re-exec dedup (designed in the M3 doc, deferred).
- **WAL CDC (post-scaling):** `pulse-cdc` now ships the cross-node `LISTEN/NOTIFY` change bus (engine-originated writes propagate to all nodes). Still pending: a logical-replication (`pgoutput`) consumer so writes made *outside* Pulse (psql, other apps) also produce `ChangeSet`s — it replaces the bus's *publish* step and reuses the same receive → `apply_change_set` path unchanged.
- **Query-builder coverage:** only `eq/gt/gte/lt/lte` (AND-combined), order by `_creation_time`, and `take/collect/first/unique`. No `OR`/`IN`/`LIKE`/`IS NULL`/JSONB ops/arbitrary order/offset in the *document* builder yet (raw `ctx.sql` is the escape hatch for everything else).
- **Actions/OLAP follow-ups (post-M6):** OLAP routing + the dedicated pool/timeout are done (set `PULSE_OLAP_DATABASE_URL` to point the OLAP pool at a real read replica). Still pending: a separate Node action worker pool with the `"use action"` directive and crash isolation (today actions share the Bun worker), typed contract-ref overloads for `ctx.runQuery/runMutation` (today dotted-path strings), and streaming/`infiniteOptions` for large analytical results.
- **Determinism + `last_mutation_id` (post-M4):** serializable transactions + `40001` retry are done. Still pending: the V8 determinism sandbox (frozen `Date.now()`/seeded RNG/no net — which makes handler *replay* on retry fully sound, though intent-based handlers are already safe), and bumping per-client `last_mutation_id` inside the mutation tx (plumbs `clientId`/`mutationId` from `@pulse/client` through the RPC envelope; integrates with the M5 offline-queue rebase watermark).
- **Local-first hardening (post-M5):** the offline queue + optimistic overlay + rebase + pluggable persistence are done; SSE `Last-Event-ID` resume/replay and a write-checkpoint guard for pull-vs-pending-write flicker remain.
- **`replace` semantics:** currently behaves like `patch` (partial); true full-row replace pending.
```
