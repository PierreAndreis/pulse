<div align="center">
  <img src="./assets/pulse-logo.png" alt="Pulse logo" width="160" height="160" />
  <h1>Pulse</h1>
  <p><strong>A reactive, local-first application platform on standard Postgres.</strong></p>
  <p>
    A <strong>Rust</strong> reactivity & sync engine and an end-to-end-typed
    <strong>TypeScript</strong> SDK — write your schema and server functions in
    TypeScript, call them from a fully-inferred client, and your queries update
    in realtime.
  </p>
</div>

<div align="center">

[![Rust](https://img.shields.io/badge/engine-Rust-CE422B?logo=rust&logoColor=white)](crates)
[![TypeScript](https://img.shields.io/badge/SDK-TypeScript-3178C6?logo=typescript&logoColor=white)](packages)
[![Postgres](https://img.shields.io/badge/database-Postgres-4169E1?logo=postgresql&logoColor=white)](scripts/dev-db.sql)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](#license)

</div>

<hr />

## The problem

Reactive backends give you a wonderful developer experience — write `query` /
`mutation` / `action` functions, define a schema, call them from a typed client,
and reads update in realtime with no manual cache invalidation. The catch is
that the great ones make you give up your database: a proprietary store you
can't reach with `psql`, `pg_dump`, BI tools, extensions like PostGIS or
pgvector, or plain SQL joins and window functions. Your data lives inside
someone else's engine.

## This solution

**Pulse keeps the database of record a standard Postgres you fully own** — and
puts the reactive programming model and end-to-end-typed DX on top of it:

- ✍️ **Author in TypeScript.** Write `query` / `mutation` / `action` functions
  and your schema with runtime validators that double as types.
- 🔗 **Call with zero codegen.** The client is inferred straight from your
  contract — an oRPC-style API that plugs into TanStack Query.
- ⚡ **Realtime by default.** Reactive queries push over SSE only when a write
  actually touches their read-set (precise, not table-wide), and aggregates like
  `count`/`sum`/`min`/`max`/`avg` are maintained incrementally from the change
  delta with no worker re-execution.
- 📴 **Local-first.** A durable IndexedDB offline queue, optimistic overlay with
  rebase, and a persisted read cache mean the UI works with the server down.
- 🔒 **Correct under contention.** Mutations are atomic and `SERIALIZABLE` with
  automatic `40001`/`40P01` retry; writes are exactly-once via idempotency keys.
- 🤝 **Conflict-free collaboration.** `v.collab()` fields are Yjs CRDTs that
  merge concurrent and offline edits instead of clobbering.
- 📊 **Real SQL when you need it.** Heavy analytical queries run on an isolated
  pool; `ctx.sql` is the raw escape hatch for joins, CTEs, and window functions.
- 📈 **Scales out.** Run many engine nodes on one Postgres; the change bus routes
  each write only to the nodes interested in the tables it touched — O(1) per
  write, not O(nodes). Tunable with [great defaults](docs/TUNING.md).

## Table of contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [The CLI](#the-cli)
- [Project layout](#project-layout)
- [Testing](#testing)
- [License](#license)

## Architecture

A write travels **node → Postgres → reactor → SSE**. The Rust engine owns the
Postgres pools, lowers the document API to SQL, and captures each procedure's
**read-set** and **write-set**; a committed write's change-set is matched — via a
table index plus per-row predicates — against only the subscriptions it could
affect, so just those queries re-run. Across multiple engine nodes it scales out
over a Postgres `LISTEN/NOTIFY` bus that routes each change **only to the nodes
interested in the tables it touched**.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design and
[`docs/decisions/`](docs/decisions) for the architecture decision records.

## Quick start

Create a fully-configured app — schema, contract, handlers, a Vite client, and
Docker Postgres — with one command:

```bash
npx @onveloz/pulse new my-app
cd my-app
pnpm install
pnpm db        # start Postgres (docker compose up -d)
pnpm gen       # generate the typed data model
pnpm engine    # run the Rust engine
pnpm dev       # start the app
```

### From this repo

To hack on Pulse itself or run the bundled example:

```bash
# 1. Install deps and start Postgres (logical replication enabled)
pnpm install
docker compose up -d

# 2. Build the engine
cargo build -p pulse-server

# 3. Generate the typed data model from your schema
pnpm pulse gen packages/examples-chat/src/schema.ts

# 4. Run the engine against the example app
pnpm pulse dev packages/examples-chat/src/app.ts
```

## The CLI

```
pulse new <name>                  scaffold a fully-configured app
pulse gen [schema.ts]             generate the Doc/Id data model
pulse migrate dev [name]          create a migration from the schema diff, then apply it
                                  (--create-only writes the file without applying)
pulse migrate deploy              apply pending migrations in order (CI / production)
pulse migrate status              show each migration as applied / pending / drifted
pulse db push                     sync the schema straight to the DB, no files (fast loop)
pulse dev [app.ts]                run the engine + worker against an app
pulse start [app.ts]              run the engine in production (after migrate deploy)
pulse deploy [app.ts]             build a release bundle
```

Migrations are file-based and editable, Prisma/Drizzle style. See
[`docs/MIGRATIONS.md`](docs/MIGRATIONS.md).

## Project layout

```
crates/                Rust workspace — the engine
  pulse-core           domain types: Lsn, ChangeSet, ReadSet, ProcedureKind
  pulse-sql            OLTP/OLAP pools, query builder → SQL, read-set capture
  pulse-collab         Yjs CRDT merge (yrs) for v.collab() fields
  pulse-cdc            cross-node change bus (LISTEN/NOTIFY)
  pulse-reactor        subscription registry: read-set matching + invalidation
  pulse-sse            SSE transport
  pulse-jsruntime      TS query/mutation execution + SERIALIZABLE tx + retry
  pulse-server         the engine binary (axum app, wiring)

packages/              TypeScript workspace — the SDK
  @pulse/schema        defineSchema / defineTable + v validators
  @pulse/contract      the oc contract builder (dependency-free)
  @pulse/server        os builder, implement(), middleware, handler ctx
  @pulse/client        createClient inference, local-first, offline queue
  @pulse/react         TanStack Query React bindings
  @pulse/runtime-node  the handler/action worker entrypoint
  @pulse/cli           pulse new | gen | migrate dev/deploy/status | db push | dev | start
  @pulse/examples-chat end-to-end example app
```

## Testing

```bash
pnpm typecheck && pnpm test       # TypeScript: typecheck + unit
cargo test                        # Rust: unit + DB-backed suites
pnpm test:integration             # full engine + Postgres (needs Docker)
```

The integration suite proves the correctness story end-to-end: cross-node
convergence, retry exhaustion into clean conflicts, money-transfer deadlock +
conservation, local-first overlay rebase, and exactly-once mutations.

## License

MIT
