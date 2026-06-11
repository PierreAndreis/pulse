# Changelog

All notable changes to Pulse (the `@onveloz/pulse-*` packages and the `pulse-server` engine) are recorded here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Pulse is pre-1.0: a **minor** bump signals new features, a **patch** signals fixes. Each version's GitHub Release carries the prebuilt `pulse-server` binaries that `@onveloz/pulse-engine` downloads.

## [0.2.0] - 2026-06-10

_The large feature release since v0.1.5 — reactive queries, incremental aggregates, multi-node scale-out, WAL/CDC ingest, and a fuller CLI. Changes are grouped by the package each ships in._

#### Engine (`pulse-server` binary, fetched by `@onveloz/pulse-engine`)
- Reactive query builder now covers nearly all single-table SQL with precise (or safely-coarse) reactive invalidation: `eq`/`neq`/`gt`/`gte`/`lt`/`lte`, `and`/`or`/`in` (lowered to DNF so OR/IN stay precise), `like`/`ilike`, `is null`/`is not null`, multi-column `orderBy`, offset pagination, scalar aggregates (`count`, `countDistinct`, `sum`, `min`, `max`, `avg`), and relational joins via handler composition. Fixed two reactivity bugs: read-sets were frozen at subscribe and never refreshed on re-execution (joins to newly-referenced rows went stale), and `orderBy` never deserialized (snake_case vs camelCase) so it silently sorted by creation time.
- Added reactive `groupBy(field).count()/sum()/min()/max()/avg()/countDistinct()` returning one `{key, value}` row per group, with the same precise invalidation as scalar aggregates.
- Added reactive `not(...)` completing the boolean filter algebra; negation is De-Morgan-pushed to leaves so it keeps precise reactivity (LIKE/ILIKE under NOT broaden safely).
- Added optional reactive `HAVING` on grouped aggregates (e.g. `.groupBy('tag').count({ gte: 2 })`), filtering post-aggregation without changing the read-set.
- Aggregate semantics aligned to SQL standard: `sum` returns NULL over an empty/all-NULL set (consistent with min/max/avg and Drizzle/Prisma), and added `count(field)` (non-NULL count).
- Fixed `ctx.db.replace(id, value)` to have true replace semantics — it now writes every user column (omitted ones set to NULL, preserving `_id`/`_creationTime`), instead of behaving like `patch` and leaving omitted columns intact. An omitted NOT NULL column surfaces a constraint error; unknown fields are rejected.
- Incremental View Maintenance (IVM): reactive aggregates are now maintained directly from the change delta with zero worker re-executions where possible — complete for `count(*)`, `sum`, `min`, `max`, and `avg` over both integer (int8) and double-precision columns. Falls back to re-execution for the cases it can't maintain exactly (e.g. an extreme-holding row leaving for min/max, emptied sets, count(distinct)/count(field)/grouped); re-exec remains the source of truth. Fractional-float sums are approximate; integer sums and min/max are exact.
- New `ivmApplied` field on `GET /metrics` reporting how many subscription updates were served by IVM (no worker re-exec), giving operators the IVM hit-rate.
- Batched point lookups: `DbOp::GetMany` collapses N concurrent `ctx.db.get(id)` calls into one `WHERE _id = ANY(...)` round-trip (DataLoader), preserving point-key read-set precision; missing/unparseable ids resolve to null.
- Real commit LSNs: mutations now carry an actual Postgres commit position (was always `Lsn::ZERO`), and each subscription's emitted `commitLsn` over SSE is guaranteed monotonically non-decreasing — a usable client consistency watermark.
- WAL/CDC consumer (opt-in via `PULSE_WAL=1`): out-of-band Postgres writes (raw SQL, other services, triggers) — previously invisible to the engine's write-set capture — now invalidate live subscriptions and push over SSE. Decodes pgoutput from a logical replication slot, uses advisory-lock leader election so exactly one node consumes the slot (with failover, gated by `PULSE_ROLE`), and dedups echoes of this node's own in-engine writes (Mode B). An out-of-band `TRUNCATE` triggers a coarse resync of subscriptions on the affected tables. Requires `wal_level=logical` + superuser for `CREATE PUBLICATION FOR ALL TABLES`. New `walDeduped` counter on `/metrics`.
- SSE resume (M5): `/sync` now honors the standard `Last-Event-ID` header (with `?lastEventId=` fallback) — a reconnecting client replays only the events it missed from a per-client ring buffer, or receives a `resync` control frame when the gap rolled past the buffer or the server lost state. An `id <= lastEventId` guard drops duplicates across the reconnect seam.
- Interest-routed change bus replacing the broadcast bus: a publisher NOTIFYs only the per-node channels of nodes that hold live subscriptions on the touched tables (registry `_pulse_node_interest`, TTL self-healing), eliminating O(nodes) fanout where every node evaluated every change. Measured ~8x less cross-node traffic at N=8 nodes. A global resync (or routing failure) still broadcasts. The bus listener now reconnects with backoff on connection loss and emits a resync to recover the missed window (a transient Postgres drop previously killed cross-node replication permanently).
- New operational tuning env knobs, all defaulting to current behavior: `PULSE_INTEREST_TTL_SECS` (30, interest freshness window), `PULSE_HEARTBEAT_MS` (10000, interest refresh interval, clamped to TTL/3), `PULSE_WAL_SAMPLE_MS` (100, commit-watermark sampler), `PULSE_SSE_BUFFER` (256, per-client SSE channel capacity), `PULSE_BUS_CHUNK` (default on, see below), and `PULSE_BUS_BROADCAST` (force global broadcast / routing kill-switch).
- Chunked publish (`PULSE_BUS_CHUNK`, default on) fixes the bulk-write 8KB cliff: a transaction writing more than ~8 rows previously blew past the NOTIFY cap and degraded cross-node delivery to a coarse table resync; oversized change-sets are now split into consecutive precise messages that each fit under the cap. Relatedly, oversized payloads that still can't fit degrade to a table-scoped resync (only subs on the touched tables re-run) rather than a global resync.
- Performance: table-indexed subscription matcher so a write only tests subs referencing a touched table (invalidation scales with the changed table, not total sub count — stays ~5µs as idle subs grow to 100k); identical subscriptions `(path, input, headers)` are coalesced into a single re-execution and fanned out (N identical subscribers now cost one round-trip); column-level read pruning for aggregate subscriptions (a value-only UPDATE is pruned unless it touches a depended-on column, so a bare `count()` ignores unrelated value updates); candidate matching runs by reference, cloning only matched subs; per-write propagation cost reduced by sampling the WAL LSN in a background task (removing an in-tx round-trip) and folding the interest lookup + all NOTIFYs into a single statement on one connection; and an O(1)-space zero-alloc LIKE matcher (~90x faster on prefix/suffix patterns).
- `/metrics` additionally exposes cross-node latency decomposition (`busLagMs`, `applyMs`), `busEvents` and active routing mode, and a `resyncs` coarse-resync counter.

#### `@onveloz/pulse-cli`
- `pulse migrate --apply` now applies the schema to a live database: additive changes (new tables/columns/indexes) apply automatically, while destructive changes (drops) and risky alters require consent — interactively it prompts before applying (default no), and non-interactively (no TTY) it refuses and exits 1 unless `--force` is passed. A `migrate` script (`pulse migrate --apply`) was added to the scaffold.
- New `pulse start <app.ts> [--migrate]` command for production: runs the prebuilt engine + worker with no codegen at boot (errors clearly if `app/_generated` is missing), and skips migrations unless `--migrate` is passed (which applies only safe additive changes). The scaffold Dockerfile now uses `pulse start --migrate` instead of `pulse dev`.
- Path-taking commands now have zero-arg defaults: `gen`/`migrate` default to `app/schema.ts`; `dev`/`start`/`deploy` default to `app/app.ts`. A leading flag is no longer mistaken for the path (e.g. `pulse dev --start vite`), missing files give a clear "run from your project root, or pass it explicitly" error instead of module-not-found, and the DB-down hint is now package-manager-agnostic (`docker compose up -d`).
- `pulse migrate` is now zero-config: schema defaults to `app/schema.ts`, and `--diff` falls back through `--database-url` → `DATABASE_URL` → the local docker-compose Postgres, so it works with no configuration instead of throwing.
- Schema gains `v.int()`, an integer column type that validates JS integers (`Number.isInteger`), maps to a Postgres `bigint`, and codegens to a TS `number`; `v.number()` still maps to double precision. Integer fields are carried in the change image, enabling incremental reactive `sum`/`min`/`max` over them.
- Generated apps now scaffold a tested deployment Dockerfile + `.dockerignore` for the Pulse backend (engine + bun worker), baking in the typed data model and pre-downloading the engine binary so startup needs no network.
- Generated apps now scaffold `AGENTS.md` (the Pulse build model, run commands, and house-rules; adds a Better Auth section with `--auth`) and a `CLAUDE.md` that imports it via `@AGENTS.md`.
- The CLI's platform detection now selects musl binaries on Alpine for arm64 (previously hard-coded to gnu) and adds the `aarch64-unknown-linux-musl` target, so the engine runs on Alpine on both x86_64 and arm64.

#### `@onveloz/pulse-runtime-node`
- Reactive query builder vastly expanded to cover nearly all single-table SQL with precise (or safely-coarse) reactive invalidation: filters `eq/neq/gt/gte/lt/lte`, `and/or/in` (OR/IN keep precise reactivity via DNF lowering), `like/ilike`, and `is null`/`is not null`; multi-column `ORDER BY`; offset pagination; reactive aggregates `count`, `count(distinct)`, `sum`, `min`, `max`, `avg`; and relational joins composed via handlers with precise, dynamically-tracked dependencies.
- Added `q.not(...)` completing the boolean filter algebra; renders `NOT(expr)` in SQL and pushes negation to leaves (De Morgan) so it keeps precise reactivity, with `LIKE`/`ILIKE` under `NOT` broadening safely.
- Added `groupBy(field).count()/sum()/min()/max()/avg()/countDistinct()` returning one `{key, value}` row per group, reusing the query filter read-set for the same precise reactive invalidation as scalar aggregates.
- Added an optional reactive `HAVING` predicate on grouped aggregates (e.g. `.groupBy('tag').count({ gte: 2 })`) that keeps only groups satisfying the comparison; filters post-aggregation without changing the read-set.
- `withIndex(name)` now actually orders by the named index's columns (previously the name was ignored and ordering stayed fixed to `_creationTime`): ascending by default, `.order(dir)` flips direction, and an explicit `.order(field)` overrides.
- Concurrent `ctx.db.get(id)` calls on the same table within a microtask tick (e.g. `Promise.all`) are now batched into a single `GetMany` op via a DataLoader; duplicate ids share one fetch and a missing id resolves to `null`. Sequential `for...await` gets still flush one-per-tick.
- Added `count(field)` (non-NULL count of a column); `sum` now returns `NULL` over an empty or all-NULL set (SQL standard, consistent with `min`/`max`/`avg`).
- Fixed reactivity bugs: read-sets were frozen at subscribe and never refreshed on re-execution, so a query's dependencies (e.g. a join to a newly-referenced row) went stale — read-sets are now refreshed on every re-run; and the query `orderBy` field never deserialized (snake_case vs camelCase), so order-by-field silently sorted by creation time.

#### `@onveloz/pulse-server` (procedure runtime)
- New `.filter(q => ...)` clause on `QueryBuilder` with a full predicate algebra over any column (not just indexes): `eq`/`neq`/`gt`/`gte`/`lt`/`lte`, `like`/`ilike` (string fields), `isNull`/`isNotNull`, `in`, and the boolean combinators `and`/`or`/`not` — all reactive, with precise (or safely-coarse) read-set invalidation. Exposes new exported types `FilterBuilder` and `FilterCond`.
- `order()` now takes an optional field argument (`order(direction, field?)`) and can be chained for multi-column sorts; previously sorted only by `_creationTime`. Fixes a bug where the `orderBy` field never deserialized (snake_case vs camelCase), so a chosen sort field was silently ignored and rows sorted by creation time.
- New `paginate({ limit, offset? })` for offset pagination on `QueryBuilder`.
- New reactive scalar aggregates on `QueryBuilder`: `count()`, `countDistinct(field)`, `sum(field)`, `min(field)`, `max(field)`, `avg(field)`. `count()` also accepts an optional field to count non-null values (`count(field)`). `sum()` now returns `number | null` (null over an empty/all-null set, per SQL standard) — previously typed/returned `number`.
- New `groupBy(field)` returning a `GroupedQuery` whose aggregate methods (`count`/`countDistinct`/`sum`/`min`/`max`/`avg`) resolve to one `{ key, value }` row per group, with the same filter-precise reactivity. Exposes new exported type `GroupedQuery`.
- Grouped aggregates accept an optional `HAVING` predicate (e.g. `.groupBy('tag').count({ gte: 2 })`) to keep only groups whose aggregate satisfies the comparison. Exposes new exported type `HavingPredicate`.
- Reactive joins are now supported (relational reads via handler composition), tracked precisely per row.
- Bug fix: query read-sets were frozen at subscribe time and never refreshed on re-execution, so dependencies added on re-run (e.g. a join to a newly-referenced row) went stale; the read-set is now recomputed on every re-run, keeping reactive invalidation correct.

#### `@onveloz/pulse-client`
- SSE streams now resume on reconnect: the client tracks the highest event id and resends it as the standard `Last-Event-ID` header, so a reconnecting client replays only the events it missed instead of re-subscribing everything. A duplicate guard (`id <= lastEventId`) drops events delivered across the reconnect seam, and a server `resync` control frame triggers a full re-registration of all subscriptions (e.g. after a server restart or when the replay buffer rolled past the gap). Subscriptions added while disconnected are registered on reconnect without re-subscribing the ones the server still holds.
- Fixed reactive aggregates being coerced from `null` to `[]`: a live `sum`/`min`/`max`/`avg` over an empty set is SQL NULL, and the server's `data: null` is now preserved and delivered to subscribers as `null` (honoring the `number | null` output type) instead of an empty array. A genuinely missing `data` field still defaults to `[]`.

#### `@onveloz/pulse-schema`
- Added `v.int()` column validator: validates JS integers (`Number.isInteger`), maps to a Postgres `bigint` column, and codegens to a TypeScript `number`. Additive and non-breaking — `v.number()` still maps to `double precision`. Integer columns are carried in the change image, enabling reactive `sum`/`min`/`max` aggregates over an int field to be maintained incrementally instead of re-executed.

#### `@onveloz/pulse-engine` (binary resolver)
- Added an `aarch64-unknown-linux-musl` target so the engine binary now resolves on ARM64 Alpine (musl); previously Linux arm64 was hard-coded to glibc (`aarch64-unknown-linux-gnu`) and would pick the wrong/incompatible binary on Alpine.
- Linux binary selection now picks musl vs glibc by detected libc on both x64 and arm64 (the `aarch64-unknown-linux-musl` triple was added to the public `EngineTarget` type).
- `detectTarget()` gained an optional third `muslHost` parameter (defaults to runtime libc detection) allowing the libc choice to be overridden.

## [0.1.5] - 2026-06-02

### Added
- `pulse new` is now an interactive scaffolder (@clack/prompts): it walks you through project name, package manager, template, auth, dependency install, Postgres setup, and `git init`, with full flag/non-TTY fallback for scripted use.
- Package-manager support beyond npm: pnpm, yarn, and bun are auto-detected, and generated command strings/README adapt to the one you pick.
- New `minimal` template alongside the default.
- `--auth` flag scaffolds an end-to-end Better Auth setup: a `betterAuth + jwt()` server served by the Vite dev server (`toNodeHandler` at `/api/auth/*`), bearer verification via `pulseAuth` that populates `ctx.userId`, user-scoped todos, a sign-in/up UI, and an `auth:migrate` script. Better Auth's tables live in a dedicated `auth` Postgres schema so `pulse dev`'s public-scoped sync never drops them.

### Fixed
- Base template no longer fails typecheck in the generated app: added the missing `src/vite-env.d.ts` (for `import.meta.env`) and corrected the reactive subscribe call to `subscribe({}, ...)` instead of `subscribe(undefined, ...)`.

## [0.1.4] - 2026-06-02

### Changed
- `pulse` schema sync now applies destructive drops automatically only when nothing is lost — an empty table or a column whose values are all NULL is dropped, while any table with rows or column with data is refused and listed for your review. Destructive diffs are reported as structured drops so each one can be data-checked before being applied.

### Fixed
- Corrected the `pulse new` next-steps hint to a single `pnpm dev` (removed the stale separate gen/engine steps).

## [0.1.3] - 2026-06-02

### Added
- `pulse dev` is now an all-in-one command (Convex-style): it generates the typed data model, syncs your schema to the database, starts the engine, and optionally runs your frontend via `--start <cmd>` (e.g. `pulse dev app/app.ts --start vite`). Schema sync reuses the diff engine to auto-apply safe additive changes (new tables/columns/indexes) while refusing risky alters (type/nullability changes) and destructive drops, printing them for review via `pulse migrate --diff`. Engine-managed `_pulse*` tables are excluded from drop detection.

### Changed
- `pg` is now a direct dependency of `@onveloz/pulse-cli` (promoted from an optional peer) since schema sync requires it.
- Scaffolded projects use a single `pnpm dev` command and align the default DB name with the CLI default, so a fresh project connects with zero config.

## [0.1.2] - 2026-06-02

### Fixed
- Fresh installs now work identically across npm, pnpm, yarn, and bun. The `@onveloz/pulse-engine` package no longer downloads its binary via a `postinstall` hook (which package managers gate differently — pnpm 11 blocked it, leaving the binary missing); the engine binary is instead fetched lazily on the first `pulse dev` via the CLI's `ensureEngine()`. No build-script approval is required, and the `pnpm.onlyBuiltDependencies` / bun `trustedDependencies` allowlist is gone from scaffolded apps.

### Changed
- Scaffolded apps now pin the Pulse dependency to `^<cli-version>` injected from the CLI's own version, instead of a hardcoded constant that could drift out of sync.

## [0.1.1] - 2026-06-02

### Added
- New `npx @onveloz/pulse new` umbrella package for scaffolding a Pulse app. Scaffolded projects now allowlist the engine and esbuild postinstall scripts (pnpm `onlyBuiltDependencies` / bun `trustedDependencies`), so the engine binary downloads instead of being silently blocked by `ERR_PNPM_IGNORED_BUILDS`.

### Fixed
- `pulse dev` and `pulse deploy` no longer fail with "Module not found" on a clean npm/pnpm/bun install. The CLI now resolves its runtime-node worker through normal node module resolution and declares `@onveloz/pulse-runtime-node` as a dependency, fixing the previously monorepo-only worker path.

## [0.1.0] - 2026-06-02

_Initial release — the foundational Pulse engine and SDK._

### Added

- Reactive Postgres platform: author `query` / `mutation` / `action` functions and your schema in TypeScript, then call them from a fully-inferred client (no codegen). Reactive queries re-run and push over SSE only when a write actually touches their read-set — precise invalidation, not table-wide.
- Schema with runtime validators that double as types (`v.int()`, etc.), including `v.collab()` Yjs CRDT fields that merge concurrent and offline edits instead of clobbering.
- TypeScript SDK: end-to-end-typed client inferred from your contract, TanStack Query bindings, auto-reconnecting SSE, and a one-line collab editor binding over Yjs.
- Local-first client: durable IndexedDB offline queue, optimistic overlay with rebase, and a persisted read cache so the UI keeps working with the server down.
- Correctness guarantees: mutations are atomic and `SERIALIZABLE` with automatic `40001`/`40P01` retry; writes are exactly-once via client idempotency keys recorded in-transaction.
- Cross-node sync via Postgres `LISTEN`/`NOTIFY`, so subscriptions on any node see committed changes.
- Raw SQL escape hatch (`ctx.sql`) for joins, CTEs, and window functions, running on an isolated analytical pool.
- `@onveloz/pulse-auth`: a Better Auth (JWT/JWKS) plugin — `pulseAuth({ jwksUrl, issuer, resolveUserId })` exposes an `authed` middleware with `ctx.auth` / `ctx.userId`; `bearerAuth(getToken)` wires tokens on the client. Works with any provider that issues a signed JWT.
- CLI: schema codegen (`pulse gen`), additive DDL with live migration diff, `pulse dev` to run the engine, `pulse deploy` for release bundles, and `pulse new <name>` to scaffold a complete runnable app.
- `@onveloz/pulse-bundler`: `definePulseApp()` — a one-line Vite + React + SSE-safe dev server preset with SDK resolution.
- `@onveloz/pulse-engine`: Prisma-style binary fetcher that downloads and SHA256-verifies the matching `pulse-server` for your platform from GitHub Releases.

### Fixed

- camelCase table names (e.g. `issueLabels`) that Postgres folds to lowercase now resolve correctly, including `table:uuid` decoding of their id fields.
- Offline queue serializes its operations so concurrent cold-cache `enqueue()` calls no longer silently drop writes.
- Derived/optimistic queries are now notified precisely on confirmed changes — queries deriving from a changed key no longer stay stale, and blanket over-notification on every change is gone.

### Performance

- Fanned-out subscriptions re-execute in parallel, so a single write affecting N subscriptions no longer pays N sequential worker + Postgres round-trips.
- The reactor no longer holds the global clients lock across an SSE channel send, so one stalled/slow consumer can't head-of-line-block pushes and client registration for everyone else.

<!-- Version diffs (commit ranges) -->
[0.2.0]: https://github.com/PierreAndreis/pulse/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/PierreAndreis/pulse/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/PierreAndreis/pulse/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/PierreAndreis/pulse/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/PierreAndreis/pulse/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/PierreAndreis/pulse/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/PierreAndreis/pulse/releases/tag/v0.1.0
