# Changelog

All notable changes to Pulse (the `@onveloz/pulse-*` packages and the `pulse-server` engine) are recorded here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Pulse is pre-1.0: a **minor** bump signals new features, a **patch** signals fixes. Each version's GitHub Release carries the prebuilt `pulse-server` binaries that `@onveloz/pulse-engine` downloads.

## [Unreleased]

#### CLI (`@onveloz/pulse-cli`)
- **File-based migrations (Prisma/Drizzle style).** `pulse migrate dev [name]` diffs the schema against the last snapshot and writes an editable `migrations/NNNN_<name>.sql` (destructive drops commented out) plus a snapshot, then applies pending migrations to the dev DB and regenerates the data model. `pulse migrate deploy` applies pending files in order (each in its own transaction), recorded in a `_pulse_migrations` journal, and **refused if an already-applied file was edited** (content-hash drift). `pulse migrate status` lists each as applied / pending / drifted. `pulse db push` keeps the old no-files live sync.
- Generated migrations now handle **index removal** (`DROP INDEX`, scoped so primary keys and engine-managed indexes are never touched) and **index redefinition** (an index that keeps its name but changes columns is dropped and re-created).

#### Engine (`pulse-server` binary)
- New `/metrics` field **`ivmPushed`**: IVM-maintained values actually delivered to a client (post diff-suppression), the delivery-confirmed companion to `ivmApplied`.

## [0.2.0] - 2026-06-10

_Reactive queries + incremental aggregates, multi-node scale-out, WAL/CDC ingest, and a production CLI. Grouped by package._

#### Engine (`pulse-server` binary, via `@onveloz/pulse-engine`)
- **Incremental aggregates (IVM):** reactive `count`/`sum`/`min`/`max`/`avg` over `int8` and `double` columns are maintained from the change delta with no worker re-execution (falling back to re-exec where exact maintenance isn't possible — emptied sets, an extreme-holder leaving, `count(distinct)`). `ivmApplied` on `/metrics` reports the hit-rate.
- **Reactive query coverage:** filters (`eq/neq/gt/gte/lt/lte`, `and/or/in`, `like/ilike`, `is null`, `not`), multi-column `orderBy`, offset pagination, scalar + grouped (`groupBy`/`HAVING`) aggregates, and handler-composed joins — all with precise (DNF-lowered) invalidation. SQL-standard nulls (`sum` of an empty set → `NULL`).
- **`replace()`** now has true full-row semantics — omitted columns are set to `NULL` (it previously behaved like `patch`).
- **Out-of-band writes (WAL/CDC, opt-in `PULSE_WAL=1`):** raw-SQL / other-service writes now invalidate live subscriptions — decodes pgoutput from a logical slot, leader-elected via advisory lock, dedups in-engine echoes.
- **Multi-node scale-out:** interest-routed change bus (NOTIFYs only nodes subscribed to the touched tables — ~8× less cross-node traffic at 8 nodes; reconnect + resync on drop), real commit LSNs with a monotonic per-subscription watermark, and SSE resume via `Last-Event-ID`.
- **Batched `ctx.db.get()`** — concurrent same-table gets collapse into one `WHERE _id = ANY(...)` round-trip (DataLoader).
- **Performance:** table-indexed matcher (invalidation scales with the changed table — ~5µs at 100k idle subs), identical-subscription coalescing, aggregate column-pruning, chunked publish (`PULSE_BUS_CHUNK`, fixes the >8-row NOTIFY cliff), and an O(1)-space LIKE matcher.
- **Knobs & metrics:** new env tuning (`PULSE_INTEREST_TTL_SECS`, `PULSE_HEARTBEAT_MS`, `PULSE_WAL_SAMPLE_MS`, `PULSE_SSE_BUFFER`, `PULSE_BUS_CHUNK`, `PULSE_BUS_BROADCAST`), all defaulting to prior behavior; new `/metrics` fields (`walDeduped`, `busLagMs`/`applyMs`, `busEvents`, `resyncs`).

#### `@onveloz/pulse-server` + `@onveloz/pulse-runtime-node` (query-builder surface)
- The TypeScript API for the engine features above: `.filter()` (full predicate algebra incl. `not`), chainable multi-column `.order(dir, field?)`, `.paginate({ limit, offset })`, aggregates (`count`/`countDistinct`/`sum`/`min`/`max`/`avg`; `sum` is now `number | null`), `groupBy(field)` with optional `HAVING`, and `withIndex(name)` now ordering by the index's columns (the name was previously ignored). New exported types: `FilterBuilder`, `GroupedQuery`, `HavingPredicate`.

#### `@onveloz/pulse-schema`
- `v.int()` — an integer column (Postgres `bigint`, codegens to `number`) alongside `v.number()` (double precision). Integer fields are carried in the change image, which is what lets `sum`/`min`/`max` over them be maintained incrementally.

#### `@onveloz/pulse-cli`
- `pulse start <app> [--migrate]` for production (prebuilt engine + worker, no boot-time codegen).
- `pulse migrate --apply` auto-applies safe additive changes; destructive/risky ones require consent (or `--force` non-interactively). Zero-config: path commands default to `app/schema.ts` / `app/app.ts`, and `migrate` falls back through `DATABASE_URL` / the local docker Postgres.
- Scaffolds a tested deployment Dockerfile and `AGENTS.md` / `CLAUDE.md`; selects musl binaries on Alpine (incl. arm64).

#### `@onveloz/pulse-client`
- SSE resume on reconnect (`Last-Event-ID` replay + `resync`) — a dropped connection replays only missed events instead of re-subscribing everything.
- Fixed: a reactive `sum`/`min`/`max`/`avg` over an empty set is now delivered as `null` instead of being coerced to `[]`.

#### `@onveloz/pulse-engine`
- Added the `aarch64-unknown-linux-musl` target; Linux binary selection now picks musl vs glibc by detected libc on both x64 and arm64.

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
[Unreleased]: https://github.com/PierreAndreis/pulse/compare/v0.2.0...main
[0.2.0]: https://github.com/PierreAndreis/pulse/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/PierreAndreis/pulse/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/PierreAndreis/pulse/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/PierreAndreis/pulse/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/PierreAndreis/pulse/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/PierreAndreis/pulse/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/PierreAndreis/pulse/releases/tag/v0.1.0
