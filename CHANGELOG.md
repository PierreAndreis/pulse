# Changelog

All notable changes to Pulse (the `@onveloz/pulse-*` packages and the `pulse-server` engine) are recorded here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Pulse is pre-1.0: a **minor** bump signals new features, a **patch** signals fixes. Each version's GitHub Release carries the prebuilt `pulse-server` binaries that `@onveloz/pulse-engine` downloads.

## [0.2.0] - 2026-06-10

_The large feature release since v0.1.5 — reactive queries, incremental aggregates, multi-node scale-out, WAL/CDC ingest, and a fuller CLI._

### Added
- **Reactive query builder** covering nearly all single-table SQL with precise (or safely-coarse) invalidation: `eq/neq/gt/gte/lt/lte`, `and/or/in` (kept precise via DNF lowering), `like/ilike`, `is null/is not null`, multi-column `orderBy`, offset pagination, and relational joins composed across handlers with per-row dependency tracking.
- **Reactive aggregates** — `count`, `sum`, `min`, `max`, `avg` — plus reactive `GROUP BY`, `HAVING` on grouped aggregates, and reactive `NOT` (De Morgan push-down).
- **`v.int()` column type** — a true integer (Postgres `bigint`, codegens to `number`) alongside `v.number()` (double precision). Additive and non-breaking. Integer columns are what let field-aggregate IVM actually fire (see Performance).
- **`withIndex(name)`** now orders by a declared index's columns (`.index(name, [cols])`), honoring `.order('asc'|'desc')` and explicit `.order(field)` overrides — previously the name was ignored and ordering was fixed to `_creationTime`.
- **Batched `ctx.db.get()`** — concurrent same-table gets in one tick (e.g. `Promise.all`) collapse into a single `GetMany` round-trip via a DataLoader, with per-key read-set precision preserved.
- **Multi-node scale-out**: an interest-routed change bus that NOTIFYs only nodes subscribed to the touched tables (with global-broadcast fallback for correctness), real commit LSNs with a monotonic per-subscription watermark, and automatic bus reconnect with missed-window resync.
- **WAL/CDC consumer (opt-in)** so out-of-band writes (writes not made through Pulse) go live: decodes `pgoutput` from a logical replication slot, dedups in-engine echoes, and elects a single leader via advisory lock.
- **Operational tuning knobs**, all defaulting to current behavior: `PULSE_INTEREST_TTL_SECS`, `PULSE_HEARTBEAT_MS`, `PULSE_WAL_SAMPLE_MS`, `PULSE_SSE_BUFFER`, and `PULSE_BUS_CHUNK` (chunked publish, on by default). A tuning guide documents every knob and default.
- **CLI**: `pulse start` for production (the scaffolded Dockerfile now uses it instead of `dev`); `pulse migrate --apply` with a Prisma-style destructive-change guard; zero-config migrate (default schema + `DATABASE_URL`); zero-arg defaults and clearer errors across path commands; and scaffolding for a tested deployment Dockerfile plus `AGENTS.md`/`CLAUDE.md` house-rules.

### Changed
- `ctx.db.replace(id, value)` now has true replace semantics: every user column is written, with omitted columns set to NULL (an omitted NOT NULL column raises a constraint error). It previously behaved like `patch`, leaving omitted columns intact.

### Fixed
- A reactive `sum`/`min`/`max`/`avg` over an empty set is now correctly delivered as `null` (SQL NULL) instead of being coerced to `[]`, matching the `number | null` output type.
- Engine binary now builds across a broad range of Linux targets (slim / old-glibc gnu and Alpine musl).

### Performance
- Reactive aggregates (`count`, `sum`, `min`, `max`, `avg`) are now maintained **incrementally** from change deltas — the new value is pushed without re-running the query. `min`/`max` maintain a rising extreme and fall back to a re-exec only when the row holding the extreme leaves; `avg` is seeded from the engine and maintained from running sum+count; fractional-float sums remain approximate (re-exec is the source of truth). `count(distinct)` intentionally still re-execs.
- Invalidation now scales with the changed table rather than total subscription count (table-indexed matcher), identical subscriptions coalesce into a single re-execution, aggregate subscriptions prune to only the columns they read, oversized cross-node payloads are scoped to touched tables instead of triggering a global resync, and the `LIKE` matcher runs in O(1) space with zero-alloc fast paths.

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

I have everything I need. This is the initial release — the vast majority is the foundational commit, plus the auth plugin, a SQL fix, client/reactor fixes, and the rest is CI/release plumbing.

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
