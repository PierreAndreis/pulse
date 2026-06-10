# 03. Instrumented `ctx.db`, SQL Lowering, Text-Cast Binding, and Table-Qualified Ids

- **Status:** Accepted — consistent with `docs/ARCHITECTURE.md` (M1/M2 milestones, sections on "instrumented `ctx.db`" and table-qualified ids). One intentional deviation: the handler runtime is a Node/Bun worker driven over NDJSON/stdio, not an embedded V8 isolate. ARCHITECTURE.md already records this as a deliberate M1 simplification (`pulse-jsruntime` interface kept; embedded V8 deferred to M4). Read-set capture is currently **table-level only**, not the key/range tiering the architecture targets.

## Context & Problem

Pulse exposes a document-style `ctx.db` API (`get`, `query(...).withIndex(...).take/collect/first/unique`, `insert`, `patch`, `replace`, `delete`) plus a raw `ctx.sql` tagged template for analytics. User handlers run in JavaScript (the worker), but the design principle is **"Rust is the query engine"**: Rust owns the Postgres pools, SQL generation, and read/write-set capture for reactivity.

That forces several coupled decisions:

1. **Where does SQL get built?** If the worker built SQL strings and the engine just ran them, the engine could not capture read/write-sets reliably, could not enforce id encoding, and would have to trust handler-built SQL. So `ctx.db` must be *instrumented* — it cannot touch Postgres directly.
2. **How do dynamically-shaped values cross the JS↔Rust↔Postgres boundary?** Handler values are arbitrary JSON (`string | number | bool | null | object`), and the column types are only known at runtime from introspection. Writing per-type `sqlx` bind/decode plumbing for every Postgres type would be a large, brittle surface.
3. **How are ids represented?** Postgres stores bare `uuid`s, but the public API uses table-qualified ids that carry their table (so `ctx.db.get(id)` can route without a separate table argument, and so ids are self-describing on the wire). The engine needs to know which columns are ids and which table each references.

## Decision

### 1. `ctx.db` is a thin instrumented proxy; the engine owns SQL

`makeDb(requestId)` in the worker builds **no SQL**. Each method serializes a structured `DbOp` and sends it to the engine as a `dbop` NDJSON message, awaiting a correlated `dbresult`. The query builder is fluent but inert — `withIndex`/`order` only accumulate predicates and an order direction; the terminal method (`take`/`collect`/`first`/`unique`) is what emits the op. The wire `DbOp` is a tagged union owned by `pulse-sql`:

```
Get    { table, id }
Query  { table, predicates: [{ field, op: eq|gt|gte|lt|lte, value }], order?: asc|desc, limit?, mode: take|collect|first|unique }
Insert { table, value: {field: json} }
Patch  { table, id, fields }
Replace{ table, id, value }
Delete { table, id }
Raw    { sql, params }      // ctx.sql
```

`table` for id-addressed ops (`get`/`patch`/`replace`/`delete`) is derived in the worker from the id prefix (`tableOf(id)` splits on `:`), so those calls take no explicit table argument. The engine's `execute_op(pool, catalog, op)` is the single place SQL is generated and run.

Because the engine sees every op, it captures the read/write-set *before* executing: `DbOp::access()` returns `Some((table, is_write))` for the document ops and `None` for `Raw`. The runtime records `table` into the request's `reads` or `writes` set prior to running the op (so it is in place before the handler can `complete`). This is **table-level** granularity today — `Raw` is opaque and captures nothing.

### 2. SQL lowering (document builder → SQL)

`execute_op` lowers each op to a parameterized statement. Key shapes:

- **Get:** `SELECT <select_list> FROM <table> WHERE _id = $1::uuid`, returning the first row or `null`.
- **Query:** `SELECT <select_list> FROM <table>` + optional `WHERE` (predicates AND-joined as `<col> <op> $n::<cast>`) + optional `ORDER BY _creation_time ASC|DESC` + a `LIMIT` chosen by mode (`take` → user limit, `first` → `LIMIT 1`, `unique` → `LIMIT 2`, `collect` → none). `unique` errors (`NotUnique`) if more than one row comes back. Order is always by `_creation_time`; index names from `withIndex` are accepted by the builder but **ignored** by lowering.
- **Insert:** `INSERT INTO <table> (...) VALUES (...) RETURNING <select_list>` (or `DEFAULT VALUES` when no fields are given). Returns only the new `_id` string.
- **Patch:** lowers to `UPDATE <table> SET <col> = $n::<cast>, ... WHERE _id = $k::uuid` over only the provided fields; an empty field map is a no-op returning `null`. **Replace:** lowers to an `UPDATE` over **every** user column — provided fields to their value, omitted ones to `NULL` — preserving the system columns (`_id`, `_creationTime`). An omitted `NOT NULL` column surfaces a constraint error, since a full replace must supply every required field. Unknown fields are rejected in both.
- **Delete:** `DELETE FROM <table> WHERE _id = $1::uuid`, returns `null`.
- **Raw:** the user-authored SQL is wrapped as `SELECT to_jsonb(__pulse_sub) AS j FROM ( <user sql> ) AS __pulse_sub`, so arbitrary result columns decode dynamically as one `jsonb` value per row; returns an array.

Column/field name mapping is handled by `naming`: logical camelCase fields ↔ snake_case Postgres columns, with `_id` and `_creationTime`/`_creation_time` special-cased.

### 3. Uniform text-cast binding

Values **always** cross the SQL boundary as `text`, in both directions, so binding and decoding stay fully dynamic without per-type `sqlx` plumbing:

- **Reads:** every column is selected as `<col>::text AS "<col>"` (`Table::select_list`). Rows are read as `Option<String>` and converted to JSON by `text_to_json`, keyed off the column's coarse `PgTypeClass` (`Int8`→number, `Float8`→number, `Bool`→`true`/`t`, `Jsonb`→parsed JSON, else string; id columns → encoded id; `NULL`→`null`).
- **Writes:** params are bound as `Option<String>` (`json_to_bind`) and the **cast lives in the SQL text** as `$n::<cast>`, where `<cast>` comes from `PgTypeClass::cast()` (`uuid|text|int8|float8|bool|timestamptz|jsonb`, with `Other`→`text`). `null` JSON binds as `None`.
- For `Raw`, params are bound by `raw_bind` (also text); there is no catalog to consult, so values are stringified directly and id-looking strings are decoded heuristically (below).

`PgTypeClass` is a deliberately coarse classification of the Postgres `udt_name` (e.g. `int2|int4|int8`→`Int8`, `float4|float8|numeric`→`Float8`, `varchar|bpchar|name|citext`→`Text`). It exists only to drive the cast target and the text→JSON decode, not to model the full type system.

### 4. Table-qualified ids (`"table:uuid"`) from schema metadata

Public ids are `"<table>:<uuid>"`. `encode_id`/`decode_id` are the canonical helpers. The engine knows which columns are ids via the **catalog**, built by `introspect()` merging two sources:

- Postgres `information_schema.columns` (column name, `udt_name`, nullability) for `public`.
- Schema metadata sent by the worker in the `manifest` message, derived from the validator `describe()` output. Each field's `FieldMeta { kind, ref_table }` marks `kind == "id"` fields and their referenced table. The worker unwraps `optional` wrappers before classifying.

Rules: a `_id` column always references its own table; a user-declared `id` field references its `ref_table`. A column with an `id_ref` is encoded on read (`encode_id(ref_table, uuid)`) and decoded on write (`decode_id(value)` before binding as `uuid`). `decode_id` tolerates a bare uuid (returns it as-is). For `Raw` params, `decode_id_param` strips a `table:` prefix **only if** the suffix parses as a valid `Uuid`, so ordinary `"a:b"` strings pass through untouched.

## Alternatives Considered

- **Worker builds SQL, engine just executes it.** Rejected: the engine could not reliably capture read/write-sets (the whole reactivity model), could not enforce id encoding centrally, and would execute handler-controlled SQL for the document path. Keeping lowering in Rust is what lets "Rust is the query engine" hold even with a JS worker.
- **Per-type `sqlx` bind/decode (typed columns).** Rejected: requires enumerating and maintaining bind/decode arms for every Postgres type, and the column types are only known at runtime from introspection. The uniform text cast collapses this to one path at the cost of relying on Postgres's text I/O for correctness.
- **Bare-uuid ids with a separate table argument on every call.** Rejected: makes ids non-self-describing on the wire and forces every `get`/`patch`/`delete` to pass a table. Embedding the table in the id keeps the ergonomics and lets the worker route ops with just the id.
- **Engine-side SQL parsing of `ctx.sql` for fine-grained read-sets.** Rejected for now: arbitrary SQL (joins/CTEs/window functions) is hard to analyze soundly. `Raw` falls back to no read-set capture (table-level coarseness is the architecture's stated fallback), trading reactive precision for full Postgres power on the analytical path.
- **Embedded V8 isolate for handlers (the long-term target).** Deferred to M4. A Node/Bun worker over NDJSON reaches the reactive slice fastest while keeping the same `pulse-jsruntime` boundary.

## Consequences

**Pros**
- One dynamic value path (text in/out) — no per-type plumbing; new scalar types only need a `PgTypeClass` arm and a cast.
- The engine is the sole SQL author for the document API, so read/write-set capture, id encoding, and name mapping are enforced in exactly one place.
- Ids are self-describing; the worker routes id-addressed ops with no extra table argument.
- `ctx.sql` gets full Postgres power with automatic id-param decoding and dynamic result decoding via `to_jsonb`.

**Cons / costs later**
- **Text round-tripping leans on Postgres text representations.** Float precision, timestamp formatting, and `Bool` parsing (`"true"|"t"`) are tied to PG's text I/O; edge cases (e.g. numeric precision, non-UTC timestamps) may need revisiting. `Other` types silently degrade to `text`.
- **Read-set is table-level only.** Every reactive query invalidates on any write to a touched table; the key/range tiering in ARCHITECTURE.md §3.5 is not implemented here. `Raw` captures nothing, so analytical queries are not reactive.
- **Patch vs Replace are now distinguished in SQL.** Patch `SET`s only the provided fields (empty map → no-op). Replace writes every user column — omitted ones to `NULL` — for true full-document overwrite, preserving `_id`/`_creationTime`; an omitted `NOT NULL` column errors. (Was a latent correctness gap; fixed.)
- **`withIndex` is cosmetic.** Index names are ignored and ordering is hard-coded to `_creation_time`; predicates always become a flat `WHERE ... AND ...`. There is no index selection or true range scan yet.
- **Raw id decoding is heuristic.** `decode_id_param` guesses based on a valid-uuid suffix; a non-id string shaped like `prefix:<valid-uuid>` would be silently rewritten.
- **Catalog is built once from a manifest snapshot.** Schema changes after startup are not reflected without re-introspection.

## Testing Decisions

Verification is end-to-end through the public client (`@pulse/client`) against **real Postgres**, exercising the worker→engine→PG path — never the engine internals. This matches the existing harness (`tests/integration/harness.ts`) which boots `target/debug/pulse-server` with the Bun worker and the chat example app, then drives procedures via the typed client and asserts on returned documents. A good test here asserts *observable behavior* of `ctx.db`/`ctx.sql`, not generated SQL strings or `PgTypeClass` arms.

Prior art already covering this decision:
- `tests/integration/messages.test.ts` — `insert`/`query` round-trips, large-body and unicode verbatim preservation (exercises text-cast binding for `text`/`jsonb`), and explicitly `"encodes ids as table:uuid and round-trips the channel id"` (asserts `_id` matches `^messages:[0-9a-f-]{36}$` and that a returned id is reusable in a follow-up query — the table-qualified-id decision).
- `tests/integration/analytical.test.ts` — `ctx.sql` raw queries over real data, including CTEs + `GROUP BY` + aggregates (the `to_jsonb` wrapping + raw param binding decision).
- `crates/pulse-sql/src/naming.rs` has unit tests for the camelCase↔snake_case mapping and round-trips — the one piece tested in isolation because it is pure and deterministic.

What a good *new* test looks like for the gaps above: a client-level test that sends a value of each scalar kind (`int8`, `float8`, `bool`, `timestamptz`, `jsonb`, id ref) and asserts the value read back equals what was written (text round-trip fidelity). (Replace semantics are now covered: `tests/integration/orm.test.ts` asserts `replace` nulls omitted optional fields while `patch` leaves them intact.)

## Out of Scope / Deferred

- **Tiered (key/range-level) read-set capture** and fine-grained invalidation — deferred (ARCHITECTURE.md M2/M3); current capture is table-level.
- **Reactivity for `ctx.sql`** / static analysis of raw SQL read-sets — deferred; `Raw` is opaque.
- **Correct `replace` semantics** (nulling unspecified columns) — **shipped**.
- **Real index selection / range scans** behind `withIndex` — ignored today; ordering fixed to `_creation_time`.
- **Embedded V8 deterministic sandbox** for handlers — deferred to M4; current runtime is a Node/Bun worker.
- **Richer / exact Postgres type handling** beyond the coarse `PgTypeClass` (precise numerics, timestamp normalization, arrays, enums, extension types) — deferred.
- **Re-introspection on schema change** — catalog is built once from the manifest.
