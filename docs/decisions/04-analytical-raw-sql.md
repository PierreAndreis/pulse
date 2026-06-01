# 04. Analytical Raw `ctx.sql` via `to_jsonb` Wrapping

- **Status:** Accepted — implemented as the M6 raw-SQL slice pulled forward (see ARCHITECTURE.md §11 "Analytical raw SQL"). One part is an explicit, documented **deviation**: analytical procedures run on the OLTP pool today, not the dedicated read-replica/`statement_timeout` path the architecture describes (ARCHITECTURE.md §6, §15 "OLAP isolation"). That isolation is pending.

## Context & Problem

The document query builder (`ctx.db`) deliberately supports only a thin slice of SQL: `eq/gt/gte/lt/lte` predicates AND-combined, `order by _creation_time`, and `take/collect/first/unique` (ARCHITECTURE.md §15). That is enough for the reactive hot path, where fine-grained read-set capture matters, but it cannot express the queries an analytical procedure (`oc.analytical()`) actually needs: joins across tables, CTEs, `GROUP BY` + aggregates, window functions, subqueries.

We needed an escape hatch that:
- gives handler authors the **full power of Postgres SQL** for heavy/aggregate reads, without growing the document builder into a SQL compiler;
- works with **arbitrary, statically-unknown result shapes** (a `GROUP BY` produces columns that exist in no catalog table);
- keeps Pulse's id ergonomics — handlers pass `Id<"t">` values that on the wire are table-qualified strings (`"channels:<uuid>"`), but Postgres columns hold bare `uuid`;
- stays **read-only** and stays out of the reactor (an analytical query must never register a read-set or block invalidation).

The document-builder ops (`Get/Query/Insert/...`) decode results column-by-column using the catalog (`text_to_json` keyed by `Column.type_class` / `id_ref`). That machinery is useless for an opaque `SELECT` whose columns are not catalog columns — so raw SQL needs a different decode strategy.

## Decision

Add a new opaque op variant to the SQL layer and expose it as a tagged-template `ctx.sql` on analytical contexts.

**Wire op** (`pulse-sql`, `DbOp`):

```rust
/// Raw analytical SQL (read-only). The user writes the SQL and any casts;
/// params are bound as text with table-qualified ids decoded to their uuid.
Raw { sql: String, #[serde(default)] params: Vec<Value> }
```

1. **Author surface — tagged template.** `ctx.sql` is a `SqlTag`: `<Row = …>(strings, ...values) => Promise<Row[]>`. The Node worker assembles the template into a parameterized statement, turning each interpolation into a positional placeholder `$1, $2, …` and collecting the values, then emits `{ kind: "raw", sql, params }`. Authors write their own casts inline (e.g. `${input.channelId}::uuid`), so the engine never has to infer column types for the user's SQL. The generic `<Row>` is a pure TypeScript assertion on the returned row shape — unchecked at runtime.

2. **Dynamic decode via `to_jsonb` wrapping.** Because the result columns are not catalog columns, the engine cannot decode them per-column. Instead it wraps the user's SQL in a derived table and lets Postgres serialize each row to one JSON value:

   ```sql
   SELECT to_jsonb(__pulse_sub) AS j FROM ( <user sql> ) AS __pulse_sub
   ```

   Each output row is read as a single `jsonb` column `j` and returned as one element of a JSON array. Postgres owns the type→JSON mapping (ints stay numbers, `bool` stays boolean, nested `jsonb` stays an object), so any column list works without a catalog lookup.

3. **Param binding — text with an id-decode heuristic.** Params are bound positionally as text (mirroring how the document-builder ops bind everything cast-to/from `text`). Strings get a heuristic id decode: a `"prefix:rest"` string is stripped to `rest` **only when** `prefix` is non-empty **and** `rest` parses as a UUID; otherwise the string passes through verbatim. This lets a handler interpolate an `Id<"channels">` directly and have it bind as the bare uuid, while ordinary strings containing a colon (URLs, timestamps, `"a:b"`) are left untouched.

   ```rust
   fn decode_id_param(s: &str) -> &str {
       if let Some((prefix, rest)) = s.split_once(':') {
           if !prefix.is_empty() && Uuid::parse_str(rest).is_ok() { return rest; }
       }
       s
   }
   ```

   `null` binds as SQL `NULL`; numbers/bools bind as their text form; arrays/objects bind as their JSON text (the author casts as needed).

4. **Opaque to the reactor.** `DbOp::access()` returns `Some((table, is_write))` for every document op but `None` for `Raw`. The reactor uses `access()` to build read/write-sets; returning `None` means a raw query contributes no read-set and no write-set. This is the coarse end of the read-set spectrum (`ReadSet.tables` exists as the "un-analyzable raw SQL" fallback in `pulse-core`), but for analytical procedures it is intentionally *nothing*: analytical procedures are non-reactive, so there is no subscription to invalidate. Raw SQL is documented as read-only by contract; the variant is not write-guarded at the SQL layer (see Consequences).

## Alternatives Considered

- **Grow the document builder to cover joins/CTEs/aggregates.** Rejected: turns a deliberately shallow, reactive-friendly builder into a full SQL compiler, with type inference for arbitrary projections — exactly the complexity the builder's narrow scope avoids. The architecture explicitly frames raw `ctx.sql` as the escape hatch for "everything else" (ARCHITECTURE.md §15).

- **Per-column decode of raw results using runtime Postgres type OIDs.** Rejected: would require reading each result column's type from the row metadata and replicating Postgres's type→JSON logic in Rust for every type (numeric, arrays, ranges, composite, extension types like PostGIS/pgvector). `to_jsonb` already does this correctly inside Postgres for free. The cost is one wrapping subquery.

- **Return raw text / typed Rust structs per query.** Rejected: text loses JSON structure (numbers vs strings, nesting); typed structs are impossible when the shape is unknown at compile time. A single dynamic `jsonb` value per row is the only shape that handles arbitrary projections and round-trips cleanly to the TS `Row` generic.

- **Decode ids by inspecting the SQL / catalog to know which params are ids.** Rejected as too clever and fragile for a v1 escape hatch — it would mean parsing arbitrary SQL. The `prefix:uuid` heuristic is a cheap, local string test that handles the only ambiguous case (a real id) and leaves everything else alone.

- **Forbid raw params entirely; require authors to inline literals.** Rejected: invites SQL injection and breaks the ergonomic tagged-template surface. Positional binds keep values out of the SQL string.

- **Route analytical queries to a replica pool now (full §6 isolation).** Deferred — see Out of Scope.

## Consequences

Pros:
- Full Postgres power for analytical handlers (joins, CTEs, window functions, aggregates, extensions) with a one-line author surface and zero catalog coupling.
- Arbitrary result shapes "just work" — no codegen, no per-query decode logic; Postgres owns serialization.
- Ids interpolate naturally; the heuristic keeps the `Id<"t">` ergonomics of the document builder on the raw path.
- The op is cleanly opaque to the reactor (`access() == None`), so the analytical path can never accidentally create a subscription or block invalidation.

Cons / future cost:
- **Read-only is by convention, not enforced.** The variant carries no write guard at the SQL layer; an author could put a writing CTE (`INSERT ... RETURNING`, `DELETE`) in `ctx.sql`. Since it bypasses `access()`, such a write would be invisible to read-set/write-set tracking and would *not* invalidate subscribers — a correctness footgun. Enforcement (e.g. running on a read-only transaction / replica) is the job of the deferred OLAP-isolation work.
- **No reactivity, by design.** Results never update; this is correct for analytical procedures but means raw SQL is the wrong tool for anything that should be live.
- **`to_jsonb` materializes each row as JSON** in Postgres — fine for analytical/aggregate result sets, an extra cost to keep in mind for very wide/large outputs (streaming large results is deferred, ARCHITECTURE.md §6).
- **The id-decode heuristic can be wrong in principle:** a non-id string that happens to be `"<nonempty>:<valid-uuid>"` would be silently stripped to the uuid. Considered acceptable — such strings are vanishingly rare and the author can cast/format to avoid the shape.
- **OLTP-pool deviation** (see Status): a long analytical scan currently runs on the reactive pool and is bounded only by that pool's `statement_timeout`, so it *can* contend with reactive traffic until the replica path lands.

## Testing Decisions

A good test here exercises the raw path **end to end through the public client**, not the internal op shape or the `to_jsonb` string. It sends real data via the public API, calls an `analytical` procedure that uses `ctx.sql`, and asserts on the decoded result — proving the template→placeholder assembly, param/id binding, `to_jsonb` decode, and array shaping all compose.

Prior art already exists: `tests/integration/analytical.test.ts` (driven by `tests/integration/harness.ts`, which boots the real engine against Postgres and uses `@pulse/client`). The example procedures it covers live in `packages/examples-chat/src/messages.ts`:
- `summarize` — a plain `SELECT body ... WHERE channel_id = ${id}::uuid ORDER BY ... LIMIT` over real rows; verifies a simple raw query decodes and counts correctly (`"Summary of 3 messages"`).
- `stats` — a CTE + `GROUP BY` + scalar-subquery aggregates returning `total` / `distinct_authors`; verifies CTEs/aggregates and that snake_case projection columns survive the `to_jsonb` round-trip into the typed `Row`.

What good coverage should add (same external style, via the client):
- An **id param** interpolated as `${input.someId}` (no explicit cast) returns the rows for the decoded uuid — locks in the `decode_id_param` heuristic through behavior.
- A **non-id colon string** param (e.g. a literal containing `"a:b"`) is bound verbatim and matches — guards the heuristic's lower bound.
- **`null` / numeric / bool** params bind correctly.

Tests should assert on returned data only; the wrapping SQL, placeholder numbering, and `access()` are implementation details and must not be asserted directly.

## Out of Scope / Deferred

- **OLAP isolation (the §6 path).** Routing analytical queries to a dedicated read-replica pool with its own sizing, longer `statement_timeout`, and `max_standby_streaming_delay`-tuned cancellation is **deferred**; today analytical runs on the OLTP pool (ARCHITECTURE.md §15). This is the natural home for *enforcing* read-only (read-only tx / replica) and for replica-lag-freshness semantics.
- **Write protection / SQL validation of `ctx.sql`.** No parsing or `read-only` guard on the raw statement in this slice.
- **Streaming large analytical results** as an async iterator / chunked response, and `infiniteOptions` pagination over them (ARCHITECTURE.md §6).
- **Materialized-view backing** for popular analytical queries (ARCHITECTURE.md §6).
- **Fine-grained read-set extraction from raw SQL.** Raw stays opaque (`access() == None`); the `ReadSet.tables` coarse fallback is not wired up for the raw path because analytical procedures are non-reactive.
- **Index-range read-set matching** for the document builder (M3) is unrelated and tracked separately.
