# M3 — Reactive Precision

Status: design (drives TDD implementation)
Owner: engine
Scope crates: `pulse-core`, `pulse-sql`, `pulse-jsruntime`, `pulse-server`, `pulse-reactor`, `pulse-sse`, `pulse-cdc`; package `packages/runtime-node` (worker descriptor), `packages/schema` (already emits indexes).

---

## 1. Goal & headline win

**Today** invalidation is purely table-name set intersection. `crates/pulse-server/src/reactor.rs::matching` returns every subscription where `!s.tables.is_disjoint(writes)`, and both sides are bare `HashSet<String>` of table names (`Capture { reads, writes }` in `crates/pulse-jsruntime/src/lib.rs:55`). So any write to `messages` re-runs *every* `messages` subscription regardless of `channelId`.

This is observable in the existing integration test `tests/integration/reactive.test.ts:75` ("each subscriber only ever sees its own channel's data"), whose comment admits: *"Even though a messages write re-runs the other subscription (coarse, table-level matching), its result never contains another channel's data."* The result is correct but the re-execution is wasted.

**M3 headline win (observable behavior):**

> A `messages.send` into channel **B** does **not** re-run a `messages.list` subscription that read only channel **A**. No SSE push is delivered to the channel-A subscriber, and the channel-A handler is not re-executed.

Measured through the public `@pulse/client`: subscribe client A to `messages.list({channelId: A})`, record callback invocations, send into channel B, assert the channel-A callback count does **not** increase. The same test that currently passes coarsely (line 75) is tightened from "result doesn't *contain* B's data" to "callback is not *invoked* at all".

Secondary wins folded in: close the raw-SQL correctness hole (raw reads are currently *never* invalidated — `DbOp::access` returns `None` for `Raw`), stop redundant pushes via value diffing, and cut the scaling seams (`Reactor` trait, single `apply_change_set(ChangeSet)` publisher, `id:seq` + `commit_lsn` on SSE) so WAL/CDC can drive the identical path later.

---

## 2. Chosen model

### 2.1 Decision: predicate-as-filter matching (Candidate 1), with Candidate 3's "extend in place, keep fallbacks" discipline

We **blend candidates 1 and 3** and **reject the index-range-tuple model (candidate 2)** as the primary representation.

Why filter-vs-row over index-range tuples:

- **The worker already lowers `.withIndex("by_channel", q => q.eq("channelId", A))` into plain field predicates** (`worker.ts:71` `rangeBuilder` pushes `{field, op, value}`; `withIndex` only calls the fn and *discards* `_indexName` at `worker.ts:84`). The decisive chat case is an `eq` on the real field `channelId` — it needs **no index metadata at all** to match precisely. Candidate 2's whole apparatus (thread `indexes` through the manifest, map index→ordered columns, B-tree prefix bound tuples, `KeyValue` total order) is required only for true multi-column range semantics, which the chat example does not exercise. That is a later optimization, not M3 correctness.
- A WAL `Change` carries old+new row images exactly like an in-engine `RETURNING` row, so a predicate-evaluation matcher works **unchanged** whether the row came from in-engine `RETURNING` or from `pgoutput`. This is the single-matcher property we want for the CDC future.
- We keep candidate 3's safety posture: every read we cannot analyze (raw SQL, full scan, uncoercible predicate value) degrades to a **table-level wildcard** that matches every change to that table. **No false negatives, ever** — precision is opportunistic, correctness is guaranteed.

We **reuse** the existing `pulse-core` types (`ReadSet`, `Change`, `ChangeSet`, `TableId`, `PrimaryKey`, `KeyValue`, `ChangeOp`, `Lsn`, `SubscriptionId`) and `ReadSet::matches_change`, extending them in place rather than inventing parallel types. The current `IndexRange`/`IndexBound` on `ReadSet` stay defined (CDC/range work will use them) but the **live matcher operates on the new resolved `Filter` form**, so there is one matching path, not two.

### 2.2 Data-model changes (`pulse-core`, interface/shape level)

`crates/pulse-core/src/change.rs` — `Change` gains row images so predicates can be evaluated; `ChangeSet` is unchanged in shape.

```rust
/// Column field (camelCase) -> value, for the columns any predicate can filter on.
/// Floats/jsonb/large text are intentionally omitted (KeyValue has no Float);
/// a filter on such a column degrades to table-wildcard at capture time.
pub type RowValues = std::collections::HashMap<String, KeyValue>;

pub struct Change {
    pub table: TableId,
    pub key: PrimaryKey,          // _id; kept for point-key match + dedup
    pub op: ChangeOp,             // Insert | Update | Delete (unchanged enum)
    pub new: Option<RowValues>,   // post-image: Some for Insert/Update, None for Delete
    pub old: Option<RowValues>,   // pre-image: Some for Update/Delete, None for Insert
}
```

`crates/pulse-core/src/readset.rs` — add analyzed per-table filters. Keep `tables` (coarse fallback) and `keys` (point lookups). The unused `ranges` field stays for the CDC/range path but is **not** consulted by the live matcher.

```rust
pub enum FilterOp { Eq, Gt, Gte, Lt, Lte }   // mirrors pulse_sql::PredOp

pub struct Cond { pub field: String, pub op: FilterOp, pub value: KeyValue }

/// One analyzed read of a table. `conds` are AND-ed. Empty conds = whole-table read.
pub struct Filter { pub conds: Vec<Cond> }

pub struct ReadSet {
    pub tables: HashSet<TableId>,                     // coarse fallback (raw / full scan / uncoercible)
    pub keys: HashMap<TableId, HashSet<PrimaryKey>>,  // point get(id)
    pub filters: HashMap<TableId, Vec<Filter>>,       // NEW: analyzed query predicates
    pub ranges: HashMap<TableId, Vec<IndexRange>>,    // reserved for CDC/range work; not matched in M3
}
```

`FilterOp` lives in `pulse-core` (no dep on `pulse-sql`); `pulse-sql::PredOp` gets a `From<PredOp> for FilterOp` conversion at the capture site. `KeyValue` already covers `Int/Text/Uuid/Bool/Null` and is `Hash + Eq`. Range ops (`Gt/Gte/Lt/Lte`) need ordering — add a `KeyValue::partial_cmp`-style helper that orders within a variant (`Int`, `Uuid` as bytes, `Text` lexical, `Bool`) and returns `None` across variants; cross-variant compares do not occur in practice because a single column is single-typed.

`crates/pulse-jsruntime/src/lib.rs` — `ExecResult` stops returning `Vec<String>`:

```rust
pub struct ExecResult {
    pub value: Value,
    pub read_set: ReadSet,        // was: reads: Vec<String>
    pub changes: Vec<Change>,     // was: writes: Vec<String>
}
struct Capture { read_set: ReadSet, changes: Vec<Change> }   // was two HashSet<String>
```

SSE push payload (`crates/pulse-server`, see §3.3) gains `id`, `seq`, `commitLsn`.

### 2.3 Capture changes

**Reads → `ReadSet`** — built at the existing capture site in `pulse-jsruntime::reader_loop` on each `WorkerOut::Dbop` (`lib.rs:217`), where the `DbOp` and `Catalog` are both in hand. The `DbOp` enum is in `crates/pulse-sql/src/ops.rs`.

- `DbOp::Get { table, id }` → `read_set.add_key(table, PrimaryKey::single(KeyValue::Uuid(decode_id(id))))`. Exact point read.
- `DbOp::Query { table, predicates, .. }` → build one `Filter`. For each `pulse_sql::Predicate { field, op, value }`, resolve the column via the catalog (`Table::column_by_field`) to get its `PgTypeClass` + `id_ref`, then `Cond { field, op: op.into(), value: KeyValue::from_json(value, col) }`. **Empty `predicates` (`.collect()`/`.take()` with no `withIndex`) → `read_set.add_table(table)`** (whole-table wildcard). If a predicate value cannot coerce to `KeyValue` (float/jsonb) → drop that `Cond`; if dropping leaves the filter unsafe to evaluate, fall back to `add_table(table)`. `order`/`limit` are **not** part of matching (a top-N can be invalidated by any in-window insert; ignoring the limit is the safe over-approximation).
- `DbOp::Raw { sql, .. }` → **must stop returning `None`.** Change `DbOp::access`/capture so raw reads `add_table` for every table the SQL references. Minimum correct behavior: textual identifier scan of the SQL against catalog table names; if uncertain, add **all** catalog tables. This closes the silent-staleness hole (today raw-backed subs like `messages.summarize`/`stats` never invalidate). Raw is over-invalidated (acceptable — analytical).

`id_ref` columns: predicate values arrive as encoded `"table:uuid"`; decode to the raw uuid (`decode_id`) into `KeyValue::Uuid` so they compare equal to `RETURNING`-sourced values.

**Writes → `Vec<Change>`** — at the same `Dbop` site when `op.access()` is a write. We need the affected `PrimaryKey` plus the values of columns any subscription could filter on, which requires the row image(s). `execute_op` (`ops.rs:212`) is extended to surface `(Value, Option<Change>)` (or push into a `&mut Vec<Change>`):

- `Insert`: already `RETURNING t.select_list()` (`ops.rs:297`); the full new row is in hand before it is reduced to the id (`ops.rs:304`). Build `Change { op: Insert, key: PK(_id), new: Some(row_values), old: None }`. The externally returned `Value` (the id string) is **unchanged** — capture is a side channel.
- `Patch`/`Replace` (`update`, `ops.rs:345`): today returns `Value::Null` with a bare `UPDATE`. Change to capture **both** images in one serializable statement:
  `WITH old AS (SELECT <cols> FROM t WHERE _id=$ FOR UPDATE), upd AS (UPDATE t SET ... WHERE _id=$ RETURNING <cols>) SELECT (old), (upd)`.
  `Change { op: Update, key, new: Some, old: Some }`. The old image is **required** for the filter-move case (§2.4).
- `Delete` (`ops.rs:317`): change to `DELETE ... RETURNING <select_list>` so the leaving row's values travel. `Change { op: Delete, key, new: None, old: Some }`.
- Raw writes: documented read-only; left untracked (log if a `Raw` mutates).

**Worker descriptor / schema:** the chat `eq` case needs **no** descriptor change — `rangeBuilder` already forwards `channelId` as a field predicate. For the (deferred) true index-range path, `worker.ts` would forward `_indexName` (`worker.ts:84`) and the `indexes` already produced by `describe()` (`packages/schema/src/schema.ts:48,67` emits `indexes: IndexDefinition[]`) but **dropped** in the worker manifest builder (`worker.ts:159` keeps only `fields`); `SchemaMeta`/`TableSchema` in `crates/pulse-sql/src/catalog.rs:113` would gain `indexes`. None of that is on the M3 correctness path.

### 2.4 Matching algorithm

Implemented as `ReadSet::matches_change(&Change) -> bool` in `pulse-core` (extends the existing method at `readset.rs:55`).

```
matches_change(c):
  if self.tables.contains(c.table):                  return true   // coarse / raw / full-scan reader
  if self.keys[c.table].contains(c.key):             return true   // point get(id) of this row
  if let Some(filters) = self.filters.get(c.table):
       return filters.any(|f| filter_matches(f, c))
  return false                                                      // table not referenced

filter_matches(f, c):
  // OR over images so a row ENTERING (new) or LEAVING (old) the filter invalidates.
  let hit = |row| f.conds.iter().all(|cond| eval(cond, row))
  c.new.map_or(false, hit) || c.old.map_or(false, hit)

eval(cond, row):
  match row.get(cond.field):
    Some(v) => apply(cond.op, v, cond.value)   // Eq via KeyValue==; range via partial_cmp helper
    None    => true                            // value not captured -> conservatively match (no miss)
```

Reactor side (`pulse-reactor`): keep a coarse `HashMap<TableId, HashSet<SubKey>>` index built from each `ReadSet::referenced_tables()` (already unions tables+keys+filters keys — extend it to include `filters`). `apply_change_set(cs)` iterates `cs.changes`, for each change looks up only candidate subs for `change.table`, runs `matches_change`, unions matched subs into a **dirty set** (dedup so a multi-row tx re-runs each sub at most once), then re-executes + pushes each dirty sub once stamped with `cs.commit_lsn`.

**Worked decisive case.** Sub `messages.list({channelId: A})` records `filters[messages] = [Filter{conds:[Cond{channelId, Eq, Uuid(A)}]}]`. `messages.send({channelId: B})` produces `Change{table: messages, op: Insert, new: {channelId: Uuid(B), authorId, body}, old: None}`. `filter_matches`: `eval(channelId Eq A, new)` → `B == A` → false; `old` is None → `false`. **Not matched → channel-A sub not re-run, no push.** A send into channel A → `new.channelId == A` → matched.

**Edge cases:**

- **Patch moves a row across a filter** (`UPDATE messages SET channelId A→B`): `old{channelId:A}` AND `new{channelId:B}`. Channel-A sub matches via **old** (row left → must drop it); channel-B sub matches via **new** (row entered). The `new OR old` evaluation handles it. This is *why* `update()` must capture the pre-image; new-only would silently strip the row from A's clients.
- **Delete**: `new = None`, `old = Some(image)`. Any filter that matched the deleted row's old values matches and re-runs (sees the row gone). Wildcard/empty-cond subs always match. Requires `DELETE ... RETURNING`.
- **Range vs eq**: `eq` is `KeyValue==`. `gt/gte/lt/lte` use the `partial_cmp` helper on `KeyValue` (timestamps as `Int8` epoch). Multiple conds AND together (e.g. `eq(channelId,A).gte(_creationTime,t)`). A predicate on a column we cannot order (float/jsonb) → that read fell back to `add_table` at capture time, so it over-invalidates rather than mis-matching.
- **Full-scan / no-predicate query** (`.collect()` with no `withIndex`): `add_table` → matches any change to that table. Correct (over-broad, never wrong). This is the *only* shape that keeps today's behavior, now scoped to one table instead of all subs on it.
- **Raw SQL reads** (`ctx.sql`): `add_table` per referenced table (or all tables if unparseable). Fixes today's never-invalidate hole.
- **Multi-row transaction**: multiple `Change`s; dirty-set dedup re-runs each affected sub once per `ChangeSet`.
- **id encoding mismatch**: normalize both predicate values and captured column values through `decode_id` so `KeyValue::Uuid` compares equal regardless of `"table:uuid"` vs raw uuid.
- **Composite / non-uuid PK**: `PrimaryKey` is already `Vec<KeyValue>`; only single-`_id` uuid is exercised now, but the shape generalizes.

---

## 3. Folded scaling seams

### 3.1 `Reactor` trait + `pulse-reactor` extraction

Move `crates/pulse-server/src/reactor.rs` into `crates/pulse-reactor` (today a doc-comment stub) behind a trait. `pulse-server` deletes its in-file module, depends on `pulse-reactor`, and holds `reactor: Arc<dyn Reactor>` in `AppState`.

```rust
#[async_trait]
pub trait Reactor: Send + Sync {
    async fn register_client(&self, client_id: String) -> mpsc::Receiver<SsePush>;
    async fn remove_client(&self, client_id: &str);
    async fn add_subscription(&self, sub: Subscription);
    async fn remove_subscription(&self, client_id: &str, sub: &str);
    /// The ONE invalidation entry point. Prefilter -> match -> dedup -> re-exec -> diff -> push.
    async fn apply_change_set(&self, cs: ChangeSet);
}

pub struct Subscription {
    pub client_id: String,
    pub sub: String,
    pub path: Vec<String>,
    pub input: Value,
    pub headers: HashMap<String, String>,
    pub read_set: ReadSet,          // was: tables: HashSet<String>
    pub last: Option<Value>,        // last pushed value, for diffing (§4)
}
```

`InMemoryReactor` is the current dashmap/Mutex impl plus the per-table sub index. Re-execution still needs the worker, so the reactor takes an injected re-executor to stay free of the `pulse-jsruntime` dep:

```rust
#[async_trait]
pub trait ReExecutor: Send + Sync {
    async fn exec(&self, path: Vec<String>, input: Value, headers: HashMap<String, String>)
        -> Result<Value, WorkerError>;
}
```

`pulse-server` implements `ReExecutor` over `pulse_jsruntime::Worker` and hands it to the reactor at construction.

### 3.2 Single `apply_change_set(ChangeSet)` publisher seam

Today `rpc()` (`main.rs:143`) builds a `HashSet<String>` and spawns `invalidate(state, writes)`. Replace with:

```
mutation result -> ExecResult.changes
                -> ChangeSet { commit_lsn, changes }
                -> reactor.apply_change_set(cs)
```

`invalidate()` collapses into `apply_change_set`. `commit_lsn` for the in-engine path is a synthetic monotonic counter (or `Lsn::ZERO`) until CDC supplies a real WAL position; clients treat it as opaque-monotonic.

**This is the seam CDC plugs into unchanged.** `pulse-cdc` (doc stub today) decodes `pgoutput` Insert/Update/Delete into the *same* `ChangeSet` (old/new images from `REPLICA IDENTITY FULL`) and calls the *same* `apply_change_set` — no second matching path, no re-modeling. A bus/queue can sit in front of `apply_change_set` without changing its signature. `subscribe()` (`main.rs:199`) is unchanged except it stores `read_set: ReadSet` instead of `tables`.

### 3.3 `id:seq` + `commit_lsn` on SSE pushes

`push_payload` (`main.rs:116`) currently emits `{ sub, data }`. Extend to:

```jsonc
{ "sub": "<id>", "id": "<clientSeq>", "seq": 0, "commitLsn": "0/0", "data": <value> }
```

- `seq`: per-client monotonic `u64` the reactor increments on every push to that client (stored on the client registry entry).
- `id`: the SSE event id — `sync()` (`main.rs:185`) maps each payload to `Event::default().id(seq).data(...)` so reconnect with `Last-Event-ID` resumes.
- `commitLsn`: `ChangeSet.commit_lsn` (`Lsn` already serializes to `X/Y`).

This is the hook for `pulse-sse`'s promised ring-buffer replay (`crates/pulse-sse/src/lib.rs` stub): it buffers keyed on the event `id` and emits `resync` when the buffer has rolled past. Additive fields only — no client protocol break.

---

## 4. Result deltas + cross-client dedup

**Result diffing (implement now, minimal form).** `apply_change_set` re-executes a dirty sub, then compares the new JSON value to `Subscription.last`; **if equal, skip the push entirely.** This kills the redundant-push half of over-invalidation even where matching is coarse (wildcard/raw). Whole-value equality is the cheap, high-value first step. Row-level array deltas (`{added, removed, updated}` keyed by `_id`) are a **deferred** follow-up — the `Change` row images already give the per-row identity to drive it later.

**Cross-client dedup (design now, defer the optimization).** The **match** phase is already shared: one `matches_change` pass over a `ChangeSet` yields all dirty subs. The expensive part is re-execution. Two clients with the same `(path, input)` but **different auth headers** can legitimately get different results (the chat handlers run `authedBase` middleware — `messages.ts:16`). Therefore:

- Dedup the match decision freely (identical input → identical `ReadSet`).
- **Do NOT share re-execution across clients with different auth headers** — that would leak one client's RLS-filtered rows to another (a security bug, not a perf bug).
- Key the re-exec phase by `ExecKey = hash(path, canonical(input), auth-relevant-headers)`. Subscriptions sharing an `ExecKey` re-execute once and fan the single result + `seq`-stamped push out per client. Default to hashing the full auth-relevant header set (safe-but-pessimal). Subscriptions stay registered per `(client_id, sub)` for routing/unsubscribe; `ExecKey` only collapses the handler run.

For M3 we ship per-subscription re-execution (correct, simple) and leave `ExecKey` collapsing as a documented follow-up keyed off the same `Subscription` fields.

---

## 5. TDD slice list (ordered)

Each slice is one behavior testable through the public `@pulse/client` via the existing integration harness (`tests/integration/harness.ts`: `client.<ns>.<proc>.subscribe({input}, cb)`, `makeClient(token)`, `.call(...)`, `reset()`, `GENERAL_CHANNEL`). "Red" = the failing test idea; "Green" = the minimal implementation. Slices are vertical: each lands a thin path from `pulse-core` types through capture, matching, and SSE.

**Slice 1 — Tracer bullet: cross-channel insert does not re-run a foreign-channel subscription.**
- Red: subscribe A to `messages.list({channelId: A})`; record callback count after initial push; `makeClient("w").messages.send({channelId: B, body:"x"})`; assert A's callback count is unchanged after a settle window. (Tighten `reactive.test.ts:75` from "result excludes B" to "callback not invoked".)
- Green: add `RowValues` + `new`/`old` to `Change`; add `Filter`/`Cond`/`FilterOp` + `filters` to `ReadSet` and the predicate branch of `matches_change`; capture `Insert`'s `RETURNING` row into `Change.new` (already fetched at `ops.rs:304`); capture `Query` predicates into `read_set.filters` (the `eq` only); thread `ReadSet`/`Vec<Change>` through `ExecResult`; build a `ChangeSet` in `rpc()` and match per-change. Same-channel send still pushes (regression-guarded by the existing `reactive.test.ts:27`/`:52`).

**Slice 2 — Point lookup precision.**
- Red: a subscription whose handler does only `ctx.db.get(id_X)` is re-run when `id_X` is patched, and **not** re-run when a *different* row `id_Y` in the same table is patched.
- Green: capture `DbOp::Get` into `read_set.keys`; ensure `Patch` produces a `Change` with the correct `key` (it carries the explicit `id`). Uses the existing `keys` branch of `matches_change` (already present at `readset.rs:59`).

**Slice 3 — Patch moves a row across a filter (old-image invalidation).**
- Red: A subscribes `list({channelId: A})` and sees message M; another client patches M's `channelId` from A to B; A's subscription is re-run and M disappears from A's result; a B subscriber gains M.
- Green: change `update()` to the `WITH old/upd` form capturing both images; emit `Change{op:Update, old, new}`; rely on the `new OR old` evaluation in `filter_matches`.

**Slice 4 — Delete invalidation via pre-image.**
- Red: A subscribes `list({channelId: A})`, sees M; M is deleted; A is re-run and M disappears.
- Green: change `Delete` to `DELETE ... RETURNING select_list()`; emit `Change{op:Delete, old:Some, new:None}`.

**Slice 5 — Range predicate precision.**
- Red: subscribe to a query with `eq(channelId,A).gte(_creationTime, T)`; an insert with `_creationTime < T` does **not** re-run it; an insert `>= T` does. (May require a small test procedure exposing the range read.)
- Green: add the `KeyValue` ordering helper; implement `apply(Gt/Gte/Lt/Lte)` in `eval`; multi-`Cond` AND already handled.

**Slice 6 — Full-scan / no-predicate query falls back to table-level (no miss).**
- Red: a subscription doing `ctx.db.query("messages").collect()` (no `withIndex`) is re-run by **any** `messages` write, including a foreign channel. (Asserts the safe fallback still fires.)
- Green: empty-predicate `Query` → `read_set.add_table(table)`; the `tables` branch of `matches_change` matches.

**Slice 7 — Raw-SQL read is invalidated (close the silent-staleness hole).**
- Red: subscribe to a raw-SQL-backed reactive procedure reading `messages`; a `messages.send` re-runs it. (Today it never would — `DbOp::access` returns `None` for `Raw`.)
- Green: capture `DbOp::Raw` → `add_table` for every referenced table (textual scan; all-tables fallback if unparseable).

**Slice 8 — No redundant push when the recomputed value is unchanged.**
- Red: subscribe A to `list({channelId: A})`; perform a write that re-runs A (e.g. via a coarse/raw sub, or a no-op patch) but yields an identical result; assert A's callback is **not** invoked again.
- Green: store `Subscription.last`; in `apply_change_set`, skip the push when `new_value == last`.

**Slice 9 — Reactor extraction behind the trait + single `apply_change_set` entry point.**
- Red: existing reactive suite (`reactive.test.ts`) stays green after the move; add a unit test in `pulse-reactor` asserting an injected `ChangeSet` drives matching via the trait (no `pulse-server`-internal reactor).
- Green: move `Subscription`/reactor into `pulse-reactor` behind `trait Reactor` + `ReExecutor`; `pulse-server` holds `Arc<dyn Reactor>`; `rpc()` builds a `ChangeSet` and calls `apply_change_set`. Pure refactor — behavior locked by slices 1–8.

**Slice 10 — `id:seq` + `commitLsn` on SSE pushes.**
- Red: a client-facing test (or a transport-level assertion through `@pulse/client`'s `sync` transport) sees monotonically increasing `seq`/event `id` and a `commitLsn` field on pushes.
- Green: thread a per-client `seq` counter through the reactor; extend `push_payload` and `sync()`'s `Event` mapping with `id`/`seq`/`commitLsn`.

Slice 1 is the **tracer bullet** for precision and the slice the headline win is measured against; everything after deepens precision (2–7), removes waste (8), and lands the seams (9–10).

---

## 6. Out of scope / risks

**Out of scope (M3):**
- True index-range tuple matching with B-tree prefix bounds (candidate 2). Forwarding `_indexName` + `indexes` through the manifest and `SchemaMeta`/`Catalog`. Range-on-sort-key top-N window precision (we over-approximate `take(N)` by ignoring the limit).
- Row-level array deltas (`{added, removed, updated}`); we ship whole-value-equality push skip only.
- `ExecKey` cross-client re-exec collapsing (designed in §4, deferred to per-sub re-exec).
- Live WAL/CDC ingestion (`pulse-cdc` decode loop, replication slot, `REPLICA IDENTITY FULL`). M3 wires the `apply_change_set(ChangeSet)` seam CDC will use, but the in-engine capture path remains the only `ChangeSet` producer.
- Float/jsonb predicate precision (these degrade to table-wildcard, by design).

**Risks:**
- **Mutation SQL change is behavioral, not additive.** `update()`/`Delete` must `RETURNING` and `Patch` must read the pre-image, inside the existing single serializable tx, without changing the externally returned `Value` (Insert still returns the id; Patch/Replace/Delete still return null). Getting the move case (emit both images) wrong silently strips rows from a subscriber. Highest-correctness-risk piece — lock it with slices 3 and 4 before refactoring.
- **Predicate eval must mirror Postgres WHERE semantics** (id encode/decode, null handling, `_creationTime` ordering). Divergence → missed invalidation (stale client) or false match (wasted re-exec). `decode_id` normalization on both sides and the `None => true` conservative branch bias toward over-invalidation, never a miss.
- **Per-table sub index** is new mutable shared state under the dashmap/Mutex; must stay in sync on add/remove subscription and remove client or matching leaks/misses.
- **Cross-client auth correctness** (when `ExecKey` lands): hashing too few headers risks leaking RLS-filtered data; default to the full auth-relevant set.
- **Synthetic `commit_lsn`**: clients must treat it as opaque-monotonic, not a real WAL position, until CDC lands, or the in-engine→CDC transition could double-deliver/skip.
- **Multi-crate ripple**: the `ExecResult` type change touches `pulse-core` → `pulse-sql` → `pulse-jsruntime` → `pulse-server`. Sequence per the slices: core types → sql capture → runtime `ExecResult` → reactor/server. Extend the existing `readset.rs` unit tests to lock the channel-A/B prune before the reactor refactor.
