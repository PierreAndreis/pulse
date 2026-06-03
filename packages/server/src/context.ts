import type { CollabField, Doc, Id } from "@onveloz/pulse-schema";
import type { ProcedureKind } from "@onveloz/pulse-contract";

/** Fields the engine injects on insert. */
type SystemFieldKeys = "_id" | "_creationTime";

/** Keys of a doc whose value is a collaborative (`v.collab()`) field. */
type CollabKeys<T extends string> = {
  [K in keyof Doc<T>]: Doc<T>[K] extends CollabField ? K : never;
}[keyof Doc<T>];

/**
 * A document's user-writable shape on insert: system fields stripped, and
 * collaborative fields made optional (they default to an empty Yjs doc and are
 * edited via `applyCollab`, never set directly).
 */
export type WithoutSystemFields<T extends string> = Omit<
  Doc<T>,
  SystemFieldKeys | CollabKeys<T>
> &
  Partial<Pick<Doc<T>, CollabKeys<T>>>;

/** Builds an index range predicate (`eq`/`gt`/`gte`/`lt`/`lte`) for a query. */
export interface IndexRangeBuilder<T extends string> {
  eq<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): IndexRangeBuilder<T>;
  gt<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): IndexRangeBuilder<T>;
  gte<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): IndexRangeBuilder<T>;
  lt<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): IndexRangeBuilder<T>;
  lte<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): IndexRangeBuilder<T>;
}

/** Opaque handle for one filter predicate produced by a {@link FilterBuilder}. */
declare const filterCondBrand: unique symbol;
export type FilterCond = { readonly [filterCondBrand]: true };

/**
 * Builds one ORM-style predicate (`field <op> value`) over any column — not just
 * an index. The comparison feeds both the SQL `WHERE` and the reactive read-set,
 * so invalidation stays precise (a write only re-runs the query if the changed
 * row matches). Chain multiple `.filter()` calls to AND them together.
 */
export interface FilterBuilder<T extends string> {
  eq<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): FilterCond;
  neq<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): FilterCond;
  gt<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): FilterCond;
  gte<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): FilterCond;
  lt<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): FilterCond;
  lte<F extends keyof Doc<T> & string>(field: F, value: Doc<T>[F]): FilterCond;
  /** SQL `LIKE` on a string field (`%` = any run, `_` = one char). Precise reactivity. */
  like<F extends StringKeys<T>>(field: F, pattern: string): FilterCond;
  /** Case-insensitive `ILIKE`. Precise reactivity. */
  ilike<F extends StringKeys<T>>(field: F, pattern: string): FilterCond;
  /** `field IS NULL`. (Filters precisely in SQL; reactivity is table-coarse.) */
  isNull<F extends keyof Doc<T> & string>(field: F): FilterCond;
  /** `field IS NOT NULL`. (Filters precisely in SQL; reactivity is table-coarse.) */
  isNotNull<F extends keyof Doc<T> & string>(field: F): FilterCond;
  /** `field` ∈ `values` — sugar for an OR of equalities. Precise reactivity. */
  in<F extends keyof Doc<T> & string>(field: F, values: ReadonlyArray<Doc<T>[F]>): FilterCond;
  /** All sub-conditions must hold. */
  and(...conds: FilterCond[]): FilterCond;
  /** Any sub-condition holds — a real reactive OR (recorded as multiple filters). */
  or(...conds: FilterCond[]): FilterCond;
  /** Negation. Pushed to leaves (De Morgan) for precise reactivity where possible. */
  not(cond: FilterCond): FilterCond;
}

/** A reactive-safe, instrumented query over one table. Reads feed the read-set. */
export interface QueryBuilder<T extends string> {
  withIndex(
    indexName: string,
    range?: (q: IndexRangeBuilder<T>) => IndexRangeBuilder<T>,
  ): QueryBuilder<T>;
  /** Narrow by an arbitrary-column predicate (ANDed with any prior filters). */
  filter(predicate: (q: FilterBuilder<T>) => FilterCond): QueryBuilder<T>;
  /** Sort by `field` (default `_creationTime`). Chain `.order(...)` calls for a
   *  multi-column sort (keys applied in order). Doesn't affect reactivity. */
  order(direction: "asc" | "desc", field?: keyof Doc<T> & string): QueryBuilder<T>;
  /** Offset-paginate: skip `offset` rows and resolve the next `limit`. Pair with
   *  `order()` for a stable page. */
  paginate(opts: { limit: number; offset?: number }): Promise<Doc<T>[]>;
  /** Resolve up to `n` documents. */
  take(n: number): Promise<Doc<T>[]>;
  /** Resolve all matching documents. */
  collect(): Promise<Doc<T>[]>;
  /** First matching document or null. */
  first(): Promise<Doc<T> | null>;
  /** Exactly one; rejects if more than one matches. */
  unique(): Promise<Doc<T> | null>;
  /** Count matching rows (`count(*)`), or non-null values of `field` if given.
   *  Reactive: any write to a row matching this query's filters recomputes it. */
  count(field?: keyof Doc<T> & string): Promise<number>;
  /** Count distinct values of a field over the matching rows. Reactive. */
  countDistinct(field: keyof Doc<T> & string): Promise<number>;
  /** Sum a numeric field over the matching rows (null over an empty/all-null set,
   *  per SQL). Reactive, like {@link count}. */
  sum(field: NumericKeys<T>): Promise<number | null>;
  /** Min of a numeric field over the matching rows (null if none). Reactive. */
  min(field: NumericKeys<T>): Promise<number | null>;
  /** Max of a numeric field over the matching rows (null if none). Reactive. */
  max(field: NumericKeys<T>): Promise<number | null>;
  /** Average of a numeric field over the matching rows (null if none). Reactive. */
  avg(field: NumericKeys<T>): Promise<number | null>;
  /** Group the (filtered) rows by `field`; the returned handle's aggregate methods
   *  resolve to one `{ key, value }` row per group. Same filter-precise reactivity
   *  as a scalar aggregate. */
  groupBy<F extends keyof Doc<T> & string>(field: F): GroupedQuery<T, Doc<T>[F]>;
}

/** Grouped-aggregate handle from {@link QueryBuilder.groupBy}. Each method
 *  resolves to one row per group: `{ key: <group value>, value: <aggregate> }`. */
export interface GroupedQuery<T extends string, K> {
  count(): Promise<Array<{ key: K; value: number }>>;
  countDistinct(field: keyof Doc<T> & string): Promise<Array<{ key: K; value: number }>>;
  sum(field: NumericKeys<T>): Promise<Array<{ key: K; value: number | null }>>;
  min(field: NumericKeys<T>): Promise<Array<{ key: K; value: number | null }>>;
  max(field: NumericKeys<T>): Promise<Array<{ key: K; value: number | null }>>;
  avg(field: NumericKeys<T>): Promise<Array<{ key: K; value: number | null }>>;
}

/** Keys of a doc whose value is a number — the valid targets for `sum()`. */
type NumericKeys<T extends string> = {
  [K in keyof Doc<T>]: Doc<T>[K] extends number ? K : never;
}[keyof Doc<T>] &
  string;

/** Keys of a doc whose value is a string — the valid targets for `like`/`ilike`. */
type StringKeys<T extends string> = {
  [K in keyof Doc<T>]: Doc<T>[K] extends string ? K : never;
}[keyof Doc<T>] &
  string;

/** Read-only database handle (queries + analytical). Every read is captured. */
export interface DatabaseReader {
  get<T extends string>(id: Id<T>): Promise<Doc<T> | null>;
  query<T extends string>(table: T): QueryBuilder<T>;
  /** Read a collaborative (`v.collab()`) field's current Yjs state (base64). */
  getCollab<T extends string>(id: Id<T>, field: string): Promise<string>;
}

/** Read-write handle (mutations). Runs inside one serializable transaction. */
export interface DatabaseWriter extends DatabaseReader {
  insert<T extends string>(table: T, doc: WithoutSystemFields<T>): Promise<Id<T>>;
  patch<T extends string>(id: Id<T>, fields: Partial<WithoutSystemFields<T>>): Promise<void>;
  replace<T extends string>(id: Id<T>, doc: WithoutSystemFields<T>): Promise<void>;
  delete<T extends string>(id: Id<T>): Promise<void>;
  /** Merge a base64 Yjs update into a collab field; returns the merged state (base64). */
  applyCollab<T extends string>(id: Id<T>, field: string, update: string): Promise<string>;
}

/** Tagged-template raw SQL accessor for the analytical path. */
export interface SqlTag {
  <Row = Record<string, unknown>>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<Row[]>;
}

/**
 * Re-entry handles available inside actions. Actions are non-deterministic and
 * non-transactional; they compose deterministic queries/mutations by calling
 * back into the engine. A target is addressed by its dotted procedure path
 * (e.g. `"messages.send"`). (Typed contract-ref overloads are a follow-up.)
 */
export interface ActionRunner {
  runQuery<Output = unknown, Input = unknown>(path: string, input?: Input): Promise<Output>;
  runMutation<Output = unknown, Input = unknown>(path: string, input?: Input): Promise<Output>;
  runAction<Output = unknown, Input = unknown>(path: string, input?: Input): Promise<Output>;
}

/** Context additions provided by the runtime based on the procedure kind. */
export type KindContext<K extends ProcedureKind> = K extends "reactive"
  ? { db: DatabaseReader; sql: SqlTag }
  : K extends "mutation"
    ? { db: DatabaseWriter }
    : K extends "analytical"
      ? { db: DatabaseReader; sql: SqlTag }
      : K extends "action"
        ? ActionRunner
        : Record<never, never>;
