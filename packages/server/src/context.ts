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

/** A reactive-safe, instrumented query over one table. Reads feed the read-set. */
export interface QueryBuilder<T extends string> {
  withIndex(
    indexName: string,
    range?: (q: IndexRangeBuilder<T>) => IndexRangeBuilder<T>,
  ): QueryBuilder<T>;
  order(direction: "asc" | "desc"): QueryBuilder<T>;
  /** Resolve up to `n` documents. */
  take(n: number): Promise<Doc<T>[]>;
  /** Resolve all matching documents. */
  collect(): Promise<Doc<T>[]>;
  /** First matching document or null. */
  first(): Promise<Doc<T> | null>;
  /** Exactly one; rejects if more than one matches. */
  unique(): Promise<Doc<T> | null>;
}

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
