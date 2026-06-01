import type { Prettify } from "./types.js";

/**
 * A document id, branded by its table name so ids of different tables are not
 * assignable to each other. At runtime an id is just a string (uuid v7).
 */
export type Id<TableName extends string> = string & { readonly __pulseIdTable: TableName };

/**
 * Opaque marker for a `v.collab()` field. Apps never read/write this directly —
 * the client binds it to an editor via `useCollab` / `usePulseEditor`. It exists
 * only so the contract can distinguish collab fields at the type level (e.g. to
 * expose `pulse.notes.body.collab(...)` only on collab fields).
 */
export type CollabField = { readonly __pulseCollab: true };

/** Fields Pulse injects on every table. */
export interface SystemFields<TableName extends string = string> {
  _id: Id<TableName>;
  /** Creation time in epoch milliseconds. */
  _creationTime: number;
}

/**
 * Module-augmentable data model. `pulse gen` writes a declaration that augments
 * this interface with `{ [table]: Doc }`, giving precise `Doc<"table">` types.
 * Until then, `Doc<T>` falls back to the system fields plus an open record.
 */
// deno-lint-ignore no-empty-interface
export interface PulseDataModel {}

export type TableNames = [keyof PulseDataModel] extends [never]
  ? string
  : keyof PulseDataModel & string;

/** The stored document type for a table. */
export type Doc<TableName extends string> = TableName extends keyof PulseDataModel
  ? PulseDataModel[TableName]
  : Prettify<SystemFields<TableName> & Record<string, unknown>>;
