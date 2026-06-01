import type { Doc, Id, SystemFields } from "./model.js";
import {
  type AnyValidator,
  type InferObjectType,
  type Prettify,
  type ValidatorDescription,
} from "./types.js";

export interface IndexDefinition {
  name: string;
  /** Columns in the index, in order. May include the system column `_creationTime`. */
  columns: string[];
}

export interface TableDefinition<Fields extends Record<string, AnyValidator>> {
  readonly fields: Fields;
  readonly indexes: IndexDefinition[];
  /**
   * Register a B-tree index. The engine also uses this to capture index-range
   * read-sets for reactive queries.
   */
  index(
    name: string,
    columns: readonly ((keyof Fields & string) | "_creationTime")[],
  ): TableDefinition<Fields>;
}

export function defineTable<F extends Record<string, AnyValidator>>(
  fields: F,
): TableDefinition<F> {
  const indexes: IndexDefinition[] = [];
  const def: TableDefinition<F> = {
    fields,
    indexes,
    index(name, columns) {
      indexes.push({ name, columns: [...columns] as string[] });
      return def;
    },
  };
  return def;
}

export type AnyTableDefinition = TableDefinition<Record<string, AnyValidator>>;

export interface SchemaDefinition<Tables extends Record<string, AnyTableDefinition>> {
  readonly tables: Tables;
  /** Flat description used by DDL/migration generation. */
  describe(): Record<string, { fields: Record<string, ValidatorDescription>; indexes: IndexDefinition[] }>;
}

export function defineSchema<Tables extends Record<string, AnyTableDefinition>>(
  tables: Tables,
): SchemaDefinition<Tables> {
  return {
    tables,
    describe() {
      const out: Record<
        string,
        { fields: Record<string, ValidatorDescription>; indexes: IndexDefinition[] }
      > = {};
      for (const [name, table] of Object.entries(tables)) {
        out[name] = {
          fields: Object.fromEntries(
            Object.entries(table.fields).map(([k, val]) => [k, val.describe()]),
          ),
          indexes: table.indexes,
        };
      }
      return out;
    },
  };
}

/** The full stored document type for a table in a schema (system fields + user fields). */
export type DocFromSchema<
  S extends SchemaDefinition<Record<string, AnyTableDefinition>>,
  T extends keyof S["tables"],
> = Prettify<
  SystemFields<T & string> & InferObjectType<S["tables"][T]["fields"]>
>;

/** Derive the `{ table: Doc }` data model from a schema — used by `pulse gen`. */
export type DataModelFromSchema<
  S extends SchemaDefinition<Record<string, AnyTableDefinition>>,
> = {
  [T in keyof S["tables"] & string]: DocFromSchema<S, T>;
};

export type { Doc, Id, SystemFields };
