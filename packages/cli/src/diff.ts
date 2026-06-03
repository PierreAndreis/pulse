import type {
  AnyTableDefinition,
  SchemaDefinition,
  ValidatorDescription,
} from "@onveloz/pulse-schema";
import { fieldToColumn, pgType } from "./ddl.js";

/** One live Postgres column, as read from `information_schema.columns`. */
export interface LiveColumn {
  /** Postgres `data_type` (e.g. "text", "double precision", "bytea"). */
  type: string;
  /** True when the column is `NOT NULL`. */
  notNull: boolean;
}

/** The live database shape: table → column name → column info. */
export type LiveSchema = Record<string, Record<string, LiveColumn>>;

/** A single information_schema.columns row (the subset we read). */
export interface InfoSchemaRow {
  table_name: string;
  column_name: string;
  data_type: string;
  is_nullable: "YES" | "NO";
}

/** Build a {@link LiveSchema} from raw `information_schema.columns` rows. */
export function parseLiveColumns(rows: InfoSchemaRow[]): LiveSchema {
  const live: LiveSchema = {};
  for (const r of rows) {
    (live[r.table_name] ??= {})[r.column_name] = {
      type: r.data_type,
      notNull: r.is_nullable === "NO",
    };
  }
  return live;
}

/** The Postgres column type a desired field lowers to (unwrapping `optional`). */
function desiredColumnType(desc: ValidatorDescription): string {
  return pgType(desc.kind === "optional" ? desc.inner : desc);
}

/** Whether a desired field is nullable in Postgres (optional or collab). */
function desiredNullable(desc: ValidatorDescription): boolean {
  return desc.kind === "optional" || desc.kind === "collab";
}

/**
 * `information_schema.data_type` strings differ from the type names we emit in
 * DDL (e.g. we write `uuid`, PG reports `uuid`; we write `text`, PG reports
 * `text`; but `bytea` ↔ `bytea`, `double precision` ↔ `double precision`,
 * `jsonb` ↔ `jsonb`, `boolean` ↔ `boolean`). Normalize both sides so a type
 * comparison doesn't flag spurious drift. PG also reports `timestamp with time
 * zone`, `character varying`, etc., which our generator never emits — they
 * compare unequal and surface as a real (manual-review) change.
 */
function normalizeType(t: string): string {
  const s = t.toLowerCase().trim();
  if (s === "character varying" || s === "varchar") return "text";
  if (s === "bool") return "boolean";
  if (s === "float8") return "double precision";
  if (s === "int8" || s === "bigint") return "bigint";
  return s;
}

export interface SchemaDiff {
  /** Safe, additive statements (new tables, new columns, new indexes). */
  additive: string[];
  /** Type/nullability changes — emitted as ALTERs, but flagged for review since
   *  a type change can fail or lose data on a non-empty table. */
  alters: string[];
  /** Destructive operations (columns/tables present in the DB but not the
   *  schema). NEVER auto-applied — emitted as commented-out SQL + warnings. */
  destructive: string[];
}

/** True when there is nothing to apply. */
export function isEmptyDiff(d: SchemaDiff): boolean {
  return d.additive.length === 0 && d.alters.length === 0 && d.destructive.length === 0;
}

/**
 * Diff the desired schema against the live database and produce migration SQL,
 * split by risk:
 *  - **additive** — `CREATE TABLE` for new tables, `ADD COLUMN` for new fields,
 *    `CREATE INDEX IF NOT EXISTS` for indexes. Safe to auto-apply.
 *  - **alters** — `ALTER COLUMN … TYPE` / `SET|DROP NOT NULL` where the live
 *    column's type/nullability drifted from the schema. Emitted but flagged.
 *  - **destructive** — tables/columns in the DB but not the schema. Emitted as
 *    commented-out `DROP`s with a warning; never executed automatically.
 *
 * The system columns `_id` / `_creation_time` are managed by the engine and are
 * neither added nor dropped here.
 */
export function diffSchema(
  live: LiveSchema,
  schema: SchemaDefinition<Record<string, AnyTableDefinition>>,
  /** Names of indexes already present in the DB (e.g. "users_by_email"), so an
   *  existing index isn't re-emitted and a matching DB shows an empty diff. */
  liveIndexes: ReadonlySet<string> = new Set(),
): SchemaDiff {
  const described = schema.describe();
  const additive: string[] = [];
  const alters: string[] = [];
  const destructive: string[] = [];

  const SYSTEM_COLS = new Set(["_id", "_creation_time"]);

  for (const [table, { fields, indexes }] of Object.entries(described)) {
    const liveTable = live[table];

    if (!liveTable) {
      // Whole table is new → full CREATE TABLE (additive).
      const cols = [
        `  _id uuid primary key default gen_random_uuid()`,
        `  _creation_time bigint not null default (extract(epoch from now()) * 1000)::bigint`,
      ];
      for (const [field, desc] of Object.entries(fields)) {
        const inner = desc.kind === "optional" ? desc.inner : desc;
        const nn = desiredNullable(desc) ? "" : " not null";
        cols.push(`  ${fieldToColumn(field)} ${pgType(inner)}${nn}`);
      }
      additive.push(`create table if not exists ${table} (\n${cols.join(",\n")}\n);`);
    } else {
      // Table exists → diff each desired field against the live column.
      for (const [field, desc] of Object.entries(fields)) {
        const col = fieldToColumn(field);
        const wantType = desiredColumnType(desc);
        const wantNullable = desiredNullable(desc);
        const liveCol = liveTable[col];

        if (!liveCol) {
          // New column. A NOT NULL add needs a default or the table must be
          // empty; emit nullable-safe and flag if the schema wants NOT NULL.
          additive.push(`alter table ${table} add column if not exists ${col} ${wantType};`);
          if (!wantNullable) {
            alters.push(
              `-- review: ${table}.${col} is NOT NULL in the schema; backfill then ` +
                `\`alter table ${table} alter column ${col} set not null;\``,
            );
          }
          continue;
        }

        if (normalizeType(liveCol.type) !== normalizeType(wantType)) {
          alters.push(
            `alter table ${table} alter column ${col} type ${wantType} ` +
              `using ${col}::${wantType}; -- review: type change ` +
              `(${liveCol.type} → ${wantType}) may fail/lose data on non-empty data`,
          );
        }
        if (liveCol.notNull && wantNullable) {
          alters.push(`alter table ${table} alter column ${col} drop not null;`);
        } else if (!liveCol.notNull && !wantNullable) {
          alters.push(
            `-- review: ${table}.${col} should be NOT NULL; backfill then ` +
              `\`alter table ${table} alter column ${col} set not null;\``,
          );
        }
      }

      // Columns in the DB but not the schema → destructive (never auto-applied).
      const desiredCols = new Set(Object.keys(fields).map(fieldToColumn));
      for (const liveColName of Object.keys(liveTable)) {
        if (SYSTEM_COLS.has(liveColName)) continue;
        if (!desiredCols.has(liveColName)) {
          destructive.push(`-- drop: alter table ${table} drop column ${liveColName};`);
        }
      }
    }

    // Only create indexes the DB doesn't already have (so a matching DB diffs
    // empty). A new table has none of its indexes yet → all emitted.
    for (const index of indexes) {
      const name = `${table}_${index.name}`;
      if (liveIndexes.has(name)) continue;
      const columns = index.columns.map(fieldToColumn).join(", ");
      additive.push(`create index if not exists ${name} on ${table} (${columns});`);
    }
  }

  // Tables in the DB but not the schema → destructive. Skip engine-managed
  // tables (the `_pulse*` namespace, e.g. `_pulse_mutations`) — they're never in
  // the user's schema and must not be flagged for dropping on every sync.
  for (const liveTable of Object.keys(live)) {
    if (liveTable.startsWith("_pulse")) continue;
    if (!(liveTable in described)) {
      destructive.push(`-- drop: drop table ${liveTable};`);
    }
  }

  return { additive, alters, destructive };
}

/** Render a {@link SchemaDiff} as an annotated SQL migration script. */
export function renderDiff(d: SchemaDiff): string {
  if (isEmptyDiff(d)) return "-- no changes: database matches schema\n";
  const out: string[] = [];
  if (d.additive.length) {
    out.push("-- ── additive (safe to apply) ──────────────────────────────");
    out.push(...d.additive);
  }
  if (d.alters.length) {
    out.push("\n-- ── alters (review before applying) ───────────────────────");
    out.push(...d.alters);
  }
  if (d.destructive.length) {
    out.push("\n-- ── destructive (NOT applied automatically — uncomment to run) ──");
    out.push(...d.destructive);
  }
  return out.join("\n") + "\n";
}

/** SQL that reads the columns this diff needs from the live database. */
export const INTROSPECT_SQL =
  "SELECT table_name, column_name, data_type, is_nullable " +
  "FROM information_schema.columns WHERE table_schema = 'public' " +
  "ORDER BY table_name, ordinal_position";

/** SQL that lists existing index names in the public schema. */
export const INTROSPECT_INDEXES_SQL =
  "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'";
