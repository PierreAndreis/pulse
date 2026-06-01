import type {
  AnyTableDefinition,
  SchemaDefinition,
  ValidatorDescription,
} from "@onveloz/pulse-schema";

/** camelCase logical field → snake_case Postgres column (mirrors pulse-sql/naming.rs). */
export function fieldToColumn(field: string): string {
  if (field === "_id") return "_id";
  if (field === "_creationTime") return "_creation_time";
  let out = "";
  for (const ch of field) {
    if (ch >= "A" && ch <= "Z") out += "_" + ch.toLowerCase();
    else out += ch;
  }
  return out;
}

/** Postgres column type for a (non-optional) validator description. */
export function pgType(desc: ValidatorDescription): string {
  switch (desc.kind) {
    case "string":
      return "text";
    case "number":
      return "double precision";
    case "boolean":
      return "boolean";
    case "id":
      return "uuid";
    case "literal":
      return typeof desc.value === "number"
        ? "double precision"
        : typeof desc.value === "boolean"
          ? "boolean"
          : "text";
    case "union":
      // Unions of same-typed literals (e.g. role) collapse to that base type;
      // mixed unions fall back to jsonb.
      return desc.members.every((m) => m.kind === "literal" && typeof m.value === "string")
        ? "text"
        : "jsonb";
    case "array":
    case "object":
    case "any":
      return "jsonb";
    case "optional":
      return pgType(desc.inner);
    case "null":
      return "jsonb";
    case "doc":
      return "jsonb";
    case "collab":
      // CRDT field: opaque Yjs binary state, merged server-side.
      return "bytea";
  }
}

/**
 * Generate idempotent DDL for a schema: `CREATE TABLE IF NOT EXISTS` with the
 * injected system columns (`_id uuid`, `_creation_time bigint`) plus user
 * columns, and `CREATE INDEX IF NOT EXISTS` for each declared index.
 *
 * This applies the schema additively (safe to re-run). Column drift detection
 * and `ALTER`/drop migrations are a follow-up; this covers create-and-extend.
 */
export function generateDDL(
  schema: SchemaDefinition<Record<string, AnyTableDefinition>>,
): string {
  const described = schema.describe();
  const statements: string[] = [];

  for (const [table, { fields, indexes }] of Object.entries(described)) {
    const cols = [
      `  _id uuid primary key default gen_random_uuid()`,
      `  _creation_time bigint not null default (extract(epoch from now()) * 1000)::bigint`,
    ];
    for (const [field, desc] of Object.entries(fields)) {
      // Collab fields start empty (NULL → fresh Yjs doc), so they're nullable.
      const nullable = desc.kind === "optional" || desc.kind === "collab";
      const inner = desc.kind === "optional" ? desc.inner : desc;
      cols.push(`  ${fieldToColumn(field)} ${pgType(inner)}${nullable ? "" : " not null"}`);
    }
    statements.push(`create table if not exists ${table} (\n${cols.join(",\n")}\n);`);

    for (const index of indexes) {
      const columns = index.columns.map(fieldToColumn).join(", ");
      statements.push(
        `create index if not exists ${table}_${index.name} on ${table} (${columns});`,
      );
    }
  }

  return statements.join("\n\n") + "\n";
}
