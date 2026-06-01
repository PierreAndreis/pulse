import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { defineSchema, defineTable, v } from "../../packages/schema/src/index.js";
import { generateDDL } from "../../packages/cli/src/ddl.js";

const execFileAsync = promisify(execFile);
const PG_CONTAINER = process.env.PULSE_PG_CONTAINER ?? "pulse-pg";
const DATABASE_URL =
  process.env.PULSE_TEST_DATABASE_URL ?? "postgres://pulse:pulse@localhost:54329/pulse";

/** Run SQL in the dev Postgres, returning trimmed tab-separated rows.
 *  Prefers a direct `psql $DATABASE_URL` (CI has psql but no named container);
 *  falls back to `docker exec` into the local dev container. */
async function psql(sql: string): Promise<string> {
  try {
    const { stdout } = await execFileAsync("psql", [DATABASE_URL, "-tAc", sql]);
    return stdout.trim();
  } catch {
    const { stdout } = await execFileAsync("docker", [
      "exec",
      PG_CONTAINER,
      "psql",
      "-U",
      "pulse",
      "-d",
      "pulse",
      "-tAc",
      sql,
    ]);
    return stdout.trim();
  }
}

// A throwaway schema that doesn't collide with the app tables.
const schema = defineSchema({
  widgets: defineTable({
    label: v.string(),
    qty: v.number(),
    tag: v.optional(v.string()),
  }).index("by_label", ["label"]),
});

describe("pulse migrate — DDL generation + apply (M7)", () => {
  beforeAll(async () => {
    await psql("drop table if exists widgets cascade");
  });
  afterAll(async () => {
    await psql("drop table if exists widgets cascade");
  });

  test("generated DDL applies, is idempotent, and introspects back to the schema", async () => {
    const ddl = generateDDL(schema);

    // Apply twice — idempotent (CREATE ... IF NOT EXISTS), no error second time.
    await psql(ddl);
    await psql(ddl);

    // Columns round-trip (system columns + user columns, snake_cased).
    const cols = await psql(
      "select column_name || ':' || data_type from information_schema.columns " +
        "where table_name = 'widgets' order by ordinal_position",
    );
    const set = new Set(cols.split("\n"));
    expect(set).toContain("_id:uuid");
    expect(set).toContain("_creation_time:bigint");
    expect(set).toContain("label:text");
    expect(set).toContain("qty:double precision");
    expect(set).toContain("tag:text");

    // `tag` (optional) is nullable; `label` (required) is not.
    const nullability = await psql(
      "select column_name || ':' || is_nullable from information_schema.columns " +
        "where table_name = 'widgets' and column_name in ('label','tag') order by column_name",
    );
    expect(nullability).toContain("label:NO");
    expect(nullability).toContain("tag:YES");

    // The declared index exists.
    const idx = await psql(
      "select indexname from pg_indexes where tablename = 'widgets' and indexname = 'widgets_by_label'",
    );
    expect(idx).toBe("widgets_by_label");
  });
});
