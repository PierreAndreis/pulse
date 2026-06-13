import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { defineSchema, defineTable, v } from "../../packages/schema/src/index.js";
import { generateDDL } from "../../packages/cli/src/ddl.js";
import {
  applyPending,
  diffAgainstSnapshot,
  migrationTag,
  readAppliedMigrations,
  readMigrations,
  renderMigration,
  schemaToSnapshot,
  type Snapshot,
} from "../../packages/cli/src/migrate.js";

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

// The file-based migration lifecycle, end-to-end against real Postgres: generate
// editable SQL from the schema diff, apply it through the real `applyPending`
// (each migration in its own transaction over one persistent connection), and
// verify the journal, idempotency, drift refusal, and index redefinition.
interface PgConn {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
  connect(): Promise<void>;
  end(): Promise<void>;
}

describe("pulse migrate — file lifecycle on real Postgres (deploy / journal / drift / reindex)", () => {
  let client: PgConn;
  let dir: string;

  // Write a migration (and its snapshot) for `schema` against `prev`, returning
  // the new snapshot — exactly what `migrate dev` persists.
  async function generate(
    schema: ReturnType<typeof defineSchema>,
    prev: Snapshot | null,
    idx: number,
    name: string,
  ): Promise<Snapshot> {
    const diff = diffAgainstSnapshot(schema, prev);
    const tag = migrationTag(idx, name);
    await mkdir(join(dir, "meta"), { recursive: true });
    await writeFile(join(dir, `${tag}.sql`), renderMigration(tag, diff));
    const snap = schemaToSnapshot(schema);
    await writeFile(join(dir, "meta", `${tag}.snapshot.json`), JSON.stringify(snap));
    return snap;
  }

  // The ordered columns Postgres reports for an index (empty if it's gone).
  async function indexColumns(name: string): Promise<string[]> {
    const { rows } = await client.query(
      "SELECT a.attname AS col FROM pg_index ix " +
        "JOIN pg_class ic ON ic.oid = ix.indexrelid " +
        "JOIN pg_class tc ON tc.oid = ix.indrelid " +
        "JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON true " +
        "LEFT JOIN pg_attribute a ON a.attrelid = tc.oid AND a.attnum = k.attnum " +
        "WHERE ic.relname = $1 ORDER BY k.ord",
      [name],
    );
    return rows.map((r) => r.col as string);
  }

  const v1 = defineSchema({
    gizmos: defineTable({ name: v.string(), qty: v.int() }).index("by_qty", ["qty"]),
  });

  beforeAll(async () => {
    const pg = (await import("pg" as string)) as {
      Client: new (cfg: { connectionString: string }) => PgConn;
    };
    client = new pg.Client({ connectionString: DATABASE_URL });
    await client.connect();
    await client.query("drop table if exists gizmos cascade");
    await client.query("drop table if exists _pulse_migrations cascade");
    dir = await mkdtemp(join(tmpdir(), "pulse-mig-it-"));
  });

  afterAll(async () => {
    await client.query("drop table if exists gizmos cascade");
    await client.query("drop table if exists _pulse_migrations cascade");
    await client.end();
  });

  test("generate then deploy creates the table + index, records the journal, and is idempotent", async () => {
    await generate(v1, null, 0, "init");
    const n = await applyPending(client, await readMigrations(dir));
    expect(n).toBe(1);

    const { rows: tbl } = await client.query("select to_regclass('gizmos') as t");
    expect(tbl[0]!.t).toBe("gizmos");
    expect(await indexColumns("gizmos_by_qty")).toEqual(["qty"]);

    const applied = await readAppliedMigrations(client);
    expect([...applied.keys()]).toEqual(["0000_init"]);

    // Re-running applies nothing (already recorded).
    expect(await applyPending(client, await readMigrations(dir))).toBe(0);
  });

  test("redefining an index's columns generates drop+create and re-creates it in Postgres", async () => {
    const v2 = defineSchema({
      gizmos: defineTable({ name: v.string(), qty: v.int() }).index("by_qty", ["name"]), // qty -> name
    });
    await generate(v2, schemaToSnapshot(v1), 1, "reindex");

    const sql = await readFile(join(dir, "0001_reindex.sql"), "utf8");
    expect(sql).toContain("drop index if exists gizmos_by_qty;");
    expect(sql).toContain("create index if not exists gizmos_by_qty on gizmos (name);");

    expect(await applyPending(client, await readMigrations(dir))).toBe(1);
    expect(await indexColumns("gizmos_by_qty")).toEqual(["name"]); // now covers (name)
  });

  test("deploy refuses a migration whose already-applied file was edited (hash drift)", async () => {
    await writeFile(join(dir, "0000_init.sql"), "-- tampered after apply\n");
    await expect(applyPending(client, await readMigrations(dir))).rejects.toThrow(
      /already applied but its file changed/,
    );
  });
});
