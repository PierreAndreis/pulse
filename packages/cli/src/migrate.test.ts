import { describe, expect, it } from "vitest";
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  applyPending,
  diffAgainstSnapshot,
  hashSql,
  migrationsDirFor,
  migrationStates,
  migrationTag,
  readAppliedMigrations,
  readLastSnapshot,
  readMigrations,
  renderMigration,
  schemaToSnapshot,
  type MigrationClient,
  type OnDiskMigration,
  type Snapshot,
} from "./migrate.js";

const v1 = defineSchema({
  widgets: defineTable({ name: v.string(), qty: v.int(), tag: v.optional(v.string()) }).index(
    "by_qty",
    ["qty"],
  ),
});

describe("schemaToSnapshot", () => {
  it("captures system + field columns, types, nullability, and index names", () => {
    const snap = schemaToSnapshot(v1);
    expect(snap.version).toBe(1);
    const cols = snap.columns.widgets!;
    expect(cols._id).toEqual({ type: "uuid", notNull: true });
    expect(cols._creation_time).toEqual({ type: "bigint", notNull: true });
    expect(cols.name).toEqual({ type: "text", notNull: true });
    expect(cols.qty).toEqual({ type: "bigint", notNull: true }); // v.int → bigint
    expect(cols.tag).toEqual({ type: "text", notNull: false }); // optional
    expect(snap.indexes).toEqual(["widgets_by_qty"]);
    expect(snap.indexColumns).toEqual({ widgets_by_qty: ["qty"] });
  });
});

describe("diffAgainstSnapshot", () => {
  it("first migration (no prior snapshot) creates every table + index", () => {
    const diff = diffAgainstSnapshot(v1, null);
    expect(diff.additive.some((s) => s.startsWith("create table if not exists widgets"))).toBe(true);
    expect(diff.additive.some((s) => /create index .*widgets_by_qty/i.test(s))).toBe(true);
    expect(diff.destructive).toHaveLength(0);
  });

  it("an added column is a single additive ADD COLUMN against the last snapshot", () => {
    const prev = schemaToSnapshot(v1);
    const v2 = defineSchema({
      widgets: defineTable({
        name: v.string(),
        qty: v.int(),
        tag: v.optional(v.string()),
        score: v.optional(v.int()),
      }).index("by_qty", ["qty"]),
    });
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.additive).toEqual(["alter table widgets add column if not exists score bigint;"]);
    expect(diff.alters).toHaveLength(0);
    expect(diff.destructive).toHaveLength(0);
  });

  it("a removed column is destructive (never additive)", () => {
    const prev = schemaToSnapshot(v1);
    const v2 = defineSchema({
      widgets: defineTable({ name: v.string(), qty: v.int() }).index("by_qty", ["qty"]),
    });
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.destructive).toEqual([{ kind: "column", table: "widgets", column: "tag" }]);
    expect(diff.additive).toHaveLength(0);
  });

  it("a removed index is a DROP INDEX against the last snapshot", () => {
    const prev = schemaToSnapshot(v1); // snapshot has index widgets_by_qty
    const v2 = defineSchema({
      widgets: defineTable({ name: v.string(), qty: v.int(), tag: v.optional(v.string()) }),
    });
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.additive).toEqual(["drop index if exists widgets_by_qty;"]);
    expect(diff.destructive).toHaveLength(0);
  });

  it("no schema change against its own snapshot yields an empty diff", () => {
    const diff = diffAgainstSnapshot(v1, schemaToSnapshot(v1));
    expect(diff.additive).toHaveLength(0);
    expect(diff.alters).toHaveLength(0);
    expect(diff.destructive).toHaveLength(0);
  });
});

describe("renderMigration", () => {
  it("emits additive live and destructive commented-out", () => {
    const diff = {
      additive: ["alter table widgets add column if not exists score bigint;"],
      alters: [],
      destructive: [{ kind: "column", table: "widgets", column: "tag" } as const],
    };
    const sql = renderMigration("0001_demo", diff);
    expect(sql).toContain("-- 0001_demo");
    expect(sql).toContain("alter table widgets add column if not exists score bigint;");
    // The drop is present but commented out (data-loss opt-in).
    expect(sql).toMatch(/^-- alter table widgets drop column/m);
    expect(sql).not.toMatch(/^alter table widgets drop column/m);
  });
});

describe("migrationTag / hashSql", () => {
  it("pads the index and slugifies the name", () => {
    expect(migrationTag(0, "init")).toBe("0000_init");
    expect(migrationTag(3, "Add Score!!")).toBe("0003_add_score");
    expect(migrationTag(12, "")).toBe("0012_migration");
  });

  it("hash is deterministic and changes when the SQL is edited", () => {
    expect(hashSql("create table x;")).toBe(hashSql("create table x;"));
    expect(hashSql("create table x;")).not.toBe(hashSql("create table y;"));
    expect(hashSql("")).toMatch(/^[0-9a-f]{8}$/);
  });
});

// Sanity: a snapshot round-trips through JSON (it's persisted as a file).
describe("snapshot persistence", () => {
  it("survives JSON.stringify/parse unchanged", () => {
    const snap = schemaToSnapshot(v1);
    const round: Snapshot = JSON.parse(JSON.stringify(snap));
    expect(round).toEqual(snap);
    expect(diffAgainstSnapshot(v1, round).additive).toHaveLength(0);
  });
});

// A fake Postgres client for the migration runner: an in-memory `_pulse_migrations`
// journal, an ordered log of every SQL it saw (to assert tx wrapping), and an
// optional substring that makes a migration's SQL throw (to exercise rollback).
class FakeClient implements MigrationClient {
  journal = new Map<string, string>();
  log: string[] = [];
  failOn?: string;
  async query(sql: string, params?: unknown[]): Promise<{ rows: unknown[] }> {
    this.log.push(sql);
    if (/^create table if not exists _pulse_migrations/.test(sql)) return { rows: [] };
    if (/^select tag, hash from _pulse_migrations/.test(sql)) {
      return { rows: [...this.journal].map(([tag, hash]) => ({ tag, hash })) };
    }
    if (sql === "begin" || sql === "commit" || sql === "rollback") return { rows: [] };
    if (/^insert into _pulse_migrations/.test(sql)) {
      const [tag, hash] = params as [string, string];
      this.journal.set(tag, hash);
      return { rows: [] };
    }
    if (this.failOn && sql.includes(this.failOn)) throw new Error("syntax error");
    return { rows: [] };
  }
}

const mig = (idx: number, tag: string, sql: string): OnDiskMigration => ({
  idx,
  tag,
  sql,
  hash: hashSql(sql),
});

describe("applyPending (migration runner)", () => {
  it("applies pending migrations in order, each in a begin/commit tx, and records them", async () => {
    const c = new FakeClient();
    const applied: string[] = [];
    const migs = [mig(0, "0000_a", "create table a ();"), mig(1, "0001_b", "create table b ();")];
    const n = await applyPending(c, migs, (t) => applied.push(t));

    expect(n).toBe(2);
    expect(applied).toEqual(["0000_a", "0001_b"]);
    expect([...c.journal.keys()]).toEqual(["0000_a", "0001_b"]);
    const begin = c.log.indexOf("begin");
    expect(c.log.slice(begin, begin + 4)).toEqual([
      "begin",
      "create table a ();",
      "insert into _pulse_migrations (tag, hash) values ($1, $2)",
      "commit",
    ]);
    expect(c.log).not.toContain("rollback");
  });

  it("is idempotent — a second run applies nothing", async () => {
    const c = new FakeClient();
    const migs = [mig(0, "0000_a", "create table a ();")];
    await applyPending(c, migs);
    expect(await applyPending(c, migs)).toBe(0);
  });

  it("skips already-applied migrations and runs only the new ones", async () => {
    const c = new FakeClient();
    c.journal.set("0000_a", hashSql("create table a ();"));
    const migs = [mig(0, "0000_a", "create table a ();"), mig(1, "0001_b", "create table b ();")];
    const applied: string[] = [];
    expect(await applyPending(c, migs, (t) => applied.push(t))).toBe(1);
    expect(applied).toEqual(["0001_b"]);
  });

  it("throws on hash drift — an applied migration's file was edited", async () => {
    const c = new FakeClient();
    c.journal.set("0000_a", hashSql("create table a ();"));
    const migs = [mig(0, "0000_a", "create table a_edited ();")];
    await expect(applyPending(c, migs)).rejects.toThrow(/already applied but its file changed/);
    expect(c.log).not.toContain("begin"); // refuses before running anything
  });

  it("rolls back and wraps the error when a migration's SQL fails — nothing recorded", async () => {
    const c = new FakeClient();
    c.failOn = "explode";
    const migs = [mig(0, "0000_bad", "explode now;")];
    await expect(applyPending(c, migs)).rejects.toThrow(/migration 0000_bad failed: syntax error/);
    expect(c.log).toContain("rollback");
    expect(c.journal.size).toBe(0);
  });
});

describe("readMigrations / readLastSnapshot (filesystem)", () => {
  it("reads NNNN_*.sql in order with content hashes, ignoring other files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "pulse-mig-"));
    // written out of order, with non-migration files mixed in
    await writeFile(join(dir, "0001_add.sql"), "alter table a add b int;");
    await writeFile(join(dir, "0000_init.sql"), "create table a ();");
    await writeFile(join(dir, "README.md"), "# not a migration");
    await writeFile(join(dir, "notes.sql"), "-- missing NNNN_ prefix");

    const migs = await readMigrations(dir);
    expect(migs.map((m) => m.tag)).toEqual(["0000_init", "0001_add"]); // sorted, filtered
    expect(migs.map((m) => m.idx)).toEqual([0, 1]);
    expect(migs[0]!.hash).toBe(hashSql("create table a ();"));
  });

  it("returns [] when the migrations directory does not exist", async () => {
    const dir = await mkdtemp(join(tmpdir(), "pulse-mig-"));
    expect(await readMigrations(join(dir, "nope"))).toEqual([]);
  });

  it("reads the latest snapshot from meta/, or null when there is none", async () => {
    const dir = await mkdtemp(join(tmpdir(), "pulse-mig-"));
    expect(await readLastSnapshot(dir)).toBeNull(); // no meta/ yet

    await mkdir(join(dir, "meta"), { recursive: true });
    const snap = schemaToSnapshot(v1);
    await writeFile(join(dir, "meta", "0000_init.snapshot.json"), JSON.stringify(snap));
    await writeFile(join(dir, "meta", "0001_add.snapshot.json"), JSON.stringify(snap));
    const last = await readLastSnapshot(dir);
    expect(last).toEqual(snap); // highest-numbered snapshot wins
  });
});

describe("migrationsDirFor", () => {
  it("is the `migrations/` dir next to the schema file", () => {
    expect(migrationsDirFor("/app/schema.ts")).toBe("/app/migrations");
    expect(migrationsDirFor("/app/db/schema.ts")).toBe("/app/db/migrations");
  });
});

describe("migrationStates", () => {
  it("classifies each migration as applied / pending / drift", () => {
    const migs = [mig(0, "0000_a", "A;"), mig(1, "0001_b", "B;"), mig(2, "0002_c", "C;")];
    const applied = new Map([
      ["0000_a", hashSql("A;")], // matches → applied
      ["0001_b", "deadbeef"], // recorded but hash differs → drift
    ]); // 0002_c absent → pending
    expect(migrationStates(migs, applied)).toEqual([
      { tag: "0000_a", state: "applied" },
      { tag: "0001_b", state: "drift" },
      { tag: "0002_c", state: "pending" },
    ]);
  });
});

describe("readAppliedMigrations", () => {
  it("ensures the journal table first, then returns the recorded tag → hash", async () => {
    const c = new FakeClient();
    c.journal.set("0000_a", "h1");
    const m = await readAppliedMigrations(c);
    expect(m.get("0000_a")).toBe("h1");
    expect(c.log[0]).toMatch(/^create table if not exists _pulse_migrations/);
  });
});

// Scenarios ported from Prisma's schema-engine migration tests
// (schema-engine/sql-migration-tests/tests/migrations/{basic,indexes}.rs),
// translated to our snapshot-diff model. We only port what our surface supports:
// tables, columns, types, nullability, and named multi-column indexes — not
// Prisma's foreign keys, unique constraints, enums, defaults, or renames-as-rename
// (we have no rename detection; a renamed index is a drop + create).
describe("prisma-derived scenarios", () => {
  // basic.rs::adding_multiple_optional_fields_to_an_existing_model_works
  it("adding multiple optional fields is all-additive, no alters", () => {
    const prev = schemaToSnapshot(v1);
    const v2 = defineSchema({
      widgets: defineTable({
        name: v.string(),
        qty: v.int(),
        tag: v.optional(v.string()),
        note: v.optional(v.string()),
        score: v.optional(v.int()),
      }).index("by_qty", ["qty"]),
    });
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.additive).toEqual([
      "alter table widgets add column if not exists note text;",
      "alter table widgets add column if not exists score bigint;",
    ]);
    expect(diff.alters).toHaveLength(0);
    expect(diff.destructive).toHaveLength(0);
  });

  // basic.rs::a_model_can_be_removed
  it("removing a whole table is destructive, never additive", () => {
    const prev = schemaToSnapshot(
      defineSchema({
        widgets: defineTable({ name: v.string() }),
        gadgets: defineTable({ label: v.string() }),
      }),
    );
    const v2 = defineSchema({ widgets: defineTable({ name: v.string() }) });
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.destructive).toEqual([{ kind: "table", table: "gadgets" }]);
    expect(diff.additive).toHaveLength(0);
  });

  // basic.rs::created_at_does_not_get_arbitrarily_migrated — the engine-managed
  // system columns (_id / _creation_time) must never show spurious drift.
  it("re-applying an identical schema is a no-op (system columns never drift)", () => {
    const wide = defineSchema({
      things: defineTable({
        title: v.string(),
        count: v.int(),
        ratio: v.number(),
        active: v.boolean(),
        meta: v.optional(v.string()),
      }).index("by_count", ["count"]),
    });
    const diff = diffAgainstSnapshot(wide, schemaToSnapshot(wide));
    expect(diff.additive).toHaveLength(0);
    expect(diff.alters).toHaveLength(0);
    expect(diff.destructive).toHaveLength(0);
  });

  // indexes.rs::model_with_multiple_indexes_works — multiple indexes on one table;
  // removing some drops only those, leaving the rest untouched.
  it("a table can carry multiple indexes; removing some drops only those", () => {
    const prev = schemaToSnapshot(
      defineSchema({
        widgets: defineTable({ name: v.string(), qty: v.int(), tag: v.optional(v.string()) })
          .index("by_qty", ["qty"])
          .index("by_name", ["name"])
          .index("by_tag", ["tag"]),
      }),
    );
    const v2 = defineSchema({
      widgets: defineTable({ name: v.string(), qty: v.int(), tag: v.optional(v.string()) }).index(
        "by_qty",
        ["qty"],
      ),
    });
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.additive).toContain("drop index if exists widgets_by_name;");
    expect(diff.additive).toContain("drop index if exists widgets_by_tag;");
    expect(diff.additive).not.toContain("drop index if exists widgets_by_qty;");
    expect(diff.destructive).toHaveLength(0);
  });

  // indexes.rs::index_renaming_must_work — we have no rename detection, so a
  // renamed index is a drop of the old name plus a create of the new one.
  it("renaming an index drops the old and creates the new", () => {
    const prev = schemaToSnapshot(v1); // index widgets_by_qty
    const v2 = defineSchema({
      widgets: defineTable({ name: v.string(), qty: v.int(), tag: v.optional(v.string()) }).index(
        "by_quantity",
        ["qty"],
      ),
    });
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.additive).toContain("create index if not exists widgets_by_quantity on widgets (qty);");
    expect(diff.additive).toContain("drop index if exists widgets_by_qty;");
  });

  // indexes.rs::column_type_migrations_should_not_implicitly_drop_indexes — a
  // column type change must ALTER the column, never drop the index that covers it.
  it("a column type change never implicitly drops its index", () => {
    const prev = schemaToSnapshot(
      defineSchema({
        widgets: defineTable({ name: v.string(), qty: v.int() }).index("by_qty", ["qty"]),
      }),
    );
    const v2 = defineSchema({
      widgets: defineTable({ name: v.string(), qty: v.number() }).index("by_qty", ["qty"]),
    }); // qty: int8 → double precision
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.alters.join("\n")).toContain("alter table widgets alter column qty type double precision");
    expect(diff.additive.join("\n")).not.toContain("drop index");
  });

  // indexes.rs::column_type_migrations_should_not_implicitly_drop_compound_indexes
  it("a column type change never drops a compound index", () => {
    const prev = schemaToSnapshot(
      defineSchema({
        widgets: defineTable({ name: v.string(), qty: v.int() }).index("by_qty_name", [
          "qty",
          "name",
        ]),
      }),
    );
    const v2 = defineSchema({
      widgets: defineTable({ name: v.string(), qty: v.number() }).index("by_qty_name", [
        "qty",
        "name",
      ]),
    });
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.additive.join("\n")).not.toContain("drop index");
  });

  // indexes.rs::index_updates_with_rename_must_work — an index that keeps its
  // NAME but changes its COLUMNS is dropped and re-created. The snapshot records
  // each index's columns, so the change is no longer invisible.
  it("redefining an index's columns under the same name re-creates it", () => {
    const prev = schemaToSnapshot(
      defineSchema({
        widgets: defineTable({ name: v.string(), qty: v.int() }).index("by_it", ["qty"]),
      }),
    );
    const v2 = defineSchema({
      widgets: defineTable({ name: v.string(), qty: v.int() }).index("by_it", ["name"]), // qty → name
    });
    const diff = diffAgainstSnapshot(v2, prev);
    expect(diff.additive).toEqual([
      "drop index if exists widgets_by_it;",
      "create index if not exists widgets_by_it on widgets (name);",
    ]);
    expect(diff.destructive).toHaveLength(0);
  });
});
