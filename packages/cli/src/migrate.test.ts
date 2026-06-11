import { describe, expect, it } from "vitest";
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";
import {
  diffAgainstSnapshot,
  hashSql,
  migrationTag,
  renderMigration,
  schemaToSnapshot,
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
