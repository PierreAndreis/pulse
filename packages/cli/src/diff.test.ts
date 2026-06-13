import { describe, expect, it } from "vitest";
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";
import {
  diffSchema,
  isEmptyDiff,
  parseLiveColumns,
  parseLiveIndexes,
  renderDiff,
  type IndexColumnRow,
  type InfoSchemaRow,
  type LiveSchema,
} from "./diff.js";

// A live snapshot matching this schema exactly (system cols + user cols).
const baseSchema = defineSchema({
  users: defineTable({
    name: v.string(),
    email: v.string(),
  }).index("by_email", ["email"]),
});

const liveUsers: LiveSchema = {
  users: {
    _id: { type: "uuid", notNull: true },
    _creation_time: { type: "bigint", notNull: true },
    name: { type: "text", notNull: true },
    email: { type: "text", notNull: true },
  },
};

describe("parseLiveColumns", () => {
  it("groups information_schema rows by table → column", () => {
    const rows: InfoSchemaRow[] = [
      { table_name: "users", column_name: "name", data_type: "text", is_nullable: "NO" },
      { table_name: "users", column_name: "bio", data_type: "text", is_nullable: "YES" },
      { table_name: "posts", column_name: "title", data_type: "text", is_nullable: "NO" },
    ];
    const live = parseLiveColumns(rows);
    expect(Object.keys(live)).toEqual(["users", "posts"]);
    expect(live.users!.name).toEqual({ type: "text", notNull: true });
    expect(live.users!.bio!.notNull).toBe(false);
  });
});

describe("diffSchema", () => {
  it("is empty when the live DB already matches the schema", () => {
    const d = diffSchema(liveUsers, baseSchema, new Map([["users_by_email", ["email"]]]));
    expect(isEmptyDiff(d)).toBe(true);
    expect(renderDiff(d)).toContain("no changes");
  });

  it("emits CREATE INDEX only for an index the DB lacks", () => {
    // same columns, but the index doesn't exist yet → emitted
    const d = diffSchema(liveUsers, baseSchema, new Map());
    expect(d.additive.join("\n")).toContain("create index if not exists users_by_email");
    expect(d.additive.length).toBe(1); // ONLY the index, no spurious column work
  });

  it("emits CREATE TABLE for a table absent from the DB", () => {
    const d = diffSchema({}, baseSchema);
    expect(d.additive.join("\n")).toContain("create table if not exists users");
    expect(d.additive.join("\n")).toContain("email text not null");
    // index for the new table is asserted too
    expect(d.additive.join("\n")).toContain("create index if not exists users_by_email");
    expect(d.destructive).toEqual([]);
  });

  it("emits ADD COLUMN for a new field on an existing table", () => {
    const schema = defineSchema({
      users: defineTable({ name: v.string(), email: v.string(), bio: v.optional(v.string()) }),
    });
    const d = diffSchema(liveUsers, schema);
    expect(d.additive.join("\n")).toContain("alter table users add column if not exists bio text");
    // bio is optional → no NOT NULL review note
    expect(d.alters).toEqual([]);
  });

  it("flags a NOT NULL new column for backfill review instead of a blind NOT NULL add", () => {
    const schema = defineSchema({
      users: defineTable({ name: v.string(), email: v.string(), age: v.number() }),
    });
    const d = diffSchema(liveUsers, schema);
    expect(d.additive.join("\n")).toContain("add column if not exists age double precision");
    expect(d.additive.join("\n")).not.toContain("age double precision not null");
    expect(d.alters.join("\n")).toMatch(/review: users\.age is NOT NULL/);
  });

  it("emits an ALTER TYPE (flagged) when a column's type drifts", () => {
    const schema = defineSchema({
      users: defineTable({ name: v.string(), email: v.number() }), // email text → number
    });
    const d = diffSchema(liveUsers, schema);
    expect(d.alters.join("\n")).toContain("alter table users alter column email type double precision");
    expect(d.alters.join("\n")).toContain("review: type change");
  });

  it("does not flag drift for equivalent type spellings (text vs character varying)", () => {
    const live: LiveSchema = {
      users: {
        _id: { type: "uuid", notNull: true },
        _creation_time: { type: "bigint", notNull: true },
        name: { type: "character varying", notNull: true },
        email: { type: "text", notNull: true },
      },
    };
    const d = diffSchema(live, baseSchema, new Map([["users_by_email", ["email"]]]));
    expect(isEmptyDiff(d)).toBe(true);
  });

  it("emits DROP NOT NULL when a field became optional", () => {
    const schema = defineSchema({
      users: defineTable({ name: v.string(), email: v.optional(v.string()) }),
    });
    const d = diffSchema(liveUsers, schema);
    expect(d.alters.join("\n")).toContain("alter table users alter column email drop not null");
  });

  it("lists dropped columns and tables as structured destructive drops", () => {
    const live: LiveSchema = {
      users: { ...liveUsers.users, legacy: { type: "text", notNull: false } },
      old_table: { _id: { type: "uuid", notNull: true } },
    };
    const d = diffSchema(live, baseSchema);
    expect(d.destructive).toContainEqual({ kind: "column", table: "users", column: "legacy" });
    expect(d.destructive).toContainEqual({ kind: "table", table: "old_table" });
    // rendered as commented-out drops for the migration script
    expect(renderDiff(d)).toContain("-- drop: alter table users drop column legacy;");
    expect(renderDiff(d)).toContain("-- drop: drop table old_table;");
  });

  it("never drops the engine-managed system columns", () => {
    // live has only system cols + matching user cols → nothing destructive
    const d = diffSchema(liveUsers, baseSchema);
    expect(d.destructive).toEqual([]);
  });

  it("treats float8/bool aliases as matching number/boolean (no spurious ALTER TYPE)", () => {
    const schema = defineSchema({
      t: defineTable({ n: v.number(), flag: v.boolean() }),
    });
    // _creation_time stays `bigint` (system col) — the int8↔bigint alias path is
    // exercised here too: PG reports `int8`, the live DDL/normalizer treats it as
    // `bigint`, so a matching bigint column shows no drift.
    const live: LiveSchema = {
      t: {
        _id: { type: "uuid", notNull: true },
        _creation_time: { type: "int8", notNull: true },
        n: { type: "float8", notNull: true },
        flag: { type: "bool", notNull: true },
      },
    };
    const d = diffSchema(live, schema);
    expect(isEmptyDiff(d)).toBe(true);
    expect(d.alters).toEqual([]);
  });

  it("flags a nullable live column that the schema wants NOT NULL", () => {
    const live: LiveSchema = {
      users: {
        ...liveUsers.users,
        email: { type: "text", notNull: false }, // schema email is non-optional v.string()
      },
    };
    const d = diffSchema(live, baseSchema, new Map([["users_by_email", ["email"]]]));
    expect(d.alters.join("\n")).toContain("review: users.email should be NOT NULL");
  });

  it("skips the engine-managed _pulse_mutations table (not flagged destructive)", () => {
    const live: LiveSchema = {
      ...liveUsers,
      _pulse_mutations: { _id: { type: "uuid", notNull: true } },
    };
    const d = diffSchema(live, baseSchema, new Map([["users_by_email", ["email"]]]));
    expect(d.destructive).not.toContainEqual({ kind: "table", table: "_pulse_mutations" });
    expect(d.destructive).toEqual([]);
  });

  it("emits DROP INDEX when a managed index is removed from the schema", () => {
    // schema no longer declares any index; the DB still has users_by_email
    const schema = defineSchema({
      users: defineTable({ name: v.string(), email: v.string() }),
    });
    const d = diffSchema(
      liveUsers,
      schema,
      new Map([
        ["users_pkey", ["_id"]],
        ["users_by_email", ["email"]],
      ]),
    );
    expect(d.additive).toContain("drop index if exists users_by_email;");
    // never the primary key index
    expect(d.additive.join("\n")).not.toContain("users_pkey");
    expect(d.destructive).toEqual([]);
  });

  it("re-creates an index that kept its name but changed columns", () => {
    // live users_by_email covers (name); the schema's index covers (email)
    const d = diffSchema(liveUsers, baseSchema, new Map([["users_by_email", ["name"]]]));
    expect(d.additive).toContain("drop index if exists users_by_email;");
    expect(d.additive).toContain("create index if not exists users_by_email on users (email);");
    // drop precedes the re-create
    expect(d.additive.indexOf("drop index if exists users_by_email;")).toBeLessThan(
      d.additive.indexOf("create index if not exists users_by_email on users (email);"),
    );
  });

  it("does not re-create an index whose columns are unknown (legacy snapshot)", () => {
    // empty column list = "columns unknown" → treated as a match, no churn
    const d = diffSchema(liveUsers, baseSchema, new Map([["users_by_email", []]]));
    expect(isEmptyDiff(d)).toBe(true);
  });

  it("never drops a primary key or an index on an unmanaged table", () => {
    // baseSchema still declares users_by_email, so it stays. The live DB also has
    // a pkey and an index on a table not in the schema — neither must be dropped.
    const d = diffSchema(
      liveUsers,
      baseSchema,
      new Map([
        ["users_by_email", ["email"]],
        ["users_pkey", ["_id"]],
        ["posts_by_slug", ["slug"]],
      ]),
    );
    expect(d.additive.join("\n")).not.toContain("drop index");
  });

  it("renders all three sections with their headers when each is non-empty", () => {
    const schema = defineSchema({
      users: defineTable({ name: v.string(), email: v.string(), age: v.number() }),
    });
    const live: LiveSchema = {
      users: { ...liveUsers.users, legacy: { type: "text", notNull: false } },
      old_table: { _id: { type: "uuid", notNull: true } },
    };
    const d = diffSchema(live, schema);
    expect(d.additive.length).toBeGreaterThan(0);
    expect(d.alters.length).toBeGreaterThan(0);
    expect(d.destructive.length).toBeGreaterThan(0);
    const out = renderDiff(d);
    expect(out).toContain("additive (safe to apply)");
    expect(out).toContain("alters (review before applying)");
    expect(out).toContain("destructive (auto-applied only when empty; else review)");
  });
});

describe("parseLiveIndexes", () => {
  it("groups index-column rows into name → ordered columns", () => {
    const rows: IndexColumnRow[] = [
      { indexname: "widgets_by_qty_name", column_name: "qty", ord: 1 },
      { indexname: "widgets_by_qty_name", column_name: "name", ord: 2 },
      { indexname: "widgets_pkey", column_name: "_id", ord: 1 },
    ];
    const map = parseLiveIndexes(rows);
    expect(map.get("widgets_by_qty_name")).toEqual(["qty", "name"]); // order preserved
    expect(map.get("widgets_pkey")).toEqual(["_id"]);
  });

  it("keeps an expression-member index present but with its null column skipped", () => {
    const rows: IndexColumnRow[] = [
      { indexname: "widgets_expr", column_name: null, ord: 1 },
      { indexname: "widgets_expr", column_name: "qty", ord: 2 },
    ];
    const map = parseLiveIndexes(rows);
    expect(map.get("widgets_expr")).toEqual(["qty"]);
  });
});
