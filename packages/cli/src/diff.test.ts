import { describe, expect, it } from "vitest";
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";
import {
  diffSchema,
  isEmptyDiff,
  parseLiveColumns,
  renderDiff,
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
    const d = diffSchema(liveUsers, baseSchema, new Set(["users_by_email"]));
    expect(isEmptyDiff(d)).toBe(true);
    expect(renderDiff(d)).toContain("no changes");
  });

  it("emits CREATE INDEX only for an index the DB lacks", () => {
    // same columns, but the index doesn't exist yet → emitted
    const d = diffSchema(liveUsers, baseSchema, new Set());
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
    const d = diffSchema(live, baseSchema, new Set(["users_by_email"]));
    expect(isEmptyDiff(d)).toBe(true);
  });

  it("emits DROP NOT NULL when a field became optional", () => {
    const schema = defineSchema({
      users: defineTable({ name: v.string(), email: v.optional(v.string()) }),
    });
    const d = diffSchema(liveUsers, schema);
    expect(d.alters.join("\n")).toContain("alter table users alter column email drop not null");
  });

  it("lists dropped columns and tables as destructive, never auto-applied", () => {
    const live: LiveSchema = {
      users: { ...liveUsers.users, legacy: { type: "text", notNull: false } },
      old_table: { _id: { type: "uuid", notNull: true } },
    };
    const d = diffSchema(live, baseSchema);
    expect(d.destructive.join("\n")).toContain("drop column legacy");
    expect(d.destructive.join("\n")).toContain("drop table old_table");
    // destructive is commented out in the rendered script
    expect(renderDiff(d)).toContain("-- drop:");
    expect(renderDiff(d)).toContain("NOT applied automatically");
  });

  it("never drops the engine-managed system columns", () => {
    // live has only system cols + matching user cols → nothing destructive
    const d = diffSchema(liveUsers, baseSchema);
    expect(d.destructive).toEqual([]);
  });
});
