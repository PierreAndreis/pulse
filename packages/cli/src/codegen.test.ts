import { describe, expect, it } from "vitest";
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";
import { generateDataModel } from "./codegen.js";
import { generateDDL } from "./ddl.js";

describe("collab fields", () => {
  const s = defineSchema({
    notes: defineTable({ title: v.string(), body: v.collab() }),
  });

  it("generates a CollabField type + imports it", () => {
    const out = generateDataModel(s);
    expect(out).toContain("body: CollabField;");
    expect(out).toContain("import type { Id, CollabField }");
  });

  it("maps collab to a nullable bytea column", () => {
    const ddl = generateDDL(s);
    expect(ddl).toContain("body bytea");
    expect(ddl).not.toContain("body bytea not null");
  });
});

const schema = defineSchema({
  users: defineTable({
    name: v.string(),
    email: v.string(),
    role: v.union(v.literal("admin"), v.literal("member")),
  }).index("by_email", ["email"]),

  messages: defineTable({
    authorId: v.id("users"),
    channelId: v.id("channels"),
    body: v.string(),
    editedAt: v.optional(v.number()),
  }).index("by_channel", ["channelId", "_creationTime"]),
});

describe("generateDataModel", () => {
  const out = generateDataModel(schema);

  it("emits a PulseDataModel augmentation with system fields", () => {
    expect(out).toContain('declare module "@onveloz/pulse-schema"');
    expect(out).toContain("interface PulseDataModel");
    expect(out).toContain('_id: Id<"users">;');
    expect(out).toContain("_creationTime: number;");
  });

  it("maps validators to precise TS types", () => {
    expect(out).toContain('role: "admin" | "member";');
    expect(out).toContain('channelId: Id<"channels">;');
    expect(out).toContain("body: string;");
  });

  it("marks optional fields with `?`", () => {
    expect(out).toContain("editedAt?: number;");
    expect(out).not.toContain("editedAt: number;");
  });
});

describe("generateDDL", () => {
  const ddl = generateDDL(schema);

  it("creates tables idempotently with system columns", () => {
    expect(ddl).toContain("create table if not exists users (");
    expect(ddl).toContain("_id uuid primary key default gen_random_uuid()");
    expect(ddl).toContain("_creation_time bigint not null");
  });

  it("maps fields to snake_case columns and pg types", () => {
    expect(ddl).toContain("author_id uuid not null");
    expect(ddl).toContain("channel_id uuid not null");
    expect(ddl).toContain("role text not null"); // union of string literals
    expect(ddl).toContain("body text not null");
  });

  it("makes optional fields nullable", () => {
    expect(ddl).toContain("edited_at double precision");
    expect(ddl).not.toContain("edited_at double precision not null");
  });

  it("emits indexes with snake_case columns", () => {
    expect(ddl).toContain(
      "create index if not exists messages_by_channel on messages (channel_id, _creation_time);",
    );
    expect(ddl).toContain("create index if not exists users_by_email on users (email);");
  });
});
