import { describe, expect, it } from "vitest";
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";
import { fieldToColumn, generateDDL } from "./ddl.js";

describe("pgType jsonb arms via generateDDL", () => {
  const ddl = generateDDL(
    defineSchema({
      things: defineTable({
        tags: v.array(v.string()),
        meta: v.object({ a: v.string() }),
        whatever: v.any(),
        nothing: v.null(),
        owner: v.doc("users"),
        optionalTags: v.optional(v.array(v.string())),
      }),
    }),
  );

  it("maps array/object/any/null/doc to jsonb not null", () => {
    expect(ddl).toContain("tags jsonb not null");
    expect(ddl).toContain("meta jsonb not null");
    expect(ddl).toContain("whatever jsonb not null");
    expect(ddl).toContain("nothing jsonb not null");
    expect(ddl).toContain("owner jsonb not null");
  });

  it("drops `not null` for an optional jsonb column", () => {
    expect(ddl).toContain("optional_tags jsonb");
    expect(ddl).not.toContain("optional_tags jsonb not null");
  });
});

describe("pgType union arms via generateDDL", () => {
  it("collapses an all-string-literal union to text", () => {
    const ddl = generateDDL(
      defineSchema({
        users: defineTable({
          role: v.union(v.literal("admin"), v.literal("member")),
        }),
      }),
    );
    expect(ddl).toContain("role text not null");
  });

  it("falls back to jsonb for a mixed-type literal union", () => {
    const ddl = generateDDL(
      defineSchema({
        users: defineTable({
          mixed: v.union(v.literal("a"), v.literal(2)),
        }),
      }),
    );
    expect(ddl).toContain("mixed jsonb not null");
    expect(ddl).not.toContain("mixed text");
  });
});

describe("fieldToColumn", () => {
  it("preserves system fields and snake_cases camelCase", () => {
    expect(fieldToColumn("_id")).toBe("_id");
    expect(fieldToColumn("_creationTime")).toBe("_creation_time");
    expect(fieldToColumn("ownerUserId")).toBe("owner_user_id");
  });
});
