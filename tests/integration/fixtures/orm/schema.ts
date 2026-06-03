import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";

// Fixture schema for the ORM integration suite: varied column types (string,
// number, nullable number, boolean, nullable string, id-ref) to exercise filters,
// aggregates, ordering, grouping, and joins.
export default defineSchema({
  authors: defineTable({ name: v.string() }),
  widgets: defineTable({
    name: v.string(),
    qty: v.number(),
    price: v.optional(v.number()),
    active: v.boolean(),
    tag: v.optional(v.string()),
    authorId: v.optional(v.id("authors")),
  }),
});
