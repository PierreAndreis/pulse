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
    // Integer column (bigint): its values are carried in the change image, so a
    // reactive max(score) is maintained by IVM (not re-executed) — unlike qty/price
    // (double precision), which aren't captured and always re-exec.
    score: v.optional(v.int()),
    active: v.boolean(),
    tag: v.optional(v.string()),
    authorId: v.optional(v.id("authors")),
    // Freeform JSON (jsonb) — exercises object/array/nested round-trip through the
    // text → ::jsonb → text boundary.
    meta: v.optional(v.any()),
  }).index("by_qty", ["qty"]),
});
