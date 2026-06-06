// A realistic-ish "shop" domain, shaped to exercise distinct bottleneck classes:
// - orders/lineitems/products → joins (N+1) + heavy aggregates over big tables,
// - events → a HOT append-only table (every action logs one) → wide fan-out,
// - products.blob → fat documents (memory + payload pressure).
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";

export default defineSchema({
  users: defineTable({ name: v.string() }),
  products: defineTable({
    name: v.string(),
    price: v.number(),
    // Optional large payload to study fat-document memory / SSE cost.
    blob: v.optional(v.string()),
  }),
  orders: defineTable({
    userId: v.id("users"),
    status: v.string(), // "pending" | "paid" | "shipped"
    total: v.number(),
  }),
  lineitems: defineTable({
    orderId: v.id("orders"),
    productId: v.id("products"),
    qty: v.number(),
    price: v.number(),
  }),
  events: defineTable({ kind: v.string(), note: v.string() }),
});
