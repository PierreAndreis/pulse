import { defineSchema, defineTable, v } from "@pulse/schema";

export default defineSchema({
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

  channels: defineTable({
    name: v.string(),
    isPrivate: v.boolean(),
  }),

  counters: defineTable({
    name: v.string(),
    value: v.number(),
  }),

  // Money-transfer surface for the S2 multi-row deadlock / conservation tests:
  // `transfer` debits one account and credits another in one mutation (one
  // BEGIN…COMMIT); concurrent opposing transfers provoke 40P01 deadlocks.
  accounts: defineTable({
    name: v.string(),
    balance: v.number(),
  }),

  transfers: defineTable({
    fromId: v.id("accounts"),
    toId: v.id("accounts"),
    amount: v.number(),
  }),

  // A collaborative document: `body` is a CRDT (Yjs) field that merges offline /
  // concurrent edits instead of clobbering (the long-Notion-page case).
  notes: defineTable({
    title: v.string(),
    body: v.collab(),
  }),
});
