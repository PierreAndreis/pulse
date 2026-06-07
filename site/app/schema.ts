// Live-cursor presence for the Pulse landing page — one short-lived row per
// visitor. `x`/`y` are viewport fractions (0..1) so cursors land correctly across
// screen sizes; `updatedAt` drives staleness (a cursor that stops reporting fades).
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";

export default defineSchema({
  cursors: defineTable({
    clientId: v.string(), // stable per browser tab/session
    x: v.number(),
    y: v.number(),
    country: v.string(), // ISO-3166 alpha-2 from IP geo (e.g. "US"); "" if unknown
    color: v.string(), // hex, assigned per visitor
    updatedAt: v.number(), // epoch ms of the last move
  }),
});
