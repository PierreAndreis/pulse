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
    channel: v.string(), // page path the visitor is on (e.g. "/", "/docs.html")
    scrollY: v.number(), // scroll position as a fraction of scrollable height (0..1)
    // Shared text selection: absolute character offsets into the page's text
    // (see CursorPresence). -1/-1 means "no selection". Offsets are layout-
    // independent, so each viewer rebuilds the highlight rects for its own layout.
    selStart: v.number(),
    selEnd: v.number(),
  }),
});
