import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";
import "./_generated/dataModel.js"; // typed ctx.db (Doc<"cursors">) — no `any`

const os = implement(contract);
const STALE_MS = 8_000; // no move within this window ⇒ the cursor is considered gone

// `ctx`, `input`, and each `c` are fully inferred (Doc<"cursors">) from the
// contract + generated data model — the typed Pulse handler shape.
export const list = os.presence.list.handler(async ({ ctx }) => {
  const cutoff = Date.now() - STALE_MS;
  const all = await ctx.db.query("cursors").collect();
  return all.filter((c) => c.updatedAt > cutoff);
});

export const move = os.presence.move.handler(async ({ ctx, input }) => {
  const now = Date.now();
  const all = await ctx.db.query("cursors").collect();
  const mine = all.find((c) => c.clientId === input.clientId);
  if (mine) {
    await ctx.db.patch(mine._id, {
      x: input.x,
      y: input.y,
      country: input.country,
      color: input.color,
      updatedAt: now,
    });
  } else {
    await ctx.db.insert("cursors", {
      clientId: input.clientId,
      x: input.x,
      y: input.y,
      country: input.country,
      color: input.color,
      updatedAt: now,
      selStart: -1,
      selEnd: -1,
    });
  }
  // Opportunistic GC: collect() reads the whole table, so drop long-stale rows
  // to keep it small (sessions that never sent a `leave`).
  const cutoff = now - STALE_MS * 4;
  for (const c of all) {
    if (c.clientId !== input.clientId && c.updatedAt < cutoff) await ctx.db.delete(c._id);
  }
  return null;
});

export const select = os.presence.select.handler(async ({ ctx, input }) => {
  const now = Date.now();
  const all = await ctx.db.query("cursors").collect();
  const mine = all.find((c) => c.clientId === input.clientId);
  if (mine) {
    await ctx.db.patch(mine._id, {
      selStart: input.selStart,
      selEnd: input.selEnd,
      updatedAt: now,
    });
  }
  // No row yet ⇒ the visitor hasn't moved; nothing to attach a selection to.
  return null;
});

export const leave = os.presence.leave.handler(async ({ ctx, input }) => {
  const all = await ctx.db.query("cursors").collect();
  const mine = all.find((c) => c.clientId === input.clientId);
  if (mine) await ctx.db.delete(mine._id);
  return null;
});
