import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";
import { authedBase } from "./middleware.js";
import "./_generated/dataModel.js";

const os = implement(contract);

export const create = os.counters.create.use(authedBase).handler(async ({ ctx, input }) => {
  return ctx.db.insert("counters", { name: input.name, value: 0 });
});

export const get = os.counters.get.use(authedBase).handler(async ({ ctx, input }) => {
  const doc = await ctx.db.get(input.id);
  return doc?.value ?? 0;
});

// Read-modify-write: reads the current value, then writes value+1. Concurrent
// increments of the same row conflict under SERIALIZABLE; the engine retries the
// (deterministic) handler, so the final value equals the number of increments —
// no lost updates.
export const increment = os.counters.increment.use(authedBase).handler(async ({ ctx, input, errors }) => {
  const doc = await ctx.db.get(input.id);
  if (!doc) throw errors.NOT_FOUND();
  await ctx.db.patch(input.id, { value: doc.value + 1 });
  return null;
});
