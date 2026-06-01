import { implement } from "@pulse/server";
import { contract } from "./contract.js";
import { authedBase } from "./middleware.js";
import "./_generated/dataModel.js";

const os = implement(contract);

export const create = os.accounts.create.use(authedBase).handler(async ({ ctx, input }) => {
  return ctx.db.insert("accounts", { name: input.name, balance: input.balance });
});

export const get = os.accounts.get.use(authedBase).handler(async ({ ctx, input, errors }) => {
  const doc = await ctx.db.get(input.id);
  if (!doc) throw errors.NOT_FOUND();
  return { id: doc._id, name: doc.name, balance: doc.balance };
});

export const list = os.accounts.list.use(authedBase).handler(async ({ ctx }) => {
  return ctx.db.query("accounts").collect();
});

// Debit `from`, credit `to`, and record the ledger row — all in ONE mutation, so
// the engine runs them inside a single SERIALIZABLE BEGIN…COMMIT (atomic: either
// both balances move and the ledger row exists, or nothing does).
//
// IMPORTANT: we intentionally read/write the two rows in the caller-supplied
// from→to order and do NOT sort them into a canonical lock order. Concurrent
// opposing transfers (A→B and B→A) therefore acquire the two rows in opposing
// order and provoke a real Postgres deadlock (40P01), which the engine's retry
// loop must absorb. A well-meaning "lock rows in sorted order" optimization would
// silently neuter the S2 deadlock test — do not add it.
export const transfer = os.accounts.transfer.use(authedBase).handler(async ({ ctx, input, errors }) => {
  const from = await ctx.db.get(input.from);
  const to = await ctx.db.get(input.to);
  if (!from || !to) throw errors.NOT_FOUND();
  if (from.balance < input.amount) throw errors.INSUFFICIENT_FUNDS({ balance: from.balance });

  await ctx.db.patch(input.from, { balance: from.balance - input.amount });
  await ctx.db.patch(input.to, { balance: to.balance + input.amount });
  await ctx.db.insert("transfers", {
    fromId: input.from,
    toId: input.to,
    amount: input.amount,
  });
  return null;
});
