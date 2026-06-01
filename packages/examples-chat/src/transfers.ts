import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";
import { authedBase } from "./middleware.js";
import "./_generated/dataModel.js";

const os = implement(contract);

// Raw count of ledger rows — reconciled against the number of committed transfer
// calls (catches a created/dropped ledger entry even when balances net plausibly).
export const countRaw = os.transfers.countRaw.use(authedBase).handler(async ({ ctx }) => {
  const rows = await ctx.sql<{ n: number }>`SELECT count(*)::int AS n FROM transfers`;
  return { count: rows[0]?.n ?? 0 };
});
