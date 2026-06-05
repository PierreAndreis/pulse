// Dynamically built handlers: count{i} reads table t{i}; add{i} inserts into it.
// Each procedure's path comes from `os.w.count{i}` (the contract), so exporting
// them in a plain object is fine — the worker registers by path, not export name.
import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";
import { K } from "./schema.js";

const os = implement(contract) as any;

const w: Record<string, unknown> = {};
for (let i = 0; i < K; i++) {
  const table = `t${i}`;
  w[`count${i}`] = os.w[`count${i}`].handler(async ({ ctx }: any) =>
    (ctx.db.query(table) as any).count(),
  );
  w[`add${i}`] = os.w[`add${i}`].handler(async ({ ctx, input }: any) => {
    const id = await ctx.db.insert(table, { n: input.n });
    return await ctx.db.get(id);
  });
}

export { w };
