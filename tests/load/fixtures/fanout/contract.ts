// One reactive count + one insert mutation per table t0..t{K-1}.
import { oc } from "@onveloz/pulse-contract";
import { v } from "@onveloz/pulse-schema";
import { K } from "./schema.js";

const w: Record<string, unknown> = {};
for (let i = 0; i < K; i++) {
  w[`count${i}`] = oc.reactive().output(v.number());
  w[`add${i}`] = oc.mutation().input(v.object({ n: v.number() })).output(v.any());
}

export const contract = { w } as { w: Record<string, never> };
