// Fan-out fixture: K independent tables (t0..t{K-1}), so each node can hold
// interest in a DISTINCT table — the setup that shows interest routing (one
// interested node per change) beating broadcast (every node) as K/N grow.
// K is read from PULSE_FANOUT_TABLES so the benchmark and the engines agree.
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";

export const K = Number(process.env.PULSE_FANOUT_TABLES ?? 8);

const tables: Record<string, ReturnType<typeof defineTable>> = {};
for (let i = 0; i < K; i++) {
  tables[`t${i}`] = defineTable({ n: v.number() });
}

export default defineSchema(tables);
