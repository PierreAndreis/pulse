// What does the cross-node machinery cost WRITES? Same workload, two nodes (A
// writes, B subscribes so publish does real work), measured under interest routing
// vs broadcast on the same machine — the delta is routing's write-path overhead.
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { resolve } from "node:path";
import { createClient, type Client } from "@onveloz/pulse-client";
import { startEngine, applySql, type Harness } from "../integration/harness.js";
import { generateDDL } from "../../packages/cli/src/ddl.js";
import ormSchema from "../integration/fixtures/orm/schema.js";
import type { contract } from "../integration/fixtures/orm/contract.js";
import { runLoad, summarize, fmt } from "./metrics.js";

const APP = resolve(process.cwd(), "tests/integration/fixtures/orm/app.ts");
const WRITES = 400;
const CONCURRENCY = 32;

beforeAll(async () => {
  await applySql(generateDDL(ormSchema));
}, 120_000);
afterAll(async () => {
  await applySql("drop table if exists widgets; drop table if exists authors");
});
beforeEach(async () => {
  await applySql("truncate widgets, authors");
});

async function measure(broadcast: boolean) {
  const env = { PULSE_BUS_BROADCAST: broadcast ? "1" : "0" };
  const a = await startEngine({ app: APP, oltpMaxConns: 16, env });
  const b = await startEngine({ app: APP, oltpMaxConns: 16, env });
  const ca = createClient<typeof contract>({ url: a.baseUrl, headers: () => ({ authorization: "Bearer t" }) });
  const cb = createClient<typeof contract>({ url: b.baseUrl, headers: () => ({ authorization: "Bearer t" }) });
  // A subscriber on B makes every write actually publish + route across the bus.
  const unsub = cb.w.activeCount.subscribe({}, () => {});
  await new Promise((r) => setTimeout(r, 600)); // let interest register
  try {
    // Warm up, then measure.
    await runLoad(50, CONCURRENCY, (i) => ca.w.addWidget.call({ name: `warm${i}`, qty: 1, active: true }));
    const { latencies, errors, wallMs } = await runLoad(WRITES, CONCURRENCY, (i) =>
      ca.w.addWidget.call({ name: `w${i}`, qty: 1, active: true }),
    );
    return summarize(latencies, errors, wallMs);
  } finally {
    unsub();
    await a.stop();
    await b.stop();
  }
}

describe("write cost: routing vs broadcast (same machine)", () => {
  test(
    "writes throughput under routed vs broadcast",
    async () => {
      const routed = await measure(false);
      await applySql("truncate widgets, authors");
      const broadcast = await measure(true);

      const lossPct = ((broadcast.opsPerSec - routed.opsPerSec) / broadcast.opsPerSec) * 100;
      // eslint-disable-next-line no-console
      console.log(
        `\nwrite cost (2 nodes, ${WRITES} writes c=${CONCURRENCY})\n` +
          "  " + fmt("routed   ", routed) + "\n" +
          "  " + fmt("broadcast", broadcast) + "\n" +
          `  routing costs ${lossPct.toFixed(1)}% write throughput vs broadcast` +
          ` (${Math.round(broadcast.opsPerSec)} → ${Math.round(routed.opsPerSec)} ops/s)`,
      );
      expect(routed.errors).toBe(0);
      expect(broadcast.errors).toBe(0);
    },
    180_000,
  );
});
