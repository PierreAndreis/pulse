// Scaling curve: how cross-node bus traffic grows with cluster size N under
// interest routing vs broadcast. For each N, run N engines (each watching a
// distinct table) and total the /metrics busEvents across the cluster.
//
// Broadcast is O(N) per write (every node sees every change); routing is ~O(1)
// (only the interested node). The ratio should climb roughly linearly with N.
//
// Run: PULSE_TEST_DATABASE_URL=... PULSE_PG_CONTAINER=... \
//        npx vitest run --config vitest.load.config.ts tests/load/scaling.test.ts
// Override the curve with PULSE_SCALING_NS="2,4,8,16,32".
import { afterAll, describe, expect, test } from "vitest";
import { resolve } from "node:path";
import { createClient } from "@onveloz/pulse-client";
import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";
import { startEngine, applySql, type Harness } from "../integration/harness.js";
import { generateDDL } from "../../packages/cli/src/ddl.js";

const APP = resolve(process.cwd(), "tests/load/fixtures/fanout/app.ts");
const NS = (process.env.PULSE_SCALING_NS ?? "2,4,8,16").split(",").map((s) => parseInt(s.trim(), 10));
const MAX_N = Math.max(...NS);
const WRITES_PER_TABLE = 8;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function schemaFor(k: number): any {
  const tables: Record<string, ReturnType<typeof defineTable>> = {};
  for (let i = 0; i < k; i++) tables[`t${i}`] = defineTable({ n: v.number() });
  return defineSchema(tables);
}

async function startNode(k: number, broadcast: boolean): Promise<Harness> {
  const env = {
    PULSE_FANOUT_TABLES: String(k),
    PULSE_BUS_BROADCAST: broadcast ? "1" : "0",
    PULSE_OLAP_MAX_CONNS: "1",
  };
  for (let attempt = 0; ; attempt++) {
    try {
      return await startEngine({ app: APP, oltpMaxConns: 2, env });
    } catch (e) {
      if (attempt >= 3) throw e;
    }
  }
}

/** Total bus events the cluster received for K=N nodes/tables in the given mode. */
async function busTotal(n: number, broadcast: boolean): Promise<number> {
  const nodes = await Promise.all(Array.from({ length: n }, () => startNode(n, broadcast)));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const clients = nodes.map((h) => createClient({ url: h.baseUrl, headers: () => ({ authorization: "Bearer t" }) }) as any);
  const seen = nodes.map(() => 0);
  const unsubs = clients.map((c, i) => c.w[`count${i}`].subscribe({}, () => (seen[i] += 1)));
  try {
    for (let t = 0; t < 400 && !seen.every((s) => s >= 1); t++) await sleep(50);
    await sleep(750);
    for (let round = 0; round < WRITES_PER_TABLE; round++) {
      for (let i = 0; i < n; i++) await clients[0].w[`add${i}`].call({ n: round });
    }
    await sleep(1500 + n * 100);
    const per = await Promise.all(
      nodes.map(async (h) => ((await (await fetch(`${h.baseUrl}/metrics`)).json()) as { busEvents: number }).busEvents),
    );
    return per.reduce((a, b) => a + b, 0);
  } finally {
    for (const u of unsubs) u();
    await Promise.all(nodes.map((h) => h.stop()));
  }
}

describe("scaling curve: routed vs broadcast bus traffic across N", () => {
  test(
    "broadcast traffic grows ~O(N) while routing stays ~O(1) per write",
    async () => {
      const rows: { n: number; routed: number; broadcast: number; ratio: number }[] = [];
      for (const n of NS) {
        // Fresh schema for this N.
        await applySql(Array.from({ length: MAX_N }, (_, i) => `drop table if exists t${i}`).join("; "));
        await applySql(generateDDL(schemaFor(n)));
        const routed = await busTotal(n, false);
        const broadcast = await busTotal(n, true);
        rows.push({ n, routed, broadcast, ratio: broadcast / Math.max(1, routed) });
      }

      const writes = (n: number) => WRITES_PER_TABLE * n;
      const table = rows
        .map(
          (r) =>
            `  N=${String(r.n).padStart(2)}  routed=${String(r.routed).padStart(5)} (${(r.routed / writes(r.n)).toFixed(2)}/w)  ` +
            `broadcast=${String(r.broadcast).padStart(6)} (${(r.broadcast / writes(r.n)).toFixed(2)}/w)  ${r.ratio.toFixed(1)}x`,
        )
        .join("\n");
      // eslint-disable-next-line no-console
      console.log(`\nscaling curve (writes/table=${WRITES_PER_TABLE})\n${table}`);

      // Routing stays ~1 delivery/write regardless of N; broadcast ≈ N-1/write.
      for (const r of rows) {
        expect(r.routed).toBeLessThan(writes(r.n) * 2);
        expect(r.broadcast / writes(r.n)).toBeGreaterThan((r.n - 1) * 0.7);
      }
      // The savings ratio must grow with N (monotonically across the curve).
      for (let i = 1; i < rows.length; i++) {
        expect(rows[i]!.ratio).toBeGreaterThan(rows[i - 1]!.ratio);
      }
    },
    600_000,
  );
});

afterAll(async () => {
  await applySql(Array.from({ length: MAX_N }, (_, i) => `drop table if exists t${i}`).join("; "));
});
