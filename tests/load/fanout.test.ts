// Multi-node fan-out benchmark: N engines on one Postgres, each subscribed to a
// DISTINCT table. Measures the cross-node "network layer" work — bus events each
// node receives (summed across the cluster) — under interest routing vs broadcast.
//
// Routing delivers a change only to the node(s) interested in the touched table
// (≈ O(1) per write); broadcast delivers every change to every node (O(N)). The
// gap widens with N — the payoff a 2-node bench can't show.
//
// Run: PULSE_TEST_DATABASE_URL=... PULSE_PG_CONTAINER=... \
//        npx vitest run --config vitest.load.config.ts tests/load/fanout.test.ts
import { afterAll, describe, expect, test } from "vitest";
import { resolve } from "node:path";
import { createClient } from "@onveloz/pulse-client";
import { startEngine, applySql, type Harness } from "../integration/harness.js";
import { generateDDL } from "../../packages/cli/src/ddl.js";
import fanoutSchema, { K } from "./fixtures/fanout/schema.js";

const APP = resolve(process.cwd(), "tests/load/fixtures/fanout/app.ts");
const N = K; // one node per table
const WRITES_PER_TABLE = 15;

async function waitFor(p: () => boolean, ms = 30_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (p()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("waitFor: timeout");
}

async function busEvents(baseUrl: string): Promise<number> {
  const r = await fetch(`${baseUrl}/metrics`);
  return ((await r.json()) as { busEvents: number }).busEvents;
}

interface PhaseResult {
  total: number;
  perNode: number[];
}

/** Start N nodes (each watching its own table), drive writes to every table from
 *  node 0, and total the bus events the cluster received. */
/** Start one engine, retrying a couple times — starting many in parallel can hit a
 *  transient random-port collision, which surfaces as an early exit. */
async function startNode(env: Record<string, string>): Promise<Harness> {
  let lastErr: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      return await startEngine({ app: APP, oltpMaxConns: 3, env });
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr;
}

async function runPhase(broadcast: boolean): Promise<PhaseResult> {
  const env = {
    PULSE_FANOUT_TABLES: String(K),
    PULSE_BUS_BROADCAST: broadcast ? "1" : "0",
    PULSE_OLAP_MAX_CONNS: "2",
  };
  const nodes: Harness[] = await Promise.all(Array.from({ length: N }, () => startNode(env)));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const clients = nodes.map((h) => createClient({ url: h.baseUrl, headers: () => ({ authorization: "Bearer t" }) }) as any);
  const seen = nodes.map(() => 0);
  const unsubs = clients.map((c, i) => c.w[`count${i}`].subscribe({}, () => (seen[i] += 1)));
  try {
    await waitFor(() => seen.every((s) => s >= 1)); // initial push ⇒ interest registered
    await new Promise((r) => setTimeout(r, 750)); // let interest settle on every node

    // All writes originate on node 0, touching each table in turn.
    for (let round = 0; round < WRITES_PER_TABLE; round++) {
      for (let i = 0; i < K; i++) {
        await clients[0].w[`add${i}`].call({ n: round });
      }
    }
    await new Promise((r) => setTimeout(r, 2000)); // let propagation quiesce

    const perNode = await Promise.all(nodes.map((h) => busEvents(h.baseUrl)));
    return { total: perNode.reduce((a, b) => a + b, 0), perNode };
  } finally {
    for (const u of unsubs) u();
    await Promise.all(nodes.map((h) => h.stop()));
  }
}

describe("fan-out: interest routing vs broadcast across N nodes", () => {
  test(
    "routing keeps cross-node bus traffic ≈O(1)/write while broadcast is O(N)",
    async () => {
      await applySql(generateDDL(fanoutSchema));

      const routed = await runPhase(false);
      const broadcast = await runPhase(true);
      const ratio = broadcast.total / Math.max(1, routed.total);
      const writes = WRITES_PER_TABLE * K;

      // eslint-disable-next-line no-console
      console.log(
        `\nfan-out  nodes=${N} tables=${K} writes=${writes}\n` +
          `  routed    bus events: ${routed.total}  (${(routed.total / writes).toFixed(2)}/write)\n` +
          `  broadcast bus events: ${broadcast.total}  (${(broadcast.total / writes).toFixed(2)}/write)\n` +
          `  routing cut cross-node traffic ${ratio.toFixed(1)}x`,
      );

      // Broadcast fans every write to all N-1 other nodes; routing to ~1 interested
      // node. With N nodes the gap should be large (well over 2x).
      expect(broadcast.total).toBeGreaterThan(routed.total * 2);
      // Routed traffic should be roughly one delivery per write (the single
      // interested node), not N — generous ceiling to stay robust.
      expect(routed.total).toBeLessThan(writes * 2);
    },
    300_000,
  );
});

afterAll(async () => {
  const drops = Array.from({ length: K }, (_, i) => `drop table if exists t${i}`).join("; ");
  await applySql(drops);
});
