// Cross-container (multi-node) benchmark: two independent pulse-server engines
// share one Postgres. A write on node A must invalidate + push to a subscriber
// on node B via the LISTEN/NOTIFY change bus (crates/pulse-cdc). We measure:
//   1. cross-node push latency (write on A → SSE push on B), and
//   2. server memory at scale (RSS per live subscription on B).
//
// Run: PULSE_TEST_DATABASE_URL=... PULSE_PG_CONTAINER=... \
//        npx vitest run --config vitest.load.config.ts tests/load/replication.test.ts
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";
import { createClient, type Client } from "@onveloz/pulse-client";
import { startEngine, applySql, type Harness } from "../integration/harness.js";
import { generateDDL } from "../../packages/cli/src/ddl.js";
import ormSchema from "../integration/fixtures/orm/schema.js";
import type { contract } from "../integration/fixtures/orm/contract.js";
import { summarize, fmt, runLoad } from "./metrics.js";

const execFileAsync = promisify(execFile);
const APP = resolve(process.cwd(), "tests/integration/fixtures/orm/app.ts");

let a: Harness; // node that receives writes
let b: Harness; // node whose subscribers must be invalidated cross-bus
let ca: Client<typeof contract>;
let cb: Client<typeof contract>;

const mkClient = (h: Harness): Client<typeof contract> =>
  createClient<typeof contract>({ url: h.baseUrl, headers: () => ({ authorization: "Bearer test" }) });

/** Resident set size of a pid in KB (macOS/Linux `ps`). */
async function rssKb(pid: number | undefined): Promise<number> {
  if (!pid) return 0;
  const { stdout } = await execFileAsync("ps", ["-o", "rss=", "-p", String(pid)]);
  return parseInt(stdout.trim(), 10) || 0;
}

async function waitFor(predicate: () => boolean, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error("waitFor: timeout");
}

beforeAll(async () => {
  await applySql(generateDDL(ormSchema));
  await applySql("truncate widgets, authors");
  // Two engines, same DATABASE_URL → two nodes on one Postgres, distinct node_ids.
  a = await startEngine({ app: APP, oltpMaxConns: 16 });
  b = await startEngine({ app: APP, oltpMaxConns: 16 });
  ca = mkClient(a);
  cb = mkClient(b);
}, 120_000);

afterAll(async () => {
  await a?.stop();
  await b?.stop();
  await applySql("drop table if exists widgets; drop table if exists authors");
});

describe("replication: cross-node push latency (LISTEN/NOTIFY bus)", () => {
  test(
    "a write on node A pushes to a subscriber on node B",
    async () => {
      // Subscribe on B; collect the active-widget count it sees.
      const seen: number[] = [];
      const unsub = cb.w.activeCount.subscribe({}, (n) => seen.push(n as number));
      try {
        await waitFor(() => seen.length >= 1);
        expect(seen.at(-1)).toBe(0); // empty table

        // Fire N writes on A, each must cross the bus and bump B's count.
        const N = 50;
        const latencies: number[] = [];
        for (let i = 1; i <= N; i++) {
          const target = i;
          const t0 = performance.now();
          await ca.w.addWidget.call({ name: `w${i}`, qty: 1, active: true });
          await waitFor(() => seen.at(-1) === target, 10_000);
          latencies.push(performance.now() - t0);
        }
        const s = summarize(latencies, 0, latencies.reduce((x, y) => x + y, 0));
        // eslint-disable-next-line no-console
        console.log("\n" + fmt("cross-node write→push (A→bus→B)", s));

        expect(seen.at(-1)).toBe(N); // every cross-node write landed
        // The write itself + a Postgres NOTIFY round-trip + re-exec on B. Loose
        // ceiling — this catches a broken bus (which would time out), not a perf
        // regression target.
        expect(s.p95).toBeLessThan(1500);
      } finally {
        unsub();
      }
    },
    120_000,
  );

  test(
    "cross-node replication recovers after a Postgres restart (listener reconnect + resync)",
    async () => {
      const seen: number[] = [];
      const unsub = cb.w.activeCount.subscribe({}, (n) => seen.push(n as number));
      try {
        await waitFor(() => seen.length >= 1);
        const base = seen.at(-1)!; // current active count (prior tests left rows)

        // Baseline: a write on A crosses the bus to B.
        await ca.w.addWidget.call({ name: "pre-restart", qty: 1, active: true });
        await waitFor(() => seen.at(-1) === base + 1, 10_000);

        // Bounce Postgres — every node's listener connection drops.
        await execFileAsync("docker", ["restart", process.env.PULSE_PG_CONTAINER ?? "pulse-prune-test"]);
        for (let i = 0; i < 60; i++) {
          try {
            await execFileAsync("docker", ["exec", process.env.PULSE_PG_CONTAINER ?? "pulse-prune-test", "pg_isready", "-U", "pulse", "-d", "pulse"]);
            break;
          } catch {
            await new Promise((r) => setTimeout(r, 500));
          }
        }
        // Give the engines' pools + bus listeners time to reconnect (500ms backoff).
        await new Promise((r) => setTimeout(r, 2500));

        // A write on A must again reach B — proving the listener reconnected and the
        // bus is live. Retry the write itself: A's pool may need a reconnect too.
        let wrote = false;
        for (let i = 0; i < 10 && !wrote; i++) {
          try {
            await ca.w.addWidget.call({ name: "post-restart", qty: 1, active: true });
            wrote = true;
          } catch {
            await new Promise((r) => setTimeout(r, 500));
          }
        }
        expect(wrote).toBe(true);
        await waitFor(() => (seen.at(-1) ?? 0) >= base + 2, 20_000);
      } finally {
        unsub();
      }
    },
    120_000,
  );
});

describe("replication: write throughput with the bus active", () => {
  test(
    "sustained concurrent writes on A keep flowing and reach B",
    async () => {
      // A subscriber on B so every write also fans out cross-node (publish + NOTIFY
      // + remote re-exec are all exercised, not just the local commit path).
      let lastSeen = 0;
      const unsub = cb.w.activeCount.subscribe({}, (n) => {
        lastSeen = n as number;
      });
      try {
        await waitFor(() => lastSeen >= 0);
        const base = lastSeen;

        const W = 400;
        const { latencies, errors, wallMs } = await runLoad(W, 32, (i) =>
          ca.w.addWidget.call({ name: `t${i}`, qty: 1, active: true }),
        );
        const s = summarize(latencies, errors, wallMs);
        // eslint-disable-next-line no-console
        console.log("\n" + fmt("writes on A (c=32, bus on)", s));

        expect(s.errors).toBe(0);
        // All writes durably landed and propagated to B's cross-node count.
        await waitFor(() => lastSeen >= base + W, 30_000);
      } finally {
        unsub();
      }
    },
    120_000,
  );
});

describe("replication: server memory at scale", () => {
  test(
    "RSS per live subscription on node B",
    async () => {
      // Seed a non-trivial result so each subscription retains a real `last` value.
      for (let i = 0; i < 100; i++) {
        await ca.w.addWidget.call({ name: `seed${i}`, qty: i, active: i % 2 === 0 });
      }
      // Let the existing activeCount sub settle, then measure a clean baseline.
      await new Promise((r) => setTimeout(r, 500));
      const before = await rssKb(b.pid);

      const S = 500;
      let delivered = 0;
      const unsubs: Array<() => void> = [];
      // Distinct inputs → distinct read-sets/results, a realistic memory spread.
      // Open in small batches so 500 initial executions don't stampede B's single
      // worker / SSE buffer all at once.
      for (let i = 0; i < S; i++) {
        unsubs.push(
          cb.w.page.subscribe({ input: { limit: 20, offset: i % 50 } }, () => {
            delivered++;
          }),
        );
        if (i % 50 === 49) await new Promise((r) => setTimeout(r, 50));
      }
      try {
        await waitFor(() => delivered >= S, 100_000);
      } catch {
        // eslint-disable-next-line no-console
        console.log(`\n[mem] only ${delivered}/${S} initial pushes delivered before timeout`);
        throw new Error(`only ${delivered}/${S} subs delivered`);
      }
      await new Promise((r) => setTimeout(r, 1000)); // let allocations settle
      const after = await rssKb(b.pid);

      const perSubKb = (after - before) / S;
      // eslint-disable-next-line no-console
      console.log(
        `\nmemory: ${S} subs on node B | RSS ${before}KB → ${after}KB ` +
          `(+${after - before}KB, ${perSubKb.toFixed(2)} KB/sub)`,
      );

      for (const u of unsubs) u();
      // Sanity: subscriptions cost memory but stay bounded (no per-sub MB leak).
      expect(after).toBeGreaterThan(before);
      expect(perSubKb).toBeLessThan(64); // 64 KB/sub ceiling — catches gross bloat
    },
    120_000,
  );
});
