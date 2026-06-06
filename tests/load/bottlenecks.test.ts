// Bottleneck atlas: a realistic "shop" workload (orders/lineitems/products/events)
// driven into distinct styles of slowness, each MEASURED, so we see where Pulse
// actually hurts — not just where the happy-path benchmarks shine. These are
// measurements: they print numbers and assert only loose sanity (no flaky gates).
//
//   A. heavy re-exec   — aggregate re-exec latency vs table size (no incremental view)
//   B. fan-out         — identical subs coalesce (1 re-exec) vs distinct subs (S re-execs)
//   C. N+1 join        — orderDetail re-exec grows linearly with item count
//   D. bulk write cap  — a big tx blows past the 8KB NOTIFY cap → coarse cross-node resync
//   E. write contention— same-row writes serialize; distinct-row writes scale
//
// Run: PULSE_TEST_DATABASE_URL=... PULSE_PG_CONTAINER=... \
//        npx vitest run --config vitest.load.config.ts tests/load/bottlenecks.test.ts
import { afterAll, beforeAll, beforeEach, describe, test, expect } from "vitest";
import { resolve } from "node:path";
import { createClient } from "@onveloz/pulse-client";
import { startEngine, applySql, type Harness } from "../integration/harness.js";
import { generateDDL } from "../../packages/cli/src/ddl.js";
import shopSchema from "./fixtures/shop/schema.js";
import { runLoad, summarize } from "./metrics.js";

const APP = resolve(process.cwd(), "tests/load/fixtures/shop/app.ts");
const TABLES = ["users", "products", "orders", "lineitems", "events"];
const UID = "users:00000000-0000-0000-0000-000000000001"; // userId is stored, never read
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const log = (s: string) => console.log(s); // eslint-disable-line no-console

async function waitFor(p: () => boolean, ms = 30_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (p()) return;
    await sleep(20);
  }
  throw new Error("waitFor: timeout");
}
const median = (xs: number[]): number => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)] ?? 0;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const mk = (hh: Harness): any => createClient({ url: hh.baseUrl, headers: () => ({ authorization: "Bearer t" }) });

/** Bulk-insert rows straight into Postgres — invisible to the reactor, so it only
 *  changes table SIZE (ideal for studying re-exec cost vs data volume). */
async function seedOrders(n: number): Promise<void> {
  if (n > 0)
    await applySql(
      `insert into orders (user_id, status, total)
       select gen_random_uuid(), (array['pending','paid','shipped'])[1+floor(random()*3)], (random()*50)::float
       from generate_series(1, ${n})`,
    );
}
async function seedEvents(n: number): Promise<void> {
  if (n > 0)
    await applySql(`insert into events (kind, note) select 'seed', 'x' from generate_series(1, ${n})`);
}

let h: Harness;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let c: any;

beforeAll(async () => {
  await applySql(generateDDL(shopSchema));
  h = await startEngine({ app: APP, oltpMaxConns: 16 });
  c = mk(h);
  await c.s.placeOrder.call({ userId: UID, items: 1 }); // warm the worker (first call is cold)
}, 120_000);
afterAll(async () => {
  await h?.stop();
  await applySql(TABLES.map((t) => `drop table if exists ${t}`).join("; "));
});
beforeEach(async () => {
  await applySql(`truncate ${TABLES.join(", ")}`);
});

describe("bottleneck atlas (realistic shop workload)", () => {
  test(
    "A. heavy re-exec — aggregate re-exec latency vs table size",
    async () => {
      const sizes = [0, 5_000, 25_000, 100_000];
      const rows: { n: number; ms: number }[] = [];
      for (const n of sizes) {
        await applySql(`truncate orders`);
        await seedOrders(n);
        const seen: number[] = [];
        let lastAt = 0;
        const unsub = c.s.revenue.subscribe({}, () => (seen.push(1), (lastAt = performance.now())));
        try {
          await waitFor(() => seen.length >= 1);
          const lat: number[] = [];
          for (let i = 0; i < 7; i++) {
            const before = seen.length;
            const t0 = performance.now();
            await c.s.placeOrder.call({ userId: UID, items: 1 });
            await waitFor(() => seen.length > before, 30_000);
            if (i > 0) lat.push(lastAt - t0); // drop the first (warm)
          }
          rows.push({ n, ms: median(lat) });
        } finally {
          unsub();
        }
      }
      log(
        "\nA. revenue = sum(total) over all orders; push latency per write (no incremental view):\n" +
          rows.map((r) => `   orders=${String(r.n).padStart(7)}  re-exec+push ${r.ms.toFixed(1)}ms`).join("\n"),
      );
      expect(rows.length).toBe(sizes.length);
    },
    240_000,
  );

  test(
    "B. fan-out — identical subs coalesce (1 re-exec), distinct subs each re-execute",
    async () => {
      await seedEvents(300); // every offset window non-empty and shifts on insert
      const S = 80;
      log("\nB. one write to a HOT table (events), fan-out to 80 subscribers:");

      // B1: identical subscriptions (same query, no input) → coalesced to 1 re-exec.
      {
        const seen = new Array(S).fill(0);
        const unsubs = Array.from({ length: S }, (_, i) => c.s.activityFeed.subscribe({}, () => (seen[i] += 1)));
        await waitFor(() => seen.every((x: number) => x >= 1));
        const t0 = performance.now();
        await c.s.logEvent.call({ kind: "x" });
        await waitFor(() => seen.every((x: number) => x >= 2), 60_000);
        log(`   identical (activityFeed): all updated in ${(performance.now() - t0).toFixed(1)}ms (1 coalesced re-exec)`);
        for (const u of unsubs) u();
      }
      // B2: distinct subscriptions (distinct offset) → S separate re-execs.
      {
        const seen = new Array(S).fill(0);
        const unsubs = Array.from({ length: S }, (_, i) => c.s.feed.subscribe({ input: { offset: i } }, () => (seen[i] += 1)));
        await waitFor(() => seen.every((x: number) => x >= 1));
        const t0 = performance.now();
        await c.s.logEvent.call({ kind: "y" });
        await waitFor(() => seen.every((x: number) => x >= 2), 60_000);
        log(`   distinct  (feed offset_i): all updated in ${(performance.now() - t0).toFixed(1)}ms (${S} re-execs)`);
        for (const u of unsubs) u();
      }
    },
    180_000,
  );

  test(
    "C. N+1 join — orderDetail re-exec grows linearly with item count",
    async () => {
      // Measured via direct calls: a reactive re-exec runs this exact handler, so
      // its cost IS the per-invalidation cost. (1 query + 1 get per item.)
      const counts = [1, 10, 50, 200];
      const rows: { items: number; ms: number }[] = [];
      for (const items of counts) {
        await applySql(`truncate orders, lineitems, events`);
        const order = (await c.s.placeOrder.call({ userId: UID, items })) as { _id: string };
        await c.s.orderDetail.call({ id: order._id }); // warm
        const lat: number[] = [];
        for (let i = 0; i < 6; i++) {
          const t0 = performance.now();
          await c.s.orderDetail.call({ id: order._id });
          lat.push(performance.now() - t0);
        }
        rows.push({ items, ms: median(lat) });
      }
      log(
        "\nC. orderDetail = order + items + get(product) per item; handler cost (paid on every re-exec):\n" +
          rows.map((r) => `   items=${String(r.items).padStart(4)}  re-exec+push ${r.ms.toFixed(1)}ms  (${(r.ms / r.items).toFixed(2)}ms/item)`).join("\n"),
      );
      expect(rows.at(-1)!.ms).toBeGreaterThan(rows[0]!.ms);
    },
    240_000,
  );

  test(
    "D. bulk write — a big tx blows past the 8KB NOTIFY cap → coarse cross-node resync",
    async () => {
      const b = await startEngine({ app: APP, oltpMaxConns: 8 });
      const cb = mk(b);
      const unsub = cb.s.recentOrders.subscribe({}, () => {}); // B is interested in `orders`
      await sleep(800); // interest registered on B
      try {
        const rows: { n: number; mode: string }[] = [];
        let prev = ((await (await fetch(`${b.baseUrl}/metrics`)).json()) as { resyncs: number }).resyncs;
        for (const n of [1, 3, 8, 20, 50, 120]) {
          await c.s.bulkOrders.call({ userId: UID, count: n });
          await sleep(900);
          const m = (await (await fetch(`${b.baseUrl}/metrics`)).json()) as { resyncs: number };
          const coarse = m.resyncs > prev;
          prev = m.resyncs;
          rows.push({ n, mode: coarse ? "COARSE resync (cap exceeded)" : "precise change-set" });
        }
        log(
          "\nD. cross-node delivery of a bulk write (rows per tx) — precise vs coarse:\n" +
            rows.map((r) => `   ${String(r.n).padStart(3)} orders/tx  →  ${r.mode}`).join("\n"),
        );
        expect(rows.some((r) => r.mode.startsWith("COARSE"))).toBe(true); // big enough tx must degrade
      } finally {
        unsub();
        await b.stop();
      }
    },
    180_000,
  );

  test(
    "E. write contention — same-row writes serialize; distinct-row writes scale",
    async () => {
      // Pre-create distinct orders to spread writes across.
      const ids: string[] = [];
      for (let i = 0; i < 64; i++) ids.push(((await c.s.placeOrder.call({ userId: UID, items: 1 })) as { _id: string })._id);

      const measure = async (pick: (i: number) => string) => {
        const { latencies, errors, wallMs } = await runLoad(300, 32, (i) =>
          c.s.setStatus.call({ id: pick(i), status: i % 2 ? "paid" : "shipped" }),
        );
        return summarize(latencies, errors, wallMs);
      };
      const hot = await measure(() => ids[0]!); // all hammer ONE row
      const spread = await measure((i) => ids[i % ids.length]!); // spread across 64 rows
      log(
        "\nE. setStatus at concurrency=32 (SERIALIZABLE):\n" +
          `   same row  : ${Math.round(hot.opsPerSec)} ops/s  p95=${hot.p95.toFixed(0)}ms  ${hot.errors} surfaced-conflicts\n` +
          `   64 rows   : ${Math.round(spread.opsPerSec)} ops/s  p95=${spread.p95.toFixed(0)}ms  ${spread.errors} surfaced-conflicts\n` +
          `   → spreading writes is ${(spread.opsPerSec / Math.max(1, hot.opsPerSec)).toFixed(1)}x faster`,
      );
      expect(spread.opsPerSec).toBeGreaterThan(hot.opsPerSec);
    },
    180_000,
  );
});
