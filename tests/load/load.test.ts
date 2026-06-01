import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "../integration/harness.js";
import { fmt, runLoad, summarize, timed, type Stats } from "./metrics.js";

async function load(total: number, concurrency: number, task: (i: number) => Promise<unknown>): Promise<Stats> {
  const { latencies, errors, wallMs } = await runLoad(total, concurrency, task);
  return summarize(latencies, errors, wallMs);
}

// Bigger OLTP pool so parallelism/contention behavior is the engine's, not a
// 1-connection artifact.
let h: Harness;
beforeAll(async () => {
  h = await startEngine({ oltpMaxConns: 16 });
});
afterAll(async () => {
  await h?.stop();
});
beforeEach(async () => {
  await h.reset();
});

const A = GENERAL_CHANNEL;

describe("load: parallelization", () => {
  test(
    "read throughput rises with concurrency (reads run in parallel, not serialized)",
    async () => {
      await h.client.messages.send.call({ channelId: A, body: "seed" });
      const read = () => h.client.messages.list.call({ channelId: A });

      const serial = await load(300, 1, read);
      const parallel = await load(300, 16, read);

      // eslint-disable-next-line no-console
      console.log("\n" + fmt("reads c=1 ", serial) + "\n" + fmt("reads c=16", parallel));

      expect(serial.errors).toBe(0);
      expect(parallel.errors).toBe(0);
      // Concurrency must yield a real speedup; if reads were serialized in the
      // engine/worker, throughput would be flat. Require ≥2x (conservative).
      expect(parallel.opsPerSec).toBeGreaterThan(serial.opsPerSec * 2);
    },
  );
});

describe("load: head-of-line blocking", () => {
  test(
    "a slow query does not block fast reactive reads",
    async () => {
      await h.client.messages.send.call({ channelId: A, body: "seed" });

      // Baseline fast-read latency with nothing else running.
      const baseline = await load(50, 4, () => h.client.messages.list.call({ channelId: A }));

      // Fire a 2s slow query in the background, then hammer fast reads during it.
      const slow = h.makeClient("slow").messages.slow.call({ seconds: 2 });
      // Give the slow query a moment to be in-flight.
      await new Promise((r) => setTimeout(r, 100));
      const during = await load(50, 4, () => h.client.messages.list.call({ channelId: A }));
      const slept = await timed(() => slow);

      // eslint-disable-next-line no-console
      console.log(
        "\n" + fmt("fast reads baseline", baseline) + "\n" + fmt("fast reads w/ slow ", during) +
          `\nslow query took ${slept.ms.toFixed(0)}ms`,
      );

      expect(during.errors).toBe(0);
      // The slow query really did take ~2s (so it WAS in flight during the reads).
      expect(slept.ms).toBeGreaterThan(1500);
      // Fast reads stayed fast — p95 not dramatically inflated by the slow query.
      // Allow generous headroom but catch true serialization (which would push
      // p95 toward the 2s sleep).
      expect(during.p95).toBeLessThan(750);
    },
  );
});

describe("load: mutation contention", () => {
  test(
    "many concurrent mutations (one tx/conn each) don't deadlock and stay bounded",
    async () => {
      // 64 concurrent through a 16-connection pool → must queue, not deadlock.
      const s = await load(500, 64, (i) =>
        h.client.messages.send.call({ channelId: A, body: `load-${i}` }),
      );
      // eslint-disable-next-line no-console
      console.log("\n" + fmt("mutations c=64 pool=16", s));

      expect(s.errors).toBe(0); // no deadlock / no dropped writes
      expect(s.count).toBe(500);

      // Every write is durably present and correct (verified via the client).
      const list = await h.client.messages.list.call({ channelId: A });
      // list caps at 100; assert the count via a raw count query.
      const { count } = await h.client.messages.countRaw.call({ channelId: A });
      expect(count).toBe(500);
      expect(list.length).toBe(100);
    },
  );
});

describe("load: mixed workload", () => {
  test(
    "interleaved reads + writes sustain throughput with zero errors",
    async () => {
      await h.client.messages.send.call({ channelId: A, body: "seed" });
      const s = await load(600, 32, (i) =>
        i % 3 === 0
          ? h.client.messages.send.call({ channelId: A, body: `m-${i}` })
          : h.client.messages.list.call({ channelId: A }),
      );
      // eslint-disable-next-line no-console
      console.log("\n" + fmt("mixed r/w c=32", s));
      expect(s.errors).toBe(0);
      expect(s.p99).toBeLessThan(2000);
    },
  );
});
