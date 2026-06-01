import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "../integration/harness.js";
import { mapPool, range } from "./util.js";

let h: Harness;
beforeAll(async () => {
  h = await startEngine();
});
afterAll(async () => {
  await h?.stop();
});
beforeEach(async () => {
  await h.reset();
});

describe("stress: concurrent RPC load", () => {
  test(
    "no dropped or duplicated writes under concurrency",
    async () => {
      // Stay under the list cap (100) so correctness is verifiable via the client.
      const bodies = range(90).map((i) => `m-${i}`);
      await mapPool(bodies, 32, (body) =>
        h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body }),
      );
      const list = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
      expect(list.length).toBe(90);
      expect(new Set(list.map((m) => m.body))).toEqual(new Set(bodies));
    },
    60_000,
  );

  test(
    "sustains high concurrent volume with zero errors",
    async () => {
      const N = 1000;
      const start = Date.now();
      const results = await mapPool(range(N), 64, (i) =>
        h.client.messages.send
          .call({ channelId: GENERAL_CHANNEL, body: `v-${i}` })
          .then(() => true)
          .catch(() => false),
      );
      expect(results.filter(Boolean).length).toBe(N);
      expect(Date.now() - start).toBeLessThan(30_000);
    },
    60_000,
  );
});
