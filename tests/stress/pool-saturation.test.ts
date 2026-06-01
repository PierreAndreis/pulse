import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "../integration/harness.js";
import { mapPool, range } from "./util.js";

let h: Harness;
beforeAll(async () => {
  // Tiny OLTP pool so concurrent requests must queue for connections.
  h = await startEngine({ oltpMaxConns: 2 });
});
afterAll(async () => {
  await h?.stop();
});
beforeEach(async () => {
  await h.reset();
});

describe("stress: connection-pool saturation", () => {
  test(
    "services many concurrent requests through a 2-connection pool without deadlock",
    async () => {
      const N = 100;
      const results = await mapPool(range(N), 64, (i) =>
        h.client.messages.send
          .call({ channelId: GENERAL_CHANNEL, body: `p-${i}` })
          .then(() => true)
          .catch(() => false),
      );
      expect(results.filter(Boolean).length).toBe(N);
    },
    60_000,
  );
});
