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

describe("stress: worker backpressure", () => {
  // The single worker correlates many concurrent requests by requestId and each
  // request's db ops by opId. If correlation broke, a send would resolve to a
  // different request's document — so asserting each result matches its own
  // input is a strong correlation check under flood.
  test(
    "preserves request/response correlation under a flood",
    async () => {
      const bodies = range(200).map((i) => `corr-${i}`);
      const results = await mapPool(bodies, 64, (body) =>
        h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body }),
      );
      results.forEach((doc, i) => {
        expect(doc.body).toBe(bodies[i]);
      });
    },
    60_000,
  );
});
