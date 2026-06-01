import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "../integration/harness.js";
import { mapPool, range } from "./util.js";

let h: Harness;
beforeAll(async () => {
  h = await startEngine();
});
afterAll(async () => {
  await h?.stop();
});

describe("stress: sustained soak", () => {
  test(
    "runs continuous mixed traffic with zero errors for the soak window",
    async () => {
      const durationMs = Number(process.env.PULSE_SOAK_MS ?? 15_000);
      const deadline = Date.now() + durationMs;
      let ops = 0;
      let errors = 0;

      while (Date.now() < deadline) {
        const wave = await mapPool(range(20), 20, async (i) => {
          try {
            await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: `s-${i}` });
            await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
            return true;
          } catch {
            return false;
          }
        });
        ops += wave.length * 2;
        errors += wave.filter((ok) => !ok).length;
      }

      expect(errors).toBe(0);
      expect(ops).toBeGreaterThan(500);
    },
    120_000,
  );
});
