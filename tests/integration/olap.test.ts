import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "./harness.js";

// OLTP statement timeout deliberately low, OLAP high: a 1s query on the reactive
// (OLTP) path must time out, while the same query on the analytical (OLAP) path
// succeeds — proving the analytical path uses a separate pool + timeout budget.
let h: Harness;
beforeAll(async () => {
  h = await startEngine({ oltpStatementTimeoutMs: 500, olapStatementTimeoutMs: 10_000 });
});
afterAll(async () => {
  await h?.stop();
});

describe("OLAP isolation (M6)", () => {
  test("a reactive query exceeding the OLTP statement timeout fails", async () => {
    await expect(h.client.messages.slow.call({ seconds: 1 })).rejects.toMatchObject({
      // Postgres cancels with SQLSTATE 57014 → engine maps to INTERNAL.
      code: "INTERNAL",
    });
  });

  test("an analytical query of the same duration succeeds on the OLAP pool", async () => {
    const res = await h.client.messages.slowAnalytical.call({ seconds: 1 });
    expect(res.slept).toBe(1);
  });

  test("normal analytical queries still work end-to-end", async () => {
    await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: "olap" });
    const { count } = await h.client.messages.countRaw.call({ channelId: GENERAL_CHANNEL });
    expect(count).toBeGreaterThanOrEqual(1);
  });
});
