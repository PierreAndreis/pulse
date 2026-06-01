import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "./harness.js";

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

describe("analytical (raw ctx.sql)", () => {
  test("runs a raw SQL query over real data", async () => {
    for (const body of ["a", "b", "c"]) {
      await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body });
    }
    const res = await h.client.messages.summarize.call({ channelId: GENERAL_CHANNEL });
    expect(res.summary).toBe("Summary of 3 messages");
  });

  test("runs CTEs + GROUP BY + aggregates", async () => {
    for (const body of ["a", "b", "c", "d"]) {
      await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body });
    }
    const res = await h.client.messages.stats.call({ channelId: GENERAL_CHANNEL });
    expect(res.total).toBe(4);
    expect(res.distinctAuthors).toBe(1); // all from the seeded demo user
  });
});
