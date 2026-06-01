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

describe("actions (M6)", () => {
  // An action re-enters the engine: runs a mutation, then a query, and the
  // mutation's write is durably committed + visible.
  test("an action runs a mutation then a query and observes the result", async () => {
    const res = await h.client.actions.sendViaAction.call({
      channelId: GENERAL_CHANNEL,
      body: "from-action",
    });
    expect(res.listed).toBeGreaterThanOrEqual(1);

    // The mutation the action invoked actually committed (visible via the client).
    const list = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
    expect(list.map((m) => m.body)).toContain("from-action");
  });

  test("an unauthenticated action call is rejected (auth propagates)", async () => {
    const anon = h.makeClient(null);
    await expect(
      anon.actions.sendViaAction.call({ channelId: GENERAL_CHANNEL, body: "x" }),
    ).rejects.toMatchObject({ code: "UNAUTHORIZED" });
  });
});
