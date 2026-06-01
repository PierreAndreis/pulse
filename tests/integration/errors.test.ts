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

describe("errors", () => {
  test("rejects input that violates the contract validator", async () => {
    await expect(
      // body must be a string; send a number.
      h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: 123 as unknown as string }),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  });

  test("rejects unauthenticated calls with UNAUTHORIZED", async () => {
    const anon = h.makeClient(null);
    await expect(
      anon.messages.send.call({ channelId: GENERAL_CHANNEL, body: "no auth" }),
    ).rejects.toMatchObject({ code: "UNAUTHORIZED" });
  });

  test("a declared error surfaces with its code and structured data", async () => {
    await expect(
      h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: "__rate_limit__" }),
    ).rejects.toMatchObject({ code: "RATE_LIMITED", data: { retryAfter: 5 } });
  });
});
