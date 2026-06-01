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

describe("RPC round-trip", () => {
  // Tracer bullet: proves the whole path (client → engine → worker → ctx.db → PG).
  test("a sent message is retrievable via list", async () => {
    const created = await h.client.messages.send.call({
      channelId: GENERAL_CHANNEL,
      body: "hello world",
    });
    expect(created.body).toBe("hello world");

    const list = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
    expect(list.map((m) => m.body)).toContain("hello world");
  });
});
