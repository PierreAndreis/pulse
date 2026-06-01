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

describe("messages", () => {
  test("an empty channel lists no messages", async () => {
    const list = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
    expect(list).toEqual([]);
  });

  test("lists every message sent to a channel", async () => {
    for (const body of ["one", "two", "three"]) {
      await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body });
    }
    const list = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
    expect(list.map((m) => m.body).sort()).toEqual(["one", "three", "two"]);
  });

  test("preserves a large body verbatim", async () => {
    const body = "x".repeat(50_000);
    await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body });
    const list = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
    expect(list[0]?.body).toBe(body);
    expect(list[0]?.body.length).toBe(50_000);
  });

  test("preserves unicode verbatim", async () => {
    const body = "héllo 🌍 世界 — ∑∆";
    await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body });
    const list = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
    expect(list[0]?.body).toBe(body);
  });

  test("encodes ids as table:uuid and round-trips the channel id", async () => {
    const created = await h.client.messages.send.call({
      channelId: GENERAL_CHANNEL,
      body: "id check",
    });
    expect(created._id).toMatch(/^messages:[0-9a-f-]{36}$/);
    expect(created.channelId).toBe(GENERAL_CHANNEL);

    // The returned channel id is usable for a follow-up query.
    const list = await h.client.messages.list.call({ channelId: created.channelId });
    expect(list.map((m) => m._id)).toContain(created._id);
  });
});
