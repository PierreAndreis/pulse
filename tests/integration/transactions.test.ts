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

describe("mutation transactions (M4)", () => {
  // A mutation that writes then throws must be all-or-nothing.
  test("a mutation that fails after writing rolls back the write", async () => {
    await expect(
      h.client.messages.sendThenFail.call({ channelId: GENERAL_CHANNEL, body: "ghost" }),
    ).rejects.toMatchObject({ code: "INTERNAL" });

    const list = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
    expect(list.map((m) => m.body)).not.toContain("ghost"); // rolled back
  });

  // A successful mutation still commits.
  test("a mutation that succeeds commits its write", async () => {
    await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: "kept" });
    const list = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
    expect(list.map((m) => m.body)).toContain("kept");
  });

  // SERIALIZABLE + retry: concurrent read-modify-write increments of one row
  // must not lose updates — the final value equals the number of increments.
  test("concurrent read-modify-write increments serialize without lost updates", async () => {
    const id = await h.client.counters.create.call({ name: "hits" });
    const N = 50;
    const results = await Promise.allSettled(
      Array.from({ length: N }, () => h.makeClient("inc").counters.increment.call({ id })),
    );
    const ok = results.filter((r) => r.status === "fulfilled").length;
    expect(ok).toBe(N); // none dropped (retries absorb conflicts)

    const value = await h.client.counters.get.call({ id });
    expect(value).toBe(N); // no lost updates
  }, 60_000);
});
