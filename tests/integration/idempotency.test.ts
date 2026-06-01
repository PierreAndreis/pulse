import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "./harness.js";

// Exactly-once mutations, end-to-end through the real engine. Two browser tabs
// share one origin → one IndexedDB offline queue, and a write can be flushed
// twice (by both tabs, or by a retry after a lost ack). The engine dedupes by a
// stable client-supplied `mutationId` recorded in `_pulse_mutations` inside the
// mutation's own SERIALIZABLE transaction — so the write applies AT MOST ONCE.
let h: Harness;
beforeAll(async () => {
  h = await startEngine();
});
afterAll(async () => {
  await h?.stop();
});

const AUTH = { "content-type": "application/json", authorization: "Bearer test-token" };

async function rpc(
  path: string[],
  input: unknown,
  mutationId?: string,
): Promise<{ result?: unknown; error?: unknown }> {
  const res = await fetch(`${h.baseUrl}/rpc`, {
    method: "POST",
    headers: AUTH,
    body: JSON.stringify(mutationId ? { path, input, mutationId } : { path, input }),
  });
  return (await res.json()) as { result?: unknown; error?: unknown };
}

async function messageCount(): Promise<number> {
  const r = await rpc(["messages", "list"], { channelId: GENERAL_CHANNEL });
  return (r.result as unknown[]).length;
}

const uniqueId = (prefix: string) => `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`;

describe("exactly-once mutations (idempotency keys)", () => {
  test("the SAME mutationId applied twice creates exactly one row", async () => {
    const before = await messageCount();
    const mid = uniqueId("idem");

    const r1 = await rpc(["messages", "send"], { channelId: GENERAL_CHANNEL, body: "dedupe me" }, mid);
    const r2 = await rpc(["messages", "send"], { channelId: GENERAL_CHANNEL, body: "dedupe me" }, mid);

    expect(r1.error).toBeUndefined();
    expect(r2.error).toBeUndefined();
    expect(await messageCount()).toBe(before + 1); // applied once, not twice
    expect(r2.result).toEqual(r1.result); // the duplicate returns the recorded result
  });

  test("concurrent delivery of the same mutationId still applies once", async () => {
    const before = await messageCount();
    const mid = uniqueId("race");

    // Fire both at once — one wins the unique insert, the other blocks then dedupes.
    const [r1, r2] = await Promise.all([
      rpc(["messages", "send"], { channelId: GENERAL_CHANNEL, body: "race" }, mid),
      rpc(["messages", "send"], { channelId: GENERAL_CHANNEL, body: "race" }, mid),
    ]);

    expect(r1.error).toBeUndefined();
    expect(r2.error).toBeUndefined();
    expect(await messageCount()).toBe(before + 1);
  });

  test("distinct mutationIds each create a row (control — proves dedupe isn't over-eager)", async () => {
    const before = await messageCount();
    await rpc(["messages", "send"], { channelId: GENERAL_CHANNEL, body: "a" }, uniqueId("a"));
    await rpc(["messages", "send"], { channelId: GENERAL_CHANNEL, body: "b" }, uniqueId("b"));
    expect(await messageCount()).toBe(before + 2);
  });

  test("a mutation without a mutationId is unaffected (legacy path)", async () => {
    const before = await messageCount();
    await rpc(["messages", "send"], { channelId: GENERAL_CHANNEL, body: "no-key" });
    expect(await messageCount()).toBe(before + 1);
  });
});
