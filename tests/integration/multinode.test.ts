import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "./harness.js";

async function waitFor(pred: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await pred()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("waitFor: condition not met within timeout");
}
const settle = (ms = 500) => new Promise((r) => setTimeout(r, ms));

// Two independent engine processes against the SAME Postgres — a horizontally
// scaled deployment behind a load balancer, simulated.
let nodeA: Harness;
let nodeB: Harness;

beforeAll(async () => {
  nodeA = await startEngine();
  nodeB = await startEngine();
});
afterAll(async () => {
  await nodeA?.stop();
  await nodeB?.stop();
});
beforeEach(async () => {
  await nodeA.reset();
});

// Each test gets its OWN node-B subscriber client. A shared client dedups
// subscriptions by (path,input) and only unsubscribes server-side when the last
// listener for a key drops — so reusing one client across tests leaks a live
// subscription into the next test (an extra cross-node push). Fresh clients per
// test isolate that, mirroring precision.test.ts.
let subSeq = 0;
const subscriberB = () => nodeB.makeClient(`mn-sub-${++subSeq}`);

describe("horizontal scaling: cross-node reactivity", () => {
  // The headline: a subscriber on node B is invalidated by a write on node A,
  // via the Postgres change bus — only possible if invalidation crosses nodes.
  test("a write on node A pushes to a subscriber on node B", async () => {
    const updates: string[][] = [];
    const unsub = subscriberB().messages.list.subscribe(
      { input: { channelId: GENERAL_CHANNEL } },
      (data) => updates.push(data.map((m) => m.body)),
    );
    try {
      await waitFor(() => updates.length >= 1); // initial push from node B
      expect(updates[0]).toEqual([]);

      // Write through node A (a different process).
      await nodeA.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: "cross-node" });

      // Node B's subscriber must see it — proves the change crossed processes.
      await waitFor(() => updates.at(-1)?.includes("cross-node") ?? false);
    } finally {
      unsub();
    }
  });

  // Cross-node precision: a write to a foreign channel on node A must NOT re-run
  // a channel-A-only subscription on node B (the bus carries row images, so the
  // receiving node matches precisely — not a coarse "something changed").
  test("cross-node invalidation is precise (foreign channel does not re-run)", async () => {
    const A = GENERAL_CHANNEL;
    const B = "channels:00000000-0000-0000-0000-0000000000bb";
    let count = 0;
    const unsub = subscriberB().messages.list.subscribe({ input: { channelId: A } }, () => count++);
    try {
      await waitFor(() => count >= 1);
      const initial = count;
      // Write to channel B via node A.
      await nodeA.client.messages.send.call({ channelId: B, body: "into-B" });
      await settle();
      expect(count).toBe(initial); // channel-A sub on node B not re-run
      // ...but a write to channel A via node A does push to node B.
      await nodeA.client.messages.send.call({ channelId: A, body: "into-A" });
      await waitFor(() => count > initial);
    } finally {
      unsub();
    }
  });
});
