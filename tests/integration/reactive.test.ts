import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "./harness.js";

async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("waitFor: condition not met within timeout");
}

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

describe("reactivity", () => {
  // Tracer bullet for M2: a write from one client is pushed to a separate
  // subscriber over SSE without a manual refetch.
  test("a send is pushed to a separate subscriber over SSE", async () => {
    const updates: string[][] = [];
    const unsub = h.client.messages.list.subscribe(
      { input: { channelId: GENERAL_CHANNEL } },
      (data) => {
        updates.push(data.map((m) => m.body));
      },
    );

    try {
      // initial result arrives (empty channel)
      await waitFor(() => updates.length >= 1);
      expect(updates[0]).toEqual([]);

      // a separate client writes
      const writer = h.makeClient("writer");
      await writer.messages.send.call({ channelId: GENERAL_CHANNEL, body: "live!" });

      // the subscriber is pushed an update containing the new message
      await waitFor(() => updates.at(-1)?.includes("live!") ?? false);
    } finally {
      unsub();
    }
  });

  test("fans out a write to multiple subscribed clients", async () => {
    const a: string[][] = [];
    const b: string[][] = [];
    const ca = h.makeClient("a");
    const cb = h.makeClient("b");
    const ua = ca.messages.list.subscribe({ input: { channelId: GENERAL_CHANNEL } }, (d) =>
      a.push(d.map((m) => m.body)),
    );
    const ub = cb.messages.list.subscribe({ input: { channelId: GENERAL_CHANNEL } }, (d) =>
      b.push(d.map((m) => m.body)),
    );
    try {
      await waitFor(() => a.length >= 1 && b.length >= 1);
      await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: "broadcast" });
      await waitFor(
        () => (a.at(-1)?.includes("broadcast") ?? false) && (b.at(-1)?.includes("broadcast") ?? false),
      );
    } finally {
      ua();
      ub();
    }
  });

  test("each subscriber only ever sees its own channel's data", async () => {
    const other = "channels:00000000-0000-0000-0000-0000000000ff";
    const gen: string[][] = [];
    const oth: string[][] = [];
    const ug = h.client.messages.list.subscribe({ input: { channelId: GENERAL_CHANNEL } }, (d) =>
      gen.push(d.map((m) => m.body)),
    );
    const uo = h.makeClient("o").messages.list.subscribe({ input: { channelId: other } }, (d) =>
      oth.push(d.map((m) => m.body)),
    );
    try {
      await waitFor(() => gen.length >= 1 && oth.length >= 1);
      await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: "to-general" });
      await waitFor(() => gen.at(-1)?.includes("to-general") ?? false);
      // Even though a messages write re-runs the other subscription (coarse,
      // table-level matching), its result never contains another channel's data.
      expect(oth.every((u) => !u.includes("to-general"))).toBe(true);
    } finally {
      ug();
      uo();
    }
  });

  test("unsubscribe stops further pushes", async () => {
    const updates: string[][] = [];
    const unsub = h.client.messages.list.subscribe(
      { input: { channelId: GENERAL_CHANNEL } },
      (d) => updates.push(d.map((m) => m.body)),
    );
    await waitFor(() => updates.length >= 1);
    unsub();
    // `unsub()` POSTs /unsubscribe fire-and-forget; let it land server-side
    // before writing, so we test "unsubscribe is effective" not a delivery race.
    await new Promise((r) => setTimeout(r, 300));
    const countAtUnsub = updates.length;
    await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: "after-unsub" });
    await new Promise((r) => setTimeout(r, 500));
    expect(updates.length).toBe(countAtUnsub);
  });
});
