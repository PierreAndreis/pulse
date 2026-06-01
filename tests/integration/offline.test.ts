import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { createClient, InMemoryKV, type PulseClient } from "@pulse/client";
import type { contract } from "../../packages/examples-chat/src/contract.js";
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

/** A client whose connectivity we can toggle, backed by a chosen KV store. */
function makeFlakyClient(kv: InMemoryKV, net: { online: boolean }): PulseClient<typeof contract> {
  const realFetch = globalThis.fetch;
  return createClient<typeof contract>({
    url: h.baseUrl,
    headers: () => ({ authorization: "Bearer test" }),
    persistence: kv,
    fetch: ((input: RequestInfo | URL, init?: RequestInit) =>
      net.online
        ? realFetch(input, init)
        : Promise.reject(new Error("offline"))) as typeof fetch,
  });
}

async function waitFor(pred: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await pred()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("waitFor: condition not met within timeout");
}

describe("offline / local-first", () => {
  test("queues a mutation while offline and delivers it on reconnect (no lost writes)", async () => {
    const net = { online: false };
    const c = makeFlakyClient(new InMemoryKV(), net);

    await c.messages.send.mutate({ channelId: GENERAL_CHANNEL, body: "offline-msg" });
    expect(await c.$local.queue.size()).toBe(1);

    // Nothing reached the server yet.
    const before = await h.client.messages.list.call({ channelId: GENERAL_CHANNEL });
    expect(before.map((m) => m.body)).not.toContain("offline-msg");

    // Reconnect and drain the queue.
    net.online = true;
    await c.$local.flush();

    await waitFor(async () =>
      (await h.client.messages.list.call({ channelId: GENERAL_CHANNEL })).some(
        (m) => m.body === "offline-msg",
      ),
    );
    expect(await c.$local.queue.size()).toBe(0);
  });

  test("a reload (new client, same storage) replays the queued mutation", async () => {
    const kv = new InMemoryKV();
    const c1 = makeFlakyClient(kv, { online: false });
    await c1.messages.send.mutate({ channelId: GENERAL_CHANNEL, body: "survives-reload" });
    expect(await c1.$local.queue.size()).toBe(1);

    // "Reload": a brand-new client over the same storage, now online.
    const c2 = makeFlakyClient(kv, { online: true });
    await c2.$local.flush();

    await waitFor(async () =>
      (await h.client.messages.list.call({ channelId: GENERAL_CHANNEL })).some(
        (m) => m.body === "survives-reload",
      ),
    );
  });

  test("optimistic update is visible immediately, before the server confirms", async () => {
    // Stay offline so the optimistic state is all there is to see.
    const c = makeFlakyClient(new InMemoryKV(), { online: false });
    const views: string[][] = [];
    const unsub = c.messages.list.subscribe(
      { input: { channelId: GENERAL_CHANNEL } },
      (d) => views.push(d.map((m) => m.body)),
    );
    try {
      await c.messages.send.mutate(
        { channelId: GENERAL_CHANNEL, body: "optimistic!" },
        {
          optimistic: (store) => {
            const cur = store.getQuery<{ _id: string; body: string }>(
              ["messages", "list"],
              { channelId: GENERAL_CHANNEL },
            );
            store.setQuery(["messages", "list"], { channelId: GENERAL_CHANNEL }, [
              { _id: store.tempId("messages"), body: "optimistic!" },
              ...cur,
            ]);
          },
        },
      );
      await waitFor(() => views.at(-1)?.includes("optimistic!") ?? false);
    } finally {
      unsub();
    }
  });
});
