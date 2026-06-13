import { describe, expect, it, vi } from "vitest";
import { InMemoryKV } from "./kv.js";
import { LocalFirst } from "./localfirst.js";
import { queryKeyOf } from "./local.js";

const opts = { url: "http://x" };

describe("LocalFirst read-cache persistence (local-first)", () => {
  it("persists confirmed results and rehydrates them into a fresh instance", async () => {
    const kv = new InMemoryKV(); // shared storage = simulates a reload
    const key = queryKeyOf(["messages", "list"], { channelId: "c1" });

    // Session 1: a confirmed server result is recorded → persisted.
    const a = new LocalFirst(opts, kv);
    a.store.setConfirmed(key, [{ _id: "m1", body: "hello" }]);
    await Promise.resolve(); // let the async persist write settle
    await new Promise((r) => setTimeout(r, 0));

    // Session 2 (reload): a brand-new instance over the same storage.
    const b = new LocalFirst(opts, kv);
    let seen: unknown[] | undefined;
    b.store.subscribe(key, (docs) => (seen = docs));
    expect(seen).toBeUndefined(); // nothing in memory yet

    await b.hydrate(key); // rehydrate from persistence
    expect(seen).toEqual([{ _id: "m1", body: "hello" }]); // cached data shown — server not contacted
  });

  it("hydrate does not clobber fresher live data", async () => {
    const kv = new InMemoryKV();
    const key = queryKeyOf(["messages", "list"], { channelId: "c1" });
    await kv.set("pulse:cache:" + key, JSON.stringify([{ _id: "old" }]));

    const lf = new LocalFirst(opts, kv);
    lf.store.setConfirmed(key, [{ _id: "fresh" }]); // live push arrives first
    await lf.hydrate(key); // stale cache must NOT overwrite it
    expect(lf.store.getView(key)).toEqual([{ _id: "fresh" }]);
  });

  it("notifies queue listeners as writes enqueue and flush", async () => {
    const kv = new InMemoryKV();
    // fetch fails → write stays queued; then succeeds on manual flush.
    let online = false;
    const fetchMock = vi.fn(async () =>
      online
        ? new Response(JSON.stringify({ result: null }), { status: 200 })
        : Promise.reject(new TypeError("offline")),
    );
    const lf = new LocalFirst({ url: "http://x", fetch: fetchMock as unknown as typeof fetch }, kv);

    const sizes: number[] = [];
    lf.onQueueChange(() => void lf.queue.size().then((n) => sizes.push(n)));

    await lf.mutate(["messages", "send"], { body: "a" });
    await new Promise((r) => setTimeout(r, 10));
    expect(await lf.queue.size()).toBe(1); // queued offline

    online = true;
    await lf.flush();
    await new Promise((r) => setTimeout(r, 10));
    expect(await lf.queue.size()).toBe(0); // delivered
    expect(sizes.length).toBeGreaterThanOrEqual(2); // notified on enqueue + on drain
  });

  it("a concurrent flush does not double-send an in-flight mutation (re-entrancy guard)", async () => {
    const kv = new InMemoryKV();
    // A fetch we can hold in-flight, so the first flush stays parked on the send.
    const resolvers: Array<() => void> = [];
    const fetchMock = vi.fn(
      () =>
        new Promise<Response>((res) =>
          resolvers.push(() => res(new Response(JSON.stringify({ result: null }), { status: 200 }))),
        ),
    );
    const lf = new LocalFirst({ url: "http://x", fetch: fetchMock as unknown as typeof fetch }, kv);

    // mutate() enqueues then kicks off flush(); the send is now parked on the fetch.
    await lf.mutate(["messages", "send"], { body: "a" });
    await new Promise((r) => setTimeout(r, 0)); // let flush reach the in-flight send
    expect(fetchMock).toHaveBeenCalledTimes(1);

    // A second flush while the first is still draining must be a no-op: the queued
    // mutation is in-flight, not yet removed, so re-reading it would re-send it.
    await lf.flush();
    expect(fetchMock).toHaveBeenCalledTimes(1); // not double-sent

    // Complete the in-flight send; the mutation drains exactly once.
    resolvers.forEach((r) => r());
    await new Promise((r) => setTimeout(r, 10));
    expect(await lf.queue.size()).toBe(0);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
