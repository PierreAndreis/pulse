import { describe, expect, it } from "vitest";
import { OfflineQueue } from "./queue.js";
import { InMemoryKV, type KVStore } from "./kv.js";

describe("OfflineQueue", () => {
  it("is durable across instances (survives a reload)", async () => {
    const kv = new InMemoryKV();
    const q1 = new OfflineQueue(kv);
    await q1.enqueue({ id: "m1", path: ["messages", "send"], input: { body: "x" } });
    await q1.enqueue({ id: "m2", path: ["messages", "send"], input: { body: "y" } });
    expect(await q1.size()).toBe(2);

    // simulate reload: a fresh queue over the same storage
    const q2 = new OfflineQueue(kv);
    const all = await q2.all();
    expect(all.map((m) => m.id)).toEqual(["m1", "m2"]); // FIFO order preserved

    await q2.remove("m1");
    expect(await q2.size()).toBe(1);

    const q3 = new OfflineQueue(kv);
    expect((await q3.all()).map((m) => m.id)).toEqual(["m2"]);
  });

  it("does not lose writes when enqueues race on a cold cache", async () => {
    const kv = new InMemoryKV();
    const q = new OfflineQueue(kv);
    // Fire concurrently BEFORE any load has warmed the cache. Each enqueue
    // awaits kv.get; without a shared load promise the second clobbers the first.
    await Promise.all([
      q.enqueue({ id: "a", path: ["p"], input: 1 }),
      q.enqueue({ id: "b", path: ["p"], input: 2 }),
      q.enqueue({ id: "c", path: ["p"], input: 3 }),
    ]);
    const ids = (await q.all()).map((m) => m.id).sort();
    expect(ids).toEqual(["a", "b", "c"]); // all three survive

    // …and the durable store agrees (not just the in-memory cache).
    const reloaded = new OfflineQueue(kv);
    expect((await reloaded.all()).map((m) => m.id).sort()).toEqual(["a", "b", "c"]);
  });

  it("an enqueue racing a remove keeps a consistent persisted state", async () => {
    const kv = new InMemoryKV();
    const seed = new OfflineQueue(kv);
    await seed.enqueue({ id: "x", path: ["p"], input: 0 });

    const q = new OfflineQueue(kv); // cold cache
    await Promise.all([q.enqueue({ id: "y", path: ["p"], input: 1 }), q.remove("x")]);

    const reloaded = new OfflineQueue(kv);
    const ids = (await reloaded.all()).map((m) => m.id);
    expect(ids).toContain("y"); // the new write is never lost
    expect(ids).not.toContain("x"); // the removal is honored
  });

  it("a rejected operation does not poison the serialized chain", async () => {
    // A KV whose first `set` throws (e.g. a transient quota/IO error), then recovers.
    const inner = new InMemoryKV();
    let failNextSet = true;
    const flaky: KVStore = {
      get: (k) => inner.get(k),
      set: (k, v) => {
        if (failNextSet) {
          failNextSet = false;
          return Promise.reject(new Error("disk full"));
        }
        return inner.set(k, v);
      },
      remove: (k) => inner.remove(k),
      keys: (p) => inner.keys(p),
    };
    const q = new OfflineQueue(flaky);

    // The first enqueue's persist() rejects and surfaces to the caller…
    await expect(q.enqueue({ id: "a", path: ["p"], input: 1 })).rejects.toThrow("disk full");
    // …but the chain is not wedged: the next operation still runs and persists.
    await q.enqueue({ id: "b", path: ["p"], input: 2 });

    const reloaded = new OfflineQueue(flaky);
    expect((await reloaded.all()).map((m) => m.id)).toContain("b"); // chain survived
  });
});
