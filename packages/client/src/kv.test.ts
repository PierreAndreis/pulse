import { describe, expect, it, beforeEach } from "vitest";
import "fake-indexeddb/auto";
import { IndexedDbKV, InMemoryKV, type KVStore } from "./kv.js";

// Run the same behavioral suite against both stores so the IndexedDB impl is
// held to exactly the same contract as the in-memory one. The IndexedDB bugs
// that prompted this (s.remove vs s.delete, this.map leak, write durability)
// are all caught by `remove` + the across-instances durability test.
function suite(name: string, make: (id: string) => KVStore) {
  describe(name, () => {
    let kv: KVStore;
    beforeEach(() => {
      kv = make("db-" + Math.random().toString(36).slice(2));
    });

    it("get returns null for a missing key", async () => {
      expect(await kv.get("nope")).toBeNull();
    });

    it("set then get round-trips", async () => {
      await kv.set("a", "1");
      expect(await kv.get("a")).toBe("1");
    });

    it("set overwrites", async () => {
      await kv.set("a", "1");
      await kv.set("a", "2");
      expect(await kv.get("a")).toBe("2");
    });

    it("remove deletes a key", async () => {
      await kv.set("a", "1");
      await kv.remove("a");
      expect(await kv.get("a")).toBeNull();
    });

    it("keys lists by prefix", async () => {
      await kv.set("pulse:q:1", "x");
      await kv.set("pulse:q:2", "y");
      await kv.set("other", "z");
      const keys = await kv.keys("pulse:q:");
      expect(keys.sort()).toEqual(["pulse:q:1", "pulse:q:2"]);
    });
  });
}

suite("InMemoryKV", () => new InMemoryKV());

// A fresh IndexedDbKV on the SAME db name sees previously-committed writes —
// this is the durability property local-first relies on (survives reload).
suite("IndexedDbKV", (id) => new IndexedDbKV(id, "kv"));

describe("IndexedDbKV recovers a pre-existing db missing the store", () => {
  // Regression: the real example app hit `NotFoundError: object store not found`
  // when the db already existed at v1 without our store (so onupgradeneeded never
  // fired). open() must detect the missing store and bump the version to create it.
  it("creates the store via a version bump when it is absent", async () => {
    const dbName = "preexisting-" + Math.random().toString(36).slice(2);
    // Create the db at v1 with a DIFFERENT store, so ours is missing.
    await new Promise<void>((resolve, reject) => {
      const r = indexedDB.open(dbName, 1);
      r.onupgradeneeded = () => r.result.createObjectStore("other");
      r.onsuccess = () => {
        r.result.close();
        resolve();
      };
      r.onerror = () => reject(r.error);
    });
    // The KV store ("kv") doesn't exist yet — this must self-heal, not throw.
    const kv = new IndexedDbKV(dbName, "kv");
    await kv.set("k", "v");
    expect(await kv.get("k")).toBe("v");
  });
});

describe("IndexedDbKV durability across instances", () => {
  it("a second instance on the same db sees committed writes", async () => {
    const dbName = "durable-" + Math.random().toString(36).slice(2);
    const a = new IndexedDbKV(dbName, "kv");
    await a.set("pulse:mutation-queue", JSON.stringify([{ id: "m1" }]));

    // Simulate a page reload: brand-new instance, same database.
    const b = new IndexedDbKV(dbName, "kv");
    expect(await b.get("pulse:mutation-queue")).toBe(JSON.stringify([{ id: "m1" }]));

    await b.remove("pulse:mutation-queue");
    const c = new IndexedDbKV(dbName, "kv");
    expect(await c.get("pulse:mutation-queue")).toBeNull();
  });
});
