import { describe, expect, it, vi } from "vitest";

import { createGetLoader } from "./loader";

describe("createGetLoader", () => {
  it("collapses concurrent gets of one table into a single flush", async () => {
    const flush = vi.fn(async (_table: string, ids: string[]) =>
      ids.map((id) => ({ _id: id, n: id.length })),
    );
    const get = createGetLoader(flush);

    const [a, b, c] = await Promise.all([
      get("users", "users:1"),
      get("users", "users:22"),
      get("users", "users:333"),
    ]);

    // One round-trip carrying all three ids, in issue order.
    expect(flush).toHaveBeenCalledTimes(1);
    expect(flush.mock.calls[0]?.[1]).toEqual(["users:1", "users:22", "users:333"]);
    // Results distributed back to the matching caller.
    expect(a).toEqual({ _id: "users:1", n: 7 });
    expect(b).toEqual({ _id: "users:22", n: 8 });
    expect(c).toEqual({ _id: "users:333", n: 9 });
  });

  it("aligns results to id order and resolves a missing/short index to null", async () => {
    // flush returns the second row missing (null) and omits the third entirely.
    const flush = vi.fn(async () => [{ _id: "users:1" }, null]);
    const get = createGetLoader(flush);

    const [a, b, c] = await Promise.all([
      get("users", "users:1"),
      get("users", "users:gone"),
      get("users", "users:also-gone"),
    ]);

    expect(a).toEqual({ _id: "users:1" });
    expect(b).toBeNull();
    expect(c).toBeNull();
  });

  it("dedups duplicate ids within a batch (one fetch, both callers resolve)", async () => {
    const flush = vi.fn(async (_table: string, ids: string[]) => ids.map((id) => ({ _id: id })));
    const get = createGetLoader(flush);

    const [a, b, cDup] = await Promise.all([
      get("users", "users:1"),
      get("users", "users:2"),
      get("users", "users:1"),
    ]);

    // The duplicate id is sent once; both callers get the same row.
    expect(flush.mock.calls[0]?.[1]).toEqual(["users:1", "users:2"]);
    expect(a).toEqual({ _id: "users:1" });
    expect(cDup).toEqual({ _id: "users:1" });
    expect(b).toEqual({ _id: "users:2" });
  });

  it("separates batches by table", async () => {
    const flush = vi.fn(async (table: string, ids: string[]) =>
      ids.map((id) => ({ _id: id, table })),
    );
    const get = createGetLoader(flush);

    await Promise.all([
      get("users", "users:1"),
      get("posts", "posts:1"),
      get("users", "users:2"),
    ]);

    // One flush per distinct table.
    expect(flush).toHaveBeenCalledTimes(2);
    const byTable = Object.fromEntries(flush.mock.calls.map((c) => [c[0], c[1]]));
    expect(byTable.users).toEqual(["users:1", "users:2"]);
    expect(byTable.posts).toEqual(["posts:1"]);
  });

  it("starts a fresh batch on the next tick (sequential awaits don't batch together)", async () => {
    const flush = vi.fn(async (_table: string, ids: string[]) => ids.map((id) => ({ _id: id })));
    const get = createGetLoader(flush);

    await get("users", "users:1");
    await get("users", "users:2");

    // Each sequential await is its own tick → its own flush of size 1.
    expect(flush).toHaveBeenCalledTimes(2);
    expect(flush.mock.calls[0]?.[1]).toEqual(["users:1"]);
    expect(flush.mock.calls[1]?.[1]).toEqual(["users:2"]);
  });

  it("rejects every waiter in a batch when the flush fails", async () => {
    const boom = new Error("engine down");
    const flush = vi.fn(async () => {
      throw boom;
    });
    const get = createGetLoader(flush);

    const results = await Promise.allSettled([
      get("users", "users:1"),
      get("users", "users:2"),
    ]);

    expect(results).toEqual([
      { status: "rejected", reason: boom },
      { status: "rejected", reason: boom },
    ]);
  });
});
