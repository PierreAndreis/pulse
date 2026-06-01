import { describe, expect, it } from "vitest";
import { OfflineQueue } from "./queue.js";
import { InMemoryKV } from "./kv.js";

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
});
