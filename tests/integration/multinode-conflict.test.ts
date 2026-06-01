import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { startEngine, type Harness } from "./harness.js";

// S1 (CI-safe) — cross-node hot-row increment convergence.
//
// Two engine PROCESSES share one Postgres. K increments to a SINGLE counter row
// are fired alternating between the two nodes. `counters.increment` is the
// classic read-modify-write (get value → patch value+1), so concurrent calls
// MUST collide under SERIALIZABLE; the 25-attempt retry has to absorb every
// cross-node 40001 and converge the row to EXACTLY K. The decisive assertion is
// equality (=== K), which catches a silent lost update (< K) AND a double-commit
// over-count (> K) — the project's historical "0 errors but ledger wrong" trap.
let a: Harness;
let b: Harness;
beforeAll(async () => {
  a = await startEngine();
  b = await startEngine();
});
afterAll(async () => {
  await a?.stop();
  await b?.stop();
});

describe("S1: cross-node hot-row increment convergence", () => {
  test("K alternating-node increments converge to exactly K (no lost update)", async () => {
    await a.reset();
    // Step-0 guard: DB reachable AND reset really happened (fail loud, not skip).
    const id = await a.client.counters.create.call({ name: "xnode" });
    expect(await a.client.counters.get.call({ id })).toBe(0);

    const ca = a.makeClient("inc-a");
    const cb = b.makeClient("inc-b");
    const K = 40;
    let usedA = 0;
    let usedB = 0;

    const calls = Array.from({ length: K }, (_, i) => {
      const onA = i % 2 === 0;
      if (onA) usedA++;
      else usedB++;
      return (onA ? ca : cb).counters.increment.call({ id });
    });
    const results = await Promise.allSettled(calls);

    // 1. No rejections — SERIALIZABLE retry absorbed every cross-node 40001.
    const rejected = results
      .filter((r): r is PromiseRejectedResult => r.status === "rejected")
      .map((r) => String(r.reason?.message ?? r.reason));
    expect(rejected).toEqual([]);

    // 2. No lost update — BOTH nodes agree the value is EXACTLY K (shared-PG truth,
    //    no per-process drift).
    expect(await a.client.counters.get.call({ id })).toBe(K);
    expect(await b.client.counters.get.call({ id })).toBe(K);

    // 3. Work really crossed the process boundary — a green run can't come from
    //    every call hitting one node.
    expect(usedA).toBeGreaterThan(0);
    expect(usedB).toBeGreaterThan(0);
  }, 90_000);
});
