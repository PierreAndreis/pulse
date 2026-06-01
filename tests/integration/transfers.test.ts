import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { startEngine, type Harness } from "./harness.js";

// S2 (CI-safe) — money-transfer deadlock + conservation. `accounts.transfer` is
// the ONLY multi-row write path: it debits one row and credits another in one
// SERIALIZABLE mutation. Concurrent opposing transfers (A→B and B→A) acquire the
// two rows in opposing order → real Postgres deadlock (40P01), which the retry
// loop must absorb. The killer invariants are CONSERVATION (money sum is exact)
// and the ASYMMETRIC net (defeats a "both halves lost symmetrically" alibi).
let h: Harness;
beforeAll(async () => {
  h = await startEngine();
});
afterAll(async () => {
  await h?.stop();
});

async function balances(a: string, b: string): Promise<[number, number]> {
  const [ra, rb] = await Promise.all([
    h.client.accounts.get.call({ id: a }),
    h.client.accounts.get.call({ id: b }),
  ]);
  return [ra.balance, rb.balance];
}

describe("S2: money-transfer deadlock + conservation", () => {
  test("opposing concurrent transfers deadlock-retry, conserve, and net correctly", async () => {
    await h.reset();
    const A = await h.client.accounts.create.call({ name: "A", balance: 1000 });
    const B = await h.client.accounts.create.call({ name: "B", balance: 1000 });
    // DB-presence guard.
    expect((await balances(A, B))[0]).toBe(1000);

    const ab = h.makeClient("ab");
    const ba = h.makeClient("ba");

    // Symmetric phase: N A→B interleaved with N B→A. Opposing lock order on the
    // two rows provokes 40P01; the engine's retry must absorb every one. N is
    // bounded for the CI tier — a deadlock on 2 rows costs up to `deadlock_timeout`
    // (1s) to *detect*, so a large storm serializes; the PULSE_HEAVY tier scales
    // this up. Conservation (below) is the invariant that must hold at any N.
    const N = 10;
    const symmetric = [
      ...Array.from({ length: N }, () => ab.accounts.transfer.call({ from: A, to: B, amount: 1 })),
      ...Array.from({ length: N }, () => ba.accounts.transfer.call({ from: B, to: A, amount: 1 })),
    ];
    const results = await Promise.allSettled(symmetric);
    const rejected = results
      .filter((r): r is PromiseRejectedResult => r.status === "rejected")
      .map((r) => String(r.reason?.message ?? r.reason));
    expect(rejected).toEqual([]); // no deadlock hang, no surfaced 409

    // Conservation + net-zero: the symmetric phase moves money back and forth.
    const [a1, b1] = await balances(A, B);
    expect(a1 + b1).toBe(2000); // money neither created nor destroyed
    expect(a1).toBe(1000);
    expect(b1).toBe(1000);

    // Asymmetric phase: 15 A→B and 5 B→A → net 10 leaves A. A symmetric-only
    // loss would satisfy conservation but FAIL this exact net.
    const asymmetric = [
      ...Array.from({ length: 15 }, () => ab.accounts.transfer.call({ from: A, to: B, amount: 1 })),
      ...Array.from({ length: 5 }, () => ba.accounts.transfer.call({ from: B, to: A, amount: 1 })),
    ];
    expect((await Promise.allSettled(asymmetric)).every((r) => r.status === "fulfilled")).toBe(true);

    const [a2, b2] = await balances(A, B);
    expect(a2 + b2).toBe(2000); // still conserved
    expect(a2).toBe(990); // 1000 − net 10 out
    expect(b2).toBe(1010);

    // Ledger reconciliation: one row per committed transfer (10+10+15+5 = 40).
    const { count } = await h.client.transfers.countRaw.call({});
    expect(count).toBe(40);
  }, 90_000);

  test("an overdraw is rejected and rolls back (no partial debit)", async () => {
    await h.reset();
    const A = await h.client.accounts.create.call({ name: "A", balance: 5 });
    const B = await h.client.accounts.create.call({ name: "B", balance: 0 });

    await expect(h.client.accounts.transfer.call({ from: A, to: B, amount: 999 })).rejects.toThrow();

    // Atomic rollback: balances unchanged, no ledger row, money conserved.
    const [a, b] = await balances(A, B);
    expect(a).toBe(5);
    expect(b).toBe(0);
    expect((await h.client.transfers.countRaw.call({})).count).toBe(0);
  }, 30_000);
});
