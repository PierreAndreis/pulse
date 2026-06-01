import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { PulseClientError } from "@onveloz/pulse-client";
import { isPulseConflictError, startEngine, type Harness } from "./harness.js";

// S4 (CI-safe) — retries exhaust into a CLEAN 409 CONFLICT, never an INTERNAL/500
// or a timeout, and the row still equals EXACTLY the number that committed.
//
// `maxTxAttempts: 2` forces retry exhaustion deterministically at low concurrency
// (no flaky 200+ load needed). `oltpStatementTimeoutMs: 0` removes the statement
// timeout, so any non-409 failure is unambiguously a real bug, not a slow query.
let h: Harness;
beforeAll(async () => {
  h = await startEngine({ maxTxAttempts: 2, oltpMaxConns: 16, oltpStatementTimeoutMs: 0 });
});
afterAll(async () => {
  await h?.stop();
});

describe("S4: retry exhaustion → clean CONFLICT, no lost update", () => {
  test("contended increments exhaust into 409s; final value == committed exactly", async () => {
    await h.reset();
    const id = await h.client.counters.create.call({ name: "hot" });
    expect(await h.client.counters.get.call({ id })).toBe(0); // DB-presence guard

    const N = 40;
    const clients = Array.from({ length: N }, (_, i) => h.makeClient(`c-${i}`));
    const results = await Promise.allSettled(clients.map((c) => c.counters.increment.call({ id })));

    const fulfilled = results.filter((r) => r.status === "fulfilled").length;
    const rejected = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");

    // 1. Exhaustion actually happened — otherwise this is just the happy path.
    expect(rejected.length).toBeGreaterThan(0);

    // 2. Every rejection is a clean CONFLICT / HTTP 409 — never INTERNAL, never a
    //    timeout string. (statement timeout is disabled, so 409 is the only species.)
    for (const r of rejected) {
      expect(isPulseConflictError(r.reason)).toBe(true);
      expect((r.reason as PulseClientError).httpStatus).toBe(409);
    }

    // 3. No-op guard: at least one writer won.
    expect(fulfilled).toBeGreaterThan(0);

    // 4. The lost-update trap: final value EXACTLY equals the committed count —
    //    not `>=` (catches double-commit), not `<=` (catches silent lost update).
    expect(await h.client.counters.get.call({ id })).toBe(fulfilled);
  }, 90_000);
});
