import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { applySql, GENERAL_CHANNEL, startEngine, type Harness } from "./harness.js";

async function waitFor(predicate: () => boolean, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("waitFor: condition not met within timeout");
}

// Raw uuids behind the seeded `table:uuid` ids.
const GENERAL_UUID = GENERAL_CHANNEL.split(":")[1]!;
const DEMO_UUID = "00000000-0000-0000-0000-000000000010";

let h: Harness;
beforeAll(async () => {
  // PULSE_WAL=1 turns on the logical-slot consumer (the DB has wal_level=logical).
  h = await startEngine({ env: { PULSE_WAL: "1", PULSE_WAL_POLL_MS: "25" } });
});
afterAll(async () => {
  await h?.stop();
  // Drop the slot so it doesn't pin WAL for the rest of the suite (no consumer
  // now that the engine is stopped).
  await applySql(
    "SELECT pg_drop_replication_slot('pulse_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pulse_slot')",
  ).catch(() => {});
});
beforeEach(async () => {
  await h.reset();
});

describe("WAL/CDC: out-of-band writes invalidate subscriptions", () => {
  test("a raw INSERT made outside the engine pushes to a live subscriber", async () => {
    const updates: string[][] = [];
    const unsub = h.client.messages.list.subscribe(
      { input: { channelId: GENERAL_CHANNEL } },
      (data) => updates.push(data.map((m) => m.body)),
    );
    try {
      // Initial (empty) snapshot.
      await waitFor(() => updates.length >= 1);
      expect(updates[0]).toEqual([]);

      // A write the engine never sees — straight SQL into Postgres. Only the WAL
      // consumer can surface this; engine write-set capture is blind to it.
      await applySql(
        `INSERT INTO messages (_id, channel_id, author_id, body) ` +
          `VALUES (gen_random_uuid(), '${GENERAL_UUID}', '${DEMO_UUID}', 'out-of-band-wal')`,
      );

      // The subscription re-runs and the row appears — driven entirely by the WAL.
      await waitFor(() => updates.at(-1)?.includes("out-of-band-wal") ?? false);
    } finally {
      unsub();
    }
  });
});
