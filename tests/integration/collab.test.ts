import { afterAll, beforeAll, describe, expect, test } from "vitest";
import * as Y from "yjs";
import { startEngine, type Harness } from "./harness.js";

// The headline collaborative guarantee, end-to-end through the real engine:
// two clients edit the SAME document while one is OFFLINE (the doc changes
// underneath it), then it reconnects — and BOTH edits survive, with both
// clients converging to identical state. No clobber, no lost edits.
let h: Harness;
beforeAll(async () => {
  h = await startEngine();
});
afterAll(async () => {
  await h?.stop();
});

const b64 = (u: Uint8Array) => Buffer.from(u).toString("base64");
const fromB64 = (s: string) => new Uint8Array(Buffer.from(s, "base64"));

/** Make a Yjs delta update that inserts `text` at index 0, from a seed state. */
function edit(seedB64: string, text: string): string {
  const doc = new Y.Doc();
  if (seedB64) Y.applyUpdate(doc, fromB64(seedB64));
  const before = Y.encodeStateVector(doc);
  doc.getText("body").insert(0, text);
  return b64(Y.encodeStateAsUpdate(doc, before));
}
function readBody(stateB64: string): string {
  const doc = new Y.Doc();
  Y.applyUpdate(doc, fromB64(stateB64));
  return doc.getText("body").toString();
}

describe("collaborative documents (Yjs CRDT)", () => {
  test("offline edit + concurrent server change → both survive, converge identical", async () => {
    const c = h.client;
    const id = await c.notes.create.call({ title: "Long page" });

    // Both clients start from the same (empty) seed.
    const seed = (await c.notes.getDoc.call({ id })).state;

    // Client A goes OFFLINE and edits locally (not yet sent).
    const offlineUpdate = edit(seed, "[A offline edit] ");

    // Meanwhile client B (online) edits the same doc — the server doc moves on.
    await c.notes.applyUpdate.call({ id, update: edit(seed, "[B online edit] ") });

    // Client A reconnects: its offline update is delivered onto the changed doc.
    const merged = (await c.notes.applyUpdate.call({ id, update: offlineUpdate })).state;

    const body = readBody(merged);
    // The decisive assertion: BOTH edits are present (anti-false-positive: we
    // require both substrings, never just one).
    expect(body).toContain("[A offline edit]");
    expect(body).toContain("[B online edit]");

    // And any client reading the doc converges to the exact same state.
    const reread = readBody((await c.notes.getDoc.call({ id })).state);
    expect(reread).toBe(body);
  });

  test("applying the same update twice is idempotent (retry-safe)", async () => {
    const c = h.client;
    const id = await c.notes.create.call({ title: "Idem" });
    const seed = (await c.notes.getDoc.call({ id })).state;
    const u = edit(seed, "once");

    await c.notes.applyUpdate.call({ id, update: u });
    const after = (await c.notes.applyUpdate.call({ id, update: u })).state; // duplicate
    expect(readBody(after)).toBe("once"); // not "onceonce"
  });
});
