import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "./harness.js";

async function waitFor(pred: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await pred()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("waitFor: condition not met within timeout");
}
const settle = (ms = 400) => new Promise((r) => setTimeout(r, ms));

let h: Harness;
beforeAll(async () => {
  h = await startEngine();
});
afterAll(async () => {
  await h?.stop();
});
beforeEach(async () => {
  await h.reset();
});

const A = GENERAL_CHANNEL;
const B = "channels:00000000-0000-0000-0000-0000000000bb";
const C = "channels:00000000-0000-0000-0000-0000000000cc";

// Each test gets its own subscriber client so SSE streams / local stores never
// leak across tests (and to mirror real multi-client usage).
let subClientSeq = 0;
function subscriber() {
  return h.makeClient(`sub-${++subClientSeq}`);
}
function writer() {
  return h.makeClient(`w-${++subClientSeq}`);
}

describe("reactive precision (M3)", () => {
  // Slice 1 — tracer bullet.
  test("a write to channel B does not re-run a channel-A-only subscription", async () => {
    let count = 0;
    const unsub = subscriber().messages.list.subscribe({ input: { channelId: A } }, () => count++);
    try {
      await waitFor(() => count >= 1);
      const initial = count;
      await writer().messages.send.call({ channelId: B, body: "into-B" });
      await settle();
      expect(count).toBe(initial);
    } finally {
      unsub();
    }
  });

  test("a write to the subscribed channel still pushes", async () => {
    let count = 0;
    const unsub = subscriber().messages.list.subscribe({ input: { channelId: A } }, () => count++);
    try {
      await waitFor(() => count >= 1);
      const initial = count;
      await writer().messages.send.call({ channelId: A, body: "into-A" });
      await waitFor(() => count > initial);
    } finally {
      unsub();
    }
  });

  // Slice 2 — point-lookup precision.
  test("a get(id) subscription re-runs for that id but not a different id", async () => {
    const m1 = await writer().messages.send.call({ channelId: A, body: "one" });
    const m2 = await writer().messages.send.call({ channelId: A, body: "two" });
    let count = 0;
    const unsub = subscriber().messages.get.subscribe({ input: { id: m1._id } }, () => count++);
    try {
      await waitFor(() => count >= 1);
      const initial = count;
      await writer().messages.edit.call({ id: m2._id, body: "two!" });
      await settle();
      expect(count).toBe(initial); // editing m2 must not re-run get(m1)
      await writer().messages.edit.call({ id: m1._id, body: "one!" });
      await waitFor(() => count > initial); // editing m1 must re-run get(m1)
    } finally {
      unsub();
    }
  });

  // Slice 3 — patch moves a row across a filter (old-image invalidation).
  test("moving a message out of channel A re-runs the channel-A subscription", async () => {
    const m = await writer().messages.send.call({ channelId: A, body: "movable" });
    const seen: string[][] = [];
    const unsub = subscriber().messages.list.subscribe({ input: { channelId: A } }, (d) =>
      seen.push(d.map((x) => x.body)),
    );
    try {
      await waitFor(() => seen.length >= 1);
      expect(seen.at(-1)).toContain("movable");
      await writer().messages.move.call({ id: m._id, channelId: B });
      await waitFor(() => !(seen.at(-1)?.includes("movable") ?? true));
    } finally {
      unsub();
    }
  });

  test("moving a message into channel C re-runs the channel-C subscription", async () => {
    const m = await writer().messages.send.call({ channelId: A, body: "incoming" });
    const seen: string[][] = [];
    const unsub = subscriber().messages.list.subscribe({ input: { channelId: C } }, (d) =>
      seen.push(d.map((x) => x.body)),
    );
    try {
      await waitFor(() => seen.length >= 1);
      expect(seen.at(-1)).toEqual([]);
      await writer().messages.move.call({ id: m._id, channelId: C });
      await waitFor(() => seen.at(-1)?.includes("incoming") ?? false);
    } finally {
      unsub();
    }
  });

  // Slice 4 — delete invalidation via pre-image.
  test("deleting a message re-runs the channel subscription", async () => {
    const m = await writer().messages.send.call({ channelId: A, body: "doomed" });
    const seen: string[][] = [];
    const unsub = subscriber().messages.list.subscribe({ input: { channelId: A } }, (d) =>
      seen.push(d.map((x) => x.body)),
    );
    try {
      await waitFor(() => seen.at(-1)?.includes("doomed") ?? false);
      await writer().messages.remove.call({ id: m._id });
      await waitFor(() => !(seen.at(-1)?.includes("doomed") ?? true));
    } finally {
      unsub();
    }
  });

  // Slice 5 — range predicate precision.
  test("a range (since) subscription ignores writes before the cutoff", async () => {
    let count = 0;
    const unsub = subscriber().messages.listSince.subscribe(
      { input: { channelId: A, since: Date.now() + 10_000 } }, // cutoff far in the future
      () => count++,
    );
    try {
      await waitFor(() => count >= 1);
      const initial = count;
      await writer().messages.send.call({ channelId: A, body: "too-early" });
      await settle();
      expect(count).toBe(initial); // _creationTime < cutoff → no re-run
    } finally {
      unsub();
    }
  });

  // Slice 6 — full-scan fallback still fires (over-broad, never a miss).
  test("a full-scan (listAll) subscription is re-run by any channel's write", async () => {
    let count = 0;
    const unsub = subscriber().messages.listAll.subscribe({}, () => count++);
    try {
      await waitFor(() => count >= 1);
      const initial = count;
      await writer().messages.send.call({ channelId: B, body: "anywhere" });
      await waitFor(() => count > initial);
    } finally {
      unsub();
    }
  });

  // Slice 7 — raw-SQL-backed reactive query is invalidated (was a silent hole).
  test("a raw-SQL reactive query is invalidated by a relevant write", async () => {
    const seen: number[] = [];
    const unsub = subscriber().messages.countRaw.subscribe({ input: { channelId: A } }, (d) =>
      seen.push(d.count),
    );
    try {
      await waitFor(() => seen.length >= 1);
      const initial = seen.at(-1)!;
      await writer().messages.send.call({ channelId: A, body: "counted" });
      await waitFor(() => (seen.at(-1) ?? initial) > initial);
    } finally {
      unsub();
    }
  });

  // Slice 10 — SSE pushes carry a monotonic seq/id and a commitLsn.
  test("SSE pushes carry monotonic seq and a commitLsn", async () => {
    const clientId = `raw-sse-${++subClientSeq}`;
    const events: Array<{ seq: number; commitLsn: string; sub: string }> = [];
    const ac = new AbortController();
    // Tap the raw SSE stream directly to inspect the wire envelope.
    const streamDone = (async () => {
      const res = await fetch(`${h.baseUrl}/sync?clientId=${clientId}`, {
        headers: { accept: "text/event-stream", authorization: "Bearer t" },
        signal: ac.signal,
      });
      const reader = res.body!.getReader();
      const dec = new TextDecoder();
      let buf = "";
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let i: number;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const frame = buf.slice(0, i);
          buf = buf.slice(i + 2);
          const data = frame
            .split("\n")
            .filter((l) => l.startsWith("data:"))
            .map((l) => l.slice(5).trim())
            .join("\n");
          if (!data) continue;
          try {
            const p = JSON.parse(data);
            if (p.sub) events.push({ seq: p.seq, commitLsn: p.commitLsn, sub: p.sub });
          } catch {
            /* keepalive */
          }
        }
      }
    })();

    try {
      // Register a subscription for this raw client over /subscribe.
      const subKey = `messages.list::${JSON.stringify({ channelId: A })}`;
      await fetch(`${h.baseUrl}/subscribe`, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer t" },
        body: JSON.stringify({ clientId, sub: subKey, path: ["messages", "list"], input: { channelId: A } }),
      });
      await waitFor(() => events.length >= 1); // initial push
      await writer().messages.send.call({ channelId: A, body: "tick" });
      await waitFor(() => events.length >= 2); // invalidation push

      expect(events[0]!.seq).toBe(1);
      expect(events[1]!.seq).toBe(2); // monotonic per client
      expect(typeof events[1]!.commitLsn).toBe("string"); // X/Y form
      expect(events[1]!.commitLsn).toMatch(/^[0-9A-F]+\/[0-9A-F]+$/);
    } finally {
      ac.abort();
      await streamDone.catch(() => {});
    }
  });

  // Slice 8 — no redundant push when the recomputed value is unchanged.
  test("an edit that does not change the result produces no push", async () => {
    const m = await writer().messages.send.call({ channelId: A, body: "same" });
    let count = 0;
    const unsub = subscriber().messages.list.subscribe({ input: { channelId: A } }, () => count++);
    try {
      await waitFor(() => count >= 1);
      const initial = count;
      // Patch the body to its CURRENT value — the change matches the read-set,
      // so the sub is re-run, but the list result is byte-identical → no push.
      await writer().messages.edit.call({ id: m._id, body: "same" });
      await settle();
      expect(count).toBe(initial);
    } finally {
      unsub();
    }
  });
});
