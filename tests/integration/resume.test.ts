//! SSE resume (M5): a reconnect presents `Last-Event-ID`; the server replays
//! buffered events, or — when the gap can't be replayed (a relevant write while
//! disconnected evicted the client) — signals `resync` so the client re-subscribes
//! and converges. Driven through the raw SSE wire for deterministic disconnects.
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { GENERAL_CHANNEL, startEngine, type Harness } from "./harness.js";

async function waitFor(pred: () => boolean, timeoutMs = 6000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (pred()) return;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error("waitFor: condition not met within timeout");
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

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

type Frame = { id?: number; body: Record<string, unknown> };

/** Open a raw SSE stream for `clientId`, collecting decoded frames. */
function openStream(baseUrl: string, clientId: string, lastEventId?: number) {
  const ac = new AbortController();
  const frames: Frame[] = [];
  const headers: Record<string, string> = { accept: "text/event-stream", authorization: "Bearer t" };
  if (lastEventId !== undefined) headers["last-event-id"] = String(lastEventId);
  const done = (async () => {
    const res = await fetch(`${baseUrl}/sync?clientId=${clientId}`, { headers, signal: ac.signal });
    const reader = res.body!.getReader();
    const dec = new TextDecoder();
    let buf = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let i: number;
      while ((i = buf.indexOf("\n\n")) >= 0) {
        const raw = buf.slice(0, i);
        buf = buf.slice(i + 2);
        const idLine = raw.split("\n").find((l) => l.startsWith("id:"));
        const data = raw
          .split("\n")
          .filter((l) => l.startsWith("data:"))
          .map((l) => l.slice(5).trim())
          .join("\n");
        if (!data) continue;
        try {
          frames.push({ id: idLine ? Number(idLine.slice(3).trim()) : undefined, body: JSON.parse(data) });
        } catch {
          /* keep-alive */
        }
      }
    }
  })();
  return { ac, frames, done };
}

async function subscribe(baseUrl: string, clientId: string, sub: string): Promise<void> {
  await fetch(`${baseUrl}/subscribe`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer t" },
    body: JSON.stringify({ clientId, sub, path: ["messages", "list"], input: { channelId: GENERAL_CHANNEL } }),
  });
}

describe("SSE resume (M5)", () => {
  const SUB = `messages.list::${JSON.stringify({ channelId: GENERAL_CHANNEL })}`;

  test("a reconnect with Last-Event-ID resumes and converges after a missed write", async () => {
    const clientId = `resume-${Date.now()}`;

    // Connect, subscribe, receive the initial snapshot.
    const s1 = openStream(h.baseUrl, clientId);
    await subscribe(h.baseUrl, clientId, SUB);
    await waitFor(() => s1.frames.some((f) => f.body.sub === SUB));
    const lastId = s1.frames.at(-1)!.id!;
    expect(lastId).toBeGreaterThanOrEqual(1);

    // Disconnect; then a write lands while we're away.
    s1.ac.abort();
    await s1.done.catch(() => {});
    await sleep(150);
    await h.client.messages.send.call({ channelId: GENERAL_CHANNEL, body: "while-away" });
    await sleep(150);

    // Reconnect presenting Last-Event-ID. The server either replays the missed
    // push or asks us to resync — either way, after (re)subscribing we converge.
    const s2 = openStream(h.baseUrl, clientId, lastId);
    await waitFor(() => s2.frames.length >= 1);
    const resynced = s2.frames.some((f) => f.body.type === "resync");
    if (resynced) {
      // The client lost server-side state — re-subscribe (what the SDK does).
      await subscribe(h.baseUrl, clientId, SUB);
    }

    // The write made while disconnected is now reflected on the resumed stream.
    await waitFor(() => s2.frames.some((f) => JSON.stringify(f.body).includes("while-away")));
    s2.ac.abort();
    await s2.done.catch(() => {});
  });

  test("an unknown client presenting Last-Event-ID is told to resync", async () => {
    const s = openStream(h.baseUrl, `never-seen-${Date.now()}`, 99);
    try {
      await waitFor(() => s.frames.some((f) => f.body.type === "resync"));
    } finally {
      s.ac.abort();
      await s.done.catch(() => {});
    }
  });
});
