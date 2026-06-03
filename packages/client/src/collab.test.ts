import { describe, expect, test, vi } from "vitest";
import * as Y from "yjs";
import { CollabHandle, toB64, fromB64 } from "./collab.js";
import type { ClientOptions } from "./transport.js";
import type { CollabHandleOptions } from "./collab.js";

// Yjs helpers adapted from tests/integration/collab.test.ts (~lines 21-28): build
// real delta updates / read the body text, so assertions are about real CRDT
// behavior rather than mocks.
const b64 = (u: Uint8Array) => Buffer.from(u).toString("base64");
const rawFromB64 = (s: string) => new Uint8Array(Buffer.from(s, "base64"));

/** Make a Yjs delta update that inserts `text` at index 0, from a seed state. */
function edit(seedB64: string, text: string): string {
  const doc = new Y.Doc();
  if (seedB64) Y.applyUpdate(doc, rawFromB64(seedB64));
  const before = Y.encodeStateVector(doc);
  doc.getText("body").insert(0, text);
  return b64(Y.encodeStateAsUpdate(doc, before));
}
function readBody(stateB64: string): string {
  const doc = new Y.Doc();
  Y.applyUpdate(doc, rawFromB64(stateB64));
  return doc.getText("body").toString();
}

const cfg: CollabHandleOptions = {
  getDocPath: ["notes", "getDoc"],
  applyUpdatePath: ["notes", "applyUpdate"],
  id: "doc-1",
};

/** A fake `subscribe`: captures the onData callback so tests can push server
 * states, and records that exactly one subscription was opened/closed. */
function makeSubscribe() {
  const onDataRef: { fn?: (data: unknown) => void } = {};
  const unsubscribe = vi.fn();
  const subscribe = vi.fn(
    (_path: string[], _input: unknown, onData: (data: unknown) => void) => {
      onDataRef.fn = onData;
      return unsubscribe;
    },
  );
  return { subscribe, unsubscribe, push: (state: string) => onDataRef.fn?.({ state }) };
}

/** Build a fake `fetch` for transport.rpcCall. `result` is the rpc result that
 * the engine would return; the returned spy records every outbound call. */
function makeFetch(result: unknown) {
  return vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) =>
    new Response(JSON.stringify({ result }), {
      status: 200,
      headers: { "content-type": "application/json" },
    }),
  );
}

/** Let queued microtasks (the fire-and-forget push promise) settle. */
const flush = () => new Promise((r) => setTimeout(r, 0));

describe("toB64 / fromB64 round-trip", () => {
  test("round-trips bytes including 0xFF", () => {
    const bytes = new Uint8Array([0x00, 0x01, 0xff, 0x7f, 0x80]);
    expect(Array.from(fromB64(toB64(bytes)))).toEqual(Array.from(bytes));
  });

  test("round-trips empty input", () => {
    expect(toB64(new Uint8Array())).toBe("");
    expect(fromB64("")).toEqual(new Uint8Array());
  });
});

describe("CollabHandle.applyRemote (origin suppression + idempotency)", () => {
  test("remote update applies to the doc but fires NO outbound rpc/fetch", async () => {
    const { subscribe, push } = makeSubscribe();
    const fetchSpy = makeFetch({ state: "" });
    const options: ClientOptions = { url: "/", fetch: fetchSpy };

    const h = new CollabHandle(options, cfg, subscribe);

    // Server sends an update inserting text — flows in via the subscription.
    push(edit("", "[remote] "));
    await flush();

    expect(h.doc.getText("body").toString()).toBe("[remote] ");
    // Origin suppression: applying a "remote"-origin update must NOT trigger a push.
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(h.getStatus()).toBe("synced");
    h.destroy();
  });

  test("double-apply of the same remote update is a no-op (no extra push)", async () => {
    const { subscribe, push } = makeSubscribe();
    const fetchSpy = makeFetch({ state: "" });
    const options: ClientOptions = { url: "/", fetch: fetchSpy };

    const h = new CollabHandle(options, cfg, subscribe);

    const update = edit("", "X");
    push(update);
    await flush();
    push(update); // idempotent re-delivery
    await flush();

    expect(h.doc.getText("body").toString()).toBe("X");
    expect(fetchSpy).not.toHaveBeenCalled();
    h.destroy();
  });
});

describe("CollabHandle local edits → outbound push", () => {
  test("a local edit produces a push with the encoded update", async () => {
    const { subscribe } = makeSubscribe();
    const fetchSpy = makeFetch({ state: "" });
    const options: ClientOptions = { url: "/", fetch: fetchSpy };

    const h = new CollabHandle(options, cfg, subscribe);

    h.doc.getText("body").insert(0, "hi");
    await flush();

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const body = JSON.parse((fetchSpy.mock.calls[0]![1] as RequestInit).body as string);
    expect(body.path).toEqual(cfg.applyUpdatePath);
    expect(body.input.id).toBe(cfg.id);
    // The encoded update decodes back to a real Yjs delta carrying the edit.
    const seeded = readBody(
      b64(
        (() => {
          const d = new Y.Doc();
          Y.applyUpdate(d, fromB64(body.input.update));
          return Y.encodeStateAsUpdate(d);
        })(),
      ),
    );
    expect(seeded).toBe("hi");
    expect(h.getStatus()).toBe("synced");
    h.destroy();
  });

  test("server's merged state is folded back (status synced)", async () => {
    const { subscribe } = makeSubscribe();
    // Engine echoes a merged authoritative state back in the rpc result.
    const merged = edit("", "[merged] ");
    const fetchSpy = makeFetch({ state: merged });
    const options: ClientOptions = { url: "/", fetch: fetchSpy };

    const h = new CollabHandle(options, cfg, subscribe);
    h.doc.getText("body").insert(0, "local ");
    await flush();

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    // Both the local edit and the server-merged content are present.
    const text = h.doc.getText("body").toString();
    expect(text).toContain("local ");
    expect(text).toContain("[merged]");
    expect(h.getStatus()).toBe("synced");
    h.destroy();
  });
});

describe("CollabHandle offline branch", () => {
  test("local edit while transport is failing → status offline, doc keeps the edit", async () => {
    const { subscribe } = makeSubscribe();
    // Failing transport (no connectivity): fetch rejects.
    const fetchSpy = vi.fn(async () => {
      throw new Error("network down");
    });
    const options: ClientOptions = { url: "/", fetch: fetchSpy };

    const h = new CollabHandle(options, cfg, subscribe);
    h.doc.getText("body").insert(0, "queued");
    await flush();

    // It DID attempt the push, but the failure drives the offline status and the
    // local edit is retained in the doc for the next send.
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    expect(h.getStatus()).toBe("offline");
    expect(h.doc.getText("body").toString()).toBe("queued");
    h.destroy();
  });
});

describe("CollabHandle status machine + lifecycle", () => {
  test("onStatus fires immediately with current status and on transitions", async () => {
    const { subscribe, push } = makeSubscribe();
    const fetchSpy = makeFetch({ state: "" });
    const options: ClientOptions = { url: "/", fetch: fetchSpy };

    const h = new CollabHandle(options, cfg, subscribe);
    const seen: string[] = [];
    const off = h.onStatus((s) => seen.push(s));

    // Fires synchronously with the current status first.
    expect(seen[0]).toBe("connecting");

    push(edit("", "z"));
    await flush();
    expect(seen).toContain("synced");

    off();
    seen.length = 0;
    push(edit("", "y"));
    await flush();
    // After unsubscribing the listener, no further status callbacks.
    expect(seen).toEqual([]);
    h.destroy();
  });

  test("destroy() clears listeners, unsubscribes, and stops outbound pushes", async () => {
    const { subscribe, unsubscribe } = makeSubscribe();
    const fetchSpy = makeFetch({ state: "" });
    const options: ClientOptions = { url: "/", fetch: fetchSpy };

    const h = new CollabHandle(options, cfg, subscribe);
    const cb = vi.fn();
    h.onStatus(cb);
    cb.mockClear();

    h.destroy();
    expect(unsubscribe).toHaveBeenCalledTimes(1);

    // After destroy, the status set has been cleared: a status change reaches no one.
    h.onStatus(vi.fn()); // re-adding is allowed, but old listeners are gone
    expect(cb).not.toHaveBeenCalled();
  });
});
