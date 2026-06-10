import { afterEach, describe, expect, test, vi } from "vitest";
import { SyncClient, type SyncStatus } from "./sync.js";
import type { ClientOptions } from "./transport.js";
import type { LocalStore } from "./local.js";

// These tests exercise the PURE logic of the sync engine — SSE frame parsing,
// subscription dedupe/bookkeeping, URL joining, and the status FSM — with
// in-memory fakes (a store that captures setConfirmed, an injected fetch). No
// live socket, no real network. Private methods are reached via bracket access
// so the source file stays untouched.

/** A LocalStore stand-in that only records the confirmed writes sync makes. */
function fakeStore(): { store: LocalStore; calls: Array<[string, unknown[]]> } {
  const calls: Array<[string, unknown[]]> = [];
  const store = {
    setConfirmed(key: string, docs: unknown[]) {
      calls.push([key, docs]);
    },
  } as unknown as LocalStore;
  return { store, calls };
}

/** A reject-on-every-call fetch, so runStream throws and never opens a socket. */
function deadFetch(): ClientOptions["fetch"] {
  return vi.fn(async () => {
    throw new Error("no network");
  }) as unknown as ClientOptions["fetch"];
}

function makeClient(opts: Partial<ClientOptions> = {}) {
  const { store, calls } = fakeStore();
  const options: ClientOptions = { url: "/", fetch: deadFetch(), ...opts };
  const client = new SyncClient(options, store) as unknown as {
    handleEvent(raw: string): void;
    joinUrl(p: string): string;
    connected: boolean;
    registered: Map<string, unknown>;
    ensure(path: string[], input: unknown): void;
    release(path: string[], input: unknown): void;
    onStatus(cb: (s: SyncStatus) => void): () => void;
    setStatus(s: SyncStatus): void;
    lastEventId?: number;
    serverHasSub: Set<string>;
  };
  return { client, store, calls, options };
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("handleEvent / SSE frame parsing", () => {
  test("a well-formed data frame writes confirmed state via the store", () => {
    const { client, calls } = makeClient();
    client.handleEvent(`data: ${JSON.stringify({ sub: "k1", data: [{ _id: "m1" }] })}`);
    expect(calls).toEqual([["k1", [{ _id: "m1" }]]]);
  });

  test("multi-line data frames are joined before JSON.parse", () => {
    const { client, calls } = makeClient();
    const payload = JSON.stringify({ sub: "multi", data: [1, 2] });
    const half = Math.floor(payload.length / 2);
    // SSE allows a payload split across several `data:` lines; they re-join with \n.
    const raw = `data: ${payload.slice(0, half)}\ndata: ${payload.slice(half)}`;
    client.handleEvent(raw);
    expect(calls).toEqual([["multi", [1, 2]]]);
  });

  test("keepalive / empty frames are ignored (no store write)", () => {
    const { client, calls } = makeClient();
    client.handleEvent(": keep-alive comment\n");
    client.handleEvent("");
    client.handleEvent("event: ping");
    expect(calls).toEqual([]);
  });

  test("malformed JSON in a data frame is tolerated (no throw, no write)", () => {
    const { client, calls } = makeClient();
    expect(() => client.handleEvent("data: {not json")).not.toThrow();
    expect(calls).toEqual([]);
  });

  test("missing `data` defaults to [] via the `?? []` path", () => {
    const { client, calls } = makeClient();
    client.handleEvent(`data: ${JSON.stringify({ sub: "empty" })}`);
    expect(calls).toEqual([["empty", []]]);
  });

  test("a falsy `sub` skips the store write", () => {
    const { client, calls } = makeClient();
    client.handleEvent(`data: ${JSON.stringify({ sub: "", data: [9] })}`);
    expect(calls).toEqual([]);
  });
});

describe("joinUrl helper", () => {
  test("base without trailing slash joins a bare path", () => {
    const { client } = makeClient({ url: "https://api.example.com" });
    expect(client.joinUrl("subscribe")).toBe("https://api.example.com/subscribe");
  });

  test("base with trailing slash does not double the separator", () => {
    const { client } = makeClient({ url: "https://api.example.com/" });
    expect(client.joinUrl("subscribe")).toBe("https://api.example.com/subscribe");
  });

  test("root base produces a single leading slash", () => {
    const { client } = makeClient({ url: "/" });
    expect(client.joinUrl("sync?clientId=x")).toBe("/sync?clientId=x");
  });
});

describe("ensure() / release() subscription dedupe", () => {
  test("ensuring the same key twice registers once and issues one control POST", async () => {
    // Fake timers so the reconnect loop's backoff sleep never runs live.
    vi.useFakeTimers();
    const fetchMock = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => {
      throw new Error("no network");
    });
    const { client, calls: _calls } = makeClient({
      fetch: fetchMock as unknown as ClientOptions["fetch"],
    });
    // Pretend the stream is already open so ensure() registers immediately.
    client.connected = true;

    client.ensure(["messages", "list"], { channelId: "c1" });
    client.ensure(["messages", "list"], { channelId: "c1" });

    expect(client.registered.size).toBe(1);
    await vi.advanceTimersByTimeAsync(0);

    const subscribeCalls = fetchMock.mock.calls.filter((c) =>
      String(c[0]).endsWith("/subscribe"),
    );
    expect(subscribeCalls).toHaveLength(1);
    const body = JSON.parse((subscribeCalls[0]![1] as RequestInit).body as string);
    expect(body).toMatchObject({
      sub: "messages.list::{\"channelId\":\"c1\"}",
      path: ["messages", "list"],
      input: { channelId: "c1" },
    });
  });

  test("release deletes bookkeeping and POSTs unsubscribe", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => {
      throw new Error("no network");
    });
    const { client } = makeClient({ fetch: fetchMock as unknown as ClientOptions["fetch"] });
    client.connected = true;

    client.ensure(["messages", "list"], null);
    expect(client.registered.size).toBe(1);

    client.release(["messages", "list"], null);
    expect(client.registered.size).toBe(0);

    await vi.advanceTimersByTimeAsync(0);
    const unsub = fetchMock.mock.calls.filter((c) => String(c[0]).endsWith("/unsubscribe"));
    expect(unsub).toHaveLength(1);
    expect(JSON.parse((unsub[0]![1] as RequestInit).body as string)).toMatchObject({
      sub: "messages.list::null",
    });
  });

  test("releasing an unknown key is a no-op (no unsubscribe POST)", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => {
      throw new Error("no network");
    });
    const { client } = makeClient({ fetch: fetchMock as unknown as ClientOptions["fetch"] });

    client.release(["nope"], null);
    await vi.advanceTimersByTimeAsync(0);
    expect(fetchMock.mock.calls.filter((c) => String(c[0]).endsWith("/unsubscribe"))).toHaveLength(
      0,
    );
  });

  test("a rejected control POST is swallowed (no unhandled rejection / throw)", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => {
      throw new Error("control failed");
    });
    const { client } = makeClient({ fetch: fetchMock as unknown as ClientOptions["fetch"] });
    client.connected = true;

    // ensure() -> control() must not throw synchronously even though fetch rejects.
    expect(() => client.ensure(["a"], null)).not.toThrow();
    // Let the rejected control promise settle; it is caught internally.
    await vi.advanceTimersByTimeAsync(0);
    // Reaching here without an unhandled rejection is the assertion.
    expect(true).toBe(true);
  });
});

describe("SSE resume (Last-Event-ID / resync)", () => {
  const frame = (id: number, body: unknown) => `id: ${id}\ndata: ${JSON.stringify(body)}`;

  test("handleEvent tracks the last event id from the id: line", () => {
    const { client } = makeClient();
    client.handleEvent(frame(5, { sub: "k", data: [1] }));
    expect(client.lastEventId).toBe(5);
  });

  test("an event id <= lastEventId is dropped (replay/duplicate guard)", () => {
    const { client, calls } = makeClient();
    client.handleEvent(frame(5, { sub: "k", data: [1] }));
    calls.length = 0;
    client.handleEvent(frame(3, { sub: "k", data: [2] })); // stale → dropped
    expect(calls).toEqual([]);
    client.handleEvent(frame(6, { sub: "k", data: [3] })); // newer → applied
    expect(calls).toEqual([["k", [3]]]);
    expect(client.lastEventId).toBe(6);
  });

  test("a resync frame re-registers every subscription and adopts its id", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn(async () => {
      throw new Error("no network");
    });
    const { client } = makeClient({ fetch: fetchMock as unknown as ClientOptions["fetch"] });
    client.connected = true;
    client.ensure(["messages", "list"], { channelId: "c1" });
    await vi.advanceTimersByTimeAsync(0);
    expect(client.serverHasSub.size).toBe(1);
    fetchMock.mockClear();

    // Server lost our state (restart / buffer rolled past) → it sends a resync.
    client.handleEvent(frame(1, { type: "resync" }));
    await vi.advanceTimersByTimeAsync(0);

    const subs = fetchMock.mock.calls.filter((c) => String(c[0]).endsWith("/subscribe"));
    expect(subs).toHaveLength(1); // re-registered all subs
    expect(client.lastEventId).toBe(1); // adopted the server's stream position
  });

  test("a resync frame does not write confirmed data (it carries no sub)", () => {
    const { client, calls } = makeClient();
    client.handleEvent(frame(1, { type: "resync" }));
    expect(calls).toEqual([]);
  });
});

describe("status FSM (onStatus)", () => {
  test("onStatus fires immediately with the current status", () => {
    const { client } = makeClient();
    const seen: SyncStatus[] = [];
    client.onStatus((s) => seen.push(s));
    expect(seen).toEqual(["connecting"]);
  });

  test("setStatus dedupes repeats and broadcasts real transitions", () => {
    const { client } = makeClient();
    const seen: SyncStatus[] = [];
    client.onStatus((s) => seen.push(s));

    client.setStatus("connecting"); // same as current → ignored
    client.setStatus("connected"); // transition → fires
    client.setStatus("connected"); // repeat → ignored
    client.setStatus("disconnected"); // transition → fires

    expect(seen).toEqual(["connecting", "connected", "disconnected"]);
  });

  test("unsubscribing stops further status notifications", () => {
    const { client } = makeClient();
    const seen: SyncStatus[] = [];
    const off = client.onStatus((s) => seen.push(s));
    off();
    client.setStatus("connected");
    expect(seen).toEqual(["connecting"]);
  });
});

// NOTE: The full reconnect/backoff sequence (runStream reading a live
// ReadableStream, exponential-backoff jitter timing) needs a real socket and is
// intentionally NOT asserted here — only the pure pieces it composes are tested.
