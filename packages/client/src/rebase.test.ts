import { describe, expect, it, vi } from "vitest";
import { InMemoryKV } from "./kv.js";
import { LocalFirst } from "./localfirst.js";
import { queryKeyOf, type OptimisticUpdater } from "./local.js";

// S3 (CI-safe) — local-first optimistic overlay + rebase, driven entirely through
// the PUBLIC @pulse/client API with a controllable fake transport (the `fetch`
// option). No engine, no Postgres: pure, deterministic. Asserts the overlay is
// immediate, confirms drop the overlay, handler-rejects roll back, network errors
// keep the write durably queued (sent exactly once on reconnect), and the queue
// drains in FIFO order.

const INC_PATH = ["counters", "increment"];
const LIST = ["counters", "list"];
const INPUT = {} as const;
const VIEW = queryKeyOf(LIST, INPUT);
type Row = { id: string; value: number };

const inc: OptimisticUpdater = (s) => {
  const cur = s.getQuery<Row>(LIST, INPUT);
  s.setQuery<Row>(LIST, INPUT, [{ id: "c", value: (cur[0]?.value ?? 0) + 1 }]);
};
const value = (lf: LocalFirst): number => {
  const rows = lf.store.getView(VIEW) as Row[];
  return rows[0]!.value;
};

const okResponse = () =>
  new Response(JSON.stringify({ result: null }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
const handlerErrorResponse = () =>
  new Response(JSON.stringify({ error: { code: "BAD_REQUEST", message: "nope" } }), {
    status: 400,
    headers: { "content-type": "application/json" },
  });

/** Records every RPC's input (call order) and serves a scripted outcome. */
class FakeServer {
  readonly sent: unknown[] = [];
  mode: "ok" | "handlerError" | "network" = "ok";
  readonly fetch = (_url: string, init: { body: string }): Promise<Response> => {
    this.sent.push(JSON.parse(init.body).input);
    if (this.mode === "network") return Promise.reject(new TypeError("network down"));
    if (this.mode === "handlerError") return Promise.resolve(handlerErrorResponse());
    return Promise.resolve(okResponse());
  };
}

const tick = () => new Promise((r) => setTimeout(r, 0));

function make(server: FakeServer): LocalFirst {
  const lf = new LocalFirst(
    { url: "http://x", fetch: server.fetch as unknown as typeof fetch },
    new InMemoryKV(),
  );
  lf.store.setConfirmed(VIEW, [{ id: "c", value: 0 }]); // server truth
  return lf;
}

describe("S3: local-first overlay + rebase (public API, fake transport)", () => {
  it("applies the optimistic overlay immediately, before any confirmation", async () => {
    const server = new FakeServer();
    server.mode = "network"; // never confirms → overlay must stand on its own
    const lf = make(server);

    await lf.mutate(INC_PATH, INPUT, { optimistic: inc });
    await tick();

    expect(value(lf)).toBe(1); // shown instantly
    expect(await lf.queue.size()).toBe(1); // still queued / durable
  });

  it("confirming drops the overlay; a server push then converges the view", async () => {
    const server = new FakeServer();
    const lf = make(server);

    await lf.mutate(INC_PATH, INPUT, { optimistic: inc });
    await tick();
    expect(await lf.queue.size()).toBe(0); // delivered

    // Overlay dropped after confirm → back to last server-confirmed truth...
    expect(value(lf)).toBe(0);
    // ...until the reactive push delivers the new confirmed value.
    lf.store.setConfirmed(VIEW, [{ id: "c", value: 1 }]);
    expect(value(lf)).toBe(1);
  });

  it("a handler rejection rolls back the overlay and calls onError", async () => {
    const server = new FakeServer();
    server.mode = "handlerError";
    const lf = make(server);
    const onError = vi.fn();

    await lf.mutate(INC_PATH, INPUT, { optimistic: inc, onError });
    await tick();

    expect(onError).toHaveBeenCalledTimes(1);
    expect(value(lf)).toBe(0); // rolled back
    expect(await lf.queue.size()).toBe(0); // dropped, not retried
  });

  it("a network error keeps the write queued, then sends it exactly once on reconnect", async () => {
    const server = new FakeServer();
    server.mode = "network";
    const lf = make(server);

    await lf.mutate(INC_PATH, INPUT, { optimistic: inc });
    await tick();
    expect(await lf.queue.size()).toBe(1); // stays queued (offline)
    expect(value(lf)).toBe(1); // overlay survives
    expect(server.sent.length).toBe(1); // attempted once

    server.mode = "ok"; // back online
    await lf.flush();
    await tick();
    expect(await lf.queue.size()).toBe(0); // delivered
    expect(server.sent.length).toBe(2); // exactly one retry — no duplicate storm
  });

  it("drains the queue in FIFO order", async () => {
    const server = new FakeServer();
    server.mode = "network"; // reject each so all three enqueue (flush breaks, no hang)
    const lf = make(server);

    await lf.mutate(["counters", "a"], { n: 1 }, { optimistic: inc });
    await lf.mutate(["counters", "b"], { n: 2 }, { optimistic: inc });
    await lf.mutate(["counters", "c"], { n: 3 }, { optimistic: inc });
    await tick();
    expect(value(lf)).toBe(3); // all three overlays stacked
    expect(await lf.queue.size()).toBe(3);

    server.sent.length = 0; // ignore the failed offline attempts
    server.mode = "ok";
    await lf.flush();
    await tick();

    expect(await lf.queue.size()).toBe(0);
    // Delivered strictly in the order they were enqueued.
    expect(server.sent).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
  });
});
