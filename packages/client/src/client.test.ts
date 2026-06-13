import { describe, expect, it, vi } from "vitest";
import type { ContractRouter } from "@onveloz/pulse-contract";
import { createClient } from "./client.js";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

// Loose handle for runtime tests; full type inference is covered in examples-chat.
type AnyClient = Record<string, any>;

describe("client proxy", () => {
  it("builds the call path and posts to /rpc", async () => {
    const fetchMock = vi.fn(
      async (_url: string | URL | Request, _init?: RequestInit) =>
        jsonResponse({ result: [{ _id: "m1", body: "hi" }] }),
    );
    const client = createClient<ContractRouter>({
      url: "/",
      fetch: fetchMock as unknown as typeof fetch,
    }) as unknown as AnyClient;

    const res = await client.messages.list.call({ channelId: "c1" });
    expect(res).toEqual([{ _id: "m1", body: "hi" }]);

    const call = fetchMock.mock.calls[0]!;
    expect(call[0]).toBe("/rpc");
    expect(JSON.parse(call[1]!.body as string)).toEqual({
      path: ["messages", "list"],
      input: { channelId: "c1" },
    });
  });

  it("generates partial and exact query keys", () => {
    const client = createClient<ContractRouter>({ url: "/" }) as unknown as AnyClient;
    expect(client.messages.list.key()).toEqual([["messages", "list"]]);
    expect(client.messages.list.queryKey({ input: { channelId: "c1" } })).toEqual([
      ["messages", "list"],
      { type: "reactive", input: { channelId: "c1" } },
    ]);
  });

  it("caches nested accessors so their identity is stable (referential equality)", () => {
    const client = createClient<ContractRouter>({ url: "/" }) as unknown as AnyClient;
    expect(client.messages).toBe(client.messages); // same namespace proxy each access
    expect(client.messages.list).toBe(client.messages.list); // same procedure proxy
    expect(client.messages).not.toBe(client.users); // distinct namespaces are distinct
  });

  it("exposes the local-first + status handles only at the root", () => {
    const client = createClient<ContractRouter>({ url: "/" }) as unknown as AnyClient;
    expect(typeof client.$onSyncStatus).toBe("function");
    expect(client.$local).toBeDefined();
    // A nested path does not shadow them: `messages.$local` is a plain sub-proxy,
    // not the engine handle, so the root-only guard holds.
    expect(client.messages.$local).not.toBe(client.$local);
  });

  it("throws PulseClientError on error envelope", async () => {
    const fetchMock = vi.fn(
      async (_url: string | URL | Request, _init?: RequestInit) =>
        jsonResponse({ error: { code: "RATE_LIMITED", data: { retryAfter: 5 } } }, 429),
    );
    const client = createClient<ContractRouter>({
      url: "/",
      fetch: fetchMock as unknown as typeof fetch,
    }) as unknown as AnyClient;
    await expect(client.messages.send.call({ body: "x" })).rejects.toMatchObject({
      code: "RATE_LIMITED",
      data: { retryAfter: 5 },
    });
  });
});
