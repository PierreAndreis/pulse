import type { KVStore } from "./kv.js";

/** Error thrown by the client when an RPC call fails. */
export class PulseClientError extends Error {
  constructor(
    public readonly code: string,
    public readonly data: unknown,
    message?: string,
    public readonly httpStatus?: number,
  ) {
    super(message ?? code);
    this.name = "PulseClientError";
  }
}

export interface ClientOptions {
  /** Base URL of the Pulse engine, e.g. "/" or "https://api.example.com/". */
  url: string;
  /** Async provider of auth/other headers, called per request. */
  headers?: () => Record<string, string> | Promise<Record<string, string>>;
  /** Custom fetch (for SSR/tests). Defaults to global `fetch`. */
  fetch?: typeof fetch;
  /** Persistence for the local-first layer (offline queue). Defaults to IndexedDB in the browser, in-memory otherwise. */
  persistence?: KVStore;
}

/** Wire envelope for an RPC response. */
interface RpcResponseBody {
  result?: unknown;
  error?: { code: string; data?: unknown; message?: string };
}

function joinUrl(base: string, path: string): string {
  return `${base.replace(/\/$/, "")}/${path}`;
}

/** Execute a single RPC call against `POST {url}/rpc`.
 *
 * `mutationId` is an optional idempotency key: when a mutation is retried (lost
 * ack) or flushed by more than one tab from the shared offline queue, the engine
 * dedupes by this key so the write is applied exactly once. */
export async function rpcCall(
  options: ClientOptions,
  path: string[],
  input: unknown,
  mutationId?: string,
): Promise<unknown> {
  const doFetch = options.fetch ?? globalThis.fetch;
  const extra = (await options.headers?.()) ?? {};
  const res = await doFetch(joinUrl(options.url, "rpc"), {
    method: "POST",
    headers: { "content-type": "application/json", ...extra },
    body: JSON.stringify(mutationId ? { path, input, mutationId } : { path, input }),
  });

  let body: RpcResponseBody;
  try {
    body = (await res.json()) as RpcResponseBody;
  } catch {
    throw new PulseClientError("INTERNAL", undefined, `invalid response from server`, res.status);
  }

  if (!res.ok || body.error) {
    const err = body.error ?? { code: "INTERNAL" };
    throw new PulseClientError(err.code, err.data, err.message, res.status);
  }
  return body.result;
}
