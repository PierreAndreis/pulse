import { createClient } from "@pulse/client";
import type { contract } from "./contract.js";

// Point straight at the engine. It sets permissive CORS, so cross-origin /rpc
// and /sync (SSE) both work. We bypass Vite's dev proxy on purpose: the proxy
// buffers `text/event-stream`, so EventSource never opens through it (the stream
// would hang at "connecting…"). The demo's verifyJwt accepts any non-empty
// bearer token (→ seeded demo user).
const ENGINE =
  (import.meta as { env?: Record<string, string | undefined> }).env?.VITE_PULSE_ENGINE_URL ??
  "http://127.0.0.1:8787";

// --- Simulated offline switch (a dev tool for the showcase) -----------------
// When "offline", every network call rejects — exactly like airplane mode. The
// SDK then queues writes durably and shows last-known data from its read cache.
let offline = false;
const offlineListeners = new Set<(v: boolean) => void>();

const guardedFetch: typeof fetch = (input, init) =>
  offline
    ? Promise.reject(new TypeError("offline (simulated by the demo's Go-offline toggle)"))
    : fetch(input, init);

export const pulse = createClient<typeof contract>({
  url: ENGINE,
  fetch: guardedFetch,
  headers: () => ({ authorization: "Bearer demo" }),
});

/** Toggle the simulated network. Going back online forces an immediate reconnect
 *  (which auto-flushes the durable offline queue). */
export function setOffline(value: boolean): void {
  if (offline === value) return;
  offline = value;
  for (const cb of offlineListeners) cb(value);
  pulse.$reconnect(); // drop or re-establish the live stream right away
}

export function isOffline(): boolean {
  return offline;
}

export function onOffline(cb: (v: boolean) => void): () => void {
  offlineListeners.add(cb);
  cb(offline);
  return () => offlineListeners.delete(cb);
}
