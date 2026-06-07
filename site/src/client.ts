import { createClient, InMemoryKV } from "@onveloz/pulse-client";
import type { contract } from "../app/contract.js";

// Talks to the Pulse engine directly (its CORS is permissive; SSE isn't proxied).
// In dev it defaults to the local engine; in prod set VITE_PULSE_ENGINE_URL.
//
// Presence is real-time and ephemeral, so we use an in-memory cache instead of
// the default IndexedDB one: persisting it would resurrect stale cursors on the
// next load (showing visitors who already left until the next live change).
export const pulse = createClient<typeof contract>({
  url: import.meta.env.VITE_PULSE_ENGINE_URL ?? "http://127.0.0.1:8787",
  persistence: new InMemoryKV(),
});
