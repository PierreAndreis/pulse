import { createClient, InMemoryKV } from "@onveloz/pulse-client";
import type { contract } from "../app/contract.js";

// The engine's base URL. In dev it defaults to the local engine; in prod the
// bundler inlines VITE_PULSE_ENGINE_URL.
export const engineUrl = import.meta.env.VITE_PULSE_ENGINE_URL ?? "http://127.0.0.1:8787";

// Used for mutations (move/say/react/leave) and one-shot reads. Reactive reads
// are driven by a hand-rolled SSE reader in CursorPresence (see note there).
// In-memory cache: presence is ephemeral, so we never persist it to IndexedDB
// (that would resurrect stale cursors on the next load).
export const pulse = createClient<typeof contract>({
  url: engineUrl,
  persistence: new InMemoryKV(),
});
