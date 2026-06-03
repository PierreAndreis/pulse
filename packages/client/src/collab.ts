import * as Y from "yjs";
import { rpcCall, type ClientOptions } from "./transport.js";

/** Base64 <-> bytes (browser + node). */
export function toB64(bytes: Uint8Array): string {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return typeof btoa !== "undefined" ? btoa(bin) : Buffer.from(bytes).toString("base64");
}
export function fromB64(s: string): Uint8Array {
  if (typeof atob !== "undefined") {
    const bin = atob(s);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  return new Uint8Array(Buffer.from(s, "base64"));
}

export type CollabStatus = "connecting" | "synced" | "saving" | "offline";

export interface CollabHandleOptions {
  /** Procedure paths for the collab field (defaults follow the `notes` convention). */
  getDocPath: string[];
  applyUpdatePath: string[];
  /** The document id. */
  id: string;
}

/**
 * A live, auto-syncing `Y.Doc` for one collaborative field — the imperative core
 * the React hooks build on. Users get a real `Y.Doc` to bind to any editor and
 * **never touch the network, base64, or update encoding**:
 *
 *  - seeds the doc from the server's current state,
 *  - subscribes (reactive) so remote merges flow in,
 *  - sends local edits as Yjs updates (merged server-side; offline-safe via the
 *    client's normal retry/queue path),
 *  - is idempotent + commutative end-to-end, so concurrent/offline edits merge.
 */
export class CollabHandle {
  readonly doc = new Y.Doc();
  private status: CollabStatus = "connecting";
  private readonly statusListeners = new Set<(s: CollabStatus) => void>();
  private unsubscribe?: () => void;
  private applying = false; // guard: don't re-send updates we just applied from the server
  private destroyed = false;

  constructor(
    private readonly options: ClientOptions,
    private readonly cfg: CollabHandleOptions,
    /** Subscribe to the reactive getDoc procedure; returns an unsubscribe fn. */
    private readonly subscribe: (
      path: string[],
      input: unknown,
      onData: (data: unknown) => void,
    ) => () => void,
  ) {
    this.start();
  }

  onStatus(cb: (s: CollabStatus) => void): () => void {
    this.statusListeners.add(cb);
    cb(this.status);
    return () => this.statusListeners.delete(cb);
  }
  getStatus(): CollabStatus {
    return this.status;
  }
  private setStatus(s: CollabStatus): void {
    this.status = s;
    for (const cb of this.statusListeners) cb(s);
  }

  /** Apply a base64 Yjs state/update from the server into the local doc. */
  private applyRemote(stateB64: string): void {
    if (!stateB64) return;
    this.applying = true;
    try {
      Y.applyUpdate(this.doc, fromB64(stateB64), "remote");
    } finally {
      this.applying = false;
    }
  }

  private start(): void {
    // Local edits → send as a delta update (skip ones we just applied remotely).
    this.doc.on("update", (update: Uint8Array, origin: unknown) => {
      if (origin === "remote" || this.destroyed) return;
      void this.push(update);
    });

    // Reactive subscription: initial state + every remote merge.
    this.unsubscribe = this.subscribe(this.cfg.getDocPath, { id: this.cfg.id }, (data) => {
      const state = (data as { state?: string } | null)?.state;
      if (typeof state === "string") {
        this.applyRemote(state);
        if (this.status !== "saving") this.setStatus("synced");
      }
    });
  }

  private async push(update: Uint8Array): Promise<void> {
    this.setStatus("saving");
    try {
      const res = (await rpcCall(this.options, this.cfg.applyUpdatePath, {
        id: this.cfg.id,
        update: toB64(update),
      })) as { state?: string };
      // Server returns the merged authoritative state; fold it back in (no-op if
      // already present — Yjs merges are idempotent).
      if (res?.state) this.applyRemote(res.state);
      this.setStatus("synced");
    } catch {
      // Offline / failed: the doc keeps the local edit; it will be re-sent on the
      // next local change or when connectivity returns. (Full offline-queue
      // integration mirrors @onveloz/pulse-client's mutation queue.)
      this.setStatus("offline");
    }
  }

  destroy(): void {
    this.destroyed = true;
    this.unsubscribe?.();
    this.statusListeners.clear();
    this.doc.destroy();
  }
}

/** Re-export Yjs for power users who want shared types (Y.Map, Y.Array, …). */
export { Y };
