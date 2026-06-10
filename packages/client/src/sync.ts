import { type ClientOptions } from "./transport.js";
import { type LocalStore, queryKeyOf } from "./local.js";

/** A stable-ish per-client id for the duration of the page/session. */
function makeClientId(): string {
  return `c_${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`;
}

/**
 * Owns one multiplexed SSE stream (`GET /sync`) for the client and registers /
 * unregisters subscriptions over `POST /subscribe` and `POST /unsubscribe`. Each
 * server push `{ sub, data }` is written into the `LocalStore` confirmed layer,
 * which fans the materialized view out to listeners (and rebases optimism).
 */
export type SyncStatus = "connecting" | "connected" | "disconnected";

export class SyncClient {
  private readonly clientId = makeClientId();
  /** Active subscriptions, kept so they can be re-registered after a reconnect. */
  private readonly registered = new Map<string, { path: string[]; input: unknown }>();
  /** Highest SSE event id we've processed — resent as `Last-Event-ID` to resume. */
  private lastEventId?: number;
  /** Subscriptions the server currently holds — kept across a *resumed* reconnect
   *  so we don't re-subscribe (and re-execute) everything for a brief blip. */
  private readonly serverHasSub = new Set<string>();
  private streamAbort?: AbortController;
  private wake?: () => void;
  private started = false;
  private connected = false;
  private status: SyncStatus = "connecting";
  private readonly statusListeners = new Set<(s: SyncStatus) => void>();

  constructor(
    private readonly options: ClientOptions,
    private readonly store: LocalStore,
  ) {}

  /** Subscribe to connection-status changes (drives "live"/"offline" UI). */
  onStatus(cb: (s: SyncStatus) => void): () => void {
    this.statusListeners.add(cb);
    cb(this.status);
    return () => this.statusListeners.delete(cb);
  }
  private setStatus(s: SyncStatus): void {
    if (this.status === s) return;
    this.status = s;
    for (const cb of this.statusListeners) cb(s);
  }

  /**
   * Force an immediate (re)connect: drop the current stream and cut short any
   * pending backoff so the next attempt runs now. Used when connectivity is
   * known to have changed (e.g. `window` came back online, or a manual retry).
   */
  reconnectNow(): void {
    this.startLoop();
    this.streamAbort?.abort();
    this.wake?.();
  }

  /** Ensure the server is streaming this query to us (idempotent per query). */
  ensure(path: string[], input: unknown): void {
    const key = queryKeyOf(path, input);
    if (this.registered.has(key)) return;
    this.registered.set(key, { path, input });
    this.startLoop();
    // If already connected, register immediately; otherwise the reconnect loop
    // re-registers everything once the stream (re)opens.
    if (this.connected) this.registerSub(key, path, input);
  }

  release(path: string[], input: unknown): void {
    const key = queryKeyOf(path, input);
    if (!this.registered.delete(key)) return;
    this.serverHasSub.delete(key);
    void this.control("unsubscribe", { sub: key });
  }

  /** Register one subscription with the server and remember the server now has it. */
  private registerSub(key: string, path: string[], input: unknown): void {
    this.serverHasSub.add(key);
    void this.control("subscribe", { sub: key, path, input });
  }

  private joinUrl(p: string): string {
    return `${this.options.url.replace(/\/$/, "")}/${p}`;
  }

  private async control(action: "subscribe" | "unsubscribe", body: Record<string, unknown>): Promise<void> {
    const doFetch = this.options.fetch ?? globalThis.fetch;
    try {
      await doFetch(this.joinUrl(action), {
        method: "POST",
        headers: { "content-type": "application/json", ...((await this.options.headers?.()) ?? {}) },
        body: JSON.stringify({ clientId: this.clientId, ...body }),
      });
    } catch {
      // re-registration happens on reconnect (M5: resumability is future)
    }
  }

  /** Start the persistent connect/reconnect loop (idempotent). */
  private startLoop(): void {
    if (this.started) return;
    this.started = true;
    void this.reconnectLoop();
  }

  private async reconnectLoop(): Promise<void> {
    let attempt = 0;
    // Reconnect forever (until the page unloads). Each successful connection
    // resets the backoff; a drop re-registers all subscriptions on the next open.
    for (;;) {
      try {
        await this.runStream();
        attempt = 0; // clean end (rare) → reconnect promptly
      } catch {
        attempt++;
      }
      this.connected = false;
      this.setStatus("disconnected");
      // Exponential backoff capped at 5s, with jitter — interruptible by
      // reconnectNow() so a known connectivity change retries immediately.
      const base = Math.min(5000, 250 * 2 ** Math.min(attempt, 5));
      const delay = base / 2 + Math.random() * (base / 2);
      await new Promise<void>((resolve) => {
        this.wake = resolve;
        setTimeout(resolve, delay);
      });
      this.wake = undefined;
      this.setStatus("connecting");
    }
  }

  private async runStream(): Promise<void> {
    this.streamAbort = new AbortController();
    const doFetch = this.options.fetch ?? globalThis.fetch;
    const resuming = this.lastEventId !== undefined;
    const headers: Record<string, string> = {
      accept: "text/event-stream",
      ...((await this.options.headers?.()) ?? {}),
    };
    // Resume from the last event we saw; the server replays anything we missed
    // (or sends a `resync` frame if the gap rolled past its buffer).
    if (this.lastEventId !== undefined) headers["last-event-id"] = String(this.lastEventId);
    const res = await doFetch(this.joinUrl(`sync?clientId=${this.clientId}`), {
      method: "GET",
      headers,
      signal: this.streamAbort.signal,
    });
    if (!res.ok || !res.body) throw new Error(`SSE open failed (${res.status})`);

    this.connected = true;
    this.setStatus("connected");
    if (!resuming) {
      // Fresh stream: the server has no memory of our subs — (re)register all.
      this.serverHasSub.clear();
      for (const [key, { path, input }] of this.registered) this.registerSub(key, path, input);
    } else {
      // Resumed stream: subs registered before the drop are kept server-side.
      // Register only subs added while disconnected; a `resync` frame (server
      // lost state) triggers a full re-register in handleEvent.
      for (const [key, { path, input }] of this.registered) {
        if (!this.serverHasSub.has(key)) this.registerSub(key, path, input);
      }
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) throw new Error("SSE stream ended");
      buffer += decoder.decode(value, { stream: true });
      let idx: number;
      while ((idx = buffer.indexOf("\n\n")) >= 0) {
        const raw = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        this.handleEvent(raw);
      }
    }
  }

  private handleEvent(raw: string): void {
    const lines = raw.split("\n");
    const idLine = lines.find((l) => l.startsWith("id:"));
    const eventId = idLine ? Number(idLine.slice(3).trim()) : undefined;
    const dataLines = lines.filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim());
    if (dataLines.length === 0) {
      if (eventId !== undefined) this.lastEventId = eventId; // keep-alive with an id
      return;
    }
    let payload: { sub?: string; data?: unknown; type?: string } | undefined;
    try {
      payload = JSON.parse(dataLines.join("\n"));
    } catch {
      return; // unparseable
    }
    // A `resync` means the server lost our subscriptions (restart, or the buffer
    // rolled past) — re-register everything and adopt the server's stream position.
    // Handled before the dedup guard, since after a restart its id can be < ours.
    if (payload?.type === "resync") {
      this.serverHasSub.clear();
      for (const [key, { path, input }] of this.registered) this.registerSub(key, path, input);
      this.lastEventId = eventId;
      return;
    }
    // Drop an event we've already processed (replayed-and-also-delivered guard).
    if (eventId !== undefined && this.lastEventId !== undefined && eventId <= this.lastEventId) return;
    if (payload?.sub) {
      this.store.setConfirmed(payload.sub, (payload.data ?? []) as unknown[]);
    }
    if (eventId !== undefined) this.lastEventId = eventId;
  }
}
