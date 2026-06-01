import type { KVStore } from "./kv.js";
import { LocalStore, type OptimisticUpdater, type QueryKey } from "./local.js";
import { OfflineQueue } from "./queue.js";
import { PulseClientError, rpcCall, type ClientOptions } from "./transport.js";

const CACHE_PREFIX = "pulse:cache:";

export interface MutateOptions {
  optimistic?: OptimisticUpdater;
  /** Called if the server rejects the mutation (handler error → rolled back). */
  onError?: (error: PulseClientError) => void;
}

/**
 * Coordinates the local-first write path: apply optimism, durably enqueue, and
 * flush to the server when reachable. Network failures keep the mutation queued
 * (offline); handler errors roll the optimistic update back and drop it.
 */
export class LocalFirst {
  readonly store = new LocalStore();
  readonly queue: OfflineQueue;
  private flushing = false;
  private readonly errorHandlers = new Map<string, (e: PulseClientError) => void>();
  /** Listeners notified whenever the offline-queue size changes (for UI badges). */
  private readonly queueListeners = new Set<() => void>();

  constructor(
    private readonly options: ClientOptions,
    private readonly kv: KVStore,
  ) {
    this.queue = new OfflineQueue(kv);
    // Persist every confirmed query result so it can be rehydrated on next load
    // (true local-first: data shows immediately, even with the server down).
    this.store.onPersist = (key, docs) => {
      void this.kv.set(CACHE_PREFIX + key, JSON.stringify(docs));
    };
  }

  /** Rehydrate a query's last-known result from persistence into the store, so a
   *  fresh page load (or a down server) still shows cached data immediately. */
  async hydrate(key: QueryKey): Promise<void> {
    const raw = await this.kv.get(CACHE_PREFIX + key);
    if (raw == null) return;
    try {
      this.store.hydrate(key, JSON.parse(raw) as unknown[]);
    } catch {
      // ignore corrupt cache entry
    }
  }

  /** Subscribe to offline-queue size changes (returns an unsubscribe fn). */
  onQueueChange(cb: () => void): () => void {
    this.queueListeners.add(cb);
    return () => this.queueListeners.delete(cb);
  }
  private notifyQueue(): void {
    for (const cb of this.queueListeners) cb();
  }

  async mutate(path: string[], input: unknown, opts: MutateOptions = {}): Promise<void> {
    const id = this.nextId();
    if (opts.optimistic) this.store.addOptimistic(id, opts.optimistic);
    if (opts.onError) this.errorHandlers.set(id, opts.onError);
    await this.queue.enqueue({ id, path, input });
    this.notifyQueue();
    void this.flush();
  }

  /** Drain the queue in order. Stops at the first network error (stays offline). */
  async flush(): Promise<void> {
    if (this.flushing) return;
    this.flushing = true;
    try {
      for (const m of await this.queue.all()) {
        try {
          await rpcCall(this.options, m.path, m.input);
          // Confirmed. The subscription push refreshes the confirmed layer;
          // drop the optimistic overlay for this mutation.
          await this.queue.remove(m.id);
          this.store.removeOptimistic(m.id);
          this.errorHandlers.delete(m.id);
          this.notifyQueue();
        } catch (e) {
          if (e instanceof PulseClientError) {
            // Handler-level rejection: roll back and drop.
            await this.queue.remove(m.id);
            this.store.removeOptimistic(m.id);
            this.errorHandlers.get(m.id)?.(e);
            this.errorHandlers.delete(m.id);
            this.notifyQueue();
          } else {
            // Network error → remain queued; retry on the next flush.
            break;
          }
        }
      }
    } finally {
      this.flushing = false;
    }
  }

  /** A globally-unique mutation id. Must be unique ACROSS tabs (they share the
   *  durable queue), so a per-tab counter won't do — use a UUID. This id is also
   *  the server-side idempotency key (see rpcCall). */
  private nextId(): string {
    const g = globalThis as { crypto?: { randomUUID?: () => string } };
    return (
      g.crypto?.randomUUID?.() ??
      `m-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
    );
  }
}
