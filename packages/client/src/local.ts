/** Canonical local key for a query = path + serialized input. */
export type QueryKey = string;

export function queryKeyOf(path: string[], input: unknown): QueryKey {
  return `${path.join(".")}::${JSON.stringify(input ?? null)}`;
}

/** Handle passed to optimistic updaters. Lets a mutation speculatively rewrite
 *  the cached result of specific queries; stable `tempId`s survive rebases. */
export interface OptimisticStore {
  tempId(table: string): string;
  getQuery<T = unknown>(path: string[], input: unknown): T[];
  setQuery<T = unknown>(path: string[], input: unknown, docs: T[]): void;
}

export type OptimisticUpdater = (store: OptimisticStore) => void;

interface Pending {
  id: string;
  updater: OptimisticUpdater;
}

/**
 * Local-first cache: a confirmed layer (authoritative, from the server) and an
 * optimistic overlay (speculative, from pending mutations). The materialized
 * view = confirmed with all pending optimistic updaters replayed on top, in
 * order — so confirming/rolling back a mutation, or receiving fresh confirmed
 * data, rebases the remaining optimistic mutations correctly.
 */
export class LocalStore {
  private confirmed = new Map<QueryKey, unknown[]>();
  private view = new Map<QueryKey, unknown[]>();
  private pending: Pending[] = [];
  private listeners = new Map<QueryKey, Set<(docs: unknown[]) => void>>();

  /**
   * Optional sink for confirmed results, so the read cache can be persisted
   * (e.g. to IndexedDB) and rehydrated on the next load — true local-first: the
   * UI shows last-known data immediately, even with the server down.
   */
  onPersist?: (key: QueryKey, docs: unknown[]) => void;

  setConfirmed(key: QueryKey, docs: unknown[]): void {
    this.confirmed.set(key, docs);
    this.recompute();
    this.notify(key);
    this.onPersist?.(key, docs);
  }

  /**
   * Seed a confirmed result from persistence WITHOUT re-persisting it. Used at
   * startup to rehydrate the cache so subscribers see cached data instantly.
   * A later live server push (`setConfirmed`) overwrites it.
   */
  hydrate(key: QueryKey, docs: unknown[]): void {
    if (this.confirmed.has(key)) return; // never clobber fresher live data
    this.confirmed.set(key, docs);
    this.recompute();
    this.notify(key);
  }

  addOptimistic(id: string, updater: OptimisticUpdater): void {
    this.pending.push({ id, updater });
    this.recompute();
    this.notifyAll();
  }

  removeOptimistic(id: string): void {
    const before = this.pending.length;
    this.pending = this.pending.filter((p) => p.id !== id);
    if (this.pending.length !== before) {
      this.recompute();
      this.notifyAll();
    }
  }

  getView(key: QueryKey): unknown[] | undefined {
    return this.view.has(key) ? this.view.get(key) : this.confirmed.get(key);
  }

  hasListeners(key: QueryKey): boolean {
    return (this.listeners.get(key)?.size ?? 0) > 0;
  }

  /** Subscribe to a query's materialized view. Fires immediately if data exists. */
  subscribe(key: QueryKey, listener: (docs: unknown[]) => void): () => void {
    let set = this.listeners.get(key);
    if (!set) {
      set = new Set();
      this.listeners.set(key, set);
    }
    set.add(listener);
    const current = this.getView(key);
    if (current !== undefined) listener(current);
    return () => {
      this.listeners.get(key)?.delete(listener);
    };
  }

  private recompute(): void {
    this.view = new Map();
    for (const [k, v] of this.confirmed) this.view.set(k, v);

    const read = (path: string[], input: unknown): unknown[] => {
      const k = queryKeyOf(path, input);
      return (this.view.get(k) ?? this.confirmed.get(k) ?? []) as unknown[];
    };
    const write = (path: string[], input: unknown, docs: unknown[]): void => {
      this.view.set(queryKeyOf(path, input), docs);
    };

    for (const p of this.pending) {
      let n = 0;
      const store: OptimisticStore = {
        // Deterministic per (mutation, call index) → stable across recomputes.
        tempId: (table) => `${table}:opt-${p.id}-${n++}`,
        getQuery: <T,>(path: string[], input: unknown) => read(path, input) as T[],
        setQuery: <T,>(path: string[], input: unknown, docs: T[]) =>
          write(path, input, docs as unknown[]),
      };
      try {
        p.updater(store);
      } catch {
        // a bad optimistic updater must not corrupt the cache
      }
    }
  }

  private notify(key: QueryKey): void {
    const view = this.getView(key);
    if (view === undefined) return;
    this.listeners.get(key)?.forEach((l) => l(view));
  }

  private notifyAll(): void {
    for (const key of this.listeners.keys()) this.notify(key);
  }
}
