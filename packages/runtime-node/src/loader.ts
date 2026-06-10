type Waiter = { resolve: (v: unknown) => void; reject: (e: unknown) => void };
type Batch = { ids: string[]; waiters: Waiter[][] };

/**
 * DataLoader for point reads, keyed by table. Collapses every `get()` issued
 * within the SAME microtask tick (e.g. `Promise.all([ctx.db.get(a), ...])`) into
 * one `flush(table, ids)` call, then distributes the index-aligned result array
 * back to each caller.
 *
 * - Duplicate ids in a batch share a single slot (one fetch, both resolve).
 * - A missing/short index resolves to `null` (a missing row is not an error).
 * - A `flush` rejection rejects every waiter in that batch.
 *
 * Only gets in flight together batch; sequential `for…await` flushes one per tick
 * — transparent, just no speedup (see ADR-10).
 */
export function createGetLoader(flush: (table: string, ids: string[]) => Promise<unknown[]>) {
  const batches = new Map<string, Batch>();

  async function run(table: string): Promise<void> {
    const batch = batches.get(table);
    if (!batch) return;
    batches.delete(table);
    try {
      const rows = await flush(table, batch.ids);
      batch.ids.forEach((_id, i) => {
        const row = rows?.[i] ?? null;
        for (const w of batch.waiters[i] ?? []) w.resolve(row);
      });
    } catch (err) {
      for (const slot of batch.waiters) for (const w of slot) w.reject(err);
    }
  }

  return function get(table: string, id: unknown): Promise<unknown> {
    const key = String(id);
    let batch = batches.get(table);
    if (!batch) {
      batch = { ids: [], waiters: [] };
      batches.set(table, batch);
      queueMicrotask(() => void run(table));
    }
    const b = batch;
    let idx = b.ids.indexOf(key);
    if (idx === -1) {
      idx = b.ids.length;
      b.ids.push(key);
      b.waiters.push([]);
    }
    const slot = b.waiters[idx] ?? [];
    b.waiters[idx] = slot;
    return new Promise((resolve, reject) => {
      slot.push({ resolve, reject });
    });
  };
}
