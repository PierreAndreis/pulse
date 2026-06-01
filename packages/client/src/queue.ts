import type { KVStore } from "./kv.js";

export interface QueuedMutation {
  id: string;
  path: string[];
  input: unknown;
}

const QUEUE_KEY = "pulse:mutation-queue";

/**
 * Durable FIFO of mutations awaiting delivery. Persisted to the KV store so a
 * reload replays anything that hadn't been confirmed — no lost writes offline.
 *
 * Every operation runs through a single serialized chain (`run`), so each
 * read-modify-persist cycle is atomic even when callers fire concurrently on a
 * cold cache. Without it, two racing `enqueue`s each load the (empty) list and
 * the second `persist` clobbers the first, silently dropping a write.
 */
export class OfflineQueue {
  private cache?: QueuedMutation[];
  /** Tail of the serialized operation chain (mutual exclusion). */
  private tail: Promise<unknown> = Promise.resolve();

  constructor(private readonly kv: KVStore) {}

  /** Run `op` only after all previously-queued ops settle. */
  private run<T>(op: () => Promise<T>): Promise<T> {
    const next = this.tail.then(op, op);
    // Keep the chain alive whether this op resolves or rejects.
    this.tail = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }

  private async load(): Promise<QueuedMutation[]> {
    if (!this.cache) {
      const raw = await this.kv.get(QUEUE_KEY);
      this.cache = raw ? (JSON.parse(raw) as QueuedMutation[]) : [];
    }
    return this.cache;
  }

  private async persist(): Promise<void> {
    await this.kv.set(QUEUE_KEY, JSON.stringify(this.cache ?? []));
  }

  enqueue(mutation: QueuedMutation): Promise<void> {
    return this.run(async () => {
      const list = await this.load();
      list.push(mutation);
      await this.persist();
    });
  }

  remove(id: string): Promise<void> {
    return this.run(async () => {
      const list = await this.load();
      const idx = list.findIndex((m) => m.id === id);
      if (idx >= 0) {
        list.splice(idx, 1);
        await this.persist();
      }
    });
  }

  /** Mutations in FIFO order. */
  all(): Promise<QueuedMutation[]> {
    return this.run(async () => [...(await this.load())]);
  }

  size(): Promise<number> {
    return this.run(async () => (await this.load()).length);
  }
}
