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
 */
export class OfflineQueue {
  private cache?: QueuedMutation[];

  constructor(private readonly kv: KVStore) {}

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

  async enqueue(mutation: QueuedMutation): Promise<void> {
    const list = await this.load();
    list.push(mutation);
    await this.persist();
  }

  async remove(id: string): Promise<void> {
    const list = await this.load();
    const idx = list.findIndex((m) => m.id === id);
    if (idx >= 0) {
      list.splice(idx, 1);
      await this.persist();
    }
  }

  /** Mutations in FIFO order. */
  async all(): Promise<QueuedMutation[]> {
    return [...(await this.load())];
  }

  async size(): Promise<number> {
    return (await this.load()).length;
  }
}
