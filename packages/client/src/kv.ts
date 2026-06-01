/**
 * Pluggable key/value persistence for the local-first layer. The browser uses
 * IndexedDB; other environments (SSR/tests) fall back to in-memory.
 */
export interface KVStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
  remove(key: string): Promise<void>;
  keys(prefix?: string): Promise<string[]>;
}

export class InMemoryKV implements KVStore {
  private readonly map = new Map<string, string>();

  async get(key: string): Promise<string | null> {
    return this.map.get(key) ?? null;
  }
  async set(key: string, value: string): Promise<void> {
    this.map.set(key, value);
  }
  async remove(key: string): Promise<void> {
    this.map.delete(key);
  }
  async keys(prefix = ""): Promise<string[]> {
    return [...this.map.keys()].filter((k) => k.startsWith(prefix));
  }
}

/** Promisify a single IDBRequest. */
function reqPromise<T>(req: IDBRequest<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

export class IndexedDbKV implements KVStore {
  private dbPromise?: Promise<IDBDatabase>;

  constructor(
    private readonly dbName = "pulse",
    private readonly storeName = "kv",
  ) {}

  private open(): Promise<IDBDatabase> {
    if (this.dbPromise) return this.dbPromise;
    this.dbPromise = this.openAt().then(async (db) => {
      // If the DB already existed at this version but WITHOUT our store (an older
      // or aborted open), `onupgradeneeded` never fired — so the store is missing
      // and every transaction would throw `NotFoundError`. Bump the version to
      // force an upgrade that creates it.
      if (db.objectStoreNames.contains(this.storeName)) return db;
      const nextVersion = db.version + 1;
      db.close();
      return this.openAt(nextVersion);
    });
    return this.dbPromise;
  }

  private openAt(version?: number): Promise<IDBDatabase> {
    return new Promise<IDBDatabase>((resolve, reject) => {
      const req = version === undefined
        ? indexedDB.open(this.dbName)
        : indexedDB.open(this.dbName, version);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(this.storeName)) {
          db.createObjectStore(this.storeName);
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
      req.onblocked = () => reject(new Error("IndexedDB open blocked"));
    });
  }

  /** Run `fn` in a transaction and await BOTH the request and the tx completing. */
  private async run<T>(
    mode: IDBTransactionMode,
    fn: (s: IDBObjectStore) => IDBRequest<T>,
  ): Promise<T> {
    const db = await this.open();
    return new Promise<T>((resolve, reject) => {
      const tx = db.transaction(this.storeName, mode);
      const req = fn(tx.objectStore(this.storeName));
      let result: T;
      req.onsuccess = () => {
        result = req.result;
      };
      req.onerror = () => reject(req.error);
      tx.oncomplete = () => resolve(result);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error ?? new Error("IndexedDB tx aborted"));
    });
  }

  async get(key: string): Promise<string | null> {
    const v = await this.run<string | undefined>("readonly", (s) => s.get(key));
    return v ?? null;
  }
  async set(key: string, value: string): Promise<void> {
    await this.run("readwrite", (s) => s.put(value, key));
  }
  async remove(key: string): Promise<void> {
    // IDBObjectStore exposes `delete`, not `remove`.
    await this.run("readwrite", (s) => s.delete(key));
  }
  async keys(prefix = ""): Promise<string[]> {
    const all = await this.run<IDBValidKey[]>("readonly", (s) => s.getAllKeys());
    return all.map(String).filter((k) => k.startsWith(prefix));
  }
}

/** Pick the best persistence for the current environment. */
export function defaultKV(): KVStore {
  try {
    if (typeof indexedDB !== "undefined") return new IndexedDbKV();
  } catch {
    // indexedDB access can throw in some sandboxed contexts.
  }
  return new InMemoryKV();
}
