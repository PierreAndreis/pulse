export { createClient } from "./client.js";
export type {
  Client,
  PulseClient,
  PulseClientExtras,
  ProcedureClient,
  PulseQueryOptions,
  PulseMutationOptions,
  PulseInfiniteOptions,
  PulseQueryKey,
  PulsePartialKey,
  PulseQueryMeta,
} from "./client.js";
export { rpcCall, PulseClientError, type ClientOptions } from "./transport.js";
export { CollabHandle, Y, type CollabStatus, type CollabHandleOptions } from "./collab.js";
export type { SyncStatus } from "./sync.js";

// Local-first layer
export { LocalStore, queryKeyOf, type OptimisticStore, type OptimisticUpdater } from "./local.js";
export { LocalFirst, type MutateOptions } from "./localfirst.js";
export { OfflineQueue, type QueuedMutation } from "./queue.js";
export { InMemoryKV, IndexedDbKV, defaultKV, type KVStore } from "./kv.js";
