import type {
  AnyContractProcedure,
  ContractRouter,
  InferInput,
  InferOutput,
  InferKind,
  ProcedureKind,
} from "@pulse/contract";
import { type ClientOptions, rpcCall } from "./transport.js";
import { SyncClient, type SyncStatus } from "./sync.js";
import { LocalFirst, type MutateOptions } from "./localfirst.js";
import { queryKeyOf } from "./local.js";
import { defaultKV } from "./kv.js";
import { CollabHandle } from "./collab.js";

// ── Query keys (hierarchical, TanStack-compatible) ────────────────────────────

/** Partial key — the procedure path only. Used for broad `invalidateQueries`. */
export type PulsePartialKey = readonly [readonly string[]];
/** Exact key — path + discriminator + input. Used for `setQueryData`. */
export type PulseQueryKey = readonly [readonly string[], { type: ProcedureKind; input: unknown }];

export interface PulseQueryMeta extends Record<string, unknown> {
  pulse: { kind: ProcedureKind; path: string[] };
}

/** Shape returned by `.queryOptions()`; assignable to TanStack `useQuery`. */
export interface PulseQueryOptions<Output> {
  queryKey: PulseQueryKey;
  queryFn: () => Promise<Output>;
  meta: PulseQueryMeta;
}

/** Shape returned by `.mutationOptions()`; assignable to TanStack `useMutation`. */
export interface PulseMutationOptions<Input, Output> {
  mutationKey: PulsePartialKey;
  mutationFn: (input: Input) => Promise<Output>;
  meta: PulseQueryMeta;
}

export interface PulseInfiniteOptions<Output, PageParam> {
  queryKey: PulseQueryKey;
  queryFn: (ctx: { pageParam: PageParam }) => Promise<Output>;
  initialPageParam: PageParam;
  getNextPageParam: (lastPage: Output, allPages: Output[]) => PageParam | undefined;
  meta: PulseQueryMeta;
}

// ── Per-procedure client surface ──────────────────────────────────────────────

/** If a procedure has no input, the `input` arg is optional; otherwise required. */
type InputArg<P extends AnyContractProcedure> = [InferInput<P>] extends [void]
  ? { input?: undefined }
  : { input: InferInput<P> };

export interface ProcedureClient<P extends AnyContractProcedure> {
  /** Direct call (no cache). */
  call(input: InferInput<P>): Promise<InferOutput<P>>;
  /** Subscribe to a reactive query; `onData` fires with the initial result, on every push, and on local optimistic changes. Returns an unsubscribe fn. */
  subscribe(args: InputArg<P>, onData: (data: InferOutput<P>) => void): () => void;
  /** Local-first mutation: apply optimism, durably enqueue, and flush when reachable (offline-safe). */
  mutate(input: InferInput<P>, opts?: MutateOptions): Promise<void>;
  /** Partial (path-only) query key for broad invalidation. */
  key(): PulsePartialKey;
  /** Exact query key for a given input. */
  queryKey(args: InputArg<P>): PulseQueryKey;
  /** TanStack Query options for a reactive/analytical query. */
  queryOptions(args: InputArg<P>): PulseQueryOptions<InferOutput<P>>;
  /** TanStack Query mutation options. */
  mutationOptions(): PulseMutationOptions<InferInput<P>, InferOutput<P>>;
  /** TanStack infinite-query options for paginated procedures. */
  infiniteOptions<PageParam = unknown>(args: {
    input: (pageParam: PageParam) => InferInput<P>;
    initialPageParam: PageParam;
    getNextPageParam: (
      lastPage: InferOutput<P>,
      allPages: InferOutput<P>[],
    ) => PageParam | undefined;
  }): PulseInfiniteOptions<InferOutput<P>, PageParam>;
}

/** Recursively map a contract to a client tree. */
export type Client<Node> = Node extends AnyContractProcedure
  ? ProcedureClient<Node>
  : Node extends ContractRouter
    ? { [K in keyof Node]: Client<Node[K]> }
    : never;

/** Root-level handle for the local-first layer (flush, store, offline queue). */
export interface PulseClientExtras {
  readonly $local: LocalFirst;
  /** Subscribe to live connection status (auto-reconnect aware). Returns unsub. */
  readonly $onSyncStatus: (cb: (status: SyncStatus) => void) => () => void;
  /** Force an immediate reconnect (e.g. after connectivity returns). */
  readonly $reconnect: () => void;
}

export type PulseClient<C extends ContractRouter> = Client<C> & PulseClientExtras;

// ── Runtime (proxy-based; contract is type-only at the call site) ─────────────

const TERMINALS = new Set([
  "call",
  "subscribe",
  "mutate",
  "key",
  "queryKey",
  "queryOptions",
  "mutationOptions",
  "infiniteOptions",
]);

interface Runtime {
  sync: SyncClient;
  local: LocalFirst;
}

function makeTerminals(
  options: ClientOptions,
  path: string[],
  rt: Runtime,
): Record<string, unknown> {
  const meta = (kind: ProcedureKind): PulseQueryMeta => ({ pulse: { kind, path } });
  return {
    call: (input: unknown) => rpcCall(options, path, input),
    subscribe: (args: { input?: unknown } | undefined, onData: (data: unknown) => void) => {
      const key = queryKeyOf(path, args?.input);
      const unsub = rt.local.store.subscribe(key, onData);
      // Local-first: show last-known cached data immediately (works with the
      // server down), then the live SSE push overwrites it when it arrives.
      void rt.local.hydrate(key);
      rt.sync.ensure(path, args?.input);
      return () => {
        unsub();
        if (!rt.local.store.hasListeners(key)) rt.sync.release(path, args?.input);
      };
    },
    mutate: (input: unknown, opts?: MutateOptions) => rt.local.mutate(path, input, opts),
    key: (): PulsePartialKey => [path] as const,
    queryKey: (args?: { input?: unknown }): PulseQueryKey =>
      [path, { type: "reactive", input: args?.input }] as const,
    queryOptions: (args?: { input?: unknown }) => ({
      queryKey: [path, { type: "reactive", input: args?.input }] as PulseQueryKey,
      queryFn: () => rpcCall(options, path, args?.input),
      meta: meta("reactive"),
    }),
    mutationOptions: () => ({
      mutationKey: [path] as PulsePartialKey,
      mutationFn: (input: unknown) => rpcCall(options, path, input),
      meta: meta("mutation"),
    }),
    infiniteOptions: (args: {
      input: (p: unknown) => unknown;
      initialPageParam: unknown;
      getNextPageParam: (last: unknown, all: unknown[]) => unknown;
    }) => ({
      queryKey: [path, { type: "analytical", input: "infinite" }] as PulseQueryKey,
      queryFn: ({ pageParam }: { pageParam: unknown }) =>
        rpcCall(options, path, args.input(pageParam)),
      initialPageParam: args.initialPageParam,
      getNextPageParam: args.getNextPageParam,
      meta: meta("analytical"),
    }),
  };
}

function createProxy(options: ClientOptions, path: string[], rt: Runtime): unknown {
  const cache = new Map<string, unknown>();
  return new Proxy(
    {},
    {
      get(_target, prop: string | symbol) {
        if (typeof prop !== "string") return undefined;
        // Root-only engine handle for the local-first layer (flush/store/queue).
        if (path.length === 0 && prop === "$local") return rt.local;
        // Root-only connection-status subscription (auto-reconnect aware).
        if (path.length === 0 && prop === "$onSyncStatus") {
          return (cb: (status: SyncStatus) => void) => rt.sync.onStatus(cb);
        }
        // Root-only manual reconnect (drops the stream, retries immediately).
        if (path.length === 0 && prop === "$reconnect") {
          return () => rt.sync.reconnectNow();
        }
        if (TERMINALS.has(prop)) {
          return makeTerminals(options, path, rt)[prop];
        }
        if (!cache.has(prop)) {
          cache.set(prop, createProxy(options, [...path, prop], rt));
        }
        return cache.get(prop);
      },
    },
  );
}

/** Create a fully-typed client from a contract type. No codegen, pure inference. */
export function createClient<C extends ContractRouter>(options: ClientOptions): PulseClient<C> {
  const kv = options.persistence ?? defaultKV();
  const local = new LocalFirst(options, kv);
  const sync = new SyncClient(options, local.store);
  // Drain the durable offline queue whenever the connection (re)opens — so
  // queued writes auto-sync on reconnect, not only on the next mutate.
  sync.onStatus((s) => {
    if (s === "connected") void local.flush();
  });
  // Replay any durably-queued mutations from a previous session (reload/offline).
  void local.flush();
  return createProxy(options, [], { sync, local }) as PulseClient<C>;
}
