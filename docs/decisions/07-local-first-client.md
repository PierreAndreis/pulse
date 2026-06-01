# 07. Local-first client: offline queue, optimistic overlay + rebase, persistence

- **Status:** Accepted — partial implementation; deviates from `docs/ARCHITECTURE.md` §5.3–5.5 in several ways noted below. The modules (`LocalStore`, `OfflineQueue`, `LocalFirst`, `KVStore`) exist and are internally consistent, but are **not yet wired into `createClient`** (which only constructs `SyncClient`), and the spec's `lastMutationID` watermark / write-checkpoint guard / `online`+reload flush triggers are not implemented.

## Context & Problem

Pulse promises a local-first client (`docs/ARCHITECTURE.md` §1, §5): the cache sits under TanStack Query, mutations apply optimistically, and writes survive going offline. The user-facing requirements that force decisions here:

- A user fires a mutation while offline (or during a transient network blip). The write must not be lost: it has to be **durably queued** and replayed later, in order, even across a full page reload.
- The UI must reflect the write **immediately** (optimistic), then reconcile against authoritative server state without flicker or losing other in-flight writes.
- When a mutation is finally rejected by the server's *handler* (a real business error, e.g. `RATE_LIMITED`), the optimistic effect must be **rolled back** and the caller told — distinct from a *network* failure, which must keep the write queued for retry.
- Persistence must work in the browser (IndexedDB) and degrade gracefully in SSR/test environments (in-memory), and tests need to simulate "reload" = a fresh client object reattached to the same storage.

The hard part is reconciliation: with N pending optimistic writes and a stream of confirmed server snapshots arriving out of band (over SSE), confirming or rejecting any one write — or receiving fresh confirmed data — must leave the *remaining* pending writes correctly applied on top. This is the rebase problem.

## Decision

Four small, independently testable modules in `packages/client/src/`.

**1. `KVStore` — pluggable string KV (`kv.ts`).**

```ts
interface KVStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
  remove(key: string): Promise<void>;
  keys(prefix?: string): Promise<string[]>;
}
```

Three implementations: `InMemoryKV` (a `Map`), `IndexedDbKV` (one object store of string values, db `pulse` / store `kv`), and `defaultKV()` which returns `IndexedDbKV` when `indexedDB` is defined and falls back to `InMemoryKV` otherwise (SSR/tests). Tests inject a *shared* `InMemoryKV` into two successive clients to simulate a reload.

**2. `OfflineQueue` — durable FIFO of mutations (`queue.ts`).**

```ts
interface QueuedMutation { id: string; path: string[]; input: unknown; }
```

Stored as a single JSON array under KV key `pulse:mutation-queue`. `enqueue` appends; `remove(id)` deletes by id; `all()` returns a copy in FIFO order; `size()`. The list is lazily loaded into an in-memory `cache` on first access and re-persisted on every mutation, so a fresh `OfflineQueue` over the same KV replays anything not yet confirmed — no lost writes.

**3. `LocalStore` — confirmed layer + optimistic overlay with rebase (`local.ts`).**

Query identity is `queryKeyOf(path, input) = "${path.join('.')}::${JSON.stringify(input ?? null)}"`. The store holds:
- `confirmed: Map<QueryKey, unknown[]>` — authoritative, set from server data via `setConfirmed`.
- `pending: { id, updater }[]` — optimistic updaters in apply order.
- `view: Map<QueryKey, unknown[]>` — the materialized result = confirmed with all pending updaters replayed on top, recomputed eagerly on any change.

```ts
interface OptimisticStore {
  tempId(table: string): string;
  getQuery<T>(path: string[], input: unknown): T[];
  setQuery<T>(path: string[], input: unknown, docs: T[]): void;
}
type OptimisticUpdater = (store: OptimisticStore) => void;
```

`recompute()` rebuilds `view` from `confirmed`, then runs each pending updater in order; reads (`getQuery`) see the running `view` so updaters compose, and writes (`setQuery`) layer onto `view` only — `confirmed` is never mutated by an updater. `tempId(table)` is deterministic per `(mutation id, call index)` → `${table}:opt-${id}-${n}`, so the same temp ids are regenerated on every recompute and survive rebases. An updater that throws is swallowed so one bad updater can't corrupt the cache. `addOptimistic` / `removeOptimistic` mutate `pending` and recompute; `setConfirmed` replaces one query's confirmed docs and recomputes (this is the rebase: remaining pending updaters re-apply on top of the new confirmed data). `subscribe(key, fn)` fires immediately with the current view if present, and on every notify; `getView` falls back to `confirmed` when no overlay exists.

**4. `LocalFirst` — coordinator (`localfirst.ts`).** Owns a `LocalStore`, an `OfflineQueue`, a persisted monotonic sequence (`pulse:mutation-seq`, ids `m-<n>`), and a per-mutation `onError` map.

```ts
mutate(path, input, { optimistic?, onError? }):
  id = nextId()                       // persisted seq, survives reload
  if optimistic: store.addOptimistic(id, optimistic)
  if onError:    errorHandlers.set(id, onError)
  await queue.enqueue({ id, path, input })   // durable BEFORE network
  void flush()                        // fire-and-forget drain
```

`flush()` is reentrancy-guarded (`flushing` flag) and drains the queue in FIFO order, per mutation calling `rpcCall`:
- **Success** → `queue.remove(id)`, `store.removeOptimistic(id)`, drop the error handler. (Confirmed authoritative data is expected to arrive separately via the SSE subscription push refreshing the confirmed layer.)
- **`PulseClientError`** (handler-level rejection) → roll back: `queue.remove(id)`, `store.removeOptimistic(id)`, invoke and drop the `onError` handler.
- **Any other error** (network) → `break` out of the loop, leaving this and all later mutations queued for the next `flush()`.

The success/network distinction rides on `rpcCall` (`transport.ts`): it throws `PulseClientError` for HTTP-error/error-envelope responses and lets fetch's own rejection (no connectivity) propagate as a generic error.

## Alternatives Considered

- **Normalized document store keyed by `Id<"table">` (spec §5.1, "normalized keyed collections").** The spec describes a normalized cache where optimistic updaters call `store.insert("messages", doc)`. We chose a **per-query** cache (`path::input → docs[]`) with `getQuery`/`setQuery`/`tempId` instead. Simpler and directly matches what a subscription delivers (a query result list), but it means an optimistic insert that should appear in several queries must be written into each query explicitly, and there is no cross-query identity/dedup. **This is a deviation from §5.1/§5.3.**
- **Per-write snapshot/undo (capture old value, restore on rollback).** Rejected: doesn't compose when multiple writes touch the same query, and "receive fresh confirmed data" wouldn't rebase cleanly. Eager replay-on-top of confirmed handles confirm, rollback, and fresh-pull uniformly with one code path.
- **`lastMutationID` watermark + write-checkpoint guard (spec §5.4).** The spec rebases by replaying only mutations with `id > server.lastMutationId` and refuses to apply pulled confirmed state that would regress an unconfirmed local write. We instead rebase by **removing the specific mutation from `pending` on its own confirmation**, with no server watermark and no checkpoint guard. Simpler, and adequate while confirmation is one-RPC-per-mutation; it does not yet protect against a confirmed pull momentarily reverting a still-pending write (flicker). **Deviation from §5.4; deferred.**
- **One KV entry per queued mutation vs. a single JSON array.** Chose a single array under one key — trivial FIFO semantics and atomic persist. Cost: O(n) rewrite per mutation and full-array JSON parse on load; fine at expected queue depths, revisit if queues grow large.
- **Replace-on-error retains the mutation / retries handler errors.** Rejected: a handler rejection is deterministic, so retrying is pointless; we drop it and surface via `onError`. Only network errors retry.

## Consequences

Pros:
- No lost writes offline: durable enqueue happens *before* the network send, and both the queue and the id sequence are persisted, so a reload reconstructs the exact pending set and order.
- One uniform rebase path (`recompute`) handles optimistic apply, confirm, rollback, and fresh confirmed data; stable `tempId`s keep optimistic rows identity-stable across recomputes.
- Clean network-vs-handler-error split: transient failures retry automatically on the next flush; business rejections roll back and notify.
- Persistence is environment-agnostic and test-friendly (shared `InMemoryKV` simulates reload).

Cons / costs later:
- **Not wired up.** `createClient` (`client.ts`) only constructs `SyncClient`; nothing instantiates `LocalFirst`/`LocalStore`/`OfflineQueue` in the public client path, and `.mutationOptions()`/`mutationFn` still call `rpcCall` directly with no optimism or queue. Connecting these is outstanding work.
- **No automatic reconnect/reload flush.** `flush()` is invoked only from `mutate()`. There is no `window.addEventListener('online', ...)` and no flush-on-startup. Durability is real, but *replay* of a reload-restored queue requires an explicit `flush()` call that nothing currently makes. The "flush on reconnect/reload" requirement is therefore **not met by current code.**
- Per-query (not normalized) caching shifts the burden of multi-query fan-out onto each optimistic updater and forgoes cross-query identity.
- No `lastMutationID` watermark or write-checkpoint guard → possible flicker when a confirmed pull lands while a write is still pending.
- The full queue array is rewritten on every enqueue/remove.

## Testing Decisions

Tests should exercise **external behavior through the module interfaces**, never private fields (`view`, `confirmed`, `pending`, `flushing` are off-limits). Prior art: `packages/client/src/client.test.ts` uses Vitest with a `vi.fn()` fetch mock and a `jsonResponse` helper, and asserts on observable call args / resolved values — mirror that style.

What a good test looks like here:
- **`OfflineQueue` durability/order:** enqueue several mutations into an `InMemoryKV`; assert `all()` is FIFO; construct a *second* `OfflineQueue` over the **same** KV and assert it still returns the un-removed mutations (the reload case). Verify `remove` deletes by id and `size` tracks it.
- **`LocalStore` rebase:** `setConfirmed(key, [...])`, then `addOptimistic(id, updater)` that `setQuery`s a derived list; assert `getView`/`subscribe` reflect the overlay. Then `setConfirmed` fresh data and assert the pending updater re-applies on top (rebase). `removeOptimistic` and assert the view reverts to confirmed. Confirm a throwing updater leaves the cache intact. Assert `tempId` is stable across recomputes by reading it inside an updater and checking the same value appears after an unrelated `setConfirmed`.
- **`LocalFirst` paths:** with a mocked transport, assert (a) success removes from queue and drops the optimistic overlay; (b) a `PulseClientError` rolls back the overlay, removes from queue, and calls `onError`; (c) a thrown network error leaves the mutation queued and stops the drain so later mutations don't send out of order; (d) ids are monotonic and the seq survives a "reload" (new `LocalFirst` over the same KV continues numbering). Use a real `InMemoryKV` (it's already test-grade) rather than mocking `KVStore`.
- **`IndexedDbKV`** is not unit-tested here (needs a browser/IDB shim); `defaultKV()`'s fallback is covered implicitly by tests running under Node where `indexedDB` is undefined.

## Out of Scope / Deferred

- Wiring `LocalFirst` into `createClient` and TanStack Query (`mutationOptions`/`queryOptions`) so the public surface actually uses optimism + the queue.
- Flush triggers: `online` event, visibility/startup flush, and backoff/retry policy for repeated network failures.
- Server-side `last_mutation_id` watermark, rebase by `id > lastMutationID`, and the PowerSync-style write-checkpoint guard (spec §5.4).
- Cross-query LSN batch-advance and SSE-push-driven `setConfirmed` integration (spec §4, §5.5) — confirmation here assumes a separate sync path refreshes the confirmed layer.
- Normalized keyed collections and an `insert`/`increment`-style intent API (spec §5.1/§5.3); current overlay is per-query list replacement.
- Encryption/compaction of persisted KV, and queue size limits / eviction.
