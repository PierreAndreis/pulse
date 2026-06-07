# 10. Reactor Precision, Tuning Knobs, and the Recompute Frontier

- **Status:** Accepted — closes several cons ADR 05 booked ("over-invalidation", "re-execution cost is the hot path") and records the tuning philosophy and a benchmark-grounded map of where Pulse actually hurts. Tracked in `docs/ARCHITECTURE.md` §10 and `docs/TUNING.md`.

## Context & Problem

ADR 05's M2 matcher was deliberately coarse: a write to a table re-ran **every** subscription on that table, and matching scanned **every** subscription. Two costs follow — wasted re-executions on hot tables, and a match step that's O(total subs). Separately, the engine's behavior was governed by hardcoded constants (NOTIFY cap, heartbeat/TTL, sample intervals, buffer sizes) with no way to tune them. And a recurring question kept coming up: *for a given bad query, what actually dominates — the bus, the matching, or the recompute?* We needed numbers, not intuition.

## Decision

**Precision.** Three changes cut wasted re-execution without changing *which* subscriptions are correct to invalidate:
- **Table-indexed matcher** — the reactor keeps a `table → subscriptions` index; `apply_change_set` only tests subs referencing a touched table. The precise per-row `matches` still runs on candidates, so the index changes *how few* we test, not *which* match. Match cost now scales with subs-on-the-changed-table (benchmarked flat ~5 µs as idle subs grow 0→100 k), not the global count.
- **Column-level read pruning** — an aggregate's read-set records the value columns it reads. A value-only update to a matching row is skipped unless one of those columns (or filter membership) actually changed, so a reactive `count()` ignores updates that don't move a row in/out of its filter.
- **O(1)-space `LIKE`/`ILIKE`** — rewritten with zero-alloc fast paths (`foo%`/`%foo`/`%foo%`) + a greedy two-pointer fallback, since it runs per-change-per-filter.

**Tunability with great defaults.** Expose the operationally-relevant constants as env knobs, each defaulting to the measured-best behavior, documented in `docs/TUNING.md`: `PULSE_BUS_CHUNK`, `PULSE_BUS_BROADCAST`, `PULSE_INTEREST_TTL_SECS`, `PULSE_HEARTBEAT_MS` (clamped ≤ TTL/3 so interest can't lapse), `PULSE_WAL_SAMPLE_MS`, `PULSE_SSE_BUFFER` (plus the existing pool/timeout/retry knobs). Principle: **the default is the right answer; the knob is for when a measured bottleneck says otherwise.** `GET /metrics` exposes per-node `busEvents`/`changes`/`resyncs` and a `busLagMs`/`applyMs` latency split so those bottlenecks are observable, not guessed.

**Measure the bad cases.** A `tests/load/` bottleneck atlas drives a realistic shop workload (orders/line-items/products/an activity log) into each failure mode and records the cost, so the recompute frontier is grounded in data rather than asserted.

## What the numbers said

- **Bus is cheap; recompute is the cost.** Cross-node latency decomposes to ~1 ms propagation + ~2 ms re-exec; the write cost of all the cross-node machinery is ~nil (routed ≈ broadcast ≈ ~830 ops/s). The dominant cost is that Pulse has **no incremental view maintenance** — every invalidation re-runs the whole query.
- **Simple aggregates are fine** (~13 ms over 100 k rows); the no-IVM cost only bites for *expensive* queries.
- **Dedup is the quiet win** — identical `(path,input,headers)` subscriptions coalesce to **one** re-exec; cost scales with *distinct* matching views, not subscriber count.
- **N+1 joins are the real per-event cost** — handler composition (`query` + per-row `get`) is linear, ~0.5 ms/item, paid every re-exec.
- **Hot-row writes serialize** (~300 vs ~800 ops/s spread) — inherent to SERIALIZABLE, absorbed by the `40001` retry budget.

## Alternatives Considered

- **Per-sub coalescing/debounce window** to cut the *number* of recomputes under write storms. Deferred — it touches the core invalidation path under concurrency (risk of a missed final re-exec = a stale subscriber); left as a future knob (default off).
- **Hashing the retained last-value** to cut per-sub memory. Rejected — a hash collision suppresses a genuine push (silent staleness); correctness over memory.
- **General incremental view maintenance now.** The real fix for recompute, but a large engine that constrains supported SQL (ADR 05 / §4.1 deferred it). The tractable subset — incremental `sum/count/avg` from the change delta, and batched `get`s for N+1 — is the identified next seam, not this ADR.
- **A SQL `JOIN` instead of composition** to kill N+1. Trades the precise per-row read-set for a coarse two-table dependency; offered as a future option, not the default.

## Consequences

Pros: match cost no longer scales with global sub count; aggregates skip irrelevant updates; the system is tunable for real deployments with defaults that need no tuning; the bottleneck map turns "it feels slow" into a specific, measured cause.

Cons: the table index and per-sub `last_lsn`/read-cols add bookkeeping and a little memory per subscription; the knobs are surface area (kept minimal, all defaulted); and the headline finding stands — **delivery is cheap and precise, but an expensive query is still expensive to recompute.** Closing that (batched gets, then incremental aggregates) is the genuine frontier.

## Testing Decisions

- **Matcher (unit):** table-indexed `apply_change_set` re-execs only the changed-table subs; reindex on a read-set shift (dynamic dep) is covered; column-pruning skips value-only updates to unread columns but fires on membership flips; the `LIKE` rewrite is checked against a brute-force oracle across a string/pattern matrix.
- **Knobs (unit):** numeric env parsing (default/override/trim/garbage/empty); the SSE buffer is configurable and a tiny buffer doesn't wedge other clients.
- **Bottleneck atlas (load, opt-in):** each scenario prints measured numbers and asserts only loose sanity, so it documents reality without flaking; the chunk knob's effect is shown A/B (off → coarse resync, on → precise).
- **No regression:** all changes default to prior behavior; the full workspace + integration + load suites stay green.

## Out of Scope / Deferred

- **Incremental view maintenance** for aggregates and filtered lists (the recompute fix).
- **Batched `get`s / DataLoader** in the worker to collapse N+1 joins to one `IN` query.
- **Per-subscription coalescing/debounce** for rapid same-sub invalidations.
- **Delta push** (changed rows only) for large results, and the retained-last-value memory it would shrink.
- **Analyzer precision** — JSONB ops, true index-range tuples, and a declarable read-set for `ctx.sql` so the escape hatch isn't a recompute magnet.
