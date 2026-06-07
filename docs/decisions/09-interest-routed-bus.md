# 09. Horizontal Scale-Out — Interest-Routed `LISTEN/NOTIFY` Bus, Chunked Publish, Sampled Commit-LSN

- **Status:** Accepted — extends `docs/ARCHITECTURE.md` §10 ("Horizontal scaling") and revisits two things ADR 05 left open: it declared multi-instance reactor state **out of scope** ("single-process … does not span multiple engine instances"), and it **rejected `LISTEN/NOTIFY`** as the change source (§4.1: lossy, no replay, 8 KB cap, connection-per-listener). This ADR records the multi-node design and why NOTIFY is the right tool *for the cross-node bus role specifically* even though it was the wrong tool for the *CDC source* role.

## Context & Problem

ADR 05 built the reactor as a single in-process `Mutex<HashMap>` of subscriptions: invalidation is `read-set ∩ write-set` within one engine. That proves the thesis but caps you at one node — a subscriber on engine B never learns about a write committed on engine A.

The forcing question: *what is the smallest thing that makes a write on any node invalidate matching subscriptions on every node, without rebuilding the reactor and without standing up Kafka/Redis?* Three sub-problems fall out of choosing Postgres `LISTEN/NOTIFY` as the transport (the only zero-infrastructure option for a bring-your-own-Postgres product):

1. **Fan-out cost.** A naive broadcast NOTIFY wakes *every* node on *every* write. Measured, that's N−1 wasted deliveries/write (O(N)) — most nodes have no subscription on the changed table and do nothing but burn a match.
2. **The 8 KB NOTIFY payload cap** (the exact reason ADR 05 rejected NOTIFY). A multi-row transaction's `ChangeSet` blows past it.
3. **No replay / no ordering.** NOTIFY is fire-and-forget; a dropped listener silently misses the gap, and there's no commit position to give clients a consistency watermark.

## Decision

A **cross-node change bus in `pulse-cdc`** over Postgres `LISTEN/NOTIFY`: after a local mutation a node applies invalidation locally **and** publishes the committed `ChangeSet`; peers feed foreign change-sets into their own `apply_change_set`, so per-read-set matching is preserved across nodes (the bus carries row images). Each node drops its own messages (`node_id`), so the bus is purely additive — single-node behavior is unchanged. Three mechanisms answer the sub-problems:

**1. Interest routing (not broadcast).** Each node records the tables it currently has live subscriptions on in a `_pulse_node_interest(node_id, table_name, updated_at)` registry. A publisher resolves the changed tables → interested nodes and NOTIFYs only their **per-node channels** (`pulse_n_<id>`, id with hyphens stripped). So fan-out is O(interested nodes). The reactor registers interest the *instant* it first watches a table (synchronous `InterestSink` on the first subscription for that table), and a one-shot **catch-up re-exec** after registration closes the subscribe-vs-concurrent-remote-write race. A **heartbeat** (default 10 s, clamped ≤ TTL/3) refreshes interest and prunes dead nodes; interest **expires** after `INTEREST_TTL_SECS` (default 30). The interest lookup and every NOTIFY for a publish ride **one pooled connection**. `PULSE_BUS_BROADCAST=1` forces the old broadcast as a kill-switch/baseline.

**2. Chunked publish (the 8 KB answer).** An oversized `ChangeSet` is split into consecutive **precise** `Changes` messages each fitting the cap (`PULSE_BUS_CHUNK`, default on), each routed to the nodes interested in the tables *it* touches. Only a single change larger than the cap *alone* falls back to a scoped `ResyncTables` (touched tables → coarse re-eval); a write spanning thousands of distinct tables falls all the way back to a global `Resync`. This is what makes NOTIFY's payload cap a non-issue for normal bulk writes.

**3. Sampled commit-LSN watermark + reconnect.** Each `ChangeSet` carries a real commit position sampled off the write path (`current_wal_lsn` → `pg_current_wal_insert_lsn`, default every 100 ms into an atomic) so it never lengthens the mutation tx; the reactor clamps each subscription's emitted `commitLsn` to be monotonically non-decreasing. A dropped listener reconnects with backoff and emits a `Resync` to recover the gap — turning NOTIFY's "no replay" into bounded over-recompute rather than lost updates.

## Alternatives Considered

- **Broadcast NOTIFY to all nodes.** Simplest; correct. Rejected as the default because fan-out is O(N) — measured N−1 deliveries/write, which is pure waste at scale. Kept as `PULSE_BUS_BROADCAST` (kill-switch + the benchmark baseline that proves routing's win).
- **Redis / NATS / Kafka bus.** Better throughput and built-in fan-out/replay. Rejected for v1: it breaks "bring-your-own-Postgres, no extra infrastructure." NOTIFY is good enough until throughput says otherwise (a documented future swap).
- **WAL/`pgoutput` as the bus now.** The canonical lossless source (and the only one that also catches out-of-Pulse writes). Still deferred (per ADR 05): it's the heaviest plumbing, and it slots into the *publish* step later without touching the receive side. The sampled watermark is the cheap interim for the exact per-commit LSN it would provide.
- **In-tx LSN read** (instead of a background sampler). More precise per-commit, but adds a round-trip *inside* the SERIALIZABLE tx — measured to cut write throughput. A 100 ms sampled value is monotonic (WAL only advances) and sufficient for a watermark; rejected the precise-but-costly option.
- **Drop coarse subscriptions when oversized** vs chunking. Coarse resync is simpler but re-runs *every* sub on the table for any bulk write. Chunking keeps precision; chosen as the default, with resync as the bounded fallback.

## Consequences

Pros:
- Genuinely multi-node with **zero new infrastructure** — the routing table is a Postgres table, the transport is NOTIFY.
- Cross-node fan-out is O(interested nodes), not O(cluster): measured **ratio ≈ N** (8× at 8 nodes, ~30× at 32), and routing is **not** slower on writes (≈ broadcast, ~830 ops/s — fewer NOTIFY fan-outs).
- Bulk writes stay **precise** cross-node (chunking) instead of triggering cluster-wide coarse recompute.
- Clients get a real, monotonic `commitLsn`; the bus is no longer the latency bottleneck (~1 ms propagation, measured).
- The receive path (`apply_change_set`) is unchanged — the future WAL consumer still swaps only the publish step.

Cons / costs:
- **Correctness becomes liveness-dependent.** If a node's heartbeat stalls past the TTL, its interest expires and peers stop routing to it → it silently misses invalidations until it recovers. Broadcast had no such state. (Mitigated by the TTL/3 heartbeat clamp; reconnect-Resync covers connection drops.)
- **Dynamic-dependency window.** Interest is registered when the reactor first watches a table; a re-exec that begins reading a *new* table has a brief window before that table's interest propagates. The subscribe-time catch-up covers the initial case, not every dynamic shift.
- **Per-write registry query.** Routing costs one interest lookup + the NOTIFY on a pooled connection per publish (off the RPC critical path). An index on `_pulse_node_interest(table_name, updated_at)` keeps it cheap as the cluster grows.
- **Still NOTIFY-bound.** A single global async queue and per-payload cap remain the ceiling; the Redis/NATS/WAL swap is the escape hatch if throughput demands it.

## Testing Decisions

Verify **observable cross-node behavior through real engine processes against one Postgres**, not the registry internals or NOTIFY bytes. Pure helpers (chunking, interest SQL, routing) are unit-tested directly; the bus is tested end-to-end.

- **Routing correctness (cdc integration):** a node interested in a table receives a routed change; a node *not* interested is **not woken** (the core sharding claim).
- **Interest registry (live PG):** register/`interested_nodes`/TTL filtering/prune behave; a node is never routed its own change.
- **Chunking (unit):** an over-cap change-set splits into cap-fitting `Changes` with **no change lost or duplicated**; chunk-off degrades to `ResyncTables`; a single over-cap change resyncs.
- **Scaling (load):** N engines each on a distinct table — broadcast = N−1 events/write, routing ≈ 1/write, ratio grows monotonically with N; replicated over a real Docker network.
- **Resilience (load):** restart Postgres mid-stream → a subsequent write still reaches a cross-node subscriber (listener reconnect + Resync).
- **Watermark (unit + e2e):** emitted `commitLsn` never regresses on out-of-order or zero-stamped invalidations; raw-SSE asserts real non-zero, monotonic LSNs flow end-to-end.
- **No regression:** the full single-node integration/stress/soak suite stays green with the bus on (it's additive).

## Out of Scope / Deferred

- **WAL/`pgoutput` consumer** — out-of-Pulse writes; the exact per-commit LSN; replaces the publish step, receive side unchanged. (ADR 05, §4.3)
- **SSE connection affinity** at the load balancer, and a **sharded subscription registry** for very large per-table fan-out.
- **A non-NOTIFY bus** (Redis/NATS/WAL) if NOTIFY throughput becomes the ceiling.
- **Closing the dynamic-dependency interest window** and the liveness-vs-correctness trade (e.g. a fallback periodic full resync).
- **Client-side cross-query LSN batch-advance** that consumes the watermark (§4.4).
