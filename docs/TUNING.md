# Tuning & knobs

Every knob below has a **great default** — Pulse runs well out of the box. Reach for
these only when a measured bottleneck (see [the bottleneck atlas](#bottleneck-atlas))
tells you to. All are environment variables on the engine process.

## Cross-node bus

| Env | Default | What it does | When to change |
|---|---|---|---|
| `PULSE_BUS_CHUNK` | `1` (on) | Splits an oversized change-set into precise sub-messages instead of degrading cross-node delivery to a coarse table resync. | Leave **on**. Turn off only to reproduce the legacy 8 KB-cap behavior. Fixes bottleneck **D** (a tx of ≳8 rows otherwise triggers a coarse resync). |
| `PULSE_BUS_BROADCAST` | `0` (off) | Forces every change to broadcast to all nodes instead of interest-routing. | Kill-switch if interest routing ever misbehaves; otherwise leave off — routing is O(1)/write vs broadcast's O(N) (see scaling curve). |
| `PULSE_INTEREST_TTL_SECS` | `30` | How long a node's table-interest stays "live" without a heartbeat. Publishers route only to nodes fresh within this window. | Raise on clusters with slow/janky nodes that miss heartbeats; keep consistent across the cluster. |
| `PULSE_HEARTBEAT_MS` | `10000` | How often a node refreshes its interest + prunes dead nodes. Auto-clamped to ≤ `TTL/3`. | Lower for faster dead-node cleanup / new-table pickup; it must stay well under the TTL or a live node's interest lapses. |
| `PULSE_WAL_SAMPLE_MS` | `100` | How often the commit-watermark (`commitLsn`) is sampled from the WAL, off the write path. | Lower for finer watermark granularity (at a small extra query rate); raise to reduce background queries. |

## Reactor / delivery

| Env | Default | What it does | When to change |
|---|---|---|---|
| `PULSE_SSE_BUFFER` | `256` | Per-client SSE event buffer. A client whose buffer fills (a stalled browser) is dropped. | Raise to tolerate burstier/slower clients (costs memory per client); lower to shed slow clients sooner. |

## OLTP / OLAP pools

| Env | Default | What it does | When to change |
|---|---|---|---|
| `PULSE_OLTP_MAX_CONNS` | `10` | Mutation + reactive-read connection pool size. | Raise for higher write/read concurrency (bounded by Postgres `max_connections`). |
| `PULSE_OLAP_MAX_CONNS` | `4` | Analytical-query pool (separate budget so heavy analytics can't starve the reactive hot path). | Raise for more concurrent analytics. |
| `PULSE_OLTP_STATEMENT_TIMEOUT_MS` | `15000` | Per-statement timeout on the OLTP pool — a slow query can't pin a reactive connection. | Lower to fail slow reactive queries faster; `0` disables. |
| `PULSE_OLAP_STATEMENT_TIMEOUT_MS` | `60000` | Per-statement timeout on the OLAP pool. | Raise for very long analytics; `0` disables. |
| `PULSE_MAX_TX_ATTEMPTS` | `25` | SERIALIZABLE retry budget before surfacing `CONFLICT`. | Lower to surface contention as errors sooner; raise to absorb more hot-row contention (bottleneck **E**). |

## Process / app

`DATABASE_URL`, `PULSE_OLAP_DATABASE_URL` (defaults to `DATABASE_URL`), `PULSE_PORT`
(`8787`), `PULSE_WORKER_BIN` (`bun`), `PULSE_WORKER_SCRIPT`, `PULSE_APP`.

## `/metrics`

Each node exposes `GET /metrics`: `busEvents` (cross-node events received),
`changes`/`resyncs` (precise vs coarse), `busLagMs` (commit→deliver) and `applyMs`
(deliver→applied) latency decomposition, plus the active `broadcast` mode.

## Bottleneck atlas

Measured costs that motivate the knobs (see `tests/load/bottlenecks.test.ts`,
`scaling.test.ts`, `replication.test.ts`):

- **A. Heavy re-exec** — reactive queries re-run fully on each change (no incremental
  view). Simple aggregates are cheap (~13 ms over 100 k rows); expensive queries are not.
- **B. Fan-out** — identical subscriptions coalesce to **one** re-exec; distinct ones
  each re-execute. Cost scales with the number of *distinct* matching views.
- **C. N+1 join** — handler-composition joins are linear (~0.5 ms/item; 200 items ≈ 100 ms)
  and paid on every re-exec.
- **D. Bulk write** — a tx over the NOTIFY cap degrades to coarse resync — *unless*
  `PULSE_BUS_CHUNK` (default on) keeps it precise.
- **E. Write contention** — same-row writes serialize (~300 ops/s); spread across rows
  they scale (~800 ops/s). Conflicts are absorbed by `PULSE_MAX_TX_ATTEMPTS` retries.

Routing scaling (`scaling.test.ts`): broadcast = N-1 events/write (O(N)); routing ≈ 1/write
(O(1)) → savings ratio ≈ N (8× at N=8, ~30× at N=32). Cross-node latency decomposes to
~1 ms bus propagation + ~2 ms remote re-exec.
