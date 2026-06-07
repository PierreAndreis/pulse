# Pulse — Context

Pulse is a reactive, local-first application platform on standard Postgres: a Rust
sync engine plus a TypeScript SDK. This file names the engine concepts that aren't
obvious from the crate names. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for
the system shape and [docs/decisions/](docs/decisions/) for the decisions behind it.

## Language — engine orchestration

**Coordinator**:
The deep module (in `pulse-engine`) that owns the engine's request and cross-node
orchestration — running a procedure, propagating a mutation, registering a
subscription, applying a remote invalidation, beating the interest heartbeat. The
axum handlers in `pulse-server` are thin adapters over it.
_Avoid_: handler, service, manager.

**Seam**:
A place behaviour can be altered without editing in place — a trait the
`Coordinator` depends on, satisfied by a host adapter in `pulse-server`. The four:
`Executor`, `Reactor`, `Publisher`, `InterestRegistry`.
_Avoid_: boundary, interface (when you mean the trait slot).

**Executor**:
The seam that runs a procedure and returns its value, read-set, and write-set
(`changes`). Richer than the reactor's `ReExecutor`, which drops the write-set
because re-execution is reads only. Host-adapted over the JS-runtime worker.

**Publisher**:
The seam that publishes a committed change-set to other nodes. Best-effort: a
failed publish is logged, never surfaced (local delivery already happened; a missed
cross-node message is recovered by resync). Host-adapted over the change bus.

**InterestRegistry**:
The seam the heartbeat uses to keep this node's cross-node interest fresh
(`refresh` the watched tables, `prune` dead nodes). Distinct from the reactor's
`InterestSink`, which fires the instant a table is first watched. One host adapter
(`BusInterest`) satisfies both.

**Invalidation**:
A cross-node invalidation lifted off the wire format (`Changes` / `Tables` / `All`)
so `pulse-engine` needn't depend on the bus crate. The host's listener translates
bus events into it before handing it to the `Coordinator`.
_Avoid_: bus event (that's the wire form the host translates from).

**propagate**:
The `Coordinator` step that folds a committed mutation into local subscribers in
parallel with the routed cross-node publish — run by the host off the RPC's
critical path via `tokio::spawn`.
