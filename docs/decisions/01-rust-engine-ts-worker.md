# 01. Rust Engine + TS Handlers in a Node/Bun Worker (Embedded V8 Deferred)

- **Status:** Accepted — deviation from `docs/ARCHITECTURE.md` §7.1, which targets an embedded V8 isolate pool (`deno_core`) for queries/mutations. This ADR records why M1 ships a Node/Bun worker instead and defers embedded V8 to M4. The deviation is already documented in `ARCHITECTURE.md` §10.

## Context & Problem

The spec's target runtime for `query`/`mutation` handlers is an **embedded V8 isolate pool inside the Rust engine** (`deno_core`/`rusty_v8`). That design buys ~ms isolate startup and a deterministic, sandboxed `ctx` injected directly across the FFI boundary — which matters because reactive queries are re-executed *constantly* for invalidation (the hot path).

But embedding V8 is, per the spec itself, "the heaviest single dependency and the trickiest part of the build." The guiding roadmap principle is to **get one reactive query updating one browser end-to-end as early as possible (M2)**. Standing up `deno_core`, the FFI bridge, an isolate pool, and a deterministic sandbox before we could prove the reactivity thesis would have front-loaded the riskiest engineering against the least-validated design.

The forcing question: *what is the smallest runtime that still lets Rust own the query engine and the read/write-set capture point, so M2 can be reached without committing to V8 embedding first?*

## Decision

Run user TS `query`/`mutation` handlers in a **Node/Bun worker process** that the engine spawns and supervises. Rust remains the query engine: the worker's instrumented `ctx.db` does **not** touch Postgres directly — it proxies every db operation back to the engine over an NDJSON line protocol on stdio, and the engine executes it via `pulse-sql`. This keeps SQL lowering, Postgres ownership, and the read/write-set capture point inside Rust.

Responsibility split:

- **Rust engine (`pulse-jsruntime`)** — spawns the worker (`bin script app`), waits for its manifest, introspects the DB schema into a `Catalog`, then for each `Execute` request routes db ops to `pulse-sql::execute_op`, records the read/write-set, and returns the handler result.
- **TS worker** — loads the user app module, reports a manifest of procedures + schema, and runs handlers. Its `ctx.db` is a proxy that emits `dbop` messages and awaits `dbresult`.

The worker↔engine protocol (NDJSON over stdio; one JSON object per line):

```
Engine → Worker:
  Execute  { request_id, path, input, headers }
  Dbresult { request_id, op_id, ok, value?, error? }

Worker → Engine:
  Manifest { procedures: [{ path, kind }], schema }   // sent once at startup
  Ready                                                // catalog built; accept Execute
  Dbop     { request_id, op_id, op }                   // proxied db operation
  Complete { request_id, ok, result, error? }          // handler finished
  Log      { level, message }
```

Read/write-set capture happens in the engine, not the worker. When a `Dbop` arrives, the engine inspects `op.access()` → `(table, is_write)` and folds it into a per-request `Capture { reads, writes }` **before** servicing the op, so the set is complete before the worker can send `Complete`. The result carries the set out:

```rust
pub struct ExecResult {
    pub value: Value,
    pub reads: Vec<String>,    // tables read  → reactive read-set
    pub writes: Vec<String>,   // tables written → invalidation write-set
}
```

Concurrency/lifecycle behaviors that are part of the decision:

- The worker is long-lived; the `Worker` handle owns the `Child` to keep it alive.
- Requests are correlated by a `request_id` (UUID); a single `reader_loop` demultiplexes worker output to per-request `oneshot` channels.
- Db ops are serviced concurrently (`tokio::spawn` per `Dbop`); `request_id`/`op_id` keep replies correlated.
- Startup is gated: the engine waits for `Manifest` (to introspect the catalog) and `Ready` before accepting executions, with a 30s manifest timeout (`RuntimeError::ManifestTimeout`).
- The `WorkerConfig.bin` is the only thing distinguishing "Node" from "Bun" — both speak the same protocol.

Stdout is the protocol channel; per `ARCHITECTURE.md` §10, user `console.*` is redirected to stderr so handler logging cannot corrupt the NDJSON stream. The child's stderr is inherited.

Embedded V8 (`deno_core`) plus the deterministic sandbox (frozen time, seeded RNG, no net/fs) remains the **M4** upgrade, kept behind this crate's interface so the swap does not ripple outward.

## Alternatives Considered

- **Embed V8 now (`deno_core`/`rusty_v8`) — the spec target.** Best steady-state hot-path performance and the only path to an in-process deterministic sandbox without IPC. Rejected for M1: it is the heaviest dependency and the trickiest build, and committing to it before M2 proves the reactivity thesis front-loads the most risk against the least-validated design. Deferred to M4, behind the `pulse-jsruntime` interface.
- **Run handlers in the worker with the worker talking to Postgres directly (its own pool).** Simpler — no db-op proxy protocol. Rejected because it moves SQL lowering and the read/write-set capture point out of Rust, breaking the "Rust is the query engine" invariant. Read-set capture is the foundation of reactivity; it must live where the engine can see every access. Proxying db ops back over NDJSON is the price of keeping that capture point in Rust.
- **One worker process per request (spawn-per-call).** Trivial isolation, no demux. Rejected: process startup on every invocation is unacceptable on the invalidation re-run hot path — the same cost argument the spec makes against running *all* TS in Node workers.
- **Length-prefixed binary framing (e.g. `rmp-serde`) instead of NDJSON on stdio.** The spec's action-IPC choice. Deferred: NDJSON over stdio is debuggable and sufficient for M1/M2 throughput; the action runtime (`pulse-actions`, M6) is where binary framing earns its keep.

## Consequences

Pros:
- Reached the M2 reactive slice without the V8 embedding build, validating the reactivity thesis on a simpler runtime.
- Rust still owns Postgres, SQL lowering, and read/write-set capture — "Rust is the query engine" holds, so M2's reactor consumes `ExecResult.reads/writes` exactly as the embedded-V8 design would have produced them.
- Crash isolation: a wedged handler is a separate process, not in-engine.
- Node *and* Bun are interchangeable (just `WorkerConfig.bin`).

Cons / costs later:
- **IPC on the hot path.** Every db op is an NDJSON round-trip over stdio; every reactive re-execution pays process-boundary cost. This is precisely the overhead the embedded-V8 design exists to remove, and it is the standing reason M4 still targets `deno_core`.
- **No determinism sandbox.** The worker can call `Date.now()`/`Math.random()`/`fetch`, so re-execution is not yet provably identical — the soundness guarantee that result caching and invalidation ultimately depend on is owed until M4.
- **Single shared worker.** No isolate pool; one worker handles all concurrent executions. Db ops are serviced concurrently, but handler JS shares one process/event loop.
- **Protocol fragility.** Correctness depends on nothing else writing to the worker's stdout. The console→stderr redirect mitigates this; a dedicated protocol fd (to also guard direct `process.stdout.write`) is deferred (§10).
- **The M4 swap is owed.** When `deno_core` lands, the read/write-set capture mechanism moves from "inspect proxied db ops in the engine" to "capture across the FFI boundary." The `ExecResult` shape is designed to survive that swap; the capture *path* changes.

## Testing Decisions

Test the **external behavior of the crate's public interface** (`Worker::spawn` / `Worker::execute` → `ExecResult`), not the NDJSON wire format or the `reader_loop` internals — those are implementation details free to change when V8 lands.

A good test here drives a real worker against real Postgres and asserts observable outcomes:
- `execute` of a mutation persists rows and returns the handler value;
- `execute` of a query returns the expected rows;
- the returned `reads`/`writes` sets contain exactly the tables the handler touched (this is the contract M2's reactor depends on);
- handler errors surface as `WorkerError` (including structured `data`), and a dropped/failed worker yields an internal error rather than a hang.

Prior art is the end-to-end M1/M2 verification described in `ARCHITECTURE.md` §10: example-chat `send`/`list` exercised **through `@pulse/client` against real Postgres**, plus stress tests (concurrent load, pool saturation, worker backpressure, 15s soak) and the M2 reactivity tests (write in one client → pushed to a separate subscriber; multi-client fan-out; per-subscription isolation; unsubscribe stops pushes). New runtime behavior should be verified at that same level — through the client, end to end — rather than by asserting on protocol frames.

## Out of Scope / Deferred

- **Embedded V8 isolate pool (`deno_core`/`rusty_v8`) and the deterministic sandbox** (frozen time, seeded RNG, no net/fs) — the M4 target, kept behind this crate's interface.
- **Mutation transactions** — db ops are autocommit per op today; one `SERIALIZABLE` tx + `40001` retry is M4.
- **Dedicated protocol fd** to fully isolate the channel from direct `process.stdout.write` — deferred (§10); today guarded only by the console→stderr redirect.
- **Action runtime** (`pulse-actions`, M6) — a separate Node worker pool with its own IPC (length-prefixed framing) and `ctx.runQuery/runMutation` re-entry; out of scope for this crate.
- **Binary IPC framing** for this Q/M runtime — NDJSON stays until/unless profiling demands otherwise.
