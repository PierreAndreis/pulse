# 02. NDJSON stdio protocol; user console routed to stderr

- **Status:** Accepted. This is consistent with `docs/ARCHITECTURE.md` (Milestone M1 and the "Decisions / deviations" notes, which already record both the Node/Bun-worker choice and "stdout is the worker↔engine protocol channel; user `console.*` is redirected to stderr"). It is *not* the spec's long-term target runtime: ARCHITECTURE.md's target is an embedded V8 isolate pool inside the engine, kept behind the `pulse-jsruntime` interface for the M4 upgrade.

## Context & Problem

The engine (Rust) does not run user TS query/mutation handlers in-process today. To reach the reactive slice fastest, M1 drives handlers in a separate Node/Bun worker process that the engine spawns and supervises (`crates/pulse-jsruntime/src/lib.rs`). The two processes must exchange three things across a process boundary:

1. The engine must tell the worker which procedure to run, with input and request headers.
2. Mid-handler, the worker's instrumented `ctx.db` / `ctx.sql` must reach back to the engine, which owns Postgres, to execute each database operation and return its rows — so Rust stays the query engine and the read/write-set capture point.
3. The worker must report the handler's final result or structured error.

This needs a wire format and a correlation scheme so that concurrent requests, and the multiple db ops *within* each request, never get crossed. It also needs a transport. The transport chosen is the worker's stdio: the engine writes the worker's stdin and reads its stdout.

That last choice creates a hazard. The worker loads arbitrary user code. User handlers will call `console.log`. Node's `console.log` writes to the process's stdout — the exact byte stream the engine is parsing as the protocol. A single user log line is a non-JSON line (or worse, interleaves bytes mid-line) that corrupts the channel. In a soak/stress run with handlers that log, this manifests as the engine receiving unparseable lines, dropped correlations, and stalled requests. The fix had to make the protocol channel robust to user output.

## Decision

**Transport + framing.** One JSON object per line (NDJSON), UTF-8, `\n`-delimited, over the worker's stdio. The engine spawns the worker with stdin and stdout piped and **stderr inherited** (`Stdio::inherit()`), so the worker's stderr flows straight to the engine process's stderr / the operator's terminal. Empty/whitespace-only lines are ignored on both sides; an unparseable line is logged (engine: `tracing::warn`; worker: a `log` protocol message) and skipped rather than killing the stream.

**Message set.** Two directions, discriminated by a `type` tag, fields in `camelCase` on the wire:

Worker → engine (`WorkerOut`):
- `manifest { procedures: [{ path, kind }], schema }` — sent once at startup.
- `ready` — sent after the manifest; the engine treats the worker as serveable only after this.
- `dbop { requestId, opId, op }` — a database op a handler is awaiting.
- `complete { requestId, ok, result?, error? }` — handler finished (success result or structured `error { code, data, message? }`).
- `log { level, message }` — a structured log line from the worker itself (not user `console.*`).

Engine → worker (`EngineMsg`):
- `execute { requestId, path, input, headers }` — run a procedure.
- `dbresult { requestId, opId, ok, value?, error? }` — the result of one db op.

**Correlation (two levels).**
- *Per request:* the engine mints a UUID `requestId` per `execute`, registers a `oneshot` waiter and a fresh read/write-set `Capture` under it, and resolves the waiter when the matching `complete` arrives. The worker echoes `requestId` on every `dbop` and `complete`.
- *Per op within a request:* the worker mints a process-local monotonically increasing `opId` (`++opSeq`) per db op and tracks a pending-promise map keyed by `opId`; the engine echoes the same `opId` back on `dbresult`, and the worker resolves/rejects that exact promise. `requestId` namespaces the conversation; `opId` disambiguates the many concurrent ops inside one handler.

**Read/write-set capture happens engine-side, before the reply.** When a `dbop` arrives, the engine records the touched table into the request's `Capture` (write vs. read, via `op.access()`) **before** it executes the op and sends `dbresult`. This guarantees the capture is in place before the worker can possibly send `complete`, so invalidation sees the full set.

**Console routed to stderr (the soak-bug fix).** At worker startup, before any user module is imported, the worker overrides `console.log/info/warn/error/debug` to serialize their arguments and write to **stderr** (`process.stderr.write`), with `[warn]`/`[error]`/`[debug]` prefixes; `log`/`info` get no prefix. Stdout is reserved exclusively for the NDJSON protocol. The worker additionally serializes all its own protocol writes through a single promise chain (`writeChain`) so concurrent `send()` calls cannot interleave bytes on stdout.

```ts
// worker startup, before importing user code
const toStderr = (prefix: string) => (...args) =>
  process.stderr.write(prefix + args.map(stringify).join(" ") + "\n");
console.log = toStderr(""); console.warn = toStderr("[warn] "); /* …error/debug/info */
```

## Alternatives Considered

- **Embedded V8 isolate pool in the engine (the spec's target).** No process boundary, no stdio, no console-corruption class of bug at all. Rejected *for M1* because embedding V8 (`deno_core`) plus a deterministic sandbox is the single heaviest dependency and the trickiest part of the build; doing it first would have blocked the reactive slice. ARCHITECTURE.md keeps it as the M4 upgrade behind the `pulse-jsruntime` crate interface, so this decision is reversible without touching callers.
- **Leave `console.*` on stdout and try to filter on the engine side.** Rejected: there is no reliable way to distinguish a user log line from a protocol line once they share the stream, and partial writes can corrupt a real protocol line mid-flight. Filtering is a guess; redirecting is a guarantee.
- **A dedicated protocol file descriptor (e.g. fd 3) for NDJSON, leaving stdout entirely to user code.** This is strictly more robust — it also defends against a user calling `process.stdout.write` directly (which the console override does *not* catch). Deferred, not rejected; noted in the worker comment and in ARCHITECTURE.md as future hardening. The console redirect covers the overwhelmingly common case (`console.log`) at near-zero cost.
- **Length-prefixed binary framing over a unix socket** (what ARCHITECTURE.md sketches for the *action* worker pool). Heavier to implement and debug than line-delimited JSON. NDJSON is human-readable, trivially testable, and adequate for the q/m worker; the action pool can adopt the heavier transport independently.
- **A single global op counter with no per-request id.** Rejected: it cannot attribute db ops to a request, which is exactly what read/write-set capture needs. The two-level `requestId` + `opId` scheme is the minimum that supports concurrent requests *and* concurrent ops per request.

## Consequences

Pros:
- The protocol channel is robust to arbitrary user logging — the original soak failure mode (corrupted stream) is structurally prevented for `console.*`.
- NDJSON is debuggable by eye and with standard tooling; messages are self-describing via the `type` tag.
- Per-request `Capture` recorded before the db reply gives correct read/write-sets for reactive invalidation with no extra round-trip.
- The whole runtime sits behind the `pulse-jsruntime` interface, so swapping in embedded V8 later does not ripple into callers.

Cons / costs later:
- Every db op inside a handler is a full IPC round-trip (worker stdout → engine → Postgres → engine → worker stdin). This is the hot path for reactive re-runs and is precisely the cost ARCHITECTURE.md cites as the reason to embed V8 eventually.
- User `console.*` no longer appears on stdout and is reshaped (args stringified, level-prefixed) — it is operator/debug output on stderr, not a structured log stream. Object args are `JSON.stringify`'d (falling back to `String()` on failure), so fidelity is lossy.
- The console override does **not** catch direct `process.stdout.write` by user code; that hole remains until the dedicated-fd hardening lands.
- Correlation maps (`pending`, `captures`, `pendingOps`) are unbounded in the failure case — a `dbop` whose `requestId`/`opId` no longer has a waiter is silently dropped, and a worker that never sends `complete` leaves an entry until the process is torn down (no per-request timeout on the engine's `rx.await` today; only the 30s manifest/ready startup timeout exists).

## Testing Decisions

A good test here exercises **observable protocol behavior through the public interface**, not the private structs. The public surface is `Worker::spawn(WorkerConfig)` → `Worker::execute(path, input, headers) -> Result<ExecResult, WorkerError>` plus `procedures()`/`find()`. Tests should drive a real worker process against real Postgres (the M1 slice is explicitly verified end-to-end through `@pulse/client`) and assert on:

- Round-trip correctness: an `execute` returns the handler's result; a handler doing several `ctx.db` ops resolves each correctly (validates `opId` correlation).
- Correlation under concurrency: many simultaneous `execute` calls each get their own result and their own `ExecResult.reads`/`writes` (validates `requestId` namespacing and the pre-reply capture ordering).
- Error mapping: a handler throwing `PulseError`/validation failure surfaces as `complete { ok:false, error{code,data,message} }` and is returned as `WorkerError`; `NOT_FOUND` for an unknown path.
- **The soak-bug regression specifically:** a handler that calls `console.log` (and ideally logs heavily) must still return a clean result and must not corrupt the stream for concurrent requests. The assertion is that the protocol survives user logging — the channel stays parseable and all in-flight requests still complete. This is the test that would have caught the original bug.

ARCHITECTURE.md (M1) calls for exactly this kind of coverage: end-to-end through `@pulse/client` against real Postgres, plus stress tests (concurrent load, pool saturation, worker backpressure, and a 15s soak).

Prior art / current state: unit tests cover the pure pieces — `crates/pulse-core/src/{lsn,readset}.rs`, `crates/pulse-sql/src/naming.rs` (Rust `#[test]`), and TS suites in `packages/{client,server,schema}/src/*.test.ts`. The protocol itself is verified **behaviorally, end-to-end through the public `@pulse/client`** rather than by asserting on the wire format (the right altitude per the testing philosophy): the integration suite in `tests/integration/` (`roundtrip`, `errors`, `messages`, `analytical`, `reactive`, `offline`) and the stress suite in `tests/stress/` (`concurrent`, `pool-saturation`, `backpressure`, `soak`) drive real Execute/Dbop/Dbresult/Complete round-trips against a live engine + Postgres. The 15s soak is exactly what surfaced the stdout-corruption bug that the console→stderr redirect fixes. `crates/pulse-jsruntime` has no Rust-level unit tests (deliberately — the manager is covered through the public client); a focused wire-framing unit test for the NDJSON parser / `writeChain` would be the main outstanding addition.

## Out of Scope / Deferred

- **Embedded V8 isolate pool + deterministic sandbox** — the spec's target runtime; M4. This ADR is the interim transport behind the same crate interface.
- **Dedicated protocol fd** to also defend against direct `process.stdout.write` by user handlers. Deferred; the console redirect is the M1 mitigation.
- **Per-request execution timeout / cancellation** on the engine's `execute` await, and bounded/swept correlation maps for the worker-crash and never-completes cases (only the 30s startup manifest/ready timeout exists today).
- **Worker pool, restart/backpressure policy, and crash isolation** beyond keeping the single child alive — the action-worker-pool transport (length-prefixed unix socket) is a separate decision.
- **Structured/forwarded user logs.** Routing `console.*` to stderr is a protection measure, not a logging product; turning user logs into a first-class structured stream is out of scope here.
