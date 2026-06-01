# 08. TDD Red-Green-Refactor + Stress Testing Through Public Interfaces

- **Status:** Accepted — consistent with `docs/ARCHITECTURE.md` §10, which records that M1 and M2 shipped via TDD and were "verified end-to-end through `@pulse/client` against real Postgres, plus stress tests (concurrent load, pool saturation, worker backpressure, 15s soak)." This ADR records the methodology behind those tests so future milestones extend it rather than reinventing it.

## Context & Problem

Pulse is a reactive platform with correctness guarantees that only show up in motion: read/write-set capture must be complete before a handler completes, a write must re-run exactly the subscriptions whose read-set intersects its write-set, concurrent requests through one shared worker must stay correlated, and a write under load must neither drop nor duplicate. These are *system* properties spanning a Rust engine, an NDJSON worker protocol, and Postgres. None of them can be proven by unit-testing a function in isolation — the bug lives in the seams.

At the same time the runtime under the public interface is explicitly temporary: ADR 01 ships a Node/Bun worker now and owes an embedded-V8 swap at M4, at which point the NDJSON wire format, the `reader_loop`, and the db-op proxy all disappear. Any test coupled to those internals would have to be rewritten for a behavior that did not change.

The forcing question: *how do we build features confidently and catch the seam-level and under-load bugs, while writing tests that survive the M4 runtime swap untouched?*

Two pressures pull in opposite directions and this ADR resolves both: tests must reach deep enough to exercise the real engine/worker/Postgres path, yet must assert *only* on what a client of Pulse can observe.

## Decision

**1. Vertical-slice TDD (red → green → refactor), one behavior at a time.** Per `.claude/skills/tdd/SKILL.md`: write ONE failing test for ONE behavior, write the minimum code to pass, then refactor on green. Never write all tests then all implementation ("horizontal slicing" produces tests of *imagined* shape, not actual behavior). The first test of a slice is a **tracer bullet** that proves the whole path end-to-end before any breadth is added. The repo's tracer bullets are labelled as such:

- M1 tracer (`tests/integration/roundtrip.test.ts`): *"a sent message is retrievable via list"* — comment: "proves the whole path (client → engine → worker → ctx.db → PG)."
- M2 tracer (`tests/integration/reactive.test.ts`): *"a send is pushed to a separate subscriber over SSE"* — comment: "Tracer bullet for M2."

**2. Behavior is verified through the public client against real Postgres.** Integration and stress tests drive `@pulse/client` (`createClient<typeof contract>`) over real HTTP against a real `pulse-server` + Bun worker + Dockerized Postgres. The shared harness (`tests/integration/harness.ts`) is the only thing that touches internals, and it confines that to *setup*, not assertions. Its contract:

```ts
export interface Harness {
  client: Client<typeof contract>;                       // assert through this
  baseUrl: string;
  makeClient(token: string | null): Client<typeof contract>; // 2nd client / auth variation
  stop(): Promise<void>;
  reset(): Promise<void>;   // truncates `messages` between tests — SETUP ONLY
}
```

`reset()` runs `truncate messages` via `docker exec … psql` and is documented in-code as "setup only — assertions go via the client." That line is the discipline: the database is touched to *arrange* a test, never to *assert* its outcome. Assertions read back through `client.messages.list/send/summarize/stats/subscribe`. The two vitest projects encode the split:

- `vitest.config.ts` — fast in-package runtime + `*.test-d.ts` type tests, `include: packages/*/src/**`.
- `vitest.integration.config.ts` — `include: tests/**`, one engine, shared DB, `fileParallelism: false`, `singleFork`, generous timeouts (`testTimeout 30s`, `hookTimeout 120s`).

**3. Four stress categories, each a *correctness* probe under a *named* failure mode** — not a benchmark. Each lives in `tests/stress/` and asserts through the client; the small `mapPool(items, concurrency, fn)` helper (`tests/stress/util.ts`) drives bounded concurrency:

| Category | File | Failure mode it isolates | Public-interface assertion |
| --- | --- | --- | --- |
| Concurrent load | `concurrent.test.ts` | dropped / duplicated writes | 90 concurrent sends (under the 100 list cap) → `list.length === 90` and the body **set** equals the input set; a 1000-send wave → all succeed within 30s |
| Worker backpressure | `backpressure.test.ts` | request↔response *correlation* breaking under flood | 200 concurrent sends → each returned document's `body` equals **its own** input (`results[i].body === bodies[i]`); a crossed wire would resolve a send to another request's doc |
| Pool saturation | `pool-saturation.test.ts` | deadlock when requests outnumber DB connections | harness started with `oltpMaxConns: 2`; 100 concurrent sends through the 2-connection pool all succeed |
| Soak | `soak.test.ts` | slow leaks / degradation over time | continuous mixed send+list waves for a window (`PULSE_SOAK_MS`, default 15s) → `errors === 0` and `ops > 500` |

The design rule visible across these: keep load **within a verifiable envelope** so correctness is checkable through the client (the concurrent test deliberately stays under the 100-row list cap so it can compare full sets; raw throughput tests only assert zero errors + a loose time bound).

## Alternatives Considered

- **Assert by querying Postgres directly (read back rows from the DB).** Tempting because the DB is already up for `reset()`. Rejected: the TDD skill names this exact anti-pattern — verifying "through external means (like querying a database directly instead of using the interface)." It couples tests to the storage schema and to id encoding, and it would not catch bugs in the read path (serialization, `table:uuid` encoding, the list cap) that a real client exercises. The harness draws the line precisely here: DB access for setup, client for assertions.
- **Unit-test the engine internals (NDJSON framing, `reader_loop`, db-op proxy).** Fast and granular. Rejected as the *primary* level: these are exactly the internals ADR 01 will delete at M4: a passing-but-rewritten test proves nothing about behavior. Such tests, where they exist, are a supplement, not the contract.
- **Mock the worker / mock Postgres in integration tests.** Per `.claude/skills/tdd/SKILL.md`, mocking internal collaborators yields tests that "break when you refactor, but behavior hasn't changed." The seam bugs we care about (capture completeness, correlation, pool queuing) only exist *because* the real components interact, so mocking would erase the very failures the tests exist to catch.
- **Horizontal slicing (write the full test suite, then implement).** Rejected by the skill: bulk-written tests assert imagined shape, go insensitive to real changes, and outrun the implementation. The tracer-bullet-first labelling in the actual tests is the chosen alternative.
- **Treat stress tests as benchmarks (assert on throughput/latency numbers).** Rejected: numbers are environment-dependent and flaky in CI. The stress tests assert *correctness invariants* (no drops, correlation holds, no deadlock, zero errors) with only loose time bounds as a liveness guard, so they pass or fail on behavior, not on the machine.

## Consequences

Pros:
- Tests are written against `@pulse/client` + the `contract`, so they are immune to the M4 embedded-V8 swap — the runtime underneath can be replaced wholesale while the suite stands.
- Seam-level guarantees (capture completeness, subscription routing, correlation under flood, pool queuing) are actually exercised, because the real engine/worker/Postgres path runs.
- Each stress file documents the specific failure mode it isolates, so a red stress test points at a category of bug, not just "something is slow."
- Tracer-bullet-first means every slice is proven end-to-end before breadth is added, matching the roadmap's "one reactive query updating one browser as early as possible" principle.

Cons / costs later:
- **Slow and serial.** The integration project boots a real server + worker + Postgres, runs single-fork with no file parallelism, and soak alone is 15s+ (up to a 120s timeout). The suite will not stay sub-second.
- **External dependency on Docker/Postgres.** `reset()` shells out to `docker exec … psql` against a named container (`PULSE_PG_CONTAINER`, default `pulse-pg`); the harness expects a prebuilt `target/debug/pulse-server` and a worker runtime (`bun`). CI must provision all of this.
- **Coarse granularity.** A failing end-to-end test localizes the bug to "somewhere in the path," not to a line. Debugging leans on `PULSE_TEST_LOG`/`RUST_LOG`, not on a focused unit failure.
- **Verifiable-envelope ceiling.** Because correctness assertions require staying under caps (e.g. the 100-row list cap), the heaviest waves can only assert "zero errors," not full-set correctness — true behavior at very high volume is asserted more weakly.

## Testing Decisions

A good test here reads like a specification of an externally observable capability and would survive an internal refactor:

- **Asserts through the public interface only.** State is arranged via `harness.reset()` and `client.send`, and verified via `client.list/summarize/stats/subscribe` — never by reading Postgres directly. If renaming an internal function (or swapping the whole runtime at M4) breaks a test while behavior is unchanged, that test was wrong.
- **One behavior per test, named as a capability.** e.g. *"lists every message sent to a channel,"* *"each subscriber only ever sees its own channel's data,"* *"unsubscribe stops further pushes,"* *"rejects unauthenticated calls with UNAUTHORIZED."* The name tells you what capability exists.
- **Covers the contract's error surface, not just the happy path** (`tests/integration/errors.test.ts`): validator rejection → `BAD_REQUEST`; missing auth → `UNAUTHORIZED`; a declared error surfaces with code **and** structured `data` (`RATE_LIMITED`, `data.retryAfter: 5`).
- **Reactivity is asserted by what a subscriber observes over SSE** (`reactive.test.ts`): a write in one client is pushed to a separate subscriber; multi-client fan-out; per-subscription channel isolation (a re-run never leaks another channel's rows); unsubscribe halts pushes. A `waitFor(predicate)` poll handles the inherent async without sleeping on a fixed delay.
- **A stress test pins a named failure mode and asserts a correctness invariant**, not a throughput number, keeping load inside a verifiable envelope where it can (the concurrent test stays under the list cap to compare full sets).

Prior art to copy when adding a milestone's tests: the M1 round-trip tracer (`roundtrip.test.ts`), the broader `messages`/`analytical`/`errors` integration specs, the M2 `reactive` specs, and the four `tests/stress/*` files driven by `mapPool`. New behavior is verified at this same level — through the client, end to end — exactly as ADR 01 §Testing Decisions prescribes for the runtime crate.

## Out of Scope / Deferred

- **Determinism / replay tests** (replayed mutation produces identical reads) — meaningful only once the M4 deterministic sandbox exists (frozen time, seeded RNG, no net/fs); the current worker can call `Date.now()`/`Math.random()`, so this guarantee is not yet testable. (`ARCHITECTURE.md` §8 lists it as an M3 verify item.)
- **Chaos testing** (kill the engine mid-stream → slot replay, no dropped invalidations) — an M5 verify item in `ARCHITECTURE.md` §8; depends on the WAL/replication-slot path that does not exist yet.
- **Load/performance benchmarking with asserted latency or throughput SLAs** — the stress suite intentionally asserts correctness, not numbers; a real benchmark harness (and the analytical-replica p99 isolation load test in §8) is separate work.
- **Browser/React end-to-end tests** (`@pulse/react` hooks driving a real DOM) — current reactivity coverage exercises the SSE path through `@pulse/client`, not a rendered component.
- **CI provisioning of Docker/Postgres and the engine build** — the harness assumes these exist; orchestrating them is an infrastructure concern, not part of this decision.
- **Per-crate Rust unit tests** as a supplement to the end-to-end suite — allowed, but they are not the behavioral contract and must not become the place where externally observable behavior is asserted.
