import { spawn, type ChildProcess, execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";
import { createClient, PulseClientError, type Client } from "@onveloz/pulse-client";
import type { contract } from "../../packages/examples-chat/src/contract.js";

const execFileAsync = promisify(execFile);

const ROOT = process.cwd();
const DATABASE_URL =
  process.env.PULSE_TEST_DATABASE_URL ?? "postgres://pulse:pulse@localhost:54329/pulse";
const PG_CONTAINER = process.env.PULSE_PG_CONTAINER ?? "pulse-pg";

// Track every spawned engine so we can guarantee they die even if a test file
// aborts before its afterAll runs — a leaked engine stays attached to the shared
// Postgres change bus and pollutes other test runs (extra cross-node pushes) and
// hogs ports. One process-level cleanup hook kills the whole set on exit.
const liveEngines = new Set<ChildProcess>();
let cleanupRegistered = false;
function registerCleanup(): void {
  if (cleanupRegistered) return;
  cleanupRegistered = true;
  const killAll = (): void => {
    for (const child of liveEngines) {
      if (child.exitCode === null) child.kill("SIGKILL");
    }
  };
  process.once("exit", killAll);
  process.once("SIGINT", () => {
    killAll();
    process.exit(130);
  });
  process.once("SIGTERM", () => {
    killAll();
    process.exit(143);
  });
}

/** A retry-exhaustion conflict surfaced to the client as HTTP 409 (`CONFLICT`).
 *  Lets conflict tests distinguish an honest conflict from a timeout/INTERNAL. */
export function isPulseConflictError(err: unknown): err is PulseClientError {
  return err instanceof PulseClientError && err.code === "CONFLICT";
}

/** Seeded general channel (id encoded as `table:uuid`). */
export const GENERAL_CHANNEL = "channels:00000000-0000-0000-0000-000000000001";
/** Seeded demo user. */
export const DEMO_USER = "users:00000000-0000-0000-0000-000000000010";

export interface Harness {
  client: Client<typeof contract>;
  baseUrl: string;
  /** OS pid of the engine process (for RSS / memory measurement). */
  pid: number | undefined;
  /** Build another client against the same engine with a chosen bearer token (null → no auth). */
  makeClient(token: string | null): Client<typeof contract>;
  stop(): Promise<void>;
  /** Truncate the messages table between tests (setup only — assertions go via the client). */
  reset(): Promise<void>;
}

/** Run a SQL statement against the test database (psql; falls back to `docker
 *  exec` into the local dev container when host psql isn't available). Used by
 *  fixtures to create/truncate their own tables. */
export async function applySql(sql: string): Promise<void> {
  try {
    await execFileAsync("psql", [DATABASE_URL, "-v", "ON_ERROR_STOP=1", "-c", sql]);
  } catch {
    await execFileAsync("docker", [
      "exec",
      PG_CONTAINER,
      "psql",
      "-U",
      "pulse",
      "-d",
      "pulse",
      "-v",
      "ON_ERROR_STOP=1",
      "-c",
      sql,
    ]);
  }
}

export interface StartOptions {
  /** App module the engine's worker loads (default: examples-chat). */
  app?: string;
  /** Override OLTP pool size (used by pool-saturation stress tests). */
  oltpMaxConns?: number;
  /** Bearer token to send (null → omit auth header, to drive UNAUTHORIZED). */
  authToken?: string | null;
  /** OLTP per-statement timeout in ms (0 disables). */
  oltpStatementTimeoutMs?: number;
  /** OLAP per-statement timeout in ms (0 disables). */
  olapStatementTimeoutMs?: number;
  /** Max SERIALIZABLE retry attempts before CONFLICT (default 25 in the engine).
   *  Set small to force retry exhaustion deterministically at low concurrency. */
  maxTxAttempts?: number;
}

function pickPort(): number {
  return 8800 + Math.floor(Math.random() * 1000);
}

async function waitForHealth(baseUrl: string, child: ChildProcess, timeoutMs: number): Promise<void> {
  let exited: { code: number | null; signal: string | null } | null = null;
  let spawnError: Error | null = null;
  child.once("exit", (code, signal) => {
    exited = { code, signal };
  });
  child.once("error", (err) => {
    spawnError = err;
  });

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (spawnError) throw spawnError;
    if (exited) {
      throw new Error(`engine exited early (code=${exited.code}, signal=${exited.signal})`);
    }
    try {
      const res = await fetch(`${baseUrl}/health`);
      if (res.ok) return;
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`engine did not become healthy within ${timeoutMs}ms`);
}

export async function startEngine(opts: StartOptions = {}): Promise<Harness> {
  registerCleanup();
  const port = pickPort();
  const baseUrl = `http://127.0.0.1:${port}`;
  const verbose = !!process.env.PULSE_TEST_LOG;

  const child = spawn(resolve(ROOT, "target/debug/pulse-server"), [], {
    env: {
      ...process.env,
      DATABASE_URL,
      PULSE_PORT: String(port),
      PULSE_WORKER_BIN: process.env.PULSE_WORKER_BIN ?? "bun",
      PULSE_WORKER_SCRIPT: resolve(ROOT, "packages/runtime-node/src/worker.ts"),
      PULSE_APP: opts.app ?? resolve(ROOT, "packages/examples-chat/src/app.ts"),
      PULSE_OLTP_MAX_CONNS: String(opts.oltpMaxConns ?? 10),
      ...(opts.maxTxAttempts !== undefined
        ? { PULSE_MAX_TX_ATTEMPTS: String(opts.maxTxAttempts) }
        : {}),
      ...(opts.oltpStatementTimeoutMs !== undefined
        ? { PULSE_OLTP_STATEMENT_TIMEOUT_MS: String(opts.oltpStatementTimeoutMs) }
        : {}),
      ...(opts.olapStatementTimeoutMs !== undefined
        ? { PULSE_OLAP_STATEMENT_TIMEOUT_MS: String(opts.olapStatementTimeoutMs) }
        : {}),
      RUST_LOG: process.env.RUST_LOG ?? "pulse=info,warn",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout?.on("data", (d) => verbose && process.stdout.write(`[engine] ${d}`));
  child.stderr?.on("data", (d) => verbose && process.stderr.write(`[engine] ${d}`));
  liveEngines.add(child);
  child.once("exit", () => liveEngines.delete(child));

  await waitForHealth(baseUrl, child, 60_000);

  const authToken = opts.authToken === undefined ? "test-token" : opts.authToken;
  const client = createClient<typeof contract>({
    url: baseUrl,
    headers: () => (authToken === null ? {} : { authorization: `Bearer ${authToken}` }),
  });

  const makeClient = (token: string | null): Client<typeof contract> =>
    createClient<typeof contract>({
      url: baseUrl,
      headers: () => (token === null ? {} : { authorization: `Bearer ${token}` }),
    });

  return {
    client,
    baseUrl,
    pid: child.pid,
    makeClient,
    async stop() {
      if (child.exitCode === null) {
        child.kill("SIGKILL");
        await new Promise((r) => child.once("exit", r));
      }
      liveEngines.delete(child);
    },
    async reset() {
      const truncate = "truncate messages, counters, accounts, transfers";
      // Prefer a direct psql against DATABASE_URL (CI has psql but no named
      // container); fall back to `docker exec` into the local dev container
      // (local dev has the container but not psql on the host).
      try {
        await execFileAsync("psql", [DATABASE_URL, "-v", "ON_ERROR_STOP=1", "-c", truncate]);
      } catch {
        await execFileAsync("docker", [
          "exec",
          PG_CONTAINER,
          "psql",
          "-U",
          "pulse",
          "-d",
          "pulse",
          "-c",
          truncate,
        ]);
      }
    },
  };
}
