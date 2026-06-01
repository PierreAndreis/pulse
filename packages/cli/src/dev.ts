import { existsSync } from "node:fs";
import { resolve } from "node:path";

/** Resolve the pulse-server binary path, preferring release over debug.
 *  `PULSE_SERVER_BIN` overrides everything. Returns null if none is found. */
export function resolveEngineBin(
  root: string,
  env: Record<string, string | undefined> = process.env,
  exists: (p: string) => boolean = existsSync,
): string | null {
  const override = env.PULSE_SERVER_BIN;
  if (override) return exists(override) ? override : null;
  const release = resolve(root, "target/release/pulse-server");
  if (exists(release)) return release;
  const debug = resolve(root, "target/debug/pulse-server");
  if (exists(debug)) return debug;
  return null;
}

export interface DevEnvOptions {
  appPath: string;
  port?: string;
  databaseUrl?: string;
  workerBin?: string;
  workerScript: string;
}

/** Build the environment the engine process needs, from CLI options + ambient
 *  env (mirrors the integration harness so `pulse dev` matches test behavior). */
export function buildEngineEnv(
  opts: DevEnvOptions,
  ambient: Record<string, string | undefined> = process.env,
): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [k, val] of Object.entries(ambient)) if (val !== undefined) env[k] = val;
  env.PULSE_APP = opts.appPath;
  env.PULSE_WORKER_SCRIPT = opts.workerScript;
  env.PULSE_PORT = opts.port ?? ambient.PULSE_PORT ?? "8787";
  env.PULSE_WORKER_BIN = opts.workerBin ?? ambient.PULSE_WORKER_BIN ?? "bun";
  env.DATABASE_URL =
    opts.databaseUrl ?? ambient.DATABASE_URL ?? "postgres://pulse:pulse@localhost:54329/pulse";
  return env;
}
