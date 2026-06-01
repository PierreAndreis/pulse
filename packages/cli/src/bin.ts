#!/usr/bin/env node
import { spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import type { AnyTableDefinition, SchemaDefinition } from "@onveloz/pulse-schema";
import { generateDataModel } from "./codegen.js";
import { generateDDL } from "./ddl.js";
import { buildEngineEnv, resolveEngineBin } from "./dev.js";
import {
  diffSchema,
  parseLiveColumns,
  renderDiff,
  INTROSPECT_SQL,
  INTROSPECT_INDEXES_SQL,
  type InfoSchemaRow,
} from "./diff.js";

type AnySchema = SchemaDefinition<Record<string, AnyTableDefinition>>;

/** Minimal structural type for the optional `pg` Client we use in `--diff`. */
interface PgClient {
  connect(): Promise<void>;
  query(sql: string): Promise<{ rows: unknown[] }>;
  end(): Promise<void>;
}

const HELP = `pulse <command>

  gen <schema.ts> [out.ts]   generate the Doc/Id data model from a schema
                             (default out: <schemaDir>/_generated/dataModel.ts)
  migrate <schema.ts> [--out file.sql]
                             generate idempotent DDL from a schema (prints to
                             stdout, or writes to --out)
  migrate <schema.ts> --diff [--database-url URL] [--out file.sql]
                             diff the schema against the live database and emit a
                             migration script (additive / flagged-alters /
                             commented destructive). Reads DATABASE_URL if --database-url
                             is omitted.
  dev <app.ts> [--port P] [--database-url URL] [--worker-bin bun]
                             run the engine against an app module (schema +
                             handlers); streams logs until Ctrl-C.
  deploy <app.ts> [--out dir]
                             build a self-contained release bundle (app + worker +
                             generated DDL + a run script) into <dir> (default
                             ./pulse-dist).
`;

const WORKER_SCRIPT_REL = "packages/runtime-node/src/worker.ts";

/** Repo root: two levels up from packages/cli/src. */
function repoRoot(): string {
  return resolve(dirname(new URL(import.meta.url).pathname), "..", "..", "..");
}

function isSchema(v: unknown): v is AnySchema {
  return (
    !!v &&
    typeof v === "object" &&
    "tables" in v &&
    typeof (v as AnySchema).describe === "function"
  );
}

/** Import a schema module and return its schema (default export or first match). */
async function loadSchema(modulePath: string): Promise<AnySchema> {
  const mod: Record<string, unknown> = await import(resolve(modulePath));
  if (isSchema(mod.default)) return mod.default;
  for (const value of Object.values(mod)) {
    if (isSchema(value)) return value;
  }
  throw new Error(`no schema (defineSchema export) found in ${modulePath}`);
}

function flag(args: string[], name: string): string | undefined {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
}

function has(args: string[], name: string): boolean {
  return args.includes(name);
}

/**
 * Diff the schema against the live database. Uses node-postgres if installed; we
 * import it dynamically (and ONLY here) so the common `gen`/`migrate` paths never
 * need a DB driver. This is the one justified inline import: the driver is an
 * optional, command-specific dependency, not a module-level one.
 */
async function migrateDiff(schema: AnySchema, databaseUrl: string): Promise<string> {
  let pg: { default: { Client: new (cfg: { connectionString: string }) => PgClient } };
  try {
    pg = (await import("pg" as string)) as typeof pg;
  } catch {
    throw new Error(
      "`pulse migrate --diff` needs the 'pg' package. Install it: `pnpm add pg` (or `npm i pg`).",
    );
  }
  const client = new pg.default.Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    const cols = await client.query(INTROSPECT_SQL);
    const idx = await client.query(INTROSPECT_INDEXES_SQL);
    const live = parseLiveColumns(cols.rows as InfoSchemaRow[]);
    const liveIndexes = new Set<string>((idx.rows as { indexname: string }[]).map((r) => r.indexname));
    return renderDiff(diffSchema(live, schema, liveIndexes));
  } finally {
    await client.end();
  }
}

async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2);

  switch (command) {
    case undefined:
    case "help":
    case "--help":
      process.stdout.write(HELP);
      return;

    case "gen": {
      const schemaPath = args[0];
      if (!schemaPath) throw new Error("usage: pulse gen <schema.ts> [out.ts]");
      const schema = await loadSchema(schemaPath);
      const out =
        args[1] && !args[1].startsWith("-")
          ? args[1]
          : resolve(dirname(resolve(schemaPath)), "_generated", "dataModel.ts");
      await mkdir(dirname(out), { recursive: true });
      await writeFile(out, generateDataModel(schema), "utf8");
      process.stdout.write(`pulse: wrote ${out}\n`);
      return;
    }

    case "migrate": {
      const schemaPath = args[0];
      if (!schemaPath) throw new Error("usage: pulse migrate <schema.ts> [--out file.sql]");
      const schema = await loadSchema(schemaPath);
      const ddl = has(args, "--diff")
        ? await migrateDiff(
            schema,
            flag(args, "--database-url") ??
              process.env.DATABASE_URL ??
              (() => {
                throw new Error("migrate --diff needs --database-url or DATABASE_URL");
              })(),
          )
        : generateDDL(schema);
      const out = flag(args, "--out");
      if (out) {
        await writeFile(out, ddl, "utf8");
        process.stdout.write(`pulse: wrote ${out}\n`);
      } else {
        process.stdout.write(ddl);
      }
      return;
    }

    case "dev": {
      const appPath = args[0];
      if (!appPath || appPath.startsWith("-"))
        throw new Error("usage: pulse dev <app.ts> [--port P] [--database-url URL]");
      const root = repoRoot();
      const { enginePathSync, ensureEngine } = await import("@onveloz/pulse-engine");
      let bin = resolveEngineBin(root, process.env, undefined, enginePathSync);
      if (!bin) {
        // No local build and nothing cached → fetch the prebuilt engine.
        process.stdout.write("pulse: no engine found locally; downloading…\n");
        bin = await ensureEngine();
      }
      if (!bin)
        throw new Error(
          "engine binary not found — no prebuilt engine for this platform. " +
            "Build it (`cargo build -p pulse-server`) and set PULSE_SERVER_BIN.",
        );
      const env = buildEngineEnv({
        appPath: resolve(appPath),
        workerScript: resolve(root, WORKER_SCRIPT_REL),
        port: flag(args, "--port"),
        databaseUrl: flag(args, "--database-url"),
        workerBin: flag(args, "--worker-bin"),
      });
      process.stdout.write(`pulse: starting engine on :${env.PULSE_PORT} (app ${env.PULSE_APP})\n`);
      const child = spawn(bin, [], { env, stdio: "inherit" });
      // Forward termination so Ctrl-C cleanly stops the engine.
      const stop = (sig: NodeJS.Signals) => child.kill(sig);
      process.on("SIGINT", () => stop("SIGINT"));
      process.on("SIGTERM", () => stop("SIGTERM"));
      await new Promise<void>((res) => {
        child.on("exit", (code) => {
          process.exitCode = code ?? 0;
          res();
        });
        child.on("error", (err) => {
          process.stderr.write(`pulse: failed to start engine: ${err.message}\n`);
          process.exitCode = 1;
          res();
        });
      });
      return;
    }

    case "deploy": {
      const appPath = args[0];
      if (!appPath || appPath.startsWith("-"))
        throw new Error("usage: pulse deploy <app.ts> [--out dir]");
      const root = repoRoot();
      const outDir = resolve(flag(args, "--out") ?? "pulse-dist");
      const schema = await loadSchema(appPath);
      await mkdir(outDir, { recursive: true });
      // 1. Schema DDL so the target DB can be provisioned.
      await writeFile(resolve(outDir, "schema.sql"), generateDDL(schema), "utf8");
      // 2. A run script that launches the engine against the app.
      const bin = resolveEngineBin(root);
      const run =
        `#!/usr/bin/env bash\nset -euo pipefail\n` +
        `# Provision the schema, then run the engine. Set DATABASE_URL first.\n` +
        `: "\${DATABASE_URL:?set DATABASE_URL}"\n` +
        `psql "$DATABASE_URL" -f "$(dirname "$0")/schema.sql"\n` +
        `export PULSE_APP="${resolve(appPath)}"\n` +
        `export PULSE_WORKER_SCRIPT="${resolve(root, WORKER_SCRIPT_REL)}"\n` +
        `export PULSE_PORT="\${PULSE_PORT:-8787}"\n` +
        `exec "${bin ?? "./pulse-server"}"\n`;
      await writeFile(resolve(outDir, "run.sh"), run, { mode: 0o755 });
      process.stdout.write(
        `pulse: wrote deploy bundle to ${outDir} (schema.sql + run.sh)\n` +
          (bin ? "" : "pulse: warning — engine binary not built; run.sh expects ./pulse-server\n"),
      );
      return;
    }

    default:
      process.stderr.write(`pulse: unknown command '${command}'\n\n${HELP}`);
      process.exitCode = 1;
  }
}

main().catch((err: unknown) => {
  process.stderr.write(`pulse: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exitCode = 1;
});
