#!/usr/bin/env node
import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { cancel, confirm, isCancel } from "@clack/prompts";
import type { AnyTableDefinition, SchemaDefinition } from "@onveloz/pulse-schema";
import { generateDataModel } from "./codegen.js";
import { generateDDL } from "./ddl.js";
import { buildEngineEnv, resolveEngineBin, DEFAULT_DATABASE_URL } from "./dev.js";
import { runNew } from "./new.js";
import {
  diffSchema,
  dropStatement,
  parseLiveColumns,
  renderDiff,
  INTROSPECT_SQL,
  INTROSPECT_INDEXES_SQL,
  type Drop,
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

  new [name] [options]       scaffold a fully-configured Pulse app (schema,
                             contract, handlers, Vite client, Docker Postgres).
                             Interactive by default; prompts for the name,
                             package manager, and starter. Options:
                               --pm pnpm|npm|yarn|bun   package manager
                               --template todos|minimal  starter
                               --[no-]auth               wire Better Auth
                               --[no-]install            install deps
                               --[no-]db                 start Postgres
                               --[no-]git                git init
                               -y, --yes                 accept all defaults
  gen [schema.ts] [out.ts]   generate the Doc/Id data model from a schema
                             (schema defaults to app/schema.ts; default out:
                             <schemaDir>/_generated/dataModel.ts)
  migrate [schema.ts] [--out file.sql]
                             generate idempotent DDL from a schema (prints to
                             stdout, or writes to --out). Schema defaults to
                             app/schema.ts.
  migrate [schema.ts] --diff [--database-url URL] [--out file.sql]
                             diff the schema against the live database and emit a
                             migration script (additive / flagged-alters /
                             commented destructive). Database URL defaults to
                             --database-url, else DATABASE_URL, else the local
                             docker-compose Postgres.
  migrate [schema.ts] --apply [--database-url URL] [--force]
                             apply the schema to the live database. Additive
                             changes apply automatically; destructive ones (drops)
                             prompt for confirmation when interactive, and are
                             refused in a non-interactive env (CI / AI) unless
                             --force is passed.
  dev [app.ts] [--port P] [--database-url URL] [--worker-bin bun]
                             run the engine against an app module (schema +
                             handlers); streams logs until Ctrl-C. Codegens +
                             syncs the schema on boot — for local development.
                             App defaults to app/app.ts.
  start [app.ts] [--port P] [--database-url URL] [--worker-bin bun] [--migrate]
                             production serve: run the prebuilt engine + worker
                             (no codegen at boot). Pass --migrate to apply safe
                             additive schema changes; otherwise migrations are
                             left to an explicit step. Used by the Dockerfile.
                             App defaults to app/app.ts.
  deploy [app.ts] [--out dir]
                             build a self-contained release bundle (app + worker +
                             generated DDL + a run script) into <dir> (default
                             ./pulse-dist). App defaults to app/app.ts.
`;

/** Repo root: two levels up from packages/cli/src. */
function repoRoot(): string {
  return resolve(dirname(new URL(import.meta.url).pathname), "..", "..", "..");
}

/** Absolute path to the runtime-node worker entry, resolved via node module
 *  resolution so it works both in the monorepo and when installed from npm
 *  (where `@onveloz/pulse-runtime-node` is a sibling dependency of this CLI). */
function workerScriptPath(): string {
  const entry = fileURLToPath(import.meta.resolve("@onveloz/pulse-runtime-node"));
  return resolve(dirname(entry), "worker.js");
}

/** This CLI's own version, read from its package.json (dist/bin.js → ../package.json). */
function cliVersion(): string {
  const pkgPath = resolve(dirname(fileURLToPath(import.meta.url)), "..", "package.json");
  return (JSON.parse(readFileSync(pkgPath, "utf8")) as { version: string }).version;
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

/** Resolve a positional file arg, defaulting to the scaffold layout when it's
 *  omitted (or the first arg is a flag), and failing with a clear hint if the
 *  resolved file is missing — so commands work with zero args from a project root. */
function resolveFileArg(args: string[], fallback: string, cmd: string): string {
  const a = args[0];
  const p = resolve(a && !a.startsWith("-") ? a : fallback);
  if (!existsSync(p))
    throw new Error(
      `not found: ${p} — run from your project root, or pass it explicitly: \`pulse ${cmd} <path>\``,
    );
  return p;
}

/**
 * Connect to Postgres via node-postgres. We import it dynamically (the ONE
 * justified inline import in this CLI) so the no-DB paths (`gen`, `migrate`
 * without `--diff`) never need a driver. Gives a clear hint if the DB is down.
 */
async function connectPg(databaseUrl: string): Promise<PgClient> {
  let pg: { default: { Client: new (cfg: { connectionString: string }) => PgClient } };
  try {
    pg = (await import("pg" as string)) as typeof pg;
  } catch {
    throw new Error("this needs the 'pg' package. Install it: `pnpm add pg` (or `npm i pg`).");
  }
  const client = new pg.default.Client({ connectionString: databaseUrl });
  try {
    await client.connect();
  } catch (e) {
    throw new Error(
      `could not connect to Postgres at ${databaseUrl} — is it running? Start it with your ` +
        `package manager's \`db\` script (e.g. \`docker compose up -d\`). ` +
        `(${e instanceof Error ? e.message : String(e)})`,
    );
  }
  return client;
}

/** Read the live database shape (columns + index names) for diffing. */
async function introspect(
  client: PgClient,
): Promise<{ live: ReturnType<typeof parseLiveColumns>; liveIndexes: Set<string> }> {
  const cols = await client.query(INTROSPECT_SQL);
  const idx = await client.query(INTROSPECT_INDEXES_SQL);
  const live = parseLiveColumns(cols.rows as InfoSchemaRow[]);
  const liveIndexes = new Set<string>((idx.rows as { indexname: string }[]).map((r) => r.indexname));
  return { live, liveIndexes };
}

/** Diff the schema against the live database and render an annotated migration. */
async function migrateDiff(schema: AnySchema, databaseUrl: string): Promise<string> {
  const client = await connectPg(databaseUrl);
  try {
    const { live, liveIndexes } = await introspect(client);
    return renderDiff(diffSchema(live, schema, liveIndexes));
  } finally {
    await client.end();
  }
}

/**
 * Bring the database in line with the schema before the engine boots, the same
 * way `convex dev` does: auto-apply only SAFE additive changes (new tables,
 * `ADD COLUMN`, indexes), and never silently run risky alters (type/nullability
 * changes that can fail or lose data) or destructive drops — those are printed
 * for review via `pulse migrate --diff`.
 */
async function syncSchema(databaseUrl: string, schema: AnySchema): Promise<void> {
  const client = await connectPg(databaseUrl);
  try {
    const { live, liveIndexes } = await introspect(client);
    const diff = diffSchema(live, schema, liveIndexes);

    let applied = 0;
    for (const stmt of diff.additive) {
      await client.query(stmt);
      applied++;
    }

    // Drops run only when they lose no data — an empty table or an all-NULL
    // column. Anything holding data is refused and surfaced for review.
    const refusedDrops: Drop[] = [];
    for (const drop of diff.destructive) {
      if (await dropLosesNoData(client, drop)) {
        await client.query(dropStatement(drop));
        applied++;
      } else {
        refusedDrops.push(drop);
      }
    }

    // `alters` entries that are real SQL (not `-- review:` notes) plus any drops
    // that still hold data are changes we refuse to apply automatically.
    const riskyAlters = diff.alters.filter((s) => !s.trimStart().startsWith("--"));
    if (riskyAlters.length || refusedDrops.length) {
      process.stdout.write(
        `pulse: applied ${applied} safe change(s); the following hold data and need review (NOT applied):\n` +
          renderDiff({ additive: [], alters: diff.alters, destructive: refusedDrops }) +
          "Review with `pulse migrate --diff`, then apply manually.\n",
      );
    } else if (applied) {
      process.stdout.write(`pulse: schema synced (${applied} change(s) applied)\n`);
    } else {
      process.stdout.write("pulse: schema up to date\n");
    }
  } finally {
    await client.end();
  }
}

/** True when dropping `drop` would lose nothing: an empty table, or a column
 *  whose every value is NULL. Lets `pulse dev` clean up empties automatically
 *  while never silently destroying data. */
async function dropLosesNoData(client: PgClient, drop: Drop): Promise<boolean> {
  const probe =
    drop.kind === "table"
      ? `SELECT EXISTS (SELECT 1 FROM ${drop.table} LIMIT 1) AS has`
      : `SELECT EXISTS (SELECT 1 FROM ${drop.table} WHERE ${drop.column} IS NOT NULL LIMIT 1) AS has`;
  const res = await client.query(probe);
  return !(res.rows[0] as { has: boolean }).has;
}

/**
 * Apply the schema to the live database (`pulse migrate --apply`). Additive
 * changes (new tables/columns/indexes) always apply. DESTRUCTIVE changes (drops)
 * and risky alters (type/nullability changes) require confirmation, Prisma-style:
 *   - interactive (a TTY): prompt before applying — default "no";
 *   - non-interactive (CI / an AI agent driving the CLI, i.e. no TTY): refuse and
 *     exit non-zero unless `--force` is passed, so data is never dropped silently.
 */
async function applyMigration(
  databaseUrl: string,
  schema: AnySchema,
  opts: { force: boolean },
): Promise<void> {
  const client = await connectPg(databaseUrl);
  try {
    const { live, liveIndexes } = await introspect(client);
    const diff = diffSchema(live, schema, liveIndexes);

    for (const stmt of diff.additive) await client.query(stmt);

    const riskyAlters = diff.alters.filter((s) => !s.trimStart().startsWith("--"));
    const drops = diff.destructive;
    if (!riskyAlters.length && !drops.length) {
      process.stdout.write(
        diff.additive.length
          ? `pulse: applied ${diff.additive.length} change(s); schema up to date\n`
          : "pulse: schema up to date\n",
      );
      return;
    }

    // There ARE destructive/risky changes. Show exactly what they are.
    process.stdout.write(
      `pulse: applied ${diff.additive.length} safe change(s). The following are ` +
        `DESTRUCTIVE (permanent data loss):\n` +
        renderDiff({ additive: [], alters: diff.alters, destructive: drops }),
    );

    let approved = opts.force;
    if (!approved) {
      if (process.stdin.isTTY) {
        const answer = await confirm({
          message: "Apply these destructive changes? This permanently deletes data.",
          initialValue: false,
        });
        if (isCancel(answer)) {
          cancel("Aborted — no destructive changes applied.");
          return;
        }
        approved = answer === true;
      } else {
        // No TTY: an AI agent or CI is driving us. Never drop data on a guess.
        process.stderr.write(
          "pulse: refusing to apply destructive changes in a non-interactive environment. " +
            "Re-run with --force to apply them, or review with `pulse migrate --diff`.\n",
        );
        process.exitCode = 1;
        return;
      }
    }

    if (!approved) {
      process.stdout.write("pulse: skipped destructive changes (not applied).\n");
      return;
    }
    for (const alter of riskyAlters) await client.query(alter);
    for (const drop of drops) await client.query(dropStatement(drop));
    process.stdout.write(
      `pulse: applied ${riskyAlters.length + drops.length} destructive change(s).\n`,
    );
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

    case "new":
      await runNew(args, cliVersion());
      return;

    case "gen": {
      // Schema defaults to app/schema.ts; an explicit out path is the 2nd positional.
      const schemaPath = resolveFileArg(args, "app/schema.ts", "gen");
      const explicitOut = args[0] && !args[0].startsWith("-") && args[1] && !args[1].startsWith("-");
      const out = explicitOut
        ? resolve(args[1]!)
        : resolve(dirname(schemaPath), "_generated", "dataModel.ts");
      const schema = await loadSchema(schemaPath);
      await mkdir(dirname(out), { recursive: true });
      await writeFile(out, generateDataModel(schema), "utf8");
      process.stdout.write(`pulse: wrote ${out}\n`);
      return;
    }

    case "migrate": {
      // Schema defaults to app/schema.ts; a leading flag (e.g. `--diff`) isn't a path.
      const schemaPath = resolveFileArg(args, "app/schema.ts", "migrate");
      const schema = await loadSchema(schemaPath);
      // Same default as `pulse dev`: --database-url, else DATABASE_URL, else the
      // local docker-compose Postgres — so the DB-backed modes work with no config.
      const dbUrl = () =>
        flag(args, "--database-url") ?? process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL;

      // `--apply`: apply the schema to the live DB (additive auto; destructive
      // only with confirmation / --force).
      if (has(args, "--apply")) {
        await applyMigration(dbUrl(), schema, {
          force: has(args, "--force") || has(args, "--yes"),
        });
        return;
      }

      const ddl = has(args, "--diff") ? await migrateDiff(schema, dbUrl()) : generateDDL(schema);
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
      const resolvedApp = resolveFileArg(args, "app/app.ts", "dev");

      // Load the schema directly (default: schema.ts next to the app) rather than
      // the app module — the app re-exports its _generated types, which may not
      // exist yet on a fresh project. The schema only needs the SDK to load.
      const schemaPath = flag(args, "--schema") ?? resolve(dirname(resolvedApp), "schema.ts");
      const schema = await loadSchema(schemaPath);

      // 1. Codegen the typed data model the app + worker import (folds in `pulse gen`),
      // so a fresh project's `import "./_generated/dataModel.js"` resolves on first run.
      const genOut = resolve(dirname(resolvedApp), "_generated", "dataModel.ts");
      await mkdir(dirname(genOut), { recursive: true });
      await writeFile(genOut, generateDataModel(schema), "utf8");
      process.stdout.write(`pulse: generated ${genOut}\n`);

      // 2. Resolve the engine binary, downloading it on first run.
      const root = repoRoot();
      const { enginePathSync, ensureEngine } = await import("@onveloz/pulse-engine");
      let bin = resolveEngineBin(root, process.env, undefined, enginePathSync);
      if (!bin) {
        process.stdout.write("pulse: no engine found locally; downloading…\n");
        bin = await ensureEngine();
      }
      if (!bin)
        throw new Error(
          "engine binary not found — no prebuilt engine for this platform. " +
            "Build it (`cargo build -p pulse-server`) and set PULSE_SERVER_BIN.",
        );

      const env = buildEngineEnv({
        appPath: resolvedApp,
        workerScript: workerScriptPath(),
        port: flag(args, "--port"),
        databaseUrl: flag(args, "--database-url"),
        workerBin: flag(args, "--worker-bin"),
      });

      // 3. Sync the schema before boot: auto-apply safe additive changes (new
      // tables/columns/indexes) and refuse to silently run risky or destructive
      // ones (type changes, drops) — the same safety split Convex enforces.
      await syncSchema(env.DATABASE_URL!, schema); // buildEngineEnv always sets it

      // 4. Start the engine, plus an optional frontend via --start (e.g. vite), so
      // a single `pulse dev` runs the whole stack. Either process exiting tears
      // down the rest, and Ctrl-C stops both.
      process.stdout.write(`pulse: starting engine on :${env.PULSE_PORT} (app ${env.PULSE_APP})\n`);
      const children = [spawn(bin, [], { env, stdio: "inherit" })];
      const startCmd = flag(args, "--start");
      if (startCmd) children.push(spawn(startCmd, { shell: true, stdio: "inherit", env }));

      let stopping = false;
      const stopAll = (sig: NodeJS.Signals) => {
        if (stopping) return;
        stopping = true;
        for (const c of children) c.kill(sig);
      };
      process.on("SIGINT", () => stopAll("SIGINT"));
      process.on("SIGTERM", () => stopAll("SIGTERM"));
      await new Promise<void>((res) => {
        let exited = 0;
        for (const c of children) {
          c.on("exit", (code) => {
            if (code) process.exitCode = code;
            stopAll("SIGTERM");
            if (++exited >= children.length) res();
          });
          c.on("error", (err) => {
            process.stderr.write(`pulse: ${err.message}\n`);
            process.exitCode = 1;
            stopAll("SIGTERM");
          });
        }
      });
      return;
    }

    case "start": {
      const resolvedApp = resolveFileArg(args, "app/app.ts", "start");

      // Production serve. Unlike `dev`, this does NOT codegen at boot — it assumes
      // `pulse gen` already ran at build time (writing to the filesystem on a
      // read-only prod container would fail). Fail clearly if it's missing.
      const genPath = resolve(dirname(resolvedApp), "_generated", "dataModel.ts");
      if (!existsSync(genPath))
        throw new Error(
          `missing ${genPath} — run \`pulse gen <schema.ts>\` (or rebuild the image) before \`pulse start\``,
        );

      // Resolve the engine binary (download if not already cached).
      const root = repoRoot();
      const { enginePathSync, ensureEngine } = await import("@onveloz/pulse-engine");
      let bin = resolveEngineBin(root, process.env, undefined, enginePathSync);
      if (!bin) {
        process.stdout.write("pulse: no engine found locally; downloading…\n");
        bin = await ensureEngine();
      }
      if (!bin)
        throw new Error(
          "engine binary not found — no prebuilt engine for this platform. " +
            "Build it (`cargo build -p pulse-server`) and set PULSE_SERVER_BIN.",
        );

      const env = buildEngineEnv({
        appPath: resolvedApp,
        workerScript: workerScriptPath(),
        port: flag(args, "--port"),
        databaseUrl: flag(args, "--database-url"),
        workerBin: flag(args, "--worker-bin"),
      });

      // Migrations are EXPLICIT in production: only sync when asked, so scaling to
      // N replicas doesn't run DDL from every boot. Safe additive only; risky and
      // destructive changes are refused (review with `pulse migrate --diff`).
      if (has(args, "--migrate")) {
        const schemaPath = flag(args, "--schema") ?? resolve(dirname(resolvedApp), "schema.ts");
        await syncSchema(env.DATABASE_URL!, await loadSchema(schemaPath));
      }

      // Run the engine in the foreground (it IS the server) and forward signals
      // so the container stops cleanly.
      process.stdout.write(`pulse: starting engine on :${env.PULSE_PORT} (app ${env.PULSE_APP})\n`);
      const child = spawn(bin, [], { env, stdio: "inherit" });
      process.on("SIGINT", () => child.kill("SIGINT"));
      process.on("SIGTERM", () => child.kill("SIGTERM"));
      await new Promise<void>((res) => {
        child.on("exit", (code) => {
          if (code) process.exitCode = code;
          res();
        });
        child.on("error", (err) => {
          process.stderr.write(`pulse: ${err.message}\n`);
          process.exitCode = 1;
          res();
        });
      });
      return;
    }

    case "deploy": {
      const resolvedApp = resolveFileArg(args, "app/app.ts", "deploy");
      const root = repoRoot();
      const outDir = resolve(flag(args, "--out") ?? "pulse-dist");
      const schema = await loadSchema(resolvedApp);
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
        `export PULSE_APP="${resolvedApp}"\n` +
        `export PULSE_WORKER_SCRIPT="${workerScriptPath()}"\n` +
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
