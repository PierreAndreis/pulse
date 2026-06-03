// The `pulse new` flow: a Prisma/Convex-style interactive scaffold. Prompts for
// the project name, package manager, and starter, then optionally installs
// deps, starts Postgres, and inits git. Non-interactive when stdin isn't a TTY
// or `--yes` is passed — flags + defaults fill in for every prompt.

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";
import * as p from "@clack/prompts";
import { detectPackageManager, installCmd, isPackageManager, runCmd, type PackageManager } from "./pm.js";
import { scaffoldApp, type Template } from "./scaffold.js";

interface Flags {
  name?: string;
  pm?: PackageManager;
  template?: Template;
  /** Accept every default without prompting. */
  yes: boolean;
  /** Tri-state overrides: undefined = ask, true/false = forced by a flag. */
  auth?: boolean;
  install?: boolean;
  db?: boolean;
  git?: boolean;
}

/** Parse `pulse new` argv (already sliced past the `new` command). */
function parseFlags(args: string[]): Flags {
  const flags: Flags = { yes: false };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    switch (a) {
      case "--yes":
      case "-y":
        flags.yes = true;
        break;
      case "--pm": {
        const v = args[++i];
        if (!v || !isPackageManager(v)) throw new Error(`--pm must be one of pnpm|npm|yarn|bun (got ${v ?? ""})`);
        flags.pm = v;
        break;
      }
      case "--template": {
        const v = args[++i];
        if (v !== "todos" && v !== "minimal") throw new Error(`--template must be todos|minimal (got ${v ?? ""})`);
        flags.template = v;
        break;
      }
      case "--auth":
        flags.auth = true;
        break;
      case "--no-auth":
        flags.auth = false;
        break;
      case "--install":
        flags.install = true;
        break;
      case "--no-install":
        flags.install = false;
        break;
      case "--db":
        flags.db = true;
        break;
      case "--no-db":
        flags.db = false;
        break;
      case "--git":
        flags.git = true;
        break;
      case "--no-git":
        flags.git = false;
        break;
      default:
        if (a && !a.startsWith("-") && flags.name === undefined) flags.name = a;
        else if (a) throw new Error(`unknown option: ${a}`);
    }
  }
  return flags;
}

/** Run a command in `cwd`, capturing output; resolves with success + the
 *  combined stream so a spinner can stay quiet and only dump logs on failure. */
function run(cmd: string, cmdArgs: string[], cwd: string): Promise<{ ok: boolean; output: string }> {
  return new Promise((res) => {
    const child = spawn(cmd, cmdArgs, { cwd, stdio: ["ignore", "pipe", "pipe"], shell: false });
    let output = "";
    child.stdout.on("data", (d: Buffer) => (output += d));
    child.stderr.on("data", (d: Buffer) => (output += d));
    child.on("error", (e) => res({ ok: false, output: output + e.message }));
    child.on("exit", (code) => res({ ok: code === 0, output }));
  });
}

/** Abort cleanly on Ctrl-C / Esc from any clack prompt. */
function bail<T>(value: T | symbol): T {
  if (p.isCancel(value)) {
    p.cancel("Cancelled.");
    process.exit(130);
  }
  return value as T;
}

/** The whole `pulse new` experience. `version` pins the scaffold's deps. */
export async function runNew(args: string[], version: string): Promise<void> {
  const flags = parseFlags(args);
  // No TTY (CI, piped) ⇒ behave like `--yes`: never block on a prompt.
  const interactive = process.stdin.isTTY === true && !flags.yes;

  p.intro("create a Pulse app");

  const name = flags.name
    ? flags.name
    : interactive
      ? bail(
          await p.text({
            message: "Project name",
            placeholder: "my-pulse-app",
            defaultValue: "my-pulse-app",
            validate: (v) => (v && /[/\\]/.test(v) ? "no slashes in the name" : undefined),
          }),
        )
      : "my-pulse-app";

  const dir = resolve(name);
  if (existsSync(dir)) throw new Error(`${name}/ already exists — pick another name or remove it`);

  const pm: PackageManager = flags.pm
    ? flags.pm
    : interactive
      ? bail(
          await p.select({
            message: "Package manager",
            initialValue: detectPackageManager(),
            options: [
              { value: "pnpm", label: "pnpm" },
              { value: "npm", label: "npm" },
              { value: "yarn", label: "yarn" },
              { value: "bun", label: "bun" },
            ],
          }),
        )
      : detectPackageManager();

  const template: Template = flags.template
    ? flags.template
    : interactive
      ? bail(
          await p.select({
            message: "Starter template",
            initialValue: "todos" as Template,
            options: [
              { value: "todos", label: "Todos", hint: "a working reactive + offline-first demo" },
              { value: "minimal", label: "Minimal", hint: "an empty schema to build from" },
            ],
          }),
        )
      : "todos";

  const auth =
    flags.auth ??
    (interactive
      ? bail(await p.confirm({ message: "Add authentication (Better Auth)?", initialValue: false }))
      : false);

  const doInstall =
    flags.install ?? (interactive ? bail(await p.confirm({ message: "Install dependencies now?" })) : true);
  const doDb =
    flags.db ?? (interactive ? bail(await p.confirm({ message: "Start Postgres (Docker) now?", initialValue: false })) : false);
  const doGit =
    flags.git ??
    (interactive ? bail(await p.confirm({ message: "Initialize a git repository?", initialValue: true })) : false);

  // 1. Write the file map.
  const s = p.spinner();
  s.start("Scaffolding project");
  const files = scaffoldApp(name, version, { pm, template, auth });
  for (const [rel, contents] of Object.entries(files)) {
    const out = resolve(dir, rel);
    await mkdir(dirname(out), { recursive: true });
    await writeFile(out, contents, "utf8");
  }
  s.stop(`Created ${name}/ (${Object.keys(files).length} files)`);

  // 2. git init (before install so node_modules never lands in the first commit).
  if (doGit) {
    s.start("Initializing git repository");
    const init = await run("git", ["init", "-q"], dir);
    if (init.ok) {
      await run("git", ["add", "-A"], dir);
      const commit = await run("git", ["commit", "-q", "-m", "Initial commit from Pulse"], dir);
      s.stop(commit.ok ? "Initialized git repository" : "git repo created (nothing committed)");
    } else {
      s.stop("Skipped git (is git installed?)");
    }
  }

  // 3. Install dependencies.
  if (doInstall) {
    s.start(`Installing dependencies with ${pm}`);
    const res = await run(pm, pm === "yarn" ? [] : ["install"], dir);
    if (res.ok) {
      s.stop("Installed dependencies");
    } else {
      s.stop(`Install failed — run \`${installCmd(pm)}\` yourself`);
      p.log.error(res.output.trim().split("\n").slice(-12).join("\n"));
    }
  }

  // 4. Start Postgres.
  if (doDb) {
    s.start("Starting Postgres (docker compose up -d)");
    const res = await run("docker", ["compose", "up", "-d"], dir);
    s.stop(res.ok ? "Postgres is up" : "Could not start Postgres (is Docker running?)");
  }

  // 5. Tailored next steps.
  const cd = relative(process.cwd(), dir) || name;
  const steps = [`cd ${cd}`];
  if (!doInstall) steps.push(installCmd(pm));
  if (!doDb) steps.push(`${runCmd(pm, "db")}    # start Postgres (Docker)`);
  // Better Auth's tables must be migrated once, after the DB is up.
  if (auth) steps.push(`${runCmd(pm, "auth:migrate")}  # create Better Auth tables`);
  steps.push(`${runCmd(pm, "dev")}   # engine + Vite, all of it`);
  p.note(steps.join("\n"), "Next steps");
  p.outro("Happy building with Pulse 🟣");
}
