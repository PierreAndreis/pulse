import { describe, expect, it } from "vitest";
import { scaffoldApp } from "./scaffold.js";

describe("scaffoldApp", () => {
  const files = scaffoldApp("my-app", "0.1.2");

  it("produces a complete, runnable app file set", () => {
    for (const f of [
      "package.json",
      "vite.config.ts",
      "tsconfig.json",
      "docker-compose.yml",
      "index.html",
      "app/schema.ts",
      "app/contract.ts",
      "app/todos.ts",
      "app/app.ts",
      "src/client.ts",
      "src/main.tsx",
      "src/App.tsx",
      "README.md",
      "AGENTS.md",
      "CLAUDE.md",
      ".gitignore",
    ]) {
      expect(Object.keys(files)).toContain(f);
    }
  });

  it("ships agent house-rules; CLAUDE.md imports AGENTS.md", () => {
    expect(files["AGENTS.md"]).toContain("agent instructions");
    expect(files["AGENTS.md"]).toContain("app/contract.ts");
    expect(files["AGENTS.md"]).toContain("subscribe({}, cb)");
    expect(files["CLAUDE.md"]).toContain("@AGENTS.md");
    // No auth section unless auth is wired.
    expect(files["AGENTS.md"]).not.toContain("Better Auth is wired");
  });

  it("wires the @onveloz SDK deps + the bundler preset", () => {
    const pkg = JSON.parse(files["package.json"]!);
    expect(pkg.dependencies["@onveloz/pulse-client"]).toBeDefined();
    expect(pkg.devDependencies["@onveloz/pulse-bundler"]).toBeDefined();
    expect(files["vite.config.ts"]).toContain("definePulseApp");
  });

  it("emits no PM-specific install-script config (engine has no postinstall)", () => {
    const pkg = JSON.parse(files["package.json"]!);
    // The engine downloads lazily on `pulse dev`, not via a postinstall, so the
    // scaffold needs no pnpm/bun build-script allowlist — install is PM-agnostic.
    expect(pkg.pnpm).toBeUndefined();
    expect(pkg.trustedDependencies).toBeUndefined();
  });

  it("pins the @onveloz deps to the injected CLI version", () => {
    const pkg = JSON.parse(scaffoldApp("x", "9.9.9")["package.json"]!);
    expect(pkg.dependencies["@onveloz/pulse-client"]).toBe("^9.9.9");
    expect(pkg.devDependencies["@onveloz/pulse-cli"]).toBe("^9.9.9");
  });

  it("sanitizes the project name and aligns the DB with the CLI default", () => {
    const dirty = scaffoldApp("My Cool App!", "0.1.2");
    expect(dirty["package.json"]).toContain('"name": "my-cool-app-"');
    expect(dirty["docker-compose.yml"]).toContain("container_name: my-cool-app--pg");
    // DB name matches the CLI's default DATABASE_URL so `pulse dev` connects with
    // zero config (no per-app DB name to keep in sync).
    expect(dirty["docker-compose.yml"]).toContain("POSTGRES_DB: pulse");
  });

  it("uses a single `dev` command that runs the engine + frontend", () => {
    const pkg = JSON.parse(files["package.json"]!);
    expect(pkg.scripts.dev).toBe("pulse dev app/app.ts --start vite");
    expect(pkg.scripts.db).toBe("docker compose up -d");
  });

  it("schema/contract/handlers reference the same table", () => {
    expect(files["app/schema.ts"]).toContain("todos:");
    expect(files["app/contract.ts"]).toContain("todos:");
    expect(files["app/todos.ts"]).toContain('os.todos.list.handler');
  });

  it("minimal template omits the todos demo and leaves a clean slate", () => {
    const min = scaffoldApp("my-app", "0.1.2", { template: "minimal" });
    expect(min["app/todos.ts"]).toBeUndefined();
    expect(min["app/contract.ts"]).toContain("export const contract = {}");
    expect(min["app/schema.ts"]).toContain("defineSchema({");
    expect(min["src/App.tsx"]).not.toContain("pulse.todos");
  });

  it("phrases the README's commands for the chosen package manager", () => {
    const npm = scaffoldApp("my-app", "0.1.2", { pm: "npm" });
    expect(npm["README.md"]).toContain("npm run dev");
    expect(npm["README.md"]).toContain("npm install");
    const pnpm = scaffoldApp("my-app", "0.1.2", { pm: "pnpm" });
    expect(pnpm["README.md"]).toContain("pnpm dev");
  });

  it("ships ambient Vite types so `import.meta.env` typechecks", () => {
    expect(files["src/vite-env.d.ts"]).toContain('reference types="vite/client"');
  });

  describe("with auth", () => {
    const a = scaffoldApp("my-app", "9.9.9", { auth: true });

    it("adds the Better Auth server, verify middleware, and client wiring", () => {
      expect(a["app/auth.ts"]).toContain("betterAuth(");
      expect(a["app/auth.ts"]).toContain("jwt()");
      expect(a["app/authed.ts"]).toContain("pulseAuth({ jwksUrl })");
      expect(a["src/auth-client.ts"]).toContain("jwtClient()");
      expect(a["src/client.ts"]).toContain("bearerAuth(");
      expect(a["vite.config.ts"]).toContain("toNodeHandler(auth)");
    });

    it("isolates Better Auth's tables in a separate `auth` schema", () => {
      expect(a["db/init.sql"]).toContain("create schema if not exists auth");
      expect(a["docker-compose.yml"]).toContain("/docker-entrypoint-initdb.d/init.sql");
      // Better Auth migrates its own tables — they are NOT in the Pulse schema.
      expect(a["app/schema.ts"]).not.toContain("sessions:");
    });

    it("pins the auth deps and adds the migrate script", () => {
      const pkg = JSON.parse(a["package.json"]!);
      expect(pkg.dependencies["@onveloz/pulse-auth"]).toBe("^9.9.9");
      expect(pkg.dependencies["better-auth"]).toBeDefined();
      expect(pkg.scripts["auth:migrate"]).toContain("@better-auth/cli");
    });

    it("scopes todos to the signed-in user via authed handlers", () => {
      expect(a["app/schema.ts"]).toContain("ownerId: v.string()");
      expect(a["app/todos.ts"]).toContain("os.todos.list.use(authed)");
      expect(a["app/todos.ts"]).toContain("ctx.userId");
    });

    it("ignores real env files but ships an example", () => {
      expect(a[".env.example"]).toContain("BETTER_AUTH_SECRET");
      expect(a[".gitignore"]).toContain(".env");
    });

    it("documents auth in AGENTS.md", () => {
      expect(a["AGENTS.md"]).toContain("Better Auth is wired");
      expect(a["AGENTS.md"]).toContain(".use(authed)");
      expect(a["AGENTS.md"]).toContain("auth:migrate");
    });
  });
});
