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
      ".gitignore",
    ]) {
      expect(Object.keys(files)).toContain(f);
    }
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
});
