import { describe, expect, it } from "vitest";
import { scaffoldApp } from "./scaffold.js";

describe("scaffoldApp", () => {
  const files = scaffoldApp("my-app");

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

  it("allowlists the engine + esbuild install scripts for pnpm and bun", () => {
    const pkg = JSON.parse(files["package.json"]!);
    expect(pkg.pnpm.onlyBuiltDependencies).toContain("@onveloz/pulse-engine");
    expect(pkg.pnpm.onlyBuiltDependencies).toContain("esbuild");
    expect(pkg.trustedDependencies).toContain("@onveloz/pulse-engine");
    expect(pkg.trustedDependencies).toContain("esbuild");
  });

  it("uses the project name and sanitizes it for the DB", () => {
    const dirty = scaffoldApp("My Cool App!");
    expect(dirty["package.json"]).toContain('"name": "my-cool-app-"');
    // db name has dashes → underscores
    expect(dirty["docker-compose.yml"]).toMatch(/POSTGRES_DB: my_cool_app_/);
  });

  it("schema/contract/handlers reference the same table", () => {
    expect(files["app/schema.ts"]).toContain("todos:");
    expect(files["app/contract.ts"]).toContain("todos:");
    expect(files["app/todos.ts"]).toContain('os.todos.list.handler');
  });
});
