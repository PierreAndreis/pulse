import { describe, expect, it } from "vitest";
import { resolve } from "node:path";
import { buildEngineEnv, resolveEngineBin } from "./dev.js";

describe("resolveEngineBin", () => {
  const root = "/repo";
  it("prefers release over debug when both exist", () => {
    const exists = (p: string) => p.includes("target/");
    expect(resolveEngineBin(root, {}, exists)).toBe(resolve(root, "target/release/pulse-server"));
  });

  it("falls back to debug when release is missing", () => {
    const exists = (p: string) => p.endsWith("target/debug/pulse-server");
    expect(resolveEngineBin(root, {}, exists)).toBe(resolve(root, "target/debug/pulse-server"));
  });

  it("returns null when no binary is built", () => {
    expect(resolveEngineBin(root, {}, () => false)).toBeNull();
  });

  it("honors PULSE_SERVER_BIN when it exists", () => {
    const exists = (p: string) => p === "/custom/pulse-server";
    expect(resolveEngineBin(root, { PULSE_SERVER_BIN: "/custom/pulse-server" }, exists)).toBe(
      "/custom/pulse-server",
    );
  });

  it("returns null when PULSE_SERVER_BIN is set but missing", () => {
    expect(resolveEngineBin(root, { PULSE_SERVER_BIN: "/nope" }, () => false)).toBeNull();
  });
});

describe("buildEngineEnv", () => {
  it("sets the required engine env, with CLI options overriding ambient", () => {
    const env = buildEngineEnv(
      { appPath: "/app.ts", workerScript: "/worker.ts", port: "9000", databaseUrl: "postgres://x" },
      { PULSE_PORT: "1", DATABASE_URL: "postgres://ambient", PATH: "/bin" },
    );
    expect(env.PULSE_APP).toBe("/app.ts");
    expect(env.PULSE_WORKER_SCRIPT).toBe("/worker.ts");
    expect(env.PULSE_PORT).toBe("9000"); // CLI wins
    expect(env.DATABASE_URL).toBe("postgres://x"); // CLI wins
    expect(env.PULSE_WORKER_BIN).toBe("bun"); // default
    expect(env.PATH).toBe("/bin"); // ambient preserved
  });

  it("falls back to ambient then to defaults", () => {
    const env = buildEngineEnv(
      { appPath: "/app.ts", workerScript: "/worker.ts" },
      { PULSE_PORT: "5555" },
    );
    expect(env.PULSE_PORT).toBe("5555"); // ambient
    expect(env.DATABASE_URL).toBe("postgres://pulse:pulse@localhost:54329/pulse"); // default
    expect(env.PULSE_WORKER_BIN).toBe("bun"); // default
  });
});
