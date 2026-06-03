import { describe, expect, it } from "vitest";
import { detectPackageManager, installCmd, isPackageManager, runCmd } from "./pm.js";

describe("package-manager helpers", () => {
  it("detects the PM from npm_config_user_agent", () => {
    expect(detectPackageManager({ npm_config_user_agent: "pnpm/8.6.0 npm/? node/v20" })).toBe("pnpm");
    expect(detectPackageManager({ npm_config_user_agent: "yarn/1.22.0 npm/? node/v20" })).toBe("yarn");
    expect(detectPackageManager({ npm_config_user_agent: "bun/1.0.0" })).toBe("bun");
  });

  it("falls back to pnpm with no usable hint", () => {
    expect(detectPackageManager({})).toBe("pnpm");
    expect(detectPackageManager({ npm_config_user_agent: "deno/1.0" })).toBe("pnpm");
  });

  it("guards the PM union", () => {
    expect(isPackageManager("npm")).toBe(true);
    expect(isPackageManager("cargo")).toBe(false);
  });

  it("phrases install + run per PM", () => {
    expect(installCmd("yarn")).toBe("yarn");
    expect(installCmd("npm")).toBe("npm install");
    expect(runCmd("npm", "dev")).toBe("npm run dev");
    expect(runCmd("pnpm", "dev")).toBe("pnpm dev");
    expect(runCmd("bun", "dev")).toBe("bun dev");
  });
});
