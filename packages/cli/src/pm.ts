// Package-manager detection + the per-PM command strings the `new` flow needs.
// Pure (no fs/spawn) so it's trivially unit-testable.

export type PackageManager = "pnpm" | "npm" | "yarn" | "bun";

export const PACKAGE_MANAGERS: readonly PackageManager[] = ["pnpm", "npm", "yarn", "bun"];

/** True when `v` is one of the supported package managers. */
export function isPackageManager(v: string): v is PackageManager {
  return (PACKAGE_MANAGERS as readonly string[]).includes(v);
}

/**
 * Detect the package manager that invoked us. When run via `npm create`,
 * `pnpm create`, `yarn create`, or `bunx`, the PM sets `npm_config_user_agent`
 * to e.g. `pnpm/8.6.0 npm/? node/v20…` — we read the leading token. Falls back
 * to pnpm (the project's default) when there's no usable hint.
 */
export function detectPackageManager(env: NodeJS.ProcessEnv = process.env): PackageManager {
  const ua = env.npm_config_user_agent ?? "";
  const name = ua.split("/")[0];
  return name && isPackageManager(name) ? name : "pnpm";
}

/** The command that installs dependencies, e.g. `pnpm install` / `yarn`. */
export function installCmd(pm: PackageManager): string {
  return pm === "yarn" ? "yarn" : `${pm} install`;
}

/** The command that runs a package.json script, e.g. `npm run dev` / `pnpm dev`. */
export function runCmd(pm: PackageManager, script: string): string {
  return pm === "npm" ? `npm run ${script}` : `${pm} ${script}`;
}
