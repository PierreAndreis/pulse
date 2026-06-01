import { existsSync, readFileSync } from "node:fs";

/** A supported engine build target, named like the Rust target triple. */
export type EngineTarget =
  | "aarch64-apple-darwin"
  | "x86_64-apple-darwin"
  | "x86_64-unknown-linux-gnu"
  | "x86_64-unknown-linux-musl"
  | "aarch64-unknown-linux-gnu";

/** Detect whether the Linux userland is musl (Alpine) vs glibc. */
function isMusl(): boolean {
  // process.report includes the dynamic linker on Linux; fall back to scanning
  // for a musl ldd. Both are best-effort and only consulted on linux.
  try {
    const report = (process.report?.getReport?.() ?? {}) as {
      header?: { glibcVersionRuntime?: string };
    };
    if (report.header && !report.header.glibcVersionRuntime) return true;
  } catch {
    // ignore
  }
  return existsSync("/etc/alpine-release");
}

/** Resolve the engine target triple for the current host, or null if unsupported. */
export function detectTarget(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
): EngineTarget | null {
  if (platform === "darwin") {
    return arch === "arm64" ? "aarch64-apple-darwin" : "x86_64-apple-darwin";
  }
  if (platform === "linux") {
    if (arch === "arm64") return "aarch64-unknown-linux-gnu";
    if (arch === "x64") return isMusl() ? "x86_64-unknown-linux-musl" : "x86_64-unknown-linux-gnu";
  }
  return null;
}

/** The binary filename inside an extracted release asset (no .exe on unix). */
export function binName(): string {
  return "pulse-server";
}

/** Read this package's version (used to pick the matching release tag). */
export function packageVersion(pkgJsonPath: string): string {
  return (JSON.parse(readFileSync(pkgJsonPath, "utf8")) as { version: string }).version;
}
