import { createHash } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { binName, detectTarget, packageVersion, type EngineTarget } from "./platform.js";

const REPO = "PierreAndreis/pulse";
const here = dirname(fileURLToPath(import.meta.url));
const PKG_JSON = resolve(here, "..", "package.json");

/** Where downloaded engines are cached (versioned + per-target). */
export function cacheDir(version = packageVersion(PKG_JSON)): string {
  const base = process.env.PULSE_ENGINE_CACHE ?? join(homedir(), ".cache", "pulse", "engine");
  return join(base, version);
}

/** The GitHub Releases download URL for a given version + target. */
export function downloadUrl(version: string, target: EngineTarget): string {
  return `https://github.com/${REPO}/releases/download/v${version}/pulse-server-${target}`;
}

/**
 * Resolve the path to a runnable pulse-server binary, in priority order:
 *  1. `PULSE_SERVER_BIN` env override (used in dev / monorepo / Docker)
 *  2. a previously-downloaded binary in the version cache
 *  3. download it from GitHub Releases (verifying SHA256 if a sums file exists)
 *
 * Returns null only when no target matches the host and no override is set.
 */
export async function ensureEngine(opts: { download?: boolean } = {}): Promise<string | null> {
  const override = process.env.PULSE_SERVER_BIN;
  if (override) return existsSync(override) ? override : null;

  const version = packageVersion(PKG_JSON);
  const target = detectTarget();
  if (!target) return null;

  const dest = join(cacheDir(version), binName());
  if (existsSync(dest)) return dest;

  if (opts.download === false || process.env.PULSE_ENGINE_SKIP_DOWNLOAD) return null;

  mkdirSync(dirname(dest), { recursive: true });
  const url = downloadUrl(version, target);
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`pulse-engine: failed to download ${url} (HTTP ${res.status})`);
  }
  const bytes = Buffer.from(await res.arrayBuffer());

  // Integrity: the expected SHA256 must come from a trust root the attacker who
  // can edit the (mutable) GitHub Release cannot also rewrite. Prefer checksums
  // baked into THIS npm tarball (immutable + provenance-attested); fall back to
  // the Release SHA256SUMS only to fill a gap. A missing checksum for a known
  // version is FATAL — we never install an unverified binary.
  const want = expectedChecksum(version, target) ?? (await fetchReleaseChecksum(version, target));
  if (!want) {
    throw new Error(
      `pulse-engine: no known checksum for pulse-server-${target}@${version}; refusing to install. ` +
        "Build the engine and set PULSE_SERVER_BIN, or upgrade to a release that ships checksums.",
    );
  }
  const got = createHash("sha256").update(bytes).digest("hex");
  if (got !== want) {
    throw new Error(`pulse-engine: checksum mismatch for ${target} (got ${got}, want ${want})`);
  }

  writeFileSync(dest, bytes);
  chmodSync(dest, 0o755);
  return dest;
}

/** Expected SHA256 baked into this package's `checksums.json` (the immutable
 *  trust root), keyed by version → asset name. Undefined if absent. */
function expectedChecksum(version: string, target: EngineTarget): string | undefined {
  try {
    const map = JSON.parse(readFileSync(resolve(here, "..", "checksums.json"), "utf8")) as Record<
      string,
      Record<string, string>
    >;
    return map[version]?.[`pulse-server-${target}`];
  } catch {
    return undefined;
  }
}

/** Last-resort checksum from the Release's SHA256SUMS — only used to fill a gap
 *  the baked-in file doesn't cover (never the sole trust root for a known ver). */
async function fetchReleaseChecksum(
  version: string,
  target: EngineTarget,
): Promise<string | undefined> {
  try {
    const sums = await fetch(`https://github.com/${REPO}/releases/download/v${version}/SHA256SUMS`);
    if (!sums.ok) return undefined;
    return (await sums.text())
      .split("\n")
      .map((l) => l.trim().split(/\s+/))
      .find(([, name]) => name === `pulse-server-${target}`)?.[0];
  } catch {
    return undefined;
  }
}

/** Synchronous lookup of an already-resolved engine path (no download). */
export function enginePathSync(): string | null {
  const override = process.env.PULSE_SERVER_BIN;
  if (override) return existsSync(override) ? override : null;
  const dest = join(cacheDir(), binName());
  return existsSync(dest) ? dest : null;
}

export { detectTarget, downloadUrl as engineDownloadUrl };
export type { EngineTarget };
