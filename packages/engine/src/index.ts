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

  // Optional integrity check: a SHA256SUMS asset listing "<sha>  pulse-server-<target>".
  const sumsUrl = `https://github.com/${REPO}/releases/download/v${version}/SHA256SUMS`;
  try {
    const sums = await fetch(sumsUrl);
    if (sums.ok) {
      const want = (await sums.text())
        .split("\n")
        .map((l) => l.trim().split(/\s+/))
        .find(([, name]) => name === `pulse-server-${target}`)?.[0];
      if (want) {
        const got = createHash("sha256").update(bytes).digest("hex");
        if (got !== want) {
          throw new Error(`pulse-engine: checksum mismatch for ${target} (got ${got}, want ${want})`);
        }
      }
    }
  } catch (e) {
    if (e instanceof Error && e.message.includes("checksum mismatch")) throw e;
    // network hiccup fetching sums → proceed without verification (best-effort)
  }

  writeFileSync(dest, bytes);
  chmodSync(dest, 0o755);
  return dest;
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
