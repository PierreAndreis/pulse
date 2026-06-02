// Bakes the engine's checksum trust root before publishing to npm.
//
// The @onveloz/pulse-engine package downloads its native binary from the
// (mutable) GitHub Release, but verifies it against checksums shipped INSIDE the
// npm tarball (immutable + provenance-attested) — so an attacker who can edit a
// Release cannot also rewrite the expected hash. Those checksums can only be
// known after the binaries are built, so CI runs this after downloading the
// build artifacts and before `pnpm publish`.
//
// Usage: node scripts/bake-engine-checksums.mjs <artifacts-dir>
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

const artifactsDir = process.argv[2] ?? "artifacts";
const enginePkg = JSON.parse(readFileSync("packages/engine/package.json", "utf8"));
const version = enginePkg.version;

const binaries = readdirSync(artifactsDir).filter((f) => f.startsWith("pulse-server-"));
if (binaries.length === 0) {
  throw new Error(`no pulse-server-* binaries found in ${artifactsDir}`);
}

const map = { [version]: {} };
for (const name of binaries) {
  map[version][name] = createHash("sha256").update(readFileSync(join(artifactsDir, name))).digest("hex");
}

const out = resolve("packages/engine/checksums.json");
writeFileSync(out, JSON.stringify(map, null, 2) + "\n");
console.log(`baked ${binaries.length} checksum(s) for v${version} -> ${out}`);
console.log(JSON.stringify(map, null, 2));
