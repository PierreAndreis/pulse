// Committed (not built) postinstall shim. Runs on every install of this package.
// When installed as a published dependency, ./dist/postinstall.js exists and we
// run it to fetch the engine binary. In the source monorepo (no dist yet, or
// CI), it's a safe no-op so `pnpm install` never fails on a missing build.
const { existsSync } = require("node:fs");
const { pathToFileURL } = require("node:url");
const { join } = require("node:path");

const built = join(__dirname, "dist", "postinstall.js");
if (existsSync(built)) {
  import(pathToFileURL(built).href).catch((e) => {
    // Best-effort: never fail the install over an engine download.
    process.stdout.write(`pulse-engine: postinstall skipped (${e && e.message ? e.message : e})\n`);
  });
}
