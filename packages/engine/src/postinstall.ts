// Runs on `npm install @onveloz/pulse-engine`: best-effort download of the
// matching pulse-server binary. Never fails the install — a missing engine is
// recoverable later (PULSE_SERVER_BIN, Docker, or a manual `pulse` run that
// triggers ensureEngine()).
import { detectTarget } from "./platform.js";
import { ensureEngine } from "./index.js";

async function main(): Promise<void> {
  if (process.env.PULSE_ENGINE_SKIP_DOWNLOAD) {
    process.stdout.write("pulse-engine: download skipped (PULSE_ENGINE_SKIP_DOWNLOAD)\n");
    return;
  }
  const target = detectTarget();
  if (!target) {
    process.stdout.write(
      `pulse-engine: no prebuilt engine for ${process.platform}/${process.arch}; ` +
        "set PULSE_SERVER_BIN to a locally-built pulse-server.\n",
    );
    return;
  }
  try {
    const path = await ensureEngine();
    if (path) process.stdout.write(`pulse-engine: ready (${target}) at ${path}\n`);
  } catch (e) {
    process.stdout.write(
      `pulse-engine: could not download the engine (${e instanceof Error ? e.message : e}). ` +
        "It will be fetched on first use, or set PULSE_SERVER_BIN.\n",
    );
  }
}

void main();
