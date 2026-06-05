// Real-network fan-out benchmark: each engine node runs in its OWN container on a
// Docker network, talking to a Postgres container over real TCP (not loopback).
// Brings up N nodes in routed mode, measures cross-node bus traffic, tears down,
// repeats in broadcast mode, and prints the comparison.
//
// Prereq: docker image `pulse-bench:latest` (see Dockerfile.bench).
// Run:    bun scripts/bench-network.mjs        (from repo root)
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";
import { createClient } from "@onveloz/pulse-client";
import { generateDDL } from "../packages/cli/src/ddl.js";
import fanoutSchema, { K } from "../tests/load/fixtures/fanout/schema.js";

const exec = promisify(execFile);
const REPO = resolve(import.meta.dirname, "..");
const NET = "pulse-bench-net";
const PG = "pulse-bench-pg";
const IMAGE = "pulse-bench:latest";
const N = K;
const WRITES_PER_TABLE = 15;
const HOST_PORT0 = 9400; // node i published on HOST_PORT0 + i
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function dockerQuiet(args) {
  try {
    await exec("docker", args);
  } catch {
    /* ignore (already-exists / not-found) */
  }
}

async function waitHttp(url, ms = 60_000) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      /* not up */
    }
    await sleep(250);
  }
  throw new Error(`timeout waiting for ${url}`);
}

async function setup() {
  await dockerQuiet(["rm", "-f", PG, ...Array.from({ length: N }, (_, i) => `pulse-bench-n${i}`)]);
  await dockerQuiet(["network", "rm", NET]);
  await exec("docker", ["network", "create", NET]);
  await exec("docker", [
    "run", "-d", "--name", PG, "--network", NET, "--network-alias", "pg",
    "-e", "POSTGRES_USER=pulse", "-e", "POSTGRES_PASSWORD=pulse", "-e", "POSTGRES_DB=pulse",
    "-p", "54400:5432", "postgres:16-alpine",
    "-c", "wal_level=logical", "-c", "max_connections=200",
  ]);
  // Wait for PG, then create the K tables.
  for (let i = 0; i < 60; i++) {
    try {
      await exec("docker", ["exec", PG, "pg_isready", "-U", "pulse", "-d", "pulse"]);
      break;
    } catch {
      await sleep(500);
    }
  }
  await exec("docker", ["exec", "-i", PG, "psql", "-U", "pulse", "-d", "pulse", "-v", "ON_ERROR_STOP=1", "-c", generateDDL(fanoutSchema)]);
}

async function startNodes(broadcast) {
  for (let i = 0; i < N; i++) {
    await exec("docker", [
      "run", "-d", "--name", `pulse-bench-n${i}`, "--network", NET,
      "-v", `${REPO}:/app`, "-w", "/app",
      "-e", "DATABASE_URL=postgres://pulse:pulse@pg:5432/pulse",
      "-e", "PULSE_WORKER_BIN=bun",
      "-e", "PULSE_WORKER_SCRIPT=/app/packages/runtime-node/src/worker.ts",
      "-e", "PULSE_APP=/app/tests/load/fixtures/fanout/app.ts",
      "-e", `PULSE_FANOUT_TABLES=${K}`,
      "-e", `PULSE_BUS_BROADCAST=${broadcast ? "1" : "0"}`,
      "-e", "PULSE_OLTP_MAX_CONNS=3", "-e", "PULSE_OLAP_MAX_CONNS=2",
      "-p", `${HOST_PORT0 + i}:8787`,
      IMAGE,
    ]);
  }
  await Promise.all(Array.from({ length: N }, (_, i) => waitHttp(`http://localhost:${HOST_PORT0 + i}/health`)));
}

async function stopNodes() {
  await dockerQuiet(["rm", "-f", ...Array.from({ length: N }, (_, i) => `pulse-bench-n${i}`)]);
}

async function runPhase(broadcast) {
  await startNodes(broadcast);
  const urls = Array.from({ length: N }, (_, i) => `http://localhost:${HOST_PORT0 + i}`);
  const clients = urls.map((url) => createClient({ url, headers: () => ({ authorization: "Bearer t" }) }));
  const seen = urls.map(() => 0);
  const unsubs = clients.map((c, i) => c.w[`count${i}`].subscribe({}, () => (seen[i] += 1)));
  // wait for all initial pushes (interest registered)
  for (let t = 0; t < 200 && !seen.every((s) => s >= 1); t++) await sleep(50);
  await sleep(1000);
  for (let round = 0; round < WRITES_PER_TABLE; round++) {
    for (let i = 0; i < K; i++) await clients[0].w[`add${i}`].call({ n: round });
  }
  await sleep(2500);
  const busEvents = await Promise.all(urls.map(async (u) => (await (await fetch(`${u}/metrics`)).json()).busEvents));
  for (const u of unsubs) u();
  await stopNodes();
  return busEvents.reduce((a, b) => a + b, 0);
}

async function main() {
  await setup();
  const routed = await runPhase(false);
  const broadcast = await runPhase(true);
  const writes = WRITES_PER_TABLE * K;
  console.log(`\nreal-network fan-out  nodes=${N} tables=${K} writes=${writes}`);
  console.log(`  routed    bus events: ${routed}  (${(routed / writes).toFixed(2)}/write)`);
  console.log(`  broadcast bus events: ${broadcast}  (${(broadcast / writes).toFixed(2)}/write)`);
  console.log(`  routing cut cross-node traffic ${(broadcast / Math.max(1, routed)).toFixed(1)}x`);
  await dockerQuiet(["rm", "-f", PG]);
  await dockerQuiet(["network", "rm", NET]);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
