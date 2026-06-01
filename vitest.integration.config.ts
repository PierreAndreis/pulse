import { defineConfig } from "vitest/config";

// Integration + stress tests spin up the real engine (pulse-server + Bun worker)
// against the dev Postgres. They run serially in a single fork — one engine,
// shared DB — and get generous timeouts.
export default defineConfig({
  resolve: { conditions: ["development"] },
  test: {
    include: ["tests/integration/**/*.test.ts"],
    // load/ and stress/ are throughput & zero-error-under-sustained-load probes,
    // not correctness gates — they flake on shared CI runners and have their own
    // configs / on-demand runs. Keep the per-PR gate to the integration suite.
    exclude: ["**/node_modules/**", "tests/load/**", "tests/stress/**"],
    testTimeout: 30_000,
    hookTimeout: 120_000,
    fileParallelism: false,
    pool: "forks",
    poolOptions: { forks: { singleFork: true } },
  },
});
