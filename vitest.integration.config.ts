import { defineConfig } from "vitest/config";

// Integration + stress tests spin up the real engine (pulse-server + Bun worker)
// against the dev Postgres. They run serially in a single fork — one engine,
// shared DB — and get generous timeouts.
export default defineConfig({
  resolve: { conditions: ["development"] },
  test: {
    include: ["tests/**/*.test.ts"],
    // The load suite has its own config (vitest.load.config.ts) and is timing-
    // sensitive (throughput ratios), so it's not part of the correctness gate.
    exclude: ["**/node_modules/**", "tests/load/**"],
    testTimeout: 30_000,
    hookTimeout: 120_000,
    fileParallelism: false,
    pool: "forks",
    poolOptions: { forks: { singleFork: true } },
  },
});
