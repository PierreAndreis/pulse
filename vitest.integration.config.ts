import { defineConfig } from "vitest/config";

// Integration + stress tests spin up the real engine (pulse-server + Bun worker)
// against the dev Postgres. They run serially in a single fork — one engine,
// shared DB — and get generous timeouts.
export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 120_000,
    fileParallelism: false,
    pool: "forks",
    poolOptions: { forks: { singleFork: true } },
  },
});
