import { defineConfig } from "vitest/config";

// Load tests spin up the real engine + Postgres and push significant traffic.
// They run serially with generous timeouts and report metrics to the console.
export default defineConfig({
  test: {
    include: ["tests/load/**/*.test.ts"],
    testTimeout: 120_000,
    hookTimeout: 120_000,
    fileParallelism: false,
    pool: "forks",
    poolOptions: { forks: { singleFork: true } },
  },
});
