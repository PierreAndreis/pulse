import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Runtime tests only. Type-level assertions live in *.test-d.ts and are
    // verified by each package's `tsc --noEmit` (see `pnpm typecheck`), which is
    // more reliable than Vitest's bundled type checker.
    include: ["packages/*/src/**/*.{test,spec}.ts"],
    exclude: ["**/*.test-d.ts", "**/node_modules/**", "**/dist/**"],
  },
});
