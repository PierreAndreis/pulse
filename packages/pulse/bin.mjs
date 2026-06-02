#!/usr/bin/env node
// @onveloz/pulse — the umbrella entrypoint for `npx @onveloz/pulse new <name>`.
// It defers to the full Pulse CLI (scaffolder + gen/migrate/dev/deploy), so
// `npx @onveloz/pulse <cmd>` is exactly `pulse <cmd>`. Node strips the imported
// module's shebang, so re-importing the CLI bin runs its main() against argv.
import "@onveloz/pulse-cli/dist/bin.js";
