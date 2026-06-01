// The Node runtime worker that the Rust engine drives over IPC: it loads the
// user's compiled handler modules, executes a procedure by path with a given
// context + input, and (for the instrumented `ctx.db`) round-trips DB ops back
// to the engine. Built out in M1 (queries/mutations) and M6 (actions).
export const RUNTIME_PROTOCOL_VERSION = 0;
