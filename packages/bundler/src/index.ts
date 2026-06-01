import react from "@vitejs/plugin-react";
import { defineConfig, type UserConfig } from "vite";

/** The @onveloz/pulse-* packages a Pulse app consumes. Excluded from Vite's dep
 *  pre-bundling so linked/source edits are picked up and `yjs` dedupes. */
const PULSE_PACKAGES = [
  "@onveloz/pulse-client",
  "@onveloz/pulse-react",
  "@onveloz/pulse-schema",
  "@onveloz/pulse-contract",
];

export interface PulseAppOptions {
  /** Dev server port (default 5273). */
  port?: number;
  /** Dev server host (default 127.0.0.1 — binds IPv4 so the engine is reachable). */
  host?: string;
  /**
   * Engine URL. Informational only by default: the Pulse client talks to the
   * engine directly (its CORS is permissive and SSE must not be proxied/buffered).
   * Set `proxy: true` to route /rpc,/sync,etc. through Vite instead.
   */
  engineUrl?: string;
  /** Route engine paths through the Vite dev proxy. Off by default (SSE buffering). */
  proxy?: boolean;
  /** Extra Vite config, deep-merged last (escape hatch). */
  vite?: UserConfig;
}

const ENGINE_PATHS = ["/rpc", "/sync", "/subscribe", "/unsubscribe", "/health"];

/** Deep-ish merge: arrays concat, plain objects merge, scalars overwrite. */
function merge<T>(base: T, extra?: Partial<T>): T {
  if (!extra) return base;
  const out: Record<string, unknown> = { ...(base as Record<string, unknown>) };
  for (const [k, v] of Object.entries(extra as Record<string, unknown>)) {
    const cur = out[k];
    if (Array.isArray(cur) && Array.isArray(v)) out[k] = [...cur, ...v];
    else if (cur && v && typeof cur === "object" && typeof v === "object" && !Array.isArray(v))
      out[k] = merge(cur, v as Record<string, unknown>);
    else out[k] = v;
  }
  return out as T;
}

/**
 * One-line Vite config for a Pulse app. In `vite.config.ts`:
 *
 * ```ts
 * import { definePulseApp } from "@onveloz/pulse-bundler";
 * export default definePulseApp();
 * ```
 *
 * Bundles React (via @vitejs/plugin-react), keeps the SSE stream unbuffered by
 * NOT proxying the engine by default, dedupes `yjs`, and excludes the @onveloz/*
 * packages from pre-bundling so their ESM dist (or linked source) resolves cleanly.
 */
export function definePulseApp(opts: PulseAppOptions = {}): UserConfig {
  const { port = 5273, host = "127.0.0.1", engineUrl = "http://127.0.0.1:8787", proxy = false } = opts;

  const base: UserConfig = {
    plugins: [react()],
    server: {
      host,
      port,
      ...(proxy
        ? {
            proxy: Object.fromEntries(
              ENGINE_PATHS.map((p) => [p, { target: engineUrl, changeOrigin: true }]),
            ),
          }
        : {}),
    },
    // Don't pre-bundle the SDK (picks up linked/source edits); single yjs copy.
    optimizeDeps: { exclude: PULSE_PACKAGES },
    resolve: { dedupe: ["yjs", "react", "react-dom"] },
    define: { "import.meta.env.VITE_PULSE_ENGINE_URL": JSON.stringify(engineUrl) },
  };

  return defineConfig(merge(base, opts.vite));
}

/** Alias for ergonomics: `export default pulseApp()`. */
export const pulseApp = definePulseApp;
