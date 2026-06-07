import { resolve } from "node:path";
import { definePulseApp } from "@onveloz/pulse-bundler";

// The bundler inlines `engineUrl` into the client at build time. Veloz provides
// VITE_PULSE_ENGINE_URL for the deployed build; local `pulse dev` falls back to
// the local engine.
export default definePulseApp({
  engineUrl: process.env.VITE_PULSE_ENGINE_URL ?? "http://127.0.0.1:8787",
  // Two pages, each its own presence channel: the landing page and the docs.
  vite: {
    build: {
      rollupOptions: {
        input: {
          main: resolve(process.cwd(), "index.html"),
          docs: resolve(process.cwd(), "docs.html"),
        },
      },
    },
  },
});
