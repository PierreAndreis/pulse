import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const ENGINE = process.env.PULSE_ENGINE_URL ?? "http://127.0.0.1:8787";

// The Vite dev server proxies the Pulse engine's HTTP + SSE endpoints so the
// browser app can talk to it same-origin (no CORS, SSE streaming preserved).
export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5273,
    proxy: {
      "/rpc": { target: ENGINE, changeOrigin: true },
      "/subscribe": { target: ENGINE, changeOrigin: true },
      "/unsubscribe": { target: ENGINE, changeOrigin: true },
      "/sync": { target: ENGINE, changeOrigin: true },
      "/health": { target: ENGINE, changeOrigin: true },
    },
  },
});
