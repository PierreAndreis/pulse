import { useState } from "react";
import { CursorPresence } from "./CursorPresence.js";

const CMD = "npx @onveloz/pulse new";

// The landing hero, ported 1:1 from the old static index.html (styles live in
// /public/styles.css). The live-cursor layer is mounted on top.
export function App() {
  const [copied, setCopied] = useState(false);
  async function copy() {
    try {
      await navigator.clipboard.writeText(CMD);
      setCopied(true);
      setTimeout(() => setCopied(false), 1600);
    } catch {
      /* clipboard unavailable */
    }
  }
  return (
    <>
      <main className="wrap">
        <div className="title">
          <h1>Pulse</h1>
          <span className="tag">[alpha]</span>
        </div>
        <p className="tagline">Reativo e offline-first. Sobre o seu Postgres.</p>

        <div className="cmd">
          <code>
            <span className="prompt">$ </span>
            {CMD}
          </code>
          <button className={copied ? "copy done" : "copy"} onClick={copy} aria-label="Copiar">
            {copied ? "Copiado" : "Copiar"}
          </button>
        </div>

        <div className="links">
          <a href="/docs.html">Docs</a>
          <a href="https://github.com/PierreAndreis/pulse">GitHub</a>
        </div>
      </main>

      <CursorPresence />
    </>
  );
}
