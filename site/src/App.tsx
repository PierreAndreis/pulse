import { useEffect, useState } from "react";
import { CursorPresence } from "./CursorPresence.js";
import { DocsPage } from "./DocsPage.js";
import { navigate, usePath, isDocs } from "./router.js";

const CMD = "npx @onveloz/pulse new";

function Hero() {
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    document.title = "Pulse — framework reativo e offline-first sobre Postgres";
  }, []);
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
  );
}

export function App() {
  const path = usePath();

  // Intercept same-origin link clicks → client navigation (no full reload).
  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      const a = (e.target as HTMLElement | null)?.closest("a");
      const href = a?.getAttribute("href");
      if (!a || !href) return;
      if (a.target && a.target !== "_self") return;
      const url = new URL(href, location.href);
      if (url.origin !== location.origin) return; // external (e.g. GitHub)
      // In-page hash on the same route: let the browser scroll natively.
      if (url.pathname === location.pathname && url.hash) return;
      e.preventDefault();
      navigate(url.pathname + url.hash);
      if (url.hash) {
        setTimeout(() => document.querySelector(url.hash)?.scrollIntoView({ behavior: "smooth" }), 30);
      } else {
        window.scrollTo(0, 0);
      }
    };
    document.addEventListener("click", onClick);
    return () => document.removeEventListener("click", onClick);
  }, []);

  return (
    <>
      {isDocs(path) ? <DocsPage /> : <Hero />}
      {/* Keyed by route so the presence channel switches cleanly across pages. */}
      <CursorPresence key={path} />
    </>
  );
}
