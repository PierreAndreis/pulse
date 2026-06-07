import { useEffect, useRef, useState } from "react";
import type { Doc } from "@onveloz/pulse-schema";
import { pulse } from "./client.js";

type Cursor = Doc<"cursors">;

const REPORT_MS = 66; // ~15 Hz — throttle so a mouse-move flood isn't a write flood

/** Stable per-tab id, kept across reloads in this session. */
function clientId(): string {
  const key = "pulse:cid";
  let id = sessionStorage.getItem(key);
  if (!id) {
    id = Math.random().toString(36).slice(2) + Date.now().toString(36);
    sessionStorage.setItem(key, id);
  }
  return id;
}

/** Deterministic, readable color from the id. */
function colorFor(id: string): string {
  let h = 0;
  for (const ch of id) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  return `hsl(${h % 360} 85% 62%)`;
}

/** ISO-3166 alpha-2 → flag emoji (regional indicators). "" → globe. */
function flag(cc: string): string {
  if (!/^[A-Za-z]{2}$/.test(cc)) return "🌐";
  return String.fromCodePoint(...[...cc.toUpperCase()].map((c) => 0x1f1e6 + c.charCodeAt(0) - 65));
}

/** Country from the visitor's IP, with a browser-locale fallback. */
async function lookupCountry(): Promise<string> {
  try {
    const r = await fetch("https://ipapi.co/country/", { signal: AbortSignal.timeout(2500) });
    if (r.ok) {
      const t = (await r.text()).trim();
      if (/^[A-Za-z]{2}$/.test(t)) return t.toUpperCase();
    }
  } catch {
    /* fall through to locale */
  }
  const region = navigator.language?.split("-")[1];
  return region && /^[A-Za-z]{2}$/.test(region) ? region.toUpperCase() : "";
}

export function CursorPresence() {
  const [cursors, setCursors] = useState<Cursor[]>([]);
  const me = useRef(clientId());
  const color = useRef(colorFor(me.current));
  const country = useRef("");

  useEffect(() => {
    let alive = true;
    void lookupCountry().then((c) => {
      if (alive) country.current = c;
    });

    const unsub = pulse.presence.list.subscribe({}, (rows) => setCursors(rows as Cursor[]));

    // Coalesce pointer moves to one report per REPORT_MS window.
    let pending: { x: number; y: number } | null = null;
    const onMove = (e: PointerEvent) => {
      pending = { x: e.clientX / window.innerWidth, y: e.clientY / window.innerHeight };
    };
    const timer = setInterval(() => {
      if (!pending) return;
      const { x, y } = pending;
      pending = null;
      void pulse.presence.move
        .call({ clientId: me.current, x, y, country: country.current, color: color.current })
        .catch(() => {});
    }, REPORT_MS);

    const leave = () => {
      void pulse.presence.leave.call({ clientId: me.current }).catch(() => {});
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pagehide", leave);

    return () => {
      alive = false;
      unsub();
      clearInterval(timer);
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pagehide", leave);
      leave();
    };
  }, []);

  return (
    <div aria-hidden style={{ position: "fixed", inset: 0, pointerEvents: "none", zIndex: 9999 }}>
      {cursors
        .filter((c) => c.clientId !== me.current)
        .map((c) => (
          <div
            key={c.clientId}
            style={{
              position: "fixed",
              left: 0,
              top: 0,
              transform: `translate(${c.x * 100}vw, ${c.y * 100}vh)`,
              transition: "transform 90ms linear",
              willChange: "transform",
            }}
          >
            <svg width="20" height="22" viewBox="0 0 20 22" fill="none" style={{ display: "block" }}>
              <path
                d="M1 1l6.5 18 2.7-7.3 7.3-2.7L1 1z"
                fill={c.color}
                stroke="rgba(0,0,0,0.4)"
                strokeWidth="1"
              />
            </svg>
            <span
              style={{
                position: "absolute",
                left: 16,
                top: 16,
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "2px 7px",
                borderRadius: 999,
                background: c.color,
                color: "#000",
                fontSize: 12,
                fontWeight: 600,
                whiteSpace: "nowrap",
                fontFamily: "ui-sans-serif, system-ui, sans-serif",
              }}
            >
              {flag(c.country)} {c.country || "??"}
            </span>
          </div>
        ))}
    </div>
  );
}
