import { useEffect, useRef, useState } from "react";
import { getStroke } from "perfect-freehand";
import type { Doc } from "@onveloz/pulse-schema";
import { pulse } from "./client.js";

type Cursor = Doc<"cursors">;

const REPORT_MS = 45; // ~22 Hz — fast enough for smooth trails, still throttled
const SEL_REPORT_MS = 120; // selection changes are bursty; coalesce them
const TRAIL_FADE_MS = 1500; // a drawn trail point fully fades after this
const TRAIL_MAX = 80; // hard cap on points kept per cursor
const TRAIL_SIZE = 14; // stroke thickness (px) at full pressure

// tldraw's smoothing is perfect-freehand: tapered, streamlined outline.
const STROKE_OPTS = {
  size: TRAIL_SIZE,
  thinning: 0.6, // taper with speed
  smoothing: 0.6, // round off corners
  streamline: 0.5, // pull the line toward a smooth path through the points
  easing: (t: number) => t,
  last: false,
};

type Pt = { x: number; y: number; t: number };

/** perfect-freehand outline points → a smooth filled SVG path (quadratic midpoints). */
function strokePath(stroke: number[][]): string {
  if (stroke.length === 0) return "";
  const first = stroke[0]!;
  const d: (string | number)[] = ["M", first[0]!, first[1]!, "Q"];
  for (let i = 0; i < stroke.length; i++) {
    const a = stroke[i]!;
    const b = stroke[(i + 1) % stroke.length]!;
    d.push(a[0]!, a[1]!, (a[0]! + b[0]!) / 2, (a[1]! + b[1]!) / 2);
  }
  d.push("Z");
  return d.join(" ");
}

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

// ── shared text selection ──────────────────────────────────────────────────
// Selections travel as absolute character offsets into the page's text. The DOM
// is identical for every visitor, so offsets map 1:1; each viewer rebuilds the
// highlight rects against its OWN layout (handles different window sizes).
const selRoot = (): Node => document.body;

/** Absolute text offset of a (node, offset) boundary within root. */
function absOffset(root: Node, node: Node, nodeOffset: number): number {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  let count = 0;
  let n: Node | null;
  while ((n = walker.nextNode())) {
    if (node.nodeType === Node.TEXT_NODE) {
      if (n === node) return count + nodeOffset;
    } else if (node.contains(n)) {
      // Element boundary: count whole text nodes that precede the child boundary.
      const before = Array.from(node.childNodes).slice(0, nodeOffset);
      if (before.some((c) => c === n || c.contains(n))) return count;
    }
    count += n.textContent?.length ?? 0;
  }
  return count;
}

/** Reverse: map an absolute offset to a concrete (text node, offset). */
function locate(root: Node, target: number): { node: Text; offset: number } | null {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  let count = 0;
  let n: Node | null;
  while ((n = walker.nextNode())) {
    const len = n.textContent?.length ?? 0;
    if (count + len >= target) return { node: n as Text, offset: target - count };
    count += len;
  }
  return null;
}

/** This tab's current non-empty selection as offsets, or null. */
function localSelection(): { start: number; end: number } | null {
  const sel = window.getSelection();
  if (!sel || sel.rangeCount === 0 || sel.isCollapsed) return null;
  const r = sel.getRangeAt(0);
  const root = selRoot();
  const a = absOffset(root, r.startContainer, r.startOffset);
  const b = absOffset(root, r.endContainer, r.endOffset);
  const start = Math.min(a, b);
  const end = Math.max(a, b);
  return end > start ? { start, end } : null;
}

/** Viewport rects covering an offset range, for drawing a remote highlight. */
function rectsForRange(start: number, end: number): DOMRect[] {
  const root = selRoot();
  const a = locate(root, start);
  const b = locate(root, end);
  if (!a || !b) return [];
  const range = document.createRange();
  try {
    range.setStart(a.node, a.offset);
    range.setEnd(b.node, b.offset);
  } catch {
    return [];
  }
  return Array.from(range.getClientRects());
}

export function CursorPresence() {
  const [cursors, setCursors] = useState<Cursor[]>([]);
  const [, setFrame] = useState(0); // bumped each animation frame to drive fades
  const [size, setSize] = useState({ w: window.innerWidth, h: window.innerHeight });
  const me = useRef(clientId());
  const color = useRef(colorFor(me.current));
  const country = useRef("");
  const trails = useRef(new Map<string, Pt[]>());
  const ownPos = useRef<{ x: number; y: number } | null>(null);
  // Touch devices have no hover pointer, so we draw the visitor's own cursor.
  const coarse = useRef(window.matchMedia?.("(pointer: coarse)").matches ?? false);

  useEffect(() => {
    let alive = true;
    void lookupCountry().then((c) => {
      if (alive) country.current = c;
    });

    // 1. Seed current state immediately via a one-shot read, so the first paint
    //    is correct even if the subscription's initial snapshot is delayed.
    void pulse.presence.list
      .call()
      .then((rows) => {
        if (alive) applyRows(rows as Cursor[]);
      })
      .catch(() => {});

    // 2. Live updates.
    const unsub = pulse.presence.list.subscribe({}, (rows) => applyRows(rows as Cursor[]));

    function applyRows(rows: Cursor[]) {
      setCursors(rows);
      // Feed OTHER cursors' positions into their trails (own trail is local).
      const now = Date.now();
      for (const c of rows) {
        if (c.clientId === me.current) continue;
        pushTrail(c.clientId, c.x, c.y, now);
      }
    }

    function pushTrail(id: string, x: number, y: number, t: number) {
      const arr = trails.current.get(id) ?? [];
      const last = arr[arr.length - 1];
      if (last && last.x === x && last.y === y) return; // skip duplicates
      arr.push({ x, y, t });
      if (arr.length > TRAIL_MAX) arr.splice(0, arr.length - TRAIL_MAX);
      trails.current.set(id, arr);
    }

    // 3. Pointer → throttled move + instant local trail.
    let pending: { x: number; y: number } | null = null;
    const onMove = (e: PointerEvent) => {
      const x = e.clientX / window.innerWidth;
      const y = e.clientY / window.innerHeight;
      pending = { x, y };
      ownPos.current = { x, y };
      pushTrail(me.current, x, y, Date.now()); // own trail is immediate
    };
    const moveTimer = setInterval(() => {
      if (!pending) return;
      const { x, y } = pending;
      pending = null;
      void pulse.presence.move
        .call({ clientId: me.current, x, y, country: country.current, color: color.current })
        .catch(() => {});
    }, REPORT_MS);

    // 4. Text selection → throttled broadcast.
    let lastSel = "";
    let selPending = false;
    const flushSel = () => {
      selPending = false;
      const s = localSelection();
      const key = s ? `${s.start}:${s.end}` : "none";
      if (key === lastSel) return;
      lastSel = key;
      void pulse.presence.select
        .call({ clientId: me.current, selStart: s ? s.start : -1, selEnd: s ? s.end : -1 })
        .catch(() => {});
    };
    const onSelect = () => {
      if (selPending) return;
      selPending = true;
      setTimeout(flushSel, SEL_REPORT_MS);
    };
    document.addEventListener("selectionchange", onSelect);

    // 5. Animation loop: prune faded trail points and re-render.
    let raf = 0;
    const loop = () => {
      const cutoff = Date.now() - TRAIL_FADE_MS;
      for (const [id, arr] of trails.current) {
        const kept = arr.filter((p) => p.t > cutoff);
        if (kept.length) trails.current.set(id, kept);
        else trails.current.delete(id);
      }
      setFrame((f) => (f + 1) % 1_000_000);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);

    const onResize = () => setSize({ w: window.innerWidth, h: window.innerHeight });
    window.addEventListener("resize", onResize);

    const leave = () => {
      void pulse.presence.leave.call({ clientId: me.current }).catch(() => {});
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pagehide", leave);

    return () => {
      alive = false;
      unsub();
      clearInterval(moveTimer);
      cancelAnimationFrame(raf);
      document.removeEventListener("selectionchange", onSelect);
      window.removeEventListener("resize", onResize);
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pagehide", leave);
      leave();
    };
  }, []);

  const { w, h } = size;
  const now = Date.now();

  return (
    <div aria-hidden style={{ position: "fixed", inset: 0, pointerEvents: "none", zIndex: 9999 }}>
      {/* Remote text-selection highlights, behind everything. */}
      {cursors
        .filter((c) => c.clientId !== me.current && c.selStart >= 0 && c.selEnd > c.selStart)
        .flatMap((c) =>
          rectsForRange(c.selStart, c.selEnd).map((r, i) => (
            <div
              key={`${c.clientId}-sel-${i}`}
              style={{
                position: "fixed",
                left: r.left,
                top: r.top,
                width: r.width,
                height: r.height,
                background: c.color,
                opacity: 0.28,
                borderRadius: 2,
              }}
            />
          )),
        )}

      {/* Fading trails (own + others). */}
      <svg
        width={w}
        height={h}
        viewBox={`0 0 ${w} ${h}`}
        style={{ position: "fixed", inset: 0 }}
      >
        {[...trails.current.entries()].map(([id, pts]) => {
          if (pts.length < 2) return null;
          const col = id === me.current ? color.current : cursors.find((c) => c.clientId === id)?.color ?? color.current;
          // Smooth, tapered outline through the recent points (in px).
          const stroke = getStroke(
            pts.map((p) => [p.x * w, p.y * h]),
            STROKE_OPTS,
          );
          // Whole trail fades out once the cursor stops (newest point ages out).
          const newest = pts[pts.length - 1]!.t;
          const op = Math.max(0, 1 - (now - newest) / TRAIL_FADE_MS);
          if (op <= 0) return null;
          return <path key={id} d={strokePath(stroke)} fill={col} opacity={op} />;
        })}
      </svg>

      {/* Other visitors' cursors (own is the native pointer). */}
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

      {/* This visitor's own cursor — only on touch devices (no native pointer). */}
      {coarse.current && ownPos.current && (
        <svg
          width="20"
          height="22"
          viewBox="0 0 20 22"
          fill="none"
          style={{
            position: "fixed",
            left: 0,
            top: 0,
            transform: `translate(${ownPos.current.x * 100}vw, ${ownPos.current.y * 100}vh)`,
            display: "block",
          }}
        >
          <path d="M1 1l6.5 18 2.7-7.3 7.3-2.7L1 1z" fill={color.current} stroke="rgba(0,0,0,0.4)" strokeWidth="1" />
        </svg>
      )}

      {/* Live presence count. */}
      <div
        style={{
          position: "fixed",
          left: 16,
          bottom: 14,
          padding: "5px 11px",
          borderRadius: 999,
          background: "rgba(255,255,255,0.06)",
          border: "1px solid rgba(255,255,255,0.12)",
          color: "rgba(255,255,255,0.75)",
          fontSize: 12,
          fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
          display: "flex",
          alignItems: "center",
          gap: 7,
        }}
      >
        <span style={{ width: 7, height: 7, borderRadius: 999, background: "#3ddc84", display: "inline-block" }} />
        {cursors.filter((c) => c.clientId !== me.current).length + 1} here
      </div>
    </div>
  );
}
