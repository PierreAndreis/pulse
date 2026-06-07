import { useEffect, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { getStroke } from "perfect-freehand";
import type { Doc } from "@onveloz/pulse-schema";
import { pulse } from "./client.js";

type Cursor = Doc<"cursors">;

const REPORT_MS = 45; // ~22 Hz — fast enough for smooth trails, still throttled
const HEARTBEAT_MS = 4000; // republish even when idle so we stay present
const SEL_REPORT_MS = 120; // selection changes are bursty; coalesce them
const TRAIL_FADE_MS = 1500; // a drawn trail point fully fades after this
const TRAIL_MAX = 80; // hard cap on points kept per cursor
const TRAIL_SIZE = 14; // stroke thickness (px) at full pressure
const FOLLOW_KEY = "pulse:follow"; // survives a follow-driven page navigation
const JUMP_KEY = "pulse:jump"; // one-shot scroll target across a jump navigation
const FOLLOW_EASE = 0.18; // per-frame lerp toward the followed scroll position

const STROKE_OPTS = {
  size: TRAIL_SIZE,
  thinning: 0.6,
  smoothing: 0.6,
  streamline: 0.5,
  easing: (t: number) => t,
  last: false,
};

type Pt = { x: number; y: number; t: number };

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

function hash(id: string): number {
  let h = 0;
  for (const ch of id) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  return h;
}

/** Deterministic, readable color from the id. */
function colorFor(id: string): string {
  return `hsl(${hash(id) % 360} 85% 62%)`;
}

const ADJ = [
  "Swift", "Calm", "Bold", "Bright", "Brave", "Clever", "Cosmic", "Fuzzy", "Gentle", "Jolly",
  "Lucky", "Mellow", "Nimble", "Quiet", "Rapid", "Sly", "Sunny", "Witty", "Zen", "Electric",
];
const ANIMAL = [
  "Otter", "Falcon", "Panda", "Fox", "Lynx", "Heron", "Koala", "Wolf", "Tapir", "Gecko",
  "Moth", "Crane", "Bison", "Orca", "Raven", "Newt", "Quail", "Yak", "Ibis", "Seal",
];

const EMOJIS = ["🎉", "❤️", "🔥", "👍", "😮"]; // number keys 1–5
const EMOTE_MS = 1700; // how long a floating reaction lives

/** A friendly, deterministic name from the id (no extra data to broadcast). */
function nameFor(id: string): string {
  const h = hash(id);
  return `${ADJ[h % ADJ.length]} ${ANIMAL[(h >> 5) % ANIMAL.length]}`;
}

/** ISO-3166 alpha-2 → flag emoji (regional indicators). "" → globe. */
function flag(cc: string): string {
  if (!/^[A-Za-z]{2}$/.test(cc)) return "🌐";
  return String.fromCodePoint(...[...cc.toUpperCase()].map((c) => 0x1f1e6 + c.charCodeAt(0) - 65));
}

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

const scrollMax = () => Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
const scrollFrac = () => (scrollMax() > 0 ? window.scrollY / scrollMax() : 0);

// ── shared text selection (layout-independent character offsets) ────────────
const selRoot = (): Node => document.body;

function absOffset(root: Node, node: Node, nodeOffset: number): number {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  let count = 0;
  let n: Node | null;
  while ((n = walker.nextNode())) {
    if (node.nodeType === Node.TEXT_NODE) {
      if (n === node) return count + nodeOffset;
    } else if (node.contains(n)) {
      const before = Array.from(node.childNodes).slice(0, nodeOffset);
      if (before.some((c) => c === n || c.contains(n))) return count;
    }
    count += n.textContent?.length ?? 0;
  }
  return count;
}

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
  const [, setFrame] = useState(0);
  const [size, setSize] = useState({ w: window.innerWidth, h: window.innerHeight });
  const [followId, setFollowId] = useState<string | null>(() => sessionStorage.getItem(FOLLOW_KEY));
  const [chatting, setChatting] = useState(false);
  const [chatText, setChatText] = useState("");
  const [ownMessage, setOwnMessage] = useState("");
  const chattingRef = useRef(false);
  chattingRef.current = chatting;
  const chatInput = useRef<HTMLInputElement>(null);
  const emotes = useRef<{ key: number; x: number; y: number; emoji: string; color: string; t: number }[]>([]);
  const emoteSeq = useRef(0);
  const prevEmoteAt = useRef(new Map<string, number>());
  const me = useRef(clientId());
  const color = useRef(colorFor(me.current));
  const country = useRef("");
  const channel = useRef(window.location.pathname).current;
  const trails = useRef(new Map<string, Pt[]>());
  const ownPos = useRef<{ x: number; y: number } | null>(null);
  const coarse = useRef(window.matchMedia?.("(pointer: coarse)").matches ?? false);
  const followIdRef = useRef(followId);
  followIdRef.current = followId;
  const followScroll = useRef(0); // followed visitor's scroll fraction
  const followActive = useRef(false); // following AND on the same page
  const cursorsRef = useRef<Cursor[]>([]);
  cursorsRef.current = cursors;

  function stopFollow() {
    sessionStorage.removeItem(FOLLOW_KEY);
    followActive.current = false;
    setFollowId(null);
  }

  // One-shot jump to a visitor: navigate to their page if needed, else smooth-scroll.
  function jumpTo(id: string) {
    const t = cursorsRef.current.find((c) => c.clientId === id);
    if (!t) return;
    if (t.channel && t.channel !== channel) {
      sessionStorage.setItem(JUMP_KEY, String(t.scrollY));
      window.location.href = t.channel;
      return;
    }
    window.scrollTo({ top: t.scrollY * scrollMax(), behavior: "smooth" });
  }

  function spawnEmote(x: number, y: number, emoji: string, col: string) {
    emotes.current.push({ key: emoteSeq.current++, x, y, emoji, color: col, t: Date.now() });
  }
  function sendReact(emoji: string) {
    const p = ownPos.current ?? { x: 0.5, y: 0.5 };
    spawnEmote(p.x, p.y, emoji, color.current); // instant local feedback
    void pulse.presence.react.call({ clientId: me.current, emote: emoji }).catch(() => {});
  }

  // Own selection uses the visitor's presence color; also defines the follow-border pulse.
  useEffect(() => {
    const style = document.createElement("style");
    const tint = color.current.replace(")", " / 0.4)");
    style.textContent =
      `::selection{background:${tint};} ::-moz-selection{background:${tint};}` +
      `@keyframes pulseFollowBorder{0%,100%{opacity:.45}50%{opacity:.9}}` +
      `@keyframes emoteFloat{0%{transform:translateY(0) scale(.6);opacity:0}15%{opacity:1;transform:translateY(-6px) scale(1.2)}100%{transform:translateY(-70px) scale(1);opacity:0}}`;
    document.head.appendChild(style);
    return () => style.remove();
  }, []);

  useEffect(() => {
    let alive = true;
    void lookupCountry().then((c) => {
      if (alive) country.current = c;
    });

    // Consume a one-shot jump target left by a cross-page jump.
    const jumpRaw = sessionStorage.getItem(JUMP_KEY);
    if (jumpRaw) {
      sessionStorage.removeItem(JUMP_KEY);
      const frac = Number(jumpRaw);
      if (!Number.isNaN(frac)) {
        setTimeout(() => window.scrollTo({ top: frac * scrollMax(), behavior: "smooth" }), 350);
      }
    }

    const pos = { x: 0.5, y: 0.5 };
    let dirty = false;
    let lastPublish = 0;
    const publish = () => {
      dirty = false;
      lastPublish = Date.now();
      void pulse.presence.move
        .call({
          clientId: me.current,
          x: pos.x,
          y: pos.y,
          country: country.current,
          color: color.current,
          channel,
          scrollY: scrollFrac(),
        })
        .catch(() => {});
    };

    void pulse.presence.list
      .call()
      .then((rows) => alive && applyRows(rows as Cursor[]))
      .catch(() => {});
    const unsub = pulse.presence.list.subscribe({}, (rows) => applyRows(rows as Cursor[]));

    function applyRows(rows: Cursor[]) {
      setCursors(rows);
      const now = Date.now();
      for (const c of rows) {
        if (c.clientId === me.current || c.channel !== channel) continue;
        pushTrail(c.clientId, c.x, c.y, now);
        // Animate a remote reaction the first time we see its timestamp.
        if (c.emote && c.emoteAt > (prevEmoteAt.current.get(c.clientId) ?? 0)) {
          prevEmoteAt.current.set(c.clientId, c.emoteAt);
          spawnEmote(c.x, c.y, c.emote, c.color);
        }
      }
    }
    function pushTrail(id: string, x: number, y: number, t: number) {
      const arr = trails.current.get(id) ?? [];
      const last = arr[arr.length - 1];
      if (last && last.x === x && last.y === y) return;
      arr.push({ x, y, t });
      if (arr.length > TRAIL_MAX) arr.splice(0, arr.length - TRAIL_MAX);
      trails.current.set(id, arr);
    }

    const onMove = (e: PointerEvent) => {
      pos.x = e.clientX / window.innerWidth;
      pos.y = e.clientY / window.innerHeight;
      ownPos.current = { x: pos.x, y: pos.y };
      dirty = true;
      pushTrail(me.current, pos.x, pos.y, Date.now());
    };
    const onScroll = () => {
      dirty = true; // broadcast my (possibly follow-driven) scroll position
    };
    // Any deliberate scroll input hands control back to the viewer.
    const userTookOver = () => {
      if (followIdRef.current) stopFollow();
    };
    const onKey = (e: KeyboardEvent) => {
      if (chattingRef.current) return; // the chat input owns its keys
      const tag = (e.target as HTMLElement | null)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA") return;
      if (e.key === "/") {
        e.preventDefault();
        setChatting(true);
        return;
      }
      if (/^[1-5]$/.test(e.key)) {
        sendReact(EMOJIS[Number(e.key) - 1]!);
        return;
      }
      if (e.key === "Escape" && followIdRef.current) stopFollow();
      else if (
        followIdRef.current &&
        ["ArrowUp", "ArrowDown", "PageUp", "PageDown", "Home", "End", " "].includes(e.key)
      )
        stopFollow();
    };

    const publishTimer = setInterval(() => {
      if (dirty || Date.now() - lastPublish > HEARTBEAT_MS) publish();
    }, REPORT_MS);

    // Selection → throttled broadcast.
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

    // Animation loop: eased follow-scroll, trail pruning, re-render.
    let raf = 0;
    const loop = () => {
      if (followActive.current) {
        const target = followScroll.current * scrollMax();
        const cur = window.scrollY;
        if (Math.abs(target - cur) > 0.5) window.scrollTo(0, cur + (target - cur) * FOLLOW_EASE);
      }
      const tnow = Date.now();
      const cutoff = tnow - TRAIL_FADE_MS;
      for (const [id, arr] of trails.current) {
        const kept = arr.filter((p) => p.t > cutoff);
        if (kept.length) trails.current.set(id, kept);
        else trails.current.delete(id);
      }
      if (emotes.current.some((em) => tnow - em.t > EMOTE_MS))
        emotes.current = emotes.current.filter((em) => tnow - em.t <= EMOTE_MS);
      setFrame((f) => (f + 1) % 1_000_000);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);

    const onResize = () => setSize({ w: window.innerWidth, h: window.innerHeight });
    const leave = () => {
      void pulse.presence.leave.call({ clientId: me.current }).catch(() => {});
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("wheel", userTookOver, { passive: true });
    window.addEventListener("touchstart", userTookOver, { passive: true });
    window.addEventListener("resize", onResize);
    window.addEventListener("keydown", onKey);
    window.addEventListener("pagehide", leave);

    return () => {
      alive = false;
      unsub();
      clearInterval(publishTimer);
      cancelAnimationFrame(raf);
      document.removeEventListener("selectionchange", onSelect);
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("wheel", userTookOver);
      window.removeEventListener("touchstart", userTookOver);
      window.removeEventListener("resize", onResize);
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("pagehide", leave);
      leave();
    };
  }, [channel]);

  // Follow: drive the eased scroll target, and navigate to the leader's page.
  useEffect(() => {
    if (!followId) {
      followActive.current = false;
      return;
    }
    const t = cursors.find((c) => c.clientId === followId);
    if (!t) return;
    if (t.channel && t.channel !== channel) {
      sessionStorage.setItem(FOLLOW_KEY, followId);
      window.location.href = t.channel;
      return;
    }
    followScroll.current = t.scrollY;
    followActive.current = true;
  }, [cursors, followId, channel]);

  // Focus the chat field when chat opens.
  useEffect(() => {
    if (chatting) chatInput.current?.focus();
  }, [chatting]);

  function onChatChange(v: string) {
    setChatText(v);
    void pulse.presence.say.call({ clientId: me.current, message: v }).catch(() => {});
  }
  function commitChat() {
    const msg = chatText.trim();
    setChatting(false);
    setChatText("");
    setOwnMessage(msg); // show my own bubble too
    if (msg) {
      // Leave the bubble up briefly for everyone, then clear it.
      setTimeout(() => {
        setOwnMessage("");
        void pulse.presence.say.call({ clientId: me.current, message: "" }).catch(() => {});
      }, 5000);
    } else {
      void pulse.presence.say.call({ clientId: me.current, message: "" }).catch(() => {});
    }
  }
  function cancelChat() {
    setChatting(false);
    setChatText("");
    setOwnMessage("");
    void pulse.presence.say.call({ clientId: me.current, message: "" }).catch(() => {});
  }

  function toggleFollow(id: string) {
    if (followId === id) {
      stopFollow();
    } else {
      sessionStorage.setItem(FOLLOW_KEY, id);
      setFollowId(id);
    }
  }

  const { w, h } = size;
  const now = Date.now();
  const here = cursors.filter((c) => c.clientId !== me.current && c.channel === channel);
  const followed = followId ? cursors.find((c) => c.clientId === followId) : undefined;

  return (
    <div aria-hidden style={{ position: "fixed", inset: 0, pointerEvents: "none", zIndex: 9999 }}>
      {/* Pulsing border while following. */}
      {followed && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            border: `3px solid ${followed.color}`,
            boxSizing: "border-box",
            pointerEvents: "none",
            animation: "pulseFollowBorder 1.6s ease-in-out infinite",
          }}
        />
      )}

      {/* Remote text-selection highlights (this channel only). */}
      {here
        .filter((c) => c.selStart >= 0 && c.selEnd > c.selStart)
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

      {/* Smooth fading trails (own + others on this channel). */}
      <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} style={{ position: "fixed", inset: 0 }}>
        {[...trails.current.entries()].map(([id, pts]) => {
          if (pts.length < 2) return null;
          if (id !== me.current && !here.some((c) => c.clientId === id)) return null;
          const col = id === me.current ? color.current : here.find((c) => c.clientId === id)?.color ?? color.current;
          const stroke = getStroke(
            pts.map((p) => [p.x * w, p.y * h]),
            STROKE_OPTS,
          );
          const newest = pts[pts.length - 1]!.t;
          const op = Math.max(0, 1 - (now - newest) / TRAIL_FADE_MS);
          if (op <= 0) return null;
          return <path key={id} d={strokePath(stroke)} fill={col} opacity={op} />;
        })}
      </svg>

      {/* Other visitors' cursors (this channel). Click to jump to them. */}
      {here.map((c) => (
        <div
          key={c.clientId}
          onClick={() => jumpTo(c.clientId)}
          title={`Jump to ${nameFor(c.clientId)}`}
          style={{
            position: "fixed",
            left: 0,
            top: 0,
            transform: `translate(${c.x * 100}vw, ${c.y * 100}vh)`,
            transition: "transform 90ms linear",
            willChange: "transform",
            pointerEvents: "auto",
            cursor: "pointer",
          }}
        >
          <svg width="20" height="22" viewBox="0 0 20 22" fill="none" style={{ display: "block" }}>
            <path d="M1 1l6.5 18 2.7-7.3 7.3-2.7L1 1z" fill={c.color} stroke="rgba(0,0,0,0.4)" strokeWidth="1" />
          </svg>
          {c.message && (
            <span
              style={{
                position: "absolute",
                left: 16,
                top: -10,
                maxWidth: 240,
                padding: "5px 10px",
                borderRadius: 12,
                borderBottomLeftRadius: 3,
                background: c.color,
                color: "#000",
                fontSize: 13,
                fontWeight: 500,
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontFamily: "ui-sans-serif, system-ui, sans-serif",
                boxShadow: "0 3px 12px rgba(0,0,0,0.35)",
              }}
            >
              {c.message}
            </span>
          )}
          <span
            style={{
              position: "absolute",
              left: 16,
              top: 16,
              display: "inline-flex",
              alignItems: "center",
              gap: 5,
              padding: "2px 8px",
              borderRadius: 999,
              background: c.color,
              color: "#000",
              fontSize: 12,
              fontWeight: 600,
              whiteSpace: "nowrap",
              fontFamily: "ui-sans-serif, system-ui, sans-serif",
              boxShadow: "0 2px 8px rgba(0,0,0,0.3)",
            }}
          >
            {nameFor(c.clientId)} {flag(c.country)}
          </span>
        </div>
      ))}

      {/* Own cursor — only on touch devices (no native pointer). */}
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

      {/* Following banner. */}
      {followed && (
        <div
          style={{
            position: "fixed",
            top: 14,
            left: "50%",
            transform: "translateX(-50%)",
            pointerEvents: "auto",
            display: "flex",
            alignItems: "center",
            gap: 10,
            padding: "7px 14px",
            borderRadius: 999,
            background: followed.color,
            color: "#000",
            fontSize: 13,
            fontWeight: 600,
            fontFamily: "ui-sans-serif, system-ui, sans-serif",
            boxShadow: "0 4px 20px rgba(0,0,0,0.35)",
          }}
        >
          Following {nameFor(followed.clientId)} {flag(followed.country)}
          <button
            onClick={stopFollow}
            style={{
              pointerEvents: "auto",
              border: "none",
              borderRadius: 999,
              background: "rgba(0,0,0,0.25)",
              color: "#fff",
              fontSize: 11,
              fontWeight: 600,
              padding: "2px 8px",
              cursor: "pointer",
            }}
          >
            Stop · Esc
          </button>
        </div>
      )}

      {/* Floating emoji reactions. */}
      {emotes.current.map((em) => (
        <div
          key={em.key}
          style={{ position: "fixed", left: 0, top: 0, transform: `translate(${em.x * 100}vw, ${em.y * 100}vh)`, pointerEvents: "none" }}
        >
          <span
            style={{
              position: "absolute",
              left: -4,
              top: -8,
              fontSize: 26,
              animation: `emoteFloat ${EMOTE_MS}ms ease-out forwards`,
              filter: `drop-shadow(0 2px 8px ${em.color})`,
            }}
          >
            {em.emoji}
          </span>
        </div>
      ))}

      {/* Your own message bubble, so you see what you said too. */}
      {ownMessage && !chatting && (
        <div
          style={{
            position: "fixed",
            left: 0,
            top: 0,
            transform: `translate(${(ownPos.current?.x ?? 0.5) * 100}vw, ${(ownPos.current?.y ?? 0.5) * 100}vh)`,
            pointerEvents: "none",
          }}
        >
          <span
            style={{
              position: "absolute",
              left: 16,
              top: -10,
              maxWidth: 240,
              padding: "5px 10px",
              borderRadius: 12,
              borderBottomLeftRadius: 3,
              background: color.current,
              color: "#000",
              fontSize: 13,
              fontWeight: 500,
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontFamily: "ui-sans-serif, system-ui, sans-serif",
              boxShadow: "0 3px 12px rgba(0,0,0,0.35)",
            }}
          >
            {ownMessage}
          </span>
        </div>
      )}

      {/* Local cursor-chat input (press "/"). */}
      {chatting && (
        <div
          style={{
            position: "fixed",
            left: 0,
            top: 0,
            transform: `translate(${(ownPos.current?.x ?? 0.5) * 100}vw, ${(ownPos.current?.y ?? 0.5) * 100}vh)`,
            pointerEvents: "auto",
          }}
        >
          <input
            ref={chatInput}
            value={chatText}
            maxLength={140}
            placeholder="Say something…  ↵"
            onChange={(e) => onChatChange(e.target.value)}
            onBlur={commitChat}
            onKeyDown={(e) => {
              e.stopPropagation();
              if (e.key === "Enter") {
                e.preventDefault();
                commitChat();
              } else if (e.key === "Escape") {
                e.preventDefault();
                cancelChat();
              }
            }}
            style={{
              position: "absolute",
              left: 16,
              top: -10,
              width: 220,
              padding: "6px 11px",
              borderRadius: 12,
              borderBottomLeftRadius: 3,
              border: `2px solid ${color.current}`,
              background: "#111",
              color: "#fff",
              fontSize: 13,
              outline: "none",
              fontFamily: "ui-sans-serif, system-ui, sans-serif",
            }}
          />
        </div>
      )}

      {/* Presence panel: who's here — jump (↗) or follow. */}
      <div
        style={{
          position: "fixed",
          left: 16,
          bottom: 14,
          pointerEvents: "auto",
          minWidth: 160,
          padding: "8px 10px",
          borderRadius: 12,
          background: "rgba(20,20,22,0.85)",
          border: "1px solid rgba(255,255,255,0.12)",
          backdropFilter: "blur(8px)",
          color: "rgba(255,255,255,0.85)",
          fontSize: 12,
          fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: here.length ? 6 : 0 }}>
          <span style={{ width: 7, height: 7, borderRadius: 999, background: "#3ddc84", display: "inline-block" }} />
          {here.length + 1} here
        </div>
        {here.map((c) => {
          const isFollowing = followId === c.clientId;
          return (
            <div
              key={c.clientId}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                padding: "3px 4px",
                margin: "1px 0",
                borderRadius: 7,
                background: isFollowing ? c.color : "transparent",
                color: isFollowing ? "#000" : "rgba(255,255,255,0.85)",
              }}
            >
              <span style={{ width: 8, height: 8, borderRadius: 999, background: c.color, display: "inline-block", flex: "0 0 auto" }} />
              <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {nameFor(c.clientId)} {flag(c.country)}
              </span>
              <button
                onClick={() => jumpTo(c.clientId)}
                title="Jump to"
                style={{ marginLeft: "auto", ...iconBtn(isFollowing) }}
              >
                ↗
              </button>
              <button
                onClick={() => toggleFollow(c.clientId)}
                title={isFollowing ? "Stop following" : "Follow"}
                style={iconBtn(isFollowing)}
              >
                {isFollowing ? "■" : "⦿"}
              </button>
            </div>
          );
        })}
        <div style={{ marginTop: 6, paddingTop: 6, borderTop: "1px solid rgba(255,255,255,0.1)", opacity: 0.6, fontSize: 11 }}>
          / chat · 1–5 react
        </div>
      </div>
    </div>
  );
}

function iconBtn(active: boolean): CSSProperties {
  return {
    pointerEvents: "auto",
    cursor: "pointer",
    border: "none",
    borderRadius: 6,
    width: 20,
    height: 20,
    fontSize: 12,
    lineHeight: "20px",
    padding: 0,
    background: active ? "rgba(0,0,0,0.2)" : "rgba(255,255,255,0.08)",
    color: active ? "#000" : "rgba(255,255,255,0.85)",
    fontFamily: "inherit",
  };
}

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
