import { type CSSProperties, useEffect, useRef, useState } from "react";
import type { Doc, Id } from "@onveloz/pulse-schema";
import type { SyncStatus } from "@onveloz/pulse-client";
import { isOffline, onOffline, pulse, setOffline } from "./client.js";

const GENERAL = "channels:00000000-0000-0000-0000-000000000001" as Id<"channels">;
const ME = "users:00000000-0000-0000-0000-000000000001" as Id<"users">;
type Message = Doc<"messages">;

// Optimistic rows carry a temp id minted by the SDK ("...:opt-...") until the
// server confirms them — we use that to render a per-message sync indicator.
const isPending = (m: Message) => String(m._id).includes("opt-");

// This example dogfoods the REAL @onveloz/pulse-client SDK end to end — nothing here
// re-implements fetch, SSE, optimism, IndexedDB, or reconnect:
//   • pulse.messages.list.subscribe(...)  → reactive SSE, auto-reconnecting
//   • pulse.$local.mutate(..., {optimistic}) → instant UI + DURABLE offline queue
//   • pulse.$onSyncStatus / $reconnect     → live connection state + manual retry
//   • read cache persists, so data shows on reload even with the server down
export function ChatApp() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [status, setStatus] = useState<SyncStatus>("connecting");
  const [offline, setOff] = useState(isOffline());
  const [body, setBody] = useState("");
  const [queued, setQueued] = useState(0);
  const listRef = useRef<HTMLDivElement>(null);

  const refreshQueue = () => pulse.$local.queue.size().then(setQueued);

  useEffect(() => {
    const unsub = pulse.messages.list.subscribe({ input: { channelId: GENERAL } }, (data) => {
      setMessages([...(data as Message[])].reverse()); // server newest-first → show oldest-first
    });
    const unsubStatus = pulse.$onSyncStatus(setStatus);
    const unsubOffline = onOffline(setOff);
    const unsubQueue = pulse.$local.onQueueChange(() => void refreshQueue());
    void refreshQueue();
    return () => {
      unsub();
      unsubStatus();
      unsubOffline();
      unsubQueue();
    };
  }, []);

  useEffect(() => {
    listRef.current?.scrollTo({ top: listRef.current.scrollHeight });
  }, [messages]);

  async function send() {
    const text = body.trim();
    if (!text) return;
    setBody("");
    // Local-first write: optimistic row shows instantly (even offline), the
    // mutation is durably queued, and it auto-flushes when the connection is up.
    await pulse.$local.mutate(
      ["messages", "send"],
      { channelId: GENERAL, body: text },
      {
        optimistic: (store) => {
          const input = { channelId: GENERAL };
          const current = store.getQuery<Message>(["messages", "list"], input);
          const optimisticDoc = {
            _id: store.tempId("messages") as Id<"messages">,
            _creationTime: 0, // unknown until confirmed; rendered as "now"
            authorId: ME,
            channelId: GENERAL,
            body: text,
          } satisfies Message;
          store.setQuery(["messages", "list"], input, [optimisticDoc, ...current]);
        },
      },
    );
  }

  // Display status folds the simulated-offline switch into the live SSE state.
  const view: "live" | "offline" | "reconnecting" = offline
    ? "offline"
    : status === "connected"
      ? "live"
      : "reconnecting";
  const dot = { live: "#16a34a", offline: "#ef4444", reconnecting: "#f59e0b" }[view];
  const label = { live: "live", offline: "offline", reconnecting: "reconnecting…" }[view];

  return (
    <div style={styles.page}>
      <style>{KEYFRAMES}</style>
      <div style={styles.card}>
        <header style={styles.header}>
          <div>
            <div style={styles.title}>Pulse</div>
            <div style={styles.subtitle}>local-first reactive chat · #general</div>
          </div>
          <div style={styles.statusWrap}>
            <span
              style={{
                ...styles.dot,
                background: dot,
                animation: view === "live" ? "pulseDot 1.6s ease-in-out infinite" : "none",
              }}
            />
            <span style={{ ...styles.statusText, color: dot }}>{label}</span>
          </div>
        </header>

        <div style={styles.toolbar}>
          <button
            style={{ ...styles.toggle, ...(offline ? styles.toggleOff : styles.toggleOn) }}
            onClick={() => setOffline(!offline)}
          >
            {offline ? "📴 Offline — tap to reconnect" : "📡 Online — tap to go offline"}
          </button>
          <span style={styles.toolHint}>simulates airplane mode</span>
        </div>

        {offline && (
          <div style={styles.banner}>
            {queued > 0
              ? `${queued} change${queued > 1 ? "s" : ""} saved locally — will sync when you reconnect`
              : "Offline — messages you send are saved locally and sync on reconnect"}
          </div>
        )}

        <div ref={listRef} style={styles.list}>
          {messages.length === 0 ? (
            <div style={styles.empty}>No messages yet — say something below.</div>
          ) : (
            messages.map((m) => {
              const pending = isPending(m);
              return (
                <div key={m._id} style={{ ...styles.msg, opacity: pending ? 0.7 : 1 }}>
                  <div style={styles.body}>{m.body}</div>
                  <div style={styles.meta}>
                    <span>{pending ? "now" : new Date(m._creationTime).toLocaleTimeString()}</span>
                    <span style={styles.tick} title={pending ? "sending…" : "delivered"}>
                      {pending ? "⟳ sending" : "✓ sent"}
                    </span>
                  </div>
                </div>
              );
            })
          )}
        </div>

        <div style={styles.composer}>
          <input
            style={styles.input}
            value={body}
            placeholder="Message #general"
            onChange={(e) => setBody(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && send()}
          />
          <button style={styles.button} onClick={send} disabled={!body.trim()}>
            Send
          </button>
        </div>
      </div>

      <p style={styles.hint}>
        Try it: go <b>offline</b>, send a few messages (they appear instantly, marked <i>sending</i>),
        then <b>reload</b> — they're still here (read cache + durable queue). Go back <b>online</b> and
        watch them auto-sync. Open a second tab for live cross-client updates.
      </p>
    </div>
  );
}

const KEYFRAMES = `@keyframes pulseDot { 0%,100% { box-shadow: 0 0 0 0 rgba(22,163,74,0.5);} 50% { box-shadow: 0 0 0 5px rgba(22,163,74,0);} }`;

const styles: Record<string, CSSProperties> = {
  page: { minHeight: "100vh", margin: 0, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 14, background: "linear-gradient(160deg,#eef2ff,#f8fafc 40%,#ecfeff)", fontFamily: "ui-sans-serif, system-ui, -apple-system, sans-serif" },
  card: { width: 460, height: 620, background: "#fff", borderRadius: 18, boxShadow: "0 20px 60px rgba(30,41,59,0.18)", display: "flex", flexDirection: "column", overflow: "hidden", border: "1px solid #eef0f4" },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "16px 20px", borderBottom: "1px solid #eef0f2" },
  title: { fontWeight: 800, fontSize: 19, letterSpacing: -0.3 },
  subtitle: { color: "#6b7280", fontSize: 12 },
  statusWrap: { display: "flex", alignItems: "center", gap: 7 },
  dot: { width: 9, height: 9, borderRadius: 999, display: "inline-block" },
  statusText: { fontSize: 12, fontWeight: 700, textTransform: "lowercase" },
  toolbar: { display: "flex", alignItems: "center", gap: 10, padding: "10px 16px", background: "#f9fafb", borderBottom: "1px solid #eef0f2" },
  toggle: { border: "none", borderRadius: 9, padding: "7px 12px", fontSize: 13, fontWeight: 700, cursor: "pointer", transition: "background 0.15s" },
  toggleOn: { background: "#dcfce7", color: "#166534" },
  toggleOff: { background: "#fee2e2", color: "#991b1b" },
  toolHint: { color: "#9ca3af", fontSize: 12 },
  banner: { padding: "9px 16px", background: "#fef3c7", color: "#92400e", fontSize: 13, fontWeight: 500 },
  list: { flex: 1, overflowY: "auto", padding: 16, display: "flex", flexDirection: "column", gap: 8 },
  empty: { color: "#9ca3af", textAlign: "center", marginTop: 40 },
  msg: { background: "#f8fafc", borderRadius: 12, padding: "9px 12px", border: "1px solid #f1f5f9" },
  body: { fontSize: 14, color: "#111827", wordBreak: "break-word" },
  meta: { display: "flex", justifyContent: "space-between", marginTop: 4, fontSize: 11, color: "#9ca3af" },
  tick: { fontVariant: "tabular-nums" },
  composer: { display: "flex", gap: 8, padding: 16, borderTop: "1px solid #eef0f2" },
  input: { flex: 1, border: "1px solid #d1d5db", borderRadius: 11, padding: "11px 13px", fontSize: 14, outline: "none" },
  button: { background: "#4f46e5", color: "#fff", border: "none", borderRadius: 11, padding: "0 18px", fontSize: 14, fontWeight: 700, cursor: "pointer" },
  hint: { color: "#64748b", fontSize: 12.5, maxWidth: 460, textAlign: "center", lineHeight: 1.5 },
};
