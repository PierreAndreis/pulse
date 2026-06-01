import { oc } from "@pulse/contract";
import { v } from "@pulse/schema";

export const contract = {
  messages: {
    list: oc
      .reactive()
      .input(v.object({ channelId: v.id("channels") }))
      .output(v.array(v.doc("messages"))),

    send: oc
      .mutation()
      .input(v.object({ channelId: v.id("channels"), body: v.string() }))
      .output(v.doc("messages"))
      .errors({ RATE_LIMITED: { data: v.object({ retryAfter: v.number() }) } }),

    summarize: oc
      .analytical()
      .input(v.object({ channelId: v.id("channels") }))
      .output(v.object({ summary: v.string() })),

    stats: oc
      .analytical()
      .input(v.object({ channelId: v.id("channels") }))
      .output(v.object({ total: v.number(), distinctAuthors: v.number() })),

    // ── M3 precision test surface ──────────────────────────────────────────
    // A point-lookup reactive query (read-set = one key).
    get: oc
      .reactive()
      .input(v.object({ id: v.id("messages") }))
      .output(v.union(v.doc("messages"), v.null())),

    // Patch a message body in place (same channel).
    edit: oc
      .mutation()
      .input(v.object({ id: v.id("messages"), body: v.string() }))
      .output(v.null()),

    // Move a message to another channel (changes a filtered column).
    move: oc
      .mutation()
      .input(v.object({ id: v.id("messages"), channelId: v.id("channels") }))
      .output(v.null()),

    // Delete a message.
    remove: oc.mutation().input(v.object({ id: v.id("messages") })).output(v.null()),

    // Range query: messages in a channel created at/after `since`.
    listSince: oc
      .reactive()
      .input(v.object({ channelId: v.id("channels"), since: v.number() }))
      .output(v.array(v.doc("messages"))),

    // Full-scan query (no index predicate) → coarse table-level read-set.
    listAll: oc.reactive().output(v.array(v.doc("messages"))),

    // Raw-SQL-backed reactive query → coarse table-level read-set.
    countRaw: oc
      .reactive()
      .input(v.object({ channelId: v.id("channels") }))
      .output(v.object({ count: v.number() })),

    // ── M4 transaction test surface ────────────────────────────────────────
    // Inserts a message, then throws — must roll back (atomic mutation).
    sendThenFail: oc
      .mutation()
      .input(v.object({ channelId: v.id("channels"), body: v.string() }))
      .output(v.null()),

    // ── Load-test surface ──────────────────────────────────────────────────
    // A deliberately slow query (pg_sleep) to probe head-of-line blocking:
    // a fast reactive read must not wait behind this.
    slow: oc
      .reactive()
      .input(v.object({ seconds: v.number() }))
      .output(v.object({ slept: v.number() })),

    // Analytical counterpart of `slow` — routed to the OLAP pool (longer timeout)
    // to prove a heavy analytical query isn't bound by the OLTP statement timeout.
    slowAnalytical: oc
      .analytical()
      .input(v.object({ seconds: v.number() }))
      .output(v.object({ slept: v.number() })),
  },

  // ── M4 serializable read-modify-write surface ──────────────────────────────
  counters: {
    create: oc.mutation().input(v.object({ name: v.string() })).output(v.id("counters")),
    get: oc.reactive().input(v.object({ id: v.id("counters") })).output(v.number()),
    // Reads the current value then writes value+1 — a classic lost-update race
    // that SERIALIZABLE + retry must make safe.
    increment: oc.mutation().input(v.object({ id: v.id("counters") })).output(v.null()),
  },

  // ── S2 money-transfer surface (multi-row deadlock + conservation) ──────────
  accounts: {
    create: oc
      .mutation()
      .input(v.object({ name: v.string(), balance: v.number() }))
      .output(v.id("accounts")),
    get: oc
      .reactive()
      .input(v.object({ id: v.id("accounts") }))
      .output(v.object({ id: v.id("accounts"), name: v.string(), balance: v.number() })),
    list: oc.reactive().output(v.array(v.doc("accounts"))),
    // Debit `from`, credit `to` in ONE mutation (one BEGIN…COMMIT). Rejects an
    // overdraw with a handler error (rolled back). Does NOT canonical-sort the
    // two rows — opposing acquisition order is what provokes the 40P01 deadlock
    // the S2 test relies on; a "fix" that sorts them would neuter that test.
    transfer: oc
      .mutation()
      .input(v.object({ from: v.id("accounts"), to: v.id("accounts"), amount: v.number() }))
      .output(v.null())
      .errors({ INSUFFICIENT_FUNDS: { data: v.object({ balance: v.number() }) } }),
  },

  transfers: {
    // Raw count of ledger rows, to reconcile against committed transfer calls.
    countRaw: oc.reactive().output(v.object({ count: v.number() })),
  },

  // ── M6 action surface ──────────────────────────────────────────────────────
  actions: {
    // A side-effecting action that re-enters the engine: runs a mutation, then a
    // query, and reports what it observed (the action→mutation→query pattern).
    sendViaAction: oc
      .action()
      .input(v.object({ channelId: v.id("channels"), body: v.string() }))
      .output(v.object({ listed: v.number() })),
  },

  // ── Collaborative documents (Yjs CRDT) ──────────────────────────────────────
  notes: {
    create: oc.mutation().input(v.object({ title: v.string() })).output(v.id("notes")),
    // Current Yjs document state (base64). The client seeds its Y.Doc from this.
    getDoc: oc.reactive().input(v.object({ id: v.id("notes") })).output(v.object({ state: v.string() })),
    // Merge a base64 Yjs update; returns the merged state. Reactive subscribers
    // to getDoc are invalidated → they refetch and merge.
    applyUpdate: oc
      .mutation()
      .input(v.object({ id: v.id("notes"), update: v.string() }))
      .output(v.object({ state: v.string() })),
  },
};
