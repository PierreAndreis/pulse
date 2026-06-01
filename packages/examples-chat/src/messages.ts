import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";
import { authedBase } from "./middleware.js";
import "./_generated/dataModel.js";

const os = implement(contract);

async function isRateLimited(_userId: string): Promise<boolean> {
  return false;
}

function summarizeText(rows: { body: string }[]): string {
  return `Summary of ${rows.length} messages`;
}

export const list = os.messages.list.use(authedBase).handler(async ({ ctx, input }) => {
  // ctx.db is instrumented: every read is captured into the read-set.
  return ctx.db
    .query("messages")
    .withIndex("by_channel", (q) => q.eq("channelId", input.channelId))
    .order("desc")
    .take(100);
});

export const send = os.messages.send.use(authedBase).handler(async ({ ctx, input, errors }) => {
  // Demo: the sentinel body "__rate_limit__" trips the limiter, exercising the
  // declared-error (with structured data) path end-to-end.
  if (input.body === "__rate_limit__" || (await isRateLimited(ctx.user.id)))
    throw errors.RATE_LIMITED({ retryAfter: 5 });
  // ctx.db here is read-write, running inside ONE serializable tx.
  const id = await ctx.db.insert("messages", {
    channelId: input.channelId,
    authorId: ctx.user.id,
    body: input.body,
  });
  const doc = await ctx.db.get(id);
  if (!doc) throw errors.INTERNAL();
  return doc;
});

export const summarize = os.messages.summarize.use(authedBase).handler(async ({ ctx, input }) => {
  const rows = await ctx.sql<{ body: string }>`
    SELECT body FROM messages WHERE channel_id = ${input.channelId}::uuid
    ORDER BY _creation_time DESC LIMIT 5000`;
  return { summary: summarizeText(rows) };
});

// ── M3 precision test handlers ───────────────────────────────────────────────

export const get = os.messages.get.use(authedBase).handler(async ({ ctx, input }) => {
  return ctx.db.get(input.id);
});

// M4: inserts a message and then fails. With per-mutation transactions the
// insert must roll back, so the message never appears.
export const sendThenFail = os.messages.sendThenFail
  .use(authedBase)
  .handler(async ({ ctx, input, errors }) => {
    await ctx.db.insert("messages", {
      channelId: input.channelId,
      authorId: ctx.user.id,
      body: input.body,
    });
    throw errors.INTERNAL();
  });

export const edit = os.messages.edit.use(authedBase).handler(async ({ ctx, input }) => {
  await ctx.db.patch(input.id, { body: input.body });
  return null;
});

export const move = os.messages.move.use(authedBase).handler(async ({ ctx, input }) => {
  await ctx.db.patch(input.id, { channelId: input.channelId });
  return null;
});

export const remove = os.messages.remove.use(authedBase).handler(async ({ ctx, input }) => {
  await ctx.db.delete(input.id);
  return null;
});

export const listSince = os.messages.listSince.use(authedBase).handler(async ({ ctx, input }) => {
  return ctx.db
    .query("messages")
    .withIndex("by_channel", (q) => q.eq("channelId", input.channelId).gte("_creationTime", input.since))
    .order("desc")
    .take(100);
});

// Load test: a deliberately slow query, to probe head-of-line blocking.
export const slow = os.messages.slow.use(authedBase).handler(async ({ ctx, input }) => {
  // Param binds as text; pg_sleep needs float8, so cast explicitly.
  await ctx.sql`SELECT pg_sleep(${input.seconds}::float8)`;
  return { slept: input.seconds };
});

// Analytical slow query — runs on the OLAP pool (longer statement timeout).
export const slowAnalytical = os.messages.slowAnalytical
  .use(authedBase)
  .handler(async ({ ctx, input }) => {
    await ctx.sql`SELECT pg_sleep(${input.seconds}::float8)`;
    return { slept: input.seconds };
  });

export const listAll = os.messages.listAll.use(authedBase).handler(async ({ ctx }) => {
  return ctx.db.query("messages").collect();
});

export const countRaw = os.messages.countRaw.use(authedBase).handler(async ({ ctx, input }) => {
  const rows = await ctx.sql<{ n: number }>`
    SELECT count(*)::int AS n FROM messages WHERE channel_id = ${input.channelId}::uuid`;
  return { count: rows[0]?.n ?? 0 };
});

export const stats = os.messages.stats.use(authedBase).handler(async ({ ctx, input }) => {
  // Demonstrates a CTE + GROUP BY + aggregates through the raw SQL path.
  const rows = await ctx.sql<{ total: number; distinct_authors: number }>`
    WITH per_author AS (
      SELECT author_id, count(*) AS c
      FROM messages
      WHERE channel_id = ${input.channelId}::uuid
      GROUP BY author_id
    )
    SELECT
      (SELECT count(*)::int FROM messages WHERE channel_id = ${input.channelId}::uuid) AS total,
      (SELECT count(*)::int FROM per_author) AS distinct_authors`;
  const row = rows[0] ?? { total: 0, distinct_authors: 0 };
  return { total: row.total, distinctAuthors: row.distinct_authors };
});
