import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";

const os = implement(contract);
// Loose `any` on the query builder: this fixture skips codegen, so the ambient
// Doc<"widgets"> types aren't registered — runtime behavior is what's under test.
const q = (ctx: any) => ctx.db.query("widgets") as any;
const EVIL = "x'); DROP TABLE widgets; --";

export const list = os.w.list.handler(async ({ ctx }) => q(ctx).order("asc", "name").collect());
export const activeOnly = os.w.activeOnly.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.eq("active", true)).order("asc", "name").collect());
export const orFilter = os.w.orFilter.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.or(x.eq("qty", 1), x.eq("qty", 3))).order("asc", "name").collect());
export const inTags = os.w.inTags.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.in("tag", ["x", "y"])).order("asc", "name").collect());
export const notActive = os.w.notActive.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.not(x.eq("active", true))).order("asc", "name").collect());
export const likeName = os.w.likeName.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.like("name", "%a%")).order("asc", "name").collect());
export const ilikeName = os.w.ilikeName.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.ilike("name", "A%")).order("asc", "name").collect());
export const nullPrice = os.w.nullPrice.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.isNull("price")).order("asc", "name").collect());
export const hasPrice = os.w.hasPrice.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.isNotNull("price")).order("asc", "name").collect());
export const emptyIn = os.w.emptyIn.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.in("tag", [])).collect());
export const notEmptyIn = os.w.notEmptyIn.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.not(x.in("tag", []))).collect());
export const inject = os.w.inject.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.eq("name", EVIL)).collect());

export const multiSort = os.w.multiSort.handler(async ({ ctx }) =>
  q(ctx).order("asc", "active").order("desc", "name").collect());
export const page = os.w.page.handler(async ({ ctx, input }: any) =>
  q(ctx).order("asc", "name").paginate({ limit: input.limit, offset: input.offset }));

export const count = os.w.count.handler(async ({ ctx }) => q(ctx).count());
export const activeCount = os.w.activeCount.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.eq("active", true)).count());
export const countPrice = os.w.countPrice.handler(async ({ ctx }) => q(ctx).count("price"));
export const countDistinctTag = os.w.countDistinctTag.handler(async ({ ctx }) => q(ctx).countDistinct("tag"));
export const sumPrice = os.w.sumPrice.handler(async ({ ctx }) => q(ctx).sum("price"));
export const avgQty = os.w.avgQty.handler(async ({ ctx }) => q(ctx).avg("qty"));
export const minQty = os.w.minQty.handler(async ({ ctx }) => q(ctx).min("qty"));
export const maxQty = os.w.maxQty.handler(async ({ ctx }) => q(ctx).max("qty"));
// max() over an INTEGER field with a filter — int values are in the change image,
// so the reactor maintains this by IVM (no worker re-exec) on a new high score.
export const maxScoreActive = os.w.maxScoreActive.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.eq("active", true)).max("score"));
// max() over a DOUBLE PRECISION field with a filter — qty's float values are now in
// the change image too, so this is also maintained incrementally (IVM), not re-run.
export const maxQtyActive = os.w.maxQtyActive.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.eq("active", true)).max("qty"));
// sum() over an int field with a filter — maintained from the delta (IVM), with a
// fall-back to re-exec when the running sum hits 0 (ambiguous: empty set vs cancel).
export const sumScoreActive = os.w.sumScoreActive.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.eq("active", true)).sum("score"));
// avg() over a filtered int field — the reactor maintains it from the running
// sum+count the engine seeds, recomputing sum/count from the change delta.
export const avgScoreActive = os.w.avgScoreActive.handler(async ({ ctx }) =>
  q(ctx).filter((x: any) => x.eq("active", true)).avg("score"));

export const byActive = os.w.byActive.handler(async ({ ctx }) => q(ctx).groupBy("active").count());
export const sumByTag = os.w.sumByTag.handler(async ({ ctx }) => q(ctx).groupBy("tag").sum("price"));
export const bigTags = os.w.bigTags.handler(async ({ ctx }: any) => q(ctx).groupBy("tag").count({ gte: 2 }));

// Reactive join via handler composition.
export const withAuthor = os.w.withAuthor.handler(async ({ ctx }) => {
  const ws = await q(ctx).order("asc", "name").collect();
  // Batched join: issue every author get() concurrently so the worker's DataLoader
  // collapses them into one GetMany (ANY($1::uuid[])) round-trip. Each id keeps its
  // own point key in the read-set, so a change to any joined author re-fires this sub.
  const authors = await Promise.all(ws.map((w) => (w.authorId ? ctx.db.get(w.authorId) : null)));
  return ws.map((w, i) => ({ name: w.name, author: (authors[i] as { name?: string } | null)?.name ?? null }));
});

export const addAuthor = os.w.addAuthor.handler(async ({ ctx, input }: any) => {
  const id = await ctx.db.insert("authors", { name: input.name });
  return await ctx.db.get(id);
});
export const addWidget = os.w.addWidget.handler(async ({ ctx, input }: any) => {
  const doc: any = { name: input.name, qty: input.qty, active: input.active };
  if (input.price !== undefined) doc.price = input.price;
  if (input.score !== undefined) doc.score = input.score;
  if (input.tag !== undefined) doc.tag = input.tag;
  if (input.meta !== undefined) doc.meta = input.meta;
  if (input.authorId !== undefined) doc.authorId = input.authorId;
  const id = await ctx.db.insert("widgets", doc);
  return await ctx.db.get(id);
});
export const renameAuthor = os.w.renameAuthor.handler(async ({ ctx, input }: any) => {
  await ctx.db.patch(input.id, { name: input.name });
  return null;
});
export const setActive = os.w.setActive.handler(async ({ ctx, input }: any) => {
  await ctx.db.patch(input.id, { active: input.active });
  return null;
});
export const renameWidget = os.w.renameWidget.handler(async ({ ctx, input }: any) => {
  await ctx.db.patch(input.id, { name: input.name });
  return null;
});
// Full replace: the row becomes exactly the provided value — optional fields the
// caller omits are nulled (distinct from patch, which would leave them intact).
export const replaceWidget = os.w.replaceWidget.handler(async ({ ctx, input }: any) => {
  const { id, ...value } = input;
  await ctx.db.replace(id, value);
  return null;
});
export const remove = os.w.remove.handler(async ({ ctx, input }: any) => {
  await ctx.db.delete(input.id);
  return null;
});
