import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";

const os = implement(contract);
// Loose typing: this fixture skips codegen; runtime behavior is what's measured.
const q = (ctx: any, table: string) => ctx.db.query(table) as any;
const STATUSES = ["pending", "paid", "shipped"];

// --- reactive queries ---

// Heavy aggregate: a full scan summing every order's total.
export const revenue = os.s.revenue.handler(async ({ ctx }) => q(ctx, "orders").sum("total"));

// Dashboard: several full scans (count + revenue + per-status counts) in one query
// → re-runs entirely on ANY order write, cost grows with table size.
export const dashboard = os.s.dashboard.handler(async ({ ctx }: any) => {
  const orders = await q(ctx, "orders").count();
  const revenue = await q(ctx, "orders").sum("total");
  const byStatus = await q(ctx, "orders").groupBy("status").count();
  return { orders, revenue, byStatus };
});

export const recentOrders = os.s.recentOrders.handler(async ({ ctx }: any) =>
  q(ctx, "orders").order("desc", "_creationTime").paginate({ limit: 100, offset: 0 }),
);

export const ordersByStatus = os.s.ordersByStatus.handler(async ({ ctx }: any) =>
  q(ctx, "orders").groupBy("status").count(),
);

// N+1 join via handler composition: order → its items → product per item.
export const orderDetail = os.s.orderDetail.handler(async ({ ctx, input }: any) => {
  const order = await ctx.db.get(input.id);
  if (!order) return null;
  const items = await q(ctx, "lineitems").filter((x: any) => x.eq("orderId", input.id)).collect();
  const enriched: any[] = [];
  for (const it of items) {
    const product = await ctx.db.get(it.productId); // N+1
    enriched.push({ ...it, product: product?.name ?? null });
  }
  return { order, items: enriched };
});

// HOT table: every action appends an event, so every feed subscriber re-runs.
export const activityFeed = os.s.activityFeed.handler(async ({ ctx }: any) =>
  q(ctx, "events").order("desc", "_creationTime").paginate({ limit: 50, offset: 0 }),
);

// Same hot table, but a distinct input (offset) per subscriber → no coalescing,
// so each subscriber is its own re-execution on every event.
export const feed = os.s.feed.handler(async ({ ctx, input }: any) =>
  q(ctx, "events").order("desc", "_creationTime").paginate({ limit: 50, offset: input.offset }),
);

// Precise (control): a filtered scan that only re-runs for the matching user.
export const userOrders = os.s.userOrders.handler(async ({ ctx, input }: any) =>
  q(ctx, "orders").filter((x: any) => x.eq("userId", input.userId)).collect(),
);

// --- mutations ---

export const seed = os.s.seed.handler(async ({ ctx, input }: any) => {
  const blob = input.blobKb ? "x".repeat(input.blobKb * 1024) : undefined;
  const users: string[] = [];
  for (let i = 0; i < input.users; i++) users.push(await ctx.db.insert("users", { name: `u${i}` }));
  const products: string[] = [];
  for (let i = 0; i < input.products; i++) {
    const doc: any = { name: `p${i}`, price: (i % 50) + 1 };
    if (blob) doc.blob = blob;
    products.push(await ctx.db.insert("products", doc));
  }
  return { users, products };
});

export const placeOrder = os.s.placeOrder.handler(async ({ ctx, input }: any) => {
  const id = await ctx.db.insert("orders", { userId: input.userId, status: "pending", total: 0 });
  let total = 0;
  for (let i = 0; i < input.items; i++) {
    const price = (i % 50) + 1;
    total += price;
    await ctx.db.insert("lineitems", { orderId: id, productId: id, qty: 1, price });
  }
  await ctx.db.patch(id, { total });
  await ctx.db.insert("events", { kind: "order_placed", note: String(id) });
  return await ctx.db.get(id);
});

// One transaction writing many rows → a large ChangeSet that can exceed the
// NOTIFY 8KB cap, degrading cross-node delivery to a coarse table resync.
export const bulkOrders = os.s.bulkOrders.handler(async ({ ctx, input }: any) => {
  for (let i = 0; i < input.count; i++) {
    const id = await ctx.db.insert("orders", { userId: input.userId, status: "pending", total: (i % 50) + 1 });
    await ctx.db.insert("lineitems", { orderId: id, productId: id, qty: 1, price: (i % 50) + 1 });
    await ctx.db.insert("events", { kind: "bulk", note: String(i) });
  }
  return { count: input.count };
});

export const setStatus = os.s.setStatus.handler(async ({ ctx, input }: any) => {
  await ctx.db.patch(input.id, { status: input.status });
  await ctx.db.insert("events", { kind: "status", note: input.status });
  return null;
});

export const logEvent = os.s.logEvent.handler(async ({ ctx, input }: any) => {
  const id = await ctx.db.insert("events", { kind: input.kind, note: "x" });
  return await ctx.db.get(id);
});

export const firstUser = os.s.firstUser.handler(async ({ ctx }: any) => {
  const us = await q(ctx, "users").paginate({ limit: 1, offset: 0 });
  return us[0] ?? null;
});

export const firstOrder = os.s.firstOrder.handler(async ({ ctx }: any) => {
  const os2 = await q(ctx, "orders").paginate({ limit: 1, offset: 0 });
  return os2[0] ?? null;
});
