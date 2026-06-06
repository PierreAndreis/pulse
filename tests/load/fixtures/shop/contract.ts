import { oc } from "@onveloz/pulse-contract";
import { v } from "@onveloz/pulse-schema";

const rows = () => oc.reactive().output(v.array(v.any()));

export const contract = {
  s: {
    // --- reactive queries (each maps to a bottleneck class) ---
    revenue: oc.reactive().output(v.number()), // sum over all orders — heavy aggregate
    dashboard: oc.reactive().output(v.any()), // count + sum + group → several full scans
    recentOrders: rows(), // order desc + limit — re-runs on any order change
    ordersByStatus: rows(), // groupBy
    orderDetail: oc.reactive().input(v.object({ id: v.id("orders") })).output(v.any()), // N+1 join
    activityFeed: rows(), // HOT table (events) — identical subs (coalesce)
    feed: oc.reactive().input(v.object({ offset: v.number() })).output(v.array(v.any())), // distinct subs (no coalesce)
    userOrders: oc.reactive().input(v.object({ userId: v.id("users") })).output(v.array(v.any())), // precise (control)

    // --- mutations ---
    seed: oc
      .mutation()
      .input(v.object({ users: v.number(), products: v.number(), blobKb: v.optional(v.number()) }))
      .output(v.any()),
    placeOrder: oc
      .mutation()
      .input(v.object({ userId: v.id("users"), items: v.number() }))
      .output(v.any()),
    bulkOrders: oc
      .mutation()
      .input(v.object({ userId: v.id("users"), count: v.number() }))
      .output(v.any()), // one big tx → may blow past the NOTIFY 8KB cap
    setStatus: oc
      .mutation()
      .input(v.object({ id: v.id("orders"), status: v.string() }))
      .output(v.null()),
    logEvent: oc.mutation().input(v.object({ kind: v.string() })).output(v.any()), // hot-table writer
    firstUser: oc.mutation().output(v.any()),
    firstOrder: oc.mutation().output(v.any()),
  },
};
