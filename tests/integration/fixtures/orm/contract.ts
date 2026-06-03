import { oc } from "@onveloz/pulse-contract";
import { v } from "@onveloz/pulse-schema";

const rows = () => oc.reactive().output(v.array(v.any()));
const numN = () => oc.reactive().output(v.union(v.number(), v.null()));
const num = () => oc.reactive().output(v.number());

export const contract = {
  w: {
    // filters
    list: rows(),
    activeOnly: rows(),
    orFilter: rows(),
    inTags: rows(),
    notActive: rows(),
    likeName: rows(),
    ilikeName: rows(),
    nullPrice: rows(),
    hasPrice: rows(),
    emptyIn: rows(),
    notEmptyIn: rows(),
    inject: rows(),
    // ordering + pagination
    multiSort: rows(),
    page: oc.reactive().input(v.object({ limit: v.number(), offset: v.number() })).output(v.array(v.any())),
    // aggregates
    count: num(),
    countPrice: num(),
    countDistinctTag: num(),
    sumPrice: numN(),
    avgQty: numN(),
    minQty: numN(),
    maxQty: numN(),
    // grouping
    byActive: rows(),
    sumByTag: rows(),
    // join
    withAuthor: rows(),
    // mutations
    addAuthor: oc.mutation().input(v.object({ name: v.string() })).output(v.any()),
    addWidget: oc.mutation().input(v.object({ name: v.string(), qty: v.number(), price: v.optional(v.number()), active: v.boolean(), tag: v.optional(v.string()), authorId: v.optional(v.id("authors")) })).output(v.any()),
    renameAuthor: oc.mutation().input(v.object({ id: v.id("authors"), name: v.string() })).output(v.null()),
    setActive: oc.mutation().input(v.object({ id: v.id("widgets"), active: v.boolean() })).output(v.null()),
    remove: oc.mutation().input(v.object({ id: v.id("widgets") })).output(v.null()),
  },
};
