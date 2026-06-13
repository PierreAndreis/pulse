import { describe, expect, it } from "vitest";
import { defineSchema, defineTable } from "./schema.js";
import { v } from "./validators.js";

describe("defineTable", () => {
  it("keeps the fields and starts with no indexes", () => {
    const t = defineTable({ name: v.string(), qty: v.int() });
    expect(Object.keys(t.fields)).toEqual(["name", "qty"]);
    expect(t.indexes).toEqual([]);
  });

  it("index() is chainable and accumulates indexes in declaration order", () => {
    const t = defineTable({ name: v.string(), qty: v.int() })
      .index("by_qty", ["qty"])
      .index("by_name_qty", ["name", "qty"]);
    expect(t.indexes).toEqual([
      { name: "by_qty", columns: ["qty"] },
      { name: "by_name_qty", columns: ["name", "qty"] },
    ]);
  });

  it("index() returns the same definition (so the chain mutates one table)", () => {
    const t = defineTable({ qty: v.int() });
    expect(t.index("by_qty", ["qty"])).toBe(t);
  });

  it("copies the columns array, so mutating the caller's array can't corrupt the index", () => {
    const cols: ("qty" | "other")[] = ["qty"];
    const t = defineTable({ qty: v.int(), other: v.int() }).index("by_qty", cols);
    cols.push("other"); // mutate after registering
    expect(t.indexes[0]!.columns).toEqual(["qty"]);
  });

  it("allows the system column _creationTime in an index", () => {
    const t = defineTable({ qty: v.int() }).index("by_time", ["_creationTime", "qty"]);
    expect(t.indexes[0]!.columns).toEqual(["_creationTime", "qty"]);
  });
});

describe("defineSchema.describe", () => {
  it("emits each table's described fields and its indexes", () => {
    const schema = defineSchema({
      widgets: defineTable({
        name: v.string(),
        qty: v.int(),
        tag: v.optional(v.string()),
      }).index("by_qty", ["qty"]),
    });
    expect(schema.describe()).toEqual({
      widgets: {
        fields: {
          name: { kind: "string" },
          qty: { kind: "int" },
          tag: { kind: "optional", inner: { kind: "string" } },
        },
        indexes: [{ name: "by_qty", columns: ["qty"] }],
      },
    });
  });

  it("describes every table and preserves the original `tables` reference", () => {
    const widgets = defineTable({ name: v.string() });
    const gadgets = defineTable({ label: v.string() }).index("by_label", ["label"]);
    const schema = defineSchema({ widgets, gadgets });

    expect(Object.keys(schema.describe())).toEqual(["widgets", "gadgets"]);
    expect(schema.describe().gadgets!.indexes).toEqual([{ name: "by_label", columns: ["label"] }]);
    expect(schema.tables.widgets).toBe(widgets);
  });

  it("is a pure projection — calling it twice yields equal output", () => {
    const schema = defineSchema({ t: defineTable({ a: v.number() }).index("by_a", ["a"]) });
    expect(schema.describe()).toEqual(schema.describe());
  });
});
