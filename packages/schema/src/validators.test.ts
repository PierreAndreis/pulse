import { describe, expect, it } from "vitest";
import { v } from "./validators.js";
import { ValidationError } from "./types.js";

describe("validators — parse", () => {
  it("parses primitives and rejects mismatches", () => {
    expect(v.string().parse("a")).toBe("a");
    expect(v.number().parse(3)).toBe(3);
    expect(v.boolean().parse(true)).toBe(true);
    expect(v.null().parse(null)).toBeNull();
    expect(() => v.string().parse(1)).toThrow(ValidationError);
    expect(() => v.number().parse(Number.NaN)).toThrow(ValidationError);
  });

  it("handles object optional fields", () => {
    const val = v.object({ a: v.string(), b: v.optional(v.number()) });
    expect(val.parse({ a: "x" })).toEqual({ a: "x" });
    expect(val.parse({ a: "x", b: 2 })).toEqual({ a: "x", b: 2 });
    expect(() => val.parse({})).toThrow(ValidationError);
  });

  it("validates arrays element-wise with path", () => {
    const val = v.array(v.number());
    expect(val.parse([1, 2, 3])).toEqual([1, 2, 3]);
    try {
      val.parse([1, "x"]);
      throw new Error("should have thrown");
    } catch (e) {
      expect(e).toBeInstanceOf(ValidationError);
      expect((e as ValidationError).path).toEqual([1]);
    }
  });

  it("matches union members", () => {
    const val = v.union(v.literal("admin"), v.literal("member"));
    expect(val.parse("admin")).toBe("admin");
    expect(() => val.parse("other")).toThrow(ValidationError);
  });

  it("describe() emits a serializable shape", () => {
    const val = v.object({ id: v.id("users"), tags: v.array(v.string()) });
    expect(val.describe()).toEqual({
      kind: "object",
      fields: {
        id: { kind: "id", table: "users" },
        tags: { kind: "array", element: { kind: "string" } },
      },
      optional: [],
    });
  });
});
