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

  it("int accepts integers and rejects non-integers", () => {
    expect(v.int().parse(3)).toBe(3);
    expect(v.int().parse(-42)).toBe(-42);
    expect(v.int().parse(0)).toBe(0);
    expect(() => v.int().parse(1.5)).toThrow(ValidationError);
    expect(() => v.int().parse(Number.NaN)).toThrow(ValidationError);
    expect(() => v.int().parse("3")).toThrow(ValidationError);
    expect(v.int().describe()).toEqual({ kind: "int" });
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

  it("any passes through any value and describes itself", () => {
    const val = v.any();
    const payload = { nested: [1, "two", null] };
    expect(val.parse(payload)).toBe(payload);
    expect(val.parse(42)).toBe(42);
    expect(val.describe()).toEqual({ kind: "any" });
  });

  it("literal accepts the exact value, rejects others, and describes", () => {
    const val = v.literal("admin");
    expect(val.parse("admin")).toBe("admin");
    expect(() => val.parse("member")).toThrow(ValidationError);
    expect(val.describe()).toEqual({ kind: "literal", value: "admin" });
  });

  it("id accepts strings, rejects non-strings, and describes its table", () => {
    const val = v.id("users");
    expect(val.parse("u_123")).toBe("u_123");
    expect(() => val.parse(123)).toThrow(ValidationError);
    expect(val.describe()).toEqual({ kind: "id", table: "users" });
  });

  it("doc accepts objects, rejects non-objects, and describes its table", () => {
    const val = v.doc("messages");
    const row = { id: "m_1", body: "hi" };
    expect(val.parse(row)).toBe(row);
    expect(() => val.parse(null)).toThrow(ValidationError);
    expect(() => val.parse("not-a-doc")).toThrow(ValidationError);
    expect(val.describe()).toEqual({ kind: "doc", table: "messages" });
  });

  it("collab passes its opaque value through and describes itself", () => {
    const val = v.collab();
    const state = "base64state==";
    expect(val.parse(state)).toBe(state);
    expect(val.describe()).toEqual({ kind: "collab" });
  });

  it("optional yields undefined for missing values and delegates otherwise", () => {
    const val = v.optional(v.number());
    expect(val.parse(undefined)).toBeUndefined();
    expect(val.parse(5)).toBe(5);
    expect(val.optionality).toBe("optional");
    expect(() => val.parse("nope")).toThrow(ValidationError);
    expect(val.describe()).toEqual({ kind: "optional", inner: { kind: "number" } });
  });

  it("object rejects non-object inputs at the current path", () => {
    const val = v.object({ a: v.string() });
    for (const bad of [42, "str", null, [1, 2]]) {
      try {
        val.parse(bad);
        throw new Error("should have thrown");
      } catch (e) {
        expect(e).toBeInstanceOf(ValidationError);
        expect((e as ValidationError).path).toEqual([]);
      }
    }
  });

  it("nested object reports the failing inner field path", () => {
    const val = v.object({ user: v.object({ name: v.string() }) });
    try {
      val.parse({ user: { name: 99 } });
      throw new Error("should have thrown");
    } catch (e) {
      expect(e).toBeInstanceOf(ValidationError);
      expect((e as ValidationError).path).toEqual(["user", "name"]);
    }
  });
});
