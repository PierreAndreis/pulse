import { describe, expect, test } from "vitest";
import { v } from "@onveloz/pulse-schema";
import { makeErrors, PulseError } from "./errors.js";

const BUILTIN_CODES = [
  "UNAUTHORIZED",
  "FORBIDDEN",
  "NOT_FOUND",
  "CONFLICT",
  "BAD_REQUEST",
  "INTERNAL",
] as const;

describe("makeErrors", () => {
  test("exposes all builtin error constructors with sane defaults", () => {
    const errors = makeErrors({});
    for (const code of BUILTIN_CODES) {
      const err = errors[code]();
      expect(err).toBeInstanceOf(PulseError);
      expect(err).toBeInstanceOf(Error);
      expect(err.code).toBe(code);
      expect(err.data).toBeUndefined();
      expect(err.message).toBe(code);
      expect(err.name).toBe("PulseError");
    }
  });

  test("builtin called with a custom message overrides message, keeps code", () => {
    const errors = makeErrors({});
    const err = errors.UNAUTHORIZED({ message: "custom" });
    expect(err.message).toBe("custom");
    expect(err.code).toBe("UNAUTHORIZED");
    expect(err.data).toBeUndefined();
  });

  test("declared error carries its data and defaults message to code", () => {
    const errors = makeErrors({
      rateLimited: { data: v.object({ retryAfter: v.number() }) },
    });
    const err = errors.rateLimited({ retryAfter: 30 });
    expect(err.code).toBe("rateLimited");
    expect(err.data).toEqual({ retryAfter: 30 });
    expect(err.message).toBe("rateLimited");
  });

  test("declared error called with (data, opts) overrides message, preserves data", () => {
    const errors = makeErrors({
      rateLimited: { data: v.object({ retryAfter: v.number() }) },
    });
    const err = errors.rateLimited({ retryAfter: 5 }, { message: "too many" });
    expect(err.message).toBe("too many");
    expect(err.data).toEqual({ retryAfter: 5 });
    expect(err.code).toBe("rateLimited");
  });

  test("declared errors coexist with builtins without collision", () => {
    const errors = makeErrors({
      rateLimited: { data: v.object({ retryAfter: v.number() }) },
    });
    const builtin = errors.NOT_FOUND();
    expect(builtin.code).toBe("NOT_FOUND");
    expect(builtin.data).toBeUndefined();

    const declared = errors.rateLimited({ retryAfter: 1 });
    expect(declared.code).toBe("rateLimited");
    expect(declared.data).toEqual({ retryAfter: 1 });
  });

  test("throw/catch round-trip preserves type, code, and data", () => {
    const errors = makeErrors({
      rateLimited: { data: v.object({ retryAfter: v.number() }) },
    });
    try {
      throw errors.rateLimited({ retryAfter: 42 });
    } catch (caught) {
      expect(caught).toBeInstanceOf(PulseError);
      const err = caught as PulseError<{ retryAfter: number }>;
      expect(err.code).toBe("rateLimited");
      expect(err.data).toEqual({ retryAfter: 42 });
    }
  });
});
