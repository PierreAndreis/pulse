import { describe, test, expect } from "vitest";
import { v } from "@onveloz/pulse-schema";
import { isProcedure, oc } from "./index.js";

describe("oc procedure builders", () => {
  test("each kind starts as { kind, errors: {} }", () => {
    expect(oc.reactive().def).toEqual({ kind: "reactive", errors: {} });
    expect(oc.mutation().def).toEqual({ kind: "mutation", errors: {} });
    expect(oc.analytical().def).toEqual({ kind: "analytical", errors: {} });
    expect(oc.action().def).toEqual({ kind: "action", errors: {} });
  });

  test("input()/output() set def.input/def.output", () => {
    const input = v.object({ a: v.number() });
    const output = v.number();

    const proc = oc.mutation().input(input).output(output);

    expect(proc.def.input).toBe(input);
    expect(proc.def.output).toBe(output);
    expect(proc.def.kind).toBe("mutation");
  });

  test("chaining is immutable — a fresh builder is unchanged", () => {
    const fresh = oc.mutation();
    fresh.input(v.number()).output(v.string());

    expect(fresh.def).toEqual({ kind: "mutation", errors: {} });
    expect(fresh.def.input).toBeUndefined();
    expect(fresh.def.output).toBeUndefined();
  });

  test("errors() merges rather than replaces", () => {
    const A = { data: v.string() };
    const B = { data: v.number() };

    const proc = oc.reactive().errors({ A }).errors({ B });

    expect(Object.keys(proc.def.errors).sort()).toEqual(["A", "B"]);
    expect(proc.def.errors.A).toBe(A);
    expect(proc.def.errors.B).toBe(B);
  });

  test("errors() does not mutate the earlier builder's errors", () => {
    const first = oc.reactive().errors({ A: { data: v.string() } });
    first.errors({ B: { data: v.number() } });

    expect(Object.keys(first.def.errors)).toEqual(["A"]);
  });
});

describe("isProcedure", () => {
  test("a built procedure is a procedure", () => {
    expect(isProcedure(oc.reactive())).toBe(true);
  });

  test("a fully chained procedure is still a procedure", () => {
    const proc = oc
      .mutation()
      .input(v.object({ a: v.number() }))
      .output(v.number())
      .errors({ Conflict: { data: v.string() } });

    expect(isProcedure(proc)).toBe(true);
  });

  test("any object with an object def is a procedure", () => {
    expect(isProcedure({ def: {} })).toBe(true);
  });

  test("non-procedures are rejected", () => {
    expect(isProcedure(null)).toBe(false);
    expect(isProcedure({})).toBe(false);
    expect(isProcedure({ def: "x" })).toBe(false);
    expect(isProcedure(42)).toBe(false);
  });
});
