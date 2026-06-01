import { describe, expect, it } from "vitest";
import { oc } from "@onveloz/pulse-contract";
import { v } from "@onveloz/pulse-schema";
import { executeProcedure, implement, os } from "./index.js";

describe("server builder", () => {
  it("runs the middleware onion then the handler", async () => {
    const contract = {
      math: {
        add: oc
          .mutation()
          .input(v.object({ a: v.number(), b: v.number() }))
          .output(v.number()),
      },
    };
    const impl = implement(contract);

    const order: string[] = [];
    const proc = impl.math.add
      .use(
        os.middleware(async ({ next }) => {
          order.push("before");
          const r = await next({ context: { traced: true } });
          order.push("after");
          return r;
        }),
      )
      .handler(async ({ ctx, input }) => {
        order.push("handler");
        expect(ctx.traced).toBe(true);
        return input.a + input.b;
      });

    const result = await executeProcedure(
      proc,
      { headers: new Headers(), requestId: "r1" },
      { a: 2, b: 3 },
    );

    expect(result).toBe(5);
    expect(order).toEqual(["before", "handler", "after"]);
    expect(proc.path).toEqual(["math", "add"]);
    expect(proc.def.kind).toBe("mutation");
  });

  it("composes reusable base builders", async () => {
    const contract = { ping: oc.reactive().output(v.string()) };
    const impl = implement(contract);
    const base = os
      .use(os.middleware(async ({ next }) => next({ context: { a: 1 } })))
      .use(os.middleware(async ({ next }) => next({ context: { b: 2 } })));

    const proc = impl.ping.use(base).handler(async ({ ctx }) => {
      return `${ctx.a}-${ctx.b}`;
    });

    const result = await executeProcedure(proc, { headers: new Headers(), requestId: "r" }, undefined);
    expect(result).toBe("1-2");
  });
});
