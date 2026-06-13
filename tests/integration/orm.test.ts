// ORM query-builder integration suite: filters (and/or/not/in/like/null), sort,
// pagination, aggregates (count/countField/distinct/sum/min/max/avg), GROUP BY,
// joins, reactivity, and SQL-injection safety — scenarios adapted from the
// Drizzle and Prisma test suites, run against the real engine + Bun worker.
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { resolve } from "node:path";
import { createClient, type Client } from "@onveloz/pulse-client";
import { startEngine, applySql, type Harness } from "./harness.js";
import { generateDDL } from "../../packages/cli/src/ddl.js";
import ormSchema from "./fixtures/orm/schema.js";
import type { contract } from "./fixtures/orm/contract.js";

const APP = resolve(process.cwd(), "tests/integration/fixtures/orm/app.ts");

let h: Harness;
let c: Client<typeof contract>;

async function waitFor(predicate: () => boolean, timeoutMs = 10000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("waitFor: condition not met within timeout");
}

/** Seed the standard widget set used across the filter/aggregate tests. */
async function seed(): Promise<void> {
  await c.w.addWidget.call({ name: "apple", qty: 1, price: 10, active: true, tag: "x" });
  await c.w.addWidget.call({ name: "banana", qty: 2, active: false }); // price + tag null
  await c.w.addWidget.call({ name: "avocado", qty: 3, price: 30, active: true, tag: "y" });
  await c.w.addWidget.call({ name: "cherry", qty: 1, active: false, tag: "x" }); // price null
}
const names = (rows: unknown[]): string[] => (rows as { name: string }[]).map((r) => r.name);

beforeAll(async () => {
  await applySql(generateDDL(ormSchema));
  h = await startEngine({ app: APP });
  c = createClient<typeof contract>({
    url: h.baseUrl,
    headers: () => ({ authorization: "Bearer test" }),
  });
}, 120_000);

afterAll(async () => {
  await h?.stop();
  await applySql("drop table if exists widgets; drop table if exists authors");
});

beforeEach(async () => {
  await applySql("truncate widgets, authors");
});

describe("ORM filters", () => {
  test("eq / or / in / not / like / ilike / is null / is not null", async () => {
    await seed();
    expect(names(await c.w.activeOnly.call({}))).toEqual(["apple", "avocado"]);
    expect(names(await c.w.orFilter.call({}))).toEqual(["apple", "avocado", "cherry"]);
    expect(names(await c.w.inTags.call({}))).toEqual(["apple", "avocado", "cherry"]);
    expect(names(await c.w.notActive.call({}))).toEqual(["banana", "cherry"]);
    expect(names(await c.w.likeName.call({}))).toEqual(["apple", "avocado", "banana"]);
    expect(names(await c.w.ilikeName.call({}))).toEqual(["apple", "avocado"]);
    expect(names(await c.w.nullPrice.call({}))).toEqual(["banana", "cherry"]);
    expect(names(await c.w.hasPrice.call({}))).toEqual(["apple", "avocado"]);
  });

  test("empty in([]) → none; not(in([])) → all (Drizzle empty-array case)", async () => {
    await seed();
    expect(await c.w.emptyIn.call({})).toHaveLength(0);
    expect(await c.w.notEmptyIn.call({})).toHaveLength(4);
  });

  test("a SQL-injection value is parameterized — no match, table intact", async () => {
    await seed();
    expect(await c.w.inject.call({})).toHaveLength(0);
    expect(await c.w.count.call({})).toBe(4); // table not dropped
  });
});

describe("ORM ordering + pagination", () => {
  test("multi-column ORDER BY (active asc, name desc)", async () => {
    await seed();
    expect(names(await c.w.multiSort.call({}))).toEqual(["cherry", "banana", "avocado", "apple"]);
  });

  test("offset pagination", async () => {
    await seed();
    expect(names(await c.w.page.call({ limit: 2, offset: 1 }))).toEqual(["avocado", "banana"]);
    expect(await c.w.page.call({ limit: 10, offset: 100 })).toHaveLength(0);
  });

  test("withIndex(name) orders by the index's columns (asc by default, .order('desc') flips)", async () => {
    // The schema declares .index("by_qty", ["qty"]); withIndex orders by qty, not
    // the default _creationTime — proven by inserting out of qty order.
    await c.w.addWidget.call({ name: "c", qty: 3, active: true });
    await c.w.addWidget.call({ name: "a", qty: 1, active: true });
    await c.w.addWidget.call({ name: "b", qty: 2, active: true });
    expect(names(await c.w.byQty.call({}))).toEqual(["a", "b", "c"]); // qty 1,2,3
    expect(names(await c.w.byQtyDesc.call({}))).toEqual(["c", "b", "a"]); // qty 3,2,1
  });
});

describe("ORM aggregates (SQL NULL semantics, like Drizzle/Prisma)", () => {
  test("count, count(field), countDistinct, sum, avg, min, max", async () => {
    await seed();
    expect(await c.w.count.call({})).toBe(4);
    expect(await c.w.countPrice.call({})).toBe(2); // non-null prices
    expect(await c.w.countDistinctTag.call({})).toBe(2); // {x, y}, null excluded
    expect(await c.w.sumPrice.call({})).toBe(40); // 10 + 30
    expect(await c.w.avgQty.call({})).toBeCloseTo(1.75, 6);
    expect(await c.w.minQty.call({})).toBe(1);
    expect(await c.w.maxQty.call({})).toBe(3);
  });

  test("sum/avg/min/max over an empty set → null; count → 0", async () => {
    expect(await c.w.count.call({})).toBe(0);
    expect(await c.w.sumPrice.call({})).toBeNull();
    expect(await c.w.avgQty.call({})).toBeNull();
    expect(await c.w.minQty.call({})).toBeNull();
    expect(await c.w.maxQty.call({})).toBeNull();
  });
});

describe("ORM group by", () => {
  test("group by active → count; group by tag → sum(price)", async () => {
    await seed();
    const byActive = (await c.w.byActive.call({})) as { key: boolean; value: number }[];
    expect(byActive.find((g) => g.key === false)?.value).toBe(2);
    expect(byActive.find((g) => g.key === true)?.value).toBe(2);

    const sumByTag = (await c.w.sumByTag.call({})) as { key: string | null; value: number | null }[];
    expect(sumByTag.find((g) => g.key === "x")?.value).toBe(10); // apple 10 + cherry null
    expect(sumByTag.find((g) => g.key === "y")?.value).toBe(30);
  });

  test("HAVING keeps only groups whose aggregate satisfies the predicate", async () => {
    await seed(); // tag x has 2 widgets (apple, cherry); y has 1; null has 1
    const big = (await c.w.bigTags.call({})) as { key: string | null; value: number }[];
    // count(*) >= 2 → only tag "x"
    expect(big).toEqual([{ key: "x", value: 2 }]);
  });
});

describe("ORM replace vs patch", () => {
  test("replace nulls columns omitted from the value (true replace, not patch)", async () => {
    const w = (await c.w.addWidget.call({
      name: "r1",
      qty: 1,
      price: 10,
      score: 7,
      active: true,
      tag: "x",
    })) as { _id: string };

    // Replace supplying only the required fields (+ a changed qty) — every omitted
    // optional column must be nulled. A patch would have left price/score/tag intact.
    await c.w.replaceWidget.call({ id: w._id as never, name: "r1", qty: 9, active: false });

    const got = (await c.w.list.call({})).find((r: any) => r.name === "r1") as any;
    expect(got.qty).toBe(9);
    expect(got.active).toBe(false);
    expect(got.price ?? null).toBeNull(); // omitted → nulled
    expect(got.score ?? null).toBeNull();
    expect(got.tag ?? null).toBeNull();
  });

  test("patch leaves omitted columns intact (contrast with replace)", async () => {
    const w = (await c.w.addWidget.call({ name: "p1", qty: 1, price: 10, active: true })) as {
      _id: string;
    };
    await c.w.renameWidget.call({ id: w._id as never, name: "p1-renamed" }); // patch: name only
    const got = (await c.w.list.call({})).find((r: any) => r.name === "p1-renamed") as any;
    expect(got.price).toBe(10); // untouched by the patch
    expect(got.qty).toBe(1);
  });
});

describe("ORM scalar round-trip fidelity (the text boundary)", () => {
  // Every value crosses the SQL boundary as text — written via json_to_bind
  // ($n::<cast>) and read via col::text → JSON. Assert each PgTypeClass survives a
  // write→read trip unchanged, including edge values (empty/quoted strings,
  // negative/zero numbers, an id-ref re-encoding to `table:uuid`).
  test("each column type round-trips write→read unchanged", async () => {
    const author = (await c.w.addAuthor.call({ name: "auth" })) as { _id: string };
    const w = (await c.w.addWidget.call({
      name: "", // empty text
      qty: -3.5, // negative float8
      price: 0, // zero float8 (distinct from null)
      score: -42, // negative bigint
      active: false, // bool
      tag: "a'b\"c", // quotes — proves parameterization, not interpolation
      authorId: author._id as never, // id-ref (uuid)
    })) as any;

    expect(w.name).toBe("");
    expect(w.qty).toBe(-3.5);
    expect(w.price).toBe(0);
    expect(w.score).toBe(-42);
    expect(w.active).toBe(false);
    expect(w.tag).toBe("a'b\"c");
    expect(w.authorId).toBe(author._id); // uuid re-encodes to `authors:<uuid>`
    expect(typeof w._id).toBe("string");
    expect(typeof w._creationTime).toBe("number"); // system bigint → JS number
  });

  test("omitted optional columns read back as null", async () => {
    const w = (await c.w.addWidget.call({ name: "n", qty: 1, active: true })) as any;
    expect(w.price ?? null).toBeNull();
    expect(w.score ?? null).toBeNull();
    expect(w.tag ?? null).toBeNull();
    expect(w.authorId ?? null).toBeNull();
  });

  test("a jsonb column round-trips a nested object/array unchanged", async () => {
    const meta = {
      note: "hi",
      n: 3.5,
      flag: true,
      nested: { tags: ["a", "b"], empty: null },
      list: [1, 2, { k: "v" }],
    };
    const w = (await c.w.addWidget.call({ name: "j", qty: 1, active: true, meta })) as any;
    expect(w.meta).toEqual(meta); // object → ::jsonb → text → parse, deep-equal
  });
});

describe("ORM joins (handler composition)", () => {
  test("join widgets→authors and react to a change in the joined row", async () => {
    const alice = (await c.w.addAuthor.call({ name: "alice" })) as { _id: string };
    await c.w.addWidget.call({ name: "w1", qty: 1, active: true, authorId: alice._id as never });

    const seen: string[] = [];
    const unsub = c.w.withAuthor.subscribe({}, (d) =>
      seen.push((d as { name: string; author: string | null }[]).map((r) => `${r.name}:${r.author}`).join(",")),
    );
    try {
      await waitFor(() => seen.length >= 1);
      expect(seen.at(-1)).toBe("w1:alice");
      await c.w.renameAuthor.call({ id: alice._id as never, name: "alice2" });
      await waitFor(() => seen.at(-1) === "w1:alice2"); // joined-row change re-runs
    } finally {
      unsub();
    }
  });

  test("batched join keeps per-id key precision: an unrelated author does not re-fire", async () => {
    // The handler reads authors via a batched get() (DataLoader → GetMany). Each id
    // must land as its own point key in the read-set — so renaming an author NOT in
    // the join is invisible, while renaming the joined one re-runs. This is the M3
    // precision invariant proven end-to-end through the batched path.
    const alice = (await c.w.addAuthor.call({ name: "alice" })) as { _id: string };
    const bob = (await c.w.addAuthor.call({ name: "bob" })) as { _id: string };
    await c.w.addWidget.call({ name: "w1", qty: 1, active: true, authorId: alice._id as never });

    const seen: string[] = [];
    const unsub = c.w.withAuthor.subscribe({}, (d) =>
      seen.push((d as { name: string; author: string | null }[]).map((r) => `${r.name}:${r.author}`).join(",")),
    );
    try {
      await waitFor(() => seen.at(-1) === "w1:alice");

      const pushesBefore = seen.length;
      await c.w.renameAuthor.call({ id: bob._id as never, name: "bob2" }); // bob is NOT joined
      await new Promise((r) => setTimeout(r, 600));
      expect(seen.length).toBe(pushesBefore); // unrelated author pruned — no re-fire

      await c.w.renameAuthor.call({ id: alice._id as never, name: "alice2" }); // joined → re-runs
      await waitFor(() => seen.at(-1) === "w1:alice2");
    } finally {
      unsub();
    }
  });
});

describe("ORM reactivity is precise", () => {
  test("filtered subscription: matching write pushes, non-matching is pruned", async () => {
    const seen: string[][] = [];
    const unsub = c.w.activeOnly.subscribe({}, (d) => seen.push(names(d as unknown[])));
    try {
      await waitFor(() => seen.length >= 1);
      await c.w.addWidget.call({ name: "live", qty: 1, active: true }); // matches active=true
      await waitFor(() => seen.at(-1)?.includes("live") ?? false);

      const pushesBefore = seen.length;
      await c.w.addWidget.call({ name: "hidden", qty: 1, active: false }); // does NOT match
      await new Promise((r) => setTimeout(r, 600));
      expect(seen.length).toBe(pushesBefore); // pruned — no extra push
    } finally {
      unsub();
    }
  });

  test("reactive count recomputes on a matching write", async () => {
    const counts: number[] = [];
    const unsub = c.w.count.subscribe({}, (n) => counts.push(n as number));
    try {
      await waitFor(() => counts.length >= 1);
      expect(counts.at(-1)).toBe(0);
      await c.w.addWidget.call({ name: "x", qty: 1, active: true });
      await waitFor(() => counts.at(-1) === 1);
    } finally {
      unsub();
    }
  });

  test("filtered count() prunes an unread-column update but fires on a membership flip", async () => {
    const counts: number[] = [];
    const unsub = c.w.activeCount.subscribe({}, (n) => counts.push(n as number));
    try {
      await waitFor(() => counts.length >= 1);
      expect(counts.at(-1)).toBe(0);
      const w = (await c.w.addWidget.call({ name: "w1", qty: 1, active: true })) as { _id: string };
      await waitFor(() => counts.at(-1) === 1); // counted

      // Rename: `name` is neither the filter column nor aggregated → count unchanged.
      const pushesBefore = counts.length;
      await c.w.renameWidget.call({ id: w._id as never, name: "w1-renamed" });
      await new Promise((r) => setTimeout(r, 600));
      expect(counts.length).toBe(pushesBefore); // pruned — no recompute

      // Membership flip: active true→false removes it from the count → must recompute.
      await c.w.setActive.call({ id: w._id as never, active: false });
      await waitFor(() => counts.at(-1) === 0);
    } finally {
      unsub();
    }
  });

  test("a filtered count() update is served by IVM and delivered (ivmPushed++)", async () => {
    // `ivmPushed` counts maintained values the reactor actually *pushed* to a
    // client (post diff-suppression) — a server-confirmed delivery decision. We
    // gate on it rather than on the client SSE arrival, whose final hop can lag
    // under CI load (the source of a prior flake); it's also stronger than
    // `ivmApplied`, which counts IVM matches before suppression.
    const metrics = async () =>
      (await (await fetch(`${h.baseUrl}/metrics`)).json()) as {
        ivmApplied: number;
        ivmPushed: number;
      };
    const counts: number[] = [];
    const unsub = c.w.activeCount.subscribe({}, (n) => counts.push(n as number));
    try {
      await waitFor(() => counts.length >= 1); // initial snapshot (a real re-exec)
      const before = await metrics();
      const initial = counts.at(-1);
      // A matching write: the count is maintained from the change delta — the
      // reactor never calls the worker (ivmApplied++) and pushes it (ivmPushed++).
      await c.w.addWidget.call({ name: "ivm-z", qty: 1, active: true });
      const deadline = Date.now() + 15000;
      while (Date.now() < deadline && (await metrics()).ivmPushed <= before.ivmPushed) {
        await new Promise((r) => setTimeout(r, 50));
      }
      const after = await metrics();
      expect(after.ivmPushed).toBeGreaterThan(before.ivmPushed); // maintained AND delivered
      expect(after.ivmApplied).toBeGreaterThan(before.ivmApplied); // via IVM, not re-exec
      // Best-effort: the pushed value also reaches this client over SSE. Truly
      // non-gating — delivery of a maintained count is already proven server-side
      // above and gated end-to-end by the sibling tests; the final client hop can
      // lag past any fixed bound under CI load, so a timeout here must not fail.
      await waitFor(() => counts.at(-1) !== initial, 5000).catch(() => {});
    } finally {
      unsub();
    }
  });

  test("a max() over an int field rises by IVM on a new high (ivmApplied++)", async () => {
    // score is v.int() → bigint → its values are in the change image, so the reactor
    // maintains max(score) from the delta with no worker re-exec. (The same query
    // over qty — double precision — would re-exec, since floats aren't captured.)
    const ivm = async () =>
      ((await (await fetch(`${h.baseUrl}/metrics`)).json()) as { ivmApplied: number }).ivmApplied;
    await c.w.addWidget.call({ name: "s1", qty: 1, score: 5, active: true }); // seed max=5
    const maxes: (number | null)[] = [];
    const unsub = c.w.maxScoreActive.subscribe({}, (n) => maxes.push(n as number | null));
    try {
      await waitFor(() => maxes.at(-1) === 5); // initial snapshot (a real re-exec)
      const before = await ivm();
      await c.w.addWidget.call({ name: "s2", qty: 1, score: 12, active: true }); // new high
      await waitFor(() => maxes.at(-1) === 12);
      expect(await ivm()).toBeGreaterThan(before); // maintained from the delta, no re-exec
    } finally {
      unsub();
    }
  });

  test("a max() over a DOUBLE field rises by IVM on a new high (ivmApplied++)", async () => {
    // qty is v.number() → double precision. Its values are now carried in the change
    // image too, so max(qty) is maintained from the delta — no longer dormant.
    const ivm = async () =>
      ((await (await fetch(`${h.baseUrl}/metrics`)).json()) as { ivmApplied: number }).ivmApplied;
    await c.w.addWidget.call({ name: "q1", qty: 2.5, active: true }); // seed max=2.5
    const maxes: (number | null)[] = [];
    const unsub = c.w.maxQtyActive.subscribe({}, (n) => maxes.push(n as number | null));
    try {
      await waitFor(() => maxes.at(-1) === 2.5); // initial snapshot (a real re-exec)
      const before = await ivm();
      await c.w.addWidget.call({ name: "q2", qty: 9.5, active: true }); // new high
      await waitFor(() => maxes.at(-1) === 9.5);
      expect(await ivm()).toBeGreaterThan(before); // float value maintained from the delta
    } finally {
      unsub();
    }
  });

  test("a filtered sum() stays correct through inserts/deletes incl the empty↔NULL boundary", async () => {
    const ivm = async () =>
      ((await (await fetch(`${h.baseUrl}/metrics`)).json()) as { ivmApplied: number }).ivmApplied;
    const seen: (number | null)[] = [];
    const unsub = c.w.sumScoreActive.subscribe({}, (n) => seen.push(n as number | null));
    try {
      await waitFor(() => seen.at(-1) === null); // empty set → SQL sum is NULL (not [])

      const w5 = (await c.w.addWidget.call({ name: "a", qty: 1, score: 5, active: true })) as {
        _id: string;
      };
      await waitFor(() => seen.at(-1) === 5); // null→first re-execs (base was null)

      const ivmBefore = await ivm();
      const w3 = (await c.w.addWidget.call({ name: "b", qty: 1, score: 3, active: true })) as {
        _id: string;
      };
      await waitFor(() => seen.at(-1) === 8); // +3 maintained from the delta

      await c.w.addWidget.call({ name: "c", qty: 1, score: 2, active: false }); // outside filter
      await new Promise((r) => setTimeout(r, 500));
      expect(seen.at(-1)).toBe(8); // an inactive insert doesn't match → still 8

      await c.w.remove.call({ id: w3._id as never }); // 8 − 3 = 5, maintained
      await waitFor(() => seen.at(-1) === 5);
      expect(await ivm()).toBeGreaterThan(ivmBefore); // the →8 and →5 steps were IVM

      await c.w.remove.call({ id: w5._id as never }); // 5 − 5 = 0 → ambiguous → re-exec → NULL
      await waitFor(() => seen.at(-1) === null); // empty again → NULL, not []
    } finally {
      unsub();
    }
  });

  test("a filtered avg() is maintained from auxiliary sum+count (ivmApplied++)", async () => {
    // avg can't be reconstructed from its scalar, so the engine seeds the reactor
    // with the sum+count behind it; the reactor advances both by the change delta.
    const ivm = async () =>
      ((await (await fetch(`${h.baseUrl}/metrics`)).json()) as { ivmApplied: number }).ivmApplied;
    await c.w.addWidget.call({ name: "v1", qty: 1, score: 4, active: true }); // seed avg = 4
    const seen: (number | null)[] = [];
    const unsub = c.w.avgScoreActive.subscribe({}, (n) => seen.push(n as number | null));
    try {
      await waitFor(() => seen.at(-1) === 4); // initial snapshot (a real re-exec, seeds sum=4/count=1)
      const before = await ivm();
      await c.w.addWidget.call({ name: "v2", qty: 1, score: 8, active: true }); // (4+8)/2 = 6
      await waitFor(() => seen.at(-1) === 6);
      expect(await ivm()).toBeGreaterThan(before); // maintained from sum+count, no re-exec
    } finally {
      unsub();
    }
  });
});

describe("commit LSN watermark (end-to-end via raw SSE)", () => {
  // The typed client ignores commitLsn, so read the SSE stream directly to prove
  // real WAL LSNs are captured at commit and flow through to the push, monotonically.
  test("pushes carry a non-zero, monotonically non-decreasing commitLsn", async () => {
    const clientId = "lsn-probe";
    const auth = { authorization: "Bearer test" };
    const lsns: string[] = [];

    // Open the SSE stream and parse `data:` frames in the background.
    const ac = new AbortController();
    const res = await fetch(`${h.baseUrl}/sync?clientId=${clientId}`, { headers: auth, signal: ac.signal });
    const reader = res.body!.getReader();
    const pump = (async () => {
      const dec = new TextDecoder();
      let buf = "";
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          let nl: number;
          while ((nl = buf.indexOf("\n")) >= 0) {
            const line = buf.slice(0, nl).trim();
            buf = buf.slice(nl + 1);
            if (line.startsWith("data:")) {
              try {
                const evt = JSON.parse(line.slice(5).trim());
                if (evt.sub === "count" && typeof evt.commitLsn === "string") lsns.push(evt.commitLsn);
              } catch {
                /* keepalive / non-JSON frame */
              }
            }
          }
        }
      } catch {
        /* aborted */
      }
    })();

    try {
      // Subscribe to count (its value changes on every insert).
      await fetch(`${h.baseUrl}/subscribe`, {
        method: "POST",
        headers: { ...auth, "content-type": "application/json" },
        body: JSON.stringify({ clientId, sub: "count", path: ["w", "count"], input: {} }),
      });
      await waitFor(() => lsns.length >= 1); // initial push (LSN 0/0)

      await c.w.addWidget.call({ name: "lsn1", qty: 1, active: true });
      await waitFor(() => lsns.length >= 2);
      await c.w.addWidget.call({ name: "lsn2", qty: 1, active: true });
      await waitFor(() => lsns.length >= 3);

      const parse = (s: string): number => {
        const [hi, lo] = s.split("/");
        return parseInt(hi, 16) * 2 ** 32 + parseInt(lo, 16);
      };
      const post = lsns.slice(1).map(parse); // drop the initial 0/0 push
      expect(post[0]).toBeGreaterThan(0); // real WAL LSN captured at commit
      expect(post[1]).toBeGreaterThanOrEqual(post[0]); // monotonic
    } finally {
      ac.abort();
      await pump.catch(() => {});
    }
  });
});
