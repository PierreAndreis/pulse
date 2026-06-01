import { describe, expect, it } from "vitest";
import { LocalStore, queryKeyOf, type OptimisticStore } from "./local.js";

const PATH = ["messages", "list"];
const INPUT = { channelId: "c1" };
const KEY = queryKeyOf(PATH, INPUT);

type Doc = { _id: string; body?: string };
const bodies = (s: LocalStore): (string | undefined)[] =>
  (s.getView(KEY) as Doc[]).map((d) => d.body);

describe("LocalStore", () => {
  it("serves confirmed data", () => {
    const s = new LocalStore();
    s.setConfirmed(KEY, [{ _id: "a" }]);
    expect(s.getView(KEY)).toEqual([{ _id: "a" }]);
  });

  it("shows the optimistic overlay over confirmed immediately", () => {
    const s = new LocalStore();
    s.setConfirmed(KEY, [{ _id: "a", body: "old" }]);
    s.addOptimistic("m1", (st: OptimisticStore) => {
      const cur = st.getQuery<Doc>(PATH, INPUT);
      st.setQuery(PATH, INPUT, [{ _id: st.tempId("messages"), body: "new" }, ...cur]);
    });
    expect(bodies(s)).toEqual(["new", "old"]);
  });

  it("rolls back when the optimistic mutation is removed", () => {
    const s = new LocalStore();
    s.setConfirmed(KEY, [{ _id: "a" }]);
    s.addOptimistic("m1", (st) => st.setQuery(PATH, INPUT, [{ _id: "opt" }]));
    expect((s.getView(KEY) as Doc[]).length).toBe(1);
    s.removeOptimistic("m1");
    expect(s.getView(KEY)).toEqual([{ _id: "a" }]);
  });

  it("rebases pending optimism over freshly-confirmed data", () => {
    const s = new LocalStore();
    s.setConfirmed(KEY, [{ _id: "a", body: "a" }]);
    s.addOptimistic("m1", (st) => {
      const cur = st.getQuery<Doc>(PATH, INPUT);
      st.setQuery(PATH, INPUT, [...cur, { _id: "opt", body: "mine" }]);
    });
    // a confirmed update arrives (e.g. another client's message)
    s.setConfirmed(KEY, [
      { _id: "a", body: "a" },
      { _id: "b", body: "other" },
    ]);
    expect(bodies(s)).toEqual(["a", "other", "mine"]);
  });

  it("keeps temp ids stable across recomputes", () => {
    const s = new LocalStore();
    s.setConfirmed(KEY, []);
    s.addOptimistic("m1", (st) =>
      st.setQuery(PATH, INPUT, [{ _id: st.tempId("messages") }]),
    );
    const first = (s.getView(KEY) as Doc[])[0]!._id;
    s.setConfirmed(KEY, [{ _id: "x" }]); // forces a recompute
    const stillThere = (s.getView(KEY) as Doc[]).find((d) => d._id !== "x")!._id;
    expect(stillThere).toBe(first);
  });

  it("notifies listeners with the materialized view", () => {
    const s = new LocalStore();
    const seen: (string | undefined)[][] = [];
    s.subscribe(KEY, (v) => seen.push((v as Doc[]).map((d) => d.body)));
    s.setConfirmed(KEY, [{ _id: "a", body: "x" }]);
    s.addOptimistic("m1", (st) => st.setQuery(PATH, INPUT, [{ _id: "opt", body: "y" }]));
    expect(seen.at(-1)).toEqual(["y"]);
  });

  it("reports listener presence for cleanup", () => {
    const s = new LocalStore();
    const unsub = s.subscribe(KEY, () => {});
    expect(s.hasListeners(KEY)).toBe(true);
    unsub();
    expect(s.hasListeners(KEY)).toBe(false);
  });
});
