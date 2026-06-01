import { expectTypeOf, test } from "vitest";
import type { InferInput, InferKind, InferOutput } from "@onveloz/pulse-contract";
import type { Doc, Id } from "@onveloz/pulse-schema";
import { contract } from "./contract.js";
import "./_generated/dataModel.js";

test("contract inference end-to-end", () => {
  type ListInput = InferInput<typeof contract.messages.list>;
  type ListOutput = InferOutput<typeof contract.messages.list>;
  type ListKind = InferKind<typeof contract.messages.list>;

  expectTypeOf<ListInput>().toEqualTypeOf<{ channelId: Id<"channels"> }>();
  expectTypeOf<ListOutput>().toEqualTypeOf<Doc<"messages">[]>();
  expectTypeOf<ListKind>().toEqualTypeOf<"reactive">();

  type SendInput = InferInput<typeof contract.messages.send>;
  type SendOutput = InferOutput<typeof contract.messages.send>;
  expectTypeOf<SendInput>().toEqualTypeOf<{ channelId: Id<"channels">; body: string }>();
  expectTypeOf<SendOutput>().toEqualTypeOf<Doc<"messages">>();

  type SummarizeOutput = InferOutput<typeof contract.messages.summarize>;
  expectTypeOf<SummarizeOutput>().toEqualTypeOf<{ summary: string }>();
});

test("doc precision via augmented data model", () => {
  // editedAt is optional; role is a literal union; ids are branded.
  expectTypeOf<Doc<"messages">["channelId"]>().toEqualTypeOf<Id<"channels">>();
  expectTypeOf<Doc<"messages">["editedAt"]>().toEqualTypeOf<number | undefined>();
  expectTypeOf<Doc<"users">["role"]>().toEqualTypeOf<"admin" | "member">();
});
