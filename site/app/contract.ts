import { oc } from "@onveloz/pulse-contract";
import { v } from "@onveloz/pulse-schema";
import "./_generated/dataModel.js"; // registers Doc types for v.doc("cursors")

export const contract = {
  presence: {
    // Reactive: every cursor active in the last few seconds. Each move re-runs
    // this for every viewer — the intended "everyone sees everyone" fan-out.
    list: oc.reactive().output(v.array(v.doc("cursors"))),
    // Upsert my cursor (called ~15x/s while the pointer moves, throttled client-side).
    move: oc
      .mutation()
      .input(
        v.object({
          clientId: v.string(),
          x: v.number(),
          y: v.number(),
          country: v.string(),
          color: v.string(),
          channel: v.string(),
          scrollY: v.number(),
        }),
      )
      .output(v.null()),
    // Share this visitor's current text selection (character offsets; -1/-1 = none).
    select: oc
      .mutation()
      .input(
        v.object({
          clientId: v.string(),
          selStart: v.number(),
          selEnd: v.number(),
        }),
      )
      .output(v.null()),
    // Live cursor chat: set ("" clears the bubble).
    say: oc
      .mutation()
      .input(v.object({ clientId: v.string(), message: v.string() }))
      .output(v.null()),
    // Emoji reaction: a momentary burst everyone animates.
    react: oc
      .mutation()
      .input(v.object({ clientId: v.string(), emote: v.string() }))
      .output(v.null()),
    // Best-effort removal on tab close.
    leave: oc.mutation().input(v.object({ clientId: v.string() })).output(v.null()),
  },
};
