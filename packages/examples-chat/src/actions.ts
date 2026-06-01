import { implement } from "@pulse/server";
import type { Doc } from "@pulse/schema";
import { contract } from "./contract.js";
import { authedBase } from "./middleware.js";
import "./_generated/dataModel.js";

const os = implement(contract);

// An action: non-transactional, may do external I/O, and composes deterministic
// procedures by re-entering the engine. Here it sends a message (a mutation,
// atomic on its own) then reads the channel back (a query) and reports the count.
export const sendViaAction = os.actions.sendViaAction
  .use(authedBase)
  .handler(async ({ ctx, input }) => {
    await ctx.runMutation("messages.send", { channelId: input.channelId, body: input.body });
    const list = await ctx.runQuery<Doc<"messages">[]>("messages.list", {
      channelId: input.channelId,
    });
    return { listed: list.length };
  });
