import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";
import { authedBase } from "./middleware.js";
import "./_generated/dataModel.js";

const os = implement(contract);

export const create = os.notes.create.use(authedBase).handler(async ({ ctx, input }) => {
  // The collab `body` starts empty (NULL bytea → fresh Yjs doc on first edit).
  return ctx.db.insert("notes", { title: input.title });
});

export const getDoc = os.notes.getDoc.use(authedBase).handler(async ({ ctx, input }) => {
  const state = await ctx.db.getCollab(input.id, "body");
  return { state };
});

export const applyUpdate = os.notes.applyUpdate.use(authedBase).handler(async ({ ctx, input }) => {
  // The CRDT merge happens server-side (Rust yrs), inside this mutation's
  // serializable transaction. Concurrent/offline updates all merge, never clobber.
  const state = await ctx.db.applyCollab(input.id, "body", input.update);
  return { state };
});
