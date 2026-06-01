import { useMutation, useQuery } from "@tanstack/react-query";
import type { Id } from "@onveloz/pulse-schema";
import { pulse } from "./client.js";

export function Channel({ channelId }: { channelId: Id<"channels"> }) {
  // Reactive query → auto-subscribes over SSE (M2); updates push into the cache.
  const { data } = useQuery(pulse.messages.list.queryOptions({ input: { channelId } }));
  const send = useMutation(pulse.messages.send.mutationOptions());

  return (
    <div>
      <ul>
        {data?.map((m) => (
          <li key={m._id}>{m.body}</li>
        ))}
      </ul>
      <button onClick={() => send.mutate({ channelId, body: "hello" })}>Send</button>
    </div>
  );
}
