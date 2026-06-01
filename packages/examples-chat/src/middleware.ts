import { os } from "@onveloz/pulse-server";
import type { Id } from "@onveloz/pulse-schema";

async function verifyJwt(header: string | null): Promise<{ id: Id<"users">; name: string } | null> {
  if (!header) return null;
  // Demo only — real impl verifies a signed token. Uses the seeded demo user.
  return { id: "users:00000000-0000-0000-0000-000000000010" as Id<"users">, name: "Demo User" };
}

export const authed = os
  .$context<{ headers: Headers }>()
  .middleware(async ({ context, next, errors }) => {
    const user = await verifyJwt(context.headers.get("authorization"));
    if (!user) throw errors.UNAUTHORIZED();
    return next({ context: { user } });
  });

export const logged = os.middleware(async ({ next, path }) => {
  const start = performance.now();
  const result = await next();
  // Per-call logging is opt-in: synchronous console output on the hot path
  // serializes the worker under load. Enable with PULSE_LOG_TIMING=1.
  if (process.env.PULSE_LOG_TIMING) {
    // eslint-disable-next-line no-console
    console.log(path.join("."), performance.now() - start);
  }
  return result;
});

export const authedBase = os.use(logged).use(authed);
