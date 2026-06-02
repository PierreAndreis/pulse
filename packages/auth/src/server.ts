import { createRemoteJWKSet, jwtVerify, type JWTPayload, type JWTVerifyGetKey } from "jose";
import { os } from "@onveloz/pulse-server";

/** The verified identity attached to every authed request. */
export interface PulseIdentity {
  /** The token subject — the auth provider's stable user id (JWT `sub`). */
  subject: string;
  /** Raw verified JWT claims (email, name, etc., depending on the provider). */
  claims: JWTPayload;
}

export interface PulseAuthOptions<TUserId extends string = string> {
  /**
   * The provider's JWKS endpoint (Better Auth's jwt plugin exposes one at
   * `/api/auth/jwks`). Used to verify token signatures without a shared secret.
   * Mutually exclusive with `getKey` (for tests / custom key sources).
   */
  jwksUrl?: string;
  /** Custom key resolver (e.g. a local key in tests). Overrides `jwksUrl`. */
  getKey?: JWTVerifyGetKey;
  /** Expected `iss` claim, if you want to pin it. */
  issuer?: string;
  /** Expected `aud` claim, if you want to pin it. */
  audience?: string;
  /**
   * Map a verified identity to your app's user id (and optionally provision the
   * user on first login). Receives `ctx.db`-style access via the handler context
   * is NOT available here — keep this pure-ish or do DB work in your own
   * middleware after `authed`. Defaults to using the JWT subject verbatim.
   */
  resolveUserId?: (identity: PulseIdentity) => TUserId | Promise<TUserId>;
}

/**
 * Better-Auth (and any JWKS-issuing provider) integration for Pulse, as a
 * reusable plugin — the Convex-equivalent of a hosted auth integration. Returns
 * an `authed` middleware you compose with `.use(authed)`; it verifies the bearer
 * JWT against the provider's JWKS (in the worker, via `jose`), then exposes the
 * verified identity as `ctx.auth` and the resolved app id as `ctx.userId`.
 *
 * Auth-method-agnostic: email/password, OAuth, magic links — anything that ends
 * in a signed JWT — all flow through the same verification.
 */
export function pulseAuth<TUserId extends string = string>(options: PulseAuthOptions<TUserId>) {
  const getKey: JWTVerifyGetKey =
    options.getKey ??
    (() => {
      if (!options.jwksUrl) throw new Error("pulseAuth: provide jwksUrl or getKey");
      return createRemoteJWKSet(new URL(options.jwksUrl));
    })();

  const authed = os
    .$context<{ headers: Headers }>()
    .middleware(async ({ context, next, errors }) => {
      const raw = context.headers.get("authorization");
      const token = raw?.replace(/^Bearer\s+/i, "").trim();
      if (!token) throw errors.UNAUTHORIZED();

      let payload: JWTPayload;
      try {
        ({ payload } = await jwtVerify(token, getKey, {
          issuer: options.issuer,
          audience: options.audience,
        }));
      } catch {
        throw errors.UNAUTHORIZED();
      }
      if (!payload.sub) throw errors.UNAUTHORIZED();

      const identity: PulseIdentity = { subject: payload.sub, claims: payload };
      const userId = options.resolveUserId
        ? await options.resolveUserId(identity)
        : (payload.sub as TUserId);

      return next({ context: { auth: identity, userId } });
    });

  return { authed };
}
