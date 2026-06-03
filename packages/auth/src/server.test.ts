import { describe, expect, it } from "vitest";
import { SignJWT, generateKeyPair, type JWTVerifyGetKey } from "jose";
import { pulseAuth } from "./server.js";

// A local keypair stands in for the auth provider's JWKS, so the test verifies
// the real jose path with zero network / live server.
const { publicKey, privateKey } = await generateKeyPair("ES256");
const getKey: JWTVerifyGetKey = async () => publicKey;

async function token(claims: Record<string, unknown> = {}, sub = "user-1") {
  return new SignJWT(claims)
    .setProtectedHeader({ alg: "ES256" })
    .setSubject(sub)
    .setIssuedAt()
    .setExpirationTime("1h")
    .sign(privateKey);
}

/** Run the produced middleware against a fake request, returning the context it
 *  yields, or the thrown error code. */
async function run(
  authed: ReturnType<typeof pulseAuth>["authed"],
  authorization: string | undefined,
) {
  const mw = authed.middlewares[0]!;
  const headers = new Headers();
  if (authorization) headers.set("authorization", authorization);
  const errors = { UNAUTHORIZED: () => new Error("UNAUTHORIZED") } as never;
  try {
    const res = await mw({
      context: { headers },
      input: {},
      path: ["x"],
      errors,
      next: async (opts) => ({ context: { ...(opts?.context ?? {}) } }),
    });
    return { ctx: res.context as Record<string, unknown> };
  } catch (e) {
    return { error: (e as Error).message };
  }
}

describe("pulseAuth", () => {
  it("verifies a valid JWT and exposes subject + claims + userId", async () => {
    const { authed } = pulseAuth({ getKey });
    const { ctx } = await run(authed, `Bearer ${await token({ email: "a@x.dev" })}`);
    expect((ctx!.auth as { subject: string }).subject).toBe("user-1");
    expect((ctx!.auth as { claims: { email: string } }).claims.email).toBe("a@x.dev");
    expect(ctx!.userId).toBe("user-1"); // default: subject verbatim
  });

  it("maps the identity to an app user id via resolveUserId", async () => {
    const { authed } = pulseAuth({
      getKey,
      resolveUserId: (id) => `users:${id.claims.email as string}`,
    });
    const { ctx } = await run(authed, `Bearer ${await token({ email: "ada@x.dev" })}`);
    expect(ctx!.userId).toBe("users:ada@x.dev");
  });

  it("rejects a missing token", async () => {
    const { authed } = pulseAuth({ getKey });
    expect((await run(authed, undefined)).error).toBe("UNAUTHORIZED");
  });

  it("rejects a malformed / unsigned token", async () => {
    const { authed } = pulseAuth({ getKey });
    expect((await run(authed, "Bearer not-a-jwt")).error).toBe("UNAUTHORIZED");
  });

  it("rejects a token signed by a different key", async () => {
    const other = await generateKeyPair("ES256");
    const evil = await new SignJWT({})
      .setProtectedHeader({ alg: "ES256" })
      .setSubject("attacker")
      .setExpirationTime("1h")
      .sign(other.privateKey);
    const { authed } = pulseAuth({ getKey });
    expect((await run(authed, `Bearer ${evil}`)).error).toBe("UNAUTHORIZED");
  });

  it("rejects an expired token", async () => {
    const expired = await new SignJWT({})
      .setProtectedHeader({ alg: "ES256" })
      .setSubject("user-1")
      .setExpirationTime(1)
      .sign(privateKey);
    const { authed } = pulseAuth({ getKey });
    expect((await run(authed, `Bearer ${expired}`)).error).toBe("UNAUTHORIZED");
  });

  it("enforces issuer/audience when pinned", async () => {
    const t = await new SignJWT({})
      .setProtectedHeader({ alg: "ES256" })
      .setSubject("user-1")
      .setIssuer("https://wrong")
      .setExpirationTime("1h")
      .sign(privateKey);
    const { authed } = pulseAuth({ getKey, issuer: "https://right" });
    expect((await run(authed, `Bearer ${t}`)).error).toBe("UNAUTHORIZED");
  });

  it("throws when neither jwksUrl nor getKey is configured", () => {
    expect(() => pulseAuth({})).toThrow("pulseAuth: provide jwksUrl or getKey");
  });

  it("builds a remote JWKS getKey from jwksUrl without throwing", () => {
    expect(() => pulseAuth({ jwksUrl: "https://provider.example/api/auth/jwks" })).not.toThrow();
  });

  it("rejects a token missing the sub claim", async () => {
    // A validly-signed token with no subject must still be rejected.
    const noSub = await new SignJWT({ email: "a@x.dev" })
      .setProtectedHeader({ alg: "ES256" })
      .setExpirationTime("1h")
      .sign(privateKey);
    const { authed } = pulseAuth({ getKey });
    expect((await run(authed, `Bearer ${noSub}`)).error).toBe("UNAUTHORIZED");
  });

  it("awaits an async resolveUserId", async () => {
    const { authed } = pulseAuth({
      getKey,
      resolveUserId: async (id) => `users:${id.subject}`,
    });
    const { ctx } = await run(authed, `Bearer ${await token({}, "abc")}`);
    expect(ctx!.userId).toBe("users:abc");
  });

  it("propagates an error thrown by resolveUserId", async () => {
    const { authed } = pulseAuth({
      getKey,
      resolveUserId: () => {
        throw new Error("no such user");
      },
    });
    expect((await run(authed, `Bearer ${await token()}`)).error).toBe("no such user");
  });
});
