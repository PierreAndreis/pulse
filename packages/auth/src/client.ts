/**
 * Build the `headers` provider for `createClient`, attaching the current auth
 * token as a bearer on every request. `getToken` is called per request, so it
 * can return a fresh token (e.g. from Better Auth's client session) — return
 * null/undefined when signed out and the request goes unauthenticated.
 *
 * ```ts
 * const pulse = createClient<typeof contract>({
 *   url,
 *   headers: bearerAuth(() => authClient.getSession()?.token ?? null),
 * });
 * ```
 */
export function bearerAuth(
  getToken: () => string | null | undefined | Promise<string | null | undefined>,
): () => Promise<Record<string, string>> {
  return async () => {
    const token = await getToken();
    const headers: Record<string, string> = {};
    if (token) headers.authorization = `Bearer ${token}`;
    return headers;
  };
}
