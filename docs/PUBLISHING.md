# Publishing

Releasing is **fully automated by a git tag**. The only one-time manual setup is
an npm token; after that, `git tag vX.Y.Z && git push --tags` ships everything.

## One-time setup (the only manual pieces)

1. Own the **`@onveloz`** scope on npm (create the org/scope while logged in),
   with **2FA enabled** on that account and on the GitHub account that owns the repo.
2. Create a **Granular Access Token** scoped to **publish on `@onveloz` only**
   (no delete/org-admin rights), from a **dedicated CI npm account**, with the
   shortest workable expiry — and rotate it on a schedule. Avoid classic,
   never-expiring automation tokens.
3. Add it to the GitHub repo as the secret **`NPM_TOKEN`**
   (Settings → Secrets and variables → Actions → New repository secret).
4. Configure the **`production`** environment (Settings → Environments) with
   **required reviewers** — both `publish.yml` and `release.yml` are gated on it,
   so a tag push can't ship without a human approval.
5. Run **`scripts/protect-repo.sh`** once to apply branch + tag protection
   (requires admin on the repo).

> **Better end state — eliminate the static token.** Once provenance is live
> (it is, via `id-token: write` + `--provenance`), configure npm **Trusted
> Publishing (OIDC)** for `repo=PierreAndreis/pulse, workflow=publish.yml,
> environment=production` and **delete `NPM_TOKEN` entirely** — OIDC replaces it,
> so there is no long-lived credential to leak.

After that, `git tag vX.Y.Z && git push --tags` ships everything (subject to the
environment approval).

## Releasing

```bash
# bump versions across packages (keep them in lockstep), commit, then:
git tag v0.1.0
git push origin v0.1.0
```

Pushing the tag triggers two workflows in parallel:

- **`release.yml`** — builds `pulse-server` for every target
  (`aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu`,
  `aarch64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`) and attaches the
  binaries + `SHA256SUMS` to the GitHub Release for that tag.
- **`publish.yml`** — builds each package's `dist` and runs
  `pnpm -r publish --access public`, publishing every non-private
  `@onveloz/pulse-*` package to npm using `NPM_TOKEN`.

`@onveloz/pulse-engine`'s `postinstall` then downloads the matching engine binary
from that Release on the consumer's machine (Prisma-style), or honors a
`PULSE_SERVER_BIN` override / `PULSE_ENGINE_SKIP_DOWNLOAD`.

## What ships where

| Package | Registry | Notes |
| --- | --- | --- |
| `@onveloz/pulse-schema` `-contract` `-server` `-client` `-react` `-runtime-node` | npm | compiled `dist` + `.d.ts` |
| `@onveloz/pulse-cli` | npm | `bin: pulse` |
| `@onveloz/pulse-engine` | npm | postinstall fetches the binary |
| `pulse-server` (Rust binary, per target) | GitHub Releases | fetched by `pulse-engine` |
| `@onveloz/pulse-tsconfig`, `@onveloz/pulse-examples-chat` | — | `private: true`, never published |

## Version bumps

Keep all publishable packages on the same version as the engine, because
`pulse-engine` downloads the engine from the Release tag matching **its own**
version. The tag (`vX.Y.Z`) must equal the package versions.
