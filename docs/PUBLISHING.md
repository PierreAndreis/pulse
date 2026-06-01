# Publishing

Releasing is **fully automated by a git tag**. The only one-time manual setup is
an npm token; after that, `git tag vX.Y.Z && git push --tags` ships everything.

## One-time setup (the only manual piece)

1. Own the **`@onveloz`** scope on npm (create the org/scope while logged in).
2. Create an **automation access token** with publish rights to `@onveloz`.
3. Add it to the GitHub repo as the secret **`NPM_TOKEN`**
   (Settings → Secrets and variables → Actions → New repository secret).

That's it. No other manual step is required to publish.

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
