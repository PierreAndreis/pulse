import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cacheDir, downloadUrl, ensureEngine } from "./index.js";

describe("downloadUrl", () => {
  it("builds a versioned GitHub Releases URL per target", () => {
    expect(downloadUrl("0.1.0", "aarch64-apple-darwin")).toBe(
      "https://github.com/PierreAndreis/pulse/releases/download/v0.1.0/pulse-server-aarch64-apple-darwin",
    );
    expect(downloadUrl("9.9.9", "x86_64-unknown-linux-musl")).toBe(
      "https://github.com/PierreAndreis/pulse/releases/download/v9.9.9/pulse-server-x86_64-unknown-linux-musl",
    );
  });
});

describe("cacheDir", () => {
  it("honors PULSE_ENGINE_CACHE and appends the version segment", () => {
    const prev = process.env.PULSE_ENGINE_CACHE;
    process.env.PULSE_ENGINE_CACHE = "/tmp/some-cache";
    try {
      expect(cacheDir("1.2.3")).toBe(join("/tmp/some-cache", "1.2.3"));
    } finally {
      if (prev === undefined) delete process.env.PULSE_ENGINE_CACHE;
      else process.env.PULSE_ENGINE_CACHE = prev;
    }
  });
});

describe("ensureEngine download guards", () => {
  // Run on a host whose target resolves (the repo's CI/dev hosts are darwin/linux).
  // We point the cache at a fresh temp dir so existsSync(dest) is false and no
  // previously-cached binary short-circuits the download path, and we stub the
  // global fetch so nothing ever hits the network.
  let cache: string;
  const savedEnv: Record<string, string | undefined> = {};
  const ENV_KEYS = [
    "PULSE_SERVER_BIN",
    "PULSE_ENGINE_CACHE",
    "PULSE_ENGINE_SKIP_DOWNLOAD",
  ] as const;

  beforeEach(() => {
    for (const k of ENV_KEYS) savedEnv[k] = process.env[k];
    delete process.env.PULSE_SERVER_BIN;
    delete process.env.PULSE_ENGINE_SKIP_DOWNLOAD;
    cache = mkdtempSync(join(tmpdir(), "pulse-engine-test-"));
    process.env.PULSE_ENGINE_CACHE = cache;
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    for (const k of ENV_KEYS) {
      if (savedEnv[k] === undefined) delete process.env[k];
      else process.env[k] = savedEnv[k];
    }
    rmSync(cache, { recursive: true, force: true });
  });

  it("throws a clear error when the binary download responds not-ok", async () => {
    const fetchMock = vi.fn(async () => new Response(null, { status: 404 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(ensureEngine()).rejects.toThrow(/failed to download/);
    await expect(ensureEngine()).rejects.toThrow(/HTTP 404/);
    // one fetch (the binary download) per ensureEngine() call; no SHA256SUMS lookup
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("refuses to install on a checksum mismatch", async () => {
    const bytes = Buffer.from("not-the-real-binary");
    const wrongChecksum = createHash("sha256").update("different-bytes").digest("hex");

    // 1st fetch: the binary download (ok). 2nd fetch: the release SHA256SUMS,
    // returning a checksum that cannot match `bytes` -> mismatch guard fires.
    const fetchMock = vi.fn(async (input: string | URL) => {
      const u = String(input);
      if (u.endsWith("SHA256SUMS")) {
        // body lines are "<sha>  <asset-name>"; cover every supported target so
        // the lookup succeeds regardless of the test host's resolved triple.
        const triples = [
          "aarch64-apple-darwin",
          "x86_64-apple-darwin",
          "x86_64-unknown-linux-gnu",
          "x86_64-unknown-linux-musl",
          "aarch64-unknown-linux-gnu",
          "aarch64-unknown-linux-musl",
        ];
        const body = triples.map((t) => `${wrongChecksum}  pulse-server-${t}`).join("\n");
        return new Response(body, { status: 200 });
      }
      return new Response(bytes, { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(ensureEngine()).rejects.toThrow(/checksum mismatch/);
    // download + SHA256SUMS lookup
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("does not download when opts.download === false", async () => {
    const fetchMock = vi.fn(async () => new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(ensureEngine({ download: false })).resolves.toBeNull();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
