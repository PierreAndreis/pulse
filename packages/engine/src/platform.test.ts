import { describe, expect, it } from "vitest";
import { detectTarget } from "./platform.js";
import { engineDownloadUrl } from "./index.js";

describe("detectTarget", () => {
  it("maps darwin arm64 + x64", () => {
    expect(detectTarget("darwin", "arm64")).toBe("aarch64-apple-darwin");
    expect(detectTarget("darwin", "x64")).toBe("x86_64-apple-darwin");
  });
  it("maps linux by arch + libc (glibc vs musl/Alpine)", () => {
    // explicit muslHost so the result doesn't depend on the test host's libc
    expect(detectTarget("linux", "arm64", false)).toBe("aarch64-unknown-linux-gnu");
    expect(detectTarget("linux", "arm64", true)).toBe("aarch64-unknown-linux-musl");
    expect(detectTarget("linux", "x64", false)).toBe("x86_64-unknown-linux-gnu");
    expect(detectTarget("linux", "x64", true)).toBe("x86_64-unknown-linux-musl");
  });
  it("returns null for unsupported platforms", () => {
    expect(detectTarget("win32", "x64")).toBeNull();
    expect(detectTarget("darwin", "ia32")).toBe("x86_64-apple-darwin"); // non-arm64 → x64 build
    expect(detectTarget("freebsd", "x64")).toBeNull();
  });
});

describe("engineDownloadUrl", () => {
  it("builds a versioned GitHub Releases URL per target", () => {
    expect(engineDownloadUrl("0.1.0", "aarch64-apple-darwin")).toBe(
      "https://github.com/PierreAndreis/pulse/releases/download/v0.1.0/pulse-server-aarch64-apple-darwin",
    );
  });
});
