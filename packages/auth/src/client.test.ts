import { describe, expect, test, vi } from "vitest";
import { bearerAuth } from "./client.js";

describe("bearerAuth", () => {
  test("attaches a lowercase `authorization` bearer header for a sync token", async () => {
    const headers = await bearerAuth(() => "tok")();
    expect(headers).toEqual({ authorization: "Bearer tok" });
  });

  test("returns an empty object when the token is null", async () => {
    expect(await bearerAuth(() => null)()).toEqual({});
  });

  test("returns an empty object when the token is undefined", async () => {
    expect(await bearerAuth(() => undefined)()).toEqual({});
  });

  test("returns an empty object when the token is the empty string", async () => {
    expect(await bearerAuth(() => "")()).toEqual({});
  });

  test("awaits an async token and attaches the bearer header", async () => {
    const headers = await bearerAuth(async () => "tok")();
    expect(headers).toEqual({ authorization: "Bearer tok" });
  });

  test("invokes getToken exactly once per call", async () => {
    const getToken = vi.fn(() => "tok");
    await bearerAuth(getToken)();
    expect(getToken).toHaveBeenCalledTimes(1);

    let count = 0;
    const provider = bearerAuth(() => {
      count += 1;
      return `tok-${count}`;
    });
    expect(await provider()).toEqual({ authorization: "Bearer tok-1" });
    expect(await provider()).toEqual({ authorization: "Bearer tok-2" });
    expect(count).toBe(2);
  });
});
