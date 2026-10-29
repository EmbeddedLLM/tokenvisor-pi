import { describe, expect, test } from "bun:test";
import { providerApiKey } from "../extensions/tokenvisor-provider.ts";

describe("provider API key registration", () => {
  test("passes an available resolved key literally for both host resolvers", () => {
    const resolvedKey = "tv-pat-test-only";

    expect(providerApiKey(resolvedKey)).toBe(resolvedKey);
  });

  test("keeps the provider registered when no credential is available yet", () => {
    expect(providerApiKey(undefined)).toBe("TOKENVISOR_API_KEY");
  });
});
