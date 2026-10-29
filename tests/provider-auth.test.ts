import { describe, expect, test } from "bun:test";
import tokenVisorProvider from "../extensions/tokenvisor-provider.ts";

describe("provider API key registration", () => {
  test("registers the resolved environment key literally", async () => {
    const originalKey = process.env.TOKENVISOR_API_KEY;
    const originalBaseUrl = process.env.TOKENVISOR_BASE_URL;
    const originalArgv = process.argv;
    const originalFetch = globalThis.fetch;
    const resolvedKey = "tv-pat-provider-auth-test-only";
    const baseUrl = "https://tokenvisor-provider-auth-test.invalid/api/v1";
    let registeredProvider: { apiKey?: string; models?: Array<{ id: string }> } | undefined;
    let discoveryAuthorization: string | null = null;

    process.env.TOKENVISOR_API_KEY = resolvedKey;
    process.env.TOKENVISOR_BASE_URL = baseUrl;
    // An unsupported executable selects the extension's existing capture fallback.
    process.argv = [process.execPath, "/tmp/tokenvisor-provider-auth-test-host"];
    globalThis.fetch = async (input, init) => {
      expect(String(input)).toBe(`${baseUrl}/models`);
      discoveryAuthorization = new Headers(init?.headers).get("Authorization");
      return Response.json({
        object: "list",
        data: [{ id: "model-test", name: "Test model", type: "chat", capabilities: [] }],
      });
    };

    try {
      await tokenVisorProvider({
        registerProvider: (_name, config) => {
          registeredProvider = config as typeof registeredProvider;
        },
        registerCommand: () => undefined,
      } as never);

      expect(registeredProvider?.apiKey).toBe(resolvedKey);
      expect(registeredProvider?.models?.map(({ id }) => id)).toEqual(["model-test"]);
      expect(discoveryAuthorization).toBe(`Bearer ${resolvedKey}`);
    } finally {
      globalThis.fetch = originalFetch;
      process.argv = originalArgv;
      if (originalKey === undefined) delete process.env.TOKENVISOR_API_KEY;
      else process.env.TOKENVISOR_API_KEY = originalKey;
      if (originalBaseUrl === undefined) delete process.env.TOKENVISOR_BASE_URL;
      else process.env.TOKENVISOR_BASE_URL = originalBaseUrl;
    }
  });
});
