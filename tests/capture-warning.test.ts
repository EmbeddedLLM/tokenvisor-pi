import { afterAll, describe, expect, test } from "bun:test";
import { spawnSync } from "child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import tokenVisorProvider from "../extensions/tokenvisor-provider.ts";
import { AGENT_HEADER, parseAgentMetadata } from "../src/capture/shared.ts";
import { createCaptureWarningReporter } from "../src/capture/warnings.ts";

const tempRoot = mkdtempSync(join(tmpdir(), "tokenvisor-capture-warning-"));
afterAll(() => rmSync(tempRoot, { recursive: true, force: true }));

describe("capture limitation warnings", () => {
  test("explains sequence gaps for both harnesses while preserving independent warnings and deduplication", () => {
    const reporter = createCaptureWarningReporter();
    const messages: string[] = [];
    const sink = (message: string) => { messages.push(message); };
    const sequenceMessage = "[tokenvisor] Previous-request identity is unavailable for this request. Trace reconstruction may contain a sequence gap; export eligibility is checked separately.";
    for (const session of ["pi:session:warning-wording", "omp:session:warning-wording"]) {
      reporter.report(session, "request-sequence", "native request sequence could not be established", sink);
      reporter.report(session, "request-sequence", "native request sequence could not be established", sink);
      reporter.report(session, "request-dependencies", "request dependency linkage could not be established", sink);
    }
    expect(messages).toEqual([
      sequenceMessage,
      "[tokenvisor] Capture observed request dependency linkage could not be established; request linkage may be incomplete.",
      sequenceMessage,
      "[tokenvisor] Capture observed request dependency linkage could not be established; request linkage may be incomplete.",
    ]);
  });

  test("warns once per session and issue for the process lifetime without disabling the provider", async () => {
    if (process.env.TOKENVISOR_CAPTURE_WARNING_CHILD !== "1") {
      const child = spawnSync(process.execPath, ["test", import.meta.filename], {
        encoding: "utf8",
        timeout: 30_000,
        env: { ...process.env, TOKENVISOR_CAPTURE_WARNING_CHILD: "1" },
      });
      const output = `${child.stdout}\n${child.stderr}`;
      expect(child.error?.message ?? null).toBeNull();
      expect(child.status, output).toBe(0);
      expect(output).toContain("3 pass");
      return;
    }

    const envKeys = ["TOKENVISOR_API_KEY", "TOKENVISOR_BASE_URL", "PI_CODING_AGENT_DIR"] as const;
    const previousEnv = new Map(envKeys.map((key) => [key, process.env[key]]));
    const previousArgv = process.argv;
    const previousFetch = globalThis.fetch;
    const previousWarn = console.warn;
    const previousStreamCalls = (globalThis as any).__tokenvisorCaptureWarningCalls;
    const warnings: string[] = [];
    const hooks = new Map<string, (event: any, ctx: any) => unknown>();
    let providerConfig: any;
    const fakePiRoot = join(tempRoot, "node_modules/@earendil-works/pi-coding-agent");
    mkdirSync(join(fakePiRoot, "dist"), { recursive: true });
    writeFileSync(join(fakePiRoot, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "1.4.0" }));
    writeFileSync(join(fakePiRoot, "dist/cli.js"), "\n");
    writeFileSync(join(fakePiRoot, "dist/index.js"), [
      "export const convertToLlm = (messages) => messages;",
      "export const serializeConversation = (messages) => JSON.stringify(messages);",
      "export const sessionEntryToContextMessages = (entry) => [entry];",
    ].join("\n"));
    const fakePiAiRoot = join(fakePiRoot, "node_modules/@earendil-works/pi-ai");
    mkdirSync(join(fakePiAiRoot, "dist/api"), { recursive: true });
    writeFileSync(join(fakePiAiRoot, "package.json"), JSON.stringify({ name: "@earendil-works/pi-ai" }));
    writeFileSync(join(fakePiAiRoot, "dist/api/openai-completions.js"), [
      "export const streamSimple = (model, context, options) => {",
      "  globalThis.__tokenvisorCaptureWarningCalls.push({ model, context, options });",
      "  return { result: async () => ({ responseId: 'chatcmpl-warning-test', stopReason: 'stop', content: [] }) };",
      "};",
    ].join("\n"));
    (globalThis as any).__tokenvisorCaptureWarningCalls = [];

    process.env.TOKENVISOR_API_KEY = "tv-pat-capture-warning-test";
    process.env.TOKENVISOR_BASE_URL = "https://capture-warning-test.invalid/api/v1";
    process.env.PI_CODING_AGENT_DIR = tempRoot;
    process.argv = [process.execPath, join(fakePiRoot, "dist/cli.js")];
    globalThis.fetch = async () => Response.json({ object: "list", data: [] });
    console.warn = (message?: unknown) => warnings.push(String(message));

    function context(sessionId: string) {
      const sessionFile = join(tempRoot, `${sessionId}.jsonl`);
      writeFileSync(sessionFile, `${JSON.stringify({ type: "session", id: sessionId })}\n`);
      return {
        ui: { notify: () => { throw new Error("notification surface unavailable"); } },
        sessionManager: {
          getSessionId: () => sessionId,
          getSessionFile: () => sessionFile,
          getHeader: () => ({ id: sessionId }),
          getBranch: () => [],
        },
      };
    }

    try {
      await tokenVisorProvider({
        on: (name: string, handler: (event: any, ctx: any) => unknown) => { hooks.set(name, handler); },
        registerCommand: () => undefined,
        registerProvider: (_name: string, config: any) => { providerConfig = config; },
        appendEntry: () => undefined,
      } as never);

      const start = hooks.get("session_start");
      const shutdown = hooks.get("session_shutdown");
      expect(start).toBeDefined();
      expect(shutdown).toBeDefined();
      expect(providerConfig.api).toBe("tokenvisor-openai-completions-capture");

      expect(() => start!({ reason: "startup" }, context("warning-session-a"))).not.toThrow();
      expect(() => start!({ reason: "resume" }, context("warning-session-a"))).not.toThrow();
      expect(() => start!({ reason: "startup" }, context("warning-session-b"))).not.toThrow();
      expect(warnings).toHaveLength(2);
      expect(warnings[0]).toContain("request linkage may be incomplete");
      expect(warnings[0]).toContain("child extension support is unavailable");

      const requestContext = context("warning-session-a");
      const headers: Record<string, string> = {};
      hooks.get("before_provider_headers")!({ headers }, requestContext);
      const originalMessages = [{ role: "user", content: "preserve this request" }];
      const stream = providerConfig.streamSimple(
        { id: "fixture-model", api: "openai-completions", headers: {} },
        { messages: originalMessages },
        { sessionId: "warning-session-a", headers },
      );
      expect((await stream.result()).responseId).toBe("chatcmpl-warning-test");
      const delegated = (globalThis as any).__tokenvisorCaptureWarningCalls[0];
      expect(delegated.context.messages).toEqual(originalMessages);
      expect(delegated.options.headers[AGENT_HEADER]).toBeDefined();
      expect(parseAgentMetadata(delegated.options.headers[AGENT_HEADER])?.session.id)
        .toBe("pi:session:warning-session-a");

      shutdown!({}, context("warning-session-a"));
      start!({ reason: "resume" }, context("warning-session-a"));
      expect(warnings).toHaveLength(2);
    } finally {
      globalThis.fetch = previousFetch;
      process.argv = previousArgv;
      console.warn = previousWarn;
      if (previousStreamCalls === undefined) delete (globalThis as any).__tokenvisorCaptureWarningCalls;
      else (globalThis as any).__tokenvisorCaptureWarningCalls = previousStreamCalls;
      for (const key of envKeys) {
        const value = previousEnv.get(key);
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });

  test("falls back to the host provider when the capture stream SDK is unavailable", async () => {
    if (process.env.TOKENVISOR_CAPTURE_WARNING_MISSING_SDK_CHILD !== "1") {
      const child = spawnSync(process.execPath, ["test", "--test-name-pattern", "falls back to the host provider", import.meta.filename], {
        encoding: "utf8",
        timeout: 30_000,
        env: { ...process.env, TOKENVISOR_CAPTURE_WARNING_MISSING_SDK_CHILD: "1" },
      });
      const output = `${child.stdout}\n${child.stderr}`;
      expect(child.error?.message ?? null).toBeNull();
      expect(child.status, output).toBe(0);
      expect(output).toContain("1 pass");
      return;
    }

    const envKeys = ["TOKENVISOR_API_KEY", "TOKENVISOR_BASE_URL", "PI_CODING_AGENT_DIR"] as const;
    const previousEnv = new Map(envKeys.map((key) => [key, process.env[key]]));
    const previousArgv = process.argv;
    const previousFetch = globalThis.fetch;
    const previousWarn = console.warn;
    const warnings: string[] = [];
    const providers: any[] = [];
    let hookCount = 0;
    const fakePiRoot = join(tempRoot, "missing-sdk/node_modules/@earendil-works/pi-coding-agent");
    mkdirSync(join(fakePiRoot, "dist"), { recursive: true });
    writeFileSync(join(fakePiRoot, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "1.4.0" }));
    writeFileSync(join(fakePiRoot, "dist/cli.js"), "\n");
    writeFileSync(join(fakePiRoot, "dist/index.js"), [
      "export const convertToLlm = (messages) => messages;",
      "export const serializeConversation = (messages) => JSON.stringify(messages);",
      "export const sessionEntryToContextMessages = (entry) => [entry];",
    ].join("\n"));

    process.env.TOKENVISOR_API_KEY = "tv-pat-missing-stream-test";
    process.env.TOKENVISOR_BASE_URL = "https://missing-stream-test.invalid/api/v1";
    process.env.PI_CODING_AGENT_DIR = join(tempRoot, "missing-sdk-agent");
    process.argv = [process.execPath, join(fakePiRoot, "dist/cli.js")];
    globalThis.fetch = async () => Response.json({ object: "list", data: [] });
    console.warn = (message?: unknown) => warnings.push(String(message));

    try {
      for (let index = 0; index < 2; index += 1) {
        await tokenVisorProvider({
          on: () => { hookCount += 1; },
          registerCommand: () => undefined,
          registerProvider: (_name: string, config: any) => { providers.push(config); },
        } as never);
      }

      expect(providers).toHaveLength(2);
      expect(hookCount).toBe(0);
      expect(providers.map((provider) => provider.api)).toEqual(["openai-completions", "openai-completions"]);
      expect(providers.every((provider) => provider.streamSimple === undefined)).toBe(true);
      expect(warnings.filter((message) => message.includes("host capture or OpenAI stream API is unavailable"))).toHaveLength(1);
      expect(warnings[0]).toContain("request linkage may be incomplete");
    } finally {
      globalThis.fetch = previousFetch;
      process.argv = previousArgv;
      console.warn = previousWarn;
      for (const key of envKeys) {
        const value = previousEnv.get(key);
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });
});
