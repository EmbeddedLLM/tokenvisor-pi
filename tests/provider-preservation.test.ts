import { afterEach, describe, expect, test } from "bun:test";
import { spawnSync } from "child_process";
import { randomUUID } from "crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import tokenVisorProvider from "../extensions/tokenvisor-provider.ts";
import { AGENT_HEADER, parseAgentMetadata } from "../src/capture/shared.ts";

const roots: string[] = [];
const generatedPackages: string[] = [];
const savedArgv = process.argv;
const savedEnv = { ...process.env };
const savedFetch = globalThis.fetch;

afterEach(() => {
  process.argv = savedArgv;
  globalThis.fetch = savedFetch;
  for (const key of Object.keys(process.env)) if (!(key in savedEnv)) delete process.env[key];
  Object.assign(process.env, savedEnv);
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
  for (const packagePath of generatedPackages.splice(0)) rmSync(packagePath, { recursive: true, force: true });
  delete (globalThis as any).__TOKENVISOR_PROVIDER_CONTRACT_CAPTURE__;
});

const deepFreeze = <T>(value: T): T => {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
};

describe("registered provider input preservation", () => {
  if (process.env.TOKENVISOR_PROVIDER_CONTRACT_CHILD !== "1") {
    test("isolates provider delegate fixtures from other test files", () => {
      const child = spawnSync(process.execPath, ["test", import.meta.filename], {
        encoding: "utf8",
        timeout: 30_000,
        env: { ...process.env, TOKENVISOR_PROVIDER_CONTRACT_CHILD: "1" },
      });
      const output = `${child.stdout}\n${child.stderr}`;
      expect(child.error?.message ?? null).toBeNull();
      expect(child.status, output).toBe(0);
      expect(output).toContain("2 pass");
      return;
    });
  } else for (const host of ["pi", "omp"] as const) {
    test(`preserves delegated messages, tool schemas, model settings, and generation options under ${host}`, async () => {
      const root = mkdtempSync(join(tmpdir(), `tokenvisor-${host}-provider-contract-`));
      roots.push(root);
      const sessionId = `provider-contract-${randomUUID()}`;
      const sessionFile = join(root, `${sessionId}.jsonl`);
      const nativeHeader = { type: "session", id: sessionId };
      writeFileSync(sessionFile, `${JSON.stringify(nativeHeader)}\n`);
      const delegateDir = host === "pi"
        ? join(root, "node_modules", "@earendil-works", "pi-coding-agent")
        : join(root, "node_modules", "@oh-my-pi", "pi-coding-agent");
      const aiDir = host === "pi"
        ? join(root, "node_modules", "@earendil-works", "pi-ai")
        : join(root, "node_modules", "@oh-my-pi", "pi-ai");
      mkdirSync(join(aiDir, "dist", "api"), { recursive: true });

      let captured: { model: any; context: any; options: any } | undefined;
      const source = `export function ${host === "pi" ? "streamSimple" : "streamOpenAICompletions"}(model, context, options) {
        globalThis.__TOKENVISOR_PROVIDER_CONTRACT_CAPTURE__({ model, context, options });
        return { result: () => Promise.resolve({ responseId: "provider-contract-response", stopReason: "stop", content: [] }) };
      }`;

      if (host === "pi") {
        mkdirSync(join(delegateDir, "dist", "bundle"), { recursive: true });
        mkdirSync(join(aiDir, "dist", "api"), { recursive: true });
        writeFileSync(join(delegateDir, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "1.4.0" }));
        writeFileSync(join(delegateDir, "dist", "bundle", "cli.js"), "");
        writeFileSync(join(aiDir, "package.json"), JSON.stringify({ name: "@earendil-works/pi-ai", version: "1.4.0", type: "module" }));
        writeFileSync(join(aiDir, "dist", "api", "openai-completions.js"), source);
        process.env.PI_CODING_AGENT_DIR = root;
        process.argv = [process.execPath, join(delegateDir, "dist", "bundle", "cli.js")];
      } else {
        const packageRoot = resolve("node_modules", "@oh-my-pi", "pi-ai");
        if (await Bun.file(join(packageRoot, "package.json")).exists()) {
          throw new Error("Unexpected installed @oh-my-pi/pi-ai package in producer test environment");
        }
        generatedPackages.push(packageRoot);
        const providerDir = join(packageRoot, "providers");
        mkdirSync(providerDir, { recursive: true });
        writeFileSync(join(packageRoot, "package.json"), JSON.stringify({ name: "@oh-my-pi/pi-ai", version: "18.12.6", type: "module", exports: { "./providers/openai-completions": "./providers/openai-completions.js" } }));
        writeFileSync(join(providerDir, "openai-completions.js"), source);
        process.argv = [process.execPath, join(root, "omp")];
      }

      (globalThis as any).__TOKENVISOR_PROVIDER_CONTRACT_CAPTURE__ = (args: typeof captured) => { captured = args; };
      process.env.TOKENVISOR_API_KEY = "synthetic-provider-contract-key";
      process.env.TOKENVISOR_BASE_URL = "http://tokenvisor-provider-contract.invalid/api/v1";
      globalThis.fetch = async () => Response.json({ data: [{ id: "contract-model", type: "chat", capabilities: [] }] });

      let providerConfig: any;
      const hooks = new Map<string, (event: any, ctx: any) => unknown>();
      await tokenVisorProvider({
        on: (name: string, handler: (event: any, ctx: any) => unknown) => { hooks.set(name, handler); },
        events: { on: () => () => {}, emit: () => {} },
        registerCommand: () => {},
        registerProvider: (_name: string, config: any) => { providerConfig = config; },
        appendEntry: () => {},
      } as any);

      const contextManager = {
        getSessionId: () => sessionId,
        getSessionFile: () => sessionFile,
        getHeader: () => nativeHeader,
        getBranch: () => [],
      };
      const hostContext = { sessionManager: contextManager, agent: { kind: "main" } };
      hooks.get("session_start")!({ reason: "startup" }, hostContext);
      const requestHeaders: Record<string, string> = { "x-options-setting": "preserve" };
      if (host === "pi") hooks.get("before_provider_headers")!({ headers: requestHeaders }, hostContext);
      const expectedMetadata = requestHeaders[AGENT_HEADER];
      if (host === "pi") expect(parseAgentMetadata(expectedMetadata)?.session.id).toBe(`pi:session:${sessionId}`);

      const context = deepFreeze({
        messages: [
          { role: "user", content: [{ type: "text", text: "Inspect this tool result." }] },
          { role: "assistant", content: [{ type: "toolCall", id: "call-preserve-1", name: "lookup", arguments: { key: "alpha" } }] },
          { role: "toolResult", toolCallId: "call-preserve-1", toolName: "lookup", content: [{ type: "text", text: "tool output must survive" }], isError: false },
        ],
        tools: [{ name: "lookup", description: "Look up a value", parameters: { type: "object", properties: { key: { type: "string" } }, required: ["key"], additionalProperties: false } }],
        systemPrompt: "Keep tool semantics intact.",
      });
      const model = deepFreeze({
        ...providerConfig.models[0], provider: "tokenvisor", api: "tokenvisor-openai-completions-capture",
        baseUrl: "http://provider.invalid/v1", headers: { "x-provider-setting": "preserve", "x-tokenvisor-agent": "stale-metadata" },
        reasoning: true, input: ["text", "image"], contextWindow: 65536, maxTokens: 4096,
      });
      const options = deepFreeze({
        headers: requestHeaders,
        apiKey: "literal-key", temperature: 0.37, maxTokens: 271, reasoning: { effort: "high" },
        signal: new AbortController().signal, maxRetries: 0, ...(host === "omp" ? { sessionId } : {}),
      });
      const contextSnapshot = structuredClone(context);
      const modelSnapshot = structuredClone(model);
      const { signal, ...cloneableOptions } = options;
      const optionsSnapshot = structuredClone(cloneableOptions);

      const stream = providerConfig.streamSimple(model, context, options);
      await stream.result();

      expect(captured).toBeDefined();
      expect(captured!.context).toEqual(contextSnapshot);
      expect(captured!.model).toEqual({ ...modelSnapshot, api: "openai-completions", headers: { "x-provider-setting": "preserve" } });
      const delegatedAgentMetadata = captured!.options.headers[AGENT_HEADER];
      const delegatedMetadata = parseAgentMetadata(delegatedAgentMetadata);
      expect(delegatedMetadata?.session.id).toBe(`${host}:session:${sessionId}`);
      expect(delegatedMetadata?.stream_id).toBeTruthy();
      if (host === "pi") expect(delegatedAgentMetadata).toBe(expectedMetadata);
      expect(captured!.options).toEqual({
        ...optionsSnapshot,
        headers: { ...optionsSnapshot.headers, [AGENT_HEADER]: delegatedAgentMetadata },
        signal,
      });
      expect(context).toEqual(contextSnapshot);
      expect(model).toEqual(modelSnapshot);
      expect(options).toEqual({ ...optionsSnapshot, signal });
      delete (globalThis as any).__TOKENVISOR_PROVIDER_CONTRACT_CAPTURE__;
    });
  }
});
