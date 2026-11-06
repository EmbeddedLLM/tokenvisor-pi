import { afterAll, expect, test } from "bun:test";
import { spawnSync } from "child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import tokenVisorProvider from "../extensions/tokenvisor-provider.ts";
import { AGENT_HEADER, parseAgentMetadata } from "../src/capture/shared.ts";
import { makePiLaunchReceiptPendingRecord, PI_LAUNCH_RECEIPT_PENDING_ENTRY } from "../src/capture/pi.ts";

const roots: string[] = [];
afterAll(() => { for (const root of roots) rmSync(root, { recursive: true, force: true }); });

test("isolates metadata at the physical provider boundary and preserves request-local context", async () => {
  /** Guards external-provider privacy, routed models, pending receipts and single-use request identity. */
  if (process.env.TOKENVISOR_ISOLATION_CHILD !== "1") {
    const child = spawnSync(process.execPath, ["test", import.meta.filename], {
      encoding: "utf8", timeout: 30_000,
      env: { ...process.env, TOKENVISOR_ISOLATION_CHILD: "1" },
    });
    expect(child.error?.message ?? null).toBeNull();
    expect(child.status, `${child.stdout}\n${child.stderr}`).toBe(0);
    expect(`${child.stdout}\n${child.stderr}`).toContain("1 pass");
    return;
  }

  const root = mkdtempSync(join(tmpdir(), "tokenvisor-provider-isolation-"));
  roots.push(root);
  const host = join(root, "node_modules/@earendil-works/pi-coding-agent");
  const ai = join(root, "node_modules/@earendil-works/pi-ai");
  mkdirSync(join(host, "dist"), { recursive: true });
  mkdirSync(join(ai, "dist/api"), { recursive: true });
  writeFileSync(join(host, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "1.4.0" }));
  writeFileSync(join(host, "dist/cli.js"), "\n");
  writeFileSync(join(ai, "package.json"), JSON.stringify({ name: "@earendil-works/pi-ai", type: "module" }));
  writeFileSync(join(ai, "dist/api/openai-completions.js"), `export const streamSimple = (model, context, options) => {
    globalThis.__TOKENVISOR_ISOLATION_CAPTURE__ = { model, context, options };
    return { result: async () => ({ responseId: "isolation-response", stopReason: "stop", content: [] }) };
  };`);
  const saved = { argv: process.argv, fetch: globalThis.fetch, env: { ...process.env } };
  try {
    process.argv = [process.execPath, join(host, "dist/cli.js")];
    process.env.PI_CODING_AGENT_DIR = root;
    process.env.TOKENVISOR_API_KEY = "synthetic-isolation-key";
    process.env.TOKENVISOR_BASE_URL = "http://isolation.invalid/api/v1";
    globalThis.fetch = async () => Response.json({ data: [] });
    const file = join(root, "session.jsonl");
    const nativeHeader = { type: "session", id: "isolation-session" };
    writeFileSync(file, `${JSON.stringify(nativeHeader)}\n`);
    const receipt = {
      parent_response_id: "launch-response", tool_call_id: "launch-tool",
      child_session_id: "pi:session:child", run_id: "child-run", last_response_id: "child-response",
    };
    const branch: any[] = [{ type: "custom", customType: PI_LAUNCH_RECEIPT_PENDING_ENTRY, data: makePiLaunchReceiptPendingRecord([receipt]) }];
    const ctx: any = { model: { provider: "TokenVisor" }, sessionManager: {
      getSessionId: () => nativeHeader.id, getHeader: () => nativeHeader,
      getSessionFile: () => file, getBranch: () => branch,
    } };
    const hooks = new Map<string, Function>();
    let config: any;
    await tokenVisorProvider({
      on: (name: string, handler: Function) => hooks.set(name, handler),
      registerCommand: () => {}, registerProvider: (_name: string, value: any) => { config = value; }, appendEntry: () => {},
    } as any);
    hooks.get("session_start")!({ reason: "resume" }, ctx);
    const { createModels } = await import("../node_modules/@earendil-works/pi-ai/dist/models.js");
    const nativeRuntime = createModels() as any;
    nativeRuntime.requireProvider = () => ({});
    nativeRuntime.getAuth = async () => ({ auth: { apiKey: "synthetic-native-auth" } });
    async function prepare(provider: string) {
      return nativeRuntime.applyAuth({ provider, id: "isolation-model", api: "openai-completions" }, {
        headers: { "x-preserve": "unchanged" },
        transformHeaders: async (headers: any) => {
          hooks.get("before_provider_headers")!({ headers }, ctx);
          expect(headers).toEqual({ "x-preserve": "unchanged" });
          return headers;
        },
      });
    }
    // A TokenVisor selection may route elsewhere; foreign requests must not allocate receipt carriers.
    for (let index = 0; index < 20; index++) {
      const request = await prepare(index % 2 ? "openai" : "anthropic");
      expect(new Headers(request.requestOptions.headers).get(AGENT_HEADER)).toBeNull();
    }
    // The converse route must still capture even when the selected provider is foreign.
    ctx.model = { provider: "anthropic" };
    const request = await prepare("TokenVisor");
    branch.push({ type: "message", id: "later-entry", message: { role: "assistant", responseId: "later-response", content: [] } });
    const messages = [{ role: "user", content: "Preserve this input." }];
    await config.streamSimple(request.requestModel, { messages }, request.requestOptions).result();
    const captured = (globalThis as any).__TOKENVISOR_ISOLATION_CAPTURE__;
    const metadata = parseAgentMetadata(new Headers(captured.options.headers).get(AGENT_HEADER));
    expect(metadata?.session.id).toBe("pi:session:isolation-session");
    expect(metadata?.sequence).toEqual({ status: "start", previous_response_id: null });
    expect(metadata?.child_launches).toEqual([receipt]);
    expect(captured.context.messages).toEqual(messages);
    expect(captured.options.headers["x-preserve"]).toBe("unchanged");
    expect(new Headers(request.requestOptions.headers).get(AGENT_HEADER)).toBeNull();
    await config.streamSimple(request.requestModel, { messages }, request.requestOptions).result();
    expect(new Headers((globalThis as any).__TOKENVISOR_ISOLATION_CAPTURE__.options.headers).get(AGENT_HEADER)).toBeNull();
    const beforeShutdown = await prepare("TokenVisor");
    hooks.get("session_shutdown")!({}, ctx);
    hooks.get("session_start")!({ reason: "resume" }, ctx);
    await config.streamSimple(beforeShutdown.requestModel, { messages }, beforeShutdown.requestOptions).result();
    expect(new Headers((globalThis as any).__TOKENVISOR_ISOLATION_CAPTURE__.options.headers).get(AGENT_HEADER)).toBeNull();
    const resumed = await prepare("TokenVisor");
    await config.streamSimple(resumed.requestModel, { messages }, resumed.requestOptions).result();
    expect(parseAgentMetadata(new Headers((globalThis as any).__TOKENVISOR_ISOLATION_CAPTURE__.options.headers).get(AGENT_HEADER))?.session.id).toBe("pi:session:isolation-session");
  } finally {
    process.argv = saved.argv;
    globalThis.fetch = saved.fetch;
    for (const key of Object.keys(process.env)) if (!(key in saved.env)) delete process.env[key];
    Object.assign(process.env, saved.env);
    delete (globalThis as any).__TOKENVISOR_ISOLATION_CAPTURE__;
  }
});
