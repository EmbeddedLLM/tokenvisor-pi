import { afterEach, describe, expect, test } from "bun:test";
import { spawnSync } from "child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import tokenVisorProvider from "../extensions/tokenvisor-provider.ts";
import { parseAgentMetadata } from "../src/capture/shared.ts";
import {
  makePiLaunchReceiptPendingRecord,
  PI_LAUNCH_RECEIPT_PENDING_ENTRY,
  readPiPendingChildLaunchReceipts,
} from "../src/capture/pi.ts";

const saved = {
  argv: process.argv,
  env: { ...process.env },
  fetch: globalThis.fetch,
};
const roots: string[] = [];

afterEach(() => {
  process.argv = saved.argv;
  globalThis.fetch = saved.fetch;
  for (const key of Object.keys(process.env)) if (!(key in saved.env)) delete process.env[key];
  Object.assign(process.env, saved.env);
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

describe("registered Pi provider SDK retry", () => {
  test(process.env.TOKENVISOR_PI_SDK_RETRY_CHILD === "1"
    ? "retries the same stream with byte-identical capture metadata after pending state changes"
    : "isolates the actual SDK transport test from other provider delegate fixtures", async () => {
    if (process.env.TOKENVISOR_PI_SDK_RETRY_CHILD !== "1") {
      const child = spawnSync(process.execPath, ["test", import.meta.filename], {
        encoding: "utf8",
        timeout: 30_000,
        env: { ...process.env, TOKENVISOR_PI_SDK_RETRY_CHILD: "1" },
      });
      const output = `${child.stdout}\n${child.stderr}`;
      expect(child.error?.message ?? null).toBeNull();
      expect(child.status, output).toBe(0);
      expect(output).toContain("1 pass");
      return;
    }

    const hostCli = join(import.meta.dir, "..", "node_modules", "@earendil-works", "pi-coding-agent", "dist", "bundle", "cli.js");
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-sdk-retry-"));
    roots.push(root);
    const sessionId = "retry-root-session";
    const sessionFile = join(root, "root.jsonl");
    const header = { type: "session", id: sessionId };
    const receiptOne = {
      parent_response_id: "retry-parent-response-1",
      tool_call_id: "retry-tool-call-1",
      child_session_id: "pi:session:retry-child-1",
      run_id: "retry-run-1",
      last_response_id: "retry-child-response-1",
    };
    const receiptTwo = {
      parent_response_id: "retry-parent-response-2",
      tool_call_id: "retry-tool-call-2",
      child_session_id: "pi:session:retry-child-2",
      run_id: "retry-run-2",
      last_response_id: "retry-child-response-2",
    };
    const firstPending = makePiLaunchReceiptPendingRecord([receiptOne]);
    const secondPending = makePiLaunchReceiptPendingRecord([receiptTwo]);
    expect(firstPending).not.toBeNull();
    expect(secondPending).not.toBeNull();

    const branch: any[] = [{ type: "custom", customType: PI_LAUNCH_RECEIPT_PENDING_ENTRY, data: firstPending }];
    const hooks = new Map<string, (event: any, ctx: any) => unknown>();
    let providerConfig: any;
    const context = { sessionManager: {
      getSessionId: () => sessionId,
      getSessionFile: () => sessionFile,
      getHeader: () => header,
      getBranch: () => branch,
    } };
    const oldBaseUrl = process.env.TOKENVISOR_BASE_URL;
    process.argv = [process.execPath, hostCli];
    process.env.TOKENVISOR_API_KEY = "synthetic-retry-key";
    process.env.TOKENVISOR_BASE_URL = "http://tokenvisor-retry.invalid/api/v1";
    globalThis.fetch = async () => Response.json({ data: [{ id: "retry-model", type: "chat", capabilities: [] }] });

    await tokenVisorProvider({
      on: (name: string, handler: any) => hooks.set(name, handler),
      events: { on: () => () => {}, emit: () => {} },
      registerCommand: () => {},
      registerProvider: (_name: string, config: any) => { providerConfig = config; },
      appendEntry: () => { throw new Error("no native entry expected in the retry fixture"); },
    } as any);

    const requestHeaders: Array<{ capture: string | null; streamId: string | undefined; body: string }> = [];
    let requestCount = 0;
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(request) {
        requestCount += 1;
        return request.text().then((body) => {
          const headers = new Headers(request.headers);
          const capture = headers.get("x-tokenvisor-agent");
          requestHeaders.push({ capture, streamId: parseAgentMetadata(capture)?.stream_id, body });
          if (requestCount === 1) {
            branch.push({ type: "custom", customType: PI_LAUNCH_RECEIPT_PENDING_ENTRY, data: secondPending });
            hooks.get("session_start")!({ reason: "startup" }, context);
            return new Response("temporary unavailable", {
              status: 503,
              headers: { "retry-after-ms": "0", "content-type": "text/plain" },
            });
          }
          const model = "retry-model";
          const chunks = [
            { id: "retry-completion", object: "chat.completion.chunk", created: 1, model, choices: [{ index: 0, delta: { content: "retried" }, finish_reason: null }] },
            { id: "retry-completion", object: "chat.completion.chunk", created: 1, model, choices: [{ index: 0, delta: {}, finish_reason: "stop" }] },
          ].map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n";
          return new Response(chunks, { headers: { "content-type": "text/event-stream" } });
        });
      },
    });

    try {
      mkdirSync(root, { recursive: true });
      writeFileSync(sessionFile, JSON.stringify({ type: "session", id: sessionId }) + "\n");
      hooks.get("session_start")!({ reason: "startup" }, context);
      const headers = new Headers();
      hooks.get("before_provider_headers")!({ headers }, context);
      const initialMetadata = parseAgentMetadata(headers.get("x-tokenvisor-agent"));
      expect(initialMetadata?.child_launches).toEqual([receiptOne]);
      const stream = providerConfig.streamSimple(
        { ...providerConfig.models[0], provider: "tokenvisor", api: "openai-completions", baseUrl: `http://127.0.0.1:${server.port}/v1`, headers },
        { messages: [{ role: "user", content: [{ type: "text", text: "retry test" }] }] },
        { headers, apiKey: "synthetic-retry-key", fetch: saved.fetch, maxRetries: 1, maxRetryDelayMs: 1000 },
      );
      const result = await stream.result();

      expect(result.responseId).toBe("retry-completion");
      expect(requestCount).toBe(2);
      expect(requestHeaders).toHaveLength(2);
      expect(requestHeaders[0]!.capture).toBeTruthy();
      expect(requestHeaders[1]!.capture).toBe(requestHeaders[0]!.capture);
      expect(requestHeaders[1]!.streamId).toBe(requestHeaders[0]!.streamId);
      expect(requestHeaders[0]!.streamId).toBe(initialMetadata?.stream_id);
      expect(parseAgentMetadata(requestHeaders[0]!.capture)?.child_launches).toEqual([receiptOne]);
      expect(requestHeaders[1]!.body).toBe(requestHeaders[0]!.body);
      expect(readPiPendingChildLaunchReceipts(context as any)).toEqual([receiptOne, receiptTwo]);
      expect(parseAgentMetadata(headers.get("x-tokenvisor-agent"))?.stream_id).toBe(initialMetadata?.stream_id);
      const freshHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: freshHeaders }, context);
      const freshMetadata = parseAgentMetadata(freshHeaders.get("x-tokenvisor-agent"));
      expect(freshMetadata?.child_launches).toEqual([receiptOne, receiptTwo]);
      expect(freshMetadata?.stream_id).not.toBe(initialMetadata?.stream_id);
    } finally {
      server.stop(true);
      hooks.get("session_shutdown")!({}, context);
      if (oldBaseUrl === undefined) delete process.env.TOKENVISOR_BASE_URL;
      else process.env.TOKENVISOR_BASE_URL = oldBaseUrl;
    }
  }, 45_000);
});
