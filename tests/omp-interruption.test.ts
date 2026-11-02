import { describe, expect, mock, test } from "bun:test";
import { spawnSync } from "child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import {
  createOmpSequenceState,
  getOmpRuntimeCaptureState,
  recordOmpSequenceResult,
  recordOmpSequenceStream,
  resolveOmpNativeSequence,
} from "../src/capture/omp.ts";
import { AGENT_HEADER, parseAgentMetadata } from "../src/capture/shared.ts";

if (process.env.TOKENVISOR_OMP_LINKAGE_CHILD === "1") {
  mock.module("@oh-my-pi/pi-ai/providers/openai-completions", () => ({
    streamOpenAICompletions: (model: unknown, context: unknown, options: unknown) => {
      (globalThis as any).__tokenvisorOmpLinkageCalls.push({ model, context, options });
      return { result: () => Promise.resolve((globalThis as any).__tokenvisorOmpLinkageResults.shift()) };
    },
  }));
}

function judgeBranch(extra: Record<string, unknown> = {}) {
  return [
    { type: "message", id: "user-1", parentId: null, message: { role: "user" } },
    {
      type: "model_usage",
      id: "usage-1",
      parentId: "user-1",
      purpose: "auto-thinking",
      role: "judge",
      ...extra,
    },
  ];
}

describe("OMP interrupted judge sequence recovery", () => {
  test("uses an observed partial provider ID for a uniquely bound aborted usage", () => {
    const state = createOmpSequenceState("session-a");
    recordOmpSequenceStream(state, "stream-a", "user-1");
    // The stream settled with an SDK response ID but its output did not succeed.
    recordOmpSequenceResult(state, "stream-a", "chatcmpl-partial", false);

    expect(resolveOmpNativeSequence(judgeBranch({ stopReason: "aborted" }), state, "session-a"))
      .toEqual({ previousResponseId: "chatcmpl-partial", hasPriorHistory: true });
  });

  test("keeps missing, ambiguous, and unobserved automatic-result identities unknown", () => {
    const branch = judgeBranch({ stopReason: "error" });
    const missing = createOmpSequenceState("session-a");
    recordOmpSequenceStream(missing, "stream-a", "user-1");
    recordOmpSequenceResult(missing, "stream-a", null, false);
    expect(resolveOmpNativeSequence(branch, missing, "session-a").previousResponseId).toBeNull();

    const ambiguous = createOmpSequenceState("session-a");
    recordOmpSequenceStream(ambiguous, "stream-a", "user-1");
    recordOmpSequenceResult(ambiguous, "stream-a", "chatcmpl-a", false);
    recordOmpSequenceStream(ambiguous, "stream-b", "user-1");
    recordOmpSequenceResult(ambiguous, "stream-b", "chatcmpl-b", false);
    expect(resolveOmpNativeSequence(branch, ambiguous, "session-a").previousResponseId).toBeNull();

    const unobservedAutomaticResult = createOmpSequenceState("session-a");
    expect(resolveOmpNativeSequence(branch, unobservedAutomaticResult, "session-a").previousResponseId).toBeNull();
    expect(unobservedAutomaticResult.responseByUsageId.get("usage-1")).toBeNull();
  });

  test("does not use a nominally successful stream ID when native usage says it failed", () => {
    const state = createOmpSequenceState("session-a");
    recordOmpSequenceStream(state, "stream-a", "user-1");
    recordOmpSequenceResult(state, "stream-a", "chatcmpl-inconsistent", true);
    expect(resolveOmpNativeSequence(judgeBranch({ stopReason: "error" }), state, "session-a").previousResponseId)
      .toBeNull();
  });

  test("does not bind a failed stream to native usage that does not record failure", () => {
    const state = createOmpSequenceState("session-a");
    recordOmpSequenceStream(state, "stream-a", "user-1");
    recordOmpSequenceResult(state, "stream-a", "chatcmpl-partial", false);
    expect(resolveOmpNativeSequence(judgeBranch(), state, "session-a").previousResponseId).toBeNull();
  });

  test("registered provider preserves a request when blocking dependency metadata is unknown", async () => {
    if (process.env.TOKENVISOR_OMP_LINKAGE_CHILD !== "1") {
      const child = spawnSync(process.execPath, ["test", import.meta.filename], {
        encoding: "utf8",
        timeout: 30_000,
        env: { ...process.env, TOKENVISOR_OMP_LINKAGE_CHILD: "1" },
      });
      const output = `${child.stdout}\n${child.stderr}`;
      expect(child.error?.message ?? null).toBeNull();
      expect(child.status, output).toBe(0);
      expect(output).toContain("registered provider preserves a request when blocking dependency metadata is unknown");
      return;
    }

    const { default: tokenVisorProvider } = await import("../extensions/tokenvisor-provider.ts");
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-omp-unknown-linkage-"));
    const originalArgv = process.argv;
    const originalFetch = globalThis.fetch;
    const originalWarn = console.warn;
    const previousEnv = new Map(["TOKENVISOR_API_KEY", "TOKENVISOR_BASE_URL"].map((key) => [key, process.env[key]]));
    const warnings: string[] = [];
    const sessionId = "omp-unknown-linkage-parent";
    const parentFile = join(root, `${sessionId}.jsonl`);
    const childFile = join(root, sessionId, "agent-job-1.jsonl");
    mkdirSync(join(root, sessionId), { recursive: true });
    writeFileSync(parentFile, `${JSON.stringify({ type: "session", id: sessionId })}\n`);
    writeFileSync(childFile, `${JSON.stringify({ type: "session", id: "omp-unknown-linkage-child", parentSession: parentFile })}\n`);
    const toolCall = {
      type: "message",
      id: "task-call-entry",
      message: {
        role: "assistant",
        responseId: "chatcmpl-task-call",
        content: [{ type: "toolCall", id: "task-call-1", name: "task", arguments: {} }],
      },
    };
    const toolResultMessage = {
      role: "toolResult",
      toolName: "task",
      toolCallId: "task-call-1",
      content: [{ type: "text", text: "task output" }],
      details: {
        results: [
          { id: "agent-job-1", exitCode: 0, output: "task output", aborted: false },
          { id: "agent-job-2", exitCode: 0, output: "second output", aborted: false },
        ],
      },
    };
    const delivery = { type: "message", id: "task-delivery-1", message: toolResultMessage };
    const branch: any[] = [
      toolCall,
      delivery,
      { type: "model_usage", id: "judge-usage-unobserved", parentId: "task-delivery-1", purpose: "auto-thinking", role: "judge" },
    ];
    const hooks = new Map<string, (event: any, ctx: any) => unknown>();
    const eventHandlers = new Map<string, (payload: any) => unknown>();
    let providerConfig: any;
    const context: any = {
      ui: { notify: (message: string) => warnings.push(message) },
      sessionManager: {
        getSessionId: () => sessionId,
        getSessionFile: () => parentFile,
        getHeader: () => ({ id: sessionId }),
        getBranch: () => branch,
      },
    };
    process.argv = [process.execPath, "/tmp/omp"];
    process.env.TOKENVISOR_API_KEY = "tv-pat-omp-linkage-test";
    process.env.TOKENVISOR_BASE_URL = "https://omp-linkage-test.invalid/api/v1";
    globalThis.fetch = async () => Response.json({ object: "list", data: [] });
    console.warn = (message?: unknown) => warnings.push(String(message));
    (globalThis as any).__tokenvisorOmpLinkageCalls = [];
    (globalThis as any).__tokenvisorOmpLinkageResults = [
      { responseId: "chatcmpl-omp-aborted", stopReason: "aborted", content: [] },
      { responseId: "chatcmpl-omp-next", stopReason: "stop", content: [] },
    ];

    try {
      await tokenVisorProvider({
        on: (name: string, handler: (event: any, ctx: any) => unknown) => { hooks.set(name, handler); },
        events: { on: (name: string, handler: (payload: any) => unknown) => { eventHandlers.set(name, handler); return () => undefined; } },
        registerCommand: () => undefined,
        registerProvider: (_name: string, config: any) => { providerConfig = config; },
        appendEntry: () => undefined,
      } as never);

      hooks.get("session_start")!({ reason: "startup" }, context);
      eventHandlers.get("task:subagent:lifecycle")!({
        status: "started",
        id: "agent-job-1",
        parentToolCallId: "task-call-1",
        sessionFile: childFile,
        detached: false,
      });
      const originalMessages = [toolResultMessage];
      const stream = providerConfig.streamSimple(
        { id: "fixture-model", api: "openai-completions" },
        { messages: originalMessages },
        { sessionId },
      );
      expect((await stream.result()).responseId).toBe("chatcmpl-omp-aborted");
      const calls = (globalThis as any).__tokenvisorOmpLinkageCalls;
      const firstMetadata = parseAgentMetadata(calls[0].options.headers[AGENT_HEADER]);
      expect(calls[0].context.messages).toEqual(originalMessages);
      expect(firstMetadata?.depends_on).toBeNull();
      expect(firstMetadata?.sequence.status).toBe("unavailable");
      expect(getOmpRuntimeCaptureState().pendingTaskBindingRetirements.has(sessionId)).toBe(false);
      expect(getOmpRuntimeCaptureState().pendingTaskResultConsumptions.has(sessionId)).toBe(false);

      branch.push({
        type: "model_usage",
        id: "judge-usage-aborted",
        parentId: "judge-usage-unobserved",
        purpose: "auto-thinking",
        role: "judge",
        stopReason: "aborted",
      });
      const nextStream = providerConfig.streamSimple(
        { id: "fixture-model", api: "openai-completions" },
        { messages: originalMessages },
        { sessionId },
      );
      expect((await nextStream.result()).responseId).toBe("chatcmpl-omp-next");
      const nextMetadata = parseAgentMetadata(calls[1].options.headers[AGENT_HEADER]);
      expect(calls[1].context.messages).toEqual(originalMessages);
      expect(nextMetadata?.sequence).toEqual({ status: "linked", previous_response_id: "chatcmpl-omp-aborted" });
      expect(warnings.filter((message) => message.includes("request dependency linkage could not be established"))).toHaveLength(1);
      expect(warnings.filter((message) => message.includes("native request sequence could not be established"))).toHaveLength(1);
    } finally {
      globalThis.fetch = originalFetch;
      process.argv = originalArgv;
      console.warn = originalWarn;
      for (const [key, value] of previousEnv) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
      delete (globalThis as any).__tokenvisorOmpLinkageCalls;
      delete (globalThis as any).__tokenvisorOmpLinkageResults;
      rmSync(root, { recursive: true, force: true });
    }
  });
});
