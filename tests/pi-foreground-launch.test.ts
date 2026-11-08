import { afterEach, describe, expect, mock, test } from "bun:test";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import tokenVisorProvider from "../extensions/tokenvisor-provider.ts";
import { PI_AI_DELEGATE_SOURCE, resetPiAiDelegate } from "./helpers/pi-ai-delegate.ts";
import { parseAgentMetadata } from "../src/capture/shared.ts";
import {
  makePiCompactionRecord,
  PI_COMPACTION_ENTRY,
  PI_RUN_COMPLETION_ENTRY,
  PI_RESULT_CONSUMPTION_ENTRY,
  readPiForegroundLaunchEvidence,
  readPiPendingChildLaunchReceipts,
} from "../src/capture/pi.ts";
import {
  clearOmpEarlyTaskBindingsForSession,
  clearOmpTaskBindingRetirementsForSession,
  getOmpRuntimeCaptureState,
  OMP_RUN_COMPLETION_ENTRY,
  readOmpEarlyTaskDelegation,
} from "../src/capture/omp.ts";

mock.module("@oh-my-pi/pi-ai/providers/openai-completions", () => ({
  streamOpenAICompletions: (model: unknown, context: unknown, options: unknown) => {
    (globalThis as any).__OMP_PROVIDER_OPTIONS__ = options;
    (globalThis as any).__OMP_PROVIDER_REQUESTS__ ??= [];
    (globalThis as any).__OMP_PROVIDER_REQUESTS__.push({ model, context, options });
    return { result: () => Promise.resolve((globalThis as any).__OMP_PROVIDER_RESULT__ ?? { responseId: "omp-response", stopReason: "stop", content: [] }) };
  },
}));

const savedEnv = { ...process.env };
const savedArgv = process.argv;
const savedFetch = globalThis.fetch;
const tempRoots: string[] = [];
const backgroundPreviewFixture = JSON.parse(readFileSync(
  new URL("./fixtures/pi-native-background-preview-v1.json", import.meta.url), "utf8",
)) as any;

afterEach(() => {
  resetPiAiDelegate();
  globalThis.fetch = savedFetch;
  (globalThis as any).__OMP_PROVIDER_OPTIONS__ = undefined;
  (globalThis as any).__OMP_PROVIDER_REQUESTS__ = undefined;
  (globalThis as any).__OMP_PROVIDER_RESULT__ = undefined;
  for (const key of Object.keys(process.env)) if (!(key in savedEnv)) delete process.env[key];
  Object.assign(process.env, savedEnv);
  process.argv = savedArgv;
  for (const root of tempRoots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function writeSession(file: string, header: Record<string, unknown>, entries: any[]): void {
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, [header, ...entries].map((entry) => JSON.stringify(entry)).join("\n") + "\n");
}

async function createHarness(subagentsVersion = "0.83.0") {
  const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-foreground-test-"));
  tempRoots.push(root);
  const host = join(root, "node_modules", "@earendil-works", "pi-coding-agent");
  const ai = join(root, "node_modules", "@earendil-works", "pi-ai");
  const subagents = join(root, "npm", "node_modules", "pi-subagents");
  for (const path of [join(host, "dist"), join(ai, "dist", "api"), join(subagents, "bundle"), join(subagents, "src", "runs", "background")]) {
    mkdirSync(path, { recursive: true });
  }
  writeFileSync(join(host, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "1.4.0" }));
  writeFileSync(join(host, "dist", "cli.js"), "");
  writeFileSync(join(ai, "package.json"), JSON.stringify({ name: "@earendil-works/pi-ai", version: "1.4.0", type: "module" }));
  writeFileSync(join(ai, "dist", "api", "openai-completions.js"),
    PI_AI_DELEGATE_SOURCE);
  writeFileSync(join(subagents, "package.json"), JSON.stringify({
    name: "pi-subagents", version: subagentsVersion, type: "module",
    exports: { "./required-child-extensions": "./bundle/required-child-extensions.js" },
  }));
  writeFileSync(join(subagents, "bundle", "required-child-extensions.js"), "export function registerRequiredChildExtensions() { return { dispose() {} }; }");
  writeFileSync(join(subagents, "src", "runs", "background", "subagent-runner-bootstrap.js"), "");

  process.env.PI_CODING_AGENT_DIR = root;
  process.env.TOKENVISOR_API_KEY = "synthetic-key";
  process.env.TOKENVISOR_BASE_URL = "http://127.0.0.1:1/api/v1";
  delete process.env.PI_SUBAGENT_CHILD;
  delete process.env.PI_SUBAGENT_RUNNER_PID;
  process.argv = [process.execPath, join(host, "dist", "cli.js")];
  globalThis.fetch = async () => Response.json({ data: [{ id: "synthetic", type: "chat", capabilities: [] }] });

  const hooks = new Map<string, (event: any, ctx: any) => unknown>();
  let providerConfig: any;
  const branches = new Map<string, any[]>();
  const files = new Map<string, string>();
  const headers = new Map<string, any>();
  const failAppendTypes = new Set<string>();
  const sessionContext = (sessionId: string) => ({ sessionManager: {
    getSessionId: () => sessionId,
    getSessionFile: () => files.get(sessionId),
    getHeader: () => headers.get(sessionId),
    getBranch: () => branches.get(sessionId) ?? [],
  } });
  const appended: any[] = [];
  await tokenVisorProvider({
    on: (name: string, handler: any) => hooks.set(name, handler),
    events: { on: () => () => {}, emit: () => {} },
    registerCommand: () => {},
    registerProvider: (_name: string, config: any) => { providerConfig = config; },
    appendEntry: (customType: string, data: any) => {
      if (failAppendTypes.has(customType)) throw new Error("synthetic append failure");
      const sessionId = currentSession;
      if (!sessionId) throw new Error("missing current test session");
      const branch = branches.get(sessionId)!;
      const entry = { type: "custom_message", id: `custom-${appended.length}`, parentId: branch.at(-1)?.id, customType, data };
      appended.push(entry);
      branch.push(entry);
    },
  } as any);
  let currentSession: string | null = null;
  const appendFor = (sessionId: string) => { currentSession = sessionId; };
  return { root, hooks, providerConfig, branches, files, headers, sessionContext, appended, appendFor, failAppendTypes };
}

async function dispatchPiMetadata(
  harness: Awaited<ReturnType<typeof createHarness>>,
  headers: Headers,
  messages: any[],
  responseId: string,
  stopReason = "stop",
) {
  (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId, stopReason, content: [] };

  await harness.providerConfig.streamSimple(
    { id: "synthetic", api: "openai-completions", headers },
    { messages },
    { headers },
  ).result();
  const options = (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__;
  return parseAgentMetadata(new Headers(options?.headers ?? {}).get("x-tokenvisor-agent"));
}

describe("Pi foreground launch receipts", () => {
  test("keeps a detached child's launch through status queries, completion, and parent consumption", async () => {
    const h = await createHarness();
    const parentId = "status-query-parent";
    const childId = "status-query-child";
    const runId = "status-query-run";
    const launchId = "launch-worker";
    const parentFile = join(h.root, "parent.jsonl");
    const childFile = join(h.root, "child.jsonl");
    const asyncDir = join(h.root, "async-subagent-runs", runId);
    const parentHeader = { type: "session", id: parentId };
    const childHeader = { type: "session", id: childId, parentSession: parentFile };
    const parentBranch: any[] = [
      { type: "message", id: "launch-entry", message: {
        role: "assistant", responseId: "launch-response", stopReason: "toolUse",
        content: [{ type: "toolCall", name: "subagent", id: launchId, arguments: { agent: "worker", async: true } }],
      } },
      { type: "message", id: "launch-result", message: {
        role: "toolResult", toolName: "subagent", toolCallId: launchId, isError: false,
        content: [{ type: "text", text: "Detached worker started" }],
        details: { mode: "single", runId, toolCallId: launchId, results: [] },
      } },
    ];
    const childBranch: any[] = [
      { type: "message", id: "child-user", message: { role: "user", content: [{ type: "text", text: "Read two files" }] } },
    ];
    for (const [id, file, header, branch] of [
      [parentId, parentFile, parentHeader, parentBranch],
      [childId, childFile, childHeader, childBranch],
    ] as const) {
      h.files.set(id, file);
      h.headers.set(id, header);
      h.branches.set(id, branch);
      writeSession(file, header, branch);
    }
    mkdirSync(asyncDir, { recursive: true });
    process.env.PI_SUBAGENTS_TEMP_ROOT = h.root;
    const writeStatus = (state: string) => writeFileSync(join(asyncDir, "status.json"), JSON.stringify({
      lifecycleArtifactVersion: 3, runId, toolCallId: launchId, sessionId: parentFile,
      pid: process.pid, state, mode: "single", sessionFile: childFile,
      steps: [{ sessionFile: childFile, agent: "worker", exitCode: state === "complete" ? 0 : undefined }],
    }));
    const inspectRun = (index: number, action = "status") => {
      const callId = `inspect-${index}`;
      parentBranch.push(
        { type: "message", id: `inspect-call-${index}`, message: {
          role: "assistant", responseId: `inspect-response-${index}`, stopReason: "toolUse",
          content: [{ type: "toolCall", name: "subagent", id: callId, arguments: { action, id: runId } }],
        } },
        { type: "message", id: `inspect-result-${index}`, message: {
          role: "toolResult", toolName: "subagent", toolCallId: callId, isError: false,
          content: [{ type: "text", text: "State: running" }],
          details: { mode: "single", runId, toolCallId: launchId, results: [] },
        } },
      );
      writeSession(parentFile, parentHeader, parentBranch);
    };
    writeStatus("running");
    inspectRun(1);
    process.env.PI_SUBAGENT_CHILD = "1";
    process.env.PI_SUBAGENT_RUNNER_PID = String(process.pid);
    process.env.PI_SUBAGENT_PARENT_SESSION = parentId;
    process.argv = [process.execPath,
      join(h.root, "npm", "node_modules", "pi-subagents", "src", "runs", "background", "subagent-runner-bootstrap.js"),
      join(h.root, `async-cfg-${runId}.json`),
    ];
    const childContext = h.sessionContext(childId);
    h.appendFor(childId);
    h.hooks.get("session_start")!({ reason: "startup" }, childContext);
    const captureChild = async (responseId: string, stopReason: string) => {
      const headers = new Headers();
      h.hooks.get("before_provider_headers")!({ headers }, childContext);
      return dispatchPiMetadata(h, headers, childBranch.map((entry) => entry.message).filter(Boolean), responseId, stopReason);
    };
    const first = await captureChild("child-first-response", "toolUse");
    expect(first?.delegation).toEqual({ run_id: runId, tool_call_id: launchId });
    expect(first?.sequence).toEqual({ status: "start", previous_response_id: null });
    childBranch.push({ type: "message", id: "child-first", message: {
      role: "assistant", responseId: "child-first-response", stopReason: "toolUse", content: [],
    } });
    writeSession(childFile, childHeader, childBranch);
    inspectRun(2);
    inspectRun(3, "debug.run");
    const last = await captureChild("child-final-response", "stop");
    expect(last?.delegation).toEqual({ run_id: runId, tool_call_id: launchId });
    expect(last?.sequence.previous_response_id).toBe("child-first-response");
    const output = "Verified both files";
    childBranch.push({ type: "message", id: "child-final", message: {
      role: "assistant", responseId: "child-final-response", stopReason: "stop", content: [{ type: "text", text: output }],
    } });
    writeSession(childFile, childHeader, childBranch);
    writeStatus("complete");
    h.hooks.get("agent_end")!({ messages: childBranch.map((entry) => entry.message).filter(Boolean) }, childContext);
    expect(childBranch.filter((entry) => entry.customType === PI_RUN_COMPLETION_ENTRY).map((entry) => entry.data)).toEqual([
      { version: 1, runId, sessionId: childId, responseId: "child-final-response" },
    ]);
    writeSession(childFile, childHeader, childBranch);

    delete process.env.PI_SUBAGENT_CHILD;
    delete process.env.PI_SUBAGENT_RUNNER_PID;
    delete process.env.PI_SUBAGENT_PARENT_SESSION;
    process.argv = [process.execPath, join(h.root, "node_modules", "@earendil-works", "pi-coding-agent", "dist", "cli.js")];
    const notice = {
      type: "custom_message", id: "child-notice", customType: "subagent-notify",
      details: { runs: [{ runId, status: "completed", agent: "worker", asyncDir, sessionLabel: "Session file", sessionValue: childFile }] },
      content: `Background task completed: **worker**\n\nworker:\n${output}\n\nRetention-managed async directory: ${asyncDir}\n\nSession file: ${childFile}\n\nParent action: Review the result`,
    };
    parentBranch.push(notice);
    writeSession(parentFile, parentHeader, parentBranch);
    const parentContext = h.sessionContext(parentId);
    h.appendFor(parentId);
    h.hooks.get("session_start")!({ reason: "startup" }, parentContext);
    const headers = new Headers();
    h.hooks.get("before_provider_headers")!({ headers }, parentContext);
    const delivered = `<pi_subagents_notice type="subagent-notify" origin="automated message from the pi-subagents extension, not typed by the user">\n${notice.content}\n</pi_subagents_notice>`;
    const consumer = await dispatchPiMetadata(h, headers, [{ role: "user", content: [{ type: "text", text: delivered }] }], "parent-consumer");
    const dependency = { session_id: `pi:session:${childId}`, response_id: "child-final-response", run_id: runId };
    expect(consumer?.depends_on).toEqual([dependency]);
    expect(consumer?.sequence.previous_response_id).toBe("inspect-response-3");
    parentBranch.push({ type: "message", id: "consumer-entry", message: {
      role: "assistant", responseId: "parent-consumer", stopReason: "stop", content: [],
    } });
    h.hooks.get("agent_end")!({ messages: parentBranch.map((entry) => entry.message).filter(Boolean) }, parentContext);
    expect(parentBranch.filter((entry) => entry.customType === PI_RESULT_CONSUMPTION_ENTRY).map((entry) => entry.data.dependencies)).toEqual([[dependency]]);
  });

  test("resolves only the exact pinned long background preview through the registered provider", async () => {
    const h = await createHarness();
    const fixture = backgroundPreviewFixture;
    const rootId = fixture.parent_session_id;
    const childId = fixture.child_session_id;
    const runId = fixture.run_id;
    const agent = fixture.agent;
    const sessionsRoot = join(h.root, "sessions");
    const parentFile = join(sessionsRoot, "root.jsonl");
    const childFile = join(sessionsRoot, "root", runId, "run-0", "session.jsonl");
    const asyncDir = join(h.root, "async-subagent-runs", runId);
    const nativeSessionDir = join(sessionsRoot, "root", runId, `async-${runId}`);
    const artifactsDir = join(sessionsRoot, "subagent-artifacts");
    const artifactFile = join(artifactsDir, `${runId}_${agent}_output.md`);
    process.env.PI_SUBAGENTS_TEMP_ROOT = h.root;

    const parentCall = { type: "message", id: "parent-call-entry", message: {
      role: "assistant", responseId: fixture.parent_response_id, stopReason: "toolUse",
      content: [{ type: "toolCall", id: fixture.tool_call_id, name: "subagent", arguments: {} }],
    } };
    const parentResult = { type: "message", id: "parent-result-entry", message: {
      role: "toolResult", toolName: "subagent", toolCallId: fixture.tool_call_id, isError: false,
      content: [{ type: "text", text: "Background task started" }], details: { runId, toolCallId: fixture.tool_call_id },
    } };
    const parentHeader = { type: "session", id: rootId };
    const childHeader = { type: "session", id: childId, parentSession: parentFile };
    mkdirSync(dirname(parentFile), { recursive: true });
    mkdirSync(dirname(childFile), { recursive: true });
    mkdirSync(asyncDir, { recursive: true });
    mkdirSync(nativeSessionDir, { recursive: true });
    mkdirSync(artifactsDir, { recursive: true });

    const normalizedOutput = "H".repeat(fixture.normalized_output_shape.ascii_prefix_characters) + "\n" +
      fixture.normalized_output_shape.unicode_segment +
      "x".repeat(fixture.normalized_output_shape.ascii_suffix_characters);
    expect(normalizedOutput.length).toBe(fixture.expected.normalized_utf16_code_units);
    expect(Buffer.byteLength(normalizedOutput, "utf8")).toBe(fixture.expected.normalized_utf8_bytes);
    const rawOutput = `${normalizedOutput}\n\`\`\`acceptance-report\nsynthetic acceptance footer\n\`\`\``;
    const writeChild = (output: string, responseId = fixture.terminal_response_id, markerId = responseId) => {
      writeSession(childFile, childHeader, [
        { type: "message", id: "child-terminal-entry", message: {
          role: "assistant", responseId, stopReason: "stop", content: [{ type: "text", text: output }],
        } },
        { type: "custom", id: "completion-entry", customType: "tokenvisor-agent-run-completion-v1", data: {
          runId, sessionId: childId, responseId: markerId,
        } },
      ]);
    };
    const writeStatus = (artifactDirectory = artifactsDir, overrides: Record<string, unknown> = {}) => writeFileSync(join(asyncDir, "status.json"), JSON.stringify({
      lifecycleArtifactVersion: 3,
      runId,
      toolCallId: fixture.tool_call_id,
      sessionId: parentFile,
      sessionFile: childFile,
      state: "complete",
      mode: "single",
      artifactsDir: artifactDirectory,
      sessionDir: nativeSessionDir,
      steps: [{ sessionFile: childFile, agent, exitCode: 0 }],
      ...overrides,
    }));
    const writeNotice = (preview: string, noticeRunId = runId, pointerSessionFile = childFile) => {
      const notice = {
        type: "custom_message",
        id: "background-notice",
        customType: "subagent-notify",
        details: { runs: [{
          runId: noticeRunId,
          status: "completed",
          agent,
          asyncDir,
          sessionLabel: "Session file",
          sessionValue: pointerSessionFile,
        }] },
        content: `Background task completed: **${agent}**\n\n${agent}:\n${preview}\n\n` +
          `Retention-managed async directory: ${asyncDir}\n\nSession file: ${pointerSessionFile}\n\n` +
          `Parent action: ${fixture.tool_call_id}`,
      };
      return writeNoticeEntry(notice);
    };
    function writeNoticeEntry(notice: any) {
      const parentEntries = [parentCall, parentResult, notice];
      writeSession(parentFile, parentHeader, parentEntries);
      h.files.set(rootId, parentFile);
      h.headers.set(rootId, parentHeader);
      h.branches.set(rootId, parentEntries);
      return notice;
    }
    const streamMetadata = async (effectiveNoticeContent: string) => {
      const headers = new Headers();
      const ctx = h.sessionContext(rootId);
      h.hooks.get("before_provider_headers")!({ headers }, ctx);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId: "carrier-response", stopReason: "stop", content: [] };

      await h.providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers },
        { messages: [{ role: "user", content: [{
          type: "text",
          text: `<pi_subagents_notice type="subagent-notify" origin="automated message from the pi-subagents extension, not typed by the user">\n${effectiveNoticeContent}\n</pi_subagents_notice>`,
        }] }] },
        { headers },
      ).result();
      const options = (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__;
      return parseAgentMetadata(new Headers(options?.headers ?? {}).get("x-tokenvisor-agent"));
    };
    const expectNoPreviewDependency = async (effectiveNoticeContent: string) => {
      const metadata = await streamMetadata(effectiveNoticeContent);
      expect(metadata).not.toBeNull();
      expect(metadata?.session.id).toBe(`pi:session:${rootId}`);
      expect(metadata?.depends_on).toBeNull();
    };

    const capPreview = (output: string) => {
      const prefix = output.slice(0, fixture.expected.inline_preview_head_code_units);
      return `${prefix}\n…\nFull output: ${artifactFile} (${fixture.expected.inline_preview_size_text}, ` +
        `${fixture.expected.inline_preview_line_text}). Read it if needed.`;
    };

    h.appendFor(rootId);
    h.hooks.get("session_start")!({ reason: "startup" }, h.sessionContext(rootId));
    writeChild(rawOutput);
    writeStatus();
    writeFileSync(artifactFile, normalizedOutput);
    let notice = writeNotice(capPreview(normalizedOutput));
    expect(await streamMetadata(notice.content)).toMatchObject({ depends_on: [fixture.expected.dependency] });

    // The pinned status type makes both the top-level session alias and scratch directory optional.
    writeStatus(artifactsDir, { sessionFile: null, sessionDir: undefined });
    expect(await streamMetadata(notice.content)).toMatchObject({ depends_on: [fixture.expected.dependency] });
    writeStatus(artifactsDir, { sessionFile: undefined, sessionDir: undefined });
    expect(await streamMetadata(notice.content)).toMatchObject({ depends_on: [fixture.expected.dependency] });
    writeStatus(artifactsDir, { sessionFile: parentFile });
    await expectNoPreviewDependency(notice.content);
    writeStatus(artifactsDir, { sessionFile: 42 });
    await expectNoPreviewDependency(notice.content);
    writeStatus(artifactsDir, { steps: [{ sessionFile: childFile, agent, exitCode: 0 }, { sessionFile: childFile, agent, exitCode: 0 }] });
    await expectNoPreviewDependency(notice.content);
    writeStatus(artifactsDir, { steps: [{ agent, exitCode: 0 }] });
    await expectNoPreviewDependency(notice.content);
    writeStatus();

    const compatibleRunnerHarness = await createHarness("0.84.0");
    process.env.PI_SUBAGENTS_TEMP_ROOT = h.root;
    const compatibleRunnerContext = h.sessionContext(rootId);
    compatibleRunnerHarness.hooks.get("session_start")!({ reason: "startup" }, compatibleRunnerContext);
    const compatibleRunnerHeaders = new Headers();
    compatibleRunnerHarness.hooks.get("before_provider_headers")!({ headers: compatibleRunnerHeaders }, compatibleRunnerContext);
    const compatibleRunnerMetadata = await dispatchPiMetadata(
      compatibleRunnerHarness,
      compatibleRunnerHeaders,
      [{ role: "user", content: [{
        type: "text",
        text: `<pi_subagents_notice type="subagent-notify" origin="automated message from the pi-subagents extension, not typed by the user">\n${notice.content}\n</pi_subagents_notice>`,
      }] }],
      "compatible-runner-carrier",
    );
    expect(compatibleRunnerMetadata).not.toBeNull();
    expect(compatibleRunnerMetadata?.session.id).toBe(`pi:session:${rootId}`);
    expect(compatibleRunnerMetadata?.depends_on).toEqual([fixture.expected.dependency]);
    expect(compatibleRunnerMetadata?.source.runner_version).toBe("0.84.0");
    compatibleRunnerHarness.hooks.get("session_shutdown")!({}, compatibleRunnerContext);
    process.env.PI_CODING_AGENT_DIR = h.root;
    process.argv = [process.execPath, join(h.root, "node_modules", "@earendil-works", "pi-coding-agent", "dist", "cli.js")];

    // With no acceptance fence the run artifact retains terminal whitespace exactly.
    for (const ending of ["\n", "\r\n"]) {
      const trailingOutput = `${normalizedOutput}${ending}`;
      writeChild(trailingOutput);
      writeStatus();
      writeFileSync(artifactFile, trailingOutput);
      notice = writeNotice(
        `${trailingOutput.slice(0, 2_000)}\n…\nFull output: ${artifactFile} (2.2 KB, 2 lines). Read it if needed.`,
      );
      expect(await streamMetadata(notice.content)).toMatchObject({ depends_on: [fixture.expected.dependency] });
      if (ending === "\r\n") {
        writeFileSync(artifactFile, `${normalizedOutput}\n`);
        await expectNoPreviewDependency(notice.content);
      }
    }

    // The exact 2,000-code-unit output uses the existing untruncated path.
    const exactBoundary = "z".repeat(2_000);
    writeChild(exactBoundary);
    writeFileSync(artifactFile, exactBoundary);
    notice = writeNotice(exactBoundary);
    expect(await streamMetadata(notice.content)).toMatchObject({ depends_on: [fixture.expected.dependency] });

    // The inline cap applies to the raw output length, including terminal newlines.
    for (const ending of ["\n", "\r\n"]) {
      const cappedBoundary = `${"z".repeat(2_000)}${ending}`;
      writeChild(cappedBoundary);
      writeFileSync(artifactFile, cappedBoundary);
      notice = writeNotice(
        `${"z".repeat(2_000)}\n…\nFull output: ${artifactFile} (2.0 KB, 1 line). Read it if needed.`,
      );
      expect(await streamMetadata(notice.content)).toMatchObject({ depends_on: [fixture.expected.dependency] });
    }

    // At 2,001 UTF-16 code units the renderer adds its native pointer footer.
    const overBoundary = "z".repeat(2_001);
    writeChild(overBoundary);
    writeFileSync(artifactFile, overBoundary);
    notice = writeNotice(`${overBoundary.slice(0, 2_000)}\n…\nFull output: ${artifactFile} (2.0 KB, 1 line). Read it if needed.`);
    expect(await streamMetadata(notice.content)).toMatchObject({ depends_on: [fixture.expected.dependency] });

    const overProofLimit = `${"y".repeat(32 * 1024)}\n`;
    writeChild(overProofLimit);
    writeFileSync(artifactFile, overProofLimit);
    notice = writeNotice(`${"y".repeat(2_000)}\n…\nFull output: ${artifactFile} (32.0 KB, 1 line). Read it if needed.`);
    await expectNoPreviewDependency(notice.content);

    writeChild(rawOutput);
    writeFileSync(artifactFile, normalizedOutput);
    notice = writeNotice(capPreview(normalizedOutput));
    const withoutPointer = writeNoticeEntry({ ...notice, content: notice.content.replace(artifactFile, join(artifactsDir, "other-run_worker_output.md")) });
    await expectNoPreviewDependency(withoutPointer.content);
    const wrongMarker = writeNoticeEntry({ ...notice, content: notice.content.replace("\n…\nFull output:", "\n...[preview truncated]\nFull output:") });
    await expectNoPreviewDependency(wrongMarker.content);
    const wrongRun = writeNotice(capPreview(normalizedOutput), "run-preview-wrong");
    await expectNoPreviewDependency(wrongRun.content);

    const neighborArtifactsDir = join(sessionsRoot, "other-run", "subagent-artifacts");
    mkdirSync(neighborArtifactsDir, { recursive: true });
    writeStatus(neighborArtifactsDir);
    notice = writeNotice(capPreview(normalizedOutput));
    await expectNoPreviewDependency(notice.content);
    writeStatus();

    writeFileSync(artifactFile, "different full artifact");
    await expectNoPreviewDependency(notice.content);
    writeFileSync(artifactFile, normalizedOutput);

    const changedTerminal = `${normalizedOutput.slice(0, 2_000)}different suffix`;
    writeChild(changedTerminal, "terminal-preview-later");
    notice = writeNotice(capPreview(normalizedOutput));
    await expectNoPreviewDependency(notice.content);

    writeChild(normalizedOutput, "terminal-preview-later", fixture.terminal_response_id);
    notice = writeNotice(capPreview(normalizedOutput));
    await expectNoPreviewDependency(notice.content);

    writeChild(rawOutput);
    writeFileSync(artifactFile, normalizedOutput);
    notice = writeNotice(capPreview(normalizedOutput));
    const modifiedEffectiveInput = await streamMetadata(notice.content.replace("H".repeat(50), "X".repeat(50)));
    expect(modifiedEffectiveInput).not.toBeNull();
    expect(modifiedEffectiveInput?.session.id).toBe(`pi:session:${rootId}`);
    expect(modifiedEffectiveInput?.depends_on).toBeNull();

    if (process.env.TOKENVISOR_PI_FOREGROUND_BENCHMARK === "1") {
      await (await import("../scripts/benchmarks/capture.ts")).benchmarkPiPreview({
        h, rootId, parentCall, parentResult, notice, writeSession, parentFile, parentHeader,
      });
    }
    h.hooks.get("session_shutdown")!({}, h.sessionContext(rootId));
  });

  test("preserves verified child identity before delegation is available", async () => {
    const h = await createHarness();
    const rootId = "root-session";
    const childId = "fresh-child";
    const parentFile = join(h.root, "parent.jsonl");
    const childFile = join(h.root, "child.jsonl");
    const parentHeader = { type: "session", id: rootId };
    const parentCall = { type: "message", id: "parent-call-entry", message: {
      role: "assistant", responseId: "parent-response", stopReason: "toolUse",
      content: [{ type: "toolCall", id: "call-1", name: "subagent", arguments: {} }],
    } };
    const parentResult = { type: "message", id: "parent-result-entry", message: {
      role: "toolResult", toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: "child answer" }],
      details: { mode: "single", runId: "run-1", results: [{ context: "fresh", sessionFile: childFile, exitCode: 0 }] },
    } };
    writeSession(parentFile, parentHeader, [parentCall, parentResult]);
    const childHeader = { type: "session", id: childId, parentSession: parentFile };
    const childUser = { type: "message", id: "child-user", message: { role: "user", content: [{ type: "text", text: "task" }] } };
    const childTerminal = { type: "message", id: "child-terminal", message: {
      role: "assistant", responseId: "child-response", stopReason: "stop", content: [{ type: "text", text: "child answer" }],
    } };
    writeSession(childFile, childHeader, [childUser, childTerminal]);
    h.files.set(rootId, parentFile);
    h.headers.set(rootId, parentHeader);
    h.branches.set(rootId, [parentCall, parentResult]);
    h.appendFor(rootId);
    h.hooks.get("session_start")!({ reason: "startup" }, h.sessionContext(rootId));

    const childContext = h.sessionContext(childId);
    h.files.set(childId, childFile);
    h.headers.set(childId, childHeader);
    h.branches.set(childId, [parentCall, childUser, childTerminal]);
    const childHeaders = new Headers();
    h.hooks.get("before_provider_headers")!({ headers: childHeaders }, childContext);
    const childMetadata = await dispatchPiMetadata(h, childHeaders, [childUser.message], "child-identity-carrier");
    expect(childMetadata?.session).toEqual({ id: `pi:session:${childId}`, root_id: `pi:session:${rootId}`, parent_id: `pi:session:${rootId}` });
    expect(childMetadata?.delegation).toBeNull();
    h.hooks.get("tool_result")!({
      toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: "child answer" }],
      details: { mode: "single", runId: "run-1", results: [{ context: "fresh", sessionFile: childFile, exitCode: 0 }] },
    }, h.sessionContext(rootId));
    const parentHeaders = new Headers();
    h.hooks.get("before_provider_headers")!({ headers: parentHeaders }, h.sessionContext(rootId));
    const parentMetadata = await dispatchPiMetadata(h, parentHeaders, [parentCall.message, parentResult.message], "parent-identity-carrier");
    expect(parentMetadata?.child_launches[0]?.child_session_id).toBe(childMetadata?.session.id);
  });

  test("creates a result dependency only for the exact ordinary child output", async () => {
    const h = await createHarness();
    const rootId = "root-session";
    const childId = "fresh-child";
    const parentFile = join(h.root, "parent.jsonl");
    const childFile = join(h.root, "child.jsonl");
    const parentHeader = { type: "session", id: rootId };
    const parentCall = { type: "message", id: "parent-call-entry", message: {
      role: "assistant", responseId: "parent-response", stopReason: "toolUse",
      content: [{ type: "toolCall", id: "call-1", name: "subagent", arguments: {} }],
    } };
    const toolResult = { type: "message", id: "result-entry", message: {
      role: "toolResult", toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: "child answer" }],
      details: { mode: "single", runId: "run-1", results: [{ context: "fresh", sessionFile: childFile, exitCode: 0 }] },
    } };
    writeSession(parentFile, parentHeader, [parentCall, toolResult]);
    writeSession(childFile, { type: "session", id: childId, parentSession: parentFile }, [{
      type: "message", id: "child-answer-entry", message: {
        role: "assistant", responseId: "child-response", stopReason: "stop", content: [{ type: "text", text: "child answer" }],
      },
    }]);
    h.files.set(rootId, parentFile);
    h.headers.set(rootId, parentHeader);
    h.branches.set(rootId, [parentCall, toolResult]);
    h.appendFor(rootId);
    const ctx = h.sessionContext(rootId);
    h.hooks.get("session_start")!({ reason: "startup" }, ctx);
    h.hooks.get("tool_result")!({
      toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: "child answer" }],
      details: { mode: "single", runId: "run-1", results: [{ context: "fresh", sessionFile: childFile, exitCode: 0 }] },
    }, ctx);

    const runWithEffectiveMessages = async (responseId: string, messages: any[]) => {
      const requestHeaders = new Headers();
      h.hooks.get("before_provider_headers")!({ headers: requestHeaders }, ctx);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId, stopReason: "stop", content: [] };

      await h.providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: requestHeaders },
        { messages },
        { headers: requestHeaders },
      ).result();
      const options = (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__;
      return parseAgentMetadata(new Headers(options?.headers ?? {}).get("x-tokenvisor-agent"));
    };
    const exactResult = await runWithEffectiveMessages("carrier-exact", [parentCall.message, toolResult.message]);
    expect(exactResult?.child_launches).toHaveLength(1);
    expect(exactResult?.depends_on).toEqual([{
      session_id: `pi:session:${childId}`, response_id: "child-response", run_id: "run-1",
    }]);
    const absentResult = await runWithEffectiveMessages("carrier-absent", [{
      role: "user", content: [{ type: "text", text: "child answer" }],
    }]);
    expect(absentResult?.child_launches).toHaveLength(1);
    expect(absentResult?.depends_on).toEqual([]);
    const changedResult = await runWithEffectiveMessages("carrier-changed", [
      { role: "toolResult", toolCallId: "call-1", content: [{ type: "text", text: "changed output" }] },
      { role: "user", content: [{ type: "text", text: "child answer" }] },
    ]);
    expect(changedResult?.child_launches).toHaveLength(1);
    expect(changedResult?.depends_on).toEqual([]);
  });

  test("accepts Pi's structured fan-out and mission wrappers around the exact inline child output", async () => {
    const h = await createHarness();
    const rootId = "root-session";
    const childId = "fresh-child";
    const parentFile = join(h.root, "parent.jsonl");
    const childFile = join(h.root, "child.jsonl");
    const parentHeader = { type: "session", id: rootId };
    const parentCall = { type: "message", id: "parent-call-entry", message: {
      role: "assistant", responseId: "parent-response", stopReason: "toolUse",
      content: [{ type: "toolCall", id: "call-1", name: "subagent", arguments: {} }],
    } };
    const finalOutput = "NATIVE_CHILD_ACK";
    const missionId = "9aebeb6f-a41e-435d-b9eb-67097b0fd578";
    const deliveredText = `Run fan-out: 1/64 used, 63 remaining\n${finalOutput}\nMission: ${missionId} (completed)`;
    const details = {
      mode: "single",
      runId: "run-1",
      runFanoutBudget: { used: 1, limit: 64, remaining: 63 },
      mission: { id: missionId, status: "completed" },
      results: [{
        context: "fresh", sessionFile: childFile, exitCode: 0,
        outputMode: "inline", outputState: "present", finalOutput,
      }],
    };
    const toolResult = { type: "message", id: "result-entry", message: {
      role: "toolResult", toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: deliveredText }], details,
    } };
    writeSession(parentFile, parentHeader, [parentCall, toolResult]);
    writeSession(childFile, { type: "session", id: childId, parentSession: parentFile }, [{
      type: "message", id: "child-answer-entry", message: {
        role: "assistant", responseId: "child-response", stopReason: "stop", content: [{ type: "text", text: finalOutput }],
      },
    }]);
    h.files.set(rootId, parentFile);
    h.headers.set(rootId, parentHeader);
    h.branches.set(rootId, [parentCall, toolResult]);
    h.appendFor(rootId);
    const ctx = h.sessionContext(rootId);
    h.hooks.get("session_start")!({ reason: "startup" }, ctx);
    expect(readPiForegroundLaunchEvidence({
      toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: `untrusted prefix\n${finalOutput}` }], details,
    }, ctx)?.deliveredOutputSha256).toBeUndefined();
    h.hooks.get("tool_result")!({
      toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: deliveredText }], details,
    }, ctx);
    expect(h.appended.find((entry) => entry.customType === "tokenvisor-agent-foreground-delivery-v1")?.data.receipt.last_response_id)
      .toBe("child-response");
    const preparedHeaders = new Headers();
    h.hooks.get("before_provider_headers")!({ headers: preparedHeaders }, ctx);

    const headers = preparedHeaders;
    (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId: "carrier", stopReason: "stop", content: [] };

    await h.providerConfig.streamSimple(
      { id: "synthetic", api: "openai-completions", headers },
      { messages: [parentCall.message, toolResult.message] },
      { headers },
    ).result();
    const options = (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__;
    const metadata = parseAgentMetadata(new Headers(options?.headers ?? {}).get("x-tokenvisor-agent"));
    expect(metadata?.depends_on).toEqual([{
      session_id: `pi:session:${childId}`, response_id: "child-response", run_id: "run-1",
    }]);

    // A later native tool_result hook can replace the stored ordinary output after this
    // provider's hook persisted its witness. Preserve identity, but treat the mismatch as filtered.
    toolResult.message.content = [{ type: "text", text: "NATIVE_OUTPUT_WITHHELD" }];
    const filteredHeaders = new Headers();
    h.hooks.get("before_provider_headers")!({ headers: filteredHeaders }, ctx);
    const filteredMetadata = await dispatchPiMetadata(
      h, filteredHeaders, [parentCall.message, toolResult.message], "filtered-inline-carrier",
    );
    expect(filteredMetadata?.child_launches).toHaveLength(1);
    expect(filteredMetadata?.depends_on).toEqual([]);
    expect(readPiPendingChildLaunchReceipts(ctx)).toHaveLength(1);
  });

  test("keeps the launch receipt when persisting its output witness fails", async () => {
    const h = await createHarness();
    const rootId = "root-session";
    const childId = "fresh-child";
    const parentFile = join(h.root, "parent.jsonl");
    const childFile = join(h.root, "child.jsonl");
    const parentHeader = { type: "session", id: rootId };
    const parentCall = { type: "message", id: "parent-call-entry", message: {
      role: "assistant", responseId: "parent-response", stopReason: "toolUse",
      content: [{ type: "toolCall", id: "call-1", name: "subagent", arguments: {} }],
    } };
    const toolResult = { type: "message", id: "result-entry", message: {
      role: "toolResult", toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: "child answer" }],
      details: { mode: "single", runId: "run-1", results: [{ context: "fresh", sessionFile: childFile, exitCode: 0 }] },
    } };
    writeSession(parentFile, parentHeader, [parentCall, toolResult]);
    writeSession(childFile, { type: "session", id: childId, parentSession: parentFile }, [{
      type: "message", message: { role: "assistant", responseId: "child-response", stopReason: "stop", content: [{ type: "text", text: "child answer" }] },
    }]);
    h.files.set(rootId, parentFile);
    h.headers.set(rootId, parentHeader);
    h.branches.set(rootId, [parentCall, toolResult]);
    h.appendFor(rootId);
    const ctx = h.sessionContext(rootId);
    h.hooks.get("session_start")!({ reason: "startup" }, ctx);
    h.failAppendTypes.add("tokenvisor-agent-foreground-delivery-v1");
    h.hooks.get("tool_result")!({
      toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: "child answer" }],
      details: { mode: "single", runId: "run-1", results: [{ context: "fresh", sessionFile: childFile, exitCode: 0 }] },
    }, ctx);
    const headers = new Headers();
    h.hooks.get("before_provider_headers")!({ headers }, ctx);
    const metadata = await dispatchPiMetadata(h, headers, [parentCall.message, toolResult.message], "witness-failure-carrier");
    expect(metadata?.child_launches).toHaveLength(1);
    expect(metadata?.child_launches[0]?.child_session_id).toBe(`pi:session:${childId}`);
    expect(metadata?.depends_on).toEqual([]);
    expect(h.appended.some((entry) => entry.customType === "tokenvisor-agent-launch-receipts-v1")).toBe(true);
    expect(h.appended.some((entry) => entry.customType === "tokenvisor-agent-foreground-delivery-v1")).toBe(false);
  });

  test("distinguishes same-role child calls by tool-call ID and response lineage", () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-reused-child-role-"));
    tempRoots.push(root);
    const parentFile = join(root, "parent.jsonl");
    const firstChildFile = join(root, "first-child.jsonl");
    const secondChildFile = join(root, "second-child.jsonl");
    const firstCall = { type: "message", id: "parent-call-one", message: {
      role: "assistant", responseId: "parent-response-one", stopReason: "toolUse",
      content: [{ type: "toolCall", id: "same-role-call-one", name: "subagent", arguments: { agent: "worker" } }],
    } };
    const secondCall = { type: "message", id: "parent-call-two", message: {
      role: "assistant", responseId: "parent-response-two", stopReason: "toolUse",
      content: [{ type: "toolCall", id: "same-role-call-two", name: "subagent", arguments: { agent: "worker" } }],
    } };
    writeSession(parentFile, { type: "session", id: "root-session" }, [firstCall, secondCall]);
    writeSession(firstChildFile, { type: "session", id: "child-one", parentSession: parentFile }, [{
      type: "message", message: { role: "assistant", responseId: "response-one", stopReason: "stop", content: [{ type: "text", text: "first" }] },
    }]);
    writeSession(secondChildFile, { type: "session", id: "child-two", parentSession: parentFile }, [{
      type: "message", id: "child-two-intermediate", message: {
        role: "assistant", responseId: "response-two-intermediate", stopReason: "stop", content: [{ type: "text", text: "work in progress" }],
      },
    }, {
      type: "message", message: { role: "assistant", responseId: "response-two", stopReason: "stop", content: [{ type: "text", text: "second" }] },
    }]);
    const ctx = { sessionManager: {
      getSessionId: () => "root-session",
      getSessionFile: () => parentFile,
      getHeader: () => ({ id: "root-session" }),
      getBranch: () => [firstCall, secondCall],
    } } as any;
    const evidence = readPiForegroundLaunchEvidence({
      toolName: "subagent", toolCallId: "same-role-call-two", isError: false,
      content: [{ type: "text", text: "second" }],
      details: { mode: "single", runId: "run-two", results: [{ context: "fresh", sessionFile: secondChildFile, exitCode: 0 }] },
    }, ctx);
    expect(evidence?.receipt).toEqual({
      parent_response_id: "parent-response-two", tool_call_id: "same-role-call-two", child_session_id: "pi:session:child-two",
      run_id: "run-two", last_response_id: "response-two",
    });
    expect(evidence?.deliveredOutputSha256).toMatch(/^[0-9a-f]{64}$/);
  });

  test("keeps a filtered launch receipt pending, then retires it only after a committed carrier response", async () => {
    const h = await createHarness();
    const rootId = "root-session";
    const childId = "fresh-child";
    const parentFile = join(h.root, "parent.jsonl");
    const childFile = join(h.root, "child.jsonl");
    const parentHeader = { type: "session", id: rootId };
    const parentCall = { type: "message", id: "parent-call-entry", message: {
      role: "assistant", responseId: "parent-response", stopReason: "toolUse",
      content: [{ type: "toolCall", id: "call-1", name: "subagent", arguments: {} }],
    } };
    const toolResult = { type: "message", id: "result-entry", message: {
      role: "toolResult", toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: "filtered output" }],
      details: { mode: "single", runId: "run-1", results: [{ context: "fresh", sessionFile: childFile, exitCode: 0 }] },
    } };
    writeSession(parentFile, parentHeader, [parentCall, toolResult]);
    writeSession(childFile, { type: "session", id: childId, parentSession: parentFile }, [{
      type: "message", id: "child-answer-entry", message: {
        role: "assistant", responseId: "child-response", stopReason: "stop", content: [{ type: "text", text: "child answer" }],
      },
    }]);
    h.files.set(rootId, parentFile);
    h.headers.set(rootId, parentHeader);
    h.branches.set(rootId, [parentCall, toolResult]);
    h.appendFor(rootId);
    const ctx = h.sessionContext(rootId);
    h.hooks.get("session_start")!({ reason: "startup" }, ctx);

    const filteredEvent = {
      toolName: "subagent", toolCallId: "call-1", isError: false,
      content: [{ type: "text", text: "filtered output" }],
      details: { mode: "single", runId: "run-1", results: [{ context: "fresh", sessionFile: childFile, exitCode: 0 }] },
    };
    h.hooks.get("tool_result")!(filteredEvent, ctx);
    h.hooks.get("tool_result")!(filteredEvent, ctx);
    expect(h.appended.filter((entry) => entry.customType === "tokenvisor-agent-launch-receipts-v1")).toHaveLength(1);
    expect(h.appended.filter((entry) => entry.customType === "tokenvisor-agent-foreground-delivery-v1")).toHaveLength(0);
    let headers = new Headers();
    h.hooks.get("before_provider_headers")!({ headers }, ctx);
    (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId: "failed-carrier", stopReason: "error", content: [] };

    await h.providerConfig.streamSimple({ id: "synthetic", api: "openai-completions", headers }, { messages: [parentCall.message, toolResult.message] }, { headers }).result();
    const filteredOptions = (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__;
    const filteredMetadata = parseAgentMetadata(new Headers(filteredOptions?.headers ?? {}).get("x-tokenvisor-agent"));
    expect(filteredMetadata?.child_launches).toEqual([{
      parent_response_id: "parent-response", tool_call_id: "call-1", child_session_id: `pi:session:${childId}`,
      run_id: "run-1", last_response_id: "child-response",
    }]);
    expect(filteredMetadata?.depends_on).toEqual([]);
    const failedBranch = h.branches.get(rootId)!;
    h.appendFor(rootId);
    h.hooks.get("agent_end")!({ messages: [{ role: "assistant", responseId: "failed-carrier", stopReason: "error" }] }, ctx);
    expect(readPiPendingChildLaunchReceipts(ctx)).toHaveLength(1);

    headers = new Headers();
    h.hooks.get("before_provider_headers")!({ headers }, ctx);
    (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId: "good-carrier", stopReason: "stop", content: [{ type: "text", text: "ok" }] };

    await h.providerConfig.streamSimple({ id: "synthetic", api: "openai-completions", headers }, { messages: [parentCall.message, toolResult.message] }, { headers }).result();
    h.branches.set(rootId, [...failedBranch, { type: "message", id: "good-carrier-entry", message: {
      role: "assistant", responseId: "good-carrier", stopReason: "stop", content: [{ type: "text", text: "ok" }],
    } }]);
    h.appendFor(rootId);
    h.hooks.get("agent_end")!({ messages: [{ role: "assistant", responseId: "good-carrier", stopReason: "stop" }] }, ctx);
    expect(h.appended.some((entry) => entry.customType === "tokenvisor-agent-launch-receipt-consumption-v1" && entry.data.response_id === "good-carrier")).toBe(true);
    expect(readPiPendingChildLaunchReceipts(ctx)).toEqual([]);
  });

  test("rehydrates pending receipts from the committed compaction snapshot", () => {
    const h = { sessionManager: {
      getSessionId: () => "root-session",
      getSessionFile: () => undefined,
      getHeader: () => ({ id: "root-session" }),
      getBranch: () => [],
    } } as any;
    const receipt = {
      parent_response_id: "parent-response", tool_call_id: "call-1", child_session_id: "child-session",
      run_id: "run-1", last_response_id: "child-response",
    };
    const nativeCompaction = { type: "compaction", id: "compaction-1" };
    const record = {
      type: "custom_message", customType: "tokenvisor-agent-compaction-v1",
      data: makePiCompactionRecord("compaction-1", [], false, [], false, [], [receipt]),
    };
    h.sessionManager.getBranch = () => [nativeCompaction, record];
    expect(readPiPendingChildLaunchReceipts(h)).toEqual([receipt]);
  });

  test("drains more than eight persisted receipts over successive committed streams", async () => {
    const h = await createHarness();
    const rootId = "root-session";
    const parentFile = join(h.root, "parent.jsonl");
    const parentHeader = { type: "session", id: rootId };
    const receipts = Array.from({ length: 9 }, (_, index) => ({
      parent_response_id: `parent-response-${index}`,
      tool_call_id: `call-${index}`,
      child_session_id: `child-session-${index}`,
      run_id: `run-${index}`,
      last_response_id: `child-response-${index}`,
    }));
    writeSession(parentFile, parentHeader, []);
    const branch: any[] = [{
      type: "custom_message", id: "seeded-pending-receipts", customType: "tokenvisor-agent-launch-receipts-v1",
      data: { version: 1, receipts },
    }];
    h.files.set(rootId, parentFile);
    h.headers.set(rootId, parentHeader);
    h.branches.set(rootId, branch);
    h.appendFor(rootId);
    const ctx = h.sessionContext(rootId);
    h.hooks.get("session_start")!({ reason: "startup" }, ctx);

    const sendCarrier = async (responseId: string) => {
      const headers = new Headers();
      h.hooks.get("before_provider_headers")!({ headers }, ctx);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId, stopReason: "stop", content: [{ type: "text", text: "carrier" }] };

      await h.providerConfig.streamSimple({ id: "synthetic", api: "openai-completions", headers }, { messages: [] }, { headers }).result();
      const options = (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__;
      const outgoing = parseAgentMetadata(new Headers(options?.headers ?? {}).get("x-tokenvisor-agent"));
      branch.push({ type: "message", id: `${responseId}-entry`, message: {
        role: "assistant", responseId, stopReason: "stop", content: [{ type: "text", text: "carrier" }],
      } });
      h.appendFor(rootId);
      h.hooks.get("agent_end")!({ messages: [{ role: "assistant", responseId, stopReason: "stop" }] }, ctx);
      return outgoing;
    };

    const first = await sendCarrier("carrier-one");
    expect(first?.child_launches).toEqual(receipts.slice(0, 8));
    expect(readPiPendingChildLaunchReceipts(ctx)).toEqual(receipts.slice(8));
    const second = await sendCarrier("carrier-two");
    expect(second?.child_launches).toEqual(receipts.slice(8));
    expect(readPiPendingChildLaunchReceipts(ctx)).toEqual([]);
  });

  test("releases native child session contexts after repeated Pi startup and shutdown", async () => {
    const h = await createHarness();
    const parentId = "parent-session";
    const parentFile = join(h.root, "parent.jsonl");
    const parentHeader = { type: "session", id: parentId };
    const parentCall = { type: "message", id: "parent-call", message: {
      role: "assistant", responseId: "parent-response", stopReason: "toolUse",
      content: [{ type: "toolCall", id: "call-1", name: "subagent", arguments: {} }],
    } };
    writeSession(parentFile, parentHeader, [parentCall]);
    const startAndStop = (index: number): WeakRef<object> => {
      const sessionId = `child-session-${index}`;
      const sessionFile = join(h.root, `${sessionId}.jsonl`);
      const header = { type: "session", id: sessionId, parentSession: parentFile };
      const childUser = { type: "message", id: `child-user-${index}`, message: {
        role: "user", content: [{ type: "text", text: `task ${index}` }],
      } };
      writeSession(sessionFile, header, [childUser]);
      h.files.set(sessionId, sessionFile);
      h.headers.set(sessionId, header);
      h.branches.set(sessionId, [parentCall, childUser]);
      let ctx: any = h.sessionContext(sessionId);
      const reference = new WeakRef(ctx);
      h.hooks.get("session_start")!({ reason: "startup" }, ctx);
      h.hooks.get("session_shutdown")!({}, ctx);
      ctx = null;
      return reference;
    };
    const weakContexts: WeakRef<object>[] = [];
    for (let index = 0; index < 48; index += 1) {
      const reference = await new Promise<WeakRef<object>>((resolve) => {
        setTimeout(() => {
          resolve(startAndStop(index));
        }, 0);
      });
      weakContexts.push(reference);
    }
    for (let index = 0; index < 3; index += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
      Bun.gc(true);
      await Promise.resolve();
    }
    const retainedContextIndices = weakContexts.flatMap((reference, index) => reference.deref() ? [index] : []);
    expect(retainedContextIndices, `native child contexts still strongly referenced: ${retainedContextIndices.join(",")}`)
      .toEqual([]);
  });

  test("uses committed native OMP compaction proof for known-empty provider dependencies", async () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-omp-compaction-hook-test-"));
    tempRoots.push(root);
    const oldExecPath = process.execPath;
    try {
      const versionExecutable = join(root, "omp-version");
      writeFileSync(versionExecutable, "#!/bin/sh\nprintf 'omp/18.12.6\\n'\n");
      chmodSync(versionExecutable, 0o755);
      process.execPath = versionExecutable;
      const hostExecutable = join(root, "node_modules", "@oh-my-pi", "pi-coding-agent", "dist", "cli.js");
      mkdirSync(dirname(hostExecutable), { recursive: true });
      writeFileSync(hostExecutable, "");
      process.argv = [versionExecutable, hostExecutable];
      process.env.TOKENVISOR_API_KEY = "synthetic-key";
      process.env.TOKENVISOR_BASE_URL = "http://127.0.0.1:1/api/v1";
      globalThis.fetch = async () => Response.json({ data: [{ id: "synthetic", type: "chat", capabilities: [] }] });

      const sessionId = "omp-compaction-root";
      const sessionFile = join(root, "root.jsonl");
      const header = { type: "session", id: sessionId };
      writeSession(sessionFile, header, []);
      const beforeResponse = { type: "message", id: "before-entry", message: {
        role: "assistant", responseId: "before-response", stopReason: "toolUse",
        content: [{ type: "toolCall", id: "pending-task-call", name: "task", arguments: {} }],
      } };
      const oldNativeCompaction = {
        type: "compaction", id: "old-compact", firstKeptEntryId: "before-entry", method: "snapcompact", fromExtension: false,
      };
      const branch: any[] = [beforeResponse, oldNativeCompaction, {
        type: "custom", customType: PI_COMPACTION_ENTRY,
        data: makePiCompactionRecord("old-compact", [], true, [], true),
      }];
      const branches = new Map([[sessionId, branch]]);
      let currentSession = sessionId;
      let appendId = 0;
      let providerConfig: any;
      const hooks = new Map<string, (event: any, ctx: any) => unknown>();
      const eventHandlers = new Map<string, (event: any) => unknown>();
      const ctx = { sessionManager: {
        getSessionId: () => sessionId,
        getSessionFile: () => sessionFile,
        getHeader: () => header,
        getBranch: () => branches.get(sessionId) ?? [],
      } };
      await tokenVisorProvider({
        on: (name: string, handler: any) => hooks.set(name, handler),
        events: { on: (name: string, handler: any) => { eventHandlers.set(name, handler); return () => {}; }, emit: () => {} },
        registerCommand: () => {},
        registerProvider: (_name: string, config: any) => { providerConfig = config; },
        appendEntry: (customType: string, data: any) => {
          const current = branches.get(currentSession)!;
          current.push({ type: "custom", id: `hook-entry-${appendId++}`, customType, data });
        },
      } as any);
      hooks.get("session_start")!({ reason: "startup" }, ctx);

      const commitCompaction = (id: string, eventId = id) => {
        currentSession = sessionId;
        hooks.get("session_before_compact")!({}, ctx);
        const compactionEntry = {
          type: "compaction", id, firstKeptEntryId: "before-entry", method: "snapcompact", fromExtension: false,
        };
        branch.push(compactionEntry);
        hooks.get("session_compact")!({
          fromExtension: false,
          compactionEntry: { ...compactionEntry, id: eventId },
        }, ctx);
      };
      const captureNextMetadata = async (responseId: string) => {
        (globalThis as any).__OMP_PROVIDER_RESULT__ = { responseId, stopReason: "stop", content: [] };
        await providerConfig.streamSimple(
          { id: "synthetic", api: "openai-completions" },
          { messages: [] },
          { sessionId },
        ).result();
        const options = (globalThis as any).__OMP_PROVIDER_OPTIONS__;
        return parseAgentMetadata(new Headers(options?.headers ?? {}).get("x-tokenvisor-agent"));
      };

      commitCompaction("known-empty-compact");
      const knownEmpty = await captureNextMetadata("after-known-empty");
      expect(knownEmpty?.depends_on).toEqual([]);
      expect(knownEmpty?.sequence.previous_response_id).toBe("before-response");
      branch.push({ type: "message", id: "after-known-empty-entry", message: {
        role: "assistant", responseId: "after-known-empty", stopReason: "stop", content: [],
      } });

      const childSessionFile = join(root, "child.jsonl");
      writeSession(childSessionFile, { type: "session", id: "omp-pending-child", parentSession: sessionFile }, []);
      const lifecycle = {
        id: "pending-agent-id",
        agent: "worker",
        parentToolCallId: "pending-task-call",
        detached: false,
        sessionFile: childSessionFile,
        status: "started",
      };
      eventHandlers.get("task:subagent:lifecycle")!(lifecycle);
      expect(getOmpRuntimeCaptureState().earlyTaskBindings.size).toBe(1);
      const originalPredecessor = structuredClone(beforeResponse);
      commitCompaction("pending-input-compact");
      const pendingUnavailable = await captureNextMetadata("after-pending-input");
      expect(pendingUnavailable?.depends_on).toBeNull();
      expect(pendingUnavailable?.sequence.previous_response_id).toBe("after-known-empty");
      expect(branch.find((entry) => entry.id === "before-entry")).toEqual(originalPredecessor);
      expect(branch.filter((entry) => entry.type === "message").map((entry) => entry.id)).toEqual([
        "before-entry", "after-known-empty-entry",
      ]);
      clearOmpEarlyTaskBindingsForSession(getOmpRuntimeCaptureState(), sessionId);
      branch.push({ type: "message", id: "after-pending-input-entry", message: {
        role: "assistant", responseId: "after-pending-input", stopReason: "stop", content: [],
      } });

      const resetNativeCompaction = {
        type: "compaction", id: "reset-compact", firstKeptEntryId: "after-pending-input-entry", method: "snapcompact", fromExtension: false,
      };
      const afterKnownEntry = branch.find((entry) => entry.id === "after-known-empty-entry")!;
      const afterPendingEntry = branch.find((entry) => entry.id === "after-pending-input-entry")!;
      branch.splice(0, branch.length, beforeResponse, afterKnownEntry, afterPendingEntry, resetNativeCompaction, {
        type: "custom", customType: PI_COMPACTION_ENTRY,
        data: makePiCompactionRecord("reset-compact", [], true, [], true),
      });

      commitCompaction("mismatched-compact", "stale-event-compact");
      const unavailable = await captureNextMetadata("after-mismatched-commit");
      expect(unavailable?.depends_on).toBeNull();
      expect(unavailable?.sequence.previous_response_id).toBe("after-pending-input");
      expect(branch.find((entry) => entry.id === "before-entry")).toEqual(originalPredecessor);
      expect(branch.filter((entry) => entry.type === "message").map((entry) => entry.id)).toEqual([
        "before-entry", "after-known-empty-entry", "after-pending-input-entry",
      ]);
      expect(getOmpRuntimeCaptureState().branchReaders.has(sessionId)).toBe(true);

      if (process.env.TOKENVISOR_OMP_CAPTURE_BENCHMARK === "1") {
        await (await import("../scripts/benchmarks/capture.ts")).benchmarkOmpCompaction({
          branch, sessionId, hooks, ctx, captureNextMetadata, root,
          selectSession: () => { currentSession = sessionId; },
        });
      }

      hooks.get("session_shutdown")!({}, ctx);
      expect(getOmpRuntimeCaptureState().branchReaders.has(sessionId)).toBe(false);
    } finally {
      process.execPath = oldExecPath;
    }
  });

  test("keeps verified OMP child identity when no task binding is available", async () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-omp-unbound-child-test-"));
    tempRoots.push(root);
    const oldExecPath = process.execPath;
    try {
      const versionExecutable = join(root, "omp-version");
      writeFileSync(versionExecutable, "#!/bin/sh\nprintf 'omp/18.12.6\\n'\n");
      chmodSync(versionExecutable, 0o755);
      process.execPath = versionExecutable;
      const hostExecutable = join(root, "node_modules", "@oh-my-pi", "pi-coding-agent", "dist", "cli.js");
      process.argv = [versionExecutable, hostExecutable];
      process.env.TOKENVISOR_API_KEY = "synthetic-key";
      process.env.TOKENVISOR_BASE_URL = "http://127.0.0.1:1/api/v1";
      globalThis.fetch = async () => Response.json({ data: [{ id: "synthetic", type: "chat", capabilities: [] }] });

      const parentFile = join(root, "parent.jsonl");
      const childFile = join(root, "child.jsonl");
      const rootId = "omp-root-session";
      const childId = "omp-child-session";
      const parentHeader = { type: "session", id: rootId };
      const childHeader = { type: "session", id: childId, parentSession: parentFile };
      writeSession(parentFile, parentHeader, []);
      writeSession(childFile, childHeader, [{ type: "message", message: { role: "user", content: "fresh task" } }]);

      const hooks = new Map<string, (event: any, ctx: any) => unknown>();
      let providerConfig: any;
      const branch = [{ type: "message", id: "child-user", message: { role: "user", content: "fresh task" } }];
      const pi = {
        on: (name: string, handler: any) => hooks.set(name, handler),
        events: { on: () => () => {}, emit: () => {} },
        registerCommand: () => {},
        registerProvider: (_name: string, config: any) => { providerConfig = config; },
        appendEntry: () => { throw new Error("no custom entries expected"); },
      };
      await tokenVisorProvider(pi as any);
      const ctx = {
        agent: { id: "worker-unbound", kind: "subagent" },
        sessionManager: {
          getSessionId: () => childId,
          getSessionFile: () => childFile,
          getHeader: () => childHeader,
          getBranch: () => branch,
        },
      };
      hooks.get("session_start")!({ reason: "startup" }, ctx);

      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions" },
        { messages: branch.map((entry) => entry.message) },
        { sessionId: childId },
      ).result();
      const options = (globalThis as any).__OMP_PROVIDER_OPTIONS__;
      const metadata = parseAgentMetadata(new Headers(options?.headers ?? {}).get("x-tokenvisor-agent"));
      expect(metadata?.session).toEqual({
        id: `omp:session:${childId}`, root_id: `omp:session:${rootId}`, parent_id: `omp:session:${rootId}`,
      });
      expect(metadata?.delegation).toBeNull();
      expect(metadata?.sequence).toEqual({ status: "start", previous_response_id: null });
    } finally {
      process.execPath = oldExecPath;
    }
  });

  test("retires filtered task bindings through shared state across provider registrations", async () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-omp-shared-retirement-test-"));
    tempRoots.push(root);
    const oldExecPath = process.execPath;
    const oldArgv = process.argv;
    const parentId = "01a1967e-parent-b925-73fd-b992-d0d11d570d4c";
    const childId = "01a1967e-child-bc0a-71a2-aea3-8a7689baae80";
    const allocatedId = "Registry-shared-retirement";
    const toolCallId = "call-shared-retirement";
    const parentFile = join(root, "parent.jsonl");
    const childFile = join(root, parentId, `${allocatedId}.jsonl`);
    const parentHeader = { type: "session", id: parentId };
    const childHeader = { type: "session", id: childId, parentSession: parentFile };
    const taskSummary = `<task-result id="${allocatedId}" agent="worker" status="completed" duration="1s">\n\n<output>\nchild answer\n</output>\n</task-result>`;
    const taskCall = { type: "message", id: "task-call-entry", message: {
      role: "assistant", responseId: "task-parent-response", stopReason: "toolUse",
      content: [{ type: "toolCall", id: toolCallId, name: "task", arguments: { agent: "worker" } }],
    } };
    const taskResult = { type: "message", id: "task-result-entry", message: {
      role: "toolResult", toolName: "task", toolCallId, isError: false,
      content: [{ type: "text", text: taskSummary }],
      details: { results: [{ id: allocatedId, exitCode: 0, output: "child answer", truncated: false, aborted: false }] },
    } };
    writeSession(parentFile, parentHeader, [taskCall, taskResult]);
    writeSession(childFile, childHeader, [
      { type: "message", id: "child-terminal-entry", message: {
        role: "assistant", responseId: "child-terminal", stopReason: "stop", content: [{ type: "text", text: "child answer" }],
      } },
      { type: "custom", customType: OMP_RUN_COMPLETION_ENTRY, data: {
        version: 1, runId: `omp:task:${allocatedId}`, parentSessionId: parentId, sessionId: childId, responseId: "child-terminal",
      } },
    ]);

    try {
      const versionExecutable = join(root, "omp-version");
      writeFileSync(versionExecutable, "#!/bin/sh\nprintf 'omp/18.12.6\\n'\n");
      chmodSync(versionExecutable, 0o755);
      process.execPath = versionExecutable;
      process.argv = [versionExecutable, join(root, "node_modules", "@oh-my-pi", "pi-coding-agent", "dist", "cli.js")];
      process.env.TOKENVISOR_API_KEY = "synthetic-key";
      process.env.TOKENVISOR_BASE_URL = "http://127.0.0.1:1/api/v1";
      globalThis.fetch = async () => Response.json({ data: [{ id: "synthetic", type: "chat", capabilities: [] }] });

      const branches = new Map<string, any[]>();
      let appendedId = 0;
      const register = async () => {
        const hooks = new Map<string, Array<(event: any, ctx: any) => unknown>>();
        const lifecycleListeners: Array<(payload: any) => void> = [];
        let providerConfig: any;
        let appendSessionId: string | null = null;
        await tokenVisorProvider({
          on: (name: string, handler: any) => hooks.set(name, [...(hooks.get(name) ?? []), handler]),
          events: { on: (_name: string, handler: any) => { lifecycleListeners.push(handler); return () => {}; }, emit: () => {} },
          registerCommand: () => {},
          registerProvider: (_name: string, config: any) => { providerConfig = config; },
          appendEntry: (customType: string, data: any) => {
            if (!appendSessionId) throw new Error("missing append session in OMP provider fixture");
            const branch = branches.get(appendSessionId);
            if (!branch) throw new Error("missing native branch in OMP provider fixture");
            branch.push({
              type: "custom_message",
              id: `omp-custom-${appendedId++}`,
              parentId: branch.at(-1)?.id,
              customType,
              data,
            });
          },
        } as any);
        return {
          hooks,
          lifecycleListeners,
          providerConfig,
          setAppendSession: (sessionId: string) => { appendSessionId = sessionId; },
        };
      };
      const first = await register();
      const second = await register();
      const invoke = (instance: typeof first, name: string, event: any, context: any) => {
        for (const handler of instance.hooks.get(name) ?? []) handler(event, context);
      };
      const parentBranch: any[] = [taskCall, taskResult];
      branches.set(parentId, parentBranch);
      const parentContext = {
        agent: { kind: "main" },
        sessionManager: {
          getSessionId: () => parentId,
          getSessionFile: () => parentFile,
          getHeader: () => parentHeader,
          getBranch: () => parentBranch,
        },
      };
      invoke(first, "session_start", { reason: "startup" }, parentContext);
      invoke(second, "session_start", { reason: "startup" }, parentContext);
      for (const listener of first.lifecycleListeners) listener({
        status: "started", id: allocatedId, agent: "worker", parentToolCallId: toolCallId,
        detached: false, sessionFile: childFile,
      });

      const state = getOmpRuntimeCaptureState();
      expect(readOmpEarlyTaskDelegation(state, allocatedId, childFile)).toEqual({
        runId: `omp:task:${allocatedId}`, toolCallId, detached: false,
      });
      const filteredResult = {
        ...taskResult.message,
        content: [{ type: "text", text: "FILTERED_AFTER_NATIVE_COMMIT" }],
      };
      const headers = new Headers();
      await second.providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers },
        { messages: [taskCall.message, filteredResult] },
        { sessionId: parentId, headers },
      ).result();
      await Promise.resolve();
      expect(parseAgentMetadata(new Headers((globalThis as any).__OMP_PROVIDER_OPTIONS__?.headers ?? {}).get("x-tokenvisor-agent"))?.depends_on).toEqual([]);
      expect(state.pendingTaskBindingRetirements.get(parentId)?.get("omp-response")?.bindingIds).toEqual([allocatedId]);

      const childContext = {
        sessionManager: {
          getSessionId: () => childId,
          getSessionFile: () => childFile,
          getHeader: () => childHeader,
          getBranch: () => [],
        },
      };
      invoke(second, "session_shutdown", {}, childContext);
      expect(readOmpEarlyTaskDelegation(state, allocatedId, childFile)).not.toBeNull();

      invoke(first, "agent_end", {}, parentContext);
      expect(state.pendingTaskBindingRetirements.get(parentId)?.has("omp-response")).toBe(true);
      expect(readOmpEarlyTaskDelegation(state, allocatedId, childFile)).not.toBeNull();

      parentBranch.push({ type: "message", id: "committed-carrier", message: {
        role: "assistant", responseId: "omp-response", stopReason: "stop", content: [],
      } });
      invoke(first, "agent_end", {}, parentContext);
      expect(state.pendingTaskBindingRetirements.has(parentId)).toBe(false);
      expect(readOmpEarlyTaskDelegation(state, allocatedId, childFile)).toBeNull();

      invoke(first, "session_shutdown", {}, parentContext);
      invoke(second, "session_shutdown", {}, parentContext);
      branches.delete(parentId);

      if (process.env.TOKENVISOR_OMP_CAPTURE_BENCHMARK === "1") {
        await (await import("../scripts/benchmarks/capture.ts")).benchmarkOmpProvider({ root, branches, first, second, invoke, state, writeSession });
      }
    } finally {
      const state = getOmpRuntimeCaptureState();
      clearOmpTaskBindingRetirementsForSession(state, parentId);
      clearOmpEarlyTaskBindingsForSession(state, parentId);
      process.execPath = oldExecPath;
      process.argv = oldArgv;
    }
  });
});

if (process.env.TOKENVISOR_PI_FOREGROUND_BENCHMARK === "1") {
  test("warm foreground handling delta against the existing identity-capture baseline", async () => {
    await (await import("../scripts/benchmarks/capture.ts")).benchmarkPiForeground({ createHarness, writeSession });
  });
}
