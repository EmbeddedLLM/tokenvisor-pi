import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "crypto";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import { commitPiResultConsumption, derivePiIdentity, filterPiDependenciesForRequestInput, makePiCompactionRecord, PI_COMPACTION_ENTRY, PI_RESULT_CONSUMPTION_ENTRY, PI_RUN_COMPLETION_ENTRY, readPiOwnedBranch, readPiPersistedConsumedDependencies, readPiPersistedConsumedRunIds, readPiRunCompletion, readPiSubagentDelegation, readPiUnconsumedRunDependencies, resolveCompactionFrontier, resolvePiCompactionRequestInput } from "../src/capture/pi.ts";
import { encodeAgentMetadata, parseAgentMetadata } from "../src/capture/shared.ts";
import { loadPiRequiredExtensionsApi } from "../src/capture/pi-runtime.ts";
import tokenVisorProvider from "../extensions/tokenvisor-provider.ts";

const roots: string[] = [];
function piEnvelopeForTest(
  ctx: Parameters<typeof derivePiIdentity>[0],
  streamId: string,
  input?: Parameters<typeof derivePiIdentity>[1],
) {
  return encodeAgentMetadata(derivePiIdentity(ctx, input), streamId);
}
const savedEnv = { ...process.env };
const savedArgv = process.argv;
afterEach(() => {
  for (const key of Object.keys(process.env)) if (!(key in savedEnv)) delete process.env[key];
  Object.assign(process.env, savedEnv);
  process.argv = savedArgv;
  delete (globalThis as any).__PI_TEST_STREAM_RESULT__;
  delete (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__;
  delete (globalThis as any).__PI_TEST_CAPTURED_CONTEXT__;
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-workflow-test-"));
  roots.push(root);
  process.env.PI_SUBAGENTS_TEMP_ROOT = root;
  const parentFile = join(root, "parent.jsonl");
  const parentEntries = [
    { id: "parent-a", type: "message", message: { role: "assistant", responseId: "parent-response", content: [{ type: "toolCall", name: "subagent", id: "workflow-call", arguments: { workflow: true, async: false } }] } },
    { id: "parent-b", type: "message", message: { role: "toolResult", toolCallId: "workflow-call", details: {
      mode: "workflow", runId: "workflow-run",
      workflowChildren: { version: 1, workflowRunId: "workflow-run", parentToolCallId: "workflow-call", inventoryComplete: false, workflowState: "running", children: [] },
    } } },
  ];
  const writeSession = (file: string, id: string, entries: unknown[], parent?: string) => writeFileSync(file, [
    { type: "session", id, ...(parent ? { parentSession: parent } : {}) }, ...entries,
  ].map((value) => JSON.stringify(value)).join("\n") + "\n");
  writeSession(parentFile, "parent", parentEntries);
  function context(id: string, file: string, branch: unknown[], parent?: string) {
    return { sessionManager: {
      getSessionId: () => id, getSessionFile: () => file,
      getHeader: () => ({ id, ...(parent ? { parentSession: parent } : {}) }),
      getBranch: () => branch,
    } } as any;
  }
  function status(runId: string, value: unknown) {
    const file = join(root, "async-subagent-runs", runId, "status.json");
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, JSON.stringify(value));
    return file;
  }
  return { root, parentFile, parentEntries, writeSession, context, status };
}

function addWorkflowDelivery(f: ReturnType<typeof fixture>, branch: any[], workflowRunId: string, suffix: string, childCount = 3) {
  const toolCallId = `workflow-call-${suffix}`;
  const children = Array.from({ length: childCount }, (_, index) => `task-${index + 1}`).map((childKey, index) => {
    const runId = `child-${suffix}-${index + 1}`;
    const sessionId = `child-session-${suffix}-${index + 1}`;
    const responseId = `child-response-${suffix}-${index + 1}`;
    const output = `child-output-${suffix}-${index + 1}`;
    const sessionFile = join(f.root, `${runId}.jsonl`);
    f.writeSession(sessionFile, sessionId, [
      { type: "message", id: `assistant-${runId}`, message: { role: "assistant", responseId, stopReason: "stop", content: [{ type: "text", text: output }] } },
      { type: "custom", customType: PI_RUN_COMPLETION_ENTRY, data: { version: 1, runId, sessionId, responseId } },
    ], f.parentFile);
    f.status(runId, {
      lifecycleArtifactVersion: 3,
      runId,
      sessionId: f.parentFile,
      parentWorkflowRunId: workflowRunId,
      workflowKey: childKey,
      mode: "single",
      state: "complete",
      sessionFile,
      steps: [{ sessionFile }],
    });
    return { workflowKey: childKey, runId, status: "completed", sessionId, responseId, output };
  });
  const childRuns = children.map(({ workflowKey, runId, status }) => ({ workflowKey, runId, status }));
  const childOutputs = children.map(({ workflowKey, runId, status }) => ({ workflowKey, runId, status, outputArtifactPath: `output/${runId}.txt` }));
  const call = { type: "message", id: `call-${suffix}`, message: { role: "assistant", responseId: `launch-response-${suffix}`, content: [{
    type: "toolCall", name: "subagent", id: toolCallId, arguments: { workflow: true },
  }] } };
  const ack = { type: "message", id: `ack-${suffix}`, message: { role: "toolResult", toolCallId, details: {
    mode: "workflow", runId: workflowRunId,
    workflowChildren: { version: 1, workflowRunId, parentToolCallId: toolCallId, inventoryComplete: false, workflowState: "running", children: [] },
  } } };
  branch.push(call, ack);
  for (const child of children) {
    branch.push({
      type: "custom_message",
      id: `notice-${child.runId}`,
      customType: "subagent-incremental-child-notify",
      details: { childKey: child.workflowKey, outcome: "completed" },
      content: `Workflow child completed: **${child.workflowKey}**\nWorkflow run: ${workflowRunId}\nChild run: ${child.runId}\nOutput: output/${child.runId}.txt\nStatus: workflow still running`,
    });
  }
  const childOutputBlocks = children.map((child) => [
    `- key=${child.workflowKey} run=${child.runId} status=completed`,
    `  Saved output: unavailable`,
    `  Output artifact (retention-managed): output/${child.runId}.txt`,
    `  Preview:`,
    `    | ${child.output}`,
  ].join("\n")).join("\n");
  const run = { runId: workflowRunId, workflowRunId, status: "completed", childRuns, childOutputs };
  branch.push({
    type: "custom_message",
    id: `aggregate-${suffix}`,
    customType: "subagent-notify",
    details: { runs: [run] },
    content: `Background task completed: **workflow**\nWorkflow receipt: workflow-receipt.json\n\nWorkflow run: ${workflowRunId}\nChild runs: ${childRuns.map((child) => `${child.workflowKey}=${child.runId} (completed)`).join(", ")}\n\nChild outputs:\n${childOutputBlocks}`,
  });
  f.status(workflowRunId, {
    runId: workflowRunId, toolCallId, sessionId: f.parentFile, mode: "workflow", state: "complete",
    workflowChildren: { version: 1, workflowRunId, parentToolCallId: toolCallId, inventoryComplete: true,
      workflowState: "completed", children: children.map(({ workflowKey, runId }) => ({ childId: workflowKey, runId, state: "completed" })) },
  });
  return { toolCallId, workflowRunId, children, run };
}

function addDirectDelivery(f: ReturnType<typeof fixture>, branch: any[], suffix: string, includeCompletionMarker = true) {
  const runId = `direct-run-${suffix}`;
  const toolCallId = `direct-call-${suffix}`;
  const sessionId = `direct-session-${suffix}`;
  const responseId = `direct-response-${suffix}`;
  const output = `direct-output-${suffix}`;
  const sessionFile = join(f.root, `${runId}.jsonl`);
  const asyncDir = join(f.root, "async-subagent-runs", runId);
  const childEntries = [
    { type: "message", id: `assistant-${runId}`, message: { role: "assistant", responseId, stopReason: "stop", content: [{ type: "text", text: output }] } },
    ...(includeCompletionMarker ? [{ type: "custom", customType: PI_RUN_COMPLETION_ENTRY, data: { version: 1, runId, sessionId, responseId } }] : []),
  ];
  f.writeSession(sessionFile, sessionId, childEntries, f.parentFile);
  f.status(runId, {
    lifecycleArtifactVersion: 3,
    runId,
    sessionId: f.parentFile,
    toolCallId,
    mode: "single",
    state: "complete",
    pid: process.pid,
    sessionFile,
    steps: [{ sessionFile }],
  });
  branch.push({ type: "message", id: `call-${suffix}`, message: {
    role: "assistant", responseId: `launch-${suffix}`, content: [{
      type: "toolCall", name: "subagent", id: toolCallId, arguments: { agent: "worker", task: "Review the change" },
    }],
  } });
  const notice = {
    type: "custom_message",
    id: `notice-${runId}`,
    customType: "subagent-notify",
    details: { runs: [{ runId, status: "completed", agent: "worker", asyncDir, sessionLabel: "Session file", sessionValue: sessionFile }] },
    content: `Background task completed: **worker**\n\nworker:\n${output}\n\nRetention-managed async directory: ${asyncDir}\n\nSession file: ${sessionFile}\n\nParent action: Review the result`,
  };
  branch.push(notice);
  return { runId, sessionId, responseId, output, sessionFile, asyncDir, notice };
}

function addNativeForegroundBatch(
  f: ReturnType<typeof fixture>,
  branch: any[],
  batch: number,
  output: string,
) {
  const toolCallId = `foreground-call-${batch}`;
  const workflowRunId = toolCallId;
  const children = [1, 2, 3].map((index) => ({
    key: `task-${index}`,
    runId: `foreground-child-${index}-batch-${batch}`,
    sessionId: `continued-child-session-${index}`,
    responseId: `foreground-response-${index}-batch-${batch}`,
  }));
  const call = {
    type: "message",
    id: `foreground-call-entry-${batch}`,
    message: {
      role: "assistant",
      responseId: `foreground-launch-response-${batch}`,
      content: [{
        type: "toolCall",
        name: "subagent",
        id: toolCallId,
        arguments: {
          workflow: "parallel",
          async: false,
          args: { tasks: children.map((child) => ({ agent: "worker", task: `Run ${child.key}` })) },
        },
      }],
    },
  };
  const workflowValue = batch === 1
    ? {
      ok: true,
      children: children.map((child) => ({ key: child.key, agent: "worker", ok: true, output })),
    }
    : children.map((child) => ({
      key: child.key,
      ok: true,
      asyncDir: `fixture://workflow/${child.runId}`,
      agent: "worker",
      runId: child.runId,
      output,
      resolvedContext: "fresh",
      results: [{ index: 0, runId: child.runId, finalOutput: output, outputState: "present" }],
    }));
  const inventory = {
    version: 1,
    workflowRunId,
    parentToolCallId: toolCallId,
    inventoryComplete: true,
    workflowState: "completed",
    children: children.map((child) => ({ childId: child.key, runId: child.runId, state: "completed" })),
  };
  branch.push(call, {
    type: "message",
    id: `foreground-result-${batch}`,
    message: {
      role: "toolResult",
      toolCallId,
      details: {
        mode: "workflow",
        runId: workflowRunId,
        results: children.map((child) => ({
          index: 0,
          workflowKey: child.key,
          runId: child.runId,
          agent: "worker",
          sessionFile: join(f.root, `${child.sessionId}.jsonl`),
          exitCode: 0,
          finalOutput: output,
          outputState: "present",
        })),
        workflowChildren: inventory,
        workflow: {
          value: workflowValue,
          trace: children.map((child) => ({ operation: "run", key: child.key, state: "completed", runId: child.runId })),
          emits: [{ event: "children-settled" }],
          console: [{ level: "log", text: "foreground workflow settled" }],
        },
      },
      content: `Run fan-out: 3/64 used, 61 remaining\nWorkflow completed.\n\nReturn:\n${JSON.stringify(workflowValue, null, 2)}\n\nEmitted:\n${JSON.stringify({ event: "children-settled" }, null, 2)}\n\nConsole:\n[log] foreground workflow settled\n\nCall trace:\n${children.map((child) => `- run ${child.key}: completed (${child.runId})`).join("\n")}`,
    },
  });

  for (const child of children) {
    const childFile = join(f.root, `${child.sessionId}.jsonl`);
    const entries = batch === 1
      ? []
      : readFileSync(childFile, "utf8").trim().split("\n").map((line) => JSON.parse(line)).slice(1);
    entries.push(
      {
        type: "message",
        id: `assistant-${child.runId}`,
        message: {
          role: "assistant",
          responseId: child.responseId,
          stopReason: "stop",
          content: [{ type: "text", text: output }],
        },
      },
      {
        type: "custom",
        customType: PI_RUN_COMPLETION_ENTRY,
        data: { version: 1, runId: child.runId, sessionId: child.sessionId, responseId: child.responseId },
      },
    );
    f.writeSession(childFile, child.sessionId, entries, f.parentFile);
    f.status(child.runId, {
      lifecycleArtifactVersion: 3,
      runId: child.runId,
      sessionId: f.parentFile,
      parentWorkflowRunId: workflowRunId,
      workflowKey: child.key,
      mode: "single",
      state: "complete",
      sessionFile: childFile,
      steps: [{ sessionFile: childFile }],
    });
  }
  return { workflowRunId, children };
}

function commitWorkflowDelivery(
  f: ReturnType<typeof fixture>,
  branch: any[],
  ctx: any,
  workflow: ReturnType<typeof addWorkflowDelivery>,
  index: number,
  label: string,
) {
  f.writeSession(f.parentFile, "parent", branch);
  const dependencies = readPiUnconsumedRunDependencies(ctx);
  expect(dependencies).toEqual(workflow.children.map((child) => ({
    session_id: `pi:session:${child.sessionId}`,
    response_id: child.responseId,
    run_id: child.runId,
  })));
  const responseId = `${label}-consumer-response-${index}`;
  branch.push({ type: "message", id: `${label}-consumer-${index}`, message: {
    role: "assistant", responseId, content: [{ type: "text", text: "Consumed the current workflow outputs." }],
  } });
  expect(commitPiResultConsumption(
    ctx,
    `${label}-consumer-stream-${index}`,
    responseId,
    dependencies!,
    (customType, data) => branch.push({ type: "custom", customType, data }),
  )).toBe(true);
  f.writeSession(f.parentFile, "parent", branch);
}

async function setupProviderHookHarness(harnessVersion = "1.4.0", runnerVersion = "0.83.0") {
  const f = fixture();
  const host = join(f.root, "node_modules", "@earendil-works", "pi-coding-agent");
  const ai = join(host, "node_modules", "@earendil-works", "pi-ai");
  const pkg = join(f.root, "npm", "node_modules", "pi-subagents");
  for (const dir of [join(host, "dist"), join(ai, "dist", "api"), join(pkg, "bundle")]) mkdirSync(dir, { recursive: true });
  writeFileSync(join(host, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: harnessVersion }));
  writeFileSync(join(host, "dist", "cli.js"), "");
  writeFileSync(join(host, "dist", "index.js"), `
    export function convertToLlm(messages) { return messages; }
    export function serializeConversation(messages) {
      return messages.map((message) => (message.content ?? []).map((part) => part.text ?? "").join("")).join("\\n");
    }
    export function sessionEntryToContextMessages(entry) {
      return [{ role: "user", content: [{ type: "text", text: entry.summary }] }];
    }
  `);
  writeFileSync(join(ai, "package.json"), JSON.stringify({ name: "@earendil-works/pi-ai", version: "1.4.0", type: "module" }));
  writeFileSync(join(ai, "dist", "api", "openai-completions.js"), 'export function streamSimple(model, context, options) { globalThis.__PI_TEST_CAPTURED_OPTIONS__ = options; globalThis.__PI_TEST_CAPTURED_CONTEXT__ = context; return { result: () => Promise.resolve(globalThis.__PI_TEST_STREAM_RESULT__) }; }');
  writeFileSync(join(pkg, "package.json"), JSON.stringify({ name: "pi-subagents", version: runnerVersion, type: "module", exports: { "./required-child-extensions": "./bundle/required-child-extensions.js" } }));
  const moduleFile = join(pkg, "bundle", "required-child-extensions.js");
  writeFileSync(moduleFile, 'export const calls = []; export function registerRequiredChildExtensions(input) { calls.push(input); return { dispose() { calls.push("disposed"); } }; }');
  process.env.PI_CODING_AGENT_DIR = f.root;
  process.env.TOKENVISOR_API_KEY = "synthetic-key";
  process.env.TOKENVISOR_BASE_URL = "http://127.0.0.1:1/api/v1";
  delete process.env.PI_SUBAGENT_CHILD;
  delete process.env.PI_SUBAGENT_PARENT_SESSION;
  process.argv = [process.execPath, join(host, "dist", "cli.js")];
  const originalFetch = globalThis.fetch;
  const hooks = new Map<string, (event: any, ctx: any) => unknown>();
  let providerConfig: any;
  let branch: any[] = [];
  let appendBranch: any[] | null = branch;
  try {
    globalThis.fetch = async () => Response.json({ data: [{ id: "synthetic", type: "chat", capabilities: [] }] });
    await tokenVisorProvider({
      on: (name: string, handler: any) => hooks.set(name, handler),
      registerCommand: () => {},
      registerProvider: (_name: string, config: any) => { providerConfig = config; },
      appendEntry: (customType: string, data: any) => appendBranch?.push({
        type: "custom_message",
        id: `provider-entry-${appendBranch.length + 1}`,
        parentId: appendBranch.at(-1)?.id,
        customType,
        data,
      }),
    } as any);
    branch = [];
    appendBranch = branch;
    f.writeSession(f.parentFile, "parent", branch);
    const ctx = f.context("parent", f.parentFile, branch);
    const api = await import(moduleFile);
    return {
      f,
      hooks,
      providerConfig,
      api,
      branch,
      ctx,
      cleanup() { globalThis.fetch = originalFetch; },
    };
  } catch (error) {
    globalThis.fetch = originalFetch;
    throw error;
  }
}

function piSubagentsNoticeContextText(content: string): string {
  return `<pi_subagents_notice type="subagent-notify" origin="automated message from the pi-subagents extension, not typed by the user">\n${content}\n</pi_subagents_notice>`;
}

describe("Pi inherited history and workflow provenance", () => {
  test("reconciles three foreground workflow batches across continued children, JSON returns, and compaction", () => {
    const f = fixture();
    const branch: any[] = [];
    const output = `${"same returned output with escaped JSON characters: \"quoted\" \\ path\n".repeat(40)}🌍 end`;
    expect(Buffer.byteLength(output, "utf8")).toBeGreaterThan(2_000);

    const first = addNativeForegroundBatch(f, branch, 1, output);
    f.writeSession(f.parentFile, "parent", branch);
    const firstDependencies = readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch));
    expect(firstDependencies).toEqual([
      { session_id: "pi:session:continued-child-session-1", response_id: "foreground-response-1-batch-1", run_id: "foreground-child-1-batch-1" },
      { session_id: "pi:session:continued-child-session-2", response_id: "foreground-response-2-batch-1", run_id: "foreground-child-2-batch-1" },
      { session_id: "pi:session:continued-child-session-3", response_id: "foreground-response-3-batch-1", run_id: "foreground-child-3-batch-1" },
    ]);
    expect(new Set(first.children.map((child) => child.runId)).size).toBe(3);
    branch.push({ id: "foreground-consumer-1", type: "message", message: { role: "assistant", responseId: "foreground-consumer-response-1" } });
    expect(commitPiResultConsumption(
      f.context("parent", f.parentFile, branch),
      "foreground-consumer-stream-1",
      "foreground-consumer-response-1",
      firstDependencies!,
      (customType, data) => branch.push({ type: "custom", customType, data }),
    )).toBe(true);
    branch.push(
      { id: "foreground-compaction-1", type: "compaction", firstKeptEntryId: "foreground-result-1", fromHook: false },
      { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord(
        "foreground-compaction-1", ["foreground-summary-stream-1"], true,
        [first.workflowRunId], false, firstDependencies!,
      ) },
    );
    f.writeSession(f.parentFile, "parent", branch);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toEqual([]);

    const second = addNativeForegroundBatch(f, branch, 2, output);
    f.writeSession(f.parentFile, "parent", branch);
    const secondDependencies = readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch));
    expect(secondDependencies).toEqual([
      { session_id: "pi:session:continued-child-session-1", response_id: "foreground-response-1-batch-2", run_id: "foreground-child-1-batch-2" },
      { session_id: "pi:session:continued-child-session-2", response_id: "foreground-response-2-batch-2", run_id: "foreground-child-2-batch-2" },
      { session_id: "pi:session:continued-child-session-3", response_id: "foreground-response-3-batch-2", run_id: "foreground-child-3-batch-2" },
    ]);
    expect(new Set(second.children.map((child) => child.runId)).size).toBe(3);
    branch.push({ id: "foreground-consumer-2", type: "message", message: { role: "assistant", responseId: "foreground-consumer-response-2" } });
    expect(commitPiResultConsumption(
      f.context("parent", f.parentFile, branch),
      "foreground-consumer-stream-2",
      "foreground-consumer-response-2",
      secondDependencies!,
      (customType, data) => branch.push({ type: "custom", customType, data }),
    )).toBe(true);

    const third = addNativeForegroundBatch(f, branch, 3, output);
    f.writeSession(f.parentFile, "parent", branch);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toEqual([
      { session_id: "pi:session:continued-child-session-1", response_id: "foreground-response-1-batch-3", run_id: "foreground-child-1-batch-3" },
      { session_id: "pi:session:continued-child-session-2", response_id: "foreground-response-2-batch-3", run_id: "foreground-child-2-batch-3" },
      { session_id: "pi:session:continued-child-session-3", response_id: "foreground-response-3-batch-3", run_id: "foreground-child-3-batch-3" },
    ]);
    expect(new Set(third.children.map((child) => child.runId)).size).toBe(3);
  });

  test("rejects ambiguous native workflow completions and incomplete or altered JSON returns", () => {
    const invalidDelivery = (mutate: (f: ReturnType<typeof fixture>, branch: any[], childFile: string, result: any) => void) => {
      const f = fixture();
      const branch: any[] = [];
      const workflow = addNativeForegroundBatch(f, branch, 1, "repeated child output");
      const childFile = join(f.root, `${workflow.children[0]!.sessionId}.jsonl`);
      const result = branch.find((entry) => entry.id === "foreground-result-1");
      mutate(f, branch, childFile, result);
      f.writeSession(f.parentFile, "parent", branch);
      return readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch));
    };

    expect(invalidDelivery((_f, _branch, childFile) => {
      const entries = readFileSync(childFile, "utf8").trim().split("\n").map((line) => JSON.parse(line)).slice(1);
      entries.push({ ...entries.at(-1) });
      _f.writeSession(childFile, "continued-child-session-1", entries, _f.parentFile);
    })).toBeNull();
    expect(invalidDelivery((_f, _branch, childFile) => {
      const entries = readFileSync(childFile, "utf8").trim().split("\n").map((line) => JSON.parse(line)).slice(1);
      entries.push({
        ...entries.at(-1),
        data: { ...entries.at(-1).data, responseId: "conflicting-response-id" },
      });
      _f.writeSession(childFile, "continued-child-session-1", entries, _f.parentFile);
    })).toBeNull();
    expect(invalidDelivery((_f, _branch, childFile) => {
      const entries = readFileSync(childFile, "utf8").trim().split("\n").map((line) => JSON.parse(line)).slice(1);
      _f.writeSession(childFile, "continued-child-session-1", entries.slice(0, -1), _f.parentFile);
    })).toBeNull();
    expect(invalidDelivery((_f, _branch, childFile) => {
      const entries = readFileSync(childFile, "utf8").trim().split("\n").map((line) => JSON.parse(line)).slice(1);
      entries.splice(1, 0, {
        ...entries[0],
      });
      _f.writeSession(childFile, "continued-child-session-1", entries, _f.parentFile);
    })).toBeNull();
    expect(invalidDelivery((_f, _branch, _childFile, result) => {
      result.message.content = 'Return: {"ok":true,"children":[{"key":"task-1"';
    })).toBeNull();
    expect(invalidDelivery((_f, _branch, _childFile, result) => {
      result.message.content = "NATIVE_OUTPUT_WITHHELD";
    })).toBeNull();
    expect(invalidDelivery((_f, _branch, _childFile, result) => {
      result.message.content = "Workflow completed.\n\nReturn:\nrepeated child output";
    })).toBeNull();
    expect(invalidDelivery((_f, _branch, _childFile, result) => {
      result.message.details.workflowChildren.children[0].childId = "wrong-key";
    })).toBeNull();
    expect(invalidDelivery((_f, _branch, _childFile, result) => {
      result.message.details.workflowChildren.children[0].runId = "wrong-run";
    })).toBeNull();
    expect(invalidDelivery((_f, _branch, _childFile, result) => {
      const returnStart = result.message.content.indexOf("Return:\n") + "Return:\n".length;
      const emittedStart = result.message.content.indexOf("\n\nEmitted:", returnStart);
      const callTraceStart = result.message.content.indexOf("\n\nCall trace:", returnStart);
      const valueEnd = emittedStart < 0 ? callTraceStart : emittedStart;
      const valueText = result.message.content.slice(returnStart, valueEnd);
      const parsed = JSON.parse(valueText);
      parsed.children[0].output += " transformed";
      result.message.content = `${result.message.content.slice(0, returnStart)}${JSON.stringify(parsed, null, 2)}${result.message.content.slice(valueEnd)}`;
    })).toBeNull();
  });

  test("registered provider binds only the exact foreground workflow result present in parent input", async () => {
    const harness = await setupProviderHookHarness("1.4.7", "0.84.0");
    const { f, hooks, providerConfig, branch, ctx } = harness;
    try {
      addNativeForegroundBatch(f, branch, 1, "exact structured child output");
      f.writeSession(f.parentFile, "parent", branch);
      const beforeProviderHeaders = hooks.get("before_provider_headers");
      expect(typeof beforeProviderHeaders).toBe("function");
      if (typeof beforeProviderHeaders !== "function") return;
      const capture = async (messages: any[], responseId: string) => {
        const headers = new Headers();
        beforeProviderHeaders({ headers }, ctx);
        (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
          responseId,
          stopReason: "stop",
          content: [{ type: "text", text: "Parent response" }],
        };
        await providerConfig.streamSimple(
          { id: "synthetic", api: "openai-completions", headers },
          { messages },
          { headers },
        ).result();
        await Promise.resolve();
        return parseAgentMetadata(new Headers(
          (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers,
        ).get("x-tokenvisor-agent"));
      };
      const parentMessages = branch
        .filter((entry) => entry.type === "message")
        .map((entry) => entry.message);
      const verified = await capture(parentMessages, "workflow-input-response-1");
      expect(verified?.source).toMatchObject({ harness: "pi", harness_version: "1.4.7", runner_version: "0.84.0" });
      expect(verified?.depends_on).toEqual([
        { session_id: "pi:session:continued-child-session-1", response_id: "foreground-response-1-batch-1", run_id: "foreground-child-1-batch-1" },
        { session_id: "pi:session:continued-child-session-2", response_id: "foreground-response-2-batch-1", run_id: "foreground-child-2-batch-1" },
        { session_id: "pi:session:continued-child-session-3", response_id: "foreground-response-3-batch-1", run_id: "foreground-child-3-batch-1" },
      ]);
      expect(verified?.session.root_id).toBe("pi:session:parent");

      const filteredMessages = parentMessages.map((message) => message.role === "toolResult"
        ? { ...message, content: "NATIVE_OUTPUT_WITHHELD" }
        : message);
      const filtered = await capture(filteredMessages, "workflow-input-response-2");
      expect(filtered?.depends_on).toEqual([]);
      expect(filtered?.session.root_id).toBe("pi:session:parent");
    } finally {
      harness.cleanup();
    }
  });

  test("commits all batches and resolves retained delivery across two compaction cuts", async () => {
    const expectedByBatch = [
      [
        { session_id: "pi:session:continued-child-session-1", response_id: "foreground-response-1-batch-1", run_id: "foreground-child-1-batch-1" },
        { session_id: "pi:session:continued-child-session-2", response_id: "foreground-response-2-batch-1", run_id: "foreground-child-2-batch-1" },
        { session_id: "pi:session:continued-child-session-3", response_id: "foreground-response-3-batch-1", run_id: "foreground-child-3-batch-1" },
      ],
      [
        { session_id: "pi:session:continued-child-session-1", response_id: "foreground-response-1-batch-2", run_id: "foreground-child-1-batch-2" },
        { session_id: "pi:session:continued-child-session-2", response_id: "foreground-response-2-batch-2", run_id: "foreground-child-2-batch-2" },
        { session_id: "pi:session:continued-child-session-3", response_id: "foreground-response-3-batch-2", run_id: "foreground-child-3-batch-2" },
      ],
      [
        { session_id: "pi:session:continued-child-session-1", response_id: "foreground-response-1-batch-3", run_id: "foreground-child-1-batch-3" },
        { session_id: "pi:session:continued-child-session-2", response_id: "foreground-response-2-batch-3", run_id: "foreground-child-2-batch-3" },
        { session_id: "pi:session:continued-child-session-3", response_id: "foreground-response-3-batch-3", run_id: "foreground-child-3-batch-3" },
      ],
    ];

    for (const cutoff of ["third-launch", "after-all"] as const) {
      const harness = await setupProviderHookHarness("1.4.7", "0.84.0");
      const { f, hooks, providerConfig, branch, ctx } = harness;
      try {
        for (const batch of [1, 2, 3]) {
          addNativeForegroundBatch(
            f,
            branch,
            batch,
            "same output with quote \" slash \\ newline\nand Unicode 🌍",
          );
          f.writeSession(f.parentFile, "parent", branch);
          const headers = new Headers();
          hooks.get("before_provider_headers")!({ headers }, ctx);
          const candidate = parseAgentMetadata(headers.get("x-tokenvisor-agent"));
          const expected = expectedByBatch[batch - 1]!;
          expect(candidate?.depends_on).toEqual(expected);
          expect(candidate?.source).toMatchObject({
            harness: "pi",
            harness_version: "1.4.7",
            runner_version: "0.84.0",
          });

          const responseId = `foreground-committed-parent-response-${batch}`;
          (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
            responseId,
            stopReason: "stop",
            content: [{ type: "text", text: `Consumed workflow batch ${batch}.` }],
          };
          await providerConfig.streamSimple(
            { id: "synthetic", api: "openai-completions", headers },
            { messages: branch.filter((entry) => entry.type === "message").map((entry) => entry.message) },
            { headers },
          ).result();
          await Promise.resolve();
          const delivered = parseAgentMetadata(new Headers(
            (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers,
          ).get("x-tokenvisor-agent"));
          expect(delivered?.depends_on).toEqual(expected);
          branch.push({
            id: `${responseId}-entry`,
            type: "message",
            message: {
              role: "assistant",
              responseId,
              stopReason: "stop",
              content: [{ type: "text", text: `Consumed workflow batch ${batch}.` }],
            },
          });
          hooks.get("agent_end")!({
            messages: [{ role: "assistant", responseId, stopReason: "stop" }],
          }, ctx);
          f.writeSession(f.parentFile, "parent", branch);
        }

        const resultRecords = branch.filter(
          (entry) => entry.customType === PI_RESULT_CONSUMPTION_ENTRY,
        );
        expect(resultRecords).toHaveLength(3);
        expect(resultRecords.map((entry) => entry.data.dependencies)).toEqual(expectedByBatch);
        expect(new Set(expectedByBatch.flat().map((dependency) => dependency.run_id)).size).toBe(9);
        expect(readPiUnconsumedRunDependencies(ctx)).toEqual([]);

        const retainedUserEntry = {
          id: `bundle11-retained-user-${cutoff}`,
          type: "message",
          message: { role: "user", content: [{ type: "text", text: "Retained continuation boundary." }] },
        };
        branch.push(retainedUserEntry);
        const retainedBoundary = cutoff === "third-launch"
          ? branch.find((entry) => entry.id === "foreground-call-entry-3")
          : retainedUserEntry;
        expect(retainedBoundary).toBeDefined();
        const retainedDependencies = cutoff === "third-launch" ? expectedByBatch[2]! : [];
        const retainedRunIds = retainedDependencies.map((dependency) => dependency.run_id);

        f.writeSession(f.parentFile, "parent", branch);
        hooks.get("session_before_compact")!({ preparation: {
          firstKeptEntryId: retainedBoundary.id,
          messagesToSummarize: [],
          isSplitTurn: false,
        } }, ctx);
        const compactionEntry = {
          id: `bundle11-mechanical-compaction-${cutoff}`,
          type: "compaction",
          summary: "Cliff retained the continuation boundary.",
          firstKeptEntryId: retainedBoundary.id,
          fromHook: true,
        };
        branch.push(compactionEntry);
        f.writeSession(f.parentFile, "parent", branch);
        const capturedOptionsBeforeCompaction = (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__;
        hooks.get("session_compact")!({ fromExtension: true, compactionEntry }, ctx);
        const compactionRecord = branch.find((entry) =>
          entry.customType === PI_COMPACTION_ENTRY && entry.data?.compactionEntryId === compactionEntry.id,
        );
        expect(compactionRecord?.data).toMatchObject({
          available: true,
          provenNone: true,
          contributors: [],
          consumedRunIds: retainedRunIds,
          consumedDependencies: retainedDependencies,
        });
        expect(readPiPersistedConsumedDependencies(ctx, retainedBoundary.id)).toEqual(
          retainedDependencies,
        );
        expect((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__).toBe(capturedOptionsBeforeCompaction);

        const continuationHeaders = new Headers();
        hooks.get("before_provider_headers")!({ headers: continuationHeaders }, ctx);
        const continuationCandidate = parseAgentMetadata(
          continuationHeaders.get("x-tokenvisor-agent"),
        );
        expect(continuationCandidate).toMatchObject({
          session: { id: "pi:session:parent", root_id: "pi:session:parent" },
          depends_on: [],
        });
        const retainedNativeMessages = branch
          .slice(branch.indexOf(retainedBoundary), branch.indexOf(compactionEntry))
          .filter((entry) => entry.type === "message")
          .map((entry) => entry.message);
        const continuationResponse = `bundle11-continuation-response-${cutoff}`;
        (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
          responseId: continuationResponse,
          stopReason: "stop",
          content: [{ type: "text", text: "Continue after mechanical compaction." }],
        };
        await providerConfig.streamSimple(
          { id: "synthetic", api: "openai-completions", headers: continuationHeaders },
          {
            messages: [
              { role: "user", content: [{ type: "text", text: "Cliff retained the continuation boundary." }] },
              ...retainedNativeMessages,
            ],
          },
          { headers: continuationHeaders },
        ).result();
        await Promise.resolve();
        expect(parseAgentMetadata(new Headers(
          (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers,
        ).get("x-tokenvisor-agent"))?.depends_on).toEqual([]);
      } finally {
        harness.cleanup();
      }
    }
  });

  test("finalizes candidate result references against the exact request-local message set", () => {
    const dependency = {
      session_id: "pi:session:child-session",
      response_id: "child-response",
      run_id: "child-run",
    };
    const deliveryText = "Background task completed: **worker**\n\nworker:\n\nexact result";
    const witnesses = [{ dependency, deliveredText: deliveryText }];

    expect(filterPiDependenciesForRequestInput(
      [dependency],
      witnesses,
      [{ role: "user", content: [{ type: "text", text: deliveryText }] }],
    )).toEqual([dependency]);
    expect(filterPiDependenciesForRequestInput(
      [dependency],
      witnesses,
      [{ role: "user", content: [{ type: "text", text: `Earlier context:\n${deliveryText}\nContinue.` }] }],
    )).toBeNull();
    const omittedDependency = { ...dependency, response_id: "other-child-response", run_id: "other-child-run" };
    const finalizedMixed = filterPiDependenciesForRequestInput(
      [dependency, omittedDependency],
      [...witnesses, { dependency: omittedDependency, deliveredText: "omitted native child output" }],
      [{ role: "user", content: [{ type: "text", text: deliveryText }] }],
    );
    expect(finalizedMixed).toBeNull();
    const selectedMixed = filterPiDependenciesForRequestInput(
      [dependency, omittedDependency],
      [...witnesses, { dependency: omittedDependency, deliveredText: "omitted native child output" }],
      [{ role: "user", content: [{ type: "text", text: deliveryText }] }],
      true,
    );
    expect(selectedMixed).toEqual([dependency]);
    expect(selectedMixed?.flatMap((dependency) => dependency.run_id ? [dependency.run_id] : [])).toEqual(["child-run"]);
    expect(filterPiDependenciesForRequestInput(
      [dependency],
      witnesses,
      [{ role: "user", content: [{ type: "text", text: "The pending notice was excluded from this summary." }] }],
    )).toBeNull();
    expect(filterPiDependenciesForRequestInput(
      [dependency],
      witnesses,
      [{ role: "user", content: [{ type: "text", text: "The pending notice was excluded from this summary." }] }],
      true,
    )).toEqual([]);
    expect(filterPiDependenciesForRequestInput(
      [dependency],
      witnesses,
      [{ role: "assistant", content: [{ type: "text", text: deliveryText }] }],
    )).toBeNull();
    expect(filterPiDependenciesForRequestInput([dependency], [], [])).toBeNull();
    expect(filterPiDependenciesForRequestInput([dependency], witnesses, undefined)).toBeNull();

    const compactionStream = { session_id: "pi:session:parent", stream_id: "summary-stream" };
    expect(filterPiDependenciesForRequestInput(
      [compactionStream],
      [{ dependency: compactionStream, deliveredText: "S1 summary" }],
      [{ role: "user", content: [{ type: "text", text: "S2 turn-prefix summary" }] }],
      true,
    )).toEqual([]);
    expect(filterPiDependenciesForRequestInput(
      [compactionStream],
      [{ dependency: compactionStream, deliveredText: "merged S1 and S2 summary" }],
      [{ role: "user", content: [{ type: "text", text: "merged S1 and S2 summary" }] }],
      true,
    )).toEqual([compactionStream]);
    expect(filterPiDependenciesForRequestInput(
      [compactionStream],
      [],
      [{ role: "user", content: [{ type: "text", text: "merged summary" }] }],
    )).toBeNull();

    const foregroundDependency = {
      session_id: "pi:session:fresh-child",
      response_id: "fresh-terminal",
      run_id: "fresh-run",
    };
    const otherDependency = { session_id: "pi:session:other-child", response_id: "other-response", run_id: "other-run" };
    const foregroundText = "OK";
    const foregroundWitness = {
      dependency: foregroundDependency,
      deliveredText: foregroundText,
      toolCallId: "subagent-call-1",
      deliveredOutputSha256: createHash("sha256").update(foregroundText, "utf8").digest("hex"),
    };
    const otherWitness = { dependency: otherDependency, deliveredText: "other valid output" };
    const unrelatedSameText = [
      { role: "user", content: [{ type: "text", text: foregroundText }] },
      { role: "user", content: [{ type: "text", text: "other valid output" }] },
    ];
    expect(filterPiDependenciesForRequestInput(
      [foregroundDependency, otherDependency], [foregroundWitness, otherWitness], unrelatedSameText,
    )).toEqual([otherDependency]);
    expect(filterPiDependenciesForRequestInput(
      [foregroundDependency, otherDependency], [foregroundWitness, otherWitness], [
        { role: "toolResult", toolCallId: "subagent-call-1", content: [{ type: "text", text: "changed" }] },
        ...unrelatedSameText,
      ],
    )).toEqual([otherDependency]);
    expect(filterPiDependenciesForRequestInput(
      [foregroundDependency, otherDependency], [foregroundWitness, otherWitness], [
        { role: "toolResult", toolCallId: "subagent-call-1", content: [{ type: "text", text: foregroundText }] },
        unrelatedSameText[1],
      ],
    )).toEqual([foregroundDependency, otherDependency]);
  });

  test("uses the pinned S1/S2 compaction source selection and the merged continuation summary", () => {
    const renderer = {
      convertToLlm: (messages: unknown[]) => messages,
      serializeConversation: (messages: any[]) => messages.map((message) => message.content?.[0]?.text ?? "").join("\n"),
    };
    const deliveryText = "native child output notice";
    const dependency = { session_id: "pi:session:child", response_id: "child-response", run_id: "child-run" };
    const witness = [{ dependency, deliveredText: deliveryText }];
    const priorSummary = { session_id: "pi:session:parent", stream_id: "prior-compaction-stream" };
    const priorSummaryWitness = [{ dependency: priorSummary, deliveredText: "<previous-summary>\nprior compaction summary\n</previous-summary>" }];
    const preparation = {
      messagesToSummarize: [{ role: "user", content: [{ type: "text", text: deliveryText }] }],
      turnPrefixMessages: [{ role: "user", content: [{ type: "text", text: "later unfinished turn" }] }],
      isSplitTurn: true,
      previousSummary: "prior compaction summary",
    };
    const turnPrefixPrompt = "# Conversation\nlater unfinished turn\n\n# Instructions\nsource prompt";
    const turnPrefixInput = resolvePiCompactionRequestInput(preparation, [{
      role: "user",
      content: [{ type: "text", text: turnPrefixPrompt }],
    }], renderer);
    expect(turnPrefixInput).toEqual(preparation.turnPrefixMessages);
    expect(filterPiDependenciesForRequestInput([dependency], witness, turnPrefixInput, true)).toEqual([]);
    expect(filterPiDependenciesForRequestInput([priorSummary], priorSummaryWitness, turnPrefixInput, true)).toEqual([]);

    const historyPrompt = `<conversation>\n${deliveryText}\n</conversation>\n\n<previous-summary>\nprior compaction summary\n</previous-summary>\n\nsource prompt`;
    const historyInput = resolvePiCompactionRequestInput(preparation, [{
      role: "user",
      content: [{ type: "text", text: historyPrompt }],
    }], renderer);
    expect(historyInput).toHaveLength(2);
    expect(filterPiDependenciesForRequestInput([dependency], witness, historyInput, true)).toEqual([dependency]);
    expect(filterPiDependenciesForRequestInput([priorSummary], priorSummaryWitness, historyInput, true)).toEqual([priorSummary]);
    expect(resolvePiCompactionRequestInput(preparation, [{
      role: "user",
      content: [{ type: "text", text: `<conversation>\n${deliveryText}\n</conversation>\n\nsource prompt` }],
    }], renderer)).toBeNull();
    expect(resolvePiCompactionRequestInput(preparation, [{
      role: "user",
      content: [{ type: "text", text: historyPrompt.replace("prior compaction summary", "different summary") }],
    }], renderer)).toBeNull();
    expect(resolvePiCompactionRequestInput(preparation, [{
      role: "user",
      content: [{ type: "text", text: "not the native source prompt" }],
    }], renderer)).toBeNull();
  });

  test("uses the delivered workflow snapshot for reused child labels and treats notices as status only", () => {
    const f = fixture();
    const branch: any[] = [];
    const first = addWorkflowDelivery(f, branch, "workflow-first", "first");
    f.writeSession(f.parentFile, "parent", branch);

    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch.slice(0, 2)))).toEqual([]);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch.slice(0, 5)))).toEqual([]);
    const firstDependencies = readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch));
    expect(firstDependencies).toEqual(first.children.map((child) => ({
      session_id: `pi:session:${child.sessionId}`,
      response_id: child.responseId,
      run_id: child.runId,
    })));

    branch.push({ type: "message", id: "first-consumer", message: { role: "assistant", responseId: "first-consumer-response", content: [{ type: "text", text: "received" }] } });
    const second = addWorkflowDelivery(f, branch, "workflow-second", "second");
    f.writeSession(f.parentFile, "parent", branch);
    f.status(first.workflowRunId, { runId: first.workflowRunId, sessionId: "other-parent", mode: "workflow", state: "running" });
    f.status(second.workflowRunId, { runId: second.workflowRunId, sessionId: "other-parent", mode: "workflow", state: "running" });
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toEqual(second.children.map((child) => ({
      session_id: `pi:session:${child.sessionId}`,
      response_id: child.responseId,
      run_id: child.runId,
    })));
  });

  test("keeps contradictory delivery evidence unknown instead of joining by child label", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-invalid", "invalid");
    f.writeSession(f.parentFile, "parent", branch);
    const conflicting = branch.map((entry) => entry.customType === "subagent-incremental-child-notify"
      ? { ...entry, details: { ...entry.details, childKey: "task-other" } }
      : entry);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, conflicting))).toBeNull();

    const duplicateLabel = branch.map((entry) => entry.customType === "subagent-incremental-child-notify"
      ? { ...entry, content: `${entry.content}\nWorkflow run: ${workflow.workflowRunId}` }
      : entry);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, duplicateLabel))).toBeNull();

    const wrongMember = branch.map((entry) => entry.customType === "subagent-notify"
      ? { ...entry, details: { runs: [{ ...entry.details.runs[0], childRuns: entry.details.runs[0].childRuns.map((child: any) => child.runId === workflow.children[0].runId ? { ...child, runId: "other-run" } : child) }] } }
      : entry);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, wrongMember))).toBeNull();

    const malformedOutputLabel = branch.map((entry) => entry.customType === "subagent-notify"
      ? { ...entry, content: entry.content.replace("Child outputs:", "Child outputs:\n- key=malformed label run=other-run status=completed") }
      : entry);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, malformedOutputLabel))).toBeNull();
  });

  test("requires exact committed child output and an exact completion marker", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-output-witness", "witness");
    const failedChild = workflow.children[0]!;
    const statusPath = join(f.root, "async-subagent-runs", failedChild.runId, "status.json");
    const status = JSON.parse(readFileSync(statusPath, "utf8"));
    writeFileSync(statusPath, JSON.stringify({ ...status, state: "failed" }));
    const childFile = status.steps[0].sessionFile;
    const childEntries = readFileSync(childFile, "utf8").trim().split("\n")
      .map((line) => JSON.parse(line)).filter((entry) => entry.customType !== PI_RUN_COMPLETION_ENTRY);
    writeFileSync(childFile, `${childEntries.map((entry: unknown) => JSON.stringify(entry)).join("\n")}\n`);

    const notice = branch.find((entry) => entry.customType === "subagent-notify");
    const run = notice.details.runs[0];
    run.childRuns[0].status = "failed";
    run.childOutputs[0].status = "failed";
    notice.content = notice.content
      .replace(`${failedChild.workflowKey}=${failedChild.runId} (completed)`, `${failedChild.workflowKey}=${failedChild.runId} (failed)`)
      .replace(`key=${failedChild.workflowKey} run=${failedChild.runId} status=completed`, `key=${failedChild.workflowKey} run=${failedChild.runId} status=failed`);

    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toBeNull();
    childEntries.push({
      type: "custom",
      customType: PI_RUN_COMPLETION_ENTRY,
      data: { version: 1, runId: failedChild.runId, sessionId: failedChild.sessionId, responseId: failedChild.responseId },
    });
    writeFileSync(childFile, `${childEntries.map((entry: unknown) => JSON.stringify(entry)).join("\n")}\n`);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toEqual(workflow.children.map((child) => ({
      session_id: `pi:session:${child.sessionId}`,
      response_id: child.responseId,
      run_id: child.runId,
    })));

    notice.content = notice.content.replace(`| ${failedChild.output}`, "| a similar but different output");
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toBeNull();
  });

  test("treats markerless IDless output as ambiguous while honoring an exact current-run marker", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-reused-child", "reused-child");
    const child = workflow.children[1]!;
    const statusPath = join(f.root, "async-subagent-runs", child.runId, "status.json");
    const status = JSON.parse(readFileSync(statusPath, "utf8"));
    const childFile = status.steps[0].sessionFile;
    const outputEntry = (id: string, responseId?: string) => ({
      type: "message",
      id,
      message: {
        role: "assistant",
        ...(responseId === undefined ? {} : { responseId }),
        stopReason: "stop",
        content: [{ type: "text", text: child.output }],
      },
    });

    for (const [entryId, responseId] of [
      ["current-run-idless-output", undefined],
      ["current-run-invalid-id-output", "response/invalid"],
    ] as const) {
      f.writeSession(childFile, child.sessionId, [
        outputEntry("old-run-output", "old-run-response"),
        outputEntry(entryId, responseId),
      ], f.parentFile);
      expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toBeNull();
    }

    const currentResponseId = "current-run-response";
    const currentMarker = { type: "custom", customType: PI_RUN_COMPLETION_ENTRY, data: {
      version: 1, runId: child.runId, sessionId: child.sessionId, responseId: currentResponseId,
    } };
    f.writeSession(childFile, child.sessionId, [
      outputEntry("old-run-output", "old-run-response"),
      outputEntry("current-run-output", currentResponseId),
      currentMarker,
    ], f.parentFile);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toEqual(
      workflow.children.map((item) => ({
        session_id: `pi:session:${item.sessionId}`,
        response_id: item === child ? currentResponseId : item.responseId,
        run_id: item.runId,
      })),
    );

    f.writeSession(childFile, child.sessionId, [
      outputEntry("old-run-output", "old-run-response"),
      outputEntry("current-run-output", currentResponseId),
      currentMarker,
      { ...currentMarker },
    ], f.parentFile);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toBeNull();

    f.writeSession(childFile, child.sessionId, [
      outputEntry("old-run-output", "old-run-response"),
      outputEntry("current-run-output", currentResponseId),
    ], f.parentFile);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toBeNull();
  });

  test("retains consumed terminal workflow proof after its async artifacts expire", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-expired", "expired");
    f.writeSession(f.parentFile, "parent", branch);
    const ctx = f.context("parent", f.parentFile, branch);
    const dependencies = readPiUnconsumedRunDependencies(ctx)!;
    const notice = branch.find((entry) => entry.id === "aggregate-expired")!;
    const responseId = "expired-workflow-consumer-response";
    branch.push({ type: "message", id: "expired-workflow-consumer", message: {
      role: "assistant", responseId, content: [{ type: "text", text: "Consumed the workflow results." }],
    } });
    expect(commitPiResultConsumption(ctx, "expired-workflow-consumer-stream", responseId, dependencies,
      (customType, data) => branch.push({ type: "custom", customType, data }))).toBe(true);
    expect(readPiPersistedConsumedDependencies(ctx, notice.id)).toEqual(dependencies);
    for (const child of workflow.children) {
      const statusPath = join(f.root, "async-subagent-runs", child.runId, "status.json");
      const status = JSON.parse(readFileSync(statusPath, "utf8"));
      rmSync(statusPath, { force: true });
      rmSync(status.steps[0].sessionFile, { force: true });
    }

    expect(readPiPersistedConsumedDependencies(ctx, notice.id)).toEqual(dependencies);
  });

  test("requires one complete unambiguous consumer record before reusing retained proof", () => {
    for (const mode of ["partial", "conflicting"] as const) {
      const f = fixture();
      const branch: any[] = [];
      const workflow = addWorkflowDelivery(f, branch, `workflow-expired-${mode}`, `expired-${mode}`);
      f.writeSession(f.parentFile, "parent", branch);
      const ctx = f.context("parent", f.parentFile, branch);
      const dependencies = readPiUnconsumedRunDependencies(ctx)!;
      const notice = branch.find((entry) => entry.id === `aggregate-expired-${mode}`)!;
      const responseId = `expired-${mode}-consumer-response`;
      branch.push({ type: "message", id: `expired-${mode}-consumer`, message: {
        role: "assistant", responseId, content: [{ type: "text", text: "Consumed workflow results." }],
      } });
      const committed = mode === "partial" ? dependencies.slice(0, -1) : dependencies;
      expect(commitPiResultConsumption(ctx, `expired-${mode}-consumer-stream`, responseId, committed,
        (customType, data) => branch.push({ type: "custom", customType, data }))).toBe(true);
      if (mode === "conflicting") {
        const conflictingDependency = { ...dependencies[0]!, response_id: "conflicting-child-response" };
        expect(commitPiResultConsumption(ctx, "conflicting-second-stream", responseId, [conflictingDependency],
          (customType, data) => branch.push({ type: "custom", customType, data }))).toBe(true);
      }
      for (const child of workflow.children) {
        const statusPath = join(f.root, "async-subagent-runs", child.runId, "status.json");
        const status = JSON.parse(readFileSync(statusPath, "utf8"));
        rmSync(statusPath, { force: true });
        rmSync(status.steps[0].sessionFile, { force: true });
      }

      expect(readPiPersistedConsumedDependencies(ctx, notice.id)).toBeNull();
    }
  });

  test("does not clear a retained output after an assistant response without its consumer receipt", () => {
    const f = fixture();
    const branch: any[] = [];
    addWorkflowDelivery(f, branch, "workflow-without-receipt", "without-receipt");
    f.writeSession(f.parentFile, "parent", branch);
    const ctx = f.context("parent", f.parentFile, branch);
    const notice = branch.find((entry) => entry.id === "aggregate-without-receipt")!;
    branch.push({ type: "message", id: "unrecorded-consumer-response-entry", message: {
      role: "assistant", responseId: "unrecorded-consumer-response", content: [{ type: "text", text: "Response without durable receipt." }],
    } });

    expect(readPiPersistedConsumedDependencies(ctx, notice.id)).toBeNull();
  });

  test("retains consumed direct notification proof only for its exact child run", () => {
    const f = fixture();
    const branch: any[] = [];
    const delivery = addDirectDelivery(f, branch, "expired");
    f.writeSession(f.parentFile, "parent", branch);
    const ctx = f.context("parent", f.parentFile, branch);
    const dependencies = readPiUnconsumedRunDependencies(ctx)!;
    expect(dependencies).toEqual([{
      session_id: `pi:session:${delivery.sessionId}`,
      response_id: delivery.responseId,
      run_id: delivery.runId,
    }]);
    const responseId = "direct-consumer-response";
    branch.push({ type: "message", id: "direct-consumer", message: {
      role: "assistant", responseId, content: [{ type: "text", text: "Consumed direct result." }],
    } });
    expect(commitPiResultConsumption(ctx, "direct-consumer-stream", responseId, dependencies,
      (customType, data) => branch.push({ type: "custom", customType, data }))).toBe(true);
    const statusPath = join(f.root, "async-subagent-runs", delivery.runId, "status.json");
    rmSync(statusPath, { force: true });
    rmSync(delivery.sessionFile, { force: true });

    expect(readPiPersistedConsumedDependencies(ctx, delivery.notice.id)).toEqual(dependencies);
  });

  test("keeps an unrelated retained direct run known-empty after a partial run-scoped receipt", () => {
    const f = fixture();
    const branch: any[] = [];
    const first = addDirectDelivery(f, branch, "group-first");
    const second = addDirectDelivery(f, branch, "group-second");
    expect(branch.indexOf(first.notice)).toBeLessThan(branch.indexOf(second.notice));
    const firstCall = branch.find((entry) => entry.id === "call-group-first")!;
    const secondCallIndex = branch.findIndex((entry) => entry.id === "call-group-second");
    const secondCall = branch.splice(secondCallIndex, 1)[0]!;
    firstCall.message.content.push(...secondCall.message.content);
    f.writeSession(f.parentFile, "parent", branch);
    const ctx = f.context("parent", f.parentFile, branch);
    const dependencies = readPiUnconsumedRunDependencies(ctx)!;
    expect(dependencies).toHaveLength(2);
    expect(dependencies.every((dependency) => dependency.run_id !== undefined)).toBe(true);
    const responseId = "direct-group-partial-consumer-response";
    branch.push({ type: "message", id: "direct-group-partial-consumer", message: {
      role: "assistant", responseId, content: [{ type: "text", text: "Consumed one of two direct outputs." }],
    } });
    expect(commitPiResultConsumption(ctx, "direct-group-partial-consumer-stream", responseId, [dependencies[0]!],
      (customType, data) => branch.push({ type: "custom", customType, data }))).toBe(true);

    expect(readPiPersistedConsumedDependencies(ctx, second.notice.id)).toEqual([]);
  });

  test("keeps legacy runless direct receipts unknown when compaction prunes part of the consumer window", () => {
    const f = fixture();
    const branch: any[] = [];
    const first = addDirectDelivery(f, branch, "legacy-group-first");
    const second = addDirectDelivery(f, branch, "legacy-group-second");
    const firstCall = branch.find((entry) => entry.id === "call-legacy-group-first")!;
    const secondCallIndex = branch.findIndex((entry) => entry.id === "call-legacy-group-second");
    const secondCall = branch.splice(secondCallIndex, 1)[0]!;
    firstCall.message.content.push(...secondCall.message.content);
    f.writeSession(f.parentFile, "parent", branch);
    const ctx = f.context("parent", f.parentFile, branch);
    const dependencies = readPiUnconsumedRunDependencies(ctx)!;
    expect(dependencies).toHaveLength(2);

    const responseId = "legacy-direct-group-partial-consumer-response";
    branch.push({ type: "message", id: "legacy-direct-group-partial-consumer", message: {
      role: "assistant", responseId, content: [{ type: "text", text: "Consumed one of two direct outputs." }],
    } });
    const runlessReference = {
      session_id: dependencies[0]!.session_id,
      response_id: dependencies[0]!.response_id,
    };
    expect(commitPiResultConsumption(ctx, "legacy-direct-group-partial-consumer-stream", responseId, [runlessReference],
      (customType, data) => branch.push({ type: "custom", customType, data }))).toBe(true);

    expect(readPiPersistedConsumedDependencies(ctx, second.notice.id)).toBeNull();
  });

  test("matches the pinned custom-message context wrapper and keeps the raw compaction witness", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-context-wrapper", "context-wrapper");
    f.writeSession(f.parentFile, "parent", branch);
    const notice = branch.find((entry) => entry.customType === "subagent-notify");
    const witnesses: Array<{ dependency: any; deliveredText: string }> = [];
    const dependencies = readPiUnconsumedRunDependencies(
      f.context("parent", f.parentFile, branch),
      undefined,
      (dependency, deliveredText) => witnesses.push({ dependency, deliveredText }),
    );
    expect(dependencies).toEqual(workflow.children.map((child) => ({
      session_id: `pi:session:${child.sessionId}`,
      response_id: child.responseId,
      run_id: child.runId,
    })));
    const firstDependency = dependencies![0]!;
    const firstWitnesses = witnesses.filter((witness) => witness.dependency.response_id === firstDependency.response_id);
    expect(new Set(firstWitnesses.map((witness) => witness.deliveredText))).toEqual(new Set([
      notice.content,
      piSubagentsNoticeContextText(notice.content),
    ]));
    expect(filterPiDependenciesForRequestInput(
      [firstDependency],
      firstWitnesses,
      [{ role: "user", content: [{ type: "text", text: piSubagentsNoticeContextText(notice.content) }] }],
    )).toEqual([firstDependency]);
    expect(filterPiDependenciesForRequestInput(
      [firstDependency],
      firstWitnesses,
      [{ role: "user", content: [{ type: "text", text: notice.content }] }],
    )).toEqual([firstDependency]);
    expect(filterPiDependenciesForRequestInput(
      [firstDependency],
      firstWitnesses,
      [{ role: "user", content: [{ type: "text", text: piSubagentsNoticeContextText(notice.content).replace("origin=", "source=") }] }],
    )).toBeNull();
  });

  test("uses only the pinned acceptance-report transform when binding child previews", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-acceptance-output", "acceptance");
    const child = workflow.children[0]!;
    const status = JSON.parse(readFileSync(join(f.root, "async-subagent-runs", child.runId, "status.json"), "utf8"));
    const childEntries = readFileSync(status.sessionFile, "utf8").trim().split("\n").map((line) => JSON.parse(line));
    const response = childEntries.find((entry: any) => entry.message?.responseId === child.responseId);
    response.message.content = [{ type: "text", text: `\n\n${child.output}\n\`\`\`acceptance-report\n{}\n\`\`\`` }];
    writeFileSync(status.sessionFile, `${childEntries.map((entry: unknown) => JSON.stringify(entry)).join("\n")}\n`);

    const notice = branch.find((entry) => entry.customType === "subagent-notify");
    const marker = `- key=${child.workflowKey} run=${child.runId} status=completed\n  Saved output: unavailable\n  Output artifact (retention-managed): output/${child.runId}.txt\n  Preview:\n    | ${child.output}`;
    notice.content = notice.content.replace(marker, marker.replace(`    | ${child.output}`, `    | \n    | \n    | ${child.output}`));
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toEqual(workflow.children.map((item) => ({
      session_id: `pi:session:${item.sessionId}`,
      response_id: item.responseId,
      run_id: item.runId,
    })));

    response.message.content = [{ type: "text", text: `\n\n${child.output}\n\`\`\`json\n{}\n\`\`\`` }];
    writeFileSync(status.sessionFile, `${childEntries.map((entry: unknown) => JSON.stringify(entry)).join("\n")}\n`);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toBeNull();
  });

  test("matches the pinned UTF-8 head truncation for workflow previews", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-preview-truncation", "truncate");
    const child = workflow.children[0]!;
    const fullOutput = "a".repeat(2_100);
    const marker = "...[preview truncated]";
    const headLength = 2_000 - Buffer.byteLength(marker, "utf8");
    const preview = `${fullOutput.slice(0, headLength)}${marker}`;
    const status = JSON.parse(readFileSync(join(f.root, "async-subagent-runs", child.runId, "status.json"), "utf8"));
    const childEntries = readFileSync(status.sessionFile, "utf8").trim().split("\n").map((line) => JSON.parse(line));
    const response = childEntries.find((entry: any) => entry.message?.responseId === child.responseId);
    response.message.content = [{ type: "text", text: fullOutput }];
    writeFileSync(status.sessionFile, `${childEntries.map((entry: unknown) => JSON.stringify(entry)).join("\n")}\n`);

    const notice = branch.find((entry) => entry.customType === "subagent-notify");
    notice.content = notice.content.replace(`| ${child.output}`, `| ${preview}`);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toEqual(workflow.children.map((item) => ({
      session_id: `pi:session:${item.sessionId}`,
      response_id: item.responseId,
      run_id: item.runId,
    })));
  });

  test("treats status-only results as empty and unkeyed returned output as unknown", () => {
    const f = fixture();
    const launch = f.parentEntries[0]!;
    const statusCall = { id: "status-call", type: "message", message: {
      role: "assistant", content: [{ type: "toolCall", name: "subagent", id: "status-call-id", arguments: { action: "status", id: "workflow-run" } }],
    } };
    const workflowChildren = {
      version: 1, workflowRunId: "workflow-run", parentToolCallId: "workflow-call", inventoryComplete: true,
      workflowState: "completed", children: [{ childId: "task-a", runId: "child-run", state: "completed" }],
    };
    f.status("workflow-run", { runId: "workflow-run", sessionId: f.parentFile, mode: "workflow", state: "complete", toolCallId: "workflow-call" });
    const makeResult = (content: string) => ({ type: "message", message: {
      role: "toolResult", toolCallId: "status-call-id",
      details: { mode: "single", runId: "workflow-run", workflowChildren }, content,
    } });

    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [launch, statusCall, makeResult("Status: complete\nReturn: {}")]))).toEqual([]);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [launch, statusCall, makeResult('Status: complete\nReturn: {"task-a":"output-a"}')]))).toBeNull();
    const unkeyedWithoutInventory = {
      type: "message", message: {
        role: "toolResult", toolCallId: "status-call-id", details: { mode: "single", runId: "workflow-run" },
        content: 'Status: complete\nReturn: {"task-a":"output-a"}',
      },
    };
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [launch, statusCall, unkeyedWithoutInventory]))).toBeNull();
  });

  test("treats an exact status return as redundant only after the same aggregate was consumed", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-status-return", "status-return");
    const dependencies = readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))!;
    const returnedText = workflow.children[0]!.output;
    const resultEntry = (text: string) => ({ type: "message", message: {
      role: "toolResult",
      toolCallId: "status-return-call-id",
      details: {
        mode: "single",
        runId: workflow.workflowRunId,
        workflowChildren: {
          version: 1,
          workflowRunId: workflow.workflowRunId,
          parentToolCallId: workflow.toolCallId,
          inventoryComplete: true,
          workflowState: "completed",
          children: workflow.children.map((child) => ({
            childId: child.workflowKey,
            runId: child.runId,
            state: child.status,
          })),
        },
      },
      content: `Status: complete\nReturn: ${JSON.stringify(text)}`,
    } });
    const statusCall = { type: "message", message: { role: "assistant", content: [{
      type: "toolCall",
      name: "subagent",
      id: "status-return-call-id",
      arguments: { action: "status", id: workflow.workflowRunId },
    }] } };

    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...branch, statusCall, resultEntry(returnedText)]))).toBeNull();

    branch.push({ type: "message", id: "status-return-consumer", message: {
      role: "assistant", responseId: "status-return-consumer-response", content: [{ type: "text", text: "received" }],
    } });
    const ctx = f.context("parent", f.parentFile, branch);
    expect(commitPiResultConsumption(ctx, "status-return-consumer-stream", "status-return-consumer-response", dependencies,
      (customType, data) => branch.push({ type: "custom", customType, data }))).toBe(true);

    const committedPrefix = [...branch, statusCall];
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...committedPrefix, resultEntry(returnedText)]))).toEqual([]);
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...committedPrefix, resultEntry(`${returnedText} changed`)]))).toBeNull();
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...committedPrefix, resultEntry("new status output")]))).toBeNull();
  });

  test("persists consumption only when the exact receiving response is committed to the native branch", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-consumed", "consumed");
    f.writeSession(f.parentFile, "parent", branch);
    const ctx = f.context("parent", f.parentFile, branch);
    const dependencies = readPiUnconsumedRunDependencies(ctx)!;
    const appendEntry = (customType: string, data: Record<string, unknown>) => branch.push({ type: "custom", customType, data });
    expect(commitPiResultConsumption(ctx, "consumer-stream", "not-committed", dependencies, appendEntry)).toBe(false);
    expect(branch.some((entry) => entry.customType === PI_RESULT_CONSUMPTION_ENTRY)).toBe(false);

    branch.push({ type: "message", id: "consumer-message", message: { role: "assistant", responseId: "consumer-response", content: [{ type: "text", text: "received" }] } });
    expect(commitPiResultConsumption(ctx, "consumer-stream", "consumer-response", dependencies, appendEntry)).toBe(true);
    expect(readPiPersistedConsumedRunIds(ctx)).toEqual(workflow.children.map((child) => child.runId));
    const aggregate = branch.find((entry) => entry.customType === "subagent-notify");
    branch.push({ ...aggregate, id: "replayed-notification" });
    expect(readPiUnconsumedRunDependencies(ctx)).toEqual([]);
  });

  test("retains inline workflow proof after artifact mutation and repeated notification", () => {
    const f = fixture();
    const branch: any[] = [];
    const historicalWorkflow = addWorkflowDelivery(f, branch, "workflow-inline-historical", "inline-historical");
    const ctx = f.context("parent", f.parentFile, branch);
    const historicalResponseId = "inline-historical-consumer-response";
    branch.push({ type: "message", id: "inline-historical-consumer", message: {
      role: "assistant", responseId: historicalResponseId, content: [{ type: "text", text: "Consumed an earlier workflow." }],
    } });
    expect(commitPiResultConsumption(
      ctx,
      "inline-historical-consumer-stream",
      historicalResponseId,
      historicalWorkflow.children.map((child) => ({
        session_id: `pi:session:${child.sessionId}`,
        response_id: child.responseId,
        run_id: child.runId,
      })),
      (customType, data) => branch.push({ type: "custom", customType, data }),
    )).toBe(true);
    const workflow = addWorkflowDelivery(f, branch, "workflow-inline-retained", "inline-retained");
    const aggregate = branch.find((entry) => entry.customType === "subagent-notify" &&
      entry.details?.runs?.some((run: any) => run.workflowRunId === workflow.workflowRunId))!;
    const inlineEntry = {
      type: "message",
      id: "inline-workflow-result-entry",
      message: {
        role: "toolResult",
        toolCallId: workflow.toolCallId,
        details: {
          mode: "workflow",
          runId: workflow.workflowRunId,
          workflowChildren: {
            version: 1,
            workflowRunId: workflow.workflowRunId,
            parentToolCallId: workflow.toolCallId,
            inventoryComplete: true,
            workflowState: "completed",
            children: workflow.children.map((child) => ({
              childId: child.workflowKey,
              runId: child.runId,
              state: "completed",
            })),
          },
        },
        content: [{ type: "text", text: aggregate.content }],
      },
    };
    branch.push(inlineEntry);
    const responseId = "inline-workflow-consumer-response";
    branch.push({ type: "message", id: "inline-workflow-consumer", message: {
      role: "assistant", responseId, content: [{ type: "text", text: "Consumed inline workflow output." }],
    } });
    const dependencies = workflow.children.map((child) => ({
      session_id: `pi:session:${child.sessionId}`,
      response_id: child.responseId,
      run_id: child.runId,
    }));
    expect(commitPiResultConsumption(
      ctx,
      "inline-workflow-consumer-stream",
      responseId,
      dependencies,
      (customType, data) => branch.push({ type: "custom", customType, data }),
    )).toBe(true);
    expect(readPiPersistedConsumedDependencies(ctx, inlineEntry.id)).toEqual(dependencies);

    const firstChild = workflow.children[0]!;
    const childStatusPath = join(f.root, "async-subagent-runs", firstChild.runId, "status.json");
    const childStatus = JSON.parse(readFileSync(childStatusPath, "utf8"));
    const childFile = childStatus.steps[0].sessionFile;
    const retainedArtifacts = workflow.children.map((child) => {
      const statusPath = join(f.root, "async-subagent-runs", child.runId, "status.json");
      const statusText = readFileSync(statusPath, "utf8");
      const sessionPath = JSON.parse(statusText).steps[0].sessionFile;
      const sessionText = readFileSync(sessionPath, "utf8");
      rmSync(statusPath, { force: true });
      rmSync(sessionPath, { force: true });
      return { statusPath, statusText, sessionPath, sessionText };
    });
    expect(readPiPersistedConsumedDependencies(ctx, inlineEntry.id)).toEqual(dependencies);
    for (const artifact of retainedArtifacts) {
      mkdirSync(dirname(artifact.statusPath), { recursive: true });
      writeFileSync(artifact.statusPath, artifact.statusText);
      writeFileSync(artifact.sessionPath, artifact.sessionText);
    }
    const childEntries = readFileSync(childFile, "utf8").trim().split("\n").map((line) => JSON.parse(line));
    f.writeSession(childFile, firstChild.sessionId, [
      ...childEntries.slice(1),
      { type: "message", id: "duplicate-retained-child-response", message: {
        role: "assistant",
        responseId: firstChild.responseId,
        stopReason: "stop",
        content: [{ type: "text", text: firstChild.output }],
      } },
    ], f.parentFile);
    expect(readPiPersistedConsumedDependencies(ctx, inlineEntry.id)).toEqual(dependencies);
    branch.push({ ...aggregate, id: "ambiguous-retained-workflow-aggregate" });
    expect(readPiPersistedConsumedDependencies(ctx, inlineEntry.id)).toEqual(dependencies);
  });

  test("does not let a compacted run ID suppress a later exact response from that run", () => {
    const f = fixture();
    const branch: any[] = [];
    const workflow = addWorkflowDelivery(f, branch, "workflow-resumed", "resumed");
    f.writeSession(f.parentFile, "parent", branch);
    const oldDependencies = readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))!;
    const firstChild = workflow.children[0]!;
    const statusFile = join(f.root, "async-subagent-runs", firstChild.runId, "status.json");
    const status = JSON.parse(readFileSync(statusFile, "utf8"));
    const childFile = status.steps[0].sessionFile;
    const childEntries = readFileSync(childFile, "utf8").trim().split("\n").map((line) => JSON.parse(line));
    const oldResponseId = firstChild.responseId;
    const newResponseId = "child-response-resumed";
    for (const entry of childEntries) {
      if (entry.message?.responseId === oldResponseId) entry.message.responseId = newResponseId;
      if (entry.customType === PI_RUN_COMPLETION_ENTRY && entry.data?.responseId === oldResponseId) entry.data.responseId = newResponseId;
    }
    writeFileSync(childFile, childEntries.map((entry: unknown) => JSON.stringify(entry)).join("\n") + "\n");

    const compactedBranch = [
      ...branch,
      { id: "compact-resumed", type: "compaction", firstKeptEntryId: "compact-resumed", fromHook: false },
      { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord("compact-resumed", ["summary-stream"], true,
        oldDependencies.map((dependency) => dependency.run_id!), false, oldDependencies) },
      { id: "resume-summary", type: "message", message: { role: "assistant", responseId: "resume-summary-response" } },
      { ...branch.find((entry) => entry.customType === "subagent-notify"), id: "resumed-delivery" },
    ];
    const actual = readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, compactedBranch));
    expect(actual).toEqual([{
      session_id: `pi:session:${firstChild.sessionId}`,
      response_id: newResponseId,
      run_id: firstChild.runId,
    }]);
  });

  test("starts a delegated child after copied history and keeps its own predecessor on resume", () => {
    const f = fixture();
    const childFile = join(f.root, "child.jsonl");
    f.writeSession(childFile, "child", f.parentEntries, f.parentFile);
    const branch = [...f.parentEntries, { id: "child-user", type: "message", message: { role: "user" } }];
    const ctx = f.context("child", childFile, branch, f.parentFile);
    const options = { delegation: { runId: "child-run", toolCallId: "workflow-call" } };
    expect(readPiOwnedBranch(ctx)).toEqual(branch.slice(2));
    let metadata = parseAgentMetadata(piEnvelopeForTest(ctx, "first", options));
    expect(metadata?.sequence).toEqual({ status: "start", previous_response_id: null });
    expect(metadata?.session).toEqual({ id: "pi:session:child", root_id: "pi:session:parent", parent_id: "pi:session:parent" });
    branch.push({ id: "child-a", type: "message", message: { role: "assistant", responseId: "child-response" } });
    metadata = parseAgentMetadata(piEnvelopeForTest(ctx, "second", options));
    expect(metadata?.sequence).toEqual({ status: "linked", previous_response_id: "child-response" });
    expect(metadata?.depends_on).toEqual([]);
    writeFileSync(f.parentFile, "incomplete-json\n");
    metadata = parseAgentMetadata(piEnvelopeForTest(ctx, "unknown", options));
    expect(metadata?.sequence.status).toBe("unavailable");
    expect(metadata?.depends_on).toBeNull();
  });

  test("normalizes a retained boundary inside inherited history to the empty owned prefix", () => {
    const f = fixture();
    const childFile = join(f.root, "delegated-boundary-child.jsonl");
    const branch = [...f.parentEntries, { id: "delegated-owned-assistant", type: "message", message: {
      role: "assistant", responseId: "delegated-owned-response", content: [{ type: "text", text: "owned response" }],
    } }];
    f.writeSession(childFile, "delegated-boundary-child", branch, f.parentFile);
    const ctx = f.context("delegated-boundary-child", childFile, branch, f.parentFile);

    expect(readPiOwnedBranch(ctx)).toEqual(branch.slice(f.parentEntries.length));
    expect(readPiPersistedConsumedDependencies(ctx, "parent-a")).toEqual([]);
  });

  test("keeps pending output unknown when a supplied native compaction cut is unresolved", () => {
    for (const mode of ["missing", "duplicate"] as const) {
      const f = fixture();
      const branch: any[] = [];
      const workflow = addWorkflowDelivery(f, branch, `workflow-invalid-cut-${mode}`, `invalid-cut-${mode}`);
      const dependencies = workflow.children.map((child) => ({
        session_id: `pi:session:${child.sessionId}`,
        response_id: child.responseId,
        run_id: child.runId,
      }));
      const validCut = { id: `valid-native-cut-${mode}`, type: "message", message: { role: "user" } };
      branch.push(validCut);
      const validCompaction = {
        id: `valid-compaction-before-invalid-cut-${mode}`,
        type: "compaction",
        firstKeptEntryId: validCut.id,
        fromHook: false,
      };
      branch.push(validCompaction, { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord(
        validCompaction.id, ["earlier-summary"], true, dependencies.map((dependency) => dependency.run_id!), false, dependencies,
      ) });
      const firstKeptEntryId = mode === "missing" ? "absent-native-cut" : "duplicate-native-cut";
      if (mode === "duplicate") {
        branch.push({ id: firstKeptEntryId, type: "message", message: { role: "user" } });
        branch.push({ id: firstKeptEntryId, type: "message", message: { role: "user" } });
      }
      const nativeCompaction = { id: `invalid-cut-compaction-${mode}`, type: "compaction", firstKeptEntryId, fromHook: false };
      branch.push(nativeCompaction);
      branch.push({ type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord(
        nativeCompaction.id, ["invalid-cut-summary-stream"], true, [], false, dependencies,
      ) });
      f.writeSession(f.parentFile, "parent", branch);
      const ctx = f.context("parent", f.parentFile, branch);

      expect(readPiUnconsumedRunDependencies(ctx)).toBeNull();
    }
  });

  test("does not let a later compaction repair an unavailable first covering summary", () => {
    const firstCheckpoints = [
      { label: "unavailable", fromHook: false, available: false, contributors: [], provenNone: false },
      { label: "mechanical", fromHook: true, available: true, contributors: ["carried-summary"], provenNone: false },
      { label: "empty-frontier", fromHook: false, available: true, contributors: [], provenNone: true },
    ];
    for (const checkpoint of firstCheckpoints) {
      const f = fixture();
      const branch: any[] = [];
      const workflow = addWorkflowDelivery(f, branch, `workflow-first-cut-${checkpoint.label}`, `first-cut-${checkpoint.label}`);
      const dependencies = workflow.children.map((child) => ({
        session_id: `pi:session:${child.sessionId}`,
        response_id: child.responseId,
        run_id: child.runId,
      }));
      const firstCut = { id: `first-cut-${checkpoint.label}`, type: "message", message: { role: "user" } };
      branch.push(firstCut);
      const firstCompaction = {
        id: `first-checkpoint-${checkpoint.label}`,
        type: "compaction",
        firstKeptEntryId: firstCut.id,
        fromHook: checkpoint.fromHook,
      };
      branch.push(firstCompaction, { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord(
        firstCompaction.id, checkpoint.contributors, checkpoint.available, [], checkpoint.provenNone,
      ) });
      const secondCut = { id: `second-cut-${checkpoint.label}`, type: "message", message: { role: "user" } };
      branch.push(secondCut);
      const secondCompaction = {
        id: `second-checkpoint-${checkpoint.label}`,
        type: "compaction",
        firstKeptEntryId: secondCut.id,
        fromHook: false,
      };
      branch.push(secondCompaction, { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord(
        secondCompaction.id, ["later-summary"], true, dependencies.map((dependency) => dependency.run_id!), false, dependencies,
      ) });
      f.writeSession(f.parentFile, "parent", branch);

      expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toBeNull();
    }
  });

  test("uses captured summary coverage when its source filter omitted a child output", () => {
    const f = fixture();
    const branch: any[] = [];
    addWorkflowDelivery(f, branch, "workflow-source-filtered-summary", "source-filtered-summary");
    const firstKeptEntryId = "source-filtered-summary-cut";
    branch.push({ id: firstKeptEntryId, type: "message", message: { role: "user" } });
    const nativeCompaction = {
      id: "source-filtered-summary-compaction",
      type: "compaction",
      firstKeptEntryId,
      fromHook: false,
    };
    branch.push(nativeCompaction, { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord(
      nativeCompaction.id, ["captured-summary"], true,
    ) });
    f.writeSession(f.parentFile, "parent", branch);

    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, branch))).toEqual([]);
  });

  test("does not accept inherited entries after child-owned history", () => {
    const f = fixture();
    const childFile = join(f.root, "child.jsonl");
    f.writeSession(childFile, "child", [], f.parentFile);
    const ctx = f.context("child", childFile, [{ id: "child-user", type: "message" }, ...f.parentEntries], f.parentFile);
    expect(readPiOwnedBranch(ctx)).toBeNull();
  });

  test("matches a pinned encoded workflow ID back to its native call ID", () => {
    const f = fixture();
    const toolCallId = "workflow.call.jsonl";
    const encodedWorkflowId = `~sha256-${createHash("sha256").update(toolCallId).digest("hex")}`;
    f.writeSession(f.parentFile, "parent", [{ id: "call", type: "message", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent", id: toolCallId, arguments: { workflow: true } }] } }]);
    const childFile = join(f.root, "encoded-child.jsonl");
    f.writeSession(childFile, "child", [], f.parentFile);
    f.status("encoded-child-run", { lifecycleArtifactVersion: 3, runId: "encoded-child-run", sessionId: f.parentFile, mode: "single", state: "running", pid: process.pid,
      sessionFile: childFile, steps: [{ sessionFile: childFile }], parentWorkflowRunId: encodedWorkflowId, workflowKey: "task-a" });
    // Foreground workflows can have durable child-run status but no parent workflow status file.
    const runner = join(f.root, "node_modules", "pi-subagents", "src", "runs", "background", "subagent-runner-bootstrap.js");
    mkdirSync(dirname(runner), { recursive: true });
    writeFileSync(join(f.root, "node_modules", "pi-subagents", "package.json"), JSON.stringify({ name: "pi-subagents", version: "0.83.0" }));
    writeFileSync(runner, "");
    process.argv = [process.execPath, runner, join(f.root, "async-cfg-encoded-child-run.json")];
    process.env.PI_SUBAGENT_CHILD = "1";
    process.env.PI_SUBAGENT_RUNNER_PID = String(process.pid);
    expect(readPiSubagentDelegation(childFile, "parent")).toEqual({ runId: "encoded-child-run", toolCallId });

    const workflowStatusPath = join(f.root, "async-subagent-runs", encodedWorkflowId, "status.json");
    mkdirSync(dirname(workflowStatusPath), { recursive: true });
    writeFileSync(workflowStatusPath, "not valid status JSON");
    expect(readPiSubagentDelegation(childFile, "parent")).toBeNull();
    f.status(encodedWorkflowId, {
      runId: encodedWorkflowId,
      sessionId: f.parentFile,
      mode: "workflow",
      state: "running",
    });
    expect(readPiSubagentDelegation(childFile, "parent")).toBeNull();
    f.status(encodedWorkflowId, {
      runId: encodedWorkflowId,
      toolCallId: "different-native-call",
      sessionId: f.parentFile,
      mode: "workflow",
      state: "running",
    });
    expect(readPiSubagentDelegation(childFile, "parent")).toBeNull();
    f.status(encodedWorkflowId, {
      runId: encodedWorkflowId,
      toolCallId,
      sessionId: "another-parent",
      mode: "workflow",
      state: "running",
    });
    expect(readPiSubagentDelegation(childFile, "parent")).toBeNull();
  });

  test("attributes blocking workflow children from the exact native parent call and consumes inline results", () => {
    const f = fixture();
    const childFile = join(f.root, "child.jsonl");
    f.writeSession(childFile, "child", [
      { type: "message", id: "child-assistant", message: { role: "assistant", responseId: "child-response", stopReason: "stop", content: [{ type: "text", text: "child output" }] } },
      { type: "custom", customType: PI_RUN_COMPLETION_ENTRY, data: { runId: "child-run", sessionId: "child", responseId: "child-response" } },
    ], f.parentFile);
    const childStatus = {
      lifecycleArtifactVersion: 3, runId: "child-run", sessionId: f.parentFile, mode: "single", state: "complete", pid: process.pid,
      sessionFile: childFile, steps: [{ sessionFile: childFile }], parentWorkflowRunId: "workflow-call", workflowKey: "task-a", toolCallId: "synthetic-launch-id",
    };
    f.status("child-run", childStatus);
    const workflowChildren = {
      version: 1, parentToolCallId: "workflow-call", workflowRunId: "workflow-call", inventoryComplete: true,
      workflowState: "completed", children: [{ childId: "task-a", runId: "child-run", state: "completed" }],
    };
    const result = { id: "result", type: "message", message: {
      role: "toolResult", toolCallId: "workflow-call", details: { mode: "workflow", runId: "workflow-call", workflowChildren },
      content: "Workflow run: workflow-call\nChild runs: task-a=child-run (completed)\n\nChild outputs:\n- key=task-a run=child-run status=completed\n  Preview:\n    | child output",
    } };
    f.status("workflow-call", { runId: "workflow-call", toolCallId: "workflow-call", sessionId: f.parentFile, mode: "workflow", state: "running" });
    process.env.PI_SUBAGENT_CHILD = "1";
    process.env.PI_SUBAGENT_RUNNER_PID = String(process.pid);
    const runner = join(f.root, "node_modules", "pi-subagents", "src", "runs", "background", "subagent-runner-bootstrap.js");
    mkdirSync(dirname(runner), { recursive: true });
    writeFileSync(join(f.root, "node_modules", "pi-subagents", "package.json"), JSON.stringify({ name: "pi-subagents", version: "0.83.0" }));
    writeFileSync(runner, "");
    process.argv = [process.execPath, runner, join(f.root, "async-cfg-child-run.json")];
    expect(readPiSubagentDelegation(childFile, "parent", "terminal")).toEqual({ runId: "child-run", toolCallId: "workflow-call" });
    expect(readPiRunCompletion("child-run", "parent", "workflow-call", f.parentFile)?.responseId).toBe("child-response");
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...f.parentEntries, result]))).toEqual([
      { session_id: "pi:session:child", response_id: "child-response", run_id: "child-run" },
    ]);
    const failed = { ...result, message: { ...result.message, details: { ...result.message.details, workflowChildren: { ...workflowChildren, children: [{ childId: "task-a", runId: "missing-failed-run", state: "failed" }] } } } };
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...f.parentEntries, failed]))).toBeNull();
    const rejected = { ...result, message: { ...result.message, details: { ...result.message.details, workflowChildren: { ...workflowChildren, workflowState: "failed" } } } };
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...f.parentEntries, rejected]))).toEqual([
      { session_id: "pi:session:child", response_id: "child-response", run_id: "child-run" },
    ]);
    const malformed = { ...result, message: { ...result.message, details: { ...result.message.details, workflowChildren: { ...workflowChildren, parentToolCallId: "another-call" } } } };
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...f.parentEntries, malformed]))).toBeNull();
    const empty = { ...result, message: { ...result.message, details: { ...result.message.details, workflowChildren: { ...workflowChildren, children: [] } } } };
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...f.parentEntries, empty]))).toEqual([]);
    const consumedDependencies = readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...f.parentEntries, result]))!;
    const compaction = { id: "compact-after-workflow", type: "compaction", firstKeptEntryId: "compact-after-workflow", fromHook: false };
    const compactedBranch = [...f.parentEntries, result, compaction, { type: "custom", customType: PI_COMPACTION_ENTRY,
      data: makePiCompactionRecord("compact-after-workflow", ["summary-stream"], true, ["workflow-call"], false, consumedDependencies) }];
    const compacted = f.context("parent", f.parentFile, compactedBranch);
    expect(resolveCompactionFrontier(["prior-stream"], [], false, true)).toEqual({ available: true, streamIds: ["prior-stream"], provenNone: false });
    expect(readPiUnconsumedRunDependencies(compacted)).toEqual([]);
    compactedBranch.push({ id: "after-compaction-consumer", type: "message", message: { role: "assistant", responseId: "after-compaction" } });
    expect(readPiUnconsumedRunDependencies(compacted)).toEqual([]);
    const ordinaryCall = { ...f.parentEntries[0], message: { ...f.parentEntries[0].message, content: [{ type: "toolCall", name: "subagent", id: "workflow-call", arguments: { agent: "probe", task: "Do work" } }] } };
    f.writeSession(f.parentFile, "parent", [ordinaryCall, f.parentEntries[1]]);
    expect(readPiSubagentDelegation(childFile, "parent", "terminal")).toBeNull();
    f.writeSession(f.parentFile, "parent", [...f.parentEntries, f.parentEntries[0]]);
    expect(readPiSubagentDelegation(childFile, "parent", "terminal")).toBeNull();
    f.writeSession(f.parentFile, "parent", f.parentEntries);
    f.status("workflow-call", { mode: "workflow", runId: "workflow-call", sessionId: "another-parent", workflowChildren: { version: 1, workflowRunId: "workflow-call", parentToolCallId: "workflow-call", children: [{ childId: "task-a", runId: "child-run" }] } });
    expect(readPiSubagentDelegation(childFile, "parent", "terminal")).toBeNull();
    f.status("workflow-call", { mode: "workflow", runId: "workflow-call", sessionId: f.parentFile, workflowChildren: { version: 1, workflowRunId: "workflow-call", parentToolCallId: "workflow-call", children: [{ childId: "task-a", runId: "child-run" }] } });
    const malformedStatus = { ...childStatus, parentWorkflowRunId: "../workflow-call" };
    f.status("child-run", malformedStatus);
    expect(readPiSubagentDelegation(childFile, "parent", "terminal")).toBeNull();
  });

  test("expands a completed workflow into exact child results and binds synthetic launch IDs", () => {
    const f = fixture();
    const workflow = {
      runId: "workflow-run", sessionId: f.parentFile, mode: "workflow", state: "complete", toolCallId: "workflow-call",
      workflowChildren: { version: 1, workflowRunId: "workflow-run", parentToolCallId: "workflow-call", inventoryComplete: true,
        children: [{ childId: "task-a", runId: "child-a", state: "completed" }, { childId: "task-b", runId: "child-b", state: "completed" }] },
    };
    const workflowPath = f.status("workflow-run", workflow);
    const childPaths: string[] = [];
    for (const suffix of ["a", "b"]) {
      const runId = `child-${suffix}`;
      const childFile = join(f.root, `${runId}.jsonl`);
      childPaths.push(childFile);
      f.writeSession(childFile, runId, [
        { type: "message", id: `assistant-${suffix}`, message: { role: "assistant", responseId: `response-${suffix}`, stopReason: "stop", content: [{ type: "text", text: `output-${suffix}` }] } },
        { type: "custom", customType: PI_RUN_COMPLETION_ENTRY, data: { runId, sessionId: runId, responseId: `response-${suffix}` } },
      ], f.parentFile);
      f.status(runId, { lifecycleArtifactVersion: 3, runId, sessionId: f.parentFile, mode: "single", state: "complete", pid: process.pid,
        sessionFile: childFile, steps: [{ sessionFile: childFile }], parentWorkflowRunId: "workflow-run", workflowKey: `task-${suffix}`, toolCallId: `synthetic-${suffix}` });
    }
    const childRuns = workflow.workflowChildren.children.map((child) => ({ workflowKey: child.childId, runId: child.runId, status: child.state }));
    const childOutputs = childRuns.map((child) => ({ ...child, outputArtifactPath: `output/${child.runId}.txt` }));
    const notice = { id: "notice", type: "custom_message", customType: "subagent-notify", details: {
      runs: [{ runId: "workflow-run", workflowRunId: "workflow-run", status: "completed", childRuns, childOutputs }],
    }, content: "Background task completed: **workflow**\nWorkflow receipt: workflow-receipt.json\n\nWorkflow run: workflow-run\nChild runs: task-a=child-a (completed), task-b=child-b (completed)\n\nChild outputs:\n- key=task-a run=child-a status=completed\n  Preview:\n    | output-a\n- key=task-b run=child-b status=completed\n  Preview:\n    | output-b" };
    const branch = [...f.parentEntries, notice];
    const ctx = f.context("parent", f.parentFile, branch);
    const incremental = { id: "incremental", type: "custom_message", customType: "subagent-incremental-child-notify", details: { childKey: "task-a", outcome: "completed" },
      content: "Workflow child completed: **task-a**\nWorkflow run: workflow-run\nChild run: child-a\nOutput: output/child-a.txt\nStatus: workflow still running" };
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...f.parentEntries, incremental]))).toEqual([]);
    const unknownIncremental = { ...incremental, details: { childKey: "unknown", outcome: "completed" }, content: incremental.content.replace("task-a", "unknown") };
    expect(readPiUnconsumedRunDependencies(f.context("parent", f.parentFile, [...f.parentEntries, unknownIncremental]))).toEqual([]);
    expect(readPiUnconsumedRunDependencies(ctx)).toEqual([
      { session_id: "pi:session:child-a", response_id: "response-a", run_id: "child-a" },
      { session_id: "pi:session:child-b", response_id: "response-b", run_id: "child-b" },
    ]);
    const runner = join(f.root, "node_modules", "pi-subagents", "src", "runs", "background", "subagent-runner-bootstrap.js");
    mkdirSync(dirname(runner), { recursive: true });
    writeFileSync(join(f.root, "node_modules", "pi-subagents", "package.json"), JSON.stringify({ name: "pi-subagents", version: "0.83.0" }));
    writeFileSync(runner, "");
    process.argv = [process.execPath, runner, join(f.root, "async-cfg-child-a.json")];
    process.env.PI_SUBAGENT_CHILD = "1";
    process.env.PI_SUBAGENT_RUNNER_PID = String(process.pid);
    expect(readPiSubagentDelegation(childPaths[0], "parent", "terminal")).toEqual({ runId: "child-a", toolCallId: "workflow-call" });
    expect(readPiRunCompletion("child-a", "parent", "workflow-call", f.parentFile)?.responseId).toBe("response-a");
    expect(readPiRunCompletion("child-a", "parent", "synthetic-a", f.parentFile)).toBeNull();
    writeFileSync(workflowPath, JSON.stringify({ ...workflow, workflowChildren: { ...workflow.workflowChildren, inventoryComplete: false } }));
    const exactDependencies = [
      { session_id: "pi:session:child-a", response_id: "response-a", run_id: "child-a" },
      { session_id: "pi:session:child-b", response_id: "response-b", run_id: "child-b" },
    ];
    expect(readPiUnconsumedRunDependencies(ctx)).toEqual(exactDependencies);
    writeFileSync(workflowPath, JSON.stringify({ ...workflow, sessionId: "other-parent" }));
    expect(readPiUnconsumedRunDependencies(ctx)).toEqual(exactDependencies);
    for (const children of [[null], [workflow.workflowChildren.children[0], workflow.workflowChildren.children[0]]]) {
      writeFileSync(workflowPath, JSON.stringify({ ...workflow, workflowChildren: { ...workflow.workflowChildren, children } }));
      expect(readPiUnconsumedRunDependencies(ctx)).toEqual(exactDependencies);
    }
    writeFileSync(workflowPath, JSON.stringify(workflow));
    writeFileSync(childPaths[1], readFileSync(childPaths[1], "utf8").split("\n").slice(0, 1).join("\n") + "\n");
    expect(readPiUnconsumedRunDependencies(ctx)).toBeNull();
    branch.push({ id: "consumer", type: "message", message: { role: "assistant", responseId: "parent-consumer" } } as any);
    expect(readPiUnconsumedRunDependencies(ctx)).toEqual([]);
  });

  test("loads a compatible public child-extension API by capability and rejects malformed exports", async () => {
    const f = fixture();
    process.env.PI_CODING_AGENT_DIR = f.root;
    const pkg = join(f.root, "npm", "node_modules", "pi-subagents");
    mkdirSync(join(pkg, "bundle"), { recursive: true });
    const manifest = { name: "pi-subagents", version: "0.83.0", type: "module", exports: { "./required-child-extensions": "./bundle/required-child-extensions.js" } };
    writeFileSync(join(pkg, "package.json"), JSON.stringify(manifest));
    writeFileSync(join(pkg, "bundle", "required-child-extensions.js"), "export function registerRequiredChildExtensions(input) { return { input, dispose() {} }; }");
    const api = await loadPiRequiredExtensionsApi();
    expect(typeof api?.registerRequiredChildExtensions).toBe("function");
    writeFileSync(join(pkg, "package.json"), JSON.stringify({ ...manifest, version: "0.84.0" }));
    expect(typeof (await loadPiRequiredExtensionsApi())?.registerRequiredChildExtensions).toBe("function");
    const malformedAgentDir = join(f.root, "malformed-agent");
    const malformedPackage = join(malformedAgentDir, "npm", "node_modules", "pi-subagents");
    mkdirSync(join(malformedPackage, "bundle"), { recursive: true });
    writeFileSync(join(malformedPackage, "package.json"), JSON.stringify({
      ...manifest,
      version: "0.84.0",
      exports: { "./required-child-extensions": "./bundle/malformed-required-child-extensions.js" },
    }));
    writeFileSync(join(malformedPackage, "bundle", "malformed-required-child-extensions.js"), "export const registerRequiredChildExtensions = true;");
    process.env.PI_CODING_AGENT_DIR = malformedAgentDir;
    expect(await loadPiRequiredExtensionsApi()).toBeNull();
  });

  test("uses the first committed compaction summary for expired child output across two compactions", async () => {
    const harness = await setupProviderHookHarness();
    const { f, hooks, providerConfig, branch, ctx } = harness;
    try {
      const workflow = addWorkflowDelivery(f, branch, "workflow-summary-expiry", "summary-expiry");
      const notice = branch.find((entry) => entry.id === `aggregate-summary-expiry`)!;
      const firstCut = { id: "summary-expiry-cut-one", type: "message", message: {
        role: "user", content: [{ type: "text", text: "after first summary" }],
      } };
      branch.push(firstCut);
      const firstPreparation = {
        firstKeptEntryId: firstCut.id,
        messagesToSummarize: [{ role: "user", content: [{ type: "text", text: notice.content }] }],
        isSplitTurn: false,
      };
      hooks.get("session_before_compact")!({ preparation: firstPreparation }, ctx);
      const firstHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: firstHeaders }, ctx);
      const firstCandidate = parseAgentMetadata(firstHeaders.get("x-tokenvisor-agent"));
      const childDependencies = workflow.children.map((child) => ({
        session_id: `pi:session:${child.sessionId}`,
        response_id: child.responseId,
        run_id: child.runId,
      }));
      expect(firstCandidate?.depends_on).toEqual(childDependencies);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: "summary-expiry-first-response",
        stopReason: "stop",
        content: [{ type: "text", text: "First summary includes the delivered child output." }],
      };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: firstHeaders },
        { messages: [{ role: "user", content: [{ type: "text", text: `<conversation>\n${notice.content}\n</conversation>\n\nsource prompt` }] }] },
        { headers: firstHeaders },
      ).result();
      await Promise.resolve();
      const firstMetadata = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(firstMetadata?.depends_on).toEqual(childDependencies);

      const firstCompaction = {
        id: "summary-expiry-compaction-one",
        type: "compaction",
        summary: "First summary includes the delivered child output.",
        firstKeptEntryId: firstCut.id,
        fromHook: false,
      };
      branch.push(firstCompaction);
      hooks.get("session_compact")!({ fromExtension: false }, ctx);
      const firstRecord = branch.find((entry) =>
        entry.customType === PI_COMPACTION_ENTRY && entry.data?.compactionEntryId === firstCompaction.id,
      );
      expect(firstRecord?.data).toMatchObject({ available: true, consumedDependencies: childDependencies });

      for (const child of workflow.children) {
        const statusPath = join(f.root, "async-subagent-runs", child.runId, "status.json");
        const status = JSON.parse(readFileSync(statusPath, "utf8"));
        rmSync(statusPath, { force: true });
        rmSync(status.steps[0].sessionFile, { force: true });
      }

      const secondCut = { id: "summary-expiry-cut-two", type: "message", message: {
        role: "user", content: [{ type: "text", text: "after second summary" }],
      } };
      branch.push(secondCut);
      hooks.get("session_before_compact")!({ preparation: {
        firstKeptEntryId: secondCut.id,
        messagesToSummarize: [
          { role: "user", content: [{ type: "text", text: firstCompaction.summary }] },
          firstCut.message,
        ],
        isSplitTurn: false,
      } }, ctx);
      const secondHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: secondHeaders }, ctx);
      const secondCandidate = parseAgentMetadata(secondHeaders.get("x-tokenvisor-agent"));
      expect(secondCandidate?.depends_on).toEqual([{
        session_id: "pi:session:parent",
        stream_id: firstMetadata?.stream_id,
      }]);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: "summary-expiry-second-response",
        stopReason: "stop",
        content: [{ type: "text", text: "Second summary carries the first summary." }],
      };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: secondHeaders },
        { messages: [{ role: "user", content: [{ type: "text", text: `<conversation>\n${firstCompaction.summary}\nafter first summary\n</conversation>\n\nsource prompt` }] }] },
        { headers: secondHeaders },
      ).result();
      await Promise.resolve();
      const secondMetadata = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(secondMetadata?.depends_on).toEqual(secondCandidate?.depends_on);

      const secondCompaction = {
        id: "summary-expiry-compaction-two",
        type: "compaction",
        summary: "Second summary carries the first summary.",
        firstKeptEntryId: secondCut.id,
        fromHook: false,
      };
      branch.push(secondCompaction);
      hooks.get("session_compact")!({ fromExtension: false }, ctx);
      const continuationHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: continuationHeaders }, ctx);
      expect(parseAgentMetadata(continuationHeaders.get("x-tokenvisor-agent"))?.depends_on).toEqual([{
        session_id: "pi:session:parent",
        stream_id: secondMetadata?.stream_id,
      }]);
    } finally {
      harness.cleanup();
    }
  });

  test("keeps direct child refs through a captured compaction summary", async () => {
    const harness = await setupProviderHookHarness();
    const { f, hooks, providerConfig, branch, ctx } = harness;
    try {
      const delivery = addDirectDelivery(f, branch, "direct-compaction");
      const firstCut = { id: "direct-compaction-cut", type: "message", message: {
        role: "user", content: [{ type: "text", text: "after direct result" }],
      } };
      branch.push(firstCut);
      hooks.get("session_before_compact")!({ preparation: {
        firstKeptEntryId: firstCut.id,
        messagesToSummarize: [{ role: "user", content: [{ type: "text", text: delivery.notice.content }] }],
        isSplitTurn: false,
      } }, ctx);
      const summaryHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: summaryHeaders }, ctx);
      const summaryCandidate = parseAgentMetadata(summaryHeaders.get("x-tokenvisor-agent"));
      const directDependency = {
        session_id: `pi:session:${delivery.sessionId}`,
        response_id: delivery.responseId,
        run_id: delivery.runId,
      };
      expect(summaryCandidate?.depends_on).toEqual([directDependency]);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: "direct-compaction-summary-response",
        stopReason: "stop",
        content: [{ type: "text", text: "Summary of the direct result." }],
      };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: summaryHeaders },
        { messages: [{ role: "user", content: [{ type: "text", text: `<conversation>\n${delivery.notice.content}\n</conversation>\n\nsource prompt` }] }] },
        { headers: summaryHeaders },
      ).result();
      await Promise.resolve();
      const summaryMetadata = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(summaryMetadata?.depends_on).toEqual([directDependency]);

      const nativeCompaction = {
        id: "direct-compaction-entry",
        type: "compaction",
        summary: "Summary of the direct result.",
        firstKeptEntryId: firstCut.id,
        fromHook: false,
      };
      branch.push(nativeCompaction);
      hooks.get("session_compact")!({ fromExtension: false }, ctx);
      const compactionRecord = branch.find((entry) =>
        entry.customType === PI_COMPACTION_ENTRY && entry.data?.compactionEntryId === nativeCompaction.id,
      );
      expect(compactionRecord?.data).toMatchObject({ available: true, consumedDependencies: [directDependency] });

      const statusPath = join(f.root, "async-subagent-runs", delivery.runId, "status.json");
      rmSync(statusPath, { force: true });
      rmSync(delivery.sessionFile, { force: true });
      const continuationHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: continuationHeaders }, ctx);
      expect(parseAgentMetadata(continuationHeaders.get("x-tokenvisor-agent"))?.depends_on).toEqual([{
        session_id: "pi:session:parent",
        stream_id: summaryMetadata?.stream_id,
      }]);
    } finally {
      harness.cleanup();
    }
  });

  test("keeps split-turn S1/S2 ordering separate from data consumption through continuation", async () => {
    const harness = await setupProviderHookHarness();
    const { f, hooks, providerConfig, api, branch, ctx } = harness;
    try {
      branch.push({ id: "pre-compaction-history", type: "message", message: {
        role: "user", content: [{ type: "text", text: "older history" }],
      } });
      const workflow = addWorkflowDelivery(f, branch, "workflow-hook", "hook");
      f.writeSession(f.parentFile, "parent", branch);
      expect(hooks.has("session_start")).toBe(true);
      hooks.get("session_start")!({ reason: "startup" }, ctx);
      expect(api.calls[0].sessionId).toBe("parent");
      expect(api.calls[0].extensions).toEqual([{ id: "tokenvisor-provider", path: new URL("../extensions/tokenvisor-provider.ts", import.meta.url).pathname }]);
      hooks.get("session_start")!({ reason: "resume" }, ctx);
      expect(api.calls[1]).toBe("disposed");
      const headers = new Headers();
      hooks.get("before_provider_headers")!({ headers }, ctx);
      const candidate = parseAgentMetadata(headers.get("x-tokenvisor-agent"));
      expect(candidate?.depends_on).toEqual(workflow.children.map((child) => ({
        session_id: `pi:session:${child.sessionId}`,
        response_id: child.responseId,
        run_id: child.runId,
      })));
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId: "request-excluded", stopReason: "error", content: [] };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers },
        { messages: [] },
        { headers },
      ).result();
      const omitted = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(omitted?.stream_id).toBe(candidate?.stream_id);
      expect(omitted?.depends_on).toBeNull();

      const includedHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: includedHeaders }, ctx);
      const includedCandidate = parseAgentMetadata(includedHeaders.get("x-tokenvisor-agent"));
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId: "request-included", stopReason: "error", content: [] };
      const noticeText = piSubagentsNoticeContextText(branch.find((entry) => entry.customType === "subagent-notify" &&
        entry.details?.runs?.some((run: any) => run.workflowRunId === workflow.workflowRunId))!.content);
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: includedHeaders },
        { messages: [{ role: "user", content: [{ type: "text", text: noticeText }] }] },
        { headers: includedHeaders },
      ).result();
      const included = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(included?.stream_id).toBe(includedCandidate?.stream_id);
      expect(included?.depends_on).toEqual(candidate?.depends_on);
      const compactBoundary = "call-hook";
      expect(branch.some((entry) => entry.id === compactBoundary)).toBe(true);
      const preparation = {
        firstKeptEntryId: compactBoundary,
        messagesToSummarize: [{ role: "user", content: [{ type: "text", text: "older history" }] }],
        turnPrefixMessages: [{ role: "user", content: [{ type: "text", text: "unfinished turn" }] }],
        isSplitTurn: true,
      };
      hooks.get("session_before_compact")!({ preparation }, ctx);

      const summaryStreams: Array<{ stream_id: string; response_id: string }> = [];
      const runSummaryStream = async (prompt: string, responseId: string, content: unknown[] = [{ type: "text", text: `${responseId} summary output` }]) => {
        const summaryHeaders = new Headers();
        hooks.get("before_provider_headers")!({ headers: summaryHeaders }, ctx);
        const candidate = parseAgentMetadata(summaryHeaders.get("x-tokenvisor-agent"));
        expect(candidate?.depends_on).toEqual(workflow.children.map((child) => ({
            session_id: `pi:session:${child.sessionId}`,
            response_id: child.responseId,
            run_id: child.runId,
          })));
        (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
          responseId,
          stopReason: "stop",
          content,
        };
        await providerConfig.streamSimple(
          { id: "synthetic", api: "openai-completions", headers: summaryHeaders },
          { messages: [{ role: "user", content: [{ type: "text", text: prompt }] }] },
          { headers: summaryHeaders },
        ).result();
        await Promise.resolve();
        const finalized = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
        summaryStreams.push({ stream_id: candidate!.stream_id, response_id: responseId });
        return finalized;
      };

      const historySummary = await runSummaryStream(
        "<conversation>\nolder history\n</conversation>\n\nsource prompt",
        "history-summary-response",
        [
          { type: "thinking", thinking: "native S1 reasoning block" },
          { type: "text", text: "history-summary-response summary output" },
        ],
      );
      expect(historySummary?.depends_on).toEqual([]);
      const turnPrefixSummary = await runSummaryStream(
        "# Conversation\nunfinished turn\n\n# Instructions\nsource prompt",
        "turn-prefix-summary-response",
      );
      expect(turnPrefixSummary?.depends_on).toEqual([]);
      expect(turnPrefixSummary?.sequence.previous_response_id).toBe("history-summary-response");
      expect(summaryStreams.map(({ response_id }) => response_id)).toEqual([
        "history-summary-response",
        "turn-prefix-summary-response",
      ]);

      const firstRetainedEntry = branch.findIndex((entry) => entry.id === compactBoundary);
      expect(firstRetainedEntry).toBeGreaterThan(0);
      const compactionSummary = "merged history and turn-prefix summaries";
      const nativeCompaction = {
        id: "native-compaction-after-summary",
        type: "compaction",
        summary: compactionSummary,
        firstKeptEntryId: compactBoundary,
        fromHook: false,
      };
      branch.push(nativeCompaction);
      hooks.get("session_compact")!({ fromExtension: false }, ctx);

      const compactionRecordEntry = branch.find((entry) =>
        entry.customType === PI_COMPACTION_ENTRY && entry.data?.compactionEntryId === nativeCompaction.id,
      );
      expect(compactionRecordEntry?.data).toMatchObject({
        compactionEntryId: nativeCompaction.id,
        contributors: [historySummary?.stream_id, turnPrefixSummary?.stream_id].map((stream_id) => ({ stream_id })),
        consumedRunIds: [],
        consumedDependencies: [],
        available: true,
      });

      const continuationHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: continuationHeaders }, ctx);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: "post-compaction-continuation",
        stopReason: "stop",
        content: [{ type: "text", text: "continued" }],
      };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: continuationHeaders },
        { messages: [
          { role: "user", content: [{ type: "text", text: compactionSummary }] },
          { role: "user", content: [{ type: "text", text: piSubagentsNoticeContextText(branch.find((entry) => entry.customType === "subagent-notify" &&
            entry.details?.runs?.some((run: any) => run.workflowRunId === workflow.workflowRunId))!.content) }] },
        ] },
        { headers: continuationHeaders },
      ).result();
      await Promise.resolve();
      const continuationMetadata = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(continuationMetadata?.depends_on).toEqual([
        { session_id: "pi:session:parent", stream_id: historySummary?.stream_id },
        { session_id: "pi:session:parent", stream_id: turnPrefixSummary?.stream_id },
        ...workflow.children.map((child) => ({
          session_id: `pi:session:${child.sessionId}`,
          response_id: child.responseId,
          run_id: child.runId,
        })),
      ]);

      const continuationResponse = {
        id: "continuation-response-entry",
        type: "message",
        message: { role: "assistant", responseId: "post-compaction-continuation", stopReason: "stop" },
      };
      branch.push(continuationResponse);
      hooks.get("agent_end")!({ messages: [continuationResponse.message] }, ctx);
      const resultConsumption = branch.find((entry) =>
        entry.customType === PI_RESULT_CONSUMPTION_ENTRY && entry.data?.response_id === "post-compaction-continuation",
      );
      expect(resultConsumption?.data.dependencies).toEqual(continuationMetadata?.depends_on);

      for (const child of workflow.children) {
        const statusPath = join(f.root, "async-subagent-runs", child.runId, "status.json");
        const status = JSON.parse(readFileSync(statusPath, "utf8"));
        rmSync(statusPath, { force: true });
        rmSync(status.steps[0].sessionFile, { force: true });
      }
      const retainedNoticeId = `notice-${workflow.children[0]!.runId}`;
      hooks.get("session_before_compact")!({ preparation: {
        firstKeptEntryId: retainedNoticeId,
        messagesToSummarize: [{ role: "user", content: [{ type: "text", text: "older history" }] }],
        isSplitTurn: false,
      } }, ctx);
      const retainedSummaryHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: retainedSummaryHeaders }, ctx);
      const retainedSummaryCandidate = parseAgentMetadata(retainedSummaryHeaders.get("x-tokenvisor-agent"));
      expect(retainedSummaryCandidate?.depends_on).toEqual([]);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: "retained-compaction-summary-response",
        stopReason: "stop",
        content: [{ type: "text", text: "retained native summary" }],
      };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: retainedSummaryHeaders },
        { messages: [{ role: "user", content: [{ type: "text", text: "<conversation>\nolder history\n</conversation>\n\nsource prompt" }] }] },
        { headers: retainedSummaryHeaders },
      ).result();
      await Promise.resolve();
      const retainedSummaryMetadata = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(retainedSummaryMetadata?.depends_on).toEqual([]);
      const retainedNoticeIndex = branch.findIndex((entry) => entry.id === retainedNoticeId);
      expect(retainedNoticeIndex).toBeGreaterThan(0);
      const retainedNativeCompaction = {
        id: "native-compaction-with-expired-child",
        type: "compaction",
        summary: "retained native summary",
        firstKeptEntryId: retainedNoticeId,
        fromHook: false,
      };
      branch.push(retainedNativeCompaction);
      hooks.get("session_compact")!({ fromExtension: false }, ctx);
      const retainedCompactionRecord = branch.find((entry) =>
        entry.customType === PI_COMPACTION_ENTRY && entry.data?.compactionEntryId === retainedNativeCompaction.id,
      );
      expect(retainedCompactionRecord?.data).toMatchObject({
        available: true,
        consumedDependencies: workflow.children.map((child) => ({
          session_id: `pi:session:${child.sessionId}`,
          response_id: child.responseId,
          run_id: child.runId,
        })),
      });
      const resumedHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: resumedHeaders }, ctx);
      const resumedCandidate = parseAgentMetadata(resumedHeaders.get("x-tokenvisor-agent"));
      expect(resumedCandidate?.depends_on).toEqual([{
        session_id: "pi:session:parent",
        stream_id: retainedSummaryCandidate?.stream_id,
      }]);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: "expired-child-resume-response",
        stopReason: "stop",
        content: [{ type: "text", text: "continued after artifact expiry" }],
      };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: resumedHeaders },
        { messages: [
          { role: "user", content: [{ type: "text", text: "retained native summary" }] },
          { role: "user", content: [{ type: "text", text: piSubagentsNoticeContextText(branch.find((entry) =>
            entry.customType === "subagent-notify" && entry.details?.runs?.some((run: any) => run.workflowRunId === workflow.workflowRunId))!.content) }] },
        ] },
        { headers: resumedHeaders },
      ).result();
      await Promise.resolve();
      const resumedMetadata = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(resumedMetadata?.depends_on).toEqual(resumedCandidate?.depends_on);

      hooks.get("session_shutdown")!({}, ctx);
      expect(api.calls[3]).toBe("disposed");
    } finally {
      harness.cleanup();
    }
  });

  test("compacts a retained status-only progress notice and carries its summary into continuation", async () => {
    const harness = await setupProviderHookHarness();
    const { f, hooks, providerConfig, branch, ctx } = harness;
    try {
      branch.push({ id: "progress-history", type: "message", message: {
        role: "user", content: [{ type: "text", text: "Older progress history" }],
      } });
      const workflow = addWorkflowDelivery(f, branch, "workflow-progress-only", "progress-only");
      const progressNoticeId = `notice-${workflow.children[0]!.runId}`;
      const firstKeptEntryId = "call-progress-only";
      const aggregateIndex = branch.findIndex((entry) => entry.id === `aggregate-progress-only`);
      expect(aggregateIndex).toBeGreaterThanOrEqual(0);
      const progressNotice = branch.find((entry) => entry.id === progressNoticeId)!;
      branch.splice(aggregateIndex, 1);
      for (let index = branch.length - 1; index >= 0; index--) {
        if (branch[index]?.customType === "subagent-incremental-child-notify" && branch[index]?.id !== progressNoticeId) branch.splice(index, 1);
      }
      f.writeSession(f.parentFile, "parent", branch);

      hooks.get("session_before_compact")!({ preparation: {
        firstKeptEntryId,
        messagesToSummarize: [{ role: "user", content: [{ type: "text", text: "Older progress history" }] }],
        isSplitTurn: false,
      } }, ctx);
      const summaryHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: summaryHeaders }, ctx);
      const summaryCandidate = parseAgentMetadata(summaryHeaders.get("x-tokenvisor-agent"));
      expect(summaryCandidate?.depends_on).toEqual([]);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: "progress-only-summary-response",
        stopReason: "stop",
        content: [{ type: "text", text: "Progress status summary" }],
      };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: summaryHeaders },
        { messages: [{ role: "user", content: [{ type: "text", text: "<conversation>\nOlder progress history\n</conversation>\n\nsource prompt" }] }] },
        { headers: summaryHeaders },
      ).result();
      await Promise.resolve();
      const summaryMetadata = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(summaryMetadata?.depends_on).toEqual([]);

      const keptIndex = branch.findIndex((entry) => entry.id === progressNoticeId);
      expect(keptIndex).toBeGreaterThan(0);
      const nativeCompaction = {
        id: "progress-only-compaction",
        type: "compaction",
        summary: "Progress status summary",
        firstKeptEntryId,
        fromHook: false,
      };
      branch.push(nativeCompaction);
      hooks.get("session_compact")!({ fromExtension: false }, ctx);
      const compactionRecord = branch.find((entry) =>
        entry.customType === PI_COMPACTION_ENTRY && entry.data?.compactionEntryId === nativeCompaction.id,
      );
      expect(compactionRecord?.data).toMatchObject({
        available: true,
        contributors: [{ stream_id: summaryCandidate?.stream_id }],
        consumedDependencies: [],
      });

      const continuationHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: continuationHeaders }, ctx);
      const continuationCandidate = parseAgentMetadata(continuationHeaders.get("x-tokenvisor-agent"));
      expect(continuationCandidate?.depends_on).toEqual([{
        session_id: "pi:session:parent",
        stream_id: summaryMetadata?.stream_id,
      }]);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: "progress-only-continuation-response",
        stopReason: "stop",
        content: [{ type: "text", text: "Continued from progress summary" }],
      };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: continuationHeaders },
        { messages: [
          { role: "user", content: [{ type: "text", text: nativeCompaction.summary }] },
          { role: "user", content: [{ type: "text", text: piSubagentsNoticeContextText(progressNotice.content) }] },
        ] },
        { headers: continuationHeaders },
      ).result();
      await Promise.resolve();
      const continuationMetadata = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(continuationMetadata?.depends_on).toEqual(continuationCandidate?.depends_on);
    } finally {
      harness.cleanup();
    }
  });

  test("keeps the required retained compaction proof unknown when nine refs exceed the cap", async () => {
    const harness = await setupProviderHookHarness();
    const { f, hooks, providerConfig, branch, ctx } = harness;
    try {
      branch.push({ id: "over-budget-history", type: "message", message: {
        role: "user", content: [{ type: "text", text: "Independent retained history" }],
      } });
      const workflows = [];
      for (let index = 0; index < 3; index++) {
        const workflow = addWorkflowDelivery(f, branch, `workflow-over-budget-${index}`, `over-budget-${index}`);
        workflows.push(workflow);
        commitWorkflowDelivery(f, branch, ctx, workflow, index, "over-budget");
      }
      const firstKeptEntryId = "call-over-budget-0";
      for (let index = 0; index < workflows.length; index++) {
        expect(readPiPersistedConsumedDependencies(ctx, `aggregate-over-budget-${index}`))
          .toHaveLength((workflows.length - index) * 3);
      }
      expect(readPiPersistedConsumedDependencies(ctx, firstKeptEntryId)).toHaveLength(9);

      hooks.get("session_before_compact")!({ preparation: {
        firstKeptEntryId,
        messagesToSummarize: [{ role: "user", content: [{ type: "text", text: "Independent retained history" }] }],
        isSplitTurn: false,
      } }, ctx);
      const summaryHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: summaryHeaders }, ctx);
      const summaryCandidate = parseAgentMetadata(summaryHeaders.get("x-tokenvisor-agent"));
      expect(summaryCandidate?.depends_on).toEqual([]);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: "over-budget-summary-response",
        stopReason: "stop",
        content: [{ type: "text", text: "Independent history summary" }],
      };
      await providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers: summaryHeaders },
        { messages: [{ role: "user", content: [{ type: "text", text: "<conversation>\nIndependent retained history\n</conversation>\n\nsource prompt" }] }] },
        { headers: summaryHeaders },
      ).result();
      await Promise.resolve();
      const summaryMetadata = parseAgentMetadata(new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__.headers).get("x-tokenvisor-agent"));
      expect(summaryMetadata?.depends_on).toEqual([]);

      const keptIndex = branch.findIndex((entry) => entry.id === firstKeptEntryId);
      expect(keptIndex).toBeGreaterThan(0);
      const nativeCompaction = {
        id: "over-budget-native-compaction",
        type: "compaction",
        summary: "Independent history summary",
        firstKeptEntryId,
        fromHook: false,
      };
      branch.push(nativeCompaction);
      hooks.get("session_compact")!({ fromExtension: false }, ctx);
      const compactionRecord = branch.find((entry) =>
        entry.customType === PI_COMPACTION_ENTRY && entry.data?.compactionEntryId === nativeCompaction.id,
      );
      expect(compactionRecord?.data).toMatchObject({ available: false, consumedDependencies: [] });

      const continuationHeaders = new Headers();
      hooks.get("before_provider_headers")!({ headers: continuationHeaders }, ctx);
      expect(parseAgentMetadata(continuationHeaders.get("x-tokenvisor-agent"))?.depends_on).toBeNull();
    } finally {
      harness.cleanup();
    }
  });
});
