import { afterEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { loadPiCompactionRenderer, loadPiRequiredExtensionsApi } from "../src/capture/pi-runtime.ts";
import { PI_RUN_COMPLETION_ENTRY, readPiParentToolCallId, readPiRunCompletion, readPiUnconsumedRunDependencies } from "../src/capture/pi.ts";

const roots: string[] = [];
const savedEnv = { ...process.env };
afterEach(() => {
  for (const key of Object.keys(process.env)) if (!(key in savedEnv)) delete process.env[key];
  Object.assign(process.env, savedEnv);
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function tempRoot() {
  const root = mkdtempSync(join(tmpdir(), "pi-runtime-identity-"));
  roots.push(root);
  return root;
}

describe("Pi runtime identity", () => {
  test("keeps preferred early binding through status observations before the launch result exists", () => {
    const f = completionFixture();
    const branch = delegationEntries(f.runId);
    branch.splice(1, 1);
    branch.push(
      { type: "message", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent", id: "status-call", arguments: { action: "status", id: f.runId } }] } },
      { type: "message", message: { role: "toolResult", toolCallId: "status-call", details: { runId: f.runId, toolCallId: "tool-call" } } },
    );
    f.writeParent(branch);
    expect(readPiParentToolCallId(f.parentFile, "parent-identity", f.runId, "tool-call")).toBe("tool-call");
    expect(readPiParentToolCallId(f.parentFile, "parent-identity", f.runId)).toBeNull();
    expect(f.read(branch)?.responseId).toBe(f.responseId);
  });

  test("binds a resume launch to its own new run rather than treating every action as inspection", () => {
    const f = completionFixture();
    const branch = delegationEntries(f.runId, "resume");
    f.writeParent(branch);
    expect(readPiParentToolCallId(f.parentFile, "parent-identity", f.runId)).toBe("tool-call");
    expect(f.read(branch)?.responseId).toBe(f.responseId);
  });

  test.each([
    "second-launch", "resume-same-run", "unknown-action", "duplicate-call",
    "duplicate-result", "unmatched-status-result", "duplicate-status-call", "preferred-mismatch",
  ])("rejects conflicting delegation evidence through disk and branch readers: %s", (scenario) => {
    const f = completionFixture();
    const branch = delegationEntries(f.runId);
    f.writeParent(branch);
    expect(readPiParentToolCallId(f.parentFile, "parent-identity", f.runId, "tool-call")).toBe("tool-call");
    expect(f.read(branch)?.responseId).toBe(f.responseId);
    let preferred = "tool-call";
    if (scenario === "duplicate-call") {
      branch.push(structuredClone(branch[0]));
    } else if (scenario === "duplicate-result") {
      branch.push(structuredClone(branch[1]));
    } else if (scenario === "preferred-mismatch") {
      preferred = "other-call";
      f.writeStatus({ toolCallId: preferred });
    } else {
      const action = scenario === "resume-same-run" ? "resume"
        : scenario === "unknown-action" ? "future-inspection"
        : scenario.includes("status") ? "status" : undefined;
      const otherCall = { type: "message", message: { role: "assistant", content: [{
        type: "toolCall", name: "subagent", id: "other-call", arguments: action ? { action, id: f.runId } : { agent: "worker" },
      }] } };
      if (scenario !== "unmatched-status-result") branch.push(otherCall);
      if (scenario === "duplicate-status-call") branch.push(structuredClone(otherCall));
      branch.push({ type: "message", message: {
        role: "toolResult", toolCallId: "other-call", details: { runId: f.runId, toolCallId: "tool-call" },
      } });
    }
    f.writeParent(branch);
    expect(readPiParentToolCallId(f.parentFile, "parent-identity", f.runId, preferred)).toBeNull();
    expect(f.read(branch)).toBeNull();
  });

  test("finds the owning package manifest above a deeply nested public export", async () => {
    const root = tempRoot();
    const agentDir = join(root, "agent");
    const packageRoot = join(agentDir, "npm", "node_modules", "pi-subagents");
    const exportDir = join(packageRoot, "lib", "runtime", "required");
    mkdirSync(exportDir, { recursive: true });
    process.env.PI_CODING_AGENT_DIR = agentDir;
    writeFileSync(join(packageRoot, "package.json"), JSON.stringify({
      name: "pi-subagents",
      exports: { "./required-child-extensions": "./lib/runtime/required/entry.js" },
    }));
    writeFileSync(join(exportDir, "entry.js"), "export function registerRequiredChildExtensions() { return { dispose() {} }; }");
    expect(typeof (await loadPiRequiredExtensionsApi())?.registerRequiredChildExtensions).toBe("function");

    const wrongAgent = join(root, "wrong-agent");
    const wrongPackage = join(wrongAgent, "npm", "node_modules", "pi-subagents");
    mkdirSync(wrongPackage, { recursive: true });
    process.env.PI_CODING_AGENT_DIR = wrongAgent;
    writeFileSync(join(wrongPackage, "package.json"), JSON.stringify({ name: "another-package" }));
    expect(await loadPiRequiredExtensionsApi()).toBeNull();
    const malformedAgent = join(root, "malformed-agent");
    const malformedPackage = join(malformedAgent, "npm", "node_modules", "pi-subagents");
    mkdirSync(join(malformedPackage, "lib", "runtime", "required"), { recursive: true });
    process.env.PI_CODING_AGENT_DIR = malformedAgent;
    writeFileSync(join(malformedPackage, "package.json"), JSON.stringify({
      name: "pi-subagents",
      exports: { "./required-child-extensions": "./lib/runtime/required/bad.js" },
    }));
    writeFileSync(join(malformedPackage, "lib", "runtime", "required", "bad.js"), "export const registerRequiredChildExtensions = true;");
    expect(await loadPiRequiredExtensionsApi()).toBeNull();
  });

  test("loads the renderer from a verified child host root when runner argv is elsewhere", async () => {
    const root = tempRoot();
    const hostRoot = join(root, "host-package");
    mkdirSync(join(hostRoot, "dist"), { recursive: true });
    writeFileSync(join(hostRoot, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent" }));
    writeFileSync(join(hostRoot, "dist", "index.js"), [
      "export function convertToLlm(messages) { return messages; }",
      "export function serializeConversation(messages) { return JSON.stringify(messages); }",
      "export function sessionEntryToContextMessages(entry) { return [entry]; }",
    ].join("\n"));
    process.env.PI_SUBAGENT_CHILD = "1";
    process.env.PI_SUBAGENT_RUNNER_PID = String(process.pid);
    process.env.PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT = hostRoot;
    expect(typeof (await loadPiCompactionRenderer(join(root, "runner", "bootstrap.js")))?.convertToLlm).toBe("function");

    const wrongRoot = join(root, "wrong-host");
    mkdirSync(join(wrongRoot, "dist"), { recursive: true });
    writeFileSync(join(wrongRoot, "package.json"), JSON.stringify({ name: "another-package" }));
    writeFileSync(join(wrongRoot, "dist", "index.js"), "export function convertToLlm() {};");
    process.env.PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT = wrongRoot;
    expect(await loadPiCompactionRenderer(join(root, "runner", "bootstrap.js"))).toBeNull();

    process.env.PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT = hostRoot;
    process.env.PI_SUBAGENT_RUNNER_PID = String(process.pid + 1);
    expect(await loadPiCompactionRenderer(join(root, "runner", "bootstrap.js"))).toBeNull();
  });

  test("canonicalizes duplicate status paths in run completion and consumed output", () => {
    const f = completionFixture();
    const alias = join(f.root, "child-alias.jsonl");
    symlinkSync(f.childFile, alias);
    f.writeStatus({ steps: [{ sessionFile: alias }] });
    expect(f.read()).toEqual({ runId: f.runId, sessionId: f.childId, responseId: f.responseId });
    expect(f.readDependencies()).toEqual([{ session_id: `pi:session:${f.childId}`, response_id: f.responseId, run_id: f.runId }]);
  });

  test("allows a missing optional path before a valid path in consumed output", () => {
    const f = completionFixture();
    f.writeStatus({ sessionFile: join(f.root, "not-yet-written.jsonl"), steps: [{ sessionFile: f.childFile }] });
    expect(f.read()).toEqual({ runId: f.runId, sessionId: f.childId, responseId: f.responseId });
    expect(f.readDependencies()).toEqual([{ session_id: `pi:session:${f.childId}`, response_id: f.responseId, run_id: f.runId }]);
  });

  test("rejects contradictory usable child path aliases even when one has the valid marker", () => {
    const f = completionFixture();
    const other = f.writeChild("child-other", "response-other", "different-run", join(f.root, "other-child.jsonl"));
    f.writeStatus({ steps: [{ sessionFile: other }] });
    expect(f.read()).toBeNull();
    expect(f.readDependencies()).toBeNull();
  });

  test("rejects a readable alias bound to a different parent session", () => {
    const f = completionFixture();
    const otherParent = join(f.root, "other-parent.jsonl");
    writeFileSync(otherParent, `${JSON.stringify({ type: "session", id: "other-parent" })}\n`);
    const wrongParentChild = f.writeChild("wrong-parent-child", "wrong-parent-response", "different-run",
      join(f.root, "wrong-parent-child.jsonl"), otherParent);
    f.writeStatus({ steps: [{ sessionFile: wrongParentChild }] });
    expect(f.read()).toBeNull();
    expect(f.readDependencies()).toBeNull();
  });

  test("rejects a readable status path with a malformed native header", () => {
    const f = completionFixture();
    const malformed = join(f.root, "malformed-child.jsonl");
    writeFileSync(malformed, `${JSON.stringify({ type: "session", parentSession: join(f.root, "parent.jsonl") })}\n`);
    f.writeStatus({ sessionFile: malformed, steps: [{ sessionFile: f.childFile }] });
    expect(f.read()).toBeNull();
    expect(f.readDependencies()).toBeNull();
  });

  test("fails closed on malformed status alias containers without throwing", () => {
    const f = completionFixture();
    for (const overrides of [{ steps: {} }, { steps: [null] }, { sessionFile: 42 }]) {
      f.writeStatus(overrides);
      expect(() => f.read()).not.toThrow();
      expect(f.read()).toBeNull();
      expect(() => f.readDependencies()).not.toThrow();
      expect(f.readDependencies()).toBeNull();
    }
  });

  test("resolves a long direct preview when the optional top-level path is missing", () => {
    const f = completionFixture();
    const fullOutput = "x".repeat(2_500);
    f.writeChild(f.childId, f.responseId, f.runId, f.childFile, join(f.root, "parent.jsonl"), fullOutput);
    const artifactsDir = join(f.root, "subagent-artifacts");
    mkdirSync(artifactsDir, { recursive: true });
    writeFileSync(join(artifactsDir, `${f.runId}_worker_output.md`), fullOutput);
    const asyncDir = join(f.root, "async-subagent-runs", f.runId);
    f.writeStatus({
      sessionFile: join(f.root, "missing-session-alias.jsonl"),
      artifactsDir,
      steps: [{ sessionFile: f.childFile, agent: "worker", exitCode: 0 }],
    });
    const preview = `${fullOutput.slice(0, 2_000)}\n…\nFull output: ${join(artifactsDir, `${f.runId}_worker_output.md`)} (2.4 KB, 1 line). Read it if needed.`;
    expect(f.readDependencies(preview)).toEqual([{ session_id: `pi:session:${f.childId}`, response_id: f.responseId, run_id: f.runId }]);
  });

});

function completionFixture() {
  const root = tempRoot();
  const runId = "run-identity";
  const parentId = "parent-identity";
  const childId = "child-identity";
  const responseId = "response-identity";
  const parentFile = join(root, "parent.jsonl");
  const childFile = join(root, "child.jsonl");
  const statusFile = join(root, "async-subagent-runs", runId, "status.json");
  mkdirSync(join(root, "async-subagent-runs", runId), { recursive: true });
  process.env.PI_SUBAGENTS_TEMP_ROOT = root;
  writeFileSync(parentFile, `${JSON.stringify({ type: "session", id: parentId })}\n`);
  const writeChild = (id: string, response: string, markerRun: string, path = childFile, parent = parentFile, output = "Completion output") => {
    writeFileSync(path, [
      { type: "session", id, parentSession: parent },
      { type: "message", message: { role: "assistant", responseId: response, stopReason: "stop", content: [{ type: "text", text: output }] } },
      { type: "custom", customType: PI_RUN_COMPLETION_ENTRY, data: { runId: markerRun, sessionId: id, responseId: response } },
    ].map((entry) => JSON.stringify(entry)).join("\n") + "\n");
    return path;
  };
  writeChild(childId, responseId, runId);
  let status: Record<string, unknown> = {};
  const writeStatus = (overrides: Record<string, unknown> = {}) => {
    status = {
      lifecycleArtifactVersion: 3, runId, sessionId: parentFile, toolCallId: "tool-call",
      mode: "single", state: "complete", sessionFile: childFile,
      steps: [{ sessionFile: childFile }],
      ...overrides,
    };
    writeFileSync(statusFile, JSON.stringify(status));
  };
  writeStatus();
  return {
    root, parentFile, childFile, runId, childId, responseId, writeChild, writeStatus,
    writeParent: (branch: any[]) => writeFileSync(parentFile,
      [{ type: "session", id: parentId }, ...branch].map((entry) => JSON.stringify(entry)).join("\n") + "\n"),
    read: (branch: any[] = [{
      type: "message", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent", id: "tool-call" }] },
    }]) => readPiRunCompletion(runId, parentId, "tool-call", parentFile, branch),
    readDependencies: (output = "Completion output") => {
      const asyncDir = join(root, "async-subagent-runs", runId);
      const branch = [
        { type: "message", id: "launch", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent", id: "tool-call" }] } },
        {
          type: "custom_message", id: "notice", customType: "subagent-notify",
          details: { runs: [{ runId, status: "completed", agent: "worker", asyncDir, sessionLabel: "Session file", sessionValue: childFile }] },
          content: `Background task completed: **worker**\n\nworker:\n${output}\n\nRetention-managed async directory: ${asyncDir}\n\nSession file: ${childFile}\n\nParent action: Review the result`,
        },
      ];
      return readPiUnconsumedRunDependencies({ sessionManager: {
        getSessionId: () => "parent-identity", getSessionFile: () => join(root, "parent.jsonl"),
        getBranch: () => branch,
      } } as any);
    },
  };
}

function delegationEntries(runId: string, action?: string): any[] {
  return [
    { type: "message", message: { role: "assistant", content: [{
      type: "toolCall", name: "subagent", id: "tool-call", arguments: action ? { action, id: "previous-run" } : { agent: "worker", async: true },
    }] } },
    { type: "message", message: { role: "toolResult", toolCallId: "tool-call", details: { runId, toolCallId: "tool-call" } } },
  ];
}
