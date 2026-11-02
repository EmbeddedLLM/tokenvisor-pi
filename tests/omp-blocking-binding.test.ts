import { afterAll, describe, expect, test } from "bun:test";
import { createHash } from "crypto";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import {
  clearOmpEarlyTaskBindingsForSession,
  clearOmpTaskBindingRetirementsForSession,
  queueOmpTaskBindingRetirement,
  readOmpBlockingTaskResultDependencies,
  readOmpEarlyTaskDelegation,
  readOmpTaskResultDependencies,
  registerOmpTaskLifecycleStarted,
  OMP_RUN_COMPLETION_ENTRY,
  retireOmpEarlyTaskBinding,
  type OmpRuntimeCaptureState,
} from "../src/capture/omp.ts";
import { readNativeSessionHeader } from "../src/capture/native-session.ts";

const fixturesRoot = mkdtempSync(join(tmpdir(), "tokenvisor-omp-blocking-binding-"));
afterAll(() => rmSync(fixturesRoot, { recursive: true, force: true }));

function createFixture(name: string, detached = false) {
  const directory = join(fixturesRoot, name);
  const parentSessionId = `01a1967e-${name}-b925-73fd-b992-d0d11d570d4c`;
  const childSessionId = `01a1967e-${name}-bc0a-71a2-aea3-8a7689baae80`;
  const allocatedId = `Registry-${name}`;
  const toolCallId = `call-${name}`;
  const parentSessionFile = join(directory, `${parentSessionId}.jsonl`);
  const childSessionFile = join(directory, parentSessionId, `${allocatedId}.jsonl`);
  mkdirSync(join(directory, parentSessionId), { recursive: true });
  writeFileSync(parentSessionFile, `${JSON.stringify({ type: "session", id: parentSessionId })}\n`);
  writeFileSync(childSessionFile, `${JSON.stringify({
    type: "session",
    id: childSessionId,
    parentSession: parentSessionFile,
  })}\n`);
  const state = {
    earlyTaskBindings: new Map(),
    pendingTaskBindingRetirements: new Map(),
  } as unknown as OmpRuntimeCaptureState;
  const lifecycle = {
    status: "started",
    id: allocatedId,
    agent: "arbitrary-display-role",
    parentToolCallId: toolCallId,
    detached,
    sessionFile: childSessionFile,
  };
  return {
    state,
    parentSessionId,
    childSessionId,
    allocatedId,
    toolCallId,
    parentSessionFile,
    childSessionFile,
    lifecycle,
  };
}

function taskSummary(allocatedId: string, output: string): string {
  return `<task-result id="${allocatedId}" agent="arbitrary-display-role" status="completed" duration="1s">\n\n<output>\n${output}\n</output>\n</task-result>`;
}

function blockingBranch(
  fixture: ReturnType<typeof createFixture>,
  output = "child answer",
  content = taskSummary(fixture.allocatedId, output),
) {
  const parentResponseId = `response-${fixture.parentSessionId}`;
  const resultEntryId = `result-entry-${fixture.parentSessionId}`;
  const assistant = {
    type: "message",
    id: `request-entry-${fixture.parentSessionId}`,
    message: {
      role: "assistant",
      responseId: parentResponseId,
      content: [{ type: "toolCall", id: fixture.toolCallId, name: "task", arguments: { agent: "arbitrary-display-role" } }],
    },
  };
  const result = {
    type: "message",
    id: resultEntryId,
    message: {
      role: "toolResult",
      toolName: "task",
      toolCallId: fixture.toolCallId,
      content: [{ type: "text", text: content }],
      details: {
        results: [{ id: fixture.allocatedId, exitCode: 0, output, truncated: false, aborted: false }],
      },
    },
  };
  return { branch: [assistant, result], effectiveMessages: [result.message], resultEntryId };
}

function appendChildResult(fixture: ReturnType<typeof createFixture>): void {
  const records = [
    {
      type: "message",
      id: "child-first-entry",
      message: { role: "assistant", responseId: "child-intermediate", stopReason: "stop", content: [{ type: "text", text: "thinking" }] },
    },
    {
      type: "message",
      id: "child-terminal-entry",
      message: { role: "assistant", responseId: "child-terminal", stopReason: "stop", content: [{ type: "text", text: "child answer" }] },
    },
    {
      type: "custom",
      customType: OMP_RUN_COMPLETION_ENTRY,
      data: {
        version: 1,
        runId: `omp:task:${fixture.allocatedId}`,
        parentSessionId: fixture.parentSessionId,
        sessionId: fixture.childSessionId,
        responseId: "child-terminal",
      },
    },
  ];
  writeFileSync(fixture.childSessionFile, `${records.map((record) => JSON.stringify(record)).join("\n")}\n`, { flag: "a" });
}

function register(fixture: ReturnType<typeof createFixture>): boolean {
  return registerOmpTaskLifecycleStarted(
    fixture.state,
    fixture.parentSessionFile,
    fixture.parentSessionId,
    fixture.lifecycle,
  );
}

describe("OMP fresh blocking-task binding", () => {
  if (process.env.TOKENVISOR_OMP_CAPTURE_BENCHMARK === "1") {
    test("measures the added lifecycle and blocking-result helper cost", () => {
      const fixture = createFixture(`benchmark-${process.pid}`);
      appendChildResult(fixture);
      const built = blockingBranch(fixture);
      const inputFingerprint = createHash("sha256").update(JSON.stringify({
        parentId: fixture.parentSessionId,
        branch: built.branch,
        effectiveMessages: built.effectiveMessages,
      })).digest("hex");
      const baseCapture = () => {
        const header = readNativeSessionHeader(fixture.parentSessionFile);
        const dependencies = readOmpTaskResultDependencies(
          fixture.parentSessionFile,
          fixture.parentSessionId,
          built.branch,
          built.effectiveMessages,
        );
        if (header?.id !== fixture.parentSessionId || dependencies?.dependencies.length !== 0) {
          throw new Error("The identity-only benchmark baseline changed its result");
        }
      };
      const lifecycleCapture = () => {
        if (!register(fixture)) throw new Error("The native lifecycle fixture did not validate");
        const result = readOmpBlockingTaskResultDependencies(
          fixture.state,
          fixture.parentSessionFile,
          fixture.parentSessionId,
          built.branch,
          built.effectiveMessages,
        );
        if (result?.dependencies.length !== 1 || result.references.length !== 1) {
          throw new Error("The blocking-result fixture did not resolve one child response");
        }
      };
      const baselineSamples: number[] = [];
      const lifecycleSamples: number[] = [];
      const dispatchSamples: number[] = [];
      const extractionSamples: number[] = [];
      for (let index = 0; index < 8; index += 1) {
        baseCapture();
        lifecycleCapture();
      }
      for (let index = 0; index < 32; index += 1) {
        let start = performance.now();
        baseCapture();
        baselineSamples.push(performance.now() - start);

        start = performance.now();
        baseCapture();
        const identityCaptured = performance.now();
        if (!register(fixture)) throw new Error("The native lifecycle fixture did not validate");
        const dispatched = performance.now();
        const result = readOmpBlockingTaskResultDependencies(
          fixture.state,
          fixture.parentSessionFile,
          fixture.parentSessionId,
          built.branch,
          built.effectiveMessages,
        );
        const finished = performance.now();
        if (result?.dependencies.length !== 1 || result.references.length !== 1) {
          throw new Error("The blocking-result fixture did not resolve one child response");
        }
        dispatchSamples.push(dispatched - identityCaptured);
        extractionSamples.push(finished - dispatched);
        lifecycleSamples.push(finished - start);
      }
      const percentile = (samples: number[], fraction: number) => {
        const ordered = [...samples].sort((left, right) => left - right);
        return Number(ordered[Math.ceil(ordered.length * fraction) - 1]!.toFixed(3));
      };
      console.log("OMP_CAPTURE_BENCHMARK", JSON.stringify({
        samples: baselineSamples.length,
        warmup: 8,
        inputFingerprint,
        baselineIdentityAndExistingDependencyP50P95Ms: [percentile(baselineSamples, 0.5), percentile(baselineSamples, 0.95)],
        lifecycleRegistrationP50P95Ms: [percentile(dispatchSamples, 0.5), percentile(dispatchSamples, 0.95)],
        blockingExtractionP50P95Ms: [percentile(extractionSamples, 0.5), percentile(extractionSamples, 0.95)],
        enabledCombinedP50P95Ms: [percentile(lifecycleSamples, 0.5), percentile(lifecycleSamples, 0.95)],
        delegateCalls: 0,
        note: "Direct capture-helper measurement; provider dispatch, request serialization, and delegate/cache behavior are outside this benchmark.",
      }));
      clearOmpEarlyTaskBindingsForSession(fixture.state, fixture.parentSessionId);
      clearOmpTaskBindingRetirementsForSession(fixture.state, fixture.parentSessionId);
      expect([...fixture.state.earlyTaskBindings.values()].filter((binding: any) =>
        binding.parentSessionId === fixture.parentSessionId,
      )).toHaveLength(0);
      expect(fixture.state.pendingTaskBindingRetirements.has(fixture.parentSessionId)).toBe(false);
    });
  }

  test("shares bounded response-keyed retirement records across provider registrations", () => {
    /** Ensures child-side stream capture can queue exact binding retirement for the parent callback. */
    const fixture = createFixture("shared-retirement-queue");
    const responseId = "root-response-after-task";
    const streamId = "root-stream-after-task";
    expect(queueOmpTaskBindingRetirement(
      fixture.state,
      fixture.parentSessionId,
      responseId,
      streamId,
      [fixture.allocatedId, fixture.allocatedId],
    )).toBe(true);
    expect(queueOmpTaskBindingRetirement(
      fixture.state,
      fixture.parentSessionId,
      responseId,
      streamId,
      [fixture.allocatedId],
    )).toBe(true);
    expect(fixture.state.pendingTaskBindingRetirements.get(fixture.parentSessionId)).toEqual(
      new Map([[responseId, { streamId, bindingIds: [fixture.allocatedId] }]]),
    );
    expect(queueOmpTaskBindingRetirement(
      fixture.state,
      fixture.parentSessionId,
      responseId,
      "different-stream",
      [fixture.allocatedId],
    )).toBe(false);
    for (let index = 0; index < 63; index += 1) {
      expect(queueOmpTaskBindingRetirement(
        fixture.state,
        fixture.parentSessionId,
        `response-${index}`,
        `stream-${index}`,
        [`binding-${index}`],
      )).toBe(true);
    }
    expect(queueOmpTaskBindingRetirement(
      fixture.state,
      fixture.parentSessionId,
      "overflow-response",
      "overflow-stream",
      ["overflow-binding"],
    )).toBe(false);
    clearOmpTaskBindingRetirementsForSession(fixture.state, fixture.parentSessionId);
    expect(fixture.state.pendingTaskBindingRetirements.has(fixture.parentSessionId)).toBe(false);
  });

  test("returns an empty result before parent-file validation when no task result exists", () => {
    /** Ensures ordinary root requests without a blocking task result do not get unresolved dependencies. */
    const fixture = createFixture("no-task-result");
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      undefined,
      fixture.parentSessionId,
      [],
      [],
    )).toEqual({ dependencies: [], references: [], bindingIds: [] });
  });

  test("keeps an actual task result unresolved when its native parent file is unavailable", () => {
    /** Ensures the no-result fast path does not weaken native parent proof for task deliveries. */
    const fixture = createFixture("missing-parent-for-task-result");
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      `${fixture.parentSessionFile}.missing`,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toBeNull();
  });

  test("resolves a started binding by allocated ID and the exact terminal child response", () => {
    /** Ensures an arbitrary role label and allocated task ID bind through native session evidence, not names. */
    const fixture = createFixture("inline");
    appendChildResult(fixture);

    expect(register(fixture)).toBe(true);
    expect(readOmpEarlyTaskDelegation(fixture.state, fixture.allocatedId, fixture.childSessionFile)).toEqual({
      runId: `omp:task:${fixture.allocatedId}`,
      toolCallId: fixture.toolCallId,
      detached: false,
    });
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({
      dependencies: [{
        session_id: `omp:session:${fixture.childSessionId}`,
        response_id: "child-terminal",
        run_id: `omp:task:${fixture.allocatedId}`,
      }],
      bindingIds: [fixture.allocatedId],
      references: [{
        sessionId: fixture.childSessionId,
        responseId: "child-terminal",
        runId: `omp:task:${fixture.allocatedId}`,
        deliveryId: built.resultEntryId,
        jobId: fixture.allocatedId,
      }],
    });
    clearOmpEarlyTaskBindingsForSession(fixture.state, fixture.parentSessionId);
    expect(readOmpEarlyTaskDelegation(fixture.state, fixture.allocatedId, fixture.childSessionFile)).toBeNull();
  });

  test("keeps launch identity but does not infer delivery after the normal tool output is filtered", () => {
    /** Ensures a successful child launch alone does not become a delivered-result dependency. */
    const fixture = createFixture("filtered");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    const filtered = blockingBranch(fixture, "child answer", "OMP_OUTPUT_WITHHELD");
    const result = readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      filtered.branch,
      filtered.effectiveMessages,
    );
    expect(result).toEqual({ dependencies: [], references: [], bindingIds: [fixture.allocatedId] });
  });

  test("retires a verified launch when the effective request omits the native task result", () => {
    /** Ensures request-local filtering cannot retain a native lifecycle binding after the parent continues. */
    const fixture = createFixture("result-omitted-from-context");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      [],
    )).toEqual({ dependencies: [], references: [], bindingIds: [fixture.allocatedId] });
  });

  test("retires a verified launch when effective request content differs from the native result", () => {
    /** Ensures changed request content suppresses delivery inference without suppressing cleanup identity. */
    const fixture = createFixture("result-content-changed-in-context");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    const built = blockingBranch(fixture);
    const nativeResultContent = structuredClone(built.branch[1]!.message.content);
    const changedEffectiveMessage = {
      ...built.effectiveMessages[0]!,
      content: [{ type: "text", text: "FILTERED_AFTER_NATIVE_COMMIT" }],
    };
    expect(built.branch[1]!.message.content).toEqual(nativeResultContent);
    expect(changedEffectiveMessage.content).not.toEqual(nativeResultContent);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      [changedEffectiveMessage],
    )).toEqual({ dependencies: [], references: [], bindingIds: [fixture.allocatedId] });
  });

  test("marks the dependency unavailable when the verified child file contains malformed JSON", () => {
    /** Ensures a known successful delivery with unreadable child evidence cannot export as dependency-free. */
    const fixture = createFixture("malformed-child-json");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    writeFileSync(fixture.childSessionFile, `${JSON.stringify({
      type: "session",
      id: fixture.childSessionId,
      parentSession: fixture.parentSessionFile,
    })}\n{malformed-json}\n`);
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: null, references: [], bindingIds: [fixture.allocatedId] });
  });

  test("marks the dependency unavailable when the verified child file ends in a partial record", () => {
    /** Ensures a truncated native child file remains unresolved instead of producing an empty dependency list. */
    const fixture = createFixture("truncated-child-file");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    writeFileSync(fixture.childSessionFile, `${JSON.stringify({
      type: "session",
      id: fixture.childSessionId,
      parentSession: fixture.parentSessionFile,
    })}\n{"type":"message","message":`);
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: null, references: [], bindingIds: [fixture.allocatedId] });
  });

  test("marks a truncated successful task result unavailable after verifying its launch", () => {
    /** Ensures OMP's explicit output truncation flag cannot look like a dependency-free successful export. */
    const fixture = createFixture("truncated-task-output");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    const built = blockingBranch(fixture);
    built.branch[1]!.message.details.results[0].truncated = true;
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: null, references: [], bindingIds: [fixture.allocatedId] });
  });

  test("marks ambiguous matching child responses unavailable", () => {
    /** Ensures multiple child responses matching one delivered output are diagnosed rather than guessed. */
    const fixture = createFixture("ambiguous-child-response");
    appendChildResult(fixture);
    writeFileSync(fixture.childSessionFile, `${JSON.stringify({
      type: "message",
      id: "child-duplicate-response-entry",
      message: {
        role: "assistant",
        responseId: "child-terminal",
        stopReason: "stop",
        content: [{ type: "text", text: "child answer" }],
      },
    })}\n`, { flag: "a" });
    expect(register(fixture)).toBe(true);
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: null, references: [], bindingIds: [fixture.allocatedId] });
  });

  test("marks a multi-result blocking channel unavailable instead of exporting a partial dependency set", () => {
    /** Ensures unsupported task batches fail closed when a known child launch appears in the result. */
    const fixture = createFixture("multi-result-task-delivery");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    const built = blockingBranch(fixture);
    built.branch[1]!.message.details.results.push({
      id: "second-allocated-id",
      exitCode: 0,
      output: "second child answer",
      truncated: false,
      aborted: false,
    });
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: null, references: [], bindingIds: [fixture.allocatedId] });
  });

  test("does not bind a detached task through the blocking path", () => {
    /** Ensures detached lifecycle events stay on their separately qualified explicit-wait path. */
    const fixture = createFixture("detached", true);
    appendChildResult(fixture);
    expect(register(fixture)).toBe(false);
    expect(readOmpEarlyTaskDelegation(fixture.state, fixture.allocatedId, fixture.childSessionFile)).toBeNull();
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: [], references: [], bindingIds: [] });
  });

  test("does not use the last child or latest response when the allocated result ID is unknown", () => {
    /** Ensures a successful result with a known task call but unknown allocated ID stays unresolved. */
    const fixture = createFixture("unknown-id");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    const built = blockingBranch(fixture);
    built.branch[1].message.details.results[0].id = "different-allocated-id";
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: null, references: [], bindingIds: [fixture.allocatedId] });
  });

  test("marks a successful result unavailable when the registered child header is later corrupted", () => {
    /** Ensures loss of native child identity after launch registration cannot become a dependency-free export. */
    const fixture = createFixture("corrupted-child-header");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    writeFileSync(fixture.childSessionFile, `${JSON.stringify({
      type: "session",
      id: "different-child-session",
      parentSession: fixture.parentSessionFile,
    })}\n`, { flag: "w" });
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: null, references: [], bindingIds: [fixture.allocatedId] });
  });

  test("marks a successful result unavailable when its registered binding is conflicted", () => {
    /** Ensures conflicting lifecycle identities for a known task call cannot be silently ignored. */
    const fixture = createFixture("conflicted-binding");
    appendChildResult(fixture);
    expect(register(fixture)).toBe(true);
    const alternateChildFile = `${fixture.childSessionFile}.alternate`;
    writeFileSync(alternateChildFile, `${JSON.stringify({
      type: "session",
      id: "alternate-child-session",
      parentSession: fixture.parentSessionFile,
    })}\n`);
    expect(registerOmpTaskLifecycleStarted(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      {
        ...fixture.lifecycle,
        sessionFile: alternateChildFile,
      },
    )).toBe(false);
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: null, references: [], bindingIds: [fixture.allocatedId] });
  });

  test("marks an earlier completion marker unavailable after a later child response", () => {
    /** Ensures resumed or otherwise extended child histories cannot silently lose a known delivery dependency. */
    const fixture = createFixture("nonterminal-marker");
    appendChildResult(fixture);
    writeFileSync(fixture.childSessionFile, `${JSON.stringify({
      type: "message",
      id: "child-later-entry",
      message: { role: "assistant", responseId: "child-later", stopReason: "stop", content: [{ type: "text", text: "newer answer" }] },
    })}\n`, { flag: "a" });
    expect(register(fixture)).toBe(true);
    const built = blockingBranch(fixture);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({ dependencies: null, references: [], bindingIds: [fixture.allocatedId] });
  });

  test("allows helper-level retirement to recycle filtered launch bindings beyond the registry cap", () => {
    /** Ensures the bounded binding helper can recycle entries when its caller retires committed work. */
    const fixture = createFixture("filtered-registry-recycle");
    const branch: Array<Record<string, any>> = [];
    for (let index = 0; index < 80; index += 1) {
      const agentId = `Allocated-${index}`;
      const childSessionId = `01a1967e-${index}-bc0a-71a2-aea3-8a7689baae80`;
      const childSessionFile = join(dirname(fixture.parentSessionFile), fixture.parentSessionId, `${agentId}.jsonl`);
      writeFileSync(childSessionFile, `${JSON.stringify({
        type: "session",
        id: childSessionId,
        parentSession: fixture.parentSessionFile,
      })}\n`);
      const toolCallId = `call-filtered-${index}`;
      const lifecycle = {
        status: "started",
        id: agentId,
        agent: "reused-display-name",
        parentToolCallId: toolCallId,
        detached: false,
        sessionFile: childSessionFile,
      };
      expect(registerOmpTaskLifecycleStarted(
        fixture.state,
        fixture.parentSessionFile,
        fixture.parentSessionId,
        lifecycle,
      )).toBe(true);
      expect(fixture.state.earlyTaskBindings.size).toBe(1);

      branch.push({
        type: "message",
        id: `parent-request-${index}`,
        message: {
          role: "assistant",
          responseId: `parent-response-${index}`,
          content: [{ type: "toolCall", id: toolCallId, name: "task", arguments: { agent: "reused-display-name" } }],
        },
      });
      const toolResult = {
        type: "message",
        id: `parent-result-${index}`,
        message: {
          role: "toolResult",
          toolName: "task",
          toolCallId,
          content: [{ type: "text", text: taskSummary(agentId, `answer-${index}`) }],
          details: { results: [{ id: agentId, exitCode: 0, output: `answer-${index}`, truncated: false }] },
        },
      };
      branch.push(toolResult);
      const result = readOmpBlockingTaskResultDependencies(
        fixture.state,
        fixture.parentSessionFile,
        fixture.parentSessionId,
        branch,
        [],
      );
      expect(result).toEqual({ dependencies: [], references: [], bindingIds: [agentId] });

      clearOmpEarlyTaskBindingsForSession(fixture.state, childSessionId);
      expect(fixture.state.earlyTaskBindings.size).toBe(1);
      branch.push({
        type: "message",
        id: `parent-committed-${index}`,
        message: { role: "assistant", responseId: `parent-committed-response-${index}`, content: [{ type: "text", text: "continued" }] },
      });
      retireOmpEarlyTaskBinding(fixture.state, fixture.parentSessionId, agentId);
      expect(fixture.state.earlyTaskBindings.size).toBe(0);
    }
  });

  test("binds the default structured yield to its exact caller response without a stop marker", () => {
    /** Ensures task.structured true without a caller schema uses the exact untyped yield caller. */
    const fixture = createFixture("structured-yield");
    const structuredData = { answer: "ready", count: 3 };
    const records = [
      {
        type: "message",
        id: "child-yield-call-entry",
        message: {
          role: "assistant",
          responseId: "child-yield-call",
          stopReason: "toolUse",
          content: [{ type: "toolCall", id: "yield-structured", name: "yield", arguments: { data: structuredData } }],
        },
      },
      {
        type: "message",
        id: "child-yield-result-entry",
        message: {
          role: "toolResult",
          toolName: "yield",
          toolCallId: "yield-structured",
          details: { status: "success", data: structuredData },
        },
      },
    ];
    writeFileSync(fixture.childSessionFile, `${records.map((record) => JSON.stringify(record)).join("\n")}\n`, { flag: "a" });
    expect(register(fixture)).toBe(true);
    const output = JSON.stringify(structuredData, null, 2);
    const built = blockingBranch(fixture, output);
    expect(readOmpBlockingTaskResultDependencies(
      fixture.state,
      fixture.parentSessionFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    )).toEqual({
      dependencies: [{
        session_id: `omp:session:${fixture.childSessionId}`,
        response_id: "child-yield-call",
        run_id: `omp:task:${fixture.allocatedId}`,
      }],
      bindingIds: [fixture.allocatedId],
      references: [{
        sessionId: fixture.childSessionId,
        responseId: "child-yield-call",
        runId: `omp:task:${fixture.allocatedId}`,
        deliveryId: built.resultEntryId,
        jobId: fixture.allocatedId,
      }],
    });
  });
});
