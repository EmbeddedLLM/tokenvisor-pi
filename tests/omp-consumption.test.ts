import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import {
  OMP_RUN_COMPLETION_ENTRY,
  OMP_TASK_RESULT_CONSUMPTION_ENTRY,
  readOmpTaskResultDependencies,
} from "../src/capture/omp.ts";

const fixturesRoot = mkdtempSync(join(tmpdir(), "tokenvisor-omp-consumption-"));
afterAll(() => rmSync(fixturesRoot, { recursive: true, force: true }));

function createFixture(name: string) {
  const parentSessionId = `01a17d39-${name}-7173-a625-742eb0b76cd2`;
  const childSessionId = `01a17d39-${name}-8173-a625-742eb0b76cd2`;
  const agentUrlId = `Arbitrary-${name}`;
  const parentDirectory = join(fixturesRoot, name);
  const parentFile = join(parentDirectory, `${parentSessionId}.jsonl`);
  const childDirectory = join(parentDirectory, parentSessionId);
  const childFile = join(childDirectory, `${agentUrlId}.jsonl`);
  mkdirSync(childDirectory, { recursive: true });
  writeFileSync(parentFile, `${JSON.stringify({ type: "session", id: parentSessionId })}\n`);
  writeFileSync(childFile, `${JSON.stringify({ type: "session", id: childSessionId, parentSession: parentFile })}\n`);
  return { parentSessionId, childSessionId, agentUrlId, parentFile, childFile };
}

type ChildRecord = Record<string, any>;

function appendChildRecords(childFile: string, records: ChildRecord[]): void {
  writeFileSync(childFile, `${records.map((record) => JSON.stringify(record)).join("\n")}\n`, { flag: "a" });
}

function assistant(responseId: string, content: unknown[], stopReason = "stop"): ChildRecord {
  return {
    type: "message",
    id: `entry-${responseId}`,
    message: { role: "assistant", responseId, stopReason, content },
  };
}

function completion(fixture: ReturnType<typeof createFixture>, responseId: string): ChildRecord {
  return {
    type: "custom",
    customType: OMP_RUN_COMPLETION_ENTRY,
    data: {
      version: 1,
      runId: `omp:task:${fixture.agentUrlId}`,
      parentSessionId: fixture.parentSessionId,
      sessionId: fixture.childSessionId,
      responseId,
    },
  };
}

function buildWaitFixture(
  fixture: ReturnType<typeof createFixture>,
  job: Record<string, any> | Record<string, any>[],
  childRecords: ChildRecord[],
  parentRecords: ChildRecord[] = [],
) {
  appendChildRecords(fixture.childFile, childRecords);
  const waitToolCallId = `wait-call-${fixture.agentUrlId}`;
  const waitDeliveryId = `wait-result-${fixture.agentUrlId}`;
  const taskToolCallId = `task-call-${fixture.agentUrlId}`;
  const taskCall = {
    type: "message",
    id: `task-request-${fixture.agentUrlId}`,
    message: {
      role: "assistant",
      responseId: `task-request-response-${fixture.agentUrlId}`,
      content: [{ type: "toolCall", id: taskToolCallId, name: "task", arguments: {} }],
    },
  };
  const taskResult = {
    type: "message",
    id: `task-result-${fixture.agentUrlId}`,
    message: {
      role: "toolResult",
      toolName: "task",
      toolCallId: taskToolCallId,
      details: { progress: [{ id: fixture.agentUrlId }] },
    },
  };
  const waitCall = {
    type: "message",
    id: `wait-request-${fixture.agentUrlId}`,
    message: {
      role: "assistant",
      responseId: `wait-request-response-${fixture.agentUrlId}`,
      content: [{ type: "toolCall", id: waitToolCallId, name: "wait", arguments: {} }],
    },
  };
  const waitResult = {
    type: "message",
    id: waitDeliveryId,
    message: {
      role: "toolResult",
      toolName: "wait",
      toolCallId: waitToolCallId,
      details: { op: "wait", jobs: Array.isArray(job) ? job : [job] },
    },
  };
  const branch = [...parentRecords, taskCall, taskResult, waitCall, waitResult];
  const effectiveMessages = [
    {
      role: "toolResult",
      toolCallId: waitToolCallId,
      details: { op: "wait", jobs: Array.isArray(job) ? job : [job] },
    },
  ];
  return { branch, effectiveMessages, waitDeliveryId };
}

function taskJob(fixture: ReturnType<typeof createFixture>, resultText: string, id = `${fixture.agentUrlId}-2`) {
  return {
    id,
    type: "task",
    status: "completed",
    label: fixture.agentUrlId,
    agentUrlId: fixture.agentUrlId,
    resultText: resultText.replace(/^<task-result id="[^"]+"/, `<task-result id="${fixture.agentUrlId}"`),
  };
}

function taskSummary(output: string): string {
  return `<task-result id="Arbitrary" agent="task" status="completed" duration="1s">\n<output>\n${output}\n</output>\n</task-result>`;
}

function nativeTaskSummary(agentUrlId: string, output: string): string {
  return `<task-result id="${agentUrlId}" agent="yieldprobe" status="completed" duration="4.7s">\n\n<output>\n${output}\n</output>\n</task-result>\n\n${agentUrlId} is now idle — message it via \`write agent://${agentUrlId}\` to follow up; transcript at history://${agentUrlId}`;
}

function yieldCall(responseId: string | null, toolCallId: string, data: unknown): ChildRecord {
  return {
    type: "message",
    id: `entry-${responseId ?? "idless"}-${toolCallId}`,
    message: {
      role: "assistant",
      ...(responseId ? { responseId } : {}),
      stopReason: "toolUse",
      content: [{ type: "toolCall", id: toolCallId, name: "yield", arguments: { data } }],
    },
  };
}

function yieldResult(toolCallId: string, data: unknown): ChildRecord {
  return {
    type: "message",
    id: `yield-result-${toolCallId}`,
    message: {
      role: "toolResult",
      toolName: "yield",
      toolCallId,
      details: { status: "success", data },
    },
  };
}

describe("OMP explicit task-result consumption", () => {
  test("binds a task-summary body to one exact normalized native assistant response", () => {
    const fixture = createFixture("plain-task");
    const responseId = "chatcmpl-plain-task-response";
    const answer = "Report  \n\n| Name  | Value |\n| --- | :---: |\n| item  | ready  |\n";
    const normalized = "Report\n\n|Name|Value|\n|---|:---:|\n|item|ready|";
    const job = taskJob(fixture, taskSummary(normalized));
    const built = buildWaitFixture(fixture, job, [
      assistant(responseId, [{ type: "text", text: answer }]),
      completion(fixture, responseId),
    ]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toEqual({
      dependencies: [{
        session_id: `omp:session:${fixture.childSessionId}`,
        response_id: responseId,
        run_id: `omp:task:${fixture.agentUrlId}`,
      }],
      references: [{
        sessionId: fixture.childSessionId,
        responseId,
        runId: `omp:task:${fixture.agentUrlId}`,
        deliveryId: built.waitDeliveryId,
        jobId: job.id,
      }],
    });
  });

  test("binds a plain wake result by its exact trimmed text and keeps the suffixed job id", () => {
    const fixture = createFixture("plain-wake");
    const responseId = "chatcmpl-plain-wake-response";
    const job = taskJob(fixture, "Wake result", `${fixture.agentUrlId}-3`);
    const built = buildWaitFixture(fixture, job, [
      assistant(responseId, [{ type: "text", text: "  Wake result\n" }]),
      completion(fixture, responseId),
    ]);

    const result = readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages);
    expect(result?.dependencies[0]?.response_id).toBe(responseId);
    expect(result?.references[0]?.jobId).toBe(`${fixture.agentUrlId}-3`);
  });

  test("maps a single exact final-yield payload to the response that called yield", () => {
    const fixture = createFixture("final-yield");
    const responseId = "chatcmpl-final-yield-response";
    const terminalResponseId = "chatcmpl-final-yield-terminal";
    const yieldCallId = "yield-call-final-payload";
    const data = { answer: "ready", count: 3 };
    const job = {
      ...taskJob(fixture, taskSummary(JSON.stringify(data, null, 2))),
      structured: { source: "caller", mode: "permissive", status: "valid", data },
    };
    const built = buildWaitFixture(fixture, job, [
      assistant(responseId, [{ type: "toolCall", id: yieldCallId, name: "yield", arguments: { data } }], "toolUse"),
      {
        type: "message",
        id: "yield-result-entry",
        message: {
          role: "toolResult",
          toolName: "yield",
          toolCallId: yieldCallId,
          details: { status: "success", data },
        },
      },
      assistant(terminalResponseId, [{ type: "text", text: "Submission accepted." }]),
      completion(fixture, terminalResponseId),
    ]);

    const result = readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages);
    expect(result?.dependencies[0]?.response_id).toBe(responseId);
  });

  test("maps the native untyped yield payload from the rendered task summary to its toolUse response", () => {
    const fixture = createFixture("native-untyped-yield");
    const responseId = "chatcmpl-native-untyped-yield";
    const yieldCallId = "chatcmpl-tool-native-untyped-yield";
    const data = { phase: "native-yield", token: "OMP-YIELD-QUAL-20261031" };
    const job = taskJob(
      fixture,
      nativeTaskSummary(fixture.agentUrlId, JSON.stringify(data, null, 2)),
      fixture.agentUrlId,
    );
    const built = buildWaitFixture(fixture, job, [
      yieldCall(responseId, yieldCallId, data),
      yieldResult(yieldCallId, data),
    ]);

    const result = readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages);
    expect(result?.dependencies[0]?.response_id).toBe(responseId);
    expect(result?.references[0]?.jobId).toBe(fixture.agentUrlId);
  });

  test("allows another nonmatching yield in the same child session", () => {
    const fixture = createFixture("multiple-yields");
    const oldData = { result: "older" };
    const currentData = { result: "current" };
    const oldCallId = "yield-old";
    const currentCallId = "yield-current";
    const currentResponseId = "chatcmpl-current-yield";
    const job = taskJob(fixture, taskSummary(JSON.stringify(currentData, null, 2)));
    const built = buildWaitFixture(fixture, job, [
      yieldCall("chatcmpl-old-yield", oldCallId, oldData),
      yieldResult(oldCallId, oldData),
      yieldCall(currentResponseId, currentCallId, currentData),
      yieldResult(currentCallId, currentData),
    ]);

    const result = readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages);
    expect(result?.dependencies[0]?.response_id).toBe(currentResponseId);
  });

  test("maps an untyped string yield from its exact rendered task output", () => {
    const fixture = createFixture("native-string-yield");
    const responseId = "chatcmpl-native-string-yield";
    const callId = "yield-native-string";
    const payload = "exact string payload";
    const job = taskJob(fixture, taskSummary(payload));
    const built = buildWaitFixture(fixture, job, [
      yieldCall(responseId, callId, payload),
      yieldResult(callId, payload),
    ]);

    const result = readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages);
    expect(result?.dependencies[0]?.response_id).toBe(responseId);
  });

  test("leaves duplicate rendered yield payloads unknown, including an IDless caller", () => {
    const fixture = createFixture("duplicate-yields");
    const data = { result: "same" };
    const job = taskJob(fixture, taskSummary(JSON.stringify(data, null, 2)));
    const previousConsumerId = "chatcmpl-previous-yield-consumer";
    const previousConsumer = {
      type: "message",
      id: "previous-yield-consumer-entry",
      message: { role: "assistant", responseId: previousConsumerId, content: [{ type: "text", text: "consumed" }] },
    };
    const previousConsumption = {
      type: "custom",
      customType: OMP_TASK_RESULT_CONSUMPTION_ENTRY,
      data: {
        version: 1,
        parentSessionId: fixture.parentSessionId,
        consumer: { type: "assistant", id: previousConsumerId },
        references: [{
          sessionId: fixture.childSessionId,
          responseId: "chatcmpl-first-yield",
          runId: `omp:task:${fixture.agentUrlId}`,
          deliveryId: "prior-yield-delivery",
          jobId: "prior-yield-job",
        }],
      },
    };
    const built = buildWaitFixture(fixture, job, [
      yieldCall("chatcmpl-first-yield", "yield-first", data),
      yieldResult("yield-first", data),
      yieldCall(null, "yield-idless", data),
      yieldResult("yield-idless", data),
    ], [previousConsumer, previousConsumption]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toBeNull();
  });

  test("keeps a sole matching yield with a missing native response ID unknown", () => {
    const fixture = createFixture("idless-yield-caller");
    const data = { result: "unattributed" };
    const callId = "yield-idless-caller";
    const job = taskJob(fixture, taskSummary(JSON.stringify(data, null, 2)));
    const built = buildWaitFixture(fixture, job, [
      yieldCall(null, callId, data),
      yieldResult(callId, data),
    ]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toBeNull();
  });

  test("counts a plain assistant duplicate alongside an exact yield source", () => {
    const fixture = createFixture("yield-plain-duplicate");
    const data = { result: "same" };
    const output = JSON.stringify(data, null, 2);
    const responseId = "chatcmpl-yield-plain-duplicate";
    const callId = "yield-plain-duplicate";
    const job = taskJob(fixture, taskSummary(output));
    const built = buildWaitFixture(fixture, job, [
      yieldCall(responseId, callId, data),
      yieldResult(callId, data),
      assistant("chatcmpl-plain-duplicate", [{ type: "text", text: output }]),
      completion(fixture, "chatcmpl-plain-duplicate"),
    ]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toBeNull();
  });

  test("keeps schema-transformed yield output unknown", () => {
    const fixture = createFixture("schema-yield-unknown");
    const data = { answer: "normalized" };
    const responseId = "chatcmpl-schema-yield";
    const callId = "yield-schema-yield";
    const job = taskJob(fixture, taskSummary(JSON.stringify(data, null, 2)));
    const typedYieldResult = {
      type: "message",
      id: "typed-yield-result-entry",
      message: {
        role: "toolResult",
        toolName: "yield",
        toolCallId: callId,
        details: { status: "success", type: "object", data },
      },
    };
    const built = buildWaitFixture(fixture, job, [
      yieldCall(responseId, callId, data),
      typedYieldResult,
    ]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toBeNull();
  });

  test("does not pick the only unconsumed completion when exact output is ambiguous", () => {
    const fixture = createFixture("duplicate-output");
    const firstResponse = "chatcmpl-duplicate-first";
    const secondResponse = "chatcmpl-duplicate-second";
    const job = taskJob(fixture, "same result");
    const previousConsumer = {
      type: "message",
      id: "previous-consumer-entry",
      message: { role: "assistant", responseId: "chatcmpl-previous-consumer", content: [{ type: "text", text: "ack" }] },
    };
    const previousConsumption = {
      type: "custom",
      customType: OMP_TASK_RESULT_CONSUMPTION_ENTRY,
      data: {
        version: 1,
        parentSessionId: fixture.parentSessionId,
        consumer: { type: "assistant", id: "chatcmpl-previous-consumer" },
        references: [{
          sessionId: fixture.childSessionId,
          responseId: firstResponse,
          runId: `omp:task:${fixture.agentUrlId}`,
          deliveryId: "prior-delivery",
          jobId: "prior-job",
        }],
      },
    };
    const built = buildWaitFixture(fixture, job, [
      assistant(firstResponse, [{ type: "text", text: "same result" }]),
      completion(fixture, firstResponse),
      assistant(secondResponse, [{ type: "text", text: "same result" }]),
      completion(fixture, secondResponse),
    ], [previousConsumer, previousConsumption]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toBeNull();
  });

  test("does not bind a new job to the sole unconsumed marker without a result witness", () => {
    const fixture = createFixture("unwitnessed-latest");
    const firstResponse = "chatcmpl-unwitnessed-first";
    const latestResponse = "chatcmpl-unwitnessed-latest";
    const job = taskJob(fixture, taskSummary("Delivered output with no native source"));
    const previousConsumerId = "chatcmpl-unwitnessed-previous-consumer";
    const previousConsumer = {
      type: "message",
      id: "unwitnessed-previous-consumer-entry",
      message: { role: "assistant", responseId: previousConsumerId, content: [{ type: "text", text: "ack" }] },
    };
    const previousConsumption = {
      type: "custom",
      customType: OMP_TASK_RESULT_CONSUMPTION_ENTRY,
      data: {
        version: 1,
        parentSessionId: fixture.parentSessionId,
        consumer: { type: "assistant", id: previousConsumerId },
        references: [{
          sessionId: fixture.childSessionId,
          responseId: firstResponse,
          runId: `omp:task:${fixture.agentUrlId}`,
          deliveryId: "previous-unwitnessed-delivery",
          jobId: "previous-unwitnessed-job",
        }],
      },
    };
    const built = buildWaitFixture(fixture, job, [
      assistant(firstResponse, [{ type: "text", text: "Historical output" }]),
      completion(fixture, firstResponse),
      assistant(latestResponse, [{ type: "text", text: "Latest but unrelated output" }]),
      completion(fixture, latestResponse),
    ], [previousConsumer, previousConsumption]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toBeNull();
  });

  test("rejects a task summary whose native agent ID disagrees with the completed job", () => {
    const fixture = createFixture("mismatched-summary-id");
    const responseId = "chatcmpl-mismatched-summary-id";
    const job = taskJob(fixture, taskSummary("Exact output"));
    job.resultText = job.resultText.replace(`<task-result id="${fixture.agentUrlId}"`, '<task-result id="OtherAgent"');
    const built = buildWaitFixture(fixture, job, [
      assistant(responseId, [{ type: "text", text: "Exact output" }]),
      completion(fixture, responseId),
    ]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toBeNull();
  });

  test("does not create a second claim for an already-consumed delivery id", () => {
    const fixture = createFixture("repeat-delivery");
    const responseId = "chatcmpl-repeat-delivery-response";
    const job = taskJob(fixture, "Wake result");
    const priorConsumerId = "chatcmpl-repeat-delivery-consumer";
    const priorConsumer = {
      type: "message",
      id: "repeat-consumer-entry",
      message: { role: "assistant", responseId: priorConsumerId, content: [{ type: "text", text: "already consumed" }] },
    };
    const priorConsumption = {
      type: "custom",
      customType: OMP_TASK_RESULT_CONSUMPTION_ENTRY,
      data: {
        version: 1,
        parentSessionId: fixture.parentSessionId,
        consumer: { type: "assistant", id: priorConsumerId },
        references: [{
          sessionId: fixture.childSessionId,
          responseId,
          runId: `omp:task:${fixture.agentUrlId}`,
          deliveryId: `wait-result-${fixture.agentUrlId}`,
          jobId: job.id,
        }],
      },
    };
    const built = buildWaitFixture(fixture, job, [
      assistant(responseId, [{ type: "text", text: "Wake result" }]),
      completion(fixture, responseId),
    ], [priorConsumer, priorConsumption]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toEqual({
      dependencies: [],
      references: [],
    });
  });

  test("binds earlier exact output instead of the latest child completion", () => {
    const fixture = createFixture("earlier-output");
    const earlierResponse = "chatcmpl-earlier-output";
    const laterResponse = "chatcmpl-later-output";
    const job = taskJob(fixture, "<task-result id=\"Arbitrary\" agent=\"task\" status=\"completed\" duration=\"1s\">\n<output>\nEarlier answer\n</output>\n</task-result>");
    const built = buildWaitFixture(fixture, job, [
      assistant(earlierResponse, [{ type: "text", text: "Earlier answer" }]),
      completion(fixture, earlierResponse),
      assistant(laterResponse, [{ type: "text", text: "Later answer" }]),
      completion(fixture, laterResponse),
    ]);

    const result = readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages);
    expect(result?.dependencies[0]?.response_id).toBe(earlierResponse);
  });

  test("does not ignore an unmarked assistant with duplicate plain output", () => {
    const fixture = createFixture("unmarked-duplicate");
    const earlierResponse = "chatcmpl-unmarked-duplicate";
    const markedResponse = "chatcmpl-marked-duplicate";
    const job = taskJob(fixture, "Duplicate answer");
    const built = buildWaitFixture(fixture, job, [
      assistant(earlierResponse, [{ type: "text", text: "Duplicate answer" }]),
      assistant(markedResponse, [{ type: "text", text: "Duplicate answer" }]),
      completion(fixture, markedResponse),
    ]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toBeNull();
  });

  test("binds multiple same-agent jobs to distinct exact responses in one wait", () => {
    const fixture = createFixture("same-agent-jobs");
    const firstResponse = "chatcmpl-same-agent-first";
    const secondResponse = "chatcmpl-same-agent-second";
    const jobs = [
      taskJob(fixture, taskSummary("First result"), fixture.agentUrlId),
      taskJob(fixture, taskSummary("Second result"), `${fixture.agentUrlId}-2`),
    ];
    const built = buildWaitFixture(fixture, jobs, [
      assistant(firstResponse, [{ type: "text", text: "First result" }]),
      completion(fixture, firstResponse),
      assistant(secondResponse, [{ type: "text", text: "Second result" }]),
      completion(fixture, secondResponse),
    ]);

    const result = readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages);
    expect(result?.dependencies.map((dependency) => dependency.response_id)).toEqual([firstResponse, secondResponse]);
    expect(result?.references.map((reference) => reference.jobId)).toEqual([fixture.agentUrlId, `${fixture.agentUrlId}-2`]);
  });

  test("leaves truncated previews and unbound structured results unknown", () => {
    const fixture = createFixture("unsupported-results");
    const responseId = "chatcmpl-unsupported-response";
    const childRecords = [assistant(responseId, [{ type: "text", text: "Full answer" }]), completion(fixture, responseId)];
    const previewJob = taskJob(fixture, "<task-result id=\"Arbitrary\" status=\"completed\">\n<preview full-output=\"agent://Arbitrary\">Full…</preview>\n</task-result>");
    const preview = buildWaitFixture(fixture, previewJob, childRecords);
    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, preview.branch, preview.effectiveMessages)).toBeNull();

    const structuredJob = {
      ...taskJob(fixture, taskSummary("{\"answer\":\"different\"}")),
      structured: { source: "caller", mode: "permissive", status: "valid", data: { answer: "different" } },
    };
    const structured = buildWaitFixture(fixture, structuredJob, childRecords);
    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, structured.branch, structured.effectiveMessages)).toBeNull();
  });

  test("allows HTML-like text inside the task output slot while rejecting envelope markers", () => {
    const fixture = createFixture("html-output-slot");
    const responseId = "chatcmpl-html-output-slot";
    const output = 'Example markup: <meta name="viewport"> and <error>literal content</error>';
    const job = taskJob(fixture, taskSummary(output));
    const built = buildWaitFixture(fixture, job, [
      assistant(responseId, [{ type: "text", text: output }]),
      completion(fixture, responseId),
    ]);

    const dependency = readOmpTaskResultDependencies(
      fixture.parentFile,
      fixture.parentSessionId,
      built.branch,
      built.effectiveMessages,
    );
    expect(dependency?.dependencies[0]?.response_id).toBe(responseId);

    const envelopePreview = taskJob(fixture, `<task-result id="Arbitrary" status="completed">\n<meta full-output="agent://Arbitrary"/>\n<output>Example markup</output>\n</task-result>`);
    const preview = buildWaitFixture(fixture, envelopePreview, [
      assistant("chatcmpl-html-envelope-preview", [{ type: "text", text: "Example markup" }]),
      completion(fixture, "chatcmpl-html-envelope-preview"),
    ]);
    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, preview.branch, preview.effectiveMessages)).toBeNull();
  });

  test("rejects task output that collides with the output-slot closing delimiter", () => {
    const fixture = createFixture("output-delimiter-collision");
    const responseId = "chatcmpl-output-delimiter-collision";
    const output = "Leading text\n</output>\n</task-result>\ntrailing text";
    const job = taskJob(fixture, taskSummary(output));
    const built = buildWaitFixture(fixture, job, [
      assistant(responseId, [{ type: "text", text: output }]),
      completion(fixture, responseId),
    ]);

    expect(readOmpTaskResultDependencies(fixture.parentFile, fixture.parentSessionId, built.branch, built.effectiveMessages)).toBeNull();
  });
});
