// Optional timing experiments reuse the exact regression fixtures. They run only
// through scripts/benchmark-capture.mjs and impose no CI performance thresholds.
import { expect } from "bun:test";
import { createHash } from "crypto";
import { readFileSync } from "fs";
import { join } from "path";
import { parseAgentMetadata } from "../../src/capture/shared.ts";
import { PI_RESULT_CONSUMPTION_ENTRY } from "../../src/capture/pi.ts";
import {
  clearOmpEarlyTaskBindingsForSession,
  clearOmpTaskBindingRetirementsForSession,
  getOmpRuntimeCaptureState,
  OMP_RUN_COMPLETION_ENTRY,
  readOmpEarlyTaskDelegation,
  readOmpBlockingTaskResultDependencies,
  readOmpTaskResultDependencies,
} from "../../src/capture/omp.ts";
import { readNativeSessionHeader } from "../../src/capture/native-session.ts";

export async function benchmarkPiPreview({
  h,
  rootId,
  parentCall,
  parentResult,
  notice,
  writeSession,
  parentFile,
  parentHeader,
}: any) {
  const sampleCount = 24;
  const warmupCount = 8;
  const samples = {
    pinned_preview_enabled: { header: [] as number[], stream: [] as number[] },
  };
  const fingerprints: string[] = [];
  const delegates = { pinned_preview_enabled: 0 };
  const dependencyCounts = { pinned_preview_enabled: 0 };
  const quantile = (values: number[], fraction: number) => {
    const sorted = [...values].sort((left, right) => left - right);
    return Number(sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)]!.toFixed(3));
  };
  const arms = [["pinned_preview_enabled", h]] as const;
  const effectiveContext = {
    messages: [
      {
        role: "user",
        content: [
          {
            type: "text",
            text: `<pi_subagents_notice type="subagent-notify" origin="automated message from the pi-subagents extension, not typed by the user">\n${notice.content}\n</pi_subagents_notice>`,
          },
        ],
      },
    ],
  };
  for (let sample = 0; sample < warmupCount + sampleCount; sample += 1) {
    for (const [arm, harness] of arms) {
      const marker = `PI_PREVIEW_TRACE ${sample} ${arm}`;
      const ctx = h.sessionContext(rootId);
      const sampleBranch = [parentCall, parentResult, notice];
      writeSession(parentFile, parentHeader, sampleBranch);
      h.branches.set(rootId, sampleBranch);
      harness.branches.set(rootId, sampleBranch);
      harness.hooks.get("session_shutdown")!({}, ctx);
      harness.hooks.get("session_start")!({ reason: "startup" }, ctx);
      expect(ctx.sessionManager.getBranch()).toBe(sampleBranch);
      const headers = new Headers();
      process.stderr.write(`${marker} header begin\n`);
      const headerStart = performance.now();
      harness.hooks.get("before_provider_headers")!({ headers }, ctx);
      const headerEnd = performance.now();
      process.stderr.write(`${marker} header end\n`);
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: `bench-preview-${sample}-${arm}`,
        stopReason: "stop",
        content: [],
      };
      (globalThis as any).__PI_TEST_CAPTURED_CALLS__ = 0;
      const model = { id: "synthetic", api: "openai-completions", headers };
      process.stderr.write(`${marker} stream begin\n`);
      const streamStart = performance.now();
      await harness.providerConfig.streamSimple(model, effectiveContext, { headers }).result();
      const streamEnd = performance.now();
      process.stderr.write(`${marker} stream end\n`);
      await Promise.resolve();
      const captured = (globalThis as any).__PI_TEST_CAPTURED_REQUEST__;
      const metadata = parseAgentMetadata(
        new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__?.headers ?? []).get("x-tokenvisor-agent"),
      );
      const modelInput = { ...captured?.model };
      delete modelInput.headers;
      fingerprints.push(
        createHash("sha256")
          .update(JSON.stringify({ model: modelInput, context: captured?.context }))
          .digest("hex"),
      );
      if (sample >= warmupCount) {
        samples[arm].header.push(headerEnd - headerStart);
        samples[arm].stream.push(streamEnd - streamStart);
        delegates[arm] += (globalThis as any).__PI_TEST_CAPTURED_CALLS__;
        dependencyCounts[arm] += metadata?.depends_on?.length ?? 0;
      }
      const carrierResponseId = `bench-preview-${sample}-${arm}`;
      sampleBranch.push({
        type: "message",
        id: `${carrierResponseId}-entry`,
        message: {
          role: "assistant",
          responseId: carrierResponseId,
          stopReason: "stop",
          content: [],
        },
      });
      harness.appendFor(rootId);
      const carrierEvent = {
        messages: [
          {
            role: "assistant",
            responseId: carrierResponseId,
            stopReason: "stop",
          },
        ],
      };
      harness.hooks.get("agent_end")!(carrierEvent, ctx);
      const consumedCount = sampleBranch.filter(
        (entry: any) =>
          entry.customType === PI_RESULT_CONSUMPTION_ENTRY && entry.data?.response_id === carrierResponseId,
      ).length;
      expect(consumedCount).toBe(metadata?.depends_on?.length ? 1 : 0);
      harness.hooks.get("agent_end")!(carrierEvent, ctx);
      expect(
        sampleBranch.filter(
          (entry: any) =>
            entry.customType === PI_RESULT_CONSUMPTION_ENTRY && entry.data?.response_id === carrierResponseId,
        ).length,
      ).toBe(consumedCount);
      harness.hooks.get("session_shutdown")!({}, ctx);
    }
  }
  const fingerprintSet = new Set(fingerprints);
  expect(fingerprintSet.size).toBe(1);
  expect(delegates).toEqual({ pinned_preview_enabled: sampleCount });
  expect(dependencyCounts).toEqual({ pinned_preview_enabled: sampleCount });
  const timings = Object.fromEntries(
    Object.entries(samples).map(([arm, phases]) => [
      arm,
      Object.fromEntries(
        Object.entries(phases).map(([phase, values]) => [
          phase,
          { p50_ms: quantile(values, 0.5), p95_ms: quantile(values, 0.95) },
        ]),
      ),
    ]),
  );
  console.log(
    `PI_PINNED_PREVIEW_BENCHMARK ${JSON.stringify({
      classification:
        "absolute registered Pi callback cost for verified background previews; no disabled-preview baseline seam",
      samples_per_arm: sampleCount,
      warmup_per_arm: warmupCount,
      timings_ms_p50_p95: timings,
      delegate_calls: delegates,
      expected_result_dependencies: dependencyCounts,
      effective_input_fingerprint_equal: fingerprintSet.size === 1,
      trace_root: h.root,
      trace_markers: "PI_PREVIEW_TRACE <sample> <arm> <header|stream> <begin|end>",
      limitations: [
        "fake Pi AI delegate; no serialized HTTP body, cache, network, or production model latency measured",
      ],
    })}`,
  );
}

export async function benchmarkOmpCompaction({
  branch,
  sessionId,
  hooks,
  ctx,
  captureNextMetadata,
  root,
  selectSession,
}: any) {
  const sampleCount = 32;
  const warmupCount = 8;
  const compactMs: number[] = [];
  const fingerprints: string[] = [];
  const delegateCallsBefore = ((globalThis as any).__OMP_PROVIDER_REQUESTS__ ?? []).length;
  const quantile = (values: number[], fraction: number) => {
    const sorted = [...values].sort((left, right) => left - right);
    return Number(sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)]!.toFixed(3));
  };
  for (let sample = 0; sample < warmupCount + sampleCount; sample += 1) {
    const predecessorId = `omp-bench-predecessor-${sample}`;
    const predecessor = {
      type: "message",
      id: `omp-bench-entry-${sample}`,
      message: {
        role: "assistant",
        responseId: predecessorId,
        stopReason: "stop",
        content: [],
      },
    };
    branch.splice(0, branch.length, predecessor);
    selectSession();
    hooks.get("session_before_compact")!({}, ctx);
    const compactionEntry = {
      type: "compaction",
      id: `omp-bench-compact-${sample}`,
      firstKeptEntryId: predecessor.id,
      method: "snapcompact",
      fromExtension: false,
    };
    branch.push(compactionEntry);
    process.stderr.write(`OMP_COMPACTION_TRACE ${sample} handler begin\n`);
    const compactStart = performance.now();
    hooks.get("session_compact")!({ fromExtension: false, compactionEntry }, ctx);
    const compactEnd = performance.now();
    process.stderr.write(`OMP_COMPACTION_TRACE ${sample} handler end\n`);

    const carrierResponseId = `omp-bench-carrier-${sample}`;
    const metadata = await captureNextMetadata(carrierResponseId);
    expect(metadata?.depends_on).toEqual([]);
    expect(metadata?.sequence.previous_response_id).toBe(predecessorId);
    const captured = (globalThis as any).__OMP_PROVIDER_REQUESTS__?.at(-1);
    const modelInput = { ...captured?.model };
    delete modelInput.headers;
    fingerprints.push(
      createHash("sha256")
        .update(
          JSON.stringify({
            model: modelInput,
            context: captured?.context,
          }),
        )
        .digest("hex"),
    );
    if (sample >= warmupCount) compactMs.push(compactEnd - compactStart);
    branch.push({
      type: "message",
      id: `omp-bench-carrier-entry-${sample}`,
      message: {
        role: "assistant",
        responseId: carrierResponseId,
        stopReason: "stop",
        content: [],
      },
    });
  }
  expect(fingerprints).toHaveLength(warmupCount + sampleCount);
  expect(new Set(fingerprints).size).toBe(1);
  const streamDelegateCalls = ((globalThis as any).__OMP_PROVIDER_REQUESTS__ ?? []).length - delegateCallsBefore;
  expect(streamDelegateCalls).toBe(warmupCount + sampleCount);
  console.log(
    `OMP_COMPACTION_BENCHMARK ${JSON.stringify({
      classification: "absolute registered OMP session_compact callback time; no incremental baseline seam",
      samples: sampleCount,
      warmup: warmupCount,
      handler_ms: { p50: quantile(compactMs, 0.5), p95: quantile(compactMs, 0.95) },
      identical_next_stream_input_fingerprint: new Set(fingerprints).size === 1,
      next_stream_delegate_calls: streamDelegateCalls,
      next_stream_known_empty_dependencies: true,
      trace_root: root,
      trace_markers: "OMP_COMPACTION_TRACE <sample> handler <begin|end>",
      limitations: [
        "in-memory branch fixture; callback timing is absolute",
        "no real serialized request, cache, or inference timing",
      ],
    })}`,
  );
  const captureState = getOmpRuntimeCaptureState();
  expect(captureState.pendingTaskResultConsumptions.has(sessionId)).toBe(false);
  expect(captureState.pendingTaskBindingRetirements.has(sessionId)).toBe(false);
  expect(captureState.earlyTaskBindings.size).toBe(0);
}

export async function benchmarkOmpProvider({ root, branches, first, second, invoke, state, writeSession }: any) {
  const quantile = (values: number[], fraction: number) => {
    const sorted = [...values].sort((left, right) => left - right);
    return Number(sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)]!.toFixed(3));
  };
  const measurements = {
    baseline: {
      lifecycle: [] as number[],
      streamDispatch: [] as number[],
      streamResult: [] as number[],
      commit: [] as number[],
    },
    foreground_enabled: {
      lifecycle: [] as number[],
      streamDispatch: [] as number[],
      streamResult: [] as number[],
      commit: [] as number[],
    },
  };
  const fingerprints = new Map<string, string>();
  const sampleCount = 32;
  const warmupCount = 8;
  const traceMark = (sample: number, arm: string, phase: string) => {
    process.stderr.write(`OMP_BENCHMARK_TRACE ${sample} ${arm} ${phase}\n`);
  };
  for (let sample = 0; sample < warmupCount + sampleCount; sample += 1) {
    for (const arm of ["baseline", "foreground_enabled"] as const) {
      const suffix = `sample-${sample}`;
      const sampleParentId = `01a1967e-${suffix}-b925-73fd-b992-d0d11d570d4c`;
      const sampleChildId = `01a1967e-${suffix}-bc0a-71a2-aea3-8a7689baae80`;
      const sampleAllocatedId = `Registry-${suffix}`;
      const sampleToolCallId = `call-${suffix}`;
      const sampleParentFile = join(root, `${arm}-${suffix}-parent.jsonl`);
      const sampleChildFile = join(root, `${arm}-${suffix}-child.jsonl`);
      const sampleParentHeader = { type: "session", id: sampleParentId };
      const sampleChildHeader = { type: "session", id: sampleChildId, parentSession: sampleParentFile };
      const sampleOutput = "child answer";
      const sampleTaskSummary = `<task-result id="${sampleAllocatedId}" agent="worker" status="completed" duration="1s">\n\n<output>\n${sampleOutput}\n</output>\n</task-result>`;
      const sampleTaskCall = {
        type: "message",
        id: `task-call-${suffix}`,
        message: {
          role: "assistant",
          responseId: `task-parent-response-${suffix}`,
          stopReason: "toolUse",
          content: [{ type: "toolCall", id: sampleToolCallId, name: "task", arguments: { agent: "worker" } }],
        },
      };
      const sampleTaskResult = {
        type: "message",
        id: `task-result-${suffix}`,
        message: {
          role: "toolResult",
          toolName: "task",
          toolCallId: sampleToolCallId,
          isError: false,
          content: [{ type: "text", text: sampleTaskSummary }],
          details: {
            results: [{ id: sampleAllocatedId, exitCode: 0, output: sampleOutput, truncated: false, aborted: false }],
          },
        },
      };
      const sampleChildTerminal = {
        type: "message",
        id: `child-terminal-${suffix}`,
        message: {
          role: "assistant",
          responseId: `child-terminal-response-${suffix}`,
          stopReason: "stop",
          content: [{ type: "text", text: sampleOutput }],
        },
      };
      writeSession(sampleParentFile, sampleParentHeader, [sampleTaskCall, sampleTaskResult]);
      writeSession(sampleChildFile, sampleChildHeader, [
        sampleChildTerminal,
        {
          type: "custom",
          customType: OMP_RUN_COMPLETION_ENTRY,
          data: {
            version: 1,
            runId: `omp:task:${sampleAllocatedId}`,
            parentSessionId: sampleParentId,
            sessionId: sampleChildId,
            responseId: `child-terminal-response-${suffix}`,
          },
        },
      ]);
      const sampleBranch: any[] = [sampleTaskCall, sampleTaskResult];
      branches.set(sampleParentId, sampleBranch);
      const sampleContext = {
        agent: { kind: "main" },
        sessionManager: {
          getSessionId: () => sampleParentId,
          getSessionFile: () => sampleParentFile,
          getHeader: () => sampleParentHeader,
          getBranch: () => sampleBranch,
        },
      };
      invoke(first, "session_start", { reason: "startup" }, sampleContext);
      invoke(second, "session_start", { reason: "startup" }, sampleContext);

      traceMark(sample, arm, "lifecycle-start");
      const lifecycleStart = performance.now();
      if (arm === "foreground_enabled") {
        for (const listener of first.lifecycleListeners)
          listener({
            status: "started",
            id: sampleAllocatedId,
            agent: "worker",
            parentToolCallId: sampleToolCallId,
            detached: false,
            sessionFile: sampleChildFile,
          });
      }
      const lifecycleEnd = performance.now();
      traceMark(sample, arm, "lifecycle-end");
      if (arm === "foreground_enabled") {
        expect(readOmpEarlyTaskDelegation(state, sampleAllocatedId, sampleChildFile)).toEqual({
          runId: `omp:task:${sampleAllocatedId}`,
          toolCallId: sampleToolCallId,
          detached: false,
        });
      }

      const headers = new Headers();
      const model = { id: "synthetic", api: "openai-completions", headers };
      const context = { messages: [sampleTaskCall.message, sampleTaskResult.message] };
      const responseId = `carrier-${arm}-${sample}`;
      (globalThis as any).__OMP_PROVIDER_RESULT__ = { responseId, stopReason: "stop", content: [] };
      (globalThis as any).__OMP_PROVIDER_REQUESTS__ = [];
      traceMark(sample, arm, "stream-start");
      const dispatchStart = performance.now();
      const stream = second.providerConfig.streamSimple(model, context, { sessionId: sampleParentId, headers });
      const dispatchEnd = performance.now();
      await stream.result();
      const streamEnd = performance.now();
      traceMark(sample, arm, "stream-end");

      const requests = (globalThis as any).__OMP_PROVIDER_REQUESTS__ as Array<{
        model: any;
        context: any;
        options: any;
      }>;
      expect(requests).toHaveLength(1);
      const request = requests[0]!;
      const requestModel = { ...request.model };
      delete requestModel.headers;
      const fingerprint = createHash("sha256")
        .update(JSON.stringify({ model: requestModel, context: request.context }))
        .digest("hex");
      fingerprints.set(`${sample}.${arm}`, fingerprint);
      const metadata = parseAgentMetadata(new Headers(request.options?.headers ?? {}).get("x-tokenvisor-agent"));
      expect(metadata?.session.id).toBe(`omp:session:${sampleParentId}`);
      expect(metadata?.depends_on).toEqual(
        arm === "baseline"
          ? null
          : [
              {
                session_id: `omp:session:${sampleChildId}`,
                response_id: `child-terminal-response-${suffix}`,
                run_id: `omp:task:${sampleAllocatedId}`,
              },
            ],
      );

      sampleBranch.push({
        type: "message",
        id: `carrier-entry-${suffix}`,
        message: {
          role: "assistant",
          responseId,
          stopReason: "stop",
          content: [],
        },
      });
      second.setAppendSession(sampleParentId);
      traceMark(sample, arm, "commit-start");
      const commitStart = performance.now();
      invoke(first, "agent_end", {}, sampleContext);
      const commitEnd = performance.now();
      traceMark(sample, arm, "commit-end");
      expect(state.pendingTaskBindingRetirements.has(sampleParentId)).toBe(false);
      expect(state.pendingTaskResultConsumptions.has(sampleParentId)).toBe(false);
      expect(readOmpEarlyTaskDelegation(state, sampleAllocatedId, sampleChildFile)).toBeNull();
      const samples = sample >= warmupCount ? measurements[arm] : null;
      if (samples) {
        samples.lifecycle.push(lifecycleEnd - lifecycleStart);
        samples.streamDispatch.push(dispatchEnd - dispatchStart);
        samples.streamResult.push(streamEnd - dispatchEnd);
        samples.commit.push(commitEnd - commitStart);
      }
      invoke(first, "session_shutdown", {}, sampleContext);
      invoke(second, "session_shutdown", {}, sampleContext);
      branches.delete(sampleParentId);
    }
    expect(fingerprints.get(`${sample}.baseline`)).toBe(fingerprints.get(`${sample}.foreground_enabled`));
  }

  const report: Record<string, Record<string, [number, number]>> = {};
  for (const [arm, samples] of Object.entries(measurements)) {
    report[arm] = Object.fromEntries(
      Object.entries(samples).map(([stage, values]) => [stage, [quantile(values, 0.5), quantile(values, 0.95)]]),
    );
  }
  console.log(
    "OMP_PROVIDER_CAPTURE_BENCHMARK",
    JSON.stringify({
      samplesPerArm: sampleCount,
      warmupPerArm: warmupCount,
      inputFingerprintsEqual: true,
      delegatesPerSampleAndArm: 1,
      timingsMsP50P95: report,
      note: "Registered provider hook and stream-result delta over existing identity capture; fake delegate does not measure serialized HTTP body, cache, network, or real delegate latency.",
    }),
  );
}

export async function benchmarkPiForeground({ createHarness, writeSession }: any) {
  const sampleCount = 24;
  const contract = JSON.parse(
    readFileSync(new URL("../../tests/fixtures/foreground-launch-contract-v1.json", import.meta.url), "utf8"),
  ) as {
    cases: Array<{ name: string; expected: { execution_groups: Record<string, string[]> } }>;
  };
  const shapes = [
    {
      name: "single",
      sourceCase: "normal-returned-output",
      inferenceCount: contract.cases.find((item) => item.name === "normal-returned-output")!.expected.execution_groups[
        "run-a"
      ]!.length,
    },
    {
      name: "multi",
      sourceCase: "multi-inference-child-span",
      inferenceCount: contract.cases.find((item) => item.name === "multi-inference-child-span")!.expected
        .execution_groups["run-c"]!.length,
    },
  ];
  const arms = {
    baseline: await createHarness(),
    foreground_enabled: await createHarness(),
  } as const;
  const timings = new Map<string, { dispatch: number[]; toolReturn: number[]; header: number[]; stream: number[] }>();
  const fingerprints: string[] = [];
  const counts: Record<string, { delegateCalls: number; launches: number; dependencies: number }> = {};
  const traceRoots = Object.fromEntries(Object.entries(arms).map(([name, harness]) => [name, harness.root]));
  const quantile = (values: number[], fraction: number) => {
    const sorted = [...values].sort((left, right) => left - right);
    return Number(sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)]!.toFixed(3));
  };

  for (const shape of shapes) {
    for (let sample = 0; sample < sampleCount; sample += 1) {
      for (const [armName, harness] of Object.entries(arms)) {
        const rootId = `bench-parent-${armName}-${shape.name}-${sample}`;
        const childId = `bench-child-${armName}-${shape.name}-${sample}`;
        const parentFile = join(harness.root, `${armName}-${shape.name}-${sample}-parent.jsonl`);
        const childFile = join(harness.root, `${armName}-${shape.name}-${sample}-child.jsonl`);
        const parentHeader = { type: "session", id: rootId };
        const childHeader = { type: "session", id: childId, parentSession: parentFile };
        const history = Array.from({ length: 12 }, (_, index) => ({
          type: "message",
          id: `history-${index}`,
          message:
            index % 2 === 0
              ? { role: "user", content: [{ type: "text", text: `synthetic history ${index}` }] }
              : {
                  role: "assistant",
                  responseId: `history-response-${index}`,
                  stopReason: "stop",
                  content: [{ type: "text", text: `synthetic answer ${index}` }],
                },
        }));
        const parentCall = {
          type: "message",
          id: "launch-entry",
          message: {
            role: "assistant",
            responseId: "bench-parent-response",
            stopReason: "toolUse",
            content: [{ type: "toolCall", id: "bench-tool-call", name: "subagent", arguments: { agent: "worker" } }],
          },
        };
        const output = "SYNTHETIC_CHILD_ACK";
        const missionId = "9aebeb6f-a41e-435d-b9eb-67097b0fd578";
        const toolResultText = `Run fan-out: 1/64 used, 63 remaining\n${output}\nMission: ${missionId} (completed)`;
        const details = {
          mode: "single",
          runId: "bench-run",
          runFanoutBudget: { used: 1, limit: 64, remaining: 63 },
          mission: { id: missionId, status: "completed" },
          results: [
            {
              context: "fresh",
              sessionFile: childFile,
              exitCode: 0,
              outputMode: "inline",
              outputState: "present",
              finalOutput: output,
            },
          ],
        };
        const toolResult = {
          type: "message",
          id: "result-entry",
          message: {
            role: "toolResult",
            toolName: "subagent",
            toolCallId: "bench-tool-call",
            isError: false,
            content: [{ type: "text", text: toolResultText }],
            details,
          },
        };
        const childMessages = Array.from({ length: shape.inferenceCount }, (_, index) => ({
          type: "message",
          id: `child-response-${index}`,
          message: {
            role: "assistant",
            responseId:
              index === shape.inferenceCount - 1 ? "bench-child-terminal" : `bench-child-intermediate-${index}`,
            stopReason: "stop",
            content: [
              { type: "text", text: index === shape.inferenceCount - 1 ? output : `synthetic intermediate ${index}` },
            ],
          },
        }));
        writeSession(parentFile, parentHeader, [...history, parentCall, toolResult]);
        writeSession(childFile, childHeader, childMessages);
        const branch = [...history, parentCall, toolResult];
        harness.files.set(rootId, parentFile);
        harness.headers.set(rootId, parentHeader);
        harness.branches.set(rootId, branch);
        harness.appendFor(rootId);
        const ctx = harness.sessionContext(rootId);
        harness.hooks.get("session_start")!({ reason: "startup" }, ctx);

        const dispatchStart = performance.now();
        const handler = harness.hooks.get("tool_result")!;
        const handlerStart = performance.now();
        handler(
          armName === "baseline"
            ? { toolName: "read" }
            : {
                toolName: "subagent",
                toolCallId: "bench-tool-call",
                isError: false,
                content: toolResult.message.content,
                details,
              },
          ctx,
        );
        const handlerEnd = performance.now();
        const dispatchEnd = performance.now();
        const headers = new Headers();
        const headerStart = performance.now();
        harness.hooks.get("before_provider_headers")!({ headers }, ctx);
        const headerEnd = performance.now();

        const messages = [
          ...history.map((entry) => entry.message),
          parentCall.message,
          {
            role: "toolResult",
            toolName: "subagent",
            toolCallId: "bench-tool-call",
            isError: false,
            content: toolResult.message.content,
          },
        ];
        const context = { messages };
        (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
          responseId: `bench-carrier-${sample}`,
          stopReason: "stop",
          content: [],
        };
        (globalThis as any).__PI_TEST_CAPTURED_CALLS__ = 0;
        const model = { id: "synthetic", api: "openai-completions", headers };
        const streamStart = performance.now();
        await harness.providerConfig.streamSimple(model, context, { headers }).result();
        const streamEnd = performance.now();
        const captured = (globalThis as any).__PI_TEST_CAPTURED_REQUEST__;
        const metadata = parseAgentMetadata(new Headers(captured?.options?.headers ?? {}).get("x-tokenvisor-agent"));
        const modelInput = { ...captured?.model };
        delete modelInput.headers;
        const inputFingerprint = createHash("sha256")
          .update(
            JSON.stringify({
              model: modelInput,
              context: captured?.context,
            }),
          )
          .digest("hex");
        fingerprints.push(`${shape.name}:${inputFingerprint}`);
        const key = `${armName}.${shape.name}`;
        const item = timings.get(key) ?? { dispatch: [], toolReturn: [], header: [], stream: [] };
        item.dispatch.push(dispatchEnd - dispatchStart);
        item.toolReturn.push(handlerEnd - handlerStart);
        item.header.push(headerEnd - headerStart);
        item.stream.push(streamEnd - streamStart);
        timings.set(key, item);
        const count = counts[key] ?? { delegateCalls: 0, launches: 0, dependencies: 0 };
        count.delegateCalls += (globalThis as any).__PI_TEST_CAPTURED_CALLS__;
        count.launches += metadata?.child_launches.length ?? 0;
        count.dependencies += metadata?.depends_on?.length ?? 0;
        counts[key] = count;

        harness.hooks.get("session_shutdown")!({}, ctx);
        harness.branches.delete(rootId);
        harness.files.delete(rootId);
        harness.headers.delete(rootId);
      }
    }
  }

  const summary = Object.fromEntries(
    [...timings].map(([key, values]) => [
      key,
      Object.fromEntries(
        Object.entries(values).map(([metric, samples]) => [
          metric,
          { p50_ms: quantile(samples, 0.5), p95_ms: quantile(samples, 0.95) },
        ]),
      ),
    ]),
  );
  expect(counts["baseline.single"]?.launches).toBe(0);
  expect(counts["baseline.single"]?.dependencies).toBe(0);
  expect(counts["foreground_enabled.single"]?.launches).toBe(sampleCount);
  expect(counts["foreground_enabled.single"]?.dependencies).toBe(sampleCount);
  expect(counts["foreground_enabled.multi"]?.launches).toBe(sampleCount);
  expect(counts["foreground_enabled.multi"]?.dependencies).toBe(sampleCount);
  expect(new Set(fingerprints.map((item) => item.split(":")[1])).size).toBe(1);
  console.log(
    `PI_FOREGROUND_BENCHMARK ${JSON.stringify({
      classification: "foreground-handling delta over existing identity capture; not total capture overhead",
      samples_per_arm_shape: sampleCount,
      child_inference_shapes: shapes.map(({ name, sourceCase, inferenceCount }) => ({
        name,
        source_case: sourceCase,
        inference_count: inferenceCount,
      })),
      timings: summary,
      counts,
      delegate_body_input_fingerprint_equal: new Set(fingerprints.map((item) => item.split(":")[1])).size === 1,
      prior_state_release_regression: {
        test: "releases native child session contexts after repeated Pi startup and shutdown",
        sessions: 48,
        gc_cycles: 3,
      },
      trace_roots: traceRoots,
      limitations: ["fake Pi AI delegate observes model/context inputs, not serialized HTTP body or cache hits"],
    })}`,
  );
}

export async function benchmarkOmpHelpers({ createFixture, appendChildResult, blockingBranch, register }: any) {
  const fixture = createFixture(`benchmark-${process.pid}`);
  appendChildResult(fixture);
  const built = blockingBranch(fixture);
  const inputFingerprint = createHash("sha256")
    .update(
      JSON.stringify({
        parentId: fixture.parentSessionId,
        branch: built.branch,
        effectiveMessages: built.effectiveMessages,
      }),
    )
    .digest("hex");
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
  console.log(
    "OMP_CAPTURE_BENCHMARK",
    JSON.stringify({
      samples: baselineSamples.length,
      warmup: 8,
      inputFingerprint,
      baselineIdentityAndExistingDependencyP50P95Ms: [
        percentile(baselineSamples, 0.5),
        percentile(baselineSamples, 0.95),
      ],
      lifecycleRegistrationP50P95Ms: [percentile(dispatchSamples, 0.5), percentile(dispatchSamples, 0.95)],
      blockingExtractionP50P95Ms: [percentile(extractionSamples, 0.5), percentile(extractionSamples, 0.95)],
      enabledCombinedP50P95Ms: [percentile(lifecycleSamples, 0.5), percentile(lifecycleSamples, 0.95)],
      delegateCalls: 0,
      note: "Direct capture-helper measurement; provider dispatch, request serialization, and delegate/cache behavior are outside this benchmark.",
    }),
  );
  clearOmpEarlyTaskBindingsForSession(fixture.state, fixture.parentSessionId);
  clearOmpTaskBindingRetirementsForSession(fixture.state, fixture.parentSessionId);
  expect(
    [...fixture.state.earlyTaskBindings.values()].filter(
      (binding: any) => binding.parentSessionId === fixture.parentSessionId,
    ),
  ).toHaveLength(0);
  expect(fixture.state.pendingTaskBindingRetirements.has(fixture.parentSessionId)).toBe(false);
}
