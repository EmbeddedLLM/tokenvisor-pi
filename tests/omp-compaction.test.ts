import { describe, expect, test } from "bun:test";
import {
  clearOmpEarlyTaskBindingsForSession,
  OMP_TASK_RESULT_CONSUMPTION_ENTRY,
  assessOmpCompactionFrontier,
  type OmpRuntimeCaptureState,
} from "../src/capture/omp.ts";
import { resolveCompactionFrontier } from "../src/capture/pi.ts";

const sessionId = "omp-human-root";
const compactionEntry = {
  type: "compaction",
  id: "compact-human-1",
  firstKeptEntryId: "response-before-compact",
  method: "snapcompact",
  fromExtension: false,
};

function stateFor(branch: Array<Record<string, any>>): OmpRuntimeCaptureState {
  return {
    lineageBySession: new Map(),
    compactionStreams: new Map(),
    compactionCaptureActive: new Set(),
    compactionStreamFailures: new Set(),
    compactionPriorStreams: new Map(),
    compactionDependencies: new Map(),
    branchReaders: new Map([[sessionId, () => branch]]),
    sessionFiles: new Map(),
    appendEntryBySession: new Map(),
    sequenceBySession: new Map(),
    pendingTaskResultConsumptions: new Map(),
    pendingTaskBindingRetirements: new Map(),
    compactionTaskResultReferences: new Map(),
    taskResultReferencesByStream: new Map(),
    earlyTaskBindings: new Map(),
  };
}

function snapcompactBranch(extra: Array<Record<string, any>> = []): Array<Record<string, any>> {
  return [
    {
      type: "message",
      id: "response-before-compact",
      message: { role: "assistant", responseId: "response-before-compact", stopReason: "stop" },
    },
    ...extra,
    { ...compactionEntry },
  ];
}

function snapcompactEvent(entry: Record<string, any> = compactionEntry): Record<string, any> {
  return { fromExtension: false, compactionEntry: { ...entry } };
}

function frontierFor(
  assessment: ReturnType<typeof assessOmpCompactionFrontier>,
  previous: string[] | null = [],
  failed = false,
) {
  return resolveCompactionFrontier(
    previous,
    [],
    failed,
    assessment.sourceProvenMechanicalCompaction && !assessment.hasUnresolvedInputs,
  );
}

describe("OMP native compaction frontier", () => {
  test("preserves a known-empty frontier for the committed native snapcompact event", () => {
    const branch = snapcompactBranch();
    const assessment = assessOmpCompactionFrontier(stateFor(branch), sessionId, snapcompactEvent());

    expect(assessment).toEqual({
      sourceProvenMechanicalCompaction: true,
      hasUnresolvedInputs: false,
    });
    expect(frontierFor(assessment)).toEqual({
      available: true,
      streamIds: [],
      provenNone: true,
    });
    expect(frontierFor(assessment, ["earlier-known-stream"])).toEqual({
      available: true,
      streamIds: ["earlier-known-stream"],
      provenNone: false,
    });
    expect(frontierFor(assessment, null).available).toBe(false);
    expect(frontierFor(assessment, [], true).available).toBe(false);
  });

  test("keeps stale, mismatched, or non-snapcompact commits unavailable", () => {
    const branch = snapcompactBranch();
    const state = stateFor(branch);

    const staleEvent = snapcompactEvent({ ...compactionEntry, id: "old-compact" });
    expect(assessOmpCompactionFrontier(state, sessionId, staleEvent)).toEqual({
      sourceProvenMechanicalCompaction: false,
      hasUnresolvedInputs: true,
    });

    const missingCommit = assessOmpCompactionFrontier(stateFor(snapcompactBranch().slice(0, -1)), sessionId, snapcompactEvent());
    expect(missingCommit.hasUnresolvedInputs).toBe(true);
    expect(frontierFor(missingCommit).available).toBe(false);

    for (const method of [undefined, "llm", "remote"]) {
      const entry = { ...compactionEntry, ...(method === undefined ? { method: undefined } : { method }) };
      const candidate = snapcompactBranch().map((row) => row.type === "compaction" ? entry : row);
      const assessment = assessOmpCompactionFrontier(stateFor(candidate), sessionId, snapcompactEvent(entry));
      expect(assessment.sourceProvenMechanicalCompaction).toBe(false);
      expect(assessment.hasUnresolvedInputs).toBe(false);
      expect(frontierFor(assessment).available).toBe(false);
    }

    const extensionEvent = { fromExtension: true, compactionEntry: { ...compactionEntry, fromExtension: true } };
    const extensionBranch = snapcompactBranch().map((row) => row.type === "compaction"
      ? { ...compactionEntry, fromExtension: true }
      : row);
    expect(assessOmpCompactionFrontier(stateFor(extensionBranch), sessionId, extensionEvent).sourceProvenMechanicalCompaction).toBe(false);
  });

  test("does not prove an empty frontier while attached child output can still be pending", () => {
    const cases: Array<(state: OmpRuntimeCaptureState) => void> = [
      (state) => state.pendingTaskResultConsumptions.set(sessionId, new Map([["response", []]])),
      (state) => state.pendingTaskBindingRetirements.set(sessionId, new Map([["response", { streamId: "stream", bindingIds: ["child"] }]])),
      (state) => state.earlyTaskBindings.set(JSON.stringify([sessionId, "child"]), {
        parentSessionId: sessionId,
        parentSessionFile: "/sessions/root.jsonl",
        rootSessionId: sessionId,
        agentId: "child",
        toolCallId: "task-call",
        childSessionFile: "/sessions/root/child.jsonl",
        childSessionId: "child-session",
        detached: false,
      }),
    ];

    for (const addPendingState of cases) {
      const branch = snapcompactBranch();
      const state = stateFor(branch);
      addPendingState(state);
      const assessment = assessOmpCompactionFrontier(state, sessionId, snapcompactEvent());
      expect(assessment.sourceProvenMechanicalCompaction).toBe(true);
      expect(assessment.hasUnresolvedInputs).toBe(true);
      expect(frontierFor(assessment).available).toBe(false);
    }

    expect(resolveCompactionFrontier([], ["captured-summary-stream"], false, false)).toEqual({
      available: true,
      streamIds: ["captured-summary-stream"],
      provenNone: false,
    });
  });

  test("child shutdown alone leaves the parent launch binding unresolved at compaction", () => {
    const branch = snapcompactBranch();
    const state = stateFor(branch);
    const childSessionId = "child-session";
    state.earlyTaskBindings.set(JSON.stringify([sessionId, "child"]), {
      parentSessionId: sessionId,
      parentSessionFile: "/sessions/root.jsonl",
      rootSessionId: sessionId,
      agentId: "child",
      toolCallId: "task-call",
      childSessionFile: "/sessions/root/child.jsonl",
      childSessionId,
      detached: false,
    });

    // The lifecycle shutdown handler clears bindings owned by the exiting
    // session. A child's exit must not clear its binding in the parent.
    clearOmpEarlyTaskBindingsForSession(state, childSessionId);
    const assessment = assessOmpCompactionFrontier(state, sessionId, snapcompactEvent());
    expect(assessment.hasUnresolvedInputs).toBe(true);
    expect(frontierFor(assessment).available).toBe(false);
  });

  test("does not treat a completed detached wait result as consumed without its exact marker", () => {
    const waitResult = {
      type: "message",
      id: "wait-delivery",
      message: {
        role: "toolResult",
        toolName: "wait",
        toolCallId: "wait-call",
        details: {
          op: "wait",
          jobs: [{ id: "job-1", agentUrlId: "child-1", type: "task", status: "completed", resultText: "child output" }],
        },
      },
    };
    const branch = snapcompactBranch([waitResult]);
    const state = stateFor(branch);

    const unconsumed = assessOmpCompactionFrontier(state, sessionId, snapcompactEvent());
    expect(unconsumed.hasUnresolvedInputs).toBe(true);
    expect(frontierFor(unconsumed).available).toBe(false);

    const consumer = {
      type: "message",
      id: "consumer-entry",
      message: { role: "assistant", responseId: "consumer-response", stopReason: "stop" },
    };
    const reference = {
      sessionId: "child-session",
      responseId: "child-response",
      runId: "omp:task:child-1",
      deliveryId: "wait-delivery",
      jobId: "child-1",
    };
    const consumedBranch = [
      ...branch.slice(0, -1),
      consumer,
      {
        type: "custom",
        customType: OMP_TASK_RESULT_CONSUMPTION_ENTRY,
        data: {
          version: 1,
          parentSessionId: sessionId,
          consumer: { type: "assistant", id: "consumer-response" },
          references: [reference],
        },
      },
      branch.at(-1)!,
    ];
    const consumed = assessOmpCompactionFrontier(stateFor(consumedBranch), sessionId, snapcompactEvent());
    expect(consumed.hasUnresolvedInputs).toBe(false);
    expect(frontierFor(consumed).provenNone).toBe(true);
  });

  test("keeps historical async and wait deliveries outside the pending frontier", () => {
    const asyncResult = {
      type: "custom",
      customType: "async-result",
      id: "historical-async-result",
      data: { jobs: [{ type: "task", status: "completed" }] },
    };
    const waitResult = {
      type: "message",
      id: "historical-wait-delivery",
      message: {
        role: "toolResult",
        toolName: "wait",
        toolCallId: "wait-call",
        details: {
          op: "wait",
          jobs: [{ id: "job-1", agentUrlId: "child-1", type: "task", status: "completed", resultText: "child output" }],
        },
      },
    };
    const consumer = {
      type: "message",
      id: "historical-consumer-entry",
      message: { role: "assistant", responseId: "historical-consumer", stopReason: "stop" },
    };

    for (const delivery of [asyncResult, waitResult]) {
      const branch = snapcompactBranch([delivery, consumer]);
      const assessment = assessOmpCompactionFrontier(stateFor(branch), sessionId, snapcompactEvent());
      expect(assessment.hasUnresolvedInputs).toBe(false);
      expect(frontierFor(assessment).provenNone).toBe(true);
    }
  });

  test("does not let IDless responses or a zero-call compaction clear pending async evidence", () => {
    const asyncResult = {
      type: "custom",
      customType: "async-result",
      id: "pending-async-result",
      data: { jobs: [{ type: "task", status: "completed" }] },
    };
    const idlessAssistant = {
      type: "message",
      id: "idless-assistant-entry",
      message: { role: "assistant", stopReason: "stop" },
    };
    const idlessBranch = snapcompactBranch([asyncResult, idlessAssistant]);
    const idless = assessOmpCompactionFrontier(stateFor(idlessBranch), sessionId, snapcompactEvent());
    expect(idless.hasUnresolvedInputs).toBe(true);
    expect(frontierFor(idless).available).toBe(false);

    const zeroCallBranch = [asyncResult, { ...compactionEntry }];
    const zeroCall = assessOmpCompactionFrontier(stateFor(zeroCallBranch), sessionId, snapcompactEvent());
    expect(zeroCall.hasUnresolvedInputs).toBe(true);
    expect(frontierFor(zeroCall).available).toBe(false);
  });

  test("keeps automatic async results and unresolved peer wakes unknown", () => {
    const asyncResultBranch = snapcompactBranch([{
      type: "custom",
      customType: "async-result",
      id: "async-result-delivery",
      data: { jobs: [{ type: "task", status: "completed" }] },
    }]);
    expect(assessOmpCompactionFrontier(stateFor(asyncResultBranch), sessionId, snapcompactEvent()).hasUnresolvedInputs).toBe(true);

    const peerWakeBranch = snapcompactBranch([{
      type: "custom",
      customType: "irc:incoming",
      id: "peer-wake",
      data: { source: { type: "peer" } },
    }]);
    expect(assessOmpCompactionFrontier(stateFor(peerWakeBranch), sessionId, snapcompactEvent()).hasUnresolvedInputs).toBe(true);
  });
});
