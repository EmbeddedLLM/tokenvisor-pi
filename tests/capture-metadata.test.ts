import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import { findPiAiPackageRoot } from "../src/capture/pi-runtime.ts";
import {
  AGENT_HEADER_BUDGET,
  encodeAgentMetadata,
  parseAgentMetadata,
  claimFreshRequestMetadata,
  clearAgentMetadataHeader,
  recordFreshRequestMetadata,
  stripAgentMetadataHeader,
  type AgentMetadata,
  type MetadataIdentity,
} from "../src/capture/shared.ts";
import {
  createPiEnvelopeForStream,
  isPiForkRoot,
  makePiCompactionRecord,
  PI_COMPACTION_ENTRY,
  PI_FORK_ROOT_ENTRY,
  readPiSubagentDelegation,
  readPiParentToolCallId,
  readPiUnconsumedRunDependencies,
  readPiForkBoundaryIndex,
  readLatestPiCompactionContributors,
  latestNativeCompactionId,
  resolvePiSubagentRunnerRunId,
  resolveCompactionFrontier,
} from "../src/capture/pi.ts";
import {
  createOmpEnvelope,
  createOmpSequenceState,
  deriveOmpIdentity,
  deriveOmpSessionLineage,
  pendingOmpCompactionDependencies,
  readOmpForkBoundaryIndex,
  readOmpTaskDelegation,
  readOmpTaskResultDependencies,
  OMP_RUN_COMPLETION_ENTRY,
  recordOmpCompactionResponse,
  recordOmpSequenceResult,
  recordOmpSequenceStream,
  resolveOmpNativeSequence,
} from "../src/capture/omp.ts";

const ids = {
  root: "01a17d39-3861-7173-a625-742eb0b76cd2",
  child: "01a17d39-ab24-7173-a625-743063a7c084",
  stream: "d4fda0f7-2528-41f7-97e8-e49956c8c05a",
  previous: "chatcmpl-a1b7f6aa1ec4ae62",
};
const nativeFixtureRoot = mkdtempSync(join(tmpdir(), "tokenvisor-native-session-fixture-"));
afterAll(() => rmSync(nativeFixtureRoot, { recursive: true, force: true }));

function nativeSessionFile(id: string, parentSession?: string): string {
  const path = join(nativeFixtureRoot, `${id}.jsonl`);
  writeFileSync(path, `${JSON.stringify({ type: "session", id, ...(parentSession ? { parentSession } : {}) })}\n`);
  return path;
}

function piContext(overrides: Record<string, unknown> = {}) {
  const branch = (overrides.branch as Array<Record<string, unknown>> | undefined) ?? [];
  const sessionId = (overrides.sessionId as string | undefined) ?? ids.root;
  return {
    sessionManager: {
      getSessionId: () => sessionId,
      getSessionFile: () => (overrides.sessionFile as string | undefined) ?? nativeSessionFile(
        sessionId,
        overrides.parentSession ? nativeSessionFile(overrides.parentSession as string) : undefined,
      ),
      getHeader: () => ({ id: sessionId, parentSession: overrides.parentSession ? nativeSessionFile(overrides.parentSession as string) : undefined }),
      getBranch: () => branch,
    },
  } as any;
}

describe("shared agent metadata", () => {
  test("matches canonical v1 conformance fixtures through the producer encoder", () => {
    const document = JSON.parse(readFileSync(new URL("../conformance/agent-metadata-v1.json", import.meta.url), "utf8")) as {
      version: number;
      fixtures: Array<{ name: string; metadata: AgentMetadata }>;
    };
    expect(document.version).toBe(1);
    expect(document.fixtures.length).toBeGreaterThanOrEqual(4);
    for (const fixture of document.fixtures) {
      const metadata = fixture.metadata;
      const prefix = `${metadata.source.harness}:session:`;
      const unprefix = (value: string | null) => value?.startsWith(prefix) ? value.slice(prefix.length) : null;
      const identity: MetadataIdentity = {
        harness: metadata.source.harness,
        harnessVersion: metadata.source.harness_version,
        extensionVersion: metadata.source.extension_version,
        runnerVersion: metadata.source.runner_version,
        sessionId: unprefix(metadata.session.id),
        rootSessionId: unprefix(metadata.session.root_id),
        parentSessionId: unprefix(metadata.session.parent_id),
        previousResponseId: metadata.sequence.previous_response_id,
        hasPriorHistory: metadata.sequence.status !== "start",
        delegation: metadata.delegation ? {
          runId: metadata.delegation.run_id,
          toolCallId: metadata.delegation.tool_call_id,
        } : null,
        dependsOn: metadata.depends_on,
      };
      const encoded = encodeAgentMetadata(identity, metadata.stream_id);
      expect(encoded).not.toBeNull();
      expect(Buffer.byteLength(encoded!, "ascii")).toBeLessThanOrEqual(AGENT_HEADER_BUDGET);
      expect(encoded).toMatch(/^[\x20-\x7e]+$/);
      expect(JSON.parse(encoded!)).toEqual(metadata);
      expect(parseAgentMetadata(encoded)).toEqual(metadata);
    }
  });

  test("uses only fresh native request metadata and clears every reserved header spelling", () => {
    const cached = new Map<string, string>();
    const encoded = createPiEnvelopeForStream(piContext(), "fresh-request-stream");
    expect(encoded).not.toBeNull();
    expect(recordFreshRequestMetadata(cached, encoded!)).toBe(true);
    const candidate = parseAgentMetadata(encoded)!;
    const old = JSON.stringify({ ...candidate, stream_id: "old-request-stream" });
    expect(claimFreshRequestMetadata(cached, old)).toBeNull();
    expect(claimFreshRequestMetadata(cached, encoded)).toEqual({ encoded, metadata: candidate });
    expect(claimFreshRequestMetadata(cached, encoded)).toBeNull();
    expect(stripAgentMetadataHeader({
      authorization: "Bearer local",
      ["x-tokenvisor-agent"]: "stale-lowercase",
      ["X-TOKENVISOR-AGENT"]: "stale-uppercase",
      "x-session-id": "native-affinity",
    })).toEqual({ authorization: "Bearer local", "x-session-id": "native-affinity" });
    const mutable = { ["x-tokenvisor-agent"]: "stale-lowercase", ["X-TOKENVISOR-AGENT"]: "stale-uppercase", "x-session-id": "native-affinity" };
    clearAgentMetadataHeader(mutable);
    expect(mutable).toEqual({ "x-session-id": "native-affinity" });
    const headers = new Headers({ "X-tokenvisor-agent": "stale", "x-session-id": "native-affinity" });
    const stripped = stripAgentMetadataHeader(headers);
    expect(stripped?.["x-tokenvisor-agent"]).toBeUndefined();
    expect(stripped?.["x-session-id"]).toBe("native-affinity");
  });

  test("encodes compact ASCII metadata within the fixed 4096-byte limit", () => {
    const encoded = encodeAgentMetadata({
      harness: "pi",
      harnessVersion: "1.4.0",
      extensionVersion: "1.1.0-dev",
      runnerVersion: "0.83.0",
      sessionId: ids.root,
      rootSessionId: ids.root,
      parentSessionId: null,
      previousResponseId: null,
      hasPriorHistory: false,
      delegation: null,
      dependsOn: [],
    });

    expect(encoded).not.toBeNull();
    expect(Buffer.byteLength(encoded!, "ascii")).toBeLessThanOrEqual(AGENT_HEADER_BUDGET);
    expect(encoded).toMatch(/^[\x20-\x7e]+$/);
    expect(parseAgentMetadata(encoded)?.session.id).toBe(`pi:session:${ids.root}`);
  });

  test("preserves valid identity when dependencies exceed the header budget", () => {
    const dependencies = Array.from({ length: 8 }, (_, index) => ({
      session_id: `session-${String(index).padStart(248, "x")}`,
      response_id: `response-${String(index).padStart(247, "x")}`,
      stream_id: `stream-${String(index).padStart(249, "x")}`,
      run_id: `run-${String(index).padStart(252, "x")}`,
    }));
    const encoded = encodeAgentMetadata({
      harness: "pi",
      harnessVersion: "1.4.0",
      extensionVersion: "1.1.0-dev",
      runnerVersion: "0.83.0",
      sessionId: ids.child,
      rootSessionId: ids.root,
      parentSessionId: ids.root,
      previousResponseId: ids.previous,
      hasPriorHistory: true,
      delegation: { runId: "delegated-run", toolCallId: "tool-call" },
      dependsOn: dependencies,
    }, ids.stream);

    expect(encoded).not.toBeNull();
    expect(Buffer.byteLength(encoded!, "ascii")).toBeLessThanOrEqual(AGENT_HEADER_BUDGET);
    expect(encoded).toMatch(/^[\x20-\x7e]+$/);
    expect(JSON.parse(encoded!)).toEqual({
      version: 1,
      source: {
        harness: "pi",
        harness_version: "1.4.0",
        extension_version: "1.1.0-dev",
        adapter_version: 1,
        runner_version: "0.83.0",
      },
      session: {
        id: `pi:session:${ids.child}`,
        root_id: `pi:session:${ids.root}`,
        parent_id: `pi:session:${ids.root}`,
      },
      stream_id: ids.stream,
      sequence: { status: "linked", previous_response_id: ids.previous },
      delegation: { run_id: "delegated-run", tool_call_id: "tool-call" },
      depends_on: null,
    } satisfies AgentMetadata);
    expect(parseAgentMetadata(encoded)?.depends_on).toBeNull();

    const withinBudget = encodeAgentMetadata({
      harness: "pi",
      harnessVersion: "1.4.0",
      extensionVersion: "1.1.0-dev",
      sessionId: ids.child,
      rootSessionId: ids.root,
      parentSessionId: ids.root,
      previousResponseId: ids.previous,
      hasPriorHistory: true,
      delegation: { runId: "delegated-run", toolCallId: "tool-call" },
      dependsOn: [{ session_id: `pi:session:${ids.root}`, stream_id: ids.stream }],
    }, ids.stream);
    expect(parseAgentMetadata(withinBudget)?.depends_on).toEqual([
      { session_id: `pi:session:${ids.root}`, stream_id: ids.stream },
    ]);
  });

  test("does not replace a missing predecessor with a later visible assistant", () => {
    const encoded = encodeAgentMetadata({
      harness: "pi",
      harnessVersion: "1.4.0",
      extensionVersion: "1.1.0-dev",
      sessionId: ids.root,
      rootSessionId: ids.root,
      parentSessionId: null,
      previousResponseId: null,
      hasPriorHistory: true,
      delegation: null,
      dependsOn: [],
    });
    expect(parseAgentMetadata(encoded)?.sequence).toEqual({ status: "unavailable", previous_response_id: null });
    const idlessBarrier = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: [
      { type: "message", message: { role: "assistant", responseId: ids.previous } },
      { type: "message", message: { role: "assistant" } },
    ] }), ids.stream));
    expect(idlessBarrier?.sequence).toEqual({ status: "unavailable", previous_response_id: null });
    const duplicate = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: [
      { type: "message", message: { role: "assistant", responseId: ids.previous } },
      { type: "message", message: { role: "assistant", responseId: ids.previous } },
    ] }), ids.stream));
    expect(duplicate?.sequence).toEqual({ status: "unavailable", previous_response_id: null });
  });

  test("marks malformed or over-budget dependency extraction unavailable instead of known empty", () => {
    const base = {
      harness: "pi" as const,
      harnessVersion: "1.4.0",
      extensionVersion: "1.1.0-dev",
      sessionId: ids.root,
      rootSessionId: ids.root,
      parentSessionId: null,
      previousResponseId: null,
      hasPriorHistory: false,
      delegation: null,
    };
    const malformed = encodeAgentMetadata({
      ...base,
      dependsOn: [{ session_id: ids.child, stream_id: "bad id" }],
    });
    const tooMany = encodeAgentMetadata({
      ...base,
      dependsOn: Array.from({ length: 9 }, (_, index) => ({ session_id: ids.child, stream_id: `stream-${index}` })),
    });
    const nullReference = encodeAgentMetadata({ ...base, dependsOn: [null] as any });
    const bothReferenceKinds = encodeAgentMetadata({
      ...base,
      dependsOn: [{ session_id: `pi:session:${ids.child}`, stream_id: ids.stream, response_id: ids.previous }],
    });
    const bothKindsAndRun = encodeAgentMetadata({
      ...base,
      dependsOn: [{ session_id: `pi:session:${ids.child}`, stream_id: ids.stream, response_id: ids.previous, run_id: "run-7" }],
    });
    const unknownFields = encodeAgentMetadata({
      ...base,
      dependsOn: [{ session_id: `pi:session:${ids.child}`, stream_id: ids.stream, observed_native_file: "/private/path" } as any],
    });
    const futureHarness = encodeAgentMetadata({ ...base, harness: "future-gateway", dependsOn: [] });
    const invalidHarness = encodeAgentMetadata({ ...base, harness: "x".repeat(33) });
    const invalidVersion = encodeAgentMetadata({ ...base, harnessVersion: "v".repeat(65) });
    const duplicateReference = encodeAgentMetadata({
      ...base,
      dependsOn: [
        { session_id: ids.child, stream_id: ids.stream },
        { stream_id: ids.stream, session_id: ids.child },
      ],
    });
    expect(parseAgentMetadata(malformed)?.depends_on).toBeNull();
    expect(parseAgentMetadata(tooMany)?.depends_on).toBeNull();
    expect(parseAgentMetadata(nullReference)?.depends_on).toBeNull();
    expect(parseAgentMetadata(bothReferenceKinds)?.depends_on).toEqual([{
      session_id: `pi:session:${ids.child}`,
      response_id: ids.previous,
      stream_id: ids.stream,
    }]);
    expect(parseAgentMetadata(bothKindsAndRun)?.depends_on).toEqual([{
      session_id: `pi:session:${ids.child}`,
      response_id: ids.previous,
      stream_id: ids.stream,
      run_id: "run-7",
    }]);
    expect(JSON.parse(unknownFields!).depends_on).toEqual([{ session_id: `pi:session:${ids.child}`, stream_id: ids.stream }]);
    expect(JSON.parse(futureHarness!).source.harness).toBe("future-gateway");
    expect(invalidHarness).toBeNull();
    expect(invalidVersion).toBeNull();
    expect(parseAgentMetadata(duplicateReference)?.depends_on).toBeNull();
  });

  test("uses native Pi headers for the parent/root and keeps a fork root distinct", () => {
    const oldRootHint = process.env.PI_SUBAGENT_PARENT_SESSION;
    process.env.PI_SUBAGENT_PARENT_SESSION = ids.root;
    try {
    const ctx = piContext({
      sessionId: ids.child,
      parentSession: ids.root,
      branch: [{ type: "message", message: { role: "assistant", responseId: ids.previous } }],
    });
    const normal = parseAgentMetadata(createPiEnvelopeForStream(ctx, ids.stream));
    expect(normal?.session.parent_id).toBe(`pi:session:${ids.root}`);
    expect(normal?.session.root_id).toBe(`pi:session:${ids.root}`);
    expect(normal?.sequence).toEqual({ status: "linked", previous_response_id: ids.previous });
    expect(normal?.source.runner_version).toBeUndefined();
    const runnerIdentified = parseAgentMetadata(createPiEnvelopeForStream(ctx, ids.stream, { runnerVersion: "0.83.0" }));
    expect(runnerIdentified?.source.runner_version).toBe("0.83.0");

    const fork = parseAgentMetadata(createPiEnvelopeForStream(ctx, ids.stream, { forkedRoot: true }));
    expect(fork?.session.parent_id).toBeNull();
    expect(fork?.session.root_id).toBe(`pi:session:${ids.child}`);
    expect(fork?.sequence).toEqual({ status: "start", previous_response_id: null });
    expect(fork?.depends_on).toEqual([]);
    expect(isPiForkRoot(piContext({ branch: [{ customType: PI_FORK_ROOT_ENTRY, data: { version: 1, sessionId: ids.root } }] }))).toBe(true);
    expect(isPiForkRoot(piContext({ sessionId: ids.child, branch: [{ customType: PI_FORK_ROOT_ENTRY, data: { version: 1, sessionId: ids.root } }] }))).toBe(false);
    } finally {
      if (oldRootHint === undefined) delete process.env.PI_SUBAGENT_PARENT_SESSION;
      else process.env.PI_SUBAGENT_PARENT_SESSION = oldRootHint;
    }
  });

  test("scopes a resumed Pi fork to entries after its session-bound boundary", () => {
    const marker = { customType: PI_FORK_ROOT_ENTRY, data: { version: 1, sessionId: ids.child, parentBoundary: "parent-tail" } };
    const inherited = [
      { type: "message", message: { role: "assistant", responseId: ids.previous } },
      { type: "compaction", id: "inherited-compaction" },
      { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord("inherited-compaction", [ids.stream]) },
    ];
    const first = piContext({ sessionId: ids.child, parentSession: ids.root, branch: [...inherited, marker] });
    expect(readPiForkBoundaryIndex(first)).toBe(inherited.length);
    let metadata = parseAgentMetadata(createPiEnvelopeForStream(first, ids.stream));
    expect(metadata?.sequence).toEqual({ status: "start", previous_response_id: null });
    expect(metadata?.depends_on).toEqual([]);
    const afterResponse = piContext({ sessionId: ids.child, parentSession: ids.root, branch: [
      ...inherited,
      marker,
      { type: "message", message: { role: "assistant", responseId: "fork-response-1" } },
    ] });
    metadata = parseAgentMetadata(createPiEnvelopeForStream(afterResponse, ids.stream));
    expect(metadata?.sequence).toEqual({ status: "linked", previous_response_id: "fork-response-1" });
    expect(metadata?.depends_on).toEqual([]);
    const pendingNewSummary = piContext({ sessionId: ids.child, parentSession: ids.root, branch: [
      ...inherited,
      marker,
      { type: "compaction", id: "fork-compaction" },
      { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord("fork-compaction", ["fork-summary-stream"]) },
    ] });
    metadata = parseAgentMetadata(createPiEnvelopeForStream(pendingNewSummary, ids.stream));
    expect(metadata?.depends_on).toEqual([{ session_id: `pi:session:${ids.child}`, stream_id: "fork-summary-stream" }]);
  });

  test("keeps a committed compaction record resumable and rejects ambiguous provenance", () => {
    const good = piContext({
      branch: [{ type: "compaction", id: "9aabe536" }, {
        type: "custom",
        customType: PI_COMPACTION_ENTRY,
        data: makePiCompactionRecord("9aabe536", [ids.stream]),
      }],
    });
    const metadata = parseAgentMetadata(createPiEnvelopeForStream(good, "200de1b9-d205-4792-97e7-1896fd343573"));
    expect(metadata?.depends_on).toEqual([{ session_id: `pi:session:${ids.root}`, stream_id: ids.stream }]);
    const consumed = piContext({ branch: [
      { type: "compaction", id: "9aabe536" },
      { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord("9aabe536", [ids.stream]) },
      { type: "message", message: { role: "assistant", responseId: "summary-consumer" } },
    ] });
    expect(parseAgentMetadata(createPiEnvelopeForStream(consumed, ids.stream))?.depends_on).toEqual([]);
    expect(readLatestPiCompactionContributors(consumed)).toEqual([]);

    const ambiguous = piContext({
      branch: [{ type: "compaction", id: "9aabe536" }, {
        type: "custom",
        customType: PI_COMPACTION_ENTRY,
        data: makePiCompactionRecord("9aabe536", [], false),
      }],
    });
    expect(parseAgentMetadata(createPiEnvelopeForStream(ambiguous, ids.stream))?.depends_on).toBeNull();

    const staleRecord = piContext({ branch: [
      { type: "compaction", id: "first-native-compaction" },
      { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord("first-native-compaction", ["stale-stream"]) },
      { type: "compaction", id: "latest-native-compaction" },
    ] });
    expect(readLatestPiCompactionContributors(staleRecord)).toBeNull();

    const duplicatedSummaryBranch = [
      { type: "compaction", id: "first-compaction-id", summary: "same summary" },
      { type: "message", message: { role: "assistant", responseId: "after-first" } },
      { type: "compaction", id: "second-compaction-id", summary: "same summary" },
    ];
    expect(latestNativeCompactionId(duplicatedSummaryBranch)).toBe("second-compaction-id");
    expect(latestNativeCompactionId(duplicatedSummaryBranch.slice(0, 2))).toBe("first-compaction-id");
  });

  test("keeps a bounded causal frontier across three compactions", () => {
    const c1 = "compact-one";
    const c2 = "compact-two";
    const c3 = "compact-three";
    const historical = ["c1-summary-a", "c1-summary-b"];
    const branch1 = [
      { type: "compaction", id: c1 },
      { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord(c1, historical) },
    ];
    const firstConsumer = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: branch1 }), "consumer-one"));
    expect(firstConsumer?.depends_on).toEqual(historical.map((stream_id) => ({ session_id: `pi:session:${ids.root}`, stream_id })));

    const summary2First = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: branch1 }), "c2-summary-first"));
    const summary2Last = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: branch1 }), "c2-summary-last", {
      resultDependencies: [{ session_id: `pi:session:${ids.root}`, stream_id: "c2-summary-first" }],
    }));
    const expectedC2Inputs = [
      ...historical,
      "c2-summary-first",
    ].map((stream_id) => ({ session_id: `pi:session:${ids.root}`, stream_id }));
    expect(summary2First?.depends_on).toEqual(expectedC2Inputs.slice(0, 2));
    expect(summary2Last?.depends_on).toEqual(expectedC2Inputs);
    expect(Buffer.byteLength(JSON.stringify(summary2Last), "ascii")).toBeLessThanOrEqual(AGENT_HEADER_BUDGET);

    const branch2 = [
      ...branch1,
      { type: "message", message: { role: "assistant", responseId: "consumer-one-response" } },
      { type: "compaction", id: c2 },
      { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord(c2, ["c2-summary-last"]) },
    ];
    const firstAfterC2 = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: branch2 }), "consumer-two"));
    expect(firstAfterC2?.depends_on).toEqual([{ session_id: `pi:session:${ids.root}`, stream_id: "c2-summary-last" }]);
    const laterAfterC2 = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: [
      ...branch2,
      { type: "message", message: { role: "assistant", responseId: "consumer-two-response" } },
    ] }), "later-two"));
    expect(laterAfterC2?.depends_on).toEqual([]);

    const branch3Input = [
      ...branch2,
      { type: "message", message: { role: "assistant", responseId: "consumer-two-response" } },
    ];
    const summary3First = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: branch3Input }), "c3-summary-first"));
    const summary3Last = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: branch3Input }), "c3-summary-last", {
      resultDependencies: [{ session_id: `pi:session:${ids.root}`, stream_id: "c3-summary-first" }],
    }));
    expect(summary3First?.depends_on).toEqual([]);
    expect(summary3First?.sequence).toEqual({ status: "linked", previous_response_id: "consumer-two-response" });
    expect(summary3Last?.depends_on).toEqual([{ session_id: `pi:session:${ids.root}`, stream_id: "c3-summary-first" }]);
    const branch3 = [
      ...branch3Input,
      { type: "compaction", id: c3 },
      { type: "custom", customType: PI_COMPACTION_ENTRY, data: makePiCompactionRecord(c3, ["c3-summary-last"]) },
    ];
    const firstAfterC3 = parseAgentMetadata(createPiEnvelopeForStream(piContext({ branch: branch3 }), "consumer-three"));
    expect(firstAfterC3?.depends_on).toEqual([{ session_id: `pi:session:${ids.root}`, stream_id: "c3-summary-last" }]);
    expect(Buffer.byteLength(JSON.stringify(firstAfterC3), "ascii")).toBeLessThanOrEqual(AGENT_HEADER_BUDGET);

    const edges = new Map<string, string[]>([
      ["c2-summary-first", historical],
      ["c2-summary-last", [...historical, "c2-summary-first"]],
      ["consumer-two", ["c2-summary-last"]],
      ["c3-summary-first", ["consumer-two"]],
      ["c3-summary-last", ["consumer-two", "c3-summary-first"]],
      ["consumer-three", ["c3-summary-last"]],
    ]);
    const reachable = new Set<string>();
    const visit = (streamId: string) => {
      if (reachable.has(streamId)) return;
      reachable.add(streamId);
      for (const parent of edges.get(streamId) ?? []) visit(parent);
    };
    visit("consumer-three");
    for (const streamId of [...historical, "c2-summary-first", "c2-summary-last", "consumer-two", "c3-summary-first", "c3-summary-last"]) {
      expect(reachable.has(streamId)).toBe(true);
    }
  });

  test("replaces summary frontiers, carries proven mechanical compactions, and rejects ambiguity", () => {
    expect(resolveCompactionFrontier(["old-a", "old-b"], ["new-a", "new-b"], false, false)).toEqual({
      available: true,
      streamIds: ["new-a", "new-b"],
      provenNone: false,
    });
    expect(resolveCompactionFrontier(["pending"], [], false, true)).toEqual({
      available: true,
      streamIds: ["pending"],
      provenNone: false,
    });
    expect(resolveCompactionFrontier([], [], false, true)).toEqual({
      available: true,
      streamIds: [],
      provenNone: true,
    });
    expect(resolveCompactionFrontier([], [], false, false).available).toBe(false);
    expect(resolveCompactionFrontier(["pending"], [], true, true).available).toBe(false);
    expect(resolveCompactionFrontier(null, [], false, true).available).toBe(false);
    expect(resolveCompactionFrontier([], Array.from({ length: 9 }, (_, index) => `summary-${index}`), false, false).available).toBe(false);
  });

  test("keeps OMP compaction provenance when an empty length-limited auxiliary summary is committed", () => {
    const contributors: string[] = [];
    let failed = false;
    const responses = [
      { stream: "history-summary", response: "history-response", reason: "stop", text: "history" },
      { stream: "turn-prefix-summary", response: "prefix-response", reason: "stop", text: "turn prefix" },
      { stream: "short-summary", response: "short-response", reason: "length", text: "" },
    ];

    for (const response of responses) {
      const disposition = recordOmpCompactionResponse(
        contributors,
        response.stream,
        response.response,
        response.reason,
        response.text ? [{ type: "text", text: response.text }] : [],
      );
      if (disposition === "unusable") failed = true;
    }

    expect(contributors).toEqual(["history-summary", "turn-prefix-summary"]);
    expect(failed).toBe(false);
    expect(resolveCompactionFrontier([], contributors, failed, false)).toEqual({
      available: true,
      streamIds: ["history-summary", "turn-prefix-summary"],
      provenNone: false,
    });
  });

  test("includes nonempty OMP text returned with a native length stop", () => {
    const contributors: string[] = [];

    expect(recordOmpCompactionResponse(
      contributors,
      "truncated-summary",
      "truncated-response",
      "length",
      [{ type: "text", text: "partial summary text" }],
    )).toBe("contributor");
    expect(contributors).toEqual(["truncated-summary"]);
  });

  test("binds Pi child run metadata through exact native session file and runner PID", () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-capture-test-"));
    const oldRoot = process.env.PI_SUBAGENTS_TEMP_ROOT;
    const oldPid = process.env.PI_SUBAGENT_RUNNER_PID;
    const oldChild = process.env.PI_SUBAGENT_CHILD;
    const oldArgv = process.argv;
    const sessionFile = join(root, "child.jsonl");
    const parentFile = join(root, "parent.jsonl");
    const runId = "resumed-run-id";
    writeFileSync(parentFile, [
      JSON.stringify({ type: "session", id: ids.root }),
      JSON.stringify({ type: "message", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent", id: "parent-tool-call-id" }] } }),
      JSON.stringify({ type: "message", message: { role: "toolResult", toolCallId: "parent-tool-call-id", details: { runId } } }),
    ].join("\n"));
    writeFileSync(sessionFile, `${JSON.stringify({ type: "session", id: ids.child, parentSession: parentFile })}\n`);
    const runnerRoot = join(root, "node_modules", "pi-subagents");
    const runnerFile = join(runnerRoot, "src", "runs", "background", "subagent-runner-bootstrap.js");
    const statusPath = join(root, "async-subagent-runs", runId, "status.json");
    mkdirSync(dirname(runnerFile), { recursive: true });
    writeFileSync(join(runnerRoot, "package.json"), JSON.stringify({ name: "pi-subagents", version: "0.83.0" }));
    writeFileSync(runnerFile, "");
    process.env.PI_SUBAGENTS_TEMP_ROOT = root;
    process.env.PI_SUBAGENT_RUNNER_PID = String(process.pid);
    process.env.PI_SUBAGENT_CHILD = "1";
    process.argv = [process.execPath, runnerFile, join(root, `async-cfg-${runId}.json`)];
    try {
      expect(resolvePiSubagentRunnerRunId(process.argv, process.env, process.pid)).toBe(runId);
      expect(readPiParentToolCallId(parentFile, ids.root, runId)).toBe("parent-tool-call-id");
      expect(readPiParentToolCallId(parentFile, ids.root, "different-run-id")).toBeNull();
      const runPath = dirname(statusPath);
      mkdirSync(runPath, { recursive: true });
      writeFileSync(statusPath, JSON.stringify({
        runId,
        toolCallId: "parent-tool-call-id",
        lifecycleArtifactVersion: 3,
        pid: process.pid,
        sessionId: parentFile,
        mode: "single",
        state: "running",
        steps: [{ sessionFile }],
      }));
      expect(readPiSubagentDelegation(sessionFile, ids.root)).toEqual({
        runId,
        toolCallId: "parent-tool-call-id",
      });
      writeFileSync(statusPath, JSON.stringify({
        runId,
        lifecycleArtifactVersion: 3,
        pid: process.pid,
        sessionId: ids.root,
        mode: "single",
        state: "running",
        steps: [{ sessionFile }],
      }));
      expect(readPiSubagentDelegation(sessionFile, ids.root)).toEqual({ runId, toolCallId: "parent-tool-call-id" });
      writeFileSync(statusPath, JSON.stringify({
        runId,
        lifecycleArtifactVersion: 3,
        pid: process.pid,
        sessionId: ids.root,
        mode: "single",
        state: "complete",
        steps: [{ sessionFile }],
      }));
      expect(readPiSubagentDelegation(sessionFile, ids.root)).toBeNull();
      expect(readPiSubagentDelegation(sessionFile, ids.root, "terminal")).toEqual({ runId, toolCallId: "parent-tool-call-id" });
      writeFileSync(statusPath, JSON.stringify({
        runId,
        toolCallId: "parent-tool-call-id",
        lifecycleArtifactVersion: 3,
        pid: process.pid,
        sessionId: parentFile,
        mode: "single",
        state: "running",
        steps: [{ sessionFile }],
      }));
      for (let index = 0; index < 520; index += 1) {
        const unrelated = join(root, "async-subagent-runs", `unrelated-${index}`);
        mkdirSync(unrelated, { recursive: true });
        writeFileSync(join(unrelated, "status.json"), "{}");
      }
      expect(readPiSubagentDelegation(sessionFile, ids.root)?.runId).toBe(runId);
      writeFileSync(statusPath, JSON.stringify({
        runId,
        toolCallId: "other-tool-call",
        lifecycleArtifactVersion: 3,
        pid: process.pid,
        sessionId: ids.root,
        mode: "single",
        state: "running",
        steps: [{ sessionFile }],
      }));
      expect(readPiSubagentDelegation(sessionFile, ids.root)).toBeNull();
      writeFileSync(statusPath, JSON.stringify({
        runId,
        toolCallId: "parent-tool-call-id",
        lifecycleArtifactVersion: 3,
        pid: process.pid,
        sessionId: "wrong-native-parent-id",
        mode: "single",
        state: "running",
        steps: [{ sessionFile }],
      }));
      expect(readPiSubagentDelegation(sessionFile, ids.root)).toBeNull();
      writeFileSync(statusPath, JSON.stringify({
        runId,
        toolCallId: "parent-tool-call-id",
        lifecycleArtifactVersion: 3,
        pid: process.pid,
        sessionId: ids.root,
        mode: "single",
        state: "running",
        steps: [{ sessionFile }],
      }));
      expect(readPiSubagentDelegation(sessionFile, ids.root)?.runId).toBe(runId);
      process.argv = [process.execPath, runnerFile, join(root, "outside", `async-cfg-${runId}.json`)];
      expect(resolvePiSubagentRunnerRunId(process.argv, process.env, process.pid)).toBeNull();
    } finally {
      process.argv = oldArgv;
      if (oldRoot === undefined) delete process.env.PI_SUBAGENTS_TEMP_ROOT;
      else process.env.PI_SUBAGENTS_TEMP_ROOT = oldRoot;
      if (oldPid === undefined) delete process.env.PI_SUBAGENT_RUNNER_PID;
      else process.env.PI_SUBAGENT_RUNNER_PID = oldPid;
      if (oldChild === undefined) delete process.env.PI_SUBAGENT_CHILD;
      else process.env.PI_SUBAGENT_CHILD = oldChild;
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("locates Pi AI from the actual nested host installation layout", () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-runtime-test-"));
    try {
      const host = join(root, "node_modules", "@earendil-works", "pi-coding-agent");
      const executable = join(host, "dist", "bundle", "cli.js");
      const piAi = join(host, "node_modules", "@earendil-works", "pi-ai");
      mkdirSync(join(host, "dist", "bundle"), { recursive: true });
      mkdirSync(piAi, { recursive: true });
      writeFileSync(join(host, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent" }));
      writeFileSync(executable, "");
      writeFileSync(join(piAi, "package.json"), JSON.stringify({ name: "@earendil-works/pi-ai", version: "1.4.0" }));
      expect(findPiAiPackageRoot(executable)).toBe(piAi);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("links only the first parent inference after a completed native subagent notice", () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-result-test-"));
    const oldRoot = process.env.PI_SUBAGENTS_TEMP_ROOT;
    try {
      const childFile = join(root, "child.jsonl");
      const parentFile = join(root, "parent.jsonl");
      writeFileSync(parentFile, [
        JSON.stringify({ type: "session", id: ids.root }),
        JSON.stringify({ type: "message", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent", id: "call-42" }] } }),
        JSON.stringify({ type: "message", message: { role: "toolResult", toolCallId: "call-42", details: { runId: "run-42" } } }),
      ].join("\n"));
      writeFileSync(childFile, [
        JSON.stringify({ type: "session", id: ids.child, parentSession: parentFile }),
        JSON.stringify({ type: "custom", customType: "tokenvisor-agent-run-completion-v1", data: {
          runId: "run-42", sessionId: ids.child, responseId: ids.previous,
        } }),
      ].join("\n"));
      const statusDir = join(root, "async-subagent-runs", "run-42");
      mkdirSync(statusDir, { recursive: true });
      const statusPath = join(statusDir, "status.json");
      const status = {
        lifecycleArtifactVersion: 3,
        runId: "run-42",
        sessionId: parentFile,
        sessionFile: childFile,
        state: "complete",
        mode: "single",
        steps: [{ sessionFile: childFile }],
      };
      writeFileSync(statusPath, JSON.stringify(status));
      for (let index = 0; index < 520; index += 1) {
        const unrelated = join(root, "async-subagent-runs", `unrelated-${index}`);
        mkdirSync(unrelated, { recursive: true });
        writeFileSync(join(unrelated, "status.json"), "{}");
      }
      process.env.PI_SUBAGENTS_TEMP_ROOT = root;
      const notice = {
        type: "custom_message",
        customType: "subagent-notify",
        details: { runs: [{ runId: "run-42", status: "completed" }] },
      };
      const launch = { type: "message", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent", id: "call-42" }] } };
      const fresh = piContext({ branch: [launch, notice], sessionFile: parentFile });
      expect(readPiUnconsumedRunDependencies(fresh)).toEqual([{
        session_id: `pi:session:${ids.child}`,
        response_id: ids.previous,
        run_id: "run-42",
      }]);
      writeFileSync(statusPath, JSON.stringify({ ...status, sessionId: ids.root }));
      expect(readPiUnconsumedRunDependencies(fresh)).toEqual([{
        session_id: `pi:session:${ids.child}`,
        response_id: ids.previous,
        run_id: "run-42",
      }]);
      writeFileSync(statusPath, JSON.stringify({ ...status, sessionId: "unrelated-parent-id" }));
      expect(readPiUnconsumedRunDependencies(fresh)).toBeNull();
      writeFileSync(statusPath, JSON.stringify(status));
      const consumed = piContext({ branch: [launch, notice, { type: "message", message: { role: "assistant", responseId: "parent-consumer" } }], sessionFile: parentFile });
      expect(readPiUnconsumedRunDependencies(consumed)).toEqual([]);
      const summarized = piContext({ branch: [launch, notice, { type: "compaction", id: "compact-1" }, {
        type: "custom",
        customType: PI_COMPACTION_ENTRY,
        data: { ...makePiCompactionRecord("compact-1", [ids.stream]), consumedRunIds: ["run-42"] },
      }], sessionFile: parentFile });
      expect(readPiUnconsumedRunDependencies(summarized)).toEqual([]);
    } finally {
      if (oldRoot === undefined) delete process.env.PI_SUBAGENTS_TEMP_ROOT;
      else process.env.PI_SUBAGENTS_TEMP_ROOT = oldRoot;
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("does not treat OMP cache-affinity parentSessionId as native ancestry", () => {
    const identity = deriveOmpIdentity({ sessionId: ids.root, parentSessionId: ids.child, messages: [] });
    expect(identity?.rootSessionId).toBeNull();
    expect(identity?.parentSessionId).toBeNull();
    const encoded = createOmpEnvelope({ sessionId: ids.root, parentSessionId: ids.child, messages: [] }, ids.stream);
    expect(parseAgentMetadata(encoded)?.session.parent_id).toBeNull();
    const duplicate = parseAgentMetadata(createOmpEnvelope({ sessionId: ids.root, messages: [
      { role: "assistant", responseId: ids.previous },
      { role: "assistant", responseId: ids.previous },
    ] }, ids.stream));
    expect(duplicate?.sequence).toEqual({ status: "unavailable", previous_response_id: null });
  });

  test("links a captured OMP auto-thinking judge to the ordinary request through native usage", () => {
    const state = createOmpSequenceState("child-run");
    const branch: Array<Record<string, any>> = [
      { type: "message", id: "user-1", parentId: null, message: { role: "user" } },
    ];
    expect(resolveOmpNativeSequence(branch, state, "child-run")).toEqual({
      previousResponseId: null,
      hasPriorHistory: false,
    });
    recordOmpSequenceStream(state, "judge-stream", "user-1");
    recordOmpSequenceResult(state, "judge-stream", "chatcmpl-judge-1");
    branch.push({
      type: "model_usage",
      id: "usage-1",
      parentId: "user-1",
      purpose: "auto-thinking",
      role: "judge",
    });
    branch.push({ type: "thinking_level_change", id: "thinking-1", parentId: "usage-1", configured: "auto" });

    expect(resolveOmpNativeSequence(branch, state, "child-run")).toEqual({
      previousResponseId: "chatcmpl-judge-1",
      hasPriorHistory: true,
    });
    const ordinary = createOmpEnvelope({
      sessionId: ids.child,
      nativeRootSessionId: ids.root,
      nativeParentSessionId: ids.root,
      delegation: { runId: "omp:task:TaskA", toolCallId: "task-call" },
      messages: [{ role: "user" }],
      nativeSequence: { previousResponseId: "chatcmpl-judge-1", hasPriorHistory: true },
    }, ids.stream);
    expect(parseAgentMetadata(ordinary)?.sequence).toEqual({
      status: "linked",
      previous_response_id: "chatcmpl-judge-1",
    });

    branch.push({ type: "message", id: "assistant-1", parentId: "thinking-1", message: { role: "assistant", responseId: "chatcmpl-ordinary-1" } });
    expect(resolveOmpNativeSequence(branch, state, "child-run")).toEqual({
      previousResponseId: "chatcmpl-ordinary-1",
      hasPriorHistory: true,
    });
  });

  test("uses the active transcript predecessor for repeated judge attempts and preserves the latest successful response", () => {
    const state = createOmpSequenceState("root");
    const branch: Array<Record<string, any>> = [
      { type: "message", id: "assistant-old", parentId: null, message: { role: "assistant", responseId: ids.previous } },
      { type: "message", id: "user-2", parentId: "assistant-old", message: { role: "user" } },
    ];
    expect(resolveOmpNativeSequence(branch, state, "root").previousResponseId).toBe(ids.previous);
    recordOmpSequenceStream(state, "judge-1", "user-2");
    recordOmpSequenceResult(state, "judge-1", "chatcmpl-judge-1");
    branch.push({ type: "model_usage", id: "usage-1", parentId: "user-2", purpose: "auto-thinking", role: "judge" });
    expect(resolveOmpNativeSequence(branch, state, "root").previousResponseId).toBe("chatcmpl-judge-1");

    recordOmpSequenceStream(state, "judge-2", "usage-1");
    recordOmpSequenceResult(state, "judge-2", "chatcmpl-judge-2");
    branch.push({ type: "model_usage", id: "usage-2", parentId: "usage-1", purpose: "auto-thinking", role: "judge" });
    expect(resolveOmpNativeSequence(branch, state, "root")).toEqual({
      previousResponseId: "chatcmpl-judge-2",
      hasPriorHistory: true,
    });
  });

  test("fails closed for failed, ambiguous, resumed, and cross-session OMP judge state", () => {
    const branch = [
      { type: "message", id: "user-3", parentId: null, message: { role: "user" } },
      { type: "model_usage", id: "usage-3", parentId: "user-3", purpose: "auto-thinking", role: "judge" },
    ];
    const failed = createOmpSequenceState("child");
    recordOmpSequenceStream(failed, "failed-stream", "user-3");
    recordOmpSequenceResult(failed, "failed-stream", null);
    expect(resolveOmpNativeSequence(branch, failed, "child")).toEqual({
      previousResponseId: null,
      hasPriorHistory: true,
    });

    for (const failure of [
      { stopReason: "error" },
      { errorMessage: "provider failed" },
    ]) {
      const nativeFailure = createOmpSequenceState("child");
      recordOmpSequenceStream(nativeFailure, "failed-native-stream", "user-3");
      recordOmpSequenceResult(nativeFailure, "failed-native-stream", "chatcmpl-reported-before-native-failure");
      expect(resolveOmpNativeSequence([
        { type: "message", id: "user-3", parentId: null, message: { role: "user" } },
        {
          type: "model_usage",
          id: "usage-failure",
          parentId: "user-3",
          purpose: "auto-thinking",
          role: "judge",
          ...failure,
        },
      ], nativeFailure, "child")).toEqual({ previousResponseId: null, hasPriorHistory: true });
    }

    const ambiguous = createOmpSequenceState("child");
    recordOmpSequenceStream(ambiguous, "parallel-a", "user-3");
    recordOmpSequenceResult(ambiguous, "parallel-a", "chatcmpl-a");
    recordOmpSequenceStream(ambiguous, "parallel-b", "user-3");
    recordOmpSequenceResult(ambiguous, "parallel-b", "chatcmpl-b");
    expect(resolveOmpNativeSequence(branch, ambiguous, "child")).toEqual({
      previousResponseId: null,
      hasPriorHistory: true,
    });

    const resumed = createOmpSequenceState("child");
    expect(resolveOmpNativeSequence(branch, resumed, "child")).toEqual({
      previousResponseId: null,
      hasPriorHistory: true,
    });

    const actorA = createOmpSequenceState("actor-a");
    const actorB = createOmpSequenceState("actor-b");
    recordOmpSequenceStream(actorA, "actor-a-stream", "user-3");
    recordOmpSequenceResult(actorA, "actor-a-stream", "chatcmpl-actor-a");
    expect(resolveOmpNativeSequence(branch, actorB, "actor-b")).toEqual({
      previousResponseId: null,
      hasPriorHistory: true,
    });
  });

  test("keeps ordinary OMP sequencing and compaction references unchanged without a judge entry", () => {
    const branch = [
      { type: "message", id: "assistant", parentId: null, message: { role: "assistant", responseId: ids.previous } },
      { type: "compaction", id: "compact-1" },
    ];
    expect(resolveOmpNativeSequence(branch, createOmpSequenceState("root"), "root")).toEqual({
      previousResponseId: ids.previous,
      hasPriorHistory: true,
    });
    const refs = [{ session_id: `omp:session:${ids.root}`, stream_id: ids.stream }];
    expect(pendingOmpCompactionDependencies(branch, refs)).toEqual(refs);
  });

  test("preserves the native assistant predecessor when compaction has a pending stream on the same leaf", () => {
    const state = createOmpSequenceState("root");
    recordOmpSequenceStream(state, "summary-stream", "assistant");
    recordOmpSequenceResult(state, "summary-stream", "chatcmpl-summary");
    const branch = [
      { type: "message", id: "assistant", parentId: null, message: { role: "assistant", responseId: ids.previous } },
      { type: "compaction", id: "compact-1" },
    ];
    expect(resolveOmpNativeSequence(branch, state, "root")).toEqual({
      previousResponseId: ids.previous,
      hasPriorHistory: true,
    });
    expect(state.pending.size).toBe(0);

    const refs = [{ session_id: `omp:session:${ids.root}`, stream_id: "summary-stream" }];
    expect(pendingOmpCompactionDependencies(branch, refs)).toEqual(refs);
    const consumed = [...branch, {
      type: "message",
      id: "consumer",
      parentId: "compact-1",
      message: { role: "assistant", responseId: "chatcmpl-consumer" },
    }];
    expect(resolveOmpNativeSequence(consumed, state, "root")).toEqual({
      previousResponseId: "chatcmpl-consumer",
      hasPriorHistory: true,
    });
    expect(pendingOmpCompactionDependencies(consumed, refs)).toEqual([]);
  });

  test("retries a unique native judge match if its stream result arrives after the usage entry", () => {
    const state = createOmpSequenceState("root");
    const branch = [
      { type: "message", id: "user-4", parentId: null, message: { role: "user" } },
      { type: "model_usage", id: "usage-4", parentId: "user-4", purpose: "auto-thinking", role: "judge" },
    ];
    recordOmpSequenceStream(state, "late-stream", "user-4");
    expect(resolveOmpNativeSequence(branch, state, "root")).toEqual({
      previousResponseId: null,
      hasPriorHistory: true,
    });
    expect(state.responseByUsageId.has("usage-4")).toBe(false);
    recordOmpSequenceResult(state, "late-stream", "chatcmpl-late-result");
    expect(resolveOmpNativeSequence(branch, state, "root")).toEqual({
      previousResponseId: "chatcmpl-late-result",
      hasPriorHistory: true,
    });
  });

  test("binds OMP task identity through child registry id and the parent's native task result", () => {
    const parentFile = nativeSessionFile(ids.root);
    const childFile = join(nativeFixtureRoot, `${ids.child}.jsonl`);
    writeFileSync(childFile, `${JSON.stringify({ type: "title", title: "child" })}\n${JSON.stringify({ type: "session", id: ids.child, parentSession: parentFile })}\n`);
    const agentId = "HuskyBuzzard";
    writeFileSync(parentFile, [
      JSON.stringify({ type: "title", title: "fixture" }),
      JSON.stringify({ type: "session", id: ids.root }),
      JSON.stringify({ type: "message", message: {
        role: "assistant",
        content: [{ type: "toolCall", id: "omp-task-call-1" }],
      } }),
      JSON.stringify({ type: "message", message: {
        role: "toolResult",
        toolCallId: "omp-task-call-1",
        details: { progress: [{ id: agentId, status: "completed" }] },
      } }),
    ].join("\n"));
    expect(deriveOmpSessionLineage(ids.child, parentFile, false, childFile)).toEqual({
      rootSessionId: ids.root,
      parentSessionId: ids.root,
    });
    expect(readOmpTaskDelegation(parentFile, ids.root, agentId)).toEqual({
      runId: `omp:task:${agentId}`,
      toolCallId: "omp-task-call-1",
    });
    expect(readOmpTaskDelegation(parentFile, ids.root, "UnrelatedTask")).toBeNull();
    expect(deriveOmpSessionLineage(ids.root, undefined, false, join(nativeFixtureRoot, "not-yet-written.jsonl"), true)).toEqual({
      rootSessionId: ids.root,
      parentSessionId: null,
    });
    expect(deriveOmpSessionLineage(ids.root, undefined, false, join(nativeFixtureRoot, "not-yet-written.jsonl"))).toEqual({
      rootSessionId: null,
      parentSessionId: null,
    });
  });

  test("binds an OMP wait result to the durable terminal child response", () => {
    const parentFile = join(nativeFixtureRoot, `${ids.root}.jsonl`);
    const childFile = join(nativeFixtureRoot, ids.root, "TaskA.jsonl");
    mkdirSync(join(nativeFixtureRoot, ids.root), { recursive: true });
    writeFileSync(parentFile, `${JSON.stringify({ type: "session", id: ids.root })}\n`);
    writeFileSync(childFile, [
      JSON.stringify({ type: "title", title: "child" }),
      JSON.stringify({ type: "session", id: ids.child, parentSession: parentFile }),
      JSON.stringify({ type: "message", message: { role: "assistant", stopReason: "stop", responseId: ids.previous } }),
      JSON.stringify({ type: "custom", customType: OMP_RUN_COMPLETION_ENTRY, data: {
        parentSessionId: ids.root, sessionId: ids.child, responseId: ids.previous,
      } }),
    ].join("\n"));
    const branch = [
      { type: "message", message: { role: "assistant", content: [{ type: "toolCall", id: "task-call" }] } },
      { type: "message", message: { role: "toolResult", toolCallId: "task-call", details: { progress: [{ id: "TaskA", agent: "task" }] } } },
      { type: "message", message: { role: "toolResult", details: { jobs: [{ id: "TaskA", type: "task", status: "completed" }] } } },
    ];
    expect(readOmpTaskResultDependencies(parentFile, ids.root, branch)).toEqual({
      dependencies: [{ session_id: `omp:session:${ids.child}`, response_id: ids.previous, run_id: "omp:task:TaskA" }],
      runIds: ["omp:task:TaskA"],
    });
    expect(readOmpTaskResultDependencies(parentFile, ids.root, [
      ...branch,
      { type: "message", message: { role: "assistant", responseId: "parent-consumed" } },
    ])).toEqual({ dependencies: [], runIds: [] });
    expect(readOmpTaskResultDependencies(join(nativeFixtureRoot, "missing-parent.jsonl"), ids.root, [
      { type: "message", message: { role: "assistant", responseId: "no-task-results" } },
    ])).toEqual({ dependencies: [], runIds: [] });
  });

  test("exposes compaction refs for one consumer frontier and scopes resumed forks", () => {
    const refs = [{ session_id: `omp:session:${ids.root}`, stream_id: ids.stream }];
    const compacted = [{ type: "compaction", id: "omp-compact-1" }];
    expect(pendingOmpCompactionDependencies(compacted, refs)).toEqual(refs);
    const afterConsumer = [...compacted, { type: "message", message: { role: "assistant", responseId: "consumer" } }];
    expect(pendingOmpCompactionDependencies(afterConsumer, refs)).toEqual([]);
    expect(pendingOmpCompactionDependencies([...afterConsumer, { type: "compaction", id: "omp-compact-2" }], refs)).toEqual(refs);

    const marker = { type: "custom", customType: PI_FORK_ROOT_ENTRY, data: { sessionId: ids.child, parentBoundary: "old-tail" } };
    const copied = [{ type: "compaction", id: "inherited" }, { type: "message", message: { role: "assistant", responseId: ids.previous } }];
    const forkBranch = [...copied, marker];
    const boundary = readOmpForkBoundaryIndex(forkBranch, ids.child);
    expect(boundary).toBe(copied.length);
    const ownBranch = forkBranch.slice(boundary! + 1);
    expect(pendingOmpCompactionDependencies(ownBranch, refs)).toEqual(refs);
    expect(deriveOmpIdentity({ sessionId: ids.child, isForkedRoot: true, messages: [] }, ids.stream, []).hasPriorHistory).toBe(false);
    const later = deriveOmpIdentity({
      sessionId: ids.child,
      isForkedRoot: true,
      messages: [{ role: "assistant", responseId: "fork-response" }],
    }, ids.stream, refs);
    expect(later?.previousResponseId).toBe("fork-response");
    expect(later?.dependsOn).toEqual(refs);
  });
});
