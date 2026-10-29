import { encodeAgentMetadata, type AgentMetadata, type MetadataIdentity } from "./shared.ts";
import { readNativeSessionHeader, readNativeSessionRootId } from "./native-session.ts";
import { closeSync, fstatSync, openSync, readSync, realpathSync } from "fs";
import { dirname, join, basename } from "path";

export const OMP_EXTENSION_VERSION = "1.1.0-dev";
export const OMP_RUNNER_VERSION = "18.12.6";
export const OMP_RUN_COMPLETION_ENTRY = "tokenvisor-agent-run-completion-v1";
export const OMP_TASK_RESULT_CONSUMPTION_ENTRY = "tokenvisor-agent-task-result-consumption-v1";
const PI_FORK_ROOT_ENTRY = "tokenvisor-agent-fork-root-v1";

export type OmpCompactionResponseDisposition = "contributor" | "empty" | "unusable";

/** Match OMP's local compaction consumer: non-error text is accepted, but empty truncations add no text. */
export function classifyOmpCompactionResponse(
  stopReason: unknown,
  content: unknown,
): OmpCompactionResponseDisposition {
  if (stopReason === "stop") return "contributor";
  if (stopReason !== "length") return "unusable";
  if (!Array.isArray(content)) return "empty";
  const hasText = content.some((part) =>
    part !== null && typeof part === "object" &&
    (part as { type?: unknown }).type === "text" &&
    typeof (part as { text?: unknown }).text === "string" &&
    (part as { text: string }).text.length > 0,
  );
  return hasText ? "contributor" : "empty";
}

/** Record only response streams whose returned text can enter OMP's native compaction entry. */
export function recordOmpCompactionResponse(
  streamIds: string[],
  streamId: string,
  responseId: string | null,
  stopReason: unknown,
  content: unknown,
): OmpCompactionResponseDisposition {
  if (!responseId) return "unusable";
  const disposition = classifyOmpCompactionResponse(stopReason, content);
  if (disposition === "contributor" && !streamIds.includes(streamId)) streamIds.push(streamId);
  return disposition;
}

interface MessageLike {
  role?: string;
  responseId?: string;
}

function isNativeId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 256 && /^[A-Za-z0-9._:-]+$/.test(value);
}

export interface OmpCaptureContext {
  sessionId?: string;
  agentId?: string;
  delegation?: MetadataIdentity["delegation"];
  nativeParentSessionId?: string;
  nativeRootSessionId?: string | null;
  isKnownRoot?: boolean;
  isForkedRoot?: boolean;
  parentSessionId?: string;
  messages?: MessageLike[];
  nativeSequence?: { previousResponseId: string | null; hasPriorHistory: boolean };
}

export interface OmpSequenceStream {
  anchorId: string | null;
  responseId: string | null;
  terminal: boolean;
}

export interface OmpSequenceState {
  pending: Map<string, OmpSequenceStream>;
  responseByUsageId: Map<string, string | null>;
  lineageKey: string | null;
}

export function createOmpSequenceState(lineageKey: string | null = null): OmpSequenceState {
  return { pending: new Map(), responseByUsageId: new Map(), lineageKey };
}

/** Reconcile captured streams only against OMP's exact native judge usage entry. */
export function resolveOmpNativeSequence(
  branch: Array<Record<string, any>>,
  state: OmpSequenceState,
  lineageKey: string | null,
): { previousResponseId: string | null; hasPriorHistory: boolean } {
  if (state.lineageKey !== lineageKey) {
    state.pending.clear();
    state.responseByUsageId.clear();
    state.lineageKey = lineageKey;
  }

  const assistantEntries = branch.flatMap((entry, index) => {
    const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
    return message.role === "assistant" ? [{ entry, index, message }] : [];
  });
  const latestAssistant = assistantEntries.at(-1);
  const previousAssistantId = typeof latestAssistant?.message.responseId === "string"
    ? latestAssistant.message.responseId
    : null;
  const hasPriorHistory = latestAssistant !== undefined;
  const latestAssistantIndex = latestAssistant?.index ?? -1;
  if (previousAssistantId && assistantEntries.filter(({ message }) => message.responseId === previousAssistantId).length !== 1) {
    return { previousResponseId: null, hasPriorHistory: true };
  }

  for (const [streamId, stream] of state.pending) {
    const anchorIndex = stream.anchorId === null
      ? -1
      : branch.findIndex((entry) => entry.id === stream.anchorId);
    const representedByAssistant = stream.responseId !== null && assistantEntries.some(
      ({ message }) => message.responseId === stream.responseId,
    );
    if (representedByAssistant || anchorIndex < latestAssistantIndex) state.pending.delete(streamId);
  }

  const modelUsages = branch.flatMap((entry, index) => {
    if (index <= latestAssistantIndex || entry.type !== "model_usage") return [];
    return [{ entry, index }];
  });
  const judgeUsages = modelUsages.filter(({ entry }) => entry.purpose === "auto-thinking" && entry.role === "judge");
  const activeUsageIds = new Set(judgeUsages.map(({ entry }) => entry.id));
  for (const usageId of state.responseByUsageId.keys()) {
    if (!activeUsageIds.has(usageId)) state.responseByUsageId.delete(usageId);
  }
  for (const { entry } of judgeUsages) {
    if (!state.responseByUsageId.has(entry.id)) {
      const matches = isNativeId(entry.parentId)
        ? [...state.pending.entries()].filter(([, stream]) => stream.anchorId === entry.parentId)
        : [];
      if (matches.length === 1 && !matches[0][1].terminal) {
        continue;
      }
      if (matches.length !== 1) {
        state.responseByUsageId.set(entry.id, null);
      } else {
        const [streamId, stream] = matches[0];
        const failed = entry.stopReason === "error" ||
          (typeof entry.errorMessage === "string" && entry.errorMessage.length > 0);
        state.responseByUsageId.set(entry.id, failed ? null : stream.responseId);
        state.pending.delete(streamId);
      }
    }
    if (state.responseByUsageId.size > 16) state.responseByUsageId.clear();
  }

  const latestModelUsage = modelUsages.at(-1)?.entry;
  if (latestModelUsage) {
    if (latestModelUsage.purpose !== "auto-thinking" || latestModelUsage.role !== "judge") {
      return { previousResponseId: null, hasPriorHistory: true };
    }
    const latestJudgeUsage = latestModelUsage;
    const responseId = state.responseByUsageId.get(latestJudgeUsage.id);
    const matchesInBranch = responseId
      ? assistantEntries.filter(({ message }) => message.responseId === responseId).length
      : 0;
    const matchesInUsage = responseId
      ? judgeUsages.filter(({ entry }) => state.responseByUsageId.get(entry.id) === responseId).length
      : 0;
    if (responseId && matchesInBranch === 0 && matchesInUsage === 1) {
      return { previousResponseId: responseId, hasPriorHistory: true };
    }
    return { previousResponseId: null, hasPriorHistory: true };
  }

  if (state.pending.size > 0) {
    state.pending.clear();
    return previousAssistantId
      ? { previousResponseId: previousAssistantId, hasPriorHistory: true }
      : { previousResponseId: null, hasPriorHistory: true };
  }
  return { previousResponseId: previousAssistantId, hasPriorHistory };
}

export function recordOmpSequenceStream(
  state: OmpSequenceState,
  streamId: string,
  anchorId: string | null,
): void {
  if (state.pending.size >= 16) state.pending.clear();
  state.pending.set(streamId, { anchorId, responseId: null, terminal: false });
}

export function recordOmpSequenceResult(
  state: OmpSequenceState,
  streamId: string,
  responseId: string | null,
): void {
  const stream = state.pending.get(streamId);
  if (!stream) return;
  stream.responseId = responseId;
  stream.terminal = true;
}

export interface OmpSessionLineageBinding {
  rootSessionId: string | null;
  parentSessionId: string | null;
  delegation: MetadataIdentity["delegation"];
  forkedRoot?: boolean;
}

export interface OmpRuntimeCaptureState {
  lineageBySession: Map<string, OmpSessionLineageBinding>;
  compactionStreams: Map<string, string[]>;
  compactionCaptureActive: Set<string>;
  compactionStreamFailures: Set<string>;
  compactionPriorStreams: Map<string, string[] | null>;
  compactionDependencies: Map<string, AgentMetadata["depends_on"]>;
  branchReaders: Map<string, () => Array<Record<string, any>>>;
  sessionFiles: Map<string, string>;
  appendEntryBySession: Map<string, (customType: string, data?: unknown) => void>;
  sequenceBySession: Map<string, OmpSequenceState>;
  pendingTaskResultConsumptions: Map<string, Map<string, OmpTaskResultReference[]>>;
  compactionTaskResultReferences: Map<string, OmpTaskResultReference[]>;
  taskResultReferencesByStream: Map<string, OmpTaskResultStreamReferences>;
}

export interface OmpTaskResultReference {
  sessionId: string;
  responseId: string;
  runId: string;
  deliveryId: string;
  jobId: string;
}

export interface OmpTaskResultStreamReferences {
  sessionId: string;
  references: OmpTaskResultReference[];
}

interface OmpTaskResultCompletion {
  sessionId: string;
  responseId: string;
  runId: string;
}

type OmpTaskResultConsumer = { type: "assistant" | "compaction"; id: string };

function taskResultReferenceKey(reference: Pick<OmpTaskResultReference, "sessionId" | "responseId">): string {
  return JSON.stringify([reference.sessionId, reference.responseId]);
}

/** Build a consumption marker only for a consumer already committed to the native branch. */
export function makeOmpTaskResultConsumptionRecord(
  branch: Array<Record<string, any>>,
  parentSessionId: string,
  consumer: OmpTaskResultConsumer,
  references: readonly OmpTaskResultReference[],
): Record<string, unknown> | null {
  if (!isNativeId(parentSessionId) || !isNativeId(consumer.id) || references.length === 0 || references.length > 8) return null;
  const consumerIndexes = branch.flatMap((entry, index) => {
    const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
    if (consumer.type === "assistant") {
      return message.role === "assistant" && message.responseId === consumer.id ? [index] : [];
    }
    return entry.type === "compaction" && entry.id === consumer.id ? [index] : [];
  });
  if (consumerIndexes.length !== 1) return null;
  const seen = new Set<string>();
  const refs: OmpTaskResultReference[] = [];
  for (const reference of references) {
    if (!isNativeId(reference.sessionId) || !isNativeId(reference.responseId) || !isNativeId(reference.runId) ||
      !isNativeId(reference.deliveryId) || !isNativeId(reference.jobId)) return null;
    const key = taskResultReferenceKey(reference);
    if (seen.has(key)) return null;
    seen.add(key);
    refs.push({ ...reference });
  }
  return {
    version: 1,
    parentSessionId,
    consumer,
    references: refs,
  };
}

function hasCommittedOmpTaskResultConsumer(
  branch: Array<Record<string, any>>,
  consumerId: string,
  references: readonly OmpTaskResultReference[],
): boolean {
  return branch.some((entry) => {
    const data = entry.customType === OMP_TASK_RESULT_CONSUMPTION_ENTRY && entry.data && typeof entry.data === "object"
      ? entry.data as Record<string, any>
      : null;
    if (data?.version !== 1 || data.consumer?.type !== "assistant" || data.consumer.id !== consumerId || !Array.isArray(data.references)) return false;
    const recorded = new Set(data.references.map((reference: any) => JSON.stringify([
      reference?.sessionId,
      reference?.responseId,
      reference?.runId,
      reference?.deliveryId,
      reference?.jobId,
    ])));
    return references.every((reference) => recorded.has(JSON.stringify([
      reference.sessionId,
      reference.responseId,
      reference.runId,
      reference.deliveryId,
      reference.jobId,
    ])));
  });
}

/** Persist consumption only after the exact parent response is present in its owning native branch. */
export function commitPendingOmpTaskResultConsumers(
  sessionId: string,
  branch: Array<Record<string, any>>,
  pending: Map<string, OmpTaskResultReference[]>,
  appendEntry: ((customType: string, data?: unknown) => void) | undefined,
  readBranch: () => Array<Record<string, any>>,
): Array<Record<string, any>> {
  if (!appendEntry) return branch;
  for (const [responseId, references] of pending) {
    if (hasCommittedOmpTaskResultConsumer(branch, responseId, references)) {
      pending.delete(responseId);
      continue;
    }
    const record = makeOmpTaskResultConsumptionRecord(
      branch,
      sessionId,
      { type: "assistant", id: responseId },
      references,
    );
    if (!record) continue;
    appendEntry(OMP_TASK_RESULT_CONSUMPTION_ENTRY, record);
    const committedBranch = readBranch();
    if (hasCommittedOmpTaskResultConsumer(committedBranch, responseId, references)) pending.delete(responseId);
    branch = committedBranch;
  }
  return branch;
}

/** Read only response-specific consumption markers whose consumer is committed in this branch. */
export function readOmpConsumedTaskResultReferences(
  branch: Array<Record<string, any>>,
  parentSessionId: string,
): { references: Set<string>; deliveryIds: Set<string> } | null {
  const references = new Set<string>();
  const deliveryIds = new Set<string>();
  const consumerKeys = new Set<string>();
  for (let recordIndex = 0; recordIndex < branch.length; recordIndex += 1) {
    const entry = branch[recordIndex];
    if (entry.customType !== OMP_TASK_RESULT_CONSUMPTION_ENTRY) continue;
    const data = entry.data && typeof entry.data === "object" ? entry.data as Record<string, any> : null;
    if (!data || data.parentSessionId !== parentSessionId) continue;
    if (data.version !== 1 || !data.consumer || !["assistant", "compaction"].includes(data.consumer.type) ||
      !isNativeId(data.consumer.id) || !Array.isArray(data.references) || data.references.length === 0 || data.references.length > 8) return null;
    const consumerKey = JSON.stringify([data.consumer.type, data.consumer.id]);
    if (consumerKeys.has(consumerKey)) return null;
    consumerKeys.add(consumerKey);
    const consumerIndexes = branch.flatMap((candidate, index) => {
      const message = candidate.message && typeof candidate.message === "object" ? candidate.message : candidate;
      if (data.consumer.type === "assistant") {
        return message.role === "assistant" && message.responseId === data.consumer.id ? [index] : [];
      }
      return candidate.type === "compaction" && candidate.id === data.consumer.id ? [index] : [];
    });
    if (consumerIndexes.length !== 1 || consumerIndexes[0] >= recordIndex) return null;
    const refsSeen = new Set<string>();
    for (const reference of data.references) {
      if (!reference || !isNativeId(reference.sessionId) || !isNativeId(reference.responseId) || !isNativeId(reference.runId)) return null;
      if (!isNativeId(reference.deliveryId) || !isNativeId(reference.jobId)) return null;
      const key = taskResultReferenceKey(reference);
      if (refsSeen.has(key)) return null;
      refsSeen.add(key);
      references.add(key);
      deliveryIds.add(reference.deliveryId);
    }
  }
  return { references, deliveryIds };
}

const OMP_LINEAGE_STATE = Symbol.for("tokenvisor-pi.agent-capture.omp-lineage.v1");

/** Per-session host context shared by the child extension instance and inherited provider callback. */
export function getOmpRuntimeCaptureState(): OmpRuntimeCaptureState {
  const runtime = globalThis as any;
  runtime[OMP_LINEAGE_STATE] ??= {
    lineageBySession: new Map(),
    compactionStreams: new Map(),
    compactionCaptureActive: new Set(),
    compactionStreamFailures: new Set(),
    compactionPriorStreams: new Map(),
    compactionDependencies: new Map(),
    branchReaders: new Map(),
    sessionFiles: new Map(),
    appendEntryBySession: new Map(),
    sequenceBySession: new Map(),
    pendingTaskResultConsumptions: new Map(),
    compactionTaskResultReferences: new Map(),
    taskResultReferencesByStream: new Map(),
  } satisfies OmpRuntimeCaptureState;
  runtime[OMP_LINEAGE_STATE].sequenceBySession ??= new Map();
  runtime[OMP_LINEAGE_STATE].pendingTaskResultConsumptions ??= new Map();
  runtime[OMP_LINEAGE_STATE].appendEntryBySession ??= new Map();
  runtime[OMP_LINEAGE_STATE].compactionTaskResultReferences ??= new Map();
  runtime[OMP_LINEAGE_STATE].taskResultReferencesByStream ??= new Map();
  return runtime[OMP_LINEAGE_STATE] as OmpRuntimeCaptureState;
}

export function readOmpTaskResultDependencies(
  parentSessionFile: string | undefined,
  parentSessionId: string,
  branch: Array<Record<string, any>>,
  effectiveMessages: Array<Record<string, any>> = [],
): { dependencies: NonNullable<AgentMetadata["depends_on"]>; references: OmpTaskResultReference[] } | null {
  const calls: string[] = [];
  const pendingTaskCalls = new Map<string, string[]>();
  let lastAssistantIndex = -1;
  for (let index = 0; index < branch.length; index += 1) {
    const raw = branch[index];
    const message = raw.message && typeof raw.message === "object" ? raw.message : raw;
    if (message.role === "assistant") {
      if (Array.isArray(message.content)) {
        for (const part of message.content) {
          if ((part?.type === "toolCall" || part?.type === "tool_call") && typeof part.id === "string") calls.push(part.id);
        }
      }
      lastAssistantIndex = index;
    }
  }
  for (const raw of branch) {
    const message = raw.message && typeof raw.message === "object" ? raw.message : raw;
    if (message.role !== "toolResult" && message.role !== "tool_result") continue;
    const details = message.details;
    for (const progress of Array.isArray(details?.progress) ? details.progress : []) {
      if (!isNativeId(progress?.id)) continue;
      const found = pendingTaskCalls.get(progress.id) ?? [];
      if (typeof message.toolCallId === "string") found.push(message.toolCallId);
      pendingTaskCalls.set(progress.id, found);
    }
  }
  const deliveryCandidates: Array<{ jobId: string; agentUrlId: string; deliveryId: string }> = [];
  for (const raw of branch.slice(lastAssistantIndex + 1)) {
    const message = raw.message && typeof raw.message === "object" ? raw.message : raw;
    const details = message.details ?? raw.details;
    const isWaitResult = message.role === "toolResult" && (details?.op === "wait" || details?.op === "cancel");
    const isAsyncResult = (message.role === "custom" && message.customType === "async-result") ||
      (raw.type === "custom_message" && raw.customType === "async-result");
    if (!isWaitResult && !isAsyncResult) continue;
    for (const job of Array.isArray(details?.jobs) ? details.jobs : []) {
      if (job?.type !== "task" || job.status !== "completed") continue;
      // Native automatic deliveries lose their job identity during conversion.
      if (isAsyncResult) return null;
      if (isWaitResult && !Object.hasOwn(job, "resultText") && !Object.hasOwn(job, "structured")) continue;
      const jobId = job.id;
      const agentUrlId = job.agentUrlId;
      if (!isNativeId(jobId) || !isNativeId(agentUrlId)) return null;
      const deliveryId = raw.id;
      if (!isNativeId(deliveryId)) return null;
      const effectiveMatches = effectiveMessages.filter((candidateMessage) =>
        candidateMessage.role === "toolResult" && candidateMessage.toolCallId === message.toolCallId,
      );
      if (effectiveMatches.length !== 1) return null;
      const candidateDetails = effectiveMatches[0].details;
      if (!candidateDetails || candidateDetails.op !== "wait" && candidateDetails.op !== "cancel" || !Array.isArray(candidateDetails.jobs)) return null;
      if (!candidateDetails.jobs.some((candidateJob: any) =>
        candidateJob?.id === jobId && candidateJob?.agentUrlId === agentUrlId && candidateJob?.type === "task" && candidateJob?.status === "completed" &&
        (Object.hasOwn(candidateJob, "resultText") || Object.hasOwn(candidateJob, "structured")),
      )) return null;
      deliveryCandidates.push({ jobId, agentUrlId, deliveryId });
    }
  }
  if (deliveryCandidates.length === 0) return { dependencies: [], references: [] };
  if (!parentSessionFile || readNativeSessionHeader(parentSessionFile)?.id !== parentSessionId) return null;
  const consumed = readOmpConsumedTaskResultReferences(branch, parentSessionId);
  if (!consumed) return null;
  const uniqueCandidates = new Map<string, { jobId: string; agentUrlId: string; deliveryId: string }>();
  for (const candidate of deliveryCandidates) {
    const key = JSON.stringify([candidate.deliveryId, candidate.jobId]);
    if (uniqueCandidates.has(key)) continue;
    if (consumed.deliveryIds.has(candidate.deliveryId)) continue;
    if ([...uniqueCandidates.values()].some((previous) => previous.agentUrlId === candidate.agentUrlId)) return null;
    uniqueCandidates.set(key, candidate);
  }
  if (uniqueCandidates.size === 0) return { dependencies: [], references: [] };
  const dependencies: NonNullable<AgentMetadata["depends_on"]> = [];
  const references: OmpTaskResultReference[] = [];
  const usedReferences = new Set<string>();
  for (const { jobId, agentUrlId, deliveryId } of uniqueCandidates.values()) {
    const runId = `omp:task:${agentUrlId}`;
    const taskCalls = [...new Set(pendingTaskCalls.get(agentUrlId) ?? [])];
    if (taskCalls.length !== 1 || calls.filter((id) => id === taskCalls[0]).length !== 1) return null;
    const childFile = join(dirname(parentSessionFile), basename(parentSessionFile, ".jsonl"), `${agentUrlId}.jsonl`);
    const completions = readOmpRunCompletions(childFile, parentSessionId, runId);
    if (!completions) return null;
    const available = completions.filter((completion) => !consumed.references.has(taskResultReferenceKey(completion)));
    if (available.length !== 1) return null;
    const completion = { ...available[0], deliveryId, jobId };
    const completionKey = taskResultReferenceKey(completion);
    if (usedReferences.has(completionKey)) return null;
    usedReferences.add(completionKey);
    dependencies.push({ session_id: `omp:session:${completion.sessionId}`, response_id: completion.responseId, run_id: completion.runId });
    references.push(completion);
  }
  if (dependencies.length > 8) return null;
  return { dependencies, references };
}

export function pendingOmpCompactionDependencies(
  branch: Array<Record<string, any>>,
  dependencies: AgentMetadata["depends_on"],
): AgentMetadata["depends_on"] {
  if (dependencies === null) return null;
  const latestCompactionIndex = branch.reduce((found, entry, index) => entry.type === "compaction" ? index : found, -1);
  if (latestCompactionIndex < 0) return dependencies;
  const summaryWasConsumed = branch.slice(latestCompactionIndex + 1).some((raw) => {
    const message = raw.message && typeof raw.message === "object" ? raw.message : raw;
    return message.role === "assistant";
  });
  return summaryWasConsumed ? [] : dependencies;
}

/** A recent IRC arrival has no response-specific peer source in the public extension API. */
export function hasUnresolvedOmpPeerWake(branch: Array<Record<string, any>>): boolean {
  let latestConsumerIndex = -1;
  for (let index = 0; index < branch.length; index += 1) {
    const entry = branch[index];
    const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
    // A compaction entry may be a model-free transformation, so it does not
    // establish that an inference consumed the incoming peer message.
    if (message.role === "assistant" && isNativeId(message.responseId)) latestConsumerIndex = index;
  }
  return branch.slice(latestConsumerIndex + 1).some((entry) => {
    const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
    return message.customType === "irc:incoming" || entry.customType === "irc:incoming";
  });
}

export function resolveOmpPeerWakeDependencies(
  branch: Array<Record<string, any>>,
  dependencies: AgentMetadata["depends_on"],
): AgentMetadata["depends_on"] {
  return hasUnresolvedOmpPeerWake(branch) ? null : dependencies;
}

export function readOmpForkBoundaryIndex(branch: Array<Record<string, any>>, sessionId: string): number | null {
  const index = branch.findIndex((raw) => {
    const data = raw.data && typeof raw.data === "object" ? raw.data as Record<string, unknown> : null;
    return raw.customType === PI_FORK_ROOT_ENTRY && data?.sessionId === sessionId;
  });
  return index < 0 ? null : index;
}

function readOmpRunCompletions(
  childSessionFile: string,
  parentSessionId: string,
  runId: string,
): OmpTaskResultCompletion[] | null {
  const header = readNativeSessionHeader(childSessionFile);
  if (!header?.parentSession || readNativeSessionHeader(header.parentSession)?.id !== parentSessionId) return null;
  let fd: number | undefined;
  try {
    fd = openSync(childSessionFile, "r");
    const stat = fstatSync(fd);
    const maxBytes = 4 * 1024 * 1024;
    if (stat.size > maxBytes) return null;
    const length = stat.size;
    const buffer = Buffer.alloc(length);
    readSync(fd, buffer, 0, length, stat.size - length);
    if (length > 0 && buffer[length - 1] !== 0x0a) return null;
    const completions: OmpTaskResultCompletion[] = [];
    const seen = new Set<string>();
    for (const line of buffer.toString("utf8").split("\n")) {
      if (!line) continue;
      let entry: { customType?: string; data?: Record<string, unknown> };
      try { entry = JSON.parse(line) as typeof entry; } catch { return null; }
      if (entry.customType !== OMP_RUN_COMPLETION_ENTRY) continue;
      const data = entry.data;
      if (!data || data.version !== 1 || !isNativeId(data.runId) || !isNativeId(data.parentSessionId) ||
        !isNativeId(data.sessionId) || !isNativeId(data.responseId)) return null;
      if (data.runId !== runId || data.parentSessionId !== parentSessionId || data.sessionId !== header.id) continue;
      const completion = { sessionId: header.id, responseId: data.responseId, runId };
      const key = taskResultReferenceKey(completion);
      if (!seen.has(key)) completions.push(completion);
      seen.add(key);
    }
    return completions;
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
  return null;
}

export function readOmpTaskDelegation(
  parentSessionFile: string | undefined,
  parentSessionId: string | null,
  agentId: string | undefined,
): { runId: string; toolCallId: string } | null {
  if (!parentSessionFile || !parentSessionId || !agentId || !/^[A-Za-z0-9._:-]{1,256}$/.test(agentId)) return null;
  if (readNativeSessionHeader(parentSessionFile)?.id !== parentSessionId) return null;
  let fd: number | undefined;
  try {
    fd = openSync(parentSessionFile, "r");
    const stat = fstatSync(fd);
    const length = Math.min(stat.size, 8 * 1024 * 1024);
    const buffer = Buffer.alloc(length);
    readSync(fd, buffer, 0, length, stat.size - length);
    const matches: string[] = [];
    const parentToolCallIds: string[] = [];
    for (const line of buffer.toString("utf8").split("\n")) {
      if (!line) continue;
      let entry: { type?: string; message?: Record<string, unknown> };
      try { entry = JSON.parse(line) as typeof entry; } catch { continue; }
      const message = entry.message ?? entry as Record<string, unknown>;
      if (message.role === "assistant" && Array.isArray(message.content)) {
        for (const part of message.content as Array<{ type?: unknown; id?: unknown }>) {
          if (part.type === "toolCall" && typeof part.id === "string") parentToolCallIds.push(part.id);
        }
      }
      const details = message.details as { progress?: Array<{ id?: unknown }> } | undefined;
      if (message.role !== "toolResult" || !Array.isArray(details?.progress) || !details.progress.some((item) => item.id === agentId)) continue;
      const toolCallId = message.toolCallId;
      if (typeof toolCallId === "string" && /^[A-Za-z0-9._:-]{1,256}$/.test(toolCallId)) matches.push(toolCallId);
    }
    const unique = [...new Set(matches)];
    return unique.length === 1 && parentToolCallIds.filter((id) => id === unique[0]).length === 1
      ? { runId: `omp:task:${agentId}`, toolCallId: unique[0] }
      : null;
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

export function deriveOmpSessionLineage(
  sessionId: string,
  parentSessionFile: string | undefined,
  forkedRoot = false,
  nativeSessionFile?: string,
  authoritativeRoot = false,
): { rootSessionId: string | null; parentSessionId: string | null } {
  const currentHeader = nativeSessionFile ? readNativeSessionHeader(nativeSessionFile) : null;
  if (currentHeader && currentHeader.id !== sessionId) {
    return { rootSessionId: null, parentSessionId: null };
  }
  if (forkedRoot) return { rootSessionId: sessionId, parentSessionId: null };
  if (!currentHeader) {
    return authoritativeRoot && !parentSessionFile
      ? { rootSessionId: sessionId, parentSessionId: null }
      : { rootSessionId: null, parentSessionId: null };
  }
  if (!currentHeader.parentSession) {
    return parentSessionFile
      ? { rootSessionId: null, parentSessionId: null }
      : { rootSessionId: sessionId, parentSessionId: null };
  }
  if (!parentSessionFile || !sameNativePath(currentHeader.parentSession, parentSessionFile)) {
    return { rootSessionId: null, parentSessionId: null };
  }
  const parent = readNativeSessionHeader(parentSessionFile);
  return {
    rootSessionId: readNativeSessionRootId(parentSessionFile),
    parentSessionId: parent?.id ?? null,
  };
}

function sameNativePath(left: string, right: string): boolean {
  try { return realpathSync(left) === realpathSync(right); } catch { return left === right; }
}

export function deriveOmpIdentity(
  options: OmpCaptureContext,
  streamId?: string,
  dependencies: AgentMetadata["depends_on"] = [],
): MetadataIdentity | null {
  const sessionId = options.sessionId;
  if (typeof sessionId !== "string" || !/^[A-Za-z0-9._:-]{1,256}$/.test(sessionId)) return null;
  let previousResponseId: string | null = options.nativeSequence?.previousResponseId ?? null;
  let hasPriorHistory = options.nativeSequence?.hasPriorHistory ?? false;
  if (!options.nativeSequence) {
  for (let index = (options.messages?.length ?? 0) - 1; index >= 0; index -= 1) {
    const message = options.messages?.[index];
    if (message?.role !== "assistant") continue;
    hasPriorHistory = true;
    previousResponseId = typeof message.responseId === "string" ? message.responseId : null;
    break;
  }
  if (previousResponseId && (options.messages ?? []).filter((message) => message.role === "assistant" && message.responseId === previousResponseId).length > 1) {
    previousResponseId = null;
  }
  }
  return {
    harness: "omp",
    harnessVersion: "18.12.6",
    extensionVersion: OMP_EXTENSION_VERSION,
    runnerVersion: OMP_RUNNER_VERSION,
    sessionId,
    rootSessionId: options.nativeRootSessionId !== undefined
      ? options.nativeRootSessionId
      : options.isKnownRoot
        ? sessionId
        : null,
    parentSessionId: options.nativeParentSessionId ?? null,
    previousResponseId,
    hasPriorHistory,
    delegation: options.delegation ?? null,
    dependsOn: dependencies,
  };
}

export function createOmpEnvelope(
  options: OmpCaptureContext,
  streamId: string,
  dependencies: AgentMetadata["depends_on"] = [],
): string | null {
  const identity = deriveOmpIdentity(options, streamId, dependencies);
  if (!identity) return null;
  return encodeAgentMetadata(identity, streamId);
}
