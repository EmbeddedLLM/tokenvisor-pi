import { encodeAgentMetadata, EXTENSION_VERSION, type AgentMetadata, type MetadataIdentity } from "./shared.ts";
import { readNativeSessionHeader, readNativeSessionRootId } from "./native-session.ts";
import { closeSync, fstatSync, openSync, readSync, realpathSync } from "fs";
import { dirname, join, basename } from "path";
import { isDeepStrictEqual } from "util";

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
  delegation?: MetadataIdentity["delegation"];
  nativeParentSessionId?: string;
  nativeRootSessionId?: string | null;
  parentSessionId?: string;
  messages?: MessageLike[];
  nativeSequence?: { previousResponseId: string | null; hasPriorHistory: boolean };
}

export interface OmpSequenceStream {
  anchorId: string | null;
  responseId: string | null;
  streamSucceeded: boolean;
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
        // Request identity can be recovered only when native and provider
        // terminal outcomes agree. An observed ID on an abort is sequence proof,
        // not successful output consumption.
        const failed = entry.stopReason === "error" || entry.stopReason === "aborted" ||
          (typeof entry.errorMessage === "string" && entry.errorMessage.length > 0);
        const streamFailed = !stream.streamSucceeded;
        state.responseByUsageId.set(entry.id, failed === streamFailed ? stream.responseId : null);
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
  state.pending.set(streamId, { anchorId, responseId: null, streamSucceeded: false, terminal: false });
}

export function recordOmpSequenceResult(
  state: OmpSequenceState,
  streamId: string,
  responseId: string | null,
  streamSucceeded = responseId !== null,
): void {
  const stream = state.pending.get(streamId);
  if (!stream) return;
  stream.responseId = responseId;
  stream.streamSucceeded = streamSucceeded;
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
  pendingTaskBindingRetirements: Map<string, Map<string, OmpPendingTaskBindingRetirement>>;
  compactionTaskResultReferences: Map<string, OmpTaskResultReference[]>;
  taskResultReferencesByStream: Map<string, OmpTaskResultStreamReferences>;
  earlyTaskBindings: Map<string, OmpEarlyTaskBinding | OmpEarlyTaskBindingConflict>;
}

export interface OmpCompactionFrontierAssessment {
  sourceProvenMechanicalCompaction: boolean;
  hasUnresolvedInputs: boolean;
}

/**
 * Assess an OMP compaction once, at the committed compaction hook. This keeps
 * native method proof separate from pending child/peer output; callers may
 * use mechanical proof to carry an empty frontier only when both are clear.
 */
export function assessOmpCompactionFrontier(
  state: OmpRuntimeCaptureState,
  sessionId: string,
  event: unknown,
): OmpCompactionFrontierAssessment {
  const branchReader = state.branchReaders.get(sessionId);
  if (!branchReader) return { sourceProvenMechanicalCompaction: false, hasUnresolvedInputs: true };

  let branch: Array<Record<string, any>>;
  try {
    branch = branchReader();
  } catch {
    return { sourceProvenMechanicalCompaction: false, hasUnresolvedInputs: true };
  }
  if (!Array.isArray(branch) || !event || typeof event !== "object") {
    return { sourceProvenMechanicalCompaction: false, hasUnresolvedInputs: true };
  }

  const payload = event as Record<string, any>;
  const committed = payload.compactionEntry;
  if (!committed || typeof committed !== "object" || committed.type !== "compaction" ||
      !isNativeId(committed.id) || !isNativeId(committed.firstKeptEntryId)) {
    return { sourceProvenMechanicalCompaction: false, hasUnresolvedInputs: true };
  }
  const compactions = branch.filter((entry) => entry.type === "compaction");
  const matchingCommits = compactions.filter((entry) => entry.id === committed.id);
  const latestCommit = compactions.at(-1);
  if (matchingCommits.length !== 1 || latestCommit !== matchingCommits[0] ||
      latestCommit?.firstKeptEntryId !== committed.firstKeptEntryId ||
      latestCommit?.method !== committed.method ||
      latestCommit?.fromExtension !== committed.fromExtension) {
    return { sourceProvenMechanicalCompaction: false, hasUnresolvedInputs: true };
  }

  const sourceProvenMechanicalCompaction = payload.fromExtension === false &&
    committed.fromExtension === false && committed.method === "snapcompact";
  const hasUnresolvedInputs = hasPendingOmpCompactionInputs(state, sessionId, branch);
  return { sourceProvenMechanicalCompaction, hasUnresolvedInputs };
}

function hasPendingOmpCompactionInputs(
  state: OmpRuntimeCaptureState,
  sessionId: string,
  branch: Array<Record<string, any>>,
): boolean {
  if ((state.pendingTaskResultConsumptions.get(sessionId)?.size ?? 0) > 0 ||
      (state.pendingTaskBindingRetirements.get(sessionId)?.size ?? 0) > 0 ||
      (state.compactionTaskResultReferences.get(sessionId)?.length ?? 0) > 0) return true;

  for (const binding of state.earlyTaskBindings.values()) {
    if (binding.parentSessionId === sessionId) return true;
  }

  const consumed = readOmpConsumedTaskResultReferences(branch, sessionId);
  if (!consumed) return true;
  let latestAssistantIndex = -1;
  for (let index = 0; index < branch.length; index += 1) {
    const entry = branch[index]!;
    const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
    if (message.role === "assistant" && isNativeId(message.responseId)) latestAssistantIndex = index;
  }
  const pendingDeliverySuffix = branch.slice(latestAssistantIndex + 1);

  // Automatic async-result messages lack response-specific sources. Only a
  // result after the latest identified assistant response remains pending.
  for (const entry of pendingDeliverySuffix) {
    const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
    if (message.customType === "async-result" || entry.customType === "async-result" || entry.type === "custom_message" &&
        entry.customType === "async-result") return true;
  }

  for (const entry of pendingDeliverySuffix) {
    const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
    const details = message.details ?? entry.details;
    if ((message.role !== "toolResult" && message.role !== "tool_result") ||
        details?.op !== "wait" && details?.op !== "cancel") continue;
    const hasCompletedTaskOutput = Array.isArray(details?.jobs) && details.jobs.some((job: any) =>
      job?.type === "task" && job.status === "completed" &&
      (Object.hasOwn(job, "resultText") || Object.hasOwn(job, "structured")),
    );
    if (!hasCompletedTaskOutput) continue;
    if (!isNativeId(entry.id) || !consumed.deliveryIds.has(entry.id)) return true;
  }

  return hasUnresolvedOmpPeerWake(branch);
}

export interface OmpEarlyTaskBindingConflict {
  parentSessionId: string;
  agentId: string;
  toolCallIds: string[];
  conflict: true;
}

export interface OmpPendingTaskBindingRetirement {
  streamId: string;
  bindingIds: string[];
}

export interface OmpEarlyTaskBinding {
  parentSessionId: string;
  parentSessionFile: string;
  rootSessionId: string;
  agentId: string;
  toolCallId: string;
  childSessionFile: string;
  childSessionId: string;
  detached: boolean;
}

const MAX_OMP_EARLY_TASK_BINDINGS_PER_PARENT = 64;

function earlyTaskBindingKey(parentSessionId: string, agentId: string): string {
  return JSON.stringify([parentSessionId, agentId]);
}

/** Capture OMP's started event before its first child provider call, retaining only verified identity. */
export function registerOmpTaskLifecycleStarted(
  state: OmpRuntimeCaptureState,
  parentSessionFile: string | undefined,
  parentSessionId: string,
  event: unknown,
): boolean {
  if (!parentSessionFile || !isNativeId(parentSessionId) || !event || typeof event !== "object") return false;
  const payload = event as Record<string, unknown>;
  if (payload.status !== "started" || !isNativeId(payload.id) || !isNativeId(payload.parentToolCallId) ||
      typeof payload.sessionFile !== "string" || typeof payload.detached !== "boolean" || payload.detached) return false;

  let canonicalParentFile: string;
  let canonicalChildFile: string;
  try {
    canonicalParentFile = realpathSync(parentSessionFile);
    canonicalChildFile = realpathSync(payload.sessionFile);
  } catch {
    return false;
  }
  const parentHeader = readNativeSessionHeader(canonicalParentFile);
  const childHeader = readNativeSessionHeader(canonicalChildFile);
  const rootSessionId = readNativeSessionRootId(canonicalParentFile);
  if (parentHeader?.id !== parentSessionId || !childHeader?.parentSession ||
      childHeader.parentSession !== canonicalParentFile || !isNativeId(childHeader.id) ||
      readNativeSessionRootId(canonicalChildFile) !== rootSessionId || !rootSessionId) return false;

  const key = earlyTaskBindingKey(parentSessionId, payload.id);
  const existing = state.earlyTaskBindings.get(key);
  const binding: OmpEarlyTaskBinding = {
    parentSessionId,
    parentSessionFile: canonicalParentFile,
    rootSessionId,
    agentId: payload.id,
    toolCallId: payload.parentToolCallId,
    childSessionFile: canonicalChildFile,
    childSessionId: childHeader.id,
    detached: payload.detached,
  };
  if (existing !== undefined) {
    if ("conflict" in existing || !isDeepStrictEqual(existing, binding)) {
      const toolCallIds = "conflict" in existing
        ? [...existing.toolCallIds, payload.parentToolCallId]
        : [existing.toolCallId, payload.parentToolCallId];
      state.earlyTaskBindings.set(key, {
        parentSessionId,
        agentId: payload.id,
        toolCallIds: [...new Set(toolCallIds)],
        conflict: true,
      });
      return false;
    }
    return true;
  }
  let parentBindingCount = 0;
  for (const value of state.earlyTaskBindings.values()) {
    if (value.parentSessionId === parentSessionId) parentBindingCount += 1;
  }
  if (parentBindingCount >= MAX_OMP_EARLY_TASK_BINDINGS_PER_PARENT) return false;
  state.earlyTaskBindings.set(key, binding);
  return true;
}

/** Resolve the started event against a child provider's native session, never its display name. */
export function readOmpEarlyTaskDelegation(
  state: OmpRuntimeCaptureState,
  childAgentId: string | undefined,
  childSessionFile: string | undefined,
): (MetadataIdentity["delegation"] & { detached: false }) | null {
  if (!isNativeId(childAgentId) || !childSessionFile) return null;
  let canonicalChildFile: string;
  try { canonicalChildFile = realpathSync(childSessionFile); } catch { return null; }
  const childHeader = readNativeSessionHeader(canonicalChildFile);
  if (!childHeader?.parentSession) return null;
  let canonicalParentFile: string;
  try { canonicalParentFile = realpathSync(childHeader.parentSession); } catch { return null; }
  const parentHeader = readNativeSessionHeader(canonicalParentFile);
  if (!parentHeader) return null;
  const binding = state.earlyTaskBindings.get(earlyTaskBindingKey(parentHeader.id, childAgentId));
  if (!binding || "conflict" in binding || binding.detached || binding.agentId !== childAgentId ||
      binding.childSessionFile !== canonicalChildFile || binding.childSessionId !== childHeader.id ||
      binding.parentSessionFile !== canonicalParentFile || binding.parentSessionId !== parentHeader.id ||
      binding.rootSessionId !== readNativeSessionRootId(canonicalChildFile) ||
      binding.rootSessionId !== readNativeSessionRootId(canonicalParentFile) ||
      childHeader.parentSession !== canonicalParentFile) return null;
  return { runId: `omp:task:${binding.agentId}`, toolCallId: binding.toolCallId, detached: false };
}

/** Drop launch bindings owned by the session when its parent lifecycle ends. */
export function clearOmpEarlyTaskBindingsForSession(
  state: OmpRuntimeCaptureState,
  sessionId: string,
): void {
  for (const [key, binding] of state.earlyTaskBindings) {
    if (binding.parentSessionId === sessionId) state.earlyTaskBindings.delete(key);
  }
}

/** Retire one binding only after the parent's result-consumption marker commits. */
export function retireOmpEarlyTaskBinding(
  state: OmpRuntimeCaptureState,
  parentSessionId: string,
  agentId: string,
): void {
  if (!isNativeId(parentSessionId) || !isNativeId(agentId)) return;
  state.earlyTaskBindings.delete(earlyTaskBindingKey(parentSessionId, agentId));
}

/** Queue verified launch IDs until the matching parent response is committed. */
export function queueOmpTaskBindingRetirement(
  state: OmpRuntimeCaptureState,
  sessionId: string,
  responseId: string,
  streamId: string,
  bindingIds: string[],
): boolean {
  if (!isNativeId(sessionId) || !isNativeId(responseId) || !isNativeId(streamId) || bindingIds.length === 0 ||
      bindingIds.some((bindingId) => !isNativeId(bindingId))) return false;
  const current = state.pendingTaskBindingRetirements.get(sessionId) ?? new Map();
  const next = { streamId, bindingIds: [...new Set(bindingIds)] };
  const existing = current.get(responseId);
  if (existing) return isDeepStrictEqual(existing, next);
  if (current.size >= 64) return false;
  current.set(responseId, next);
  state.pendingTaskBindingRetirements.set(sessionId, current);
  return true;
}

export function clearOmpTaskBindingRetirementsForSession(
  state: OmpRuntimeCaptureState,
  sessionId: string,
): void {
  state.pendingTaskBindingRetirements.delete(sessionId);
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

interface OmpNativeAssistantMessage {
  responseId: string | null;
  stopReason?: string;
  content?: unknown[];
}

interface OmpChildSessionEvidence {
  sessionId: string;
  completions: OmpTaskResultCompletion[];
  assistantMessages: OmpNativeAssistantMessage[];
  latestAssistantResponseId: string | null;
  toolResults: Array<Record<string, any>>;
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
    pendingTaskBindingRetirements: new Map(),
    compactionTaskResultReferences: new Map(),
    taskResultReferencesByStream: new Map(),
    earlyTaskBindings: new Map(),
  } satisfies OmpRuntimeCaptureState;
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
  const deliveryCandidates: Array<{
    jobId: string;
    agentUrlId: string;
    deliveryId: string;
    resultText?: unknown;
    structured?: unknown;
  }> = [];
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
        (Object.hasOwn(candidateJob, "resultText") || Object.hasOwn(candidateJob, "structured")) &&
        candidateJob?.resultText === job.resultText && isDeepStrictEqual(candidateJob?.structured, job.structured),
      )) return null;
      deliveryCandidates.push({ jobId, agentUrlId, deliveryId, resultText: job.resultText, structured: job.structured });
    }
  }
  if (deliveryCandidates.length === 0) return { dependencies: [], references: [] };
  if (!parentSessionFile || readNativeSessionHeader(parentSessionFile)?.id !== parentSessionId) return null;
  const consumed = readOmpConsumedTaskResultReferences(branch, parentSessionId);
  if (!consumed) return null;
  const uniqueCandidates = new Map<string, {
    jobId: string;
    agentUrlId: string;
    deliveryId: string;
    resultText?: unknown;
    structured?: unknown;
  }>();
  for (const candidate of deliveryCandidates) {
    const key = JSON.stringify([candidate.deliveryId, candidate.jobId]);
    if (uniqueCandidates.has(key)) continue;
    if (consumed.deliveryIds.has(candidate.deliveryId)) continue;
    uniqueCandidates.set(key, candidate);
  }
  if (uniqueCandidates.size === 0) return { dependencies: [], references: [] };
  const dependencies: NonNullable<AgentMetadata["depends_on"]> = [];
  const references: OmpTaskResultReference[] = [];
  const usedReferences = new Set<string>();
  for (const { jobId, agentUrlId, deliveryId, resultText, structured } of uniqueCandidates.values()) {
    const runId = `omp:task:${agentUrlId}`;
    const taskCalls = [...new Set(pendingTaskCalls.get(agentUrlId) ?? [])];
    if (taskCalls.length !== 1 || calls.filter((id) => id === taskCalls[0]).length !== 1) return null;
    const childFile = join(dirname(parentSessionFile), basename(parentSessionFile, ".jsonl"), `${agentUrlId}.jsonl`);
    const child = readOmpChildSessionEvidence(childFile, parentSessionId, runId);
    if (!child) return null;
    const matchingResponseIds = readOmpTaskResultResponseIds(
      { agentUrlId, resultText, structured },
      child.assistantMessages,
      child.toolResults,
      child.completions.map((completion) => completion.responseId),
    );
    if (matchingResponseIds.length !== 1) return null;
    const completion = {
      sessionId: child.sessionId,
      responseId: matchingResponseIds[0],
      runId,
      deliveryId,
      jobId,
    };
    const completionKey = taskResultReferenceKey(completion);
    if (usedReferences.has(completionKey)) return null;
    usedReferences.add(completionKey);
    dependencies.push({ session_id: `omp:session:${completion.sessionId}`, response_id: completion.responseId, run_id: completion.runId });
    references.push(completion);
  }
  if (dependencies.length > 8) return null;
  return { dependencies, references };
}

function earlyTaskBindingsForToolCall(
  state: OmpRuntimeCaptureState,
  parentSessionId: string,
  toolCallId: string,
): Array<OmpEarlyTaskBinding | OmpEarlyTaskBindingConflict> {
  return [...state.earlyTaskBindings.values()].filter((binding) =>
    binding.parentSessionId === parentSessionId &&
    ("conflict" in binding ? binding.toolCallIds.includes(toolCallId) : binding.toolCallId === toolCallId),
  );
}

/** Resolve only a successful blocking task result whose ordinary tool output is in this request. */
export function readOmpBlockingTaskResultDependencies(
  state: OmpRuntimeCaptureState,
  parentSessionFile: string | undefined,
  parentSessionId: string,
  branch: Array<Record<string, any>>,
  effectiveMessages: Array<Record<string, any>>,
): {
  dependencies: AgentMetadata["depends_on"];
  references: OmpTaskResultReference[];
  bindingIds: string[];
} | null {
  let latestAssistantIndex = -1;
  const toolCalls = new Map<string, Array<{ responseId: string | null; name: unknown; index: number }>>();
  for (let index = 0; index < branch.length; index += 1) {
    const raw = branch[index];
    const message = raw.message && typeof raw.message === "object" ? raw.message : raw;
    if (message.role !== "assistant") continue;
    latestAssistantIndex = index;
    for (const part of Array.isArray(message.content) ? message.content : []) {
      if ((part?.type !== "toolCall" && part?.type !== "tool_call") || !isNativeId(part.id)) continue;
      const matches = toolCalls.get(part.id) ?? [];
      matches.push({ responseId: isNativeId(message.responseId) ? message.responseId : null, name: part.name, index });
      toolCalls.set(part.id, matches);
    }
  }

  const hasCurrentTaskResult = branch.some((raw, index) => {
    if (index <= latestAssistantIndex) return false;
    const message = raw.message && typeof raw.message === "object" ? raw.message : raw;
    return message.role === "toolResult" && message.toolName === "task" &&
      isNativeId(message.toolCallId) && isNativeId(raw.id);
  });
  if (!hasCurrentTaskResult) return { dependencies: [], references: [], bindingIds: [] };

  if (!parentSessionFile || readNativeSessionHeader(parentSessionFile)?.id !== parentSessionId) return null;
  const consumed = readOmpConsumedTaskResultReferences(branch, parentSessionId);
  if (!consumed) return null;

  const dependencies: NonNullable<AgentMetadata["depends_on"]> = [];
  const references: OmpTaskResultReference[] = [];
  const bindingIds: string[] = [];
  let unavailable = false;
  for (let index = latestAssistantIndex + 1; index < branch.length; index += 1) {
    const raw = branch[index];
    const message = raw.message && typeof raw.message === "object" ? raw.message : raw;
    const details = message.details ?? raw.details;
    if (message.role !== "toolResult" || message.toolName !== "task" ||
        !isNativeId(message.toolCallId) || !isNativeId(raw.id)) continue;
    const knownBindings = earlyTaskBindingsForToolCall(state, parentSessionId, message.toolCallId);
    for (const known of knownBindings) {
      if (!bindingIds.includes(known.agentId)) bindingIds.push(known.agentId);
    }
    const effectiveMatches = effectiveMessages.filter((candidate) =>
      candidate.role === "toolResult" && candidate.toolName === "task" && candidate.toolCallId === message.toolCallId,
    );
    if (effectiveMatches.length === 0) continue;
    if (effectiveMatches.length !== 1) {
      unavailable = true;
      continue;
    }
    if (!isDeepStrictEqual(effectiveMatches[0].content, message.content)) continue;

    if (message.isError === true) continue;
    const results = Array.isArray(details?.results) ? details.results : [];
    if (results.length === 0) {
      if (knownBindings.length > 0) unavailable = true;
      continue;
    }
    const successfulResults = results.filter((result: any) =>
      result && result.exitCode === 0 && result.aborted !== true && result.error === undefined &&
      typeof result.output === "string" && result.output.length > 0,
    );
    if (successfulResults.length === 0) continue;
    if (consumed.deliveryIds.has(raw.id)) continue;
    const callMatches = toolCalls.get(message.toolCallId) ?? [];
    if (callMatches.length !== 1 || callMatches[0].name !== "task" || !callMatches[0].responseId ||
        callMatches[0].index >= index) {
      unavailable = true;
      continue;
    }
    if (knownBindings.length === 0) {
      unavailable = true;
      continue;
    }
    if (results.length !== 1 || knownBindings.length !== 1 || "conflict" in knownBindings[0]) {
      unavailable = true;
      continue;
    }
    const result = results[0];
    if (!result || !isNativeId(result.id)) {
      unavailable = true;
      continue;
    }
    const binding = state.earlyTaskBindings.get(earlyTaskBindingKey(parentSessionId, result.id));
    if (!binding || "conflict" in binding || binding.detached || binding.toolCallId !== message.toolCallId) {
      unavailable = true;
      continue;
    }
    if (binding.agentId !== knownBindings[0].agentId || binding.toolCallId !== message.toolCallId) {
      unavailable = true;
      continue;
    }
    const earlyDelegation = readOmpEarlyTaskDelegation(state, result.id, binding.childSessionFile);
    if (!earlyDelegation || earlyDelegation.toolCallId !== message.toolCallId) {
      unavailable = true;
      continue;
    }
    if (result.truncated === true) {
      unavailable = true;
      continue;
    }
    if (typeof result.output !== "string" || result.output.length === 0) continue;
    const outputTexts = Array.isArray(message.content)
      ? message.content.flatMap((part: any) => part?.type === "text" && typeof part.text === "string" ? [part.text] : [])
      : [];
    if (outputTexts.length === 0) continue;
    if (outputTexts.length !== 1) {
      unavailable = true;
      continue;
    }
    if (!outputTexts[0].startsWith(`<task-result`)) continue;
    if (!outputTexts[0].startsWith(`<task-result id="${result.id}" `)) {
      unavailable = true;
      continue;
    }
    const formattedOutput = taskResultOutputSlot(outputTexts[0], result.id);
    if (formattedOutput === null || formatOmpPlainTaskResultOutput(formattedOutput) !== formatOmpPlainTaskResultOutput(result.output)) {
      unavailable = true;
      continue;
    }

    const child = readOmpChildSessionEvidence(binding.childSessionFile, parentSessionId, earlyDelegation.runId);
    if (!child || child.sessionId !== binding.childSessionId) {
      unavailable = true;
      continue;
    }
    const responseIds = readOmpTaskResultResponseIds(
      { agentUrlId: result.id, resultText: outputTexts[0], structured: result.structuredOutput },
      child.assistantMessages,
      child.toolResults,
      child.completions.map((completion) => completion.responseId),
    );
    if (responseIds.length !== 1) {
      unavailable = true;
      continue;
    }
    const responseId = responseIds[0];
    const responseMatches = child.assistantMessages.filter((assistant) => assistant.responseId === responseId);
    if (responseMatches.length !== 1) {
      unavailable = true;
      continue;
    }
    const yieldResponseIds = result.structuredOutput !== undefined
      ? readOmpFinalYieldResponseIds(result.structuredOutput, child.assistantMessages, child.toolResults)
      : readOmpRenderedYieldResponseIds(formattedOutput, child.assistantMessages, child.toolResults);
    const isExactYieldResponse = yieldResponseIds !== null && yieldResponseIds.includes(responseId);
    if (yieldResponseIds === null || (!isExactYieldResponse &&
        (responseId !== child.latestAssistantResponseId || !child.completions.some((completion) => completion.responseId === responseId)))) {
      unavailable = true;
      continue;
    }
    const reference: OmpTaskResultReference = {
      sessionId: child.sessionId,
      responseId,
      runId: earlyDelegation.runId,
      deliveryId: raw.id,
      jobId: result.id,
    };
    if (consumed.references.has(taskResultReferenceKey(reference))) continue;
    if (references.some((previous) => taskResultReferenceKey(previous) === taskResultReferenceKey(reference))) {
      unavailable = true;
      continue;
    }
    dependencies.push({
      session_id: `omp:session:${child.sessionId}`,
      response_id: responseId,
      run_id: earlyDelegation.runId,
    });
    references.push(reference);
  }
  if (dependencies.length > 8) return { dependencies: null, references: [], bindingIds };
  return { dependencies: unavailable ? null : dependencies, references, bindingIds };
}

function taskResultOutputSlot(resultText: string, agentUrlId: string): string | null {
  if (!resultText.startsWith(`<task-result id="${agentUrlId}" `)) return null;
  const open = "\n<output>\n";
  const close = "\n</output>\n</task-result>";
  const start = resultText.indexOf(open);
  const end = resultText.lastIndexOf(close);
  if (start < 0 || end < start || resultText.indexOf(open, start + open.length) >= 0 ||
    resultText.lastIndexOf(close) !== resultText.indexOf(close)) return null;
  const envelope = `${resultText.slice(0, start)}${resultText.slice(end + close.length)}`;
  if (envelope.includes("<preview ") || envelope.includes("<meta ") || envelope.includes("<error>") ||
    envelope.includes("<abort-reason>") || envelope.includes("<merge-summary>")) return null;
  return resultText.slice(start + open.length, end);
}

/** Mirror the output-relevant part of OMP's pinned `prompt.format()` behavior. */
function formatOmpPlainTaskResultOutput(value: string): string {
  const escaped = value.replace(/<(?=\s*\/?\s*(?:irc|system-[a-z][a-z-]*)(?![\w-]))/gi, "&lt;").trim();
  const rawLines = escaped.split("\n");
  const lines: string[] = [];
  let inCodeBlock = false;
  for (let index = 0; index < rawLines.length; index += 1) {
    let line = rawLines[index].trimEnd();
    const indent = line.length - line.trimStart().length;
    const trimmed = line.slice(indent);
    const isFence = trimmed.startsWith("```") || trimmed.startsWith("~~~");
    if (isFence) {
      inCodeBlock = !inCodeBlock;
      lines.push(line);
      continue;
    }
    if (inCodeBlock) {
      lines.push(line);
      continue;
    }
    if (trimmed.startsWith("|") && trimmed.endsWith("|")) {
      if (/^\|[-:\s|]+\|$/.test(trimmed)) {
        const cells = trimmed.split("|").filter((cell) => cell.trim());
        line = `${line.slice(0, indent)}|${cells.map((cell) => {
          const value = cell.trim();
          const left = value.startsWith(":");
          const right = value.endsWith(":");
          return left && right ? ":---:" : left ? ":---" : right ? "---:" : "---";
        }).join("|")}|`;
      } else {
        line = `${line.slice(0, indent)}${trimmed.split("|").map((cell) => cell.trim()).join("|")}`;
      }
    }
    if (!line) {
      const next = rawLines[index + 1];
      if (next === undefined || next.length === 0 || !/\S/.test(next)) {
        while (lines.length > 0 && lines[lines.length - 1] === "") lines.pop();
        while (index + 1 < rawLines.length && !/\S/.test(rawLines[index + 1])) index += 1;
        continue;
      }
      if (lines.length === 0 || lines[lines.length - 1] === "") continue;
    }
    if (trimmed.startsWith("</")) {
      while (lines.length > 0 && lines[lines.length - 1] === "") lines.pop();
    }
    lines.push(line);
  }
  while (lines.length > 0 && lines[lines.length - 1] === "") lines.pop();
  return lines.join("\n");
}

function assistantText(message: OmpNativeAssistantMessage): string | null {
  if (message.stopReason !== "stop" || !Array.isArray(message.content)) return null;
  if (message.content.some((part: any) => part?.type === "toolCall" || part?.type === "tool_call")) return null;
  return message.content
    .flatMap((part: any) => part?.type === "text" && typeof part.text === "string" ? [part.text] : [])
    .join("\n");
}

function matchOmpExactAssistantText(
  expected: string,
  assistantMessages: OmpNativeAssistantMessage[],
  normalize: (value: string) => string,
): OmpNativeAssistantMessage[] {
  const matches: OmpNativeAssistantMessage[] = [];
  for (const message of assistantMessages) {
    const text = assistantText(message);
    if (text === null) continue;
    if (normalize(text) === expected) matches.push(message);
  }
  return matches;
}

function yieldCallersForMatchingData(
  matches: Array<{ message: Record<string, any>; details: Record<string, any> }>,
  assistantMessages: OmpNativeAssistantMessage[],
): string[] | null {
  if (matches.length !== 1) return matches.length === 0 ? [] : null;
  const { message } = matches[0];
  if (!isNativeId(message.toolCallId)) return null;
  const callers = assistantMessages.filter((assistant) => Array.isArray(assistant.content) &&
    assistant.content.some((part: any) =>
      (part?.type === "toolCall" || part?.type === "tool_call") && part.name === "yield" && part.id === message.toolCallId,
    ));
  return callers.length === 1 && callers[0].responseId ? [callers[0].responseId] : null;
}

function matchingSuccessfulYieldResults(
  assistantMessages: OmpNativeAssistantMessage[],
  toolResults: Array<Record<string, any>>,
  matchesData: (data: unknown) => boolean,
): string[] | null {
  const matches: Array<{ message: Record<string, any>; details: Record<string, any> }> = [];
  for (const message of toolResults) {
    const details = message.details;
    if (message.toolName !== "yield" || !details || details.status !== "success" ||
      details.data === undefined || details.data === null || !matchesData(details.data)) continue;
    if (details.type !== undefined || details.useLastTurn === true || details.schemaOverridden === true) return null;
    matches.push({ message, details });
  }
  return yieldCallersForMatchingData(matches, assistantMessages);
}

function hasUnsupportedSuccessfulYield(toolResults: Array<Record<string, any>>): boolean {
  return toolResults.some((message) => {
    const details = message.details;
    return message.toolName === "yield" && details?.status === "success" &&
      (details.type !== undefined || details.useLastTurn === true || details.schemaOverridden === true);
  });
}

function readOmpFinalYieldResponseIds(
  structured: Record<string, any>,
  assistantMessages: OmpNativeAssistantMessage[],
  toolResults: Array<Record<string, any>>,
): string[] | null {
  if (structured.status !== "valid" || !Object.hasOwn(structured, "data")) return [];
  return matchingSuccessfulYieldResults(
    assistantMessages,
    toolResults,
    (data) => isDeepStrictEqual(structured.data, data),
  );
}

function readOmpRenderedYieldResponseIds(
  output: string,
  assistantMessages: OmpNativeAssistantMessage[],
  toolResults: Array<Record<string, any>>,
): string[] | null {
  return matchingSuccessfulYieldResults(assistantMessages, toolResults, (data) => {
    const rendered = typeof data === "string" ? data : JSON.stringify(data, null, 2);
    return rendered !== undefined && formatOmpPlainTaskResultOutput(rendered) === output;
  });
}

function readOmpTaskResultResponseIds(
  job: { agentUrlId: string; resultText?: unknown; structured?: unknown },
  assistantMessages: OmpNativeAssistantMessage[],
  toolResults: Array<Record<string, any>>,
  completedResponseIds: string[],
): string[] {
  // Qualify only source-reconstructable plain outputs and one untyped final yield;
  // incremental/schema-normalized outputs and automatic deliveries remain unknown.
  const completed = new Set(completedResponseIds);
  const structured = job.structured;
  if (structured !== undefined) {
    if (!structured || typeof structured !== "object" || !Object.hasOwn(structured, "data")) return [];
    return readOmpFinalYieldResponseIds(structured as Record<string, any>, assistantMessages, toolResults) ?? [];
  }
  if (typeof job.resultText !== "string") return [];
  const output = taskResultOutputSlot(job.resultText, job.agentUrlId);
  if (output !== null) {
    // OMP switches to a preview at 5,000 characters when a full output artifact exists.
    // Larger unmarked bodies may also be tail-truncated if artifact creation failed.
    if (output.length > 5000) return [];
    // Advanced yield modes can render a value that is not reconstructable from details.data.
    if (hasUnsupportedSuccessfulYield(toolResults)) return [];
    const plainMatches = matchOmpExactAssistantText(output, assistantMessages, formatOmpPlainTaskResultOutput);
    const yieldMatches = readOmpRenderedYieldResponseIds(output, assistantMessages, toolResults);
    if (yieldMatches === null) return [];
    const candidates = [...plainMatches.map((message) => ({
      responseId: message.responseId,
      completed: message.responseId !== null && completed.has(message.responseId),
    })), ...yieldMatches.map((responseId) => ({ responseId, completed: true }))];
    return candidates.length === 1 && candidates[0].responseId && candidates[0].completed
      ? [candidates[0].responseId]
      : [];
  } else {
    if (job.resultText.startsWith("<task-result")) return [];
    const matches = matchOmpExactAssistantText(job.resultText, assistantMessages, (text) => text.trim());
    return matches.length === 1 && matches[0].responseId && completed.has(matches[0].responseId)
      ? [matches[0].responseId]
      : [];
  }
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

function readOmpChildSessionEvidence(
  childSessionFile: string,
  parentSessionId: string,
  runId: string,
): OmpChildSessionEvidence | null {
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
    const seenCompletions = new Set<string>();
    const assistantMessages: OmpNativeAssistantMessage[] = [];
    let latestAssistantResponseId: string | null = null;
    const toolResults: Array<Record<string, any>> = [];
    for (const line of buffer.toString("utf8").split("\n")) {
      if (!line) continue;
      let entry: Record<string, any>;
      try { entry = JSON.parse(line) as Record<string, any>; } catch { return null; }
      const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
      if (message.role === "assistant") {
        latestAssistantResponseId = isNativeId(message.responseId) ? message.responseId : null;
        assistantMessages.push({
          responseId: isNativeId(message.responseId) ? message.responseId : null,
          stopReason: typeof message.stopReason === "string" ? message.stopReason : undefined,
          content: Array.isArray(message.content) ? message.content : undefined,
        });
      } else if (message.role === "toolResult" || message.role === "tool_result") {
        toolResults.push(message);
      }
      if (entry.customType !== OMP_RUN_COMPLETION_ENTRY) continue;
      const data = entry.data;
      if (!data || data.version !== 1 || !isNativeId(data.runId) || !isNativeId(data.parentSessionId) ||
        !isNativeId(data.sessionId) || !isNativeId(data.responseId)) return null;
      if (data.runId !== runId || data.parentSessionId !== parentSessionId || data.sessionId !== header.id) continue;
      const completion = { sessionId: header.id, responseId: data.responseId, runId };
      const key = taskResultReferenceKey(completion);
      if (!seenCompletions.has(key)) completions.push(completion);
      seenCompletions.add(key);
    }
    return { sessionId: header.id, completions, assistantMessages, latestAssistantResponseId, toolResults };
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
  dependencies: AgentMetadata["depends_on"] = [],
  harnessVersion = "unknown",
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
    harnessVersion,
    extensionVersion: EXTENSION_VERSION,
    runnerVersion: harnessVersion,
    sessionId,
    rootSessionId: options.nativeRootSessionId ?? null,
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
  harnessVersion = "unknown",
): string | null {
  const identity = deriveOmpIdentity(options, dependencies, harnessVersion);
  if (!identity) return null;
  return encodeAgentMetadata(identity, streamId);
}
