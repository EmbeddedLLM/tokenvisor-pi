import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { createHash } from "crypto";
import { closeSync, fstatSync, lstatSync, openSync, readFileSync, readSync, realpathSync } from "fs";
import { homedir, tmpdir, userInfo } from "os";
import { basename, dirname, join, relative, resolve } from "path";
import { readNativeSessionHeader, readNativeSessionRootId } from "./native-session.ts";
import {
  appendPendingChildLaunches,
  isChildLaunchReceipt,
  MAX_CHILD_LAUNCHES_PER_ENVELOPE,
  MAX_PENDING_CHILD_LAUNCHES,
  type AgentMetadata,
  type ChildLaunchReceipt,
  type MetadataIdentity,
} from "./shared.ts";

export const PI_EXTENSION_VERSION = "1.1.0-dev";
export const PI_COMPACTION_ENTRY = "tokenvisor-agent-compaction-v1";
export const PI_FORK_ROOT_ENTRY = "tokenvisor-agent-fork-root-v1";
export const PI_RUN_COMPLETION_ENTRY = "tokenvisor-agent-run-completion-v1";
export const PI_RESULT_CONSUMPTION_ENTRY = "tokenvisor-agent-result-consumption-v1";
export const PI_LAUNCH_RECEIPT_PENDING_ENTRY = "tokenvisor-agent-launch-receipts-v1";
export const PI_LAUNCH_RECEIPT_CONSUMPTION_ENTRY = "tokenvisor-agent-launch-receipt-consumption-v1";
export const PI_FOREGROUND_DELIVERY_ENTRY = "tokenvisor-agent-foreground-delivery-v1";

interface SessionEntryLike {
  id?: string;
  type?: string;
  customType?: string;
  role?: string;
  isError?: unknown;
  content?: unknown;
  toolCallId?: unknown;
  data?: unknown;
  summary?: unknown;
  parentId?: string | null;
  responseId?: string;
  details?: unknown;
  message?: { role?: string; responseId?: string; stopReason?: string; content?: unknown };
}

interface PiCompactionRecord {
  compactionEntryId: string;
  contributors: Array<{ stream_id: string }>;
  consumedRunIds?: string[];
  consumedDependencies?: PiDependencyRef[];
  pendingChildLaunches?: ChildLaunchReceipt[];
  available?: boolean;
  provenNone?: boolean;
}

type PiDependencyRef = NonNullable<AgentMetadata["depends_on"]>[number];

export interface PiDependencyDeliveryWitness {
  dependency: PiDependencyRef;
  deliveredText: string;
  toolCallId?: string;
  deliveredOutputSha256?: string;
}

export interface PiCompactionRenderer {
  convertToLlm(messages: unknown[]): unknown[];
  serializeConversation(messages: unknown[]): string;
}

export function resolvePiCompactionRequestInput(
  preparation: any,
  requestMessages: unknown,
  renderer: PiCompactionRenderer | null,
): unknown[] | null {
  if (!renderer || !preparation || !Array.isArray(requestMessages) || requestMessages.length !== 1) return null;
  const request = requestMessages[0];
  if (!request || typeof request !== "object" || (request as { role?: unknown }).role !== "user") return null;
  const prompt = readPiSingleTextContent((request as { content?: unknown }).content);
  if (prompt === null) return null;

  const candidates: Array<{ kind: "history" | "turn_prefix"; messages: unknown[] }> = [];
  if (Array.isArray(preparation.messagesToSummarize) && preparation.messagesToSummarize.length > 0) {
    candidates.push({ kind: "history", messages: preparation.messagesToSummarize });
  }
  if (preparation.isSplitTurn === true && Array.isArray(preparation.turnPrefixMessages) && preparation.turnPrefixMessages.length > 0) {
    candidates.push({ kind: "turn_prefix", messages: preparation.turnPrefixMessages });
  }

  const matches = candidates.flatMap((candidate) => {
    try {
      const converted = renderer.convertToLlm(candidate.messages);
      const conversation = renderer.serializeConversation(converted);
      const prefix = candidate.kind === "history"
        ? `<conversation>\n${conversation}\n</conversation>\n\n`
        : `# Conversation\n${conversation}\n\n# Instructions\n`;
      return conversation.length > 0 && prompt.startsWith(prefix) ? [{ candidate, converted, prefix }] : [];
    } catch {
      return [];
    }
  });
  if (matches.length !== 1) return null;

  const { candidate, converted, prefix } = matches[0]!;
  const sourceInput = [...converted];
  if (candidate.kind === "history" && typeof preparation.previousSummary === "string" && preparation.previousSummary.length > 0) {
    const previousSummaryFrame = `<previous-summary>\n${preparation.previousSummary}\n</previous-summary>\n\n`;
    if (!prompt.startsWith(`${prefix}${previousSummaryFrame}`)) return null;
    sourceInput.push({
      role: "user",
      content: [{ type: "text", text: previousSummaryFrame.trimEnd() }],
    });
  }
  return sourceInput;
}

interface PiResultConsumptionRecord {
  version: 1;
  stream_id: string;
  response_id: string;
  dependencies: PiDependencyRef[];
}

interface PiLaunchReceiptPendingRecord {
  version: 1;
  receipts: ChildLaunchReceipt[];
}

interface PiLaunchReceiptConsumptionRecord {
  version: 1;
  stream_id: string;
  response_id: string;
  receipts: ChildLaunchReceipt[];
}

interface PiForegroundDeliveryRecord {
  version: 1;
  receipt: ChildLaunchReceipt;
  delivered_output_sha256: string;
}

function childLaunchKey(receipt: ChildLaunchReceipt): string {
  return JSON.stringify([
    receipt.parent_response_id,
    receipt.tool_call_id,
    receipt.child_session_id,
    receipt.run_id,
    receipt.last_response_id,
  ]);
}

function validPiLaunchReceiptPendingRecord(data: PiLaunchReceiptPendingRecord | null): data is PiLaunchReceiptPendingRecord {
  return !!data && data.version === 1 && Array.isArray(data.receipts) && data.receipts.length > 0 &&
    data.receipts.length <= MAX_PENDING_CHILD_LAUNCHES && data.receipts.every(isChildLaunchReceipt);
}

function validPiLaunchReceiptConsumptionRecord(data: PiLaunchReceiptConsumptionRecord | null): data is PiLaunchReceiptConsumptionRecord {
  return !!data && data.version === 1 && isNativeId(data.stream_id) && isNativeId(data.response_id) &&
    Array.isArray(data.receipts) && data.receipts.length > 0 &&
    data.receipts.length <= MAX_CHILD_LAUNCHES_PER_ENVELOPE && data.receipts.every(isChildLaunchReceipt) &&
    new Set(data.receipts.map(childLaunchKey)).size === data.receipts.length;
}

function validPiForegroundDeliveryRecord(data: PiForegroundDeliveryRecord | null): data is PiForegroundDeliveryRecord {
  return !!data && data.version === 1 && isChildLaunchReceipt(data.receipt) &&
    typeof data.delivered_output_sha256 === "string" && /^[0-9a-f]{64}$/.test(data.delivered_output_sha256);
}

interface PiSubagentStatus {
  lifecycleArtifactVersion?: unknown;
  runId?: unknown;
  toolCallId?: unknown;
  pid?: unknown;
  sessionId?: unknown;
  state?: unknown;
  mode?: unknown;
  sessionFile?: unknown;
  artifactsDir?: unknown;
  sessionDir?: unknown;
  steps?: Array<{ sessionFile?: unknown; agent?: unknown; exitCode?: unknown }>;
  parentWorkflowRunId?: unknown;
  workflowKey?: unknown;
  workflowChildren?: {
    version?: unknown;
    workflowRunId?: unknown;
    parentToolCallId?: unknown;
    inventoryComplete?: unknown;
    workflowState?: unknown;
    children?: Array<{ childId?: unknown; runId?: unknown; state?: unknown }>;
  };
}

interface NativeCompactionLike {
  type?: string;
  id?: string;
  summary?: unknown;
  firstKeptEntryId?: string;
  fromHook?: boolean;
}

export interface PiRunCompletion {
  runId: string;
  sessionId: string;
  responseId: string;
}

export interface CompactionFrontier {
  available: boolean;
  streamIds: string[];
  provenNone: boolean;
}

/** Replace the covered frontier, or carry it through a source-proven mechanical compaction. */
export function resolveCompactionFrontier(
  previous: readonly string[] | null,
  current: readonly string[],
  failed: boolean,
  sourceProvenMechanicalCompaction: boolean,
): CompactionFrontier {
  const observed = [...new Set(current)];
  const provenMechanical = sourceProvenMechanicalCompaction && observed.length === 0 && !failed;
  if (previous === null || failed) return { available: false, streamIds: observed, provenNone: false };
  const frontier = provenMechanical ? [...new Set(previous)] : observed;
  if (!provenMechanical && observed.length === 0) return { available: false, streamIds: [], provenNone: false };
  if (frontier.length > 8) return { available: false, streamIds: frontier, provenNone: false };
  return { available: true, streamIds: frontier, provenNone: provenMechanical && frontier.length === 0 };
}

function isNativeId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 256 && /^[A-Za-z0-9._:-]+$/.test(value);
}

/**
 * Keep dependency references only when their exact source-rendered text is a
 * complete text block in the request-local Pi context.
 */
export function filterPiDependenciesForRequestInput(
  dependencies: AgentMetadata["depends_on"],
  witnesses: readonly PiDependencyDeliveryWitness[],
  requestMessages: unknown,
  selectedNativeSourceInput = false,
): AgentMetadata["depends_on"] {
  if (dependencies === null) return null;
  if (dependencies.length === 0) return [];
  if (!Array.isArray(requestMessages)) return null;

  const requestText = requestMessages.flatMap((message) => {
    if (!message || typeof message !== "object") return [];
    const role = (message as { role?: unknown }).role;
    if (role !== "user" && role !== "toolResult" && role !== "custom") return [];
    const content = (message as { content?: unknown }).content;
    if (typeof content === "string") return [content];
    if (!Array.isArray(content)) return [];
    return content.flatMap((block) =>
      block && typeof block === "object" && (block as { type?: unknown }).type === "text" &&
        typeof (block as { text?: unknown }).text === "string"
        ? [(block as { text: string }).text]
        : [],
    );
  });

  const witnessesByDependency = new Map<string, string[]>();
  for (const witness of witnesses) {
    if (!witness || !isValidPiDependencyRef(witness.dependency) ||
        typeof witness.deliveredText !== "string" || witness.deliveredText.length === 0 ||
        witness.toolCallId !== undefined && !isNativeId(witness.toolCallId) ||
        witness.deliveredOutputSha256 !== undefined && !/^[0-9a-f]{64}$/.test(witness.deliveredOutputSha256) ||
        (witness.toolCallId === undefined) !== (witness.deliveredOutputSha256 === undefined)) return null;
    const key = piDependencyKey(witness.dependency);
    const values = witnessesByDependency.get(key) ?? [];
    values.push(witness.deliveredText);
    witnessesByDependency.set(key, values);
  }

  const selected: PiDependencyRef[] = [];
  for (const dependency of dependencies) {
    const deliveredTexts = witnessesByDependency.get(piDependencyKey(dependency));
    if (!deliveredTexts?.length) return null;
    const dependencyWitnesses = witnesses.filter((witness) => piDependencyKey(witness.dependency) === piDependencyKey(dependency));
    const foregroundWitnesses = dependencyWitnesses.filter((witness) => witness.toolCallId !== undefined);
    const delivered = foregroundWitnesses.length > 0
      ? foregroundWitnesses.some((witness) => requestMessages.some((message) => {
        if (!message || typeof message !== "object") return false;
        const candidate = message as { role?: unknown; toolCallId?: unknown; content?: unknown };
        if (candidate.role !== "toolResult" || candidate.toolCallId !== witness.toolCallId) return false;
        const text = readPiSingleTextContent(candidate.content);
        return text !== null && text === witness.deliveredText &&
          createHash("sha256").update(text, "utf8").digest("hex") === witness.deliveredOutputSha256;
      }))
      : deliveredTexts.some((deliveredText) => requestText.some((text) => text === deliveredText));
    if (delivered) {
      selected.push({ ...dependency });
    } else if (foregroundWitnesses.length === 0 && !selectedNativeSourceInput) {
      return null;
    }
  }
  return selected;
}

function resolvePiSubagentsTempRoot(env: NodeJS.ProcessEnv = process.env): string {
  const configured = env.PI_SUBAGENTS_TEMP_ROOT?.trim();
  if (configured) return resolve(configured);
  const sanitize = (value: string) => value.trim().replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "unknown";
  if (typeof process.getuid === "function") return join(tmpdir(), `pi-subagents-uid-${process.getuid()}`);
  for (const name of ["USERNAME", "USER", "LOGNAME"]) {
    if (env[name]) return join(tmpdir(), `pi-subagents-user-${sanitize(env[name]!)}`);
  }
  try {
    const username = userInfo().username;
    if (username) return join(tmpdir(), `pi-subagents-user-${sanitize(username)}`);
  } catch {
    // Fall through to the home directory identity used by pi-subagents.
  }
  const home = env.USERPROFILE ?? env.HOME;
  if (home) return join(tmpdir(), `pi-subagents-home-${sanitize(home)}`);
  try {
    const fallbackHome = homedir();
    if (fallbackHome) return join(tmpdir(), `pi-subagents-home-${sanitize(fallbackHome)}`);
  } catch {
    // Fall through to the shared scope used by pi-subagents.
  }
  return join(tmpdir(), "pi-subagents-shared");
}

/**
 * Resolve the active pi-subagents execution from its durable status artifact.
 * The exact session file and runner PID are both required; directory names and
 * timestamps are not used to attribute a child to a run.
 */
export function readPiSubagentDelegation(
  sessionFile: string | undefined,
  parentSessionId: string | null,
  phase: "active" | "terminal" = "active",
  parentBranch?: SessionEntryLike[] | null,
): { runId: string; toolCallId: string } | null {
  const runnerPid = process.env.PI_SUBAGENT_RUNNER_PID;
  const runId = resolvePiSubagentRunnerRunId(process.argv, process.env, process.pid);
  if (!sessionFile || !parentSessionId || !runnerPid || !/^\d+$/.test(runnerPid) || !runId) return null;
  const childHeader = readNativeSessionHeader(sessionFile);
  const parentSessionFile = childHeader?.parentSession;
  if (!childHeader || !parentSessionFile || readNativeSessionHeader(parentSessionFile)?.id !== parentSessionId) return null;
  let canonicalSessionFile: string;
  let canonicalParentFile: string;
  try {
    canonicalSessionFile = realpathSync(sessionFile);
    canonicalParentFile = realpathSync(parentSessionFile);
  } catch { return null; }
  const status = readPiStatusFile(join(resolvePiSubagentsTempRoot(), "async-subagent-runs", runId, "status.json"));
  const stateIsUsable = status?.state === "running" || phase === "terminal" && status?.state === "complete";
  if (!status || status.lifecycleArtifactVersion !== 3 || status.runId !== runId || !stateIsUsable || status.mode !== "single" || !Array.isArray(status.steps) || status.steps.length !== 1 || String(status.pid) !== runnerPid) return null;
  if (!statusParentMatches(status.sessionId, parentSessionId, canonicalParentFile)) return null;
  const statusFiles = resolvePiStatusSessionFiles(status, parentSessionId, canonicalParentFile);
  if (!statusFiles) return null;
  const exactChildFile = statusFiles.has(canonicalSessionFile);
  if (!exactChildFile && !(statusFiles.size === 0 && status.sessionFile === undefined && status.steps?.[0]?.sessionFile == null)) return null;
  if (status.toolCallId !== undefined && !isNativeId(status.toolCallId)) return null;
  const toolCallId = readPiDelegationSource(status, runId, parentSessionId, canonicalParentFile, parentBranch);
  if (!toolCallId) return null;
  // The current runner PID plus native parent/child headers binds startup before
  // pi-subagents publishes the step session path.
  return { runId, toolCallId };
}

function readPiDelegationSource(
  status: PiSubagentStatus,
  runId: string,
  parentSessionId: string,
  parentFile: string,
  parentBranch?: SessionEntryLike[] | null,
): string | null {
  if (status.parentWorkflowRunId === undefined || status.parentWorkflowRunId === null) {
    if (parentBranch?.length) return readPiParentToolCallIdFromBranch(parentBranch, runId, isNativeId(status.toolCallId) ? status.toolCallId : undefined);
    return readPiParentToolCallId(parentFile, parentSessionId, runId, isNativeId(status.toolCallId) ? status.toolCallId : undefined);
  }
  if (!isPiEncodedIndexSegment(status.parentWorkflowRunId) || !isNativeId(status.workflowKey)) return null;
  const branchCallId = parentBranch ? readPiWorkflowToolCallIdFromBranch(parentBranch, status.parentWorkflowRunId) : null;
  const nativeCallId = readPiNativeWorkflowToolCallId(parentFile, parentSessionId, status.parentWorkflowRunId);
  const workflowStatusPath = join(resolvePiSubagentsTempRoot(), "async-subagent-runs", status.parentWorkflowRunId, "status.json");
  let workflowStatusExists = false;
  try {
    lstatSync(workflowStatusPath);
    workflowStatusExists = true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") return null;
  }
  const workflow = readPiStatusFile(workflowStatusPath);
  if (branchCallId && nativeCallId && branchCallId !== nativeCallId) return null;
  if (!workflow) return workflowStatusExists ? null : branchCallId ?? nativeCallId;
  if (workflow.mode !== "workflow" || workflow.runId !== status.parentWorkflowRunId ||
      !statusParentMatches(workflow.sessionId, parentSessionId, parentFile) || !isNativeId(workflow.toolCallId)) return null;
  if (workflow?.toolCallId && (branchCallId && branchCallId !== workflow.toolCallId ||
      nativeCallId && nativeCallId !== workflow.toolCallId)) return null;
  if (branchCallId) return branchCallId;
  if (nativeCallId) return nativeCallId;
  // Foreground workflows can start their awaited child before producing a
  // workflow status artifact; their exact encoded native call is sufficient.
  if (!workflow || !isNativeId(workflow.toolCallId)) return null;
  if (parentBranch?.length) return readPiWorkflowToolCallIdFromBranch(parentBranch, status.parentWorkflowRunId, workflow.toolCallId);
  return readPiNativeWorkflowToolCallIdByRun(parentFile, parentSessionId, status.parentWorkflowRunId, workflow.toolCallId);
}

function encodePiSubagentIndexSegment(value: string): string {
  const hashed = () => `~sha256-${createHash("sha256").update(value).digest("hex")}`;
  let encoded: string;
  try { encoded = encodeURIComponent(value); } catch { return hashed(); }
  const reservedName = /^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i;
  const portable = encoded.length > 0 && encoded !== "." && encoded !== ".." && !encoded.endsWith(".") &&
    !reservedName.test(encoded) && !/%5C/i.test(encoded) && !/\.[A-Za-z][A-Za-z0-9]{0,7}$/.test(encoded);
  return Buffer.byteLength(encoded, "utf8") <= 255 && portable ? encoded : hashed();
}

function isPiEncodedIndexSegment(value: unknown): value is string {
  if (typeof value !== "string" || !value || value.length > 255 || value === "." || value === ".." || value.endsWith(".") ||
      !/^[A-Za-z0-9._~:%-]+$/.test(value)) return false;
  if (/^~sha256-[a-f0-9]{64}$/.test(value)) return true;
  try { return encodePiSubagentIndexSegment(decodeURIComponent(value)) === value; }
  catch { return false; }
}

function readPiNativeWorkflowToolCallId(parentSessionFile: string, parentSessionId: string, encodedRunId: string): string | null {
  if (!isPiEncodedIndexSegment(encodedRunId) || readNativeSessionHeader(parentSessionFile)?.id !== parentSessionId) return null;
  let fd: number | undefined;
  try {
    fd = openSync(parentSessionFile, "r");
    const stat = fstatSync(fd);
    if (stat.size <= 0 || stat.size > 8 * 1024 * 1024) return null;
    const buffer = Buffer.alloc(stat.size);
    readSync(fd, buffer, 0, buffer.length, 0);
    const matches: string[] = [];
    for (const line of buffer.toString("utf8").split("\n")) {
      if (!line) continue;
      let entry: SessionEntryLike;
      try { entry = JSON.parse(line) as SessionEntryLike; } catch { return null; }
      const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
      if (message.role !== "assistant" || !Array.isArray(message.content)) continue;
      for (const part of message.content as Array<{ type?: unknown; id?: unknown; name?: unknown; arguments?: unknown }>) {
        if ((part.type !== "toolCall" && part.type !== "tool_call") || part.name !== "subagent" || !isNativeId(part.id) || encodePiSubagentIndexSegment(part.id) !== encodedRunId) continue;
        const args = part.arguments && typeof part.arguments === "object" ? part.arguments as Record<string, unknown> : null;
        const workflow = args?.workflow;
        const optionScript = args?.options && typeof args.options === "object" ? (args.options as Record<string, unknown>).script : undefined;
        const isScript = workflow === true || typeof workflow === "string" && workflow.trim().length > 0 ||
          typeof optionScript === "string" && optionScript.trim().length > 0;
        matches.push(isScript ? part.id : "");
      }
    }
    return matches.length === 1 && matches[0] ? matches[0] : null;
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

function readPiNativeWorkflowToolCallIdByRun(
  parentSessionFile: string,
  parentSessionId: string,
  workflowRunId: string,
  preferredToolCallId: string,
): string | null {
  if (!isNativeId(workflowRunId) || !isNativeId(preferredToolCallId) ||
      readNativeSessionHeader(parentSessionFile)?.id !== parentSessionId) return null;
  let fd: number | undefined;
  try {
    fd = openSync(parentSessionFile, "r");
    const stat = fstatSync(fd);
    if (stat.size <= 0 || stat.size > 8 * 1024 * 1024) return null;
    const buffer = Buffer.alloc(stat.size);
    readSync(fd, buffer, 0, buffer.length, 0);
    const resultToolCallIds: string[] = [];
    const workflowCallIds: string[] = [];
    for (const line of buffer.toString("utf8").split("\n")) {
      if (!line) continue;
      const entry = JSON.parse(line) as SessionEntryLike;
      const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
      const details = message.details as { runId?: unknown } | undefined;
      if (message.role === "toolResult" && details?.runId === workflowRunId && isNativeId(message.toolCallId)) {
        resultToolCallIds.push(message.toolCallId);
      }
      if (message.role !== "assistant" || !Array.isArray(message.content)) continue;
      for (const part of message.content as Array<{ type?: unknown; name?: unknown; id?: unknown; arguments?: unknown }>) {
        if ((part.type !== "toolCall" && part.type !== "tool_call") || part.name !== "subagent" || part.id !== preferredToolCallId) continue;
        const args = part.arguments && typeof part.arguments === "object" ? part.arguments as Record<string, unknown> : null;
        const options = args?.options && typeof args.options === "object" ? args.options as Record<string, unknown> : null;
        if (args?.workflow === true || typeof args?.workflow === "string" && args.workflow.trim().length > 0 ||
            typeof options?.script === "string" && options.script.trim().length > 0) workflowCallIds.push(preferredToolCallId);
      }
    }
    const uniqueResultIds = [...new Set(resultToolCallIds)];
    return uniqueResultIds.length <= 1 && (!uniqueResultIds.length || uniqueResultIds[0] === preferredToolCallId) && workflowCallIds.length === 1
      ? preferredToolCallId
      : null;
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

export function readPiParentToolCallId(
  parentSessionFile: string,
  parentSessionId: string,
  runId: string,
  preferredToolCallId?: string,
): string | null {
  if (!isNativeId(runId) || readNativeSessionHeader(parentSessionFile)?.id !== parentSessionId) return null;
  let fd: number | undefined;
  try {
    fd = openSync(parentSessionFile, "r");
    const stat = fstatSync(fd);
    if (stat.size <= 0 || stat.size > 8 * 1024 * 1024) return null;
    const buffer = Buffer.alloc(stat.size);
    if (readSync(fd, buffer, 0, buffer.length, 0) !== buffer.length) return null;
    const branch = buffer.toString("utf8").split("\n").filter(Boolean)
      .map((line) => JSON.parse(line) as SessionEntryLike);
    return readPiParentToolCallIdFromBranch(branch, runId, preferredToolCallId);
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

function readPiParentToolCallIdFromBranch(
  branch: SessionEntryLike[],
  runId: string,
  preferredToolCallId?: string,
): string | null {
  if (!isNativeId(runId)) return null;
  const calls = new Map<string, { count: number; observation: boolean }>();
  for (const entry of branch) {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    if (message.role !== "assistant" || !Array.isArray(message.content)) continue;
    for (const part of message.content as Array<{ type?: unknown; id?: unknown; name?: unknown; arguments?: { action?: unknown } }>) {
      if ((part.type !== "toolCall" && part.type !== "tool_call") || part.name !== "subagent" || !isNativeId(part.id)) continue;
      calls.set(part.id, {
        count: (calls.get(part.id)?.count ?? 0) + 1,
        observation: part.arguments?.action === "status" || part.arguments?.action === "debug.run",
      });
    }
  }
  const results: string[] = [];
  for (const entry of branch) {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    const details = message.details as { runId?: unknown } | undefined;
    if (message.role !== "toolResult" || details?.runId !== runId || !isNativeId(message.toolCallId)) continue;
    const call = calls.get(message.toolCallId);
    // Status snapshots name the run they inspect; their own call is not its launch.
    // Resume and unfamiliar actions remain launch candidates, retaining conflict checks.
    if (call?.count === 1 && call.observation) continue;
    results.push(message.toolCallId);
  }
  if (results.length > 1) return null;
  const candidate = preferredToolCallId ?? results[0];
  if (!candidate || !isNativeId(candidate) || results.length === 1 && results[0] !== candidate) return null;
  const source = calls.get(candidate);
  return source?.count === 1 && !source.observation ? candidate : null;
}

/** Resolve the pinned runner's config suffix, used only as a locator for its status artifact. */
export function resolvePiSubagentRunnerRunId(
  argv: string[],
  env: NodeJS.ProcessEnv = process.env,
  currentPid = process.pid,
): string | null {
  if (env.PI_SUBAGENT_CHILD !== "1" || env.PI_SUBAGENT_RUNNER_PID !== String(currentPid)) return null;
  const runnerEntry = argv[1];
  const configPath = argv[2];
  if (!runnerEntry || !configPath || !isAbsolutePath(configPath)) return null;
  let canonicalRunner: string;
  try { canonicalRunner = realpathSync(runnerEntry); } catch { return null; }
  let packageRoot: string | null = null;
  let candidate = dirname(canonicalRunner);
  for (let depth = 0; depth < 8; depth += 1) {
    try {
      const manifest = JSON.parse(readFileSync(join(candidate, "package.json"), "utf8")) as { name?: unknown };
      if (manifest.name === "pi-subagents") {
        packageRoot = candidate;
        break;
      }
    } catch {
      // Continue up the exact runner entry path.
    }
    const parent = dirname(candidate);
    if (parent === candidate) break;
    candidate = parent;
  }
  if (!packageRoot) return null;
  const entryRelativePath = relative(packageRoot, canonicalRunner).replaceAll("\\", "/");
  if (!/^src\/runs\/background\/subagent-runner-bootstrap\.(?:js|ts)$/.test(entryRelativePath)) return null;
  const tempRoot = resolvePiSubagentsTempRoot(env);
  if (resolve(dirname(configPath)) !== tempRoot) return null;
  const match = /^async-cfg-([A-Za-z0-9._:-]{1,256})\.json$/.exec(basename(configPath));
  return match?.[1] ?? null;
}

function isAbsolutePath(value: string): boolean {
  return value.startsWith("/") || /^[A-Za-z]:[\\/]/.test(value);
}

function statusParentMatches(statusSessionId: unknown, parentSessionId: string, canonicalParentFile: string): boolean {
  if (isNativeId(statusSessionId)) {
    return statusSessionId === parentSessionId && readNativeSessionHeader(canonicalParentFile)?.id === parentSessionId;
  }
  if (typeof statusSessionId !== "string") return false;
  try {
    const statusParentFile = realpathSync(statusSessionId);
    return statusParentFile === canonicalParentFile && readNativeSessionHeader(statusParentFile)?.id === parentSessionId;
  } catch {
    return false;
  }
}

function resolvePiStatusSessionFiles(
  status: PiSubagentStatus,
  parentSessionId?: string,
  parentSessionFile?: string,
): Map<string, { id: string; parentSession: string }> | null {
  const files = new Map<string, { id: string; parentSession: string }>();
  if (status.steps !== undefined && status.steps !== null &&
      (!Array.isArray(status.steps) || status.steps.some((step) => !step || typeof step !== "object"))) return null;
  const aliases = [status.sessionFile, ...(status.steps ?? []).map((step) => step.sessionFile)];
  for (const alias of aliases) {
    if (alias === undefined || alias === null) continue;
    if (typeof alias !== "string") return null;
    let canonical: string;
    try {
      canonical = realpathSync(alias);
    } catch {
      // A missing optional alias does not invalidate another usable alias.
      continue;
    }
    const header = readNativeSessionHeader(canonical);
    if (!header) return null;
    if (!header.parentSession) return null;
    let canonicalParent: string;
    try { canonicalParent = realpathSync(header.parentSession); }
    catch { return null; }
    const parentHeader = readNativeSessionHeader(canonicalParent);
    if (!parentHeader || parentSessionId !== undefined && parentHeader.id !== parentSessionId) return null;
    if (parentSessionFile !== undefined) {
      try { if (canonicalParent !== realpathSync(parentSessionFile)) return null; }
      catch { return null; }
    }
    files.set(canonical, { id: header.id, parentSession: canonicalParent });
  }
  return files.size <= 1 ? files : null;
}

function readPiStatusFile(path: string): PiSubagentStatus | null {
  try {
    const fd = openSync(path, "r");
    try {
      const stat = fstatSync(fd);
      if (stat.size <= 0 || stat.size > 256 * 1024) return null;
      const buffer = Buffer.alloc(stat.size);
      readSync(fd, buffer, 0, buffer.length, 0);
      return JSON.parse(buffer.toString("utf8")) as PiSubagentStatus;
    } finally {
      closeSync(fd);
    }
  } catch {
    return null;
  }
}

function readPiSubagentStatus(runId: string, parentSessionId?: string, parentSessionFile?: string): PiSubagentStatus | null {
  if (!isNativeId(runId)) return null;
  const status = readPiStatusFile(join(resolvePiSubagentsTempRoot(), "async-subagent-runs", runId, "status.json"));
  if (!status || status.lifecycleArtifactVersion !== 3 || status.runId !== runId) return null;
  if (parentSessionId) {
    if (!parentSessionFile) return null;
    let canonicalParentFile: string;
    try { canonicalParentFile = realpathSync(parentSessionFile); } catch { return null; }
    if (!statusParentMatches(status.sessionId, parentSessionId, canonicalParentFile)) return null;
  }
  return status;
}

function readPiInlineWorkflowResult(
  entry: SessionEntryLike,
  parentFile: string | undefined,
  branchBeforeEntry: SessionEntryLike[],
  allowStatusReturnProof = true,
): { toolCallId: string; workflowRunId: string; children: Array<{ childId?: unknown; runId?: unknown; state?: unknown }>; outputs: Map<string, PiRenderedChildOutput>; statusOnly: boolean; deliveryText: string } | null | undefined {
  const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
  if (message.role !== "toolResult") return undefined;
  const details = message.details && typeof message.details === "object" ? message.details as Record<string, unknown> : null;
  if (details?.mode !== "workflow" && details?.workflowChildren === undefined) {
    if (details?.mode !== "single" || !isNativeId(message.toolCallId)) return undefined;
    const statusTarget = readPiWorkflowStatusTargetInBranch(branchBeforeEntry, message.toolCallId);
    if (!statusTarget) return undefined;
    const content = readPiSingleTextContent(message.content);
    if (content === null) return null;
    if (readPiStatusReturnContent(content) !== "none") return null;
    return { toolCallId: message.toolCallId, workflowRunId: statusTarget, children: [], outputs: new Map(), statusOnly: true, deliveryText: content };
  }
  const inventory = details?.workflowChildren as PiSubagentStatus["workflowChildren"] | undefined;
  // The async launch response is an acknowledgement, not a delivered child output.
  if (details?.mode === "workflow" && inventory?.workflowState === "running") return undefined;
  if (!parentFile || !isNativeId(message.toolCallId) || inventory?.version !== 1 ||
      !isNativeId(inventory.parentToolCallId) || !isNativeId(inventory.workflowRunId) ||
      details?.runId !== inventory.workflowRunId || inventory.inventoryComplete !== true ||
      !["completed", "failed", "stopped"].includes(String(inventory.workflowState)) ||
      !Array.isArray(inventory.children) || inventory.children.length > 256 ||
      !isPiWorkflowToolCallInBranch(branchBeforeEntry, inventory.workflowRunId, inventory.parentToolCallId)) return null;
  const isStatusResult = details?.mode === "single";
  if (isStatusResult) {
    if (!isPiWorkflowStatusToolCallInBranch(branchBeforeEntry, message.toolCallId, inventory.workflowRunId)) return null;
  } else if (details?.mode !== "workflow" || inventory.parentToolCallId !== message.toolCallId) return null;
  const children = inventory.children;
  if (inventory.workflowState !== "completed" && children.length === 0) return null;
  if (children.some((child) => !child || !isNativeId(child.childId) || child.runId !== undefined && !isNativeId(child.runId) ||
      !["completed", "failed", "paused", "stopped", "rejected", "detached"].includes(String(child.state)))) return null;
  const childIds = children.map((child) => child.childId);
  const runIds = children.flatMap((child) => child.runId === undefined ? [] : [child.runId]);
  if (new Set(childIds).size !== childIds.length || new Set(runIds).size !== runIds.length) return null;
  const content = readPiSingleTextContent(message.content);
  if (content === null) return null;
  const outputs = readPiRenderedChildOutputs(content) ??
    (!isStatusResult && details?.mode === "workflow"
      ? readPiWorkflowReturnOutputs(content, children, (details.workflow as { value?: unknown } | undefined)?.value)
      : null);
  if (isStatusResult && !outputs) {
    const returnValue = readPiStatusReturnString(content);
    if (returnValue === undefined) return null;
    if (returnValue !== null) {
      if (!allowStatusReturnProof) return null;
      const parentSessionId = readNativeSessionHeader(parentFile)?.id;
      if (!parentSessionId) return null;
      const redundant = readPiPreviouslyDeliveredWorkflowStatusReturn(
        returnValue,
        inventory.workflowRunId,
        children,
        branchBeforeEntry,
        parentSessionId,
        parentFile,
      );
      if (!redundant) return null;
    }
    return { toolCallId: inventory.parentToolCallId, workflowRunId: inventory.workflowRunId, children, outputs: new Map(), statusOnly: true, deliveryText: content };
  }
  if (!outputs) return null;
  return { toolCallId: inventory.parentToolCallId, workflowRunId: inventory.workflowRunId, children, outputs, statusOnly: false, deliveryText: content };
}

export function readPiSingleTextContent(content: unknown): string | null {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return null;
  const textBlocks = content.filter((block) => block && typeof block === "object" &&
    (block as { type?: unknown }).type === "text");
  if (textBlocks.length !== content.length || textBlocks.length !== 1 ||
      typeof (textBlocks[0] as { text?: unknown } | undefined)?.text !== "string") return null;
  return (textBlocks[0] as { text: string }).text;
}

function isPiWorkflowStatusToolCallInBranch(branch: SessionEntryLike[], toolCallId: string, workflowRunId: string): boolean {
  return readPiWorkflowStatusTargetInBranch(branch, toolCallId) === workflowRunId;
}

function readPiWorkflowStatusTargetInBranch(branch: SessionEntryLike[], toolCallId: string): string | null {
  const matches: string[] = [];
  for (const entry of branch) {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    if (message.role !== "assistant" || !Array.isArray(message.content)) continue;
    for (const part of message.content as Array<{ type?: unknown; name?: unknown; id?: unknown; arguments?: unknown }>) {
      if ((part.type !== "toolCall" && part.type !== "tool_call") || part.name !== "subagent" || part.id !== toolCallId) continue;
      const args = part.arguments && typeof part.arguments === "object" ? part.arguments as Record<string, unknown> : null;
      if (args?.action === "status" && isNativeId(args.id)) matches.push(args.id);
    }
  }
  return matches.length === 1 ? matches[0]! : null;
}

interface PiRenderedChildOutput {
  status: string;
  preview: string | null;
  output?: string;
}

function readPiRenderedChildOutputs(content: string): Map<string, PiRenderedChildOutput> | null {
  const lines = content.split("\n");
  const headers = lines.flatMap((line, index) => line === "Child outputs:" ? [index] : []);
  if (headers.length !== 1) return null;
  const outputBlocks = new Map<string, PiRenderedChildOutput>();
  for (let index = headers[0]! + 1; index < lines.length; index += 1) {
    const match = lines[index]!.match(/^- key=([A-Za-z0-9._:-]+) run=([A-Za-z0-9._:-]+) status=([A-Za-z0-9._:-]+)$/);
    if (!match) {
      if (lines[index]!.startsWith("- key=")) return null;
      continue;
    }
    const key = `${match[1]}:${match[2]}`;
    if (outputBlocks.has(key)) return null;
    let preview: string | null = null;
    let unavailable = false;
    for (let cursor = index + 1; cursor < lines.length && !lines[cursor]!.startsWith("- key="); cursor += 1) {
      if (lines[cursor] === "  Preview:") {
        if (preview !== null || unavailable) return null;
        const previewLines: string[] = [];
        for (let previewCursor = cursor + 1; previewCursor < lines.length && lines[previewCursor]!.startsWith("    | "); previewCursor += 1) {
          previewLines.push(lines[previewCursor]!.slice("    | ".length));
        }
        if (previewLines.length === 0) return null;
        preview = previewLines.join("\n");
      } else if (lines[cursor]!.startsWith("  Preview: unavailable (")) {
        if (preview !== null || unavailable) return null;
        unavailable = true;
      }
    }
    outputBlocks.set(key, { status: match[3]!, preview });
  }
  return outputBlocks;
}

function readPiWorkflowReturnOutputs(
  content: string,
  children: Array<{ childId?: unknown; runId?: unknown; state?: unknown }>,
  producerValue: unknown,
): Map<string, PiRenderedChildOutput> | null {
  const lines = content.split("\n");
  const returnHeaders = lines.flatMap((line, index) => line === "Return:" ? [index] : []);
  const emittedHeaders = lines.flatMap((line, index) => line === "Emitted:" ? [index] : []);
  const consoleHeaders = lines.flatMap((line, index) => line === "Console:" ? [index] : []);
  const traceHeaders = lines.flatMap((line, index) => line === "Call trace:" ? [index] : []);
  if (returnHeaders.length !== 1 || emittedHeaders.length > 1 || consoleHeaders.length > 1 || traceHeaders.length !== 1) return null;
  const returnIndex = returnHeaders[0]!;
  const traceIndex = traceHeaders[0]!;
  if (traceIndex <= returnIndex + 1 || emittedHeaders[0] !== undefined && emittedHeaders[0] <= returnIndex ||
      consoleHeaders[0] !== undefined && consoleHeaders[0] <= returnIndex ||
      emittedHeaders[0] !== undefined && consoleHeaders[0] !== undefined && emittedHeaders[0] > consoleHeaders[0] ||
      consoleHeaders[0] !== undefined && consoleHeaders[0] > traceIndex ||
      emittedHeaders[0] !== undefined && emittedHeaders[0] > traceIndex) return null;
  const sectionIndex = Math.min(traceIndex, emittedHeaders[0] ?? Number.POSITIVE_INFINITY, consoleHeaders[0] ?? Number.POSITIVE_INFINITY);
  if (sectionIndex <= returnIndex + 1 || lines[sectionIndex - 1] !== "") return null;
  const serialized = lines.slice(returnIndex + 1, sectionIndex - 1).join("\n");
  if (serialized !== JSON.stringify(producerValue, null, 2)) return null;
  let value: unknown;
  try { value = JSON.parse(serialized); } catch { return null; }
  const returnedChildren = Array.isArray(value)
    ? value
    : value && typeof value === "object" && !Array.isArray(value) &&
      (value as { ok?: unknown }).ok === true && Array.isArray((value as { children?: unknown }).children)
      ? (value as { children: unknown[] }).children
      : null;
  if (!returnedChildren || returnedChildren.length !== children.length || returnedChildren.length > 256) return null;

  const childrenByKey = new Map<string, { childId: string; runId: string; state: string }>();
  for (const child of children) {
    if (!isNativeId(child.childId) || !isNativeId(child.runId) ||
        (child.state !== "completed" && child.state !== "failed")) return null;
    childrenByKey.set(child.childId, { childId: child.childId, runId: child.runId, state: child.state });
  }
  if (childrenByKey.size !== children.length) return null;

  const outputs = new Map<string, PiRenderedChildOutput>();
  for (const item of returnedChildren) {
    if (!item || typeof item !== "object" || Array.isArray(item)) return null;
    const returned = item as { key?: unknown; runId?: unknown; ok?: unknown; output?: unknown };
    if (typeof returned.key !== "string" || (returned.runId !== undefined && !isNativeId(returned.runId)) ||
        returned.ok !== undefined && typeof returned.ok !== "boolean" ||
        typeof returned.output !== "string" || returned.output.length === 0 || returned.output.length > 32 * 1024) return null;
    const child = childrenByKey.get(returned.key);
    if (!child || returned.runId !== undefined && returned.runId !== child.runId ||
        returned.ok !== undefined && returned.ok !== (child.state === "completed")) return null;
    const key = `${child.childId}:${child.runId}`;
    if (outputs.has(key)) return null;
    outputs.set(key, { status: child.state, preview: null, output: returned.output });
  }
  return outputs.size === children.length ? outputs : null;
}

function readPiStatusReturnContent(content: string): "none" | "output" | "invalid" {
  const returnLines = content.split("\n").filter((line) => line.startsWith("Return: "));
  if (returnLines.length === 0) return "none";
  if (returnLines.length !== 1) return "invalid";
  const serialized = returnLines[0]!.slice("Return: ".length);
  if (serialized.endsWith("…")) return "invalid";
  try {
    const value = JSON.parse(serialized) as unknown;
    if (value === null || value === "" || Array.isArray(value) && value.length === 0 ||
        !!value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0) return "none";
    return "output";
  } catch {
    return "invalid";
  }
}

function readPiStatusReturnString(content: string): string | null | undefined {
  const classification = readPiStatusReturnContent(content);
  if (classification === "none") return null;
  if (classification !== "output") return undefined;
  const line = content.split("\n").find((candidate) => candidate.startsWith("Return: "));
  if (!line) return undefined;
  try {
    const value = JSON.parse(line.slice("Return: ".length)) as unknown;
    return typeof value === "string" && value.length > 0 && value.length <= 32 * 1024 ? value : undefined;
  } catch {
    return undefined;
  }
}

function readPiPreviouslyDeliveredWorkflowStatusReturn(
  returnedText: string,
  workflowRunId: string,
  statusChildren: Array<{ childId?: unknown; runId?: unknown; state?: unknown }>,
  branchBeforeStatus: SessionEntryLike[],
  parentSessionId: string,
  parentSessionFile: string,
): NonNullable<AgentMetadata["depends_on"]> | null {
  const candidates: NonNullable<AgentMetadata["depends_on"]>[] = [];
  for (let index = 0; index < branchBeforeStatus.length; index += 1) {
    const entry = branchBeforeStatus[index]!;
    if (entry.customType !== "subagent-notify" || typeof entry.content !== "string" ||
        entry.content.split(returnedText).length !== 2) continue;
    const details = entry.details as { runs?: PiWorkflowNotificationRun[] } | undefined;
    if (!Array.isArray(details?.runs)) continue;
    for (const run of details.runs) {
      if (run?.workflowRunId !== workflowRunId || run.runId !== workflowRunId || !Array.isArray(run.childRuns)) continue;
      const aggregateChildren = run.childRuns.map((child) => ({
        childId: child.workflowKey,
        runId: child.runId,
        state: child.status,
      })).sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
      const returnedChildren = statusChildren.map((child) => ({
        childId: child.childId,
        runId: child.runId,
        state: child.state,
      })).sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
      if (JSON.stringify(aggregateChildren) !== JSON.stringify(returnedChildren)) continue;
      const delivery = readPiWorkflowNotificationDependencies(
        entry,
        run,
        branchBeforeStatus.slice(0, index),
        parentSessionId,
        parentSessionFile,
      );
      const consumedAfterDelivery = readPiPersistedDependencyKeys(branchBeforeStatus.slice(index + 1));
      if (!delivery || delivery.dependencies.length === 0 ||
          delivery.dependencies.some((dependency) => !consumedAfterDelivery.has(piDependencyKey(dependency)))) continue;
      candidates.push(delivery.dependencies);
    }
  }
  return candidates.length === 1 ? candidates[0]! : null;
}

interface PiIncrementalChildNotice {
  childKey: string;
  workflowRunId?: string;
  runId?: string;
}

/** Parse only the pinned runner's exact, labeled delivery fields. */
function readPiIncrementalChildNotice(entry: SessionEntryLike): PiIncrementalChildNotice | null {
  const details = entry.details as { childKey?: unknown; outcome?: unknown } | undefined;
  if (!isNativeId(details?.childKey) ||
      !["completed", "failed", "paused", "stopped"].includes(String(details?.outcome))) return null;
  const content = typeof entry.content === "string" ? entry.content : "";
  const lines = content.split("\n");
  const headline = lines[0]?.match(/^Workflow child (completed|failed|paused \(needs attention\)|stopped): \*\*([A-Za-z0-9._:-]+)\*\*$/);
  const headlineOutcome = headline?.[1] === "paused (needs attention)" ? "paused" : headline?.[1];
  const workflowLine = lines[1]?.match(/^Workflow run: ([A-Za-z0-9._:-]+)$/);
  const childLine = lines[2]?.match(/^Child run: ([A-Za-z0-9._:-]+)$/);
  const hasNoIdentity = !workflowLine && !childLine && !lines.some((line) => line.startsWith("Workflow run:") || line.startsWith("Child run:"));
  if (!headline || headlineOutcome !== details.outcome || headline[2] !== details.childKey || !((workflowLine && childLine) || hasNoIdentity) ||
      lines.filter((line) => line.startsWith("Workflow run:")).length !== 1 ||
      lines.filter((line) => line.startsWith("Child run:")).length !== 1 ||
      workflowLine && !isNativeId(workflowLine[1]) || childLine && !isNativeId(childLine[1])) return null;
  return { childKey: details.childKey, ...(workflowLine ? { workflowRunId: workflowLine[1] } : {}), ...(childLine ? { runId: childLine[1] } : {}) };
}

/** Bind a workflow run to the matching native tool call using only the request-prefix branch. */
function isPiWorkflowToolCallInBranch(branch: SessionEntryLike[], workflowRunId: string, toolCallId: string): boolean {
  const resultMatches = branch.filter((entry) => {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    const details = message.details as { runId?: unknown; mode?: unknown } | undefined;
    return message.role === "toolResult" && message.toolCallId === toolCallId &&
      details?.mode === "workflow" && details.runId === workflowRunId;
  });
  if (resultMatches.some((entry) => {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    return message.toolCallId !== toolCallId;
  })) return false;
  const resultIndex = resultMatches.length ? branch.indexOf(resultMatches[0]!) : branch.length;
  let matchingCalls = 0;
  for (const entry of branch.slice(0, resultIndex)) {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    if (message.role !== "assistant" || !Array.isArray(message.content)) continue;
    for (const part of message.content as Array<{ type?: unknown; name?: unknown; id?: unknown; arguments?: unknown }>) {
      if ((part.type !== "toolCall" && part.type !== "tool_call") || part.name !== "subagent" || part.id !== toolCallId) continue;
      const args = part.arguments && typeof part.arguments === "object" ? part.arguments as Record<string, unknown> : null;
      const options = args?.options && typeof args.options === "object" ? args.options as Record<string, unknown> : null;
      const isWorkflow = args?.workflow === true || typeof args?.workflow === "string" && args.workflow.trim().length > 0 ||
        typeof options?.script === "string" && options.script.trim().length > 0;
      if (isWorkflow) matchingCalls += 1;
    }
  }
  return matchingCalls === 1;
}

function readPiWorkflowToolCallIdFromBranch(branch: SessionEntryLike[], workflowRunId: string, preferredToolCallId?: string): string | null {
  const results = branch.filter((entry) => {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    const details = message.details as { runId?: unknown; mode?: unknown } | undefined;
    return message.role === "toolResult" && details?.mode === "workflow" && details.runId === workflowRunId;
  });
  const callIds = [...new Set(results.map((entry) => {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    return message.toolCallId;
  }).filter(isNativeId))];
  if (callIds.length > 1) return null;
  const candidate = preferredToolCallId ?? callIds[0];
  if (!candidate || callIds.length === 1 && callIds[0] !== candidate ||
      !isPiWorkflowToolCallInBranch(branch, workflowRunId, candidate)) return null;
  return candidate;
}

interface PiWorkflowChildSnapshot {
  workflowKey?: unknown;
  runId?: unknown;
  status?: unknown;
}

interface PiWorkflowNotificationRun {
  runId?: unknown;
  workflowRunId?: unknown;
  status?: unknown;
  agent?: unknown;
  asyncDir?: unknown;
  sessionLabel?: unknown;
  sessionValue?: unknown;
  childRuns?: PiWorkflowChildSnapshot[];
  childOutputs?: PiWorkflowChildSnapshot[];
}

function readPiWorkflowNotificationDependencies(
  entry: SessionEntryLike,
  run: PiWorkflowNotificationRun,
  branchBeforeEntry: SessionEntryLike[],
  parentSessionId: string,
  parentSessionFile: string | undefined,
  retainedCommittedDependencies?: ReadonlyMap<string, PiDependencyRef>,
): { dependencies: NonNullable<AgentMetadata["depends_on"]>; consumedRunIds: string[]; deliveryText: string } | null | undefined {
  if (run.workflowRunId === undefined) return undefined;
  const workflowRunId = run.workflowRunId;
  if (!isNativeId(workflowRunId) || run.runId !== workflowRunId ||
      run.status !== "completed" || !parentSessionFile ||
      !Array.isArray(run.childRuns) || !Array.isArray(run.childOutputs) ||
      run.childRuns.length > 256 || run.childOutputs.length > 256) return null;
  const toolCallId = readPiWorkflowToolCallIdFromBranch(branchBeforeEntry, workflowRunId);
  if (!toolCallId) return null;
  const childRuns = run.childRuns;
  const childOutputs = run.childOutputs;
  if (childRuns.some((child) => !child || !isNativeId(child.workflowKey) || !isNativeId(child.runId) ||
      !["completed", "failed", "paused", "stopped", "rejected", "detached"].includes(String(child.status))) ||
      childOutputs.some((child) => !child || !isNativeId(child.workflowKey) || !isNativeId(child.runId) ||
      !["completed", "failed", "paused", "stopped", "rejected", "detached"].includes(String(child.status)))) return null;
  const childKey = (child: PiWorkflowChildSnapshot) => child.workflowKey as string;
  const childRunId = (child: PiWorkflowChildSnapshot) => child.runId as string;
  const uniqueRunKeys = (children: PiWorkflowChildSnapshot[]) => new Set(children.map((child) => `${childKey(child)}:${childRunId(child)}`)).size === children.length;
  if (!uniqueRunKeys(childRuns) || !uniqueRunKeys(childOutputs) || childRuns.length !== childOutputs.length) return null;
  const childOutputByKey = new Map(childOutputs.map((child) => [`${childKey(child)}:${childRunId(child)}`, child]));
  if (childRuns.some((child) => {
    const output = childOutputByKey.get(`${childKey(child)}:${childRunId(child)}`);
    return !output || output.status !== child.status;
  })) return null;

  // Validate the native renderer's run index and each keyed output block. These
  // labels are identity evidence; free-form output text is never searched for IDs.
  const content = typeof entry.content === "string" ? entry.content : "";
  const lines = content.split("\n");
  if (lines.filter((line) => line === `Workflow run: ${workflowRunId}`).length !== 1) return null;
  const expectedChildRunLine = `Child runs: ${childRuns.map((child) => `${childKey(child)}=${childRunId(child)} (${String(child.status)})`).join(", ")}`;
  const childRunLines = lines.filter((line) => line.startsWith("Child runs:"));
  if (childRunLines.length !== 1 || childRunLines[0] !== expectedChildRunLine) return null;
  const outputBlocks = readPiRenderedChildOutputs(content);
  if (!outputBlocks || outputBlocks.size !== childOutputs.length ||
      childOutputs.some((child) => !outputBlocks.has(`${childKey(child)}:${childRunId(child)}`))) return null;

  const dependencies: NonNullable<AgentMetadata["depends_on"]> = [];
  const consumedRunIds = retainedCommittedDependencies === undefined ? [workflowRunId] : [];
  for (const child of childRuns) {
    const key = `${childKey(child)}:${childRunId(child)}`;
    const output = childOutputByKey.get(key)!;
    const outputBlock = outputBlocks.get(key)!;
    if (output.status !== child.status || outputBlock.status !== child.status || typeof outputBlock.preview !== "string" ||
        (child.status !== "completed" && child.status !== "failed")) return null;
    if (retainedCommittedDependencies !== undefined) {
      const dependency = retainedCommittedDependencies.get(childRunId(child));
      if (dependency) {
        dependencies.push({ ...dependency });
        consumedRunIds.push(childRunId(child));
      }
      continue;
    }
    const completion = readPiWorkflowChildOutput(childRunId(child), workflowRunId, childKey(child), parentSessionId, parentSessionFile, outputBlock.preview);
    if (!completion) return null;
    dependencies.push({ session_id: `pi:session:${completion.sessionId}`, response_id: completion.responseId, run_id: completion.runId });
    consumedRunIds.push(completion.runId);
  }
  return { dependencies, consumedRunIds: [...new Set(consumedRunIds)], deliveryText: content };
}

export function readPiRunCompletion(
  runId: string,
  parentSessionId: string,
  toolCallId: string,
  parentSessionFile?: string,
  parentBranch?: SessionEntryLike[] | null,
): PiRunCompletion | null {
  const status = readPiSubagentStatus(runId, parentSessionId, parentSessionFile);
  if (!status || !parentSessionFile || readPiDelegationSource(status, runId, parentSessionId, parentSessionFile, parentBranch) !== toolCallId || status.state !== "complete" || status.mode !== "single" || !Array.isArray(status.steps) || status.steps.length !== 1) return null;
  return readPiCompletionFromStatus(runId, status, parentSessionId, parentSessionFile);
}

export interface PiForegroundLaunchEvidence {
  receipt: ChildLaunchReceipt;
  deliveredOutputSha256?: string;
}

/** Bind a fresh attached child only after Pi's exact ordinary tool result exists. */
export function readPiForegroundLaunchEvidence(event: unknown, ctx: ExtensionContext): PiForegroundLaunchEvidence | null {
  if (!event || typeof event !== "object") return null;
  const value = event as {
    toolName?: unknown;
    toolCallId?: unknown;
    isError?: unknown;
    content?: unknown;
    details?: unknown;
  };
  const details = value.details && typeof value.details === "object" ? value.details as Record<string, unknown> : null;
  const results = details?.results;
  if (value.toolName !== "subagent" || value.isError !== false || !isNativeId(value.toolCallId) ||
      details?.mode !== "single" || !isNativeId(details.runId) || details.detached === true || details.interrupted === true ||
      details.toolCallId !== undefined && details.toolCallId !== value.toolCallId ||
      !Array.isArray(results) || results.length !== 1) return null;
  const result = results[0] as Record<string, unknown> | null;
  if (!result || result.context !== "fresh" || result.exitCode !== 0 || result.error !== undefined || result.detached === true ||
      result.interrupted === true || result.stopped === true || result.timedOut === true || result.outputPartial === true ||
      typeof result.sessionFile !== "string") return null;

  const session = ctx as SessionContextLike;
  const parentSessionId = session.sessionManager.getSessionId();
  const parentSessionFile = session.sessionManager.getSessionFile?.();
  const parentHeader = readNativeSessionHeader(parentSessionFile);
  if (!parentSessionFile || parentHeader?.id !== parentSessionId || parentHeader.parentSession) return null;
  const parentRootId = readNativeSessionRootId(parentSessionFile);
  if (parentRootId !== parentSessionId) return null;

  const branch = readPiOwnedBranch(ctx);
  if (!branch) return null;
  const launchResponses = branch.flatMap((entry) => {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    if (message.role !== "assistant" || !isNativeId(message.responseId) || !Array.isArray(message.content)) return [];
    const matches = message.content.some((part: any) =>
      (part?.type === "toolCall" || part?.type === "tool_call") && part.name === "subagent" && part.id === value.toolCallId,
    );
    return matches ? [message.responseId] : [];
  });
  if (launchResponses.length !== 1 || branch.filter((entry) => {
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    return message.role === "assistant" && message.responseId === launchResponses[0];
  }).length !== 1) return null;

  const terminal = readPiFreshChildTerminal(result.sessionFile, parentSessionFile, parentSessionId);
  if (!terminal) return null;
  const receipt: ChildLaunchReceipt = {
    parent_response_id: launchResponses[0]!,
    tool_call_id: value.toolCallId,
    child_session_id: `pi:session:${terminal.sessionId}`,
    run_id: details.runId,
    last_response_id: terminal.responseId,
  };
  if (!isChildLaunchReceipt(receipt)) return null;

  const deliveredText = readPiSingleTextContent(value.content);
  if (!terminal.output || !deliveredText) return { receipt };
  const resultOutput = result.finalOutput;
  const hasSupportedDelivery = deliveredText === terminal.output ||
    result.outputMode === "inline" && result.outputState === "present" && resultOutput === terminal.output &&
    isPiForegroundRenderedOutput(deliveredText, terminal.output, details);
  if (!hasSupportedDelivery) return { receipt };
  return {
    receipt,
    deliveredOutputSha256: createHash("sha256").update(deliveredText, "utf8").digest("hex"),
  };
}

function isPiForegroundRenderedOutput(
  deliveredText: string,
  terminalOutput: string,
  details: Record<string, unknown>,
): boolean {
  const budget = details.runFanoutBudget;
  if (!budget || typeof budget !== "object") return false;
  const snapshot = budget as { used?: unknown; limit?: unknown; remaining?: unknown };
  if (!Number.isSafeInteger(snapshot.used) || !Number.isSafeInteger(snapshot.limit) ||
      !Number.isSafeInteger(snapshot.remaining) || (snapshot.used as number) < 0 ||
      (snapshot.limit as number) < 1 || (snapshot.remaining as number) < 0 ||
      (snapshot.used as number) + (snapshot.remaining as number) !== snapshot.limit) return false;

  const prefix = `Run fan-out: ${snapshot.used}/${snapshot.limit} used, ${snapshot.remaining} remaining\n`;
  const rendered = `${prefix}${terminalOutput}`;
  if (deliveredText === rendered) return true;

  const mission = details.mission;
  if (!mission || typeof mission !== "object") return false;
  const { id, status } = mission as { id?: unknown; status?: unknown };
  if (!isNativeId(id) || typeof status !== "string" || !/^[A-Za-z][A-Za-z0-9 _-]{0,63}$/.test(status)) return false;
  return deliveredText === `${rendered}\nMission: ${id} (${status})`;
}

function readPiFreshChildTerminal(
  sessionFile: string,
  parentSessionFile: string,
  parentSessionId: string,
): { sessionId: string; responseId: string; output: string | null; artifactOutput: string | null } | null {
  const header = readNativeSessionHeader(sessionFile);
  if (!header || !isNativeId(header.id) || !header.parentSession ||
      readNativeSessionHeader(header.parentSession)?.id !== parentSessionId) return null;
  try {
    if (realpathSync(header.parentSession) !== realpathSync(parentSessionFile)) return null;
  } catch {
    return null;
  }
  const rootId = readNativeSessionRootId(sessionFile);
  if (rootId !== parentSessionId) return null;
  let fd: number | undefined;
  try {
    fd = openSync(sessionFile, "r");
    const stat = fstatSync(fd);
    if (stat.size <= 0 || stat.size > 8 * 1024 * 1024) return null;
    const buffer = Buffer.alloc(stat.size);
    if (readSync(fd, buffer, 0, stat.size, 0) !== stat.size) return null;
    const assistantMessages: Array<NonNullable<SessionEntryLike["message"]>> = [];
    for (const line of buffer.toString("utf8").split("\n")) {
      if (!line) continue;
      let entry: SessionEntryLike;
      try { entry = JSON.parse(line) as SessionEntryLike; } catch { return null; }
      const message = entry.message && typeof entry.message === "object" ? entry.message : null;
      if (message?.role === "assistant") assistantMessages.push(message);
    }
    const terminal = assistantMessages.at(-1);
    if (!terminal || terminal.stopReason !== "stop" || !isNativeId(terminal.responseId) ||
        assistantMessages.filter((message) => message.responseId === terminal.responseId).length !== 1) return null;
    const rawOutput = piAssistantText(terminal.content);
    return {
      sessionId: header.id,
      responseId: terminal.responseId,
      output: normalizePiPinnedAssistantOutput(rawOutput, "direct"),
      artifactOutput: projectPiPinnedArtifactOutput(rawOutput),
    };
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

function readPiWorkflowChildOutput(
  runId: string,
  workflowRunId: string,
  childKey: string,
  parentSessionId: string,
  parentSessionFile: string,
  output: string,
): PiRunCompletion | null {
  const status = readPiSubagentStatus(runId, parentSessionId, parentSessionFile);
  if (!status || status.runId !== runId || status.mode !== "single" ||
      (status.state !== "complete" && status.state !== "failed") ||
      status.parentWorkflowRunId !== workflowRunId || status.workflowKey !== childKey ||
      !Array.isArray(status.steps) || status.steps.length !== 1) return null;
  return readPiExactCommittedOutput(status, runId, parentSessionId, parentSessionFile, output, "workflow");
}

function readPiWorkflowReturnedChildOutput(
  runId: string,
  workflowRunId: string,
  childKey: string,
  parentSessionId: string,
  parentSessionFile: string,
  returnedOutput: string,
): PiRunCompletion | null {
  const status = readPiSubagentStatus(runId, parentSessionId, parentSessionFile);
  if (!status || status.runId !== runId || status.mode !== "single" ||
      (status.state !== "complete" && status.state !== "failed") ||
      status.parentWorkflowRunId !== workflowRunId || status.workflowKey !== childKey ||
      !Array.isArray(status.steps) || status.steps.length !== 1) return null;

  const completion = readPiCompletionFromStatus(runId, status, parentSessionId, parentSessionFile);
  if (!completion) return null;
  return readPiCompletionOutput(status, completion, parentSessionId, parentSessionFile, returnedOutput, "returned");
}

function readPiExactCommittedOutput(
  status: PiSubagentStatus,
  runId: string,
  parentSessionId: string,
  parentSessionFile: string,
  output: string,
  renderer: "workflow" | "direct",
): PiRunCompletion | null {
  const completion = readPiCompletionFromStatus(runId, status, parentSessionId, parentSessionFile);
  if (!completion) return null;
  return readPiCompletionOutput(status, completion, parentSessionId, parentSessionFile, output, renderer);
}

function readPiCompletionOutput(
  status: PiSubagentStatus,
  completion: PiRunCompletion,
  parentSessionId: string,
  parentSessionFile: string,
  expectedOutput: string,
  projection: "returned" | "workflow" | "direct",
): PiRunCompletion | null {
  if (expectedOutput.length === 0 || expectedOutput.length > 32 * 1024) return null;
  let canonicalParentFile: string;
  try {
    canonicalParentFile = realpathSync(parentSessionFile);
    if (readNativeSessionHeader(canonicalParentFile)?.id !== parentSessionId) return null;
  } catch {
    return null;
  }
  const sessionFiles = resolvePiStatusSessionFiles(status, parentSessionId, canonicalParentFile);
  if (!sessionFiles || sessionFiles.size !== 1) return null;
  const [[sessionFile, nativeHeader]] = sessionFiles;
  if (nativeHeader.id !== completion.sessionId) return null;
  try {
    const fd = openSync(sessionFile, "r");
    try {
      const stat = fstatSync(fd);
      if (!stat.isFile() || stat.size <= 0 || stat.size > 8 * 1024 * 1024) return null;
      const buffer = Buffer.alloc(stat.size);
      if (readSync(fd, buffer, 0, buffer.length, 0) !== buffer.length) return null;
      let matches = 0;
      let renderedOutput: string | null = null;
      for (const line of buffer.toString("utf8").split("\n")) {
        if (!line) continue;
        let entry: SessionEntryLike;
        try { entry = JSON.parse(line) as SessionEntryLike; } catch { return null; }
        const message = entry.message && typeof entry.message === "object" ? entry.message : null;
        if (message?.role !== "assistant" || message.responseId !== completion.responseId) continue;
        matches += 1;
        if (message.stopReason !== "stop") return null;
        const text = piAssistantText(message.content);
        renderedOutput = projection === "returned"
          ? projectPiPinnedArtifactOutput(text)
          : normalizePiPinnedAssistantOutput(text, projection);
      }
      return matches === 1 && renderedOutput === expectedOutput ? completion : null;
    } finally {
      closeSync(fd);
    }
  } catch {
    // Missing or partial native files leave this output unresolved.
    return null;
  }
}

function piAssistantText(content: unknown): string | null {
  if (!Array.isArray(content)) return null;
  const texts = content.flatMap((part) => {
    if (!part || typeof part !== "object" || (part as { type?: unknown }).type !== "text" ||
        typeof (part as { text?: unknown }).text !== "string") return [];
    return [(part as { text: string }).text];
  });
  return texts.length === 0 ? null : texts.join("\n");
}

/** Match the pinned runner's acceptance-report removal and its notification trim. */
function normalizePiPinnedAssistantOutput(text: string | null, renderer: "workflow" | "direct"): string | null {
  if (text === null) return null;
  const acceptanceReport = /\n?```acceptance[-_]report\s*\n[\s\S]*?```\s*$/i.exec(text);
  const output = acceptanceReport ? text.slice(0, acceptanceReport.index).trimEnd() : text;
  if (renderer === "direct") return output.trimEnd();
  if (/\r|[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/.test(output)) return null;
  return truncatePiWorkflowPreview(output);
}

/** The pinned direct-run artifact drops its trailing acceptance fence, but otherwise keeps output bytes. */
function projectPiPinnedArtifactOutput(text: string | null): string | null {
  if (text === null) return null;
  const acceptanceReport = /\n?```acceptance[-_]report\s*\n[\s\S]*?```\s*$/i.exec(text);
  return acceptanceReport ? text.slice(0, acceptanceReport.index).trimEnd() : text;
}

function truncatePiWorkflowPreview(output: string): string {
  const bytes = Buffer.from(output, "utf8");
  const marker = "...[preview truncated]";
  const markerBytes = Buffer.byteLength(marker, "utf8");
  if (bytes.length <= 2_000) return output;
  let end = 2_000 - markerBytes;
  while (end > 0 && (bytes[end]! & 0xc0) === 0x80) end -= 1;
  return `${bytes.subarray(0, end).toString("utf8")}${marker}`;
}

function readPiCompletionFromStatus(
  runId: string,
  status: PiSubagentStatus,
  parentSessionId: string,
  parentSessionFile: string,
): PiRunCompletion | null {
  const sessionFiles = resolvePiStatusSessionFiles(status, parentSessionId, parentSessionFile);
  if (!sessionFiles || sessionFiles.size !== 1) return null;
  const [sessionFile, nativeHeader] = sessionFiles.entries().next().value ?? [];
  if (!sessionFile || !nativeHeader) return null;

  let fd: number | undefined;
  try {
    fd = openSync(sessionFile, "r");
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.size <= 0 || stat.size > 8 * 1024 * 1024) return null;
    const buffer = Buffer.alloc(stat.size);
    if (readSync(fd, buffer, 0, buffer.length, 0) !== buffer.length) return null;
    const parsed = buffer.toString("utf8").split("\n").filter(Boolean)
      .map((line) => JSON.parse(line) as SessionEntryLike);
    const assistantResponseCounts = new Map<string, number>();
    for (const entry of parsed) {
      const message = entry.message;
      if (message?.role === "assistant" && isNativeId(message.responseId)) {
        assistantResponseCounts.set(message.responseId, (assistantResponseCounts.get(message.responseId) ?? 0) + 1);
      }
    }
    const markers = parsed.flatMap((entry) => {
      if (entry.customType !== PI_RUN_COMPLETION_ENTRY) return [];
      const data = customData(entry);
      return data?.runId === runId ? [{ sessionId: data.sessionId, responseId: data.responseId }] : [];
    });
    if (markers.length !== 1) return null;
    const marker = markers[0]!;
    if (marker.sessionId !== nativeHeader.id || !isNativeId(marker.sessionId) || !isNativeId(marker.responseId) ||
        assistantResponseCounts.get(marker.responseId) !== 1) return null;
    return { runId, sessionId: marker.sessionId, responseId: marker.responseId };
  } catch {
    // Missing or partial native files leave this dependency unresolved.
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

export function readPiUnconsumedRunDependencies(
  ctx: ExtensionContext,
  onResolvedDependency?: (
    dependency: PiDependencyRef,
    deliveredText: string,
    channel?: { toolCallId: string; deliveredOutputSha256: string },
  ) => void,
): AgentMetadata["depends_on"] {
  const session = ctx as SessionContextLike;
  const sessionId = session.sessionManager.getSessionId();
  const fullBranch = readPiOwnedBranch(ctx);
  const forkBoundaryIndex = readPiForkBoundaryIndex(session);
  if (forkBoundaryIndex === null && fullBranch === null) return null;
  const branch = forkBoundaryIndex === null ? fullBranch! : entries(session).slice(forkBoundaryIndex + 1);
  let lastAssistantIndex = -1;
  for (let index = 0; index < branch.length; index += 1) {
    if (branch[index].type === "message" && branch[index].message?.role === "assistant") lastAssistantIndex = index;
  }
  const dependencies: NonNullable<AgentMetadata["depends_on"]> = [];
  const deliveryWitnesses = new Map<string, {
    dependency: PiDependencyRef;
    witnesses: Array<{ deliveredText: string; toolCallId?: string; deliveredOutputSha256?: string }>;
  }>();
  const addDependency = (
    dependency: PiDependencyRef,
    deliveredText: string,
    channel?: { toolCallId: string; deliveredOutputSha256: string },
  ) => {
    dependencies.push(dependency);
    const key = piDependencyKey(dependency);
    const witness = deliveryWitnesses.get(key) ?? { dependency, witnesses: [] };
    const item = { deliveredText, ...channel };
    if (!witness.witnesses.some((previous) => JSON.stringify(previous) === JSON.stringify(item))) witness.witnesses.push(item);
    deliveryWitnesses.set(key, witness);
  };
  const addSubagentNoticeDependency = (dependency: PiDependencyRef, content: string) => {
    addDependency(dependency, content);
    const contextText = renderPiSubagentsNoticeContext(content);
    if (contextText !== content) addDependency(dependency, contextText);
  };
  let unresolved = false;
  const persistedDependencyKeys = readPiPersistedDependencyKeys(branch);
  const latestNativeCompactionIndex = branch.reduce((found, entry, index) => entry.type === "compaction" ? index : found, -1);
  const latestNativeCompaction = latestNativeCompactionIndex >= 0
    ? branch[latestNativeCompactionIndex] as NativeCompactionLike
    : null;
  const latestCutWasSupplied = latestNativeCompaction !== null &&
    Object.prototype.hasOwnProperty.call(latestNativeCompaction, "firstKeptEntryId");
  const latestKeptIndex = latestNativeCompaction && isNativeId(latestNativeCompaction.firstKeptEntryId)
    ? readPiOwnedRetainedBoundaryIndex(ctx, branch, latestNativeCompaction.firstKeptEntryId)
    : null;
  const latestCutIsInvalid = latestCutWasSupplied &&
    (latestKeptIndex === null || latestKeptIndex > latestNativeCompactionIndex);
  const firstCoveringCompaction = (entryIndex: number): true | false | null => {
    for (let compactionIndex = entryIndex + 1; compactionIndex < branch.length; compactionIndex += 1) {
      const native = branch[compactionIndex];
      if (native?.type !== "compaction") continue;
      const compaction = native as NativeCompactionLike;
      if (!isNativeId(compaction.firstKeptEntryId)) return null;
      const keptIndex = readPiOwnedRetainedBoundaryIndex(ctx, branch, compaction.firstKeptEntryId);
      if (keptIndex === null || keptIndex > compactionIndex) return null;
      if (entryIndex >= keptIndex) continue;

      const records = branch.filter((item) => item.customType === PI_COMPACTION_ENTRY &&
        customData(item)?.compactionEntryId === compaction.id);
      const record = records[0];
      const recordData = record ? customData(record) as unknown as PiCompactionRecord : null;
      if (records.length !== 1 || compaction.fromHook === true ||
          !validPiCompactionRecord(recordData, record, branch) || recordData.contributors.length === 0) return null;
      return true;
    }
    return false;
  };
  for (const entry of branch.slice(lastAssistantIndex + 1)) {
    const parentSessionFile = session.sessionManager.getSessionFile?.();
    const entryIndex = branch.indexOf(entry);
    if (entry.customType === PI_FOREGROUND_DELIVERY_ENTRY) {
      const delivery = readPiForegroundDeliveryDependency(entry, branch, lastAssistantIndex, parentSessionFile, sessionId);
      if (!delivery) {
        unresolved = true;
        continue;
      }
      if (delivery.status === "filtered") continue;
      addDependency(delivery.dependency, delivery.deliveredText, {
        toolCallId: delivery.toolCallId,
        deliveredOutputSha256: delivery.deliveredOutputSha256,
      });
      continue;
    }
    if (entry.type === "custom_message" && entry.customType === "subagent-incremental-child-notify") {
      // Incremental notices carry status and an artifact path, not consumed output.
      if (!readPiIncrementalChildNotice(entry)) unresolved = true;
      continue;
    }
    const inlineCandidate = readPiInlineWorkflowResult(
      entry,
      parentSessionFile,
      branch.slice(0, entryIndex),
      false,
    );
    if (inlineCandidate?.statusOnly) continue;
    const isSubagentNotification = entry.type === "custom_message" && entry.customType === "subagent-notify";
    const mayContainOutput = inlineCandidate !== undefined || isSubagentNotification;
    if (!mayContainOutput) continue;
    if (latestCutIsInvalid) {
      unresolved = true;
      continue;
    }
    const summaryProof = firstCoveringCompaction(entryIndex);
    if (summaryProof === null) {
      unresolved = true;
      continue;
    }
    if (summaryProof) continue;
    const inlineWorkflow = inlineCandidate === null
      ? readPiInlineWorkflowResult(entry, parentSessionFile, branch.slice(0, entryIndex))
      : inlineCandidate;
    if (inlineWorkflow === null) {
      unresolved = true;
      continue;
    }
    if (inlineWorkflow) {
      if (inlineWorkflow.statusOnly) continue;
      for (const child of inlineWorkflow.children) {
        const output = isNativeId(child.childId) && isNativeId(child.runId)
          ? inlineWorkflow.outputs.get(`${child.childId}:${child.runId}`)
          : undefined;
        if ((child.state !== "completed" && child.state !== "failed") || !isNativeId(child.childId) ||
            !isNativeId(child.runId) || output?.status !== child.state ||
            typeof output.output !== "string" && typeof output.preview !== "string") {
          unresolved = true;
          continue;
        }
        const completion = output.output !== undefined
          ? readPiWorkflowReturnedChildOutput(child.runId, inlineWorkflow.workflowRunId, child.childId, sessionId, parentSessionFile, output.output)
          : typeof output.preview === "string"
            ? readPiWorkflowChildOutput(child.runId, inlineWorkflow.workflowRunId, child.childId, sessionId, parentSessionFile, output.preview)
            : null;
        if (!completion) {
          unresolved = true;
          continue;
        }
        addDependency(
          { session_id: `pi:session:${completion.sessionId}`, response_id: completion.responseId, run_id: completion.runId },
          inlineWorkflow.deliveryText,
          {
            toolCallId: inlineWorkflow.toolCallId,
            deliveredOutputSha256: createHash("sha256").update(inlineWorkflow.deliveryText, "utf8").digest("hex"),
          },
        );
      }
      continue;
    }
    if (entry.type !== "custom_message") continue;
    let runs: PiWorkflowNotificationRun[] | undefined;
    if (entry.customType === "subagent-notify") {
      runs = (entry.details as { runs?: typeof runs } | undefined)?.runs;
    } else continue;
    if (!Array.isArray(runs)) {
      unresolved = true;
      continue;
    }
    for (const run of runs) {
      if (!isNativeId(run.runId)) {
        unresolved = true;
        continue;
      }
      const workflowDelivery = readPiWorkflowNotificationDependencies(
        entry,
        run,
        branch.slice(0, entryIndex),
        sessionId,
        parentSessionFile,
      );
      if (workflowDelivery !== undefined) {
        if (!workflowDelivery) {
          unresolved = true;
          continue;
        }
        for (const dependency of workflowDelivery.dependencies) {
          addSubagentNoticeDependency(dependency, workflowDelivery.deliveryText);
        }
        continue;
      }
      if (run.workflowRunId === undefined) {
        if (run.status !== "completed" && run.status !== "failed") {
          unresolved = true;
          continue;
        }
        const status = readPiSubagentStatus(run.runId, sessionId, parentSessionFile);
        const resolved = status && parentSessionFile
          ? resolvePiDirectNotificationDependency(
            entry,
            run,
            status,
            sessionId,
            parentSessionFile,
            branch.slice(0, entryIndex),
          )
          : null;
        if (!resolved) {
          unresolved = true;
          continue;
        }
        if (resolved.kind === "empty") continue;
        addSubagentNoticeDependency(resolved.dependency, typeof entry.content === "string" ? entry.content : "");
        continue;
      }
      if (run.status !== "completed") {
        unresolved = true;
        continue;
      }
      const status = readPiSubagentStatus(run.runId, sessionId, parentSessionFile);
      const parentPrefix = branch.slice(0, entryIndex);
      const toolCallId = parentSessionFile && status
        ? readPiDelegationSource(status, run.runId, sessionId, parentSessionFile, parentPrefix)
        : null;
      if (!toolCallId) {
        unresolved = true;
        continue;
      }
      const completion = readPiRunCompletion(run.runId, sessionId, toolCallId, parentSessionFile, parentPrefix);
      if (!completion) {
        unresolved = true;
        continue;
      }
      const dependency = {
        session_id: `pi:session:${completion.sessionId}`,
        response_id: completion.responseId,
        run_id: completion.runId,
      };
      addDependency(dependency, typeof entry.content === "string" ? entry.content : "");
    }
  }
  const unique = [...new Map(dependencies
    .filter((ref) => !persistedDependencyKeys.has(piDependencyKey(ref)))
    .map((ref) => [piDependencyKey(ref), ref])).values()];
  if (unresolved || unique.length > 8) return null;
  for (const dependency of unique) {
    const witness = deliveryWitnesses.get(piDependencyKey(dependency));
    if (!witness || witness.witnesses.length === 0) return null;
    for (const item of witness.witnesses) {
      onResolvedDependency?.(dependency, item.deliveredText,
        item.toolCallId && item.deliveredOutputSha256
          ? { toolCallId: item.toolCallId, deliveredOutputSha256: item.deliveredOutputSha256 }
          : undefined,
      );
    }
  }
  return unique;
}

function readPiForegroundDeliveryDependency(
  entry: SessionEntryLike,
  branch: SessionEntryLike[],
  lastAssistantIndex: number,
  parentSessionFile: string | undefined,
  parentSessionId: string,
): { status: "filtered" } | {
  status: "delivered";
  dependency: PiDependencyRef;
  deliveredText: string;
  toolCallId: string;
  deliveredOutputSha256: string;
} | null {
  const data = customData(entry) as unknown as PiForegroundDeliveryRecord | null;
  if (!parentSessionFile || !validPiForegroundDeliveryRecord(data)) return null;
  const { receipt } = data;
  const launchResponses = branch.flatMap((candidate) => {
    const message = (candidate.message && typeof candidate.message === "object" ? candidate.message : candidate) as SessionEntryLike;
    if (message.role !== "assistant" || message.responseId !== receipt.parent_response_id || !Array.isArray(message.content)) return [];
    return message.content.some((part: any) =>
      (part?.type === "toolCall" || part?.type === "tool_call") && part.name === "subagent" && part.id === receipt.tool_call_id,
    ) ? [candidate] : [];
  });
  if (launchResponses.length !== 1 || branch.indexOf(launchResponses[0]!) > lastAssistantIndex) return null;

  const resultEntries = branch.flatMap((candidate, index) => {
    if (index <= lastAssistantIndex) return [];
    const message = (candidate.message && typeof candidate.message === "object" ? candidate.message : candidate) as SessionEntryLike;
    const details = message.details as { mode?: unknown; runId?: unknown; results?: unknown } | undefined;
    return message.role === "toolResult" && message.toolCallId === receipt.tool_call_id &&
      details?.mode === "single" && details.runId === receipt.run_id ? [{ message, details }] : [];
  });
  if (resultEntries.length !== 1) return null;
  if ((resultEntries[0]!.message as SessionEntryLike).isError !== false) return { status: "filtered" };
  const result = resultEntries[0]!.details.results;
  if (!Array.isArray(result) || result.length !== 1 ||
      !result[0] || typeof result[0] !== "object" || typeof (result[0] as { sessionFile?: unknown }).sessionFile !== "string") return null;
  const childHeader = readNativeSessionHeader((result[0] as { sessionFile: string }).sessionFile);
  const childSessionId = receipt.child_session_id.startsWith("pi:session:")
    ? receipt.child_session_id.slice("pi:session:".length)
    : null;
  if (!childSessionId || !isNativeId(childSessionId) || childHeader?.id !== childSessionId || !childHeader.parentSession ||
      readNativeSessionHeader(childHeader.parentSession)?.id !== parentSessionId) return null;
  try {
    if (realpathSync(childHeader.parentSession) !== realpathSync(parentSessionFile)) return null;
  } catch {
    return null;
  }
  const deliveredText = readPiSingleTextContent(resultEntries[0]!.message.content);
  if (!deliveredText || createHash("sha256").update(deliveredText, "utf8").digest("hex") !== data.delivered_output_sha256) {
    return { status: "filtered" };
  }
  return {
    status: "delivered",
    dependency: {
      session_id: receipt.child_session_id,
      response_id: receipt.last_response_id,
      run_id: receipt.run_id,
    },
    deliveredText,
    toolCallId: receipt.tool_call_id,
    deliveredOutputSha256: data.delivered_output_sha256,
  };
}

export function makePiResultConsumptionRecord(
  streamId: string,
  responseId: string,
  dependencies: readonly PiDependencyRef[],
): PiResultConsumptionRecord | null {
  if (!isNativeId(streamId) || !isNativeId(responseId) || dependencies.length === 0 || dependencies.length > 8) return null;
  const keys = dependencies.map(piDependencyKey);
  if (new Set(keys).size !== keys.length || dependencies.some((ref) => !isValidPiDependencyRef(ref))) return null;
  return { version: 1, stream_id: streamId, response_id: responseId, dependencies: dependencies.map((ref) => ({ ...ref })) };
}

export function commitPiResultConsumption(
  ctx: ExtensionContext,
  streamId: string,
  responseId: string,
  dependencies: readonly PiDependencyRef[],
  appendEntry: (customType: string, data: Record<string, unknown>) => void,
): boolean {
  const session = ctx as SessionContextLike;
  const branch = entries(session);
  const committedCount = branch.filter((entry) => entry.type === "message" &&
    entry.message?.role === "assistant" && entry.message.responseId === responseId).length;
  const record = makePiResultConsumptionRecord(streamId, responseId, dependencies);
  if (committedCount !== 1 || !record) return false;
  appendEntry(PI_RESULT_CONSUMPTION_ENTRY, record as unknown as Record<string, unknown>);
  return true;
}

export function makePiLaunchReceiptPendingRecord(
  receipts: readonly ChildLaunchReceipt[],
): PiLaunchReceiptPendingRecord | null {
  if (receipts.length === 0 || receipts.length > MAX_PENDING_CHILD_LAUNCHES ||
      receipts.some((receipt) => !isChildLaunchReceipt(receipt))) return null;
  return { version: 1, receipts: receipts.map((receipt) => ({ ...receipt })) };
}

export function makePiLaunchReceiptConsumptionRecord(
  streamId: string,
  responseId: string,
  receipts: readonly ChildLaunchReceipt[],
): PiLaunchReceiptConsumptionRecord | null {
  if (!isNativeId(streamId) || !isNativeId(responseId) || receipts.length === 0 ||
      receipts.length > MAX_CHILD_LAUNCHES_PER_ENVELOPE || receipts.some((receipt) => !isChildLaunchReceipt(receipt)) ||
      new Set(receipts.map(childLaunchKey)).size !== receipts.length) return null;
  return { version: 1, stream_id: streamId, response_id: responseId, receipts: receipts.map((receipt) => ({ ...receipt })) };
}

export function commitPiLaunchReceiptConsumption(
  ctx: ExtensionContext,
  streamId: string,
  responseId: string,
  receipts: readonly ChildLaunchReceipt[],
  appendEntry: (customType: string, data: Record<string, unknown>) => void,
): boolean {
  const branch = entries(ctx as SessionContextLike);
  const committedCount = branch.filter((entry) => entry.type === "message" &&
    entry.message?.role === "assistant" && entry.message.responseId === responseId).length;
  const record = makePiLaunchReceiptConsumptionRecord(streamId, responseId, receipts);
  if (committedCount !== 1 || !record) return false;
  appendEntry(PI_LAUNCH_RECEIPT_CONSUMPTION_ENTRY, record as unknown as Record<string, unknown>);
  return true;
}

/** Rehydrate only compact receipt events since the latest committed compaction snapshot. */
export function readPiPendingChildLaunchReceipts(ctx: ExtensionContext): ChildLaunchReceipt[] | null {
  const branch = readPiOwnedBranch(ctx);
  if (branch === null) return null;
  const latestNativeCompactionIndex = branch.reduce((found, entry, index) => entry.type === "compaction" ? index : found, -1);
  let pending: ChildLaunchReceipt[] = [];
  let startIndex = 0;
  if (latestNativeCompactionIndex >= 0) {
    const native = branch[latestNativeCompactionIndex]! as NativeCompactionLike;
    const snapshot = readPiCompactionPendingLaunchSnapshot(branch, native);
    if (!snapshot) return null;
    pending = snapshot.map((receipt) => ({ ...receipt }));
    startIndex = latestNativeCompactionIndex + 1;
  }

  const seen = new Set(pending.map(childLaunchKey));
  for (const entry of branch.slice(startIndex)) {
    if (entry.customType === PI_LAUNCH_RECEIPT_PENDING_ENTRY) {
      const data = customData(entry) as unknown as PiLaunchReceiptPendingRecord | null;
      if (!validPiLaunchReceiptPendingRecord(data)) return null;
      const appended = appendPendingChildLaunches(pending, data.receipts);
      if (appended.overflow > 0) return null;
      pending = appended.pending;
      seen.clear();
      for (const receipt of pending) seen.add(childLaunchKey(receipt));
    } else if (entry.customType === PI_LAUNCH_RECEIPT_CONSUMPTION_ENTRY) {
      const data = customData(entry) as unknown as PiLaunchReceiptConsumptionRecord | null;
      if (!validPiLaunchReceiptConsumptionRecord(data)) return null;
      const responseIndexes = branch.flatMap((candidate, index) => candidate.type === "message" &&
        candidate.message?.role === "assistant" && candidate.message.responseId === data.response_id ? [index] : []);
      if (responseIndexes.length !== 1 || responseIndexes[0]! >= branch.indexOf(entry)) return null;
      const consumed = new Set(data.receipts.map(childLaunchKey));
      pending = pending.filter((receipt) => !consumed.has(childLaunchKey(receipt)));
      seen.clear();
      for (const receipt of pending) seen.add(childLaunchKey(receipt));
    }
  }
  return pending.length <= MAX_PENDING_CHILD_LAUNCHES && [...seen].length === pending.length ? pending : null;
}

function readPiCompactionPendingLaunchSnapshot(
  branch: SessionEntryLike[],
  native: NativeCompactionLike,
): ChildLaunchReceipt[] | null {
  const records = branch.filter((entry) => entry.customType === PI_COMPACTION_ENTRY &&
    customData(entry)?.compactionEntryId === native.id);
  if (records.length !== 1) return null;
  const record = records[0]!;
  const data = customData(record) as unknown as PiCompactionRecord | null;
  if (!data || data.compactionEntryId !== native.id ||
      data.pendingChildLaunches !== undefined && (!Array.isArray(data.pendingChildLaunches) ||
        data.pendingChildLaunches.length > MAX_PENDING_CHILD_LAUNCHES ||
        !data.pendingChildLaunches.every(isChildLaunchReceipt) ||
        new Set(data.pendingChildLaunches.map(childLaunchKey)).size !== data.pendingChildLaunches.length)) return null;
  return (data.pendingChildLaunches ?? []).map((receipt) => ({ ...receipt }));
}

/** Detect a repeated native launch even after its receipt was retired. */
export function hasPiKnownChildLaunchReceipt(ctx: ExtensionContext, receipt: ChildLaunchReceipt): boolean {
  if (!isChildLaunchReceipt(receipt)) return false;
  const branch = readPiOwnedBranch(ctx);
  if (branch === null) return false;
  const key = childLaunchKey(receipt);
  return branch.some((entry) => {
    if (entry.customType === PI_LAUNCH_RECEIPT_PENDING_ENTRY) {
      const data = customData(entry) as unknown as PiLaunchReceiptPendingRecord | null;
      return validPiLaunchReceiptPendingRecord(data) && data.receipts.some((candidate) => childLaunchKey(candidate) === key);
    }
    if (entry.customType === PI_LAUNCH_RECEIPT_CONSUMPTION_ENTRY) {
      const data = customData(entry) as unknown as PiLaunchReceiptConsumptionRecord | null;
      return validPiLaunchReceiptConsumptionRecord(data) && data.receipts.some((candidate) => childLaunchKey(candidate) === key);
    }
    if (entry.customType === PI_COMPACTION_ENTRY) {
      const data = customData(entry) as unknown as PiCompactionRecord | null;
      return Array.isArray(data?.pendingChildLaunches) && data.pendingChildLaunches.some((candidate) =>
        isChildLaunchReceipt(candidate) && childLaunchKey(candidate) === key,
      );
    }
    return false;
  });
}

export function readPiPersistedConsumedRunIds(
  ctx: ExtensionContext,
  firstKeptEntryId?: string,
): string[] | null {
  const branch = readPiOwnedBranch(ctx);
  if (branch === null) return firstKeptEntryId === undefined ? [] : null;
  const consumed = new Set<string>();
  const latestCompactionIndex = branch.reduce((found, entry, index) => entry.type === "compaction" ? index : found, -1);
  for (const entry of branch.slice(Math.max(0, latestCompactionIndex))) {
    if (entry.customType === PI_RESULT_CONSUMPTION_ENTRY) {
      const data = customData(entry) as unknown as PiResultConsumptionRecord | null;
      if (!validPiResultConsumptionRecord(data)) continue;
      for (const dependency of data.dependencies) if (dependency.run_id) consumed.add(dependency.run_id);
    } else if (entry.customType === PI_COMPACTION_ENTRY) {
      const data = customData(entry);
      for (const runId of (data?.consumedRunIds as unknown[] | undefined) ?? []) if (isNativeId(runId)) consumed.add(runId);
    }
  }
  if (firstKeptEntryId === undefined) return [...consumed];
  const retainedIndex = readPiOwnedRetainedBoundaryIndex(ctx, branch, firstKeptEntryId);
  if (retainedIndex === null) return null;
  const retainedProof = readPiRetainedNotificationProof(ctx, branch, retainedIndex);
  if (retainedProof === null) return null;
  const retainedRunIds = new Set(retainedProof.runIds);
  return [...consumed].filter((runId) => retainedRunIds.has(runId));
}

export function readPiPersistedConsumedDependencies(
  ctx: ExtensionContext,
  firstKeptEntryId?: string,
): PiDependencyRef[] | null {
  const branch = readPiOwnedBranch(ctx);
  if (branch === null) return firstKeptEntryId === undefined ? [] : null;
  const dependencies = new Map<string, PiDependencyRef>();
  const latestCompactionIndex = branch.reduce((found, entry, index) => entry.type === "compaction" ? index : found, -1);
  for (const entry of branch.slice(Math.max(0, latestCompactionIndex))) {
    if (entry.customType === PI_RESULT_CONSUMPTION_ENTRY) {
      const data = customData(entry) as unknown as PiResultConsumptionRecord | null;
      if (!validPiResultConsumptionRecord(data)) continue;
      for (const dependency of data.dependencies) dependencies.set(piDependencyKey(dependency), dependency);
    } else if (entry.customType === PI_COMPACTION_ENTRY) {
      const data = customData(entry) as unknown as PiCompactionRecord | null;
      if (!validPiCompactionRecord(data, entry, branch)) continue;
      for (const dependency of data.consumedDependencies ?? []) dependencies.set(piDependencyKey(dependency), dependency);
    }
  }
  if (firstKeptEntryId === undefined) return [...dependencies.values()].map((dependency) => ({ ...dependency }));

  const retainedIndex = readPiOwnedRetainedBoundaryIndex(ctx, branch, firstKeptEntryId);
  if (retainedIndex === null) return null;
  const retainedProof = readPiRetainedNotificationProof(ctx, branch, retainedIndex);
  if (retainedProof === null) return null;
  const retainedKeys = new Set(retainedProof.dependencies.map(piDependencyKey));
  return [...dependencies.values()]
    .filter((dependency) => retainedKeys.has(piDependencyKey(dependency)))
    .map((dependency) => ({ ...dependency }));
}

function readPiOwnedRetainedBoundaryIndex(
  ctx: ExtensionContext,
  ownedBranch: SessionEntryLike[],
  firstKeptEntryId: string,
): number | null {
  const fullBranch = entries(ctx as SessionContextLike);
  const boundaryIndexes = fullBranch.flatMap((entry, index) => entry.id === firstKeptEntryId ? [index] : []);
  if (boundaryIndexes.length !== 1) return null;
  const ownedStart = fullBranch.length - ownedBranch.length;
  if (ownedStart < 0) return null;
  if (ownedBranch.some((entry, index) => {
    const fullEntry = fullBranch[ownedStart + index];
    return !fullEntry || entry !== fullEntry &&
      (!isNativeId(entry.id) || entry.id !== fullEntry.id || entry.type !== fullEntry.type);
  })) return null;
  return Math.max(0, boundaryIndexes[0]! - ownedStart);
}

function readPiRetainedNotificationProof(
  ctx: ExtensionContext,
  branch: SessionEntryLike[],
  firstKeptIndex: number,
): { dependencies: PiDependencyRef[]; runIds: string[] } | null {
  const session = ctx as SessionContextLike;
  const sessionId = session.sessionManager.getSessionId();
  const parentSessionFile = session.sessionManager.getSessionFile?.();
  const retained: PiDependencyRef[] = [];
  const retainedRunIds = new Set<string>();
  for (let index = firstKeptIndex; index < branch.length; index++) {
    const entry = branch[index]!;
    if (entry.type === "custom_message" && entry.customType === "subagent-incremental-child-notify") {
      if (!readPiIncrementalChildNotice(entry)) return null;
      continue;
    }
    const inlineWorkflow = readPiInlineWorkflowResult(entry, parentSessionFile, branch.slice(0, index));
    if (inlineWorkflow === null) return null;
    if (inlineWorkflow) {
      if (inlineWorkflow.statusOnly) continue;
      const expectedRunIds = inlineWorkflow.children.flatMap((child) =>
        (child.state === "completed" || child.state === "failed") && isNativeId(child.runId) ? [child.runId] : [],
      );
      const committed = readPiCommittedDeliveryDependencies(branch, index, expectedRunIds);
      if (committed.kind === "ambiguous") return null;
      for (const child of inlineWorkflow.children) {
        const output = isNativeId(child.childId) && isNativeId(child.runId)
          ? inlineWorkflow.outputs.get(`${child.childId}:${child.runId}`)
          : undefined;
        if ((child.state !== "completed" && child.state !== "failed") || !isNativeId(child.childId) ||
            !isNativeId(child.runId) || output?.status !== child.state ||
            typeof output.output !== "string" && typeof output.preview !== "string") return null;
        const dependency = committed.dependenciesByRunId.get(child.runId);
        if (dependency) {
          retained.push({ ...dependency });
          retainedRunIds.add(child.runId);
        }
      }
      continue;
    }
    if (entry.type !== "custom_message" || entry.customType !== "subagent-notify") continue;
    const runs = (entry.details as { runs?: PiWorkflowNotificationRun[] } | undefined)?.runs;
    if (!Array.isArray(runs)) return null;
    for (const run of runs) {
      if (!isNativeId(run.runId)) return null;
      let retainedCommittedDependencies: ReadonlyMap<string, PiDependencyRef> | undefined;
      if (run.workflowRunId !== undefined) {
        const expectedRunIds = Array.isArray(run.childRuns)
          ? run.childRuns.flatMap((child) => child && (child.status === "completed" || child.status === "failed") && isNativeId(child.runId) ? [child.runId] : [])
          : [];
        const committed = readPiCommittedDeliveryDependencies(branch, index, expectedRunIds);
        if (committed.kind === "ambiguous") return null;
        retainedCommittedDependencies = committed.dependenciesByRunId;
      }
      const workflowDelivery = readPiWorkflowNotificationDependencies(
        entry,
        run,
        branch.slice(0, index),
        sessionId,
        parentSessionFile,
        retainedCommittedDependencies,
      );
      if (workflowDelivery !== undefined) {
        if (!workflowDelivery) return null;
        retained.push(...workflowDelivery.dependencies);
        for (const runId of workflowDelivery.consumedRunIds) retainedRunIds.add(runId);
        continue;
      }

      if (!parentSessionFile) return null;
      const directOutput = readPiDirectNotificationOutput(entry, run);
      if (directOutput === null) return null;
      if (directOutput.length === 0) continue;
      const committed = readPiRetainedDirectDeliveryProof(branch, firstKeptIndex, index, run.runId);
      if (committed.kind === "ambiguous") return null;
      retained.push(...committed.dependencies.map((dependency) => ({ ...dependency })));
      for (const runId of committed.runIds) retainedRunIds.add(runId);
      continue;
    }
  }
  return {
    dependencies: [...new Map(retained.map((dependency) => [piDependencyKey(dependency), dependency])).values()],
    runIds: [...retainedRunIds],
  };
}

type PiCommittedDeliveryProof = {
  kind: "none" | "ambiguous" | "committed";
  dependenciesByRunId: Map<string, PiDependencyRef>;
};

type PiCommittedConsumerReceipt = {
  kind: "none" | "ambiguous" | "committed";
  record?: PiResultConsumptionRecord;
  responseIndex?: number;
  previousAssistantIndex?: number;
};

type PiRetainedDirectProof = {
  kind: "none" | "ambiguous" | "committed";
  dependencies: PiDependencyRef[];
  runIds: string[];
};

function readPiCommittedConsumerReceipt(
  branch: SessionEntryLike[],
  deliveryIndex: number,
): PiCommittedConsumerReceipt {
  const responseIndex = branch.findIndex((entry, index) => index > deliveryIndex && entry.type === "message" && entry.message?.role === "assistant");
  if (responseIndex < 0) return { kind: "none" };
  const responseId = branch[responseIndex]!.message?.responseId;
  const responseEntries = branch.filter((entry) => entry.type === "message" && entry.message?.role === "assistant" && entry.message.responseId === responseId);
  const associatedRecords = branch.flatMap((entry, index) => {
    if (index <= responseIndex || entry.customType !== PI_RESULT_CONSUMPTION_ENTRY) return [];
    const data = customData(entry);
    if (data?.response_id !== responseId) return [];
    return [data as unknown as PiResultConsumptionRecord];
  });
  if (associatedRecords.length === 0) return { kind: "none", responseIndex };
  if (!isNativeId(responseId) || responseEntries.length !== 1 || associatedRecords.length !== 1) {
    return { kind: "ambiguous" };
  }
  const data = associatedRecords[0]!;
  if (!validPiResultConsumptionRecord(data)) return { kind: "ambiguous" };
  let previousAssistantIndex = -1;
  for (let index = responseIndex - 1; index >= 0; index--) {
    if (branch[index]?.type === "message" && branch[index]?.message?.role === "assistant") {
      previousAssistantIndex = index;
      break;
    }
  }
  return { kind: "committed", record: data, responseIndex, previousAssistantIndex };
}

function readPiCommittedDeliveryDependencies(
  branch: SessionEntryLike[],
  deliveryIndex: number,
  expectedRunIds: string[],
): PiCommittedDeliveryProof {
  const empty = (): PiCommittedDeliveryProof => ({ kind: "none", dependenciesByRunId: new Map() });
  if (expectedRunIds.length === 0) return empty();
  if (expectedRunIds.some((runId) => !isNativeId(runId)) ||
      new Set(expectedRunIds).size !== expectedRunIds.length) return { kind: "ambiguous", dependenciesByRunId: new Map() };
  const receipt = readPiCommittedConsumerReceipt(branch, deliveryIndex);
  if (receipt.kind === "none") return receipt.responseIndex === undefined
    ? empty()
    : { kind: "ambiguous", dependenciesByRunId: new Map() };
  if (receipt.kind === "ambiguous" || !receipt.record) return { kind: "ambiguous", dependenciesByRunId: new Map() };
  const data = receipt.record;
  const expected = new Set(expectedRunIds);
  const matching = data.dependencies.filter((dependency) => dependency.run_id && expected.has(dependency.run_id));
  if (matching.length === 0) {
    return data.dependencies.some((dependency) => dependency.run_id === undefined)
      ? { kind: "ambiguous", dependenciesByRunId: new Map() }
      : empty();
  }
  const byRunId = new Map<string, PiDependencyRef>();
  for (const dependency of matching) {
    const runId = dependency.run_id!;
    if (byRunId.has(runId)) return { kind: "ambiguous", dependenciesByRunId: new Map() };
    byRunId.set(runId, { ...dependency });
  }
  if (expectedRunIds.some((runId) => !byRunId.has(runId))) return { kind: "ambiguous", dependenciesByRunId: new Map() };
  return { kind: "committed", dependenciesByRunId: byRunId };
}

function readPiRetainedDirectDeliveryProof(
  branch: SessionEntryLike[],
  firstKeptIndex: number,
  deliveryIndex: number,
  currentRunId: string,
): PiRetainedDirectProof {
  const empty = (): PiRetainedDirectProof => ({ kind: "none", dependencies: [], runIds: [] });
  const receipt = readPiCommittedConsumerReceipt(branch, deliveryIndex);
  if (receipt.kind === "none") return receipt.responseIndex === undefined
    ? empty()
    : { kind: "ambiguous", dependencies: [], runIds: [] };
  if (receipt.kind === "ambiguous" || !receipt.record || receipt.responseIndex === undefined ||
      receipt.previousAssistantIndex === undefined) return { kind: "ambiguous", dependencies: [], runIds: [] };

  const directNotices: Array<{ index: number; runId: string }> = [];
  const workflowRunIds = new Set<string>();
  for (let index = receipt.previousAssistantIndex + 1; index < receipt.responseIndex; index += 1) {
    const entry = branch[index]!;
    const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
    const inlineDetails = message.details as { workflowChildren?: { children?: Array<{ runId?: unknown; state?: unknown }> } } | undefined;
    for (const child of Array.isArray(inlineDetails?.workflowChildren?.children) ? inlineDetails.workflowChildren.children : []) {
      if ((child.state === "completed" || child.state === "failed") && isNativeId(child.runId)) workflowRunIds.add(child.runId);
    }
    if (entry.type === "custom_message" && entry.customType === "subagent-notify") {
      const runs = (entry.details as { runs?: PiWorkflowNotificationRun[] } | undefined)?.runs;
      if (!Array.isArray(runs)) return { kind: "ambiguous", dependencies: [], runIds: [] };
      for (const run of runs) {
        if (run.workflowRunId !== undefined) {
          for (const child of Array.isArray(run.childRuns) ? run.childRuns : []) {
            if ((child.status === "completed" || child.status === "failed") && isNativeId(child.runId)) workflowRunIds.add(child.runId);
          }
        }
      }
    }
    if (entry.type !== "custom_message" || entry.customType !== "subagent-notify") continue;
    const runs = (entry.details as { runs?: PiWorkflowNotificationRun[] } | undefined)?.runs;
    if (!Array.isArray(runs)) return { kind: "ambiguous", dependencies: [], runIds: [] };
    for (const run of runs) {
      if (run.workflowRunId !== undefined) continue;
      if (!isNativeId(run.runId) || (run.status !== "completed" && run.status !== "failed")) continue;
      const output = readPiDirectNotificationOutput(entry, run);
      if (output === null) return { kind: "ambiguous", dependencies: [], runIds: [] };
      if (output.length > 0) directNotices.push({ index, runId: run.runId });
    }
  }
  if (!directNotices.some((notice) => notice.index === deliveryIndex && notice.runId === currentRunId) ||
      new Set(directNotices.map((notice) => notice.runId)).size !== directNotices.length) {
    return { kind: "ambiguous", dependencies: [], runIds: [] };
  }

  const dependencies = receipt.record.dependencies;
  const directRunIds = new Set(directNotices.map((notice) => notice.runId));
  const dependenciesByRunId = new Map<string, PiDependencyRef>();
  for (const dependency of dependencies) {
    if (!dependency.run_id || !directRunIds.has(dependency.run_id)) continue;
    if (dependenciesByRunId.has(dependency.run_id)) return { kind: "ambiguous", dependencies: [], runIds: [] };
    dependenciesByRunId.set(dependency.run_id, { ...dependency });
  }
  const unknownRunIds = dependencies.flatMap((dependency) => dependency.run_id &&
    !directRunIds.has(dependency.run_id) && !workflowRunIds.has(dependency.run_id) ? [dependency.run_id] : []);
  if (unknownRunIds.length > 0) return { kind: "ambiguous", dependencies: [], runIds: [] };

  const runlessDependencies = dependencies.filter((dependency) => dependency.run_id === undefined &&
    dependency.response_id !== undefined && dependency.stream_id === undefined);
  const unresolvedDirectNotices = directNotices.filter((notice) => !dependenciesByRunId.has(notice.runId));
  if (runlessDependencies.length > 0) {
    if (runlessDependencies.length !== unresolvedDirectNotices.length ||
        directNotices.some((notice) => notice.index < firstKeptIndex)) {
      return { kind: "ambiguous", dependencies: [], runIds: [] };
    }
    return {
      kind: "committed",
      dependencies: [...dependenciesByRunId.values(), ...runlessDependencies.map((dependency) => ({ ...dependency }))],
      runIds: [...dependenciesByRunId.keys()],
    };
  }
  const currentDependency = dependenciesByRunId.get(currentRunId);
  if (!currentDependency) return empty();
  return { kind: "committed", dependencies: [{ ...currentDependency }], runIds: [currentRunId] };
}

function readPiPersistedDependencyKeys(branch: SessionEntryLike[]): Set<string> {
  const keys = new Set<string>();
  const latestCompactionIndex = branch.reduce((found, entry, index) => entry.type === "compaction" ? index : found, -1);
  for (const entry of branch.slice(Math.max(0, latestCompactionIndex))) {
    if (entry.customType === PI_RESULT_CONSUMPTION_ENTRY) {
      const data = customData(entry) as unknown as PiResultConsumptionRecord | null;
      if (validPiResultConsumptionRecord(data)) for (const dependency of data.dependencies) keys.add(piDependencyKey(dependency));
    } else if (entry.customType === PI_COMPACTION_ENTRY) {
      const data = customData(entry) as unknown as PiCompactionRecord | null;
      if (validPiCompactionRecord(data, entry, branch)) {
        for (const dependency of data.consumedDependencies ?? []) keys.add(piDependencyKey(dependency));
      }
    }
  }
  return keys;
}

function validPiResultConsumptionRecord(data: PiResultConsumptionRecord | null): data is PiResultConsumptionRecord {
  return !!data && data.version === 1 && isNativeId(data.stream_id) && isNativeId(data.response_id) &&
    Array.isArray(data.dependencies) && data.dependencies.length > 0 && data.dependencies.length <= 8 &&
    data.dependencies.every(isValidPiDependencyRef) &&
    new Set(data.dependencies.map(piDependencyKey)).size === data.dependencies.length;
}

function isValidPiDependencyRef(ref: PiDependencyRef): boolean {
  return !!ref && isNativeId(ref.session_id) &&
    (isNativeId(ref.response_id) || isNativeId(ref.stream_id)) &&
    (ref.run_id === undefined || isNativeId(ref.run_id));
}

function piDependencyKey(ref: PiDependencyRef): string {
  return JSON.stringify([ref.session_id, ref.response_id ?? null, ref.stream_id ?? null, ref.run_id ?? null]);
}

function renderPiSubagentsNoticeContext(content: string): string {
  // Preserve the runner's notice boundary without using its version as an identity gate.
  return `<pi_subagents_notice type="subagent-notify" origin="automated message from the pi-subagents extension, not typed by the user">\n${content}\n</pi_subagents_notice>`;
}

function readPiDirectNotificationOutput(
  entry: SessionEntryLike,
  run: PiWorkflowNotificationRun,
  status?: PiSubagentStatus,
  parentSessionId?: string,
  parentSessionFile?: string,
): string | null {
  if (!isNativeId(run.runId) ||
      (run.status !== "completed" && run.status !== "failed") ||
      typeof run.agent !== "string" || run.agent.length === 0 ||
      typeof run.asyncDir !== "string" || run.asyncDir.length === 0 ||
      run.sessionLabel !== "Session file" || typeof run.sessionValue !== "string" ||
      status && (status.runId !== run.runId || status.mode !== "single") || typeof entry.content !== "string") return null;
  if (status) {
    const statusFiles = resolvePiStatusSessionFiles(status, parentSessionId, parentSessionFile);
    if (!statusFiles || statusFiles.size !== 1) return null;
    let renderedSessionFile: string;
    try {
      renderedSessionFile = realpathSync(run.sessionValue);
      if (!statusFiles.has(renderedSessionFile)) return null;
    } catch {
      return null;
    }
  }
  const lines = entry.content.split("\n");
  const expectedHeader = `Background task ${run.status}: **${run.agent}**`;
  if (lines[0] !== expectedHeader || lines[1] !== "" || lines[2] !== `${run.agent}:`) return null;
  const footerIndexes = lines.flatMap((line, index) => line.startsWith("Retention-managed async directory: ") ? [index] : []);
  if (footerIndexes.length !== 1) return null;
  const footerIndex = footerIndexes[0]!;
  if (footerIndex < 4 || lines[footerIndex - 1] !== "" ||
      lines[footerIndex] !== `Retention-managed async directory: ${run.asyncDir}` ||
      lines[footerIndex + 1] !== "" ||
      lines[footerIndex + 2] !== `Session file: ${run.sessionValue}` || lines[footerIndex + 3] !== "" ||
      !lines[footerIndex + 4]?.startsWith("Parent action: ") ||
      lines[footerIndex + 4]!.length === "Parent action: ".length || footerIndex + 5 !== lines.length) return null;
  const output = lines.slice(3, footerIndex - 1).join("\n");
  if (output.length === 0 || output.length > 32 * 1024) return null;
  return output;
}

function resolvePiDirectNotificationDependency(
  entry: SessionEntryLike,
  run: PiWorkflowNotificationRun,
  status: PiSubagentStatus,
  parentSessionId: string,
  parentSessionFile: string,
  parentBranch: SessionEntryLike[],
): { kind: "empty" } | { kind: "dependency"; dependency: PiDependencyRef; runId: string } | null {
  if (!isNativeId(run.runId) || (run.status !== "completed" && run.status !== "failed") ||
      status.parentWorkflowRunId !== undefined || status.mode !== "single" ||
      (status.state !== "complete" && status.state !== "failed") || !Array.isArray(status.steps) || status.steps.length !== 1 ||
      !readPiDelegationSource(status, run.runId, parentSessionId, parentSessionFile, parentBranch)) return null;
  const output = readPiDirectNotificationOutput(entry, run, status, parentSessionId, parentSessionFile);
  if (output === null) return null;
  if (output === "") return { kind: "empty" };
  const completion = readPiExactCommittedOutput(status, run.runId, parentSessionId, parentSessionFile, output, "direct") ??
    (run.status === "completed"
      ? readPiPinnedDirectPreviewCompletion(run, status, parentSessionId, parentSessionFile, output)
      : null);
  if (!completion) return null;
  return {
    kind: "dependency",
    runId: run.runId,
    dependency: {
      session_id: `pi:session:${completion.sessionId}`,
      response_id: completion.responseId,
      ...(readPiCompletionFromStatus(run.runId, status, parentSessionId, parentSessionFile)?.responseId === completion.responseId
        ? { run_id: completion.runId }
        : {}),
    },
  };
}

/**
 * Accept only the exact long-output preview emitted by the pinned pi-subagents
 * 0.83.0 direct notification renderer. The full artifact and current native
 * terminal response remain the identity proof; the preview is only a view.
 */
function readPiPinnedDirectPreviewCompletion(
  run: PiWorkflowNotificationRun,
  status: PiSubagentStatus,
  parentSessionId: string,
  parentSessionFile: string,
  preview: string,
): PiRunCompletion | null {
  if (!isNativeId(run.runId) || typeof run.agent !== "string" ||
      run.status !== "completed" || status.state !== "complete" ||
      status.mode !== "single" || !Array.isArray(status.steps) || status.steps.length !== 1 ||
      typeof status.artifactsDir !== "string" || typeof status.steps[0]?.sessionFile !== "string" ||
      status.steps[0].agent !== run.agent || status.steps[0].exitCode !== 0 ||
      typeof run.asyncDir !== "string" || typeof run.sessionValue !== "string") return null;

  const runId = run.runId;
  const resolvedSessionFiles = resolvePiStatusSessionFiles(status, parentSessionId, parentSessionFile);
  if (!resolvedSessionFiles || resolvedSessionFiles.size !== 1) return null;
  let statusSessionFile: string;
  try { statusSessionFile = realpathSync(status.steps[0].sessionFile as string); }
  catch { return null; }
  if (!resolvedSessionFiles.has(statusSessionFile)) return null;
  const statusArtifactsDir = status.artifactsDir as string;
  const asyncDir = run.asyncDir;
  const sessionValue = run.sessionValue;
  const agent = run.agent;
  const completion = readPiCompletionFromStatus(runId, status, parentSessionId, parentSessionFile);
  const terminal = readPiFreshChildTerminal(statusSessionFile, parentSessionFile, parentSessionId);
  if (!completion || !terminal || completion.sessionId !== terminal.sessionId ||
      completion.responseId !== terminal.responseId ||
      readPiLatestRunCompletionMarker(statusSessionFile, runId, terminal.sessionId) !== terminal.responseId ||
      terminal.output === null || terminal.output.length === 0 || terminal.artifactOutput === null ||
      terminal.artifactOutput.length <= 2_000 || terminal.artifactOutput.length > 32 * 1024) return null;

  // A stale completion marker or a later child turn cannot authorize an older
  // matching output: readPiCompletionFromStatus must identify this latest stop.
  let canonicalParent: string;
  let canonicalArtifactDir: string;
  let canonicalAsyncDir: string;
  let canonicalNoticeSession: string;
  try {
    canonicalParent = realpathSync(parentSessionFile);
    canonicalArtifactDir = realpathSync(statusArtifactsDir);
    canonicalAsyncDir = realpathSync(asyncDir);
    canonicalNoticeSession = realpathSync(sessionValue);
    const parentHeader = readNativeSessionHeader(canonicalParent);
    if (parentHeader?.id !== parentSessionId || parentHeader.parentSession ||
        readNativeSessionRootId(canonicalParent) !== parentSessionId ||
        canonicalArtifactDir !== join(dirname(canonicalParent), "subagent-artifacts") ||
        canonicalAsyncDir !== join(resolvePiSubagentsTempRoot(), "async-subagent-runs", runId) ||
        canonicalNoticeSession !== realpathSync(statusSessionFile)) return null;
  } catch {
    return null;
  }
  const safeAgent = agent.replace(/[^\w.-]/g, "_");
  if (!safeAgent || safeAgent === "." || safeAgent === "..") return null;
  const expectedArtifact = join(canonicalArtifactDir, `${runId}_${safeAgent}_output.md`);
  let artifact: string;
  let artifactFd: number | undefined;
  try {
    artifact = realpathSync(expectedArtifact);
    if (artifact !== expectedArtifact || dirname(artifact) !== canonicalArtifactDir) return null;
    artifactFd = openSync(artifact, "r");
    const stat = fstatSync(artifactFd);
    if (!stat.isFile() || stat.size <= 0 || stat.size > 8 * 1024 * 1024) return null;
    const bytes = Buffer.alloc(stat.size);
    if (readSync(artifactFd, bytes, 0, stat.size, 0) !== stat.size) return null;
    const fullOutput = bytes.toString("utf8");
    if (!Buffer.from(fullOutput, "utf8").equals(bytes) || fullOutput !== terminal.artifactOutput) return null;
    const expectedPreview = renderPiSubagents083InlinePreview(fullOutput, artifact);
    return preview === expectedPreview ? completion : null;
  } catch {
    return null;
  } finally {
    if (artifactFd !== undefined) closeSync(artifactFd);
  }
}

function readPiLatestRunCompletionMarker(sessionFile: string, runId: string, sessionId: string): string | null {
  let fd: number | undefined;
  try {
    fd = openSync(sessionFile, "r");
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.size <= 0 || stat.size > 8 * 1024 * 1024) return null;
    const buffer = Buffer.alloc(stat.size);
    if (readSync(fd, buffer, 0, stat.size, 0) !== stat.size) return null;
    const assistantResponseIds: string[] = [];
    const completionResponseIds: string[] = [];
    for (const line of buffer.toString("utf8").split("\n")) {
      if (!line) continue;
      let entry: SessionEntryLike;
      try { entry = JSON.parse(line) as SessionEntryLike; } catch { return null; }
      const message = entry.message && typeof entry.message === "object" ? entry.message : null;
      if (message?.role === "assistant" && isNativeId(message.responseId)) assistantResponseIds.push(message.responseId);
      if (entry.customType !== PI_RUN_COMPLETION_ENTRY) continue;
      const data = customData(entry);
      if (data?.runId === runId && data.sessionId === sessionId) {
        if (!isNativeId(data.responseId)) return null;
        completionResponseIds.push(data.responseId);
      }
    }
    const latest = assistantResponseIds.at(-1);
    if (!latest || completionResponseIds.length !== 1 || completionResponseIds[0] !== latest ||
        assistantResponseIds.filter((responseId) => responseId === latest).length !== 1) return null;
    return latest;
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

function renderPiSubagents083InlinePreview(output: string, artifactPath: string): string {
  if (output.length <= 2_000) return output;
  let headEnd = 2_000;
  const lineEnd = output.lastIndexOf("\n", headEnd);
  if (lineEnd >= 1_800) headEnd = lineEnd;
  if (/[\uD800-\uDBFF]/.test(output[headEnd - 1] ?? "")) headEnd -= 1;
  const byteSize = Buffer.byteLength(output, "utf8");
  const size = byteSize < 1_024 ? `${byteSize} B` : (() => {
    const units = ["KB", "MB", "GB", "TB"];
    let scaled = byteSize / 1_024;
    let unit = 0;
    while (scaled >= 1_024 && unit < units.length - 1) {
      scaled /= 1_024;
      unit += 1;
    }
    return `${scaled.toFixed(1)} ${units[unit]}`;
  })();
  const lines = output.split(/\r\n|\r|\n/).length - (/(?:\r\n|\r|\n)$/.test(output) ? 1 : 0);
  return `${output.slice(0, headEnd)}\n…\nFull output: ${artifactPath} (${size}, ${lines} ${lines === 1 ? "line" : "lines"}). Read it if needed.`;
}

/** Read a native Pi session UUID from its session-file header, never from a path. */
export function readPiNativeSessionId(sessionFile: string | undefined): string | null {
  return readNativeSessionHeader(sessionFile)?.id ?? null;
}

function customData(entry: SessionEntryLike): Record<string, unknown> | null {
  return entry.data && typeof entry.data === "object" ? entry.data as Record<string, unknown> : null;
}

interface SessionContextLike extends ExtensionContext {
  sessionManager: ExtensionContext["sessionManager"] & {
    getHeader?: () => { id?: string; parentSession?: string };
    getBranch?: () => SessionEntryLike[];
    getSessionFile?: () => string | undefined;
  };
}

function entries(ctx: SessionContextLike): SessionEntryLike[] {
  try {
    return ctx.sessionManager.getBranch?.() ?? [];
  } catch {
    return [];
  }
}

/** Copied transcript entries are context, not earlier inferences by this child. */
export function readPiOwnedBranch(ctx: ExtensionContext): SessionEntryLike[] | null {
  const session = ctx as SessionContextLike;
  const branch = entries(session);
  const parentFile = readNativeSessionHeader(session.sessionManager.getSessionFile?.())?.parentSession;
  if (!parentFile) return branch;
  let fd: number | undefined;
  try {
    fd = openSync(parentFile, "r");
    const size = fstatSync(fd).size;
    if (size <= 0 || size > 8 * 1024 * 1024) return null;
    const buffer = Buffer.alloc(size);
    if (readSync(fd, buffer, 0, size, 0) !== size) return null;
    const parentIds = new Set<string>();
    for (const line of buffer.toString("utf8").split("\n")) {
      if (!line) continue;
      const entry = JSON.parse(line) as SessionEntryLike;
      if (typeof entry.id === "string") parentIds.add(entry.id);
    }
    let boundary = 0;
    while (boundary < branch.length && typeof branch[boundary].id === "string" && parentIds.has(branch[boundary].id!)) boundary += 1;
    const owned = branch.slice(boundary);
    if (owned.some((entry) => !entry.id || parentIds.has(entry.id))) return null;
    return owned;
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

/** Return the inherited parent prefix used to bind a child run at its request boundary. */
export function readPiParentBranchForChild(ctx: ExtensionContext): SessionEntryLike[] | null {
  const session = ctx as SessionContextLike;
  const header = readNativeSessionHeader(session.sessionManager.getSessionFile?.());
  if (!header?.parentSession) return null;
  const fullBranch = entries(session);
  const ownedBranch = readPiOwnedBranch(ctx);
  if (ownedBranch === null || ownedBranch.length > fullBranch.length) return null;
  return fullBranch.slice(0, fullBranch.length - ownedBranch.length);
}

export function derivePiIdentity(
  ctx: ExtensionContext,
  input: {
    forkedRoot?: boolean;
    harnessVersion?: string;
    runnerVersion?: string;
    resultDependencies?: AgentMetadata["depends_on"];
    delegation?: MetadataIdentity["delegation"];
    childLaunches?: ChildLaunchReceipt[];
  } = {},
): MetadataIdentity {
  const session = ctx as SessionContextLike;
  const header = session.sessionManager.getHeader?.();
  const sessionId = session.sessionManager.getSessionId();
  const fullBranch = readPiOwnedBranch(ctx);
  const branchUnavailable = fullBranch === null;
  const forkBoundaryIndex = readPiForkBoundaryIndex(session);
  const isForkedRoot = input.forkedRoot === true || forkBoundaryIndex !== null;
  const branch = forkBoundaryIndex !== null
    ? entries(session).slice(forkBoundaryIndex + 1)
    : input.forkedRoot
      ? []
      : fullBranch ?? [];
  const nativeHeader = readNativeSessionHeader(session.sessionManager.getSessionFile?.());
  const headerIsBound = nativeHeader?.id === sessionId;
  const nativeParentFile = nativeHeader?.parentSession;
  const parentSessionId = isForkedRoot ? null : readPiNativeSessionId(nativeParentFile);
  const nativeRootId = isForkedRoot
    ? sessionId
    : nativeParentFile
      ? readNativeSessionRootId(nativeParentFile)
      : sessionId;
  const runnerRootId = process.env.PI_SUBAGENT_PARENT_SESSION;
  const managerParentMatches = isForkedRoot || (!header?.parentSession && !nativeParentFile)
    ? true
    : !!header?.parentSession && !!nativeParentFile && sameNativeSessionPath(header.parentSession, nativeParentFile);
  const rootSessionId = !headerIsBound || !managerParentMatches || (!isForkedRoot && runnerRootId && runnerRootId !== nativeRootId)
    ? null
    : nativeRootId;
  let previousResponseId: string | null = null;
  let hasPriorHistory = branchUnavailable && !isForkedRoot;
  for (let index = branch.length - 1; index >= 0; index -= 1) {
    const entry = branch[index];
    if (entry.type !== "message" || entry.message?.role !== "assistant") continue;
    hasPriorHistory = true;
    previousResponseId = typeof entry.message.responseId === "string" ? entry.message.responseId : null;
    break;
  }
  if (previousResponseId && branch.filter((entry) => entry.type === "message" && entry.message?.role === "assistant" && entry.message.responseId === previousResponseId).length > 1) {
    previousResponseId = null;
  }

  const latestNativeCompactionIndex = branch.reduce((found, entry, index) => entry.type === "compaction" ? index : found, -1);
  const latestNativeCompaction = latestNativeCompactionIndex >= 0 ? branch[latestNativeCompactionIndex] : undefined;
  const latestCompaction = latestNativeCompaction
    ? branch.find((entry) => entry.customType === PI_COMPACTION_ENTRY && customData(entry)?.compactionEntryId === latestNativeCompaction.id)
    : [...branch].reverse().find((entry) => entry.customType === PI_COMPACTION_ENTRY);
  const compactionRecord = latestCompaction ? customData(latestCompaction) as unknown as PiCompactionRecord : undefined;
  const hasNativeCompaction = branch.some((entry) => entry.type === "compaction");
  const compactionHasBeenConsumed = latestNativeCompactionIndex >= 0 && branch.slice(latestNativeCompactionIndex + 1).some((entry) =>
    entry.type === "message" && entry.message?.role === "assistant",
  );
  const compactionDependencies = compactionHasBeenConsumed
    ? []
    : !latestCompaction
      ? (hasNativeCompaction ? null : [])
    : !validPiCompactionRecord(compactionRecord, latestCompaction, branch)
      ? null
    : compactionRecord!.contributors.map((ref) => ({
      session_id: `pi:session:${sessionId}`,
      stream_id: ref.stream_id,
    }));
  const dependsOn = isForkedRoot && branch.length === 0
    ? []
    : branchUnavailable && !isForkedRoot || compactionDependencies === null || input.resultDependencies === null
    ? null
    : [...compactionDependencies, ...(input.resultDependencies ?? [])];

  return {
    harness: "pi",
    harnessVersion: input.harnessVersion ?? "unknown",
    extensionVersion: PI_EXTENSION_VERSION,
    runnerVersion: input.runnerVersion,
    sessionId,
    rootSessionId,
    parentSessionId,
    previousResponseId,
    hasPriorHistory,
    delegation: input.delegation ?? null,
    dependsOn,
    childLaunches: input.childLaunches,
  };
}

function sameNativeSessionPath(left: string, right: string): boolean {
  try { return realpathSync(left) === realpathSync(right); } catch { return left === right; }
}

export function readLatestPiCompactionContributors(ctx: ExtensionContext, startIndex = 0): string[] | null {
  const branch = entries(ctx as SessionContextLike).slice(Math.max(0, startIndex));
  const latestNative = [...branch].reverse().find((entry) => entry.type === "compaction");
  if (!latestNative) return [];
  const latestNativeIndex = branch.lastIndexOf(latestNative);
  if (branch.slice(latestNativeIndex + 1).some((entry) => entry.type === "message" && entry.message?.role === "assistant")) return [];
  const recordEntry = branch.find((entry) =>
    entry.customType === PI_COMPACTION_ENTRY && customData(entry)?.compactionEntryId === latestNative.id,
  );
  const data = recordEntry ? customData(recordEntry) as unknown as PiCompactionRecord : undefined;
  if (!recordEntry) return null;
  if (!validPiCompactionRecord(data, recordEntry, branch)) return null;
  return data!.contributors.map((ref) => ref.stream_id);
}

export function readLatestPiCompactionContributorWitnesses(ctx: ExtensionContext): PiDependencyDeliveryWitness[] | null {
  const session = ctx as SessionContextLike;
  const branch = readPiOwnedBranch(ctx);
  if (branch === null) return null;
  const latestNative = [...branch].reverse().find((entry) => entry.type === "compaction");
  if (!latestNative) return [];
  const latestIndex = branch.lastIndexOf(latestNative);
  if (branch.slice(latestIndex + 1).some((entry) => entry.type === "message" && entry.message?.role === "assistant")) return [];
  const record = branch.find((entry) => entry.customType === PI_COMPACTION_ENTRY && customData(entry)?.compactionEntryId === latestNative.id);
  const data = record ? customData(record) as unknown as PiCompactionRecord : undefined;
  const summary = latestNative.summary;
  if (!record || !validPiCompactionRecord(data, record, branch) || typeof summary !== "string" || summary.length === 0) return null;
  const sessionId = session.sessionManager.getSessionId();
  return data!.contributors.map(({ stream_id }) => ({
    dependency: { session_id: `pi:session:${sessionId}`, stream_id },
    deliveredText: summary,
  }));
}

export function latestNativeCompactionId(branch: readonly NativeCompactionLike[]): string | null {
  for (let index = branch.length - 1; index >= 0; index -= 1) {
    if (branch[index]?.type === "compaction" && isNativeId(branch[index]?.id)) return branch[index]!.id!;
  }
  return null;
}

export function readLatestPiCompactionConsumedRuns(ctx: ExtensionContext, startIndex = 0): string[] | null {
  const branch = entries(ctx as SessionContextLike).slice(Math.max(0, startIndex));
  const native = [...branch].reverse().find((entry) => entry.type === "compaction");
  if (!native) return [];
  const record = branch.find((entry) => entry.customType === PI_COMPACTION_ENTRY && customData(entry)?.compactionEntryId === native.id);
  const data = record ? customData(record) as unknown as PiCompactionRecord : null;
  if (!validPiCompactionRecord(data, record, branch)) return null;
  const consumed = (customData(record!)?.consumedRunIds ?? []) as unknown;
  return Array.isArray(consumed) && consumed.every(isNativeId) ? consumed : null;
}

function validPiCompactionRecord(
  data: PiCompactionRecord | null | undefined,
  recordEntry: SessionEntryLike | undefined,
  branch: SessionEntryLike[],
): data is PiCompactionRecord {
  if (!data || data.available !== true || !Array.isArray(data.contributors)) return false;
  if (data.contributors.length === 0 && data.provenNone !== true) return false;
  if (!data.contributors.every((ref) => ref !== null && typeof ref === "object" && isNativeId(ref.stream_id))) return false;
  if (data.consumedDependencies !== undefined && (!Array.isArray(data.consumedDependencies) ||
      data.consumedDependencies.length > 8 || !data.consumedDependencies.every(isValidPiDependencyRef) ||
      new Set(data.consumedDependencies.map(piDependencyKey)).size !== data.consumedDependencies.length)) return false;
  if (data.pendingChildLaunches !== undefined && (!Array.isArray(data.pendingChildLaunches) ||
      data.pendingChildLaunches.length > MAX_PENDING_CHILD_LAUNCHES ||
      !data.pendingChildLaunches.every(isChildLaunchReceipt) ||
      new Set(data.pendingChildLaunches.map(childLaunchKey)).size !== data.pendingChildLaunches.length)) return false;
  const native = branch.find((entry) => entry.type === "compaction" && entry.id === data.compactionEntryId);
  return !!recordEntry && recordEntry.customType === PI_COMPACTION_ENTRY && !!native;
}

export function readPiForkBoundaryIndex(ctx: ExtensionContext): number | null {
  const session = ctx as SessionContextLike;
  const sessionId = session.sessionManager.getSessionId();
  const branch = entries(session);
  const index = branch.findIndex((entry) => entry.customType === PI_FORK_ROOT_ENTRY && customData(entry)?.sessionId === sessionId);
  return index < 0 ? null : index;
}

export function makePiCompactionRecord(
  compactionEntryId: string,
  streamIds: readonly string[],
  available = true,
  consumedRunIds: readonly string[] = [],
  provenNone = false,
  consumedDependencies: readonly PiDependencyRef[] = [],
  pendingChildLaunches: readonly ChildLaunchReceipt[] = [],
): {
  compactionEntryId: string;
  contributors: Array<{ stream_id: string }>;
  consumedRunIds: string[];
  consumedDependencies: PiDependencyRef[];
  pendingChildLaunches?: ChildLaunchReceipt[];
  available: boolean;
  provenNone: boolean;
} {
  return {
    compactionEntryId,
    contributors: [...new Set(streamIds)].map((stream_id) => ({ stream_id })),
    consumedRunIds: [...new Set(consumedRunIds)],
    consumedDependencies: [...new Map(consumedDependencies.filter(isValidPiDependencyRef).map((dependency) => [piDependencyKey(dependency), { ...dependency }])).values()],
    ...(pendingChildLaunches.length ? { pendingChildLaunches: pendingChildLaunches.map((receipt) => ({ ...receipt })) } : {}),
    available,
    provenNone,
  };
}
