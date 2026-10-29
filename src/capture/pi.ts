import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { closeSync, fstatSync, openSync, readFileSync, readSync, realpathSync } from "fs";
import { homedir, tmpdir, userInfo } from "os";
import { basename, dirname, join, relative, resolve } from "path";
import { readNativeSessionHeader, readNativeSessionRootId } from "./native-session.ts";
import { encodeAgentMetadata, type AgentMetadata, type MetadataIdentity } from "./shared.ts";

export const PI_EXTENSION_VERSION = "1.1.0-dev";
export const PI_RUNNER_VERSION = "0.83.0";
export const PI_COMPACTION_ENTRY = "tokenvisor-agent-compaction-v1";
export const PI_FORK_ROOT_ENTRY = "tokenvisor-agent-fork-root-v1";
export const PI_RUN_COMPLETION_ENTRY = "tokenvisor-agent-run-completion-v1";

interface SessionEntryLike {
  id?: string;
  type?: string;
  customType?: string;
  role?: string;
  content?: unknown;
  toolCallId?: unknown;
  data?: unknown;
  parentId?: string | null;
  details?: unknown;
  message?: { role?: string; responseId?: string; content?: unknown };
}

interface PiCompactionRecord {
  compactionEntryId: string;
  contributors: Array<{ stream_id: string }>;
  consumedRunIds?: string[];
  available?: boolean;
  provenNone?: boolean;
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
  steps?: Array<{ sessionFile?: unknown }>;
}

interface NativeCompactionLike {
  type?: string;
  id?: string;
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
  extensionProvidedCompaction: boolean,
): CompactionFrontier {
  const observed = [...new Set(current)];
  const provenMechanical = extensionProvidedCompaction && observed.length === 0 && !failed;
  if (previous === null || failed) return { available: false, streamIds: observed, provenNone: false };
  const frontier = provenMechanical ? [...new Set(previous)] : observed;
  if (!provenMechanical && observed.length === 0) return { available: false, streamIds: [], provenNone: false };
  if (frontier.length > 8) return { available: false, streamIds: frontier, provenNone: false };
  return { available: true, streamIds: frontier, provenNone: provenMechanical && frontier.length === 0 };
}

function isNativeId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 256 && /^[A-Za-z0-9._:-]+$/.test(value);
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
  if (!status || status.lifecycleArtifactVersion !== 3 || status.runId !== runId || !stateIsUsable || status.mode !== "single" || status.steps?.length !== 1 || String(status.pid) !== runnerPid) return null;
  if (!statusParentMatches(status.sessionId, parentSessionId, canonicalParentFile)) return null;
  const files = [status.sessionFile, ...(status.steps ?? []).map((step) => step.sessionFile)];
  const exactChildFile = files.some((file) => {
    if (typeof file !== "string") return false;
    try { return realpathSync(file) === canonicalSessionFile; } catch { return false; }
  });
  if (!exactChildFile && !(status.sessionFile === undefined && status.steps?.[0]?.sessionFile == null)) return null;
  if (status.toolCallId !== undefined && !isNativeId(status.toolCallId)) return null;
  const toolCallId = readPiParentToolCallId(
    canonicalParentFile,
    parentSessionId,
    runId,
    typeof status.toolCallId === "string" ? status.toolCallId : undefined,
  );
  if (!toolCallId) return null;
  // The current runner PID plus native parent/child headers binds startup before
  // pi-subagents publishes the step session path.
  return { runId, toolCallId };
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
    readSync(fd, buffer, 0, buffer.length, 0);
    const toolResults: string[] = [];
    const subagentCalls: string[] = [];
    for (const line of buffer.toString("utf8").split("\n")) {
      if (!line) continue;
      let entry: SessionEntryLike;
      try { entry = JSON.parse(line) as SessionEntryLike; } catch { return null; }
      const message = (entry.message && typeof entry.message === "object" ? entry.message : entry) as SessionEntryLike;
      if (message.role === "toolResult") {
        const details = message.details as { runId?: unknown } | undefined;
        if (details?.runId === runId && isNativeId(message.toolCallId)) toolResults.push(message.toolCallId);
      }
      if (message.role === "assistant" && Array.isArray(message.content)) {
        for (const part of message.content as Array<{ type?: unknown; id?: unknown; name?: unknown }>) {
          if ((part.type === "toolCall" || part.type === "tool_call") && part.name === "subagent" && isNativeId(part.id)) {
            subagentCalls.push(part.id);
          }
        }
      }
    }
    const results = [...new Set(toolResults)];
    const candidate = preferredToolCallId ?? (toolResults.length === 1 ? toolResults[0] : undefined);
    if (!candidate || !isNativeId(candidate) || subagentCalls.filter((id) => id === candidate).length !== 1) return null;
    if (toolResults.length > 1 || results.length === 1 && results[0] !== candidate) return null;
    return candidate;
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
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
      const manifest = JSON.parse(readFileSync(join(candidate, "package.json"), "utf8")) as { name?: unknown; version?: unknown };
      if (manifest.name === "pi-subagents" && manifest.version === PI_RUNNER_VERSION) {
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

export function readPiRunCompletion(runId: string, parentSessionId: string, toolCallId: string, parentSessionFile?: string): PiRunCompletion | null {
  const status = readPiSubagentStatus(runId, parentSessionId, parentSessionFile);
  if (!status || status.toolCallId !== undefined && status.toolCallId !== toolCallId || status.state !== "complete" || status.mode !== "single" || status.steps?.length !== 1) return null;
  const sessionFiles = [status.sessionFile, ...(status.steps ?? []).map((step) => step.sessionFile)]
    .filter((file): file is string => typeof file === "string");
  const completions = new Map<string, PiRunCompletion>();
  for (const sessionFile of sessionFiles) {
    try {
      const nativeHeader = readNativeSessionHeader(sessionFile);
      if (!nativeHeader?.parentSession || readNativeSessionHeader(nativeHeader.parentSession)?.id !== parentSessionId) continue;
      if (parentSessionFile) {
        try { if (realpathSync(nativeHeader.parentSession) !== realpathSync(parentSessionFile)) continue; }
        catch { continue; }
      }
      const fd = openSync(sessionFile, "r");
      try {
        const stat = fstatSync(fd);
        const length = Math.min(stat.size, 4 * 1024 * 1024);
        const buffer = Buffer.alloc(length);
        readSync(fd, buffer, 0, length, stat.size - length);
        const lines = buffer.toString("utf8").split("\n");
        for (const line of lines.reverse()) {
          if (!line) continue;
          const entry = JSON.parse(line) as SessionEntryLike;
          if (entry.customType !== PI_RUN_COMPLETION_ENTRY) continue;
          const data = customData(entry);
          if (data?.runId === runId && data.sessionId === nativeHeader.id && isNativeId(data.sessionId) && isNativeId(data.responseId)) {
            completions.set(`${data.sessionId}:${data.responseId}`, {
              runId,
              sessionId: data.sessionId,
              responseId: data.responseId,
            });
            break;
          }
        }
      } finally {
        closeSync(fd);
      }
    } catch {
      // Missing or partial native files leave this dependency unresolved.
    }
  }
  return completions.size === 1 ? [...completions.values()][0] : null;
}

export function readPiUnconsumedRunDependencies(ctx: ExtensionContext, onResolvedRun?: (runId: string) => void): AgentMetadata["depends_on"] {
  const session = ctx as SessionContextLike;
  const sessionId = session.sessionManager.getSessionId();
  const fullBranch = entries(session);
  const forkBoundaryIndex = readPiForkBoundaryIndex(session);
  const branch = forkBoundaryIndex === null ? fullBranch : fullBranch.slice(forkBoundaryIndex + 1);
  let lastAssistantIndex = -1;
  for (let index = 0; index < branch.length; index += 1) {
    if (branch[index].type === "message" && branch[index].message?.role === "assistant") lastAssistantIndex = index;
  }
  const dependencies: NonNullable<AgentMetadata["depends_on"]> = [];
  let unresolved = false;
  const compactionConsumedRunIds = new Set<string>();
  const latestCompactionIndex = branch.reduce((found, entry, index) => entry.type === "compaction" ? index : found, -1);
  for (const entry of branch) {
    if (entry.customType !== PI_COMPACTION_ENTRY) continue;
    const data = customData(entry);
    if (!validPiCompactionRecord(data as unknown as PiCompactionRecord | null, entry, branch)) continue;
    for (const runId of (data?.consumedRunIds as unknown[] | undefined) ?? []) {
      if (isNativeId(runId)) compactionConsumedRunIds.add(runId);
    }
  }
  for (const entry of branch.slice(lastAssistantIndex + 1)) {
    if (entry.type !== "custom_message" || entry.customType !== "subagent-notify") continue;
    const runs = (entry.details as { runs?: Array<{ runId?: unknown; toolCallId?: unknown; status?: unknown }> } | undefined)?.runs;
    if (!Array.isArray(runs)) {
      unresolved = true;
      continue;
    }
    for (const run of runs) {
      if (run.status !== "completed") continue;
      if (!isNativeId(run.runId)) {
        unresolved = true;
        continue;
      }
      if (compactionConsumedRunIds.has(run.runId)) continue;
      if (latestCompactionIndex > branch.indexOf(entry)) {
        const nativeCompaction = branch[latestCompactionIndex];
        const record = branch.find((item) => item.customType === PI_COMPACTION_ENTRY && customData(item)?.compactionEntryId === nativeCompaction.id);
        const recordData = record ? customData(record) as unknown as PiCompactionRecord : null;
        if (!validPiCompactionRecord(recordData, record, branch) || !compactionConsumedRunIds.has(run.runId)) {
          unresolved = true;
        }
        continue;
      }
      const parentSessionFile = session.sessionManager.getSessionFile?.();
      const status = readPiSubagentStatus(run.runId, sessionId, parentSessionFile);
      const toolCallId = parentSessionFile
        ? readPiParentToolCallId(parentSessionFile, sessionId, run.runId, isNativeId(status?.toolCallId) ? status.toolCallId : undefined)
        : null;
      if (!toolCallId) {
        unresolved = true;
        continue;
      }
      const completion = readPiRunCompletion(run.runId, sessionId, toolCallId, parentSessionFile);
      if (!completion) {
        unresolved = true;
        continue;
      }
      dependencies.push({
        session_id: `pi:session:${completion.sessionId}`,
        response_id: completion.responseId,
        run_id: completion.runId,
      });
      onResolvedRun?.(run.runId);
    }
  }
  if (unresolved || dependencies.length > 8) return null;
  return dependencies;
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

export function derivePiIdentity(
  ctx: ExtensionContext,
  input: { forkedRoot?: boolean; runnerVersion?: string; resultDependencies?: AgentMetadata["depends_on"]; delegation?: MetadataIdentity["delegation"] } = {},
): MetadataIdentity {
  const session = ctx as SessionContextLike;
  const header = session.sessionManager.getHeader?.();
  const sessionId = session.sessionManager.getSessionId();
  const fullBranch = entries(session);
  const forkBoundaryIndex = readPiForkBoundaryIndex(session);
  const isForkedRoot = input.forkedRoot === true || forkBoundaryIndex !== null;
  const branch = forkBoundaryIndex !== null
    ? fullBranch.slice(forkBoundaryIndex + 1)
    : input.forkedRoot
      ? []
      : fullBranch;
  const nativeHeader = readNativeSessionHeader(session.sessionManager.getSessionFile?.());
  const headerIsBound = nativeHeader?.id === sessionId;
  const nativeParentFile = nativeHeader?.parentSession;
  const hasNativeParent = Boolean(nativeParentFile);
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
  let hasPriorHistory = false;
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
    : compactionDependencies === null || input.resultDependencies === null
    ? null
    : [...compactionDependencies, ...(input.resultDependencies ?? [])];

  return {
    harness: "pi",
    harnessVersion: "1.4.0",
    extensionVersion: PI_EXTENSION_VERSION,
    runnerVersion: input.runnerVersion,
    sessionId,
    rootSessionId,
    parentSessionId,
    previousResponseId,
    hasPriorHistory,
    delegation: input.delegation ?? null,
    dependsOn,
  };
}

function sameNativeSessionPath(left: string, right: string): boolean {
  try { return realpathSync(left) === realpathSync(right); } catch { return left === right; }
}

export function createPiEnvelopeForStream(
  ctx: ExtensionContext,
  streamId: string,
  input: {
    forkedRoot?: boolean;
    runnerVersion?: string;
    delegation?: MetadataIdentity["delegation"];
    resultDependencies?: AgentMetadata["depends_on"];
  } = {},
): string | null {
  const metadata = derivePiIdentity(ctx, input);
  return encodeAgentMetadata(metadata, streamId);
}

export function readPiCompactionContributors(ctx: ExtensionContext, compactionEntryId: string): string[] | null {
  const branch = entries(ctx as SessionContextLike);
  const recordEntry = branch.find((entry) => entry.customType === PI_COMPACTION_ENTRY && customData(entry)?.compactionEntryId === compactionEntryId);
  const data = recordEntry ? customData(recordEntry) as unknown as PiCompactionRecord : undefined;
  if (!recordEntry) return branch.some((entry) => entry.type === "compaction" && entry.id === compactionEntryId) ? null : [];
  if (!validPiCompactionRecord(data, recordEntry, branch)) return null;
  return data!.contributors.map((ref) => ref.stream_id);
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

export function isPiForkRoot(ctx: ExtensionContext): boolean {
  return readPiForkBoundaryIndex(ctx) !== null;
}

export function makePiCompactionRecord(
  compactionEntryId: string,
  streamIds: readonly string[],
  available = true,
  consumedRunIds: readonly string[] = [],
  provenNone = false,
): { compactionEntryId: string; contributors: Array<{ stream_id: string }>; consumedRunIds: string[]; available: boolean; provenNone: boolean } {
  return {
    compactionEntryId,
    contributors: [...new Set(streamIds)].map((stream_id) => ({ stream_id })),
    consumedRunIds: [...new Set(consumedRunIds)],
    available,
    provenNone,
  };
}
