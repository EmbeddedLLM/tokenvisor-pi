/**
 * TokenVisor Provider Extension
 *
 * Dynamic model discovery from https://devstudio.tokenvisor.ai
 *
 * Works under both pi (earendil-works) and oh-my-pi (omp). The extension API
 * (`ExtensionAPI`, `registerProvider`, `registerCommand`) is identical across
 * both hosts; only credential storage differs:
 *
 *   - pi:  /login → "Use an API key" stores the key in ~/.pi/agent/auth.json
 *   - omp: /login is OAuth-only and has NO API-key entry UI, so custom
 *          API-key providers are expected to source their key from an env var.
 *
 * To bridge the two, this extension:
 *   - Resolves the key from the env var or ~/.pi/agent/auth.json, then mirrors
 *     it into process.env.TOKENVISOR_API_KEY before registering the provider.
 *     When available, the resolved key is passed directly as the provider's
 *     apiKey because Pi and OMP interpret env-var references differently.
 *   - Registers a /tokenvisor-login command that interactively prompts for the
 *     key (since omp's /login won't list TokenVisor), persists it to
 *     ~/.pi/agent/auth.json (shared with pi's /login), and reloads.
 *
 * Usage:
 *   pi    # /login → "Use an API key" → TokenVisor → enter your API key → /reload
 *         #   — or — /tokenvisor-login
 *   omp   # /tokenvisor-login  (omp /login is OAuth-only and won't list TokenVisor)
 *         #   — or — export TOKENVISOR_API_KEY=...
 *   # Then /model to select a TokenVisor model
 *
 * Base URL resolution (in order):
 *   1. TOKENVISOR_BASE_URL environment variable
 *   2. Fallback: https://devstudio.tokenvisor.ai/api/v1
 *
 * API key resolution (in order):
 *   1. TOKENVISOR_API_KEY environment variable (omp-idiomatic; always honored)
 *   2. ~/.pi/agent/auth.json (set via pi /login or /tokenvisor-login)
 * The resolved key is mirrored into TOKENVISOR_API_KEY at load time for the
 * current process, then registered directly as apiKey when available.
 */

// Type-only import: erased at transpile time, so it resolves under pi
// (@earendil-works/pi-coding-agent) and is a no-op under omp
// (@oh-my-pi/pi-coding-agent) without a runtime module lookup.
import type { ExtensionAPI, ProviderConfig } from "@earendil-works/pi-coding-agent";
import { appendFileSync, chmodSync, readFileSync, writeFileSync, existsSync, mkdirSync, renameSync } from "fs";
import { randomUUID } from "crypto";
import { dirname, join } from "path";
import { homedir } from "os";
import { fileURLToPath, pathToFileURL } from "url";
import {
  AGENT_HEADER,
  claimFreshRequestMetadata,
  clearAgentMetadataHeader,
  headerValue,
  parseAgentMetadata,
  recordFreshRequestMetadata,
  stripAgentMetadataHeader,
  withAgentMetadataHeader,
  writeAgentMetadataHeader,
  type AgentMetadata,
} from "../src/capture/shared.ts";
import { findPiAiPackageRoot } from "../src/capture/pi-runtime.ts";
import {
  detectRuntimeHost,
  isNativeForkStartup,
  readOmpHarnessVersion,
  readPiHarnessVersion,
  readPiSubagentRunnerVersion,
  resolvePiSubagentPackageRoot,
  supportsAgentMetadataCapture,
  type RuntimeHost,
} from "../src/capture/runtime-host.ts";
import { readNativeSessionHeader } from "../src/capture/native-session.ts";
import {
  createPiEnvelopeForStream,
  isPiForkRoot,
  makePiCompactionRecord,
  PI_COMPACTION_ENTRY,
  PI_FORK_ROOT_ENTRY,
  PI_RUN_COMPLETION_ENTRY,
  readPiNativeSessionId,
  readPiSubagentDelegation,
  readPiUnconsumedRunDependencies,
  readLatestPiCompactionContributors,
  readLatestPiCompactionConsumedRuns,
  resolveCompactionFrontier,
  latestNativeCompactionId,
} from "../src/capture/pi.ts";
import {
  createOmpEnvelope,
  createOmpSequenceState,
  deriveOmpSessionLineage,
  getOmpRuntimeCaptureState,
  OMP_RUN_COMPLETION_ENTRY,
  pendingOmpCompactionDependencies,
  recordOmpSequenceResult,
  recordOmpSequenceStream,
  readOmpForkBoundaryIndex,
  readOmpTaskDelegation,
  readOmpTaskResultDependencies,
  recordOmpCompactionResponse,
  resolveOmpNativeSequence,
} from "../src/capture/omp.ts";

const ENV_KEY = "TOKENVISOR_API_KEY";
const DEFAULT_BASE_URL = "https://devstudio.tokenvisor.ai/api/v1";
const AUTH_PATH = join(homedir(), ".pi", "agent", "auth.json");

export function providerApiKey(apiKey: string | undefined): string {
  // Pi supports $ENV_VAR references; OMP instead looks up the entire string as
  // an env-var name. Passing the resolved key works under both hosts.
  return apiKey ?? ENV_KEY;
}

interface TokenVisorModel {
  id: string;
  name: string;
  type: string;
  capabilities: string[];
  context_length: number | null;
  max_output_tokens: number | null;
}

interface TokenVisorModelList {
  object: string;
  data: TokenVisorModel[];
}

type ModelSpec = {
  id: string;
  name: string;
  reasoning: boolean;
  input: ("text" | "image")[];
  contextWindow: number;
  maxTokens: number;
  cost: { input: number; output: number; cacheRead: number; cacheWrite: number };
  /** Session-correlation compat: pi sends x-session-id; omp sends it via promptCacheSessionHeader. */
  compat?: typeof SESSION_COMPAT;
};

type CaptureStream = NonNullable<ProviderConfig["streamSimple"]>;
type CaptureStreamOptions = Parameters<CaptureStream>[2];
type CaptureContext = { sessionId: string; streamId: string };

const CAPTURE_API = "tokenvisor-openai-completions-capture";

function appendMetadataDiagnostic(path: string | undefined, event: Record<string, unknown>): void {
  if (!path) return;
  try {
    mkdirSync(dirname(path), { recursive: true });
    if (existsSync(path)) chmodSync(path, 0o600);
    appendFileSync(path, `${JSON.stringify({ type: "agent_metadata", ...event })}\n`, { mode: 0o600 });
  } catch (error) {
    console.warn(`[tokenvisor] Metadata diagnostic write failed: ${error instanceof Error ? error.name : "Error"}`);
  }
}

function observeCaptureResult<T extends { result: () => Promise<{ responseId?: string; stopReason?: string; content?: unknown }> }>(
  stream: T,
  context: CaptureContext,
  capturePath: string | undefined,
  runtimeHost: RuntimeHost,
  onTerminal: (streamId: string, responseId: string | null, stopReason: string | undefined, content: unknown) => void,
): T {
  const originalResult = stream.result.bind(stream);
  let recorded = false;
  const terminal = originalResult();
  const record = (responseId?: string, stopReason?: string, content?: unknown) => {
    if (recorded) return;
    recorded = true;
    const responseSucceeded = stopReason === "stop" || stopReason === "toolUse" || stopReason === "length";
    const safeResponseId = responseSucceeded && typeof responseId === "string" && /^[A-Za-z0-9._:-]{1,256}$/.test(responseId)
      ? responseId
      : null;
    try {
      onTerminal(context.streamId, safeResponseId, stopReason, content);
    } catch (error) {
      console.warn(`[tokenvisor] Metadata result observer failed: ${error instanceof Error ? error.name : "Error"}`);
    }
    appendMetadataDiagnostic(capturePath, {
      session_id: context.sessionId,
      stream_id: context.streamId,
      response_id: safeResponseId,
      status: safeResponseId ? "terminal_response_id_observed" : "terminal_response_id_unavailable",
    });
  };
  void terminal.then((result) => record(result.responseId, result.stopReason, result.content), () => record());
  return new Proxy(stream, {
    get(target, property) {
      if (property === "result") return () => terminal;
      const value = Reflect.get(target, property, target) as unknown;
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function addPiCaptureStream(
  config: ProviderConfig,
  streamSimple: CaptureStream,
): ProviderConfig {
  return { ...config, api: CAPTURE_API, streamSimple };
}

type BuiltInStream = CaptureStream;
const builtInStreams = new Map<RuntimeHost, Promise<BuiltInStream>>();

async function resolveBuiltInStream(host: RuntimeHost): Promise<BuiltInStream> {
  let builtInStream = builtInStreams.get(host);
  if (!builtInStream) {
    builtInStream = (async () => {
      if (host === "pi") {
        const subagentPackageRoot = resolvePiSubagentPackageRoot();
        const packageRoot = findPiAiPackageRoot(
          process.argv[1],
          fileURLToPath(import.meta.url),
          subagentPackageRoot ? join(subagentPackageRoot, "package.json") : undefined,
        );
        if (!packageRoot) throw new Error("Could not locate Pi's installed @earendil-works/pi-ai package");
        const packageInfo = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf-8")) as { version?: string };
        if (packageInfo.version !== "1.4.0") throw new Error("Pi capture requires @earendil-works/pi-ai 1.4.0");
        const moduleUrl = pathToFileURL(join(packageRoot, "dist", "api", "openai-completions.js")).href;
        const provider = await import(moduleUrl) as { streamSimple?: BuiltInStream };
        if (provider.streamSimple) return provider.streamSimple;
      } else {
        const provider = await import("@oh-my-pi/pi-ai/providers/openai-completions") as {
          streamOpenAICompletions?: BuiltInStream;
        };
        if (provider.streamOpenAICompletions) return provider.streamOpenAICompletions;
      }
      throw new Error(`OpenAI completions stream is unavailable for ${host}`);
    })();
    builtInStreams.set(host, builtInStream);
  }
  return builtInStream;
}

/**
 * Ask hosts to attach a session-correlation id on every outgoing request:
 *   pi  -> x-session-id: <raw session uuid>   (sendSessionAffinityHeaders + openrouter format)
 *   omp -> x-session-id: pc_<promptCacheKey ?? sessionId> (promptCacheSessionHeader; pi ignores this key)
 */
const SESSION_COMPAT = {
  sendSessionAffinityHeaders: true,
  sessionAffinityFormat: "openrouter" as const,
  promptCacheSessionHeader: "x-session-id",
} as const;

function convertModel(model: TokenVisorModel): ModelSpec {
  const capabilities = model.capabilities ?? [];
  return {
    id: model.id,
    name: model.name || model.id,
    reasoning: capabilities.includes("reasoning"),
    input: capabilities.includes("image")
      ? ["text", "image"]
      : ["text"],
    contextWindow: model.context_length ?? 128000,
    maxTokens: model.max_output_tokens ?? (model.context_length ? Math.floor(model.context_length / 2) : 16384),
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    compat: SESSION_COMPAT,
  };
}

async function discoverModels(baseUrl: string, apiKey: string): Promise<ModelSpec[]> {
  const response = await fetch(`${baseUrl}/models`, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
  });
  if (!response.ok) throw new Error(`Models endpoint returned ${response.status}`);
  const data = (await response.json()) as TokenVisorModelList;
  return (data.data ?? []).map(convertModel);
}

/**
 * Read a TokenVisor API key from ~/.pi/agent/auth.json (where pi's /login and
 * /tokenvisor-login both write it). omp has no API-key login UI, so on a machine
 * that runs both hosts this is the bridge that lets omp reuse a key entered
 * under either host. On an omp-only machine this file won't exist unless
 * /tokenvisor-login created it, and the user can set TOKENVISOR_API_KEY instead.
 */
function readPiStoredApiKey(): string | undefined {
  if (!existsSync(AUTH_PATH)) return undefined;
  try {
    const data = JSON.parse(readFileSync(AUTH_PATH, "utf-8"));
    const cred = data["TokenVisor"] ?? data["tokenvisor"];
    if (cred?.type === "api_key" && typeof cred.key === "string") return cred.key;
  } catch {
    // Ignore parse errors
  }
  return undefined;
}

/** Persist the TokenVisor API key into ~/.pi/agent/auth.json (atomic rewrite). */
function savePiStoredApiKey(key: string): void {
  const dir = join(homedir(), ".pi", "agent");
  mkdirSync(dir, { recursive: true });
  let data: Record<string, unknown> = {};
  if (existsSync(AUTH_PATH)) {
    try {
      const parsed = JSON.parse(readFileSync(AUTH_PATH, "utf-8"));
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        data = parsed as Record<string, unknown>;
      }
    } catch {
      // Corrupt or unreadable — start fresh.
    }
  }
  data["TokenVisor"] = { type: "api_key", key };
  const tmp = `${AUTH_PATH}.tmp`;
  writeFileSync(tmp, JSON.stringify(data, null, 2), { mode: 0o600 });
  renameSync(tmp, AUTH_PATH);
}

function resolveApiKey(): string | undefined {
  // 1. Explicit env var wins (omp-idiomatic source, user-controlled)
  const envKey = process.env[ENV_KEY];
  if (envKey) return envKey;
  // 2. Fall back to a pi-stored credential
  return readPiStoredApiKey();
}

export default async function (pi: ExtensionAPI) {
  const baseUrl = process.env["TOKENVISOR_BASE_URL"] ?? DEFAULT_BASE_URL;
  const apiKey = resolveApiKey();

  // Keep the resolved key available in the process environment for extension
  // integrations. ProviderConfig.apiKey is passed as a literal below because
  // Pi interpolates $ENV_VAR references while OMP checks the whole string as
  // an env-var name; bare names become the literal bearer token in Pi.
  if (apiKey && !process.env[ENV_KEY]) {
    process.env[ENV_KEY] = apiKey;
  }

  let models: ModelSpec[] = [];

  if (apiKey) {
    try {
      models = await discoverModels(baseUrl, apiKey);
      console.log(`[tokenvisor] Loaded ${models.length} models`);
    } catch (err) {
      console.warn(`[tokenvisor] Model discovery failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // Placeholder model so the provider appears in pi's /login API-key list and in
  // omp's /model picker before discovery has run. Replaced once a key is present
  // and /reload triggers model discovery from the API.
  if (models.length === 0) {
    models.push({
      id: "tokenvisor",
      name: "TokenVisor",
      reasoning: false,
      input: ["text"],
      contextWindow: 128000,
      maxTokens: 16384,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    });
  }

  const metadataDiagnosticPath = process.env.TOKENVISOR_AGENT_METADATA_DIAGNOSTICS;
  const capturePath = metadataDiagnosticPath;
  let captureActive = true;
  let runtimeHost: RuntimeHost | null = null;
  let piRunnerVersion: string | undefined;
  if (captureActive) {
    try {
      runtimeHost = detectRuntimeHost(process.argv[1]);
      if (runtimeHost === "pi") {
        const harnessVersion = readPiHarnessVersion(process.argv[1]);
        piRunnerVersion = readPiSubagentRunnerVersion(process.argv[1]) ?? undefined;
        if (!supportsAgentMetadataCapture(runtimeHost, harnessVersion, piRunnerVersion, process.env.PI_SUBAGENT_CHILD === "1")) {
          appendMetadataDiagnostic(metadataDiagnosticPath, {
            status: "unsupported_host_version",
            host: "pi",
            harness_version: harnessVersion,
            runner_version: piRunnerVersion ?? null,
          });
          throw new Error("Unsupported Pi capture runtime version");
        }
      } else {
        const harnessVersion = readOmpHarnessVersion();
        if (!supportsAgentMetadataCapture(runtimeHost, harnessVersion)) {
          appendMetadataDiagnostic(metadataDiagnosticPath, {
            status: "unsupported_host_version",
            host: "omp",
            harness_version: harnessVersion,
          });
          throw new Error("Unsupported OMP capture runtime version");
        }
      }
    } catch (error) {
      appendMetadataDiagnostic(metadataDiagnosticPath, {
        status: "capture_runtime_unavailable",
        reason: error instanceof Error ? error.name : "Error",
      });
      runtimeHost = null;
      captureActive = false;
      if (!metadataDiagnosticPath) {
        console.warn(`[tokenvisor] Agent metadata unavailable: ${error instanceof Error ? error.message : "runtime version unavailable"}`);
      }
    }
  }
  const ompCaptureState = runtimeHost === "omp" ? getOmpRuntimeCaptureState() : null;
  const compactionStreams = ompCaptureState?.compactionStreams ?? new Map<string, string[]>();
  const compactionCaptureActive = ompCaptureState?.compactionCaptureActive ?? new Set<string>();
  const compactionStreamFailures = ompCaptureState?.compactionStreamFailures ?? new Set<string>();
  const compactionPriorStreams = ompCaptureState?.compactionPriorStreams ?? new Map<string, string[] | null>();
  const compactionConsumedRunIds = new Map<string, string[]>();
  const compactionRunIdsByStream = new Map<string, string[]>();
  const piRequestMetadataByStream = new Map<string, string>();
  const ompDependenciesBySession = ompCaptureState?.compactionDependencies ?? new Map<string, AgentMetadata["depends_on"]>();
  const ompLineageBySession = ompCaptureState?.lineageBySession ?? new Map<string, {
    rootSessionId: string | null;
    parentSessionId: string | null;
    delegation: { runId: string; toolCallId: string } | null;
    forkedRoot?: boolean;
  }>();
  const ompBranchReaders = ompCaptureState?.branchReaders ?? new Map<string, () => Array<Record<string, any>>>();
  const ompSessionFiles = ompCaptureState?.sessionFiles ?? new Map<string, string>();
  const ompSequenceBySession = ompCaptureState?.sequenceBySession ?? new Map<string, ReturnType<typeof createOmpSequenceState>>();
  const forkedRoots = new Set<string>();
  const captureHandler = pi as ExtensionAPI & {
    on(event: string, handler: (event: any, ctx: any) => unknown): () => void;
  };

  if (captureActive) {
    captureHandler.on("session_start", (event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      const currentNativeHeader = readNativeSessionHeader(ctx.sessionManager.getSessionFile?.());
      const startupFork = runtimeHost !== null && isNativeForkStartup({
        argv: process.argv,
        host: runtimeHost,
        sessionId,
        currentHeader: currentNativeHeader,
        parentHeader: readNativeSessionHeader(currentNativeHeader?.parentSession),
        agentKind: (ctx as any).agent?.kind,
        piSubagentChild: process.env.PI_SUBAGENT_CHILD,
        reason: event.reason,
      });
      const forkEvent = event.reason === "fork" || startupFork;
      if (forkEvent) {
        const parentBoundary = ctx.sessionManager.getBranch().at(-1)?.id ?? null;
        pi.appendEntry(PI_FORK_ROOT_ENTRY, { version: 1, sessionId, parentBoundary });
      }
      const forkBoundaryIndex = readOmpForkBoundaryIndex(ctx.sessionManager.getBranch(), sessionId);
      const isFork = forkEvent || forkBoundaryIndex !== null;
      const storedCompactionStreams = readLatestPiCompactionContributors(ctx, forkBoundaryIndex === null ? 0 : forkBoundaryIndex + 1);
      compactionConsumedRunIds.set(
        sessionId,
        readLatestPiCompactionConsumedRuns(ctx, forkBoundaryIndex === null ? 0 : forkBoundaryIndex + 1) ?? [],
      );
      const parentSessionFile = ctx.sessionManager.getHeader?.()?.parentSession;
      const lineage = deriveOmpSessionLineage(
        sessionId,
        parentSessionFile,
        isFork,
        ctx.sessionManager.getSessionFile?.(),
        (ctx as any).agent?.kind === "main",
      );
      const ompAgentId = (ctx as unknown as { agent?: { id?: string } }).agent?.id;
      ompLineageBySession.set(sessionId, {
        ...lineage,
        forkedRoot: isFork,
        delegation: parentSessionFile && !isFork
          ? readOmpTaskDelegation(parentSessionFile, lineage.parentSessionId, ompAgentId)
          : null,
      });
      ompBranchReaders.set(sessionId, () => ctx.sessionManager.getBranch());
      ompSequenceBySession.set(sessionId, createOmpSequenceState());
      const nativeSessionFile = ctx.sessionManager.getSessionFile?.();
      if (nativeSessionFile) ompSessionFiles.set(sessionId, nativeSessionFile);
      ompDependenciesBySession.set(
        sessionId,
        storedCompactionStreams?.map((stream_id) => ({ session_id: `omp:session:${sessionId}`, stream_id })) ?? null,
      );
      if (isFork) {
        forkedRoots.add(sessionId);
      }
    });
    if (runtimeHost === "omp") captureHandler.on("session_shutdown", (_event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      ompLineageBySession.delete(sessionId);
      ompBranchReaders.delete(sessionId);
      ompSessionFiles.delete(sessionId);
      ompSequenceBySession.delete(sessionId);
      ompDependenciesBySession.delete(sessionId);
      compactionConsumedRunIds.delete(sessionId);
      compactionStreams.delete(sessionId);
      compactionCaptureActive.delete(sessionId);
      compactionStreamFailures.delete(sessionId);
      compactionPriorStreams.delete(sessionId);
    });
    if (runtimeHost === "omp") captureHandler.on("agent_end", (event: any, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      const lineage = ompLineageBySession.get(sessionId);
      if (!lineage?.parentSessionId || event.willContinue === true || event.awaitingAsyncWork === true) return;
      const messages: any[] = Array.isArray(event.messages) ? event.messages : [];
      const lastAssistant = [...messages].reverse().find((message: any) => message?.role === "assistant");
      if (lastAssistant?.stopReason !== "stop" || typeof lastAssistant?.responseId !== "string") return;
      pi.appendEntry(OMP_RUN_COMPLETION_ENTRY, {
        version: 1,
        ...(lineage.delegation ? { runId: lineage.delegation.runId } : {}),
        parentSessionId: lineage.parentSessionId,
        sessionId,
        responseId: lastAssistant.responseId,
      });
    });
    if (runtimeHost === "pi") captureHandler.on("agent_end", (event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      if (!ctx.sessionManager.getHeader?.()?.parentSession || forkedRoots.has(sessionId)) return;
      const parentSessionId = readPiNativeSessionId(ctx.sessionManager.getHeader?.()?.parentSession);
      const delegation = readPiSubagentDelegation(ctx.sessionManager.getSessionFile?.(), parentSessionId, "terminal");
      if (!delegation) {
        appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "run_completion_unavailable" });
        return;
      }
      const messages: any[] = Array.isArray(event.messages) ? event.messages : [];
      const lastAssistant = [...messages].reverse().find((message: any) => message?.role === "assistant");
      if (lastAssistant?.stopReason !== "stop" || typeof lastAssistant?.responseId !== "string") {
        appendMetadataDiagnostic(capturePath, {
          session_id: `pi:session:${sessionId}`,
          run_id: delegation.runId,
          status: "run_completion_not_terminal",
        });
        return;
      }
      pi.appendEntry(PI_RUN_COMPLETION_ENTRY, {
        version: 1,
        runId: delegation.runId,
        sessionId,
        responseId: lastAssistant.responseId,
      });
    });
    captureHandler.on("session_before_compact", (_event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      compactionStreams.set(sessionId, []);
      if (runtimeHost === "pi") compactionCaptureActive.add(sessionId);
      else compactionCaptureActive.delete(sessionId);
      compactionStreamFailures.delete(sessionId);
      compactionConsumedRunIds.set(sessionId, []);
      compactionPriorStreams.set(sessionId, readLatestPiCompactionContributors(ctx));
    });
    captureHandler.on("session.compacting", (_event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      compactionStreams.set(sessionId, []);
      compactionCaptureActive.add(sessionId);
      compactionStreamFailures.delete(sessionId);
      compactionConsumedRunIds.set(sessionId, []);
      compactionPriorStreams.set(sessionId, readLatestPiCompactionContributors(ctx));
    });
    captureHandler.on("session_compact", (_event: { fromExtension?: boolean }, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      const committedCompactionEntryId = latestNativeCompactionId(ctx.sessionManager.getBranch());
      if (!committedCompactionEntryId) {
        appendMetadataDiagnostic(capturePath, {
          session_id: `pi:session:${sessionId}`,
          status: "compaction_commit_unavailable",
        });
        return;
      }
      const currentStreams = compactionStreams.get(sessionId) ?? [];
      compactionStreams.delete(sessionId);
      compactionCaptureActive.delete(sessionId);
      const hadStreamFailure = compactionStreamFailures.delete(sessionId);
      const currentConsumedRuns = compactionConsumedRunIds.get(sessionId) ?? [];
      compactionConsumedRunIds.delete(sessionId);
      const previousStreams = compactionPriorStreams.get(sessionId) ?? null;
      compactionPriorStreams.delete(sessionId);
      const frontier = resolveCompactionFrontier(
        previousStreams,
        currentStreams,
        hadStreamFailure,
        _event.fromExtension === true,
      );
      const contributors = frontier.streamIds;
      const available = frontier.available;
      pi.appendEntry(
        PI_COMPACTION_ENTRY,
        makePiCompactionRecord(committedCompactionEntryId, contributors, available, available ? currentConsumedRuns : [], frontier.provenNone),
      );
      ompDependenciesBySession.set(
        sessionId,
        available ? contributors.map((stream_id) => ({ session_id: `omp:session:${sessionId}`, stream_id })) : null,
      );
      compactionConsumedRunIds.set(sessionId, available ? currentConsumedRuns : []);
      appendMetadataDiagnostic(capturePath, {
        session_id: `${runtimeHost}:session:${sessionId}`,
        compaction_entry_id: committedCompactionEntryId,
        contributors: available ? contributors : null,
        status: available ? "compaction_provenance_recorded" : "compaction_provenance_ambiguous",
      });
    });
    captureHandler.on("session_compact_failed", (_event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      compactionStreams.delete(sessionId);
      compactionCaptureActive.delete(sessionId);
      compactionStreamFailures.delete(sessionId);
      compactionPriorStreams.delete(sessionId);
      compactionConsumedRunIds.delete(sessionId);
    });
    if (runtimeHost === "pi") captureHandler.on("before_provider_headers", (event, ctx) => {
      const headers = event.headers;
      clearAgentMetadataHeader(headers);
      const sessionId = ctx.sessionManager.getSessionId();
      const streamId = randomUUID();
      const forkedRoot = forkedRoots.has(sessionId);
      const parentSessionFile = ctx.sessionManager.getHeader?.()?.parentSession;
      const parentSessionId = readPiNativeSessionId(parentSessionFile);
      const delegation = parentSessionFile && !forkedRoot
        ? readPiSubagentDelegation(ctx.sessionManager.getSessionFile?.(), parentSessionId)
        : null;
      if (parentSessionFile && !forkedRoot && !delegation) {
        appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "delegation_unavailable" });
        return;
      }
      const resolvedRunIds: string[] = [];
      let resultDependencies = readPiUnconsumedRunDependencies(ctx, (runId) => resolvedRunIds.push(runId));
      if (resultDependencies !== null && compactionStreams.has(sessionId)) {
        const consumed = compactionConsumedRunIds.get(sessionId) ?? [];
        compactionConsumedRunIds.set(sessionId, [...new Set([...consumed, ...resolvedRunIds])]);
        const earlierSummaryStreams = compactionStreams.get(sessionId) ?? [];
        resultDependencies = [
          ...earlierSummaryStreams.map((stream_id) => ({ session_id: `pi:session:${sessionId}`, stream_id })),
          ...resultDependencies,
        ];
      }
      if (resultDependencies === null) {
        appendMetadataDiagnostic(capturePath, {
          session_id: `pi:session:${sessionId}`,
          status: "result_dependency_unavailable",
        });
      }
      if (compactionCaptureActive.has(sessionId)) compactionRunIdsByStream.set(streamId, resolvedRunIds);
      const metadata = createPiEnvelopeForStream(ctx, streamId, {
        forkedRoot,
        runnerVersion: piRunnerVersion,
        delegation,
        resultDependencies,
      });
      if (!metadata) {
        if (compactionCaptureActive.has(sessionId)) compactionStreamFailures.add(sessionId);
        appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "metadata_unavailable" });
        return;
      }
      const parsed = parseAgentMetadata(metadata);
      if (!parsed || parsed.session.root_id === null) {
        appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "native_lineage_unavailable" });
        return;
      }
      if (!recordFreshRequestMetadata(piRequestMetadataByStream, metadata)) {
        appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "stream_binding_unavailable" });
        return;
      }
      writeAgentMetadataHeader(headers, metadata);
    });
  }

  const providerConfig: ProviderConfig = {
    name: "TokenVisor",
    baseUrl,
    apiKey: providerApiKey(apiKey),
    api: captureActive ? CAPTURE_API : "openai-completions",
    authHeader: true,
    models,
  };

  if (captureActive) {
    const delegate = await resolveBuiltInStream(runtimeHost!);
    const captureStream: CaptureStream = (model, context, options) => {
      const modelHeaders = stripAgentMetadataHeader(model.headers);
      let streamModel = { ...model, api: "openai-completions", ...(model.headers === undefined ? {} : { headers: modelHeaders }) };
      let streamOptions = options === undefined
        ? undefined
        : { ...options, headers: stripAgentMetadataHeader(options.headers) as CaptureStreamOptions["headers"] };
      let encoded: string | undefined;
      let trustedPiMetadata: AgentMetadata | null = null;
      let ompSequenceState: ReturnType<typeof createOmpSequenceState> | undefined;
      let ompSequenceAnchorId: string | null = null;
      let ompNativeSequence: { previousResponseId: string | null; hasPriorHistory: boolean } | undefined;
      if (runtimeHost === "pi") {
        const reservedHeader = headerValue(options?.headers) ?? headerValue(model.headers);
        const claim = claimFreshRequestMetadata(piRequestMetadataByStream, reservedHeader);
        if (claim) {
          encoded = claim.encoded;
          trustedPiMetadata = claim.metadata;
          streamOptions = {
            ...(streamOptions ?? {}),
            headers: withAgentMetadataHeader(streamOptions?.headers, claim.encoded) as CaptureStreamOptions["headers"],
          };
        } else if (reservedHeader !== undefined) {
          appendMetadataDiagnostic(capturePath, { status: "stream_binding_unavailable" });
        }
      }
      if (runtimeHost === "omp" && typeof options?.sessionId === "string") {
        const streamId = randomUUID();
        let dependencies = ompDependenciesBySession.has(options.sessionId)
          ? ompDependenciesBySession.get(options.sessionId) ?? null
          : [];
        const branchReader = ompBranchReaders.get(options.sessionId);
        const sessionFile = ompSessionFiles.get(options.sessionId);
        let ompMessages = (context as { messages?: Array<{ role?: string; responseId?: string }> }).messages;
        const lineage = ompLineageBySession.get(options.sessionId);
        if (branchReader) {
          const fullBranch = branchReader();
          const forkBoundary = readOmpForkBoundaryIndex(fullBranch, options.sessionId);
          const branch = forkBoundary === null ? fullBranch : fullBranch.slice(forkBoundary + 1);
          ompSequenceState = ompSequenceBySession.get(options.sessionId);
          if (ompSequenceState) {
            const lineageKey = lineage
              ? JSON.stringify([lineage.rootSessionId, lineage.parentSessionId, lineage.delegation?.runId ?? null, lineage.delegation?.toolCallId ?? null, lineage.forkedRoot === true])
              : null;
            ompNativeSequence = resolveOmpNativeSequence(branch, ompSequenceState, lineageKey);
            const leaf = branch.at(-1);
            ompSequenceAnchorId = typeof leaf?.id === "string" ? leaf.id : null;
            ompMessages = branch
              .filter((entry) => entry.type === "message")
              .map((entry) => entry.message ?? {});
          }
        }
        if (branchReader && sessionFile) {
          const fullBranch = branchReader();
          const forkBoundary = readOmpForkBoundaryIndex(fullBranch, options.sessionId);
          const branch = forkBoundary === null ? fullBranch : fullBranch.slice(forkBoundary + 1);
          dependencies = pendingOmpCompactionDependencies(branch, dependencies);
          if (dependencies !== null && compactionCaptureActive.has(options.sessionId)) {
            dependencies = [
              ...dependencies,
              ...(compactionStreams.get(options.sessionId) ?? []).map((stream_id) => ({
                session_id: `omp:session:${options.sessionId}`,
                stream_id,
              })),
            ];
          }
          const result = readOmpTaskResultDependencies(
            sessionFile,
            options.sessionId,
            branch,
            compactionConsumedRunIds.get(options.sessionId) ?? [],
          );
          dependencies = result === null || dependencies === null ? null : [...dependencies, ...result.dependencies];
          if (forkBoundary !== null) {
            ompMessages = branch
              .filter((entry) => entry.type === "message")
              .map((entry) => entry.message ?? {});
          }
          if (compactionCaptureActive.has(options.sessionId) && result) {
            compactionRunIdsByStream.set(streamId, result.runIds);
          }
        }
        if (!lineage || (lineage.parentSessionId && !lineage.delegation) || (!lineage.parentSessionId && lineage.rootSessionId !== options.sessionId)) {
          appendMetadataDiagnostic(capturePath, {
            session_id: `omp:session:${options.sessionId}`,
            known_session_count: ompLineageBySession.size,
            status: lineage?.parentSessionId ? "delegation_unavailable" : "native_lineage_unavailable",
          });
          return delegate(streamModel, context, streamOptions);
        }
        encoded = createOmpEnvelope(
          {
            sessionId: options.sessionId,
            nativeParentSessionId: lineage?.parentSessionId,
            nativeRootSessionId: lineage?.rootSessionId,
            delegation: lineage?.delegation,
            isForkedRoot: lineage?.forkedRoot,
            messages: ompMessages,
            nativeSequence: ompNativeSequence,
          },
          streamId,
          dependencies,
        ) ?? undefined;
        if (encoded) {
          streamOptions = {
            ...(streamOptions ?? {}),
            headers: withAgentMetadataHeader(streamOptions?.headers, encoded) as CaptureStreamOptions["headers"],
          };
        }
      }
      const metadata = runtimeHost === "pi"
        ? trustedPiMetadata
        : parseAgentMetadata(encoded);
      if (!metadata) {
        if (runtimeHost === "omp" && typeof options?.sessionId === "string" && compactionCaptureActive.has(options.sessionId)) {
          compactionStreamFailures.add(options.sessionId);
        }
        if (!encoded) appendMetadataDiagnostic(capturePath, { status: "stream_metadata_unavailable" });
        return delegate(streamModel, context, streamOptions);
      }
      const streamContext: CaptureContext = {
        sessionId: metadata.session.id,
        streamId: metadata.stream_id,
      };
      if (runtimeHost === "omp" && ompSequenceState) {
        recordOmpSequenceStream(ompSequenceState, metadata.stream_id, ompSequenceAnchorId);
      }
      const stream = delegate(streamModel, context, streamOptions);
      return observeCaptureResult(stream, streamContext, capturePath, runtimeHost!, (streamId, responseId, stopReason, content) => {
        if (runtimeHost === "omp" && ompSequenceState) {
          recordOmpSequenceResult(ompSequenceState, streamId, responseId);
        }
        const sessionSeparator = ":session:";
        const values = compactionStreams.get(metadata.session.id.slice(metadata.session.id.indexOf(sessionSeparator) + sessionSeparator.length));
        const sessionKey = metadata.session.id.slice(metadata.session.id.indexOf(sessionSeparator) + sessionSeparator.length);
        if (values && compactionCaptureActive.has(sessionKey)) {
          if (runtimeHost === "omp") {
            const disposition = recordOmpCompactionResponse(values, streamId, responseId, stopReason, content);
            if (disposition === "unusable") compactionStreamFailures.add(sessionKey);
            if (disposition === "contributor") {
              const consumed = compactionConsumedRunIds.get(sessionKey) ?? [];
              compactionConsumedRunIds.set(sessionKey, [...new Set([...consumed, ...(compactionRunIdsByStream.get(streamId) ?? [])])]);
            }
          } else if (!responseId || stopReason !== "stop") {
            compactionStreamFailures.add(sessionKey);
          } else {
            if (!values.includes(streamId)) values.push(streamId);
            const consumed = compactionConsumedRunIds.get(sessionKey) ?? [];
            compactionConsumedRunIds.set(sessionKey, [...new Set([...consumed, ...(compactionRunIdsByStream.get(streamId) ?? [])])]);
          }
        }
        compactionRunIdsByStream.delete(streamId);
      });
    };
    pi.registerProvider("TokenVisor", addPiCaptureStream(providerConfig, captureStream));
  } else {
    pi.registerProvider("TokenVisor", providerConfig);
  }

  // Interactive API-key entry. Under pi this mirrors the built-in /login; under
  // omp it is the only interactive way to set the key, since omp's /login is
  // OAuth-only and never lists custom API-key providers like TokenVisor.
  pi.registerCommand("tokenvisor-login", {
    description: "Enter or update your TokenVisor API key",
    handler: async (_args, ctx) => {
      const input = await ctx.ui.input("TokenVisor API key", "tv-pat-...");
      const key = input?.trim();
      if (!key) {
        ctx.ui.notify("TokenVisor login cancelled.", "info");
        return;
      }
      try {
        savePiStoredApiKey(key);
      } catch (err) {
        ctx.ui.notify(
          `Failed to save TokenVisor key: ${err instanceof Error ? err.message : String(err)}`,
          "error",
        );
        return;
      }
      // Make the key effective for the current process immediately; the reload
      // below re-runs this factory, which re-reads the key, re-mirrors it into
      // the env, and re-discovers models.
      process.env[ENV_KEY] = key;
      ctx.ui.notify("TokenVisor API key saved. Reloading…", "info");
      // Reload is terminal for this handler — must return immediately after.
      await ctx.reload();
      return;
    },
  });

  if (!apiKey) {
    console.log(
      `[tokenvisor] No API key found. Run /tokenvisor-login, set ${ENV_KEY} in` +
        " your environment (omp has no API-key /login), or run /login under pi," +
        " then /reload.",
    );
  }
}
