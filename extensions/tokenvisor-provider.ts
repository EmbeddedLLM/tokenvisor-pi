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
  encodeAgentMetadata,
  appendPendingChildLaunches,
  parseAgentMetadata,
  packChildLaunches,
  stripAgentMetadataHeader,
  withAgentMetadataHeader,
  type AgentMetadata,
  type MetadataIdentity,
  type ChildLaunchReceipt,
} from "../src/capture/shared.ts";
import { findPiAiPackageRoot, loadPiCompactionRenderer, loadPiRequiredExtensionsApi, type PiCompactionRendererApi } from "../src/capture/pi-runtime.ts";
import {
  detectRuntimeHost,
  isNativeForkStartup,
  readOmpHarnessVersion,
  readPiHarnessVersion,
  readPiSubagentRunnerVersion,
  resolvePiSubagentPackageRoot,
  type RuntimeHost,
} from "../src/capture/runtime-host.ts";
import { readNativeSessionHeader } from "../src/capture/native-session.ts";
import { createCaptureWarningReporter } from "../src/capture/warnings.ts";
import {
  derivePiIdentity,
  commitPiResultConsumption,
  commitPiLaunchReceiptConsumption,
  filterPiDependenciesForRequestInput,
  resolvePiCompactionRequestInput,
  readPiSingleTextContent,
  readLatestPiCompactionContributorWitnesses,
  type PiDependencyDeliveryWitness,
  isPiForkRoot,
  makePiCompactionRecord,
  makePiLaunchReceiptPendingRecord,
  PI_COMPACTION_ENTRY,
  PI_FORK_ROOT_ENTRY,
  PI_RUN_COMPLETION_ENTRY,
  readPiNativeSessionId,
  readPiParentBranchForChild,
  readPiPersistedConsumedRunIds,
  readPiPersistedConsumedDependencies,
  readPiSubagentDelegation,
  readPiForegroundLaunchEvidence,
  readPiPendingChildLaunchReceipts,
  hasPiKnownChildLaunchReceipt,
  readPiUnconsumedRunDependencies,
  readLatestPiCompactionContributors,
  readLatestPiCompactionConsumedRuns,
  resolveCompactionFrontier,
  latestNativeCompactionId,
  PI_FOREGROUND_DELIVERY_ENTRY,
  PI_LAUNCH_RECEIPT_PENDING_ENTRY,
  PI_LAUNCH_RECEIPT_CONSUMPTION_ENTRY,
} from "../src/capture/pi.ts";
import {
  createOmpEnvelope,
  createOmpSequenceState,
  commitPendingOmpTaskResultConsumers,
  deriveOmpSessionLineage,
  getOmpRuntimeCaptureState,
  makeOmpTaskResultConsumptionRecord,
  OMP_RUN_COMPLETION_ENTRY,
  OMP_TASK_RESULT_CONSUMPTION_ENTRY,
  pendingOmpCompactionDependencies,
  recordOmpSequenceResult,
  recordOmpSequenceStream,
  readOmpForkBoundaryIndex,
  readOmpTaskDelegation,
  readOmpTaskResultDependencies,
  registerOmpTaskLifecycleStarted,
  readOmpEarlyTaskDelegation,
  clearOmpEarlyTaskBindingsForSession,
  readOmpBlockingTaskResultDependencies,
  retireOmpEarlyTaskBinding,
  queueOmpTaskBindingRetirement,
  clearOmpTaskBindingRetirementsForSession,
  assessOmpCompactionFrontier,
  resolveOmpPeerWakeDependencies,
  type OmpTaskResultReference,
  recordOmpCompactionResponse,
  resolveOmpNativeSequence,
} from "../src/capture/omp.ts";

const ENV_KEY = "TOKENVISOR_API_KEY";
const DEFAULT_BASE_URL = "https://devstudio.tokenvisor.ai/api/v1";
const AUTH_PATH = join(homedir(), ".pi", "agent", "auth.json");

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

function reencodeTrustedPiMetadata(metadata: AgentMetadata): string | null {
  const sessionId = metadata.session.id.startsWith("pi:session:") ? metadata.session.id.slice("pi:session:".length) : null;
  const rootSessionId = metadata.session.root_id?.startsWith("pi:session:")
    ? metadata.session.root_id.slice("pi:session:".length)
    : null;
  const parentSessionId = metadata.session.parent_id?.startsWith("pi:session:")
    ? metadata.session.parent_id.slice("pi:session:".length)
    : null;
  if (!sessionId) return null;
  const identity: MetadataIdentity = {
    harness: metadata.source.harness,
    harnessVersion: metadata.source.harness_version,
    extensionVersion: metadata.source.extension_version,
    runnerVersion: metadata.source.runner_version,
    sessionId,
    rootSessionId,
    parentSessionId,
    previousResponseId: metadata.sequence.previous_response_id,
    hasPriorHistory: metadata.sequence.status !== "start",
    delegation: metadata.delegation
      ? { runId: metadata.delegation.run_id, toolCallId: metadata.delegation.tool_call_id }
      : null,
    dependsOn: metadata.depends_on,
    childLaunches: metadata.child_launches,
  };
  return encodeAgentMetadata(identity, metadata.stream_id);
}

function readPiRenderedCompactionWitnesses(ctx: any, renderer: PiCompactionRendererApi | null): PiDependencyDeliveryWitness[] {
  if (!renderer) return [];
  const witnesses = readLatestPiCompactionContributorWitnesses(ctx);
  if (!witnesses?.length) return [];
  const branch = ctx.sessionManager.getBranch();
  const nativeCompaction = [...branch].reverse().find((entry: any) => entry.type === "compaction");
  if (!nativeCompaction) return [];
  try {
    const rendered = renderer.convertToLlm(renderer.sessionEntryToContextMessages(nativeCompaction));
    const textBlocks = rendered.flatMap((message: any) => message?.role === "user"
      ? [readPiSingleTextContent(message.content)].filter((text): text is string => text !== null)
      : []);
    if (textBlocks.length !== 1) return [];
    const previousSummaryText = typeof nativeCompaction.summary === "string"
      ? `<previous-summary>\n${nativeCompaction.summary}\n</previous-summary>`
      : null;
    return witnesses.flatMap((witness) => [
      { dependency: witness.dependency, deliveredText: textBlocks[0]! },
      ...(previousSummaryText ? [{ dependency: witness.dependency, deliveredText: previousSummaryText }] : []),
    ]);
  } catch {
    return [];
  }
}

const CAPTURE_API = "tokenvisor-openai-completions-capture";
const captureWarningReporter = ((globalThis as any)[Symbol.for("tokenvisor-pi.capture-warnings.v1")] ??=
  createCaptureWarningReporter());

function warnCaptureLimitation(ctx: any, sessionId: string, code: string, issue: string): void {
  captureWarningReporter.report(sessionId, code, issue, (message) => {
    if (typeof ctx?.ui?.notify === "function") {
      try {
        ctx.ui.notify(message, "warning");
        return;
      } catch {
        // Fall back to stderr if the host notification surface is unavailable.
      }
    }
    console.warn(message);
  });
}

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
  onTerminal: (streamId: string, responseId: string | null, stopReason: string | undefined, content: unknown, observedResponseId: string | null) => void,
): T {
  const originalResult = stream.result.bind(stream);
  let recorded = false;
  const terminal = originalResult();
  const record = (responseId?: string, stopReason?: string, content?: unknown) => {
    if (recorded) return;
    recorded = true;
    const responseSucceeded = stopReason === "stop" || stopReason === "toolUse" || stopReason === "length";
    const safeObservedResponseId = typeof responseId === "string" && /^[A-Za-z0-9._:-]{1,256}$/.test(responseId)
      ? responseId
      : null;
    const safeResponseId = responseSucceeded ? safeObservedResponseId : null;
    try {
      onTerminal(context.streamId, safeResponseId, stopReason, content, safeObservedResponseId);
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
        const moduleUrl = pathToFileURL(join(packageRoot, "dist", "api", "openai-completions.js")).href;
        const provider = await import(moduleUrl) as { streamSimple?: BuiltInStream };
        if (typeof provider.streamSimple === "function") return provider.streamSimple;
      } else {
        const provider = await import("@oh-my-pi/pi-ai/providers/openai-completions") as {
          streamOpenAICompletions?: BuiltInStream;
        };
        if (typeof provider.streamOpenAICompletions === "function") return provider.streamOpenAICompletions;
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
  let builtInStream: BuiltInStream | null = null;
  let runtimeHarnessVersion = "unknown";
  let piRunnerVersion: string | undefined;
  let piCompactionRenderer: PiCompactionRendererApi | null = null;
  let childExtensions: Awaited<ReturnType<typeof loadPiRequiredExtensionsApi>> = null;
  if (captureActive) {
    try {
      if (typeof (pi as any).on !== "function") throw new Error("Host event API is unavailable");
      runtimeHost = detectRuntimeHost(process.argv[1]);
      if (runtimeHost === "pi") {
        runtimeHarnessVersion = readPiHarnessVersion(process.argv[1]) ?? "unknown";
        piRunnerVersion = readPiSubagentRunnerVersion(process.argv[1]) ?? undefined;
        childExtensions = await loadPiRequiredExtensionsApi();
        piRunnerVersion = piRunnerVersion ?? childExtensions?.packageVersion;
        piCompactionRenderer = await loadPiCompactionRenderer(process.argv[1]);
      } else {
        runtimeHarnessVersion = readOmpHarnessVersion() ?? "unknown";
      }
      builtInStream = await resolveBuiltInStream(runtimeHost);
    } catch (error) {
      appendMetadataDiagnostic(metadataDiagnosticPath, {
        status: "capture_runtime_unavailable",
        reason: error instanceof Error ? error.name : "Error",
      });
      warnCaptureLimitation(null, "process", "capture-runtime", "the host capture or OpenAI stream API is unavailable");
      runtimeHost = null;
      captureActive = false;
    }
  }
  const ompCaptureState = runtimeHost === "omp" ? getOmpRuntimeCaptureState() : null;
  const compactionStreams = ompCaptureState?.compactionStreams ?? new Map<string, string[]>();
  const compactionCaptureActive = ompCaptureState?.compactionCaptureActive ?? new Set<string>();
  const compactionStreamFailures = ompCaptureState?.compactionStreamFailures ?? new Set<string>();
  const compactionPriorStreams = ompCaptureState?.compactionPriorStreams ?? new Map<string, string[] | null>();
  const compactionConsumedRunIds = new Map<string, string[]>();
  const compactionRunIdsByStream = new Map<string, string[]>();
  const compactionConsumedDependencies = new Map<string, NonNullable<AgentMetadata["depends_on"]>>();
  const compactionDependenciesByStream = new Map<string, NonNullable<AgentMetadata["depends_on"]>>();
  const compactionSessionByStream = new Map<string, string>();
  const piCompactionPreparationBySession = new Map<string, any>();
  const piCompactionLastResponseBySession = new Map<string, string>();
  const clearPiCompactionStreams = (sessionId: string) => {
    for (const [streamId, owner] of compactionSessionByStream) {
      if (owner !== sessionId) continue;
      compactionSessionByStream.delete(streamId);
      compactionRunIdsByStream.delete(streamId);
      compactionDependenciesByStream.delete(streamId);
    }
  };
  const appendPiCompactionDependencies = (sessionId: string, dependencies: NonNullable<AgentMetadata["depends_on"]>) => {
    const previous = compactionConsumedDependencies.get(sessionId) ?? [];
    const next = [...new Map([...previous, ...dependencies]
      .filter((dependency) => dependency.response_id)
      .map((dependency) => [JSON.stringify([dependency.session_id, dependency.response_id, dependency.run_id]), { ...dependency }])).values()];
    if (next.length > 8) {
      compactionConsumedDependencies.delete(sessionId);
      compactionStreamFailures.add(sessionId);
      return;
    }
    compactionConsumedDependencies.set(sessionId, next);
  };
  const seedPiCompactionState = (sessionId: string, ctx: any, firstKeptEntryId?: string) => {
    const dependencies = readPiPersistedConsumedDependencies(ctx, firstKeptEntryId);
    const runIds = readPiPersistedConsumedRunIds(ctx, firstKeptEntryId);
    if (dependencies === null || dependencies.length > 8 || runIds === null) {
      compactionConsumedDependencies.delete(sessionId);
      compactionConsumedRunIds.delete(sessionId);
      compactionStreamFailures.add(sessionId);
      return;
    }
    compactionConsumedDependencies.set(sessionId, dependencies);
    compactionConsumedRunIds.set(sessionId, runIds);
  };
  const compactionTaskResultReferences = ompCaptureState?.compactionTaskResultReferences ?? new Map<string, OmpTaskResultReference[]>();
  const ompTaskResultReferencesByStream = ompCaptureState?.taskResultReferencesByStream ?? new Map<string, { sessionId: string; references: OmpTaskResultReference[] }>();
  const ompTaskBindingIdsByStream = new Map<string, { sessionId: string; bindingIds: string[] }>();
  const piRequestClaims = new WeakMap<object, () => { encoded: string; witnesses: PiDependencyDeliveryWitness[] } | undefined>();
  const piRequestOwners = new Map<string, object>();
  const piPendingResultConsumptions = new Map<string, Map<string, { streamId: string; responseId: string; dependencies: NonNullable<AgentMetadata["depends_on"]> }>>();
  const piPendingLaunchReceiptsBySession = new Map<string, ChildLaunchReceipt[]>();
  const piLaunchReceiptsByStream = new Map<string, ChildLaunchReceipt[]>();
  const piLaunchReceiptSessionByStream = new Map<string, string>();
  const piPendingLaunchReceiptConsumptions = new Map<string, Map<string, { streamId: string; responseId: string; receipts: ChildLaunchReceipt[] }>>();
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
  const retireCommittedOmpBindings = (
    sessionId: string,
    before: Map<string, OmpTaskResultReference[]>,
    after: Map<string, OmpTaskResultReference[]>,
  ) => {
    if (!ompCaptureState) return;
    const stillPending = new Set([...after.values()].flatMap((references) => references.map((reference) => reference.jobId)));
    for (const reference of [...before.values()].flat()) {
      if (!stillPending.has(reference.jobId)) retireOmpEarlyTaskBinding(ompCaptureState, sessionId, reference.jobId);
    }
  };

  if (captureActive) {
    const childRegistrations = new Map<string, { dispose(): void }>();
    captureHandler.on("session_start", (event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      if (runtimeHost === "pi") {
        const pendingReceipts = readPiPendingChildLaunchReceipts(ctx);
        if (pendingReceipts === null) {
          piPendingLaunchReceiptsBySession.delete(sessionId);
          appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "launch_receipt_state_unavailable" });
        } else {
          piPendingLaunchReceiptsBySession.set(sessionId, pendingReceipts);
        }
      }
      if (childExtensions) {
        try {
          childRegistrations.get(sessionId)?.dispose();
          const registration = childExtensions.registerRequiredChildExtensions({
            sessionId,
            extensions: [{ id: "tokenvisor-provider", path: fileURLToPath(import.meta.url) }],
          });
          if (!registration || typeof registration.dispose !== "function") {
            throw new Error("Required child extension registration returned no disposer");
          }
          childRegistrations.set(sessionId, registration);
        } catch {
          appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "child_extension_registration_unavailable" });
          warnCaptureLimitation(ctx, `pi:session:${sessionId}`, "child-extension-registration", "required child extension registration failed");
        }
      } else {
        warnCaptureLimitation(ctx, `pi:session:${sessionId}`, "child-extension-api", "required child extension support is unavailable");
      }
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
      if (runtimeHost === "pi") compactionConsumedDependencies.set(sessionId, readPiPersistedConsumedDependencies(ctx) ?? []);
      if (runtimeHost === "omp" && ompCaptureState) {
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
            ? readOmpEarlyTaskDelegation(ompCaptureState, ompAgentId, ctx.sessionManager.getSessionFile?.()) ??
              readOmpTaskDelegation(parentSessionFile, lineage.parentSessionId, ompAgentId)
            : null,
        });
        ompBranchReaders.set(sessionId, () => ctx.sessionManager.getBranch());
        ompCaptureState.appendEntryBySession.set(sessionId, (customType, data) => pi.appendEntry(customType, data));
        ompSequenceBySession.set(sessionId, createOmpSequenceState());
        const nativeSessionFile = ctx.sessionManager.getSessionFile?.();
        if (nativeSessionFile) ompSessionFiles.set(sessionId, nativeSessionFile);
        ompDependenciesBySession.set(
          sessionId,
          storedCompactionStreams?.map((stream_id) => ({ session_id: `omp:session:${sessionId}`, stream_id })) ?? null,
        );
      }
      if (isFork) {
        forkedRoots.add(sessionId);
      }
    });
    if (runtimeHost === "pi") captureHandler.on("session_shutdown", (_event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      piRequestOwners.delete(sessionId);
      childRegistrations.get(sessionId)?.dispose();
      childRegistrations.delete(sessionId);
      piPendingResultConsumptions.delete(sessionId);
      piPendingLaunchReceiptConsumptions.delete(sessionId);
      piPendingLaunchReceiptsBySession.delete(sessionId);
      for (const [streamId, ownerSessionId] of piLaunchReceiptSessionByStream) {
        if (ownerSessionId === sessionId) {
          piLaunchReceiptSessionByStream.delete(streamId);
          piLaunchReceiptsByStream.delete(streamId);
        }
      }
      compactionConsumedDependencies.delete(sessionId);
      compactionConsumedRunIds.delete(sessionId);
      compactionStreams.delete(sessionId);
      compactionCaptureActive.delete(sessionId);
      compactionStreamFailures.delete(sessionId);
      compactionPriorStreams.delete(sessionId);
      piCompactionPreparationBySession.delete(sessionId);
      piCompactionLastResponseBySession.delete(sessionId);
      forkedRoots.delete(sessionId);
      clearPiCompactionStreams(sessionId);
    });
    if (runtimeHost === "omp") captureHandler.on("session_shutdown", (_event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      ompLineageBySession.delete(sessionId);
      ompBranchReaders.delete(sessionId);
      ompSessionFiles.delete(sessionId);
      ompCaptureState?.appendEntryBySession.delete(sessionId);
      compactionTaskResultReferences.delete(sessionId);
      ompSequenceBySession.delete(sessionId);
      ompDependenciesBySession.delete(sessionId);
      compactionConsumedRunIds.delete(sessionId);
      compactionStreams.delete(sessionId);
      compactionCaptureActive.delete(sessionId);
      compactionStreamFailures.delete(sessionId);
      compactionPriorStreams.delete(sessionId);
      ompCaptureState?.pendingTaskResultConsumptions.delete(sessionId);
      if (ompCaptureState) clearOmpTaskBindingRetirementsForSession(ompCaptureState, sessionId);
      for (const [streamId, captured] of ompTaskBindingIdsByStream) {
        if (captured.sessionId === sessionId) ompTaskBindingIdsByStream.delete(streamId);
      }
      if (ompCaptureState) clearOmpEarlyTaskBindingsForSession(ompCaptureState, sessionId);
      for (const [streamId, capture] of ompTaskResultReferencesByStream) {
        if (capture.sessionId === sessionId) ompTaskResultReferencesByStream.delete(streamId);
      }
    });
    if (runtimeHost === "pi") captureHandler.on("tool_result", (event, ctx) => {
      if ((event as any)?.toolName !== "subagent") return;
      const evidence = readPiForegroundLaunchEvidence(event, ctx);
      const sessionId = ctx.sessionManager.getSessionId();
      if (!evidence) {
        appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "foreground_launch_unavailable" });
        return;
      }
      const pending = piPendingLaunchReceiptsBySession.get(sessionId) ?? readPiPendingChildLaunchReceipts(ctx);
      if (pending === null) {
        appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "launch_receipt_state_unavailable" });
        return;
      }
      const alreadyKnown = hasPiKnownChildLaunchReceipt(ctx, evidence.receipt);
      const appended = alreadyKnown
        ? { pending, accepted: [], overflow: 0 }
        : appendPendingChildLaunches(pending, [evidence.receipt]);
      if (appended.overflow > 0) {
        appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "launch_receipt_pending_cap" });
      }
      if (appended.accepted.length > 0) {
        const record = makePiLaunchReceiptPendingRecord(appended.accepted);
        if (!record) return;
        try {
          pi.appendEntry(PI_LAUNCH_RECEIPT_PENDING_ENTRY, record);
          piPendingLaunchReceiptsBySession.set(sessionId, appended.pending);
        } catch {
          appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "launch_receipt_persist_failed" });
          return;
        }
      }
      if (evidence.deliveredOutputSha256) {
        const branch = ctx.sessionManager.getBranch();
        const alreadyRecorded = branch.some((entry: any) => entry.customType === PI_FOREGROUND_DELIVERY_ENTRY &&
          entry.data?.receipt?.parent_response_id === evidence.receipt.parent_response_id &&
          entry.data?.receipt?.tool_call_id === evidence.receipt.tool_call_id &&
          entry.data?.receipt?.child_session_id === evidence.receipt.child_session_id &&
          entry.data?.receipt?.run_id === evidence.receipt.run_id &&
          entry.data?.receipt?.last_response_id === evidence.receipt.last_response_id &&
          entry.data?.delivered_output_sha256 === evidence.deliveredOutputSha256);
        if (!alreadyRecorded) {
          try {
            pi.appendEntry(PI_FOREGROUND_DELIVERY_ENTRY, {
              version: 1,
              receipt: evidence.receipt,
              delivered_output_sha256: evidence.deliveredOutputSha256,
            });
          } catch {
            appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "foreground_delivery_persist_failed" });
          }
        }
      }
    });
    if (runtimeHost === "omp" && ompCaptureState && pi.events?.on) {
      pi.events.on("task:subagent:lifecycle", (payload) => {
        if (!payload || typeof payload !== "object" || (payload as any).status !== "started") return;
        const parentToolCallId = (payload as any).parentToolCallId;
        const candidates = [...ompBranchReaders.entries()].flatMap(([parentSessionId, readBranch]) => {
          const sessionFile = ompSessionFiles.get(parentSessionId);
          if (!sessionFile || !Array.isArray(readBranch())) return [];
          const matches = readBranch().flatMap((entry) => {
            const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
            return message.role === "assistant" && Array.isArray(message.content) && message.content.some((part: any) =>
              (part?.type === "toolCall" || part?.type === "tool_call") && part.name === "task" && part.id === parentToolCallId,
            ) ? [entry] : [];
          });
          return matches.length === 1 ? [{ parentSessionId, sessionFile }] : [];
        });
        if (candidates.length === 1) {
          registerOmpTaskLifecycleStarted(ompCaptureState, candidates[0]!.sessionFile, candidates[0]!.parentSessionId, payload);
        }
      });
    }
    if (runtimeHost === "omp") captureHandler.on("agent_end", (_event: any, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      const pending = ompCaptureState?.pendingTaskResultConsumptions.get(sessionId);
      if (pending?.size) {
        const before = new Map([...pending].map(([responseId, references]) => [responseId, references.map((reference) => ({ ...reference }))]));
        commitPendingOmpTaskResultConsumers(
          sessionId,
          ctx.sessionManager.getBranch(),
          pending,
          ompCaptureState?.appendEntryBySession.get(sessionId),
          () => ctx.sessionManager.getBranch(),
        );
        retireCommittedOmpBindings(sessionId, before, pending);
        if (pending.size === 0) ompCaptureState?.pendingTaskResultConsumptions.delete(sessionId);
      }
      const retirements = ompCaptureState?.pendingTaskBindingRetirements.get(sessionId);
      if (retirements?.size && ompCaptureState) {
        const branch = ctx.sessionManager.getBranch();
        const unresolvedReferenceIds = new Set([...(ompCaptureState.pendingTaskResultConsumptions.get(sessionId)?.values() ?? [])]
          .flatMap((references) => references.map((reference) => reference.jobId)));
        for (const [responseId, retirement] of retirements) {
          const matches = branch.filter((entry: any) => {
            const message = entry.message && typeof entry.message === "object" ? entry.message : entry;
            return message.role === "assistant" && message.responseId === responseId;
          });
          if (matches.length !== 1) continue;
          const remaining: string[] = [];
          for (const bindingId of retirement.bindingIds) {
            if (unresolvedReferenceIds.has(bindingId)) remaining.push(bindingId);
            else retireOmpEarlyTaskBinding(ompCaptureState, sessionId, bindingId);
          }
          if (remaining.length === 0) retirements.delete(responseId);
          else retirement.bindingIds = remaining;
        }
        if (retirements.size === 0) ompCaptureState?.pendingTaskBindingRetirements.delete(sessionId);
      }
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
      const pending = piPendingResultConsumptions.get(sessionId);
      if (pending?.size) {
        for (const [responseId, consumption] of pending) {
            if (!commitPiResultConsumption(ctx, consumption.streamId, responseId, consumption.dependencies, pi.appendEntry)) continue;
            pending.delete(responseId);
        }
        if (pending.size === 0) piPendingResultConsumptions.delete(sessionId);
      }
      const pendingLaunchReceipts = piPendingLaunchReceiptConsumptions.get(sessionId);
      if (pendingLaunchReceipts?.size) {
        for (const [responseId, consumption] of pendingLaunchReceipts) {
          try {
            if (!commitPiLaunchReceiptConsumption(ctx, consumption.streamId, responseId, consumption.receipts, pi.appendEntry)) continue;
            const current = piPendingLaunchReceiptsBySession.get(sessionId) ?? [];
            const retired = new Set(consumption.receipts.map((receipt) => JSON.stringify([
              receipt.parent_response_id, receipt.tool_call_id, receipt.child_session_id, receipt.run_id, receipt.last_response_id,
            ])));
            piPendingLaunchReceiptsBySession.set(sessionId, current.filter((receipt) => !retired.has(JSON.stringify([
              receipt.parent_response_id, receipt.tool_call_id, receipt.child_session_id, receipt.run_id, receipt.last_response_id,
            ]))));
            pendingLaunchReceipts.delete(responseId);
          } catch {
            appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "launch_receipt_retirement_failed" });
          }
        }
        if (pendingLaunchReceipts.size === 0) piPendingLaunchReceiptConsumptions.delete(sessionId);
      }
      if (!ctx.sessionManager.getHeader?.()?.parentSession || forkedRoots.has(sessionId)) return;
      const parentSessionId = readPiNativeSessionId(ctx.sessionManager.getHeader?.()?.parentSession);
      const delegation = readPiSubagentDelegation(ctx.sessionManager.getSessionFile?.(), parentSessionId, "terminal", readPiParentBranchForChild(ctx));
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
    captureHandler.on("session_before_compact", (event: any, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      if (runtimeHost === "pi" && event?.preparation) piCompactionPreparationBySession.set(sessionId, event.preparation);
      piCompactionLastResponseBySession.delete(sessionId);
      compactionStreams.set(sessionId, []);
      if (runtimeHost === "pi") compactionCaptureActive.add(sessionId);
      else compactionCaptureActive.delete(sessionId);
      compactionStreamFailures.delete(sessionId);
      if (runtimeHost === "pi") seedPiCompactionState(
        sessionId,
        ctx,
        event?.preparation?.firstKeptEntryId,
      );
      else compactionConsumedRunIds.set(sessionId, []);
      compactionPriorStreams.set(sessionId, readLatestPiCompactionContributors(ctx));
    });
    captureHandler.on("session.compacting", (_event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      compactionStreams.set(sessionId, []);
      compactionCaptureActive.add(sessionId);
      compactionStreamFailures.delete(sessionId);
      if (runtimeHost === "pi") seedPiCompactionState(
        sessionId,
        ctx,
        piCompactionPreparationBySession.get(sessionId)?.firstKeptEntryId,
      );
      else compactionConsumedRunIds.set(sessionId, []);
      compactionPriorStreams.set(sessionId, readLatestPiCompactionContributors(ctx));
    });
    captureHandler.on("session_compact", (_event: { fromExtension?: boolean; compactionEntry?: unknown }, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      const committedCompactionEntryId = latestNativeCompactionId(ctx.sessionManager.getBranch());
      if (!committedCompactionEntryId) {
        compactionTaskResultReferences.delete(sessionId);
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
      const currentConsumedDependencies = compactionConsumedDependencies.get(sessionId) ?? [];
      compactionConsumedDependencies.delete(sessionId);
      const previousStreams = compactionPriorStreams.get(sessionId) ?? null;
      compactionPriorStreams.delete(sessionId);
      let sourceProvenMechanicalCompaction = _event.fromExtension === true;
      if (runtimeHost === "omp" && ompCaptureState) {
        const proof = assessOmpCompactionFrontier(ompCaptureState, sessionId, _event);
        sourceProvenMechanicalCompaction = !proof.hasUnresolvedInputs &&
          (_event.fromExtension === true || proof.sourceProvenMechanicalCompaction);
      }
      const frontier = resolveCompactionFrontier(
        previousStreams,
        currentStreams,
        hadStreamFailure,
        sourceProvenMechanicalCompaction,
      );
      const contributors = frontier.streamIds;
      const available = frontier.available;
      pi.appendEntry(
        PI_COMPACTION_ENTRY,
        makePiCompactionRecord(
          committedCompactionEntryId,
          contributors,
          available,
          available ? currentConsumedRuns : [],
          frontier.provenNone,
          available ? currentConsumedDependencies : [],
          piPendingLaunchReceiptsBySession.get(sessionId) ?? [],
        ),
      );
      ompDependenciesBySession.set(
        sessionId,
        available ? contributors.map((stream_id) => ({ session_id: `omp:session:${sessionId}`, stream_id })) : null,
      );
      compactionConsumedRunIds.set(sessionId, available ? currentConsumedRuns : []);
      compactionConsumedDependencies.set(sessionId, available ? currentConsumedDependencies : []);
      const consumedTaskResultReferences = compactionTaskResultReferences.get(sessionId) ?? [];
      compactionTaskResultReferences.delete(sessionId);
      if (available && consumedTaskResultReferences.length > 0) {
        const branch = ctx.sessionManager.getBranch();
        const record = makeOmpTaskResultConsumptionRecord(
          branch,
          sessionId,
          { type: "compaction", id: committedCompactionEntryId },
          consumedTaskResultReferences,
        );
        if (record) ompCaptureState?.appendEntryBySession.get(sessionId)?.(OMP_TASK_RESULT_CONSUMPTION_ENTRY, record);
      }
      appendMetadataDiagnostic(capturePath, {
        session_id: `${runtimeHost}:session:${sessionId}`,
        compaction_entry_id: committedCompactionEntryId,
        contributors: available ? contributors : null,
        status: available ? "compaction_provenance_recorded" : "compaction_provenance_ambiguous",
      });
      clearPiCompactionStreams(sessionId);
      piCompactionPreparationBySession.delete(sessionId);
      piCompactionLastResponseBySession.delete(sessionId);
    });
    captureHandler.on("session_compact_failed", (_event, ctx) => {
      const sessionId = ctx.sessionManager.getSessionId();
      compactionStreams.delete(sessionId);
      compactionCaptureActive.delete(sessionId);
      compactionStreamFailures.delete(sessionId);
      compactionPriorStreams.delete(sessionId);
      compactionConsumedRunIds.delete(sessionId);
      compactionConsumedDependencies.delete(sessionId);
      clearPiCompactionStreams(sessionId);
      piCompactionPreparationBySession.delete(sessionId);
      piCompactionLastResponseBySession.delete(sessionId);
      compactionTaskResultReferences.delete(sessionId);
    });
    if (runtimeHost === "pi") captureHandler.on("before_provider_headers", (event, ctx) => {
      // Freeze native evidence here, but keep it off the wire. Only the physical
      // TokenVisor provider may claim it and commit capture bookkeeping.
      piRequestClaims.set(event.headers, preparePiRequestMetadata(ctx));
    });
  }

  const preparePiRequestMetadata = (ctx: any) => {
    const sessionId = ctx.sessionManager.getSessionId();
    let owner = piRequestOwners.get(sessionId);
    if (!owner) {
      owner = {};
      piRequestOwners.set(sessionId, owner);
    }
    let atCapacity = false;
    const effects: Array<() => void> = [];
    const diagnostic = (record: Parameters<typeof appendMetadataDiagnostic>[1]) => {
      effects.push(() => appendMetadataDiagnostic(capturePath, record));
    };
    const prepare = () => {
      const streamId = randomUUID();
      const forkedRoot = forkedRoots.has(sessionId);
      const parentSessionFile = ctx.sessionManager.getHeader?.()?.parentSession;
      const parentSessionId = readPiNativeSessionId(parentSessionFile);
      const delegation = parentSessionFile && !forkedRoot
        ? readPiSubagentDelegation(ctx.sessionManager.getSessionFile?.(), parentSessionId, "active", readPiParentBranchForChild(ctx))
        : null;
      const deliveryWitnesses: PiDependencyDeliveryWitness[] = [];
      deliveryWitnesses.push(...readPiRenderedCompactionWitnesses(ctx, piCompactionRenderer));
      let resultDependencies = readPiUnconsumedRunDependencies(
        ctx,
        (dependency, deliveredText, channel) => deliveryWitnesses.push({ dependency, deliveredText, ...channel }),
      );
      if (resultDependencies === null) {
        if (runtimeHost === "pi" && compactionCaptureActive.has(sessionId)) effects.push(() => compactionStreamFailures.add(sessionId));
        diagnostic({
          session_id: `pi:session:${sessionId}`,
          status: "result_dependency_unavailable",
        });
      }
      if (compactionCaptureActive.has(sessionId)) {
        // Candidate lineage is not consumed until the provider wrapper sees the
        // request-local input and finalizes the outgoing metadata.
        effects.push(() => compactionRunIdsByStream.set(streamId, []));
        effects.push(() => compactionDependenciesByStream.set(streamId, []));
        effects.push(() => compactionSessionByStream.set(streamId, sessionId));
      }
      const identity = derivePiIdentity(ctx, {
        forkedRoot,
        harnessVersion: runtimeHarnessVersion,
        runnerVersion: piRunnerVersion,
        delegation,
        resultDependencies,
      });
      if (!identity) {
        if (compactionCaptureActive.has(sessionId)) effects.push(() => compactionStreamFailures.add(sessionId));
        diagnostic({ session_id: `pi:session:${sessionId}`, status: "metadata_unavailable" });
        effects.push(() => warnCaptureLimitation(ctx, `pi:session:${sessionId}`, "request-identity", "request identity could not be resolved"));
        return;
      }
      const pendingLaunchReceipts = piPendingLaunchReceiptsBySession.get(sessionId) ?? [];
      effects.push(() => {
        const outstanding = [...piLaunchReceiptSessionByStream.values()].filter((owner) => owner === sessionId).length;
        if (outstanding >= 16) {
          appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "launch_receipt_stream_binding_cap" });
          atCapacity = true;
        }
      });
      const packed = packChildLaunches(identity, streamId, pendingLaunchReceipts);
      let metadata = packed.encoded;
      if (!metadata) {
        if (compactionCaptureActive.has(sessionId)) effects.push(() => compactionStreamFailures.add(sessionId));
        diagnostic({ session_id: `pi:session:${sessionId}`, status: "metadata_unavailable" });
        return;
      }
      if (packed.unencodableIndices.length > 0) {
        diagnostic({
          session_id: `pi:session:${sessionId}`,
          count: packed.unencodableIndices.length,
          status: "launch_receipt_unencodable",
        });
      }
      let parsed = parseAgentMetadata(metadata);
      if (!parsed || parsed.session.root_id === null) {
        diagnostic({ session_id: `pi:session:${sessionId}`, status: "native_lineage_unavailable" });
        return;
      }
      const previousSummaryResponseId = compactionCaptureActive.has(sessionId)
        ? piCompactionLastResponseBySession.get(sessionId)
        : undefined;
      if (previousSummaryResponseId) {
        const sequenced = reencodeTrustedPiMetadata({
          ...parsed,
          sequence: { status: "linked", previous_response_id: previousSummaryResponseId },
        });
        if (!sequenced) {
          effects.push(() => compactionStreamFailures.add(sessionId));
          diagnostic({ session_id: `pi:session:${sessionId}`, status: "compaction_sequence_unavailable" });
          return;
        }
        metadata = sequenced;
        parsed = parseAgentMetadata(metadata);
      }
      if (packed.included.length > 0) {
        const receipts = packed.included.map((receipt) => ({ ...receipt }));
        effects.push(() => {
          piLaunchReceiptsByStream.set(streamId, receipts);
          piLaunchReceiptSessionByStream.set(streamId, sessionId);
        });
      }
      return { encoded: metadata, witnesses: deliveryWitnesses };
    };
    const prepared = prepare();
    return () => {
      if (piRequestOwners.get(sessionId) !== owner) {
        appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "stream_binding_unavailable" });
        return;
      }
      for (const effect of effects) {
        effect();
        if (atCapacity) return;
      }
      return prepared;
    };
  };

  const providerConfig: ProviderConfig = {
    name: "TokenVisor",
    baseUrl,
    // Pi expands $ENV_VAR references while OMP treats the whole string as a
    // variable name, so pass an available resolved key literally.
    apiKey: apiKey ?? ENV_KEY,
    api: captureActive ? CAPTURE_API : "openai-completions",
    authHeader: true,
    models,
  };

  if (captureActive && builtInStream) {
    const delegate = builtInStream;
    const captureStream: CaptureStream = (model, context, options) => {
      const modelHeaders = stripAgentMetadataHeader(model.headers);
      let streamModel = { ...model, api: "openai-completions", ...(model.headers === undefined ? {} : { headers: modelHeaders }) };
      let streamOptions = options === undefined
        ? undefined
        : { ...options, headers: stripAgentMetadataHeader(options.headers) as CaptureStreamOptions["headers"] };
      let encoded: string | undefined;
      let trustedPiMetadata: AgentMetadata | null = null;
      let piDeliveryWitnesses: PiDependencyDeliveryWitness[] = [];
      let ompSequenceState: ReturnType<typeof createOmpSequenceState> | undefined;
      let ompSequenceAnchorId: string | null = null;
      let ompNativeSequence: { previousResponseId: string | null; hasPriorHistory: boolean } | undefined;
      let ompTaskResultReferences: OmpTaskResultReference[] | undefined;
      let ompTaskBindingIds: string[] | undefined;
      if (runtimeHost === "pi") {
        const requestHeaders = options?.headers ?? model.headers;
        const claim = requestHeaders && piRequestClaims.get(requestHeaders);
        if (requestHeaders) piRequestClaims.delete(requestHeaders);
        const prepared = claim?.();
        if (prepared) {
          encoded = prepared.encoded;
          trustedPiMetadata = parseAgentMetadata(encoded);
          piDeliveryWitnesses = prepared.witnesses;
        }
      }
      if (runtimeHost === "omp" && typeof options?.sessionId === "string") {
        const streamId = randomUUID();
        let dependencies = ompDependenciesBySession.has(options.sessionId)
          ? ompDependenciesBySession.get(options.sessionId) ?? null
          : [];
        const branchReader = ompBranchReaders.get(options.sessionId);
        const sessionFile = ompSessionFiles.get(options.sessionId);
        const effectiveMessages = (context as { messages?: Array<Record<string, any>> }).messages ?? [];
        let ompMessages = effectiveMessages;
        const lineage = ompLineageBySession.get(options.sessionId);
        if (branchReader) {
          const fullBranch = branchReader();
          const forkBoundary = readOmpForkBoundaryIndex(fullBranch, options.sessionId);
          let branch = forkBoundary === null ? fullBranch : fullBranch.slice(forkBoundary + 1);
          const pendingConsumptions = ompCaptureState?.pendingTaskResultConsumptions.get(options.sessionId);
          if (pendingConsumptions?.size) {
            const before = new Map([...pendingConsumptions].map(([responseId, references]) => [responseId, references.map((reference) => ({ ...reference }))]));
            branch = commitPendingOmpTaskResultConsumers(
              options.sessionId,
              branch,
              pendingConsumptions,
              ompCaptureState?.appendEntryBySession.get(options.sessionId),
              branchReader,
            );
            retireCommittedOmpBindings(options.sessionId, before, pendingConsumptions);
            if (pendingConsumptions.size === 0) ompCaptureState?.pendingTaskResultConsumptions.delete(options.sessionId);
            const committedBranch = branchReader();
            const committedForkBoundary = readOmpForkBoundaryIndex(committedBranch, options.sessionId);
            branch = committedForkBoundary === null ? committedBranch : committedBranch.slice(committedForkBoundary + 1);
          }
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
          dependencies = pendingOmpCompactionDependencies(
            branch,
            resolveOmpPeerWakeDependencies(branch, dependencies),
          );
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
            effectiveMessages,
          );
          const blockingResult = readOmpBlockingTaskResultDependencies(
            ompCaptureState!,
            sessionFile,
            options.sessionId,
            branch,
            effectiveMessages,
          );
          dependencies = result === null || blockingResult === null ||
            result.dependencies === null || blockingResult.dependencies === null || dependencies === null
            ? null
            : [...dependencies, ...result.dependencies, ...blockingResult.dependencies];
          if (forkBoundary !== null) {
            ompMessages = branch
              .filter((entry) => entry.type === "message")
              .map((entry) => entry.message ?? {});
          }
          if (dependencies !== null) {
            ompTaskResultReferences = [
              ...(result?.references ?? []),
              ...(blockingResult?.references ?? []),
            ];
          }
          ompTaskBindingIds = blockingResult?.bindingIds ?? [];
        }
        if (!lineage || (!lineage.parentSessionId && lineage.rootSessionId !== options.sessionId)) {
          appendMetadataDiagnostic(capturePath, {
            session_id: `omp:session:${options.sessionId}`,
            known_session_count: ompLineageBySession.size,
            status: lineage?.parentSessionId ? "delegation_unavailable" : "native_lineage_unavailable",
          });
          warnCaptureLimitation(null, `omp:session:${options.sessionId}`, "native-lineage", "native session lineage could not be resolved");
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
          runtimeHarnessVersion,
        ) ?? undefined;
        if (encoded) {
          streamOptions = {
            ...(streamOptions ?? {}),
            headers: withAgentMetadataHeader(streamOptions?.headers, encoded) as CaptureStreamOptions["headers"],
          };
        }
      }
      if (runtimeHost === "pi" && trustedPiMetadata && encoded) {
        const witnesses = piDeliveryWitnesses;
        const requestMessages = (context as { messages?: unknown }).messages;
        const sessionId = trustedPiMetadata.session.id.slice("pi:session:".length);
        const hasDependencies = trustedPiMetadata.depends_on !== null && trustedPiMetadata.depends_on.length > 0;
        const compactionInput = hasDependencies && compactionCaptureActive.has(sessionId)
          ? resolvePiCompactionRequestInput(piCompactionPreparationBySession.get(sessionId), requestMessages, piCompactionRenderer)
          : null;
        const requestDependencies = !hasDependencies
          ? trustedPiMetadata.depends_on
          : compactionCaptureActive.has(sessionId)
          ? compactionInput === null
            ? null
            : filterPiDependenciesForRequestInput(trustedPiMetadata.depends_on, witnesses, compactionInput, true)
          : filterPiDependenciesForRequestInput(trustedPiMetadata.depends_on, witnesses, requestMessages);
        const finalizedMetadata = { ...trustedPiMetadata, depends_on: requestDependencies };
        const finalizedHeader = reencodeTrustedPiMetadata(finalizedMetadata);
        if (!finalizedHeader) {
          trustedPiMetadata = null;
          encoded = undefined;
          appendMetadataDiagnostic(capturePath, { status: "request_input_metadata_unavailable" });
        } else {
          trustedPiMetadata = finalizedMetadata;
          encoded = finalizedHeader;
          streamOptions = {
            ...(streamOptions ?? {}),
            headers: withAgentMetadataHeader(streamOptions?.headers, finalizedHeader) as CaptureStreamOptions["headers"],
          };
          if (compactionCaptureActive.has(sessionId)) {
            compactionRunIdsByStream.set(
              finalizedMetadata.stream_id,
              finalizedMetadata.depends_on?.flatMap((dependency) => dependency.run_id ? [dependency.run_id] : []) ?? [],
            );
            compactionDependenciesByStream.set(
              finalizedMetadata.stream_id,
              finalizedMetadata.depends_on?.filter((dependency) => dependency.response_id) ?? [],
            );
          }
        }
      }
      const metadata = runtimeHost === "pi"
        ? trustedPiMetadata
        : parseAgentMetadata(encoded);
      if (metadata?.sequence.status === "unavailable") {
        warnCaptureLimitation(null, metadata.session.id, "request-sequence", "native request sequence could not be established");
      }
      if (metadata && metadata.depends_on === null) {
        warnCaptureLimitation(null, metadata.session.id, "request-dependencies", "request dependency linkage could not be established");
      }
      if (!metadata) {
        if (runtimeHost === "omp" && typeof options?.sessionId === "string" && compactionCaptureActive.has(options.sessionId)) {
          compactionStreamFailures.add(options.sessionId);
        }
        if (!encoded) {
          appendMetadataDiagnostic(capturePath, { status: "stream_metadata_unavailable" });
          warnCaptureLimitation(null, runtimeHost === "omp" && typeof options?.sessionId === "string"
            ? `omp:session:${options.sessionId}`
            : runtimeHost === "pi" && typeof options?.sessionId === "string"
            ? `pi:session:${options.sessionId}`
            : "process", "request-metadata", "request metadata could not be encoded");
        }
        return delegate(streamModel, context, streamOptions);
      }
      const streamContext: CaptureContext = {
        sessionId: metadata.session.id,
        streamId: metadata.stream_id,
      };
      if (runtimeHost === "omp" && ompSequenceState) {
        recordOmpSequenceStream(ompSequenceState, metadata.stream_id, ompSequenceAnchorId);
      }
      if (runtimeHost === "omp" && ompTaskResultReferences?.length) {
        ompTaskResultReferencesByStream.set(metadata.stream_id, { sessionId: options?.sessionId ?? "", references: ompTaskResultReferences });
      }
      if (runtimeHost === "omp" && ompTaskBindingIds?.length && typeof options?.sessionId === "string") {
        const outstanding = [...ompTaskBindingIdsByStream.values()].filter((capture) => capture.sessionId === options.sessionId).length;
        if (outstanding < 64) {
          ompTaskBindingIdsByStream.set(metadata.stream_id, { sessionId: options.sessionId, bindingIds: [...new Set(ompTaskBindingIds)] });
        } else {
          appendMetadataDiagnostic(capturePath, { session_id: `omp:session:${options.sessionId}`, status: "task_binding_stream_cap" });
        }
      }
      const stream = delegate(streamModel, context, streamOptions);
      return observeCaptureResult(stream, streamContext, capturePath, runtimeHost!, (streamId, responseId, stopReason, content, observedResponseId) => {
        if (runtimeHost === "omp" && ompSequenceState) {
          recordOmpSequenceResult(ompSequenceState, streamId, observedResponseId, responseId !== null);
        }
        const sessionSeparator = ":session:";
        const values = compactionStreams.get(metadata.session.id.slice(metadata.session.id.indexOf(sessionSeparator) + sessionSeparator.length));
        const sessionKey = metadata.session.id.slice(metadata.session.id.indexOf(sessionSeparator) + sessionSeparator.length);
        if (runtimeHost === "pi" && compactionCaptureActive.has(sessionKey)) {
          if (responseId && stopReason === "stop") piCompactionLastResponseBySession.set(sessionKey, responseId);
        }
        if (values && compactionCaptureActive.has(sessionKey)) {
          if (runtimeHost === "omp") {
            const disposition = recordOmpCompactionResponse(values, streamId, responseId, stopReason, content);
            if (disposition === "unusable") compactionStreamFailures.add(sessionKey);
            if (disposition === "contributor") {
              const consumedReferences = compactionTaskResultReferences.get(sessionKey) ?? [];
              const nextReferences = [...consumedReferences, ...(ompTaskResultReferencesByStream.get(streamId)?.references ?? [])];
              const uniqueReferences = [...new Map(nextReferences.map((reference) => [JSON.stringify([reference.sessionId, reference.responseId]), reference])).values()];
              if (uniqueReferences.length > 8) {
                compactionTaskResultReferences.delete(sessionKey);
                compactionStreamFailures.add(sessionKey);
              } else {
                compactionTaskResultReferences.set(sessionKey, uniqueReferences);
              }
            }
          } else if (!responseId || stopReason !== "stop") {
            compactionStreamFailures.add(sessionKey);
          } else {
            if (!values.includes(streamId)) values.push(streamId);
            const consumed = compactionConsumedRunIds.get(sessionKey) ?? [];
            compactionConsumedRunIds.set(sessionKey, [...new Set([...consumed, ...(compactionRunIdsByStream.get(streamId) ?? [])])]);
            appendPiCompactionDependencies(sessionKey, compactionDependenciesByStream.get(streamId) ?? []);
          }
        }
        if (runtimeHost === "omp" && responseId) {
          const references = ompTaskResultReferencesByStream.get(streamId)?.references;
          if (references?.length && !compactionCaptureActive.has(sessionKey)) {
            const pending = ompCaptureState?.pendingTaskResultConsumptions.get(sessionKey) ?? new Map<string, OmpTaskResultReference[]>();
            if (pending.size < 16) {
              pending.set(responseId, references);
              ompCaptureState?.pendingTaskResultConsumptions.set(sessionKey, pending);
            } else {
              pending.clear();
              ompCaptureState?.pendingTaskResultConsumptions.delete(sessionKey);
              appendMetadataDiagnostic(capturePath, {
                session_id: `omp:session:${sessionKey}`,
                status: "task_result_consumption_overflow",
              });
            }
          }
        }
        if (runtimeHost === "omp") {
          const captured = ompTaskBindingIdsByStream.get(streamId);
          if (captured) {
            if (responseId && !queueOmpTaskBindingRetirement(ompCaptureState!, captured.sessionId, responseId, streamId, captured.bindingIds)) {
              appendMetadataDiagnostic(capturePath, { session_id: `omp:session:${captured.sessionId}`, status: "task_binding_retirement_cap" });
            }
            ompTaskBindingIdsByStream.delete(streamId);
          }
        }
        if (runtimeHost === "pi" && responseId && metadata.depends_on && metadata.depends_on.length > 0) {
          const sessionId = metadata.session.id.slice("pi:session:".length);
          const pending = piPendingResultConsumptions.get(sessionId) ?? new Map<string, { streamId: string; responseId: string; dependencies: NonNullable<AgentMetadata["depends_on"]> }>();
          if (pending.size < 16) {
            pending.set(responseId, {
              streamId: metadata.stream_id,
              responseId,
              dependencies: metadata.depends_on.map((dependency) => ({ ...dependency })),
            });
            piPendingResultConsumptions.set(sessionId, pending);
          } else {
            appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "result_consumption_overflow" });
          }
        }
        if (runtimeHost === "pi") {
          const receipts = piLaunchReceiptsByStream.get(streamId) ?? [];
          const sessionId = metadata.session.id.slice("pi:session:".length);
          if (responseId && receipts.length > 0) {
            const pending = piPendingLaunchReceiptConsumptions.get(sessionId) ?? new Map<string, { streamId: string; responseId: string; receipts: ChildLaunchReceipt[] }>();
            if (pending.size < 16) {
              pending.set(responseId, { streamId, responseId, receipts: receipts.map((receipt) => ({ ...receipt })) });
              piPendingLaunchReceiptConsumptions.set(sessionId, pending);
            } else {
              appendMetadataDiagnostic(capturePath, { session_id: `pi:session:${sessionId}`, status: "launch_receipt_consumption_overflow" });
            }
          }
          piLaunchReceiptsByStream.delete(streamId);
          piLaunchReceiptSessionByStream.delete(streamId);
        }
        ompTaskResultReferencesByStream.delete(streamId);
        compactionRunIdsByStream.delete(streamId);
        compactionDependenciesByStream.delete(streamId);
        compactionSessionByStream.delete(streamId);
      });
    };
    pi.registerProvider("TokenVisor", { ...providerConfig, api: CAPTURE_API, streamSimple: captureStream });
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
