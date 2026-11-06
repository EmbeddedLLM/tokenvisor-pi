import { randomUUID } from "crypto";

export const AGENT_HEADER = "X-TokenVisor-Agent";
export const AGENT_HEADER_BUDGET = 4096;
export const MAX_AGENT_DEPENDENCIES = 8;
export const MAX_CHILD_LAUNCHES_PER_ENVELOPE = 8;
export const MAX_PENDING_CHILD_LAUNCHES = 64;

export interface ChildLaunchReceipt {
  parent_response_id: string;
  tool_call_id: string;
  child_session_id: string;
  run_id: string;
  last_response_id: string;
}

export interface AgentMetadata {
  version: 1;
  source: {
    harness: string;
    harness_version: string;
    extension_version: string;
    adapter_version: 1;
    runner_version?: string;
  };
  session: { id: string; root_id: string | null; parent_id: string | null };
  stream_id: string;
  sequence: { status: "start" | "linked" | "unavailable"; previous_response_id: string | null };
  delegation: { run_id: string; tool_call_id: string } | null;
  depends_on: Array<{ session_id: string; response_id?: string; stream_id?: string; run_id?: string }> | null;
  child_launches: ChildLaunchReceipt[];
}

export interface MetadataIdentity {
  harness: string;
  harnessVersion: string;
  extensionVersion: string;
  runnerVersion?: string;
  sessionId: string | null;
  rootSessionId: string | null;
  parentSessionId: string | null;
  previousResponseId: string | null;
  hasPriorHistory: boolean;
  delegation: { runId: string; toolCallId: string } | null;
  dependsOn: AgentMetadata["depends_on"];
  childLaunches?: ChildLaunchReceipt[];
}

function isSafeId(value: unknown, maxLength = 256): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength && /^[A-Za-z0-9._:-]+$/.test(value);
}

function isBoundedAscii(value: unknown, maxLength: number): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength && /^[\x20-\x7e]+$/.test(value);
}

export function isChildLaunchReceipt(value: unknown): value is ChildLaunchReceipt {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const receipt = value as Record<string, unknown>;
  return Object.keys(receipt).length === 5 &&
    Object.keys(receipt).every((key) => [
      "parent_response_id", "tool_call_id", "child_session_id", "run_id", "last_response_id",
    ].includes(key)) &&
    isSafeId(receipt.parent_response_id) &&
    isSafeId(receipt.tool_call_id) &&
    isSafeId(receipt.child_session_id) &&
    isSafeId(receipt.run_id) &&
    isSafeId(receipt.last_response_id);
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

function hasValidChildLaunches(value: unknown): value is ChildLaunchReceipt[] {
  return Array.isArray(value) && value.length <= MAX_CHILD_LAUNCHES_PER_ENVELOPE &&
    value.every(isChildLaunchReceipt) &&
    new Set(value.map(childLaunchKey)).size === value.length;
}

function cleanIdentity(identity: MetadataIdentity, streamId?: string): AgentMetadata | null {
  if (!isBoundedAscii(identity.harness, 32) || !isBoundedAscii(identity.harnessVersion, 64) ||
      !isBoundedAscii(identity.extensionVersion, 64) ||
      identity.runnerVersion !== undefined && !isBoundedAscii(identity.runnerVersion, 64) ||
      !isSafeId(identity.sessionId)) return null;
  const sessionId = `${identity.harness}:session:${identity.sessionId}`;
  if (!isSafeId(sessionId)) return null;
  const rootId = isSafeId(identity.rootSessionId)
    ? `${identity.harness}:session:${identity.rootSessionId}`
    : identity.rootSessionId === identity.sessionId
      ? sessionId
      : null;
  const parentId = isSafeId(identity.parentSessionId)
    ? `${identity.harness}:session:${identity.parentSessionId}`
    : null;
  if (rootId !== null && !isSafeId(rootId) || parentId !== null && !isSafeId(parentId)) return null;
  const previousResponseId = isSafeId(identity.previousResponseId) ? identity.previousResponseId : null;
  const delegation = identity.delegation === null
    ? null
    : isSafeId(identity.delegation.runId) && isSafeId(identity.delegation.toolCallId)
      ? { run_id: identity.delegation.runId, tool_call_id: identity.delegation.toolCallId }
      : undefined;
  const dependencyKeys = Array.isArray(identity.dependsOn)
    ? identity.dependsOn.map((ref) => ref && typeof ref === "object"
      ? JSON.stringify([ref.session_id, ref.response_id ?? null, ref.stream_id ?? null, ref.run_id ?? null])
      : null)
    : [];
  const dependenciesAreValid = identity.dependsOn === null || (
    Array.isArray(identity.dependsOn) && identity.dependsOn.length <= MAX_AGENT_DEPENDENCIES &&
    dependencyKeys.every((key): key is string => key !== null) &&
    new Set(dependencyKeys).size === dependencyKeys.length && identity.dependsOn.every((ref) => {
      if (!ref || typeof ref !== "object" || !isSafeId(ref.session_id)) return false;
      const hasResponse = ref.response_id !== undefined;
      const hasStream = ref.stream_id !== undefined;
      return (hasResponse || hasStream) &&
        (hasResponse ? isSafeId(ref.response_id) : true) &&
        (hasStream ? isSafeId(ref.stream_id) : true) &&
        (ref.run_id === undefined || isSafeId(ref.run_id));
    })
  );
  if (delegation === undefined) return null;
  const dependsOn = !dependenciesAreValid
    ? null
    : identity.dependsOn === null
      ? null
      : identity.dependsOn.map((ref) => ({
      session_id: ref.session_id,
      ...(ref.response_id === undefined ? {} : { response_id: ref.response_id }),
      ...(ref.stream_id === undefined ? {} : { stream_id: ref.stream_id }),
      ...(ref.run_id === undefined ? {} : { run_id: ref.run_id }),
      }));
  const childLaunches = identity.childLaunches ?? [];
  if (!hasValidChildLaunches(childLaunches)) return null;
  if (streamId !== undefined && !isSafeId(streamId)) return null;

  return {
    version: 1,
    source: {
      harness: identity.harness,
      harness_version: identity.harnessVersion,
      extension_version: identity.extensionVersion,
      adapter_version: 1,
      ...(identity.runnerVersion ? { runner_version: identity.runnerVersion } : {}),
    },
    session: { id: sessionId, root_id: rootId, parent_id: parentId },
    stream_id: streamId ?? randomUUID(),
    sequence: previousResponseId
      ? { status: "linked", previous_response_id: previousResponseId }
      : identity.hasPriorHistory
        ? { status: "unavailable", previous_response_id: null }
        : { status: "start", previous_response_id: null },
    delegation,
    depends_on: dependsOn,
    child_launches: childLaunches.map((receipt) => ({ ...receipt })),
  };
}

export function encodeAgentMetadata(identity: MetadataIdentity, streamId?: string): string | null {
  const metadata = cleanIdentity(identity, streamId);
  if (!metadata) return null;
  const base = cleanIdentity({ ...identity, childLaunches: [] }, streamId);
  if (!base) return null;
  if (Buffer.byteLength(JSON.stringify(base), "ascii") > AGENT_HEADER_BUDGET && metadata.depends_on !== null) {
    metadata.depends_on = null;
  }
  const encoded = JSON.stringify(metadata);
  if (!/^[\x20-\x7e]+$/.test(encoded) || Buffer.byteLength(encoded, "ascii") > AGENT_HEADER_BUDGET) return null;
  return encoded;
}

export function parseAgentMetadata(value: unknown): AgentMetadata | null {
  if (typeof value !== "string" || !/^[\x20-\x7e]*$/.test(value) ||
      Buffer.byteLength(value, "ascii") > AGENT_HEADER_BUDGET) return null;
  try {
    const parsed = JSON.parse(value) as Partial<AgentMetadata> & { child_launches?: unknown };
    if (parsed.version !== 1 || !isSafeId(parsed.stream_id) || !isSafeId(parsed.session?.id)) return null;
    if (parsed.child_launches !== undefined && !hasValidChildLaunches(parsed.child_launches)) return null;
    return { ...parsed, child_launches: parsed.child_launches ?? [] } as AgentMetadata;
  } catch {
    return null;
  }
}

export interface PendingChildLaunchesResult {
  pending: ChildLaunchReceipt[];
  accepted: ChildLaunchReceipt[];
  overflow: number;
}

/** Adds receipts without evicting evidence already waiting for a committed carrier request. */
export function appendPendingChildLaunches(
  pending: ChildLaunchReceipt[],
  additions: ChildLaunchReceipt[],
): PendingChildLaunchesResult {
  const next = pending.filter(isChildLaunchReceipt).map((receipt) => ({ ...receipt }));
  const seen = new Set(next.map(childLaunchKey));
  const accepted: ChildLaunchReceipt[] = [];
  let overflow = 0;
  for (const receipt of additions) {
    if (!isChildLaunchReceipt(receipt)) {
      overflow++;
      continue;
    }
    const key = childLaunchKey(receipt);
    if (seen.has(key)) continue;
    if (next.length >= MAX_PENDING_CHILD_LAUNCHES) {
      overflow++;
      continue;
    }
    const copy = { ...receipt };
    next.push(copy);
    accepted.push(copy);
    seen.add(key);
  }
  return { pending: next, accepted, overflow };
}

export interface PackedChildLaunchesResult {
  encoded: string | null;
  included: ChildLaunchReceipt[];
  remaining: ChildLaunchReceipt[];
  unencodableIndices: number[];
}

/** Selects the oldest receipts that fit beside frozen request identity and dependencies. */
export function packChildLaunches(
  identity: MetadataIdentity,
  streamId: string,
  pending: ChildLaunchReceipt[],
): PackedChildLaunchesResult {
  const base = cleanIdentity({ ...identity, childLaunches: [] }, streamId);
  if (!base) return { encoded: null, included: [], remaining: [...pending], unencodableIndices: [] };

  let encoded = JSON.stringify(base);
  if (Buffer.byteLength(encoded, "ascii") > AGENT_HEADER_BUDGET && base.depends_on !== null) {
    base.depends_on = null;
    encoded = JSON.stringify(base);
  }
  if (Buffer.byteLength(encoded, "ascii") > AGENT_HEADER_BUDGET || !/^[\x20-\x7e]+$/.test(encoded)) {
    return { encoded: null, included: [], remaining: [...pending], unencodableIndices: [] };
  }

  const included: ChildLaunchReceipt[] = [];
  const remaining: ChildLaunchReceipt[] = [];
  const unencodableIndices: number[] = [];
  const minimal = { ...base, depends_on: null, child_launches: [] };
  const minimalBytes = Buffer.byteLength(JSON.stringify(minimal), "ascii");

  for (const [index, receipt] of pending.entries()) {
    if (!isChildLaunchReceipt(receipt)) {
      unencodableIndices.push(index);
      continue;
    }
    const copy = { ...receipt };
    if (included.length >= MAX_CHILD_LAUNCHES_PER_ENVELOPE) {
      remaining.push(copy);
      continue;
    }
    const candidate = [...included, copy];
    const candidateEncoded = JSON.stringify({ ...base, child_launches: candidate });
    if (Buffer.byteLength(candidateEncoded, "ascii") <= AGENT_HEADER_BUDGET) {
      included.push(copy);
      encoded = candidateEncoded;
      continue;
    }
    const aloneEncoded = JSON.stringify({ ...minimal, child_launches: [copy] });
    if (minimalBytes > AGENT_HEADER_BUDGET || Buffer.byteLength(aloneEncoded, "ascii") > AGENT_HEADER_BUDGET) {
      unencodableIndices.push(index);
    } else {
      remaining.push(copy);
    }
  }

  return { encoded, included, remaining, unencodableIndices };
}

export function stripAgentMetadataHeader(headers: unknown): Record<string, string> | undefined {
  if (headers instanceof Headers) {
    const result: Record<string, string> = {};
    headers.forEach((value, name) => {
      if (name.toLowerCase() !== AGENT_HEADER.toLowerCase()) result[name] = value;
    });
    return result;
  }
  if (!headers || typeof headers !== "object" || Array.isArray(headers)) return undefined;
  return Object.fromEntries(Object.entries(headers as Record<string, unknown>)
    .filter(([name]) => name.toLowerCase() !== AGENT_HEADER.toLowerCase())) as Record<string, string>;
}

export function withAgentMetadataHeader(headers: unknown, encoded: string): Record<string, string> {
  const result = stripAgentMetadataHeader(headers);
  return { ...(result ?? {}), [AGENT_HEADER]: encoded };
}
