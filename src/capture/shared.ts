import { randomUUID } from "crypto";

export const AGENT_HEADER = "X-TokenVisor-Agent";
export const AGENT_HEADER_BUDGET = 4096;

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
}

function isSafeId(value: unknown, maxLength = 256): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength && /^[A-Za-z0-9._:-]+$/.test(value);
}

function isBoundedAscii(value: unknown, maxLength: number): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength && /^[\x20-\x7e]+$/.test(value);
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
    Array.isArray(identity.dependsOn) && identity.dependsOn.length <= 8 &&
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
  };
}

export function encodeAgentMetadata(identity: MetadataIdentity, streamId?: string): string | null {
  const metadata = cleanIdentity(identity, streamId);
  if (!metadata) return null;
  const encoded = JSON.stringify(metadata);
  if (!/^[\x20-\x7e]+$/.test(encoded) || Buffer.byteLength(encoded, "ascii") > AGENT_HEADER_BUDGET) return null;
  return encoded;
}

export function parseAgentMetadata(value: unknown): AgentMetadata | null {
  if (typeof value !== "string" || Buffer.byteLength(value, "ascii") > AGENT_HEADER_BUDGET) return null;
  try {
    const parsed = JSON.parse(value) as AgentMetadata;
    if (parsed.version !== 1 || !isSafeId(parsed.stream_id) || !isSafeId(parsed.session?.id)) return null;
    return parsed;
  } catch {
    return null;
  }
}

export function headerValue(headers: unknown, name = AGENT_HEADER): string | undefined {
  if (headers instanceof Headers) return headers.get(name) ?? undefined;
  if (!headers || typeof headers !== "object") return undefined;
  const record = headers as Record<string, unknown>;
  const key = Object.keys(record).find((candidate) => candidate.toLowerCase() === name.toLowerCase());
  return key && typeof record[key] === "string" ? record[key] as string : undefined;
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

export function writeAgentMetadataHeader(headers: unknown, encoded: string): void {
  if (headers instanceof Headers) {
    headers.set(AGENT_HEADER, encoded);
    return;
  }
  if (headers && typeof headers === "object" && !Array.isArray(headers)) {
    (headers as Record<string, unknown>)[AGENT_HEADER] = encoded;
  }
}

export function withAgentMetadataHeader(headers: unknown, encoded: string): Record<string, string> {
  const result = stripAgentMetadataHeader(headers);
  return { ...(result ?? {}), [AGENT_HEADER]: encoded };
}

/** Clears every case variant before a native request snapshots new identity. */
export function clearAgentMetadataHeader(headers: unknown): void {
  if (headers instanceof Headers) {
    headers.delete(AGENT_HEADER);
    return;
  }
  if (!headers || typeof headers !== "object" || Array.isArray(headers)) return;
  for (const name of Object.keys(headers)) {
    if (name.toLowerCase() === AGENT_HEADER.toLowerCase()) delete (headers as Record<string, unknown>)[name];
  }
}

export function recordFreshRequestMetadata(cache: Map<string, string>, encoded: string, maxEntries = 256): boolean {
  const metadata = parseAgentMetadata(encoded);
  if (!metadata || !Number.isSafeInteger(maxEntries) || maxEntries < 1) return false;
  cache.set(metadata.stream_id, encoded);
  while (cache.size > maxEntries) cache.delete(cache.keys().next().value as string);
  return true;
}

export function claimFreshRequestMetadata(
  cache: Map<string, string>,
  candidate: unknown,
): { encoded: string; metadata: AgentMetadata } | null {
  if (typeof candidate !== "string") return null;
  const parsed = parseAgentMetadata(candidate);
  if (!parsed) return null;
  const encoded = cache.get(parsed.stream_id);
  cache.delete(parsed.stream_id);
  if (!encoded || encoded !== candidate) return null;
  const metadata = parseAgentMetadata(encoded);
  return metadata ? { encoded, metadata } : null;
}
