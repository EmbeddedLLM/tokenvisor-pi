import { closeSync, openSync, readSync, realpathSync } from "fs";

function isNativeId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 256 && /^[A-Za-z0-9._:-]+$/.test(value);
}

/** Read a native Pi/OMP session header from a bounded JSONL preamble. */
export function readNativeSessionHeader(sessionFile: string | undefined): { id: string; parentSession?: string } | null {
  if (!sessionFile) return null;
  let descriptor: number | undefined;
  try {
    descriptor = openSync(sessionFile, "r");
    const buffer = Buffer.alloc(4096);
    const bytesRead = readSync(descriptor, buffer, 0, buffer.length, 0);
    const lines = buffer.subarray(0, bytesRead).toString("utf8").split("\n").slice(0, 8);
    for (const line of lines) {
      if (!line) continue;
      try {
        const header = JSON.parse(line) as { type?: unknown; id?: unknown; parentSession?: unknown };
        if (header.type === "session" && isNativeId(header.id)) {
          return { id: header.id, ...(typeof header.parentSession === "string" ? { parentSession: header.parentSession } : {}) };
        }
      } catch {
        // Continue through bounded native preamble entries.
      }
    }
    return null;
  } catch {
    return null;
  } finally {
    if (descriptor !== undefined) closeSync(descriptor);
  }
}

export function readNativeSessionRootId(sessionFile: string | undefined): string | null {
  const visited = new Set<string>();
  let current = sessionFile;
  let rootId: string | null = null;
  for (let depth = 0; current && depth < 16; depth += 1) {
    let canonical: string;
    try { canonical = realpathSync(current); } catch { return null; }
    if (visited.has(canonical)) return null;
    visited.add(canonical);
    const header = readNativeSessionHeader(canonical);
    if (!header) return null;
    rootId = header.id;
    current = header.parentSession;
  }
  return current ? null : rootId;
}
