import { existsSync, readFileSync, realpathSync } from "fs";
import { dirname, join } from "path";
import { createRequire } from "module";
import { homedir } from "os";
import { pathToFileURL } from "url";
import { resolvePiSubagentPackageRoot } from "./runtime-host.ts";

export interface PiRequiredExtensionsApi {
  packageVersion?: string;
  registerRequiredChildExtensions(input: {
    sessionId: string;
    extensions: Array<{ id: string; path: string }>;
  }): { dispose(): void };
}

export interface PiCompactionRendererApi {
  convertToLlm(messages: unknown[]): unknown[];
  serializeConversation(messages: unknown[]): string;
  sessionEntryToContextMessages(entry: unknown): unknown[];
}

/** Load the host's public context renderer when its required methods are available. */
export async function loadPiCompactionRenderer(startPath: string | undefined): Promise<PiCompactionRendererApi | null> {
  const hostRoot = findPiCodingAgentPackageRoot(startPath) ?? resolvePiSubagentPackageRoot();
  if (!hostRoot) return null;
  try {
    const manifest = JSON.parse(readFileSync(join(hostRoot, "package.json"), "utf8")) as { name?: string };
    if (manifest.name !== "@earendil-works/pi-coding-agent") return null;
    const entry = pathToFileURL(join(hostRoot, "dist/index.js")).href;
    const api = await import(entry) as Partial<PiCompactionRendererApi>;
    if (typeof api.convertToLlm !== "function" || typeof api.serializeConversation !== "function" ||
        typeof api.sessionEntryToContextMessages !== "function") return null;
    return api as PiCompactionRendererApi;
  } catch {
    return null;
  }
}

/** Use pi-subagents' public API when installed; it is not a provider dependency. */
export async function loadPiRequiredExtensionsApi(): Promise<PiRequiredExtensionsApi | null> {
  const agentDir = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
  const require = createRequire(import.meta.url);
  try {
    const entry = require.resolve("pi-subagents/required-child-extensions", {
      paths: [join(agentDir, "npm"), process.cwd()],
    });
    let packageRoot = dirname(entry);
    let manifest: { name?: unknown; version?: unknown } | null = null;
    while (true) {
      try {
        const candidate = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8")) as { name?: unknown; version?: unknown };
        if (candidate.name === "pi-subagents") {
          manifest = candidate;
          break;
        }
      } catch {
        // Export targets may be nested arbitrarily deeply inside their package.
      }
      const parent = dirname(packageRoot);
      if (parent === packageRoot) break;
      packageRoot = parent;
    }
    if (!manifest) return null;
    const api = await import(pathToFileURL(entry).href) as Partial<PiRequiredExtensionsApi>;
    if (typeof api.registerRequiredChildExtensions !== "function") return null;
    return {
      registerRequiredChildExtensions: api.registerRequiredChildExtensions.bind(api),
      ...(typeof manifest.version === "string" && manifest.version.length > 0 ? { packageVersion: manifest.version } : {}),
    };
  } catch {
    return null;
  }
}

/** Locate the host's pinned Pi AI package from its executable or extension path. */
export function findPiAiPackageRoot(...startPaths: Array<string | undefined>): string | null {
  for (const start of startPaths) {
    if (!start) continue;
    let current: string;
    try {
      current = dirname(realpathSync(start));
    } catch {
      continue;
    }
    while (true) {
      const packageJson = join(current, "package.json");
      if (existsSync(packageJson)) {
        try {
          const packageInfo = JSON.parse(readFileSync(packageJson, "utf8")) as { name?: string };
          if (packageInfo.name === "@earendil-works/pi-coding-agent") {
            const searchPaths = createRequire(packageJson).resolve.paths("@earendil-works/pi-ai") ?? [];
            for (const searchPath of searchPaths) {
              const candidate = join(searchPath, "@earendil-works", "pi-ai");
              if (existsSync(candidate)) return findVerifiedPiAiRoot(candidate);
            }
          }
        } catch {
          // Continue walking when an ancestor is not a readable package.
        }
      }
      const parent = dirname(current);
      if (parent === current) break;
      current = parent;
    }
  }
  return null;
}

function findPiCodingAgentPackageRoot(startPath: string | undefined): string | null {
  if (!startPath) return null;
  let current: string;
  try { current = dirname(realpathSync(startPath)); } catch { return null; }
  while (true) {
    try {
      const packageInfo = JSON.parse(readFileSync(join(current, "package.json"), "utf8")) as { name?: unknown };
      if (packageInfo.name === "@earendil-works/pi-coding-agent") {
        return current;
      }
    } catch {
      // Continue through executable ancestors until the pinned host package is found.
    }
    const parent = dirname(current);
    if (parent === current) return null;
    current = parent;
  }
}

function findVerifiedPiAiRoot(packageRoot: string): string | null {
  try {
    const resolvedRoot = realpathSync(packageRoot);
    const packageInfo = JSON.parse(readFileSync(join(resolvedRoot, "package.json"), "utf8")) as { name?: unknown };
    return packageInfo.name === "@earendil-works/pi-ai" ? resolvedRoot : null;
  } catch {
    return null;
  }
}
