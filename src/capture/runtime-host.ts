import { execFileSync } from "child_process";
import { readFileSync, realpathSync } from "fs";
import { basename, dirname, join, relative, resolve } from "path";

export type RuntimeHost = "pi" | "omp";
export const PI_HOST_VERSION = "1.4.0";
export const OMP_HOST_VERSION = "18.12.6";
export const PI_SUBAGENTS_VERSION = "0.83.0";
const ompVersionCache = new Map<string, string | null>();

interface NativeSessionHeaderLike {
  id?: unknown;
  parentSession?: unknown;
}

export function isNativeForkStartup(input: {
  argv: string[];
  host: RuntimeHost;
  sessionId: string;
  currentHeader: NativeSessionHeaderLike | null;
  parentHeader?: NativeSessionHeaderLike | null;
  agentKind?: string;
  piSubagentChild?: string;
  reason?: string | null;
}): boolean {
  const args = input.argv.slice(2);
  const hasForkFlag = args.some((arg, index) => arg === "--fork" && Boolean(args[index + 1]) || arg.startsWith("--fork="));
  if (!hasForkFlag || (input.reason !== undefined && input.reason !== null && input.reason !== "startup")) return false;
  if (input.currentHeader?.id !== input.sessionId || typeof input.currentHeader.parentSession !== "string") return false;
  if (input.host === "pi") {
    return input.piSubagentChild !== "1" && input.parentHeader?.id !== undefined && input.parentHeader.id !== null;
  }
  return input.agentKind === "main" && /^[A-Za-z0-9._:-]{1,256}$/.test(input.currentHeader.parentSession);
}

export function resolvePiSubagentPackageRoot(
  env: NodeJS.ProcessEnv = process.env,
  currentPid = process.pid,
): string | null {
  if (env.PI_SUBAGENT_CHILD !== "1" || env.PI_SUBAGENT_RUNNER_PID !== String(currentPid)) return null;
  const packageRoot = env.PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT?.trim();
  if (!packageRoot) return null;
  try {
    const canonical = realpathSync(packageRoot);
    const packageInfo = JSON.parse(readFileSync(resolve(canonical, "package.json"), "utf8")) as {
      name?: unknown;
      version?: unknown;
    };
    return packageInfo.name === "@earendil-works/pi-coding-agent" && packageInfo.version === PI_HOST_VERSION
      ? canonical
      : null;
  } catch {
    return null;
  }
}

export function readPiHarnessVersion(
  argv1: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
  currentPid = process.pid,
): string | null {
  const childRoot = resolvePiSubagentPackageRoot(env, currentPid);
  if (childRoot) return readPackageVersion(childRoot, "@earendil-works/pi-coding-agent");
  if (!argv1) return null;
  let current: string;
  try { current = dirname(realpathSync(argv1)); } catch { return null; }
  for (let depth = 0; depth < 12; depth += 1) {
    const version = readPackageVersion(current, "@earendil-works/pi-coding-agent");
    if (version) return version;
    const parent = dirname(current);
    if (parent === current) break;
    current = parent;
  }
  return null;
}

export function readPiSubagentRunnerVersion(argv1: string | undefined): string | null {
  if (!argv1) return null;
  let canonical: string;
  try { canonical = realpathSync(argv1); } catch { return null; }
  let current = dirname(canonical);
  for (let depth = 0; depth < 8; depth += 1) {
    const version = readPackageVersion(current, "pi-subagents");
    if (version) {
      const relativePath = relative(current, canonical).replaceAll("\\", "/");
      return /^src\/runs\/background\/subagent-runner-bootstrap\.(?:js|ts)$/.test(relativePath)
        ? version
        : null;
    }
    const parent = dirname(current);
    if (parent === current) break;
    current = parent;
  }
  return null;
}

export function readOmpHarnessVersion(executable = process.execPath): string | null {
  const cached = ompVersionCache.get(executable);
  if (cached !== undefined) return cached;
  let version: string | null = null;
  try {
    const output = execFileSync(executable, ["--version"], {
      encoding: "utf8",
      timeout: 2000,
      maxBuffer: 2048,
      stdio: ["ignore", "pipe", "ignore"],
    });
    version = parseOmpVersionOutput(output);
  } catch {
    version = null;
  }
  ompVersionCache.set(executable, version);
  return version;
}

export function parseOmpVersionOutput(output: string): string | null {
  return /^omp\/(\d+\.\d+\.\d+)$/m.exec(output.trim())?.[1] ?? null;
}

export function supportsAgentMetadataCapture(
  host: RuntimeHost,
  harnessVersion: string | null,
  runnerVersion?: string | null,
  piChildRuntime = false,
): boolean {
  if (host === "omp") return harnessVersion === OMP_HOST_VERSION;
  return harnessVersion === PI_HOST_VERSION && (!piChildRuntime || runnerVersion === PI_SUBAGENTS_VERSION);
}

function readPackageVersion(packageRoot: string, expectedName: string): string | null {
  try {
    const manifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8")) as { name?: unknown; version?: unknown };
    return manifest.name === expectedName && typeof manifest.version === "string" ? manifest.version : null;
  } catch {
    return null;
  }
}

export function detectRuntimeHost(
  argv1: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
  currentPid = process.pid,
): RuntimeHost {
  if (resolvePiSubagentPackageRoot(env, currentPid)) return "pi";
  if (!argv1) throw new Error("Cannot identify TokenVisor extension host");
  const executable = resolve(argv1);
  const name = basename(executable).toLowerCase();
  if (name === "omp" || /^omp-(?:linux|darwin|win32)-(?:x64|arm64)$/.test(name) || executable.includes("/@oh-my-pi/pi-coding-agent/")) return "omp";
  if (name === "pi" || executable.includes("/@earendil-works/pi-coding-agent/")) return "pi";
  throw new Error("Unsupported TokenVisor provider host executable");
}
