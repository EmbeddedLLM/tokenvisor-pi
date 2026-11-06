import { describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import {
  detectRuntimeHost,
  isNativeForkStartup,
  parseOmpVersionOutput,
  readOmpHarnessVersion,
  readPiHarnessVersion,
  readPiSubagentRunnerVersion,
  resolvePiSubagentPackageRoot,
} from "../src/capture/runtime-host.ts";

describe("runtime host identification", () => {
  test("accepts the source-identified Pi 1.4 child runner and resolves its host package root", () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-runner-host-"));
    try {
      writeFileSync(join(root, "package.json"), JSON.stringify({
        name: "@earendil-works/pi-coding-agent",
        version: "1.4.0",
      }));
      const env = {
        PI_SUBAGENT_CHILD: "1",
        PI_SUBAGENT_RUNNER_PID: "7812",
        PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT: root,
      } as NodeJS.ProcessEnv;
      expect(resolvePiSubagentPackageRoot(env, 7812)).toBe(root);
      expect(detectRuntimeHost("/tmp/subagent-runner.js", env, 7812)).toBe("pi");
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("rejects absent markers, a stale runner PID, and a package root with the wrong identity", () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-runner-reject-"));
    try {
      writeFileSync(join(root, "package.json"), JSON.stringify({
        name: "@earendil-works/not-pi-coding-agent",
        version: "1.5.0",
      }));
      const env = {
        PI_SUBAGENT_CHILD: "1",
        PI_SUBAGENT_RUNNER_PID: "7812",
        PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT: root,
      } as NodeJS.ProcessEnv;
      expect(resolvePiSubagentPackageRoot(env, 7813)).toBeNull();
      expect(resolvePiSubagentPackageRoot({ ...env, PI_SUBAGENT_CHILD: undefined }, 7812)).toBeNull();
      expect(resolvePiSubagentPackageRoot(env, 7812)).toBeNull();
      expect(() => detectRuntimeHost("/tmp/subagent-runner.js", env, 7813)).toThrow();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("keeps ordinary Pi and compiled OMP executable detection", () => {
    expect(detectRuntimeHost("/usr/local/bin/pi", {}, 10)).toBe("pi");
    expect(detectRuntimeHost("/tmp/omp-linux-x64", {}, 10)).toBe("omp");
    expect(() => detectRuntimeHost("/tmp/other-agent", {}, 10)).toThrow();
  });

  test("recognizes only startup forks confirmed by the pinned host's native header shape", () => {
    const piArgv = ["node", "/usr/bin/pi", "--fork", "/sessions/source.jsonl"];
    expect(isNativeForkStartup({
      argv: piArgv,
      host: "pi",
      sessionId: "fork-session",
      currentHeader: { id: "fork-session", parentSession: "/sessions/source.jsonl" },
      parentHeader: { id: "source-session" },
      reason: "startup",
    })).toBe(true);
    expect(isNativeForkStartup({
      argv: piArgv,
      host: "pi",
      sessionId: "fork-session",
      currentHeader: { id: "fork-session", parentSession: "/sessions/source.jsonl" },
      parentHeader: { id: "source-session" },
      piSubagentChild: "1",
      reason: "startup",
    })).toBe(false);
    expect(isNativeForkStartup({
      argv: ["bun", "/omp", "--fork", "source-session"],
      host: "omp",
      sessionId: "fork-session",
      currentHeader: { id: "fork-session", parentSession: "source-session" },
      agentKind: "main",
      reason: null,
    })).toBe(true);
    expect(isNativeForkStartup({
      argv: ["bun", "/omp", "--fork", "source-session"],
      host: "omp",
      sessionId: "fork-session",
      currentHeader: { id: "fork-session", parentSession: "source-session" },
      agentKind: "sub",
      reason: null,
    })).toBe(false);
    expect(isNativeForkStartup({
      argv: ["bun", "/omp", "--resume", "source-session"],
      host: "omp",
      sessionId: "fork-session",
      currentHeader: { id: "fork-session", parentSession: "source-session" },
      agentKind: "main",
      reason: null,
    })).toBe(false);
  });

  test("records actual host and runner versions from verified manifests and executable output", () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-version-facts-"));
    try {
      const piRoot = join(root, "node_modules", "@earendil-works", "pi-coding-agent");
      const piEntry = join(piRoot, "dist", "bundle", "cli.js");
      const runnerRoot = join(root, "node_modules", "pi-subagents");
      const runnerEntry = join(runnerRoot, "src", "runs", "background", "subagent-runner-bootstrap.js");
      mkdirSync(dirname(piEntry), { recursive: true });
      mkdirSync(dirname(runnerEntry), { recursive: true });
      writeFileSync(join(piRoot, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "1.4.0" }));
      writeFileSync(join(runnerRoot, "package.json"), JSON.stringify({ name: "pi-subagents", version: "0.83.0" }));
      writeFileSync(piEntry, "");
      writeFileSync(runnerEntry, "");
      expect(readPiHarnessVersion(piEntry)).toBe("1.4.0");
      expect(readPiSubagentRunnerVersion(runnerEntry)).toBe("0.83.0");
      expect(readPiHarnessVersion(piEntry)).toBe("1.4.0");
      expect(readPiHarnessVersion(join(root, "unverified.js"))).toBeNull();
      writeFileSync(join(runnerRoot, "package.json"), JSON.stringify({ name: "pi-subagents", version: "0.84.0" }));
      expect(readPiSubagentRunnerVersion(runnerEntry)).toBe("0.84.0");
      writeFileSync(join(runnerRoot, "package.json"), JSON.stringify({ name: "pi-subagents", version: "0.83.0" }));

      const childEnv = {
        PI_SUBAGENT_CHILD: "1",
        PI_SUBAGENT_RUNNER_PID: "7812",
        PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT: piRoot,
      } as NodeJS.ProcessEnv;
      expect(readPiHarnessVersion(runnerEntry, childEnv, 7812)).toBe("1.4.0");
      expect(readPiHarnessVersion(runnerEntry, childEnv, 7813)).toBeNull();

      writeFileSync(join(piRoot, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "1.5.0" }));
      expect(readPiHarnessVersion(piEntry)).toBe("1.5.0");
      expect(parseOmpVersionOutput("omp/18.12.6\n")).toBe("18.12.6");
      expect(parseOmpVersionOutput("18.12.6\n")).toBeNull();
      expect(readOmpHarnessVersion("/nonexistent/omp")).toBeNull();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
