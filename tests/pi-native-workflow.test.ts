import { afterEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import tokenVisorProvider from "../extensions/tokenvisor-provider.ts";
import { parseAgentMetadata } from "../src/capture/shared.ts";

const fixturePath = new URL("./fixtures/pi-native-workflow-prefix-v1.json", import.meta.url);
const fixture = JSON.parse(readFileSync(fixturePath, "utf8")) as any;
const tempRoots: string[] = [];
const savedEnv = { ...process.env };
const savedArgv = process.argv;
const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  (globalThis as any).__PI_TEST_STREAM_RESULT__ = undefined;
  (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__ = undefined;
  (globalThis as any).__PI_TEST_CAPTURED_CONTEXT__ = undefined;
  for (const key of Object.keys(process.env)) if (!(key in savedEnv)) delete process.env[key];
  Object.assign(process.env, savedEnv);
  process.argv = savedArgv;
  for (const root of tempRoots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function ancestry(entries: any[], leaf: any): any[] {
  const byId = new Map(entries.filter((entry) => typeof entry.id === "string").map((entry) => [entry.id, entry]));
  const reverse: any[] = [];
  const seen = new Set<string>();
  let current = leaf;
  while (current && typeof current.id === "string" && !seen.has(current.id)) {
    seen.add(current.id);
    reverse.push(current);
    current = typeof current.parentId === "string" ? byId.get(current.parentId) : undefined;
  }
  return reverse.reverse();
}

function requestMessagesFromPiBranch(branch: any[]): any[] {
  return branch.flatMap((entry) => {
    if (entry.type === "message" && entry.message && typeof entry.message === "object") return [entry.message];
    if (entry.type === "custom_message") {
      if (typeof entry.content === "string") {
        const text = ["subagent-notify", "subagent-incremental-child-notify"].includes(entry.customType)
          ? `<pi_subagents_notice type="${entry.customType}" origin="automated message from the pi-subagents extension, not typed by the user">\n${entry.content}\n</pi_subagents_notice>`
          : entry.content;
        return [{ role: "user", content: [{ type: "text", text }] }];
      }
      if (Array.isArray(entry.content)) return [{ role: "user", content: entry.content }];
    }
    if (entry.type === "compaction" && typeof entry.summary === "string") {
      return [{ role: "user", content: [{ type: "text", text: entry.summary }] }];
    }
    return [];
  });
}

function writeSession(file: string, header: any, entries: any[]): void {
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, [header, ...entries].map((entry) => JSON.stringify(entry)).join("\n") + "\n");
}

describe("Pi native workflow request-prefix replay", () => {
  test("emits the independent native signature for all 29 actual provider-header boundaries", async () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-pi-native-workflow-test-"));
    tempRoots.push(root);
    const nativeFixture = structuredClone(fixture);
    const outputByRunId = new Map<string, string>();
    const witnessShapes: Record<string, { previewLength: number; responseLength: number; renderer: "workflow" | "direct" }> = {
      "8ccc4dab-4c91-43b5-bfb6-5d0c24519c88": { previewLength: 5, responseLength: 5, renderer: "workflow" },
      "2cab92e5-537d-40b0-9431-324b66961ccb": { previewLength: 5, responseLength: 5, renderer: "workflow" },
      "65d8fd74-26be-4327-8127-3e17e78ffecc": { previewLength: 5, responseLength: 5, renderer: "workflow" },
      "f3e53b45-7035-4ee7-816e-23e417e8658f": { previewLength: 8, responseLength: 8, renderer: "workflow" },
      "fd44a4de-45fb-457a-8a35-32329b9f7c8c": { previewLength: 373, responseLength: 1279, renderer: "workflow" },
      "f6a67d7d-8ffd-40b3-b46e-5dda24b0d1eb": { previewLength: 8, responseLength: 1101, renderer: "workflow" },
      "2d3c33b1-713f-497a-8015-28156e330eea": { previewLength: 53, responseLength: 1464, renderer: "direct" },
    };
    const sentinel = (runId: string, length: number) => runId.replaceAll("-", "").repeat(Math.ceil(length / 32)).slice(0, length);
    for (const status of nativeFixture.statuses as any[]) {
      if (status.mode !== "single" || typeof status.sessionFile !== "string" || !status.sessionFile.startsWith("@child:")) continue;
      const childSessionId = status.sessionFile.slice("@child:".length);
      const child = nativeFixture.child_sessions[childSessionId];
      const completion = child?.entries.find((entry: any) => entry.customType === "tokenvisor-agent-run-completion-v1" &&
        entry.data?.runId === status.runId && entry.data?.sessionId === childSessionId);
      const response = child?.entries.find((entry: any) => entry.message?.role === "assistant" &&
        entry.message?.responseId === completion?.data?.responseId);
      expect(completion, `missing native completion witness for ${status.runId}`).toBeDefined();
      expect(response, `missing native response for ${status.runId}`).toBeDefined();
      // The checked-in fixture redacts payload text. Inject a deterministic test-only
      // sentinel while retaining the private output's exact/prefix/substring shape.
      const shape = witnessShapes[status.runId];
      expect(shape, `missing redaction witness shape for ${status.runId}`).toBeDefined();
      const output = sentinel(status.runId, shape.previewLength - (shape.renderer === "workflow" ? 2 : 0));
      outputByRunId.set(status.runId, output);
      response.message.stopReason = "stop";
      const sourceOutput = `\n\n${output}`;
      const acceptanceStart = "\n```acceptance-report\n";
      const acceptanceEnd = "\n```";
      const paddingLength = shape.responseLength - sourceOutput.length - acceptanceStart.length - acceptanceEnd.length;
      const text = paddingLength >= 0
        ? `${sourceOutput}${acceptanceStart}${"x".repeat(paddingLength)}${acceptanceEnd}`
        : sourceOutput;
      expect(text.length).toBe(shape.responseLength);
      response.message.content = [{ type: "text", text }];
    }
    for (const entry of nativeFixture.root_session.entries as any[]) {
      if (entry.customType !== "subagent-notify" || typeof entry.content !== "string") continue;
      entry.content = entry.content.replace(
        /(- key=[A-Za-z0-9._:-]+ run=([A-Za-z0-9._:-]+) status=[A-Za-z0-9._:-]+\n(?:[^\n]*\n)*?    \| )\[redacted child output\]/g,
        (_match: string, prefix: string, runId: string) => `${prefix}\n    | \n    | ${outputByRunId.get(runId) ?? "[redacted child output]"}`,
      );
    }
    const rootFile = join(root, "root.jsonl");
    const rootHeader = nativeFixture.root_session.header;
    const rootEntries = nativeFixture.root_session.entries;
    writeSession(rootFile, rootHeader, rootEntries);

    const host = join(root, "node_modules", "@earendil-works", "pi-coding-agent");
    const ai = join(host, "node_modules", "@earendil-works", "pi-ai");
    const subagents = join(root, "npm", "node_modules", "pi-subagents");
    for (const path of [join(host, "dist"), join(ai, "dist", "api"), join(subagents, "bundle"), join(subagents, "src", "runs", "background")]) {
      mkdirSync(path, { recursive: true });
    }
    writeFileSync(join(host, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "1.4.0" }));
    writeFileSync(join(host, "dist", "cli.js"), "");
    writeFileSync(join(ai, "package.json"), JSON.stringify({ name: "@earendil-works/pi-ai", version: "1.4.0", type: "module" }));
    writeFileSync(join(ai, "dist", "api", "openai-completions.js"), 'export function streamSimple(model, context, options) { globalThis.__PI_TEST_CAPTURED_OPTIONS__ = options; globalThis.__PI_TEST_CAPTURED_CONTEXT__ = context; return { result: () => Promise.resolve(globalThis.__PI_TEST_STREAM_RESULT__) }; }');
    writeFileSync(join(subagents, "package.json"), JSON.stringify({
      name: "pi-subagents",
      version: "0.83.0",
      type: "module",
      exports: { "./required-child-extensions": "./bundle/required-child-extensions.js" },
    }));
    writeFileSync(join(subagents, "bundle", "required-child-extensions.js"), "export function registerRequiredChildExtensions() { return { dispose() {} }; }");
    writeFileSync(join(subagents, "src", "runs", "background", "subagent-runner-bootstrap.js"), "");

    process.env.PI_CODING_AGENT_DIR = root;
    process.env.PI_SUBAGENTS_TEMP_ROOT = root;
    process.env.TOKENVISOR_API_KEY = "synthetic-key";
    process.env.TOKENVISOR_BASE_URL = "http://127.0.0.1:1/api/v1";
    delete process.env.PI_SUBAGENT_CHILD;
    delete process.env.PI_SUBAGENT_RUNNER_PID;
    process.argv = [process.execPath, join(host, "dist", "cli.js")];
    globalThis.fetch = async () => Response.json({ data: [{ id: "synthetic", type: "chat", capabilities: [] }] });

    const childFiles = new Map<string, string>();
    const childEntries = new Map<string, any[]>();
    const statusesByRun = new Map<string, any>();
    const statusFiles = new Map<string, string>();
    for (const child of Object.entries(nativeFixture.child_sessions) as Array<[string, any]>) {
      const [sessionId, data] = child;
      const file = join(root, "children", `${sessionId}.jsonl`);
      const header = { ...data.header, parentSession: rootFile };
      writeSession(file, header, data.entries);
      childFiles.set(sessionId, file);
      childEntries.set(sessionId, data.entries);
    }
    for (const status of nativeFixture.statuses as any[]) {
      const safeStatus = { ...status };
      safeStatus.sessionId = rootFile;
      if (typeof safeStatus.sessionFile === "string" && safeStatus.sessionFile.startsWith("@child:")) {
        safeStatus.sessionFile = childFiles.get(safeStatus.sessionFile.slice("@child:".length));
      }
      safeStatus.steps = (safeStatus.steps ?? []).map((step: any) => ({
        ...step,
        sessionFile: typeof step.sessionFile === "string" && step.sessionFile.startsWith("@child:")
          ? childFiles.get(step.sessionFile.slice("@child:".length))
          : step.sessionFile,
      }));
      if (safeStatus.mode === "workflow") {
        const file = join(root, "async-subagent-runs", safeStatus.runId, "status.json");
        mkdirSync(dirname(file), { recursive: true });
        writeFileSync(file, JSON.stringify(safeStatus));
        statusesByRun.set(safeStatus.runId, safeStatus);
      } else {
        statusesByRun.set(safeStatus.runId, safeStatus);
        const file = join(root, "async-subagent-runs", safeStatus.runId, "status.json");
        mkdirSync(dirname(file), { recursive: true });
        writeFileSync(file, JSON.stringify(safeStatus));
        statusFiles.set(safeStatus.runId, file);
      }
    }
    for (const entry of nativeFixture.root_session.entries as any[]) {
      if (entry.customType !== "subagent-notify" || !Array.isArray(entry.details?.runs)) continue;
      for (const run of entry.details.runs) {
        if (run.workflowRunId !== undefined || typeof run.runId !== "string") continue;
        const status = statusesByRun.get(run.runId);
        const output = outputByRunId.get(run.runId);
        if (!status || !output) continue;
        run.agent = "worker";
        run.asyncDir = join(root, "async-subagent-runs", run.runId);
        run.sessionLabel = "Session file";
        run.sessionValue = status.sessionFile;
        entry.content = [
          "Background task completed: **worker**",
          "",
          "worker:",
          "",
          "",
          output,
          "",
          `Retention-managed async directory: ${run.asyncDir}`,
          "",
          `Session file: ${run.sessionValue}`,
          "",
          "Parent action: fixture://parent-action",
        ].join("\n");
      }
    }

    const hooks = new Map<string, (event: any, ctx: any) => unknown>();
    let providerConfig: any;
    const appendedEntries: any[] = [];
    const persistedConsumptionRecords: Array<{ sessionId: string; anchorResponseId: string; requestIndex: number; entry: any }> = [];
    let currentBranchForAppend: any[] | null = null;
    let currentSessionForAppend: string | null = null;
    let currentResponseForAppend: string | null = null;
    let currentRequestIndexForAppend = -1;
    await tokenVisorProvider({
      on: (name: string, handler: any) => hooks.set(name, handler),
      registerCommand: () => {},
      registerProvider: (_name: string, config: any) => { providerConfig = config; },
      appendEntry: (customType: string, data: any) => {
        const entry = {
          type: "custom_message",
          id: `replayed-agent-event-${appendedEntries.length + 1}`,
          parentId: currentBranchForAppend?.at(-1)?.id,
          customType,
          data,
        };
        appendedEntries.push(entry);
        currentBranchForAppend?.push(entry);
        if (customType === "tokenvisor-agent-result-consumption-v1") {
          if (currentSessionForAppend && currentResponseForAppend) {
            persistedConsumptionRecords.push({
              sessionId: currentSessionForAppend,
              anchorResponseId: currentResponseForAppend,
              requestIndex: currentRequestIndexForAppend,
              entry,
            });
          }
        }
      },
    } as any);
    const beforeHeaders = hooks.get("before_provider_headers");
    expect(beforeHeaders).toBeDefined();

    const requestByResponse = new Map((nativeFixture.requests as any[]).map((request) => [request.response_id, request]));
    const auditByResponse = new Map(Object.entries(nativeFixture.native_oracle.row_aliases_by_native_response_id).map(([responseId, alias]) => [responseId, alias]));
    const responseByAlias = new Map([...auditByResponse].map(([responseId, alias]) => [alias, responseId]));
    const sequenceEdges = nativeFixture.native_oracle.independent_edges.filter((edge: any) => edge.kind === "sequence");
    const delegationEdges = nativeFixture.native_oracle.independent_edges.filter((edge: any) => edge.kind === "delegated_run_start");
    const resultEdges = nativeFixture.native_oracle.independent_edges.filter((edge: any) => edge.kind === "result_dependency");
    const metadataByResponse = new Map<string, any>();

    for (const [requestIndex, request] of (nativeFixture.requests as any[]).entries()) {
      const childSessionId = request.session_id;
      const isRoot = childSessionId === rootHeader.id;
      let file = rootFile;
      let header = rootHeader;
      let branch: any[];
      let target: any;
      let activeStatus: any | null = null;
      if (isRoot) {
        target = rootEntries.find((entry: any) => entry.message?.responseId === request.response_id);
        expect(target).toBeDefined();
        branch = ancestry(rootEntries, target).slice(0, -1);
        process.argv = [process.execPath, join(host, "dist", "cli.js")];
        delete process.env.PI_SUBAGENT_CHILD;
        delete process.env.PI_SUBAGENT_RUNNER_PID;
      } else {
        file = childFiles.get(childSessionId)!;
        header = nativeFixture.child_sessions[childSessionId].header;
        header = { ...header, parentSession: rootFile };
        const status = [...statusesByRun.values()].find((value) => value.sessionFile === file);
        expect(status).toBeDefined();
        const parentCallId = status.parentWorkflowRunId
          ? statusesByRun.get(status.parentWorkflowRunId)?.toolCallId
          : status.toolCallId;
        const launch = rootEntries.find((entry: any) => entry.message?.role === "assistant" &&
          entry.message.content?.some((part: any) => part.type === "toolCall" && part.id === parentCallId));
        expect(launch).toBeDefined();
        const own = childEntries.get(childSessionId)!;
        target = own.find((entry: any) => entry.message?.responseId === request.response_id);
        expect(target).toBeDefined();
        branch = [...ancestry(rootEntries, launch), ...ancestry(own, target).slice(0, -1)];
        process.argv = [process.execPath, join(subagents, "src", "runs", "background", "subagent-runner-bootstrap.js"), join(root, `async-cfg-${status.runId}.json`)];
        process.env.PI_SUBAGENT_CHILD = "1";
        process.env.PI_SUBAGENT_RUNNER_PID = String(process.pid);
        activeStatus = { ...status, state: "running", pid: process.pid };
        writeFileSync(statusFiles.get(status.runId)!, JSON.stringify(activeStatus));
      }

      const ancestorResponseIds = new Set(branch.flatMap((entry) =>
        entry.message?.role === "assistant" && typeof entry.message.responseId === "string" ? [entry.message.responseId] : [],
      ));
      for (const record of persistedConsumptionRecords) {
        if (record.sessionId === childSessionId && record.requestIndex < requestIndex && ancestorResponseIds.has(record.anchorResponseId)) {
          branch.push(record.entry);
        }
      }

      const ctx = { sessionManager: {
        getSessionId: () => childSessionId,
        getSessionFile: () => file,
        getHeader: () => header,
        getBranch: () => branch,
      } };
      const headers = new Headers();
      beforeHeaders!({ headers }, ctx);
      if (activeStatus) writeFileSync(statusFiles.get(activeStatus.runId)!, JSON.stringify(statusesByRun.get(activeStatus.runId)));
      const candidateMetadata = parseAgentMetadata(headers.get("x-tokenvisor-agent"));
      expect(candidateMetadata, `missing metadata candidate for ${request.response_id}`).toBeDefined();
      const nativeResponse = target.message;
      (globalThis as any).__PI_TEST_STREAM_RESULT__ = {
        responseId: request.response_id,
        stopReason: nativeResponse.stopReason,
        content: nativeResponse.content,
      };
      const replayedStream = providerConfig.streamSimple(
        { id: "synthetic", api: "openai-completions", headers },
        { messages: requestMessagesFromPiBranch(branch) },
        { headers },
      );
      await replayedStream.result();
      await Promise.resolve();
      const outgoingHeaders = new Headers((globalThis as any).__PI_TEST_CAPTURED_OPTIONS__?.headers ?? {});
      const metadata = parseAgentMetadata(outgoingHeaders.get("x-tokenvisor-agent"));
      expect(metadata, `missing metadata for ${request.response_id}`).toBeDefined();
      metadataByResponse.set(request.response_id, metadata);

      if (nativeResponse.stopReason === "stop") {
        const branchAfterResponse = [...branch, target];
        currentBranchForAppend = branchAfterResponse;
        currentSessionForAppend = childSessionId;
        currentResponseForAppend = request.response_id;
        currentRequestIndexForAppend = requestIndex;
        const childStatus = isRoot ? null : [...statusesByRun.values()].find((value) => value.sessionFile === file);
        const savedChildStatus = childStatus ? { ...childStatus } : null;
        if (childStatus) {
          writeFileSync(statusFiles.get(childStatus.runId)!, JSON.stringify({ ...childStatus, state: "complete", pid: process.pid }));
        }
        hooks.get("agent_end")!({ messages: [nativeResponse] }, {
          sessionManager: {
            getSessionId: () => childSessionId,
            getSessionFile: () => file,
            getHeader: () => header,
            getBranch: () => branchAfterResponse,
          },
        });
        if (childStatus && savedChildStatus) writeFileSync(statusFiles.get(childStatus.runId)!, JSON.stringify(savedChildStatus));
        currentBranchForAppend = null;
        currentSessionForAppend = null;
        currentResponseForAppend = null;
        currentRequestIndexForAppend = -1;
      }

      const alias = auditByResponse.get(request.response_id);
      expect(alias).toBe(request.audit_id);
      const ownSequenceEdge = sequenceEdges.find((edge: any) => edge.target === alias);
      const expectedPreviousResponse = ownSequenceEdge ? responseByAlias.get(ownSequenceEdge.source) : null;
      expect(metadata!.sequence).toEqual(expectedPreviousResponse
        ? { status: "linked", previous_response_id: expectedPreviousResponse }
        : { status: "start", previous_response_id: null });

      const expectedDependencies = resultEdges.filter((edge: any) => edge.target === alias).map((edge: any) => {
        const sourceResponse = responseByAlias.get(edge.source)!;
        const sourceRequest = requestByResponse.get(sourceResponse)!;
        const sourceStatus = [...statusesByRun.values()].find((value) => value.sessionId === rootFile && value.sessionFile === childFiles.get(sourceRequest.session_id));
        return {
          session_id: `pi:session:${sourceRequest.session_id}`,
          response_id: sourceResponse,
          run_id: sourceStatus.runId,
        };
      });
      expect(metadata!.depends_on).toEqual(expectedDependencies);

      const expectedDelegation = isRoot ? null : (() => {
        const status = [...statusesByRun.values()].find((value) => value.sessionFile === file)!;
        const toolCallId = status.parentWorkflowRunId ? statusesByRun.get(status.parentWorkflowRunId)?.toolCallId : status.toolCallId;
        return { run_id: status.runId, tool_call_id: toolCallId };
      })();
      expect(metadata!.delegation).toEqual(expectedDelegation);
    }

    expect(metadataByResponse.size).toBe(29);
    expect([...metadataByResponse.values()].filter((metadata) => metadata.sequence.status === "linked")).toHaveLength(21);
    expect([...metadataByResponse.values()].filter((metadata) => metadata.sequence.status === "start")).toHaveLength(8);
    expect([...metadataByResponse.values()].filter((metadata) => metadata.sequence.status === "unavailable")).toHaveLength(0);
    expect([...metadataByResponse.values()].reduce((count, metadata) => count + (metadata.depends_on?.length ?? 0), 0)).toBe(7);
    expect(delegationEdges).toHaveLength(7);
    expect(resultEdges).toHaveLength(7);

    const consumedResponseId = "chatcmpl-a7a860a5ede4f4e1";
    const committedConsumption = appendedEntries.find((entry) =>
      entry.customType === "tokenvisor-agent-result-consumption-v1" && entry.data.response_id === consumedResponseId);
    expect(committedConsumption, "the registered provider wrapper commits the delivered result").toBeDefined();
    expect(committedConsumption.data.dependencies).toEqual(metadataByResponse.get(consumedResponseId).depends_on);

    const failedResponseId = "chatcmpl-acec830f37ab994a";
    const failedNativeResponse = rootEntries.find((entry: any) => entry.message?.responseId === failedResponseId);
    const failedBranch = ancestry(rootEntries, failedNativeResponse).slice(0, -1);
    const failedAncestorResponseIds = new Set(failedBranch.flatMap((entry) =>
      entry.message?.role === "assistant" && typeof entry.message.responseId === "string" ? [entry.message.responseId] : [],
    ));
    for (const record of persistedConsumptionRecords) {
      if (record.sessionId === rootHeader.id && failedAncestorResponseIds.has(record.anchorResponseId)) failedBranch.push(record.entry);
    }
    const failedHeaders = new Headers();
    beforeHeaders!({ headers: failedHeaders }, {
      sessionManager: {
        getSessionId: () => rootHeader.id,
        getSessionFile: () => rootFile,
        getHeader: () => rootHeader,
        getBranch: () => failedBranch,
      },
    });
    expect(parseAgentMetadata(failedHeaders.get("x-tokenvisor-agent"))?.depends_on).toHaveLength(1);
    failedBranch.push(failedNativeResponse);
    (globalThis as any).__PI_TEST_STREAM_RESULT__ = { responseId: failedResponseId, stopReason: "error", content: [] };
    await providerConfig.streamSimple(
      { id: "synthetic", api: "openai-completions", headers: failedHeaders },
      { messages: requestMessagesFromPiBranch(failedBranch) },
      { headers: failedHeaders },
    ).result();
    await Promise.resolve();
    const committedCountBeforeError = appendedEntries.filter((entry) => entry.customType === "tokenvisor-agent-result-consumption-v1").length;
    const agentEnd = hooks.get("agent_end");
    expect(agentEnd).toBeDefined();
    agentEnd!({ messages: [{ role: "assistant", responseId: failedResponseId, stopReason: "error" }] }, {
      sessionManager: {
        getSessionId: () => rootHeader.id,
        getSessionFile: () => rootFile,
        getHeader: () => rootHeader,
        getBranch: () => failedBranch,
      },
    });
    expect(appendedEntries.filter((entry) => entry.customType === "tokenvisor-agent-result-consumption-v1")).toHaveLength(committedCountBeforeError);
  });
});
