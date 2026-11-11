# tokenvisor-pi

[TokenVisor](https://devstudio.tokenvisor.ai) custom provider for [pi](https://github.com/earendil-works/pi-coding-agent) and [oh-my-pi (omp)](https://github.com/can1357/oh-my-pi).

Dynamically discovers models from your TokenVisor account and registers them as a provider. Both hosts use the provider registration API; credential storage differs, which this extension bridges automatically (see [How it works](#how-it-works)). Capture hooks and runtime adapters are host-specific.

## Install

### Pi

```bash
pi install git:github.com/EmbeddedLLM/tokenvisor-pi
```

Or add to `.pi/settings.json`:

```json
{ "packages": ["git:github.com/EmbeddedLLM/tokenvisor-pi"] }
```

### OMP

```bash
omp plugin install https://github.com/EmbeddedLLM/tokenvisor-pi
```

Or add to `~/.omp/plugins/package.json`:

```json
{
  "dependencies": {
    "tokenvisor-pi": "https://github.com/EmbeddedLLM/tokenvisor-pi"
  }
}
```

The `package.json` `pi` manifest is read by both hosts (omp falls back to the `pi` field).

### Update or replace a development installation

After the changes are pushed to GitHub, update an existing GitHub installation with:

```bash
pi update git:github.com/EmbeddedLLM/tokenvisor-pi
omp plugin install https://github.com/EmbeddedLLM/tokenvisor-pi
```

OMP refreshes direct GitHub packages through `plugin install`. Its `plugin upgrade` command handles marketplace installations using a `name@marketplace` ID.

If you installed from a local development path, first remove that registration. Use `pi list` to find the configured source, then run `pi remove <source>`. For a linked OMP installation, run `omp plugin uninstall tokenvisor-pi`. Install again using the GitHub command for each host above. A local path or link continues loading the local checkout until you replace it.

Restart Pi or OMP after installing or updating, then select a TokenVisor model with `/model`. These commands install the TokenVisor provider into your existing Pi/OMP installations.

To verify the installed release, run `omp plugin list`, or use `pi list` to locate Pi's package checkout and read its `package.json` version. New captured requests report the same package version in `source.extension_version`. Already-running sessions keep their loaded code until restarted; the metadata envelope's `version` remains `1` independently of package releases.

## Agent metadata

On supported runtimes, the extension automatically attaches a bounded `X-TokenVisor-Agent` envelope to TokenVisor provider calls. The envelope contains session, stream, sequence, delegation, and dependency identifiers; it never contains prompts or tool output. Transport retries reuse the same envelope. There is no capture enable/disable setting.

Pi's global header hook keeps the request's native evidence private. Only the registered TokenVisor provider wrapper claims that evidence and adds the envelope to its outgoing request. Requests routed to other providers receive no injected metadata and do not consume pending launch receipts, even when the selected model differs from the physical request provider. Evidence is frozen at the native hook boundary, before child status files can change, and each request claim is single-use.

For local metadata diagnostics, optionally set `TOKENVISOR_AGENT_METADATA_DIAGNOSTICS=/path/to/private.jsonl`. This writes identifiers and extraction statuses with file mode `0600`; it does not control metadata transmission.

When capture detects a specific limitation, it warns once per session and distinct issue in the process that request linkage may be incomplete. The provider continues inference and keeps any valid metadata it can establish. Unknown package versions alone do not trigger a warning. See [Known OMP provenance gaps](#known-omp-provenance-gaps) for the affected paths and their audit/export impact.

Run `bun test` and `bun run typecheck` from this package checkout to validate the producer. Producer CI runs these checks after `npm ci` with Node `22.19.0` and Bun `1.3.14`; live Pi/OMP qualification remains a separate opt-in procedure and is not run by this CI job.

Run `npm run benchmark:capture` for optional synthetic capture timings. The timing loops and reports live in `scripts/benchmarks/capture.ts` and reuse the regression fixtures through small benchmark entry points. They check fixture correctness and report timings without performance thresholds. These measurements use fake delegates, require no running server or native Pi/OMP process, and do not measure HTTP serialization, network latency, model latency, or cache behavior. Ordinary producer CI does not run these experiments.

To check that this producer's canonical schema and examples match TokenVisor's backend fixture copies, run `bun run check:backend-conformance -- /path/to/TokenVisor/services/api/tests/utils/audit/fixtures`. The checker reads that supplied fixture directory and compares those two JSON contracts structurally, ignoring formatting. It does not fetch a backend repository or query a running backend. Backend contract tests separately validate the copied examples against its DTO.

### Runtime qualification

Capture activates from the available host APIs and verified native session/run evidence, rather than an exact package-version allowlist. Detected Pi, OMP and applicable pi-subagents versions are recorded as provenance; unavailable host versions are explicitly `unknown`. A compatible upgrade does not disable capture merely because its version changed. If the host's OpenAI stream API is unavailable, the extension warns and registers the standard provider without capture; other missing capabilities or ambiguous native evidence leave the affected relationship unavailable without inventing identities. Install the complete package checkout through the host's extension/package manager; metadata modules are part of that checkout, so loading a loose copy of `extensions/tokenvisor-provider.ts` is not supported.

With pi-subagents installed in Pi's agent npm directory or resolvable from the current project, the provider checks and uses the public `pi-subagents/required-child-extensions` registration API on session start and disposes its registration on shutdown. Native child launches retain the provider even with `extensions: []`; this includes workflow children. An existing registration owned by another host is preserved and diagnosed locally instead of interrupting the parent. When that optional API is unavailable, child capture still requires an explicit extension allowlist. This does not enable capture in external CLI or remote runners.

Inherited parent entries are input context, not earlier requests by the child. The Pi adapter excludes their native entry IDs from child sequencing and dependency extraction. Missing, malformed, interleaved, or over-8-MiB parent evidence leaves linkage unknown. Workflow children retain their own execution run IDs across a continued native session. Their initiating parent tool call is resolved exactly from native evidence; foreground workflows do not require an async parent workflow status file, while any existing malformed or contradictory status still rejects the binding. Final workflow notifications expand into exact child completion references. Valid incremental notifications are status-only and add no output-consumption dependency. Missing output proof, ambiguous inventories or response identities, and dependency-budget overflow remain unknown. Run completion records identify responses; output comparison verifies delivery rather than selecting response identity. These rules do not reconstruct arbitrary script-internal dependencies between workflow children.

Compaction carries consumption proof only for output notices retained by the native context boundary, rather than every historical child output. Irrelevant old consumption does not exhaust the eight-reference budget; genuinely required over-budget proof remains unknown. Committed proof survives expiration of async child artifacts, including exact response references without a run ID. When a notice is removed from context, its first covering native compaction must have an available captured ordinary summary; continuation follows the summary frontier without reopening old child files. An unavailable first checkpoint, a source-marked mechanical compaction covering fresh output, or an unresolved supplied cutoff leaves that relationship unknown. Both native summary outputs can contribute to continuation even though the second summary request is sequence-linked to the first without consuming its output.

A native Pi loopback validation covers two detached workflow children (one inheriting history), each issuing two requests, with no child extension allowlist. Its seven requests resolve through TokenVisor's pure graph resolver to seven nodes and eight edges from every seed, with no diagnostics. This does not qualify hosted inference, database persistence, arbitrary workflows, nested replay, or AIPerf export for this new case. OMP `18.12.6` has native `task`/`tasks[]` batching rather than this Pi scripted-workflow API; batch identity has a reader regression, while native batch and seance capture remain separate qualification cases.

Pi `1.4.0` / pi-subagents `0.84.0` has native serving/audit/graph qualification for two serial foreground workflows (`async: false`) awaiting background-runner children in one continued session. Both first child requests carry distinct execution run IDs and the exact initiating parent calls before the parent receives either result. Identical child outputs still resolve through separate run completion records. The five successful requests rebuild to five nodes and seven edges without diagnostics. Foreground JSON delivery accepts the observed child-keyed array and `{ok, children}` return shapes, verifies the complete serialized `Return` section and actual parent input, and compares full output against the declared completion. Missing, duplicate or conflicting completion records, duplicate response identities, truncated returns and unsupported transformations remain unknown. This does not qualify every workflow shape or nested execution. The current WEKA exporter rejects multiple runs in one child session; this is capture/graph qualification, not export qualification. An empty graph diagnostic list cannot detect child requests that never acquired correlating metadata. Native assistant messages can report `api: "openai-completions"` even when the capture wrapper ran; that label alone is not a capture check.

The wire value is one ASCII JSON object, at most 4096 bytes:

```json
{
  "version": 1,
  "source": {
    "harness": "pi",
    "harness_version": "1.4.0",
    "extension_version": "1.1.0-dev",
    "adapter_version": 1
  },
  "session": {
    "id": "pi:session:<native-session-id>",
    "root_id": "pi:session:<native-root-id>",
    "parent_id": null
  },
  "stream_id": "<provider-stream-uuid>",
  "sequence": { "status": "start", "previous_response_id": null },
  "delegation": null,
  "depends_on": [],
  "child_launches": []
}
```

A session is one agent's native conversation. Parent P has its own `session.id`; a fresh child C has a different ID, while both share P's `session.root_id`. C's `session.parent_id` points to P. Ordinary resume, compaction and model changes preserve that conversation identity. Continuing an existing child can preserve its session while a newly established execution uses a different run ID. Profile names such as `worker` are not session IDs.

| Identity | Meaning |
| --- | --- |
| `session.id` | Issuing native conversation, normalized to a harness namespace. |
| `session.root_id` | Root conversation shared by the agent tree; not an independently assigned trace ID. |
| `session.parent_id` | Immediate parent conversation; a hint alone does not prove a launch. |
| `delegation.run_id` / `tool_call_id` | Child execution and the exact parent tool call initiating it. |
| `stream_id` | One logical provider invocation, reused across its transport retries. |
| `depends_on` | Earlier inference outputs delivered through supported capture paths. |
| `child_launches` | Late launch evidence, separate from output consumption. |

The server assigns audit IDs after receiving requests and retains completion IDs from responses. Those IDs, caller `X-Request-ID`, and native session/run IDs are separate domains. The outgoing header cannot name its own completion before generation. The backend exporter translates the graph to WEKA's trace `id` and child `agent_id`; it does not rename the native identities in the audit.

The versioned producer contract is published at [`conformance/agent-metadata-v1.schema.json`](./conformance/agent-metadata-v1.schema.json); canonical normalized examples for Pi roots, detached children, OMP task results, unknown dependencies, and a future harness are in [`conformance/agent-metadata-v1.json`](./conformance/agent-metadata-v1.json). The optional `child_launches` array carries exact bounded child launch receipts, with at most eight entries. The schema leaves `source.harness` open to bounded printable names and treats session identifiers as opaque bounded IDs, so future producers are not constrained to Pi or OMP. A dependency may carry a response ID, a stream ID, or both when both identify the same inference; an optional run ID qualifies either form. The schema caps dependency arrays at eight references and describes `depends_on: null` as unknown extraction. Producers must also keep the serialized ASCII header within 4096 bytes. If valid dependencies make the header exceed that budget, the producer preserves the same identity and stream ID, sets `depends_on` to `null`, and retries serialization; it never truncates individual references. The schema `$id` identifies the schema; it is not a deployed TokenVisor API endpoint.

`depends_on: []` means no new output references were emitted through the supported capture paths; it does not prove that every native input or dependency was observed. `depends_on: null` means the adapter could not establish a complete relationship set. The sequence status is `start`, `linked`, or `unavailable`; `previous_response_id` is populated only when the host provides the exact preceding response ID. Failed/ambiguous ancestry, delegation, or compaction provenance is left unknown rather than inferred from timestamps, output text, or provider cache-affinity fields. Provider transport retries reuse one stream ID and identical header bytes.

OMP 18.12.6 auto-thinking judge calls are sequenced when the live native branch supplies a unique `model_usage` entry with `purpose: "auto-thinking"` and `role: "judge"`. The judge request uses the active native assistant predecessor; the following ordinary request links to the exact response ID observed for that judge stream. A uniquely bound observed aborted or failed ID may be used when native usage and provider outcomes agree, but failed output is never counted as consumed. Missing, ambiguous, mismatched, or resumed identities without a local mapping remain `unavailable`. This uses the existing v1 sequence fields and does not classify other auxiliary calls.

Pi background-runner attribution validates the lifecycle artifact structure (`lifecycleArtifactVersion: 3`), the `PI_SUBAGENT_CHILD` / `PI_SUBAGENT_RUNNER_PID` runtime markers, and the `PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT` package-root binding. The adapter verifies the runner PID and correlates the artifact's `sessionId` (which may be a native UUID or parent-session file path) against native parent/session headers and the exact native child session file. Neither representation is accepted alone as proof of identity. OMP task attribution uses its native per-session agent/job ID, parent task tool call, and child session file. A terminal child result is persisted as a small custom entry in the native child session; result delivery is established separately from launch identity. Fresh single foreground Pi children retain the qualified post-return late-receipt fallback; it is not extended to continued sessions. Host support outside the validated native data contracts is reported as unavailable; do not interpret a missing edge as a known-empty dependency set.

Pi `subagent` status and `debug.run` results can name an existing run while carrying the inspection call's own tool-call ID. The launch lookup identifies these observations through exactly one matching native tool call, so checking a running child does not change its launch binding or prevent its completion marker. Disk and live-branch lookup use the same rules. Resume and unfamiliar actions remain subject to launch-conflict checks; duplicate launch calls/results, contradictory launch identities, and a mismatched preferred source remain unavailable. These rules are guarded through registered provider hooks from status inspection to child completion and parent consumption, without a live inference requirement.

Pi's direct timeout-only failure notice contributes no inference-output dependency when its complete native text agrees with the recorded async launch, run, agent, directory and timeout. This proof uses parent history and survives expiration of child status/session files. It preserves other delivered dependencies and retained consumption proof across compaction. Timeout notices carrying partial child output, unfamiliar failure formats and conflicting launch identities remain subject to output attribution or unknown linkage. The notice stays in the inference input; child inference requests and launch relationships remain part of the audit trace.

OMP agent profile names are arbitrary; native job type identifies a task. The stable child identity is separate from an async job ID, which can change or be reused on a peer wake. The parent records response- and delivery-specific consumption in a bounded custom native session entry after its consumer commits, so exit/resume preserves the first result without suppressing a later result from the same child. The original delegated run and initiating tool call remain unchanged across the tested peer wake. These local consumption fields do not add fields to the request header.

Ambiguous completion evidence and incomplete or malformed child files produce unknown dependencies. Pi's bounded native completion reader rejects files above 8 MiB; OMP's child completion reader retains its 4 MiB limit.

Only requests routed through this TokenVisor provider can carry its metadata. In OMP, a specialist's model role can override the parent's `--model`; bundled `scout` uses `@smol`. Configure the relevant roles, such as `--smol`, and fallback policy explicitly when collecting a workload entirely through TokenVisor. Verify the child's actual provider/model and retained audits; the requested profile, parent model, or displayed API label alone does not prove collection.

Native compaction provenance is stored as a bounded custom session entry so continuation after exit/resume can point to the contributing provider streams. Each compaction records the current causal frontier, not an ever-growing union of all historical calls. A later summary request links to the prior frontier and to earlier successful summary calls from the same operation; its completed outputs replace the covered frontier. This keeps `depends_on` within the wire budget while preserving historical reachability through the linked inference nodes. Summary contributor extraction is considered available only when the host lifecycle and successful response observations establish the contributing streams; otherwise subsequent `depends_on` is `null`. The pending summary references apply through the first assistant response after that compaction; later turns do not repeat the same summary edge. Under the declared native provider capture path, a source-marked extension compaction with no observed provider calls and no failed capture carries forward the previous frontier (or records a proven empty set when none exists); an unexpected zero-call native compaction stays unavailable. This coverage does not claim to observe arbitrary extension networking outside the TokenVisor provider. Fork sessions persist a custom marker containing their native session ID and copied-branch boundary, then derive sequence and dependencies only from entries after that boundary, including after resume. Startup `--fork` is recognized only when the pinned host's native session header also confirms a fork parent.

Qualification cohorts include Pi `1.4.0` with pi-subagents `0.83.0` and `0.84.0`, and compiled OMP `18.12.6`; these are evidence pins, not runtime allowlists. Actual-host fixtures cover the named Pi foreground and detached paths, same-child continuation, compaction contributors and continuation, plus OMP task/wait consumption, compaction contributors, and pending-summary carry through a mechanical compaction. Producer fixtures establish only the emitted header; the separately qualified native serving tests also check server retention and graph reconstruction. Older backends may ignore or reject the metadata while inference continues. When accepted metadata has `depends_on: null`, trace extraction can surface the `dependencies_unavailable` diagnostic. Historical requests missing metadata are not retroactively repaired. This README makes no claim that a particular hosted deployment has the required backend support. Nested/cross-root resume and arbitrary direct HTTP remain unqualified.

### Known OMP provenance gaps

After a process restart, a successful blocking task result can remain in the effective request input before its consumer response has committed, while the process-local launch binding is gone. Such an unbound, unconsumed result produces `depends_on: null`, rather than a false empty dependency list. Filtered or replaced output, failed/aborted/empty results and already-consumed deliveries do not create new dependencies. This correction does not reconstruct lost bindings from durable evidence and requires no OMP core or TokenVisor schema change.

Two relationships remain unqualified in the tested compiled OMP `18.12.6` runtime. This version identifies the observed build; it is not a capture allowlist.

- **Peer-wake origin:** a child can continue after a peer message, but the available native evidence does not always identify the exact caller request that caused that continuation. The original delegation and the child's own request sequence can remain known while this additional causal edge is unknown.
- **Automatic result delivery:** OMP can insert a background child's result into the parent's input after completion. Provider-message conversion can discard the structured child/job/response identity needed to attribute that delivery. A known launch or child completion alone does not prove which parent request consumed that result. Qualified foreground task results and explicit waits have separate native evidence and remain supported.

For affected requests, inference continues and capture preserves the identities and relationships it can verify. Unavailable dependency extraction is emitted as `depends_on: null`, with a warning once per session, process and distinct issue. TokenVisor can retain the audits and reconstruct the evidenced graph while reporting the missing relationship, such as `dependencies_unavailable`. A model-free compaction does not repair missing provenance. The extension does not guess the missing edge from timestamps or output-text similarity.

The current TokenVisor WEKA/AIPerf exporter reports unavailable consumption as a warning when actor order and launch topology are otherwise established. Concrete missing dependency sources, identity conflicts and unsupported workloads still block export. This benchmark policy does not repair the canonical graph or make every retained trace exportable. Closing native provenance gaps requires authoritative wake/result identity that survives into the actual provider input. These limitations are separate from failed auto-thinking judge sequence recovery and TokenVisor's stable public identity across internal provider retries.

### Coordinating the backend contract

Pi and OMP have separate native adapters in `src/capture/pi.ts` and `src/capture/omp.ts`; `src/capture/shared.ts` emits their common envelope. TokenVisor's `AgentMetadataV1` validates that envelope and its audit resolver checks cross-request consistency. The backend does not read native session files, and the producer does not decide WEKA export eligibility.

Field or meaning changes must update the producer, backend model and both copies of the schema/examples together. The backend uses strict field validation: emitting a new field alone can lose metadata capture without stopping inference. Keep unknown evidence explicit, preserve actual request input, and test extraction, ingestion and graph relationships separately. A normal capture bug fix can improve evidence without changing the wire version or adding fields. Supporting version strings describe provenance, not a harness-version allowlist.

From this checkout, compare the contract files with a backend clone:

```sh
bun run check:backend-conformance -- /path/to/TokenVisor/services/api/tests/utils/audit/fixtures
```

Producer CI exercises the encoder examples, Bun regressions and source typecheck. Backend tests validate its shared examples and graph reconstruction. The parity command additionally checks that the two repositories' JSON schema and examples match; each repository's own tests do not automatically compare another checkout. Parity does not establish complete native capture. Use registered-hook regressions for affected delivery paths and scoped native qualification when harness behavior changes.

## Setup

### Under pi

1. Start `pi`
2. Run `/login` → "Use an API key" → **TokenVisor** → paste your API key (saved to `~/.pi/agent/auth.json`)
   - or run `/tokenvisor-login` (same result)
3. Run `/reload` to load models
4. Run `/model` to select a TokenVisor model

### Under omp

omp's `/login` is **OAuth-only** — it has no "enter an API key" UI, so TokenVisor will never appear there. Use the extension's own interactive command instead:

```
/tokenvisor-login
```

It prompts for your API key, saves it to `~/.pi/agent/auth.json` (shared with pi's `/login`), and reloads. Then run `/model` to select a TokenVisor model.

Alternatively, provide the key via the environment:

```bash
export TOKENVISOR_API_KEY=tv-pat-...
```

If you also have pi installed and already ran `pi /login` (or `/tokenvisor-login` under either host), you don't need to do anything else — the extension reads the key from `~/.pi/agent/auth.json` and mirrors it into the environment for omp automatically.

## Configuration

### API Key (resolution order)

1. `TOKENVISOR_API_KEY` environment variable (always honored; the omp-idiomatic source)
2. `~/.pi/agent/auth.json` (set via pi `/login` — lets omp reuse a pi-stored key on a shared machine)

The resolved key is mirrored into `TOKENVISOR_API_KEY` at load time and registered directly as the provider API key. This avoids differences in how Pi and omp resolve environment-variable references.

### Base URL

Defaults to `https://devstudio.tokenvisor.ai/api/v1`. Override with:

```bash
export TOKENVISOR_BASE_URL=https://your-custom-endpoint/api/v1
```

## How it works

Pi 1.4 resolves `$TOKENVISOR_API_KEY` as an environment-variable reference and treats a bare `TOKENVISOR_API_KEY` as a literal. omp 18.12.6 checks whether the whole configured string names an environment variable, then falls back to that string literally. Since those forms are not compatible, the extension resolves the configured credential from the environment or pi's `auth.json` and registers the resolved value directly. An explicit user-provided environment variable always takes precedence. When no key is available, the provider remains registered with a placeholder so pi can still offer it in `/login`; after `/tokenvisor-login`, the extension reloads with the entered key.

## License

MIT
