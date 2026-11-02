# tokenvisor-pi

[TokenVisor](https://devstudio.tokenvisor.ai) custom provider for [pi](https://github.com/earendil-works/pi-coding-agent) and [oh-my-pi (omp)](https://github.com/can1357/oh-my-pi).

Dynamically discovers models from your TokenVisor account and registers them as a provider. Both hosts use the provider registration API; credential storage differs, which this extension bridges automatically (see [How it works](#how-it-works)). Capture hooks and runtime adapters are host-specific.

## Install

Under pi:

```bash
pi install git:github.com/EmbeddedLLM/tokenvisor-pi
```

Or add to `.pi/settings.json`:

```json
{ "packages": ["git:github.com/EmbeddedLLM/tokenvisor-pi"] }
```

Under omp:

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

## Agent metadata

On supported runtimes, the extension automatically attaches a bounded `X-TokenVisor-Agent` envelope to TokenVisor provider calls. The envelope contains session, stream, sequence, delegation, and dependency identifiers; it never contains prompts or tool output. Transport retries reuse the same envelope. There is no capture enable/disable setting.

For local metadata diagnostics, optionally set `TOKENVISOR_AGENT_METADATA_DIAGNOSTICS=/path/to/private.jsonl`. This writes identifiers and extraction statuses with file mode `0600`; it does not control metadata transmission.

When capture detects a specific limitation, it warns once per session and distinct issue in the process that request linkage may be incomplete. The provider continues inference and keeps any valid metadata it can establish. Unknown package versions alone do not trigger a warning. OMP automatic result delivery can lose native identity during message conversion; in that case linkage remains unknown, without matching result text to guess an identity.

Run `bun test` and `bun run typecheck` from this package checkout to validate the producer. Producer CI runs these checks after `npm ci` with Node `22.19.0` and Bun `1.3.14`; live Pi/OMP qualification remains a separate opt-in procedure and is not run by this CI job.

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

The versioned producer contract is published at [`conformance/agent-metadata-v1.schema.json`](./conformance/agent-metadata-v1.schema.json); canonical normalized examples for Pi roots, detached children, OMP task results, unknown dependencies, and a future harness are in [`conformance/agent-metadata-v1.json`](./conformance/agent-metadata-v1.json). The optional `child_launches` array carries exact bounded child launch receipts, with at most eight entries. The schema leaves `source.harness` open to bounded printable names and treats session identifiers as opaque bounded IDs, so future producers are not constrained to Pi or OMP. A dependency may carry a response ID, a stream ID, or both when both identify the same inference; an optional run ID qualifies either form. The schema caps dependency arrays at eight references and describes `depends_on: null` as unknown extraction. Producers must also keep the serialized ASCII header within 4096 bytes. If valid dependencies make the header exceed that budget, the producer preserves the same identity and stream ID, sets `depends_on` to `null`, and retries serialization; it never truncates individual references. The schema `$id` identifies the schema; it is not a deployed TokenVisor API endpoint.

`depends_on: []` means extraction proved there were no current dependencies; `depends_on: null` means the adapter could not establish a complete relationship set. The sequence status is `start`, `linked`, or `unavailable`; `previous_response_id` is populated only when the host provides the exact preceding response ID. Failed/ambiguous ancestry, delegation, or compaction provenance is left unknown rather than inferred from timestamps, output text, or provider cache-affinity fields. Provider transport retries reuse one stream ID and identical header bytes.

OMP 18.12.6 auto-thinking judge calls are sequenced when the live native branch supplies a unique `model_usage` entry with `purpose: "auto-thinking"` and `role: "judge"`. The judge request uses the active native assistant predecessor; the following ordinary request links to the exact response ID observed for that judge stream. A uniquely bound observed aborted or failed ID may be used when native usage and provider outcomes agree, but failed output is never counted as consumed. Missing, ambiguous, mismatched, or resumed identities without a local mapping remain `unavailable`. This uses the existing v1 sequence fields and does not classify other auxiliary calls.

Pi background-runner attribution validates the lifecycle artifact structure (`lifecycleArtifactVersion: 3`), the `PI_SUBAGENT_CHILD` / `PI_SUBAGENT_RUNNER_PID` runtime markers, and the `PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT` package-root binding. The adapter verifies the runner PID and correlates the artifact's `sessionId` (which may be a native UUID or parent-session file path) against native parent/session headers and the exact native child session file. Neither representation is accepted alone as proof of identity. OMP task attribution uses its native per-session agent/job ID, parent task tool call, and child session file. A terminal child result is persisted as a small custom entry in the native child session; result delivery is established separately from launch identity. Fresh single foreground Pi children retain the qualified post-return late-receipt fallback; it is not extended to continued sessions. Host support outside the validated native data contracts is reported as unavailable; do not interpret a missing edge as a known-empty dependency set.

OMP agent profile names are arbitrary; native job type identifies a task. The stable child identity is separate from an async job ID, which can change or be reused on a peer wake. The parent records response- and delivery-specific consumption in a bounded custom native session entry after its consumer commits, so exit/resume preserves the first result without suppressing a later result from the same child. The original delegated run and initiating tool call remain unchanged across the tested peer wake. These local consumption fields do not add fields to the request header.

Exact peer-wake ancestry and automatic result deliveries whose identity is lost during provider-message conversion remain unknown. Ambiguous completion evidence and incomplete or malformed child files also produce unknown dependencies. Pi's bounded native completion reader rejects files above 8 MiB; OMP's child completion reader retains its 4 MiB limit. A model-free compaction does not clear unknown peer ancestry. These are capture limits, not evidence that a child ran independently.

Only requests routed through this TokenVisor provider can carry its metadata. In OMP, a specialist's model role can override the parent's `--model`; bundled `scout` uses `@smol`. Configure the relevant roles, such as `--smol`, and fallback policy explicitly when collecting a workload entirely through TokenVisor. Verify the child's actual provider/model and retained audits; the requested profile, parent model, or displayed API label alone does not prove collection.

Native compaction provenance is stored as a bounded custom session entry so continuation after exit/resume can point to the contributing provider streams. Each compaction records the current causal frontier, not an ever-growing union of all historical calls. A later summary request links to the prior frontier and to earlier successful summary calls from the same operation; its completed outputs replace the covered frontier. This keeps `depends_on` within the wire budget while preserving historical reachability through the linked inference nodes. Summary contributor extraction is considered available only when the host lifecycle and successful response observations establish the contributing streams; otherwise subsequent `depends_on` is `null`. The pending summary references apply through the first assistant response after that compaction; later turns do not repeat the same summary edge. Under the declared native provider capture path, a source-marked extension compaction with no observed provider calls and no failed capture carries forward the previous frontier (or records a proven empty set when none exists); an unexpected zero-call native compaction stays unavailable. This coverage does not claim to observe arbitrary extension networking outside the TokenVisor provider. Fork sessions persist a custom marker containing their native session ID and copied-branch boundary, then derive sequence and dependencies only from entries after that boundary, including after resume. Startup `--fork` is recognized only when the pinned host's native session header also confirms a fork parent.

Qualification cohorts include Pi `1.4.0` with pi-subagents `0.83.0` and `0.84.0`, and compiled OMP `18.12.6`; these are evidence pins, not runtime allowlists. Actual-host fixtures cover the named Pi foreground and detached paths, same-child continuation, compaction contributors and continuation, plus OMP task/wait consumption, compaction contributors, and pending-summary carry through a mechanical compaction. The TokenVisor workspace document `design_docs/agent_session_replay.md` records each cohort's exact source manifest and remaining scope limits. Producer fixtures establish only the emitted header; the separately qualified native serving tests also check server retention and graph reconstruction. Older backends may ignore the header. When accepted metadata has `depends_on: null`, trace extraction can surface the `dependencies_unavailable` diagnostic. Historical requests missing metadata are not retroactively repaired. This README makes no claim that a particular hosted deployment has the required backend support. Nested/cross-root resume and arbitrary direct HTTP remain unqualified.

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
