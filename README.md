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

Run `npm run typecheck`, `bun test tests/capture-metadata.test.ts`, and `bun test tests/runtime-host.test.ts` from this package checkout to validate the producer.

### Runtime qualification

The extension verifies the running Pi manifest is `1.4.0` or probes the OMP executable's `--version` output for `18.12.6` before attaching metadata. Unsupported or unknown host versions leave the ordinary provider path active and are reported in local diagnostics or a startup warning. Pi subagent metadata also verifies the `pi-subagents@0.83.0` runner before emitting its runner version. Install the complete package checkout through the host's extension/package manager; metadata modules are part of that checkout, so loading a loose copy of `extensions/tokenvisor-provider.ts` is not supported. For Pi subagents, the child must load this extension explicitly through the agent's `extensions` frontmatter allowlist (`subagentOnlyExtensions` is the tested configuration). Loading it in the parent does not enable it in child runtimes. Do not launch the parent with `--no-extensions` when async children need to load this extension; the child loader must be able to resolve the configured provider extension path.

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
  "depends_on": []
}
```

The versioned producer contract is published at [`conformance/agent-metadata-v1.schema.json`](./conformance/agent-metadata-v1.schema.json); canonical normalized examples for Pi roots, detached children, OMP task results, unknown dependencies, and a future harness are in [`conformance/agent-metadata-v1.json`](./conformance/agent-metadata-v1.json). The schema leaves `source.harness` open to bounded printable names and treats session identifiers as opaque bounded IDs, so future producers are not constrained to Pi or OMP. A dependency may carry a response ID, a stream ID, or both when both identify the same inference; an optional run ID qualifies either form. The schema caps dependency arrays at eight references and describes `depends_on: null` as unknown extraction. Producers must also keep the serialized ASCII header within 4096 bytes. The schema `$id` identifies the schema; it is not a deployed TokenVisor API endpoint. Backend validation and persistence are not implemented.

`depends_on: []` means extraction proved there were no current dependencies; `depends_on: null` means the adapter could not establish a complete relationship set. The sequence status is `start`, `linked`, or `unavailable`; `previous_response_id` is populated only when the host provides the exact preceding response ID. Failed/ambiguous ancestry, delegation, or compaction provenance is left unknown rather than inferred from timestamps, output text, or provider cache-affinity fields. Provider transport retries reuse one stream ID and identical header bytes.

OMP 18.12.6 auto-thinking judge calls are also sequenced when the live native branch supplies a unique `model_usage` entry with `purpose: "auto-thinking"` and `role: "judge"`. The judge request uses the active native assistant predecessor; the following ordinary request links to the exact response ID observed for that judge stream. Failed, ID-less, unmatched, concurrent-ambiguous, or resumed-without-local-mapping cases remain `unavailable`. This uses the existing v1 sequence fields and does not classify other auxiliary calls.

Pi detached child attribution depends on the pinned `pi-subagents@0.83.0` lifecycle artifact format (`lifecycleArtifactVersion: 3`), the `PI_SUBAGENT_CHILD` / `PI_SUBAGENT_RUNNER_PID` runtime markers, and the `PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT` package-root binding. The adapter verifies the runner PID and correlates the artifact's `sessionId` (which may be a native UUID or parent-session file path) against native parent/session headers and the exact native child session file. Neither representation is accepted alone as proof of identity. OMP task attribution uses its native per-session agent/job ID, parent task tool call, and child session file. A terminal child result is persisted as a small custom entry in the native child session and linked only when the parent records a completed task result. Host support outside those source-qualified paths is reported as unavailable. Pi foreground single-run delegation is not qualified for metadata delivery; do not interpret a missing edge as a known-empty dependency set.

Native compaction provenance is stored as a bounded custom session entry so continuation after exit/resume can point to the contributing provider streams. Each compaction records the current causal frontier, not an ever-growing union of all historical calls. A later summary request links to the prior frontier and to earlier successful summary calls from the same operation; its completed outputs replace the covered frontier. This keeps `depends_on` within the wire budget while preserving historical reachability through the linked inference nodes. Summary contributor extraction is considered available only when the host lifecycle and successful response observations establish the contributing streams; otherwise subsequent `depends_on` is `null`. The pending summary references apply through the first assistant response after that compaction; later turns do not repeat the same summary edge. Under the declared native provider capture path, a source-marked extension compaction with no observed provider calls and no failed capture carries forward the previous frontier (or records a proven empty set when none exists); an unexpected zero-call native compaction stays unavailable. This coverage does not claim to observe arbitrary extension networking outside the TokenVisor provider. Fork sessions persist a custom marker containing their native session ID and copied-branch boundary, then derive sequence and dependencies only from entries after that boundary, including after resume. Startup `--fork` is recognized only when the pinned host's native session header also confirms a fork parent.

Qualified producer paths are Pi `1.4.0` with `pi-subagents@0.83.0` and compiled OMP `18.12.6`. Actual-host fixtures cover Pi detached children, same-child resume under a new run/tool call, compaction contributors and continuation, plus OMP task/wait consumption, compaction contributors, and pending-summary carry through a mechanical compaction. The final Pi resume graph matches the independently derived native graph; the OMP mechanical compaction makes zero provider calls and its three-reference frontier is consumed once by the next request. The TokenVisor workspace document `design_docs/agent_session_replay.md` records the evidence cohorts, source manifest, and remaining scope limits. Local unit fixtures and wire graphs do not establish server-side audit retention: TokenVisor backend ingestion, persistence, and trace retrieval are not implemented. Pi foreground delegation, nested/cross-root resume, and arbitrary direct HTTP remain unqualified.

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
