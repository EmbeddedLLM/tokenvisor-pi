# tokenvisor-pi

[TokenVisor](https://devstudio.tokenvisor.ai) custom provider for [pi](https://github.com/earendil-works/pi-coding-agent) and [oh-my-pi (omp)](https://github.com/can1357/oh-my-pi).

Dynamically discovers models from your TokenVisor account and registers them as a provider. The extension API is identical across both hosts; the only difference is how each stores credentials, which this extension bridges automatically (see [How it works](#how-it-works)).

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
{ "dependencies": { "tokenvisor-pi": "https://github.com/EmbeddedLLM/tokenvisor-pi" } }
```

The `package.json` `pi` manifest is read by both hosts (omp falls back to the `pi` field).

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

The resolved key is mirrored into `TOKENVISOR_API_KEY` at load time so the provider's env-var-name `apiKey` resolves correctly under both hosts.

### Base URL

Defaults to `https://devstudio.tokenvisor.ai/api/v1`. Override with:

```bash
export TOKENVISOR_BASE_URL=https://your-custom-endpoint/api/v1
```

## How it works

Both pi and omp accept `pi.registerProvider("TokenVisor", { apiKey: "TOKENVISOR_API_KEY", ... })`, where `apiKey` is an **env-var name** that the host resolves at request time (`resolveConfigValue`: env-var lookup → literal fallback).

The catch: if the env var is unset, the host falls back to the literal string `"TOKENVISOR_API_KEY"` and sends a bogus `Authorization: Bearer TOKENVISOR_API_KEY` header → `401 unauthorized`. omp cannot store the key via `/login` (its `/login` only lists built-in OAuth providers), so the only omp-native source is the env var.

This extension resolves the key from the env var or pi's `auth.json`, then publishes it into `process.env.TOKENVISOR_API_KEY` before registering the provider. That makes the env-var-name form resolve to the real key under both hosts, regardless of credential-store differences. An explicit user-provided env var always takes precedence.

## License

MIT
