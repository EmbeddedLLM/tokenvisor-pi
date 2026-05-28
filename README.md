# tokenvisor-pi

[TokenVisor](https://devstudio.tokenvisor.ai) custom provider for [pi](https://github.com/earendil-works/pi-coding-agent).

Dynamically discovers models from your TokenVisor account and registers them as a provider.

## Install

```bash
pi install git:github.com/EmbeddedLLM/tokenvisor-pi
```

Or add to `.pi/settings.json`:

```json
{
  "packages": ["git:github.com/EmbeddedLLM/tokenvisor-pi"]
}
```

## Setup

1. Start pi
2. Run `/login` → "Use an API key" → **TokenVisor** → paste your API key
3. Run `/reload` to load models
4. Run `/model` to select a TokenVisor model

## Configuration

### API Key (in order of priority)

1. Stored via `/login` (saved to `~/.pi/agent/auth.json`)
2. `TOKENVISOR_API_KEY` environment variable

### Base URL

Defaults to `https://devstudio.tokenvisor.ai/api/v1`. Override with:

```bash
export TOKENVISOR_BASE_URL=https://your-custom-endpoint/api/v1
```

## License

MIT
