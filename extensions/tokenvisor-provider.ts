/**
 * TokenVisor Provider Extension
 *
 * Dynamic model discovery from https://devstudio.tokenvisor.ai
 *
 * Works under both pi (earendil-works) and oh-my-pi (omp). The extension API
 * (`ExtensionAPI`, `registerProvider`, `registerCommand`) is identical across
 * both hosts; only credential storage differs:
 *
 *   - pi:  /login → "Use an API key" stores the key in ~/.pi/agent/auth.json
 *   - omp: /login is OAuth-only and has NO API-key entry UI, so custom
 *          API-key providers are expected to source their key from an env var.
 *
 * To bridge the two, this extension:
 *   - Resolves the key from the env var or ~/.pi/agent/auth.json, then mirrors
 *     it into process.env.TOKENVISOR_API_KEY before registering the provider so
 *     both hosts' env-var-name apiKey resolution returns the real key.
 *   - Registers a /tokenvisor-login command that interactively prompts for the
 *     key (since omp's /login won't list TokenVisor), persists it to
 *     ~/.pi/agent/auth.json (shared with pi's /login), and reloads.
 *
 * Usage:
 *   pi    # /login → "Use an API key" → TokenVisor → enter your API key → /reload
 *         #   — or — /tokenvisor-login
 *   omp   # /tokenvisor-login  (omp /login is OAuth-only and won't list TokenVisor)
 *         #   — or — export TOKENVISOR_API_KEY=...
 *   # Then /model to select a TokenVisor model
 *
 * Base URL resolution (in order):
 *   1. TOKENVISOR_BASE_URL environment variable
 *   2. Fallback: https://devstudio.tokenvisor.ai/api/v1
 *
 * API key resolution (in order):
 *   1. TOKENVISOR_API_KEY environment variable (omp-idiomatic; always honored)
 *   2. ~/.pi/agent/auth.json (set via pi /login or /tokenvisor-login)
 * The resolved key is mirrored into TOKENVISOR_API_KEY at load time so the
 * provider's env-var-name apiKey resolves correctly under both hosts.
 */

// Type-only import: erased at transpile time, so it resolves under pi
// (@earendil-works/pi-coding-agent) and is a no-op under omp
// (@oh-my-pi/pi-coding-agent) without a runtime module lookup.
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { readFileSync, writeFileSync, existsSync, mkdirSync, renameSync } from "fs";
import { join } from "path";
import { homedir } from "os";

const ENV_KEY = "TOKENVISOR_API_KEY";
const DEFAULT_BASE_URL = "https://devstudio.tokenvisor.ai/api/v1";
const AUTH_PATH = join(homedir(), ".pi", "agent", "auth.json");

interface TokenVisorModel {
  id: string;
  name: string;
  type: string;
  capabilities: string[];
  context_length: number | null;
  max_output_tokens: number | null;
}

interface TokenVisorModelList {
  object: string;
  data: TokenVisorModel[];
}

type ModelSpec = {
  id: string;
  name: string;
  reasoning: boolean;
  input: readonly ("text" | "image")[];
  contextWindow: number;
  maxTokens: number;
  cost: { input: number; output: number; cacheRead: number; cacheWrite: number };
};

function convertModel(model: TokenVisorModel): ModelSpec {
  const capabilities = model.capabilities ?? [];
  return {
    id: model.id,
    name: model.name || model.id,
    reasoning: capabilities.includes("reasoning"),
    input: capabilities.includes("image")
      ? (["text", "image"] as const)
      : (["text"] as const),
    contextWindow: model.context_length ?? 128000,
    maxTokens: model.max_output_tokens ?? 16384,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  };
}

async function discoverModels(baseUrl: string, apiKey: string): Promise<ModelSpec[]> {
  const response = await fetch(`${baseUrl}/models`, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
  });
  if (!response.ok) throw new Error(`Models endpoint returned ${response.status}`);
  const data = (await response.json()) as TokenVisorModelList;
  return (data.data ?? []).map(convertModel);
}

/**
 * Read a TokenVisor API key from ~/.pi/agent/auth.json (where pi's /login and
 * /tokenvisor-login both write it). omp has no API-key login UI, so on a machine
 * that runs both hosts this is the bridge that lets omp reuse a key entered
 * under either host. On an omp-only machine this file won't exist unless
 * /tokenvisor-login created it, and the user can set TOKENVISOR_API_KEY instead.
 */
function readPiStoredApiKey(): string | undefined {
  if (!existsSync(AUTH_PATH)) return undefined;
  try {
    const data = JSON.parse(readFileSync(AUTH_PATH, "utf-8"));
    const cred = data["TokenVisor"] ?? data["tokenvisor"];
    if (cred?.type === "api_key" && typeof cred.key === "string") return cred.key;
  } catch {
    // Ignore parse errors
  }
  return undefined;
}

/** Persist the TokenVisor API key into ~/.pi/agent/auth.json (atomic rewrite). */
function savePiStoredApiKey(key: string): void {
  const dir = join(homedir(), ".pi", "agent");
  mkdirSync(dir, { recursive: true });
  let data: Record<string, unknown> = {};
  if (existsSync(AUTH_PATH)) {
    try {
      const parsed = JSON.parse(readFileSync(AUTH_PATH, "utf-8"));
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        data = parsed as Record<string, unknown>;
      }
    } catch {
      // Corrupt or unreadable — start fresh.
    }
  }
  data["TokenVisor"] = { type: "api_key", key };
  const tmp = `${AUTH_PATH}.tmp`;
  writeFileSync(tmp, JSON.stringify(data, null, 2), { mode: 0o600 });
  renameSync(tmp, AUTH_PATH);
}

function resolveApiKey(): string | undefined {
  // 1. Explicit env var wins (omp-idiomatic source, user-controlled)
  const envKey = process.env[ENV_KEY];
  if (envKey) return envKey;
  // 2. Fall back to a pi-stored credential
  return readPiStoredApiKey();
}

export default async function (pi: ExtensionAPI) {
  const baseUrl = process.env["TOKENVISOR_BASE_URL"] ?? DEFAULT_BASE_URL;
  const apiKey = resolveApiKey();

  // Publish the resolved key into the environment before registerProvider so
  // both hosts resolve the provider's env-var-name apiKey identically.
  //
  // Why this matters: ProviderConfig.apiKey is an env-var NAME that the host
  // resolves via resolveConfigValue (env-var lookup → literal fallback) when
  // building the request Authorization header. If the env var is unset, the
  // host falls back to the literal string and sends a bogus
  // "Authorization: Bearer TOKENVISOR_API_KEY" header → 401. omp cannot store
  // the key via /login (OAuth-only), so mirroring it into process.env here is
  // what makes omp requests authenticate. Only set when unset, so a real
  // user-provided env var always takes precedence.
  if (apiKey && !process.env[ENV_KEY]) {
    process.env[ENV_KEY] = apiKey;
  }

  let models: ModelSpec[] = [];

  if (apiKey) {
    try {
      models = await discoverModels(baseUrl, apiKey);
      console.log(`[tokenvisor] Loaded ${models.length} models`);
    } catch (err) {
      console.warn(`[tokenvisor] Model discovery failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // Placeholder model so the provider appears in pi's /login API-key list and in
  // omp's /model picker before discovery has run. Replaced once a key is present
  // and /reload triggers model discovery from the API.
  if (models.length === 0) {
    models.push({
      id: "tokenvisor",
      name: "TokenVisor",
      reasoning: false,
      input: ["text"] as const,
      contextWindow: 128000,
      maxTokens: 16384,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    });
  }

  pi.registerProvider("TokenVisor", {
    name: "TokenVisor",
    baseUrl,
    apiKey: ENV_KEY,
    api: "openai-completions",
    authHeader: true,
    models,
  });

  // Interactive API-key entry. Under pi this mirrors the built-in /login; under
  // omp it is the only interactive way to set the key, since omp's /login is
  // OAuth-only and never lists custom API-key providers like TokenVisor.
  pi.registerCommand("tokenvisor-login", {
    description: "Enter or update your TokenVisor API key",
    handler: async (_args, ctx) => {
      const input = await ctx.ui.input("TokenVisor API key", "tv-pat-...");
      const key = input?.trim();
      if (!key) {
        ctx.ui.notify("TokenVisor login cancelled.", "info");
        return;
      }
      try {
        savePiStoredApiKey(key);
      } catch (err) {
        ctx.ui.notify(
          `Failed to save TokenVisor key: ${err instanceof Error ? err.message : String(err)}`,
          "error",
        );
        return;
      }
      // Make the key effective for the current process immediately; the reload
      // below re-runs this factory, which re-reads the key, re-mirrors it into
      // the env, and re-discovers models.
      process.env[ENV_KEY] = key;
      ctx.ui.notify("TokenVisor API key saved. Reloading…", "info");
      // Reload is terminal for this handler — must return immediately after.
      await ctx.reload();
      return;
    },
  });

  if (!apiKey) {
    console.log(
      `[tokenvisor] No API key found. Run /tokenvisor-login, set ${ENV_KEY} in` +
        " your environment (omp has no API-key /login), or run /login under pi," +
        " then /reload.",
    );
  }
}
