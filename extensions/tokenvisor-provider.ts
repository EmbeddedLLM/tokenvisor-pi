/**
 * TokenVisor Provider Extension
 *
 * Dynamic model discovery from https://devstudio.tokenvisor.ai
 * Reads API key from auth.json, env var, or /login flow.
 *
 * Usage:
 *   pi
 *   # Then /login → "Use an API key" → TokenVisor → enter your API key
 *   # Then /reload to load models
 *   # Then /model to select a model
 *
 * Base URL resolution (in order):
 *   1. TOKENVISOR_BASE_URL environment variable
 *   2. Fallback: https://devstudio.tokenvisor.ai/api/v1
 *
 * API key resolution (in order):
 *   1. auth.json (set via /login API key entry)
 *   2. TOKENVISOR_API_KEY environment variable
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";

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


function readStoredApiKey(): string | undefined {
  const authPath = join(homedir(), ".pi", "agent", "auth.json");
  if (!existsSync(authPath)) return undefined;
  try {
    const data = JSON.parse(readFileSync(authPath, "utf-8"));
    // Try new provider ID first, then fall back to old
    const cred = data["TokenVisor"] ?? data["tokenvisor"];
    if (cred?.type === "api_key") return cred.key;
  } catch {
    // Ignore parse errors
  }
  return undefined;
}

export default async function (pi: ExtensionAPI) {
  const baseUrl = process.env["TOKENVISOR_BASE_URL"] ?? "https://devstudio.tokenvisor.ai/api/v1";

  // Resolve API key: stored creds > env var
  const apiKey = readStoredApiKey() ?? process.env["TOKENVISOR_API_KEY"];

  let models: ModelSpec[] = [];

  if (apiKey) {
    try {
      models = await discoverModels(baseUrl, apiKey);
      console.log(`[tokenvisor] Loaded ${models.length} models`);
    } catch (err) {
      console.warn(`[tokenvisor] Model discovery failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // Workaround: pi requires at least one model for a provider to appear in /login's
  // API key list. This placeholder is replaced once the user authenticates and /reload
  // triggers model discovery from the API.
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
    apiKey: "TOKENVISOR_API_KEY",
    api: "openai-completions",
    authHeader: true,
    models,
  });

  if (!apiKey) {
    console.log("[tokenvisor] No API key found. Use /login to set one, then /reload.");
  }
}
