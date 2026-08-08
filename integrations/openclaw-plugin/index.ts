/**
 * openclaw-flow — OpenClaw plugin for the Flow LLM gateway.
 *
 * Registers a "flow" provider backed by Flow's OpenAI-compatible
 * /v1/chat/completions endpoint, exposing all supported models through
 * a single gateway URL.
 */

interface FlowPluginConfig {
  gatewayUrl?: string;
  apiKey: string;
}

interface ModelEntry {
  id: string;
  name: string;
  reasoning: boolean;
  input: string[];
  cost: { input: number; output: number; cacheRead: number; cacheWrite: number };
  contextWindow: number;
  maxTokens: number;
}

const DEFAULT_GATEWAY_URL = "https://reiver.ai/api/gateway/v1";

const MODELS: ModelEntry[] = [
  {
    id: "auto",
    name: "Auto (best available)",
    reasoning: false,
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: 128000,
    maxTokens: 16384,
  },
  {
    id: "gpt-4o",
    name: "GPT-4o",
    reasoning: false,
    input: ["text", "image"],
    cost: { input: 0.0025, output: 0.01, cacheRead: 0.00125, cacheWrite: 0.0025 },
    contextWindow: 128000,
    maxTokens: 16384,
  },
  {
    id: "gpt-4o-mini",
    name: "GPT-4o Mini",
    reasoning: false,
    input: ["text", "image"],
    cost: { input: 0.00015, output: 0.0006, cacheRead: 0.000075, cacheWrite: 0.00015 },
    contextWindow: 128000,
    maxTokens: 16384,
  },
  {
    id: "o3-mini",
    name: "o3-mini",
    reasoning: true,
    input: ["text"],
    cost: { input: 0.0011, output: 0.0044, cacheRead: 0.00055, cacheWrite: 0.0011 },
    contextWindow: 200000,
    maxTokens: 100000,
  },
  {
    id: "claude-sonnet-4-5",
    name: "Claude Sonnet 4.5",
    reasoning: true,
    input: ["text", "image"],
    cost: { input: 0.003, output: 0.015, cacheRead: 0.0003, cacheWrite: 0.00375 },
    contextWindow: 200000,
    maxTokens: 8192,
  },
  {
    id: "claude-3-5-sonnet",
    name: "Claude 3.5 Sonnet",
    reasoning: false,
    input: ["text", "image"],
    cost: { input: 0.003, output: 0.015, cacheRead: 0.0003, cacheWrite: 0.00375 },
    contextWindow: 200000,
    maxTokens: 8192,
  },
  {
    id: "gemini-2.5-pro",
    name: "Gemini 2.5 Pro",
    reasoning: true,
    input: ["text", "image"],
    cost: { input: 0.00125, output: 0.01, cacheRead: 0.000315, cacheWrite: 0.00125 },
    contextWindow: 1000000,
    maxTokens: 65536,
  },
  {
    id: "gemini-2.0-flash",
    name: "Gemini 2.0 Flash",
    reasoning: false,
    input: ["text", "image"],
    cost: { input: 0.0001, output: 0.0004, cacheRead: 0.000025, cacheWrite: 0.0001 },
    contextWindow: 1000000,
    maxTokens: 8192,
  },
];

export default function activate(ctx: {
  config: FlowPluginConfig;
  registerProvider: (
    id: string,
    provider: {
      baseUrl: string;
      apiKey: string;
      api: string;
      authHeader: boolean;
      models: ModelEntry[];
    },
  ) => void;
}) {
  const { config, registerProvider } = ctx;
  const baseUrl = config.gatewayUrl ?? DEFAULT_GATEWAY_URL;

  registerProvider("flow", {
    baseUrl,
    apiKey: config.apiKey,
    api: "openai-completions",
    authHeader: true,
    models: MODELS,
  });
}
