# Flow — Features

Flow provides these features on top of basic LLM routing. Routing, observability and policy controls are provider-independent; sampling, thinking, tools and structured output remain provider/model capabilities.

## Routing & Failover

- **Per-request fallback models** — applications specify a `models` array of ordered fallbacks
- **Multi-provider routing** — same model served by different providers (e.g., Claude via Anthropic or Bedrock)
- **Provider preferences** — control provider selection with `order`, `only`, `ignore`, and `sort` fields
- **Enhanced auto-routing** — `model: "auto"` combined with latency-based sorting

Response headers indicate when failover occurs: `x-reiver-fallback-used`, `x-reiver-original-model`, `x-reiver-model-used`, `x-reiver-retry-count`.

## Semantic Caching

Two-layer cache (in-process LRU + distributed semantic cache). Eligible when `stream` is false, `temperature` is 0, no tools specified, and `n` is 1. Models that reject temperature controls, including Claude Sonnet 5, Opus 5, Fable 5 and recent Opus 4.7/4.8 variants, are excluded because the requested zero cannot make them deterministic. The `x-reiver-cache` response header reports `"hit"`, `"miss"`, or `"skip"`.

## Guardrails

Guardrails run before and after the LLM call to enforce content policies. Configured per-project.

### Trust modes

Trust modes control which message roles are treated as untrusted (required for injection detection and spotlighting):

- **Agent** — `tool` messages are untrusted. For apps where tool results carry external data.
- **Chatbot** — `user` and `tool` messages are untrusted. For apps where end users interact directly.

### Input guardrails

- Topic blocklist — rejects requests matching blocked topics
- Token cap — rejects requests exceeding a token limit
- PII block-on-detect — blocks requests if PII is detected
- Prompt injection detection — scans untrusted-role messages for injection patterns (requires trust mode)
- Input spotlighting — wraps untrusted messages in structural delimiters (requires trust mode)

### Output guardrails

- PII masking — redacts PII from responses
- Topic blocklist — blocks responses matching forbidden topics
- LLM-as-judge — uses a secondary LLM to evaluate responses
- Tool call validation — blocks unauthorized tool calls via `allowed_tools` and `blocked_tools`
- Exfiltration scanning — blocks responses with external image URLs that could leak data

## Session Budgets

Per-session cost limits. Applications set `x-reiver-session-id` and the project configures `gateway_session_budget_usd`. Requests exceeding the budget return 429.

## Output Contracts

JSON schema validation on LLM responses. Configured per prompt version via `response_format`. Failure actions: `error`, `retry`, `retry_then_passthrough`, `log_only`.

## Thinking

- Claude 5 — adaptive thinking; do not add legacy manual budget fields to Sonnet 5 or Fable 5
- Recent Claude Opus — Reiver translates the compatibility toggle to adaptive thinking where supported
- Earlier Claude models — manual extended-thinking budgets where supported
- OpenAI o-series — reasoning effort levels (low/medium/high)
- Google Gemini — thinking mode

## Multimodal Support

Applications send images and documents alongside text via `image_url` and `document_url` content parts.

## Cost Tracking

Every request is logged with token usage and cost, broken down by provider.

## Platform Management (MCP)

Gateway settings (caching, guardrails, budgets, rate limits, fallback config) are configurable via:

`execute` with `resource: 'gateway', action: 'update_settings'` — this action changes settings that affect all traffic through the LLM gateway and should be explicitly requested by the user.

To view current settings: `get` with `resource: 'gateway_settings'`.
