# Reiver AI Gateway

Unified OpenAI-compatible API gateway that routes requests to multiple LLM providers (OpenAI, Anthropic, Google Gemini, AWS Bedrock).

## Overview

The gateway accepts requests in OpenAI's chat completion format and routes them to the appropriate provider based on the model name:

- `gpt-*`, `o1-*`, `o3-*`, `o4-*` → OpenAI
- `claude-*` → Anthropic
- `gemini-*` → Google
- `bedrock/*`, `anthropic.*`, `amazon.*`, `meta.*`, `ai21.*`, `mistral.*`, `cohere.*` → AWS Bedrock
- `theta/` → Theta EdgeCloud (vLLM)

Users interact with the gateway using the standard OpenAI SDK pointed at a custom base URL:

```python
from openai import OpenAI

client = OpenAI(
    api_key="fx_project_key",
    base_url="https://reiver.ai/api/gateway/v1"
)

response = client.chat.completions.create(
    model="claude-3-opus",  # or gpt-4o, gemini-pro, etc.
    messages=[{"role": "user", "content": "Hello"}]
)
```

## Architecture

```
POST /v1/chat/completions (OpenAI-compatible)

┌───────────────────────────────────────────────┐
│              GatewayRouter                    │
│                                               │
│  model="gpt-4o"        → OpenAiProvider       │
│  model="claude-3-opus" → AnthropicProvider    │
│  model="gemini-pro"    → GoogleProvider       │
│  model="bedrock/..."   → BedrockProvider      │
│  model="theta/..."     → ThetaProvider        │
└───────────────────────────────────────────────┘

Each provider translates OpenAI format ↔ native provider format.
Observability spans are captured automatically.
```

### Key Modules

| Module             | Purpose                                                  |
|--------------------|----------------------------------------------------------|
| `router.rs`        | Model-to-provider routing via prefix lookup table        |
| `fallback.rs`      | Retry logic with exponential backoff and provider failover |
| `types.rs`         | OpenAI-compatible request/response types                 |
| `providers/`       | Per-provider translation (OpenAI, Anthropic, Google, Bedrock, Theta) |
| `cache.rs`         | Semantic response caching                                |
| `observability.rs` | Span and metrics instrumentation                         |
| `prompt_resolver.rs` | Prompt template resolution                             |
| `error.rs`         | Gateway error types                                      |

## Fallback and Retry

The gateway supports automatic failover when a provider fails. Retryable errors (rate limits, 5xx responses, timeouts, network errors) trigger retries with exponential backoff. If retries are exhausted, the gateway falls back to alternate providers from the user-configured fallback list.

Fallback models are configured per-project (`default_fallback_models` in gateway settings) or per-request (the `models` array). Per-request models take precedence over project defaults.
