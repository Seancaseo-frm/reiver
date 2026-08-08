# Routing

Flow supports OpenRouter-style routing with per-request fallback models, multi-provider model routing, and granular provider preferences.

## Per-Request Fallback Models (`models`)

Specify an ordered list of fallback models that override the project's default fallback chain for a single request.

```python
from openai import OpenAI

client = OpenAI(base_url="https://reiver.ai/api/gateway/v1", api_key="dh_...")

response = client.chat.completions.create(
    model="claude-sonnet-4-6",
    messages=[{"role": "user", "content": "Hello"}],
    extra_body={
        "models": ["gpt-4o", "gemini-2.5-flash"]
    }
)
```

**Behavior:**

- `model` is tried first (primary).
- If it fails with 5xx, 429, or timeout, models in `models` are tried in order.
- Maximum 5 fallback models per request.
- If `model` is empty and `models` is provided, `models[0]` becomes primary.
- The `models` array is stripped before forwarding to the upstream provider.
- Response `model` field reflects which model actually served the request.

## Auto-Routing (`model: "auto"`)

When `model` is set to `"auto"`, Flow selects the best available model from the project's preferred models list based on API key availability.

Combine `auto` with `models` to provide a candidate set:

```python
response = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "Hello"}],
    extra_body={
        "models": ["claude-sonnet-4-6", "gpt-4o", "gemini-2.5-flash"],
        "provider": {"sort": "latency"}
    }
)
```

When `sort: "latency"` is set, candidates are ordered by real-time P95 latency before selection.

## Provider Preferences (`provider`)

Control how the gateway selects among available providers when a model can be served by multiple backends.

```python
response = client.chat.completions.create(
    model="claude-sonnet-4-6",
    messages=[{"role": "user", "content": "Hello"}],
    extra_body={
        "provider": {
            "order": ["bedrock", "anthropic"],
            "ignore": ["google"],
            "allow_fallbacks": True,
            "sort": "latency"
        }
    }
)
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `order` | `string[]` | Preferred provider order. Providers not listed are tried last. |
| `only` | `string[]` | Restrict to only these providers. All others are excluded. |
| `ignore` | `string[]` | Skip these providers entirely. |
| `allow_fallbacks` | `bool` | Whether fallback to other models/providers is allowed. Default: `true`. |
| `sort` | `string` | Sort strategy: `"latency"` sorts endpoints by P95 response time. |

### Provider Slugs

| Slug | Provider |
|------|----------|
| `openai` | OpenAI |
| `anthropic` | Anthropic |
| `google` | Google (Gemini) |
| `bedrock` | AWS Bedrock |
| `deepseek` | DeepSeek |
| `theta` | Theta |

## Multi-Provider Models

Some models can be served through multiple providers. For example, Claude models are available both directly from Anthropic and through AWS Bedrock.

When provider preferences are set, Flow queries the model endpoint registry to find all providers capable of serving the requested model, then applies filtering and sorting to select the best endpoint.

Currently supported multi-provider models:

| Model | Providers |
|-------|-----------|
| `claude-sonnet-4-6` | Anthropic, Bedrock |
| `claude-opus-4-6` | Anthropic, Bedrock |
| `claude-haiku-4-5` | Anthropic, Bedrock |
| `claude-sonnet-4` | Anthropic, Bedrock |
| `claude-opus-4` | Anthropic, Bedrock |

## Project-Level Defaults

All routing settings can be configured as project-level defaults in the UI under **Prompt Hub Settings**. These apply when a request doesn't specify its own values.

| Setting | Description |
|---------|-------------|
| Default Fallback Models | Fallback chain used when request has no `models` array |
| Fallback Enabled | Project-level toggle for all fallback behavior |
| Provider Preferences | Default provider order, ignored providers, and sort strategy |

Per-request values always override project defaults.

## Precedence

The fallback decision follows this precedence chain:

1. **Per-request `provider.allow_fallbacks`** (highest priority)
2. **Project-level `fallback_enabled`**
3. **Server-level fallback config** (lowest priority)

For fallback models:

1. **Per-request `models` array** (highest priority)
2. **Project-level default fallback models**

For provider selection:

1. **Per-request `provider` preferences** (highest priority)
2. **Project-level provider preferences**
3. **Default platform ordering** (lowest priority)
