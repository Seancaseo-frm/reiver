# Flow — Routing

Flow supports multi-provider routing with per-request fallback models and granular provider preferences.

## Application Integration

### Per-request fallback models

Applications specify an ordered fallback list via the `models` field:

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

Behavior:
- `model` is tried first. If it fails with 5xx, 429, or timeout, `models` are tried in order.
- Maximum 5 fallback models per request.
- The `models` array is stripped before forwarding to the upstream provider.

### Auto-routing

Applications set `model: "auto"` and Flow selects the best available model:

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

### Provider preferences

Applications control provider selection when a model can be served by multiple backends:

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

Provider preference fields:

- `order` — preferred provider order; providers not listed are tried last
- `only` — restrict to only these providers
- `ignore` — skip these providers entirely
- `allow_fallbacks` — whether fallback to other models/providers is allowed (default: true)
- `sort` — `"latency"` sorts endpoints by P95 response time

Provider slugs: `openai`, `anthropic`, `google`, `bedrock`, `deepseek`, `theta`.

### Multi-provider models

Some models are available through multiple providers:

- `claude-sonnet-4-6` — Anthropic, Bedrock
- `claude-opus-4-6` — Anthropic, Bedrock
- `claude-haiku-4-5` — Anthropic, Bedrock
- `claude-sonnet-4` — Anthropic, Bedrock
- `claude-opus-4` — Anthropic, Bedrock

### Precedence

Fallback decisions follow this priority:
1. Per-request `provider.allow_fallbacks` (highest)
2. Project-level `fallback_enabled`
3. Server-level fallback config (lowest)

## Platform Management (MCP)

Project-level routing defaults (fallback models, provider preferences) are part of the gateway settings. To view or update:

- View: `get` with `resource: 'gateway_settings'`
- Update: `execute` with `resource: 'gateway', action: 'update_settings'`
