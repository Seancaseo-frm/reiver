# Features

Flow provides a suite of features on top of basic LLM routing. All features work across every supported provider.

## Routing & Failover

Flow provides OpenRouter-style routing with per-request fallback models, multi-provider model routing, and granular provider preferences. See the full [Routing](/flow/routing) page for details.

Key capabilities:

- **Per-request fallback models** — specify a `models` array of ordered fallbacks
- **Multi-provider routing** — same model served by different providers (e.g., Claude via Anthropic or Bedrock)
- **Provider preferences** — control provider selection with `order`, `only`, `ignore`, and `sort` fields
- **Enhanced auto-routing** — combine `model: "auto"` with latency-based sorting

Response headers indicate when failover occurs:

| Header | Description |
|--------|-------------|
| `x-reiver-fallback-used` | `"true"` when a fallback provider served the response |
| `x-reiver-original-model` | The model originally requested |
| `x-reiver-model-used` | The model that actually served the response |
| `x-reiver-retry-count` | Number of retries before success |

## Semantic Caching

Flow caches responses using a two-layer cache:

- **L1** — In-process LRU cache for sub-millisecond lookups.
- **L2** — Distributed semantic cache ([semcache](https://github.com/sensoris/semcache)) shared across gateway instances.

Caching is eligible when:
- `stream` is `false` (or absent)
- `temperature` is `0`
- No tools are specified
- `n` is `1` (or absent)

The `x-reiver-cache` response header reports `"hit"`, `"miss"`, or `"skip"`.

## Guardrails

Guardrails run before and after the LLM call to enforce content policies. All guardrails are configured per-project via the [Management API](/flow/management-api) or the UI.

### Trust Modes

Trust modes control which message roles are treated as untrusted. Setting a trust mode is required for prompt injection detection and input spotlighting to activate.

| Mode | Untrusted roles | When to use |
|------|----------------|-------------|
| **Agent** | `tool` | Your application owns the agent. Tool results carry external data (emails, API responses, web scrapes) that could contain injection attacks. |
| **Chatbot** | `user`, `tool` | End users interact directly with the LLM. Both user messages and tool results are untrusted. |

When no trust mode is set, the role-aware guardrails (injection detection, spotlighting) are disabled and the gateway behaves as before.

### Input Guardrails

| Guardrail | Description |
|-----------|-------------|
| **Topic blocklist** | Rejects requests that match blocked topics. |
| **Token cap** | Rejects requests exceeding a configurable token limit. |
| **PII block-on-detect** | Blocks the request entirely if PII is detected in the input. |
| **Prompt injection detection** | Scans untrusted-role messages for injection patterns including instruction overrides, role impersonation, obfuscated payloads, Base64-encoded commands, and special tokens. Requires a trust mode to be set. |
| **Input spotlighting** | Wraps untrusted-role messages in structural delimiters and injects a canary system instruction that tells the model to treat delimited content as data, not instructions. Requires a trust mode to be set. |

### Output Guardrails

| Guardrail | Description |
|-----------|-------------|
| **PII masking** | Redacts PII (names, emails, phone numbers, etc.) from the response before returning it. |
| **Topic blocklist** | Blocks responses that match forbidden topics. |
| **LLM-as-judge** | Uses a secondary LLM call to evaluate the response against custom criteria. |
| **Tool call validation** | Blocks unauthorized tool calls. Tools can be restricted per-prompt via `allowed_tools` (whitelist) and project-wide via `blocked_tools` (blocklist). If the LLM returns a tool call that is not allowed, the response is rejected. |
| **Exfiltration scanning** | Blocks responses containing data exfiltration patterns — markdown images (`![](url)`) or HTML `<img>` tags pointing to external URLs. Attackers use these to smuggle conversation data to third-party servers via URL query parameters. |

When a guardrail triggers, the response includes a structured error with the rule that fired:

| Rule name | Trigger |
|-----------|---------|
| `blocked_input_topic` | Input matched a blocked topic |
| `blocked_output_topic` | Output matched a blocked topic |
| `token_limit` | Prompt exceeded the token cap |
| `pii_blocked` | PII detected with block-on-detect enabled |
| `prompt_injection_detected` | Injection patterns found in untrusted messages |
| `tool_call_blocked` | LLM attempted a disallowed tool call |
| `exfiltration_blocked` | Response contained an external image URL |

## PII Masking

PII masking can be enabled independently of guardrails. When active, it scans both input and output for personally identifiable information and redacts it. This operates transparently — your application receives the redacted text without needing to handle PII detection itself.

## Session Budgets

Set a per-session cost limit using the `x-reiver-session-id` header and the project's `gateway_session_budget_usd` setting. Once a session exceeds the budget, further requests are rejected with a `429` error. This prevents runaway costs from chatbot loops or automated agents.

```python
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
    extra_headers={"x-reiver-session-id": "session-abc-123"}
)
```

## Explicit Session End

By default, sessions are evaluated (classified, matched against profiles, and persisted) after 30 minutes of inactivity. If you want results sooner, call the session end endpoint right after the last LLM response:

```python
import requests

requests.post(
    f"https://reiver.ai/api/gateway/v1/sessions/{session_id}/end",
    headers={"Authorization": f"Bearer {api_key}"},
)
```

The session will be evaluated approximately 30 seconds later. This is idempotent — calling it multiple times for the same session is safe. If you never call it, the 30-minute idle timeout still works as a fallback.

See the [API Reference](/flow/api-reference) for full details.

## Output Contracts

Define a JSON schema in your prompt version's response format, and Flow will validate the LLM's response against it. This is useful for structured output where downstream code expects a specific shape.

When validation fails, the behavior depends on the configured `output_failure_action`:

| Action | Description |
|--------|-------------|
| `error` | Return an error to the caller. |
| `retry` | Retry the LLM call (up to a limit). |
| `retry_then_passthrough` | Retry, then pass through the invalid response if retries are exhausted. |
| `log_only` | Pass through the response but log the violation. |

The `x-output-contract-violation` response header is set to `"true"` when a passthrough occurs.

## Extended Thinking / Introspection

Flow supports extended thinking for models that offer it:

- **Anthropic Claude** — Extended thinking with configurable budget tokens.
- **OpenAI o-series** — Reasoning effort levels (`low`, `medium`, `high`).
- **Google Gemini** — Gemini thinking mode.

```python
response = client.chat.completions.create(
    model="claude-3-5-sonnet",
    messages=[{"role": "user", "content": "Solve this step by step."}],
    extra_body={
        "thinking": {
            "type": "enabled",
            "budget_tokens": 10000
        }
    }
)
```

## Multimodal Support

Flow supports multimodal messages across providers. Send images and documents alongside text:

```python
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{
        "role": "user",
        "content": [
            {"type": "text", "text": "What's in this image?"},
            {"type": "image_url", "image_url": {"url": "https://example.com/photo.jpg"}}
        ]
    }]
)
```

Document attachments (PDF, etc.) are also supported via `document_url` content parts.

## Cost Tracking

Every request is logged with token usage and cost, broken down by provider. This data is available in the Reiver dashboard for monitoring spend across models, projects, and time periods.

## Session Telemetry (OTel Correlation)

Flow can surface OpenTelemetry spans and logs alongside a session's LLM requests by correlating on a shared session ID attribute. This gives you a unified view of everything that happened during a conversation — the LLM calls, your application's traces, and any logs — without leaving the session detail page.

To enable this, tag your OTel spans and logs with one of the following attributes:

| Attribute | Spec |
|-----------|------|
| `gen_ai.session_id` | OpenTelemetry GenAI semantic convention (preferred) |
| `llm.session_id` | Legacy / custom attribute |

The attribute value must match the `x-reiver-session-id` you send with your gateway requests.

See the full [Session Telemetry](/flow/session-telemetry) page for integration examples and the API reference.

## Observability

All gateway requests are recorded with:

- Request and response payloads
- Token counts (input, output, total)
- Latency (end-to-end and provider time)
- Provider and model used
- Cache hit/miss status
- Prompt version and rollout variant (if applicable)
- Error details (if any)

This data integrates with Reiver Watch for end-to-end tracing — a single trace can show the API request, the LLM call it triggered, and the cost of each step.
