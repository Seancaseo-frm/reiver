# API Reference

## Base URL

```
https://reiver.ai/api/gateway/v1
```

## Endpoints

### POST /chat/completions

Create a chat completion. Supports both streaming and non-streaming responses.

**Full path:** `POST /api/gateway/v1/chat/completions`

#### Request Body

```json
{
  "model": "claude-sonnet-5",
  "messages": [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "Hello!"}
  ],
  "max_tokens": 1024,
  "stream": false
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `model` | string | Yes | Interactive model identifier (for example, `"claude-sonnet-5"`). Prove availability in the Playground or response headers before adding `"auto"` or fallbacks. Batch catalogue IDs are not valid interactive choices. |
| `messages` | array | Yes | Array of message objects. Max 1000 messages, max 1MB per message. |
| `temperature` | number | No | Sampling temperature, 0.0–1.0. Provider support varies; Reiver omits it for Claude models that reject non-default sampling controls. |
| `max_tokens` | integer | No | Maximum tokens to generate. Max 1,000,000. |
| `top_p` | number | No | Nucleus sampling threshold, 0.0–1.0. Provider support varies; it is omitted for the same provider-managed Claude families. |
| `n` | integer | No | Number of completions to generate. Max 10. |
| `stream` | boolean | No | If `true`, response is streamed as Server-Sent Events. |
| `stop` | string or array | No | Stop sequence(s) where generation halts. |
| `frequency_penalty` | number | No | Frequency penalty, -2.0–2.0. |
| `presence_penalty` | number | No | Presence penalty, -2.0–2.0. |
| `user` | string | No | Stable end-user identifier. Set it to the same value as `x-reiver-user-id` for current Reiver user analytics. |
| `seed` | integer | No | Seed for deterministic sampling. |
| `tools` | array | No | Tool/function definitions the model may call. |
| `tool_choice` | string or object | No | Controls tool usage: `"none"`, `"auto"`, `"required"`, or a specific function. |
| `response_format` | object | No | Response format: `{"type": "text"}`, `{"type": "json_object"}`, or `{"type": "json_schema"}`. |
| `thinking` | object | No | Compatibility toggle for models that support it. Claude 5 uses adaptive thinking; do not send legacy manual budgets to Sonnet 5 or Fable 5. |
| `reasoning_effort` | string | No | For o-series models: `"low"`, `"medium"`, or `"high"`. |
| `models` | array | No | Ordered fallback model list. If the primary `model` fails, these are tried in order. Max 5. See [Routing](/flow/routing). |
| `provider` | object | No | Provider preference object with fields: `order` (preferred provider list), `only` (restrict to these), `ignore` (skip these), `allow_fallbacks` (bool), `sort` (`"latency"`). See [Routing](/flow/routing). |
| `prompt_config` | string | No | Name of a Flow prompt config to apply. Alternative to the `x-reiver-prompt-config` header. |
| `prompt_variables` | object | No | Template variables for the managed prompt. Alternative to `x-reiver-var-*` headers. |

#### Message Object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `role` | string | Yes | `"system"`, `"user"`, `"assistant"`, or `"tool"`. |
| `content` | string or array | Yes | Text content or array of content parts (for multimodal). |
| `name` | string | No | Author name for multi-user conversations. |
| `tool_calls` | array | No | Tool calls made by the assistant. |
| `tool_call_id` | string | No | For tool messages: ID of the tool call being responded to. |

#### Content Parts (Multimodal)

When `content` is an array, each element is a content part:

```json
[
  {"type": "text", "text": "What's in this image?"},
  {"type": "image_url", "image_url": {"url": "https://example.com/photo.jpg"}},
  {"type": "document_url", "document_url": {"url": "data:application/pdf;base64,..."}}
]
```

#### Response (Non-Streaming)

```json
{
  "id": "chatcmpl-abc123",
  "object": "chat.completion",
  "created": 1700000000,
  "model": "claude-sonnet-5",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "Hello! How can I help you?"
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 25,
    "completion_tokens": 10,
    "total_tokens": 35
  }
}
```

#### Response (Streaming)

When `stream: true`, the response is a stream of SSE events:

```
data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1700000000,"model":"claude-sonnet-5","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}

data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1700000000,"model":"claude-sonnet-5","choices":[{"index":0,"delta":{"content":"!"},"finish_reason":null}]}

data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1700000000,"model":"claude-sonnet-5","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":25,"completion_tokens":2,"total_tokens":27}}

data: [DONE]
```

### POST /embeddings

Generate vector embeddings for text input. OpenAI-compatible format.

**Full path:** `POST /api/gateway/v1/embeddings`

#### Request Body

```json
{
  "model": "text-embedding-3-small",
  "input": "Hello world"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `model` | string | Yes | Embedding model identifier (e.g., `"text-embedding-3-small"`, `"mistral/mistral-embed"`). |
| `input` | string or array | Yes | Text to embed. A single string or an array of strings. |
| `encoding_format` | string | No | `"float"` (default) or `"base64"`. |
| `dimensions` | integer | No | Output dimensions (model-dependent). |
| `user` | string | No | End-user identifier for abuse monitoring. |

#### Response

```json
{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "embedding": [0.0023, -0.0091, 0.0150],
      "index": 0
    }
  ],
  "model": "text-embedding-3-small",
  "usage": {
    "prompt_tokens": 2,
    "total_tokens": 2
  }
}
```

#### Supported Providers

OpenAI, Mistral, Together, Fireworks, DeepInfra, Cohere, Nvidia, Azure OpenAI. Providers that do not support embeddings return an `UnsupportedModel` error.

#### Guardrails

PII masking and content policy guardrails apply to embedding input text, same as chat completions. Chat-specific guardrails (spotlighting, prompt injection, output guardrails) do not apply.

### POST /sessions/{session_id}/end

Mark an LLM session as ended, scheduling evaluation after an approximately 30-second ingestion buffer instead of waiting for idle discovery. The session will be classified, matched against session profiles, and persisted.

**Full path:** `POST /api/gateway/v1/sessions/{session_id}/end`

| Parameter | Type | Description |
|-----------|------|-------------|
| `session_id` | path (string) | The session ID (same value you pass in `x-reiver-session-id`). |

#### Response (202 Accepted)

```json
{ "session_id": "sess-abc-123", "status": "evaluation_scheduled" }
```

Or if the session was already enqueued for evaluation:

```json
{ "session_id": "sess-abc-123", "status": "already_enqueued" }
```

Both cases return `202`. The caller does not need to distinguish between them. `202` confirms scheduling, not completed evaluation; normally the evaluation runs approximately 30 seconds after the call. Verify that the session becomes queryable, and see the [Session and Identity Contract](/flow/session-telemetry) for restart recovery behaviour.

#### Example

```bash
curl -X POST https://reiver.ai/api/gateway/v1/sessions/sess-abc-123/end \
  -H "Authorization: Bearer dh_your_key"
```

```python
import requests

requests.post(
    "https://reiver.ai/api/gateway/v1/sessions/sess-abc-123/end",
    headers={"Authorization": "Bearer dh_your_key"},
)
```

#### Notes

- **Idempotent** — safe to call multiple times for the same session.
- If you never call this endpoint, sessions are still evaluated automatically after 30 minutes of inactivity.
- The 30-second delay before evaluation ensures that the last LLM request has been flushed from internal buffers and is included in the session data.

### GET /models

List supported model prefixes.

**Full path:** `GET /api/gateway/v1/models`

---

## Request Headers

### Required

| Header | Description |
|--------|-------------|
| `Authorization` | `Bearer dh_your_key` — the application's Reiver SDK key (`REIVER_FLOW_API_KEY`). |

### Optional

| Header | Description |
|--------|-------------|
| `x-reiver-prompt-config` | Name of the prompt config to apply. Alternative to the `prompt_config` body field. |
| `x-reiver-session-id` | Session identifier for session budgets and session-sticky rollout allocation. |
| `x-reiver-user-id` | Stable user identifier for user-sticky rollout allocation. Also set the request body's `user` field to this value for current per-user analytics. |
| `x-reiver-force-variant` | Force a rollout variant: `"target"` or `"baseline"`. For debugging only. |
| `x-reiver-var-{name}` | Template variable value. Header name is normalized: `x-reiver-var-user-name` becomes `user_name`. Max 255 characters per value. |

## Response Headers

| Header | Description |
|--------|-------------|
| `x-reiver-provider` | Provider that served the response (e.g., `openai`, `anthropic`). |
| `x-reiver-model-used` | Model that generated the response. |
| `x-reiver-original-model` | Original requested model (present when failover occurred). |
| `x-reiver-fallback-used` | `"true"` when a fallback provider was used. |
| `x-reiver-retry-count` | Number of retries before the request succeeded. |
| `x-reiver-cache` | Cache status: `"hit"`, `"miss"`, or `"skip"`. |
| `x-reiver-warning` | Warning message (e.g., explicit mode but no prompt_config sent). |
| `x-output-contract-violation` | `"true"` when the response failed output schema validation but was passed through. |
| `x-request-id` | Unique request ID for tracing and correlation. |

## Error Responses

Errors follow a standard format:

```json
{
  "error": {
    "message": "Description of what went wrong",
    "type": "error_type",
    "code": "error_code"
  }
}
```

| HTTP Status | Condition |
|-------------|-----------|
| `400` | Validation error (empty messages, invalid temperature, etc.) |
| `401` | Missing or invalid API key / project ID |
| `404` | Prompt config not found |
| `422` | Template variable validation failed |
| `429` | Session budget exceeded |
| `500` | Provider error or internal failure |
| `503` | Provider unavailable after retries and failover |
