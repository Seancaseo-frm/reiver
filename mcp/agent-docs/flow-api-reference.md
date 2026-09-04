# Flow — Application Gateway Endpoint

> **This page documents the gateway endpoint that the user's application calls.**
> You (the agent) do not call this endpoint yourself. Use this information when the user asks you to help integrate their application with Flow — write code that their app will run, using the endpoints, headers, and request formats documented below.

Applications send chat completion requests to this OpenAI-compatible endpoint.

## Endpoint

`POST https://reiver.ai/api/gateway/v1/chat/completions`

## Authentication

Applications authenticate with a project API key: `Authorization: Bearer dh_your_key`

## Request Body

```json
{
  "model": "auto",
  "messages": [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "Hello!"}
  ],
  "temperature": 0.7,
  "max_tokens": 1024,
  "stream": false
}
```

### Fields

- `model` (string, required) — use `"auto"` for Reiver-owned routing. Use a concrete ID from MCP `list` resource `model_catalog` only when the application explicitly owns the pin.
- `messages` (array, required) — message objects with `role` and `content`. Max 1000 messages, max 1MB per message.
- `temperature` (number) — 0.0–2.0
- `max_tokens` (integer) — max tokens to generate, up to 1,000,000
- `top_p` (number) — nucleus sampling, 0.0–1.0
- `n` (integer) — completions to generate, max 10
- `stream` (boolean) — stream response as Server-Sent Events
- `stop` (string or array) — stop sequences
- `frequency_penalty` / `presence_penalty` (number) — -2.0–2.0
- `user` (string) — end-user identifier
- `seed` (integer) — deterministic sampling
- `tools` (array) — tool/function definitions
- `tool_choice` (string or object) — `"none"`, `"auto"`, `"required"`, or specific function
- `response_format` (object) — `{"type": "text"}`, `{"type": "json_object"}`, or `{"type": "json_schema"}`
- `thinking` (object) — extended thinking: `{"type": "enabled", "budget_tokens": 10000}`
- `reasoning_effort` (string) — for o-series: `"low"`, `"medium"`, `"high"`
- `models` (array) — ordered fallback models, max 5
- `provider` (object) — provider preferences: `order`, `only`, `ignore`, `allow_fallbacks`, `sort`
- `prompt_config` (string) — name of a Flow prompt config to apply
- `prompt_variables` (object) — template variables for the managed prompt

### Message object

- `role` (string, required) — `"system"`, `"user"`, `"assistant"`, or `"tool"`
- `content` (string or array) — text or array of content parts (for multimodal)
- `name` (string) — author name for multi-user conversations
- `tool_calls` (array) — tool calls made by the assistant
- `tool_call_id` (string) — for tool messages: ID of the tool call being responded to

### Multimodal content parts

```json
[
  {"type": "text", "text": "What's in this image?"},
  {"type": "image_url", "image_url": {"url": "https://example.com/photo.jpg"}},
  {"type": "document_url", "document_url": {"url": "data:application/pdf;base64,..."}}
]
```

## Request Headers

### Required

- `Authorization` — `Bearer dh_your_key` (project API key)

### Optional

- `x-reiver-prompt-config` — prompt config name
- `x-reiver-session-id` — session identifier for budgets and sticky rollout allocation
- `x-reiver-user-id` — user identifier for user-sticky rollout allocation
- `x-reiver-force-variant` — `"target"` or `"baseline"` (debugging only)
- `x-reiver-var-{name}` — template variable value (normalized: `x-reiver-var-user-name` → `user_name`)

## Response (Non-Streaming)

```json
{
  "id": "chatcmpl-abc123",
  "object": "chat.completion",
  "created": 1700000000,
  "model": "<actual-model-id>",
  "choices": [{
    "index": 0,
    "message": {"role": "assistant", "content": "Hello! How can I help you?"},
    "finish_reason": "stop"
  }],
  "usage": {"prompt_tokens": 25, "completion_tokens": 10, "total_tokens": 35}
}
```

## Response Headers

- `x-reiver-provider` — provider that served the response
- `x-reiver-model-used` — model that generated the response
- `x-reiver-original-model` — original requested model (present on failover)
- `x-reiver-fallback-used` — `"true"` when a fallback was used
- `x-reiver-retry-count` — retries before success
- `x-reiver-cache` — `"hit"`, `"miss"`, or `"skip"`
- `x-output-contract-violation` — `"true"` when response failed schema validation but was passed through
- `x-request-id` — unique request ID

## Error Responses

```json
{"error": {"message": "Description", "type": "error_type", "code": "error_code"}}
```

- 400 — validation error
- 401 — missing or invalid API key
- 404 — prompt config not found
- 422 — template variable validation failed
- 429 — session budget exceeded
- 500 — provider error
- 503 — provider unavailable after retries

## Embeddings Endpoint

`POST https://reiver.ai/api/gateway/v1/embeddings`

Generate vector embeddings for text input. OpenAI-compatible format.

### Request Body

```json
{
  "model": "text-embedding-3-small",
  "input": "Hello world",
  "encoding_format": "float",
  "dimensions": 256
}
```

### Fields

- `model` (string, required) — embedding model identifier (e.g., `"text-embedding-3-small"`, `"mistral/mistral-embed"`)
- `input` (string or array, required) — text to embed. A single string or an array of strings.
- `encoding_format` (string) — `"float"` (default) or `"base64"`
- `dimensions` (integer) — output dimensions (model-dependent)
- `user` (string) — end-user identifier

### Response

```json
{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "embedding": [0.0023, -0.0091, ...],
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

### Supported Providers

OpenAI, Mistral, Together, Fireworks, DeepInfra, Cohere, Nvidia, Azure OpenAI. Other providers return an `UnsupportedModel` error.

### Guardrails

PII masking and content policy guardrails apply to embedding input text, same as chat completions.

## Models Endpoint

`GET https://reiver.ai/api/gateway/v1/models` — lists supported model prefixes.
