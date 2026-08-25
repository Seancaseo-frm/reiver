# Management API

The management API lets you configure projects, view metrics, manage prompts, and administer provider integrations programmatically. All endpoints are under `/api/llm/...` and require an `X-Project-Id` header (set automatically when calling through the website proxy).

---

## Sessions

Base path: `/api/llm/sessions`

See also: [Session Telemetry](/flow/session-telemetry) for correlating OTel spans and logs with sessions.

### GET /

List sessions with aggregated metrics.

| Query Param | Type | Required | Description |
|---|---|---|---|
| `project_id` | UUID | yes | Project to query |
| `limit` | integer | no | Max results (default 20, max 1000) |
| `offset` | integer | no | Pagination offset |
| `user_id` | string | no | Filter by user |
| `name_pattern` | string | no | Filter by session name (LIKE) |
| `start_date` | datetime | no | Filter start |
| `end_date` | datetime | no | Filter end |

Returns a paginated list of sessions with `request_count`, `total_tokens`, `total_cost_usd`, `error_count`, and `feedback_score`.

### GET /{session_id}

Get detailed session information including token breakdown, average latency, models used, and feedback.

### GET /{session_id}/requests

List individual LLM requests within a session. Supports `limit` and `offset` pagination.

### POST /{session_id}/feedback

Submit feedback for a session.

```json
{
  "project_id": "uuid",
  "score": 4,
  "text": "Very helpful session"
}
```

`score` must be 1-5 if provided. At least one of `score` or `text` is required.

---

## Evaluation Scores

Base path: `/api/llm/scores`

### POST /

Submit a single evaluation score for an LLM request.

```json
{
  "project_id": "uuid",
  "request_id": "req-123",
  "score_name": "relevance",
  "score_value": 0.95,
  "score_type": "number",
  "reason": "Highly relevant response",
  "evaluator_type": "human"
}
```

`score_type` can be `number` (0-100), `boolean` (0 or 1), or `category`.

### GET /

List scores with optional filters: `score_name`, `evaluator_type`, `start_date`, `end_date`. Supports `limit`/`offset` pagination.

### GET /request/{request_id}

Get all evaluation scores for a specific LLM request.

### POST /batch

Submit multiple scores atomically in a single transaction.

```json
{
  "project_id": "uuid",
  "scores": [
    { "request_id": "req-1", "score_name": "relevance", "score_value": 0.9 },
    { "request_id": "req-1", "score_name": "coherence", "score_value": 0.85 }
  ]
}
```

---

## Metrics

Base path: `/api/llm/metrics`

All metrics endpoints accept `project_id` (required), `start_date`, `end_date`, `limit`, and `offset` as query parameters.

### GET /users

Per-user LLM usage: request count, session count, tokens, cost, errors, and models used.

### GET /models

Per-model metrics including latency percentiles (p50, p95, p99), error rate, token usage, and cost.

### GET /cost/daily

Daily cost breakdown: request count, input/output tokens, and total cost per day.

### GET /cost/by-model

Cost breakdown by model with percentage of total spend.

### GET /overview

Dashboard overview: total requests, sessions, users, tokens, cost, error rate, average latency, and top models. Date range capped at 30 days.

### GET /provider-latency

Real-time provider latency from the in-memory tracker. Returns p50/p95/p99 latency, sample count, and degradation status per provider. No query parameters required.

---

## Pricing

Base path: `/api/llm/pricing`

### GET /

List all model pricing. Supports `source` filter (`helicone`, `openrouter`, `manual`) and `search` (ILIKE on model/provider).

### GET /{provider}

List pricing for a specific provider.

### POST /sync

Trigger a manual pricing sync from Helicone and OpenRouter. Returns sync status, counts of models added/updated/skipped, and any errors.

### GET /sync-history

Get the last 50 pricing sync log entries.

### PUT /{provider}/{model}

Override pricing for a specific model. Sets the source to `manual` so future auto-syncs won't overwrite it.

```json
{
  "input_cost_per_1m": "3.00",
  "output_cost_per_1m": "15.00",
  "cache_read_cost_per_1m": "0.30",
  "cache_write_cost_per_1m": "3.75"
}
```

---

## Prompts and Rollouts

Base path: `/api/llm/prompts`

### Prompt Configs

#### POST /configs

Create a prompt config.

```json
{
  "project_id": "uuid",
  "name": "customer-support-v2",
  "description": "Customer support system prompt"
}
```

#### GET /configs

List prompt configs for a project. Returns config metadata, active version number, version count, and whether an active rollout exists.

#### GET /configs/{config_id}

Get a single prompt config with version metadata.

#### PUT /configs/{config_id}

Update a prompt config's name or description.

#### DELETE /configs/{config_id}

Delete a prompt config. Blocked if an active rollout exists. Cascades to all versions.

### Prompt Versions

#### POST /configs/{config_id}/versions

Create a new prompt version. Version numbers auto-increment. The first version is automatically activated.

```json
{
  "project_id": "uuid",
  "system_prompt": "You are a helpful assistant for {{company_name}}.",
  "model": "gpt-4o",
  "temperature": 0.7,
  "max_tokens": 1024,
  "variables": [
    { "name": "company_name", "type": "string", "required": true }
  ],
  "tools": [...],
  "response_format": { "type": "json_schema", "json_schema": {...} },
  "commit_message": "Added company personalization"
}
```

#### GET /configs/{config_id}/versions

List versions for a prompt config.

#### GET /configs/{config_id}/versions/{version_id}

Get a single prompt version.

### Rollouts

#### POST /rollouts

Create a progressive rollout (A/B test) between a baseline and target prompt version.

```json
{
  "project_id": "uuid",
  "config_id": "uuid",
  "target_version_id": "uuid",
  "name": "v3-canary",
  "mode": "auto",
  "allocation_type": "user_sticky",
  "stages": [
    { "weight": 10, "min_duration_minutes": 30, "min_requests": 500 },
    { "weight": 50, "min_duration_minutes": 60 },
    { "weight": 100 }
  ]
}
```

`mode` can be `auto` (worker evaluates metrics and promotes/rolls back) or `manual`.

`allocation_type` can be `random`, `user_sticky`, or `session_sticky`.

If `stages` is omitted, a default 7-stage progression is used (1% to 100%).

#### GET /rollouts

List rollouts. Supports filtering by `config_id` and `status`.

#### GET /rollouts/{rollout_id}

Get a rollout with all its stages.

#### POST /rollouts/{rollout_id}/start

Start a pending rollout.

#### POST /rollouts/{rollout_id}/pause

Pause a running rollout. Requests fall through without a managed prompt while paused.

#### POST /rollouts/{rollout_id}/promote

Manually advance to the next stage (or complete if at the final stage).

#### POST /rollouts/{rollout_id}/rollback

Roll back to baseline.

#### POST /rollouts/{rollout_id}/complete

Force-complete a rollout, skipping remaining stages. Sets the target version as active.

#### GET /rollouts/{rollout_id}/metrics

Get target vs baseline metrics comparison: request count, error rate, latency (avg + p95), cost, and an overall passing/failing/inconclusive status.

---

## Playground

Base path: `/api/llm/playground`

### POST /

Run a single playground prompt.

```json
{
  "project_id": "uuid",
  "model": "claude-sonnet-5",
  "messages": [
    { "role": "user", "content": "Explain quantum computing simply." }
  ],
  "auto_evaluate": true
}
```

Returns the model response, token usage, latency, cost, and optional LLM-as-judge evaluation scores (relevance, coherence, helpfulness).

Claude Sonnet 5 manages sampling at the provider, so its example omits `temperature`. Reiver strips unsupported sampling values if a managed prompt supplies one.

### POST /compare

Compare the same prompt across multiple models in parallel (max 5, 30s timeout per model).

```json
{
  "project_id": "uuid",
  "messages": [{ "role": "user", "content": "Hello!" }],
  "compare_models": ["claude-sonnet-5", "claude-fable-5", "claude-opus-5"]
}
```

Returns per-model responses plus a cost comparison with `tokens_per_dollar`, and identifies the fastest and cheapest model.

---

## Text Search

Base path: `/api/llm/search`

### POST /

Search LLM request history by text matching on prompts and completions.

```json
{
  "project_id": "uuid",
  "query": "password reset flow",
  "limit": 10,
  "model": "gpt-4o",
  "start_time": "2026-01-01T00:00:00Z"
}
```

Returns matching requests with content previews.

---

## Provider Integrations

Base path: `/api/llm/integrations`

### GET /

List all configured provider integrations for the project.

### POST /

Add a provider integration. API keys are encrypted at rest.

```json
{
  "provider": "openai",
  "api_key": "sk-...",
  "enabled": true
}
```

For AWS Bedrock, use `access_key_id`, `secret_access_key`, and `region` instead of `api_key`.

### PUT /{provider}

Update an integration (rotate key, toggle enabled).

### DELETE /{provider}

Remove a provider integration and its stored key.

### POST /{provider}/test

Test connectivity to a provider. Optionally pass a temporary key to test before saving.

---

## Gateway Settings

Base path: `/api/llm/settings`

### GET /

Get all gateway settings for the project.

### PUT /

Update gateway settings. Accepts the full settings object:

```json
{
  "introspection_enabled": true,
  "thinking_budget_tokens": 10000,
  "fallback_enabled": true,
  "fallback_order": ["anthropic", "openai", "google"],
  "retry_enabled": true,
  "retry_max_attempts": 3,
  "monthly_budget_usd": 500.0,
  "budget_alert_enabled": true,
  "budget_hard_stop": false,
  "rate_limit_enabled": true,
  "rate_limit_rpm": 120,
  "session_budget_usd": 1.50,
  "guardrails": {
    "trust_mode": "agent",
    "blocked_input_topics": ["violence"],
    "blocked_output_topics": [],
    "max_prompt_tokens": 4096,
    "pii_block_on_detect": false,
    "prompt_injection_detection": true,
    "spotlighting_enabled": true,
    "mask_output_pii": true,
    "min_quality_score": 0.7,
    "blocked_tools": ["send_email", "execute_sql"],
    "block_exfiltration_urls": true
  },
  "agent_enabled": true,
  "agent_scopes": ["project:read", "llm:read", "observability:read", "billing:read"]
}
```

---

## In-App Agent

Base path: `/api/agent`

The in-app AI agent provides a conversational interface backed by the platform's MCP tools. Responses stream via Server-Sent Events (SSE).

### POST /chat

Send a message to the agent. Returns an SSE stream of events.

```json
{
  "conversation_id": "uuid (optional — omit to start a new conversation)",
  "message": "What's my LLM usage this week?",
  "page_context": {
    "page": "/projects/my-project/llm/overview",
    "data": {}
  }
}
```

**SSE event types:**

| Event type | Fields | Description |
|-----------|--------|-------------|
| `conversation_created` | `conversation_id` | Emitted at the start of a new conversation |
| `text_delta` | `content` | Streamed text chunk from the assistant |
| `tool_start` | `call_id`, `name` | A tool call has started |
| `tool_result` | `call_id`, `content` | Tool execution completed |
| `done` | — | Stream is complete |
| `error` | `content` | An error occurred |

### GET /conversations

List conversations for the current project.

| Query Param | Type | Default | Description |
|---|---|---|---|
| `limit` | integer | 50 | Max results (max 100) |
| `offset` | integer | 0 | Pagination offset |

### POST /conversations

Create a new conversation.

```json
{
  "title": "Optional conversation title"
}
```

### DELETE /conversations/{id}

Delete a conversation and all its messages. Returns 404 if the conversation does not exist.

### GET /conversations/{id}/messages

List messages in a conversation, ordered chronologically.

| Query Param | Type | Default | Description |
|---|---|---|---|
| `limit` | integer | 100 | Max results |
| `offset` | integer | 0 | Pagination offset |
