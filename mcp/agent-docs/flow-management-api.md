# Flow — Management API

> **REST endpoints on this page are for the user's application backend to call programmatically.**
> You (the agent) do not call these REST endpoints yourself. When the user asks you to manage Flow resources (prompts, rollouts, integrations, settings), use the **MCP Equivalent** listed under each section. When the user asks you to help build backend code that integrates with the management API, use the REST endpoint documentation to write that code.

All REST endpoints are under `/api/llm/...` and require a project API key.

## Sessions

### REST Endpoint

- `GET /api/llm/sessions?project_id=...` — list sessions with metrics
- `GET /api/llm/sessions/{session_id}?project_id=...` — session details
- `GET /api/llm/sessions/{session_id}/requests?project_id=...` — requests in a session
- `POST /api/llm/sessions/{session_id}/feedback` — submit session feedback (score 1-5, text)

### MCP Equivalent

- List: `list` with `resource: 'sessions'`
- Get details: `get` with `resource: 'session', session_id: '...'`
- Get requests: `get` with `resource: 'session_requests', session_id: '...'`
- Submit feedback: `execute` with `resource: 'session', action: 'feedback', params: {session_id, score, text}`

## Evaluation Scores

### REST Endpoint

- `POST /api/llm/scores` — submit a score for an LLM request (score_name, score_value, score_type: number/boolean/category)
- `GET /api/llm/scores` — list scores with filters
- `GET /api/llm/scores/request/{request_id}` — scores for a specific request
- `POST /api/llm/scores/batch` — submit multiple scores atomically

### MCP Equivalent

- Submit: `execute` with `resource: 'llm_score', action: 'submit', params: {request_id, score_name, score_value, ...}`
- List: `list` with `resource: 'llm_scores'`
- Get for request: `get` with `resource: 'request_scores', request_id: '...'`

## Metrics

### REST Endpoint

- `GET /api/llm/metrics/overview` — dashboard overview (requests, sessions, cost, latency)
- `GET /api/llm/metrics/models` — per-model metrics with latency percentiles
- `GET /api/llm/metrics/cost/daily` — daily cost breakdown
- `GET /api/llm/metrics/users` — per-user usage

### MCP Equivalent

- Overview: `analyze` with `analysis: 'llm_overview'`
- Model metrics: `analyze` with `analysis: 'llm_model_metrics'`
- Daily cost: `analyze` with `analysis: 'llm_cost_daily'`
- User metrics: `analyze` with `analysis: 'llm_user_metrics'`

## Prompts and Rollouts

### REST Endpoint

- `POST /api/llm/prompts/configs` — create prompt config
- `GET /api/llm/prompts/configs` — list configs
- `PUT /api/llm/prompts/configs/{id}` — update config
- `DELETE /api/llm/prompts/configs/{id}` — delete config
- `POST /api/llm/prompts/configs/{id}/versions` — create version
- `GET /api/llm/prompts/configs/{id}/versions` — list versions
- `POST /api/llm/prompts/rollouts` — create rollout
- `POST /api/llm/prompts/rollouts/{id}/start` — start rollout
- `POST /api/llm/prompts/rollouts/{id}/pause` — pause
- `POST /api/llm/prompts/rollouts/{id}/promote` — advance to next stage
- `POST /api/llm/prompts/rollouts/{id}/rollback` — rollback
- `POST /api/llm/prompts/rollouts/{id}/complete` — complete at 100%

### MCP Equivalent

See the detailed workflows in the Prompt Management documentation. Key operations:

- Create config: `execute` with `resource: 'prompt', action: 'create_config'`
- Create version: `execute` with `resource: 'prompt', action: 'create_version'`
- Deploy: `execute` with `resource: 'prompt', action: 'deploy'` — should be explicitly requested by the user
- Promote/pause/rollback/complete: `execute` with `resource: 'prompt', action: '...'` — should be explicitly requested by the user
- Prompt config deletion is not available via MCP tools and must be done through the UI or REST API

## Playground

### REST Endpoint

- `POST /api/llm/playground` — run a single prompt
- `POST /api/llm/playground/compare` — compare up to 5 models side-by-side

### MCP Equivalent

- Run prompt: `analyze` with `analysis: 'playground', messages: [...], model: '...'`
- Compare models: `analyze` with `analysis: 'compare_models', messages: [...], compare_models: ['...']`

## Provider Integrations

### REST Endpoint

- `GET /api/llm/integrations` — list integrations
- `POST /api/llm/integrations` — add integration (provider, api_key)
- `PUT /api/llm/integrations/{provider}` — update
- `DELETE /api/llm/integrations/{provider}` — remove
- `POST /api/llm/integrations/{provider}/test` — test connectivity

### MCP Equivalent

- List: `list` with `resource: 'integrations'`
- Configure: `execute` with `resource: 'integration', action: 'configure'` — should be explicitly requested by the user
- Update: `execute` with `resource: 'integration', action: 'update'`
- Test: `execute` with `resource: 'integration', action: 'test'`

## Gateway Settings

### REST Endpoint

- `GET /api/llm/settings` — get all settings
- `PUT /api/llm/settings` — update settings (introspection, thinking budget, fallback, retry, rate limits, session budgets, guardrails, project model candidates, provider preferences, agent config)

### MCP Equivalent

- View: `get` with `resource: 'gateway_settings'`
- Update: `execute` with `resource: 'gateway', action: 'update_settings'` — changes affect all traffic through the gateway and should be explicitly requested by the user

For Reiver-owned routing, applications send `model: "auto"` and omit request-level `models` and `provider`. Configure exact live catalogue IDs in `default_fallback_models` and the project provider policy in `provider_preferences`. An empty model list lets Flow derive candidates from enabled integrations.

## Live Model Catalogue

### REST Endpoint

- `GET /api/llm/settings/models` — list current interactive models filtered to the project's enabled provider integrations

### MCP Equivalent

- `list` with `resource: 'model_catalog'`

Use this result as the source of truth before changing routing settings or explicitly pinning a model. Do not copy model IDs from static documentation.

## Text Search

### REST Endpoint

- `POST /api/llm/search` — search LLM request history by text

### MCP Equivalent

- `search` with `source: 'llm_requests', query: '...'`

## Pricing

### REST Endpoint

- `GET /api/llm/pricing` — list model pricing
- `POST /api/llm/pricing/sync` — trigger pricing sync

### MCP Equivalent

- List: `list` with `resource: 'llm_pricing'`
