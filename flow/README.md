# flow — LLM Gateway

`flow` is an OpenAI-compatible LLM gateway that proxies, routes, and normalises requests across multiple providers. It exposes a single `/chat/completions` endpoint and handles provider failover, prompt management, observability, and A/B testing transparently to callers.

## Supported Providers

| Provider | Model prefix |
|---|---|
| OpenAI | `gpt-*`, `o1-*`, `o3-*`, `o4-*` |
| Anthropic | `claude-*` |
| Google Gemini | `gemini-*` |
| AWS Bedrock | `bedrock/*`, `anthropic.*`, `amazon.*`, `meta.*`, `ai21.*`, `mistral.*`, `cohere.*` |
| Theta EdgeCloud (vLLM) | `theta/` |

## Features

### Configurable Provider Failover

Requests are automatically retried on transient errors and can fall over to an alternative provider. Fallback models are configured per-project via the `default_fallback_models` setting or per-request via the `models` array. An adaptive latency tracker deprioritises slow providers in real time and shares state across replicas via Redis.

### Prompt Playground

Interactive prompt testing with:
- **Direct model routing** — send to a specific provider/model
- **Fallback routing** — route through the same logic as live traffic (`model: "auto"` or `use_fallback_chain: true`)
- **LLM-as-judge auto-evaluation** — automatic response scoring (relevance, coherence, helpfulness) via a configurable evaluation model
- **Model comparison** — run the same prompt against up to 5 models in parallel and see cost/latency breakdowns

### LLM Introspection

Exposes the internal reasoning process of models that support it:
- **Anthropic Claude 3.7+** — extended thinking blocks via the `anthropic-beta: interleaved-thinking-2025-01-05` header
- **Google Gemini 2.x** — thinking mode with configurable token budget
- **OpenAI o-series** — reasoning effort and `reasoning_tokens` from `completion_tokens_details`

Thinking content is streamed in real time. A per-project default can be configured via the settings API (`gateway_introspection_enabled`, `gateway_thinking_budget_tokens`); per-request `thinking` config always takes precedence.

### Canary Prompt Deployments

Prompt versions can be gradually rolled out from the playground. A background worker monitors rollout health (latency, error rate, evaluation scores) and automatically promotes or rolls back the canary. Traffic split is controlled by rollout percentage.

### Prompt A/B Testing

Multiple prompt variants can be served simultaneously. Variant selection is deterministic per-session, tracked on every request (`rollout_variant`, `prompt_config_id`, `prompt_version_id`), and aggregated in ClickHouse for statistical comparison.

### Session Cost Budgets

Per-session LLM spend caps enforced in real time at the gateway layer. When `gateway_session_budget_usd` is set on a project, every request carrying an `x-reiver-session-id` header is checked against the accumulated spend for that session stored in Redis. Requests that would exceed the limit are rejected with HTTP 429 before reaching any provider.

- **Default off**: the budget is 0 by default, meaning no enforcement until explicitly configured
- **Fail open**: if Redis is unreachable the check is skipped so provider availability is never compromised
- **Soft cap**: cost is recorded after the response, so a session can overshoot by at most one request's cost
- **Auto-expiry**: Redis keys have a 24-hour TTL so there is no manual cleanup

Configured via `PUT /llm/settings` (`gateway_session_budget_usd`). Response headers `x-session-budget-limit` and `x-session-budget-used` are included on 429 rejections.

### Prompt Security / PII Masking

Prompt content is scanned and redacted before requests leave your infrastructure using the same SIMD-accelerated engine as Watch's log ingestion pipeline. PII is replaced with `[REDACTED]` inline in the message body — providers never see the raw sensitive data.

Covered patterns: SSN, credit cards, email addresses, IPv4 addresses, phone numbers (US and international), IBAN, bank account and routing numbers, AWS access and secret keys, and labeled API tokens.

Controlled by the per-project `pii_masking_enabled` setting (enabled by default, same flag that governs Watch log ingestion). No additional configuration required — turning it on for APM automatically covers the gateway too.

### Quality-Aware Routing

Per-session evaluation scores are collected and stored. Metrics are aggregated by model, provider, and (roadmap) topic/keyword to enable automatic model selection strategies optimised for correctness. See `llm_scores` and `llm_metrics` APIs.

### LLM Observability

Every request emits a full span to Kafka (topic: `KAFKA_LLM_CHUNKS_TOPIC`) with token counts, cost, latency, provider, model, prompt version, and rollout metadata. Spans are consumed by ClickHouse for querying via the `llm_sessions` and `llm_metrics` APIs.

When **OTLP trace export** is enabled (`OTLP_INGEST_URL` set), the gateway also sends one OpenTelemetry span per chat completion to the observability ingest endpoint. Those traces appear in the observability UI (Traces, metrics) under the correct project. The gateway POSTs to the ingest URL with the same Bearer token the client used, so the website proxy can validate and set `X-Project-Id`. Export is fire-and-forget and does not block the response.

### GitOps for Prompts

The playground, versioning, A/B testing, and canary deployment features together form a full prompt deployment workflow that mirrors how code is shipped:

| Stage | Feature |
|---|---|
| **Author** | Prompt Playground — iterate against live models with LLM-as-judge scoring |
| **Version** | Immutable prompt versions with parameter snapshots |
| **Test** | Model comparison — run the same prompt across up to 5 models and compare cost/quality |
| **Canary** | Canary Prompt Deployments — gradual rollout with automatic health gates |
| **Promote / Rollback** | Background worker monitors latency, error rate, and evaluation scores; promotes or rolls back automatically |
| **Audit** | Every request records `prompt_config_id`, `prompt_version_id`, and `rollout_variant` — full attribution for any quality regression |

### Managed Prompts

Managed prompts and canary rollouts apply when a request references a prompt config — either via the `prompt_config` body field or the `X-Reiver-Prompt-Config` header. Requests that don't reference a config pass through with their own messages unchanged.

**Developer setup:**

1. Remove the system prompt from application code
2. Add two fields to every LLM call:

```python
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": user_message}],
    extra_body={
        "prompt_config": "customer-support",
        "prompt_variables": {
            "user_name": current_user.name,
            "account_type": current_user.plan,
            "current_date": date.today().isoformat(),
        }
    }
)
```

After that, every prompt change and canary deployment happens in the Flow dashboard with zero code deployments. Runtime variables are passed inline alongside the messages — no environment variables, no headers, no new SDK.

`X-Reiver-Var-*` headers and `prompt_variables` body fields can both be used; headers take precedence when the same key appears in both.

### Topic-Aware Quality Routing *(Roadmap)*

Extends the existing per-session evaluation score infrastructure to learn which model performs best for each type of request. After enough traffic, the gateway automatically routes coding questions to the model that historically scores highest on coding tasks, creative prompts to a different model, and so on. No manual configuration — the routing policy is derived from your own evaluation data.

### Cross-System Cost Attribution *(Roadmap)*

Bridges Flow and Pond (the Reiver federated warehouse) so LLM session data is joinable with Stripe charges, Postgres tables, and any other connected data source using standard SQL. Enables queries like "which AI feature generated the most revenue relative to its LLM cost?" directly from a BI tool or via natural-language Text-to-SQL.

### Prompt Cost Impact Analysis *(Roadmap — Watch integration)*

When a new prompt version is tested in the playground, Watch's token-cost telemetry (already flowing through ClickHouse from every gateway request) is used to project the forward cost impact of deploying it. Before you start a canary rollout you see: "switching from v3 to v4 increases average prompt tokens by 18% — estimated +$X/month at your current traffic volume." The projection uses the actual token distribution from live traffic, not a synthetic estimate, so it accounts for the real variance in your users' inputs.

### Prompt Attribution *(Roadmap — Pond integration)*

Joins `llm_sessions` data with downstream business events in Pond (Stripe charges, product analytics, user retention signals) so you can correlate prompt versions directly with business outcomes — which prompt version drove higher conversion, lower support ticket volume, or better retention. Surfaces in the rollout dashboard alongside the standard latency and quality metrics, so the decision to promote or roll back a canary is informed by revenue impact, not just token counts. No other LLM gateway can offer this because none have the warehouse integration layer.

### Semantic Response Caching

Opt-in per-project caching of LLM responses. Controlled by `GATEWAY_CACHE_ENABLED` and `GATEWAY_CACHE_URL`.

The gateway implements a two-layer cache:

| Layer | Scope | How it works |
|---|---|---|
| **L1 — In-process LRU** | Per-instance (512 entries) | SHA-256 exact-match hash of the full request. Sub-microsecond lookups, no network round-trip. |
| **L2 — External semcache** | Shared across instances | Same SHA-256 hash sent to the configured `GATEWAY_CACHE_URL` service via HTTP. Provides cross-replica cache sharing. |

A request is eligible for caching when all of the following are true:
- `stream` is not set (or `false`)
- `temperature` is `0` or absent
- No tool/function definitions are present

Requests that don't meet these criteria are skipped (the `x-reiver-cache: skip` response header indicates this).

Cache entries are stored with a configurable TTL (`GATEWAY_CACHE_TTL_SECONDS`, default 24 hours).

### Input/Output Guardrails

Per-project content safety controls that are entirely **off by default** — each check activates only when the corresponding field is filled in via the settings UI (`PUT /llm/settings`). Checks compose independently; you can enable only what you need.

**Input guardrails** (run before the provider call):

| Setting | Default | Description |
|---|---|---|
| `blocked_input_topics` | `[]` | Keyword/phrase blocklist. Requests containing any listed phrase are rejected with HTTP 400. Case-insensitive. |
| `max_prompt_tokens` | `null` | Hard estimated token cap (characters ÷ 4). Requests exceeding the cap are rejected before any provider is called. |
| `pii_block_on_detect` | `false` | When `true`, PII found in the prompt rejects the request instead of redacting it. Requires the project-level PII masking setting to be enabled. |

**Output guardrails** (run after the provider responds, non-streaming):

| Setting | Default | Description |
|---|---|---|
| `mask_output_pii` | `false` | Mask PII in the response content and thinking/introspection blocks before returning to the client. |
| `blocked_output_topics` | `[]` | Keyword/phrase blocklist applied to the response. Blocked responses return HTTP 400. |
| `min_quality_score` | `null` | When set (0.0–1.0), the LLM-as-judge evaluator runs in the background after the response is sent. Scores (relevance, coherence, helpfulness, average) are persisted to the `llm_evaluation_scores` table and are queryable via the scores API. **The response is never delayed by this check.** |

**Violations** return HTTP 400 with body `{"error": "guardrail_violation", "message": "<detail>"}` and the `x-guardrail-rule` header identifying the triggered rule (e.g. `blocked_input_topic`, `pii_blocked`, `token_limit`, `blocked_output_topic`).

**Known limitations:**

1. **Streaming PII masking is best-effort.** PII split across SSE chunk boundaries may not be caught. For guaranteed redaction, use non-streaming mode with `mask_output_pii` enabled.
2. **Quality scoring is unavailable for streaming requests.** The LLM-as-judge evaluator requires the complete response text; it is silently skipped when `stream: true`. Use non-streaming mode if quality scoring is required.

### Prompt Contracts

Constrain what goes _into_ and what comes _out of_ a managed prompt version. Both features are fully optional and require no changes to existing prompt configurations.

#### Variable Schema Validation

Each variable slot in a prompt version can carry optional constraint fields that are validated against the incoming request values before the LLM is called. Validation errors return HTTP 422 with body `{"error": "prompt_variable_validation", ...}` and header `x-invalid-variable: <name>`.

| Field | Applies to | Description |
|---|---|---|
| `values` | `type: "enum"` (JSON key; alias `var_type`) | Allowed values (case-insensitive). Rejected if the supplied value is not in the list. |
| `max_chars` | `type: "string"` | Maximum character count. Rejected if the string is longer. |
| `min` / `max` | `type: "number"` | Inclusive numeric bounds. |
| `default` | any | Value injected when the variable is absent and not `required`. |
| `required: true` | any | Returns a validation error if the variable is absent and has no `default`. |

Variable definitions without these fields continue to work unchanged — the feature is strictly additive.

#### Output Contract Enforcement

When a prompt version has a `response_format` JSON schema set, the gateway validates the LLM response against that schema after the provider call (non-streaming only). The failure behaviour is configured via `output_failure_action` in the prompt version's `parameters` field:

| Value | Behaviour |
|---|---|
| `"error"` (default) | Return HTTP 422 `{"error": "output_contract_violation", ...}` |
| `"retry"` | Re-execute the request once. Return HTTP 422 if the retry also fails. |
| `"retry_then_passthrough"` | Re-execute once. If the retry still fails, return the original response with the `x-output-contract-violation: true` header. |
| `"log_only"` | Log the violation and return the response unmodified. |

**Known limitation:** Output contract validation is skipped for streaming requests (`stream: true`). The full response body is required for JSON schema validation. Use non-streaming mode when output enforcement is critical.

### OpenClaw Integration

Flow works as a drop-in LLM provider for [OpenClaw](https://github.com/openclaw/openclaw), the open-source AI agent assistant. Because Flow exposes an OpenAI-compatible `/v1/chat/completions` endpoint, OpenClaw can route all agent traffic through the gateway with no code changes.

**Quick setup (plugin):**

```bash
openclaw plugins install openclaw-flow
```

Then add your API key in the OpenClaw settings UI or config file. The plugin registers Flow as a provider and populates the model list automatically.

**Manual setup** (add to `openclaw.json`):

```json5
{
  models: {
    mode: "merge",
    providers: {
      flow: {
        baseUrl: "https://reiver.ai/api/gateway/v1",
        apiKey: "${FLOW_API_KEY}",
        api: "openai-completions",
        models: [
          { id: "auto", name: "Auto (best available)", reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 16384 },
          { id: "gpt-4o", name: "GPT-4o", reasoning: false, input: ["text", "image"], contextWindow: 128000, maxTokens: 16384 },
          { id: "claude-sonnet-4-5", name: "Claude Sonnet 4.5", reasoning: true, input: ["text", "image"], contextWindow: 200000, maxTokens: 8192 },
          { id: "gemini-2.5-pro", name: "Gemini 2.5 Pro", reasoning: true, input: ["text", "image"], contextWindow: 1000000, maxTokens: 65536 }
        ]
      }
    }
  }
}
```

#### Agent Cost Budgets

OpenClaw agents run autonomously — cron jobs, inbox monitoring, background tasks — with no human in the loop. A runaway agent loop can burn through hundreds of dollars overnight. Flow's `session_budget_usd` feature caps per-session LLM spend in real time. OpenClaw passes `x-reiver-session-id` per agent task and the gateway rejects requests that would exceed the budget before they reach any provider.

#### Agent-Aware Prompt Management

OpenClaw skills are defined as Markdown + YAML configs. Flow's prompt management can serve as a central prompt registry for those skills: an OpenClaw skill sends `prompt_config: "code-reviewer"` in its request body, Flow injects the managed system prompt with variables, and you A/B test or canary deploy prompt improvements from the Flow dashboard without touching OpenClaw config files. Non-technical users iterate prompts in a web UI instead of editing YAML.

#### Agent Observability

Every request through the gateway emits a full span to ClickHouse — token counts, cost, latency, provider, model, prompt version, and rollout metadata. For OpenClaw deployments this creates an agent activity audit trail: which agent made how many requests, what it spent, which models it used, and what quality scores it received. Queryable via the `llm_sessions` and `llm_metrics` APIs.

#### Guardrails for Autonomous Agents

An autonomous agent that can send emails, run shell commands, and browse the web needs safety rails. Flow's guardrails apply transparently to all OpenClaw traffic:

- **Input guardrails** — block the agent from asking the LLM to do dangerous things (keyword blocklist)
- **Output guardrails** — block the LLM from returning dangerous instructions (blocked topics)
- **Token limits** — prevent massive context windows that cost $5 per request
- **PII masking** — the agent has access to files, inbox, and calendar; the gateway redacts sensitive data before it reaches any provider

For enterprise OpenClaw deployments these are compliance requirements, not optional features.

#### Multi-Agent Routing

OpenClaw supports multiple concurrent agents. Flow's OpenRouter-style routing can intelligently route different agent types to different models — code review to Claude (better at code), email drafting to GPT-4o (better at prose), quick classification to a cheap/fast model. Per-request `models` arrays and provider preferences give callers fine-grained control over model selection and fallback behavior.

#### Response Caching

Autonomous agents often make near-identical requests. A cron job checking system health every 5 minutes sends nearly the same prompt each time. Flow's response cache returns instant responses at zero inference cost for these repetitive requests, translating directly into cost savings.

## API

### Gateway

```
POST /api/gateway/v1/chat/completions   OpenAI-compatible chat completion
GET  /api/gateway/v1/models             List supported model prefixes
```

Authentication: `X-Project-Id` and `X-User-Id` headers set by the upstream `website` proxy. Direct calls must originate from a trusted proxy CIDR (`TRUSTED_PROXY_CIDRS`).

### Response Headers

| Header | Description |
|---|---|
| `x-reiver-provider` | Provider that served the response |
| `x-reiver-model-used` | Actual model used (may differ from request after fallback) |
| `x-reiver-original-model` | Model from the original request (set when fallback occurred) |
| `x-reiver-fallback-used` | `"true"` if a fallback provider was used |
| `x-reiver-retry-count` | Number of retries before success |
| `x-reiver-cache` | `"hit"`, `"miss"`, or `"skip"` |
| `x-request-id` | Unique request ID for tracing |

### Management API

All endpoints are under `/api/llm/` (requires JWT auth from `website`):

| Path | Description |
|---|---|
| `/llm/settings` | Per-project gateway settings (introspection, fallback, rate limits) |
| `/llm/playground` | Prompt playground — run and compare |
| `/llm/prompts` | Prompt configs and version management |
| `/llm/sessions` | LLM session history and spans |
| `/llm/scores` | Evaluation score submission and retrieval |
| `/llm/metrics` | Aggregated quality metrics |
| `/llm/pricing` | Token cost lookup |
| `/llm/integrations` | Provider API key management |
| `/llm/search` | Semantic search over session history |

## Configuration

All configuration is via environment variables. A `.env` file is loaded automatically in development.

### Required

| Variable | Description |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `CLICKHOUSE_URL` | ClickHouse HTTP endpoint. Flow and website must use the same instance (website runs migrations; Flow writes spans). When using Flow's docker-compose, start the shared infra first: `docker-compose -f docker-compose.yml up -d` from repo root so ClickHouse is on port 8123; Flow then connects via `host.docker.internal:8123`. |
| `KAFKA_HOSTS` | Kafka / Redpanda broker list |
| `JWT_SECRET` | Secret for JWT verification (must match `website`) |
| `ENCRYPTION_KEY` | AES key for encrypting stored provider API keys |

### Gateway Routing

| Variable | Default | Description |
|---|---|---|
| `GATEWAY_FALLBACK_ENABLED` | `true` | Enable provider failover |
| `GATEWAY_MAX_RETRIES` | `2` | Max retry attempts before falling back |
| `GATEWAY_INITIAL_RETRY_DELAY_MS` | `500` | Base delay for exponential backoff |
| `GATEWAY_MAX_RETRY_DELAY_MS` | `10000` | Cap on retry delay |
| `GATEWAY_TIMEOUT_SECONDS` | `120` | Default provider timeout |
| `GATEWAY_TIMEOUT_OPENAI_SECONDS` | `120` | OpenAI-specific timeout |
| `GATEWAY_TIMEOUT_ANTHROPIC_SECONDS` | `120` | Anthropic-specific timeout |
| `GATEWAY_TIMEOUT_GOOGLE_SECONDS` | `120` | Google-specific timeout |
| `GATEWAY_TIMEOUT_BEDROCK_SECONDS` | `180` | Bedrock-specific timeout (higher for large models) |
| `GATEWAY_TIMEOUT_THETA_SECONDS` | `120` | Theta EdgeCloud-specific timeout |
| ~~`GATEWAY_THETA_BASE_URL`~~ | — | Deprecated. The on-demand API URL is hardcoded to `https://ondemand.thetaedgecloud.com`. |
| `TRUSTED_PROXY_CIDRS` | `[]` | CIDRs allowed to set `X-User-Id` / `X-Project-Id` headers |

### Caching

| Variable | Default | Description |
|---|---|---|
| `GATEWAY_CACHE_ENABLED` | `false` | Enable semantic response cache |
| `GATEWAY_CACHE_URL` | `http://localhost:8080` | Vector cache service URL |
| `GATEWAY_CACHE_TTL_SECONDS` | `86400` | Cache entry TTL |

### Playground

| Variable | Default | Description |
|---|---|---|
| `PLAYGROUND_EVALUATION_MODEL` | `gpt-4o-mini` | Model used for LLM-as-judge auto-evaluation |

### Observability

| Variable | Default | Description |
|---|---|---|
| `GATEWAY_LOG_CONTENT` | `false` | Log full request/response content (disable in production) |
| `KAFKA_LLM_CHUNKS_TOPIC` | `reiver.llm.chunks` | Topic for LLM span events |
| `KAFKA_SPANS_TOPIC` | `reiver.spans` | Topic for general spans |
| `OTLP_INGEST_URL` | *(none)* | Base URL for OTLP trace export (e.g. `https://website-host/api/watch/ingest`). When set, one span per chat completion is POSTed to `{url}/v1/traces` with the request's Bearer token so traces appear in the observability UI under the correct project. Omit or leave empty to disable. |

### Infrastructure

| Variable | Default | Description |
|---|---|---|
| `CLICKHOUSE_KAFKA_HOSTS` | `redpanda:9092` | Kafka hosts used by ClickHouse consumers |
| `REDIS_POOL_MAX_SIZE` | `50` | Redis connection pool size |
| `CORS_ALLOWED_ORIGINS` | `*` (dev) / `[]` (prod) | CORS origin whitelist |

### Testing without real API keys

To run the gateway and Playground (or the example app) without paying for AI requests, use the **gateway mock** so Flow calls local mock servers instead of OpenAI, Anthropic, or Google:

1. Start the mock server (in one terminal): from repo root, `cd scripts/gateway-mock`, `pip install -r requirements.txt`, then `python server.py`. It listens on ports 8090 (OpenAI), 8091 (Anthropic), 8092 (Google).
2. Start the stack with the mock base URLs set: `export GATEWAY_OPENAI_BASE_URL=http://127.0.0.1:8090`, `export GATEWAY_ANTHROPIC_BASE_URL=http://127.0.0.1:8091`, `export GATEWAY_GOOGLE_BASE_URL=http://127.0.0.1:8092/v1beta`, then run your usual dev command (e.g. `make dev`).
3. In the project (Prompt Hub > Integrations), you can use dummy keys (e.g. `sk-test-openai`); the mock does not validate them.

See `scripts/gateway-mock/README.md` for details and an optional `.env.mock` setup.

For an end-to-end client that calls the gateway via the website (chat, streaming, sessions, cache), see [examples/flow_gateway_client](../examples/flow_gateway_client).

## Binary Modes

```
reiver-flow --mode all      # HTTP API server + background workers (default)
reiver-flow --mode api      # HTTP API only
reiver-flow --mode workers  # Background workers only (rollout monitor, latency sync)
```

Listens on `0.0.0.0:3001`. Handles `SIGINT` and `SIGTERM` with graceful shutdown (30 s drain for workers).

Health endpoints:
- `GET /health` — liveness probe (always 200 if the process is running)
- `GET /ready` — readiness probe (checks Postgres, Redis, ClickHouse connectivity; returns 503 with a failure detail JSON on any miss)

## Startup Ordering

`flow` depends on:
1. **PostgreSQL** — connection pool and migration guard (validates that `website`-managed tables exist at startup)
2. **Redis** — prompt resolution cache, latency sync, semantic cache
3. **ClickHouse** — observability span writes
4. **`website` migrations** — `flow` will refuse to start if required tables (`project_settings`, `llm_sessions_metadata`, etc.) are absent

In production (Nomad), `website` should register as healthy before `flow` starts. The readiness probe at `/ready` can be used to gate traffic until all dependencies are available.

## Prompt Hub — Collaboration Features (Roadmap)

Ideas for collaboration features that would make the Prompt Hub (UI over Flow’s prompt configs, versions, and rollouts) more team-friendly:

### Attribution & transparency
- **Show who created** each prompt version (and when) in the Prompts Index / Show and in version history. The schema already has `created_by` on `llm_prompt_versions`; surface it in the UI.
- Optionally show **last modified by** and **rollout started by** so every change is attributable.

### Comments & discussion
- **Comments on a prompt config or on a specific version** (e.g. “Why we changed this”, “Revert to v3”).
- **@mentions** of org members to notify them.
- Requires a new table (e.g. `prompt_comments`: config_id, version_id nullable, user_id, body, created_at).

### Review / approval before production
- **Draft vs ready**: Versions start as draft; someone marks “ready for rollout”.
- **Approval for rollout**: Require one (or N) approvers before a rollout can start; store approvals in `llm_rollouts` or a small `rollout_approvals` table.
- In the UI: “Request rollout” → approvers get a task/list; “Approve” / “Reject” with optional comment. Fits enterprises and reduces “who changed prod?” risk.

### Activity feed
- Per prompt config (or per project): “Version 3 created by Alice”, “Rollout to v3 started by Bob”, “Rollout completed”, “Comment by Carol on v2”.
- One `prompt_activity` (or generic `activity`) table with kind, config_id, version_id, rollout_id, user_id, payload, created_at. Enables “what changed recently?” without digging through versions.

### Prompt-level roles (optional)
- If project members exist: **prompt-level permissions** (e.g. “editors” vs “viewers” for a config) so only some people can create versions or start rollouts. More complex; only worth it if multi-team projects need finer control than project-level roles.

### Sharing & discovery
- **Prompt library / templates**: Mark configs (or versions) as “template” or “shared”; allow copying into another project or “use as starting point” in the same project.
- **Tags or categories** (e.g. “support”, “summarization”) to make prompts discoverable for teams.

### Real-time or async co-editing
- **Real-time**: Multiple people in the same prompt editor (CRDT/OT or “user X is editing” + conflict handling). High effort.
- **Async**: Stronger **branch/draft** model — “Create draft from v2”, edit, then “Propose as new version” with a short description; others review in comments or approval. Reuses existing versioning; no real-time needed.

**Suggested implementation order:** (1) Surface `created_by` (and optionally “started by” on rollouts) in the UI. (2) Comments on prompts/versions and a simple activity feed. (3) Approval workflow for rollouts (draft → ready → approve → start). (4) Prompt library/templates and tags; later, prompt-level roles if needed.

## Canary deployment systems — typical configuration

In general, canary (gradual rollout) systems allow users to configure the following.

### Stages and traffic shape

- **Percentages** — e.g. 5% → 20% → 50% → 100%, with each step lasting a fixed duration or until the operator advances manually.
- **Concrete request numbers** — e.g. "send 1000 requests to canary, then 10k", or "N requests per minute to canary". Common when rollouts are defined by request volume rather than only percentage.
- **Time-based steps** — e.g. "5% for 15 minutes, then 25% for 30 minutes", often with auto-advance to the next stage or manual promotion.

### Rollback conditions

- **Error rate** — e.g. "roll back if 5xx rate > 1%" or "if error rate is 2× baseline".
- **Latency** — e.g. "roll back if p99 > 500ms" or "if latency is 1.5× baseline".
- **Success rate** — e.g. "roll back if success rate < 99%".
- **Custom metrics** — e.g. "roll back if business metric X drops by Y%".
- **Manual** — "roll back" / "abort" button; some systems also auto-rollback if the operator does not advance within a time window.

Stages (percentages and/or request counts), timing, and rollback conditions (metric thresholds and/or manual) are the standard configuration surface for canary deployments.
