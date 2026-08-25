# Reiver

Before integrating an application, read `agent://onboarding`, honour the selected Flow, Watch, or Complete track, and use its track-specific pass/fail report. This overview describes the platform; it is not a substitute for proving the selected track.

Reiver is a full-stack observability platform with three products:

- **Flow** — Prompt hub and LLM gateway. Version, test, and roll out system prompts. Route LLM requests to OpenAI, Anthropic, Google Gemini, and AWS Bedrock through a single OpenAI-compatible API. Automatic failover, semantic caching, guardrails, and cost tracking.
- **Watch** — OpenTelemetry-native application performance monitoring. Distributed tracing, error tracking, log aggregation, real-time metrics, continuous profiling, dashboards, and alerts. Any standard OTel SDK or Collector works out of the box — for infrastructure monitoring (databases, Kubernetes, message queues, hosts), use the open-source OpenTelemetry Collector receivers. Native cloud integrations with AWS, Azure, GCP, and Oracle Cloud.
- **Billing** — Usage tracking, per-project breakdowns, forecasts, and budget management.

Flow and Watch can be adopted independently. When the owner selects Complete Reiver, they become one operating loop: confirmed business context gives session labels meaning; Flow controls models, prompts and guardrails; Watch records gateway and application evidence; sessions and labels expose user and business outcomes; MCP lets you inspect that evidence and improve the system. Explain that loop in terms of the application you inspected rather than repeating generic product claims.

## Platform Operations vs Application Integration

You (the agent) manage the platform through the MCP tools provided by this server — querying data, managing dashboards, alerts, prompts, and billing. REST API endpoints and SDKs described in this documentation are for the user's application to call. They require an SDK key (`dh_...`) and do not accept the MCP agent token. When the user asks you to help integrate their application, write code that their app will run using the documented REST endpoints and SDKs. Do not substitute the MCP token for an SDK key.

The same SDK key value currently authenticates both Flow and Watch. Bind it to two distinct application secrets, `REIVER_FLOW_API_KEY` and `REIVER_WATCH_API_KEY`, so each integration has an explicit boundary and can be split later without changing application code. Provider API keys stay in Reiver and never belong in the app or agent environment.

## Common Workflows

### Investigating an issue

1. List recent exceptions or search logs for the error
2. Get the trace associated with the error to see the full request path
3. Search correlated logs using the trace ID
4. Check if an alert rule already covers this condition

### Creating and deploying a prompt version

1. Create the version — provide `system_prompt` with the complete prompt text, `model`, `temperature`, and `commit_message`
2. Read the created version back to confirm the content is correct
3. Deploy — this initiates a progressive rollout that sends live traffic to the new version; do this only when the owner's assignment explicitly delegates that authority
4. Monitor rollout metrics to decide whether to promote or rollback

### Setting up monitoring

1. Create a dashboard (or use a template)
2. Add widgets with queries against spans, logs, or metrics data
3. Create alert rules for conditions that need notification
4. Configure notification channels (Slack, PagerDuty, Discord, Teams, webhooks)

### Connecting an application to Flow

Applications connect to the LLM gateway by setting the base URL to `https://reiver.ai/api/gateway/v1` and using `REIVER_FLOW_API_KEY` for authentication. Any OpenAI-compatible client library works without modification.

### Connecting an application to Watch

Applications send OpenTelemetry data (traces, logs, metrics) to `https://reiver.ai/api/watch/ingest` with `REIVER_WATCH_API_KEY` in the `Authorization: Bearer` header. Each signal needs its own initialized pipeline; a trace exporter does not export logs or metrics. Standard OTel SDKs and Collectors are supported.
