# Reiver application onboarding

Read this resource before changing an application for Reiver. Choose the smallest track that meets the owner's goal, make only authorised changes, and return evidence from a real application path. Flow and Watch are independently valid outcomes.

## Start with context

1. If the token exposes the MCP `get` tool and has `llm:read`, call `get` with `resource: "gateway_settings"` and read `agent_soul`. Treat it as persistent project context, not unquestionable truth.
2. When Flow or Complete Reiver may be selected and the token exposes `list` with `llm:read`, call `list` with `resource: "model_catalog"`. This is the live, project-filtered source of truth for interactive model IDs. Do not guess a model slug or copy one from static documentation. If the catalogue is unavailable, report that evidence gap instead of inventing an ID.
3. Inspect the application's README, user journeys, LLM calls, telemetry setup, data model, deployment path, and relevant tests. Reconcile what exists now with Agent Soul.
4. Select one onboarding track. Honour an explicit choice. If the choice changes the architecture and remains genuinely unclear after inspection, ask one focused question.
5. State a short **My understanding** summary: what the application does, who it serves, the outcome it should produce, important failure or safety conditions, and any privacy constraints.
6. State the MCP scopes available and the authority given by the owner's assignment.

Do not make the owner repeat facts already supported by Agent Soul and the application. Ask only for material gaps or conflicts.

## Choose one track

| Track | Use it for | Definition of done |
|---|---|---|
| Flow + Prompt Hub | Route model calls through Reiver and optionally manage prompts centrally | One real application request succeeds through Flow; the actual provider and model are identifiable; any selected managed prompt is proven |
| Watch | Add application traces, structured logs, and metrics without changing model routing | One known trace, structured log, and metric are queryable under the intended stable service name |
| Complete Reiver | Connect Flow decisions and application behaviour to the same business episode | Both tracks pass; identifiers correlate; the first session ends explicitly; a second session has a new session ID and the same stable pseudonymous user ID |

Do not require Watch for a Flow-only integration. Do not require a provider or Flow request for a Watch-only integration. MCP is optional for application traffic and no track requires MCP write access merely to reach its technical baseline.

## Credential boundaries

| Credential | Where it belongs | Purpose |
|---|---|---|
| Provider key | Inside Reiver | Lets Flow call the model provider |
| SDK key | Application secret store | Authenticates application traffic to Flow and Watch |
| MCP agent token | Coding-agent secret store | Authenticates MCP within its granted scopes |

The same SDK-key value may currently be bound separately as `REIVER_FLOW_API_KEY` and `REIVER_WATCH_API_KEY`. Keep those application bindings distinct. Never substitute `REIVER_AGENT_TOKEN` for an application SDK key.

Never request, print, copy, commit, log, or place any credential in telemetry or a report.

## Authority and safe writes

Token scopes are the hard technical boundary. The owner's assignment is the behavioural boundary inside those scopes.

- Read-only scopes permit inspection and recommendations.
- A write scope makes an operation available; it does not by itself authorise every possible write.
- If the owner explicitly asks the agent to integrate and configure Reiver, perform relevant in-scope setup and verification without repeatedly asking for approval.
- Unless explicitly included, onboarding does not authorise deleting resources, changing provider credentials, increasing budgets, weakening guardrails, or modifying unrelated production systems.
- Read current state immediately before every write. Preserve unrelated settings and record a rollback path.

For gateway settings, send only the top-level fields intentionally being changed. Omitted fields remain unchanged. An explicit `session_labels: []` or `session_profiles: []` clears that collection; never send either accidentally. Read and preserve useful Agent Soul context before updating `agent_soul`.

## Flow + Prompt Hub workflow

1. Read `agent://flow/getting-started`.
2. Inspect the application's existing LLM path, current gateway settings, enabled integrations, and live `model_catalog` before choosing a routing policy.
3. Prefer Reiver-owned routing unless the owner explicitly requires an application-owned model pin:
   - For Reiver-owned routing, the application sends `model: "auto"` and omits request-level `models` and `provider`. Keep the model order and provider policy in Reiver gateway settings.
   - When explicitly authorised to configure routing, set `default_fallback_models` only from IDs in the live `model_catalog`. An empty project list lets Flow derive candidates from enabled integrations. Set `provider_preferences` only when the owner has specified that policy.
   - Do not hardcode catalogue model IDs in application code merely to reproduce Reiver's project routing. If the owner explicitly chooses an application-owned pin, select it from the live catalogue and do not add fallback chains or providers merely for onboarding.
4. Route the application's existing LLM path through `https://reiver.ai/api/gateway/v1` using `REIVER_FLOW_API_KEY`.
5. Add a managed prompt only if Prompt Hub is part of the selected goal. A model override in a managed prompt is Reiver-owned configuration; do not duplicate it in application routing unless that precedence is intentional.
6. When sessions or per-user analysis are required, agree the Session and Identity Contract before adding identifiers.
7. Prove one real application request and record the actual provider, model, fallback state, and request identifier without exposing sensitive content.
8. If the assignment requires exercised failover, use an existing staging or automated fault-injection path and verify the actual fallback response. Never disable a production integration, alter a provider key, or manufacture a production outage merely to prove fallback. If no safe path exists, distinguish configuration verification from unexercised failover in the report.

Configuration, a connection test, or a Playground-only request is not proof that the application path works.

## Watch workflow

1. Read `agent://watch/overview`.
2. Send OTLP HTTP data to `https://reiver.ai/api/watch/ingest` using `REIVER_WATCH_API_KEY`.
3. Initialise three real pipelines:
   - traces: instrumentation, tracer provider, processor, and trace exporter;
   - structured logs: logging source or bridge, logger provider, processor, and log exporter;
   - metrics: instruments, meter provider, periodic reader, and metric exporter.
4. Use one stable `service.name`. Endpoint variables alone do not create instrumentation or pipelines.
5. Generate one known trace, structured log, and metric from a real application path, then find each in Reiver.

Console output alone is not a structured OTLP log pipeline. A trace exporter does not export logs or metrics.

## Complete Reiver and the Session and Identity Contract

Read `agent://flow/session-telemetry` before editing correlation code. Agree:

- the business episode that one session represents;
- its start, successful end, failure and abandonment endings;
- the inactivity fallback;
- the stable pseudonymous user ID and anonymous-user policy;
- tenant scoping and the privacy boundary.

Send the agreed session ID in `x-reiver-session-id`. Send the same stable pseudonymous user value in the request `user` field and `x-reiver-user-id` when recording and sticky rollout behaviour are both required. For Watch correlation, use `gen_ai.session_id` and `gen_ai.user.id`.

Explicitly end a completed episode through the documented Flow end-session endpoint. Treat inactivity detection as fallback protection. A later episode receives a new session ID while the same person retains the same stable pseudonymous user ID within the agreed tenant and privacy boundary.

## Verify through MCP

Use only resources and tools exposed by the token. Missing evidence is a blocker to report, not permission to infer success.

- For Flow, verify the real request, session, provider, model, and managed prompt only when each is in scope.
- For Watch, discover available services and metric names before querying. Find the known trace, structured log, and metric independently.
- For Complete Reiver, compare the agreed identifiers across Flow and Watch and verify session continuity.
- Never claim success from settings, code, or exporter configuration alone.

## Business-aware activation

After the selected technical baseline passes:

1. Read current prompts, gateway settings, labels, profiles, dashboards, and alerts before proposing changes.
2. Map every proposed capability to a confirmed business outcome or risk. Do not create decorative configuration.
3. When authorised, merge durable project context into Agent Soul without erasing useful existing context.
4. Keep an initial label taxonomy small, precise, and non-overlapping. Create profiles, prompts, guardrails, dashboards, or alerts only when they support a real decision.
5. Verify every created or changed resource and include its rollback path.

If the token is read-only, return the same work as a proposed configuration. If Reiver does not fit the application's needs, say so.

## Final report

Return:

- selected track and **My understanding**;
- scopes and behavioural authority used;
- files and Reiver resources changed;
- a pass/fail result for every acceptance check in that track;
- concrete, non-secret evidence;
- blockers, deliberate exclusions, and rollback steps.

Do not mark a partial track as Complete Reiver, and do not hide an unverified row behind a general statement that onboarding succeeded.
