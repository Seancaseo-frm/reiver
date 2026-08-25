# Reiver application onboarding contract

Read this resource before changing an application. Then read only the resources required by the selected track: `agent://flow/getting-started`, `agent://watch/overview`, and the shared `agent://flow/session-telemetry` contract when sessions or cross-product identity are in scope.

## Goal

Integrate the user's existing application with the smallest known-good baseline for the track they selected and return track-specific evidence. Do not force a Flow-only customer to install Watch or a Watch-only customer to add an LLM provider.

Then, when the user's assignment and token scopes authorise it, configure the Reiver capabilities that are useful for this specific application and verify those changes. Do not add features merely to make Reiver appear useful.

## Select the onboarding track

Honour an explicitly selected track. If the assignment makes the desired outcome clear, state the inferred track and continue. If the choice materially changes application architecture and is genuinely unclear, ask one focused question.

| Track | Scope | Required resource |
|---|---|---|
| Flow + Prompt Hub | Provider, gateway, routing, optional managed prompts and LLM sessions | `agent://flow/getting-started` |
| Watch | Application traces, structured logs, metrics, and service correlation | `agent://watch/overview` |
| Complete Reiver | Flow + Watch + shared identity + MCP verification and business activation | Both resources plus `agent://flow/session-telemetry` |

Flow and Watch are independently valid outcomes. The Complete Reiver acceptance table is deliberately larger and must not be applied to a standalone track.

## Understand the business before configuring it

First use MCP `get` with `resource: "gateway_settings"` and read `agent_soul` when the token has `llm:read`. This is Reiver's persistent project context, not transient coding-agent memory. Reuse it across coding-agent sessions and tokens. Then inspect the repository, README, user-facing screens, routes, existing prompts, data model and tests. Reconcile the stored context with the current application before asking the owner questions.

Before creating session labels, profiles, prompts, guardrails, dashboards or alerts, present a short **My understanding** summary covering:

- what the application does and who its primary users are;
- the user's main job and the business outcome the application is intended to produce;
- what a successful session looks like;
- important failure, safety, quality and commercial outcomes;
- the real Session and Identity Contract: session unit, start, terminal events, idle fallback, stable pseudonymous user identifier, anonymous policy and tenant scoping;
- sensitive-data or regulatory constraints visible in the application.

Ask the owner only to correct material conflicts or supply material gaps. Do not make them restate facts already present in `agent_soul` or clear from the application. If the stored context remains consistent and nothing material is uncertain, state that it is being reused and continue within the authority already granted for this task.

Session labels turn technical conversations into business-readable outcomes. Propose a small initial taxonomy, normally 5-10 non-overlapping labels. For every label provide its name, precise classification definition, why it matters to the business and one synthetic example that should receive it. Avoid vague labels such as `good` or `bad` when a specific outcome such as `learner-still-stuck`, `qualified-sales-lead` or `handoff-required` would support a real decision.

Use the confirmed context to explain the combined operating loop: Flow controls providers, prompts and guardrails; Watch captures gateway and application evidence; sessions and labels translate that evidence into user and business outcomes; MCP lets the agent inspect the result and improve the system. Sell the value through this causal evidence, not unsupported marketing claims.

## Respect delegated autonomy

Reiver has no separate autonomy-mode setting. The MCP token scopes are the hard technical boundary; the owner's assignment defines the behavioural authority inside that boundary.

- With `llm:read` and `observability:read`, inspect, verify and recommend only.
- With `llm:write` and/or `observability:write`, write actions are available but are not automatically authorised merely because the token permits them.
- An instruction such as "integrate and configure Reiver autonomously" is explicit authority to perform relevant in-scope setup, testing, prompt operations and verification without repeatedly asking for approval.

Infer the intended authority from the owner's assignment. If the first write is material and that authority is unclear, ask one focused question before writing. Once authority is clear, do not ask again for every in-scope prompt version, test, rollout, label, profile, guardrail, dashboard or alert.

Unless the owner explicitly includes them, autonomous onboarding does not authorise deleting existing resources, changing provider credentials, increasing budgets, weakening existing safety controls or modifying unrelated production resources. Read current state before every update, preserve unrelated configuration, keep an audit-friendly change list and identify a rollback path.

## Credential boundaries

| Role | Expected binding | Boundary |
|---|---|---|
| Provider API key | none in the app | Stored in Reiver only; never request, print or copy it |
| Flow SDK key | `REIVER_FLOW_API_KEY` | Application runtime |
| Watch SDK key | `REIVER_WATCH_API_KEY` | Application runtime; currently the same SDK key value as Flow |
| MCP agent token | `REIVER_AGENT_TOKEN` | Coding-agent environment only; never deploy with the app |

Do not ask the user to duplicate the SDK key itself. Ask them to bind its same value to two distinct application secrets so Flow and Watch cannot accidentally read a generic or incorrectly scoped variable.

Write application code against the two SDK environment-variable names; do not ask the user to paste either value into chat or a prompt. If you must launch the application to test it, use the user's secret manager to inject the bindings into that disposable process. Otherwise, return the code changes and let the user deploy them into the already configured test runtime.

## Required workflow

### Shared start

1. Read `gateway_settings.agent_soul`, inspect the application, state the selected track, and inspect only the relevant language, framework, deployment, LLM-client, logging, and OpenTelemetry paths.
2. Present the **My understanding** summary at the level needed by that track, resolve only material gaps, and state the delegated autonomy you will use.
3. When sessions, labels, per-user analysis, or Complete Reiver are in scope, read `agent://flow/session-telemetry` and confirm the Session and Identity Contract before editing correlation code. Save it to Agent Soul when authorised.

### Flow + Prompt Hub branch

1. Establish one provider/model path already used by the application. Do not introduce a second provider, auto-routing, fallbacks, managed prompt overrides, or a batch model during the baseline.
2. Route existing LLM calls through `https://reiver.ai/api/gateway/v1` with `REIVER_FLOW_API_KEY`. Preserve the explicit model for the first run.
3. When users/sessions are in scope, send the confirmed stable `x-reiver-session-id` and `x-reiver-user-id`, set the `user` field to the same user ID, and end the session at the confirmed terminal events.
4. Add and prove a managed prompt only when Prompt Hub is part of the selected scope.
5. Run the Flow acceptance checks. Do not require Watch evidence.

### Watch branch

1. Export traces, logs, and metrics over OTLP HTTP to `https://reiver.ai/api/watch/ingest` with `REIVER_WATCH_API_KEY`.
2. Initialise all three signal pipelines. Endpoint variables alone do not install instrumentation or create providers, processors, readers, handlers, or instruments.
3. Use one stable `service.name`. When session/user correlation is in scope, add the confirmed `gen_ai.conversation.id` and `gen_ai.user.id` to related spans and logs, and also emit `gen_ai.session.id` during Reiver's compatibility period.
4. Run the Watch acceptance checks. Do not require a provider or Flow request.

### Complete Reiver branch

Run both branches with the same confirmed user/session values, explicitly end a successful session, and verify Flow and Watch independently before comparing their identifiers. Run the Complete Reiver acceptance checks below.

For every track, report blockers and missing scopes precisely; do not infer success. Continue to business-aware activation only after the selected baseline passes and only when the owner's authority and token scopes permit it.

## Signal requirements

- Traces: tracer provider, instrumentation/manual spans, span processor, OTLP trace exporter.
- Logs: logger provider or logging bridge, log record processor, OTLP log exporter. Console/stdout logging is not automatically exported.
- Metrics: meter provider, periodic metric reader, OTLP metric exporter, and at least one runtime or application instrument that produces data.
- Correlation: emit the diagnostic log while a test span is active so the log receives its trace and span context.

Preserve an existing global provider when possible. Do not register multiple competing global providers merely to add Reiver.

## Acceptance report

Return the pass/fail table for the selected track with concrete, non-secret evidence.

### Flow + Prompt Hub

| Check | Pass evidence |
|---|---|
| Provider | Selected provider connection test passed |
| Gateway | A real application request returned `200` |
| Routing | Actual `x-reiver-provider`, `x-reiver-model-used`, and `x-request-id` recorded |
| Identity | If sessions/users are in scope, the contract and matching request identifiers are proven |
| Prompt Hub | If selected, a managed prompt version was read back, tested, and used |
| Secret hygiene | No provider key, SDK key, or agent token in source, logs, tool output, or report |

### Watch

| Check | Pass evidence |
|---|---|
| Trace | Real application trace found under the expected `service.name` |
| Log | Known structured log found and correlated to the trace or conversation |
| Metric | Known application or runtime metric name and data point found |
| Service | Trace, log, and metric use the intended service identity |
| Verification | Evidence retrieved in the UI or through MCP with `observability:read` |
| Secret hygiene | No SDK key or agent token in source, telemetry, logs, tool output, or report |

### Complete Reiver

| Check | Pass evidence |
|---|---|
| Business context | **My understanding** summary confirmed or supported by clear application evidence |
| Delegated authority | Read/write scopes and the owner's behavioural boundaries stated without exposing a token |
| MCP resources | This resource was read successfully |
| Provider | Reiver provider connection test passed |
| Gateway | A real application request returned `200` |
| Routing | Actual `x-reiver-provider`, `x-reiver-model-used`, and `x-request-id` recorded |
| Trace | Application trace found under the expected `service.name` |
| Log | Known structured application log found and correlated to the test trace/session |
| Metric | Known application or runtime metric name and data point found |
| Session contract | Session unit, start/end events, idle fallback, stable pseudonymous user, anonymous policy and tenant scoping confirmed and saved in Agent Soul |
| Identity | Stable user and conversation/session identifiers found on request and telemetry |
| Session end | End call returned `202`; session becomes queryable |
| Continuity | A second session uses a new session ID and retains the same test user ID |
| Secret hygiene | No provider key, SDK key or agent token in source, logs, command output or report |

Use MCP `list` with `resource: "metric_names"` before querying metrics. Use `trace_attribute_keys` and `log_attribute_keys` to discover what arrived. Use `search` with `source: "logs"` and attribute filters for the test conversation. Never include a credential value in tool arguments or results.

If the token lacks `observability:read` or `llm:read`, state the missing scope and leave the affected row as unverified.

Do not mark a standalone Flow integration incomplete because Watch rows are absent. Do not mark a standalone Watch integration incomplete because provider, gateway, or Flow-session rows are absent.

## Business-aware activation

After the technical baseline is green:

1. Read the current gateway settings, prompts, labels, profiles, dashboards and alerts before proposing or making changes.
2. Map each proposed Reiver capability to a confirmed business outcome or risk. Configure only what is relevant.
3. When authorised, preserve the confirmed project description, technical context and important business outcomes in the existing gateway `agent_soul` settings so later Reiver agents can reuse them. Merge with rather than erase useful existing context.
4. Define the confirmed session-label taxonomy through `gateway update_settings` with `session_labels`, then create useful label-based or operational session profiles.
5. When prompt work is relevant and authorised, create a complete prompt version, read it back, test it, and roll it out only within the delegated authority. Monitor the rollout and promote or roll back from evidence.
6. Configure only relevant guardrails. Use synthetic inputs to prove the expected behaviour and do not weaken an existing control without explicit authority.
7. Create dashboards or alerts only for decisions the owner needs to make, not as decorative onboarding artefacts.
8. Send several synthetic sessions representing important success and failure outcomes, explicitly end them, and use MCP to verify their requests, telemetry, labels and profiles.
9. Return an activation report listing every resource created or changed, the evidence that it works, anything deliberately left unchanged and the rollback path.

If the token is read-only, return the same work as a proposed configuration instead of attempting writes. If a feature is unnecessary for this application, say so rather than forcing it into the setup.

## Model guidance

Use a standard synchronous model for an interactive baseline. For an existing Anthropic integration, `claude-sonnet-5` is the balanced default; `claude-fable-5` and `claude-opus-5` trade more cost/latency for capability. Fast mode is limited to supported Opus 5 and Opus 4.8 aliases, requires Anthropic preview access and costs more. `:batch` variants are asynchronous provider jobs and must not be used for an interactive Flow smoke test.

An integration connection test proves key authentication only. Prove the exact model with the Playground or a gateway response. For Sonnet 5, omit temperature and legacy manual thinking budgets; the provider manages sampling and adaptive thinking, and Reiver strips unsupported prompt-version sampling values.

The model catalogue is routing and pricing metadata, not proof that a provider key can call every historical entry. Prove the chosen model in the Playground or with an inspected gateway response.

## Fit evaluation

Explain Reiver's value as a connected operating loop for the application you inspected, not as a list of isolated features:

- provider portability and controlled fallback;
- central prompt versions and rollouts;
- correlated LLM/application traces, logs and metrics;
- per-user/session cost, quality and latency;
- agent-operated dashboards, alerts and investigations through MCP.

Connect each capability to the confirmed user or business outcome it helps measure, protect or improve. State what evidence is now visible, what decision that evidence supports and what the agent can safely change in response.

Also state when those capabilities are unnecessary. A single low-risk prototype with no portability, prompt-control or observability requirement may reasonably remain direct to its provider. Do not manufacture a positive recommendation; base it on the application's needs and the verified result.
