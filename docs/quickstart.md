# Quickstart: choose your Reiver path

Start with the smallest track that matches the outcome you need. Flow and Watch can each be onboarded and verified independently; choose Complete Reiver when you want the combined agent-operated loop.

| Track | Choose it when | Track-specific definition of done | Start |
|---|---|---|---|
| **Flow + Prompt Hub** | You want gateway routing, provider portability, prompt releases, cost, or LLM sessions | Intended provider/model serves a real request; routing headers are recorded; any selected managed prompt is proven | [Flow + Prompt Hub quickstart](/flow/getting-started) |
| **Watch** | You want application observability without changing LLM routing | A real application trace, correlated structured log, and metric are queryable under one service | [Watch quickstart](/watch/) |
| **Complete Reiver** | You want Flow, Watch, business sessions, labels, and autonomous MCP verification together | Every acceptance row on this page passes | Continue below |

The remainder of this page is the **Complete Reiver** track. It gets one existing application through Flow, sends all three OpenTelemetry signals to Watch, correlates a real user session, and gives a coding agent scoped MCP access. It intentionally has a larger definition of done than either standalone track.

The target is not merely an HTTP `200`. A completed **Complete Reiver** integration has evidence for the provider, model, gateway request, application trace, correlated log, metric, session, and user.

## 1. Understand the three credential roles

| Credential role | Where it lives | Who receives it | Recommended binding |
|---|---|---|---|
| Provider API key | Reiver **Prompt Hub → Integrations** | Reiver only | Do not put it in the application or coding agent |
| SDK key | Reiver **Settings → General → SDK keys** | Application runtime | Bind the same value separately as `REIVER_FLOW_API_KEY` and `REIVER_WATCH_API_KEY` |
| Agent token | Reiver **Agents → Tokens** | Coding agent only | `REIVER_AGENT_TOKEN` |

For the shared onboarding SDK key, select `llm:write` and `observability:write`; the UI also selects their matching read scopes. Flow gateway requests require `llm:write`. Do not add project, billing, Herd, or agent-token capabilities to the application key.

Flow and Watch accept the same SDK key value. Use two named secret bindings anyway. The explicit names prevent gateway and telemetry configuration from accidentally reading the wrong generic secret, and they let you split the credentials later without changing application code.

SDK keys and agent tokens currently both use the visible `dh_...` format. The prefix does not make them interchangeable: Reiver records the key type and rejects an SDK key at MCP and an agent token at the application endpoints.

The coding agent should write code that references the SDK secret names; do not paste either value into an agent prompt. If the agent must launch the application for its smoke test, inject the SDK bindings into that disposable test process through the platform's secret manager. Otherwise, let the agent make and test code-level changes, then have the user deploy the test environment with its normal secret bindings.

```bash
# Same Reiver SDK key value, two distinct application secret bindings.
export REIVER_FLOW_API_KEY="dh_..."
export REIVER_WATCH_API_KEY="dh_..."

# Separate scoped credential for the coding agent. Never deploy this with the app.
export REIVER_AGENT_TOKEN="dh_..."
```

::: danger Keep provider keys away from the agent
Add the Anthropic, OpenAI, or other provider key in Reiver's UI. The coding agent needs the SDK key bindings and its agent token; it does not need the provider key.
:::

## 2. Establish one known-good provider path

Start with one provider already used by the application. Add fallback providers only after the baseline is green.

1. Add the provider key in **Prompt Hub → Integrations**.
2. Select **Test** and confirm the provider connection succeeds.
3. In the Playground, select a standard synchronous model and send a short request.

The connection test proves that Reiver can authenticate the stored key; it does not prove inference on a particular model. The Playground request is the model proof.

For an Anthropic onboarding, `claude-sonnet-5` is the sensible baseline: it is the current speed/intelligence model. Use `claude-fable-5` or `claude-opus-5` only when the workload needs their extra capability. Fast mode is limited to supported Opus 5 and Opus 4.8 aliases, costs more and requires Anthropic preview access. Models ending in `:batch` are asynchronous provider jobs, not interactive Flow choices, and are deliberately excluded from Reiver's interactive selectors.

Claude Sonnet 5 manages sampling and adaptive thinking at the provider. Do not add a temperature or a legacy manual thinking budget to the baseline request; Reiver also strips unsupported prompt-version sampling values before calling Anthropic.

The model catalogue is pricing and routing metadata, not proof that a key can call every historical model shown. The Playground test is the proof for the selected model.

## 3. Prove the Flow gateway independently

Run one non-streaming request before modifying the application:

```bash
curl --include https://reiver.ai/api/gateway/v1/chat/completions \
  --header "Authorization: Bearer $REIVER_FLOW_API_KEY" \
  --header "Content-Type: application/json" \
  --header "x-reiver-session-id: onboarding-smoke-1" \
  --header "x-reiver-user-id: onboarding-user-1" \
  --data '{
    "model": "claude-sonnet-5",
    "user": "onboarding-user-1",
    "messages": [{"role": "user", "content": "Reply with: reiver-flow-ok"}]
  }'
```

Record these response values:

- HTTP status is `200`.
- `x-reiver-provider` is the intended provider.
- `x-reiver-model-used` is the model that actually served the request.
- `x-request-id` is present for correlation.

Do not continue on the basis of status alone. Unexpected provider or model headers indicate routing or fallback behaviour that must be understood first.

## 4. Connect the coding agent through MCP

Create an agent token with `project:read`, `llm:read`, and `observability:read`. The five MCP facade tools use `project:read` as their visibility gate, then enforce the relevant Flow or Watch scope for each operation. `project:read` does not permit project changes. Add `llm:write` or `observability:write` only when you want the agent to change Reiver configuration.

There is no separate autonomy-mode selector. The token scopes determine what the agent technically can do; the assignment you give it determines what it is authorised to do.

| Goal | Token scopes | Behaviour to request |
|---|---|---|
| Evaluate and verify | `project:read`, `llm:read`, `observability:read` | Inspect the application and Reiver, run read-only checks, and recommend changes |
| Configure Flow autonomously | `project:read`, `llm:write`, `observability:read` | Create and test relevant prompts, labels, profiles and gateway controls within the assignment |
| Configure the complete platform autonomously | `project:read`, `llm:write`, `observability:write` | Also create and verify relevant dashboards, alerts and Watch configuration |

A write scope makes a tool available; it is not, by itself, an instruction to use every write action. A clear onboarding assignment can authorise the agent once to act autonomously within those scopes. It should then ask only when it needs to exceed the stated boundary.

### Codex

Add to `.codex/config.toml`:

```toml
[mcp_servers.reiver]
url = "https://reiver.ai/mcp"
bearer_token_env_var = "REIVER_AGENT_TOKEN"
```

### Claude Code

Add to the project `.mcp.json`:

```json
{
  "mcpServers": {
    "reiver": {
      "type": "http",
      "url": "https://reiver.ai/mcp",
      "headers": {
        "Authorization": "Bearer ${REIVER_AGENT_TOKEN}"
      }
    }
  }
}
```

### Cursor

Add to `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "reiver": {
      "type": "http",
      "url": "https://reiver.ai/mcp",
      "headers": {
        "Authorization": "Bearer ${env:REIVER_AGENT_TOKEN}"
      }
    }
  }
}
```

Restart the client, then verify its native connection surface:

- **Codex:** run `codex mcp list` or use `/mcp` in the TUI.
- **Claude Code:** run `claude mcp list` and `claude mcp get reiver`, approve the project server if prompted, or use `/mcp`.
- **Cursor:** open **Settings → Tools & MCP** and confirm that `reiver` is enabled.

These checks prove only that the client sees the server. Before changing application code, ask the agent to list Reiver's MCP resources and read `agent://onboarding`. Successful resource content is the authentication and documentation gate.

## 5. Give the agent an evidence-based assignment

Paste this into the coding agent after its MCP token is configured and the application test runtime has the two SDK secret bindings. This version authorises autonomous configuration after the baseline is proven; remove that paragraph when you want a read-only evaluation.

```text
Read the Reiver MCP resource agent://onboarding and the resources it links to.
Read gateway_settings.agent_soul through MCP before asking me about the business;
reuse any stored project context that still matches the current application.
Inspect this application's README, user-facing behaviour, routes, prompts and
data model as well as its language, framework, LLM clients, logging stack,
request lifecycle, and existing OpenTelemetry setup before editing anything.

This is a Complete Reiver onboarding. First present a short "My understanding"
summary covering what the application does, its primary users, intended
business outcome, and important failure or safety outcomes. Include a Session
and Identity Contract stating the meaningful session unit, start event,
successful and other terminal events, idle fallback, stable pseudonymous user
ID source, anonymous-user policy, tenant scoping, and sensitive-data constraints.
Infer as much as possible from the stored Reiver context and current application.
Ask me only to resolve material conflicts or fill material gaps; do not ask me
to repeat context Reiver already holds.

For this onboarding you may act autonomously within the llm and observability
scopes granted to your Reiver token. After the baseline below passes, you may
create and test relevant prompt versions, roll them out and monitor or roll
them back, configure session labels and profiles, configure relevant guardrails,
and create useful dashboards and alerts. Do not repeatedly ask for approval for
those in-scope actions. Do not delete existing resources, change provider
credentials, increase budgets, weaken existing safety controls, or modify
unrelated production resources without asking.

Integrate Reiver with these constraints:
1. Route existing LLM calls through https://reiver.ai/api/gateway/v1 using
   REIVER_FLOW_API_KEY. Preserve the application's current explicit model for
   the first test; do not add auto-routing or fallbacks yet.
2. Export traces, logs, and metrics over OTLP HTTP to
   https://reiver.ai/api/watch/ingest using REIVER_WATCH_API_KEY. Configure and
   initialize each signal pipeline; endpoint environment variables alone are
   not proof that a pipeline exists.
3. Implement the confirmed Session and Identity Contract and merge it into the
   existing gateway_settings.agent_soul so later agent sessions can reuse it.
   Send a stable x-reiver-user-id and x-reiver-session-id on each LLM request,
   and set the OpenAI-compatible user body field to the same user id so current
   per-user gateway analytics are populated. Add gen_ai.conversation.id and
   gen_ai.user.id to related application spans and log records. Also emit the
   Reiver compatibility attribute gen_ai.session.id until every session view
   has migrated to the current conversation attribute.
4. Explicitly end the session with POST
   /api/gateway/v1/sessions/{session_id}/end when the app's session ends.
5. Keep provider keys out of the repository and agent environment. Never
   hardcode, print, or commit any Reiver credential.

Run a smoke test and report evidence for every acceptance criterion in
agent://onboarding. A successful HTTP response alone is not completion. If an
MCP read scope cannot query a signal, report the exact missing scope instead of
claiming success.

Only after every baseline check is green, use the confirmed business context to
propose a small, precise session-label taxonomy. Explain what each label means,
why it matters, and a synthetic session that should receive it. Configure the
approved or delegated labels and other relevant Reiver capabilities, send test
sessions for important success and failure outcomes, and verify the resulting
requests, telemetry, labels, profiles and controls through MCP.

Finish with a plain-English activation report: what you changed, the evidence
that each part works, what you deliberately left unchanged, the business
decisions the new evidence supports, and how to roll back your changes.
```

## 6. Configure all three Watch signals

Use the base OTLP HTTP endpoint for every signal:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="https://reiver.ai/api/watch/ingest"
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $REIVER_WATCH_API_KEY"
export OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf"
export OTEL_SERVICE_NAME="my-app"
export OTEL_TRACES_EXPORTER="otlp"
export OTEL_LOGS_EXPORTER="otlp"
export OTEL_METRICS_EXPORTER="otlp"
```

These variables configure exporters; they do not install packages or initialize providers/readers/processors. The application must also have:

- a trace provider, span processor, and OTLP trace exporter;
- a logger provider or logging bridge, log record processor, and OTLP log exporter;
- a meter provider, periodic metric reader, and OTLP metric exporter.

See [Watch setup and troubleshooting](/watch/) for signal-specific examples and the checks to run when one signal is absent.

## 7. Confirm, correlate and end the session

Before adding identifiers, agree the application's [Session and Identity Contract](/flow/session-telemetry): the meaningful business episode, its start and terminal events, the 30-minute abandonment fallback, the stable pseudonymous user ID, and the anonymous/tenant policy. Save the confirmed contract in the project's Agent Soul so later coding-agent sessions can reuse it.

Use the same stable values at both layers:

| Layer | Session | User |
|---|---|---|
| Flow request headers | `x-reiver-session-id` | `x-reiver-user-id` |
| OpenTelemetry attributes | `gen_ai.conversation.id` | `gen_ai.user.id` |

`gen_ai.conversation.id` is the current OpenTelemetry GenAI attribute. `gen_ai.user.id` is Reiver's user-correlation attribute rather than a current OTel semantic-convention field. Reiver's LLM processor also recognises the deprecated `gen_ai.session.id`; emit both session values during the compatibility period. Older deployments may contain `gen_ai.session_id` or `llm.session_id`, but new instrumentation should not introduce those spellings.

When the application's conversation or task ends:

```bash
curl --request POST \
  "https://reiver.ai/api/gateway/v1/sessions/onboarding-smoke-1/end" \
  --header "Authorization: Bearer $REIVER_FLOW_API_KEY"
```

The endpoint is idempotent and returns `202` when evaluation is scheduled or already queued. A `503 service_unavailable` response means scheduling was not confirmed; retry rather than treating the session as ended successfully.

## 8. Definition of done

The coding agent should return a pass/fail table with evidence for every row:

| Check | Required evidence |
|---|---|
| Business context | The agent's **My understanding** summary is confirmed or supported by clear application evidence |
| Delegated authority | The token scopes and the owner's behavioural boundaries are stated without exposing credentials |
| MCP | Reiver resources can be listed and `agent://onboarding` can be read |
| Provider | Provider connection test passed |
| Gateway | One real application request returned `200` |
| Routing | Actual `x-reiver-provider` and `x-reiver-model-used` recorded |
| Trace | Application trace visible in Watch under the expected service name |
| Log | A structured application log visible and correlated to that trace/session |
| Metric | At least one application or runtime metric visible under the service |
| Session contract | Session unit, start/end events, idle fallback, stable pseudonymous user, anonymous policy, and tenant scoping confirmed and saved in Agent Soul |
| Identity | Stable user and session identifiers visible on the request and telemetry |
| Session | Explicit end call returned `202`; session becomes queryable |
| Continuity | A second session uses a new session ID and retains the same test user ID |
| Secrets | No provider key, SDK key, or agent token appears in source, logs, or output |

If any row is missing, the **Complete Reiver** track is incomplete. Missing Watch evidence does not invalidate a deliberately Flow-only integration, and a Watch-only integration does not require a provider key or gateway request; use the definition of done for the selected track.

## 9. Activate Reiver around the business

Once the baseline is green, the coding agent can complete the useful setup within its delegated authority:

1. Read existing prompts, settings, labels, profiles, dashboards and alerts before changing anything.
2. Save the confirmed project description, technical context and important outcomes in Reiver's existing project-agent context so future agent sessions can reuse them.
3. Translate confirmed success, failure, safety and commercial outcomes into a small set of precise session labels.
4. Create label-based or operational session profiles that preserve and surface the sessions worth reviewing.
5. Create or improve a managed prompt when central prompt control is relevant; read it back, test it and use rollout evidence to promote or roll it back.
6. Configure guardrails only for risks that matter to this application, then prove them with synthetic inputs.
7. Create dashboards and alerts only when they support an actual owner decision.
8. Run synthetic success and failure sessions, explicitly end them, and verify the resulting traffic, telemetry, labels and controls through MCP.
9. Return a change list, evidence, deliberate omissions and rollback path.

This is the combined Reiver loop: business context gives labels meaning; Flow controls providers, prompts and guardrails; Watch records gateway and application evidence; sessions and labels expose user and business outcomes; MCP lets the agent inspect that evidence and improve the system.

## When Reiver is and is not useful

Reiver is a strong fit when the application needs one or more of: provider portability, central prompt releases, per-user/session LLM cost and quality, fallback control, or correlated application and LLM observability. A single low-risk prototype that needs none of those controls may reasonably stay on a direct provider connection. Ask the agent to evaluate those concrete needs and the verified evidence above, not to repeat marketing claims.
