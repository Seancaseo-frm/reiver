# Stephen onboarding runbook

Internal facilitator guide. Do not ask Stephen to paste any credential into chat, screen share, source code, or an agent prompt.

## Outcome

In one working session, have Stephen's coding agent understand the maths-tutor business, route one real request through Reiver, export a trace, structured log and metric, correlate one user/session, explicitly end that session, and verify the result through MCP. Once that baseline is green, let the agent continue autonomously with the business-specific labels and Reiver configuration Stephen has authorised.

The first session is a baseline, not a feature tour. Defer Qwen/DeepInfra, multiple providers, `auto`, fallbacks, managed prompts, batch models, dashboards and alerts until every acceptance row is green.

## Release sequence today

1. Revoke the SDK key exposed in the earlier transcript if that has not already happened.
2. Before enforcing the credential boundary, replace any application that is incorrectly using an **agent** token at Flow or Watch with an **SDK** key. The release intentionally makes those application endpoints return `401` for the wrong stored key type.
3. Review and merge only after every triggered CI job passes. This change directly needs Core, Flow, Website, MCP and Docs; the current path filters also exercise Watch and Pond when Core changes.
4. Deploy the origin services: Flow for current Anthropic request handling and model selectors; MCP for authenticated resources and onboarding instructions; Website for SDK-key enforcement and the `/quickstart` SPA status fix; Docs for the canonical guide and clean URLs.
5. Run `deploy/scripts/verify-onboarding-surfaces.sh` without a token, then again with a read-only `REIVER_AGENT_TOKEN`. The script proves public routes, the unauthenticated MCP `401`, and the authenticated onboarding resource.
6. Run the credentialed Flow and three-signal Watch acceptance tests in a disposable project.

No Cloudflare change is required for these origin fixes if the existing hostnames still route to their current services. If an origin returns the expected page but the public URL still fails after deployment, stop and hand the origin/public evidence to the Cloudflare owner rather than changing application code to mask the edge problem.

## Before the call: Seán

- Deploy the website, Flow, MCP and docs changes from this onboarding sprint.
- Confirm the gateway key exposed in the earlier test transcript has been revoked; use a fresh SDK key for this run.
- Run `deploy/scripts/verify-onboarding-surfaces.sh`. Set `REIVER_AGENT_TOKEN` first if you also want it to verify that MCP can read the complete onboarding resource.
- In a disposable project, confirm Anthropic's connection test and a `claude-sonnet-5` Playground request.
- Have the Reiver project ready, but do not generate or handle Stephen's provider credential for him.

## Before the call: Stephen

Stephen should have:

- access to the maths tutor repository and its deployment secret manager;
- his Anthropic API key;
- his preferred coding agent with MCP support;
- permission to create a test branch and deploy a test environment.

## Guided session

### 0–5 minutes: create the three credential roles

Stephen performs these actions in his own browser and secret manager:

1. Add the Anthropic key under **Prompt Hub → Integrations** and run **Test**. This proves authentication only; do not treat it as proof of a model call.
2. Create one SDK key under **Settings → General → SDK keys**.
3. Bind the same SDK key value to two distinct application secrets:
   - `REIVER_FLOW_API_KEY`
   - `REIVER_WATCH_API_KEY`
4. For read-only evaluation, create an agent token under **Agents → Tokens** with `llm:read` and `observability:read`. For the autonomous setup agreed for this pilot, select `llm:write` and `observability:write`; each write scope includes its matching read access.
5. Bind that token as `REIVER_AGENT_TOKEN` in the coding-agent environment only.

There is no autonomy-mode selector. The token scopes are the hard technical boundary and Stephen's pasted assignment is the behavioural authority. Do not add `project:write`; it is unnecessary for normal application onboarding. Do not put the agent token in the application runtime.

### 5–10 minutes: prove the provider and Flow in isolation

1. In the Playground, select `claude-sonnet-5`, leave sampling at **provider default**, and send: `Reply with reiver-playground-ok`.
2. Run the canonical Quickstart cURL with disposable user/session IDs.
3. Record only non-secret evidence:
   - HTTP `200`;
   - `x-reiver-provider`;
   - `x-reiver-model-used`;
   - `x-request-id`.

Stop here if the actual provider/model is unexpected. Do not hide a routing issue behind a successful status.

### 10–15 minutes: connect MCP

1. Add the client configuration from the public Quickstart.
2. Restart the coding agent.
3. Confirm the client sees `reiver`: `codex mcp list` or Codex `/mcp`; `claude mcp list` plus `claude mcp get reiver` or Claude Code `/mcp`; or **Cursor Settings → Tools & MCP**.
4. Approve the project-scoped server if the client prompts.
5. Ask it to list Reiver resources and read `agent://onboarding`.
6. Confirm it can read the resource; a visible server name alone is not proof.

### 15–20 minutes: confirm the business context and authority

Paste the autonomous assignment from the public Quickstart. The coding agent first reads `gateway_settings.agent_soul`, reuses any stored project context that still matches, inspects the current repository and presents its **My understanding** summary. Stephen corrects only material conflicts or gaps, including:

- primary maths-tutor users and intended learning/business outcome;
- successful and failed session outcomes;
- session start/end and stable learner identity;
- important safety, quality and sensitive-data constraints;
- whether relevant production prompt rollouts are inside or outside the delegated authority.

That confirmation authorises the stated work once. The agent should not interrupt Stephen for every in-scope prompt, label, profile, guardrail, dashboard or alert.

Use this as the proposed Session and Identity Contract for Stephen's maths tutor. The agent should confirm it against the real application and ask only if the code or Stephen's intended learning workflow materially disagrees:

```text
Session and Identity Contract
- Session unit: one coherent learning problem, exercise, or learning episode.
- Starts when: the learner begins or asks the first question about that problem.
- Ends successfully when: the problem is solved or the learning objective is reached.
- Also ends when: the learner abandons it, changes topic, explicitly finishes, or the app enters a terminal failure/handoff state.
- Idle fallback: Reiver's 30-minute idle evaluator is crash/abandonment protection, not the normal success boundary.
- Stable user ID source: the app's pseudonymous internal learner ID, reused across learning sessions; never an email or name.
- Anonymous-user policy: use a durable pseudonymous app/browser identifier if the app supports anonymous learners; do not send one shared `anonymous` value.
- Tenant scoping: namespace the learner ID when separate schools/organisations can issue colliding local IDs.
```

Once confirmed, the agent merges this block into the existing Agent Soul rather than replacing other useful context. A later Claude, Codex, or other MCP connection should read it there and should not ask Stephen the same questions again unless the application now contradicts it.

### 20–40 minutes: let the coding agent integrate the app

The agent then:

- point the existing OpenAI-compatible client at the Reiver base URL;
- preserve the application's explicit model for the baseline;
- initialize traces, logs and metrics, not just set an OTLP endpoint;
- pass stable user and session identifiers at the Flow and OTel layers;
- explicitly end the Flow session at the confirmed terminal events;
- keep all credentials out of source and output;
- run relevant tests and show its file-level changes;
- return the complete baseline acceptance table using MCP evidence.

Stephen remains the person who supplies secrets. Deployment and Reiver configuration may proceed autonomously only inside the authority he confirmed above.

### 40–45 minutes: verify from the outside

Run one real maths-tutor interaction. Complete this table with specific IDs/names, never secret values:

| Check | Evidence | Result |
|---|---|---|
| Business context | Confirmed **My understanding** summary | Pass / Fail |
| Delegated authority | Scopes and behavioural boundary stated, no token shown | Pass / Fail |
| MCP resource | `agent://onboarding` read | Pass / Fail |
| Provider test | Anthropic test result | Pass / Fail |
| Gateway | application request status and request ID | Pass / Fail |
| Routing | provider and model response headers | Pass / Fail |
| Trace | service name, trace ID and expected span | Pass / Fail |
| Log | known message plus matching trace/session | Pass / Fail |
| Metric | known metric name plus recent data point | Pass / Fail |
| Identity | stable user and conversation values | Pass / Fail |
| Session | end call `202` and queryable session | Pass / Fail |
| Continuity | second session has a new session ID and the same pseudonymous learner ID | Pass / Fail |
| Secrets | repository/log/output scan clean | Pass / Fail |

If one signal is missing, use the Watch troubleshooting matrix and leave the row failed. Do not broaden the test until all rows pass.

If every baseline row is green, Stephen can leave the agent running after the call to preserve the confirmed project context in Reiver, create the initial business-specific label taxonomy and profiles, configure only relevant prompts/guardrails/dashboards/alerts, generate synthetic success and failure sessions, and return its activation report. The agent must list every change, its evidence, deliberate omissions and rollback path.

## Stephen's evaluation question

After the verified run, ask his coding agent:

> Based on this application's code and the evidence you just queried, identify which Reiver capabilities solve a real current need, which are only future options, and whether the integration should remain. Cite concrete evidence and include reasons not to use Reiver. Do not repeat product marketing.

This gives Stephen an honest agent-mediated product evaluation while ensuring the agent understands what Reiver can actually prove.

## Autonomous activation only after the baseline is green

Within Stephen's confirmed authority, the agent can now act without repeated approval while introducing and verifying one controlled change at a time:

1. Save the confirmed project description, technical context and important learning outcomes in Reiver's existing project-agent context.
2. Define precise maths-tutor session labels and useful session profiles, then verify them with synthetic sessions.
3. Add or improve a managed prompt, test it, roll it out within the delegated boundary and document model-override precedence.
4. Configure guardrails tied to an identified tutor risk and prove the expected behaviour with synthetic inputs.
5. Add dashboards or alerts from metric names that actually arrived and that support a real Stephen decision.
6. Add the second provider Stephen genuinely wants, test the exact model explicitly, then add a fallback and deliberately verify the fallback headers.

Qwen Math should not be treated as silently replaced by another Qwen model. A retired catalogue slug and a current model are separate models; test any replacement explicitly.

## Go/no-go rule

Onboarding is complete only when every acceptance row passes. If the baseline cannot be made green within the call, capture the exact failed row, request ID, service name and non-secret exporter error, assign an owner, and stop adding features.
