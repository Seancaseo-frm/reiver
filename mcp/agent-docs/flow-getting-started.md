# Flow + Prompt Hub — application integration

> These endpoints are called by the user's application with an SDK key. The MCP agent token does not authenticate application gateway requests.

Read `agent://onboarding` before editing an application.

This track is independently completable. It does not require Watch, logs, or metrics unless the owner's selected scope is Complete Reiver.

## Track definition of done

- the selected provider connection test passes;
- one real application gateway request returns `200`;
- actual `x-reiver-provider`, `x-reiver-model-used`, and `x-request-id` are recorded;
- when sessions/users are in scope, the Session and Identity Contract is confirmed and identifiers are verified;
- when Prompt Hub is in scope, the managed prompt version is read back, tested, and proven on a request;
- credentials do not appear in source, logs, tool output, or the report.

Do not report a Flow-only integration as incomplete because Watch telemetry is absent. Do not create a managed prompt merely to satisfy onboarding when the owner selected gateway-only use.

## Baseline

1. Preserve the application's existing provider and explicit model.
2. Change the OpenAI-compatible base URL to `https://reiver.ai/api/gateway/v1`.
3. Read the SDK key from `REIVER_FLOW_API_KEY`.
4. Send stable `x-reiver-session-id` and `x-reiver-user-id` headers.
5. Set the OpenAI-compatible `user` body field to the same stable user ID for current per-user analytics.
6. Inspect `x-reiver-provider`, `x-reiver-model-used` and `x-request-id` on the first response.

## Python pattern

```python
import os
from openai import OpenAI

client = OpenAI(
    api_key=os.environ["REIVER_FLOW_API_KEY"],
    base_url="https://reiver.ai/api/gateway/v1",
)

response = client.chat.completions.create(
    model="claude-sonnet-5",
    user=user_id,
    messages=messages,
    extra_headers={
        "x-reiver-session-id": session_id,
        "x-reiver-user-id": user_id,
    },
)
```

Adapt the pattern to the application's existing client abstraction instead of adding a second LLM client.

## Model rule

For current Anthropic onboarding, `claude-sonnet-5` is the balanced default unless the application already relies on another current model. Leave sampling at the provider default and do not add a legacy manual thinking budget. Do not use `:batch` in an interactive request, and do not add `auto` or fallbacks until one explicit route passes.

## Explicit session end

Read `agent://flow/session-telemetry` and confirm the Session and Identity Contract first. Reiver records the first accepted request carrying a new session ID; there is no separate start call. Use the 30-minute idle evaluator only as a crash/abandonment fallback.

Call:

```text
POST https://reiver.ai/api/gateway/v1/sessions/{session_id}/end
Authorization: Bearer <REIVER_FLOW_API_KEY>
```

The endpoint is idempotent and returns `202` when evaluation is scheduled or already queued.

## Platform management through MCP

- List integrations: `list` with `resource: "integrations"`.
- Test a configured integration only when authorized: `execute` with `resource: "integration", action: "test"`.
- Read gateway metrics: `analyze` with `analysis: "llm_overview"`.
- Manage settings or prompts only with explicit user authorization and the relevant write scope.

Provider keys stay in Reiver. Never request them merely to integrate application code.
