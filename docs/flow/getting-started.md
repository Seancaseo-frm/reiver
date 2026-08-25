# Flow + Prompt Hub quickstart

Flow is Reiver's OpenAI-compatible LLM gateway and prompt hub. Existing OpenAI-compatible clients connect by changing the base URL and using a Reiver SDK key.

This is an independently completable track. It does not require Watch, application logs, or metrics. For the combined Flow, Watch, identity, and MCP workflow, use the [Complete Reiver Quickstart](/quickstart).

## Definition of done for this track

| Check | Required evidence |
|---|---|
| Provider | The selected provider connection test passed |
| Gateway | One real application request returned `200` |
| Routing | `x-reiver-provider`, `x-reiver-model-used`, and `x-request-id` were recorded |
| Identity | If the application has users/sessions, its Session and Identity Contract is confirmed and matching identifiers are visible |
| Prompt Hub | If a managed prompt is part of the chosen scope, its version was read back, tested, and used by a request |
| Secrets | Provider and SDK keys do not appear in source, logs, or output |

Gateway-only users can stop when the provider, gateway, routing, and secret checks pass. Prompt Hub and session correlation are required only when they are part of the selected use case—not as artificial onboarding work.

## Prerequisites

1. Add one provider key in Reiver under **Prompt Hub → Integrations**.
2. Run the provider connection test.
3. Create an SDK key under **Settings → General → SDK keys** and select `llm:write`; the UI also selects `llm:read`. Flow gateway requests require `llm:write`.
4. Bind it in the application runtime as `REIVER_FLOW_API_KEY`.

Provider keys remain in Reiver. They are not placed in the application or given to the coding agent.

## Python

```python
import os
from openai import OpenAI

client = OpenAI(
    api_key=os.environ["REIVER_FLOW_API_KEY"],
    base_url="https://reiver.ai/api/gateway/v1",
)

session_id = "conversation-123"
user_id = "user-456"

response = client.chat.completions.create(
    model="claude-sonnet-5",
    user=user_id,
    messages=[{"role": "user", "content": "Hello from Reiver"}],
    extra_headers={
        "x-reiver-session-id": session_id,
        "x-reiver-user-id": user_id,
    },
)

print(response.choices[0].message.content)
```

The request's `user` field currently populates per-user gateway analytics. The `x-reiver-user-id` header drives user-sticky prompt routing. Send the same stable ID in both.

## Node.js / TypeScript

```typescript
import OpenAI from "openai";

const client = new OpenAI({
  apiKey: process.env.REIVER_FLOW_API_KEY,
  baseURL: "https://reiver.ai/api/gateway/v1",
});

const response = await client.chat.completions.create(
  {
    model: "claude-sonnet-5",
    user: "user-456",
    messages: [{ role: "user", content: "Hello from Reiver" }],
  },
  {
    headers: {
      "x-reiver-session-id": "conversation-123",
      "x-reiver-user-id": "user-456",
    },
  },
);

console.log(response.choices[0].message.content);
```

## Inspect the actual route

Use cURL for the first proof because it exposes response headers directly:

```bash
curl --include https://reiver.ai/api/gateway/v1/chat/completions \
  --header "Authorization: Bearer $REIVER_FLOW_API_KEY" \
  --header "Content-Type: application/json" \
  --header "x-reiver-session-id: onboarding-smoke-1" \
  --header "x-reiver-user-id: onboarding-user-1" \
  --data '{
    "model": "claude-sonnet-5",
    "user": "onboarding-user-1",
    "messages": [{"role": "user", "content": "Reply: reiver-flow-ok"}]
  }'
```

A valid baseline records:

- HTTP `200`;
- `x-reiver-provider`;
- `x-reiver-model-used`;
- `x-request-id`.

Status alone is insufficient: a fallback could return `200` from a different provider or model.

## Model selection

For interactive Anthropic onboarding, `claude-sonnet-5` is the balanced default. Leave sampling at the provider default and do not add a legacy manual thinking budget. Fast mode is limited to supported Opus 5/4.8 aliases and requires Anthropic access. `:batch` entries are asynchronous provider jobs, not interactive Flow choices. See [Models and variants](/flow/models).

Start with one explicit model. Add `model: "auto"`, fallback models and provider preferences only after the explicit path passes.

## Streaming

Set `stream: true` as with the OpenAI API:

```python
stream = client.chat.completions.create(
    model="claude-sonnet-5",
    messages=[{"role": "user", "content": "Tell me a short story."}],
    stream=True,
)

for chunk in stream:
    if chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="")
```

## End the session

First define the application's [Session and Identity Contract](/flow/session-telemetry). Reiver starts recording a session on the first accepted request with its `x-reiver-session-id`; there is no separate start call. Explicitly finish the confirmed conversation or task so evaluation does not wait for the 30-minute crash/abandonment fallback:

```bash
curl --request POST \
  "https://reiver.ai/api/gateway/v1/sessions/conversation-123/end" \
  --header "Authorization: Bearer $REIVER_FLOW_API_KEY"
```

The idempotent endpoint returns `202` when evaluation is scheduled or already queued.

## Managed prompts

Flow is a transparent proxy until the application supplies a `prompt_config`. A managed prompt is injected only when the request does not already contain a system message.

```python
response = client.chat.completions.create(
    model="claude-sonnet-5",
    messages=[{"role": "user", "content": user_input}],
    extra_body={
        "prompt_config": "math-tutor",
        "prompt_variables": {"learner_level": "secondary"},
    },
)
```

Add prompt management after the baseline gateway and telemetry checks pass.

## Next steps

- [Session and Identity Contract](/flow/session-telemetry)
- [Prompt management](/flow/prompt-management)
- [Routing and fallbacks](/flow/routing)
- [API reference](/flow/api-reference)
- [Watch traces, logs and metrics](/watch/)
