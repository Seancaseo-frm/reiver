# Getting Started with Flow

Flow is Reiver's Prompt Hub and OpenAI-compatible LLM gateway. This is an independent onboarding track: Watch, application logs, application metrics, and MCP are not required.

## Before you start

You need two different credentials:

- A **provider key** is saved inside Reiver so Flow can call the provider. It never belongs in your application.
- An **SDK key** authenticates your application to Flow. Keep it in a secret store and bind it as `REIVER_FLOW_API_KEY`.

If you later add Watch, the same SDK-key value may currently be bound separately as `REIVER_WATCH_API_KEY`. A coding agent uses a different `REIVER_AGENT_TOKEN`; agent tokens are not application keys.

No credential belongs in code, examples, logs, or reports.

## 1. Connect a provider

Add the provider credential in **Prompt Hub → Settings** and use Reiver's connection test. Choose a model available to that provider; this guide deliberately does not prescribe one.

## 2. Send one real application request

Store the SDK key outside the code:

```bash
export REIVER_FLOW_API_KEY="<SDK key from your secret store>"
```

Point an OpenAI-compatible client at Flow:

```python
import os
from openai import OpenAI

client = OpenAI(
    api_key=os.environ["REIVER_FLOW_API_KEY"],
    base_url="https://reiver.ai/api/gateway/v1",
)

response = client.chat.completions.create(
    model=os.environ["REIVER_MODEL"],
    messages=[{"role": "user", "content": "Hello from my application"}],
)
print(response.choices[0].message.content)
```

Use a real application path rather than treating a Playground-only request as completion.

## 3. Add Prompt Hub only when needed

Managed prompts are optional. When you reference a prompt configuration and the request has no system message, Flow can inject the active managed prompt. If the request already contains a system message, Flow preserves it and skips managed-prompt injection.

See [Prompt Management](/flow/prompt-management) for versioning and rollout details.

## Definition of done

| Check | Required evidence |
|---|---|
| Provider | Reiver's provider connection test passed. |
| Gateway | One real application request succeeded through `https://reiver.ai/api/gateway/v1`. |
| Routing | The response identifies the provider and model Flow actually used. |
| Prompt Hub | If selected, the intended managed prompt version was used; otherwise this row is deliberately omitted. |
| Secrets | No provider key or SDK key appears in source, logs, screenshots, or the report. |

Watch traces, logs, metrics, and MCP evidence are not acceptance criteria for this track.

## Next steps

- Define the [Session and Identity Contract](/flow/session-telemetry) if business-episode analysis matters.
- Complete [Watch](/watch/) independently if you want application traces, logs, and metrics.
- Use the [Complete Reiver track](/quickstart) to correlate both products.
