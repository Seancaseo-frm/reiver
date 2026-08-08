# Getting Started with Flow

Flow is Reiver's prompt hub and LLM gateway. It lets you version, test, and roll out prompts centrally, and exposes an **OpenAI-compatible API** so you can connect any existing application by changing a single URL — no new SDK required.

## Quick Start

Point any OpenAI-compatible client at Reiver. Here's a Python example:

```python
from openai import OpenAI

client = OpenAI(
    api_key="dh_your_key",
    base_url="https://reiver.ai/api/gateway/v1"
)

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello!"}
    ]
)

print(response.choices[0].message.content)
```

That's it. Your existing system prompt, messages, and parameters are forwarded to the LLM provider exactly as you send them. Flow adds observability, cost tracking, and failover on top — without modifying your request.

## Using Other Providers

The same API works with any supported provider. Just change the model name:

```python
# Anthropic
response = client.chat.completions.create(
    model="claude-3-5-sonnet",
    messages=[{"role": "user", "content": "Hello!"}]
)

# Google Gemini
response = client.chat.completions.create(
    model="gemini-2.0-flash",
    messages=[{"role": "user", "content": "Hello!"}]
)

# AWS Bedrock
response = client.chat.completions.create(
    model="anthropic.claude-3-sonnet-20240229-v1:0",
    messages=[{"role": "user", "content": "Hello!"}]
)
```

Flow translates the OpenAI request format to each provider's native API automatically.

## Auto Model Selection

Set the model to `"auto"` and Flow will select the best model from your project's preferred models list:

```python
response = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "Hello!"}]
)
```

The `x-reiver-model-used` response header tells you which model was selected.

## Streaming

Flow supports Server-Sent Events (SSE) streaming for all providers:

```python
stream = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Tell me a story."}],
    stream=True
)

for chunk in stream:
    if chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="")
```

## Node.js / TypeScript

```typescript
import OpenAI from 'openai';

const client = new OpenAI({
  apiKey: 'dh_your_key',
  baseURL: 'https://reiver.ai/api/gateway/v1',
});

const response = await client.chat.completions.create({
  model: 'claude-3-5-sonnet',
  messages: [{ role: 'user', content: 'Hello!' }],
});

console.log(response.choices[0].message.content);
```

## cURL

```bash
curl https://reiver.ai/api/gateway/v1/chat/completions \
  -H "Authorization: Bearer dh_your_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4o",
    "messages": [
      {"role": "user", "content": "Hello!"}
    ]
  }'
```

## What Happens to My System Prompt?

Your existing system prompt and messages are **preserved exactly as sent**. Flow acts as a transparent proxy by default.

If you later opt in to [Prompt Management](/flow/prompt-management), Flow can inject a managed system prompt when your request doesn't already include one. If your request already contains a system message, the managed prompt is skipped — your messages are never modified. See the [Prompt Management](/flow/prompt-management) page for the full workflow.

## Next Steps

- [Prompt Management](/flow/prompt-management) — Version, test, and roll out prompts
- [Features](/flow/features) — Caching, failover, guardrails, PII masking, and more
- [Supported Models](/flow/models) — Full list of providers and models
- [API Reference](/flow/api-reference) — Endpoints, request/response types, and headers
