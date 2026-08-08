# Flow — Getting Started

> **This page documents how the user's application connects to Flow.**
> You (the agent) do not call these endpoints yourself. Use this information when the user asks you to help integrate their application — write code that their app will run, using the endpoints and patterns below. To manage Flow configuration (prompts, integrations, settings), use the MCP tools instead.

Flow is Reiver's prompt hub and LLM gateway. It exposes an OpenAI-compatible API so applications can connect by changing a single URL.

## Application Integration

Applications connect to the Flow gateway by pointing any OpenAI-compatible client at Reiver:

### Python

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

### Node.js / TypeScript

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

### cURL

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

### Streaming

Applications enable streaming by setting `stream: true`:

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

## Embeddings

Applications generate vector embeddings using the same client:

```python
embedding = client.embeddings.create(
    model="text-embedding-3-small",
    input="Hello world"
)

print(embedding.data[0].embedding[:5])  # First 5 dimensions
```

Array input for batch embedding:

```python
embeddings = client.embeddings.create(
    model="text-embedding-3-small",
    input=["First document", "Second document", "Third document"]
)

for item in embeddings.data:
    print(f"Index {item.index}: {len(item.embedding)} dimensions")
```

Supported embedding providers: OpenAI, Mistral (`mistral/mistral-embed`), Together, Fireworks, DeepInfra, Cohere, Nvidia, Azure OpenAI.

## Using Other Providers

The same gateway API works with any supported provider. Applications change only the model name:

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

Applications set the model to `"auto"` and Flow selects the best model from the project's preferred models list:

```python
response = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "Hello!"}]
)
```

The `x-reiver-model-used` response header indicates which model was selected.

## System Prompt Behavior

Application system prompts are preserved exactly as sent. If prompt management is later enabled via the `prompt_config` field, Flow can inject a managed system prompt when the request doesn't already include one. Requests that already contain a system message are never modified.

## Platform Management (MCP)

To manage Flow configuration (provider integrations, gateway settings, prompt configs), use the MCP tools:

- List integrations: `list` with `resource: 'integrations'`
- Configure a provider (should be explicitly requested by the user): `execute` with `resource: 'integration', action: 'configure'`
- Update gateway settings (should be explicitly requested by the user): `execute` with `resource: 'gateway', action: 'update_settings'`
