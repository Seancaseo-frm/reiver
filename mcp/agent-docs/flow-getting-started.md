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
    model="auto",
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
  model: 'auto',
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
    "model": "auto",
    "messages": [
      {"role": "user", "content": "Hello!"}
    ]
  }'
```

### Streaming

Applications enable streaming by setting `stream: true`:

```python
stream = client.chat.completions.create(
    model="auto",
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

## Model Selection and Providers

Applications normally set the model to `"auto"` and omit request-level `models` and `provider` overrides. Flow selects from the project's centrally configured model candidates and enabled provider integrations:

```python
response = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "Hello!"}]
)
```

The `x-reiver-model-used` response header indicates which model was selected.

Use the MCP `list` tool with `resource: "model_catalog"` before configuring project routing. This live, project-filtered catalogue is the source of truth for model IDs. Keep the ordered candidates in `default_fallback_models` and provider policy in `provider_preferences`; do not copy those IDs into application code merely to reproduce Reiver-owned routing.

Pin a concrete model in the application only when the owner explicitly wants application-owned routing. Select that ID from the live catalogue. Flow translates the OpenAI request format to the selected provider's native API automatically.

## System Prompt Behavior

Application system prompts are preserved exactly as sent. If prompt management is later enabled via the `prompt_config` field, Flow can inject a managed system prompt when the request doesn't already include one. Requests that already contain a system message are never modified.

## Platform Management (MCP)

To manage Flow configuration (provider integrations, gateway settings, prompt configs), use the MCP tools:

- List integrations: `list` with `resource: 'integrations'`
- List current project models: `list` with `resource: 'model_catalog'`
- Configure a provider (should be explicitly requested by the user): `execute` with `resource: 'integration', action: 'configure'`
- Update gateway settings (should be explicitly requested by the user): `execute` with `resource: 'gateway', action: 'update_settings'`
