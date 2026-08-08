# Supported Models

Flow routes requests to the correct provider based on the model name prefix. You can use any model supported by the provider — the table below lists known models, but any model with a matching prefix will be routed correctly.

## Providers

### OpenAI

| Model | Identifier |
|-------|-----------|
| GPT-4o | `gpt-4o` |
| GPT-4o Mini | `gpt-4o-mini` |
| GPT-4 Turbo | `gpt-4-turbo` |
| GPT-3.5 Turbo | `gpt-3.5-turbo` |
| o1 | `o1` |
| o1 Mini | `o1-mini` |
| o3 | `o3` |
| o3 Mini | `o3-mini` |

Any model starting with `gpt-`, `o1`, `o1-`, `o3`, `o3-`, `o4`, `o4-`, `chatgpt-`, `text-embedding-`, `whisper-`, `dall-e-`, or `tts-` routes to OpenAI.

### Anthropic

| Model | Identifier |
|-------|-----------|
| Claude Sonnet 4 | `claude-sonnet-4` |
| Claude Opus 4 | `claude-opus-4` |
| Claude Haiku 4.5 | `claude-haiku-4-5` |
| Claude 3.5 Sonnet | `claude-3-5-sonnet` |
| Claude 3 Opus | `claude-3-opus` |
| Claude 3.5 Haiku | `claude-3-5-haiku` |
| Claude 3 Haiku | `claude-3-haiku` |

Any model starting with `claude-` routes to Anthropic. Claude models are also available through [AWS Bedrock](/flow/routing#multi-provider-models).

### Google Gemini

| Model | Identifier |
|-------|-----------|
| Gemini 2.5 Flash | `gemini-2.5-flash` |
| Gemini 2.5 Pro | `gemini-2.5-pro` |
| Gemini 2.0 Flash | `gemini-2.0-flash` |
| Gemini 1.5 Pro | `gemini-1.5-pro` |
| Gemini 1.5 Flash | `gemini-1.5-flash` |
| Gemini Pro | `gemini-pro` |

Any model starting with `gemini-` routes to Google.

### AWS Bedrock

Use Bedrock model IDs directly. Flow recognizes these prefixes:

- `bedrock/` — e.g., `bedrock/anthropic.claude-3-sonnet-20240229-v1:0`
- `anthropic.` — e.g., `anthropic.claude-3-sonnet-20240229-v1:0`
- `amazon.` — e.g., `amazon.titan-text-express-v1`
- `meta.` — e.g., `meta.llama3-1-8b-instruct-v1:0`
- `mistral.` — e.g., `mistral.mistral-7b-instruct-v0:2`
- `cohere.` — e.g., `cohere.command-r-plus-v1:0`
- `ai21.` — e.g., `ai21.jamba-1-5-mini-v1:0`

### DeepSeek

| Model | Identifier |
|-------|-----------|
| DeepSeek Chat | `deepseek/deepseek-chat` |
| DeepSeek Reasoner | `deepseek/deepseek-reasoner` |

Any model starting with `deepseek/` routes to DeepSeek. The `deepseek/` prefix is stripped before forwarding to the DeepSeek API.

### Theta EdgeCloud

Prefix with `theta/`. Aliases use underscores to match Theta's service catalog:

| Model | Identifier |
|-------|-----------|
| Llama 3.1 70B | `theta/llama_3_1_70b` |
| Llama 3 8B | `theta/llama_3_8b` |
| Qwen3 | `theta/qwen3` |
| GPT OSS 120B | `theta/gpt_oss_120b` |
| MiniMax M2.5 | `theta/minimax_m2_5` |

Any model starting with `theta/` routes to Theta EdgeCloud on-demand. Streaming is supported.

## Auto Mode

Set the model to `"auto"` and Flow will select from your project's preferred models list based on availability and latency:

```python
response = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "Hello!"}]
)
```

Configure preferred models in your project's gateway settings. If no preference is set, the project's default model is used.

## Provider Keys

Each provider requires an API key configured in your project settings. If a request targets a provider without a configured key, it returns an error:

```
API key not configured for provider 'anthropic'
```

Configure provider keys in the Reiver dashboard under **Project Settings → Gateway → Provider Keys**.
