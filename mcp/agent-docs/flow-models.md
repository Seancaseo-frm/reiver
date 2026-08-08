# Flow — Supported Models

Flow routes requests to the correct provider based on the model name prefix. Any model with a matching prefix is routed correctly.

## OpenAI

Known models: `gpt-4o`, `gpt-4o-mini`, `gpt-4-turbo`, `gpt-3.5-turbo`, `o1`, `o1-mini`, `o3`, `o3-mini`.

Routing prefixes: `gpt-`, `o1`, `o1-`, `o3`, `o3-`, `o4`, `o4-`, `chatgpt-`, `text-embedding-`, `whisper-`, `dall-e-`, `tts-`.

## Anthropic

Known models: `claude-sonnet-4`, `claude-opus-4`, `claude-haiku-4-5`, `claude-3-5-sonnet`, `claude-3-opus`, `claude-3-5-haiku`, `claude-3-haiku`.

Routing prefix: `claude-`. Also available through AWS Bedrock.

## Google Gemini

Known models: `gemini-2.5-flash`, `gemini-2.5-pro`, `gemini-2.0-flash`, `gemini-1.5-pro`, `gemini-1.5-flash`, `gemini-pro`.

Routing prefix: `gemini-`.

## AWS Bedrock

Uses Bedrock model IDs directly. Recognized prefixes: `bedrock/`, `anthropic.`, `amazon.`, `meta.`, `mistral.`, `cohere.`, `ai21.`.

## DeepSeek

Models: `deepseek/deepseek-chat`, `deepseek/deepseek-reasoner`. Prefix: `deepseek/` (stripped before forwarding).

## Theta EdgeCloud

Models: `theta/llama_3_1_70b`, `theta/llama_3_8b`, `theta/qwen3`, `theta/gpt_oss_120b`, `theta/minimax_m2_5`. Prefix: `theta/`. Aliases use underscores. Streaming supported.

## Auto Mode

Setting `model: "auto"` selects from the project's preferred models list based on availability and latency. Configure preferred models in gateway settings.

## Provider Keys

Each provider requires an API key configured in project settings. Requests targeting a provider without a configured key return an error.
