# openclaw-flow

OpenClaw plugin for the [Flow LLM gateway](https://reiver.io). Route all your agent traffic through Flow and get cost controls, guardrails, prompt management, and observability out of the box.

## Install

```bash
openclaw plugins install openclaw-flow
```

## Setup

Add your Flow API key to `openclaw.json`:

```json5
{
  plugins: {
    "flow-gateway": {
      apiKey: "flow_your_project_key"
    }
  }
}
```

That's it. The plugin registers a `flow` provider with all supported models. Set your agent to use it:

```json5
{
  agent: {
    model: { primary: "flow/auto" }
  }
}
```

### Custom gateway URL

If you self-host Flow, override the default URL:

```json5
{
  plugins: {
    "flow-gateway": {
      gatewayUrl: "https://your-instance.example.com/v1",
      apiKey: "flow_your_project_key"
    }
  }
}
```

## Available models

| Model ID | Description |
|---|---|
| `flow/auto` | Auto-routes to the best available model based on your project settings |
| `flow/gpt-4o` | OpenAI GPT-4o |
| `flow/gpt-4o-mini` | OpenAI GPT-4o Mini |
| `flow/o3-mini` | OpenAI o3-mini (reasoning) |
| `flow/claude-sonnet-4-5` | Anthropic Claude Sonnet 4.5 (reasoning) |
| `flow/claude-3-5-sonnet` | Anthropic Claude 3.5 Sonnet |
| `flow/gemini-2.5-pro` | Google Gemini 2.5 Pro (reasoning) |
| `flow/gemini-2.0-flash` | Google Gemini 2.0 Flash |

Any model supported by your Flow project works -- the list above is pre-populated for convenience.

## What you get

- **Cost budgets** -- cap per-session spend so a runaway agent loop can't burn your wallet overnight
- **Guardrails** -- block dangerous prompts, redact PII before it reaches any provider, enforce token limits
- **Prompt management** -- iterate prompts in a web UI with A/B testing and canary deployments, no YAML editing
- **Observability** -- full audit trail of every agent request with cost, latency, model, and quality scores
- **Smart routing** -- auto-route different agent tasks to the best model for the job
- **Semantic caching** -- instant responses for repetitive agent requests at zero inference cost

## Links

- [Flow documentation](https://reiver.io/docs/flow)
- [OpenClaw guide for Flow](../../docs/openclaw-guide.md)
- [GitHub](https://github.com/your-org/reiver)
