# Models and variants

Flow routes a model identifier to a configured provider. The model catalogue changes independently of Reiver deployments, so use the live selector and test the exact model instead of relying on a long static list.

## What each catalogue means

| Source | What it proves |
|---|---|
| Project model selector | Reiver has routing metadata for a provider configured in this project |
| [Public model catalogue](https://reiver.ai/model-catalog) | Pricing/routing metadata known to Reiver |
| Provider connection test | The stored provider credential authenticates |
| Successful Playground/gateway request | That exact key, model and request path work now |
| `x-reiver-provider` and `x-reiver-model-used` | Which provider and model actually served the request |

Catalogue presence does not guarantee that every historical entry remains callable by every account. A provider can retire a model or restrict it by region/tier while pricing metadata remains available.

## Anthropic baseline

Current standard Reiver identifiers include:

| Intended use | Identifier |
|---|---|
| Highest capability / long-running agents | `claude-fable-5` |
| Complex agentic and enterprise work | `claude-opus-5` |
| Speed/intelligence balance | `claude-sonnet-5` |
| Lowest-cost current Claude family | Select the current Haiku entry shown for the Anthropic integration |

For first onboarding, use `claude-sonnet-5` unless the existing application already depends on another current Claude model.

## Standard, Fast and Batch

| Variant | Behaviour | Use it when |
|---|---|---|
| Standard, e.g. `claude-opus-5` | Normal synchronous inference | Default for applications and baseline tests |
| Fast, `claude-opus-5-fast` or `claude-opus-4.8-fast` | Same Opus model with higher output speed and premium pricing | The Anthropic account has fast-mode preview access and latency justifies the price |
| Batch, e.g. `claude-opus-5:batch` | Asynchronous provider batch pricing/processing | Offline bulk work through a provider batch API, not an interactive Flow completion |

Reiver translates the supported `-fast` aliases to Anthropic's native `speed: "fast"` request and beta header. Fast mode is currently restricted to Opus 5 and Opus 4.8 and requires Anthropic access; catalogue presence does not grant that access. Reiver hides unsupported historical fast aliases.

Reiver's project settings, prompt and Playground selectors exclude `:batch` entries because those screens issue synchronous Flow requests. Flow does not turn a normal chat-completion request into a provider batch job. The public pricing catalogue can still show batch entries for cost comparison.

## Claude sampling and thinking

Sonnet 5, Opus 5, Fable 5 and recent Opus 4.7/4.8 models reject non-default `temperature`, `top_p` and `top_k` values. Reiver omits those unsupported values at the Anthropic adapter, including temperatures inherited from managed prompt versions. The Playground labels sampling as **provider default** for these models.

Claude 5 uses adaptive thinking. Do not attach the legacy `thinking: {"type":"enabled","budget_tokens":...}` shape to Sonnet 5 or Fable 5. Reiver keeps their adaptive default and translates the legacy toggle to adaptive thinking where recent Opus models support it.

## Provider routing

Common direct-provider prefixes include:

- OpenAI: `gpt-`, `o1`–`o4` and related OpenAI identifiers;
- Anthropic: `claude-`;
- Google: `gemini-`;
- provider-qualified gateways such as `deepinfra/...`, `deepseek/...`, `mistral/...`, `together/...` and `theta/...`;
- AWS Bedrock native or `bedrock/...` identifiers.

For a provider-qualified model, copy the exact identifier from the live project selector. Do not infer that a retired specialist model has been silently replaced by the provider's latest general model.

## Explicit model first, auto later

Start with one explicit model and inspect the response:

```bash
curl --include https://reiver.ai/api/gateway/v1/chat/completions \
  --header "Authorization: Bearer $REIVER_FLOW_API_KEY" \
  --header "Content-Type: application/json" \
  --data '{"model":"claude-sonnet-5","messages":[{"role":"user","content":"Hello"}]}'
```

Only after that works should you configure `model: "auto"`, preferred models or fallback providers. Auto mode selects from project settings; it is not evidence that the originally requested provider/model worked.

## Provider keys

Provider keys are stored in Reiver under **Prompt Hub → Integrations**. The application sends only its Reiver SDK key. Test provider connectivity after adding or rotating a key, then test the exact model in the Playground.
