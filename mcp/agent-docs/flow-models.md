# Flow — model selection for agents

The model catalogue is dynamic routing/pricing metadata. It is not proof that a provider key can call every historical model entry. When integrating an application, prove the exact selection in the Playground or with a gateway response and record `x-reiver-provider` plus `x-reiver-model-used`.

## Baseline rule

Preserve the application's existing provider and explicit model for the first Reiver test. Do not add auto-routing, a fallback provider, or infer a replacement for a retired specialist model.

For current Anthropic onboarding, use `claude-sonnet-5` as the balanced default unless the app already requires `claude-fable-5` or `claude-opus-5`.

## Variants

- Standard model: synchronous application inference; use for the baseline.
- `claude-opus-5-fast` and `claude-opus-4.8-fast`: Anthropic fast-mode aliases. Reiver maps them to native `speed: "fast"`; the Anthropic account still needs research-preview access and pays premium rates.
- `:batch`: asynchronous provider batch processing/pricing metadata. Flow's interactive chat-completion endpoint does not create provider batch jobs.

Interactive project selectors exclude `:batch` and unsupported historical Claude fast aliases. The public pricing catalogue may still include them.

## Claude 5 compatibility

Claude Sonnet 5, Opus 5, Fable 5 and recent Opus 4.7/4.8 models reject non-default sampling fields. Reiver's Anthropic adapter omits `temperature` and `top_p` for them, including values inherited from managed prompts. They must not be treated as deterministic or cacheable merely because the caller requested temperature zero.

Claude 5 uses adaptive thinking. Do not add legacy manual thinking budgets to Sonnet 5 or Fable 5. Reiver preserves the adaptive default and translates the compatibility toggle where necessary. It preserves an explicit disabled request for default-on Sonnet 5 and Opus 5; Fable 5 and Mythos 5 are always-on and reject attempts to disable thinking.

## Routing identifiers

Direct providers use prefixes such as `gpt-`, `claude-` and `gemini-`. Provider-qualified gateways use identifiers such as `deepinfra/...`, `deepseek/...`, `mistral/...`, `together/...` and `theta/...`. Copy the exact live project-catalog identifier instead of guessing prefix or alias behaviour.

Each direct provider needs a key configured in Reiver. Provider keys stay in Reiver and must not be requested through MCP or placed in application code.

After the explicit path passes, the user may choose `model: "auto"`, preferred models, or fallback providers. Verify actual response headers again after each routing change.
