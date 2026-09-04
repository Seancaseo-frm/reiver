# Flow — Live Model Discovery

Model availability changes independently of these embedded docs. Never treat a static example, remembered model name, provider marketing name, or guessed slug as proof that a model is available to a Reiver project.

## Project Model Catalogue (MCP)

Before selecting or configuring a chat model, call:

```json
{"resource": "model_catalog"}
```

with the MCP `list` tool. The result contains current interactive model IDs grouped by the provider integrations enabled for this project. It is the source of truth for model IDs during onboarding.

- Use the returned `id` exactly; display names are not API identifiers.
- A provider that is not enabled for the project is intentionally absent.
- The catalogue covers interactive Flow models. Do not infer embedding or batch support from it.
- Re-read the catalogue immediately before changing project routing settings because availability can change.

If the token lacks `llm:read` or the catalogue call fails, preserve existing routing and report the missing evidence. Do not guess a replacement model.

## Reiver-Owned Routing

Applications normally send:

```json
{
  "model": "auto",
  "messages": [{"role": "user", "content": "Hello"}]
}
```

and omit the request-level `models` and `provider` fields. Reiver then owns model selection and failover through project gateway settings:

- `default_fallback_models` — ordered model candidates, using exact IDs from `model_catalog`;
- `provider_preferences` — project-level provider order, filters, fallback permission, and sort policy;
- `fallback_enabled` — project fallback switch.

An empty `default_fallback_models` list tells Flow to derive candidates from the project's enabled integrations. A configured list makes the intended order explicit. Read existing gateway settings before updating them, change only authorised fields, and preserve unrelated settings.

## Explicit Application Pinning

Pin a concrete model in application code only when the owner explicitly wants the application to own that decision or a specific workload requires it. Select the ID from the live catalogue and document that the application, rather than Reiver project settings, now owns the pin.

Managed prompt versions may also contain a model override. That is Reiver-owned configuration and takes precedence when applied; avoid duplicating the same routing decision in application code.
