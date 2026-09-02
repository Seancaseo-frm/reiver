# Flow — Routing

Flow can own model selection, provider selection, and failover at the Reiver project level. This is the default onboarding pattern because routing policy can change without an application release.

## Reiver-Owned Application Contract

The application sends `model: "auto"` and omits request-level `models` and `provider` fields:

```python
from openai import OpenAI

client = OpenAI(base_url="https://reiver.ai/api/gateway/v1", api_key="dh_...")

response = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "Hello"}],
)
```

The response headers identify the actual outcome: `x-reiver-provider`, `x-reiver-model-used`, `x-reiver-fallback-used`, `x-reiver-original-model`, and `x-reiver-retry-count`.

## Platform Management (MCP)

1. Call `list` with `resource: "model_catalog"`. Use only exact IDs returned by this live, project-filtered catalogue.
2. Call `get` with `resource: "gateway_settings"` and preserve unrelated settings.
3. When the owner has authorised the routing policy, call `execute` with `resource: "gateway"`, `action: "update_settings"`, and only the fields being changed.

Example project routing update:

```json
{
  "resource": "gateway",
  "action": "update_settings",
  "params": {
    "settings": {
      "fallback_enabled": true,
      "default_fallback_models": [
        "<first-model-id-from-model_catalog>",
        "<second-model-id-from-model_catalog>"
      ],
      "provider_preferences": {
        "order": ["<preferred-provider-slug>"],
        "allow_fallbacks": true,
        "sort": "latency"
      }
    }
  }
}
```

- `default_fallback_models` is the ordered project candidate list used for `model: "auto"` and as the fallback chain when a request omits `models`.
- An empty `default_fallback_models` list lets Flow derive candidates from the project's enabled integrations.
- `provider_preferences` supplies project defaults for `order`, `only`, `ignore`, `allow_fallbacks`, and `sort` when the request omits `provider`.
- Do not add providers, weaken policy, or change fallback behaviour merely to complete onboarding.

## Advanced Application-Owned Overrides

Per-request routing fields remain available for an application that explicitly owns routing. They override project defaults and couple policy to the application release, so do not add them during normal Reiver-owned onboarding.

```python
response = client.chat.completions.create(
    model="<primary-model-id-from-model_catalog>",
    messages=[{"role": "user", "content": "Hello"}],
    extra_body={
        "models": ["<fallback-model-id-from-model_catalog>"],
        "provider": {
            "only": ["<provider-slug>"],
            "allow_fallbacks": True,
        },
    },
)
```

The primary model is tried first. Eligible failures include upstream 5xx responses, rate limits, and timeouts. Up to five request-level fallback models are accepted, and gateway-only routing fields are removed before forwarding upstream.

## Precedence

Model candidates are resolved in this order:

1. request-level `models`;
2. project `default_fallback_models`;
3. models derived from enabled project integrations.

Request-level `provider` preferences replace project `provider_preferences`. When the effective preferences set `allow_fallbacks`, that value controls failover; otherwise both the project fallback switch and server fallback capability must permit it.

A managed prompt version may override the model after initial request resolution. Keep that override in Reiver and do not duplicate it in application code unless the precedence is intentional.

## Safe Failover Verification

An MCP Playground request with the model omitted uses `"auto"` and exercises the project's configured routing chain. A normal success verifies routing but does not prove that the secondary candidate can take over.

Exercise a failure only through an existing staging or automated fault-injection path. Verify the returned actual model/provider and fallback indicator. Do not disable a production integration, alter provider credentials, or cause a production outage just to test failover. When no safe fault path exists, report the routing configuration as verified and failover execution as not tested.
