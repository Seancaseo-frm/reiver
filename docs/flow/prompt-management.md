# Prompt Management

Flow's prompt management lets you version, test, and roll out system prompts centrally — without redeploying your application. You can A/B test prompt variants, do canary rollouts, and use template variables to personalize prompts at runtime.

## Typical Workflow

Most teams follow this progression:

### 1. Connect to the Gateway

Point your application at Flow by changing the `base_url`. Your app works identically — all messages including your system prompt pass through untouched.

```python
from openai import OpenAI

client = OpenAI(
    api_key="dh_your_key",
    base_url="https://reiver.ai/api/gateway/v1"
)
```

At this point, prompt management is not active. Requests pass through the gateway untouched.

### 2. Build Your Prompts

Create prompt configs and versions in the Reiver UI. Each version can include:

- **System prompt** (with optional template variables)
- **Model override** (e.g., pin a prompt to `gpt-4o`)
- **Temperature and max_tokens**
- **Tools / function definitions**
- **Allowed tools** (tool whitelist) — restricts which tools the LLM is permitted to call when this prompt version is active. If the LLM attempts a tool call not in the list, the gateway rejects the response. Leave empty to allow all tools.
- **Response format** (for structured output / JSON mode)

Versions are stored and versioned but have **no effect on production traffic** until you reference them in requests.

### 3. Activate via `prompt_config`

To use a managed prompt, reference the config name in your request — either via header or body field:

```python
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
    extra_body={"prompt_config": "my-prompt-config"}
)
```

Or via header:

```python
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
    extra_headers={"x-reiver-prompt-config": "my-prompt-config"}
)
```

Requests that do **not** include a `prompt_config` pass through unchanged — no managed prompt is applied and no rollout is evaluated.

### 4. Roll Out Gradually

Once you're confident in a new prompt version, create a rollout on the config to gradually shift traffic from the baseline version to the target. See [Rollouts](#rollouts) below.

## How Managed Prompts Are Applied

When a managed prompt is resolved (i.e. the request includes a `prompt_config`):

- If your request **has no system message**, a new one is created with the managed prompt.
- If your request **already has a system message**, only managed system-prompt injection is **skipped** — your messages are preserved exactly as sent. The version's other settings can still apply.
- All other messages (user, assistant, tool) are never modified.

### Override precedence

| Setting | What wins when `prompt_config` is present |
|---|---|
| System message | The managed prompt is injected only when the request has no system message |
| Model | A non-empty model on the prompt version overrides the application's explicit model or the result of `model: "auto"`; leave the version model unset to preserve the caller's selection |
| Temperature | The prompt version's temperature overrides the request value, subject to provider capability; Reiver omits it for Claude families that reject sampling controls |
| Max tokens | A positive prompt-version value overrides the request value; otherwise the request value remains |
| Tools | Request tools remain; version tools fill in only when the request has none, then `allowed_tools` filters the result |
| Response format | The request value remains; the version value fills in only when the request has none |

This is why the verified onboarding baseline does not apply a managed prompt: first prove the application's explicit provider/model route, then introduce one override at a time and inspect `x-reiver-model-used`.

## Rollouts

A rollout lets you gradually shift traffic from a **baseline** prompt version to a **target** version. Rollouts support:

- **Staged rollout**: Define stages with increasing target weight (e.g., 10% → 50% → 100%).
- **A/B testing**: Split traffic between baseline and target, then compare metrics.

### Allocation Strategies

| Strategy | Description |
|----------|-------------|
| **`random`** | Each request is randomly assigned to baseline or target based on weight. |
| **`user_sticky`** | The same user always gets the same variant. Requires `x-reiver-user-id` header. |
| **`session_sticky`** | The same session always gets the same variant. Requires `x-reiver-session-id` header. |

### Rollout Statuses

| Status | Description |
|--------|-------------|
| `pending` | Created but not yet active. |
| `running` | Actively routing traffic. |
| `paused` | Temporarily stopped; requests fall through without a managed prompt. |
| `completed` | Finished; the target version is now the default. |
| `rolled_back` | Reverted to baseline. |

### Force a Variant (Debugging)

During a rollout, you can force a specific variant for testing:

```python
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
    extra_headers={"x-reiver-force-variant": "target"}
)
```

Values: `"target"` or `"baseline"`.

## Template Variables

Managed prompts support Handlebars-style template variables. Define variables in the prompt version, then pass values at runtime:

**Prompt template:**
```
You are a helpful assistant for {{company_name}}.
The user's name is {{user_name}}.
```

**Pass variables via headers:**
```python
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
    extra_headers={
        "x-reiver-var-company-name": "Acme Corp",
        "x-reiver-var-user-name": "Alice"
    }
)
```

**Or via request body** (no 255-character limit):
```python
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
    extra_body={
        "prompt_variables": {
            "company_name": "Acme Corp",
            "user_name": "Alice"
        }
    }
)
```

Header names are normalized: `x-reiver-var-company-name` becomes the variable `company_name`. Header variables take precedence over body variables when the same key appears in both.

### Variable Validation

Each variable can define a schema with constraints (stored on the prompt version as a JSON array). The Flow gateway deserializes these with a single schema: use the JSON key **`type`** for the logical type; legacy payloads may use **`var_type`** as an alias.

- **`required`** — must be provided (or have a default).
- **`default`** — value used when the variable is not supplied.
- **`type`** — `string`, `number`, `boolean`, `json`, or `enum` (alias: `var_type`).
- **`values`** — allowed values for enum-typed variables.
- **`max_chars`** — maximum character length.
- **`min` / `max`** — numeric bounds.

If a variable fails validation, the request returns a `422` error with details about which variable and what constraint failed.

**Distinct from output validation:** constraining **inputs** (`variables` / `prompt_variables`) is separate from **`response_format`** (JSON Schema) on the same prompt version, which the gateway can enforce against the **model’s JSON output** after a non-streaming call. See the Flow README section on output contract enforcement.

## Summary Table

| Scenario | Managed system prompt injected? | Your system prompt |
|----------|-------------------------|--------------------|
| No `prompt_config` in request | No | Preserved exactly |
| `prompt_config` present, no system message in request | Yes — injected as system message | N/A |
| `prompt_config` present, system message already in request | No — skipped to avoid double-injection; other version settings can still apply | Preserved exactly |
