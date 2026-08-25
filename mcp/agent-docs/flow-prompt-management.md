# Flow — Prompt Management

Prompt management lets teams version, test, and roll out system prompts centrally without redeploying applications. Prompts support A/B testing, canary rollouts, and template variables.

## Concepts

- **Prompt config** — A named prompt entity (e.g., "customer-support"). Referenced by name in gateway requests.
- **Prompt version** — An immutable snapshot of the system prompt, model, temperature, max_tokens, tools, allowed_tools, response_format, and template variables. Versions are numbered sequentially.
- **Rollout** — A progressive traffic shift from a baseline version to a target version. Supports staged rollout (10% → 50% → 100%) and A/B testing.

## Application Integration

Applications activate a managed prompt by referencing the config name in their request:

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

Requests without `prompt_config` pass through unchanged — no managed prompt is applied.

### How managed prompts are applied

- If the request has no system message, the managed prompt is injected as a new system message.
- If the request already has a system message, only managed system-prompt injection is skipped to avoid double-injection. Other prompt-version settings can still apply.
- All other messages (user, assistant, tool) are never modified.

### Override precedence

- A non-empty prompt-version `model` overrides the application's explicit model or the result of `model: "auto"`. Leave it unset to preserve the caller's model.
- The prompt-version temperature overrides the request temperature, subject to provider capability. Reiver omits it for Claude families that reject sampling controls.
- A positive prompt-version `max_tokens` overrides the request value.
- Request tools and response format take precedence when present; version values fill them only when absent. `allowed_tools` can then filter the active tool list.

For a baseline integration, do not apply a managed prompt. Prove one explicit provider/model path first, then introduce one override at a time and inspect `x-reiver-model-used`.

### Template variables

Managed prompts support Handlebars-style variables. Applications pass values at runtime:

```python
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
    extra_body={
        "prompt_config": "my-prompt-config",
        "prompt_variables": {
            "company_name": "Acme Corp",
            "user_name": "Alice"
        }
    }
)
```

Or via headers: `x-reiver-var-company-name: Acme Corp` (normalized to `company_name`).

Variables can have validation constraints: `required`, `default`, `type` (string/number/boolean/json/enum), `values`, `max_chars`, `min`/`max`.

### Rollout variant forcing (debugging)

During a rollout, applications can force a variant for testing:

```python
extra_headers={"x-reiver-force-variant": "target"}  # or "baseline"
```

### Allocation strategies

Rollouts support three allocation strategies:

- `random` — each request is randomly assigned based on weight
- `user_sticky` — same user gets the same variant (requires `x-reiver-user-id` header)
- `session_sticky` — same session gets the same variant (requires `x-reiver-session-id` header)

## Platform Management (MCP)

### Creating a prompt config

1. Create: `execute` with `resource: 'prompt', action: 'create_config', params: {name: '...', description: '...'}`
2. Verify: `get` with `resource: 'prompt_config', config_id: '...'`

### Creating and deploying a prompt version

1. Create the version: `execute` with `resource: 'prompt', action: 'create_version', params: {config_id, system_prompt, model, temperature, commit_message}` — the `system_prompt` field is the prompt content and must contain the complete prompt text
2. Read the version back to confirm content: `get` with `resource: 'prompt_version', config_id, version_id`
3. Deploy (should be explicitly requested by the user): `execute` with `resource: 'prompt', action: 'deploy', params: {config_id, target_version_id}` — this initiates a progressive rollout and live traffic begins flowing to the new version immediately
4. Monitor: `get` with `resource: 'rollout_metrics', rollout_id` to compare baseline vs target performance

### Managing rollouts

- Promote to next stage (should be explicitly requested by the user): `execute` with `resource: 'prompt', action: 'promote', params: {rollout_id}`
- Pause (should be explicitly requested by the user): `execute` with `resource: 'prompt', action: 'pause', params: {rollout_id}`
- Rollback (should be explicitly requested by the user): `execute` with `resource: 'prompt', action: 'rollback', params: {rollout_id}`
- Complete (should be explicitly requested by the user): `execute` with `resource: 'prompt', action: 'complete', params: {rollout_id}` — makes the target version active at 100%

### Listing and inspecting

- List configs: `list` with `resource: 'prompt_configs'`
- List versions: `list` with `resource: 'prompt_versions', config_id: '...'`
- List rollouts: `list` with `resource: 'rollouts'`
- Get rollout status: `get` with `resource: 'rollout', rollout_id: '...'`
