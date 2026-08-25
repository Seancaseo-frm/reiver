# MCP Setup

Connect AI agents to Reiver using the Model Context Protocol. Configure your MCP client to connect to the Reiver API with an agent token.

::: tip What this unlocks
After completing setup, your agent can read Reiver's onboarding contract, query traces and logs, manage prompt configurations, run playground comparisons, create dashboards and alert rules, and access the platform through 5 focused tools.
:::

## Prerequisites

- A Reiver project with an **agent token** (create one under **Agents → Tokens** in your project)

::: warning SDK keys are not accepted
MCP requires an agent token. Regular SDK API keys will be rejected with a 403 error. Agent tokens provide scoped access specifically designed for AI agent integrations — they can be individually revoked and are tracked separately in audit logs.

SDK keys and agent tokens currently both begin with `dh_`; the visible prefix does not identify the key type. Label the secret `REIVER_AGENT_TOKEN` and copy the value from **Agents → Tokens**, not from the SDK-key list.
:::

## Codex

Add this to `.codex/config.toml` in the project:

```toml
[mcp_servers.reiver]
url = "https://reiver.ai/mcp"
bearer_token_env_var = "REIVER_AGENT_TOKEN"
```

Set `REIVER_AGENT_TOKEN` in the environment that launches Codex, then restart the client.

Run `codex mcp list` to confirm that `reiver` is configured. In the Codex TUI, `/mcp` shows the active connection. A listed server is only a transport check; finish by asking Codex to read `agent://onboarding`.

## Cursor

Add the following to your Cursor MCP configuration (`.cursor/mcp.json` in your project root or `~/.cursor/mcp.json` for global settings):

```json
{
  "mcpServers": {
    "reiver": {
      "type": "http",
      "url": "https://reiver.ai/mcp",
      "headers": {
        "Authorization": "Bearer ${env:REIVER_AGENT_TOKEN}"
      }
    }
  }
}
```

Then set the environment variable with your agent token:

```bash
export REIVER_AGENT_TOKEN="dh_..."
```

Add the export to your shell profile (`~/.zshrc`, `~/.bashrc`, etc.) so it persists across sessions. Restart Cursor after making changes.

Open **Cursor Settings → Tools & MCP** and confirm that `reiver` is enabled. Then ask the agent to list Reiver resources and read `agent://onboarding`; the settings indicator alone is not proof that authentication and resource access work.

::: tip
Cursor uses `${env:VAR_NAME}` syntax to interpolate environment variables in MCP config. This keeps tokens out of version control.
:::

## Claude Code

Add the native remote HTTP configuration to the project `.mcp.json`:

```json
{
  "mcpServers": {
    "reiver": {
      "type": "http",
      "url": "https://reiver.ai/mcp",
      "headers": {
        "Authorization": "Bearer ${REIVER_AGENT_TOKEN}"
      }
    }
  }
}
```

Claude Code resolves `${REIVER_AGENT_TOKEN}` from its launch environment. Restart it after changing the environment or project configuration.

Run `claude mcp list` and `claude mcp get reiver`, then approve the project-scoped server when Claude Code prompts. You can also inspect the connection with `/mcp`. Finish by asking Claude Code to read `agent://onboarding`.

::: danger Do not paste tokens directly into config files
MCP config files are readable by every agent and tool that has access to your project. If a token appears as a literal value in the file, any MCP server — not just Reiver — can read it. Always use environment variable references so the secret stays in your shell profile.
:::

## Other MCP Clients

Any MCP client that supports Streamable HTTP transport can connect. Point it at:

| Setting | Value |
|---------|-------|
| **URL** | `https://reiver.ai/mcp` |
| **Auth** | `Authorization: Bearer <your-agent-token>` |

## Scoped Permissions

Agent tokens have configurable scopes that control which tools the agent can access. Tools requiring a scope the token doesn't have are hidden entirely.

For onboarding, select the read scopes you need—normally `llm:read` and `observability:read`. A write scope implicitly grants the corresponding read scope—for example, `llm:write` also grants `llm:read`.

To grant an agent write access, include the write scopes when creating the token under **Agents → Tokens**.

### Delegating autonomy without another settings screen

Reiver has no separate autonomy-mode selector. The token scopes are the hard technical boundary; the instruction you give the coding agent defines its behavioural authority inside that boundary.

- For an evaluation, grant `llm:read` and `observability:read` and ask the agent to inspect, verify and recommend only.
- For autonomous Flow setup, grant `llm:write` and tell the agent which prompt, rollout, label, profile and gateway changes it may make.
- Add `observability:write` when it may also create dashboards, alerts or other Watch configuration.

If the owner's instruction clearly authorises autonomous onboarding, the agent should not request approval again for every in-scope action. If the first material write is not clearly authorised, it should ask one focused question before writing. A broad write scope is capability, not permission to change unrelated resources.

For example:

```text
Integrate and configure Reiver autonomously. Establish and prove a simple
working baseline first. You may then create, test and roll out relevant prompts,
configure business-specific session labels, profiles and guardrails, and create
useful dashboards and alerts within the scopes granted to this token. Verify
every change through MCP and report it. Do not delete existing resources, change
provider credentials, increase budgets, weaken safety controls or modify
unrelated production resources without asking.
```

### General

#### `project:read`
View projects, API-key metadata, and project statistics. Existing secret values are not returned.

> *"List my projects"* · *"Show key labels and suffixes for this project"* · *"Show this project's summary statistics"*

#### `project:write`
Create and update projects and generate new keys. A newly generated secret is exposed to the connected agent once, so this scope is not needed for normal application onboarding.

> *"Create a new project called mobile-app"* · *"Generate an API key for the staging project"*

#### `billing:read`
View billing usage, per-project breakdowns, forecasts, and budget status.

> *"What's my current usage?"* · *"Show the usage forecast for this billing period"* · *"Am I near my budget limit?"*

### Flow (Prompt Hub & LLM Gateway)

#### `llm:read`
View provider integrations, gateway settings, prompt configs, sessions, metrics, playground results, and evaluation scores.

> *"What providers are configured?"* · *"Show me LLM cost for the last 7 days"* · *"List prompt configs"* · *"What's the latency breakdown by model?"*

#### `llm:write`
Configure provider integrations, update gateway settings, create and deploy prompt versions, manage rollouts, and submit evaluation scores. Provider credential operations expose a highly sensitive secret to the connected agent and should not be used during normal application onboarding; enter provider keys directly in Reiver instead.

> *"Deploy prompt config customer-support to version 3"* · *"Enable PII masking in the guardrails"* · *"Rollback the current rollout"*

### Watch (APM)

#### `observability:read`
View exceptions, traces, logs, incidents, dashboards, alert rules, fired alerts, health checks, profiling data, API endpoints, services, maintenance windows, and notification channels.

> *"Show me the top exceptions this week"* · *"Get trace abc-123"* · *"Search logs for 'timeout'"* · *"Which alert rules have fired?"* · *"List health checks"*

#### `observability:write`
Create and modify dashboards, widgets, alert rules, notification channels (Slack, Teams, Discord, PagerDuty, ServiceNow, webhooks), health checks, maintenance windows, and cloud integrations (AWS, Azure, GCP, OCI). Link GitHub repos.

> *"Create a dashboard from the API monitoring template"* · *"Add a Slack notification channel"* · *"Create an alert rule for error rate > 5%"* · *"Set up a maintenance window for Saturday 2-4am"*

## Verifying the Connection

Once connected, ask your agent:

- "List Reiver's documentation resources and read `agent://onboarding`."
- "List my Reiver projects"
- "Show me the LLM gateway overview for the last 7 days"
- "List recent metric names and search for a recent application log"

The connection is ready for application onboarding only when the agent can read `agent://onboarding`. Platform data queries additionally require the corresponding read scope.

## Next Steps

- [Choose your Reiver path](/quickstart) — independent Flow, Watch, and Complete acceptance criteria
- [Available Tools](/agent/tools) — full reference for the 5 facade tools
- [In-App Agent](/agent/in-app) — use the built-in AI assistant in the Reiver UI
