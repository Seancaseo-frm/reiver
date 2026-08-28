# MCP Setup

Connect AI agents to Reiver using the Model Context Protocol. Configure your MCP client to connect to the Reiver API with an agent token.

::: tip What this unlocks
After completing setup, your agent can use Reiver's currently exposed MCP resources and tools within the scopes on its token. Read-only scopes can verify projects, Flow records, traces, logs, metrics, dashboards, and alerts without granting write access.
:::

## Prerequisites

- A Reiver project with an **agent token** (create one under **Settings → Agent Tokens** in your project)

The coding agent uses `REIVER_AGENT_TOKEN`. Your application uses an SDK key, which may currently be bound separately as `REIVER_FLOW_API_KEY` and `REIVER_WATCH_API_KEY`. A provider key stays inside Reiver. These credentials are not interchangeable, and none belongs in code, documentation examples, logs, or reports.

::: warning SDK keys are not accepted
MCP requires an agent token. Regular SDK API keys will be rejected with a 403 error. Agent tokens provide scoped access specifically designed for AI agent integrations — they can be individually revoked and are tracked separately in audit logs.
:::

Start with read-only scopes such as `project:read`, `llm:read`, and `observability:read`, adding only the scopes needed for an approved task. Flow and Watch onboarding do not require MCP write access.

The current MCP server does not expose `agent://onboarding`. Use the human [Start Here](/quickstart) guide; onboarding-specific MCP guidance is deliberately deferred to a later MCP release.

## Cursor

Add the following to your Cursor MCP configuration (`.cursor/mcp.json` in your project root or `~/.cursor/mcp.json` for global settings):

```json
{
  "mcpServers": {
    "reiver": {
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
export REIVER_AGENT_TOKEN="dh_agent_..."
```

Add the export to your shell profile (`~/.zshrc`, `~/.bashrc`, etc.) so it persists across sessions. Restart Cursor after making changes.

::: tip
Cursor uses `${env:VAR_NAME}` syntax to interpolate environment variables in MCP config. This keeps tokens out of version control.
:::

## Claude Code

Add Reiver for your current project:

```bash
claude mcp add --transport http --header "Authorization: Bearer $REIVER_AGENT_TOKEN" reiver \
  https://reiver.ai/mcp
```

Or make it available across all your projects:

```bash
claude mcp add --transport http --scope user --header "Authorization: Bearer $REIVER_AGENT_TOKEN" reiver \
  https://reiver.ai/mcp
```

Set the environment variable in your shell profile (`~/.zshrc`, `~/.bashrc`, etc.) before running the command:

```bash
export REIVER_AGENT_TOKEN="dh_agent_..."
```

To share the config with your team via `.mcp.json`, use the `mcp-remote` bridge (requires Node.js):

```json
{
  "mcpServers": {
    "reiver": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "https://reiver.ai/mcp",
        "--header",
        "Authorization:Bearer ${REIVER_AGENT_TOKEN}"
      ]
    }
  }
}
```

Since Claude Code inherits your shell environment, `${REIVER_AGENT_TOKEN}` is resolved at runtime from the export above.

::: danger Do not paste tokens directly into config files
MCP config files are readable by every agent and tool that has access to your project. If a token appears as a literal value in the file, any MCP server — not just Reiver — can read it. Always use environment variable references so the secret stays in your shell profile.
:::

## Claude Desktop

Claude Desktop does not natively support remote HTTP servers with custom headers. Use a wrapper script with the `mcp-remote` bridge instead.

### 1. Set the environment variable

Add the export to your shell profile (`~/.zshrc`, `~/.bashrc`, etc.):

```bash
export REIVER_AGENT_TOKEN="dh_agent_..."
```

### 2. Create a wrapper script

Claude Desktop is a GUI app and does not inherit shell environment variables. A small wrapper script sources your profile so the token is available:

```bash
cat > ~/.local/bin/reiver-mcp.sh << 'EOF'
#!/bin/sh
. "$HOME/.zshrc" 2>/dev/null || . "$HOME/.bashrc" 2>/dev/null
exec npx -y mcp-remote \
  https://reiver.ai/mcp \
  --header "Authorization:Bearer ${REIVER_AGENT_TOKEN}"
EOF
chmod +x ~/.local/bin/reiver-mcp.sh
```

### 3. Add to Claude Desktop config

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "reiver": {
      "command": "sh",
      "args": ["-c", "~/.local/bin/reiver-mcp.sh"]
    }
  }
}
```

Restart Claude Desktop. You should see Reiver's tools available in the tool picker.

## Other MCP Clients

Any MCP client that supports Streamable HTTP transport can connect. Point it at:

| Setting | Value |
|---------|-------|
| **URL** | `https://reiver.ai/mcp` |
| **Auth** | `Authorization: Bearer <your-agent-token>` |

## Scoped Permissions

Agent tokens have configurable scopes that control which tools the agent can access. Tools requiring a scope the token doesn't have are hidden entirely.

By default, new agent tokens are created with **read-only scopes**. A write scope implicitly grants the corresponding read scope — for example, `llm:write` also grants `llm:read`.

To grant an agent write access, include the write scopes when creating the agent token in project settings.

### General

#### `project:read`
View projects, API keys, and project statistics.

> *"List my projects"* · *"Show the API keys for this project"* · *"What are my exception stats?"*

#### `project:write`
Create and update projects, generate new API keys.

> *"Create a new project called mobile-app"* · *"Generate an API key for the staging project"*

#### `billing:read`
View billing usage, per-project breakdowns, forecasts, and budget status.

> *"What's my current usage?"* · *"Show the usage forecast for this billing period"* · *"Am I near my budget limit?"*

### Flow (Prompt Hub & LLM Gateway)

#### `llm:read`
View provider integrations, gateway settings, prompt configs, sessions, metrics, playground results, and evaluation scores.

> *"What providers are configured?"* · *"Show me LLM cost for the last 7 days"* · *"List prompt configs"* · *"What's the latency breakdown by model?"*

#### `llm:write`
Configure provider integrations (add/rotate API keys), update gateway settings, create and deploy prompt versions, manage rollouts, submit evaluation scores.

> *"Add an Anthropic integration with this API key"* · *"Deploy prompt config customer-support to version 3"* · *"Enable PII masking in the guardrails"* · *"Rollback the current rollout"*

### Watch (APM)

#### `observability:read`
View exceptions, traces, logs, incidents, dashboards, alert rules, fired alerts, health checks, profiling data, API endpoints, services, maintenance windows, and notification channels.

> *"Show me the top exceptions this week"* · *"Get trace abc-123"* · *"Search logs for 'timeout'"* · *"Which alert rules have fired?"* · *"List health checks"*

#### `observability:write`
Create and modify dashboards, widgets, alert rules, notification channels (Slack, Teams, Discord, PagerDuty, ServiceNow, webhooks), health checks, maintenance windows, and cloud integrations (AWS, Azure, GCP, OCI). Link GitHub repos.

> *"Create a dashboard from the API monitoring template"* · *"Add a Slack notification channel"* · *"Create an alert rule for error rate > 5%"* · *"Set up a maintenance window for Saturday 2-4am"*

## Verifying the Connection

Once connected, try asking your agent:

- "List my Reiver projects"
- "Show me the LLM gateway overview for the last 7 days"
- "What alert rules are configured?"

## Next Steps

- [Available Tools](/agent/tools) — full reference of all 6 tools and their operations
- [In-App Agent](/agent/in-app) — use the built-in AI assistant in the Reiver UI
