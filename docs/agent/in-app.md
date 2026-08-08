# In-App Agent (MooDeng)

Reiver includes a built-in AI assistant called **MooDeng** that runs directly in the web UI. It uses the same tools as the [MCP server](/agent/mcp-setup) to interact with the platform on your behalf.

## Setup

### 1. Configure an AI vendor integration

The in-app agent needs access to an LLM to work. Go to **Prompt Hub > Integrations** and add at least one AI vendor (OpenAI, Anthropic, Google, AWS Bedrock, etc.).

If no integrations are configured, the agent panel will show a message with a link to the integrations page.

### 2. Select an agent model

Go to **Prompt Hub > Settings** and find the **Agent Model** setting. You can either:

- **Pick a specific model** — choose from any model available through your configured integrations
- **Use "auto" mode** — the platform selects the best available model automatically, using the same logic as the LLM gateway's auto routing

### 3. Open the agent panel

Click the agent icon in the UI to open the panel. It appears as a resizable overlay on the right side of the screen.

## Using the Agent

Type a message in the input field and press Enter (or click Send). The agent will:

1. Analyze your request
2. Select and execute the appropriate tools
3. Stream the response back as it works

The agent maintains conversation history, so you can ask follow-up questions or refine requests within the same conversation.

### Example prompts

- "What's my LLM usage for the last 7 days?"
- "Show me the most frequent exceptions this week"
- "Create a dashboard from the API monitoring template"
- "Which alert rules are configured and which have fired recently?"
- "Deploy prompt config customer-support to version 3"

### Conversations

Conversations are stored per-project. You can:

- Start a new conversation at any time
- Switch between past conversations
- Delete conversations you no longer need

## Panel controls

- **Resize** — drag the left edge to adjust width
- **Fold** — click the collapse button to minimize the panel
- **Position** — the panel docks to the right side of the screen
- **Escape** — press Escape to close the panel

## Agent Permissions

The in-app agent has configurable scopes that control which tools it can use. Admins can adjust these in **Prompt Hub > Settings** under the **Agent Scopes** setting.

By default, MooDeng is configured with read-only scopes:

- `project:read`
- `llm:read`
- `observability:read`
- `billing:read`

To allow MooDeng to make changes (e.g., create alert rules, deploy prompts, configure integrations), add the corresponding write scopes. See [Scoped Permissions](/agent/mcp-setup#scoped-permissions) for the full scope reference.

All conversations are scoped to the current project — MooDeng can only access data and tools within the project you are viewing.

## How it works

The in-app agent uses the same action layer as the MCP server. When you send a message:

1. Your message is sent to the Flow service along with the conversation history
2. Flow calls the configured LLM with the available tools
3. The LLM decides which tools to call (if any) and generates a response
4. Tool results are fed back to the LLM for a final answer
5. The response streams back to the UI via Server-Sent Events

All tool calls go through the same REST APIs that the web UI uses, filtered by the agent's configured scopes.

## Auto-Investigation

MooDeng can automatically investigate incidents when they occur, so your team gets a diagnosis alongside the alert — without anyone having to open the UI.

### What gets investigated

When auto-investigation is enabled, MooDeng triggers on:

- **Alert rule firings** — when an alert rule transitions to ALERT state (metric threshold breached, data absent, etc.)
- **New exception groups** — the first time a new exception fingerprint is seen
- **Exception regressions** — when a previously-resolved exception group starts occurring again

### How it works

1. Watch detects the event and sends the normal alert/exception notification to your configured channels (Slack, Teams, Discord, PagerDuty, webhook).
2. Watch then asks Flow to run a MooDeng investigation in the background.
3. MooDeng uses the project's MCP tools (search logs, list traces, query metrics, etc.) to investigate the root cause.
4. When finished, the findings are posted to the same notification channels that received the original alert.

Investigations are stored in the `agent_investigations` table as an audit trail with full tool-call logs, the model used, and token counts.

### Enabling auto-investigation

Go to **Prompt Hub > Settings**, find the **MooDeng** section, and toggle **Auto-Investigate Alerts & Exceptions**.

Requirements:
- The in-app agent must be enabled (the MooDeng toggle)
- At least one AI provider integration must be configured
- Notification channels must be set up for the project (otherwise there's nowhere to post findings)

The investigation uses the same **Agent Scopes** configured for MooDeng. For effective investigations, ensure at least `observability:read` is included (it is by default).

### Cooldown

To avoid redundant work, MooDeng skips an investigation if one is already running for the same trigger (same alert rule or exception fingerprint) within the last 10 minutes.

### Investigation findings

Findings are posted to your notification channels in a format appropriate for each channel type:

| Channel   | Format |
|-----------|--------|
| Slack     | Attachment with investigation summary |
| Teams     | MessageCard with findings and trigger details |
| Discord   | Embed with description and footer |
| PagerDuty | Info-severity event with custom details |
| Webhook   | Structured JSON with `type: "investigation_complete"` |
