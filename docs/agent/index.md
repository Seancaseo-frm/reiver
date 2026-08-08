# AI Agent

Reiver exposes its full platform to AI agents through the **Model Context Protocol (MCP)**. Agents can manage projects, configure prompts, query APM data, build dashboards, set up alerts, and check billing — all through 6 high-level tools that cover over 100 operations.

There are two ways to use it:

## External Agents (MCP)

Connect AI agents like **Claude Desktop**, **Cursor**, **OpenClaw**, or any MCP-compatible client to Reiver. The MCP server is hosted at `reiver.ai/mcp` and uses Streamable HTTP transport — no local binary required. Authenticate with an **agent token** (created in project settings) and your agent can start making tool calls immediately.

This is useful for:

- Automating project setup and configuration from your IDE
- Querying APM data and building dashboards conversationally
- Managing prompt versions and rollouts without the UI
- Integrating Reiver into agentic workflows and pipelines

See [MCP Setup](/agent/mcp-setup) to get started, and [Available Tools](/agent/tools) for the full reference.

## In-App Agent

Reiver also includes a built-in AI assistant in the web UI. It appears as a resizable overlay panel and uses the same MCP tools to act on your behalf within the platform.

To use it:

1. Configure at least one AI vendor integration (OpenAI, Anthropic, etc.) in your project's LLM settings
2. Select a model for the agent (or use "auto" mode)
3. Open the agent panel from the UI

The in-app agent can answer questions about your project, help configure settings, and execute actions — all within the context of your current project.

See [In-App Agent](/agent/in-app) for details.

## Architecture

The MCP server is a thin orchestration layer that sits between agents and the existing Reiver REST APIs:

```mermaid
graph LR
    Agent["AI Agent"] -->|MCP Protocol| MCPServer["MCP Server"]
    MCPServer -->|REST API| Website["Website"]
    MCPServer -->|REST API| Flow["Flow"]
    MCPServer -->|REST API| Watch["Watch"]
```

No business logic is duplicated. Every tool call delegates to the same API endpoints used by the web UI, so agents always see the same data and have the same capabilities as human users.
