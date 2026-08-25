# Reiver MCP Server

MCP (Model Context Protocol) server that exposes the Reiver platform to AI agents. Supports two transports:

- **stdio** — for local agents. Authenticates with an MCP agent token at startup.
- **HTTP** — for deployment behind the website proxy. Authenticates per-request via trusted `X-Project-Id` headers. No API key needed at startup.

Application-onboarding agents must read `agent://onboarding` first, select the Flow, Watch, or Complete track, and apply only that track's definition of done. Session or cross-product correlation also requires `agent://flow/session-telemetry`.

## Quick Start

### stdio (local agent)

```bash
# Set a scoped MCP agent token. Do not use an application SDK key here.
export REIVER_API_KEY="your-agent-token"

# Run with stdio (default)
reiver-mcp
```

### HTTP (server deployment)

In HTTP mode the server sits behind the website proxy, which authenticates incoming requests and forwards them with `X-Project-Id`. No API key is required.

```bash
reiver-mcp --transport http --listen 0.0.0.0:3002
```

Direct connections (without the proxy) are also supported — agents pass a `Bearer` API key in the `Authorization` header, and the server validates it against the website.

## Claude Desktop / Cursor Configuration

Add the following to your MCP client configuration (e.g., `~/.config/claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "reiver": {
      "command": "/path/to/reiver-mcp",
      "args": ["--transport", "stdio"],
      "env": {
        "REIVER_API_KEY": "<your-agent-token>",
        "WEBSITE_URL": "https://reiver.ai",
        "FLOW_URL": "https://reiver.ai",
        "WATCH_URL": "https://reiver.ai"
      }
    }
  }
}
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REIVER_API_KEY` | — | MCP agent token (required for **stdio** transport only) |
| `WEBSITE_URL` | `http://localhost:80` | Website API base URL |
| `FLOW_URL` | `http://localhost:3001` | Flow (LLM gateway) API base URL |
| `WATCH_URL` | `http://localhost:3003` | Watch (APM) API base URL |

## Available Tools (5 facade tools)

The MCP server exposes **5 high-level tools** that dispatch to the underlying platform operations. Each tool uses a discriminator field to route to the right action.

| Tool | Discriminator | Purpose |
|------|---------------|---------|
| `search` | `source` | Find resources by text query (LLM requests, logs, web) |
| `get` | `resource` | Retrieve a specific resource by type + ID (21 resource types) |
| `list` | `resource` | Browse/list resources with optional filters (26 resource types) |
| `analyze` | `analysis` | Metrics, analytics, comparisons, diagnostics (18 analysis types) |
| `execute` | `resource` + `action` | Create, update, configure, deploy, test, run (14 resources, 39 actions) |

## Architecture

The MCP server is a thin orchestration layer:

1. **Authentication**:
   - *stdio*: Validates the MCP agent token against the website on connection, creating a single `ActionContext` for the session.
   - *HTTP*: Reads `X-Project-Id` from each request (set by the website proxy after authenticating the caller). Builds a per-request `ActionContext`. Also accepts direct `Bearer` API key auth for connections that bypass the proxy.
2. **Action Registry**: Maps MCP tool names to typed `PlatformAction` implementations.
3. **Internal HTTP Client**: Actions call the existing REST APIs (website, flow, watch) internally.
4. **MCP Protocol**: stdio uses `rmcp` for JSON-RPC over stdin/stdout. HTTP uses a custom stateless JSON-RPC handler over Axum.

No business logic is duplicated — every action delegates to the existing API endpoints.

### Why `execute` params are untyped

The `execute` tool routes to 39 resource/action pairs, each with a different param schema. The natural representation is a discriminated union (`oneOf` in JSON Schema), where the `resource`/`action` pair selects which variant applies to `params`.

However, LLM tool-calling APIs (OpenAI, DeepSeek, Claude) require the top-level schema to be `type: "object"` and reject schemas containing `oneOf`. A discriminated union with 39 variants produces `oneOf`, which breaks tool registration across all major providers. This was discovered in production and fixed in commits `43cecdff` and `f9b29622` by changing `params` from a typed enum to an opaque `serde_json::Value`.

To compensate for the lost schema, all param schemas are documented inline in the tool's `description()` string. LLMs read this as natural language context and use it to construct correct `params` objects. This is more robust than a formal schema that providers silently reject.

**When adding a new resource/action pair**, always update the description in `execute_action.rs` with the new params schema, an example if the action is non-trivial, and add a corresponding `match` arm in the `execute()` method.

## Building

```bash
cd mcp
cargo build --release
```

## Docker

The Dockerfile builds the HTTP transport by default (for K8s deployment):

```bash
docker build -f deploy/docker/Dockerfile.mcp -t reiver-mcp .
docker run reiver-mcp
```

For stdio (local use), pass the MCP agent token:

```bash
docker run -e REIVER_API_KEY=... reiver-mcp ./reiver-mcp --transport stdio
```
