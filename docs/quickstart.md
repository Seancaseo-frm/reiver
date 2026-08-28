# Start Here

Choose one track. Flow and Watch are independently useful, and neither requires MCP. Complete Reiver joins them when you need model and application evidence around the same customer episode.

## Credentials: three different jobs

| Credential | Where it belongs | What it does |
|---|---|---|
| Provider key | Inside Reiver | Lets Flow call a model provider. It does not belong in the application. |
| SDK key | Application secret store | Authenticates application traffic to Flow and Watch. The same SDK-key value may currently be bound separately as `REIVER_FLOW_API_KEY` and `REIVER_WATCH_API_KEY`. |
| Agent token | Coding-agent secret store | `REIVER_AGENT_TOKEN` authenticates MCP and is limited by its scopes. |

These credentials are not interchangeable. No credential belongs in source code, documentation examples, telemetry, logs, screenshots, or reports.

## Track 1: Flow + Prompt Hub

Use this when you want one gateway for model calls or centrally managed prompts. It does **not** require Watch, application logs, application metrics, or MCP.

1. Add a provider key in Reiver and test the provider connection.
2. Create an SDK key and expose it to the application as `REIVER_FLOW_API_KEY`.
3. Follow [Getting Started with Flow](/flow/getting-started) to send one real application request.
4. Add a managed prompt only if central prompt control is part of your goal.

**Done means:** a real application request succeeds through the Flow gateway; its actual provider and model can be identified; and no credential appears in code, logs, or the verification report.

## Track 2: Watch

Use this when you want application observability without changing model routing. It does **not** require a provider key, a Flow request, or MCP write access.

1. Create an SDK key and expose it to the application as `REIVER_WATCH_API_KEY`.
2. Build real trace, structured-log, and metric pipelines as described in [Watch](/watch/).
3. Generate one known item from each pipeline and find it in Reiver.

An OTLP endpoint is only a destination. It does not install instrumentation, collect stdout, create structured logs, or produce metrics.

**Done means:** a real application trace, a known structured log, and a known application or runtime metric are each queryable under the intended stable service name. The SDK key is absent from all three signals and from the report.

## Track 3: Complete Reiver

Use this when you want Flow decisions and application behavior connected to the same business episode.

1. Complete the Flow and Watch tracks.
2. Agree the [Session and Identity Contract](/flow/session-telemetry) before choosing identifiers.
3. Send the same session and stable pseudonymous user IDs through Flow and application telemetry.
4. Explicitly end the session when its business episode ends.
5. Start a second episode with a new session ID and the same stable pseudonymous user ID.
6. Optionally connect MCP with a separate agent token for read-only verification. MCP is not required to send application traffic.

**Done means:**

| Evidence | Acceptance check |
|---|---|
| Flow | A real request succeeded and its actual provider and model are identifiable. |
| Watch | One trace, structured log, and metric are queryable. |
| Correlation | Flow and Watch evidence carry the agreed session and stable pseudonymous user IDs. |
| Session end | The application explicitly ended the first session; the inactivity timeout remains fallback only. |
| Continuity | A second session has a new session ID and the same pseudonymous user ID. |
| Agent verification | If MCP is used, the agent reads only resources supported by its token and reports evidence without sensitive content. |
| Secrets | No provider key, SDK key, or agent token appears in code, telemetry, logs, or reports. |

## Shared Session and Identity Contract

A session is the smallest meaningful business episode the customer wants to evaluate and improve—not simply a browser visit or arbitrary timeout window.

Before instrumentation, the customer or delegated agent must define:

- the session unit;
- the event that starts it;
- its successful end;
- abandonment and failure endings;
- an inactivity fallback;
- a stable pseudonymous user ID;
- how anonymous users are handled;
- how tenant scoping prevents cross-tenant correlation;
- what content and identifiers privacy rules allow Reiver to receive.

The first accepted Flow request carrying `x-reiver-session-id` starts recorded activity. The application should call the documented end-session endpoint when the episode finishes. Reiver's current 30-minute inactivity timeout is fallback protection. A second episode must receive a new session ID while retaining the same stable pseudonymous user ID for the same person.

See [Session Telemetry](/flow/session-telemetry) for the exact headers, attributes, endpoint, and privacy guidance.
