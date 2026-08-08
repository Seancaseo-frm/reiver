# Session Telemetry

Session Telemetry correlates OpenTelemetry spans and logs with your LLM sessions so you can see the full picture of what happened during a conversation — the gateway requests, your application's own traces, and any structured logs — all in one place.

## How It Works

1. You send LLM requests through the Flow gateway with `x-reiver-session-id` to group them into a session.
2. In your own application code, you annotate spans and logs with the same session ID using the `gen_ai.session_id` (or `llm.session_id`) OTel attribute.
3. Reiver queries ClickHouse for all spans and logs whose session ID attribute matches, and displays them in a **Telemetry** tab on the session detail page.

::: tip Session Availability
By default, sessions appear on the Sessions page approximately **30 minutes** after the last request (idle timeout). To speed this up, call `POST /v1/sessions/{session_id}/end` when the session is finished — the session will be evaluated ~30 seconds later. See the [API Reference](/flow/api-reference) for details.
:::

```
┌─────────────┐       x-reiver-session-id: "sess-42"
│  Your App    │─────────────────────────────────────────►  Flow Gateway
│              │                                            │
│  OTel SDK    │──  gen_ai.session_id = "sess-42"  ──►  ClickHouse
└─────────────┘                                            │
                                                           ▼
                                              Session Detail → Telemetry tab
```

## Tagging Spans

### Python (OpenTelemetry)

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

session_id = "sess-42"

with tracer.start_as_current_span("process_user_message") as span:
    span.set_attribute("gen_ai.session_id", session_id)
    # ... your application logic ...
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        extra_headers={"x-reiver-session-id": session_id},
    )
```

### Node.js (OpenTelemetry)

```javascript
const { trace } = require('@opentelemetry/api');

const tracer = trace.getTracer('my-app');
const sessionId = 'sess-42';

tracer.startActiveSpan('process_user_message', (span) => {
  span.setAttribute('gen_ai.session_id', sessionId);
  // ... your application logic ...
  span.end();
});
```

### Rust (tracing + opentelemetry)

```rust
use tracing::Span;

let session_id = "sess-42";
let span = tracing::info_span!("process_user_message",
    gen_ai.session_id = session_id,
);
let _guard = span.enter();
// ... your application logic ...
```

## Tagging Logs

You can also correlate logs. Set the same attribute on your OTel log records:

### Python

```python
import logging

logger = logging.getLogger(__name__)

# With the OTel logging bridge, extra fields become log attributes
logger.info(
    "User sent message",
    extra={"gen_ai.session_id": session_id},
)
```

### Node.js (Pino + OTel)

```javascript
const logger = require('pino')();

logger.info({ 'gen_ai.session_id': sessionId }, 'User sent message');
```

## Supported Attribute Names

Flow recognises two attribute keys (checked on both spans and logs):

| Attribute | Notes |
|-----------|-------|
| `gen_ai.session_id` | Follows the [OpenTelemetry GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/). **Preferred.** |
| `llm.session_id` | Legacy / custom convention. Supported for backwards compatibility. |

Either attribute can appear in `span_attributes` (for spans) or `log_attributes` (for logs).

## API Reference

### GET `/api/projects/{id}/llm/sessions/{session_id}/telemetry`

Returns spans and logs from ClickHouse whose `gen_ai.session_id` or `llm.session_id` attribute matches the given session ID.

**Response**

```json
{
  "spans": [
    {
      "trace_id": "abc123...",
      "span_id": "def456...",
      "parent_span_id": "",
      "span_name": "gateway.chat_completion",
      "span_kind": "SPAN_KIND_SERVER",
      "service_name": "flow-gateway",
      "timestamp": "2026-04-15 14:30:00.123456789",
      "duration_ns": 1200000000,
      "status_code": "STATUS_CODE_OK",
      "status_message": "",
      "attributes": {
        "gen_ai.session_id": "sess-42",
        "gen_ai.request.model": "gpt-4o",
        "gen_ai.usage.input_tokens": "150"
      }
    }
  ],
  "logs": [
    {
      "timestamp": "2026-04-15 14:30:01.000000000",
      "trace_id": "abc123...",
      "span_id": "def456...",
      "severity_text": "INFO",
      "service_name": "my-app",
      "body": "User sent message in session sess-42",
      "attributes": {
        "gen_ai.session_id": "sess-42"
      }
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `spans` | array | OTel spans matching this session. Sorted by timestamp ascending. Limited to 500. |
| `logs` | array | OTel logs matching this session. Sorted by timestamp ascending. Limited to 500. |

## UI

The session detail page includes a **Telemetry** tab next to **Requests**. The tab shows:

- **Spans table** — name, service, duration, status, and timestamp. Click a row to expand it and see the full trace/span IDs and all attributes.
- **Logs table** — severity, body (truncated), service, and timestamp. Click a row to see the full log body and attributes.

If no telemetry data is found, the tab displays guidance on which attribute to set.
