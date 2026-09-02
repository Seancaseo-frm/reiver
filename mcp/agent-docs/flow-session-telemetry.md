# Flow — Session Telemetry

Session Telemetry correlates OpenTelemetry spans and logs with LLM sessions, providing a unified view of gateway requests, application traces, and structured logs in one place.

## How It Works

1. Applications send LLM requests through the Flow gateway with `x-reiver-session-id` to group them into a session.
2. Application code annotates OTel spans and logs with the same session ID using `gen_ai.session_id` or `llm.session_id`.
3. Reiver queries ClickHouse for all spans and logs matching the session ID and displays them in the session detail page.

Sessions appear approximately 30 minutes after the session ends due to ingestion and processing pipelines.

## Application Integration

### Tagging spans

#### Python (OpenTelemetry)

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)
session_id = "sess-42"

with tracer.start_as_current_span("process_user_message") as span:
    span.set_attribute("gen_ai.session_id", session_id)
    response = client.chat.completions.create(
        model="auto",
        messages=messages,
        extra_headers={"x-reiver-session-id": session_id},
    )
```

#### Node.js (OpenTelemetry)

```javascript
const { trace } = require('@opentelemetry/api');

const tracer = trace.getTracer('my-app');
const sessionId = 'sess-42';

tracer.startActiveSpan('process_user_message', (span) => {
  span.setAttribute('gen_ai.session_id', sessionId);
  span.end();
});
```

#### Rust (tracing + opentelemetry)

```rust
use tracing::Span;

let session_id = "sess-42";
let span = tracing::info_span!("process_user_message",
    gen_ai.session_id = session_id,
);
let _guard = span.enter();
```

### Tagging logs

```python
import logging
logger = logging.getLogger(__name__)

logger.info(
    "User sent message",
    extra={"gen_ai.session_id": session_id},
)
```

### Supported attribute names

- `gen_ai.session_id` — OpenTelemetry GenAI semantic convention (preferred)
- `llm.session_id` — legacy/custom convention

The attribute value must match the `x-reiver-session-id` sent with gateway requests.

## Platform Management (MCP)

To query session data:

- List sessions: `list` with `resource: 'sessions'`
- Get session details: `get` with `resource: 'session', session_id: '...'`
- Get session requests: `get` with `resource: 'session_requests', session_id: '...'`
