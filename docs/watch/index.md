# Watch — Application Performance Monitoring

Watch is Reiver's APM product. It provides full-stack application performance monitoring with distributed tracing, error tracking, log aggregation, real-time metrics, and continuous profiling.

## Quick Start

### Step 1: Get your API key

Go to your project in Reiver and navigate to **Settings → Integrations**. Copy your project API key (starts with `dh_`).

### Step 2: Send your first traces

Configure your OpenTelemetry SDK or Collector to send data to Watch:

| Setting        | Value                                  |
|----------------|----------------------------------------|
| **Endpoint**   | `https://reiver.ai/api/watch/ingest` |
| **Protocol**   | `http/protobuf` or `http/json`         |
| **Header**     | `Authorization: Bearer <your-project-api-key>` |

Gzip compression is supported.

#### Python

```bash
pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http
```

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

exporter = OTLPSpanExporter(
    endpoint="https://reiver.ai/api/watch/ingest/v1/traces",
    headers={"Authorization": "Bearer dh_..."},
)
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)
with tracer.start_as_current_span("my-first-span"):
    print("Hello, Watch!")
```

#### Node.js

```bash
npm install @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-proto
```

```javascript
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-proto');

const sdk = new NodeSDK({
  traceExporter: new OTLPTraceExporter({
    url: 'https://reiver.ai/api/watch/ingest/v1/traces',
    headers: { Authorization: 'Bearer dh_...' },
  }),
});
sdk.start();
```

#### OTel Collector

If you already run an OpenTelemetry Collector, add a Reiver exporter:

```yaml
exporters:
  otlphttp/reiver:
    endpoint: https://reiver.ai/api/watch/ingest
    headers:
      Authorization: "Bearer dh_..."
    compression: gzip

service:
  pipelines:
    traces:
      exporters: [otlphttp/reiver]
    logs:
      exporters: [otlphttp/reiver]
    metrics:
      exporters: [otlphttp/reiver]
```

::: tip What this unlocks
Once data is flowing, you immediately get distributed tracing, error tracking, log aggregation, and real-time metrics — all visible in the Watch section of your project dashboard.
:::

### Step 3: Explore your data

Open your project in Reiver and navigate to:

- **Tracing** — see the full path of requests across services
- **Errors** — view auto-grouped exceptions with stack traces
- **Logs** — search structured logs correlated with traces

### Next Steps

- **Dashboards** — build custom dashboards from your metrics
- **Alerts** — set up alert rules with Slack, PagerDuty, Discord, Teams, ServiceNow, or webhooks
- **Health Checks** — create HTTP/TCP/SSL synthetic monitors
- **Profiling** — enable continuous profiling for CPU and memory flamegraphs

Watch also accepts AWS X-Ray segments at `/api/xray/segment` and `/api/xray/segments` with the same authorization header.

---

## Features

### Distributed Tracing
OTLP-native distributed tracing with end-to-end request visualization. See the complete path of a request across services, databases, and external APIs.

### Error Tracking
Automatic exception capture with intelligent fingerprinting that groups similar errors together. Track regressions, assign ownership, and link errors to commits via GitHub integration.

### Log Aggregation
Structured log collection with full-text and semantic search. Logs are automatically correlated with traces and spans for root cause analysis.

### Real-Time Metrics
Custom dashboards with threshold-based and anomaly detection alerting. Build dashboards from any metric your application emits.

### Continuous Profiling
CPU and memory flamegraphs linked directly to traces. Identify hot paths and memory allocations in production without sampling bias.

### Infrastructure Monitoring
Kubernetes cluster observability with live and historical views of nodes, pods, deployments, and services. CPU, memory, and disk utilization with capacity planning.

### Database Monitoring
Query explain plans, slow query detection, and connection pool monitoring. See exactly which queries are slowing down your application.

### Synthetic Monitoring
HTTP, TCP, and SSL health checks from multiple locations. Detect outages before your users do.

### Alerts & Notifications
Configurable alert rules with notifications to:
- Slack
- PagerDuty
- Discord
- Microsoft Teams
- ServiceNow
- Webhooks

### Cloud Integrations
Native monitoring integrations:
- **AWS** — CloudWatch metrics, X-Ray traces
- **Azure** — Azure Monitor integration
- **GCP** — Cloud Monitoring integration
- **Oracle Cloud** — OCI monitoring

### LLM Observability
When used alongside Flow, Watch provides visibility into LLM calls: token usage, costs, latency, and prompt/response payloads — all linked to the originating application trace.
