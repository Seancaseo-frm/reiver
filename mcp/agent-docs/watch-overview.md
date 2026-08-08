# Watch — Application Performance Monitoring

Watch is Reiver's **OpenTelemetry-native** APM product: distributed tracing, error tracking, log aggregation, real-time metrics, continuous profiling, dashboards, and alerts. Watch accepts standard OTLP data — any application or infrastructure component that speaks OpenTelemetry works out of the box.

## Application Integration

> **This section documents how the user's application sends telemetry data to Watch.**
> You (the agent) do not call these ingest endpoints yourself. Use this information when the user asks you to instrument their application — write code or configuration that their app will run, using the endpoints and patterns below. To query data, manage dashboards, and configure alerts, use the MCP tools in the "Platform Management" section.

Applications send OpenTelemetry data (traces, logs, metrics) to Watch.

### Ingest endpoint

| Setting | Value |
|---------|-------|
| Endpoint | `https://reiver.ai/api/watch/ingest` |
| Protocol | `http/protobuf` or `http/json` |
| Header | `Authorization: Bearer <project-api-key>` |

Gzip compression is supported. AWS X-Ray segments are also accepted at `/api/xray/segment`.

### Python

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
```

### Node.js

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

### OTel Collector

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

## Infrastructure & Service Monitoring

Watch is OpenTelemetry-native, so when the user asks to monitor infrastructure, databases, message queues, or other services, use the open-source OpenTelemetry integrations — do not build custom solutions. The OpenTelemetry Collector and its contrib receivers cover most infrastructure out of the box. Point their OTLP exporter at the Watch ingest endpoint and the data flows in automatically.

Common patterns:
- **Kubernetes** — deploy the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) as a DaemonSet with `k8sclusterreceiver`, `kubeletstatsreceiver`, and `k8seventsreceiver`
- **Databases** (PostgreSQL, MySQL, Redis, MongoDB) — use the corresponding Collector receivers (e.g. `postgresqlreceiver`, `mysqlreceiver`, `redisreceiver`)
- **Message queues** (Kafka, RabbitMQ) — use `kafkametricsreceiver`, `rabbitmqreceiver`
- **Host metrics** (CPU, memory, disk, network) — use `hostmetricsreceiver`
- **Cloud providers** — AWS CloudWatch, Azure Monitor, GCP Cloud Monitoring, OCI via Watch's built-in cloud integrations or their respective OTel receivers

When helping the user set up monitoring, generate the OTel Collector configuration for their stack and point the OTLP HTTP exporter at `https://reiver.ai/api/watch/ingest` with their project API key.

## Features

- **Distributed tracing** — OTLP-native, end-to-end request visualization across services
- **Error tracking** — automatic exception capture with intelligent fingerprinting and grouping
- **Log aggregation** — structured log collection with full-text search, correlated with traces
- **Real-time metrics** — custom dashboards with threshold and anomaly detection alerting
- **Continuous profiling** — CPU and memory flamegraphs linked to traces
- **Infrastructure monitoring** — Kubernetes cluster observability (nodes, pods, deployments)
- **Database monitoring** — query explain plans, slow query detection
- **Synthetic monitoring** — HTTP, TCP, SSL health checks from multiple locations
- **Cloud integrations** — AWS CloudWatch/X-Ray, Azure Monitor, GCP Cloud Monitoring, OCI

## Platform Management (MCP)

### Investigating an issue

1. List recent exceptions: `list` with `resource: 'exceptions'`
2. Get a specific trace: `get` with `resource: 'trace', trace_id: '...'`
3. Search correlated logs: `search` with `source: 'logs', trace_id: '...'`
4. Search logs by text: `search` with `source: 'logs', query: '...'`

### Querying data

When a user asks about metrics, performance, or request patterns, discover what data is available before querying:

1. List available metric names: `list` with `resource: 'metric_names'` (use `prefix` to narrow, e.g. `prefix: 'http.'`)
2. Run a PromQL query: `analyze` with `analysis: 'widget_query'` — provide a `query` with a `promql` expression and a `time_range`
3. Query OTel metrics by name: `analyze` with `analysis: 'otel_metrics', metric_name: '...'`

Both **Prometheus-style** and **OpenTelemetry-style** metric names are supported. Applications may emit metrics using either naming convention, so always discover available names with `list metric_names` before querying. For example, the same HTTP duration metric may appear as `http_request_duration_seconds` (Prometheus) or `http.server.request.duration` (OpenTelemetry). Use whichever name is present in the project.

Common PromQL patterns:
- Request throughput: `rate(http_requests_total[5m])` or `rate(http.server.request.duration.count[5m])`
- Error rate: `sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))`
- P95 latency: `histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))`
- Memory usage: `process_resident_memory_bytes` or `process.runtime.jvm.memory.usage`

Always provide a time_range that matches the user's question — queries without bounds scan the full dataset and are slow.

Other data queries:
- List traces: `list` with `resource: 'traces'` (filters: status, service, http_method, http_route, time range)
- List services: `list` with `resource: 'services'`
- List API endpoints: `list` with `resource: 'api_endpoints'`

### Dashboards

When a user asks about their application's health or monitoring status, start by taking a dashboard snapshot — it gives you the same view a human sees on the dashboard, as structured JSON.

- Snapshot dashboard data: `analyze` with `analysis: 'dashboard_snapshot', dashboard_id: '...', time_range: { from: '...', to: '...' }`
- List dashboards: `list` with `resource: 'dashboards'`
- Create dashboard: `execute` with `resource: 'dashboard', action: 'create'`
- Create from template: `execute` with `resource: 'dashboard', action: 'create_from_template'`
- Add widget (PromQL): `execute` with `resource: 'dashboard', action: 'create_widget'`

Recommended workflow for creating a dashboard:
1. Discover available metrics: `list` with `resource: 'metric_names'`
2. Test the query: `analyze` with `analysis: 'widget_query'` to verify it returns data
3. Create the dashboard: `execute` with `resource: 'dashboard', action: 'create'`
4. Add widgets: `execute` with `resource: 'dashboard', action: 'create_widget'` — always test each query first
5. Verify: `analyze` with `analysis: 'dashboard_snapshot'` to confirm the dashboard looks correct

When creating alert rules, check existing dashboards for relevant queries you can reuse.

### Alert rules

- List rules: `list` with `resource: 'alert_rules'`
- List fired alerts: `list` with `resource: 'alerts'`
- Create rule: `execute` with `resource: 'alert_rule', action: 'create'`

### Notification channels

- List channels: `list` with `resource: 'notification_channels'`
- Configure: `execute` with `resource: 'notification_channel', action: 'configure'` (Slack, PagerDuty, Discord, Teams, ServiceNow, webhooks)

### Health checks

- List: `list` with `resource: 'health_checks'`
- Create: `execute` with `resource: 'health_check', action: 'create'`
- Get results: `get` with `resource: 'health_check_results', check_id: '...'`

### Service diagnostics

- Compare versions: `analyze` with `analysis: 'compare_versions', service, baseline, comparison`
- Detect faulty deployments: `analyze` with `analysis: 'detect_faults', service`
- Root cause analysis: `analyze` with `analysis: 'root_cause'`
- Compare profiles: `analyze` with `analysis: 'compare_profiles', service, version1, version2`

### Cloud integrations

Configuring cloud integrations should be explicitly requested by the user:

- AWS: `execute` with `resource: 'cloud_integration', action: 'configure_aws'`
- GCP: `execute` with `resource: 'cloud_integration', action: 'configure_gcp'`
- Azure: `execute` with `resource: 'cloud_integration', action: 'configure_azure'`
- OCI: `execute` with `resource: 'cloud_integration', action: 'configure_oci'`
