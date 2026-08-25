# Watch — Application Performance Monitoring

Watch is Reiver's **OpenTelemetry-native** APM product: distributed tracing, error tracking, log aggregation, real-time metrics, continuous profiling, dashboards, and alerts. Watch accepts standard OTLP data — any application or infrastructure component that speaks OpenTelemetry works out of the box.

Watch is an independently completable onboarding track. It does not require a provider key, a Flow gateway request, or an MCP write scope.

### Track definition of done

- one real application trace is queryable under the expected `service.name`;
- one known structured log is queryable and correlated by trace ID or conversation ID;
- one known application or runtime metric has a recent data point;
- all three signals use the intended service identity;
- evidence is retrieved in the UI or through MCP with `observability:read`;
- no SDK key appears in source, telemetry, logs, or the report.

Do not fail a Watch-only onboarding because Flow evidence is absent. Session/user correlation is optional unless the owner wants business-episode or cross-product analysis; when they do, read `agent://flow/session-telemetry` and confirm the shared Session and Identity Contract.

## Application Integration

> **This section documents how the user's application sends telemetry data to Watch.**
> You (the agent) do not call these ingest endpoints yourself. Use this information when the user asks you to instrument their application — write code or configuration that their app will run, using the endpoints and patterns below. To query data, manage dashboards, and configure alerts, use the MCP tools in the "Platform Management" section.

Applications send OpenTelemetry data (traces, logs, metrics) to Watch.

### Ingest endpoint

| Setting | Value |
|---------|-------|
| Endpoint | `https://reiver.ai/api/watch/ingest` |
| Protocol | `http/protobuf` or `http/json` |
| Header | `Authorization: Bearer <SDK key>` |

Gzip compression is supported. AWS X-Ray segments are also accepted at `/api/xray/segment`.

Bind the SDK key to `REIVER_WATCH_API_KEY`. The MCP agent token is not accepted by Watch ingestion.

### Full-signal requirement

A complete application integration has three independently initialized pipelines:

| Signal | Required path |
|---|---|
| Traces | instrumentation/manual spans → tracer provider → span processor → OTLP trace exporter |
| Logs | logging bridge/handler → logger provider → log record processor → OTLP log exporter |
| Metrics | instruments/runtime instrumentation → meter provider → periodic reader → OTLP metric exporter |

Setting an endpoint variable does not install packages, initialize providers, bridge console logs, or create a metric instrument. A trace exporter does not export logs or metrics.

SDKs that implement standard OpenTelemetry environment configuration can use:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="https://reiver.ai/api/watch/ingest"
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $REIVER_WATCH_API_KEY"
export OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf"
export OTEL_SERVICE_NAME="my-app"
export OTEL_TRACES_EXPORTER="otlp"
export OTEL_LOGS_EXPORTER="otlp"
export OTEL_METRICS_EXPORTER="otlp"
```

The base endpoint automatically receives `/v1/traces`, `/v1/logs`, or `/v1/metrics` from conforming exporters. When configuring a signal-specific endpoint, include the complete signal path. Header parsing varies by SDK; prefer programmatic headers when available and inspect exporter diagnostics without printing the credential.

### Complete Python diagnostic

This example deliberately emits one span, one correlated structured log, and one metric. Preserve existing providers when adapting it to an application.

```python
import logging
import os

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

base = "https://reiver.ai/api/watch/ingest"
headers = {"Authorization": f"Bearer {os.environ['REIVER_WATCH_API_KEY']}"}
resource = Resource.create({"service.name": "reiver-onboarding-smoke"})

trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(BatchSpanProcessor(
    OTLPSpanExporter(endpoint=f"{base}/v1/traces", headers=headers)
))
trace.set_tracer_provider(trace_provider)

metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=f"{base}/v1/metrics", headers=headers),
    export_interval_millis=5_000,
)
metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)

logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(
    OTLPLogExporter(endpoint=f"{base}/v1/logs", headers=headers)
))
set_logger_provider(logger_provider)
logger = logging.getLogger("reiver-onboarding")
logger.setLevel(logging.INFO)
logger.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))

session_id = "onboarding-smoke-1"
user_id = "onboarding-user-1"
tracer = trace.get_tracer("reiver-onboarding")
counter = metrics.get_meter("reiver-onboarding").create_counter(
    "reiver.onboarding.requests"
)

with tracer.start_as_current_span("reiver.onboarding.smoke") as span:
    span.set_attribute("gen_ai.conversation.id", session_id)
    span.set_attribute("gen_ai.session.id", session_id)  # Reiver compatibility
    span.set_attribute("gen_ai.user.id", user_id)
    counter.add(1, {"test": "onboarding"})
    logger.info("reiver-watch-ok", extra={
        "gen_ai.conversation.id": session_id,
        "gen_ai.session.id": session_id,
        "gen_ai.user.id": user_id,
    })

trace_provider.force_flush()
metric_provider.force_flush()
logger_provider.force_flush()
```

### OTel Collector

```yaml
exporters:
  otlphttp/reiver:
    endpoint: https://reiver.ai/api/watch/ingest
    headers:
      Authorization: "Bearer ${env:REIVER_WATCH_API_KEY}"
    compression: gzip

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp/reiver]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp/reiver]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp/reiver]
```

Defining an exporter without adding it to `service.pipelines` does not enable it. Each pipeline needs at least a receiver and exporter.

### Correlation and verification

Use the same session and user values across Flow and Watch:

- Flow: `x-reiver-session-id`, `x-reiver-user-id`, and the OpenAI-compatible `user` body field.
- OTel: `gen_ai.conversation.id` and `gen_ai.user.id`; also emit `gen_ai.session.id` with the same session value during Reiver's compatibility period.

Emit the diagnostic log while the test span is active so the logging bridge can attach trace/span context. Then prove one known trace, the `reiver-watch-ok` log, and the `reiver.onboarding.requests` metric arrived under the expected `service.name`. Use MCP `list` with `resource: 'metric_names'` before querying metrics and inspect trace/log attribute keys rather than assuming an attribute arrived.

Common failures:

- traces but no logs: only tracing was initialized or stdout was never bridged;
- traces but no metrics: no periodic reader or no instrument produced a measurement;
- logs without trace IDs: the log ran outside the active span or the bridge did not copy context;
- nothing arrives: wrong protocol/path, no provider, blocked egress, auth failure, or process exit before flush;
- fragmented services: inconsistent or missing `service.name` resources.

## Infrastructure & Service Monitoring

Watch is OpenTelemetry-native, so when the user asks to monitor infrastructure, databases, message queues, or other services, use the open-source OpenTelemetry integrations — do not build custom solutions. The OpenTelemetry Collector and its contrib receivers cover most infrastructure out of the box. Point their OTLP exporter at the Watch ingest endpoint and the data flows in automatically.

Common patterns:
- **Kubernetes** — deploy the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) as a DaemonSet with `k8sclusterreceiver`, `kubeletstatsreceiver`, and `k8seventsreceiver`
- **Databases** (PostgreSQL, MySQL, Redis, MongoDB) — use the corresponding Collector receivers (e.g. `postgresqlreceiver`, `mysqlreceiver`, `redisreceiver`)
- **Message queues** (Kafka, RabbitMQ) — use `kafkametricsreceiver`, `rabbitmqreceiver`
- **Host metrics** (CPU, memory, disk, network) — use `hostmetricsreceiver`
- **Cloud providers** — AWS CloudWatch, Azure Monitor, GCP Cloud Monitoring, OCI via Watch's built-in cloud integrations or their respective OTel receivers

When helping the user set up monitoring, generate the OTel Collector configuration for their stack and point the OTLP HTTP exporter at `https://reiver.ai/api/watch/ingest` with `REIVER_WATCH_API_KEY`.

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
