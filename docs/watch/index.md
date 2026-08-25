# Watch: traces, logs and metrics

Watch accepts OpenTelemetry Protocol data over HTTP. Full observability requires three independent signal pipelines. A trace exporter does not export application logs or metrics.

This is an independently completable track. You do not need a provider API key, a Flow gateway request, or an MCP write token to onboard Watch. For the combined Flow, Watch, sessions, and agent workflow, use the [Complete Reiver Quickstart](/quickstart).

## Definition of done for this track

| Check | Required evidence |
|---|---|
| Trace | A real application trace is queryable under the expected `service.name` |
| Log | A known structured log is queryable and carries the test trace ID or conversation ID |
| Metric | A known application or runtime metric has a recent data point |
| Service | All three signals use the intended stable service identity |
| Verification | The evidence is found in Reiver's UI or through MCP with `observability:read` |
| Secrets | The SDK key is absent from source, telemetry, logs, and reports |

Session/user attributes are optional for a purely technical Watch integration. Confirm the shared [Session and Identity Contract](/flow/session-telemetry) when the owner wants business-episode or cross-product correlation.

## Credential and endpoint

Create an SDK key under **Settings → General → SDK keys** and select `observability:write`; the UI also selects `observability:read`. Bind its value to a Watch-specific application secret:

```bash
export REIVER_WATCH_API_KEY="dh_..."
```

| Setting | Value |
|---|---|
| OTLP HTTP base endpoint | `https://reiver.ai/api/watch/ingest` |
| Signal paths | `/v1/traces`, `/v1/logs`, `/v1/metrics` |
| Protocol | `http/protobuf` or `http/json` |
| Header | `Authorization: Bearer <SDK key>` |

The application runtime receives the SDK key. A Reiver MCP agent token is not accepted by Watch ingestion.

## The full-signal rule

Each signal needs a source, SDK component and exporter:

| Signal | Source | Required SDK path |
|---|---|---|
| Traces | Auto-instrumentation or manual spans | tracer provider → span processor → OTLP trace exporter |
| Logs | Logging bridge/handler or OTel logs API | logger provider → log record processor → OTLP log exporter |
| Metrics | Runtime instrumentation or explicit instruments | meter provider → periodic reader → OTLP metric exporter |

Setting `OTEL_EXPORTER_OTLP_ENDPOINT` only tells an initialized exporter where to send data. It does not install instrumentation, capture stdout, create a logging bridge, create metrics, or initialize any provider.

## Environment configuration

SDKs that implement OpenTelemetry's standard environment configuration can use:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="https://reiver.ai/api/watch/ingest"
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $REIVER_WATCH_API_KEY"
export OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf"
export OTEL_SERVICE_NAME="my-app"
export OTEL_TRACES_EXPORTER="otlp"
export OTEL_LOGS_EXPORTER="otlp"
export OTEL_METRICS_EXPORTER="otlp"
```

Environment-variable support and header parsing vary by language and distribution. Prefer the language exporter's programmatic header option when available, and verify authentication for every signal. When a signal-specific endpoint variable is used, include the complete path such as `/v1/logs`; only the base `OTEL_EXPORTER_OTLP_ENDPOINT` automatically appends signal paths.

## Complete Python smoke test

Install the SDK and OTLP HTTP exporter:

```bash
pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp-proto-http
```

This minimal program initializes all three signal pipelines and emits one item from each:

```python
import logging
import os

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

base = "https://reiver.ai/api/watch/ingest"
headers = {
    "Authorization": f"Bearer {os.environ['REIVER_WATCH_API_KEY']}",
}
resource = Resource.create({"service.name": "reiver-onboarding-smoke"})

trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(
    BatchSpanProcessor(
        OTLPSpanExporter(endpoint=f"{base}/v1/traces", headers=headers)
    )
)
trace.set_tracer_provider(trace_provider)

metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=f"{base}/v1/metrics", headers=headers),
    export_interval_millis=5_000,
)
metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)

logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(
    BatchLogRecordProcessor(
        OTLPLogExporter(endpoint=f"{base}/v1/logs", headers=headers)
    )
)
set_logger_provider(logger_provider)
otel_handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
logger = logging.getLogger("reiver-onboarding")
logger.setLevel(logging.INFO)
logger.addHandler(otel_handler)

session_id = "onboarding-smoke-1"
user_id = "onboarding-user-1"
tracer = trace.get_tracer("reiver-onboarding")
counter = metrics.get_meter("reiver-onboarding").create_counter(
    "reiver.onboarding.requests",
    unit="1",
)

with tracer.start_as_current_span("reiver.onboarding.smoke") as span:
    span.set_attribute("gen_ai.conversation.id", session_id)
    span.set_attribute("gen_ai.session.id", session_id)  # Reiver compatibility
    span.set_attribute("gen_ai.user.id", user_id)
    counter.add(1, {"test": "onboarding"})
    logger.info(
        "reiver-watch-ok",
        extra={
            "gen_ai.conversation.id": session_id,
            "gen_ai.session.id": session_id,
            "gen_ai.user.id": user_id,
        },
    )

trace_provider.force_flush()
metric_provider.force_flush()
logger_provider.force_flush()
```

Use this as a diagnostic smoke test. In the actual application, preserve any existing providers and add Reiver exporters without registering multiple competing global providers.

## OpenTelemetry Collector

A Collector is useful when several services export locally, when stdout/container logs need collection, or when you want retries and processing outside application processes.

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:

exporters:
  otlphttp/reiver:
    endpoint: https://reiver.ai/api/watch/ingest
    headers:
      Authorization: "Bearer ${env:REIVER_WATCH_API_KEY}"
    compression: gzip

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

Defining an exporter outside `service.pipelines` does not enable it. Each pipeline must name its receiver and exporter.

## Session and user correlation

Use the same stable identifiers across Flow and Watch:

| Layer | Session/conversation | User |
|---|---|---|
| Flow headers | `x-reiver-session-id` | `x-reiver-user-id` |
| Flow request body | — | OpenAI-compatible `user` field |
| OTel attributes | `gen_ai.conversation.id` | `gen_ai.user.id` |
| Reiver compatibility | `gen_ai.session.id` | — |

`gen_ai.conversation.id` is the current OpenTelemetry GenAI attribute. `gen_ai.user.id` is a Reiver correlation attribute. Reiver also accepts the deprecated `gen_ai.session.id` in its LLM processor. Emit both session values during the compatibility period so older and newer views can correlate the same value.

For trace-log correlation, emit the log while the relevant span is active and use a logging bridge that copies the current trace and span context into the OTLP log record.

## Verification

Generate one real application request, not only a synthetic exporter call, then prove:

1. **Tracing** contains the application service and expected request span.
2. **Logs** contains a known structured message such as `reiver-watch-ok`.
3. The log carries the same trace ID or conversation attribute as the span.
4. **Metrics** contains a known application/runtime metric.
5. MCP `list`/`search` operations with `observability:read` can retrieve the same evidence.

Discovery is part of verification. Use MCP `list` with `resource: "metric_names"` before querying a metric, and use `log_attribute_keys` or `trace_attribute_keys` to confirm which attributes arrived.

## Troubleshooting

| Symptom | Most likely cause | Check |
|---|---|---|
| `401` or `403` on ingest | Agent token used, malformed auth header, or wrong SDK key | Use the SDK key and inspect exporter diagnostics without printing the value |
| Nothing arrives | No provider initialized, wrong protocol/path, blocked egress, or process exits before flush | Enable OTel diagnostic logs; force-flush in a smoke test; verify `/v1/{signal}` |
| Traces arrive, logs do not | Only a tracer was configured; stdout is not an OTLP log source | Add logger provider, logging bridge/handler, processor and log exporter |
| Traces arrive, metrics do not | No meter provider/periodic reader or no instrument produced a measurement | Add the reader/exporter and emit a known counter |
| Logs arrive without trace IDs | Log emitted outside active span or bridge does not copy context | Emit inside an active span and inspect the OTLP log's trace/span IDs |
| Service is missing or fragmented | `service.name` absent or changes between signals | Set one stable `service.name` resource on all providers |
| Session cannot be found | Header/attribute values differ or session was not explicitly ended | Compare exact IDs and call Flow's `/sessions/{session_id}/end` endpoint |
| Data appears only on shutdown | Batch/metric intervals exceed the observation window | Shorten the test interval and call `force_flush`; restore production intervals later |

Do not log provider keys, SDK keys, agent tokens, prompts, or model output unless the application's privacy and capture policy explicitly permits it.

## What Watch provides after ingestion

- distributed tracing and error grouping;
- structured log search and trace correlation;
- OpenTelemetry metric discovery, dashboards and alerts;
- service and endpoint diagnostics;
- infrastructure and cloud telemetry through standard Collector receivers;
- LLM request cost, latency and token context when used with Flow.

Watch processes only the telemetry the application or Collector actually sends.
