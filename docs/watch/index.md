# Watch: Traces, Structured Logs, and Metrics

Watch accepts OpenTelemetry Protocol data over HTTP. This is an independently completable track: it does not require a provider key, Flow gateway routing, or MCP write access.

## Credential and endpoint

Create an SDK key and keep it in the application's secret store:

```bash
export REIVER_WATCH_API_KEY="<SDK key from your secret store>"
```

| Setting | Value |
|---|---|
| OTLP HTTP base endpoint | `https://reiver.ai/api/watch/ingest` |
| Signal paths | `/v1/traces`, `/v1/logs`, `/v1/metrics` |
| Protocol | `http/protobuf` or `http/json` |
| Authorization | `Bearer <SDK key>` |

The same SDK-key value may currently be bound separately as `REIVER_FLOW_API_KEY` when the application also uses Flow. `REIVER_AGENT_TOKEN` is a separate MCP credential. Do not use it for Watch ingestion; applications should use the SDK key bound as `REIVER_WATCH_API_KEY`.

No credential belongs in application code, telemetry, logs, or reports.

## An endpoint is not instrumentation

Full Watch onboarding requires three real pipelines:

| Signal | What must exist |
|---|---|
| Traces | Instrumentation that creates spans, a tracer provider, a processor, and an OTLP trace exporter. |
| Structured logs | A logging source or bridge, a logger provider, a processor, and an OTLP log exporter. Stdout alone is not an OTLP log pipeline. |
| Metrics | Runtime or application instruments, a meter provider, a periodic reader, and an OTLP metric exporter. |

Setting `OTEL_EXPORTER_OTLP_ENDPOINT` only tells initialized exporters where to send data. It does not install instrumentation or automatically create all three signals.

For SDKs that implement the standard environment configuration:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="https://reiver.ai/api/watch/ingest"
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $REIVER_WATCH_API_KEY"
export OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf"
export OTEL_SERVICE_NAME="my-application"
export OTEL_TRACES_EXPORTER="otlp"
export OTEL_LOGS_EXPORTER="otlp"
export OTEL_METRICS_EXPORTER="otlp"
```

Language distributions differ. Confirm that the application's SDK actually initializes every provider and exporter. If you use signal-specific endpoint variables, include the complete `/v1/traces`, `/v1/logs`, or `/v1/metrics` path.

## Collector example

An existing OpenTelemetry Collector can provide batching and retries. Each signal still needs a receiver and an enabled pipeline:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:

exporters:
  otlphttp/reiver:
    endpoint: https://reiver.ai/api/watch/ingest
    headers:
      Authorization: "Bearer ${env:REIVER_WATCH_API_KEY}"

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

Defining an exporter outside `service.pipelines` does not enable it.

## Definition of done

Generate evidence from a real application path, then confirm:

| Check | Required evidence |
|---|---|
| Trace | A known application trace is queryable under the expected stable `service.name`. |
| Structured log | A known structured log is queryable and carries the expected service identity. |
| Metric | A known application or runtime metric has a recent data point under that service. |
| Correlation | If required, trace and log carry the agreed session and pseudonymous user attributes. |
| Secrets | The SDK key and sensitive customer content are absent from all evidence and reports. |

Read-only MCP with `observability:read` may be used as an additional verification path, but it is optional. Do not grant MCP write access merely to complete Watch onboarding.

## Troubleshooting

| Symptom | Check |
|---|---|
| Traces arrive, logs do not | Confirm a logging bridge/provider/processor/exporter exists; stdout is not automatically exported. |
| Traces arrive, metrics do not | Confirm a meter provider, periodic reader, exporter, and an instrument producing measurements exist. |
| Nothing arrives | Check SDK diagnostics, protocol, full signal path, authorization, egress, and shutdown flushing without printing the key. |
| Services are fragmented | Use the same stable `service.name` resource for all three signals. |
| Logs lack trace correlation | Emit them within the active span and use a bridge that copies trace context. |

For combined Flow and Watch correlation, agree the [Session and Identity Contract](/flow/session-telemetry), then follow [Complete Reiver](/quickstart).
