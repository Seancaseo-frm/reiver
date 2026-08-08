# Warehouse query observability

## Span names

- `warehouse.api.query` — non-streaming SQL endpoint
- `warehouse.api.execute_query_stream` — SSE / Arrow stream variants

Attributes (when OTel is enabled):

- `warehouse.sql_hash` — first 16 hex chars of SHA-256 of the SQL text (not raw SQL)
- `warehouse.execution_path` — e.g. `cached`, `federated`, `clickhouse_json`, `clickhouse_arrow`, `stream_sse`
- `otel.status_code` — `OK` or `ERROR`; OTLP span status is set to match for error traces

## Production: export traces to Watch

Pond sends OTLP only when **both** are set (see `telemetry.rs` / config):

- `otel_exporter_endpoint` (or env equivalent)
- `otel_project_id`

Without both, traces are console-only.

## Error traces in the UI

Watch marks a trace as having an error when spans include OpenTelemetry **error** status (`STATUS_CODE_ERROR` in storage). The Tower HTTP layer sets status from HTTP status codes; these handlers also set error status on `warehouse.api.*` spans when the query handler fails so they appear in error-trace filters alongside worker spans (e.g. blockchain sync).

## Client correlation

Failed query responses include `X-Trace-Id` (hex OpenTelemetry trace id) when a trace context exists, for matching browser Network tab requests to traces in the product.
