//! Warehouse query and Pond observability helpers.
//!
//! # Error traces in Watch / ClickHouse
//!
//! The traces UI filters by **trace-level** status derived from spans: a trace counts as
//! having an error when any exported span has OpenTelemetry **span status** set to Error
//! (stored as `STATUS_CODE_ERROR` on spans in ClickHouse).
//!
//! The Tower `TraceLayer` (see `main.rs`) sets `otel.status_code` on the **HTTP** `http.request`
//! span from the HTTP status code. Inner spans such as [`warehouse.api.query`](crate::api::warehouse)
//! must also call [`mark_warehouse_query_span_error`] so query failures appear alongside
//! worker spans (e.g. blockchain `sync_chain`) when filtering error traces.
//!
//! # Production checklist
//!
//! - Set `otel_exporter_endpoint` and `otel_project_id` in Pond config (see [`crate::telemetry`])
//!   so spans reach Watch. Without both, only console logging is used.
//! - Search spans by name `warehouse.api.query` or `warehouse.api.execute_query_stream`.

use axum::http::{HeaderName, HeaderValue, Response};
use axum::response::IntoResponse;
use opentelemetry::trace::{Status, TraceContextExt};
use sha2::{Digest, Sha256};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::error::Result;

/// Short stable hash of SQL for span attributes (never log or record raw SQL).
pub fn warehouse_sql_hash(sql: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    let full = hasher.finalize();
    hex_prefix16(&full[..])
}

fn hex_prefix16(bytes: &[u8]) -> String {
    let s: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    s.chars().take(16).collect()
}

/// Record `warehouse.sql_hash` and initial `warehouse.execution_path` on the current span.
pub fn record_warehouse_query_fields(sql_hash: &str, execution_path: &str) {
    let span = tracing::Span::current();
    span.record("warehouse.sql_hash", sql_hash);
    span.record("warehouse.execution_path", execution_path);
}

/// Update `warehouse.execution_path` (e.g. `cached`, `clickhouse_json`, `federated`).
pub fn set_warehouse_query_execution_path(path: &str) {
    tracing::Span::current().record("warehouse.execution_path", path);
}

/// Mark the current warehouse query span as failed for OTLP / error-trace indexing.
pub fn mark_warehouse_query_span_error() {
    tracing::Span::current().record("otel.status_code", "ERROR");
    tracing::Span::current().set_status(Status::error("warehouse query failed"));
}

/// Mark the current warehouse query span as successful.
pub fn mark_warehouse_query_span_ok() {
    tracing::Span::current().record("otel.status_code", "OK");
    tracing::Span::current().set_status(Status::Ok);
}

/// Hex trace id for `X-Trace-Id` and structured logs (empty when OTel layer is inactive).
pub fn current_trace_id_hex() -> Option<String> {
    let cx = tracing::Span::current().context();
    let span = cx.span();
    let sc = span.span_context();
    if !sc.is_valid() {
        return None;
    }
    Some(sc.trace_id().to_string())
}

pub fn inject_x_trace_id_header<B>(response: &mut Response<B>) {
    if let Some(tid) = current_trace_id_hex() {
        if let Ok(hv) = HeaderValue::from_str(&tid) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("x-trace-id"), hv);
        }
    }
}

/// Finalize span status and response for `warehouse.api.query` / `execute_query_stream`.
///
/// Inner handlers return [`Err`]`(AppError)` on failure; we map to `Ok(Response)` with the
/// correct HTTP status so the outer `#[tracing::instrument]` closure still runs span end hooks,
/// while we explicitly set OTel ERROR on the query span (see module docs).
pub fn finalize_warehouse_query_response(result: Result<axum::response::Response>) -> Result<axum::response::Response> {
    match result {
        Ok(r) => {
            mark_warehouse_query_span_ok();
            Ok(r)
        }
        Err(e) => {
            let tid = current_trace_id_hex();
            tracing::error!(
                trace_id = ?tid,
                error = %e,
                "warehouse query handler failed"
            );
            mark_warehouse_query_span_error();
            let mut resp = e.into_response();
            inject_x_trace_id_header(&mut resp);
            Ok(resp)
        }
    }
}
