use axum::{extract::State, http::HeaderMap, response::Json, routing::post, Router};
use serde::Serialize;
use std::sync::Arc;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use tracing::{error, info};

#[derive(Debug, Serialize)]
struct SpanResponse {
    id: String,
    message: String,
}

pub fn create_spans_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/", post(ingest_span))
        .route("/batch", post(ingest_span_batch))
}

async fn ingest_span(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<crate::models::SpanPayload>,
) -> Result<Json<SpanResponse>> {
    info!(
        "[SPAN_INGEST] Starting span ingestion, span_id={}, trace_id={}",
        payload.span_id, payload.trace_id
    );

    let project_id = crate::api::extract_project_id(&headers)?;
    info!(
        "[SPAN_INGEST] Found project_id={} for span_id={}",
        project_id, payload.span_id
    );

    // Send span to Kafka (keyed by trace_id for partitioning)
    state
        .kafka
        .send_span(&payload.trace_id, &payload)
        .await
        .map_err(|e| {
            error!(
                "[SPAN_INGEST] Failed to send span to Kafka: {} for span_id={}",
                e, payload.span_id
            );
            AppError::Internal(anyhow::anyhow!("Failed to send span to Kafka: {}", e))
        })?;

    info!(
        "[SPAN_INGEST] Span sent to Kafka, span_id={}, trace_id={}",
        payload.span_id, payload.trace_id
    );

    Ok(Json(SpanResponse {
        id: payload.span_id.clone(),
        message: "Span sent to Kafka".to_string(),
    }))
}

#[derive(Debug, Serialize)]
struct BatchSpanResponse {
    ids: Vec<String>,
    count: usize,
    message: String,
}

async fn ingest_span_batch(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payloads): Json<Vec<crate::models::SpanPayload>>,
) -> Result<Json<BatchSpanResponse>> {
    if payloads.is_empty() {
        return Err(AppError::Internal(anyhow::anyhow!("Batch cannot be empty")));
    }

    if payloads.len() > 1000 {
        return Err(AppError::Internal(anyhow::anyhow!(
            "Batch size cannot exceed 1000 spans"
        )));
    }

    info!(
        "[SPAN_INGEST_BATCH] Starting batch ingestion, count={}",
        payloads.len()
    );

    let project_id = crate::api::extract_project_id(&headers)?;
    info!(
        "[SPAN_INGEST_BATCH] Found project_id={} for batch, count={}",
        project_id,
        payloads.len()
    );

    // Prepare spans with trace_id as key for partitioning
    let spans_with_keys: Vec<(String, crate::models::SpanPayload)> = payloads
        .into_iter()
        .map(|payload| (payload.trace_id.clone(), payload))
        .collect();

    let span_ids: Vec<String> = spans_with_keys
        .iter()
        .map(|(_, payload)| payload.span_id.clone())
        .collect();

    // Send all spans to Kafka in batch (keyed by trace_id)
    state
        .kafka
        .send_spans_batch(&spans_with_keys)
        .await
        .map_err(|e| {
            error!("[SPAN_INGEST_BATCH] Failed to send spans to Kafka: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to send spans to Kafka: {}", e))
        })?;

    let success_count = span_ids.len();
    info!(
        "[SPAN_INGEST_BATCH] Batch sent to Kafka: {} spans",
        success_count
    );

    Ok(Json(BatchSpanResponse {
        ids: span_ids,
        count: success_count,
        message: format!("{} spans sent to Kafka", success_count),
    }))
}
