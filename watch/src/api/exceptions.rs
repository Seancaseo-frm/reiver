use axum::{extract::State, http::HeaderMap, response::Json, routing::post, Router};
use chrono::Utc;
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use crate::kafka::ExceptionKafkaMessage;
use tracing::{error, info};

#[derive(Debug, Serialize)]
struct ExceptionResponse {
    id: Uuid,
    message: String,
}

pub fn create_exceptions_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/", post(ingest_exception))
        .route("/batch", post(ingest_exception_batch))
}

/// Convert ExceptionPayload to ExceptionKafkaMessage with fingerprint
fn payload_to_kafka_message(
    exception_id: Uuid,
    project_id: Uuid,
    payload: &crate::models::ExceptionPayload,
) -> ExceptionKafkaMessage {
    // Generate fingerprint using the same algorithm as the Kafka consumer
    let fingerprint = crate::fingerprint::generate_fingerprint(payload);

    // Format timestamp as ISO 8601 string for ClickHouse parseDateTime64BestEffort
    let timestamp = payload.timestamp.unwrap_or_else(Utc::now).to_rfc3339();

    ExceptionKafkaMessage {
        id: exception_id.to_string(),
        project_id: project_id.to_string(),
        fingerprint,
        level: payload.level.clone(),
        message: payload.message.clone(),
        exception_type: payload.exception.as_ref().map(|e| e.exception_type.clone()),
        exception_value: payload.exception.as_ref().map(|e| e.value.clone()),
        stacktrace: payload
            .exception
            .as_ref()
            .and_then(|e| e.stacktrace.as_ref())
            .map(|s| serde_json::to_string(s).unwrap_or_default())
            .unwrap_or_default(),
        context: payload
            .context
            .as_ref()
            .map(|c| serde_json::to_string(c).unwrap_or_default())
            .unwrap_or_else(|| "{}".to_string()),
        tags: payload
            .tags
            .as_ref()
            .map(|t| serde_json::to_string(t).unwrap_or_default())
            .unwrap_or_else(|| "{}".to_string()),
        user_data: payload
            .user
            .as_ref()
            .map(|u| serde_json::to_string(u).unwrap_or_default())
            .unwrap_or_else(|| "{}".to_string()),
        service_name: payload.service_name.clone(),
        timestamp,
        trace_id: payload.trace_id.clone(),
        span_id: payload.span_id.clone(),
        // Deploy version tracking (for GitHub integration)
        service_version: payload.version.clone(),
        environment: payload.environment.clone(),
        repository_url: payload.repository_url.clone(),
    }
}

async fn ingest_exception(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<crate::models::ExceptionPayload>,
) -> Result<Json<ExceptionResponse>> {
    let exception_id = Uuid::new_v4();
    info!(
        "[EXCEPTION_INGEST] Starting exception ingestion, exception_id={}",
        exception_id
    );

    let project_id = crate::api::extract_project_id(&headers)?;

    info!(
        "[EXCEPTION_INGEST] Found project_id={} for exception_id={}",
        project_id, exception_id
    );

    // Generate fingerprint and create Kafka message
    let kafka_message = payload_to_kafka_message(exception_id, project_id, &payload);
    info!(
        "[EXCEPTION_INGEST] Generated fingerprint={} for exception_id={}",
        &kafka_message.fingerprint[..16],
        exception_id
    );

    // Send exception to Kafka (PostHog-style: immediate write, no worker)
    state
        .kafka
        .send_exception(&kafka_message)
        .await
        .map_err(|e| {
            error!(
                "[EXCEPTION_INGEST] Failed to send exception to Kafka: {} for exception_id={}",
                e, exception_id
            );
            AppError::Internal(anyhow::anyhow!("Failed to send exception to Kafka: {}", e))
        })?;

    info!(
        "[EXCEPTION_INGEST] Exception sent to Kafka, exception_id={}",
        exception_id
    );

    Ok(Json(ExceptionResponse {
        id: exception_id,
        message: "Exception sent to Kafka".to_string(),
    }))
}

#[derive(Debug, Serialize)]
struct BatchExceptionResponse {
    ids: Vec<Uuid>,
    count: usize,
    message: String,
}

async fn ingest_exception_batch(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payloads): Json<Vec<crate::models::ExceptionPayload>>,
) -> Result<Json<BatchExceptionResponse>> {
    if payloads.is_empty() {
        return Err(AppError::Internal(anyhow::anyhow!("Batch cannot be empty")));
    }

    if payloads.len() > 100 {
        return Err(AppError::Internal(anyhow::anyhow!(
            "Batch size cannot exceed 100 exceptions"
        )));
    }

    info!(
        "[EXCEPTION_INGEST_BATCH] Starting batch ingestion, count={}",
        payloads.len()
    );

    let project_id = crate::api::extract_project_id(&headers)?;

    info!(
        "[EXCEPTION_INGEST_BATCH] Found project_id={} for batch, count={}",
        project_id,
        payloads.len()
    );

    // Generate fingerprints and create Kafka messages for all payloads
    let kafka_messages: Vec<ExceptionKafkaMessage> = payloads
        .iter()
        .map(|payload| {
            let exception_id = Uuid::new_v4();
            payload_to_kafka_message(exception_id, project_id, payload)
        })
        .collect();

    let ids: Vec<Uuid> = kafka_messages
        .iter()
        .map(|m| Uuid::parse_str(&m.id).unwrap_or_else(|_| Uuid::new_v4()))
        .collect();

    // Process all exceptions in the batch — send all to Kafka in batch
    state
        .kafka
        .send_exceptions_batch(&kafka_messages)
        .await
        .map_err(|e| {
            error!(
                "[EXCEPTION_INGEST_BATCH] Failed to send exceptions to Kafka: {}",
                e
            );
            AppError::Internal(anyhow::anyhow!("Failed to send exceptions to Kafka: {}", e))
        })?;

    let success_count = kafka_messages.len();

    info!(
        "[EXCEPTION_INGEST_BATCH] Batch sent to Kafka: {} exceptions",
        success_count
    );

    Ok(Json(BatchExceptionResponse {
        ids,
        count: success_count,
        message: format!("{} exceptions sent to Kafka", success_count),
    }))
}
