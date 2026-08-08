//! AWS X-Ray trace ingestion API endpoints
//!
//! This module provides endpoints for ingesting AWS X-Ray trace segments.
//! X-Ray sends trace segments in JSON format, which we convert to our internal span format.

use axum::{body::Bytes, extract::State, http::HeaderMap, response::Json, routing::post, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use crate::models::SpanPayload;

#[derive(Debug, Serialize)]
struct XRayResponse {
    segments_received: usize,
    segments_processed: usize,
    message: String,
}

/// AWS X-Ray segment format
/// X-Ray sends segments in JSON format with this structure
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields used by serde deserialization
struct XRaySegment {
    #[serde(rename = "format")]
    format: Option<String>,
    #[serde(rename = "version")]
    version: Option<u32>,
    #[serde(rename = "name")]
    name: String,
    #[serde(rename = "id")]
    id: String,
    #[serde(rename = "trace_id")]
    trace_id: String,
    #[serde(rename = "start_time")]
    start_time: f64,
    #[serde(rename = "end_time")]
    end_time: Option<f64>,
    #[serde(rename = "parent_id")]
    parent_id: Option<String>,
    #[serde(rename = "subsegments")]
    subsegments: Option<Vec<XRaySegment>>,
    #[serde(rename = "metadata")]
    metadata: Option<serde_json::Value>,
    #[serde(rename = "annotations")]
    annotations: Option<serde_json::Value>,
    #[serde(rename = "aws")]
    aws: Option<serde_json::Value>,
    #[serde(rename = "http")]
    http: Option<serde_json::Value>,
    #[serde(rename = "error")]
    error: Option<serde_json::Value>,
    #[serde(rename = "fault")]
    fault: Option<bool>,
    #[serde(rename = "throttle")]
    throttle: Option<bool>,
    #[serde(rename = "in_progress")]
    in_progress: Option<bool>,
}

pub fn create_xray_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/ingest", post(ingest_xray_segment))
        .route("/ingest/batch", post(ingest_xray_segments_batch))
}

/// Ingest a single X-Ray segment
/// POST /api/xray/ingest
///
/// Accepts X-Ray segment JSON and converts it to our span format
async fn ingest_xray_segment(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<XRayResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    // Parse X-Ray segment JSON
    let segment: XRaySegment = serde_json::from_slice(&body).map_err(|e| {
        error!("Failed to parse X-Ray segment JSON: {}", e);
        AppError::Validation(format!("Invalid X-Ray segment format: {}", e))
    })?;

    info!(
        "[XRAY_INGEST] Received X-Ray segment: id={}, trace_id={}, name={}",
        segment.id, segment.trace_id, segment.name
    );

    // Convert X-Ray segment to spans and send to Kafka
    let spans = convert_xray_segment_to_spans(&segment, project_id)?;

    let mut processed = 0;
    for span in spans {
        // Send span to Kafka (keyed by trace_id for partitioning)
        match state.kafka.send_span(&span.trace_id, &span).await {
            Ok(_) => {
                processed += 1;
                info!(
                    "[XRAY_INGEST] Sent span to Kafka: span_id={}, trace_id={}",
                    span.span_id, span.trace_id
                );
            }
            Err(e) => {
                error!(
                    "[XRAY_INGEST] Failed to send span to Kafka: {} for span_id={}",
                    e, span.span_id
                );
            }
        }
    }

    Ok(Json(XRayResponse {
        segments_received: 1,
        segments_processed: processed,
        message: format!("Processed {} spans from X-Ray segment", processed),
    }))
}

/// Ingest multiple X-Ray segments (batch)
/// POST /api/xray/ingest/batch
///
/// Accepts an array of X-Ray segment JSON objects
async fn ingest_xray_segments_batch(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(segments): Json<Vec<XRaySegment>>,
) -> Result<Json<XRayResponse>> {
    if segments.is_empty() {
        return Err(AppError::Validation("Batch cannot be empty".to_string()));
    }

    if segments.len() > 1000 {
        return Err(AppError::Validation(
            "Batch size cannot exceed 1000 segments".to_string(),
        ));
    }

    let project_id = crate::api::extract_project_id(&headers)?;

    let segments_received = segments.len();
    info!(
        "[XRAY_INGEST_BATCH] Received {} X-Ray segments",
        segments_received
    );

    let mut all_spans = Vec::new();

    // Convert all segments to spans
    for segment in segments {
        match convert_xray_segment_to_spans(&segment, project_id) {
            Ok(spans) => {
                all_spans.extend(spans);
            }
            Err(e) => {
                warn!(
                    "[XRAY_INGEST_BATCH] Failed to convert X-Ray segment {}: {}",
                    segment.id, e
                );
            }
        }
    }

    // Send all spans to Kafka in batch
    let spans_with_keys: Vec<(String, SpanPayload)> = all_spans
        .into_iter()
        .map(|span| (span.trace_id.clone(), span))
        .collect();

    let total_processed = match state.kafka.send_spans_batch(&spans_with_keys).await {
        Ok(_) => {
            let n = spans_with_keys.len();
            info!("[XRAY_INGEST_BATCH] Sent {} spans to Kafka", n);
            n
        }
        Err(e) => {
            error!("[XRAY_INGEST_BATCH] Failed to send spans to Kafka: {}", e);
            return Err(AppError::Internal(anyhow::anyhow!(
                "Failed to send spans to Kafka: {}",
                e
            )));
        }
    };

    Ok(Json(XRayResponse {
        segments_received,
        segments_processed: total_processed,
        message: format!(
            "Processed {} spans from {} X-Ray segments",
            total_processed, segments_received
        ),
    }))
}

/// Convert X-Ray segment to our SpanPayload format
/// Recursively processes subsegments as child spans
fn convert_xray_segment_to_spans(
    segment: &XRaySegment,
    project_id: Uuid,
) -> Result<Vec<SpanPayload>> {
    let mut spans = Vec::new();

    // Convert X-Ray timestamp (seconds since epoch) to DateTime
    let start_time =
        DateTime::from_timestamp(segment.start_time as i64, 0).unwrap_or_else(Utc::now);

    use crate::models::{SpanKind, StatusCode};

    // Determine status based on error/fault/throttle
    let status_code = if segment.fault.unwrap_or(false)
        || segment.throttle.unwrap_or(false)
        || segment.error.is_some()
    {
        StatusCode::StatusCodeError
    } else {
        StatusCode::StatusCodeOk
    };

    // Duration in nanoseconds (X-Ray uses seconds)
    let duration_ns = if let Some(end_time) = segment.end_time {
        ((end_time - segment.start_time) * 1_000_000_000.0) as i64
    } else {
        0
    };

    // Build span attributes from X-Ray segment data
    let mut span_attributes = std::collections::HashMap::new();

    // Add X-Ray specific attributes
    span_attributes.insert("xray.segment_id".to_string(), segment.id.clone());
    span_attributes.insert("xray.segment_name".to_string(), segment.name.clone());

    if let Some(ref annotations) = segment.annotations {
        if let serde_json::Value::Object(ann_map) = annotations {
            for (key, value) in ann_map {
                let val_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => serde_json::to_string(value).unwrap_or_default(),
                };
                span_attributes.insert(format!("xray.annotation.{}", key), val_str);
            }
        }
    }

    if let Some(ref metadata) = segment.metadata {
        span_attributes.insert(
            "xray.metadata".to_string(),
            serde_json::to_string(metadata).unwrap_or_default(),
        );
    }

    if let Some(ref aws) = segment.aws {
        span_attributes.insert(
            "xray.aws".to_string(),
            serde_json::to_string(aws).unwrap_or_default(),
        );
    }

    if let Some(ref http) = segment.http {
        span_attributes.insert(
            "xray.http".to_string(),
            serde_json::to_string(http).unwrap_or_default(),
        );

        // Extract HTTP-specific fields
        if let serde_json::Value::Object(http_map) = http {
            if let Some(serde_json::Value::String(method)) = http_map.get("request") {
                span_attributes.insert("http.method".to_string(), method.clone());
            }
            if let Some(serde_json::Value::String(url)) = http_map.get("request_url") {
                span_attributes.insert("http.url".to_string(), url.clone());
            }
            if let Some(serde_json::Value::Number(status)) = http_map.get("response") {
                if let Some(sc) = status.as_u64() {
                    span_attributes.insert("http.status_code".to_string(), sc.to_string());
                }
            }
        }
    }

    if let Some(ref error) = segment.error {
        span_attributes.insert(
            "xray.error".to_string(),
            serde_json::to_string(error).unwrap_or_default(),
        );
        span_attributes.insert("error".to_string(), "true".to_string());
    }

    // Create main span from segment (X-Ray segments are server spans by default)
    let span = SpanPayload {
        project_key: project_id.to_string(),
        trace_id: segment.trace_id.clone(),
        span_id: segment.id.clone(),
        parent_span_id: segment.parent_id.clone(),
        trace_state: None,
        span_name: segment.name.clone(),
        span_kind: SpanKind::SpanKindServer,
        service_name: Some(segment.name.clone()), // X-Ray name is typically the service name
        start_time: Some(start_time),
        duration_ns: Some(duration_ns),
        status_code,
        status_message: None,
        span_attributes,
        resource_attributes: std::collections::HashMap::new(),
        events: None,
        links: None,
    };

    spans.push(span);

    // Recursively process subsegments as child spans
    if let Some(ref subsegments) = segment.subsegments {
        for subsegment in subsegments {
            match convert_xray_segment_to_spans(subsegment, project_id) {
                Ok(mut child_spans) => {
                    // Update parent_span_id for child spans to point to this segment
                    for child_span in &mut child_spans {
                        if child_span.parent_span_id.is_none() {
                            child_span.parent_span_id = Some(segment.id.clone());
                        }
                    }
                    spans.extend(child_spans);
                }
                Err(e) => {
                    warn!(
                        "Failed to convert X-Ray subsegment {}: {}",
                        subsegment.id, e
                    );
                }
            }
        }
    }

    Ok(spans)
}
