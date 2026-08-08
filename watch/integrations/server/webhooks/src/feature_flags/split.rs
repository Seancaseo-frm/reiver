//! Split.io (Harness) webhook handler
//!
//! POST /api/v1/events/webhooks/split
//! Accepts Split.io webhook format directly, converts to our format internally

use axum::{extract::State, response::Json};
use serde_json;
use std::sync::Arc;
use tracing::{info, error};
use crate::common::{EventResponse, FeatureFlagChangeEvent, FeatureFlagEventStorage};
use chrono::Utc;

/// Split.io webhook handler
pub async fn handler<S: FeatureFlagEventStorage>(
    State(storage): State<Arc<S>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>, String> {
    info!("[EVENTS] Received Split.io webhook");
    
    let event_type = payload.get("eventType").and_then(|v| v.as_str()).unwrap_or("SPLIT_UPDATE");
    let data = payload.get("data").and_then(|v| v.as_object()).ok_or_else(|| {
        error!("[EVENTS] Missing 'data' in Split.io webhook");
        "Missing required field: data".to_string()
    })?;
    
    let flag_id = data.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
        error!("[EVENTS] Missing 'name' in Split.io webhook data");
        "Missing required field: data.name".to_string()
    })?.to_string();
    
    let env_name = payload.get("environment")
        .or_else(|| data.get("environment").and_then(|e| e.as_object()).and_then(|e| e.get("name")))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    
    let project_key = payload.get("_reiver_project_key")
        .or_else(|| payload.get("metadata").and_then(|m| m.get("reiver_project_key")))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing project_key in Split.io webhook. Add 'reiver_project_key' to webhook metadata.");
            "Missing required field: reiver_project_key (add to Split.io webhook metadata)".to_string()
        })?.to_string();
    
    let definition = data.get("definition").and_then(|v| v.as_object());
    let enabled = data.get("killed")
        .and_then(|v| v.as_bool())
        .map(|killed| !killed)
        .or_else(|| definition.and_then(|d| d.get("on")).and_then(|v| v.as_bool()))
        .unwrap_or(false);
    
    let timestamp_ms = payload.get("timestamp")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    let timestamp = chrono::DateTime::from_timestamp(timestamp_ms / 1000, ((timestamp_ms % 1000) * 1_000_000) as u32)
        .unwrap_or_else(|| Utc::now());
    
    let change_type = match event_type {
        "SPLIT_DELETED" | "SPLIT_ARCHIVED" => "delete".to_string(),
        "SPLIT_CREATED" => "create".to_string(),
        "SPLIT_UPDATE" => if enabled { "toggle_on".to_string() } else { "toggle_off".to_string() },
        _ => "toggle".to_string(),
    };
    
    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key,
        flag_id: flag_id.clone(),
        flag_name: None,
        environment: env_name,
        changed_by: None,
        change_type,
        prev_value: None,
        new_value: serde_json::json!({
            "enabled": enabled,
            "killed": data.get("killed").and_then(|v| v.as_bool()).unwrap_or(false),
        }),
        impacted_services: None,
        metadata: Some(serde_json::json!({
            "source": "split",
            "split_event_type": event_type,
            "split_webhook": payload.clone(),
        })),
        timestamp: Some(timestamp),
    };
    
    let change_id = storage.store_flag_change(change_event).await
        .map_err(|e| format!("Failed to store flag change: {}", e))?;
    
    info!("[EVENTS] Processed Split.io webhook: flag={}, change_id={}", flag_id, change_id);
    
    Ok(Json(EventResponse {
        id: change_id,
        message: "Processed Split.io feature flag change".to_string(),
    }))
}

