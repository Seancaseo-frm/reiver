//! GO Feature Flag webhook handler
//!
//! POST /api/v1/events/webhooks/gofeatureflag
//! Accepts GO Feature Flag webhook format directly, converts to our format internally

use axum::{extract::State, response::Json};
use serde_json;
use std::sync::Arc;
use tracing::{info, error};
use crate::common::{EventResponse, FeatureFlagChangeEvent, FeatureFlagEventStorage};
use chrono::Utc;

/// GO Feature Flag webhook handler
pub async fn handler<S: FeatureFlagEventStorage>(
    State(storage): State<Arc<S>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>, String> {
    info!("[EVENTS] Received GO Feature Flag webhook");
    
    let event_type = payload.get("event").and_then(|v| v.as_str()).unwrap_or("flag.updated");
    let flag_obj = payload.get("flag").and_then(|v| v.as_object()).ok_or_else(|| {
        error!("[EVENTS] Missing 'flag' in GO Feature Flag webhook");
        "Missing required field: flag".to_string()
    })?;
    
    let flag_id = flag_obj.get("key").and_then(|v| v.as_str()).ok_or_else(|| {
        error!("[EVENTS] Missing 'key' in GO Feature Flag flag object");
        "Missing required field: flag.key".to_string()
    })?.to_string();
    
    let env_name = payload.get("environment").and_then(|v| v.as_str()).map(|s| s.to_string());
    
    let project_key = payload.get("_reiver_project_key")
        .or_else(|| payload.get("metadata").and_then(|m| m.get("reiver_project_key")))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing project_key in GO Feature Flag webhook. Add 'reiver_project_key' to webhook metadata.");
            "Missing required field: reiver_project_key (add to GO Feature Flag webhook metadata)".to_string()
        })?.to_string();
    
    let enabled = flag_obj.get("defaultValue").and_then(|v| v.as_bool()).unwrap_or(false);
    
    let timestamp = payload.get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| Some(Utc::now()))
        .unwrap();
    
    let change_type = match event_type {
        "flag.deleted" => "delete".to_string(),
        "flag.created" => "create".to_string(),
        "flag.updated" => if enabled { "toggle_on".to_string() } else { "toggle_off".to_string() },
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
        new_value: serde_json::json!({"enabled": enabled}),
        impacted_services: None,
        metadata: Some(serde_json::json!({
            "source": "gofeatureflag",
            "gofeatureflag_event_type": event_type,
            "gofeatureflag_webhook": payload.clone(),
        })),
        timestamp: Some(timestamp),
    };
    
    let change_id = storage.store_flag_change(change_event).await
        .map_err(|e| format!("Failed to store flag change: {}", e))?;
    
    info!("[EVENTS] Processed GO Feature Flag webhook: flag={}, change_id={}", flag_id, change_id);
    
    Ok(Json(EventResponse {
        id: change_id,
        message: "Processed GO Feature Flag feature flag change".to_string(),
    }))
}

