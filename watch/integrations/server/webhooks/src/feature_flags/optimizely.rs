//! Optimizely webhook handler
//!
//! POST /api/v1/events/webhooks/optimizely
//! Accepts Optimizely webhook format directly, converts to our format internally

use axum::{extract::State, response::Json};
use serde_json;
use std::sync::Arc;
use tracing::{info, error};
use crate::common::{EventResponse, FeatureFlagChangeEvent, FeatureFlagEventStorage, ChangedBy};
use chrono::Utc;

/// Optimizely webhook handler
pub async fn handler<S: FeatureFlagEventStorage>(
    State(storage): State<Arc<S>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>, String> {
    info!("[EVENTS] Received Optimizely webhook");
    
    let event_type = payload.get("event").and_then(|v| v.as_str()).unwrap_or("flag.updated");
    let data = payload.get("data").and_then(|v| v.as_object()).ok_or_else(|| {
        error!("[EVENTS] Missing 'data' in Optimizely webhook");
        "Missing required field: data".to_string()
    })?;
    
    let flag_id = data.get("key").and_then(|v| v.as_str()).ok_or_else(|| {
        error!("[EVENTS] Missing 'key' in Optimizely webhook data");
        "Missing required field: data.key".to_string()
    })?.to_string();
    
    let flag_name = data.get("name").and_then(|v| v.as_str()).map(|s| s.to_string());
    let env_name = data.get("environment_key").and_then(|v| v.as_str()).map(|s| s.to_string());
    
    let project_key = payload.get("_reiver_project_key")
        .or_else(|| payload.get("metadata").and_then(|m| m.get("reiver_project_key")))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing project_key in Optimizely webhook. Add 'reiver_project_key' to webhook metadata.");
            "Missing required field: reiver_project_key (add to Optimizely webhook metadata)".to_string()
        })?.to_string();
    
    let enabled = data.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
    let changed_by = payload.get("user").and_then(|u| {
        let u_obj = u.as_object()?;
        Some(ChangedBy {
            type_: "user".to_string(),
            email: u_obj.get("email").and_then(|v| v.as_str()).map(|s| s.to_string()),
            name: None,
            id: u_obj.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    });
    
    let timestamp = payload.get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| Some(Utc::now()))
        .unwrap();
    
    let change_type = match event_type {
        "flag.deleted" => "delete".to_string(),
        "flag.created" => "create".to_string(),
        "flag.enabled" => "toggle_on".to_string(),
        "flag.disabled" => "toggle_off".to_string(),
        "flag.updated" => if enabled { "toggle_on".to_string() } else { "toggle_off".to_string() },
        _ => "toggle".to_string(),
    };
    
    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key,
        flag_id: flag_id.clone(),
        flag_name,
        environment: env_name,
        changed_by,
        change_type,
        prev_value: None,
        new_value: serde_json::json!({"enabled": enabled}),
        impacted_services: None,
        metadata: Some(serde_json::json!({
            "source": "optimizely",
            "optimizely_event": event_type,
            "optimizely_webhook": payload.clone(),
        })),
        timestamp: Some(timestamp),
    };
    
    let change_id = storage.store_flag_change(change_event).await
        .map_err(|e| format!("Failed to store flag change: {}", e))?;
    
    info!("[EVENTS] Processed Optimizely webhook: flag={}, change_id={}", flag_id, change_id);
    
    Ok(Json(EventResponse {
        id: change_id,
        message: "Processed Optimizely feature flag change".to_string(),
    }))
}

