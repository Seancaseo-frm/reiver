//! Flagsmith webhook handler
//!
//! POST /api/v1/events/webhooks/flagsmith
//! Accepts Flagsmith webhook format directly, converts to our format internally

use axum::{extract::State, response::Json};
use serde_json;
use std::sync::Arc;
use tracing::{info, error};
use crate::common::{EventResponse, FeatureFlagChangeEvent, FeatureFlagEventStorage, ChangedBy};
use chrono::Utc;

/// Flagsmith webhook handler
pub async fn handler<S: FeatureFlagEventStorage>(
    State(storage): State<Arc<S>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>, String> {
    info!("[EVENTS] Received Flagsmith webhook");
    
    // Parse Flagsmith webhook format
    let event_type = payload.get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("FLAG_UPDATED");
    
    // Extract data object
    let data = payload.get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'data' in Flagsmith webhook");
            "Missing required field: data".to_string()
        })?;
    
    // Extract feature info
    let feature = data.get("feature")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'feature' in Flagsmith webhook data");
            "Missing required field: data.feature".to_string()
        })?;
    
    let flag_key = feature.get("feature_key")
        .or_else(|| feature.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'feature_key' or 'name' in Flagsmith feature object");
            "Missing required field: data.feature.feature_key or data.feature.name".to_string()
        })?;
    
    let flag_id = flag_key.to_string();
    let flag_name = feature.get("name").and_then(|v| v.as_str()).map(|s| s.to_string());
    
    // Extract environment info
    let environment = data.get("environment")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'environment' in Flagsmith webhook data");
            "Missing required field: data.environment".to_string()
        })?;
    
    let env_name = environment.get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    
    // Extract project_key
    let project_key = payload.get("_reiver_project_key")
        .or_else(|| payload.get("metadata").and_then(|m| m.get("reiver_project_key")))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing project_key in Flagsmith webhook. Add 'reiver_project_key' to webhook metadata.");
            "Missing required field: reiver_project_key (add to Flagsmith webhook metadata)".to_string()
        })?
        .to_string();
    
    // Extract flag state
    let enabled = data.get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    
    let prev_enabled = data.get("previous_enabled")
        .and_then(|v| v.as_bool());
    
    // Extract value
    let value = data.get("value").cloned();
    let prev_value = data.get("previous_value").cloned();
    
    // Extract changed_by
    let changed_by = data.get("changed_by").and_then(|cb| {
        let cb_obj = cb.as_object()?;
        Some(ChangedBy {
            type_: "user".to_string(),
            email: cb_obj.get("email").and_then(|v| v.as_str()).map(|s| s.to_string()),
            name: cb_obj.get("first_name")
                .or_else(|| cb_obj.get("name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            id: cb_obj.get("id").and_then(|v| v.as_u64()).map(|id| id.to_string()),
        })
    });
    
    // Extract timestamp
    let timestamp = payload.get("created_date")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| Some(Utc::now()))
        .unwrap();
    
    // Determine change type
    let change_type = match event_type {
        "FLAG_DELETED" => "delete".to_string(),
        "FLAG_CREATED" => "create".to_string(),
        "FLAG_UPDATED" => {
            if let (Some(prev), Some(curr)) = (prev_enabled, Some(enabled)) {
                if prev != curr {
                    if enabled { "toggle_on".to_string() } else { "toggle_off".to_string() }
                } else {
                    "variant_change".to_string()
                }
            } else {
                if enabled { "toggle_on".to_string() } else { "toggle_off".to_string() }
            }
        },
        _ => "toggle".to_string(),
    };
    
    // Build new_value and prev_value
    let new_value_json = serde_json::json!({
        "enabled": enabled,
        "value": value,
    });
    
    let prev_value_json = if let (Some(prev_en), Some(prev_val)) = (prev_enabled, prev_value) {
        Some(serde_json::json!({
            "enabled": prev_en,
            "value": prev_val,
        }))
    } else if let Some(prev_en) = prev_enabled {
        Some(serde_json::json!({
            "enabled": prev_en,
        }))
    } else {
        None
    };
    
    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key,
        flag_id: flag_id.clone(),
        flag_name,
        environment: env_name,
        changed_by,
        change_type,
        prev_value: prev_value_json,
        new_value: new_value_json,
        impacted_services: None, // Will be auto-detected
        metadata: Some(serde_json::json!({
            "source": "flagsmith",
            "flagsmith_event_type": event_type,
            "flagsmith_webhook": payload.clone(),
        })),
        timestamp: Some(timestamp),
    };
    
    let change_id = storage.store_flag_change(change_event).await
        .map_err(|e| format!("Failed to store flag change: {}", e))?;
    
    info!("[EVENTS] Processed Flagsmith webhook: flag={}, change_id={}", flag_id, change_id);
    
    Ok(Json(EventResponse {
        id: change_id,
        message: "Processed Flagsmith feature flag change".to_string(),
    }))
}

