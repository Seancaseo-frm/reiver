//! ConfigCat webhook handler
//!
//! POST /api/v1/events/webhooks/configcat
//! Accepts ConfigCat webhook format directly, converts to our format internally

use axum::{extract::State, response::Json};
use serde_json;
use std::sync::Arc;
use tracing::{info, error};
use crate::common::{EventResponse, FeatureFlagChangeEvent, FeatureFlagEventStorage, ChangedBy};
use chrono::Utc;

/// ConfigCat webhook handler
pub async fn handler<S: FeatureFlagEventStorage>(
    State(storage): State<Arc<S>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>, String> {
    info!("[EVENTS] Received ConfigCat webhook");
    
    let feature_flag = payload.get("featureFlag")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'featureFlag' in ConfigCat webhook");
            "Missing required field: featureFlag".to_string()
        })?;
    
    let flag_id = feature_flag.get("key")
        .or_else(|| feature_flag.get("id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'key' or 'id' in ConfigCat featureFlag object");
            "Missing required field: featureFlag.key or featureFlag.id".to_string()
        })?
        .to_string();
    
    let flag_name = feature_flag.get("name").and_then(|v| v.as_str()).map(|s| s.to_string());
    let environment = payload.get("environment").and_then(|v| v.as_object());
    let env_name = environment.and_then(|e| e.get("name")).and_then(|v| v.as_str()).map(|s| s.to_string());
    
    let project_key = payload.get("_reiver_project_key")
        .or_else(|| payload.get("metadata").and_then(|m| m.get("reiver_project_key")))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing project_key in ConfigCat webhook. Add 'reiver_project_key' to webhook metadata.");
            "Missing required field: reiver_project_key (add to ConfigCat webhook metadata)".to_string()
        })?
        .to_string();
    
    let setting = payload.get("setting").and_then(|v| v.as_object());
    let enabled = setting.and_then(|s| s.get("value")).and_then(|v| v.as_bool()).unwrap_or(false);
    let prev_enabled = setting.and_then(|s| s.get("previousValue")).and_then(|v| v.as_bool());
    
    let changed_by = payload.get("user").and_then(|u| {
        let u_obj = u.as_object()?;
        Some(ChangedBy {
            type_: "user".to_string(),
            email: u_obj.get("email").and_then(|v| v.as_str()).map(|s| s.to_string()),
            name: u_obj.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()),
            id: u_obj.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    });
    
    let event_type = payload.get("event").and_then(|v| v.as_str()).unwrap_or("FEATURE_FLAG_UPDATED");
    let timestamp = payload.get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| Some(Utc::now()))
        .unwrap();
    
    let change_type = match event_type {
        "FEATURE_FLAG_DELETED" => "delete".to_string(),
        "FEATURE_FLAG_CREATED" => "create".to_string(),
        "FEATURE_FLAG_UPDATED" => {
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
    
    let prev_value_json = prev_enabled.map(|prev| serde_json::json!({"enabled": prev}));
    
    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key,
        flag_id: flag_id.clone(),
        flag_name,
        environment: env_name,
        changed_by,
        change_type,
        prev_value: prev_value_json,
        new_value: serde_json::json!({"enabled": enabled}),
        impacted_services: None,
        metadata: Some(serde_json::json!({
            "source": "configcat",
            "configcat_event": event_type,
            "configcat_webhook": payload.clone(),
        })),
        timestamp: Some(timestamp),
    };
    
    let change_id = storage.store_flag_change(change_event).await
        .map_err(|e| format!("Failed to store flag change: {}", e))?;
    
    info!("[EVENTS] Processed ConfigCat webhook: flag={}, change_id={}", flag_id, change_id);
    
    Ok(Json(EventResponse {
        id: change_id,
        message: "Processed ConfigCat feature flag change".to_string(),
    }))
}

