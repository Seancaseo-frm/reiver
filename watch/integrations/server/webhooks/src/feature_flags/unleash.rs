//! Unleash webhook handler
//!
//! POST /api/v1/events/webhooks/unleash
//! Accepts Unleash webhook format directly, converts to our format internally

use axum::{extract::State, response::Json};
use serde_json;
use std::sync::Arc;
use tracing::{info, error};
use uuid::Uuid;

use crate::common::{EventResponse, FeatureFlagChangeEvent, FeatureFlagEventStorage, ChangedBy};
use chrono::Utc;

/// Unleash webhook handler
pub async fn handler<S: FeatureFlagEventStorage>(
    State(storage): State<Arc<S>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>, String> {
    info!("[EVENTS] Received Unleash webhook");
    
    // Parse Unleash webhook format
    let event_type = payload.get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("feature-toggles-updated");
    
    // Extract data object
    let data = payload.get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'data' in Unleash webhook");
            "Missing required field: data".to_string()
        })?;
    
    let flag_name = data.get("featureName")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    
    let flag_id = flag_name.clone().unwrap_or_else(|| "unknown".to_string());
    
    // Extract project_key from data.project or metadata
    let project_key = payload.get("_reiver_project_key")
        .or_else(|| payload.get("metadata").and_then(|m| m.get("reiver_project_key")))
        .or_else(|| data.get("reiver_project_key"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing project_key in Unleash webhook. Add 'reiver_project_key' to webhook metadata.");
            "Missing required field: reiver_project_key (add to Unleash webhook metadata)".to_string()
        })?
        .to_string();
    
    // Extract environments array
    let environments = data.get("environments")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'environments' in Unleash webhook data");
            "Missing required field: data.environments".to_string()
        })?;
    
    // Extract createdBy user info
    let created_by = payload.get("createdBy").and_then(|cb| {
        let cb_obj = cb.as_object()?;
        Some(ChangedBy {
            type_: "user".to_string(),
            email: cb_obj.get("email").and_then(|v| v.as_str()).map(|s| s.to_string()),
            name: cb_obj.get("username").and_then(|v| v.as_str()).map(|s| s.to_string()),
            id: cb_obj.get("id").and_then(|v| v.as_u64()).map(|id| id.to_string()),
        })
    });
    
    // Extract timestamp
    let timestamp = payload.get("createdAt")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| Some(Utc::now()))
        .unwrap();
    
    // Process each environment change
    let mut change_ids = Vec::new();
    
    for env_data in environments {
        let env_obj = env_data.as_object().ok_or_else(|| {
            error!("[EVENTS] Invalid environment object in Unleash webhook");
            "Invalid environment object in data.environments".to_string()
        })?;
        
        let env_name = env_obj.get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                error!("[EVENTS] Missing 'name' in environment object");
                "Missing 'name' in environment object".to_string()
            })?;
        
        let enabled = env_obj.get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        
        // Determine change type based on event_type and enabled state
        let change_type = match event_type {
            "feature-toggles-archived" => "delete".to_string(),
            "feature-toggles-created" => "create".to_string(),
            "feature-toggles-updated" => if enabled { "toggle_on".to_string() } else { "toggle_off".to_string() },
            _ => "toggle".to_string(),
        };
        
        // Convert to our format
        let change_event = FeatureFlagChangeEvent {
            event_type: "feature_flag_change".to_string(),
            project_key: project_key.clone(),
            flag_id: flag_id.clone(),
            flag_name: flag_name.clone(),
            environment: Some(env_name.to_string()),
            changed_by: created_by.clone(),
            change_type,
            prev_value: None, // Unleash doesn't send previous state
            new_value: serde_json::json!({
                "enabled": enabled,
            }),
            impacted_services: None, // Will be auto-detected
            metadata: Some(serde_json::json!({
                "source": "unleash",
                "unleash_event_type": event_type,
                "unleash_project": data.get("project"),
                "unleash_webhook": payload.clone(),
            })),
            timestamp: Some(timestamp),
        };
        
        // Store the change
        let change_id = storage.store_flag_change(change_event).await
            .map_err(|e| format!("Failed to store flag change: {}", e))?;
        change_ids.push(change_id);
    }
    
    info!("[EVENTS] Processed Unleash webhook: flag={}, changes={}", flag_id, change_ids.len());
    
    Ok(Json(EventResponse {
        id: change_ids.first().copied().unwrap_or_else(|| Uuid::new_v4()),
        message: format!("Processed {} environment changes", change_ids.len()),
    }))
}

