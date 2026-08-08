//! LaunchDarkly webhook handler
//!
//! POST /api/v1/events/webhooks/launchdarkly
//! Accepts LaunchDarkly webhook format directly, converts to our format internally

use axum::{extract::State, response::Json};
use serde_json;
use std::sync::Arc;
use tracing::{info, error};
use uuid::Uuid;

use crate::common::{EventResponse, FeatureFlagChangeEvent, FeatureFlagEventStorage};
use chrono::Utc;

/// LaunchDarkly webhook handler
pub async fn handler<S: FeatureFlagEventStorage>(
    State(storage): State<Arc<S>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>, String> {
    info!("[EVENTS] Received LaunchDarkly webhook");
    
    // Parse LaunchDarkly webhook format
    let flag_key = payload.get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'key' in LaunchDarkly webhook");
            "Missing required field: key".to_string()
        })?;
    
    let flag_name = payload.get("name").and_then(|v| v.as_str()).map(|s| s.to_string());
    
    // Extract project_key from metadata or require it in query params/headers
    let project_key = payload.get("_reiver_project_key")
        .or_else(|| payload.get("metadata").and_then(|m| m.get("reiver_project_key")))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing project_key in LaunchDarkly webhook. Add 'reiver_project_key' to flag metadata.");
            "Missing required field: reiver_project_key (add to LaunchDarkly flag metadata)".to_string()
        })?
        .to_string();
    
    // Extract environment changes from LaunchDarkly format
    let environments = payload.get("environments")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'environments' in LaunchDarkly webhook");
            "Missing required field: environments".to_string()
        })?;
    
    // Process each environment change
    let mut change_ids = Vec::new();
    
    for (env_name, env_data) in environments {
        // Extract flag state for this environment
        let enabled = env_data.get("on")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        
        let archived = env_data.get("archived")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        
        // Skip archived flags
        if archived {
            continue;
        }
        
        // Extract last modified timestamp
        let timestamp_ms = env_data.get("lastModified")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| {
                payload.get("creationDate")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_else(|| Utc::now().timestamp_millis())
            });
        
        let timestamp = chrono::DateTime::from_timestamp(
            timestamp_ms / 1000,
            ((timestamp_ms % 1000) * 1_000_000) as u32
        ).unwrap_or_else(|| Utc::now());
        
        // Convert to our format
        let change_event = FeatureFlagChangeEvent {
            event_type: "feature_flag_change".to_string(),
            project_key: project_key.clone(),
            flag_id: flag_key.to_string(),
            flag_name: flag_name.clone(),
            environment: Some(env_name.clone()),
            changed_by: None, // LaunchDarkly webhook doesn't include user info
            change_type: if enabled { "toggle_on".to_string() } else { "toggle_off".to_string() },
            prev_value: None, // LaunchDarkly doesn't send previous state
            new_value: serde_json::json!({
                "enabled": enabled,
                "archived": archived,
            }),
            impacted_services: None, // Will be auto-detected
            metadata: Some(serde_json::json!({
                "source": "launchdarkly",
                "flag_version": env_data.get("version"),
                "launchdarkly_webhook": payload.clone(),
            })),
            timestamp: Some(timestamp),
        };
        
        // Store the change
        let change_id = storage.store_flag_change(change_event).await
            .map_err(|e| format!("Failed to store flag change: {}", e))?;
        change_ids.push(change_id);
    }
    
    info!("[EVENTS] Processed LaunchDarkly webhook: flag={}, changes={}", flag_key, change_ids.len());
    
    Ok(Json(EventResponse {
        id: change_ids.first().copied().unwrap_or_else(|| Uuid::new_v4()),
        message: format!("Processed {} environment changes", change_ids.len()),
    }))
}

