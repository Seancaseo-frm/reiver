//! CloudBees Feature Flags webhook handler
//!
//! POST /api/v1/events/webhooks/cloudbees
//! Accepts CloudBees webhook format directly, converts to our format internally

use axum::{extract::State, response::Json};
use serde_json;
use std::sync::Arc;
use tracing::{info, error};
use uuid::Uuid;

use crate::common::{EventResponse, FeatureFlagChangeEvent, FeatureFlagEventStorage, ChangedBy};
use chrono::Utc;

/// CloudBees webhook handler
pub async fn handler<S: FeatureFlagEventStorage>(
    State(storage): State<Arc<S>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>, String> {
    info!("[EVENTS] Received CloudBees webhook");
    
    let event_type = payload.get("eventType").and_then(|v| v.as_str()).unwrap_or("FLAG_UPDATED");
    let flag_obj = payload.get("flag").and_then(|v| v.as_object()).ok_or_else(|| {
        error!("[EVENTS] Missing 'flag' in CloudBees webhook");
        "Missing required field: flag".to_string()
    })?;
    
    let flag_id = flag_obj.get("key").and_then(|v| v.as_str()).ok_or_else(|| {
        error!("[EVENTS] Missing 'key' in CloudBees flag object");
        "Missing required field: flag.key".to_string()
    })?.to_string();
    
    let flag_name = flag_obj.get("name").and_then(|v| v.as_str()).map(|s| s.to_string());
    
    let project_key = payload.get("_reiver_project_key")
        .or_else(|| payload.get("metadata").and_then(|m| m.get("reiver_project_key")))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing project_key in CloudBees webhook. Add 'reiver_project_key' to webhook metadata.");
            "Missing required field: reiver_project_key (add to CloudBees webhook metadata)".to_string()
        })?.to_string();
    
    let environments = flag_obj.get("environments").and_then(|v| v.as_object());
    let changed_by = payload.get("user").and_then(|u| {
        let u_obj = u.as_object()?;
        Some(ChangedBy {
            type_: "user".to_string(),
            email: u_obj.get("email").and_then(|v| v.as_str()).map(|s| s.to_string()),
            name: u_obj.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()),
            id: None,
        })
    });
    
    let timestamp = payload.get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| Some(Utc::now()))
        .unwrap();
    
    let change_type = match event_type {
        "FLAG_DELETED" => "delete".to_string(),
        "FLAG_CREATED" => "create".to_string(),
        "FLAG_UPDATED" => "toggle".to_string(),
        _ => "toggle".to_string(),
    };
    
    let mut change_ids = Vec::new();
    
    if let Some(environments) = environments {
        for (env_name, env_data) in environments {
            let env_obj = match env_data.as_object() {
                Some(obj) => obj,
                None => continue,
            };
            let enabled = env_obj.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
            
            let change_event = FeatureFlagChangeEvent {
                event_type: "feature_flag_change".to_string(),
                project_key: project_key.clone(),
                flag_id: flag_id.clone(),
                flag_name: flag_name.clone(),
                environment: Some(env_name.clone()),
                changed_by: changed_by.clone(),
                change_type: change_type.clone(),
                prev_value: None,
                new_value: serde_json::json!({"enabled": enabled}),
                impacted_services: None,
                metadata: Some(serde_json::json!({
                    "source": "cloudbees",
                    "cloudbees_event_type": event_type,
                    "cloudbees_webhook": payload.clone(),
                })),
                timestamp: Some(timestamp),
            };
            
            let change_id = storage.store_flag_change(change_event).await
                .map_err(|e| format!("Failed to store flag change: {}", e))?;
            change_ids.push(change_id);
        }
    } else {
        let change_event = FeatureFlagChangeEvent {
            event_type: "feature_flag_change".to_string(),
            project_key,
            flag_id: flag_id.clone(),
            flag_name,
            environment: None,
            changed_by,
            change_type,
            prev_value: None,
            new_value: serde_json::json!({}),
            impacted_services: None,
            metadata: Some(serde_json::json!({
                "source": "cloudbees",
                "cloudbees_event_type": event_type,
                "cloudbees_webhook": payload.clone(),
            })),
            timestamp: Some(timestamp),
        };
        
        let change_id = storage.store_flag_change(change_event).await
            .map_err(|e| format!("Failed to store flag change: {}", e))?;
        change_ids.push(change_id);
    }
    
    info!("[EVENTS] Processed CloudBees webhook: flag={}, changes={}", flag_id, change_ids.len());
    
    Ok(Json(EventResponse {
        id: change_ids.first().copied().unwrap_or_else(|| Uuid::new_v4()),
        message: format!("Processed {} environment changes", change_ids.len()),
    }))
}

