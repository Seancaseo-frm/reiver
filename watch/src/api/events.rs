//! Events API for tracking feature flag changes, deployments, and other change events
//!
//! Supports tracking:
//! - Feature flag configuration changes (toggle, rollout, variant changes)
//! - Deployments (future)
//! - Other change events (future)
//!
//! Endpoints:
//! - POST /api/v1/events (Universal API - works with any provider)
//! - POST /api/v1/events/webhooks/launchdarkly (LaunchDarkly-specific handler)
//!
//! Feature Flag Vendor Support:
//! ✅ LaunchDarkly - Direct webhook handler implemented
//! ✅ Unleash - Direct webhook handler implemented
//! ✅ Flagsmith - Direct webhook handler implemented
//! ✅ ConfigCat - Direct webhook handler implemented
//! ✅ Split.io (Harness) - Direct webhook handler implemented
//! ✅ CloudBees Feature Flags - Direct webhook handler implemented
//! ✅ Optimizely - Direct webhook handler implemented
//! ✅ GO Feature Flag - Direct webhook handler implemented
//! ✅ Flipt - Direct webhook handler implemented
//! ✅ GrowthBook - Direct webhook handler implemented

use axum::{extract::State, http::HeaderMap, response::Json, routing::post, Router};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use tracing::{error, info};

/// Response for event ingestion
#[derive(Debug, Clone, serde::Serialize)]
pub struct EventResponse {
    pub id: String,
    pub message: String,
}

/// Feature flag change event
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FeatureFlagChangeEvent {
    #[serde(default)]
    pub event_type: String,
    pub project_key: String,
    #[serde(alias = "flag_key")]
    pub flag_id: String,
    pub flag_name: Option<String>,
    pub change_type: String,
    pub environment: Option<String>,
    pub prev_value: Option<serde_json::Value>,
    pub new_value: serde_json::Value,
    pub changed_by: Option<ChangedBy>,
    pub impacted_services: Option<Vec<String>>,
    pub metadata: Option<serde_json::Value>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

/// Who made the change
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangedBy {
    #[serde(rename = "type", alias = "type_")]
    pub type_: Option<String>,
    pub id: Option<String>,
    pub email: Option<String>,
    pub name: Option<String>,
}

pub fn create_events_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/", post(ingest_event))
        // Feature flag provider webhooks
        .route("/webhooks/launchdarkly", post(handle_launchdarkly_webhook))
        .route("/webhooks/unleash", post(handle_unleash_webhook))
        .route("/webhooks/flagsmith", post(handle_flagsmith_webhook))
        .route("/webhooks/configcat", post(handle_configcat_webhook))
        .route("/webhooks/split", post(handle_split_webhook))
        .route("/webhooks/cloudbees", post(handle_cloudbees_webhook))
        .route("/webhooks/optimizely", post(handle_optimizely_webhook))
        .route(
            "/webhooks/gofeatureflag",
            post(handle_gofeatureflag_webhook),
        )
        .route("/webhooks/flipt", post(handle_flipt_webhook))
        .route("/webhooks/growthbook", post(handle_growthbook_webhook))
}

/// Ingest a change event (feature flag change, deployment, etc.)
/// POST /api/v1/events
async fn ingest_event(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received event: {:?}", payload.get("event_type"));

    // Parse event type
    let event_type = payload
        .get("event_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing event_type in payload");
            AppError::Validation("Missing required field: event_type".to_string())
        })?;

    match event_type {
        "feature_flag_change" => {
            let flag_event: FeatureFlagChangeEvent =
                serde_json::from_value(payload).map_err(|e| {
                    error!("[EVENTS] Failed to parse feature flag change event: {}", e);
                    AppError::Validation(format!("Invalid feature_flag_change event: {}", e))
                })?;

            // Use trait implementation from events_storage
            let change_id = (*state)
                .store_flag_change(flag_event, project_id)
                .await
                .map_err(|e| {
                    AppError::Internal(anyhow::anyhow!("Failed to store flag change: {}", e))
                })?;

            Ok(Json(EventResponse {
                id: change_id.to_string(),
                message: "Feature flag change event stored".to_string(),
            }))
        }
        _ => {
            error!("[EVENTS] Unknown event type: {}", event_type);
            Err(AppError::Validation(format!(
                "Unknown event type: {}",
                event_type
            )))
        }
    }
}

/// Auto-detect which services use a feature flag by querying spans/metrics
/// Looks for spans with operation="experiments.IsEnabled" and experiment.id tag
async fn detect_services_using_flag(
    state: &Arc<WatchState>,
    project_id: &Uuid,
    flag_id: &str,
) -> Result<Vec<String>> {
    use chrono::Duration;
    use chrono::Utc;

    // Look back 7 days for flag evaluations
    let lookback_start = (Utc::now() - Duration::days(7)).to_rfc3339();

    // Query ClickHouse for unique service names that have evaluated this flag
    // Look for spans with tags containing experiment.id=<flag_id>
    // Note: tags is stored as JSON string, need to extract with JSONExtractString
    let query = format!(
        r#"
        SELECT DISTINCT service_name
        FROM reiver.spans
        WHERE project_id = '{}'
          AND timestamp >= parseDateTime64BestEffort('{}', 3)
          AND span_name = 'experiments.IsEnabled'
          AND span_attributes['experiment.id'] = '{}'
        LIMIT 100
        "#,
        project_id, lookback_start, flag_id
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ServiceRow {
        service_name: String,
    }

    let services: Vec<ServiceRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| {
            error!("[EVENTS] Failed to query ClickHouse for flag usage: {}", e);
            anyhow::anyhow!("ClickHouse query failed: {}", e)
        })?;

    let service_names: Vec<String> = services.into_iter().map(|s| s.service_name).collect();

    Ok(service_names)
}

/// LaunchDarkly webhook handler
/// POST /api/v1/events/webhooks/launchdarkly
/// Accepts LaunchDarkly webhook format directly, converts to our format internally
async fn handle_launchdarkly_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received LaunchDarkly webhook");

    // Parse LaunchDarkly webhook format
    let flag_key = payload.get("key").and_then(|v| v.as_str()).ok_or_else(|| {
        error!("[EVENTS] Missing 'key' in LaunchDarkly webhook");
        AppError::Validation("Missing required field: key".to_string())
    })?;

    let flag_name = payload
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Extract environment changes from LaunchDarkly format
    // LaunchDarkly sends flag data with environments object
    let environments = payload
        .get("environments")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'environments' in LaunchDarkly webhook");
            AppError::Validation("Missing required field: environments".to_string())
        })?;

    // Process each environment change
    // LaunchDarkly webhooks can have multiple environments changed in one payload
    let mut change_ids = Vec::new();

    for (env_name, env_data) in environments {
        // Extract flag state for this environment
        let enabled = env_data
            .get("on")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let archived = env_data
            .get("archived")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Skip archived flags
        if archived {
            continue;
        }

        // Extract last modified timestamp
        let timestamp_ms = env_data
            .get("lastModified")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| {
                payload
                    .get("creationDate")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
            });

        let timestamp = chrono::DateTime::from_timestamp(
            timestamp_ms / 1000,
            ((timestamp_ms % 1000) * 1_000_000) as u32,
        )
        .unwrap_or_else(|| chrono::Utc::now());

        // Convert to our format
        let change_event = FeatureFlagChangeEvent {
            event_type: "feature_flag_change".to_string(),
            project_key: project_id.to_string(),
            flag_id: flag_key.to_string(),
            flag_name: flag_name.clone(),
            environment: Some(env_name.clone()),
            changed_by: None, // LaunchDarkly webhook doesn't include user info
            change_type: if enabled {
                "toggle_on".to_string()
            } else {
                "toggle_off".to_string()
            },
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
        let change_id =
            handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;
        change_ids.push(change_id);
    }

    info!(
        "[EVENTS] Processed LaunchDarkly webhook: flag={}, changes={}",
        flag_key,
        change_ids.len()
    );

    Ok(Json(EventResponse {
        id: change_ids
            .first()
            .copied()
            .unwrap_or_else(|| Uuid::new_v4())
            .to_string(),
        message: format!("Processed {} environment changes", change_ids.len()),
    }))
}

/// Unleash webhook handler
/// POST /api/v1/events/webhooks/unleash
/// Accepts Unleash webhook format directly, converts to our format internally
///
/// Unleash webhook format (typical):
/// {
///   "type": "feature-toggles-updated",
///   "createdAt": "2024-01-01T00:00:00Z",
///   "createdBy": { "id": 1, "username": "user", "email": "user@example.com" },
///   "data": {
///     "featureName": "my-feature",
///     "project": "default",
///     "environments": [{ "name": "production", "enabled": true }]
///   }
/// }
async fn handle_unleash_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received Unleash webhook");

    // Parse Unleash webhook format
    let event_type = payload
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("feature-toggles-updated");

    // Extract data object
    let data = payload
        .get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'data' in Unleash webhook");
            AppError::Validation("Missing required field: data".to_string())
        })?;

    let flag_name = data
        .get("featureName")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let flag_id = flag_name.clone().unwrap_or_else(|| "unknown".to_string());

    // Extract environments array
    let environments = data
        .get("environments")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'environments' in Unleash webhook data");
            AppError::Validation("Missing required field: data.environments".to_string())
        })?;

    // Extract createdBy user info
    let created_by = payload.get("createdBy").and_then(|cb| {
        let cb_obj = cb.as_object()?;
        Some(ChangedBy {
            type_: Some("user".to_string()),
            email: cb_obj
                .get("email")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            name: cb_obj
                .get("username")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            id: cb_obj
                .get("id")
                .and_then(|v| v.as_u64())
                .map(|id| id.to_string()),
        })
    });

    // Extract timestamp (default to now if not provided or unparseable)
    let timestamp = payload
        .get("createdAt")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    // Process each environment change
    let mut change_ids = Vec::new();

    for env_data in environments {
        let env_obj = env_data.as_object().ok_or_else(|| {
            error!("[EVENTS] Invalid environment object in Unleash webhook");
            AppError::Validation("Invalid environment object in data.environments".to_string())
        })?;

        let env_name = env_obj
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                error!("[EVENTS] Missing 'name' in environment object");
                AppError::Validation("Missing 'name' in environment object".to_string())
            })?;

        let enabled = env_obj
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Determine change type based on event_type and enabled state
        let change_type = match event_type {
            "feature-toggles-archived" => "delete".to_string(),
            "feature-toggles-created" => "create".to_string(),
            "feature-toggles-updated" => {
                if enabled {
                    "toggle_on".to_string()
                } else {
                    "toggle_off".to_string()
                }
            }
            _ => "toggle".to_string(),
        };

        // Convert to our format
        let change_event = FeatureFlagChangeEvent {
            event_type: "feature_flag_change".to_string(),
            project_key: project_id.to_string(),
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
        let change_id =
            handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;
        change_ids.push(change_id);
    }

    info!(
        "[EVENTS] Processed Unleash webhook: flag={}, changes={}",
        flag_id,
        change_ids.len()
    );

    Ok(Json(EventResponse {
        id: change_ids
            .first()
            .copied()
            .unwrap_or_else(|| Uuid::new_v4())
            .to_string(),
        message: format!("Processed {} environment changes", change_ids.len()),
    }))
}

/// Flagsmith webhook handler
/// POST /api/v1/events/webhooks/flagsmith
/// Accepts Flagsmith webhook format directly, converts to our format internally
///
/// Flagsmith webhook format (typical):
/// {
///   "event_type": "FLAG_UPDATED",
///   "data": {
///     "feature": { "id": 123, "name": "my-feature", "feature_key": "my_feature" },
///     "environment": { "id": 456, "name": "production" },
///     "enabled": true,
///     "value": "true",
///     "previous_enabled": false,
///     "previous_value": "false",
///     "changed_by": { "email": "user@example.com", "first_name": "User" }
///   },
///   "created_date": "2024-01-01T00:00:00Z"
/// }
async fn handle_flagsmith_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received Flagsmith webhook");

    // Parse Flagsmith webhook format
    let event_type = payload
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("FLAG_UPDATED");

    // Extract data object
    let data = payload
        .get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'data' in Flagsmith webhook");
            AppError::Validation("Missing required field: data".to_string())
        })?;

    // Extract feature info
    let feature = data
        .get("feature")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'feature' in Flagsmith webhook data");
            AppError::Validation("Missing required field: data.feature".to_string())
        })?;

    let flag_key = feature
        .get("feature_key")
        .or_else(|| feature.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'feature_key' or 'name' in Flagsmith feature object");
            AppError::Validation(
                "Missing required field: data.feature.feature_key or data.feature.name".to_string(),
            )
        })?;

    let flag_id = flag_key.to_string();
    let flag_name = feature
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Extract environment info
    let environment = data
        .get("environment")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'environment' in Flagsmith webhook data");
            AppError::Validation("Missing required field: data.environment".to_string())
        })?;

    let env_name = environment
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'name' in Flagsmith environment object");
            AppError::Validation("Missing required field: data.environment.name".to_string())
        })?;

    // Extract enabled state
    let enabled = data
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Extract previous state (if available)
    let prev_enabled = data.get("previous_enabled").and_then(|v| v.as_bool());
    let prev_value = data.get("previous_value").and_then(|v| v.as_str());

    // Extract changed_by user info
    let changed_by = data.get("changed_by").and_then(|cb| {
        let cb_obj = cb.as_object()?;
        Some(ChangedBy {
            type_: Some("user".to_string()),
            email: cb_obj
                .get("email")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            name: cb_obj.get("first_name").and_then(|v| v.as_str()).map(|n| {
                let last_name = cb_obj
                    .get("last_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if last_name.is_empty() {
                    n.to_string()
                } else {
                    format!("{} {}", n, last_name)
                }
            }),
            id: cb_obj
                .get("id")
                .and_then(|v| v.as_i64())
                .map(|id| id.to_string()),
        })
    });

    // Extract timestamp
    let timestamp = payload
        .get("created_date")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| Some(chrono::Utc::now()))
        .unwrap();

    // Determine change type based on event_type
    let change_type = match event_type {
        "FLAG_DELETED" => "delete".to_string(),
        "FLAG_CREATED" => "create".to_string(),
        "FLAG_UPDATED" => {
            if let (Some(prev), Some(curr)) = (prev_enabled, Some(enabled)) {
                if prev != curr {
                    if enabled {
                        "toggle_on".to_string()
                    } else {
                        "toggle_off".to_string()
                    }
                } else {
                    "variant_change".to_string()
                }
            } else {
                if enabled {
                    "toggle_on".to_string()
                } else {
                    "toggle_off".to_string()
                }
            }
        }
        _ => "toggle".to_string(),
    };

    // Build prev_value JSON if available
    let prev_value_json = if let Some(prev) = prev_enabled {
        Some(serde_json::json!({
            "enabled": prev,
            "value": prev_value.unwrap_or(""),
        }))
    } else {
        None
    };

    // Convert to our format
    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key: project_id.to_string(),
        flag_id: flag_id.clone(),
        flag_name,
        environment: Some(env_name.to_string()),
        changed_by,
        change_type,
        prev_value: prev_value_json,
        new_value: serde_json::json!({
            "enabled": enabled,
            "value": data.get("value").and_then(|v| v.as_str()).unwrap_or(""),
        }),
        impacted_services: None, // Will be auto-detected
        metadata: Some(serde_json::json!({
            "source": "flagsmith",
            "flagsmith_event_type": event_type,
            "flagsmith_webhook": payload.clone(),
        })),
        timestamp: Some(timestamp),
    };

    // Store the change
    let change_id =
        handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;

    info!(
        "[EVENTS] Processed Flagsmith webhook: flag={}, change_id={}",
        flag_id, change_id
    );

    Ok(Json(EventResponse {
        id: change_id.to_string(),
        message: "Processed Flagsmith feature flag change".to_string(),
    }))
}

/// Internal handler for feature flag changes (used by both universal API and LaunchDarkly webhook)

/// ConfigCat webhook handler
/// POST /api/v1/events/webhooks/configcat
async fn handle_configcat_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received ConfigCat webhook");

    let feature_flag = payload
        .get("featureFlag")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'featureFlag' in ConfigCat webhook");
            AppError::Validation("Missing required field: featureFlag".to_string())
        })?;

    let flag_id = feature_flag
        .get("key")
        .or_else(|| feature_flag.get("id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'key' or 'id' in ConfigCat featureFlag object");
            AppError::Validation(
                "Missing required field: featureFlag.key or featureFlag.id".to_string(),
            )
        })?
        .to_string();

    let flag_name = feature_flag
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let environment = payload.get("environment").and_then(|v| v.as_object());
    let env_name = environment
        .and_then(|e| e.get("name"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let setting = payload.get("setting").and_then(|v| v.as_object());
    let enabled = setting
        .and_then(|s| s.get("value"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let prev_enabled = setting
        .and_then(|s| s.get("previousValue"))
        .and_then(|v| v.as_bool());

    let changed_by = payload.get("user").and_then(|u| {
        let u_obj = u.as_object()?;
        Some(ChangedBy {
            type_: Some("user".to_string()),
            email: u_obj
                .get("email")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            name: u_obj
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            id: u_obj
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        })
    });

    let event_type = payload
        .get("event")
        .and_then(|v| v.as_str())
        .unwrap_or("FEATURE_FLAG_UPDATED");
    let timestamp = payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| Some(chrono::Utc::now()))
        .unwrap();

    let change_type = match event_type {
        "FEATURE_FLAG_DELETED" => "delete".to_string(),
        "FEATURE_FLAG_CREATED" => "create".to_string(),
        "FEATURE_FLAG_UPDATED" => {
            if let (Some(prev), Some(curr)) = (prev_enabled, Some(enabled)) {
                if prev != curr {
                    if enabled {
                        "toggle_on".to_string()
                    } else {
                        "toggle_off".to_string()
                    }
                } else {
                    "variant_change".to_string()
                }
            } else {
                if enabled {
                    "toggle_on".to_string()
                } else {
                    "toggle_off".to_string()
                }
            }
        }
        _ => "toggle".to_string(),
    };

    let prev_value_json = prev_enabled.map(|prev| serde_json::json!({"enabled": prev}));

    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key: project_id.to_string(),
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

    let change_id =
        handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;

    info!(
        "[EVENTS] Processed ConfigCat webhook: flag={}, change_id={}",
        flag_id, change_id
    );

    Ok(Json(EventResponse {
        id: change_id.to_string(),
        message: "Processed ConfigCat feature flag change".to_string(),
    }))
}

/// Split.io (Harness) webhook handler
/// POST /api/v1/events/webhooks/split
async fn handle_split_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received Split.io webhook");

    let event_type = payload
        .get("eventType")
        .and_then(|v| v.as_str())
        .unwrap_or("SPLIT_UPDATE");
    let data = payload
        .get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'data' in Split.io webhook");
            AppError::Validation("Missing required field: data".to_string())
        })?;

    let flag_id = data
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'name' in Split.io webhook data");
            AppError::Validation("Missing required field: data.name".to_string())
        })?
        .to_string();

    let env_name = payload
        .get("environment")
        .or_else(|| {
            data.get("environment")
                .and_then(|e| e.as_object())
                .and_then(|e| e.get("name"))
        })
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let definition = data.get("definition").and_then(|v| v.as_object());
    let enabled = data
        .get("killed")
        .and_then(|v| v.as_bool())
        .map(|killed| !killed)
        .or_else(|| {
            definition
                .and_then(|d| d.get("on"))
                .and_then(|v| v.as_bool())
        })
        .unwrap_or(false);

    let timestamp_ms = payload
        .get("timestamp")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    let timestamp = chrono::DateTime::from_timestamp(
        timestamp_ms / 1000,
        ((timestamp_ms % 1000) * 1_000_000) as u32,
    )
    .unwrap_or_else(|| chrono::Utc::now());

    let change_type = match event_type {
        "SPLIT_DELETED" | "SPLIT_ARCHIVED" => "delete".to_string(),
        "SPLIT_CREATED" => "create".to_string(),
        "SPLIT_UPDATE" => {
            if enabled {
                "toggle_on".to_string()
            } else {
                "toggle_off".to_string()
            }
        }
        _ => "toggle".to_string(),
    };

    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key: project_id.to_string(),
        flag_id: flag_id.clone(),
        flag_name: None,
        environment: env_name,
        changed_by: None,
        change_type,
        prev_value: None,
        new_value: serde_json::json!({"enabled": enabled, "killed": data.get("killed").and_then(|v| v.as_bool()).unwrap_or(false)}),
        impacted_services: None,
        metadata: Some(
            serde_json::json!({"source": "split", "split_event_type": event_type, "split_webhook": payload.clone()}),
        ),
        timestamp: Some(timestamp),
    };

    let change_id =
        handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;
    info!(
        "[EVENTS] Processed Split.io webhook: flag={}, change_id={}",
        flag_id, change_id
    );
    Ok(Json(EventResponse {
        id: change_id.to_string(),
        message: "Processed Split.io feature flag change".to_string(),
    }))
}

/// CloudBees Feature Flags webhook handler
/// POST /api/v1/events/webhooks/cloudbees
async fn handle_cloudbees_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received CloudBees webhook");

    let event_type = payload
        .get("eventType")
        .and_then(|v| v.as_str())
        .unwrap_or("FLAG_UPDATED");
    let flag_obj = payload
        .get("flag")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'flag' in CloudBees webhook");
            AppError::Validation("Missing required field: flag".to_string())
        })?;

    let flag_id = flag_obj
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'key' in CloudBees flag object");
            AppError::Validation("Missing required field: flag.key".to_string())
        })?
        .to_string();

    let flag_name = flag_obj
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let environments = flag_obj.get("environments").and_then(|v| v.as_object());
    let changed_by = payload.get("user").and_then(|u| {
        let u_obj = u.as_object()?;
        Some(ChangedBy {
            type_: Some("user".to_string()),
            email: u_obj
                .get("email")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            name: u_obj
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            id: None,
        })
    });

    let timestamp = payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

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
            let enabled = env_obj
                .get("enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let change_event = FeatureFlagChangeEvent {
                event_type: "feature_flag_change".to_string(),
                project_key: project_id.to_string(),
                flag_id: flag_id.clone(),
                flag_name: flag_name.clone(),
                environment: Some(env_name.clone()),
                changed_by: changed_by.clone(),
                change_type: change_type.clone(),
                prev_value: None,
                new_value: serde_json::json!({"enabled": enabled}),
                impacted_services: None,
                metadata: Some(
                    serde_json::json!({"source": "cloudbees", "cloudbees_event_type": event_type, "cloudbees_webhook": payload.clone()}),
                ),
                timestamp: Some(timestamp),
            };
            let change_id =
                handle_feature_flag_change_internal(state.clone(), change_event, project_id)
                    .await?;
            change_ids.push(change_id);
        }
    } else {
        let change_event = FeatureFlagChangeEvent {
            event_type: "feature_flag_change".to_string(),
            project_key: project_id.to_string(),
            flag_id: flag_id.clone(),
            flag_name,
            environment: None,
            changed_by,
            change_type,
            prev_value: None,
            new_value: serde_json::json!({}),
            impacted_services: None,
            metadata: Some(
                serde_json::json!({"source": "cloudbees", "cloudbees_event_type": event_type, "cloudbees_webhook": payload.clone()}),
            ),
            timestamp: Some(timestamp),
        };
        let change_id =
            handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;
        change_ids.push(change_id);
    }

    info!(
        "[EVENTS] Processed CloudBees webhook: flag={}, changes={}",
        flag_id,
        change_ids.len()
    );
    Ok(Json(EventResponse {
        id: change_ids
            .first()
            .copied()
            .unwrap_or_else(|| Uuid::new_v4())
            .to_string(),
        message: format!("Processed {} environment changes", change_ids.len()),
    }))
}

/// Optimizely webhook handler
/// POST /api/v1/events/webhooks/optimizely
async fn handle_optimizely_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received Optimizely webhook");

    let event_type = payload
        .get("event")
        .and_then(|v| v.as_str())
        .unwrap_or("flag.updated");
    let data = payload
        .get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'data' in Optimizely webhook");
            AppError::Validation("Missing required field: data".to_string())
        })?;

    let flag_id = data
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'key' in Optimizely webhook data");
            AppError::Validation("Missing required field: data.key".to_string())
        })?
        .to_string();

    let flag_name = data
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let env_name = data
        .get("environment_key")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let enabled = data
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let changed_by = payload.get("user").and_then(|u| {
        let u_obj = u.as_object()?;
        Some(ChangedBy {
            type_: Some("user".to_string()),
            email: u_obj
                .get("email")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            name: None,
            id: u_obj
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        })
    });

    let timestamp = payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let change_type = match event_type {
        "flag.deleted" => "delete".to_string(),
        "flag.created" => "create".to_string(),
        "flag.enabled" => "toggle_on".to_string(),
        "flag.disabled" => "toggle_off".to_string(),
        "flag.updated" => {
            if enabled {
                "toggle_on".to_string()
            } else {
                "toggle_off".to_string()
            }
        }
        _ => "toggle".to_string(),
    };

    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key: project_id.to_string(),
        flag_id: flag_id.clone(),
        flag_name,
        environment: env_name,
        changed_by,
        change_type,
        prev_value: None,
        new_value: serde_json::json!({"enabled": enabled}),
        impacted_services: None,
        metadata: Some(
            serde_json::json!({"source": "optimizely", "optimizely_event": event_type, "optimizely_webhook": payload.clone()}),
        ),
        timestamp: Some(timestamp),
    };

    let change_id =
        handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;
    info!(
        "[EVENTS] Processed Optimizely webhook: flag={}, change_id={}",
        flag_id, change_id
    );
    Ok(Json(EventResponse {
        id: change_id.to_string(),
        message: "Processed Optimizely feature flag change".to_string(),
    }))
}

/// GO Feature Flag webhook handler
/// POST /api/v1/events/webhooks/gofeatureflag
async fn handle_gofeatureflag_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received GO Feature Flag webhook");

    let event_type = payload
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("flag.updated");
    let flag_obj = payload
        .get("flag")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'flag' in GO Feature Flag webhook");
            AppError::Validation("Missing required field: flag".to_string())
        })?;

    let flag_id = flag_obj
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'key' in GO Feature Flag flag object");
            AppError::Validation("Missing required field: flag.key".to_string())
        })?
        .to_string();

    let env_name = payload
        .get("environment")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let enabled = flag_obj
        .get("defaultValue")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let timestamp = payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let change_type = match event_type {
        "flag.deleted" => "delete".to_string(),
        "flag.created" => "create".to_string(),
        "flag.updated" => {
            if enabled {
                "toggle_on".to_string()
            } else {
                "toggle_off".to_string()
            }
        }
        _ => "toggle".to_string(),
    };

    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key: project_id.to_string(),
        flag_id: flag_id.clone(),
        flag_name: None,
        environment: env_name,
        changed_by: None,
        change_type,
        prev_value: None,
        new_value: serde_json::json!({"enabled": enabled}),
        impacted_services: None,
        metadata: Some(
            serde_json::json!({"source": "gofeatureflag", "gofeatureflag_event_type": event_type, "gofeatureflag_webhook": payload.clone()}),
        ),
        timestamp: Some(timestamp),
    };

    let change_id =
        handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;
    info!(
        "[EVENTS] Processed GO Feature Flag webhook: flag={}, change_id={}",
        flag_id, change_id
    );
    Ok(Json(EventResponse {
        id: change_id.to_string(),
        message: "Processed GO Feature Flag feature flag change".to_string(),
    }))
}

/// Flipt webhook handler
/// POST /api/v1/events/webhooks/flipt
async fn handle_flipt_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received Flipt webhook");

    let event_type = payload
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("flag.updated");
    let flag_obj = payload
        .get("flag")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'flag' in Flipt webhook");
            AppError::Validation("Missing required field: flag".to_string())
        })?;

    let flag_id = flag_obj
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'key' in Flipt flag object");
            AppError::Validation("Missing required field: flag.key".to_string())
        })?
        .to_string();

    let flag_name = flag_obj
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let enabled = flag_obj
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let namespace = payload
        .get("namespace")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let timestamp = payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let change_type = match event_type {
        "flag.deleted" => "delete".to_string(),
        "flag.created" => "create".to_string(),
        "flag.updated" => {
            if enabled {
                "toggle_on".to_string()
            } else {
                "toggle_off".to_string()
            }
        }
        _ => "toggle".to_string(),
    };

    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key: project_id.to_string(),
        flag_id: flag_id.clone(),
        flag_name,
        environment: namespace,
        changed_by: None,
        change_type,
        prev_value: None,
        new_value: serde_json::json!({"enabled": enabled}),
        impacted_services: None,
        metadata: Some(
            serde_json::json!({"source": "flipt", "flipt_event_type": event_type, "flipt_webhook": payload.clone()}),
        ),
        timestamp: Some(timestamp),
    };

    let change_id =
        handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;
    info!(
        "[EVENTS] Processed Flipt webhook: flag={}, change_id={}",
        flag_id, change_id
    );
    Ok(Json(EventResponse {
        id: change_id.to_string(),
        message: "Processed Flipt feature flag change".to_string(),
    }))
}

/// GrowthBook webhook handler
/// POST /api/v1/events/webhooks/growthbook
async fn handle_growthbook_webhook(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<EventResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("[EVENTS] Received GrowthBook webhook");

    let event_type = payload
        .get("event")
        .and_then(|v| v.as_str())
        .unwrap_or("feature.updated");
    let data = payload
        .get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'data' in GrowthBook webhook");
            AppError::Validation("Missing required field: data".to_string())
        })?;

    let flag_id = data
        .get("id")
        .or_else(|| data.get("key"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("[EVENTS] Missing 'id' or 'key' in GrowthBook webhook data");
            AppError::Validation("Missing required field: data.id or data.key".to_string())
        })?
        .to_string();

    let flag_name = data
        .get("description")
        .or_else(|| data.get("name"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let env_name = payload
        .get("environment")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let enabled = data
        .get("defaultValue")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let timestamp = payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let change_type = match event_type {
        "feature.deleted" => "delete".to_string(),
        "feature.created" => "create".to_string(),
        "feature.updated" => {
            if enabled {
                "toggle_on".to_string()
            } else {
                "toggle_off".to_string()
            }
        }
        _ => "toggle".to_string(),
    };

    let change_event = FeatureFlagChangeEvent {
        event_type: "feature_flag_change".to_string(),
        project_key: project_id.to_string(),
        flag_id: flag_id.clone(),
        flag_name,
        environment: env_name,
        changed_by: None,
        change_type,
        prev_value: None,
        new_value: serde_json::json!({"enabled": enabled}),
        impacted_services: None,
        metadata: Some(
            serde_json::json!({"source": "growthbook", "growthbook_event": event_type, "growthbook_webhook": payload.clone()}),
        ),
        timestamp: Some(timestamp),
    };

    let change_id =
        handle_feature_flag_change_internal(state.clone(), change_event, project_id).await?;
    info!(
        "[EVENTS] Processed GrowthBook webhook: flag={}, change_id={}",
        flag_id, change_id
    );
    Ok(Json(EventResponse {
        id: change_id.to_string(),
        message: "Processed GrowthBook feature flag change".to_string(),
    }))
}

async fn handle_feature_flag_change_internal(
    state: Arc<WatchState>,
    event: FeatureFlagChangeEvent,
    project_id: Uuid,
) -> Result<Uuid> {
    // Auto-detect impacted services if not provided
    let impacted_services = if let Some(services) = event.impacted_services {
        services
    } else {
        // Query ClickHouse for spans with experiment.id tag matching this flag_id
        // Look for spans with operation="experiments.IsEnabled" and tags.experiment.id=<flag_id>
        detect_services_using_flag(&state, &project_id, &event.flag_id)
            .await
            .unwrap_or_else(|e| {
                error!(
                    "[EVENTS] Failed to auto-detect services for flag {}: {}",
                    event.flag_id, e
                );
                vec![] // Continue with empty list if detection fails
            })
    };

    // Serialize changed_by
    let changed_by_json = event
        .changed_by
        .as_ref()
        .map(|cb| {
            serde_json::json!({
                "type": cb.type_,
                "email": cb.email,
                "name": cb.name,
                "id": cb.id,
            })
        })
        .unwrap_or(serde_json::Value::Null);

    // Insert into feature_flag_changes table
    let change_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO feature_flag_changes (
            project_id, flag_id, flag_name, environment, change_type,
            prev_value, new_value, changed_by, impacted_services, metadata, timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING id",
    )
    .bind(project_id)
    .bind(&event.flag_id)
    .bind(event.flag_name.as_deref())
    .bind(event.environment.as_deref())
    .bind(&event.change_type)
    .bind(event.prev_value.as_ref().map(|v| v.clone())) // JSONB directly
    .bind(event.new_value.clone()) // JSONB directly
    .bind(if changed_by_json.is_null() {
        None
    } else {
        Some(changed_by_json)
    })
    .bind(&impacted_services)
    .bind(event.metadata.as_ref().map(|v| v.clone()))
    .bind(event.timestamp.unwrap_or_else(chrono::Utc::now))
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("[EVENTS] Failed to insert feature flag change: {}", e);
        AppError::Internal(anyhow::anyhow!(
            "Failed to store feature flag change: {}",
            e
        ))
    })?;

    info!(
        "[EVENTS] Stored feature flag change: id={}, flag_id={}, services={:?}",
        change_id, event.flag_id, impacted_services
    );

    // Emit platform event for the subscription system
    if let Err(e) = state
        .event_publisher
        .emit(
            reiver_core::events::PlatformEventType::FeatureFlagChanged,
            project_id,
            format!("feature_flag:{}:{}", event.flag_id, change_id),
            serde_json::json!({
                "change_id": change_id,
                "flag_id": event.flag_id,
                "flag_name": event.flag_name,
                "change_type": event.change_type,
                "environment": event.environment,
                "impacted_services": impacted_services,
            }),
        )
        .await
    {
        tracing::warn!("Failed to emit FeatureFlagChanged event: {}", e);
    }

    Ok(change_id)
}
