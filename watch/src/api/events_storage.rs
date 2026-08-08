//! Implementation of feature flag event storage for WatchState
//!
//! This module provides storage functions for feature flag change events

use chrono::Utc;
use tracing::{error, info};
use uuid::Uuid;

use crate::api::events::FeatureFlagChangeEvent;
use crate::app_state::WatchState;
use crate::error::AppError;

impl WatchState {
    /// Store a feature flag change event
    pub async fn store_flag_change(
        &self,
        event: FeatureFlagChangeEvent,
        project_id: Uuid,
    ) -> Result<String, String> {
        // Auto-detect impacted services if not provided
        let impacted_services = if let Some(services) = &event.impacted_services {
            services.clone()
        } else {
            detect_services_using_flag(self, &project_id, &event.flag_id)
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
        .bind(event.prev_value.as_ref().cloned()) // JSONB directly
        .bind(event.new_value.clone()) // JSONB directly
        .bind(if changed_by_json.is_null() {
            None
        } else {
            Some(changed_by_json)
        })
        .bind(&impacted_services)
        .bind(event.metadata.as_ref().cloned())
        .bind(event.timestamp.unwrap_or_else(Utc::now))
        .fetch_one(&*self.db)
        .await
        .map_err(|e| {
            error!("[EVENTS] Failed to insert feature flag change: {}", e);
            format!("Failed to store feature flag change: {}", e)
        })?;

        info!(
            "[EVENTS] Stored feature flag change: id={}, flag_id={}, services={:?}",
            change_id, event.flag_id, impacted_services
        );

        // Emit platform event for the subscription system
        if let Err(e) = self
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

        Ok(change_id.to_string())
    }
}

/// Auto-detect which services use a feature flag by querying spans/metrics
/// Looks for spans with operation="experiments.IsEnabled" and experiment.id tag
async fn detect_services_using_flag(
    state: &WatchState,
    project_id: &Uuid,
    flag_id: &str,
) -> std::result::Result<Vec<String>, AppError> {
    use chrono::Duration;

    // Look back 7 days for flag evaluations
    let lookback_start = (Utc::now() - Duration::days(7)).to_rfc3339();

    // Query ClickHouse for unique service names that have evaluated this flag
    let query = format!(
        r#"
        SELECT DISTINCT service_name
        FROM reiver.spans
        WHERE project_id = toString('{}')
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
            AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e))
        })?;

    let service_names: Vec<String> = services.into_iter().map(|s| s.service_name).collect();

    Ok(service_names)
}
