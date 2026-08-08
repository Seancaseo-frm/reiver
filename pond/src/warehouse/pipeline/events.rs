use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db::DbPool;
use crate::kafka::{KafkaProducer, PipelineEventKafkaMessage};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Cron,
    Manual,
    DataInsert,
    DataChange,
    PipelineCompleted,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cron => "cron",
            Self::Manual => "manual",
            Self::DataInsert => "data.insert",
            Self::DataChange => "data.change",
            Self::PipelineCompleted => "pipeline.completed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "cron" => Some(Self::Cron),
            "manual" => Some(Self::Manual),
            "data.insert" => Some(Self::DataInsert),
            "data.change" => Some(Self::DataChange),
            "pipeline.completed" => Some(Self::PipelineCompleted),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    Pending,
    Dispatched,
    Completed,
    Failed,
}

impl EventStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Dispatched => "dispatched",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineEvent {
    pub id: Uuid,
    pub project_id: Uuid,
    pub event_type: String,
    pub source: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineSubscription {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub event_type: String,
    pub event_filter: serde_json::Value,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

pub struct EventStore {
    db: Arc<DbPool>,
    kafka: Arc<KafkaProducer>,
}

impl EventStore {
    pub fn new(db: Arc<DbPool>, kafka: Arc<KafkaProducer>) -> Self {
        Self { db, kafka }
    }

    /// Insert event into Postgres as `dispatched` (for history) and produce to Redpanda.
    pub async fn emit(
        &self,
        project_id: Uuid,
        event_type: EventType,
        source: &str,
        payload: serde_json::Value,
    ) -> Result<Uuid> {
        let id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO warehouse_pipeline_events (project_id, event_type, source, payload, status, dispatched_at)
            VALUES ($1, $2, $3, $4, 'dispatched', NOW())
            RETURNING id
            "#,
        )
        .bind(project_id)
        .bind(event_type.as_str())
        .bind(source)
        .bind(&payload)
        .fetch_one(self.db.as_ref())
        .await
        .context("failed to insert pipeline event")?;

        let kafka_msg = PipelineEventKafkaMessage {
            event_id: id,
            project_id,
            event_type: event_type.as_str().to_string(),
            source: source.to_string(),
            payload: payload.clone(),
        };

        self.kafka.send_pipeline_event(&kafka_msg).await
            .context("failed to produce pipeline event to Redpanda")?;

        Ok(id)
    }

    pub async fn complete(&self, event_id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE warehouse_pipeline_events SET status = 'completed', completed_at = NOW() WHERE id = $1",
        )
        .bind(event_id)
        .execute(self.db.as_ref())
        .await
        .context("failed to complete event")?;
        Ok(())
    }

    pub async fn fail(&self, event_id: Uuid, _error: &str) -> Result<()> {
        sqlx::query(
            "UPDATE warehouse_pipeline_events SET status = 'failed', completed_at = NOW() WHERE id = $1",
        )
        .bind(event_id)
        .execute(self.db.as_ref())
        .await
        .context("failed to mark event as failed")?;
        Ok(())
    }

    pub async fn list_events(
        &self,
        project_id: Uuid,
        limit: i64,
    ) -> Result<Vec<PipelineEvent>> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, String, String, serde_json::Value, String, DateTime<Utc>, Option<DateTime<Utc>>, Option<DateTime<Utc>>)>(
            r#"
            SELECT id, project_id, event_type, source, payload, status, created_at, dispatched_at, completed_at
            FROM warehouse_pipeline_events
            WHERE project_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.db.as_ref())
        .await
        .context("failed to list events")?;

        Ok(rows
            .into_iter()
            .map(|(id, project_id, event_type, source, payload, status, created_at, dispatched_at, completed_at)| {
                PipelineEvent {
                    id,
                    project_id,
                    event_type,
                    source,
                    payload,
                    status,
                    created_at,
                    dispatched_at,
                    completed_at,
                }
            })
            .collect())
    }

    pub async fn get_subscriptions_for_event(
        &self,
        event_type: &str,
    ) -> Result<Vec<PipelineSubscription>> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, String, serde_json::Value, bool, DateTime<Utc>)>(
            r#"
            SELECT id, pipeline_id, event_type, event_filter, enabled, created_at
            FROM warehouse_pipeline_subscriptions
            WHERE event_type = $1 AND enabled = true
            "#,
        )
        .bind(event_type)
        .fetch_all(self.db.as_ref())
        .await
        .context("failed to get subscriptions")?;

        Ok(rows
            .into_iter()
            .map(|(id, pipeline_id, event_type, event_filter, enabled, created_at)| {
                PipelineSubscription {
                    id,
                    pipeline_id,
                    event_type,
                    event_filter,
                    enabled,
                    created_at,
                }
            })
            .collect())
    }

    pub async fn create_subscription(
        &self,
        pipeline_id: Uuid,
        event_type: &str,
        event_filter: serde_json::Value,
    ) -> Result<Uuid> {
        let id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO warehouse_pipeline_subscriptions (pipeline_id, event_type, event_filter)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(pipeline_id)
        .bind(event_type)
        .bind(&event_filter)
        .fetch_one(self.db.as_ref())
        .await
        .context("failed to create subscription")?;

        Ok(id)
    }

    pub async fn delete_subscription(&self, subscription_id: Uuid) -> Result<bool> {
        let result = sqlx::query(
            "DELETE FROM warehouse_pipeline_subscriptions WHERE id = $1",
        )
        .bind(subscription_id)
        .execute(self.db.as_ref())
        .await
        .context("failed to delete subscription")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn list_subscriptions(
        &self,
        pipeline_id: Uuid,
    ) -> Result<Vec<PipelineSubscription>> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, String, serde_json::Value, bool, DateTime<Utc>)>(
            r#"
            SELECT id, pipeline_id, event_type, event_filter, enabled, created_at
            FROM warehouse_pipeline_subscriptions
            WHERE pipeline_id = $1
            ORDER BY created_at
            "#,
        )
        .bind(pipeline_id)
        .fetch_all(self.db.as_ref())
        .await
        .context("failed to list subscriptions")?;

        Ok(rows
            .into_iter()
            .map(|(id, pipeline_id, event_type, event_filter, enabled, created_at)| {
                PipelineSubscription {
                    id,
                    pipeline_id,
                    event_type,
                    event_filter,
                    enabled,
                    created_at,
                }
            })
            .collect())
    }

    /// Check if an event's payload matches a subscription's filter.
    /// An empty filter (`{}`) matches everything.
    pub fn matches_filter(event_payload: &serde_json::Value, filter: &serde_json::Value) -> bool {
        let filter_obj = match filter.as_object() {
            Some(obj) if !obj.is_empty() => obj,
            _ => return true,
        };

        let payload_obj = match event_payload.as_object() {
            Some(obj) => obj,
            None => return false,
        };

        for (key, filter_val) in filter_obj {
            match payload_obj.get(key) {
                Some(payload_val) if payload_val == filter_val => continue,
                _ => return false,
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_type_round_trip() {
        for et in [
            EventType::Cron,
            EventType::Manual,
            EventType::DataInsert,
            EventType::DataChange,
            EventType::PipelineCompleted,
        ] {
            assert_eq!(EventType::from_str(et.as_str()), Some(et));
        }
    }

    #[test]
    fn matches_filter_empty_matches_all() {
        let payload = serde_json::json!({"table": "orders", "count": 42});
        let filter = serde_json::json!({});
        assert!(EventStore::matches_filter(&payload, &filter));
    }

    #[test]
    fn matches_filter_exact_match() {
        let payload = serde_json::json!({"table": "orders", "count": 42});
        let filter = serde_json::json!({"table": "orders"});
        assert!(EventStore::matches_filter(&payload, &filter));
    }

    #[test]
    fn matches_filter_no_match() {
        let payload = serde_json::json!({"table": "users"});
        let filter = serde_json::json!({"table": "orders"});
        assert!(!EventStore::matches_filter(&payload, &filter));
    }

    #[test]
    fn matches_filter_missing_key() {
        let payload = serde_json::json!({"count": 42});
        let filter = serde_json::json!({"table": "orders"});
        assert!(!EventStore::matches_filter(&payload, &filter));
    }

    #[test]
    fn matches_filter_null_filter() {
        let payload = serde_json::json!({"table": "orders"});
        let filter = serde_json::Value::Null;
        assert!(EventStore::matches_filter(&payload, &filter));
    }
}
