//! Common types and traits for webhook handlers

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// Response returned by webhook handlers
#[derive(Debug, Serialize)]
pub struct EventResponse {
    pub id: Uuid,
    pub message: String,
}

/// Feature flag change event (internal format)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeatureFlagChangeEvent {
    pub event_type: String, // "feature_flag_change"
    pub project_key: String,
    pub flag_id: String,
    pub flag_name: Option<String>,
    pub environment: Option<String>,
    pub changed_by: Option<ChangedBy>,
    pub change_type: String, // "toggle", "rollout", "variant_change", "delete", "create"
    pub prev_value: Option<serde_json::Value>,
    pub new_value: serde_json::Value,
    pub impacted_services: Option<Vec<String>>, // Optional: auto-detected if not provided
    pub metadata: Option<serde_json::Value>,
    pub timestamp: Option<DateTime<Utc>>,
}

/// Information about who changed a feature flag
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChangedBy {
    #[serde(rename = "type")]
    pub type_: String, // "user", "system", "api"
    pub email: Option<String>,
    pub name: Option<String>,
    pub id: Option<String>,
}

/// Trait for storing feature flag change events
/// Implemented by the server to provide database access
#[async_trait::async_trait]
pub trait FeatureFlagEventStorage: Send + Sync {
    /// Store a feature flag change event and return its ID
    async fn store_flag_change(&self, event: FeatureFlagChangeEvent) -> Result<Uuid, String>;
}

