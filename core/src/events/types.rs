//! Platform event types shared across all services.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Envelope for every platform event flowing through the event bus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformEvent {
    pub id: Uuid,
    pub event_type: PlatformEventType,
    pub project_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub source: EventSource,
    pub payload: serde_json::Value,
    /// Emitter-defined dedup key. The event worker uses this with Redis
    /// `SET NX EX` to suppress duplicate notifications within a cooldown
    /// window. The emitter sets it because it understands the dedup
    /// semantics for its event type (e.g. `"provider_key_error:openai:401"`).
    #[serde(default)]
    pub dedup_key: String,
}

/// Which service produced the event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    Watch,
    Flow,
    Pond,
    Website,
    External,
}

impl fmt::Display for EventSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Watch => write!(f, "watch"),
            Self::Flow => write!(f, "flow"),
            Self::Pond => write!(f, "pond"),
            Self::Website => write!(f, "website"),
            Self::External => write!(f, "external"),
        }
    }
}

/// All known platform event types.
///
/// `Custom(String)` is the extension point for user-defined event rules
/// added in a future phase.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlatformEventType {
    // Watch events
    AlertFired,
    AlertResolved,
    ExceptionGroupCreated,
    ExceptionGroupRegressed,
    FeatureFlagChanged,

    // Flow events
    LlmGuardrailTriggered,
    AgentInvestigationCompleted,
    ProviderKeyError,
    RolloutRolledBack,
    InvestigationCompleted,

    // Pond events
    SyncJobCompleted,
    SyncJobFailed,
    PipelineStepCompleted,

    // Scheduled / internal events
    ScheduledPricingSync,

    // Extension point
    Custom(String),
}

impl fmt::Display for PlatformEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlertFired => write!(f, "alert_fired"),
            Self::AlertResolved => write!(f, "alert_resolved"),
            Self::ExceptionGroupCreated => write!(f, "exception_group_created"),
            Self::ExceptionGroupRegressed => write!(f, "exception_group_regressed"),
            Self::FeatureFlagChanged => write!(f, "feature_flag_changed"),
            Self::LlmGuardrailTriggered => write!(f, "llm_guardrail_triggered"),
            Self::AgentInvestigationCompleted => write!(f, "agent_investigation_completed"),
            Self::ProviderKeyError => write!(f, "provider_key_error"),
            Self::RolloutRolledBack => write!(f, "rollout_rolled_back"),
            Self::InvestigationCompleted => write!(f, "investigation_completed"),
            Self::SyncJobCompleted => write!(f, "sync_job_completed"),
            Self::SyncJobFailed => write!(f, "sync_job_failed"),
            Self::PipelineStepCompleted => write!(f, "pipeline_step_completed"),
            Self::ScheduledPricingSync => write!(f, "scheduled_pricing_sync"),
            Self::Custom(name) => write!(f, "custom:{}", name),
        }
    }
}

/// Sanitized payload sent to external webhooks.
/// Whitelists only safe fields — never includes internal IDs, provider keys, etc.
#[derive(Debug, Clone, Serialize)]
pub struct WebhookEventPayload {
    pub event_id: Uuid,
    pub event_type: String,
    pub project_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub source: EventSource,
    pub payload: serde_json::Value,
}

impl From<&PlatformEvent> for WebhookEventPayload {
    fn from(event: &PlatformEvent) -> Self {
        Self {
            event_id: event.id,
            event_type: event.event_type.to_string(),
            project_id: event.project_id,
            timestamp: event.timestamp,
            source: event.source.clone(),
            payload: event.payload.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The event worker matches subscriptions by comparing
    /// `PlatformEventType::to_string()` against stored strings.
    /// The Kafka bus serializes events with serde. This test ensures
    /// that the serde round-trip preserves the Display value so
    /// subscription matching doesn't silently break.
    #[test]
    fn serde_round_trip_preserves_display_string() {
        let variants = vec![
            PlatformEventType::AlertFired,
            PlatformEventType::AlertResolved,
            PlatformEventType::ExceptionGroupCreated,
            PlatformEventType::ExceptionGroupRegressed,
            PlatformEventType::FeatureFlagChanged,
            PlatformEventType::LlmGuardrailTriggered,
            PlatformEventType::AgentInvestigationCompleted,
            PlatformEventType::ProviderKeyError,
            PlatformEventType::RolloutRolledBack,
            PlatformEventType::InvestigationCompleted,
            PlatformEventType::SyncJobCompleted,
            PlatformEventType::SyncJobFailed,
            PlatformEventType::PipelineStepCompleted,
            PlatformEventType::ScheduledPricingSync,
        ];

        for original in &variants {
            let json = serde_json::to_value(original).unwrap();
            let deserialized: PlatformEventType = serde_json::from_value(json).unwrap();
            assert_eq!(
                original.to_string(),
                deserialized.to_string(),
                "Display mismatch after serde round-trip for {:?}",
                original
            );
        }
    }

    /// Serde representation must match the Display string so that
    /// subscriptions stored as e.g. "alert_fired" match the event type
    /// both when deserialized and when converted via Display.
    #[test]
    fn serde_value_matches_display() {
        let cases = vec![
            (PlatformEventType::AlertFired, "alert_fired"),
            (PlatformEventType::AlertResolved, "alert_resolved"),
            (
                PlatformEventType::ExceptionGroupCreated,
                "exception_group_created",
            ),
            (
                PlatformEventType::ExceptionGroupRegressed,
                "exception_group_regressed",
            ),
            (
                PlatformEventType::FeatureFlagChanged,
                "feature_flag_changed",
            ),
            (
                PlatformEventType::LlmGuardrailTriggered,
                "llm_guardrail_triggered",
            ),
            (
                PlatformEventType::AgentInvestigationCompleted,
                "agent_investigation_completed",
            ),
            (
                PlatformEventType::ProviderKeyError,
                "provider_key_error",
            ),
            (
                PlatformEventType::RolloutRolledBack,
                "rollout_rolled_back",
            ),
            (
                PlatformEventType::InvestigationCompleted,
                "investigation_completed",
            ),
            (PlatformEventType::SyncJobCompleted, "sync_job_completed"),
            (PlatformEventType::SyncJobFailed, "sync_job_failed"),
            (
                PlatformEventType::PipelineStepCompleted,
                "pipeline_step_completed",
            ),
            (
                PlatformEventType::ScheduledPricingSync,
                "scheduled_pricing_sync",
            ),
        ];

        for (event_type, expected_str) in &cases {
            assert_eq!(
                &event_type.to_string(),
                expected_str,
                "Display mismatch for {:?}",
                event_type
            );
            let json = serde_json::to_value(event_type).unwrap();
            assert_eq!(
                json.as_str().unwrap(),
                *expected_str,
                "Serde value mismatch for {:?}",
                event_type
            );
        }
    }

    #[test]
    fn platform_event_full_serde_round_trip() {
        let event = PlatformEvent {
            id: Uuid::new_v4(),
            event_type: PlatformEventType::AlertFired,
            project_id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            source: EventSource::Watch,
            payload: serde_json::json!({"rule_name": "High CPU"}),
            dedup_key: "alert:test-rule".to_string(),
        };

        let serialized = serde_json::to_vec(&event).unwrap();
        let deserialized: PlatformEvent = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(event.id, deserialized.id);
        assert_eq!(event.event_type, deserialized.event_type);
        assert_eq!(event.project_id, deserialized.project_id);
        assert_eq!(event.source, deserialized.source);
        assert_eq!(event.payload, deserialized.payload);
        assert_eq!(event.dedup_key, deserialized.dedup_key);
    }
}
