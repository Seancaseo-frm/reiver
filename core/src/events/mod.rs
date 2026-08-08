//! Platform event bus — shared types and publisher.
//!
//! Every service (Watch, Flow, Pond) uses `EventPublisher` to emit
//! `PlatformEvent`s onto the Kafka event bus.  A downstream event worker
//! matches events against user-configured subscriptions and dispatches
//! actions (webhooks, notifications, agent tasks).

mod types;

pub use types::*;

use crate::kafka::KafkaProducer;
use anyhow::Result;
use std::sync::Arc;
use uuid::Uuid;

/// Thin wrapper held on each service's app state.
///
/// Stamps every event with the originating `source` so consumers know
/// which product emitted it.
pub struct EventPublisher {
    kafka: Arc<KafkaProducer>,
    source: EventSource,
}

impl EventPublisher {
    pub fn new(kafka: Arc<KafkaProducer>, source: EventSource) -> Self {
        Self { kafka, source }
    }

    /// Publish a platform event to the Kafka event bus.
    ///
    /// The event is keyed by `project_id` for partition-level ordering
    /// per project (e.g. "alert_fired then alert_resolved" stay in order).
    ///
    /// `dedup_key` is an emitter-defined string used by the event worker
    /// to suppress duplicate notifications via Redis `SET NX EX`.
    /// The emitter constructs it based on its own dedup semantics.
    pub async fn emit(
        &self,
        event_type: PlatformEventType,
        project_id: Uuid,
        dedup_key: String,
        payload: serde_json::Value,
    ) -> Result<()> {
        let event = PlatformEvent {
            id: Uuid::new_v4(),
            event_type,
            project_id,
            timestamp: chrono::Utc::now(),
            source: self.source.clone(),
            payload,
            dedup_key,
        };
        self.kafka.send_platform_event(&event).await
    }
}
