//! Internal endpoint: emit scheduled platform events.
//!
//! Called by a Kubernetes CronJob to trigger scheduled events
//! (e.g. daily pricing sync). The event is published to Kafka
//! and picked up by the event worker.

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WatchState;
use reiver_core::events::PlatformEventType;

const ALLOWED_EVENT_TYPES: &[&str] = &["scheduled_pricing_sync"];

#[derive(Debug, Deserialize)]
pub struct EmitScheduledEventRequest {
    pub event_type: String,
    pub project_id: Uuid,
}

#[derive(Debug, Serialize)]
struct EmitScheduledEventResponse {
    event_id: Uuid,
    event_type: String,
}

pub fn create_scheduled_events_router() -> Router<Arc<WatchState>> {
    Router::new().route("/emit-scheduled-event", post(emit_scheduled_event))
}

async fn emit_scheduled_event(
    State(state): State<Arc<WatchState>>,
    Json(req): Json<EmitScheduledEventRequest>,
) -> Result<Json<EmitScheduledEventResponse>, StatusCode> {
    if !ALLOWED_EVENT_TYPES.contains(&req.event_type.as_str()) {
        tracing::warn!(event_type = %req.event_type, "Rejected unknown scheduled event type");
        return Err(StatusCode::BAD_REQUEST);
    }

    let event_type = match req.event_type.as_str() {
        "scheduled_pricing_sync" => PlatformEventType::ScheduledPricingSync,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let event_id = Uuid::new_v4();

    if let Err(e) = state
        .event_publisher
        .emit(
            event_type,
            req.project_id,
            format!("scheduled:{}", event_id),
            serde_json::json!({
                "triggered_by": "scheduler",
                "event_id": event_id,
            }),
        )
        .await
    {
        tracing::error!(error = %e, "Failed to emit scheduled event");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    tracing::info!(
        %event_id,
        event_type = %req.event_type,
        project_id = %req.project_id,
        "Emitted scheduled event"
    );

    Ok(Json(EmitScheduledEventResponse {
        event_id,
        event_type: req.event_type,
    }))
}
