pub mod access;
pub mod messages;
pub mod push;
pub mod registry;
pub mod settings;
pub mod tasks;
pub mod topology;

use crate::app_state::HerdState;
use axum::Router;
use std::sync::Arc;

/// REST API router (nested under /api/herd).
/// The JSON-RPC endpoint (/a2a) is merged at root level separately.
pub fn create_herd_api_router() -> Router<Arc<HerdState>> {
    Router::new()
        .merge(registry::router())
        .merge(access::router())
        .merge(tasks::router())
        .merge(push::router())
        .merge(settings::router())
        .merge(topology::router())
}
