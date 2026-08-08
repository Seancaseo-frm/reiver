pub mod warehouse;
pub mod catalog;

use axum::Router;
use std::sync::Arc;
use crate::app_state::PondState;

/// Create the Pond (Data Warehouse) API router.
pub fn create_pond_api_router() -> Router<Arc<PondState>> {
    Router::new()
        // Data Warehouse (routes already have full paths)
        .merge(warehouse::routes())
        // Catalog API
        .merge(catalog::routes())
}
