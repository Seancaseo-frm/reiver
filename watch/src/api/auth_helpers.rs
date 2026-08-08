//! Shared authentication helpers for API endpoints.
//!
//! With the website proxy handling all JWT and API key validation,
//! these helpers now extract trusted headers and query the database
//! for organization membership and admin status.

use axum::{
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::Serialize;
use tracing::error;
use uuid::Uuid;

use crate::api;
use crate::app_state::WatchState;

/// Authenticated request context containing user and organization info.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user_id: Uuid,
    pub organization_id: Uuid,
}

/// Standard error response for API endpoints.
#[derive(Serialize)]
pub struct ErrorResponse {
    pub success: bool,
    pub error: String,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            success: false,
            error: error.into(),
        }
    }
}

/// Extract authentication context from request.
///
/// Reads the trusted `X-User-Id` header (set by the website proxy after JWT
/// validation) and looks up the user's organization from the database.
pub async fn authenticate(
    headers: &HeaderMap,
    state: &WatchState,
) -> Result<AuthContext, (StatusCode, Json<ErrorResponse>)> {
    let user_id = api::extract_user_id(headers).map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse::new("Missing or invalid X-User-Id header")),
        )
    })?;

    let organization_id =
        reiver_core::authorization::get_user_organization(state.db.as_ref(), user_id)
            .await
            .map_err(|e| {
                error!(user_id = %user_id, error = %e, "Failed to get organization");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new("Internal error")),
                )
            })?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse::new(
                        "User not associated with an organization",
                    )),
                )
            })?;

    Ok(AuthContext {
        user_id,
        organization_id,
    })
}

/// Check if user is an organization admin.
/// Returns Ok(()) if user is admin, or an error response otherwise.
pub async fn require_admin(
    state: &WatchState,
    user_id: Uuid,
    organization_id: Uuid,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let is_admin =
        reiver_core::authorization::is_org_admin(state.db.as_ref(), user_id, organization_id)
            .await
            .map_err(|e| {
                error!(user_id = %user_id, error = %e, "Failed to check admin status");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new("Failed to verify permissions")),
                )
            })?;

    if !is_admin {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::new("Admin access required")),
        ));
    }

    Ok(())
}
