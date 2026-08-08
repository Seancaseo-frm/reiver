//! Shared authentication helpers for API endpoints.
//!
//! This module provides common authentication and authorization utilities
//! used across billing and payment API endpoints.

use axum::{
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::Serialize;
use sqlx;
use tracing::{debug, error};
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::auth::{authenticate_request_or_api_key, AuthIdentity};
use crate::authorization::{get_user_organization, is_org_admin};
use crate::rate_limit::RateLimitType;

/// Authenticated request context containing user and organization info.
#[derive(Debug, Clone)]
pub struct AuthCtx {
    pub user_id: Uuid,
    pub organization_id: Uuid,
}

/// Standard error response for API endpoints.
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

/// Extract authentication context from request.
/// Returns user ID and organization ID, or an error response.
pub async fn authenticate(
    headers: &HeaderMap,
    state: &WebsiteState,
) -> Result<AuthCtx, (StatusCode, Json<ErrorResponse>)> {
    let identity = authenticate_request_or_api_key(headers, state, RateLimitType::Crud)
        .await
        .map_err(|e| {
            debug!("Authentication failed: {}", e);
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::new("Authentication required")),
            )
        })?;

    match identity {
        AuthIdentity::User(user_id) => {
            let organization_id = get_user_organization(state.db.as_ref(), user_id)
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

            Ok(AuthCtx {
                user_id,
                organization_id,
            })
        }
        AuthIdentity::ApiKey { project_id } => {
            let organization_id: Uuid = sqlx::query_scalar(
                "SELECT organization_id FROM projects WHERE id = $1",
            )
            .bind(project_id)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to get project organization");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new("Internal error")),
                )
            })?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse::new("Project not found")),
                )
            })?;

            Ok(AuthCtx {
                user_id: Uuid::nil(),
                organization_id,
            })
        }
    }
}

/// Check if user is an organization admin.
/// Returns Ok(()) if user is admin, or an error response otherwise.
pub async fn require_admin(
    state: &WebsiteState,
    user_id: Uuid,
    organization_id: Uuid,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let is_admin = is_org_admin(state.db.as_ref(), user_id, organization_id)
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
