use axum::{
    extract::{Path, State},
    response::Json,
    routing::get,
    Router,
};
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::auth::authenticate_request;
use crate::error::{AppError, Result};
use crate::models::Organization;
use crate::rate_limit::RateLimitType;
use axum::http::HeaderMap;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct OrganizationWithRole {
    pub id: Uuid,
    pub name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub role: String,
}

pub fn create_organizations_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/", get(list_organizations))
        .route("/{id}", get(get_organization))
}

async fn list_organizations(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<OrganizationWithRole>>> {
    let user_id = authenticate_request(&headers, &state, RateLimitType::Crud).await?;

    let orgs = sqlx::query_as::<_, OrganizationWithRole>(
        r#"
        SELECT o.id, o.name, o.created_at, m.role
        FROM organizations o
        INNER JOIN memberships m ON o.id = m.organization_id
        WHERE m.user_id = $1 AND m.status = 'active' AND o.deleted_at IS NULL
        ORDER BY o.created_at ASC
        "#,
    )
    .bind(user_id)
    .fetch_all(&*state.db)
    .await?;

    Ok(Json(orgs))
}

async fn get_organization(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(org_id): Path<Uuid>,
) -> Result<Json<OrganizationWithRole>> {
    let user_id = authenticate_request(&headers, &state, RateLimitType::Crud).await?;

    let org = sqlx::query_as::<_, OrganizationWithRole>(
        r#"
        SELECT o.id, o.name, o.created_at, m.role
        FROM organizations o
        INNER JOIN memberships m ON o.id = m.organization_id
        WHERE o.id = $1 AND m.user_id = $2 AND m.status = 'active' AND o.deleted_at IS NULL
        "#,
    )
    .bind(org_id)
    .bind(user_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Organization not found".to_string()))?;

    Ok(Json(org))
}
