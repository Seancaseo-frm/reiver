//! Authorization helpers for API endpoints
//!
//! Provides shared authorization functions for checking user roles
//! and permissions across different API modules.

use crate::error::{AppError, Result};
use sqlx::PgPool;
use tracing::error;
use uuid::Uuid;

// ============================================================================
// Platform Admin Helpers
// ============================================================================

/// Require that the user is a platform admin (has `is_platform_admin = true`).
pub async fn require_platform_admin(db: &PgPool, user_id: Uuid) -> Result<()> {
    let is_admin: bool = sqlx::query_scalar("SELECT is_platform_admin FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(db)
        .await
        .map_err(|e| {
            error!("Failed to check platform admin status: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error"))
        })?
        .unwrap_or(false);

    if !is_admin {
        return Err(AppError::Forbidden("Platform admin access required".into()));
    }
    Ok(())
}

// ============================================================================
// Organization Membership Helpers
// ============================================================================

/// Get the organization ID for a user from their active membership.
///
/// # Arguments
/// * `db` - Database connection pool
/// * `user_id` - The user to look up
///
/// # Returns
/// * `Ok(Some(org_id))` if user has an active membership
/// * `Ok(None)` if user has no active membership
/// * `Err` on database error
pub async fn get_user_organization(db: &PgPool, user_id: Uuid) -> Result<Option<Uuid>> {
    let row: Option<(Uuid,)> = sqlx::query_as(
        r#"
        SELECT organization_id
        FROM memberships
        WHERE user_id = $1 AND status = 'active'
        LIMIT 1
        "#,
    )
    .bind(user_id)
    .fetch_optional(db)
    .await
    .map_err(|e| {
        error!("Failed to get user organization: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    Ok(row.map(|(org_id,)| org_id))
}

/// Check if a user is an admin or owner of an organization.
///
/// # Arguments
/// * `db` - Database connection pool
/// * `user_id` - The user to check
/// * `organization_id` - The organization to check membership for
///
/// # Returns
/// * `Ok(true)` if user is admin or owner with active membership
/// * `Ok(false)` if user is not admin/owner or has no active membership
/// * `Err` on database error
pub async fn is_org_admin(db: &PgPool, user_id: Uuid, organization_id: Uuid) -> Result<bool> {
    let row: Option<(String,)> = sqlx::query_as(
        r#"
        SELECT role
        FROM memberships
        WHERE user_id = $1 AND organization_id = $2 AND status = 'active'
        "#,
    )
    .bind(user_id)
    .bind(organization_id)
    .fetch_optional(db)
    .await
    .map_err(|e| {
        error!("Failed to check admin status: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    Ok(row
        .map(|(role,)| role == "owner" || role == "admin")
        .unwrap_or(false))
}

// ============================================================================
// Authorization Enforcement Helpers
// ============================================================================

/// Check if a user is an admin or owner of an organization.
///
/// # Arguments
/// * `db` - Database connection pool
/// * `user_id` - The user attempting the action
/// * `org_id` - The organization to check membership for
///
/// # Returns
/// * `Ok(())` if user is admin or owner
/// * `Err(AppError::Auth)` if user lacks permissions or is not a member
pub async fn require_org_admin(db: &PgPool, user_id: Uuid, org_id: Uuid) -> Result<()> {
    let membership: Option<(String,)> = sqlx::query_as(
        r#"
        SELECT role FROM memberships 
        WHERE user_id = $1 AND organization_id = $2 AND status = 'active'
        "#,
    )
    .bind(user_id)
    .bind(org_id)
    .fetch_optional(db)
    .await
    .map_err(|e| {
        error!("Failed to check organization membership: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    match membership {
        Some((role,)) if role == "admin" || role == "owner" => Ok(()),
        Some(_) => Err(AppError::Auth("Requires admin or owner role".to_string())),
        None => Err(AppError::Auth(
            "Not a member of this organization".to_string(),
        )),
    }
}

/// Check if a user is an admin/owner of any organization.
///
/// # Returns
/// * `Ok(())` if user is admin/owner of at least one organization
/// * `Err(AppError::Auth)` if user has no admin privileges
pub async fn require_any_org_admin(db: &PgPool, user_id: Uuid) -> Result<()> {
    let is_admin: Option<(bool,)> = sqlx::query_as(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM memberships 
            WHERE user_id = $1 AND status = 'active' AND role IN ('admin', 'owner')
        ) as is_admin
        "#,
    )
    .bind(user_id)
    .fetch_optional(db)
    .await
    .map_err(|e| {
        error!("Failed to check admin status: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    if is_admin.map(|(v,)| v).unwrap_or(false) {
        Ok(())
    } else {
        Err(AppError::Auth("Requires admin privileges".to_string()))
    }
}

/// Check if a user has admin access to another user's resources.
///
/// Returns Ok if:
/// - The current user is the target user (self-access), OR
/// - The current user is an admin/owner of an organization that the target user belongs to
///
/// # Arguments
/// * `db` - Database connection pool  
/// * `current_user_id` - The user attempting the action
/// * `target_user_id` - The user whose resources are being accessed (None means any admin check)
pub async fn require_user_admin_access(
    db: &PgPool,
    current_user_id: Uuid,
    target_user_id: Option<Uuid>,
) -> Result<()> {
    // If no target user specified, just check admin privileges
    if target_user_id.is_none() {
        return require_any_org_admin(db, current_user_id).await;
    }

    let target = target_user_id.unwrap();

    // Self-access is always allowed
    if target == current_user_id {
        return Ok(());
    }

    // Check if current user is admin of any organization that the target user belongs to
    let has_admin_access: Option<(bool,)> = sqlx::query_as(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM memberships m1
            INNER JOIN memberships m2 ON m1.organization_id = m2.organization_id
            WHERE m1.user_id = $1 AND m1.status = 'active' AND m1.role IN ('admin', 'owner')
              AND m2.user_id = $2 AND m2.status = 'active'
        ) as has_access
        "#,
    )
    .bind(current_user_id)
    .bind(target)
    .fetch_optional(db)
    .await
    .map_err(|e| {
        error!("Failed to check user admin access: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    if has_admin_access.map(|(v,)| v).unwrap_or(false) {
        Ok(())
    } else {
        Err(AppError::Auth(
            "No admin access to manage this user".to_string(),
        ))
    }
}
