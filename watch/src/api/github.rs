//! GitHub integration API endpoints.
//!
//! Provides endpoints for:
//! - Installing the Reiver GitHub App
//! - Managing GitHub installations
//! - Linking projects to repositories
//! - Fetching commit details for exceptions
//! - Receiving GitHub webhooks

use axum::{
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Redirect, Response},
    routing::{delete, get, post},
    Router,
};
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use ipnetwork::IpNetwork;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use crate::github::{
    get_version_introduction_info, parse_repo_url, verify_webhook_signature, CommitWithPulls,
    GitHubService, VersionIntroductionInfo,
};
use reiver_core::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use reiver_core::authorization::{get_user_organization, require_org_admin};

/// TTL for CSRF state tokens (10 minutes)
const CSRF_STATE_TTL_SECONDS: u64 = 600;

/// Redis key prefix for GitHub CSRF state tokens
const REDIS_KEY_GITHUB_CSRF: &str = "github:csrf";

/// Redis key prefix for GitHub commit cache
const REDIS_KEY_GITHUB_COMMIT: &str = "github:commit";

/// TTL for commit cache (1 hour)
const COMMIT_CACHE_TTL_SECONDS: u64 = 3600;

/// Maximum webhook payload size (10 MB)
/// GitHub webhooks are typically small, but we allow for large payloads
/// in case of repository events with many files.
const WEBHOOK_MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Redis key prefix for webhook delivery deduplication
const REDIS_KEY_GITHUB_DELIVERY: &str = "github:delivery";

/// TTL for webhook delivery deduplication (1 hour)
/// GitHub may retry webhooks for up to 3 days, but 1 hour covers most retries.
const WEBHOOK_DELIVERY_TTL_SECONDS: u64 = 3600;

/// Default number of items per page for list endpoints
const PAGINATION_DEFAULT_LIMIT: i64 = 100;

/// Maximum number of items per page for list endpoints
const PAGINATION_MAX_LIMIT: i64 = 1000;

/// Minimum number of items per page (at least 1 item)
const PAGINATION_MIN_LIMIT: i64 = 1;

/// Maximum pagination offset to prevent unreasonable queries
const PAGINATION_MAX_OFFSET: i64 = 1_000_000;

/// Maximum age for webhook timestamps (5 minutes in seconds)
/// Webhooks older than this are rejected to prevent delayed replay attacks.
const WEBHOOK_MAX_AGE_SECONDS: i64 = 300;

/// Generate a cryptographically random state token for CSRF protection.
fn generate_csrf_state() -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Store CSRF state in Redis with the user ID and originating project ID.
async fn store_csrf_state(
    redis: &Arc<Pool<RedisConnectionManager>>,
    state_token: &str,
    user_id: Uuid,
    project_id: Option<Uuid>,
) -> Result<()> {
    let mut conn = redis.get().await.map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    let key = format!("{}:{}", REDIS_KEY_GITHUB_CSRF, state_token);
    let value = match project_id {
        Some(pid) => format!("{}:{}", user_id, pid),
        None => user_id.to_string(),
    };
    let _: () = conn
        .set_ex(&key, value, CSRF_STATE_TTL_SECONDS)
        .await
        .map_err(|e| AppError::Redis(e))?;

    Ok(())
}

/// Validate and consume a CSRF state token.
/// Returns (user_id, optional project_id) if valid, or an error if invalid/expired.
async fn validate_csrf_state(
    redis: &Arc<Pool<RedisConnectionManager>>,
    state_token: &str,
) -> Result<(Uuid, Option<Uuid>)> {
    let mut conn = redis.get().await.map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    let key = format!("{}:{}", REDIS_KEY_GITHUB_CSRF, state_token);

    // Get and delete atomically to prevent replay attacks
    let stored: Option<String> = conn.get_del(&key).await.map_err(|e| AppError::Redis(e))?;

    let stored =
        stored.ok_or_else(|| AppError::BadRequest("Invalid or expired state token".to_string()))?;

    // Format: "user_id" or "user_id:project_id"
    let mut parts = stored.splitn(2, ':');
    let user_id = Uuid::parse_str(parts.next().unwrap_or(""))
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid user ID in CSRF state")))?;
    let project_id = parts.next().and_then(|s| Uuid::parse_str(s).ok());

    Ok((user_id, project_id))
}

/// Check if a webhook delivery has already been processed and mark it as seen.
/// Uses Redis SET NX (set if not exists) for atomic check-and-set.
///
/// Also validates webhook age to prevent delayed replay attacks. Even if an attacker
/// captures a signed webhook payload, they cannot replay it after the max age window.
///
/// Returns `true` if this delivery was already processed (duplicate) or is too old.
/// Returns `false` if this is a new, valid delivery (marked as seen).
async fn is_duplicate_delivery(
    redis: &Arc<Pool<RedisConnectionManager>>,
    delivery_id: &str,
) -> Result<bool> {
    let mut conn = redis.get().await.map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    let key = format!("{}:{}", REDIS_KEY_GITHUB_DELIVERY, delivery_id);
    let now = chrono::Utc::now().timestamp();

    // Store timestamp as value for age validation
    // SET NX EX returns:
    // - Some(()) when the key was set (new delivery)
    // - None when the key already existed (duplicate, NX condition failed)
    let result: Option<()> = redis::cmd("SET")
        .arg(&key)
        .arg(now.to_string())
        .arg("NX")
        .arg("EX")
        .arg(WEBHOOK_DELIVERY_TTL_SECONDS)
        .query_async(&mut *conn)
        .await
        .map_err(|e| AppError::Redis(e))?;

    if result.is_some() {
        // New delivery - key was just set
        return Ok(false);
    }

    // Key already exists - check if original timestamp is too old
    // This prevents replay attacks after the deduplication window
    let stored_timestamp: Option<String> = conn.get(&key).await.map_err(|e| AppError::Redis(e))?;

    if let Some(ts_str) = stored_timestamp {
        if let Ok(stored_ts) = ts_str.parse::<i64>() {
            let age = now - stored_ts;
            if age > WEBHOOK_MAX_AGE_SECONDS {
                warn!(
                    delivery_id = %delivery_id,
                    age_seconds = age,
                    max_age_seconds = WEBHOOK_MAX_AGE_SECONDS,
                    "Rejecting stale GitHub webhook delivery"
                );
                // Return true (duplicate/rejected) for stale webhooks
                return Ok(true);
            }
        }
    }

    // Duplicate delivery within acceptable age window
    Ok(true)
}

/// Validate a git commit SHA.
/// SHA must be 40 hexadecimal characters.
fn validate_commit_sha(sha: &str) -> Result<()> {
    if sha.len() != 40 {
        return Err(AppError::BadRequest(
            "Invalid commit SHA: must be 40 characters".to_string(),
        ));
    }
    if !sha.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AppError::BadRequest(
            "Invalid commit SHA: must contain only hexadecimal characters".to_string(),
        ));
    }
    Ok(())
}

/// Validate an exception fingerprint.
/// Fingerprint should be alphanumeric with dashes and underscores only.
fn validate_fingerprint(fingerprint: &str) -> Result<()> {
    if fingerprint.is_empty() || fingerprint.len() > 256 {
        return Err(AppError::BadRequest(
            "Invalid fingerprint: must be between 1 and 256 characters".to_string(),
        ));
    }
    if !fingerprint
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(AppError::BadRequest(
            "Invalid fingerprint: must contain only alphanumeric characters, dashes, and underscores".to_string()
        ));
    }
    Ok(())
}

/// Get the user's organization, returning an error if none exists.
async fn get_user_org(state: &WatchState, user_id: Uuid) -> Result<Uuid> {
    get_user_organization(state.db.as_ref(), user_id)
        .await?
        .ok_or_else(|| AppError::Forbidden("User has no organization".to_string()))
}

/// Get the GitHub service from WatchState, returning an error if not configured.
fn get_github_service(state: &WatchState) -> Result<&GitHubService> {
    state
        .github_service
        .as_ref()
        .map(|s| s.as_ref())
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("GitHub App not configured")))
}

/// Resolve a project's linked GitHub repository.
/// Returns (org_id, owner, repo_name).
async fn resolve_project_repo(
    state: &WatchState,
    project_id: Uuid,
    user_id: Uuid,
) -> Result<(Uuid, String, String)> {
    let project: Option<(Uuid, Option<String>)> = sqlx::query_as(
        r#"
        SELECT p.organization_id, p.github_repo_url FROM projects p
        JOIN memberships om ON p.organization_id = om.organization_id AND om.status = 'active'
        WHERE p.id = $1 AND om.user_id = $2
        "#,
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let (org_id, repo_url) =
        project.ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;
    let repo_url = repo_url.ok_or_else(|| {
        AppError::BadRequest("Project not linked to GitHub repository".to_string())
    })?;
    let (owner, repo) = parse_repo_url(&repo_url).ok_or_else(|| {
        AppError::BadRequest("Invalid repository URL stored for project".to_string())
    })?;

    Ok((org_id, owner, repo))
}

/// Find the GitHub installation that has access to a given repository.
async fn find_installation(
    state: &WatchState,
    org_id: Uuid,
    owner: &str,
    repo: &str,
) -> Result<i64> {
    let full_name = format!("{}/{}", owner, repo);
    sqlx::query_scalar(
        r#"
        SELECT installation_id FROM github_installations
        WHERE organization_id = $1
        AND repositories @> $2::jsonb
        LIMIT 1
        "#,
    )
    .bind(org_id)
    .bind(serde_json::json!([{"full_name": full_name}]))
    .fetch_optional(state.db.as_ref())
    .await?
    .ok_or_else(|| {
        AppError::BadRequest(format!(
            "No GitHub installation with access to repository '{}'.",
            full_name
        ))
    })
}

/// Serialize GitHub repositories to JSON for storage.
///
/// Handles cases where `full_name` is None by constructing it from owner/name.
fn serialize_repos_to_json(repos: &[crate::github::GitHubRepository]) -> Vec<serde_json::Value> {
    repos
        .iter()
        .map(|r| {
            let full_name = r.full_name.clone().unwrap_or_else(|| {
                // Construct full_name from owner login and repo name if not provided
                r.owner
                    .as_ref()
                    .map(|o| format!("{}/{}", o.login, r.name))
                    .unwrap_or_else(|| r.name.clone())
            });
            serde_json::json!({
                "name": r.name,
                "full_name": full_name,
                "private": r.private.unwrap_or(false),
                "html_url": r.html_url.clone()
            })
        })
        .collect()
}

/// Verify installation ownership and store/update the installation record.
///
/// # Security
/// Checks if the installation already belongs to a different organization
/// to prevent installation takeover attacks via race conditions.
///
/// # Arguments
/// * `db` - Database pool
/// * `org_id` - The organization claiming the installation
/// * `installation_id` - GitHub App installation ID
/// * `account_login` - GitHub account login (org or user name)
/// * `account_type` - "Organization" or "User"
/// * `repos_json` - Serialized repository list
async fn verify_and_store_installation(
    db: &sqlx::PgPool,
    org_id: Uuid,
    installation_id: u64,
    account_login: &str,
    account_type: &str,
    repos_json: Vec<serde_json::Value>,
) -> Result<()> {
    let mut tx = db.begin().await?;

    // Check if installation exists for a different organization
    let existing_org: Option<Uuid> = sqlx::query_scalar(
        "SELECT organization_id FROM github_installations WHERE installation_id = $1",
    )
    .bind(installation_id as i64)
    .fetch_optional(&mut *tx)
    .await?;

    if let Some(existing_org_id) = existing_org {
        if existing_org_id != org_id {
            tx.rollback().await?;
            warn!(
                installation_id = installation_id,
                existing_org = %existing_org_id,
                requesting_org = %org_id,
                "Attempted to claim GitHub installation owned by another organization"
            );
            return Err(AppError::Forbidden(
                "This GitHub installation is already linked to another organization".to_string(),
            ));
        }
    }

    sqlx::query(
        r#"
        INSERT INTO github_installations (
            organization_id, installation_id, account_login, account_type, repositories
        ) VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (installation_id) DO UPDATE SET
            account_login = EXCLUDED.account_login,
            account_type = EXCLUDED.account_type,
            repositories = EXCLUDED.repositories,
            updated_at = NOW()
        WHERE github_installations.organization_id = EXCLUDED.organization_id
        "#,
    )
    .bind(org_id)
    .bind(installation_id as i64)
    .bind(account_login)
    .bind(account_type)
    .bind(serde_json::Value::Array(repos_json))
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(())
}

/// Create the GitHub integration router.
///
/// Returns an empty router if GitHub App is not configured, avoiding runtime errors
/// for deployments that don't use GitHub integration.
pub fn create_github_router(config: &crate::config::Config) -> Router<Arc<WatchState>> {
    // Check if GitHub is configured
    if config.github_app_id.is_none() || config.github_app_private_key.is_none() {
        debug!("GitHub App not configured, skipping GitHub routes");
        return Router::new();
    }

    // Webhook route with body size limit to prevent DoS
    let webhook_router = Router::new()
        .route("/webhook", post(handle_github_webhook))
        .layer(DefaultBodyLimit::max(WEBHOOK_MAX_BODY_SIZE));

    Router::new()
        // Installation flow
        .route("/install", get(install_redirect))
        .route("/callback", get(installation_callback))
        // Webhook endpoint (no auth - verified by signature, body size limited)
        .merge(webhook_router)
        // Installation management
        .route("/installations", get(list_installations))
        .route(
            "/installations/{installation_id}",
            delete(delete_installation),
        )
        // Repository linking
        .route(
            "/installations/{installation_id}/repos",
            get(list_installation_repos),
        )
}

/// Create the project-specific GitHub router (for merging into projects router).
/// Routes are prefixed with /{id}/ to match the project router pattern.
///
/// Returns an empty router if GitHub App is not configured.
pub fn create_project_github_router(config: &crate::config::Config) -> Router<Arc<WatchState>> {
    // Check if GitHub is configured
    if config.github_app_id.is_none() || config.github_app_private_key.is_none() {
        return Router::new();
    }

    Router::new()
        .route(
            "/{id}/github",
            post(link_project_to_repo).delete(unlink_project_from_repo),
        )
        .route("/{id}/github/commits", get(list_recent_commits_for_project))
        .route("/{id}/github/commit/{sha}", get(get_commit_for_project))
        .route("/{id}/github/tree", get(list_directory_for_project))
        .route("/{id}/github/file", get(read_file_for_project))
        .route("/{id}/github/search", get(search_code_for_project))
        .route(
            "/{id}/github/version-info/{fingerprint}",
            get(get_version_info),
        )
}

// =============================================================================
// Installation Flow
// =============================================================================

/// Redirect user to GitHub App installation page.
///
/// GET /github/install
#[axum::debug_handler]
async fn install_redirect(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Response> {
    let user_id = crate::api::extract_user_id(&headers)?;
    let project_id = crate::api::extract_project_id_optional(&headers);

    // Generate CSRF state token
    let csrf_state = generate_csrf_state();

    // Store state token in Redis with user ID and originating project
    store_csrf_state(&state.redis, &csrf_state, user_id, project_id).await?;

    // Get the GitHub App name from config or environment
    let github_app_name = state
        .config
        .github_app_name
        .clone()
        .unwrap_or_else(|| "reiver".to_string());

    // Build URL with state parameter for CSRF protection
    let install_url = format!(
        "https://github.com/apps/{}/installations/new?state={}",
        github_app_name, csrf_state
    );

    info!("Redirecting user to GitHub App installation");

    Ok(Redirect::temporary(&install_url).into_response())
}

/// GitHub App installation callback query parameters.
#[derive(Debug, Deserialize)]
pub struct InstallationCallbackQuery {
    /// Installation ID from GitHub
    installation_id: Option<u64>,
    /// Setup action (install, update, etc.)
    setup_action: Option<String>,
    /// State parameter for CSRF protection
    state: Option<String>,
}

/// Handle GitHub App installation callback.
///
/// GET /github/callback?installation_id=...&setup_action=...&state=...
///
/// # Security
/// Rate limiting is applied before CSRF validation to prevent token enumeration attacks.
/// An attacker could otherwise probe for valid state tokens without being rate limited.
#[axum::debug_handler]
async fn installation_callback(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(query): Query<InstallationCallbackQuery>,
) -> Result<Response> {
    // Validate CSRF state token first (before any other processing)
    let csrf_state = query
        .state
        .as_ref()
        .ok_or_else(|| AppError::BadRequest("Missing state parameter".to_string()))?;

    // Validate and consume the CSRF token — returns (user_id, optional project_id)
    let (csrf_user_id, origin_project_id) = validate_csrf_state(&state.redis, csrf_state).await?;

    let user_id = crate::api::extract_user_id(&headers)?;

    // Ensure the authenticated user matches the one who initiated the flow
    if user_id != csrf_user_id {
        warn!(
            authenticated_user = %user_id,
            csrf_user = %csrf_user_id,
            "CSRF user mismatch in GitHub callback"
        );
        return Err(AppError::Forbidden(
            "User mismatch - please try again".to_string(),
        ));
    }

    info!(
        user_id = %user_id,
        installation_id = ?query.installation_id,
        setup_action = ?query.setup_action,
        "GitHub App installation callback received"
    );

    let installation_id = query
        .installation_id
        .ok_or_else(|| AppError::BadRequest("Missing installation_id parameter".to_string()))?;

    // Get user's organization and require admin privileges
    let org_id = get_user_org(&state, user_id).await?;
    require_org_admin(state.db.as_ref(), user_id, org_id).await?;

    // Get GitHub service from cached WatchState
    let github_service = get_github_service(&state)?;

    // SECURITY: Verify the installation exists and we can access it.
    // This prevents attackers from associating arbitrary installation IDs.
    // The API call will fail if the installation doesn't exist or isn't ours.
    let installation = github_service
        .get_installation(installation_id)
        .await
        .map_err(|e| {
            warn!(
                error = %e,
                installation_id = installation_id,
                user_id = %user_id,
                "Failed to verify GitHub installation - may be invalid or inaccessible"
            );
            AppError::BadRequest(
                "Could not verify GitHub installation. Please ensure you have installed the Reiver GitHub App.".to_string()
            )
        })?;

    // Get account info from the verified installation
    let account_login = installation.account.login.clone();
    let account_type = installation
        .account
        .account_type
        .clone()
        .unwrap_or_else(|| "User".to_string());

    // Fetch repositories accessible to the installation
    let repos = github_service
        .list_installation_repos(installation_id)
        .await
        .map_err(|e| {
            error!(
                error = %e,
                installation_id = installation_id,
                organization_id = %org_id,
                "Failed to fetch installation repos"
            );
            AppError::Internal(anyhow::anyhow!("Failed to fetch installation repositories"))
        })?;

    // Serialize repositories and store installation
    let repos_json = serialize_repos_to_json(&repos);

    // Verify ownership and store/update the installation record
    verify_and_store_installation(
        state.db.as_ref(),
        org_id,
        installation_id,
        &account_login,
        &account_type,
        repos_json,
    )
    .await?;

    info!(
        organization_id = %org_id,
        installation_id = installation_id,
        account_login = %account_login,
        repo_count = repos.len(),
        "GitHub installation saved"
    );

    // Redirect back to the project integrations page
    let base = state.config.api_base_url.as_deref().unwrap_or("");
    let redirect_url = match origin_project_id {
        Some(pid) => format!("{}/projects/{}/integrations?github=success", base, pid),
        None => format!("{}/integrations?github=success", base),
    };

    Ok(Redirect::temporary(&redirect_url).into_response())
}

// =============================================================================
// Installation Management
// =============================================================================

/// GitHub installation record for API responses.
#[derive(Debug, Serialize)]
pub struct GitHubInstallationResponse {
    pub id: Uuid,
    pub installation_id: i64,
    pub account_login: String,
    pub account_type: String,
    pub repositories: Vec<GitHubRepoInfo>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Repository info from installation.
#[derive(Debug, Serialize, Deserialize)]
pub struct GitHubRepoInfo {
    pub name: String,
    pub full_name: String,
    pub private: bool,
    pub html_url: Option<String>,
}

/// Pagination query parameters for list endpoints.
#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    /// Number of items to skip (default: 0)
    #[serde(default)]
    pub offset: i64,
    /// Maximum number of items to return (default: 100, max: 1000)
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    PAGINATION_DEFAULT_LIMIT
}

impl PaginationQuery {
    /// Get the clamped limit (between PAGINATION_MIN_LIMIT and PAGINATION_MAX_LIMIT)
    fn clamped_limit(&self) -> i64 {
        self.limit.clamp(PAGINATION_MIN_LIMIT, PAGINATION_MAX_LIMIT)
    }

    /// Get the offset (clamped between 0 and PAGINATION_MAX_OFFSET)
    fn clamped_offset(&self) -> i64 {
        self.offset.clamp(0, PAGINATION_MAX_OFFSET)
    }
}

/// Paginated response wrapper.
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub offset: i64,
    pub limit: i64,
    pub total: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct InstallationRow {
    id: Uuid,
    installation_id: i64,
    account_login: String,
    account_type: String,
    repositories: serde_json::Value,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<InstallationRow> for GitHubInstallationResponse {
    fn from(row: InstallationRow) -> Self {
        let repositories: Vec<GitHubRepoInfo> =
            serde_json::from_value(row.repositories).unwrap_or_default();

        Self {
            id: row.id,
            installation_id: row.installation_id,
            account_login: row.account_login,
            account_type: row.account_type,
            repositories,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

/// List GitHub installations for the organization.
///
/// GET /github/installations?offset=0&limit=100
#[axum::debug_handler]
async fn list_installations(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(pagination): Query<PaginationQuery>,
) -> Result<Json<PaginatedResponse<GitHubInstallationResponse>>> {
    let user_id = crate::api::extract_user_id(&headers)?;

    // Get user's organization
    let org_id = get_user_org(&state, user_id).await?;

    let limit = pagination.clamped_limit();
    let offset = pagination.clamped_offset();

    // Get total count
    let total: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM github_installations WHERE organization_id = $1")
            .bind(org_id)
            .fetch_one(state.db.as_ref())
            .await?;

    let installations: Vec<InstallationRow> = sqlx::query_as(
        r#"
        SELECT id, installation_id, account_login, account_type, repositories, created_at, updated_at
        FROM github_installations
        WHERE organization_id = $1
        ORDER BY created_at DESC
        LIMIT $2 OFFSET $3
        "#
    )
    .bind(org_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(state.db.as_ref())
    .await?;

    let data: Vec<GitHubInstallationResponse> =
        installations.into_iter().map(|r| r.into()).collect();

    Ok(Json(PaginatedResponse {
        data,
        offset,
        limit,
        total,
    }))
}

/// Delete a GitHub installation.
///
/// DELETE /github/installations/:installation_id
#[axum::debug_handler]
async fn delete_installation(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(installation_id): Path<i64>,
) -> Result<StatusCode> {
    let user_id = crate::api::extract_user_id(&headers)?;

    // Get user's organization and require admin privileges
    let org_id = get_user_org(&state, user_id).await?;
    require_org_admin(state.db.as_ref(), user_id, org_id).await?;

    // Delete installation (only if owned by this org)
    let deleted_row = sqlx::query_as::<_, InstallationRow>(
        "SELECT id, installation_id, account_login, account_type, repositories, created_at, updated_at FROM github_installations WHERE installation_id = $1 AND organization_id = $2"
    )
    .bind(installation_id)
    .bind(org_id)
    .fetch_optional(state.db.as_ref())
    .await
    .ok()
    .flatten();

    let result = sqlx::query(
        "DELETE FROM github_installations WHERE installation_id = $1 AND organization_id = $2",
    )
    .bind(installation_id)
    .bind(org_id)
    .execute(state.db.as_ref())
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Installation not found".to_string()));
    }

    info!(
        organization_id = %org_id,
        installation_id = installation_id,
        "GitHub installation deleted"
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .actor(user_id)
        .resource("github", org_id)
        .details(serde_json::json!({ "deleted": { "installation_id": installation_id, "account_login": deleted_row.as_ref().map(|r| &r.account_login) } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(StatusCode::NO_CONTENT)
}

/// List repositories for an installation.
///
/// GET /github/installations/:installation_id/repos?offset=0&limit=100
///
/// Uses SQL-level JSONB pagination to avoid deserializing all repositories
/// in memory when only a subset is needed.
#[axum::debug_handler]
async fn list_installation_repos(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(installation_id): Path<i64>,
    Query(pagination): Query<PaginationQuery>,
) -> Result<Json<PaginatedResponse<GitHubRepoInfo>>> {
    let user_id = crate::api::extract_user_id(&headers)?;

    // Get user's organization
    let org_id = get_user_org(&state, user_id).await?;

    let limit = pagination.clamped_limit();
    let offset = pagination.clamped_offset();

    // Get total count using jsonb_array_length (efficient, no deserialization)
    let total: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT COALESCE(jsonb_array_length(repositories), 0)::bigint
        FROM github_installations
        WHERE installation_id = $1 AND organization_id = $2
        "#,
    )
    .bind(installation_id)
    .bind(org_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let total = total.ok_or_else(|| AppError::NotFound("Installation not found".to_string()))?;

    // Get paginated repos using SQL-level JSONB slicing
    // Uses jsonb_array_elements with LIMIT/OFFSET to avoid deserializing all repos in memory
    let repos: Vec<(serde_json::Value,)> = sqlx::query_as(
        r#"
        SELECT repo
        FROM github_installations,
             LATERAL jsonb_array_elements(repositories) AS repo
        WHERE installation_id = $1 AND organization_id = $2
        ORDER BY repo->>'full_name'
        LIMIT $3 OFFSET $4
        "#,
    )
    .bind(installation_id)
    .bind(org_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(state.db.as_ref())
    .await?;

    // Deserialize only the paginated subset
    let data: Vec<GitHubRepoInfo> = repos
        .into_iter()
        .filter_map(|(repo_json,)| serde_json::from_value(repo_json).ok())
        .collect();

    Ok(Json(PaginatedResponse {
        data,
        offset,
        limit,
        total,
    }))
}

// =============================================================================
// Project Repository Linking
// =============================================================================

/// Request to link a project to a GitHub repository.
#[derive(Debug, Deserialize)]
pub struct LinkProjectRequest {
    /// Full repository URL (e.g., https://github.com/acme/myrepo)
    pub repository_url: String,
}

/// Maximum length for repository URLs (matches database column size)
const MAX_REPO_URL_LENGTH: usize = 500;

/// Link a project to a GitHub repository.
///
/// POST /projects/:id/github
#[axum::debug_handler]
async fn link_project_to_repo(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Json(request): Json<LinkProjectRequest>,
) -> Result<StatusCode> {
    let user_id = crate::api::extract_user_id(&headers)?;

    // Validate repository URL length before any processing
    if request.repository_url.len() > MAX_REPO_URL_LENGTH {
        return Err(AppError::BadRequest(format!(
            "Repository URL exceeds maximum length of {} characters",
            MAX_REPO_URL_LENGTH
        )));
    }

    // Validate repository URL format and extract owner/repo
    let (owner, repo) = parse_repo_url(&request.repository_url)
        .ok_or_else(|| AppError::BadRequest("Invalid GitHub repository URL".to_string()))?;

    let full_name = format!("{}/{}", owner, repo);

    // Verify user has access to this project
    let project_org_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        SELECT p.organization_id FROM projects p
        JOIN memberships om ON p.organization_id = om.organization_id AND om.status = 'active'
        WHERE p.id = $1 AND om.user_id = $2
        "#,
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let org_id =
        project_org_id.ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;

    // Require admin privileges for linking repository
    require_org_admin(state.db.as_ref(), user_id, org_id).await?;

    // Verify the repository is accessible via one of the organization's GitHub installations
    // The repositories column contains JSON array like: [{"full_name": "owner/repo", ...}]
    let has_access: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM github_installations
            WHERE organization_id = $1
            AND repositories @> $2::jsonb
        )
        "#,
    )
    .bind(org_id)
    .bind(serde_json::json!([{"full_name": full_name}]))
    .fetch_one(state.db.as_ref())
    .await?;

    if !has_access {
        return Err(AppError::BadRequest(format!(
            "Repository '{}' is not accessible. Please ensure the Reiver GitHub App is installed and has access to this repository.",
            full_name
        )));
    }

    // Update project with repository URL
    sqlx::query("UPDATE projects SET github_repo_url = $1 WHERE id = $2")
        .bind(&request.repository_url)
        .bind(project_id)
        .execute(state.db.as_ref())
        .await?;

    info!(
        project_id = %project_id,
        repository_url = %request.repository_url,
        "Project linked to GitHub repository"
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationCreated)
        .actor(user_id)
        .resource("github", project_id)
        .details(serde_json::json!({ "created": { "repository_url": &request.repository_url } }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(StatusCode::NO_CONTENT)
}

/// Unlink a project from its GitHub repository.
///
/// DELETE /projects/:id/github
#[axum::debug_handler]
async fn unlink_project_from_repo(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> Result<StatusCode> {
    let user_id = crate::api::extract_user_id(&headers)?;

    // Verify user has access to this project
    let project_org_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        SELECT p.organization_id FROM projects p
        JOIN memberships om ON p.organization_id = om.organization_id AND om.status = 'active'
        WHERE p.id = $1 AND om.user_id = $2
        "#,
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let org_id =
        project_org_id.ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;

    // Require admin privileges for unlinking repository
    require_org_admin(state.db.as_ref(), user_id, org_id).await?;

    let existing_url: Option<String> =
        sqlx::query_scalar("SELECT github_repo_url FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(state.db.as_ref())
            .await
            .ok()
            .flatten();

    // Clear repository URL
    sqlx::query("UPDATE projects SET github_repo_url = NULL WHERE id = $1")
        .bind(project_id)
        .execute(state.db.as_ref())
        .await?;

    info!(project_id = %project_id, "Project unlinked from GitHub repository");

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .actor(user_id)
        .resource("github", project_id)
        .details(serde_json::json!({ "deleted": { "repository_url": existing_url } }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(StatusCode::NO_CONTENT)
}

// =============================================================================
// Recent Commits
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RecentCommitsQuery {
    #[serde(default = "default_commits_limit")]
    pub limit: u8,
    pub branch: Option<String>,
}

fn default_commits_limit() -> u8 {
    10
}

/// List recent commits for a project's linked repository.
///
/// GET /projects/:id/github/commits?limit=10&branch=main
#[axum::debug_handler]
async fn list_recent_commits_for_project(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Query(query): Query<RecentCommitsQuery>,
) -> Result<Json<Vec<CommitResponse>>> {
    let user_id = crate::api::extract_user_id(&headers)?;

    let project: Option<(Uuid, Option<String>)> = sqlx::query_as(
        r#"
        SELECT p.organization_id, p.github_repo_url FROM projects p
        JOIN memberships om ON p.organization_id = om.organization_id AND om.status = 'active'
        WHERE p.id = $1 AND om.user_id = $2
        "#,
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let (org_id, repo_url) =
        project.ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;

    let repo_url = repo_url.ok_or_else(|| {
        AppError::BadRequest("Project not linked to GitHub repository".to_string())
    })?;

    let (owner, repo) = parse_repo_url(&repo_url).ok_or_else(|| {
        AppError::BadRequest("Invalid repository URL stored for project".to_string())
    })?;

    let full_name = format!("{}/{}", owner, repo);

    let installation_id: i64 = sqlx::query_scalar(
        r#"
        SELECT installation_id FROM github_installations
        WHERE organization_id = $1
        AND repositories @> $2::jsonb
        LIMIT 1
        "#,
    )
    .bind(org_id)
    .bind(serde_json::json!([{"full_name": full_name}]))
    .fetch_optional(state.db.as_ref())
    .await?
    .ok_or_else(|| {
        AppError::BadRequest(format!(
            "No GitHub installation with access to repository '{}' found.",
            full_name
        ))
    })?;

    let github_service = get_github_service(&state)?;

    let per_page = query.limit.min(30);
    let commits = github_service
        .list_recent_commits(
            installation_id as u64,
            &owner,
            &repo,
            query.branch.as_deref(),
            per_page,
        )
        .await
        .map_err(|e| {
            warn!(error = %e, project_id = %project_id, "Failed to list recent commits");
            AppError::Internal(anyhow::anyhow!("Failed to list commits"))
        })?;

    let response: Vec<CommitResponse> = commits.into_iter().map(CommitResponse::from).collect();
    Ok(Json(response))
}

// =============================================================================
// Directory Listing
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct TreeQuery {
    pub path: Option<String>,
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
}

/// List directory contents for a project's linked repository.
///
/// GET /projects/:id/github/tree?path=src&ref=main
#[axum::debug_handler]
async fn list_directory_for_project(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<Vec<crate::github::DirectoryEntry>>> {
    let user_id = crate::api::extract_user_id(&headers)?;
    let (org_id, owner, repo) = resolve_project_repo(&state, project_id, user_id).await?;
    let installation_id = find_installation(&state, org_id, &owner, &repo).await?;
    let github_service = get_github_service(&state)?;

    let entries = github_service
        .list_directory(
            installation_id as u64,
            &owner,
            &repo,
            query.path.as_deref(),
            query.git_ref.as_deref(),
        )
        .await
        .map_err(|e| {
            warn!(error = %e, project_id = %project_id, "Failed to list directory");
            AppError::Internal(anyhow::anyhow!("Failed to list directory"))
        })?;

    Ok(Json(entries))
}

// =============================================================================
// File Contents
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct FileQuery {
    pub path: String,
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
}

/// Read a file from the project's linked repository.
///
/// GET /projects/:id/github/file?path=src/main.rs&ref=main
#[axum::debug_handler]
async fn read_file_for_project(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Query(query): Query<FileQuery>,
) -> Result<Json<crate::github::FileContents>> {
    let user_id = crate::api::extract_user_id(&headers)?;
    let (org_id, owner, repo) = resolve_project_repo(&state, project_id, user_id).await?;
    let installation_id = find_installation(&state, org_id, &owner, &repo).await?;
    let github_service = get_github_service(&state)?;

    let contents = github_service
        .get_file_contents(
            installation_id as u64,
            &owner,
            &repo,
            &query.path,
            query.git_ref.as_deref(),
        )
        .await
        .map_err(|e| {
            warn!(error = %e, project_id = %project_id, path = %query.path, "Failed to read file");
            AppError::NotFound(format!("File not found: {}", query.path))
        })?;

    Ok(Json(contents))
}

// =============================================================================
// Code Search
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct SearchCodeQuery {
    pub q: String,
}

/// Search code in the project's linked repository.
///
/// GET /projects/:id/github/search?q=fn+main
#[axum::debug_handler]
async fn search_code_for_project(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Query(query): Query<SearchCodeQuery>,
) -> Result<Json<Vec<crate::github::CodeSearchResult>>> {
    let user_id = crate::api::extract_user_id(&headers)?;
    let (org_id, owner, repo) = resolve_project_repo(&state, project_id, user_id).await?;
    let installation_id = find_installation(&state, org_id, &owner, &repo).await?;
    let github_service = get_github_service(&state)?;

    let results = github_service
        .search_code(installation_id as u64, &owner, &repo, &query.q)
        .await
        .map_err(|e| {
            warn!(error = %e, project_id = %project_id, "Code search failed");
            AppError::Internal(anyhow::anyhow!("Code search failed"))
        })?;

    Ok(Json(results))
}

// =============================================================================
// Commit Lookup
// =============================================================================

/// Get commit details for a project.
///
/// GET /projects/:id/github/commit/:sha
///
/// Uses ExternalApi rate limiting (stricter) since this endpoint makes GitHub API calls.
/// This prevents users from exhausting the GitHub API rate limit (5000 req/hour per installation).
#[axum::debug_handler]
async fn get_commit_for_project(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path((project_id, sha)): Path<(Uuid, String)>,
) -> Result<Json<CommitResponse>> {
    // Validate SHA format before any processing
    validate_commit_sha(&sha)?;

    let user_id = crate::api::extract_user_id(&headers)?;

    // First, get the project and verify access
    let project: Option<(Uuid, Option<String>)> = sqlx::query_as(
        r#"
        SELECT p.organization_id, p.github_repo_url FROM projects p
        JOIN memberships om ON p.organization_id = om.organization_id AND om.status = 'active'
        WHERE p.id = $1 AND om.user_id = $2
        "#,
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let (org_id, repo_url) =
        project.ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;

    let repo_url = repo_url.ok_or_else(|| {
        AppError::BadRequest("Project not linked to GitHub repository".to_string())
    })?;

    // Parse repository URL
    let (owner, repo) = parse_repo_url(&repo_url).ok_or_else(|| {
        AppError::BadRequest("Invalid repository URL stored for project".to_string())
    })?;

    let full_name = format!("{}/{}", owner, repo);

    // Get installation that has access to this specific repository
    let installation_id: i64 = sqlx::query_scalar(
        r#"
        SELECT installation_id FROM github_installations
        WHERE organization_id = $1
        AND repositories @> $2::jsonb
        LIMIT 1
        "#
    )
    .bind(org_id)
    .bind(serde_json::json!([{"full_name": full_name}]))
    .fetch_optional(state.db.as_ref())
    .await?
    .ok_or_else(|| AppError::BadRequest(format!(
        "No GitHub installation with access to repository '{}' found. Please ensure the Reiver GitHub App has access to this repository.",
        full_name
    )))?;

    // Get GitHub service from cached WatchState
    let github_service = get_github_service(&state)?;

    // Cache key for commit data
    let cache_key = format!("{}:{}:{}:{}", REDIS_KEY_GITHUB_COMMIT, owner, repo, sha);

    // Try to get from cache first
    let cached_commit: Option<CommitWithPulls> = {
        let mut conn = state.redis.get().await.map_err(|e| {
            AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
        })?;
        let cached_json: Option<String> =
            conn.get(&cache_key).await.map_err(|e| AppError::Redis(e))?;
        cached_json.and_then(|json| serde_json::from_str(&json).ok())
    };

    let commit = if let Some(cached) = cached_commit {
        debug!(sha = %sha, "Using cached commit data");
        cached
    } else {
        // Fetch from GitHub and cache
        let commit = github_service
            .get_commit_with_pulls(installation_id as u64, &owner, &repo, &sha)
            .await
            .map_err(|e| {
                warn!(error = %e, sha = %sha, "Failed to fetch commit from GitHub");
                AppError::NotFound(format!("Commit {} not found", sha))
            })?;

        // Cache the result (non-critical, log failures for troubleshooting)
        if let Ok(json) = serde_json::to_string(&commit) {
            match state.redis.get().await {
                Ok(mut conn) => {
                    if let Err(e) = conn
                        .set_ex::<_, _, ()>(&cache_key, json, COMMIT_CACHE_TTL_SECONDS)
                        .await
                    {
                        debug!(error = %e, sha = %sha, "Failed to cache commit data in Redis");
                    }
                }
                Err(e) => {
                    debug!(error = %e, sha = %sha, "Failed to get Redis connection for commit caching");
                }
            }
        }

        commit
    };

    Ok(Json(CommitResponse::from(commit)))
}

/// Commit response for the API.
///
/// # Privacy Note
/// The `author_email` field is intentionally exposed in this response. This endpoint
/// requires authentication and verifies that the user is a member of the organization
/// that owns the project. Only authenticated project members can access commit details,
/// making it appropriate to include the commit author's email address for attribution
/// and collaboration purposes.
#[derive(Debug, Serialize)]
pub struct CommitResponse {
    pub sha: String,
    pub message: String,
    pub author_name: Option<String>,
    /// Git commit author email. Exposed only to authenticated project members.
    pub author_email: Option<String>,
    pub author_login: Option<String>,
    pub committer_name: Option<String>,
    pub committer_login: Option<String>,
    pub html_url: String,
    pub pull_requests: Vec<PullRequestResponse>,
}

/// Pull request response for the API.
#[derive(Debug, Serialize)]
pub struct PullRequestResponse {
    pub number: u64,
    pub title: String,
    pub state: String,
    pub html_url: String,
    pub merged: bool,
    pub author_login: Option<String>,
}

impl From<CommitWithPulls> for CommitResponse {
    fn from(commit: CommitWithPulls) -> Self {
        Self {
            sha: commit.sha,
            message: commit.message,
            author_name: commit.author_name,
            author_email: commit.author_email,
            author_login: commit.author_login,
            committer_name: commit.committer_name,
            committer_login: commit.committer_login,
            html_url: commit.html_url,
            pull_requests: commit
                .pull_requests
                .into_iter()
                .map(|pr| PullRequestResponse {
                    number: pr.number,
                    title: pr.title,
                    state: pr.state,
                    html_url: pr.html_url,
                    merged: pr.merged,
                    author_login: pr.author_login,
                })
                .collect(),
        }
    }
}

// =============================================================================
// Version Introduction Info
// =============================================================================

/// Query parameters for version info endpoint.
#[derive(Debug, Deserialize)]
pub struct VersionInfoQuery {
    /// Current version to compare against (optional)
    pub current_version: Option<String>,
}

/// Get version introduction information for an exception fingerprint.
///
/// GET /projects/:id/github/version-info/:fingerprint
#[axum::debug_handler]
async fn get_version_info(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path((project_id, fingerprint)): Path<(Uuid, String)>,
    Query(query): Query<VersionInfoQuery>,
) -> Result<Json<VersionIntroductionInfo>> {
    // Validate fingerprint format before any processing
    validate_fingerprint(&fingerprint)?;

    let user_id = crate::api::extract_user_id(&headers)?;

    // Verify user has access to this project
    let project_exists: Option<Uuid> = sqlx::query_scalar(
        r#"
        SELECT p.id FROM projects p
        JOIN memberships om ON p.organization_id = om.organization_id AND om.status = 'active'
        WHERE p.id = $1 AND om.user_id = $2
        "#,
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    if project_exists.is_none() {
        return Err(AppError::NotFound("Project not found".to_string()));
    }

    // Get version introduction info from ClickHouse
    let info = get_version_introduction_info(
        state.clickhouse.as_ref(),
        &project_id.to_string(),
        &fingerprint,
        query.current_version.as_deref(),
    )
    .await
    .map_err(|e| {
        error!(
            error = %e,
            project_id = %project_id,
            fingerprint = %fingerprint,
            "Failed to get version introduction info"
        );
        AppError::Internal(anyhow::anyhow!("Failed to get version info"))
    })?;

    Ok(Json(info))
}

// =============================================================================
// GitHub Webhooks
// =============================================================================

/// GitHub webhook event types we handle.
#[derive(Debug, Clone, PartialEq, Eq)]
enum GitHubWebhookEvent {
    /// Installation was deleted (app uninstalled)
    InstallationDeleted,
    /// Repositories were added to an installation
    InstallationRepositoriesAdded,
    /// Repositories were removed from an installation
    InstallationRepositoriesRemoved,
    /// Unknown/unhandled event
    Unknown(String),
}

impl GitHubWebhookEvent {
    fn from_headers(event_type: &str, action: Option<&str>) -> Self {
        match (event_type, action) {
            ("installation", Some("deleted")) => Self::InstallationDeleted,
            ("installation_repositories", Some("added")) => Self::InstallationRepositoriesAdded,
            ("installation_repositories", Some("removed")) => Self::InstallationRepositoriesRemoved,
            _ => Self::Unknown(format!("{}.{}", event_type, action.unwrap_or("none"))),
        }
    }
}

/// Extract the real client IP from a webhook request, considering trusted proxies.
///
/// When the direct connection IP is from a trusted proxy (as configured in
/// `trusted_proxy_cidrs`), this function parses the `X-Forwarded-For` header
/// to get the real client IP. Otherwise, it returns the direct connection IP.
///
/// # Security
///
/// This function only trusts `X-Forwarded-For` when the connection comes from
/// a known proxy IP. This prevents attackers from spoofing their IP by sending
/// fake headers directly.
///
/// # Arguments
/// * `socket_ip` - The IP from the direct TCP connection
/// * `headers` - HTTP headers containing potential X-Forwarded-For
/// * `trusted_proxy_cidrs` - CIDR ranges of trusted proxy IPs
///
/// # Returns
/// The real client IP as a string
fn extract_real_client_ip(
    socket_ip: &str,
    headers: &HeaderMap,
    trusted_proxy_cidrs: &[String],
) -> String {
    // If no trusted proxies configured, always use socket IP
    if trusted_proxy_cidrs.is_empty() {
        return socket_ip.to_string();
    }

    // Parse the socket IP
    let socket_addr: IpAddr = match socket_ip.parse() {
        Ok(ip) => ip,
        Err(_) => return socket_ip.to_string(),
    };

    // Check if the connection comes from a trusted proxy
    let is_trusted_proxy = trusted_proxy_cidrs.iter().any(|cidr| {
        cidr.parse::<IpNetwork>()
            .map(|network| network.contains(socket_addr))
            .unwrap_or(false)
    });

    if !is_trusted_proxy {
        // Direct connection from untrusted IP - use socket IP
        return socket_ip.to_string();
    }

    // Connection is from a trusted proxy - parse X-Forwarded-For
    // X-Forwarded-For format: "client, proxy1, proxy2"
    // The leftmost IP is the original client
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(client_ip) = xff.split(',').next() {
            let client_ip = client_ip.trim();
            // Validate the extracted IP is a valid IP address
            if client_ip.parse::<IpAddr>().is_ok() {
                debug!(
                    socket_ip = %socket_ip,
                    forwarded_ip = %client_ip,
                    "Extracted real client IP from X-Forwarded-For via trusted proxy"
                );
                return client_ip.to_string();
            }
        }
    }

    // Fallback to socket IP if X-Forwarded-For is missing or invalid
    socket_ip.to_string()
}

/// Check if client IP is in the GitHub webhook allowlist.
///
/// This is a defense-in-depth measure. Signature verification is the primary
/// security control; IP allowlisting provides additional protection against
/// attackers who might have obtained the webhook secret.
///
/// # Proxy Configuration
///
/// If deployed behind a reverse proxy, you must configure `TRUSTED_PROXY_CIDRS`
/// to enable proper client IP detection from `X-Forwarded-For` headers.
/// Without this, the allowlist will check the proxy's IP, not GitHub's IP.
///
/// # Arguments
/// * `allowlist` - List of CIDR ranges to allow (e.g., "192.30.252.0/22")
/// * `client_ip` - The client's IP address as a string (already extracted considering proxies)
///
/// # Returns
/// * `Ok(())` if allowlist is empty (disabled) or IP matches a CIDR range
/// * `Err(AppError::Forbidden)` if IP is not in the allowlist
///
/// # Note
/// Get current GitHub webhook IPs from: https://api.github.com/meta (hooks field)
fn check_webhook_ip_allowlist(allowlist: &[String], client_ip: &str) -> Result<()> {
    // If allowlist is empty, IP allowlisting is disabled
    if allowlist.is_empty() {
        return Ok(());
    }

    let ip: IpAddr = client_ip.parse().map_err(|_| {
        warn!(client_ip = %client_ip, "Invalid client IP format in webhook request");
        AppError::BadRequest("Invalid client IP".to_string())
    })?;

    for cidr in allowlist {
        match cidr.parse::<IpNetwork>() {
            Ok(network) => {
                if network.contains(ip) {
                    debug!(client_ip = %client_ip, network = %cidr, "Webhook IP in allowlist");
                    return Ok(());
                }
            }
            Err(e) => {
                // Log malformed CIDR but continue checking other entries
                warn!(cidr = %cidr, error = %e, "Invalid CIDR in webhook IP allowlist");
            }
        }
    }

    warn!(client_ip = %client_ip, "GitHub webhook from IP not in allowlist");
    Err(AppError::Forbidden("IP not in allowlist".to_string()))
}

/// GitHub webhook payload for installation events.
#[derive(Debug, Deserialize)]
struct WebhookPayload {
    action: Option<String>,
    installation: Option<WebhookInstallation>,
    repositories_added: Option<Vec<WebhookRepository>>,
    repositories_removed: Option<Vec<WebhookRepository>>,
}

#[derive(Debug, Deserialize)]
struct WebhookInstallation {
    id: u64,
    account: Option<WebhookAccount>,
}

#[derive(Debug, Deserialize)]
struct WebhookAccount {
    login: String,
    #[serde(rename = "type")]
    account_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WebhookRepository {
    name: String,
    full_name: String,
    private: Option<bool>,
}

/// Handle GitHub webhook events.
///
/// POST /github/webhook
///
/// This endpoint receives webhook events from GitHub and updates the local
/// installation state accordingly. Events handled:
/// - `installation.deleted`: Remove the installation record
/// - `installation_repositories.added`: Add repos to installation
/// - `installation_repositories.removed`: Remove repos from installation
#[axum::debug_handler]
async fn handle_github_webhook(
    State(state): State<Arc<WatchState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode> {
    // Extract real client IP considering trusted proxies for allowlist checking
    let socket_ip = addr.ip().to_string();
    let client_ip = extract_real_client_ip(&socket_ip, &headers, &state.config.trusted_proxy_cidrs);

    // Check IP allowlist (defense-in-depth, disabled if allowlist is empty)
    check_webhook_ip_allowlist(&state.config.github_webhook_ip_allowlist, &client_ip)?;

    // Get webhook secret from config
    let webhook_secret = state
        .config
        .github_app_webhook_secret
        .as_ref()
        .ok_or_else(|| {
            error!("GitHub webhook received but GITHUB_APP_WEBHOOK_SECRET is not configured");
            AppError::Internal(anyhow::anyhow!("Webhook secret not configured"))
        })?;

    // Get signature from header
    let signature = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            warn!("GitHub webhook missing X-Hub-Signature-256 header");
            AppError::BadRequest("Missing signature header".to_string())
        })?;

    // Verify signature
    if !verify_webhook_signature(webhook_secret, &body, signature) {
        warn!("GitHub webhook signature verification failed");
        return Err(AppError::BadRequest("Invalid signature".to_string()));
    }

    // Get delivery ID for deduplication (GitHub may redeliver webhooks)
    // SECURITY: Require this header to prevent attackers from bypassing deduplication
    // by omitting it. GitHub always sends this header on legitimate webhooks.
    let delivery_id = headers
        .get("x-github-delivery")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            warn!("GitHub webhook missing X-GitHub-Delivery header");
            AppError::BadRequest("Missing delivery ID header".to_string())
        })?;

    // Check for duplicate delivery
    if is_duplicate_delivery(&state.redis, delivery_id).await? {
        debug!(delivery_id = %delivery_id, "Ignoring duplicate GitHub webhook delivery");
        return Ok(StatusCode::OK);
    }

    // Get event type from header
    let event_type = headers
        .get("x-github-event")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            warn!(delivery_id = %delivery_id, "GitHub webhook missing X-GitHub-Event header");
            AppError::BadRequest("Missing event type header".to_string())
        })?;

    // Parse payload
    let payload: WebhookPayload = serde_json::from_slice(&body).map_err(|e| {
        error!(
            error = %e,
            delivery_id = %delivery_id,
            event_type = %event_type,
            "Failed to parse GitHub webhook payload"
        );
        AppError::BadRequest("Invalid webhook payload".to_string())
    })?;

    let action = payload.action.as_deref();
    let event = GitHubWebhookEvent::from_headers(event_type, action);

    debug!(
        event_type = %event_type,
        action = ?action,
        "Received GitHub webhook"
    );

    match event {
        GitHubWebhookEvent::InstallationDeleted => {
            handle_installation_deleted(&state, &payload).await?;
        }
        GitHubWebhookEvent::InstallationRepositoriesAdded => {
            handle_repositories_changed(&state, &payload, true).await?;
        }
        GitHubWebhookEvent::InstallationRepositoriesRemoved => {
            handle_repositories_changed(&state, &payload, false).await?;
        }
        GitHubWebhookEvent::Unknown(ref event_name) => {
            debug!(event = %event_name, "Ignoring unhandled GitHub webhook event");
        }
    }

    Ok(StatusCode::OK)
}

/// Handle installation.deleted webhook event.
/// Removes the installation record from the database.
async fn handle_installation_deleted(state: &WatchState, payload: &WebhookPayload) -> Result<()> {
    let installation = payload.installation.as_ref().ok_or_else(|| {
        AppError::BadRequest("Missing installation in webhook payload".to_string())
    })?;

    let installation_id = installation.id as i64;

    // Delete the installation record
    let result = sqlx::query("DELETE FROM github_installations WHERE installation_id = $1")
        .bind(installation_id)
        .execute(state.db.as_ref())
        .await?;

    if result.rows_affected() > 0 {
        info!(
            installation_id = installation_id,
            account_login = ?installation.account.as_ref().map(|a| &a.login),
            "GitHub installation deleted via webhook"
        );
    } else {
        debug!(
            installation_id = installation_id,
            "GitHub installation already deleted or not found"
        );
    }

    Ok(())
}

/// Handle installation_repositories.added/removed webhook events.
/// Updates the repositories list for the installation using atomic JSONB operations
/// to prevent race conditions from concurrent webhook deliveries.
async fn handle_repositories_changed(
    state: &WatchState,
    payload: &WebhookPayload,
    is_add: bool,
) -> Result<()> {
    let installation = payload.installation.as_ref().ok_or_else(|| {
        AppError::BadRequest("Missing installation in webhook payload".to_string())
    })?;

    let installation_id = installation.id as i64;

    if is_add {
        // Add new repositories atomically using JSONB concatenation
        // The query filters out duplicates by checking if full_name already exists
        if let Some(added) = &payload.repositories_added {
            if added.is_empty() {
                return Ok(());
            }

            let repos_to_add: Vec<serde_json::Value> = added
                .iter()
                .map(|repo| {
                    serde_json::json!({
                        "name": repo.name,
                        "full_name": repo.full_name,
                        "private": repo.private.unwrap_or(false),
                    })
                })
                .collect();

            // Atomic update: merge new repos, avoiding duplicates by full_name
            // Uses DISTINCT ON with priority to keep existing repos over new duplicates
            let result = sqlx::query(
                r#"
                UPDATE github_installations
                SET repositories = (
                    SELECT COALESCE(jsonb_agg(repo), '[]'::jsonb)
                    FROM (
                        SELECT DISTINCT ON (repo->>'full_name') repo
                        FROM (
                            -- Existing repos (priority 1 = keep on conflict)
                            SELECT jsonb_array_elements(repositories) AS repo, 1 AS priority
                            UNION ALL
                            -- New repos (priority 2 = only add if no duplicate)
                            SELECT jsonb_array_elements($1::jsonb) AS repo, 2 AS priority
                        ) AS all_repos
                        ORDER BY repo->>'full_name', priority
                    ) AS deduped
                ),
                updated_at = NOW()
                WHERE installation_id = $2
                "#,
            )
            .bind(serde_json::Value::Array(repos_to_add))
            .bind(installation_id)
            .execute(state.db.as_ref())
            .await?;

            if result.rows_affected() > 0 {
                info!(
                    installation_id = installation_id,
                    added_count = added.len(),
                    "Added repositories to GitHub installation via webhook"
                );
            } else {
                debug!(
                    installation_id = installation_id,
                    "Installation not found for repository add webhook"
                );
            }
        }
    } else {
        // Remove repositories atomically using JSONB filtering
        if let Some(removed) = &payload.repositories_removed {
            if removed.is_empty() {
                return Ok(());
            }

            let names_to_remove: Vec<String> =
                removed.iter().map(|r| r.full_name.clone()).collect();

            // Atomic update: filter out repos by full_name
            let result = sqlx::query(
                r#"
                UPDATE github_installations
                SET repositories = (
                    SELECT COALESCE(jsonb_agg(repo), '[]'::jsonb)
                    FROM jsonb_array_elements(repositories) AS repo
                    WHERE repo->>'full_name' != ALL($1)
                ),
                updated_at = NOW()
                WHERE installation_id = $2
                "#,
            )
            .bind(&names_to_remove)
            .bind(installation_id)
            .execute(state.db.as_ref())
            .await?;

            if result.rows_affected() > 0 {
                info!(
                    installation_id = installation_id,
                    removed_count = removed.len(),
                    "Removed repositories from GitHub installation via webhook"
                );
            } else {
                debug!(
                    installation_id = installation_id,
                    "Installation not found for repository remove webhook"
                );
            }
        }
    }

    Ok(())
}
