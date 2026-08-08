//! Authentication module for JWT-based stateless authentication
//!
//! # JWT Stateless Design Trade-off
//!
//! This module implements a **stateless JWT authentication** strategy, which means:
//!
//! - **No database check per request**: JWT signature and expiration are validated locally
//! - **Revoked tokens remain valid until expiry**: This is a known trade-off
//! - **Configurable token lifetime**: Default 24 hours, configurable via `JWT_EXPIRATION_HOURS`
//!
//! ## Why Stateless?
//!
//! 1. **Performance**: Avoids a database query on every authenticated API request
//! 2. **Scalability**: Tokens can be validated by any server without shared state
//! 3. **Reduced latency**: Faster request processing for the common case
//!
//! ## Security Trade-offs
//!
//! The main trade-off is that **JWTs remain valid even after logout** until they expire.
//! This means:
//! - If a user logs out, their token can still be used until expiry
//! - If an account is compromised and password is changed, old tokens still work
//!
//! ## Configuration
//!
//! - `JWT_EXPIRATION_HOURS`: Token lifetime in hours (default: 24, min: 1, max: 168)
//!   - For high-security environments, consider 1-4 hours
//!   - For better UX, use up to 168 hours (7 days) with session binding enabled
//!
//! ## Mitigations
//!
//! 1. **Configurable token lifetime**: Use shorter lifetimes for high-security environments
//! 2. **Session tracking in database**: Sessions are tracked for audit and revocation
//! 3. **`extract_user_id_with_session_check`**: Use this for security-sensitive operations
//!
//! ## When to Use Each Function
//!
//! | Function | Use Case | Database Query |
//! |----------|----------|----------------|
//! | `extract_user_id` | Normal API operations | No |
//! | `extract_user_id_with_session_check` | Security-sensitive operations | Yes |
//!
//! ### Use `extract_user_id` for:
//! - Reading data (lists, views, dashboards)
//! - Creating resources (new projects, alerts)
//! - Non-sensitive updates
//!
//! ### Use `extract_user_id_with_session_check` for:
//! - Password changes
//! - Email changes
//! - Account deletion
//! - Accessing/exporting PII
//! - Generating new API keys
//! - Any action where immediate revocation is critical

use axum::http::header::SET_COOKIE;
use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
};
use chrono::Utc;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use crate::app_state::{AuthContext, RedisPool};
use crate::db::DbPool;
use crate::error::{AppError, Result};
use crate::models::Project;
use crate::rate_limit::{check_authenticated_rate_limit, RateLimitType};

/// Minimum JWT secret length (256 bits / 32 bytes recommended for HS256)
const MIN_JWT_SECRET_LENGTH: usize = 32;

/// Create a secure JWT validation configuration.
fn create_jwt_validation() -> Validation {
    let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.set_required_spec_claims(&["exp", "sub", "iat"]);
    validation
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    /// Subject - user_id
    pub sub: String,
    /// Expiration time (Unix timestamp)
    pub exp: usize,
    /// Issued at time (Unix timestamp)
    pub iat: usize,
    /// JWT ID - unique identifier for token revocation
    pub jti: String,
    /// Whether this is an SSO token (requires session validation)
    #[serde(default)]
    pub sso: bool,
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
    pub token: String,
    pub user: UserResponse,
}

#[derive(Debug, Serialize, FromRow)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
    pub created_at: chrono::DateTime<Utc>,
    #[sqlx(default)]
    pub is_platform_admin: bool,
    #[sqlx(default)]
    pub is_approved: bool,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_role: Option<String>,
}

/// Response type that includes both JSON body and Set-Cookie header
pub struct AuthResponseWithCookie {
    pub body: AuthResponse,
    pub cookie: String,
}

impl IntoResponse for AuthResponseWithCookie {
    fn into_response(self) -> Response {
        let json_body = Json(self.body);
        let mut response = json_body.into_response();

        if let Ok(cookie_value) = self.cookie.parse() {
            response.headers_mut().insert(SET_COOKIE, cookie_value);
        }

        response
    }
}

/// Validate JWT secret meets minimum security requirements
pub fn validate_jwt_secret(secret: &str) -> Result<()> {
    if secret.len() < MIN_JWT_SECRET_LENGTH {
        return Err(AppError::Internal(anyhow::anyhow!(
            "JWT_SECRET must be at least {} characters for security. Current length: {}. \
             Generate a secure secret with: openssl rand -base64 32",
            MIN_JWT_SECRET_LENGTH,
            secret.len()
        )));
    }

    let unique_chars: std::collections::HashSet<char> = secret.chars().collect();
    let entropy_ratio = unique_chars.len() as f64 / secret.len() as f64;

    if entropy_ratio < 0.25 {
        tracing::warn!(
            "SECURITY WARNING: JWT_SECRET appears to have low entropy ({:.0}% unique characters). \
             Generate a cryptographically random secret with: openssl rand -base64 32",
            entropy_ratio * 100.0
        );
    }

    let secret_lower = secret.to_lowercase();
    let weak_patterns = [
        "secret",
        "password",
        "changeme",
        "default",
        "example",
        "test",
        "demo",
        "development",
        "production",
    ];

    for pattern in &weak_patterns {
        if secret_lower.contains(pattern) {
            tracing::warn!(
                "SECURITY WARNING: JWT_SECRET contains weak pattern '{}'. \
                 Generate a cryptographically random secret with: openssl rand -base64 32",
                pattern
            );
            break;
        }
    }

    Ok(())
}

pub fn create_jwt(user_id: &Uuid, secret: &str, expiration_hours: i64) -> Result<String> {
    let now = chrono::Utc::now();
    let exp = (now + chrono::Duration::hours(expiration_hours)).timestamp() as usize;
    let iat = now.timestamp() as usize;

    let jti = Uuid::new_v4().to_string();

    let claims = Claims {
        sub: user_id.to_string(),
        exp,
        iat,
        jti,
        sso: false,
    };

    let header = Header {
        alg: jsonwebtoken::Algorithm::HS256,
        ..Default::default()
    };

    encode(&header, &claims, &EncodingKey::from_secret(secret.as_ref()))
        .map_err(|e| AppError::Internal(anyhow::anyhow!("JWT encoding failed: {}", e)))
}

/// Extract JWT token string from request headers.
///
/// Tries Authorization header first (Bearer token), then falls back to cookie.
fn extract_token_from_headers(headers: &axum::http::HeaderMap) -> Result<String> {
    if let Some(auth_header) = headers.get("Authorization").and_then(|h| h.to_str().ok()) {
        return auth_header
            .strip_prefix("Bearer ")
            .map(|t| t.to_string())
            .ok_or_else(|| AppError::Auth("Invalid Authorization header format".to_string()));
    }

    let cookie_header = headers
        .get("cookie")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| AppError::Auth("Missing Authorization header or cookie".to_string()))?;

    let token_cookie = cookie_header
        .split("; ")
        .find(|c| c.starts_with("token="))
        .ok_or_else(|| AppError::Auth("Missing token cookie".to_string()))?;

    token_cookie
        .strip_prefix("token=")
        .map(|t| {
            urlencoding::decode(t)
                .map(|decoded| decoded.to_string())
                .unwrap_or_else(|_| t.to_string())
        })
        .ok_or_else(|| AppError::Auth("Invalid token cookie format".to_string()))
}

/// Decode and validate a JWT token, returning the claims.
fn decode_and_validate_token(token: &str, secret: &str) -> Result<Claims> {
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_ref()),
        &create_jwt_validation(),
    )
    .map_err(|e| {
        tracing::warn!("Token decode failed: {}", e);
        AppError::Auth("Invalid token".to_string())
    })?;

    Ok(token_data.claims)
}

/// Extract user ID from JWT claims.
fn extract_user_id_from_claims(claims: &Claims) -> Result<Uuid> {
    Uuid::parse_str(&claims.sub).map_err(|e| {
        tracing::warn!(
            "Failed to parse user_id from token sub claim '{}': {}",
            claims.sub,
            e
        );
        AppError::Auth("Invalid user ID in token".to_string())
    })
}

/// Extract user ID from JWT token in request headers (stateless -- no DB check).
pub fn extract_user_id(headers: &axum::http::HeaderMap, secret: &str) -> Result<Uuid> {
    let token = extract_token_from_headers(headers)?;
    let claims = decode_and_validate_token(&token, secret)?;
    extract_user_id_from_claims(&claims)
}

/// Extract user ID from JWT and verify the session has not been revoked.
/// Use for security-sensitive operations (password change, account deletion, etc.).
pub async fn extract_user_id_with_session_check(
    headers: &axum::http::HeaderMap,
    db: &DbPool,
    config: &crate::config::Config,
) -> Result<Uuid> {
    let token = extract_token_from_headers(headers)?;
    let claims = decode_and_validate_token(&token, &config.jwt_secret)?;
    let user_id = extract_user_id_from_claims(&claims)?;

    if claims.sso {
        let session_token_hash = &claims.jti;

        if session_token_hash.is_empty() {
            tracing::warn!("SSO token missing session hash (jti) for user {}", user_id);
            return Err(AppError::Auth("Invalid SSO session".to_string()));
        }

        let session_info: Option<SessionBindingInfo> = sqlx::query_as(
            r#"
            SELECT
                (revoked_at IS NULL AND expires_at > NOW()) as valid,
                ip_address::text as ip_address,
                user_agent
            FROM sso_sessions
            WHERE session_token_hash = $1
            "#,
        )
        .bind(session_token_hash)
        .fetch_optional(db)
        .await
        .map_err(|e| {
            tracing::error!("Failed to check session revocation: {}", e);
            AppError::Internal(anyhow::anyhow!("Session validation failed"))
        })?;

        match session_info {
            Some(info) if info.valid => {
                validate_session_binding(headers, &info, config)?;
            }
            Some(_) => {
                tracing::warn!("SSO session revoked or expired for user {}", user_id);
                return Err(AppError::Auth("Session has been revoked".to_string()));
            }
            None => {
                tracing::warn!("SSO token has no matching session for user {}", user_id);
                return Err(AppError::Auth("Invalid SSO session".to_string()));
            }
        }
    }

    Ok(user_id)
}

/// Session binding information from the database
#[derive(sqlx::FromRow)]
struct SessionBindingInfo {
    valid: bool,
    ip_address: Option<String>,
    user_agent: Option<String>,
}

/// Validate session binding (IP and/or user-agent) based on config.
fn validate_session_binding(
    headers: &axum::http::HeaderMap,
    session_info: &SessionBindingInfo,
    config: &crate::config::Config,
) -> Result<()> {
    if config.session_ip_binding_enabled {
        if let Some(session_ip) = &session_info.ip_address {
            let current_ip = extract_client_ip_from_headers(headers);
            if let Some(current) = current_ip {
                if &current != session_ip {
                    tracing::warn!(
                        "Session IP mismatch: expected {}, got {}",
                        session_ip,
                        current
                    );
                    return Err(AppError::Auth(
                        "Session expired due to IP address change".to_string(),
                    ));
                }
            }
        }
    }

    if config.session_user_agent_binding_enabled {
        if let Some(session_ua) = &session_info.user_agent {
            let current_ua = headers
                .get("user-agent")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string());

            if let Some(current) = current_ua {
                if &current != session_ua {
                    tracing::warn!("Session user-agent mismatch for security-bound session");
                    return Err(AppError::Auth(
                        "Session expired due to browser change".to_string(),
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Extract client IP from request headers.
fn extract_client_ip_from_headers(headers: &axum::http::HeaderMap) -> Option<String> {
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|h| h.to_str().ok()) {
        if let Some(client_ip) = xff.split(',').next() {
            return Some(client_ip.trim().to_string());
        }
    }

    if let Some(real_ip) = headers.get("x-real-ip").and_then(|h| h.to_str().ok()) {
        return Some(real_ip.to_string());
    }

    None
}

/// Combined authentication, approval gate, and rate limiting helper.
/// Extracts user_id from JWT, verifies the user is approved (cached),
/// then checks rate limits.
pub async fn authenticate_request(
    headers: &HeaderMap,
    state: &impl AuthContext,
    rate_limit_type: RateLimitType,
) -> Result<Uuid> {
    let user_id = extract_user_id(headers, &state.config().jwt_secret)?;
    check_user_approved(state.db(), state.redis(), user_id).await?;
    check_authenticated_rate_limit(state.redis(), &user_id, rate_limit_type, state.config())
        .await?;
    Ok(user_id)
}

/// Authenticated caller identity — either a user (JWT) or a project API key.
#[derive(Debug, Clone)]
pub enum AuthIdentity {
    User(Uuid),
    ApiKey { project_id: Uuid },
}

/// Authenticate via JWT first; if that fails, fall back to project API key.
/// This allows MCP / SDK callers (which send `Authorization: Bearer <api_key>`)
/// to access website routes that were previously JWT-only.
pub async fn authenticate_request_or_api_key(
    headers: &HeaderMap,
    state: &impl AuthContext,
    rate_limit_type: RateLimitType,
) -> Result<AuthIdentity> {
    if let Ok(user_id) = authenticate_request(headers, state, rate_limit_type).await {
        return Ok(AuthIdentity::User(user_id));
    }

    let token = extract_token_from_headers(headers)?;
    let project_id =
        crate::utils::validate_project_key_cached(state.redis(), state.db(), &token).await?;
    Ok(AuthIdentity::ApiKey { project_id })
}

/// Authenticate (JWT or API key) and verify access to a specific project.
///
/// For JWT callers: validates the token and checks org membership via
/// `verify_project_access`. Returns the real user ID.
///
/// For API key callers: validates the key and checks the key's project_id
/// matches the requested project. Returns `Uuid::nil()` (no user identity).
pub async fn authenticate_and_verify_project(
    headers: &HeaderMap,
    state: &impl AuthContext,
    db: &DbPool,
    project_id: Uuid,
    rate_limit_type: RateLimitType,
) -> Result<Uuid> {
    match authenticate_request_or_api_key(headers, state, rate_limit_type).await? {
        AuthIdentity::User(user_id) => {
            verify_project_access(db, project_id, user_id).await?;
            Ok(user_id)
        }
        AuthIdentity::ApiKey {
            project_id: key_pid,
        } => {
            if key_pid != project_id {
                return Err(AppError::Auth(
                    "API key does not belong to this project".into(),
                ));
            }
            Ok(Uuid::nil())
        }
    }
}

const APPROVAL_CACHE_TTL_SECONDS: u64 = 60;

/// Verify that a user account is approved (or is a platform admin).
/// Result is cached in Redis for 60 seconds to avoid a DB query per request.
pub async fn check_user_approved(
    db: &std::sync::Arc<DbPool>,
    redis: &std::sync::Arc<RedisPool>,
    user_id: Uuid,
) -> Result<()> {
    use redis::AsyncCommands;

    let cache_key = format!("user:approved:{}", user_id);

    // Try Redis cache first
    if let Ok(mut conn) = redis.get().await {
        if let Ok(Some(cached)) = conn.get::<_, Option<String>>(&cache_key).await {
            return match cached.as_str() {
                "1" => Ok(()),
                _ => Err(AppError::Forbidden(
                    "Your account is pending approval. Please contact the administrator."
                        .to_string(),
                )),
            };
        }
    }

    // Cache miss -- query DB
    #[derive(sqlx::FromRow)]
    struct ApprovalRow {
        is_approved: bool,
        is_platform_admin: bool,
    }

    let row = sqlx::query_as::<_, ApprovalRow>(
        "SELECT is_approved, is_platform_admin FROM users WHERE id = $1",
    )
    .bind(user_id)
    .fetch_optional(db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to check user approval: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    let allowed = match row {
        Some(r) => r.is_approved || r.is_platform_admin,
        None => false,
    };

    // Cache the result
    if let Ok(mut conn) = redis.get().await {
        let val = if allowed { "1" } else { "0" };
        let _: std::result::Result<(), _> = conn
            .set_ex(&cache_key, val, APPROVAL_CACHE_TTL_SECONDS)
            .await;
    }

    if allowed {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "Your account is pending approval. Please contact the administrator.".to_string(),
        ))
    }
}

/// Invalidate the approval cache for a user (call after approve/disable).
pub async fn invalidate_approval_cache(redis: &std::sync::Arc<RedisPool>, user_id: Uuid) {
    use redis::AsyncCommands;
    let cache_key = format!("user:approved:{}", user_id);
    if let Ok(mut conn) = redis.get().await {
        let _: std::result::Result<(), _> = conn.del(&cache_key).await;
    }
}

/// Verifies that a user has access to a project via organization membership.
pub async fn verify_project_access(
    db: &DbPool,
    project_id: Uuid,
    user_id: Uuid,
) -> Result<Project> {
    sqlx::query_as::<_, Project>(
        r#"SELECT DISTINCT p.* FROM projects p
        INNER JOIN memberships m ON p.organization_id = m.organization_id
        WHERE p.id = $1 AND m.user_id = $2 AND m.status = 'active'"#,
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| AppError::NotFound("Project not found or access denied".to_string()))
}

/// Verifies that a user has access to a project and returns the project along with
/// the user's role (e.g. "owner", "admin", "member", "viewer").
pub async fn verify_project_access_with_role(
    db: &DbPool,
    project_id: Uuid,
    user_id: Uuid,
) -> Result<(Project, String)> {
    #[derive(sqlx::FromRow)]
    struct ProjectWithRole {
        id: Uuid,
        organization_id: Uuid,
        name: String,
        slug: String,
        created_by: Option<Uuid>,
        created_at: chrono::DateTime<chrono::Utc>,
        settings: Option<serde_json::Value>,
        github_repo_url: Option<String>,
        role: String,
    }

    let row = sqlx::query_as::<_, ProjectWithRole>(
        r#"SELECT DISTINCT p.id, p.organization_id, p.name, p.slug, p.created_by, p.created_at,
                  p.settings, p.github_repo_url, m.role
           FROM projects p
           INNER JOIN memberships m ON p.organization_id = m.organization_id
           WHERE p.id = $1 AND m.user_id = $2 AND m.status = 'active'"#,
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| AppError::NotFound("Project not found or access denied".to_string()))?;

    let project = Project {
        id: row.id,
        organization_id: row.organization_id,
        name: row.name,
        slug: row.slug,
        created_by: row.created_by,
        created_at: row.created_at,
        settings: row.settings,
        github_repo_url: row.github_repo_url,
    };

    Ok((project, row.role))
}

/// Create a secure Set-Cookie header value for JWT tokens
pub fn create_secure_cookie(
    token: &str,
    is_production: bool,
    cookie_domain: Option<&str>,
    expiration_hours: i64,
) -> String {
    let max_age = expiration_hours * 60 * 60;
    let secure = if is_production { "; Secure" } else { "" };
    let domain = cookie_domain
        .map(|d| format!("; Domain={}", d))
        .unwrap_or_default();

    format!(
        "token={}; HttpOnly; SameSite=Lax; Path=/; Max-Age={}{}{}",
        token, max_age, secure, domain
    )
}

/// Create a Set-Cookie header to clear the auth cookie
#[allow(dead_code)]
pub fn create_logout_cookie() -> String {
    "token=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0".to_string()
}

// ============================================================================
// Axum Extractors for Authentication
// ============================================================================

/// Authenticated user information.
#[derive(Debug, Clone, Copy)]
pub struct AuthenticatedUser {
    pub user_id: Uuid,
}

/// Organization context with user and organization IDs.
#[derive(Debug, Clone, Copy)]
pub struct OrgContext {
    pub user_id: Uuid,
    pub organization_id: Uuid,
}

/// Admin organization context - verified admin/owner of the organization.
#[derive(Debug, Clone, Copy)]
pub struct AdminOrgContext {
    pub user_id: Uuid,
    pub organization_id: Uuid,
}

/// Helper function to extract authenticated user from headers.
pub async fn extract_authenticated_user(
    headers: &HeaderMap,
    state: &impl AuthContext,
) -> std::result::Result<AuthenticatedUser, (StatusCode, Json<serde_json::Value>)> {
    match authenticate_request(headers, state, RateLimitType::Crud).await {
        Ok(user_id) => Ok(AuthenticatedUser { user_id }),
        Err(_) => Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "success": false,
                "error": "Authentication required"
            })),
        )),
    }
}

/// Helper function to extract organization context from headers.
pub async fn extract_org_context(
    headers: &HeaderMap,
    state: &impl AuthContext,
) -> std::result::Result<OrgContext, (StatusCode, Json<serde_json::Value>)> {
    let user_id = authenticate_request(headers, state, RateLimitType::Crud)
        .await
        .map_err(|_| {
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({
                    "success": false,
                    "error": "Authentication required"
                })),
            )
        })?;

    let organization_id = crate::authorization::get_user_organization(state.db().as_ref(), user_id)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "success": false,
                    "error": "Failed to get organization"
                })),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "success": false,
                    "error": "User not associated with an organization"
                })),
            )
        })?;

    Ok(OrgContext {
        user_id,
        organization_id,
    })
}

/// Helper function to extract admin organization context from headers.
pub async fn extract_admin_org_context(
    headers: &HeaderMap,
    state: &impl AuthContext,
) -> std::result::Result<AdminOrgContext, (StatusCode, Json<serde_json::Value>)> {
    let OrgContext {
        user_id,
        organization_id,
    } = extract_org_context(headers, state).await?;

    let is_admin =
        crate::authorization::is_org_admin(state.db().as_ref(), user_id, organization_id)
            .await
            .map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "success": false,
                        "error": "Failed to verify permissions"
                    })),
                )
            })?;

    if !is_admin {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "success": false,
                "error": "Admin access required"
            })),
        ));
    }

    Ok(AdminOrgContext {
        user_id,
        organization_id,
    })
}

// ============================================================================
// AuthUser Extractor for Project-Scoped Endpoints
// ============================================================================

/// Authenticated user with project context.
#[derive(Debug, Clone)]
pub struct AuthUser {
    pub user_id: Uuid,
    project_id: Uuid,
}

impl AuthUser {
    pub fn current_project_id(&self) -> Result<Uuid> {
        Ok(self.project_id)
    }

    pub fn user_id(&self) -> Uuid {
        self.user_id
    }
}

/// Helper function to extract AuthUser from request headers.
pub async fn extract_auth_user(headers: &HeaderMap, state: &impl AuthContext) -> Result<AuthUser> {
    let user_id = authenticate_request(headers, state, RateLimitType::Crud).await?;

    let project_id_str = headers
        .get("x-project-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AppError::Validation("Missing x-project-id header".to_string()))?;

    let project_id: Uuid = project_id_str.parse().map_err(|_| {
        AppError::Validation("Invalid project_id format - must be a valid UUID".to_string())
    })?;

    verify_project_access(state.db(), project_id, user_id).await?;

    Ok(AuthUser {
        user_id,
        project_id,
    })
}
