//! Website auth routes: signup, login, get_me
//!
//! These routes are owned by the website since it is the single
//! identity/authentication service. Watch, Flow, and Pond do not
//! serve auth endpoints -- they verify identity by calling the website.

use axum::{
    extract::{ConnectInfo, Extension, State},
    http::HeaderMap,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::app_state::WebsiteState;
use crate::audit::{AuditEventBuilder, AuditEventType};
use reiver_core::auth::{
    self, create_jwt, create_secure_cookie, AuthResponse, AuthResponseWithCookie, UserResponse,
};
use reiver_core::db::DbPool;
use reiver_core::error::{AppError, Result};
use reiver_core::rate_limit::{check_authenticated_rate_limit, RateLimitType};

#[derive(Debug, Deserialize)]
struct SignupRequest {
    email: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
}

pub fn create_auth_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/signup", post(signup))
        .route("/login", post(login))
        .route("/me", get(get_me))
        .route("/validate-key", get(validate_key))
}

async fn signup(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(payload): Json<SignupRequest>,
) -> Result<AuthResponseWithCookie> {
    if !state.config.allow_signup || !state.config.allow_password_login {
        return Err(AppError::Auth("Registration is disabled. Please use an OAuth provider (Google, GitHub, or Microsoft).".to_string()));
    }

    let client_ip = reiver_core::rate_limit::extract_client_ip(&addr);
    reiver_core::rate_limit::check_unauthenticated_rate_limit(
        &state.redis,
        &client_ip,
        "signup",
    )
    .await?;

    let existing = sqlx::query_as::<_, UserResponse>(
        "SELECT id, email, created_at, is_platform_admin, is_approved FROM users WHERE email = $1",
    )
    .bind(&payload.email)
    .fetch_optional(&*db)
    .await?;

    if existing.is_some() {
        return Err(AppError::Validation("Email already registered".to_string()));
    }

    let password_hash = bcrypt::hash(&payload.password, bcrypt::DEFAULT_COST)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Password hashing failed: {}", e)))?;

    // Self-serve registration only (not SCIM/SSO — see platform_settings docs).
    let is_approved = crate::platform_settings::self_serve_signup_is_approved(&db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let user = sqlx::query_as::<_, UserResponse>(
        "INSERT INTO users (email, password_hash, is_approved) VALUES ($1, $2, $3) RETURNING id, email, created_at, is_platform_admin, is_approved"
    )
    .bind(&payload.email)
    .bind(&password_hash)
    .bind(is_approved)
    .fetch_one(&*db)
    .await?;

    let _ = AuditEventBuilder::new(AuditEventType::UserCreated)
        .user(user.id)
        .details(serde_json::json!({
            "email": &payload.email,
            "is_approved": is_approved,
            "method": "signup"
        }))
        .success()
        .log(&state.clickhouse)
        .await;

    if let Some(ref mailer) = state.email {
        let first_name = payload
            .email
            .split('@')
            .next()
            .unwrap_or("there")
            .to_string();
        let mailer = mailer.clone();
        let to = payload.email.clone();
        tokio::spawn(async move {
            if let Err(e) = mailer
                .send_welcome(&to, reiver_core::email::WelcomeVars { first_name })
                .await
            {
                tracing::warn!("Failed to send welcome email to {}: {}", to, e);
            }
        });
    }

    let token = create_jwt(
        &user.id,
        &state.config.jwt_secret,
        state.config.jwt_expiration_hours,
    )?;

    let is_production = std::env::var("ENVIRONMENT")
        .map(|e| e.to_lowercase() == "production")
        .unwrap_or(false);

    let cookie = create_secure_cookie(
        &token,
        is_production,
        state.config.cookie_domain.as_deref(),
        state.config.jwt_expiration_hours,
    );

    Ok(AuthResponseWithCookie {
        body: AuthResponse { token, user },
        cookie,
    })
}

async fn login(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(payload): Json<LoginRequest>,
) -> Result<AuthResponseWithCookie> {
    if !state.config.allow_password_login {
        return Err(AppError::Auth("Password login is disabled. Please use an OAuth provider (Google, GitHub, or Microsoft).".to_string()));
    }

    let client_ip = reiver_core::rate_limit::extract_client_ip(&addr);
    reiver_core::rate_limit::check_unauthenticated_rate_limit(&state.redis, &client_ip, "login")
        .await?;

    #[derive(sqlx::FromRow)]
    struct UserWithPassword {
        id: uuid::Uuid,
        email: String,
        password_hash: String,
        created_at: chrono::DateTime<chrono::Utc>,
        is_platform_admin: bool,
        is_approved: bool,
    }

    let user = sqlx::query_as::<_, UserWithPassword>(
        "SELECT id, email, password_hash, created_at, is_platform_admin, is_approved FROM users WHERE email = $1"
    )
    .bind(&payload.email)
    .fetch_optional(&*db)
    .await?
    .ok_or_else(|| AppError::Auth("Invalid credentials".to_string()))?;

    let valid = bcrypt::verify(&payload.password, &user.password_hash)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Password verification failed: {}", e)))?;

    if !valid {
        return Err(AppError::Auth("Invalid credentials".to_string()));
    }

    let token = create_jwt(
        &user.id,
        &state.config.jwt_secret,
        state.config.jwt_expiration_hours,
    )?;
    let user_response = UserResponse {
        id: user.id,
        email: user.email,
        created_at: user.created_at,
        is_platform_admin: user.is_platform_admin,
        is_approved: user.is_approved,
        org_role: None,
    };

    let is_production = std::env::var("ENVIRONMENT")
        .map(|e| e.to_lowercase() == "production")
        .unwrap_or(false);

    let cookie = create_secure_cookie(
        &token,
        is_production,
        state.config.cookie_domain.as_deref(),
        state.config.jwt_expiration_hours,
    );

    Ok(AuthResponseWithCookie {
        body: AuthResponse {
            token,
            user: user_response,
        },
        cookie,
    })
}

/// GET /api/auth/me
/// Intentionally skips the approval gate so the frontend can detect
/// `is_approved = false` and show a "pending approval" screen.
async fn get_me(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    auth_header: HeaderMap,
) -> Result<Json<UserResponse>> {
    let user_id = auth::extract_user_id(&auth_header, &state.config.jwt_secret)?;

    check_authenticated_rate_limit(&state.redis, &user_id, RateLimitType::Crud, &state.config)
        .await?;

    tracing::debug!("get_me: Looking up user");

    let user = sqlx::query_as::<_, UserResponse>(
        r#"SELECT u.id, u.email, u.created_at, u.is_platform_admin, u.is_approved,
                  m.role AS org_role
           FROM users u
           LEFT JOIN memberships m ON m.user_id = u.id AND m.status = 'active'
           WHERE u.id = $1
           LIMIT 1"#,
    )
    .bind(user_id)
    .fetch_optional(&*db)
    .await
    .map_err(|e| {
        tracing::error!("Database error in get_me for user_id {}: {}", user_id, e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    match user {
        Some(user) => {
            tracing::debug!("get_me: Found user with id: {}", user_id);
            Ok(Json(user))
        }
        None => {
            tracing::debug!("get_me: User not found for token subject");
            Err(AppError::NotFound("User not found".to_string()))
        }
    }
}

/// Validates a project API key and returns the resolved project_id, key_id, scopes, and key_type.
/// Used by the MCP server to authenticate on connection.
async fn validate_key(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    auth_header: HeaderMap,
) -> Result<Json<ValidateKeyResponse>> {
    let api_key = auth_header
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .filter(|k| !k.is_empty())
        .ok_or_else(|| AppError::Auth("Missing or invalid Bearer token".to_string()))?;

    let project_id =
        crate::utils::validate_project_key_cached(&state.redis, state.db.as_ref(), api_key).await?;

    #[derive(sqlx::FromRow)]
    struct KeyRow {
        id: uuid::Uuid,
        scopes: serde_json::Value,
        key_type: String,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
        key_prefix: Option<String>,
        label: Option<String>,
        created_by: Option<uuid::Uuid>,
        organization_id: uuid::Uuid,
    }

    let key_hash = crate::utils::hash_api_key(api_key);

    let key_row: KeyRow = sqlx::query_as(
        "SELECT pk.id, pk.scopes, pk.key_type, pk.expires_at, pk.key_prefix, pk.label, pk.created_by, p.organization_id
         FROM project_keys pk
         JOIN projects p ON p.id = pk.project_id
         WHERE pk.key_hash = $1"
    )
    .bind(&key_hash)
    .fetch_one(&*db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to resolve key info: {}", e)))?;

    if let Some(exp) = key_row.expires_at {
        if exp < chrono::Utc::now() {
            return Err(AppError::Auth("API key has expired".to_string()));
        }
    }

    let scopes: Vec<String> = serde_json::from_value(key_row.scopes).unwrap_or_default();

    Ok(Json(ValidateKeyResponse {
        project_id,
        organization_id: key_row.organization_id,
        key_id: key_row.id,
        scopes,
        key_type: key_row.key_type,
        key_prefix: key_row.key_prefix.unwrap_or_default(),
        label: key_row.label.unwrap_or_default(),
        created_by: key_row.created_by,
    }))
}

#[derive(serde::Serialize)]
struct ValidateKeyResponse {
    project_id: uuid::Uuid,
    organization_id: uuid::Uuid,
    key_id: uuid::Uuid,
    scopes: Vec<String>,
    key_type: String,
    key_prefix: String,
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_by: Option<uuid::Uuid>,
}
