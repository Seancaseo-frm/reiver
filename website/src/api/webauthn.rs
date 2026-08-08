//! WebAuthn API
//!
//! Endpoints for WebAuthn (FIDO2) security key registration and authentication.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{delete, get, post},
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;
use webauthn_rs::prelude::*;

use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::auth::extract_user_id;
use crate::error::{AppError, Result};

pub fn create_webauthn_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        // Passwordless login (primary auth - no JWT required)
        .route("/login/start", post(start_passwordless_login))
        .route("/login/finish", post(finish_passwordless_login))
        // Registration (requires existing JWT)
        .route("/register/start", post(start_registration))
        .route("/register/finish", post(finish_registration))
        // MFA authentication (requires existing JWT for step-up)
        .route("/authenticate/start", post(start_authentication))
        .route("/authenticate/finish", post(finish_authentication))
        // Management
        .route("/credentials", get(list_credentials))
        .route("/credentials/{credential_id}", delete(delete_credential))
        .route(
            "/credentials/{credential_id}",
            get(get_credential).put(update_credential),
        )
}

// ============================================================================
// Types
// ============================================================================

/// WebAuthn credential stored in database
#[derive(Debug, Clone, sqlx::FromRow)]
#[allow(dead_code)]
pub struct WebAuthnCredentialRow {
    pub id: Uuid,
    pub user_id: Uuid,
    pub credential_id: Vec<u8>,
    pub public_key: Vec<u8>,
    pub counter: i64,
    pub name: String,
    pub aaguid: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// Credential summary for listing
#[derive(Debug, Serialize)]
pub struct CredentialSummary {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub credential_id_preview: String,
}

/// Start registration request
#[derive(Debug, Deserialize)]
pub struct StartRegistrationRequest {
    pub name: Option<String>,
}

/// Finish registration request
#[derive(Debug, Deserialize)]
pub struct FinishRegistrationRequest {
    pub name: Option<String>,
    pub response: serde_json::Value,
}

/// Update credential request
#[derive(Debug, Deserialize)]
pub struct UpdateCredentialRequest {
    pub name: String,
}

// ============================================================================
// WebAuthn Instance
// ============================================================================

fn create_webauthn() -> std::result::Result<Webauthn, WebauthnError> {
    let rp_id = std::env::var("WEBAUTHN_RP_ID").unwrap_or_else(|_| "localhost".to_string());
    let rp_origin =
        std::env::var("WEBAUTHN_RP_ORIGIN").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let rp_name = std::env::var("APP_NAME").unwrap_or_else(|_| "Reiver".to_string());

    let rp_origin_url = Url::parse(&rp_origin).map_err(|_| WebauthnError::Configuration)?;

    let builder = WebauthnBuilder::new(&rp_id, &rp_origin_url)?.rp_name(&rp_name);

    builder.build()
}

// ============================================================================
// Passwordless Login (Primary Authentication)
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct StartPasswordlessLoginRequest {
    /// User's email address
    pub email: String,
}

#[derive(Debug, Serialize)]
pub struct PasswordlessLoginResponse {
    /// JWT access token (on successful authentication)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    /// User info (on successful authentication)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<PasswordlessUserInfo>,
    /// Whether WebAuthn is available for this user
    pub webauthn_available: bool,
    /// Challenge token for WebAuthn ceremony
    #[serde(skip_serializing_if = "Option::is_none")]
    pub challenge_token: Option<String>,
    /// WebAuthn options to pass to navigator.credentials.get()
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct PasswordlessUserInfo {
    pub id: Uuid,
    pub email: String,
    pub name: Option<String>,
}

/// Start passwordless login with WebAuthn
///
/// Returns WebAuthn challenge if user has security keys registered.
/// No JWT required - this is the primary authentication entry point.
async fn start_passwordless_login(
    State(state): State<Arc<WebsiteState>>,
    Json(req): Json<StartPasswordlessLoginRequest>,
) -> Result<Json<PasswordlessLoginResponse>> {
    // Look up user by email
    #[derive(sqlx::FromRow)]
    struct UserRow {
        id: Uuid,
        email: String,
        name: Option<String>,
    }

    let user = sqlx::query_as::<_, UserRow>("SELECT id, email, name FROM users WHERE email = $1")
        .bind(&req.email)
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let user = match user {
        Some(u) => u,
        None => {
            // Don't reveal whether email exists - return generic response
            return Ok(Json(PasswordlessLoginResponse {
                access_token: None,
                user: None,
                webauthn_available: false,
                challenge_token: None,
                options: None,
            }));
        }
    };

    // Check if user has WebAuthn credentials
    let has_webauthn = user_has_webauthn(&state.db, user.id).await.unwrap_or(false);

    if !has_webauthn {
        return Ok(Json(PasswordlessLoginResponse {
            access_token: None,
            user: None,
            webauthn_available: false,
            challenge_token: None,
            options: None,
        }));
    }

    // Get user's credentials
    let cred_rows: Vec<WebAuthnCredentialRow> = sqlx::query_as(
        r#"
        SELECT id, user_id, credential_id, public_key, counter, name, aaguid, created_at, last_used_at
        FROM webauthn_credentials
        WHERE user_id = $1
        "#
    )
    .bind(user.id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Convert to Passkey objects
    let passkeys: Vec<Passkey> = cred_rows
        .iter()
        .filter_map(|row| serde_json::from_slice(&row.public_key).ok())
        .collect();

    if passkeys.is_empty() {
        return Ok(Json(PasswordlessLoginResponse {
            access_token: None,
            user: None,
            webauthn_available: false,
            challenge_token: None,
            options: None,
        }));
    }

    let webauthn = create_webauthn()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("WebAuthn config error: {}", e)))?;

    // Start authentication ceremony
    let (rcr, auth_state) = webauthn
        .start_passkey_authentication(&passkeys)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("WebAuthn error: {}", e)))?;

    // Generate challenge token
    let challenge_token = Uuid::new_v4().to_string();

    // Store auth state in Redis with user info
    let state_key = format!("webauthn:passwordless:{}", challenge_token);
    let state_data = serde_json::json!({
        "user_id": user.id.to_string(),
        "email": user.email,
        "name": user.name,
        "auth_state": serde_json::to_string(&auth_state)
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Serialization error: {}", e)))?,
    });

    let redis_pool = state.redis.clone();
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    redis::cmd("SETEX")
        .arg(&state_key)
        .arg(300) // 5 minutes
        .arg(state_data.to_string())
        .query_async::<()>(&mut *conn)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    info!("Passwordless WebAuthn login started for user {}", user.id);

    Ok(Json(PasswordlessLoginResponse {
        access_token: None,
        user: None,
        webauthn_available: true,
        challenge_token: Some(challenge_token),
        options: Some(
            serde_json::to_value(&rcr)
                .map_err(|e| AppError::Internal(anyhow::anyhow!("Serialization error: {}", e)))?,
        ),
    }))
}

#[derive(Debug, Deserialize)]
pub struct FinishPasswordlessLoginRequest {
    /// Challenge token from start_passwordless_login
    pub challenge_token: String,
    /// WebAuthn response from navigator.credentials.get()
    pub response: serde_json::Value,
}

/// Finish passwordless login with WebAuthn
///
/// Verifies the WebAuthn response and returns a JWT on success.
async fn finish_passwordless_login(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<FinishPasswordlessLoginRequest>,
) -> Result<Json<PasswordlessLoginResponse>> {
    let webauthn = create_webauthn()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("WebAuthn config error: {}", e)))?;

    // Get auth state from Redis
    let state_key = format!("webauthn:passwordless:{}", req.challenge_token);
    let redis_pool = state.redis.clone();
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    let state_json: Option<String> = redis::cmd("GET")
        .arg(&state_key)
        .query_async(&mut *conn)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    let state_data: serde_json::Value = state_json
        .ok_or_else(|| AppError::Validation("Login session expired".to_string()))?
        .parse()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid session data")))?;

    // Delete the session
    let _: std::result::Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&state_key)
        .query_async(&mut *conn)
        .await;

    let user_id: Uuid = state_data["user_id"]
        .as_str()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid session data")))?
        .parse()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid user ID")))?;

    let org_id = lookup_user_organization(&state.db, user_id).await;

    let email = state_data["email"]
        .as_str()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid session data")))?;

    let name = state_data["name"].as_str().map(String::from);

    let auth_state_str = state_data["auth_state"]
        .as_str()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid session data")))?;

    let auth_state: PasskeyAuthentication = serde_json::from_str(auth_state_str)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Invalid session data: {}", e)))?;

    // Parse the response
    let auth_response: PublicKeyCredential = serde_json::from_value(req.response)
        .map_err(|e| AppError::Validation(format!("Invalid authentication response: {}", e)))?;

    // Complete authentication
    let auth_result = match webauthn.finish_passkey_authentication(&auth_response, &auth_state) {
        Ok(result) => result,
        Err(e) => {
            error!("WebAuthn passwordless login failed: {}", e);

            // Log failed attempt
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let mut audit = AuditEventBuilder::new(AuditEventType::MfaFailed)
                .user(user_id)
                .details(serde_json::json!({ "method": "webauthn", "type": "passwordless" }))
                .origin(
                    &audit_origin.origin_type,
                    &audit_origin.origin_ref,
                    &audit_origin.origin_reason,
                )
                .caller(
                    &audit_caller.caller_type,
                    &audit_caller.key_label,
                    &audit_caller.key_prefix,
                );
            if let Some(org_id) = org_id {
                audit = audit.organization(org_id);
            }
            let _ = audit.log(&state.clickhouse).await;

            return Err(AppError::Validation("Authentication failed".to_string()));
        }
    };

    // Update counter in database
    let cred_id_bytes = auth_result.cred_id().to_vec();

    sqlx::query(
        r#"
        UPDATE webauthn_credentials
        SET counter = $1, last_used_at = NOW()
        WHERE user_id = $2 AND credential_id = $3
        "#,
    )
    .bind(auth_result.counter() as i64)
    .bind(user_id)
    .bind(&cred_id_bytes)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to update credential: {}", e)))?;

    // Generate JWT using config secret (validated at startup)
    // Use same expiration and claims structure as main auth.rs
    let now = chrono::Utc::now();
    let exp = (now + chrono::Duration::hours(24)).timestamp();
    let iat = now.timestamp();
    let jti = Uuid::new_v4().to_string(); // Unique token ID for revocation

    let jwt_claims = serde_json::json!({
        "sub": user_id.to_string(),
        "exp": exp,
        "iat": iat,
        "jti": jti,
    });

    // Explicitly use HS256 algorithm
    let header = jsonwebtoken::Header {
        alg: jsonwebtoken::Algorithm::HS256,
        ..Default::default()
    };

    let token = jsonwebtoken::encode(
        &header,
        &jwt_claims,
        &jsonwebtoken::EncodingKey::from_secret(state.config.jwt_secret.as_bytes()),
    )
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to generate token: {}", e)))?;

    // Log successful login
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::MfaVerified)
        .user(user_id)
        .details(serde_json::json!({ "method": "webauthn", "type": "passwordless" }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        );
    if let Some(org_id) = org_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    info!(
        "Passwordless WebAuthn login successful for user {}",
        user_id
    );

    Ok(Json(PasswordlessLoginResponse {
        access_token: Some(token),
        user: Some(PasswordlessUserInfo {
            id: user_id,
            email: email.to_string(),
            name,
        }),
        webauthn_available: true,
        challenge_token: None,
        options: None,
    }))
}

// ============================================================================
// Registration Endpoints
// ============================================================================

/// Start WebAuthn registration
async fn start_registration(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<StartRegistrationRequest>,
) -> Result<Json<serde_json::Value>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let webauthn = create_webauthn()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("WebAuthn config error: {}", e)))?;

    // Get user info
    let user: (String, String) = sqlx::query_as("SELECT email, name FROM users WHERE id = $1")
        .bind(current_user_id)
        .fetch_one(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let (email, name) = user;

    // Get existing credentials to exclude
    let existing_creds: Vec<(Vec<u8>,)> =
        sqlx::query_as("SELECT credential_id FROM webauthn_credentials WHERE user_id = $1")
            .bind(current_user_id)
            .fetch_all(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let exclude_credentials: Vec<CredentialID> = existing_creds
        .into_iter()
        .map(|(cred_id,)| CredentialID::from(cred_id))
        .collect();

    // Start registration ceremony
    let (ccr, reg_state) = webauthn
        .start_passkey_registration(current_user_id, &email, &name, Some(exclude_credentials))
        .map_err(|e| AppError::Internal(anyhow::anyhow!("WebAuthn error: {}", e)))?;

    // Store registration state in Redis
    let state_key = format!("webauthn:reg:{}", current_user_id);
    let state_json = serde_json::to_string(&reg_state)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Serialization error: {}", e)))?;

    // Also store the credential name
    let state_with_name = serde_json::json!({
        "state": state_json,
        "name": req.name.unwrap_or_else(|| "Security Key".to_string())
    });

    let redis_pool = state.redis.clone();
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    redis::cmd("SETEX")
        .arg(&state_key)
        .arg(300) // 5 minutes
        .arg(state_with_name.to_string())
        .query_async::<()>(&mut *conn)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    info!("WebAuthn registration started for user {}", current_user_id);

    Ok(Json(serde_json::to_value(&ccr).map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Serialization error: {}", e))
    })?))
}

/// Finish WebAuthn registration
async fn finish_registration(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<FinishRegistrationRequest>,
) -> Result<Json<serde_json::Value>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let org_id = lookup_user_organization(&state.db, current_user_id).await;
    let webauthn = create_webauthn()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("WebAuthn config error: {}", e)))?;

    // Get registration state from Redis
    let state_key = format!("webauthn:reg:{}", current_user_id);
    let redis_pool = state.redis.clone();
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    let state_json: Option<String> = redis::cmd("GET")
        .arg(&state_key)
        .query_async(&mut *conn)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    let state_data: serde_json::Value = state_json
        .ok_or_else(|| AppError::Validation("Registration session expired".to_string()))?
        .parse()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid session data")))?;

    let reg_state: PasskeyRegistration =
        serde_json::from_str(state_data["state"].as_str().unwrap_or(""))
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Deserialization error: {}", e)))?;

    let cred_name = req
        .name
        .or_else(|| state_data["name"].as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "Security Key".to_string());

    // Delete the session
    let _: std::result::Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&state_key)
        .query_async(&mut *conn)
        .await;

    // Parse the response
    let reg_response: RegisterPublicKeyCredential = serde_json::from_value(req.response)
        .map_err(|e| AppError::Validation(format!("Invalid registration response: {}", e)))?;

    // Complete registration
    let passkey = webauthn
        .finish_passkey_registration(&reg_response, &reg_state)
        .map_err(|e| AppError::Validation(format!("Registration failed: {}", e)))?;

    // Serialize the passkey for storage
    let cred_id = passkey.cred_id().to_vec();
    let public_key = serde_json::to_vec(&passkey)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Serialization error: {}", e)))?;
    // AAGUID is not directly accessible in the new API, store empty for now
    let aaguid: Vec<u8> = Vec::new();

    // Store credential in database
    let credential_id: (Uuid,) = sqlx::query_as(
        r#"
        INSERT INTO webauthn_credentials (user_id, credential_id, public_key, counter, name, aaguid)
        VALUES ($1, $2, $3, 0, $4, $5)
        RETURNING id
        "#,
    )
    .bind(current_user_id)
    .bind(&cred_id)
    .bind(&public_key)
    .bind(&cred_name)
    .bind(&aaguid)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        if e.to_string().contains("duplicate key") {
            AppError::Validation("This security key is already registered".to_string())
        } else {
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        }
    })?;

    // Also create MFA enrollment record
    sqlx::query(
        r#"
        INSERT INTO mfa_enrollments (user_id, method, name, is_primary)
        VALUES ($1, 'webauthn', $2, false)
        ON CONFLICT (user_id, method, name) DO NOTHING
        "#,
    )
    .bind(current_user_id)
    .bind(&cred_name)
    .execute(&*state.db)
    .await
    .ok();

    // Log audit event
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::MfaEnrolled)
        .user(current_user_id)
        .details(serde_json::json!({
            "method": "webauthn",
            "name": cred_name
        }))
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
        .success();
    if let Some(org_id) = org_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    info!(
        "WebAuthn credential registered for user {}: {}",
        current_user_id, cred_name
    );

    Ok(Json(serde_json::json!({
        "success": true,
        "credential_id": credential_id.0,
        "name": cred_name
    })))
}

// ============================================================================
// Authentication Endpoints
// ============================================================================

/// Start WebAuthn authentication
async fn start_authentication(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let webauthn = create_webauthn()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("WebAuthn config error: {}", e)))?;

    // Get user's credentials
    let cred_rows: Vec<WebAuthnCredentialRow> = sqlx::query_as(
        r#"
        SELECT id, user_id, credential_id, public_key, counter, name, aaguid, created_at, last_used_at
        FROM webauthn_credentials
        WHERE user_id = $1
        "#
    )
    .bind(current_user_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    if cred_rows.is_empty() {
        return Err(AppError::NotFound(
            "No security keys registered".to_string(),
        ));
    }

    // Convert to Passkey objects
    let passkeys: Vec<Passkey> = cred_rows
        .iter()
        .filter_map(|row| serde_json::from_slice(&row.public_key).ok())
        .collect();

    if passkeys.is_empty() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "Failed to load credentials"
        )));
    }

    // Start authentication ceremony
    let (rcr, auth_state) = webauthn
        .start_passkey_authentication(&passkeys)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("WebAuthn error: {}", e)))?;

    // Store auth state in Redis
    let state_key = format!("webauthn:auth:{}", current_user_id);
    let state_json = serde_json::to_string(&auth_state)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Serialization error: {}", e)))?;

    let redis_pool = state.redis.clone();
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    redis::cmd("SETEX")
        .arg(&state_key)
        .arg(300) // 5 minutes
        .arg(&state_json)
        .query_async::<()>(&mut *conn)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    info!(
        "WebAuthn authentication started for user {}",
        current_user_id
    );

    Ok(Json(serde_json::to_value(&rcr).map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Serialization error: {}", e))
    })?))
}

/// Finish WebAuthn authentication
#[derive(Debug, Deserialize)]
pub struct FinishAuthenticationRequest {
    pub response: serde_json::Value,
}

async fn finish_authentication(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<FinishAuthenticationRequest>,
) -> Result<Json<serde_json::Value>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let org_id = lookup_user_organization(&state.db, current_user_id).await;
    let webauthn = create_webauthn()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("WebAuthn config error: {}", e)))?;

    // Get auth state from Redis
    let state_key = format!("webauthn:auth:{}", current_user_id);
    let redis_pool = state.redis.clone();
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    let state_json: Option<String> = redis::cmd("GET")
        .arg(&state_key)
        .query_async(&mut *conn)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    let auth_state: PasskeyAuthentication = serde_json::from_str(
        &state_json
            .ok_or_else(|| AppError::Validation("Authentication session expired".to_string()))?,
    )
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Invalid session data: {}", e)))?;

    // Delete the session
    let _: std::result::Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&state_key)
        .query_async(&mut *conn)
        .await;

    // Parse the response
    let auth_response: PublicKeyCredential = serde_json::from_value(req.response)
        .map_err(|e| AppError::Validation(format!("Invalid authentication response: {}", e)))?;

    // Complete authentication
    let auth_result = match webauthn.finish_passkey_authentication(&auth_response, &auth_state) {
        Ok(result) => result,
        Err(e) => {
            error!("WebAuthn authentication failed: {}", e);

            // Log failed attempt
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let mut audit = AuditEventBuilder::new(AuditEventType::MfaFailed)
                .user(current_user_id)
                .details(serde_json::json!({ "method": "webauthn" }))
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
                .failure(&format!("{}", e));
            if let Some(org_id) = org_id {
                audit = audit.organization(org_id);
            }
            let _ = audit.log(&state.clickhouse).await;

            return Err(AppError::Validation("Authentication failed".to_string()));
        }
    };

    // Update counter in database
    let cred_id_bytes = auth_result.cred_id().to_vec();

    sqlx::query(
        r#"
        UPDATE webauthn_credentials
        SET counter = $1, last_used_at = NOW()
        WHERE user_id = $2 AND credential_id = $3
        "#,
    )
    .bind(auth_result.counter() as i64)
    .bind(current_user_id)
    .bind(&cred_id_bytes)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Log success
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::MfaVerified)
        .user(current_user_id)
        .details(serde_json::json!({ "method": "webauthn" }))
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
        .success();
    if let Some(org_id) = org_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    info!(
        "WebAuthn authentication successful for user {}",
        current_user_id
    );

    Ok(Json(serde_json::json!({
        "success": true,
        "verified": true
    })))
}

// ============================================================================
// Credential Management
// ============================================================================

/// List all credentials for current user
async fn list_credentials(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<CredentialSummary>>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let creds: Vec<WebAuthnCredentialRow> = sqlx::query_as(
        r#"
        SELECT id, user_id, credential_id, public_key, counter, name, aaguid, created_at, last_used_at
        FROM webauthn_credentials
        WHERE user_id = $1
        ORDER BY created_at ASC
        "#
    )
    .bind(current_user_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let summaries: Vec<CredentialSummary> = creds
        .into_iter()
        .map(|c| CredentialSummary {
            id: c.id,
            name: c.name,
            created_at: c.created_at,
            last_used_at: c.last_used_at,
            credential_id_preview: hex::encode(
                &c.credential_id[..std::cmp::min(8, c.credential_id.len())],
            ) + "...",
        })
        .collect();

    Ok(Json(summaries))
}

/// Get a specific credential
async fn get_credential(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(credential_id): Path<Uuid>,
) -> Result<Json<CredentialSummary>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let cred: WebAuthnCredentialRow = sqlx::query_as(
        r#"
        SELECT id, user_id, credential_id, public_key, counter, name, aaguid, created_at, last_used_at
        FROM webauthn_credentials
        WHERE id = $1 AND user_id = $2
        "#
    )
    .bind(credential_id)
    .bind(current_user_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Credential not found".to_string()))?;

    Ok(Json(CredentialSummary {
        id: cred.id,
        name: cred.name,
        created_at: cred.created_at,
        last_used_at: cred.last_used_at,
        credential_id_preview: hex::encode(
            &cred.credential_id[..std::cmp::min(8, cred.credential_id.len())],
        ) + "...",
    }))
}

/// Update credential name
async fn update_credential(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(credential_id): Path<Uuid>,
    Json(req): Json<UpdateCredentialRequest>,
) -> Result<Json<CredentialSummary>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let org_id = lookup_user_organization(&state.db, current_user_id).await;
    let cred: WebAuthnCredentialRow = sqlx::query_as(
        r#"
        UPDATE webauthn_credentials
        SET name = $1
        WHERE id = $2 AND user_id = $3
        RETURNING id, user_id, credential_id, public_key, counter, name, aaguid, created_at, last_used_at
        "#
    )
    .bind(&req.name)
    .bind(credential_id)
    .bind(current_user_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Credential not found".to_string()))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::MfaEnrolled)
        .user(current_user_id)
        .resource("webauthn_credential", credential_id)
        .details(serde_json::json!({
            "method": "webauthn",
            "action": "credential_renamed",
            "name": &req.name
        }))
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
        .success();
    if let Some(org_id) = org_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(CredentialSummary {
        id: cred.id,
        name: cred.name,
        created_at: cred.created_at,
        last_used_at: cred.last_used_at,
        credential_id_preview: hex::encode(
            &cred.credential_id[..std::cmp::min(8, cred.credential_id.len())],
        ) + "...",
    }))
}

/// Delete a credential
async fn delete_credential(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(credential_id): Path<Uuid>,
) -> Result<StatusCode> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let org_id = lookup_user_organization(&state.db, current_user_id).await;
    let result = sqlx::query("DELETE FROM webauthn_credentials WHERE id = $1 AND user_id = $2")
        .bind(credential_id)
        .bind(current_user_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Credential not found".to_string()));
    }

    // Check if user has any remaining WebAuthn credentials
    let remaining: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM webauthn_credentials WHERE user_id = $1")
            .bind(current_user_id)
            .fetch_one(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // If no more WebAuthn credentials, remove the MFA enrollment
    if remaining.0 == 0 {
        sqlx::query("DELETE FROM mfa_enrollments WHERE user_id = $1 AND method = 'webauthn'")
            .bind(current_user_id)
            .execute(&*state.db)
            .await
            .ok();
    }

    info!(
        "WebAuthn credential {} deleted for user {}",
        credential_id, current_user_id
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::MfaDisabled)
        .user(current_user_id)
        .resource("webauthn_credential", credential_id)
        .details(serde_json::json!({
            "method": "webauthn",
            "remaining_credentials": remaining.0
        }))
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
        .success();
    if let Some(org_id) = org_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Public API for authentication flow
// ============================================================================

async fn lookup_user_organization(db: &sqlx::PgPool, user_id: Uuid) -> Option<Uuid> {
    sqlx::query_scalar(
        "SELECT organization_id FROM memberships WHERE user_id = $1 AND status = 'active' LIMIT 1",
    )
    .bind(user_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
}

/// Check if a user has WebAuthn credentials
pub async fn user_has_webauthn(db: &sqlx::PgPool, user_id: Uuid) -> anyhow::Result<bool> {
    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM webauthn_credentials WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(db)
            .await?;

    Ok(count.0 > 0)
}
