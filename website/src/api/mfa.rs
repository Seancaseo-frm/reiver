//! MFA (Multi-Factor Authentication) API
//!
//! Endpoints for managing TOTP-based MFA and recovery codes.

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{delete, get, post},
    Router,
};
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use totp_rs::{Algorithm, Secret, TOTP};
use tracing::{info, warn};
use uuid::Uuid;

use crate::app_state::RedisPool;
use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::auth::extract_user_id;
use crate::config::TotpAlgorithm;
use crate::error::{AppError, Result};
use crate::rate_limit::check_recovery_code_rate_limit;

/// Convert config TotpAlgorithm to totp-rs Algorithm
fn get_totp_algorithm(algo: TotpAlgorithm) -> Algorithm {
    match algo {
        TotpAlgorithm::Sha1 => Algorithm::SHA1,
        TotpAlgorithm::Sha256 => Algorithm::SHA256,
    }
}

// ============================================================================
// Constant-Time TOTP Verification
// ============================================================================

/// Constant-time string comparison to prevent timing attacks
///
/// This function always compares all bytes regardless of early mismatches,
/// preventing attackers from inferring correct characters via timing analysis.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

/// Verify TOTP code with constant-time comparison
///
/// This wraps totp-rs but uses constant-time comparison to prevent timing attacks.
/// The totp-rs `check_current()` method doesn't use constant-time comparison.
fn verify_totp_constant_time(totp: &TOTP, code: &str) -> bool {
    // Get the current expected code
    let expected = match totp.generate_current() {
        Ok(expected) => expected,
        Err(_) => return false,
    };

    // Use constant-time comparison
    constant_time_eq(code.as_bytes(), expected.as_bytes())
}

// ============================================================================
// TOTP Replay Protection
// ============================================================================

/// TTL for storing used TOTP codes (60 seconds covers current + previous window)
const TOTP_REPLAY_TTL_SECONDS: i64 = 60;

/// Check if a TOTP code has been used recently and mark it as used.
///
/// # Security
/// This prevents replay attacks where an attacker who intercepts a TOTP code
/// could use it within the same 30-second window. Each code can only be used once.
///
/// Returns Ok(true) if the code is fresh (not used), Ok(false) if already used.
async fn check_and_mark_totp_used(redis: &RedisPool, user_id: Uuid, code: &str) -> Result<bool> {
    // Key format: mfa:totp:used:{user_id}:{code}
    // Using both user_id and code ensures uniqueness per user
    let used_key = format!("mfa:totp:used:{}:{}", user_id, code);

    let mut conn = redis.get().await.map_err(|e| {
        warn!(
            "Failed to get Redis connection for TOTP replay check: {}",
            e
        );
        AppError::Internal(anyhow::anyhow!("Session storage error"))
    })?;

    // Use SETNX (SET if Not eXists) with expiry to atomically check and set
    // Returns 1 if key was set (code not used), 0 if key exists (code already used)
    let result: i32 = redis::cmd("SET")
        .arg(&used_key)
        .arg("1")
        .arg("NX") // Only set if not exists
        .arg("EX") // Set expiry
        .arg(TOTP_REPLAY_TTL_SECONDS)
        .query_async(&mut *conn)
        .await
        .map(|v: Option<String>| if v.is_some() { 1 } else { 0 })
        .unwrap_or(0);

    if result == 0 {
        warn!("TOTP replay attack detected for user {}", user_id);
    }

    Ok(result == 1)
}

// ============================================================================
// MFA Failure Lockout with Exponential Backoff
// ============================================================================

/// Maximum number of failed MFA attempts before lockout
const MFA_MAX_FAILURES: i32 = 5;

/// Base lockout duration in seconds (doubles with each additional failure)
const MFA_LOCKOUT_BASE_SECONDS: i64 = 30;

/// Maximum lockout duration in seconds (30 minutes)
const MFA_LOCKOUT_MAX_SECONDS: i64 = 1800;

/// TTL for failure counter in Redis (1 hour)
const MFA_FAILURE_TTL_SECONDS: i64 = 3600;

/// Check if a user is locked out from MFA attempts and enforce delay.
///
/// # Security
/// This implements exponential backoff after failed MFA attempts:
/// - After 5 failures: 30 second lockout
/// - After 6 failures: 60 second lockout  
/// - After 7 failures: 120 second lockout
/// - Maximum lockout: 30 minutes
///
/// This makes brute force attacks impractical while allowing legitimate
/// users who mistype their code to retry after a short wait.
async fn check_mfa_lockout(redis: &RedisPool, user_id: Uuid) -> Result<()> {
    let failure_key = format!("mfa:failures:{}", user_id);
    let lockout_key = format!("mfa:lockout:{}", user_id);

    let mut conn = redis.get().await.map_err(|e| {
        warn!(
            "Failed to get Redis connection for MFA lockout check: {}",
            e
        );
        AppError::Internal(anyhow::anyhow!("Session storage error"))
    })?;

    // Check if user is currently locked out
    let lockout_ttl: i64 = redis::cmd("TTL")
        .arg(&lockout_key)
        .query_async(&mut *conn)
        .await
        .unwrap_or(-2);

    if lockout_ttl > 0 {
        warn!(
            "MFA lockout active for user {}, {} seconds remaining",
            user_id, lockout_ttl
        );
        return Err(AppError::Validation(format!(
            "Too many failed attempts. Please wait {} seconds before trying again.",
            lockout_ttl
        )));
    }

    // Check failure count to warn if approaching lockout
    let failures: i32 = redis::cmd("GET")
        .arg(&failure_key)
        .query_async(&mut *conn)
        .await
        .ok()
        .and_then(|v: Option<String>| v?.parse().ok())
        .unwrap_or(0);

    if failures >= MFA_MAX_FAILURES {
        // Calculate lockout duration with exponential backoff
        let extra_failures = failures - MFA_MAX_FAILURES;
        let lockout_seconds = std::cmp::min(
            MFA_LOCKOUT_BASE_SECONDS * 2_i64.pow(extra_failures as u32),
            MFA_LOCKOUT_MAX_SECONDS,
        );

        // Set lockout
        let _: std::result::Result<(), redis::RedisError> = redis::cmd("SETEX")
            .arg(&lockout_key)
            .arg(lockout_seconds)
            .arg("1")
            .query_async(&mut *conn)
            .await;

        warn!(
            "MFA lockout triggered for user {}: {} failures, {} second lockout",
            user_id, failures, lockout_seconds
        );

        return Err(AppError::Validation(format!(
            "Too many failed attempts. Please wait {} seconds before trying again.",
            lockout_seconds
        )));
    }

    Ok(())
}

/// Record a failed MFA attempt for a user.
/// Increments the failure counter and sets TTL.
async fn record_mfa_failure(redis: &RedisPool, user_id: Uuid) {
    let failure_key = format!("mfa:failures:{}", user_id);

    if let Ok(mut conn) = redis.get().await {
        // Increment failure counter
        let _: std::result::Result<i32, redis::RedisError> = redis::cmd("INCR")
            .arg(&failure_key)
            .query_async(&mut *conn)
            .await;

        // Set TTL (reset the timer on each failure)
        let _: std::result::Result<(), redis::RedisError> = redis::cmd("EXPIRE")
            .arg(&failure_key)
            .arg(MFA_FAILURE_TTL_SECONDS)
            .query_async(&mut *conn)
            .await;
    }
}

/// Clear MFA failure counter on successful verification.
async fn clear_mfa_failures(redis: &RedisPool, user_id: Uuid) {
    let failure_key = format!("mfa:failures:{}", user_id);
    let lockout_key = format!("mfa:lockout:{}", user_id);

    if let Ok(mut conn) = redis.get().await {
        let _: std::result::Result<(), redis::RedisError> = redis::cmd("DEL")
            .arg(&failure_key)
            .arg(&lockout_key)
            .query_async(&mut *conn)
            .await;
    }
}

pub fn create_mfa_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        // TOTP endpoints
        .route("/totp/setup", post(setup_totp))
        .route("/totp/verify", post(verify_totp))
        .route("/totp/confirm", post(confirm_totp_enrollment))
        .route("/totp", delete(disable_totp))
        // Recovery codes
        .route(
            "/recovery-codes",
            get(list_recovery_codes).post(generate_recovery_codes),
        )
        .route("/recovery-codes/verify", post(verify_recovery_code))
        // Status
        .route("/status", get(get_mfa_status))
        .route("/enrollments", get(list_enrollments))
}

// ============================================================================
// Types
// ============================================================================

/// MFA enrollment stored in database
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MfaEnrollment {
    pub id: Uuid,
    pub user_id: Uuid,
    pub method: String,
    pub secret_encrypted: Option<String>,
    pub name: Option<String>,
    pub is_primary: bool,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// MFA enrollment summary (without secret)
#[derive(Debug, Serialize)]
pub struct MfaEnrollmentSummary {
    pub id: Uuid,
    pub method: String,
    pub name: Option<String>,
    pub is_primary: bool,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// Recovery code stored in database
#[derive(Debug, Clone, sqlx::FromRow)]
#[allow(dead_code)]
pub struct RecoveryCode {
    pub id: Uuid,
    pub user_id: Uuid,
    pub code_hash: String,
    pub created_at: DateTime<Utc>,
    pub used_at: Option<DateTime<Utc>>,
}

/// MFA status for a user
#[derive(Debug, Serialize)]
pub struct MfaStatus {
    pub enabled: bool,
    pub methods: Vec<String>,
    pub has_recovery_codes: bool,
    pub recovery_codes_count: i64,
}

/// TOTP setup response
#[derive(Debug, Serialize)]
pub struct TotpSetupResponse {
    pub secret: String,
    pub qr_code_url: String,
    pub issuer: String,
    pub account_name: String,
}

/// TOTP verification request
#[derive(Debug, Deserialize)]
pub struct VerifyTotpRequest {
    pub code: String,
}

/// Confirm TOTP enrollment request
#[derive(Debug, Deserialize)]
pub struct ConfirmTotpRequest {
    pub code: String,
    pub name: Option<String>,
}

/// Recovery codes response
#[derive(Debug, Serialize)]
pub struct RecoveryCodesResponse {
    pub codes: Vec<String>,
    pub message: String,
}

/// Verify recovery code request
#[derive(Debug, Deserialize)]
pub struct VerifyRecoveryCodeRequest {
    pub code: String,
}

// ============================================================================
// TOTP Endpoints
// ============================================================================

/// Start TOTP setup - generates a new secret
async fn setup_totp(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<TotpSetupResponse>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    // Get user email for the account name
    let user: (String,) = sqlx::query_as("SELECT email FROM users WHERE id = $1")
        .bind(current_user_id)
        .fetch_one(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let email = user.0;
    let issuer = std::env::var("APP_NAME").unwrap_or_else(|_| "Reiver".to_string());

    // Generate a random secret using totp-rs
    let secret = Secret::generate_secret();
    let secret_base32 = secret.to_encoded().to_string();
    let secret_bytes = secret
        .to_bytes()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Secret error: {}", e)))?;

    // Get configured TOTP algorithm
    let algorithm = get_totp_algorithm(state.config.totp_algorithm);
    let algorithm_str = state.config.totp_algorithm.as_str();

    // Create TOTP for verification
    let _totp = TOTP::new(algorithm, 6, 1, 30, secret_bytes)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("TOTP error: {}", e)))?;

    // Build the otpauth URL manually
    let qr_code_url = format!(
        "otpauth://totp/{}:{}?secret={}&issuer={}&algorithm={}&digits=6&period=30",
        urlencoding::encode(&issuer),
        urlencoding::encode(&email),
        &secret_base32,
        urlencoding::encode(&issuer),
        algorithm_str
    );

    // Store the secret temporarily in Redis (expires in 10 minutes)
    // The user must verify the code before we persist to DB
    let temp_key = format!("mfa:totp:setup:{}", current_user_id);
    let encrypted_secret = state
        .encryptor
        .encrypt(&secret_base32)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Encryption error: {}", e)))?;

    let redis_pool = state.redis.clone();
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    redis::cmd("SETEX")
        .arg(&temp_key)
        .arg(600) // 10 minutes
        .arg(&encrypted_secret)
        .query_async::<()>(&mut *conn)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    info!("TOTP setup initiated for user {}", current_user_id);

    Ok(Json(TotpSetupResponse {
        secret: secret_base32,
        qr_code_url,
        issuer,
        account_name: email,
    }))
}

/// Confirm TOTP enrollment by verifying the code
async fn confirm_totp_enrollment(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<ConfirmTotpRequest>,
) -> Result<Json<serde_json::Value>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    // Get the temporary secret from Redis
    let temp_key = format!("mfa:totp:setup:{}", current_user_id);
    let redis_pool = state.redis.clone();
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    let encrypted_secret: Option<String> = redis::cmd("GET")
        .arg(&temp_key)
        .query_async(&mut *conn)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    let encrypted_secret = encrypted_secret.ok_or_else(|| {
        AppError::Validation("TOTP setup expired. Please start again.".to_string())
    })?;

    // Decrypt the secret
    let secret_base32 = state
        .encryptor
        .decrypt(&encrypted_secret)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Decryption error: {}", e)))?;

    // Verify the code using configured algorithm
    let algorithm = get_totp_algorithm(state.config.totp_algorithm);
    let secret = Secret::Encoded(secret_base32.clone());
    let secret_bytes = secret
        .to_bytes()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Secret error: {}", e)))?;
    let totp = TOTP::new(algorithm, 6, 1, 30, secret_bytes)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("TOTP error: {}", e)))?;

    // Use constant-time comparison to prevent timing attacks
    if !verify_totp_constant_time(&totp, &req.code) {
        return Err(AppError::Validation(
            "Invalid verification code".to_string(),
        ));
    }

    // Code is valid - persist the enrollment
    let enrollment_name = req.name.unwrap_or_else(|| "Authenticator App".to_string());

    // Check if user already has TOTP enrolled
    let existing: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM mfa_enrollments WHERE user_id = $1 AND method = 'totp'")
            .bind(current_user_id)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    if existing.is_some() {
        return Err(AppError::Validation("TOTP is already enrolled".to_string()));
    }

    // Store the encrypted secret in the database
    let enrollment_id: (Uuid,) = sqlx::query_as(
        r#"
        INSERT INTO mfa_enrollments (user_id, method, secret_encrypted, name, is_primary)
        VALUES ($1, 'totp', $2, $3, true)
        RETURNING id
        "#,
    )
    .bind(current_user_id)
    .bind(&encrypted_secret)
    .bind(&enrollment_name)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Delete the temporary secret from Redis
    let _: std::result::Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&temp_key)
        .query_async(&mut *conn)
        .await;

    // Generate recovery codes
    let codes = generate_recovery_codes_internal(&state, current_user_id).await?;

    // Log audit event
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::MfaEnrolled)
        .user(current_user_id)
        .details(serde_json::json!({ "method": "totp" }))
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

    info!("TOTP enrolled for user {}", current_user_id);

    Ok(Json(serde_json::json!({
        "success": true,
        "enrollment_id": enrollment_id.0,
        "recovery_codes": codes,
        "message": "TOTP has been enabled. Save your recovery codes in a safe place."
    })))
}

/// Verify a TOTP code (for login)
async fn verify_totp(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<VerifyTotpRequest>,
) -> Result<Json<serde_json::Value>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;

    // SECURITY: Check for lockout before processing
    // Implements exponential backoff after repeated failures
    check_mfa_lockout(&state.redis, current_user_id).await?;

    // Get the user's TOTP enrollment
    let enrollment = sqlx::query_as::<_, MfaEnrollment>(
        r#"
        SELECT id, user_id, method, secret_encrypted, name, is_primary, created_at, last_used_at
        FROM mfa_enrollments
        WHERE user_id = $1 AND method = 'totp'
        "#,
    )
    .bind(current_user_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    // SECURITY: Use generic error message to prevent MFA status enumeration
    .ok_or_else(|| AppError::Validation("Invalid verification code".to_string()))?;

    let encrypted_secret = enrollment
        .secret_encrypted
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("No TOTP secret found")))?;

    // Decrypt the secret
    let secret_base32 = state
        .encryptor
        .decrypt(&encrypted_secret)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Decryption error: {}", e)))?;

    // Verify the code using configured algorithm
    let algorithm = get_totp_algorithm(state.config.totp_algorithm);
    let secret = Secret::Encoded(secret_base32);
    let secret_bytes = secret
        .to_bytes()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Secret error: {}", e)))?;
    let totp = TOTP::new(algorithm, 6, 1, 30, secret_bytes)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("TOTP error: {}", e)))?;

    // Use constant-time comparison to prevent timing attacks
    let valid = verify_totp_constant_time(&totp, &req.code);

    if !valid {
        // Record failure for lockout tracking
        record_mfa_failure(&state.redis, current_user_id).await;

        // Log failed attempt
        let audit_origin = AuditOrigin::from_headers(&headers);
        let audit_caller = AuditCaller::from_headers(&headers);
        AuditEventBuilder::new(AuditEventType::MfaFailed)
            .user(current_user_id)
            .details(serde_json::json!({ "method": "totp" }))
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
            .failure("Invalid code")
            .log(&state.clickhouse)
            .await;

        return Err(AppError::Validation(
            "Invalid verification code".to_string(),
        ));
    }

    // SECURITY: Check for TOTP replay attack
    // The same code cannot be used twice within its validity window
    let is_fresh = check_and_mark_totp_used(&state.redis, current_user_id, &req.code).await?;
    if !is_fresh {
        // Record failure for lockout tracking (replay attacks count as failures)
        record_mfa_failure(&state.redis, current_user_id).await;

        // Log replay attempt
        let audit_origin = AuditOrigin::from_headers(&headers);
        let audit_caller = AuditCaller::from_headers(&headers);
        AuditEventBuilder::new(AuditEventType::MfaFailed)
            .user(current_user_id)
            .details(serde_json::json!({ "method": "totp", "reason": "replay_attack" }))
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
            .failure("Code already used")
            .log(&state.clickhouse)
            .await;

        // SECURITY: Use generic error message - don't reveal that code was already used
        return Err(AppError::Validation(
            "Invalid verification code".to_string(),
        ));
    }

    // SECURITY: Clear failure counter on successful verification
    clear_mfa_failures(&state.redis, current_user_id).await;

    // Update last used timestamp
    sqlx::query("UPDATE mfa_enrollments SET last_used_at = NOW() WHERE id = $1")
        .bind(enrollment.id)
        .execute(&*state.db)
        .await
        .ok();

    // Log success
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::MfaVerified)
        .user(current_user_id)
        .details(serde_json::json!({ "method": "totp" }))
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

    Ok(Json(serde_json::json!({
        "success": true,
        "verified": true
    })))
}

/// Disable TOTP
async fn disable_totp(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<StatusCode> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let result = sqlx::query("DELETE FROM mfa_enrollments WHERE user_id = $1 AND method = 'totp'")
        .bind(current_user_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("TOTP is not enrolled".to_string()));
    }

    // Log audit event
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::MfaDisabled)
        .user(current_user_id)
        .details(serde_json::json!({ "method": "totp" }))
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

    info!("TOTP disabled for user {}", current_user_id);
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Recovery Codes
// ============================================================================

/// Generate new recovery codes
async fn generate_recovery_codes(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<RecoveryCodesResponse>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let codes = generate_recovery_codes_internal(&state, current_user_id).await?;

    // Log audit event
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::RecoveryCodesGenerated)
        .user(current_user_id)
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

    Ok(Json(RecoveryCodesResponse {
        codes,
        message: "New recovery codes generated. Previous codes have been invalidated.".to_string(),
    }))
}

async fn generate_recovery_codes_internal(
    state: &WebsiteState,
    user_id: Uuid,
) -> Result<Vec<String>> {
    // Delete existing recovery codes
    sqlx::query("DELETE FROM mfa_recovery_codes WHERE user_id = $1")
        .bind(user_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Generate 10 new codes using StdRng which is Send (can be held across await)
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::from_entropy();
    let mut codes = Vec::new();

    for _ in 0..10 {
        // Generate a random 12-character code for ~62 bits of entropy (36^12 possibilities)
        // This is significantly stronger than the previous 8-character codes (~41 bits)
        let code: String = (0..12)
            .map(|_| {
                let idx = rng.gen_range(0..36);
                if idx < 10 {
                    (b'0' + idx) as char
                } else {
                    (b'A' + idx - 10) as char
                }
            })
            .collect();

        // Format as XXXX-XXXX-XXXX for readability
        let formatted_code = format!("{}-{}-{}", &code[0..4], &code[4..8], &code[8..12]);

        // Hash the code for storage
        let mut hasher = Sha256::new();
        hasher.update(formatted_code.as_bytes());
        let code_hash = hex::encode(hasher.finalize());

        // Store in database
        sqlx::query("INSERT INTO mfa_recovery_codes (user_id, code_hash) VALUES ($1, $2)")
            .bind(user_id)
            .bind(&code_hash)
            .execute(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

        codes.push(formatted_code);
    }

    info!(
        "Generated {} recovery codes for user {}",
        codes.len(),
        user_id
    );
    Ok(codes)
}

/// List recovery codes (only shows count and used status, not actual codes)
async fn list_recovery_codes(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let codes: Vec<RecoveryCode> = sqlx::query_as(
        r#"
        SELECT id, user_id, code_hash, created_at, used_at
        FROM mfa_recovery_codes
        WHERE user_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(current_user_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let total = codes.len();
    let unused = codes.iter().filter(|c| c.used_at.is_none()).count();
    let used = total - unused;

    Ok(Json(serde_json::json!({
        "total": total,
        "unused": unused,
        "used": used,
        "created_at": codes.first().map(|c| c.created_at)
    })))
}

/// Verify a recovery code (consumes it)
async fn verify_recovery_code(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<VerifyRecoveryCodeRequest>,
) -> Result<Json<serde_json::Value>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;

    // SECURITY: Strict rate limiting for recovery code attempts
    // There are only 10 recovery codes, so brute force must be prevented
    // Limits: 3 attempts/minute, 5 attempts/hour
    check_recovery_code_rate_limit(&state.redis, &current_user_id).await?;

    // SECURITY: Check for lockout (shared with TOTP failures)
    check_mfa_lockout(&state.redis, current_user_id).await?;

    // Normalize the code (remove dashes, uppercase)
    let normalized_code = req.code.replace('-', "").to_uppercase();
    let formatted_code = if normalized_code.len() == 12 {
        // Format: XXXX-XXXX-XXXX (12 chars, ~62 bits entropy)
        format!(
            "{}-{}-{}",
            &normalized_code[0..4],
            &normalized_code[4..8],
            &normalized_code[8..12]
        )
    } else {
        // Invalid length - will fail hash comparison
        req.code.to_uppercase()
    };

    // Hash the code
    let mut hasher = Sha256::new();
    hasher.update(formatted_code.as_bytes());
    let code_hash = hex::encode(hasher.finalize());

    // Find and mark as used
    let result = sqlx::query(
        r#"
        UPDATE mfa_recovery_codes
        SET used_at = NOW()
        WHERE user_id = $1 AND code_hash = $2 AND used_at IS NULL
        "#,
    )
    .bind(current_user_id)
    .bind(&code_hash)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    if result.rows_affected() == 0 {
        // Record failure for lockout tracking
        record_mfa_failure(&state.redis, current_user_id).await;

        // Log failed attempt
        let audit_origin = AuditOrigin::from_headers(&headers);
        let audit_caller = AuditCaller::from_headers(&headers);
        AuditEventBuilder::new(AuditEventType::MfaFailed)
            .user(current_user_id)
            .details(serde_json::json!({ "method": "recovery_code" }))
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
            .failure("Invalid or already used code")
            .log(&state.clickhouse)
            .await;

        // SECURITY: Use generic error message to prevent recovery code enumeration
        return Err(AppError::Validation(
            "Invalid verification code".to_string(),
        ));
    }

    // SECURITY: Clear failure counter on successful verification
    clear_mfa_failures(&state.redis, current_user_id).await;

    // Log success
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::RecoveryCodeUsed)
        .user(current_user_id)
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

    // Count remaining codes
    let remaining: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM mfa_recovery_codes WHERE user_id = $1 AND used_at IS NULL",
    )
    .bind(current_user_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    info!(
        "Recovery code used for user {}, {} remaining",
        current_user_id, remaining.0
    );

    Ok(Json(serde_json::json!({
        "success": true,
        "verified": true,
        "remaining_codes": remaining.0
    })))
}

// ============================================================================
// Status Endpoints
// ============================================================================

/// Get MFA status for current user
async fn get_mfa_status(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<MfaStatus>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    // Get enrollments
    let enrollments: Vec<(String,)> =
        sqlx::query_as("SELECT method FROM mfa_enrollments WHERE user_id = $1")
            .bind(current_user_id)
            .fetch_all(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let methods: Vec<String> = enrollments.into_iter().map(|e| e.0).collect();

    // Count recovery codes
    let recovery_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM mfa_recovery_codes WHERE user_id = $1 AND used_at IS NULL",
    )
    .bind(current_user_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    Ok(Json(MfaStatus {
        enabled: !methods.is_empty(),
        methods,
        has_recovery_codes: recovery_count.0 > 0,
        recovery_codes_count: recovery_count.0,
    }))
}

/// List all MFA enrollments for current user
async fn list_enrollments(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<MfaEnrollmentSummary>>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let enrollments = sqlx::query_as::<_, MfaEnrollment>(
        r#"
        SELECT id, user_id, method, secret_encrypted, name, is_primary, created_at, last_used_at
        FROM mfa_enrollments
        WHERE user_id = $1
        ORDER BY is_primary DESC, created_at ASC
        "#,
    )
    .bind(current_user_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let summaries: Vec<MfaEnrollmentSummary> = enrollments
        .into_iter()
        .map(|e| MfaEnrollmentSummary {
            id: e.id,
            method: e.method,
            name: e.name,
            is_primary: e.is_primary,
            created_at: e.created_at,
            last_used_at: e.last_used_at,
        })
        .collect();

    Ok(Json(summaries))
}

// ============================================================================
// Public API for authentication flow
// ============================================================================

/// Check if a user has MFA enabled
pub async fn user_has_mfa_enabled(db: &sqlx::PgPool, user_id: Uuid) -> anyhow::Result<bool> {
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM mfa_enrollments WHERE user_id = $1")
        .bind(user_id)
        .fetch_one(db)
        .await?;

    Ok(count.0 > 0)
}

/// Verify MFA code (TOTP or recovery code)
///
/// This function is used by the SSO flow for MFA verification.
/// It includes lockout protection and failure tracking.
pub async fn verify_mfa_code(
    state: &WebsiteState,
    user_id: Uuid,
    code: &str,
) -> anyhow::Result<bool> {
    // SECURITY: Check for lockout before processing
    if let Err(e) = check_mfa_lockout(&state.redis, user_id).await {
        // Convert to anyhow error for the public API
        return Err(anyhow::anyhow!("{}", e));
    }

    // Try TOTP first
    // Use configured TOTP algorithm
    let algorithm = get_totp_algorithm(state.config.totp_algorithm);

    if let Ok(Some(enrollment)) = sqlx::query_as::<_, MfaEnrollment>(
        "SELECT * FROM mfa_enrollments WHERE user_id = $1 AND method = 'totp'",
    )
    .bind(user_id)
    .fetch_optional(&*state.db)
    .await
    {
        if let Some(encrypted_secret) = enrollment.secret_encrypted {
            if let Ok(secret_base32) = state.encryptor.decrypt(&encrypted_secret) {
                let secret = Secret::Encoded(secret_base32);
                if let Ok(secret_bytes) = secret.to_bytes() {
                    if let Ok(totp) = TOTP::new(algorithm, 6, 1, 30, secret_bytes) {
                        // Use constant-time comparison to prevent timing attacks
                        if verify_totp_constant_time(&totp, code) {
                            // SECURITY: Check for TOTP replay attack
                            // The same code cannot be used twice within its validity window
                            if let Ok(is_fresh) =
                                check_and_mark_totp_used(&state.redis, user_id, code).await
                            {
                                if !is_fresh {
                                    // Code was already used - replay attack
                                    record_mfa_failure(&state.redis, user_id).await;
                                    return Ok(false);
                                }
                            }

                            // Success - clear failure counter
                            clear_mfa_failures(&state.redis, user_id).await;

                            // Update last used
                            sqlx::query(
                                "UPDATE mfa_enrollments SET last_used_at = NOW() WHERE id = $1",
                            )
                            .bind(enrollment.id)
                            .execute(&*state.db)
                            .await
                            .ok();
                            return Ok(true);
                        }
                    }
                }
            }
        }
    }

    // Try recovery code
    let normalized_code = code.replace('-', "").to_uppercase();
    let formatted_code = if normalized_code.len() == 12 {
        // Format: XXXX-XXXX-XXXX (12 chars, ~62 bits entropy)
        format!(
            "{}-{}-{}",
            &normalized_code[0..4],
            &normalized_code[4..8],
            &normalized_code[8..12]
        )
    } else {
        // Invalid length - will fail hash comparison
        code.to_uppercase()
    };

    let mut hasher = Sha256::new();
    hasher.update(formatted_code.as_bytes());
    let code_hash = hex::encode(hasher.finalize());

    let result = sqlx::query(
        "UPDATE mfa_recovery_codes SET used_at = NOW() WHERE user_id = $1 AND code_hash = $2 AND used_at IS NULL"
    )
    .bind(user_id)
    .bind(&code_hash)
    .execute(&*state.db)
    .await?;

    if result.rows_affected() > 0 {
        // Success - clear failure counter
        clear_mfa_failures(&state.redis, user_id).await;
        Ok(true)
    } else {
        // Record failure
        record_mfa_failure(&state.redis, user_id).await;
        Ok(false)
    }
}
