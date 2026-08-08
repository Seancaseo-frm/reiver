//! Multi-Factor Authentication (MFA) module
//!
//! Provides:
//! - TOTP (Time-based One-Time Password) - RFC 6238
//! - WebAuthn (FIDO2) - for hardware security keys and biometrics
//! - Recovery codes

pub mod recovery;
pub mod totp;
pub mod webauthn;

pub use recovery::{RecoveryCode, RecoveryCodeManager};
pub use totp::{TotpManager, TotpSecret};
pub use webauthn::{WebAuthnCredential, WebAuthnManager};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

/// MFA method type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfaMethod {
    Totp,
    WebAuthn,
    RecoveryCode,
}

impl MfaMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            MfaMethod::Totp => "totp",
            MfaMethod::WebAuthn => "webauthn",
            MfaMethod::RecoveryCode => "recovery_code",
        }
    }
}

/// MFA enrollment status
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MfaEnrollment {
    pub id: Uuid,
    pub user_id: Uuid,
    pub method: String,
    pub name: Option<String>,
    pub is_primary: bool,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// MFA status for a user
#[derive(Debug, Clone, Serialize)]
pub struct MfaStatus {
    pub enabled: bool,
    pub methods: Vec<MfaEnrollment>,
    pub recovery_codes_remaining: u32,
}

/// Get MFA status for a user
pub async fn get_mfa_status(db: &PgPool, user_id: Uuid) -> anyhow::Result<MfaStatus> {
    let enrollments = sqlx::query_as::<_, MfaEnrollment>(
        r#"
        SELECT id, user_id, method, name, is_primary, created_at, last_used_at
        FROM mfa_enrollments
        WHERE user_id = $1
        ORDER BY is_primary DESC, created_at ASC
        "#,
    )
    .bind(user_id)
    .fetch_all(db)
    .await?;

    let recovery_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM mfa_recovery_codes WHERE user_id = $1 AND used_at IS NULL",
    )
    .bind(user_id)
    .fetch_one(db)
    .await?;

    Ok(MfaStatus {
        enabled: !enrollments.is_empty(),
        methods: enrollments,
        recovery_codes_remaining: recovery_count.0 as u32,
    })
}

/// Verify MFA code (TOTP or recovery)
///
/// For TOTP verification, an encryptor is required to decrypt the stored secret.
pub async fn verify_mfa_code(
    db: &PgPool,
    user_id: Uuid,
    code: &str,
    encryptor: &crate::crypto::RotatingSecretEncryptor,
) -> anyhow::Result<bool> {
    // First try TOTP
    let totp_manager = TotpManager::new(db);
    if totp_manager
        .verify_with_encryptor(user_id, code, encryptor)
        .await?
    {
        return Ok(true);
    }

    // Try recovery code
    let recovery_manager = RecoveryCodeManager::new(db);
    if recovery_manager.use_code(user_id, code).await? {
        return Ok(true);
    }

    Ok(false)
}
