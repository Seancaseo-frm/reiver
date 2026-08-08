//! SSO session management
//!
//! Provides:
//! - Session creation and tracking
//! - Session revocation (user logout, admin revoke, SLO)
//! - Session listing and cleanup

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::{debug, info};
use uuid::Uuid;

/// Session stored in the database
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SsoSession {
    pub id: Uuid,
    pub user_id: Uuid,
    pub sso_config_id: Uuid,
    pub session_token_hash: String,
    pub idp_session_id: Option<String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub revocation_reason: Option<String>,
}

/// Revocation reason
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevocationReason {
    UserLogout,
    AdminRevoke,
    SingleLogout,
    Expired,
    SecurityConcern,
}

impl RevocationReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            RevocationReason::UserLogout => "user_logout",
            RevocationReason::AdminRevoke => "admin_revoke",
            RevocationReason::SingleLogout => "slo",
            RevocationReason::Expired => "expired",
            RevocationReason::SecurityConcern => "security_concern",
        }
    }
}

/// Session manager
pub struct SessionManager<'a> {
    db: &'a PgPool,
    /// Default session duration in hours
    session_duration_hours: i64,
}

impl<'a> SessionManager<'a> {
    pub fn new(db: &'a PgPool) -> Self {
        Self {
            db,
            session_duration_hours: 24,
        }
    }

    pub fn with_duration(mut self, hours: i64) -> Self {
        self.session_duration_hours = hours;
        self
    }

    /// Generate a new session token
    pub fn generate_token() -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let bytes: [u8; 32] = rng.gen();
        hex::encode(bytes)
    }

    /// Hash a session token for storage
    pub fn hash_token(token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Create a new session
    pub async fn create_session(
        &self,
        user_id: Uuid,
        sso_config_id: Uuid,
        idp_session_id: Option<&str>,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<(String, SsoSession)> {
        let token = Self::generate_token();
        let token_hash = Self::hash_token(&token);
        let expires_at = Utc::now() + Duration::hours(self.session_duration_hours);

        let session = sqlx::query_as::<_, SsoSession>(
            r#"
            INSERT INTO sso_sessions (
                user_id, sso_config_id, session_token_hash,
                idp_session_id, ip_address, user_agent, expires_at
            ) VALUES ($1, $2, $3, $4, $5::inet, $6, $7)
            RETURNING id, user_id, sso_config_id, session_token_hash,
                      idp_session_id, ip_address::text, user_agent,
                      created_at, expires_at, last_activity_at,
                      revoked_at, revocation_reason
            "#,
        )
        .bind(user_id)
        .bind(sso_config_id)
        .bind(&token_hash)
        .bind(idp_session_id)
        .bind(ip_address)
        .bind(user_agent)
        .bind(expires_at)
        .fetch_one(self.db)
        .await
        .context("Failed to create session")?;

        info!("Created SSO session {} for user {}", session.id, user_id);

        Ok((token, session))
    }

    /// Validate a session token and return the session if valid
    pub async fn validate_session(&self, token: &str) -> Result<Option<SsoSession>> {
        let token_hash = Self::hash_token(token);

        let session = sqlx::query_as::<_, SsoSession>(
            r#"
            SELECT id, user_id, sso_config_id, session_token_hash,
                   idp_session_id, ip_address::text, user_agent,
                   created_at, expires_at, last_activity_at,
                   revoked_at, revocation_reason
            FROM sso_sessions
            WHERE session_token_hash = $1
              AND revoked_at IS NULL
              AND expires_at > NOW()
            "#,
        )
        .bind(&token_hash)
        .fetch_optional(self.db)
        .await
        .context("Failed to validate session")?;

        if let Some(ref s) = session {
            // Update last activity (non-critical, log failures but don't fail validation)
            if let Err(e) =
                sqlx::query("UPDATE sso_sessions SET last_activity_at = NOW() WHERE id = $1")
                    .bind(s.id)
                    .execute(self.db)
                    .await
            {
                debug!("Failed to update session last_activity_at: {}", e);
            }
        }

        Ok(session)
    }

    /// Revoke a session
    pub async fn revoke_session(&self, session_id: Uuid, reason: RevocationReason) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE sso_sessions
            SET revoked_at = NOW(), revocation_reason = $1
            WHERE id = $2 AND revoked_at IS NULL
            "#,
        )
        .bind(reason.as_str())
        .bind(session_id)
        .execute(self.db)
        .await
        .context("Failed to revoke session")?;

        info!("Revoked session {}: {}", session_id, reason.as_str());
        Ok(())
    }

    /// Revoke a session by token
    pub async fn revoke_by_token(&self, token: &str, reason: RevocationReason) -> Result<bool> {
        let token_hash = Self::hash_token(token);

        let result = sqlx::query(
            r#"
            UPDATE sso_sessions
            SET revoked_at = NOW(), revocation_reason = $1
            WHERE session_token_hash = $2 AND revoked_at IS NULL
            "#,
        )
        .bind(reason.as_str())
        .bind(&token_hash)
        .execute(self.db)
        .await
        .context("Failed to revoke session")?;

        Ok(result.rows_affected() > 0)
    }

    /// Revoke all sessions for a user
    pub async fn revoke_user_sessions(
        &self,
        user_id: Uuid,
        reason: RevocationReason,
    ) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE sso_sessions
            SET revoked_at = NOW(), revocation_reason = $1
            WHERE user_id = $2 AND revoked_at IS NULL
            "#,
        )
        .bind(reason.as_str())
        .bind(user_id)
        .execute(self.db)
        .await
        .context("Failed to revoke user sessions")?;

        let count = result.rows_affected();
        if count > 0 {
            info!(
                "Revoked {} sessions for user {}: {}",
                count,
                user_id,
                reason.as_str()
            );
        }
        Ok(count)
    }

    /// Revoke all sessions by IdP session ID (for Single Logout)
    pub async fn revoke_by_idp_session(
        &self,
        sso_config_id: Uuid,
        idp_session_id: &str,
    ) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE sso_sessions
            SET revoked_at = NOW(), revocation_reason = 'slo'
            WHERE sso_config_id = $1 AND idp_session_id = $2 AND revoked_at IS NULL
            "#,
        )
        .bind(sso_config_id)
        .bind(idp_session_id)
        .execute(self.db)
        .await
        .context("Failed to revoke sessions by IdP session")?;

        let count = result.rows_affected();
        if count > 0 {
            info!(
                "SLO: Revoked {} sessions for IdP session {}",
                count, idp_session_id
            );
        }
        Ok(count)
    }

    /// List active sessions for a user
    pub async fn list_user_sessions(&self, user_id: Uuid) -> Result<Vec<SsoSession>> {
        let sessions = sqlx::query_as::<_, SsoSession>(
            r#"
            SELECT id, user_id, sso_config_id, session_token_hash,
                   idp_session_id, ip_address::text, user_agent,
                   created_at, expires_at, last_activity_at,
                   revoked_at, revocation_reason
            FROM sso_sessions
            WHERE user_id = $1 AND revoked_at IS NULL AND expires_at > NOW()
            ORDER BY last_activity_at DESC
            "#,
        )
        .bind(user_id)
        .fetch_all(self.db)
        .await
        .context("Failed to list user sessions")?;

        Ok(sessions)
    }

    /// Cleanup expired sessions (run periodically)
    pub async fn cleanup_expired(&self) -> Result<u64> {
        // Mark expired sessions as revoked
        let result = sqlx::query(
            r#"
            UPDATE sso_sessions
            SET revoked_at = NOW(), revocation_reason = 'expired'
            WHERE expires_at < NOW() AND revoked_at IS NULL
            "#,
        )
        .execute(self.db)
        .await
        .context("Failed to cleanup expired sessions")?;

        let count = result.rows_affected();
        if count > 0 {
            debug!("Cleaned up {} expired sessions", count);
        }

        // Delete very old sessions (older than 90 days)
        let delete_result = sqlx::query(
            r#"
            DELETE FROM sso_sessions
            WHERE created_at < NOW() - INTERVAL '90 days'
            "#,
        )
        .execute(self.db)
        .await
        .context("Failed to delete old sessions")?;

        if delete_result.rows_affected() > 0 {
            debug!("Deleted {} old sessions", delete_result.rows_affected());
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_token() {
        let token1 = SessionManager::generate_token();
        let token2 = SessionManager::generate_token();

        assert_eq!(token1.len(), 64); // 32 bytes = 64 hex chars
        assert_ne!(token1, token2);
    }

    #[test]
    fn test_hash_token() {
        let token = "test-token";
        let hash1 = SessionManager::hash_token(token);
        let hash2 = SessionManager::hash_token(token);

        assert_eq!(hash1.len(), 64); // SHA-256 = 32 bytes = 64 hex chars
        assert_eq!(hash1, hash2);

        // Different tokens should produce different hashes
        let hash3 = SessionManager::hash_token("different-token");
        assert_ne!(hash1, hash3);
    }
}
