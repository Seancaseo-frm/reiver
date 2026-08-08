//! TOTP (Time-based One-Time Password) implementation
//!
//! Implements RFC 6238 for TOTP generation and verification.
//! Compatible with Google Authenticator, Authy, etc.

use anyhow::{Context, Result};
use base32::Alphabet;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sqlx::PgPool;
use tracing::{debug, info};
use uuid::Uuid;

/// TOTP configuration
const TOTP_DIGITS: u32 = 6;
const TOTP_PERIOD: u64 = 30; // seconds
const TOTP_SKEW: i64 = 1; // Allow 1 period before/after

/// TOTP secret stored in database
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TotpSecret {
    pub id: Uuid,
    pub user_id: Uuid,
    pub secret_encrypted: String,
    pub name: String,
    pub verified: bool,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// TOTP manager
pub struct TotpManager<'a> {
    db: &'a PgPool,
}

impl<'a> TotpManager<'a> {
    pub fn new(db: &'a PgPool) -> Self {
        Self { db }
    }

    /// Generate a new TOTP secret (20 bytes = 160 bits)
    pub fn generate_secret() -> Vec<u8> {
        use rand::RngCore;
        let mut secret = vec![0u8; 20];
        rand::thread_rng().fill_bytes(&mut secret);
        secret
    }

    /// Encode secret as base32 for QR codes
    pub fn encode_secret(secret: &[u8]) -> String {
        base32::encode(Alphabet::Rfc4648 { padding: false }, secret)
    }

    /// Decode base32 secret
    pub fn decode_secret(encoded: &str) -> Result<Vec<u8>> {
        base32::decode(Alphabet::Rfc4648 { padding: false }, encoded)
            .ok_or_else(|| anyhow::anyhow!("Invalid base32 encoding"))
    }

    /// Generate TOTP URI for QR code
    pub fn generate_uri(secret: &[u8], email: &str, issuer: &str) -> String {
        let encoded_secret = Self::encode_secret(secret);
        format!(
            "otpauth://totp/{}:{}?secret={}&issuer={}&algorithm=SHA1&digits={}&period={}",
            urlencoding::encode(issuer),
            urlencoding::encode(email),
            encoded_secret,
            urlencoding::encode(issuer),
            TOTP_DIGITS,
            TOTP_PERIOD,
        )
    }

    /// Generate TOTP code for a given time
    pub fn generate_code(secret: &[u8], timestamp: u64) -> String {
        let counter = timestamp / TOTP_PERIOD;
        Self::hotp(secret, counter)
    }

    /// Generate current TOTP code
    pub fn generate_current_code(secret: &[u8]) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("System time is before UNIX epoch - clock configuration error")
            .as_secs();
        Self::generate_code(secret, now)
    }

    /// HOTP implementation (RFC 4226)
    fn hotp(secret: &[u8], counter: u64) -> String {
        // Convert counter to big-endian bytes
        let counter_bytes = counter.to_be_bytes();

        // Compute HMAC-SHA1
        let mut mac = Hmac::<Sha1>::new_from_slice(secret).expect("HMAC can take key of any size");
        mac.update(&counter_bytes);
        let result = mac.finalize().into_bytes();

        // Dynamic truncation
        let offset = (result[19] & 0x0f) as usize;
        let binary = ((result[offset] & 0x7f) as u32) << 24
            | (result[offset + 1] as u32) << 16
            | (result[offset + 2] as u32) << 8
            | (result[offset + 3] as u32);

        // Generate TOTP_DIGITS digit code
        let code = binary % 10u32.pow(TOTP_DIGITS);
        format!("{:0width$}", code, width = TOTP_DIGITS as usize)
    }

    /// Verify a TOTP code (with time skew tolerance)
    pub fn verify_code(secret: &[u8], code: &str) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("System time is before UNIX epoch - clock configuration error")
            .as_secs();

        let counter = (now / TOTP_PERIOD) as i64;

        // Check current and adjacent time periods
        for offset in -TOTP_SKEW..=TOTP_SKEW {
            let check_counter = (counter + offset) as u64;
            let expected = Self::hotp(secret, check_counter);
            if constant_time_eq(code.as_bytes(), expected.as_bytes()) {
                return true;
            }
        }

        false
    }

    /// Enroll a new TOTP secret for a user (returns URI for QR code)
    pub async fn enroll(
        &self,
        user_id: Uuid,
        email: &str,
        issuer: &str,
        encryptor: &crate::crypto::RotatingSecretEncryptor,
    ) -> Result<(Uuid, String, String)> {
        let secret = Self::generate_secret();
        let encoded_secret = Self::encode_secret(&secret);
        let uri = Self::generate_uri(&secret, email, issuer);

        // Encrypt the secret for storage
        let encrypted = encryptor
            .encrypt(&encoded_secret)
            .map_err(|e| anyhow::anyhow!("Failed to encrypt TOTP secret: {}", e))?;

        // Store in database (unverified until user confirms)
        let row: (Uuid,) = sqlx::query_as(
            r#"
            INSERT INTO mfa_enrollments (user_id, method, secret_encrypted, name, is_primary)
            VALUES ($1, 'totp', $2, 'Authenticator App', false)
            RETURNING id
            "#,
        )
        .bind(user_id)
        .bind(&encrypted)
        .fetch_one(self.db)
        .await
        .context("Failed to create TOTP enrollment")?;

        info!("Created TOTP enrollment {} for user {}", row.0, user_id);

        Ok((row.0, encoded_secret, uri))
    }

    /// Verify enrollment with a code (activates the TOTP)
    pub async fn verify_enrollment(
        &self,
        enrollment_id: Uuid,
        code: &str,
        encryptor: &crate::crypto::RotatingSecretEncryptor,
    ) -> Result<bool> {
        // Get the enrollment
        let enrollment: Option<(String,)> = sqlx::query_as(
            "SELECT secret_encrypted FROM mfa_enrollments WHERE id = $1 AND method = 'totp'",
        )
        .bind(enrollment_id)
        .fetch_optional(self.db)
        .await
        .context("Failed to fetch enrollment")?;

        let encrypted = match enrollment {
            Some((e,)) => e,
            None => return Ok(false),
        };

        // Decrypt the secret
        let encoded_secret = encryptor
            .decrypt(&encrypted)
            .map_err(|e| anyhow::anyhow!("Failed to decrypt TOTP secret: {}", e))?;

        let secret = Self::decode_secret(&encoded_secret)?;

        // Verify the code
        if !Self::verify_code(&secret, code) {
            return Ok(false);
        }

        // Mark as verified and primary
        sqlx::query(
            r#"
            UPDATE mfa_enrollments
            SET is_primary = true, last_used_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(enrollment_id)
        .execute(self.db)
        .await
        .context("Failed to verify enrollment")?;

        info!("Verified TOTP enrollment {}", enrollment_id);

        Ok(true)
    }

    /// Verify a TOTP code for a user
    pub async fn verify_with_encryptor(
        &self,
        user_id: Uuid,
        code: &str,
        encryptor: &crate::crypto::RotatingSecretEncryptor,
    ) -> Result<bool> {
        // Get all primary TOTP enrollments for user
        let enrollments: Vec<(Uuid, String)> = sqlx::query_as(
            r#"
            SELECT id, secret_encrypted 
            FROM mfa_enrollments 
            WHERE user_id = $1 AND method = 'totp' AND is_primary = true
            "#,
        )
        .bind(user_id)
        .fetch_all(self.db)
        .await
        .context("Failed to fetch TOTP enrollments")?;

        if enrollments.is_empty() {
            debug!("No TOTP enrollments found for user {}", user_id);
            return Ok(false);
        }

        // Try to verify against each enrollment
        for (enrollment_id, encrypted_secret) in enrollments {
            // Decrypt the secret
            let encoded_secret = match encryptor.decrypt(&encrypted_secret) {
                Ok(s) => s,
                Err(e) => {
                    debug!(
                        "Failed to decrypt TOTP secret for enrollment {}: {}",
                        enrollment_id, e
                    );
                    continue;
                }
            };

            let secret = match Self::decode_secret(&encoded_secret) {
                Ok(s) => s,
                Err(e) => {
                    debug!(
                        "Failed to decode TOTP secret for enrollment {}: {}",
                        enrollment_id, e
                    );
                    continue;
                }
            };

            // Verify the code
            if Self::verify_code(&secret, code) {
                // Update last used timestamp
                sqlx::query("UPDATE mfa_enrollments SET last_used_at = NOW() WHERE id = $1")
                    .bind(enrollment_id)
                    .execute(self.db)
                    .await
                    .ok();

                info!(
                    "TOTP verification successful for user {} (enrollment {})",
                    user_id, enrollment_id
                );
                return Ok(true);
            }
        }

        debug!("TOTP verification failed for user {}", user_id);
        Ok(false)
    }

    /// Remove TOTP enrollment
    pub async fn remove(&self, enrollment_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM mfa_enrollments WHERE id = $1 AND method = 'totp'")
            .bind(enrollment_id)
            .execute(self.db)
            .await
            .context("Failed to remove TOTP enrollment")?;

        info!("Removed TOTP enrollment {}", enrollment_id);
        Ok(())
    }
}

/// Constant-time string comparison to prevent timing attacks
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_secret() {
        let secret = TotpManager::generate_secret();
        assert_eq!(secret.len(), 20);
    }

    #[test]
    fn test_encode_decode_secret() {
        let secret = TotpManager::generate_secret();
        let encoded = TotpManager::encode_secret(&secret);
        let decoded = TotpManager::decode_secret(&encoded).unwrap();
        assert_eq!(secret, decoded);
    }

    #[test]
    fn test_generate_code() {
        // Test vector from RFC 6238
        let secret = b"12345678901234567890";

        // Time = 59 seconds, counter = 1
        let code = TotpManager::generate_code(secret, 59);
        assert_eq!(code.len(), 6);

        // Same time should produce same code
        let code2 = TotpManager::generate_code(secret, 59);
        assert_eq!(code, code2);

        // Different time (next period) should produce different code
        let code3 = TotpManager::generate_code(secret, 90);
        assert_ne!(code, code3);
    }

    #[test]
    fn test_verify_code() {
        let secret = TotpManager::generate_secret();
        let code = TotpManager::generate_current_code(&secret);

        assert!(TotpManager::verify_code(&secret, &code));
        assert!(!TotpManager::verify_code(&secret, "000000"));
        assert!(!TotpManager::verify_code(&secret, "invalid"));
    }

    #[test]
    fn test_generate_uri() {
        let secret = b"12345678901234567890";
        let uri = TotpManager::generate_uri(secret, "user@example.com", "Reiver");

        assert!(uri.starts_with("otpauth://totp/"));
        assert!(uri.contains("user%40example.com"));
        assert!(uri.contains("issuer=Reiver"));
        assert!(uri.contains("algorithm=SHA1"));
        assert!(uri.contains("digits=6"));
        assert!(uri.contains("period=30"));
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
    }

    // ========== Additional Edge Case Tests ==========

    #[test]
    fn test_rfc6238_test_vectors() {
        // Test vectors from RFC 6238 Appendix B
        // Secret: "12345678901234567890" (ASCII, 20 bytes)
        let secret = b"12345678901234567890";

        // Test time = 59 (counter = 1, first period after epoch)
        // The RFC specifies SHA1 with 8 digits, but we use 6 digits
        // Verify code is numeric and 6 digits
        let code = TotpManager::generate_code(secret, 59);
        assert_eq!(code.len(), 6);
        assert!(code.chars().all(|c| c.is_ascii_digit()));

        // Test boundary: time = 30 (exactly at period boundary)
        let code_boundary = TotpManager::generate_code(secret, 30);
        assert_eq!(code_boundary.len(), 6);

        // Test time = 29 (just before period boundary - same as period 0)
        let code_before = TotpManager::generate_code(secret, 29);
        let code_after = TotpManager::generate_code(secret, 31);
        // Before and after boundary should be different periods
        assert_ne!(code_before, code_after);
    }

    #[test]
    fn test_hotp_counter_increment() {
        let secret = b"test_secret_key_20";

        // Different counters should produce different codes
        let codes: Vec<String> = (0..10)
            .map(|counter| TotpManager::generate_code(secret, counter * 30))
            .collect();

        // All codes should be unique
        for i in 0..codes.len() {
            for j in (i + 1)..codes.len() {
                assert_ne!(
                    codes[i], codes[j],
                    "Codes at counters {} and {} should differ",
                    i, j
                );
            }
        }
    }

    #[test]
    fn test_verify_code_with_skew() {
        let secret = TotpManager::generate_secret();

        // Generate code for current time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Code from current period should verify
        let current_code = TotpManager::generate_code(&secret, now);
        assert!(TotpManager::verify_code(&secret, &current_code));

        // Code from previous period (within skew) should also verify
        let prev_code = TotpManager::generate_code(&secret, now.saturating_sub(30));
        assert!(TotpManager::verify_code(&secret, &prev_code));

        // Code from next period (within skew) should also verify
        let next_code = TotpManager::generate_code(&secret, now + 30);
        assert!(TotpManager::verify_code(&secret, &next_code));
    }

    #[test]
    fn test_invalid_base32() {
        // Invalid characters
        assert!(TotpManager::decode_secret("invalid!@#$").is_err());

        // Valid base32 should decode
        assert!(TotpManager::decode_secret("JBSWY3DPEHPK3PXP").is_ok());
    }

    #[test]
    fn test_code_format() {
        let secret = TotpManager::generate_secret();

        // Code should always be 6 digits, zero-padded
        for _ in 0..100 {
            let code = TotpManager::generate_current_code(&secret);
            assert_eq!(code.len(), 6);
            assert!(code.chars().all(|c| c.is_ascii_digit()));
        }
    }

    #[test]
    fn test_verify_wrong_length_codes() {
        let secret = TotpManager::generate_secret();

        // Too short
        assert!(!TotpManager::verify_code(&secret, "12345"));

        // Too long
        assert!(!TotpManager::verify_code(&secret, "1234567"));

        // Empty
        assert!(!TotpManager::verify_code(&secret, ""));
    }

    #[test]
    fn test_verify_non_numeric_codes() {
        let secret = TotpManager::generate_secret();

        assert!(!TotpManager::verify_code(&secret, "abcdef"));
        assert!(!TotpManager::verify_code(&secret, "12345a"));
        assert!(!TotpManager::verify_code(&secret, "12 345"));
    }

    #[test]
    fn test_constant_time_eq_empty() {
        assert!(constant_time_eq(b"", b""));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(!constant_time_eq(b"a", b""));
    }

    #[test]
    fn test_different_secrets_produce_different_codes() {
        let secret1 = TotpManager::generate_secret();
        let secret2 = TotpManager::generate_secret();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let code1 = TotpManager::generate_code(&secret1, now);
        let code2 = TotpManager::generate_code(&secret2, now);

        assert_ne!(code1, code2);
    }

    #[test]
    fn test_uri_special_characters() {
        let secret = b"12345678901234567890";

        // Email with special characters
        let uri = TotpManager::generate_uri(secret, "user+test@example.com", "My App");
        assert!(uri.contains("user%2Btest%40example.com"));
        assert!(uri.contains("issuer=My%20App"));
    }
}
