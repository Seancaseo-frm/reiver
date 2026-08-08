//! Recovery codes for MFA backup
//!
//! Provides one-time use recovery codes as a fallback when
//! primary MFA methods are unavailable.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

/// Number of recovery codes to generate
const RECOVERY_CODE_COUNT: usize = 10;
/// Length of each recovery code (in characters)
const RECOVERY_CODE_LENGTH: usize = 8;

/// Recovery code stored in database
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RecoveryCode {
    pub id: Uuid,
    pub user_id: Uuid,
    pub code_hash: String,
    pub created_at: DateTime<Utc>,
    pub used_at: Option<DateTime<Utc>>,
}

/// Recovery code manager
pub struct RecoveryCodeManager<'a> {
    db: &'a PgPool,
}

impl<'a> RecoveryCodeManager<'a> {
    pub fn new(db: &'a PgPool) -> Self {
        Self { db }
    }

    /// Generate a random recovery code
    fn generate_code() -> String {
        use rand::Rng;
        const CHARSET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // No O, 0, I, 1

        let mut rng = rand::thread_rng();
        (0..RECOVERY_CODE_LENGTH)
            .map(|_| {
                let idx = rng.gen_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect()
    }

    /// Format code with dash for readability (e.g., "ABCD-EFGH")
    fn format_code(code: &str) -> String {
        if code.len() == RECOVERY_CODE_LENGTH {
            format!("{}-{}", &code[..4], &code[4..])
        } else {
            code.to_string()
        }
    }

    /// Hash a recovery code for storage
    fn hash_code(code: &str) -> String {
        // Normalize: remove dashes and uppercase
        let normalized = code.replace('-', "").to_uppercase();
        let mut hasher = Sha256::new();
        hasher.update(normalized.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Generate new recovery codes for a user (replaces existing codes)
    pub async fn generate(&self, user_id: Uuid) -> Result<Vec<String>> {
        // Delete existing codes
        sqlx::query("DELETE FROM mfa_recovery_codes WHERE user_id = $1")
            .bind(user_id)
            .execute(self.db)
            .await
            .context("Failed to delete existing recovery codes")?;

        // Generate new codes
        let mut codes = Vec::with_capacity(RECOVERY_CODE_COUNT);
        let mut code_hashes = Vec::with_capacity(RECOVERY_CODE_COUNT);

        for _ in 0..RECOVERY_CODE_COUNT {
            let code = Self::generate_code();
            let hash = Self::hash_code(&code);
            codes.push(Self::format_code(&code));
            code_hashes.push(hash);
        }

        // Insert all codes
        for hash in &code_hashes {
            sqlx::query("INSERT INTO mfa_recovery_codes (user_id, code_hash) VALUES ($1, $2)")
                .bind(user_id)
                .bind(hash)
                .execute(self.db)
                .await
                .context("Failed to insert recovery code")?;
        }

        info!(
            "Generated {} recovery codes for user {}",
            RECOVERY_CODE_COUNT, user_id
        );

        Ok(codes)
    }

    /// Use a recovery code (marks it as used if valid)
    pub async fn use_code(&self, user_id: Uuid, code: &str) -> Result<bool> {
        let hash = Self::hash_code(code);

        // Find and mark code as used
        let result = sqlx::query(
            r#"
            UPDATE mfa_recovery_codes
            SET used_at = NOW()
            WHERE user_id = $1 AND code_hash = $2 AND used_at IS NULL
            "#,
        )
        .bind(user_id)
        .bind(&hash)
        .execute(self.db)
        .await
        .context("Failed to use recovery code")?;

        if result.rows_affected() > 0 {
            info!("Recovery code used for user {}", user_id);

            // Check remaining codes and warn if low
            let remaining = self.count_remaining(user_id).await?;
            if remaining <= 2 {
                warn!(
                    "User {} has only {} recovery codes remaining",
                    user_id, remaining
                );
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Count remaining (unused) recovery codes
    pub async fn count_remaining(&self, user_id: Uuid) -> Result<u32> {
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM mfa_recovery_codes WHERE user_id = $1 AND used_at IS NULL",
        )
        .bind(user_id)
        .fetch_one(self.db)
        .await
        .context("Failed to count recovery codes")?;

        Ok(count.0 as u32)
    }

    /// Get all recovery codes (with used status) for display
    pub async fn list_codes(&self, user_id: Uuid) -> Result<Vec<RecoveryCode>> {
        let codes = sqlx::query_as::<_, RecoveryCode>(
            r#"
            SELECT id, user_id, code_hash, created_at, used_at
            FROM mfa_recovery_codes
            WHERE user_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(user_id)
        .fetch_all(self.db)
        .await
        .context("Failed to list recovery codes")?;

        Ok(codes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_code() {
        let code = RecoveryCodeManager::generate_code();
        assert_eq!(code.len(), RECOVERY_CODE_LENGTH);

        // Should only contain valid characters
        for c in code.chars() {
            assert!("ABCDEFGHJKLMNPQRSTUVWXYZ23456789".contains(c));
        }

        // Each code should be different
        let code2 = RecoveryCodeManager::generate_code();
        assert_ne!(code, code2);
    }

    #[test]
    fn test_format_code() {
        assert_eq!(RecoveryCodeManager::format_code("ABCDEFGH"), "ABCD-EFGH");
        assert_eq!(RecoveryCodeManager::format_code("ABC"), "ABC"); // Short codes unchanged
    }

    #[test]
    fn test_hash_code() {
        // Same code should produce same hash
        let hash1 = RecoveryCodeManager::hash_code("ABCD-EFGH");
        let hash2 = RecoveryCodeManager::hash_code("ABCDEFGH");
        let hash3 = RecoveryCodeManager::hash_code("abcd-efgh"); // Case insensitive

        assert_eq!(hash1, hash2);
        assert_eq!(hash1, hash3);

        // Different codes produce different hashes
        let hash4 = RecoveryCodeManager::hash_code("WXYZ1234");
        assert_ne!(hash1, hash4);
    }

    // ========== Additional Edge Case Tests ==========

    #[test]
    fn test_code_excludes_ambiguous_characters() {
        // Generate many codes and verify no ambiguous characters
        for _ in 0..1000 {
            let code = RecoveryCodeManager::generate_code();

            // Should not contain O, 0, I, 1 (ambiguous)
            assert!(!code.contains('O'), "Code contains O: {}", code);
            assert!(!code.contains('0'), "Code contains 0: {}", code);
            assert!(!code.contains('I'), "Code contains I: {}", code);
            assert!(!code.contains('1'), "Code contains 1: {}", code);
        }
    }

    #[test]
    fn test_code_uniqueness() {
        let mut codes = std::collections::HashSet::new();

        // Generate 1000 codes - all should be unique
        for _ in 0..1000 {
            let code = RecoveryCodeManager::generate_code();
            codes.insert(code);
        }

        // Extremely unlikely to have collisions in 1000 codes
        assert_eq!(codes.len(), 1000);
    }

    #[test]
    fn test_hash_is_sha256() {
        let hash = RecoveryCodeManager::hash_code("ABCDEFGH");

        // SHA256 produces 64 hex characters
        assert_eq!(hash.len(), 64);

        // Should be valid hex
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_hash_normalization_variations() {
        // All these variations should produce the same hash
        let variations = vec![
            "ABCD-EFGH",
            "abcd-efgh",
            "AbCd-EfGh",
            "ABCDEFGH",
            "abcdefgh",
            "aBcDeFgH",
            "ABCD--EFGH",      // Multiple dashes
            "A-B-C-D-E-F-G-H", // Many dashes
        ];

        let expected = RecoveryCodeManager::hash_code("ABCDEFGH");

        for variation in variations {
            let hash = RecoveryCodeManager::hash_code(variation);
            assert_eq!(hash, expected, "Hash mismatch for variation: {}", variation);
        }
    }

    #[test]
    fn test_format_code_edge_cases() {
        // Exact length (8 chars)
        assert_eq!(RecoveryCodeManager::format_code("12345678"), "1234-5678");

        // Shorter than expected
        assert_eq!(RecoveryCodeManager::format_code("ABC"), "ABC");
        assert_eq!(RecoveryCodeManager::format_code(""), "");

        // Longer than expected
        let long = "ABCDEFGHIJ";
        assert_eq!(RecoveryCodeManager::format_code(long), long);
    }

    #[test]
    fn test_empty_code_hash() {
        let hash = RecoveryCodeManager::hash_code("");
        // Empty string should still produce a valid hash
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_code_length_constant() {
        assert_eq!(RECOVERY_CODE_LENGTH, 8);
        assert_eq!(RECOVERY_CODE_COUNT, 10);
    }
}
