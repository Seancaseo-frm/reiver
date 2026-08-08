//! Cryptographic utilities for secret encryption and certificate management
//!
//! Provides:
//! - AES-256-GCM envelope encryption for sensitive data
//! - X.509 certificate generation for SAML SP signing
//!
//! # Key Management
//! The master encryption key should be provided via:
//! - `ENCRYPTION_KEY` environment variable (base64-encoded 32-byte key)
//! - In production, source this from AWS KMS, HashiCorp Vault, or similar

pub mod certificates;

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use rand::RngCore;
use std::fmt;
use thiserror::Error;

/// Errors that can occur during encryption/decryption
#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("Invalid encryption key: {0}")]
    InvalidKey(String),

    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),

    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),

    #[error("Invalid ciphertext format: {0}")]
    InvalidFormat(String),
}

/// Secret encryptor using AES-256-GCM
///
/// Format of encrypted data: base64(nonce || ciphertext || tag)
/// - nonce: 12 bytes
/// - ciphertext: variable length
/// - tag: 16 bytes (part of ciphertext from aes-gcm)
pub struct SecretEncryptor {
    cipher: Aes256Gcm,
}

impl SecretEncryptor {
    /// Create a new encryptor from a base64-encoded 32-byte key
    pub fn from_base64_key(key_base64: &str) -> Result<Self, CryptoError> {
        let key_bytes = BASE64
            .decode(key_base64.trim())
            .map_err(|e| CryptoError::InvalidKey(format!("Invalid base64: {}", e)))?;

        if key_bytes.len() != 32 {
            return Err(CryptoError::InvalidKey(format!(
                "Key must be 32 bytes, got {}",
                key_bytes.len()
            )));
        }

        let key: [u8; 32] = key_bytes
            .try_into()
            .map_err(|_| CryptoError::InvalidKey("Failed to convert key to array".to_string()))?;

        Self::from_key(key)
    }

    /// Create a new encryptor from a 32-byte key
    pub fn from_key(key: [u8; 32]) -> Result<Self, CryptoError> {
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|e| CryptoError::InvalidKey(format!("Failed to create cipher: {}", e)))?;

        Ok(Self { cipher })
    }

    /// Create an encryptor from the `ENCRYPTION_KEY` environment variable.
    pub fn from_env() -> Result<Self, anyhow::Error> {
        let key = std::env::var("ENCRYPTION_KEY")
            .map_err(|_| anyhow::anyhow!("ENCRYPTION_KEY not set"))?;
        Self::from_base64_key(&key).map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Generate a random 32-byte encryption key (for development/testing)
    pub fn generate_key() -> String {
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        BASE64.encode(key)
    }

    /// Encrypt plaintext and return base64-encoded ciphertext
    pub fn encrypt(&self, plaintext: &str) -> Result<String, CryptoError> {
        // Generate random 12-byte nonce
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Encrypt
        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|e| CryptoError::EncryptionFailed(format!("{}", e)))?;

        // Combine nonce + ciphertext and base64 encode
        let mut combined = Vec::with_capacity(12 + ciphertext.len());
        combined.extend_from_slice(&nonce_bytes);
        combined.extend_from_slice(&ciphertext);

        Ok(BASE64.encode(combined))
    }

    /// Decrypt base64-encoded ciphertext
    pub fn decrypt(&self, ciphertext_base64: &str) -> Result<String, CryptoError> {
        // Decode base64
        let combined = BASE64
            .decode(ciphertext_base64.trim())
            .map_err(|e| CryptoError::InvalidFormat(format!("Invalid base64: {}", e)))?;

        // Extract nonce (first 12 bytes) and ciphertext (rest)
        if combined.len() < 12 + 16 {
            // Minimum: 12 byte nonce + 16 byte tag
            return Err(CryptoError::InvalidFormat(
                "Ciphertext too short".to_string(),
            ));
        }

        let nonce = Nonce::from_slice(&combined[..12]);
        let ciphertext = &combined[12..];

        // Decrypt
        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| CryptoError::DecryptionFailed(format!("{}", e)))?;

        String::from_utf8(plaintext)
            .map_err(|e| CryptoError::DecryptionFailed(format!("Invalid UTF-8: {}", e)))
    }

    /// Try to decrypt a value to check if it's encrypted
    ///
    /// Note: This is more reliable than checking format, but has a performance cost.
    /// Only use when you genuinely need to determine if a value is encrypted.
    pub fn try_decrypt(&self, value: &str) -> Option<String> {
        self.decrypt(value).ok()
    }
}

impl fmt::Debug for SecretEncryptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretEncryptor")
            .field("cipher", &"<redacted>")
            .finish()
    }
}

// ============================================================================
// Key Rotation Support
// ============================================================================

/// Secret encryptor with key rotation support.
///
/// This encryptor supports seamless key rotation:
/// - Encrypts all new data with the primary (newest) key
/// - Decrypts data by trying the primary key first, then falling back to old keys
///
/// # Key Rotation Process
/// 1. Add the new key as primary in ENCRYPTION_KEY
/// 2. Move the old key to ENCRYPTION_KEY_OLD (comma-separated list for multiple old keys)
/// 3. Deploy the application - it will decrypt with old keys, encrypt with new
/// 4. Run re-encryption migration to update all secrets to new key
/// 5. Remove old keys from ENCRYPTION_KEY_OLD after confirming all secrets are migrated
///
/// # Example Usage
/// ```ignore
/// let encryptor = RotatingSecretEncryptor::from_env()?;
///
/// // Always encrypts with primary key
/// let encrypted = encryptor.encrypt("secret")?;
///
/// // Decrypts with primary key first, falls back to old keys
/// let decrypted = encryptor.decrypt(&encrypted)?;
///
/// // Re-encrypt to migrate to new key
/// let re_encrypted = encryptor.re_encrypt(&old_encrypted)?;
/// ```
pub struct RotatingSecretEncryptor {
    /// Primary encryptor (for new encryptions)
    primary: SecretEncryptor,
    /// Old encryptors for fallback decryption (oldest last)
    fallback_keys: Vec<SecretEncryptor>,
}

impl RotatingSecretEncryptor {
    /// Create a rotating encryptor from environment variables.
    ///
    /// Reads:
    /// - `ENCRYPTION_KEY`: Primary key (required, base64-encoded 32 bytes)
    /// - `ENCRYPTION_KEY_OLD`: Comma-separated list of old keys for fallback decryption
    pub fn from_env() -> Result<Self, CryptoError> {
        let primary_key = std::env::var("ENCRYPTION_KEY")
            .map_err(|_| CryptoError::InvalidKey("ENCRYPTION_KEY not set".to_string()))?;

        let primary = SecretEncryptor::from_base64_key(&primary_key)?;

        let fallback_keys = match std::env::var("ENCRYPTION_KEY_OLD") {
            Ok(old_keys) if !old_keys.is_empty() => old_keys
                .split(',')
                .filter(|k| !k.trim().is_empty())
                .map(|k| SecretEncryptor::from_base64_key(k.trim()))
                .collect::<Result<Vec<_>, _>>()?,
            _ => Vec::new(),
        };

        Ok(Self {
            primary,
            fallback_keys,
        })
    }

    /// Create from explicit primary and fallback keys.
    pub fn new(primary_key: &str, fallback_keys: Vec<&str>) -> Result<Self, CryptoError> {
        let primary = SecretEncryptor::from_base64_key(primary_key)?;
        let fallback_keys = fallback_keys
            .into_iter()
            .map(|k| SecretEncryptor::from_base64_key(k))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            primary,
            fallback_keys,
        })
    }

    /// Create with a single key (no rotation).
    pub fn single_key(key: &str) -> Result<Self, CryptoError> {
        Ok(Self {
            primary: SecretEncryptor::from_base64_key(key)?,
            fallback_keys: Vec::new(),
        })
    }

    /// Encrypt plaintext using the primary (newest) key.
    pub fn encrypt(&self, plaintext: &str) -> Result<String, CryptoError> {
        self.primary.encrypt(plaintext)
    }

    /// Decrypt ciphertext, trying primary key first then fallback keys.
    pub fn decrypt(&self, ciphertext: &str) -> Result<String, CryptoError> {
        // Try primary key first
        if let Ok(plaintext) = self.primary.decrypt(ciphertext) {
            return Ok(plaintext);
        }

        // Try fallback keys (most recently rotated first)
        for fallback in &self.fallback_keys {
            if let Ok(plaintext) = fallback.decrypt(ciphertext) {
                return Ok(plaintext);
            }
        }

        // All keys failed
        Err(CryptoError::DecryptionFailed(
            "Failed to decrypt with any available key".to_string(),
        ))
    }

    /// Re-encrypt a value using the primary key.
    ///
    /// This decrypts with any available key and re-encrypts with the primary key.
    /// Use this to migrate secrets to the new key after rotation.
    pub fn re_encrypt(&self, ciphertext: &str) -> Result<String, CryptoError> {
        let plaintext = self.decrypt(ciphertext)?;
        self.encrypt(&plaintext)
    }

    /// Check if a value needs re-encryption (i.e., was encrypted with an old key).
    ///
    /// Returns true if the value was encrypted with a fallback key.
    /// Returns false if encrypted with primary key or not valid encrypted data.
    pub fn needs_re_encryption(&self, ciphertext: &str) -> bool {
        // If primary key can decrypt it, no re-encryption needed
        if self.primary.decrypt(ciphertext).is_ok() {
            return false;
        }

        // Check if any fallback key can decrypt it
        for fallback in &self.fallback_keys {
            if fallback.decrypt(ciphertext).is_ok() {
                return true;
            }
        }

        // Not valid encrypted data with any key
        false
    }

    /// Get the number of fallback keys configured.
    pub fn fallback_key_count(&self) -> usize {
        self.fallback_keys.len()
    }
}

impl fmt::Debug for RotatingSecretEncryptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RotatingSecretEncryptor")
            .field("primary", &"<redacted>")
            .field("fallback_key_count", &self.fallback_keys.len())
            .finish()
    }
}

/// Trait for secret encryption to allow testing with mock implementations.
pub trait SecretEncrypt: Send + Sync {
    fn encrypt(&self, plaintext: &str) -> Result<String, CryptoError>;
    fn decrypt(&self, ciphertext: &str) -> Result<String, CryptoError>;
}

impl SecretEncrypt for SecretEncryptor {
    fn encrypt(&self, plaintext: &str) -> Result<String, CryptoError> {
        self.encrypt(plaintext)
    }

    fn decrypt(&self, ciphertext: &str) -> Result<String, CryptoError> {
        self.decrypt(ciphertext)
    }
}

impl SecretEncrypt for RotatingSecretEncryptor {
    fn encrypt(&self, plaintext: &str) -> Result<String, CryptoError> {
        self.encrypt(plaintext)
    }

    fn decrypt(&self, ciphertext: &str) -> Result<String, CryptoError> {
        self.decrypt(ciphertext)
    }
}

// ============================================================================
// Secret Value Wrapper
// ============================================================================

/// A wrapper type for sensitive strings that prevents accidental logging.
///
/// This type wraps a `String` and implements `Debug` and `Display` to mask
/// the value, preventing accidental exposure in logs, error messages, or
/// debug output.
///
/// # Example
///
/// ```
/// use reiver_core::crypto::SecretString;
///
/// let api_key = SecretString::new("sk_live_abc123");
/// println!("{:?}", api_key);  // Prints: SecretString(***REDACTED***)
/// println!("{}", api_key);    // Prints: ***REDACTED***
///
/// // Access the actual value when needed
/// let actual_key = api_key.expose();
/// ```
#[derive(Clone)]
pub struct SecretString {
    value: String,
}

impl SecretString {
    /// Create a new secret string.
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    /// Expose the secret value.
    ///
    /// Use this method only when you actually need to use the secret value
    /// (e.g., sending it to an API). Avoid logging or displaying the result.
    pub fn expose(&self) -> &str {
        &self.value
    }

    /// Consume and return the inner value.
    pub fn into_inner(self) -> String {
        self.value
    }

    /// Check if the secret is empty.
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Get the length of the secret (without exposing the value).
    pub fn len(&self) -> usize {
        self.value.len()
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SecretString")
            .field(&"***REDACTED***")
            .finish()
    }
}

impl std::fmt::Display for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "***REDACTED***")
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl PartialEq for SecretString {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for SecretString {}

/// A placeholder encryptor that does no encryption (for testing only)
///
/// This struct is only available in test builds to prevent accidental
/// use in production code.
#[cfg(test)]
pub struct NoOpEncryptor;

#[cfg(test)]
impl NoOpEncryptor {
    pub fn encrypt(&self, plaintext: &str) -> Result<String, CryptoError> {
        Ok(plaintext.to_string())
    }

    pub fn decrypt(&self, ciphertext: &str) -> Result<String, CryptoError> {
        Ok(ciphertext.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_key() {
        let key = SecretEncryptor::generate_key();
        assert_eq!(BASE64.decode(&key).unwrap().len(), 32);
    }

    #[test]
    fn test_encrypt_decrypt() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        let plaintext = "my-secret-client-secret";
        let ciphertext = encryptor.encrypt(plaintext).unwrap();

        // Ciphertext should be different from plaintext
        assert_ne!(ciphertext, plaintext);

        // Decryption should return original
        let decrypted = encryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_different_nonces() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        let plaintext = "test";
        let ciphertext1 = encryptor.encrypt(plaintext).unwrap();
        let ciphertext2 = encryptor.encrypt(plaintext).unwrap();

        // Same plaintext should produce different ciphertexts
        assert_ne!(ciphertext1, ciphertext2);

        // Both should decrypt to same value
        assert_eq!(encryptor.decrypt(&ciphertext1).unwrap(), plaintext);
        assert_eq!(encryptor.decrypt(&ciphertext2).unwrap(), plaintext);
    }

    #[test]
    fn test_invalid_key_length() {
        let result = SecretEncryptor::from_base64_key(&BASE64.encode([0u8; 16]));
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_ciphertext() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        // Too short
        assert!(encryptor.decrypt("abc").is_err());

        // Invalid base64
        assert!(encryptor.decrypt("not-base64!!!").is_err());

        // Valid base64 but wrong key
        let other_key = SecretEncryptor::generate_key();
        let other_encryptor = SecretEncryptor::from_base64_key(&other_key).unwrap();
        let ciphertext = other_encryptor.encrypt("secret").unwrap();
        assert!(encryptor.decrypt(&ciphertext).is_err());
    }

    #[test]
    fn test_try_decrypt() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        // Plain string - should fail to decrypt
        assert!(encryptor.try_decrypt("my-plain-secret").is_none());

        // Encrypted string - should decrypt successfully
        let encrypted = encryptor.encrypt("secret").unwrap();
        assert_eq!(
            encryptor.try_decrypt(&encrypted),
            Some("secret".to_string())
        );
    }

    // ========== Additional Edge Case Tests ==========

    #[test]
    fn test_empty_string_encryption() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        let ciphertext = encryptor.encrypt("").unwrap();
        let decrypted = encryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, "");
    }

    #[test]
    fn test_long_string_encryption() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        // 1MB string
        let plaintext: String = "x".repeat(1024 * 1024);
        let ciphertext = encryptor.encrypt(&plaintext).unwrap();
        let decrypted = encryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_unicode_string_encryption() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        // Various unicode characters
        let plaintexts = vec![
            "日本語テスト",
            "émoji: 🔐🔑🛡️",
            "مرحبا بالعالم",
            "Здравствуй мир",
            "mixed: ABC日本語🎉123",
        ];

        for plaintext in plaintexts {
            let ciphertext = encryptor.encrypt(plaintext).unwrap();
            let decrypted = encryptor.decrypt(&ciphertext).unwrap();
            assert_eq!(decrypted, plaintext);
        }
    }

    #[test]
    fn test_special_characters_encryption() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        let plaintext = r#"special: \n\t\r\0"'<>&;$`|"#;
        let ciphertext = encryptor.encrypt(plaintext).unwrap();
        let decrypted = encryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_null_bytes_encryption() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        let plaintext = "before\0after";
        let ciphertext = encryptor.encrypt(plaintext).unwrap();
        let decrypted = encryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_key_with_whitespace() {
        // Key with leading/trailing whitespace should be trimmed
        let key = SecretEncryptor::generate_key();
        let key_with_whitespace = format!("  {}  \n", key);

        let encryptor = SecretEncryptor::from_base64_key(&key_with_whitespace).unwrap();
        let ciphertext = encryptor.encrypt("test").unwrap();

        let encryptor2 = SecretEncryptor::from_base64_key(&key).unwrap();
        let decrypted = encryptor2.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, "test");
    }

    #[test]
    fn test_invalid_base64_key() {
        let result = SecretEncryptor::from_base64_key("not-valid-base64!!!");
        assert!(matches!(result, Err(CryptoError::InvalidKey(_))));
    }

    #[test]
    fn test_ciphertext_tampering_detected() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        let ciphertext = encryptor.encrypt("secret").unwrap();
        let decoded = BASE64.decode(&ciphertext).unwrap();

        // Tamper with the ciphertext (flip a bit in the middle)
        let mut tampered = decoded.clone();
        if tampered.len() > 15 {
            tampered[15] ^= 1;
        }
        let tampered_encoded = BASE64.encode(&tampered);

        // Should fail to decrypt
        assert!(encryptor.decrypt(&tampered_encoded).is_err());
    }

    #[test]
    fn test_nonce_uniqueness() {
        let key = SecretEncryptor::generate_key();
        let encryptor = SecretEncryptor::from_base64_key(&key).unwrap();

        // Encrypt the same message many times and verify nonces are unique
        let mut nonces = std::collections::HashSet::new();
        for _ in 0..1000 {
            let ciphertext = encryptor.encrypt("test").unwrap();
            let decoded = BASE64.decode(&ciphertext).unwrap();
            let nonce: [u8; 12] = decoded[..12].try_into().unwrap();
            nonces.insert(nonce);
        }

        // All 1000 nonces should be unique (collision is astronomically unlikely)
        assert_eq!(nonces.len(), 1000);
    }

    #[test]
    fn test_crypto_error_display() {
        let err = CryptoError::InvalidKey("test error".to_string());
        assert!(err.to_string().contains("Invalid encryption key"));

        let err = CryptoError::EncryptionFailed("test".to_string());
        assert!(err.to_string().contains("Encryption failed"));

        let err = CryptoError::DecryptionFailed("test".to_string());
        assert!(err.to_string().contains("Decryption failed"));

        let err = CryptoError::InvalidFormat("test".to_string());
        assert!(err.to_string().contains("Invalid ciphertext format"));
    }

    #[test]
    fn test_noop_encryptor() {
        let encryptor = NoOpEncryptor;

        let plaintext = "secret";
        let encrypted = encryptor.encrypt(plaintext).unwrap();
        assert_eq!(encrypted, plaintext); // NoOp doesn't actually encrypt

        let decrypted = encryptor.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    // ========== Rotating Encryptor Tests ==========

    #[test]
    fn test_rotating_encryptor_single_key() {
        let key = SecretEncryptor::generate_key();
        let encryptor = RotatingSecretEncryptor::single_key(&key).unwrap();

        let plaintext = "my-secret";
        let ciphertext = encryptor.encrypt(plaintext).unwrap();
        let decrypted = encryptor.decrypt(&ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
        assert_eq!(encryptor.fallback_key_count(), 0);
    }

    #[test]
    fn test_rotating_encryptor_with_fallback() {
        let old_key = SecretEncryptor::generate_key();
        let new_key = SecretEncryptor::generate_key();

        // Encrypt with old key
        let old_encryptor = SecretEncryptor::from_base64_key(&old_key).unwrap();
        let ciphertext_old = old_encryptor.encrypt("secret").unwrap();

        // Create rotating encryptor with new key as primary, old as fallback
        let rotating = RotatingSecretEncryptor::new(&new_key, vec![&old_key]).unwrap();

        // Should be able to decrypt old ciphertext
        let decrypted = rotating.decrypt(&ciphertext_old).unwrap();
        assert_eq!(decrypted, "secret");

        // New encryptions use new key
        let ciphertext_new = rotating.encrypt("new-secret").unwrap();

        // New encryptor (without fallback) should be able to decrypt new ciphertext
        let new_only = SecretEncryptor::from_base64_key(&new_key).unwrap();
        assert_eq!(new_only.decrypt(&ciphertext_new).unwrap(), "new-secret");

        // But old encryptor should NOT be able to decrypt new ciphertext
        assert!(old_encryptor.decrypt(&ciphertext_new).is_err());
    }

    #[test]
    fn test_rotating_encryptor_re_encrypt() {
        let old_key = SecretEncryptor::generate_key();
        let new_key = SecretEncryptor::generate_key();

        // Encrypt with old key
        let old_encryptor = SecretEncryptor::from_base64_key(&old_key).unwrap();
        let ciphertext_old = old_encryptor.encrypt("secret").unwrap();

        // Create rotating encryptor
        let rotating = RotatingSecretEncryptor::new(&new_key, vec![&old_key]).unwrap();

        // Re-encrypt to new key
        let ciphertext_new = rotating.re_encrypt(&ciphertext_old).unwrap();

        // New ciphertext should be decryptable by new key only
        let new_only = SecretEncryptor::from_base64_key(&new_key).unwrap();
        assert_eq!(new_only.decrypt(&ciphertext_new).unwrap(), "secret");
    }

    #[test]
    fn test_rotating_encryptor_needs_re_encryption() {
        let old_key = SecretEncryptor::generate_key();
        let new_key = SecretEncryptor::generate_key();

        // Encrypt with old key
        let old_encryptor = SecretEncryptor::from_base64_key(&old_key).unwrap();
        let ciphertext_old = old_encryptor.encrypt("secret").unwrap();

        // Encrypt with new key
        let new_encryptor = SecretEncryptor::from_base64_key(&new_key).unwrap();
        let ciphertext_new = new_encryptor.encrypt("secret").unwrap();

        // Create rotating encryptor
        let rotating = RotatingSecretEncryptor::new(&new_key, vec![&old_key]).unwrap();

        // Old ciphertext needs re-encryption
        assert!(rotating.needs_re_encryption(&ciphertext_old));

        // New ciphertext does NOT need re-encryption
        assert!(!rotating.needs_re_encryption(&ciphertext_new));

        // Invalid data doesn't need re-encryption (returns false, not error)
        assert!(!rotating.needs_re_encryption("not-encrypted"));
    }

    #[test]
    fn test_rotating_encryptor_multiple_fallbacks() {
        let oldest_key = SecretEncryptor::generate_key();
        let old_key = SecretEncryptor::generate_key();
        let new_key = SecretEncryptor::generate_key();

        // Encrypt with oldest key
        let oldest_encryptor = SecretEncryptor::from_base64_key(&oldest_key).unwrap();
        let ciphertext_oldest = oldest_encryptor.encrypt("oldest-secret").unwrap();

        // Encrypt with old key
        let old_encryptor = SecretEncryptor::from_base64_key(&old_key).unwrap();
        let ciphertext_old = old_encryptor.encrypt("old-secret").unwrap();

        // Create rotating encryptor with multiple fallbacks
        let rotating = RotatingSecretEncryptor::new(&new_key, vec![&old_key, &oldest_key]).unwrap();

        assert_eq!(rotating.fallback_key_count(), 2);

        // Should decrypt both old ciphertexts
        assert_eq!(
            rotating.decrypt(&ciphertext_oldest).unwrap(),
            "oldest-secret"
        );
        assert_eq!(rotating.decrypt(&ciphertext_old).unwrap(), "old-secret");
    }

    #[test]
    fn test_rotating_encryptor_all_keys_fail() {
        let key1 = SecretEncryptor::generate_key();
        let key2 = SecretEncryptor::generate_key();
        let key3 = SecretEncryptor::generate_key();

        // Encrypt with a completely different key
        let other_encryptor = SecretEncryptor::from_base64_key(&key3).unwrap();
        let ciphertext = other_encryptor.encrypt("secret").unwrap();

        // Create rotating encryptor without key3
        let rotating = RotatingSecretEncryptor::new(&key1, vec![&key2]).unwrap();

        // Should fail to decrypt
        assert!(rotating.decrypt(&ciphertext).is_err());
    }
}
