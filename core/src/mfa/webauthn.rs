//! WebAuthn (FIDO2) implementation
//!
//! Supports hardware security keys (YubiKey, etc.) and platform authenticators
//! (Touch ID, Windows Hello, etc.)

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

/// WebAuthn credential stored in database
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WebAuthnCredential {
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

/// Registration challenge stored temporarily
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationChallenge {
    pub user_id: Uuid,
    pub challenge: Vec<u8>,
    pub rp_id: String,
    pub user_name: String,
    pub user_display_name: String,
    pub created_at: DateTime<Utc>,
}

/// Authentication challenge stored temporarily
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationChallenge {
    pub user_id: Uuid,
    pub challenge: Vec<u8>,
    pub rp_id: String,
    pub allowed_credentials: Vec<Vec<u8>>,
    pub created_at: DateTime<Utc>,
}

/// WebAuthn configuration
#[derive(Debug, Clone)]
pub struct WebAuthnConfig {
    /// Relying Party ID (usually the domain)
    pub rp_id: String,
    /// Relying Party name
    pub rp_name: String,
    /// Origin for verification
    pub origin: String,
    /// Timeout for ceremonies (in milliseconds)
    pub timeout: u32,
}

impl Default for WebAuthnConfig {
    fn default() -> Self {
        Self {
            rp_id: "localhost".to_string(),
            rp_name: "Reiver".to_string(),
            origin: "http://localhost:3000".to_string(),
            timeout: 60000,
        }
    }
}

/// WebAuthn manager
pub struct WebAuthnManager<'a> {
    db: &'a PgPool,
    config: WebAuthnConfig,
}

impl<'a> WebAuthnManager<'a> {
    pub fn new(db: &'a PgPool, config: WebAuthnConfig) -> Self {
        Self { db, config }
    }

    /// Generate a random challenge (32 bytes)
    pub fn generate_challenge() -> Vec<u8> {
        use rand::RngCore;
        let mut challenge = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut challenge);
        challenge
    }

    /// Start registration ceremony
    pub async fn start_registration(
        &self,
        user_id: Uuid,
        user_name: &str,
        user_display_name: &str,
        redis: &bb8::Pool<bb8_redis::RedisConnectionManager>,
    ) -> Result<RegistrationOptions> {
        let challenge = Self::generate_challenge();

        // Get existing credentials to exclude
        let existing: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT credential_id FROM webauthn_credentials WHERE user_id = $1")
                .bind(user_id)
                .fetch_all(self.db)
                .await
                .context("Failed to fetch existing credentials")?;

        let exclude_credentials: Vec<Vec<u8>> = existing.into_iter().map(|(c,)| c).collect();

        // Store challenge in Redis (expires in 5 minutes)
        let challenge_data = RegistrationChallenge {
            user_id,
            challenge: challenge.clone(),
            rp_id: self.config.rp_id.clone(),
            user_name: user_name.to_string(),
            user_display_name: user_display_name.to_string(),
            created_at: Utc::now(),
        };

        let challenge_key = format!("webauthn:reg:{}", user_id);
        let challenge_json = serde_json::to_string(&challenge_data)?;

        let mut conn = redis
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("Redis connection failed: {}", e))?;

        redis::cmd("SETEX")
            .arg(&challenge_key)
            .arg(300) // 5 minutes
            .arg(&challenge_json)
            .query_async::<()>(&mut *conn)
            .await
            .context("Failed to store registration challenge")?;

        Ok(RegistrationOptions {
            challenge: base64_url_encode(&challenge),
            rp: RelyingParty {
                id: self.config.rp_id.clone(),
                name: self.config.rp_name.clone(),
            },
            user: UserEntity {
                id: base64_url_encode(user_id.as_bytes()),
                name: user_name.to_string(),
                display_name: user_display_name.to_string(),
            },
            pub_key_cred_params: vec![
                PubKeyCredParam {
                    alg: -7,
                    type_: "public-key".to_string(),
                }, // ES256
                PubKeyCredParam {
                    alg: -257,
                    type_: "public-key".to_string(),
                }, // RS256
            ],
            timeout: self.config.timeout,
            exclude_credentials: exclude_credentials
                .iter()
                .map(|c| CredentialDescriptor {
                    id: base64_url_encode(c),
                    type_: "public-key".to_string(),
                })
                .collect(),
            authenticator_selection: AuthenticatorSelection {
                authenticator_attachment: None,
                resident_key: Some("preferred".to_string()),
                user_verification: "preferred".to_string(),
            },
            attestation: "none".to_string(),
        })
    }

    /// Complete registration ceremony
    pub async fn complete_registration(
        &self,
        user_id: Uuid,
        credential_id: &[u8],
        public_key: &[u8],
        counter: u32,
        aaguid: Option<&[u8]>,
        name: &str,
        redis: &bb8::Pool<bb8_redis::RedisConnectionManager>,
    ) -> Result<Uuid> {
        // Verify challenge exists and belongs to user
        let challenge_key = format!("webauthn:reg:{}", user_id);

        let mut conn = redis
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("Redis connection failed: {}", e))?;

        let challenge_json: Option<String> = redis::cmd("GET")
            .arg(&challenge_key)
            .query_async(&mut *conn)
            .await
            .context("Failed to get registration challenge")?;

        if challenge_json.is_none() {
            return Err(anyhow::anyhow!(
                "Registration challenge expired or not found"
            ));
        }

        // Delete the challenge
        redis::cmd("DEL")
            .arg(&challenge_key)
            .query_async::<()>(&mut *conn)
            .await
            .ok();

        // Store the credential
        let row: (Uuid,) = sqlx::query_as(
            r#"
            INSERT INTO webauthn_credentials (
                user_id, credential_id, public_key, counter, name, aaguid
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            "#,
        )
        .bind(user_id)
        .bind(credential_id)
        .bind(public_key)
        .bind(counter as i64)
        .bind(name)
        .bind(aaguid)
        .fetch_one(self.db)
        .await
        .context("Failed to store credential")?;

        // Also create MFA enrollment record
        sqlx::query(
            r#"
            INSERT INTO mfa_enrollments (user_id, method, name, is_primary)
            VALUES ($1, 'webauthn', $2, false)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(user_id)
        .bind(name)
        .execute(self.db)
        .await
        .ok();

        info!(
            "Registered WebAuthn credential {} for user {}",
            row.0, user_id
        );

        Ok(row.0)
    }

    /// Start authentication ceremony
    pub async fn start_authentication(
        &self,
        user_id: Uuid,
        redis: &bb8::Pool<bb8_redis::RedisConnectionManager>,
    ) -> Result<AuthenticationOptions> {
        let challenge = Self::generate_challenge();

        // Get user's credentials
        let credentials: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT credential_id FROM webauthn_credentials WHERE user_id = $1")
                .bind(user_id)
                .fetch_all(self.db)
                .await
                .context("Failed to fetch credentials")?;

        if credentials.is_empty() {
            return Err(anyhow::anyhow!("No WebAuthn credentials found for user"));
        }

        let allowed: Vec<Vec<u8>> = credentials.into_iter().map(|(c,)| c).collect();

        // Store challenge in Redis
        let challenge_data = AuthenticationChallenge {
            user_id,
            challenge: challenge.clone(),
            rp_id: self.config.rp_id.clone(),
            allowed_credentials: allowed.clone(),
            created_at: Utc::now(),
        };

        let challenge_key = format!("webauthn:auth:{}", user_id);
        let challenge_json = serde_json::to_string(&challenge_data)?;

        let mut conn = redis
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("Redis connection failed: {}", e))?;

        redis::cmd("SETEX")
            .arg(&challenge_key)
            .arg(300)
            .arg(&challenge_json)
            .query_async::<()>(&mut *conn)
            .await
            .context("Failed to store auth challenge")?;

        Ok(AuthenticationOptions {
            challenge: base64_url_encode(&challenge),
            timeout: self.config.timeout,
            rp_id: self.config.rp_id.clone(),
            allow_credentials: allowed
                .iter()
                .map(|c| CredentialDescriptor {
                    id: base64_url_encode(c),
                    type_: "public-key".to_string(),
                })
                .collect(),
            user_verification: "preferred".to_string(),
        })
    }

    /// Complete authentication ceremony
    ///
    /// # Arguments
    /// * `user_id` - The user being authenticated
    /// * `credential_id` - The credential ID from the authenticator
    /// * `authenticator_data` - Raw authenticator data bytes
    /// * `client_data_json` - Raw client data JSON bytes
    /// * `signature` - The signature from the authenticator
    /// * `redis` - Redis connection pool for challenge storage
    pub async fn complete_authentication(
        &self,
        user_id: Uuid,
        credential_id: &[u8],
        authenticator_data: &[u8],
        client_data_json: &[u8],
        signature: &[u8],
        redis: &bb8::Pool<bb8_redis::RedisConnectionManager>,
    ) -> Result<bool> {
        use sha2::{Digest, Sha256};

        // Verify challenge exists
        let challenge_key = format!("webauthn:auth:{}", user_id);

        let mut conn = redis
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("Redis connection failed: {}", e))?;

        let challenge_json: Option<String> = redis::cmd("GET")
            .arg(&challenge_key)
            .query_async(&mut *conn)
            .await
            .context("Failed to get auth challenge")?;

        let challenge_data: AuthenticationChallenge = match challenge_json {
            Some(json) => serde_json::from_str(&json).context("Failed to parse challenge")?,
            None => return Err(anyhow::anyhow!("Auth challenge expired or not found")),
        };

        // Delete the challenge (single use)
        redis::cmd("DEL")
            .arg(&challenge_key)
            .query_async::<()>(&mut *conn)
            .await
            .ok();

        // Verify the credential is in the allowed list
        if !challenge_data
            .allowed_credentials
            .iter()
            .any(|c| c == credential_id)
        {
            return Err(anyhow::anyhow!("Credential not in allowed list"));
        }

        // Parse client data and verify challenge
        let client_data: serde_json::Value =
            serde_json::from_slice(client_data_json).context("Failed to parse clientDataJSON")?;

        let received_challenge = client_data
            .get("challenge")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing challenge in clientDataJSON"))?;

        let expected_challenge = base64_url_encode(&challenge_data.challenge);
        if received_challenge != expected_challenge {
            return Err(anyhow::anyhow!("Challenge mismatch"));
        }

        // Verify origin
        let origin = client_data
            .get("origin")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing origin in clientDataJSON"))?;

        if origin != self.config.origin {
            warn!(
                "Origin mismatch: expected {}, got {}",
                self.config.origin, origin
            );
            return Err(anyhow::anyhow!("Origin mismatch"));
        }

        // Verify type is webauthn.get
        let type_ = client_data
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing type in clientDataJSON"))?;

        if type_ != "webauthn.get" {
            return Err(anyhow::anyhow!("Invalid ceremony type"));
        }

        // Get stored credential (public key and counter)
        let cred: Option<(Vec<u8>, i64)> = sqlx::query_as(
            "SELECT public_key, counter FROM webauthn_credentials WHERE user_id = $1 AND credential_id = $2"
        )
        .bind(user_id)
        .bind(credential_id)
        .fetch_optional(self.db)
        .await
        .context("Failed to fetch credential")?;

        let (public_key, stored_counter) = match cred {
            Some((pk, c)) => (pk, c as u32),
            None => return Err(anyhow::anyhow!("Credential not found")),
        };

        // Extract counter from authenticator data (bytes 33-36, big-endian)
        if authenticator_data.len() < 37 {
            return Err(anyhow::anyhow!("Authenticator data too short"));
        }
        let new_counter = u32::from_be_bytes([
            authenticator_data[33],
            authenticator_data[34],
            authenticator_data[35],
            authenticator_data[36],
        ]);

        // Counter must be greater than stored (replay protection)
        if new_counter <= stored_counter {
            warn!(
                "WebAuthn counter replay attack detected for user {}",
                user_id
            );
            return Err(anyhow::anyhow!("Counter replay detected"));
        }

        // Verify signature
        // The signed data is: authenticator_data || SHA256(client_data_json)
        let client_data_hash = Sha256::digest(client_data_json);
        let mut signed_data = Vec::with_capacity(authenticator_data.len() + 32);
        signed_data.extend_from_slice(authenticator_data);
        signed_data.extend_from_slice(&client_data_hash);

        // Verify the signature using the stored public key
        // The public key is stored in COSE format; we need to verify appropriately
        if !verify_webauthn_signature(&public_key, &signed_data, signature)? {
            warn!(
                "WebAuthn signature verification failed for user {}",
                user_id
            );
            return Err(anyhow::anyhow!("Signature verification failed"));
        }

        // Update counter and last used
        sqlx::query(
            r#"
            UPDATE webauthn_credentials
            SET counter = $1, last_used_at = NOW()
            WHERE user_id = $2 AND credential_id = $3
            "#,
        )
        .bind(new_counter as i64)
        .bind(user_id)
        .bind(credential_id)
        .execute(self.db)
        .await
        .context("Failed to update credential")?;

        info!("WebAuthn authentication successful for user {}", user_id);

        Ok(true)
    }

    /// List user's WebAuthn credentials
    pub async fn list_credentials(&self, user_id: Uuid) -> Result<Vec<WebAuthnCredential>> {
        let creds = sqlx::query_as::<_, WebAuthnCredential>(
            r#"
            SELECT id, user_id, credential_id, public_key, counter, name, aaguid, created_at, last_used_at
            FROM webauthn_credentials
            WHERE user_id = $1
            ORDER BY created_at DESC
            "#
        )
        .bind(user_id)
        .fetch_all(self.db)
        .await
        .context("Failed to list credentials")?;

        Ok(creds)
    }

    /// Remove a credential
    pub async fn remove_credential(&self, credential_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM webauthn_credentials WHERE id = $1")
            .bind(credential_id)
            .execute(self.db)
            .await
            .context("Failed to remove credential")?;

        info!("Removed WebAuthn credential {}", credential_id);
        Ok(())
    }
}

// WebAuthn API types for JSON responses

#[derive(Debug, Serialize)]
pub struct RegistrationOptions {
    pub challenge: String,
    pub rp: RelyingParty,
    pub user: UserEntity,
    #[serde(rename = "pubKeyCredParams")]
    pub pub_key_cred_params: Vec<PubKeyCredParam>,
    pub timeout: u32,
    #[serde(rename = "excludeCredentials")]
    pub exclude_credentials: Vec<CredentialDescriptor>,
    #[serde(rename = "authenticatorSelection")]
    pub authenticator_selection: AuthenticatorSelection,
    pub attestation: String,
}

#[derive(Debug, Serialize)]
pub struct AuthenticationOptions {
    pub challenge: String,
    pub timeout: u32,
    #[serde(rename = "rpId")]
    pub rp_id: String,
    #[serde(rename = "allowCredentials")]
    pub allow_credentials: Vec<CredentialDescriptor>,
    #[serde(rename = "userVerification")]
    pub user_verification: String,
}

#[derive(Debug, Serialize)]
pub struct RelyingParty {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct UserEntity {
    pub id: String,
    pub name: String,
    #[serde(rename = "displayName")]
    pub display_name: String,
}

#[derive(Debug, Serialize)]
pub struct PubKeyCredParam {
    pub alg: i32,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Serialize)]
pub struct CredentialDescriptor {
    pub id: String,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Serialize)]
pub struct AuthenticatorSelection {
    #[serde(
        rename = "authenticatorAttachment",
        skip_serializing_if = "Option::is_none"
    )]
    pub authenticator_attachment: Option<String>,
    #[serde(rename = "residentKey", skip_serializing_if = "Option::is_none")]
    pub resident_key: Option<String>,
    #[serde(rename = "userVerification")]
    pub user_verification: String,
}

/// Base64 URL-safe encoding (no padding)
fn base64_url_encode(data: &[u8]) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    URL_SAFE_NO_PAD.encode(data)
}

/// Verify a WebAuthn signature using the stored COSE public key
///
/// Supports ES256 (ECDSA with P-256) and RS256 (RSA with SHA-256)
fn verify_webauthn_signature(
    cose_public_key: &[u8],
    signed_data: &[u8],
    signature: &[u8],
) -> Result<bool> {
    use ring::signature;

    // Parse COSE key to determine algorithm
    // COSE key is CBOR-encoded; we need to extract the key type and algorithm
    // Simplified parsing for common cases (ES256 and RS256)

    // Try to parse as CBOR map
    let key_map: std::collections::BTreeMap<i128, ciborium::Value> =
        ciborium::from_reader(cose_public_key)
            .map_err(|e| anyhow::anyhow!("Failed to parse COSE key: {}", e))?;

    // Key type (kty) is at label 1
    let kty = key_map
        .get(&1)
        .and_then(|v| v.as_integer())
        .map(|i| i128::from(i))
        .ok_or_else(|| anyhow::anyhow!("Missing key type in COSE key"))?;

    // Algorithm is at label 3
    let alg = key_map
        .get(&3)
        .and_then(|v| v.as_integer())
        .map(|i| i128::from(i))
        .ok_or_else(|| anyhow::anyhow!("Missing algorithm in COSE key"))?;

    match (kty, alg) {
        // EC2 key type (2) with ES256 algorithm (-7)
        (2, -7) => {
            // Extract x and y coordinates (labels -2 and -3)
            let x = key_map
                .get(&(-2i128))
                .and_then(|v| v.as_bytes())
                .ok_or_else(|| anyhow::anyhow!("Missing x coordinate"))?;
            let y = key_map
                .get(&(-3i128))
                .and_then(|v| v.as_bytes())
                .ok_or_else(|| anyhow::anyhow!("Missing y coordinate"))?;

            // Build uncompressed point format: 0x04 || x || y
            let mut public_key_bytes = Vec::with_capacity(1 + x.len() + y.len());
            public_key_bytes.push(0x04);
            public_key_bytes.extend_from_slice(x);
            public_key_bytes.extend_from_slice(y);

            // Parse the public key
            let public_key = signature::UnparsedPublicKey::new(
                &signature::ECDSA_P256_SHA256_ASN1,
                &public_key_bytes,
            );

            // Verify (signature from WebAuthn is already in ASN.1 DER format for ES256)
            public_key
                .verify(signed_data, signature)
                .map(|_| true)
                .map_err(|_| anyhow::anyhow!("ES256 signature verification failed"))
        }

        // RSA key type (3) with RS256 algorithm (-257)
        (3, -257) => {
            // Extract n and e (labels -1 and -2)
            let n = key_map
                .get(&(-1i128))
                .and_then(|v| v.as_bytes())
                .ok_or_else(|| anyhow::anyhow!("Missing RSA modulus n"))?;
            let e = key_map
                .get(&(-2i128))
                .and_then(|v| v.as_bytes())
                .ok_or_else(|| anyhow::anyhow!("Missing RSA exponent e"))?;

            // Build RSA public key in DER format
            // This is simplified; a full implementation would use proper ASN.1 encoding
            let rsa_key = build_rsa_public_key_der(n, e)?;

            let public_key =
                signature::UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA256, &rsa_key);

            public_key
                .verify(signed_data, signature)
                .map(|_| true)
                .map_err(|_| anyhow::anyhow!("RS256 signature verification failed"))
        }

        _ => Err(anyhow::anyhow!(
            "Unsupported COSE key type {} or algorithm {}",
            kty,
            alg
        )),
    }
}

/// Build an RSA public key in DER format from n and e components
fn build_rsa_public_key_der(n: &[u8], e: &[u8]) -> Result<Vec<u8>> {
    // RSA public key ASN.1 structure:
    // SEQUENCE {
    //   INTEGER n
    //   INTEGER e
    // }
    // We need to encode this in DER format

    fn encode_length(len: usize) -> Vec<u8> {
        if len < 128 {
            vec![len as u8]
        } else if len < 256 {
            vec![0x81, len as u8]
        } else {
            vec![0x82, (len >> 8) as u8, len as u8]
        }
    }

    fn encode_integer(data: &[u8]) -> Vec<u8> {
        let mut result = vec![0x02]; // INTEGER tag

        // Add leading zero if high bit is set (to keep it positive)
        let needs_padding = !data.is_empty() && (data[0] & 0x80) != 0;
        let len = data.len() + if needs_padding { 1 } else { 0 };

        result.extend(encode_length(len));
        if needs_padding {
            result.push(0x00);
        }
        result.extend_from_slice(data);
        result
    }

    let n_encoded = encode_integer(n);
    let e_encoded = encode_integer(e);

    let seq_content_len = n_encoded.len() + e_encoded.len();
    let mut result = vec![0x30]; // SEQUENCE tag
    result.extend(encode_length(seq_content_len));
    result.extend(n_encoded);
    result.extend(e_encoded);

    Ok(result)
}
