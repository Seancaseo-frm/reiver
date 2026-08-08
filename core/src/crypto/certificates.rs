//! SP Certificate management for SAML signing
//!
//! Provides certificate generation, storage, and rotation for SAML Service Provider operations.

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use openssl::asn1::Asn1Time;
use openssl::bn::BigNum;
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::rsa::Rsa;
use openssl::x509::extension::{BasicConstraints, KeyUsage, SubjectKeyIdentifier};
use openssl::x509::{X509Builder, X509Name, X509};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::info;
use uuid::Uuid;

use super::SecretEncryptor;

/// Configuration for certificate generation
#[derive(Debug, Clone)]
pub struct CertificateConfig {
    /// Organization name for the certificate subject
    pub organization: String,
    /// Common name (usually the SP entity ID or domain)
    pub common_name: String,
    /// Validity period in days
    pub validity_days: i64,
    /// RSA key size in bits
    pub key_size: u32,
}

impl Default for CertificateConfig {
    fn default() -> Self {
        Self {
            organization: "Reiver".to_string(),
            common_name: "reiver.local".to_string(),
            validity_days: 365,
            key_size: 2048,
        }
    }
}

/// Generated certificate and private key
#[derive(Debug, Clone)]
pub struct GeneratedCertificate {
    /// X.509 certificate in PEM format
    pub certificate_pem: String,
    /// RSA private key in PEM format (unencrypted)
    pub private_key_pem: String,
    /// Certificate fingerprint (SHA-256)
    pub fingerprint: String,
    /// Certificate subject DN
    pub subject_dn: String,
    /// Valid from timestamp
    pub valid_from: DateTime<Utc>,
    /// Valid until timestamp
    pub valid_until: DateTime<Utc>,
    /// Serial number
    pub serial_number: String,
}

/// SP Certificate stored in the database
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SpCertificate {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub certificate_pem: String,
    pub private_key_encrypted: String,
    pub fingerprint: String,
    pub subject_dn: Option<String>,
    pub issuer_dn: Option<String>,
    pub serial_number: Option<String>,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<Uuid>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub revoked_by: Option<Uuid>,
    pub revocation_reason: Option<String>,
}

/// Generate a self-signed X.509 certificate for SAML signing
pub fn generate_certificate(config: &CertificateConfig) -> Result<GeneratedCertificate> {
    // Generate RSA key pair
    let rsa = Rsa::generate(config.key_size)?;
    let private_key = PKey::from_rsa(rsa)?;

    // Build X.509 name
    let mut name_builder = X509Name::builder()?;
    name_builder.append_entry_by_text("O", &config.organization)?;
    name_builder.append_entry_by_text("CN", &config.common_name)?;
    let name = name_builder.build();

    // Build certificate
    let mut builder = X509Builder::new()?;
    builder.set_version(2)?; // X.509 v3

    // Set serial number (random)
    let mut serial = BigNum::new()?;
    serial.rand(128, openssl::bn::MsbOption::MAYBE_ZERO, false)?;
    let serial_asn1 = serial.to_asn1_integer()?;
    builder.set_serial_number(&serial_asn1)?;

    // Set subject and issuer (self-signed, so they're the same)
    builder.set_subject_name(&name)?;
    builder.set_issuer_name(&name)?;

    // Set validity period
    let not_before = Asn1Time::days_from_now(0)?;
    let not_after = Asn1Time::days_from_now(config.validity_days as u32)?;
    builder.set_not_before(&not_before)?;
    builder.set_not_after(&not_after)?;

    // Set public key
    builder.set_pubkey(&private_key)?;

    // Add extensions for X.509 v3
    let basic_constraints = BasicConstraints::new().critical().ca().build()?;
    builder.append_extension(basic_constraints)?;

    let key_usage = KeyUsage::new()
        .critical()
        .digital_signature()
        .key_encipherment()
        .build()?;
    builder.append_extension(key_usage)?;

    let subject_key_id = SubjectKeyIdentifier::new().build(&builder.x509v3_context(None, None))?;
    builder.append_extension(subject_key_id)?;

    // Sign the certificate
    builder.sign(&private_key, MessageDigest::sha256())?;

    let certificate = builder.build();

    // Export to PEM
    let cert_pem = String::from_utf8(certificate.to_pem()?)?;
    let key_pem = String::from_utf8(private_key.private_key_to_pem_pkcs8()?)?;

    // Calculate fingerprint
    let fingerprint = calculate_fingerprint(&certificate)?;

    // Get subject DN
    let subject_dn = certificate
        .subject_name()
        .entries()
        .map(|e| {
            format!(
                "{}={}",
                e.object().nid().short_name().unwrap_or("?"),
                e.data()
                    .as_utf8()
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    // Calculate validity dates
    let valid_from = Utc::now();
    let valid_until = valid_from + Duration::days(config.validity_days);

    Ok(GeneratedCertificate {
        certificate_pem: cert_pem,
        private_key_pem: key_pem,
        fingerprint,
        subject_dn,
        valid_from,
        valid_until,
        serial_number: serial.to_hex_str()?.to_string(),
    })
}

/// Calculate SHA-256 fingerprint of a certificate
fn calculate_fingerprint(cert: &X509) -> Result<String> {
    let der = cert.to_der()?;
    let mut hasher = Sha256::new();
    hasher.update(&der);
    let result = hasher.finalize();
    Ok(hex::encode(result))
}

/// Certificate manager for database operations
pub struct CertificateManager<'a> {
    db: &'a PgPool,
    encryptor: &'a SecretEncryptor,
}

impl<'a> CertificateManager<'a> {
    pub fn new(db: &'a PgPool, encryptor: &'a SecretEncryptor) -> Self {
        Self { db, encryptor }
    }

    /// Generate and store a new certificate for an organization
    pub async fn create_certificate(
        &self,
        organization_id: Uuid,
        config: &CertificateConfig,
        created_by: Option<Uuid>,
    ) -> Result<SpCertificate> {
        // Generate the certificate
        let cert = generate_certificate(config)?;

        // Encrypt the private key
        let encrypted_key = self
            .encryptor
            .encrypt(&cert.private_key_pem)
            .map_err(|e| anyhow::anyhow!("Failed to encrypt private key: {}", e))?;

        // Store in database
        let row = sqlx::query_as::<_, SpCertificate>(
            r#"
            INSERT INTO sp_certificates (
                organization_id, certificate_pem, private_key_encrypted,
                fingerprint, subject_dn, issuer_dn, serial_number,
                valid_from, valid_until, created_by
            ) VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, $9)
            RETURNING *
            "#,
        )
        .bind(organization_id)
        .bind(&cert.certificate_pem)
        .bind(&encrypted_key)
        .bind(&cert.fingerprint)
        .bind(&cert.subject_dn)
        .bind(&cert.serial_number)
        .bind(cert.valid_from)
        .bind(cert.valid_until)
        .bind(created_by)
        .fetch_one(self.db)
        .await
        .context("Failed to store certificate")?;

        info!(
            "Created SP certificate {} for org {}",
            row.id, organization_id
        );

        Ok(row)
    }

    /// Get the active certificate for an organization
    pub async fn get_active_certificate(
        &self,
        organization_id: Uuid,
    ) -> Result<Option<SpCertificate>> {
        let cert = sqlx::query_as::<_, SpCertificate>(
            r#"
            SELECT * FROM sp_certificates
            WHERE organization_id = $1 AND is_active = true AND revoked_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(organization_id)
        .fetch_optional(self.db)
        .await
        .context("Failed to fetch certificate")?;

        Ok(cert)
    }

    /// Get or create a certificate for an organization
    pub async fn get_or_create_certificate(
        &self,
        organization_id: Uuid,
        common_name: &str,
    ) -> Result<SpCertificate> {
        // Check for existing active certificate
        if let Some(cert) = self.get_active_certificate(organization_id).await? {
            // Check if it's still valid (not expiring soon)
            if cert.valid_until > Utc::now() + Duration::days(30) {
                return Ok(cert);
            }
            info!("Certificate {} is expiring soon, creating new one", cert.id);
        }

        // Create a new certificate
        let config = CertificateConfig {
            common_name: common_name.to_string(),
            ..Default::default()
        };

        self.create_certificate(organization_id, &config, None)
            .await
    }

    /// Decrypt the private key for a certificate
    pub fn decrypt_private_key(&self, cert: &SpCertificate) -> Result<String> {
        self.encryptor
            .decrypt(&cert.private_key_encrypted)
            .map_err(|e| anyhow::anyhow!("Failed to decrypt private key: {}", e))
    }

    /// Revoke a certificate
    pub async fn revoke_certificate(
        &self,
        cert_id: Uuid,
        reason: &str,
        revoked_by: Option<Uuid>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE sp_certificates
            SET is_active = false, revoked_at = NOW(), revoked_by = $1, revocation_reason = $2
            WHERE id = $3
            "#,
        )
        .bind(revoked_by)
        .bind(reason)
        .bind(cert_id)
        .execute(self.db)
        .await
        .context("Failed to revoke certificate")?;

        info!("Revoked certificate {}: {}", cert_id, reason);
        Ok(())
    }

    /// List all certificates for an organization
    pub async fn list_certificates(&self, organization_id: Uuid) -> Result<Vec<SpCertificate>> {
        let certs = sqlx::query_as::<_, SpCertificate>(
            r#"
            SELECT * FROM sp_certificates
            WHERE organization_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(organization_id)
        .fetch_all(self.db)
        .await
        .context("Failed to list certificates")?;

        Ok(certs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_certificate() {
        let config = CertificateConfig {
            organization: "Test Org".to_string(),
            common_name: "test.example.com".to_string(),
            validity_days: 365,
            key_size: 2048,
        };

        let cert = generate_certificate(&config).unwrap();

        // Check certificate is valid PEM
        assert!(cert.certificate_pem.contains("-----BEGIN CERTIFICATE-----"));
        assert!(cert.certificate_pem.contains("-----END CERTIFICATE-----"));

        // Check private key is valid PEM
        assert!(cert.private_key_pem.contains("-----BEGIN PRIVATE KEY-----"));
        assert!(cert.private_key_pem.contains("-----END PRIVATE KEY-----"));

        // Check fingerprint is hex
        assert_eq!(cert.fingerprint.len(), 64); // SHA-256 = 32 bytes = 64 hex chars

        // Check subject DN contains expected values
        assert!(cert.subject_dn.contains("Test Org"));
        assert!(cert.subject_dn.contains("test.example.com"));
    }
}
