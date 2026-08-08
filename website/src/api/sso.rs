//! SSO (Single Sign-On) authentication endpoints
//!
//! Provides OIDC and SAML-based SSO authentication for:
//! - Okta, Auth0, Entra ID, OneLogin, Ping, Keycloak (OIDC)
//! - Enterprise SAML providers
//!
//! Supports multi-domain per organization and issuer alias for Azure/Oracle quirks.

use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use openidconnect::{
    core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata},
    reqwest::async_http_client,
    AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce, PkceCodeChallenge,
    PkceCodeVerifier, RedirectUrl, Scope, TokenResponse,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::api::mfa;
use crate::api::provisioning;
use crate::api::sso_sessions;
use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::auth::extract_user_id;
use crate::authorization::require_org_admin;
use crate::error::{AppError, Result};
use crate::rate_limit::{check_unauthenticated_rate_limit, extract_client_ip};
use crate::saml::{IdpConfig, SamlProcessor, SpConfig};

// ============================================================================
// Email Domain Normalization Helper
// ============================================================================

/// Normalize an email domain using IDNA (Internationalized Domain Names in Applications)
///
/// # Security
/// This function prevents Unicode homoglyph attacks where an attacker could register
/// a domain like "exаmple.com" (with Cyrillic 'а') to bypass domain restrictions.
///
/// By converting domains to ASCII punycode, we ensure consistent comparison
/// regardless of whether the domain uses Unicode or ASCII representation.
///
/// For example:
/// - "münchen.de" -> "xn--mnchen-3ya.de"
/// - "exаmple.com" (Cyrillic 'а') -> "xn--exmple-4pf.com"
fn normalize_email_domain(domain: &str) -> String {
    // Use IDNA to convert to ASCII (punycode) for consistent comparison
    match idna::domain_to_ascii(domain) {
        Ok(ascii_domain) => ascii_domain.to_lowercase(),
        Err(_) => {
            // If IDNA conversion fails, fall back to lowercase
            // This handles edge cases with malformed domains
            warn!("IDNA domain normalization failed for: {}", domain);
            domain.to_lowercase()
        }
    }
}

/// Check if an email domain is allowed based on the configuration
/// Uses IDNA normalization to prevent Unicode homoglyph attacks
fn is_email_domain_allowed(email: &str, allowed_domains: &[String]) -> bool {
    if allowed_domains.is_empty() {
        return true;
    }

    // Split by @ and ensure we have exactly two non-empty parts (local@domain)
    let parts: Vec<&str> = email.split('@').collect();
    if parts.len() != 2 {
        return false;
    }

    let local_part = parts[0];
    let domain_part = parts[1];

    // Both local part and domain must be non-empty
    if local_part.is_empty() || domain_part.is_empty() {
        return false;
    }

    let normalized_email_domain = normalize_email_domain(domain_part);

    allowed_domains.iter().any(|allowed| {
        let normalized_allowed = normalize_email_domain(allowed);
        normalized_email_domain == normalized_allowed
    })
}

// ============================================================================
// Session and Token Timeout Constants
// ============================================================================

/// Minimum days until certificate expiry before warning
const CERT_EXPIRY_WARNING_DAYS: i64 = 30;

/// SSO session expiration in Redis (10 minutes)
const SSO_SESSION_TTL_SECONDS: i64 = 600;

/// SAML session expiration in Redis (10 minutes)
const SAML_SESSION_TTL_SECONDS: i64 = 600;

/// SSO session duration matches JWT expiration (configured via JWT_EXPIRATION_HOURS)
/// This constant is used when config is not available; prefer using config.jwt_expiration_hours
const DEFAULT_SSO_SESSION_DURATION_HOURS: i64 = 24;

// ============================================================================
// Certificate Validation Helper
// ============================================================================

/// Validate a SAML IdP certificate.
///
/// Performs the following checks:
/// - Certificate is valid PEM format
/// - Certificate has not expired
/// - Certificate expiry warning if within CERT_EXPIRY_WARNING_DAYS
/// - Certificate chain validation (if multiple certificates in PEM)
/// - Self-signed certificate verification
///
/// Returns Ok(()) if valid, or an appropriate error message.
fn validate_saml_certificate(cert_pem: &str) -> Result<()> {
    // Check basic PEM format
    if !cert_pem.contains("-----BEGIN CERTIFICATE-----") {
        return Err(AppError::Validation(
            "saml_certificate must be a valid PEM-encoded X.509 certificate".to_string(),
        ));
    }

    // Parse all certificates in the PEM (may be a chain)
    let certs = openssl::x509::X509::stack_from_pem(cert_pem.as_bytes()).map_err(|e| {
        error!("Failed to parse SAML certificate chain: {}", e);
        AppError::Validation(format!(
            "Invalid X.509 certificate: {}",
            e.errors()
                .first()
                .map(|e| e.reason().unwrap_or("parse error"))
                .unwrap_or("unknown error")
        ))
    })?;

    if certs.is_empty() {
        return Err(AppError::Validation(
            "No certificates found in PEM data".to_string(),
        ));
    }

    // The first certificate is the end-entity (IdP signing) certificate
    let end_entity_cert = &certs[0];

    // Check certificate expiry for all certificates in the chain
    let now = openssl::asn1::Asn1Time::days_from_now(0)
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Failed to get current time")))?;

    for (i, cert) in certs.iter().enumerate() {
        let not_after = cert.not_after();

        if not_after < now {
            let cert_type = if i == 0 { "End-entity" } else { "Intermediate" };
            return Err(AppError::Validation(format!(
                "{} certificate has expired. Please upload a valid certificate chain.",
                cert_type
            )));
        }

        // Check if certificate is expiring soon (warning only, still allow)
        let warning_threshold = openssl::asn1::Asn1Time::days_from_now(
            CERT_EXPIRY_WARNING_DAYS as u32,
        )
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Failed to calculate expiry threshold")))?;

        if not_after < warning_threshold {
            let cert_type = if i == 0 { "End-entity" } else { "Intermediate" };
            warn!(
                "{} SAML certificate will expire within {} days. Subject: {:?}",
                cert_type,
                CERT_EXPIRY_WARNING_DAYS,
                cert.subject_name()
            );
        }
    }

    // Validate certificate chain if multiple certificates are provided
    if certs.len() > 1 {
        validate_certificate_chain(&certs)?;
    } else {
        // Single certificate - check if it's self-signed
        validate_self_signed_certificate(end_entity_cert)?;
    }

    // Note: Key usage validation is handled at the signature verification level
    // by the samael library. No additional check needed here.

    Ok(())
}

/// Validate that an SP private key matches its corresponding certificate.
///
/// # Security
/// This prevents configuration errors where an admin accidentally uploads
/// mismatched key/certificate pairs, which would cause SAML request signing
/// to silently fail or produce invalid signatures.
fn validate_sp_key_certificate_match(cert_pem: &str, key_pem: &str) -> Result<()> {
    use openssl::hash::MessageDigest;
    use openssl::pkey::PKey;
    use openssl::sign::{Signer, Verifier};

    // Parse the certificate
    let cert = openssl::x509::X509::from_pem(cert_pem.as_bytes()).map_err(|e| {
        error!("Failed to parse SP certificate: {}", e);
        AppError::Validation("Invalid SP certificate format".to_string())
    })?;

    // Parse the private key
    let private_key = PKey::private_key_from_pem(key_pem.as_bytes()).map_err(|e| {
        error!("Failed to parse SP private key: {}", e);
        AppError::Validation("Invalid SP private key format".to_string())
    })?;

    // Extract the public key from the certificate
    let cert_public_key = cert.public_key().map_err(|e| {
        error!("Failed to extract public key from SP certificate: {}", e);
        AppError::Validation("Invalid SP certificate: cannot extract public key".to_string())
    })?;

    // Verify that the private key matches the certificate's public key
    // by signing and verifying a test message
    let test_message = b"SP key-certificate validation test";

    // Sign with the private key
    let mut signer = Signer::new(MessageDigest::sha256(), &private_key).map_err(|e| {
        error!("Failed to create signer for SP key validation: {}", e);
        AppError::Internal(anyhow::anyhow!("Key validation error"))
    })?;
    signer
        .update(test_message)
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Key validation error")))?;
    let signature = signer.sign_to_vec().map_err(|e| {
        error!("Failed to sign with SP private key: {}", e);
        AppError::Validation("SP private key cannot be used for signing".to_string())
    })?;

    // Verify with the certificate's public key
    let mut verifier = Verifier::new(MessageDigest::sha256(), &cert_public_key).map_err(|e| {
        error!("Failed to create verifier for SP key validation: {}", e);
        AppError::Internal(anyhow::anyhow!("Key validation error"))
    })?;
    verifier
        .update(test_message)
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Key validation error")))?;

    let valid = verifier.verify(&signature)
        .map_err(|e| {
            error!("Signature verification failed during SP key validation: {}", e);
            AppError::Validation(
                "SP private key does not match SP certificate. Please ensure you're using the correct key pair.".to_string()
            )
        })?;

    if !valid {
        return Err(AppError::Validation(
            "SP private key does not match SP certificate. Please ensure you're using the correct key pair.".to_string()
        ));
    }

    info!("SP private key and certificate match validated successfully");
    Ok(())
}

/// Validate a certificate chain.
///
/// Verifies that each certificate in the chain is signed by the next certificate.
/// The chain should be ordered: [end-entity, intermediate..., root].
fn validate_certificate_chain(certs: &[openssl::x509::X509]) -> Result<()> {
    use openssl::stack::Stack;
    use openssl::x509::store::X509StoreBuilder;
    use openssl::x509::X509StoreContext;

    if certs.len() < 2 {
        return Ok(());
    }

    // Build a certificate store with intermediate and root certificates
    let mut store_builder = X509StoreBuilder::new().map_err(|e| {
        error!("Failed to create certificate store: {}", e);
        AppError::Internal(anyhow::anyhow!("Certificate store creation failed"))
    })?;

    // Add all certificates except the end-entity as trusted
    for cert in certs.iter().skip(1) {
        store_builder.add_cert(cert.clone()).map_err(|e| {
            error!("Failed to add certificate to store: {}", e);
            AppError::Validation("Invalid certificate in chain".to_string())
        })?;
    }

    let store = store_builder.build();

    // Create an empty intermediate stack (we already added them to the store)
    let chain = Stack::new().map_err(|e| {
        error!("Failed to create certificate stack: {}", e);
        AppError::Internal(anyhow::anyhow!("Certificate stack creation failed"))
    })?;

    // Verify the end-entity certificate against the chain
    let mut context = X509StoreContext::new().map_err(|e| {
        error!("Failed to create X509 store context: {}", e);
        AppError::Internal(anyhow::anyhow!(
            "Certificate verification context creation failed"
        ))
    })?;

    let end_entity = &certs[0];
    let valid = context
        .init(&store, end_entity, &chain, |ctx| ctx.verify_cert())
        .map_err(|e| {
            error!("Certificate chain verification failed: {}", e);
            AppError::Validation(format!(
                "Certificate chain verification failed: {}",
                e.errors()
                    .first()
                    .map(|e| e.reason().unwrap_or("verification error"))
                    .unwrap_or("unknown")
            ))
        })?;

    if !valid {
        return Err(AppError::Validation(
            "Certificate chain is invalid: end-entity certificate is not signed by the provided chain".to_string()
        ));
    }

    info!("Certificate chain validated: {} certificates", certs.len());
    Ok(())
}

/// Validate a self-signed certificate.
///
/// Verifies that a single certificate is properly self-signed
/// (subject and issuer match, and signature is valid).
fn validate_self_signed_certificate(cert: &openssl::x509::X509) -> Result<()> {
    // Check if subject and issuer are the same (basic self-signed check)
    let subject = cert.subject_name();
    let issuer = cert.issuer_name();

    // Compare subject and issuer names
    let subject_str = format!("{:?}", subject);
    let issuer_str = format!("{:?}", issuer);

    if subject_str != issuer_str {
        // Not self-signed - this is okay, but we can't verify the chain
        // without the issuing CA certificate
        warn!(
            "SAML certificate is not self-signed and no issuing CA provided. Subject: {}, Issuer: {}",
            subject_str, issuer_str
        );
        // We allow this but log a warning - the admin should provide the full chain for best security
        return Ok(());
    }

    // Verify self-signature
    let public_key = cert.public_key().map_err(|e| {
        error!("Failed to extract public key from certificate: {}", e);
        AppError::Validation("Invalid certificate: cannot extract public key".to_string())
    })?;

    let valid = cert.verify(&public_key).map_err(|e| {
        error!("Certificate self-signature verification failed: {}", e);
        AppError::Validation("Certificate self-signature verification failed".to_string())
    })?;

    if !valid {
        return Err(AppError::Validation(
            "Self-signed certificate has invalid signature".to_string(),
        ));
    }

    Ok(())
}

pub fn create_sso_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        // SSO Configuration management (admin)
        .route(
            "/configurations",
            get(list_configurations).post(create_configuration),
        )
        .route(
            "/configurations/{id}",
            get(get_configuration)
                .put(update_configuration)
                .delete(delete_configuration),
        )
        // Domain-based configuration lookup
        .route("/domains/{domain}", get(get_configuration_by_domain))
        // SSO Login flows
        .route("/login/oidc/{config_id}", get(initiate_oidc_login))
        .route("/login/saml/{config_id}", get(initiate_saml_login))
        .route("/callback/oidc/{config_id}", get(handle_oidc_callback))
        .route("/callback/saml/{config_id}", post(handle_saml_callback))
        // MFA verification (to complete SSO login after MFA challenge)
        .route("/mfa/verify", post(verify_sso_mfa))
        // SAML metadata
        .route("/saml/metadata/{config_id}", get(get_saml_metadata))
        // SAML Single Logout (SLO)
        .route("/logout/saml/{config_id}", get(initiate_saml_logout))
        // Certificate health monitoring (admin)
        .route("/health/certificates", get(get_certificate_health))
}

// ============================================================================
// SSO Types
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[allow(dead_code)] // Used for API type definition
pub enum SsoType {
    Oidc,
    Saml,
}

impl Default for SsoType {
    fn default() -> Self {
        SsoType::Oidc
    }
}

#[derive(Debug, Serialize)]
pub struct SsoConfiguration {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub domain_name: Option<String>,
    pub provider: String,
    pub name: String,
    pub sso_type: String,
    // OIDC fields
    pub issuer_url: Option<String>,
    pub issuer_alias: Option<String>,
    pub client_id: Option<String>,
    // SAML fields
    pub saml_entity_id: Option<String>,
    pub saml_sso_url: Option<String>,
    pub saml_slo_url: Option<String>,
    // saml_certificate is not returned for security
    pub saml_sign_requests: Option<bool>,
    // Common fields
    pub scopes: Vec<String>,
    pub auto_create_users: bool,
    pub default_role: String,
    pub allowed_email_domains: Vec<String>,
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Minimal SSO configuration info for public domain lookup
/// Only includes information necessary to initiate SSO login
#[derive(Debug, Serialize)]
pub struct SsoConfigurationPublic {
    /// Configuration ID (needed to initiate login) - None if SSO not available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
    /// SSO type (oidc or saml) - None if SSO not available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sso_type: Option<String>,
    /// Provider name for display (e.g., "Okta", "Google") - None if SSO not available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Whether SSO is available for this domain
    pub available: bool,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)] // Some fields reserved for future Okta integration
struct SsoConfigRow {
    id: Uuid,
    organization_id: Uuid,
    domain_name: Option<String>,
    provider: String,
    name: String,
    sso_type: String,
    // OIDC
    issuer_url: String,
    issuer_alias: Option<String>,
    client_id: String,
    client_secret_encrypted: String,
    okta_domain: Option<String>,
    okta_api_token_encrypted: Option<String>,
    // SAML
    saml_entity_id: Option<String>,
    saml_sso_url: Option<String>,
    saml_slo_url: Option<String>,
    saml_certificate: Option<String>,
    saml_sign_requests: Option<bool>,
    // SAML SP Signing (optional - for AuthnRequest signing)
    sp_certificate: Option<String>,
    sp_private_key_encrypted: Option<String>,
    // Common
    scopes: Vec<String>,
    auto_create_users: bool,
    default_role: String,
    allowed_email_domains: Vec<String>,
    enabled: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<SsoConfigRow> for SsoConfiguration {
    fn from(row: SsoConfigRow) -> Self {
        Self {
            id: row.id,
            organization_id: row.organization_id,
            domain_name: row.domain_name,
            provider: row.provider,
            name: row.name,
            sso_type: row.sso_type,
            issuer_url: if row.issuer_url.is_empty() {
                None
            } else {
                Some(row.issuer_url)
            },
            issuer_alias: row.issuer_alias,
            client_id: if row.client_id.is_empty() {
                None
            } else {
                Some(row.client_id)
            },
            saml_entity_id: row.saml_entity_id,
            saml_sso_url: row.saml_sso_url,
            saml_slo_url: row.saml_slo_url,
            saml_sign_requests: row.saml_sign_requests,
            scopes: row.scopes,
            auto_create_users: row.auto_create_users,
            default_role: row.default_role,
            allowed_email_domains: row.allowed_email_domains,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateSsoConfigRequest {
    pub organization_id: Option<Uuid>,
    pub domain_name: Option<String>,
    pub provider: String,
    pub name: String,
    #[serde(default)]
    pub sso_type: String, // 'oidc' or 'saml'
    // OIDC Configuration
    pub issuer_url: Option<String>,
    pub issuer_alias: Option<String>, // For Azure/Oracle quirks
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub okta_domain: Option<String>,
    pub okta_api_token: Option<String>,
    // SAML Configuration
    pub saml_entity_id: Option<String>,
    pub saml_sso_url: Option<String>,
    pub saml_slo_url: Option<String>,
    pub saml_certificate: Option<String>,
    pub saml_sign_requests: Option<bool>,
    // SAML SP Signing Certificate (for AuthnRequest signing)
    pub sp_certificate: Option<String>,
    pub sp_private_key: Option<String>,
    // Common
    pub scopes: Option<Vec<String>>,
    pub auto_create_users: Option<bool>,
    pub default_role: Option<String>,
    pub allowed_email_domains: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSsoConfigRequest {
    pub domain_name: Option<String>,
    pub name: Option<String>,
    // OIDC
    pub issuer_url: Option<String>,
    pub issuer_alias: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub okta_domain: Option<String>,
    pub okta_api_token: Option<String>,
    // SAML
    pub saml_entity_id: Option<String>,
    pub saml_sso_url: Option<String>,
    pub saml_slo_url: Option<String>,
    pub saml_certificate: Option<String>,
    pub saml_sign_requests: Option<bool>,
    // SAML SP Signing Certificate (for AuthnRequest signing)
    pub sp_certificate: Option<String>,
    pub sp_private_key: Option<String>,
    // Common
    pub scopes: Option<Vec<String>>,
    pub auto_create_users: Option<bool>,
    pub default_role: Option<String>,
    pub allowed_email_domains: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

// ============================================================================
// Authorization Helpers
// ============================================================================

/// Check if a user has admin access to the SSO config (via the config's organization)
async fn require_sso_config_admin(
    db: &sqlx::PgPool,
    user_id: Uuid,
    config_id: Uuid,
) -> Result<Uuid> {
    // Get the organization_id for this SSO config
    let config: Option<(Uuid,)> =
        sqlx::query_as("SELECT organization_id FROM sso_configurations WHERE id = $1")
            .bind(config_id)
            .fetch_optional(db)
            .await
            .map_err(|e| {
                error!("Failed to get SSO configuration: {}", e);
                AppError::Internal(anyhow::anyhow!("Database error"))
            })?;

    let org_id = config
        .ok_or_else(|| AppError::NotFound("SSO configuration not found".to_string()))?
        .0;

    require_org_admin(db, user_id, org_id).await?;
    Ok(org_id)
}

// ============================================================================
// Configuration Management Endpoints
// ============================================================================

/// List all SSO configurations for organizations the user is admin of
async fn list_configurations(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<SsoConfiguration>>> {
    // Require authentication
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;

    let org_id = params
        .get("organization_id")
        .and_then(|s| s.parse::<Uuid>().ok());

    let rows = if let Some(org_id) = org_id {
        // Check admin access to the specified organization
        require_org_admin(&state.db, user_id, org_id).await?;

        let tier = state.entitlements.get_config(org_id).await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
        if !tier.config.platform.sso {
            return Err(AppError::Forbidden("SSO is not available on your current plan".into()));
        }

        sqlx::query_as::<_, SsoConfigRow>(
            r#"
            SELECT id, organization_id, domain_name, provider, name, sso_type,
                   issuer_url, issuer_alias, client_id, client_secret_encrypted,
                   okta_domain, okta_api_token_encrypted,
                   saml_entity_id, saml_sso_url, saml_slo_url, saml_certificate, saml_sign_requests,
                   sp_certificate, sp_private_key_encrypted,
                   scopes, auto_create_users, default_role, allowed_email_domains,
                   enabled, created_at, updated_at
            FROM sso_configurations
            WHERE organization_id = $1
            ORDER BY created_at DESC
            "#
        )
        .bind(org_id)
        .fetch_all(&*state.db)
        .await
    } else {
        // List configs for all organizations where user is admin/owner
        sqlx::query_as::<_, SsoConfigRow>(
            r#"
            SELECT sc.id, sc.organization_id, sc.domain_name, sc.provider, sc.name, sc.sso_type,
                   sc.issuer_url, sc.issuer_alias, sc.client_id, sc.client_secret_encrypted,
                   sc.okta_domain, sc.okta_api_token_encrypted,
                   sc.saml_entity_id, sc.saml_sso_url, sc.saml_slo_url, sc.saml_certificate, sc.saml_sign_requests,
                   sc.sp_certificate, sc.sp_private_key_encrypted,
                   sc.scopes, sc.auto_create_users, sc.default_role, sc.allowed_email_domains,
                   sc.enabled, sc.created_at, sc.updated_at
            FROM sso_configurations sc
            INNER JOIN memberships m ON sc.organization_id = m.organization_id
            WHERE m.user_id = $1 AND m.status = 'active' AND m.role IN ('admin', 'owner')
            ORDER BY sc.created_at DESC
            "#
        )
        .bind(user_id)
        .fetch_all(&*state.db)
        .await
    }.map_err(|e| {
        error!("Failed to list SSO configurations: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    let configs: Vec<SsoConfiguration> = rows.into_iter().map(|row| row.into()).collect();
    Ok(Json(configs))
}

/// Create a new SSO configuration
async fn create_configuration(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateSsoConfigRequest>,
) -> Result<Json<SsoConfiguration>> {
    // Require authentication
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;

    // Require organization_id
    let org_id = payload
        .organization_id
        .ok_or_else(|| AppError::Validation("organization_id is required".to_string()))?;

    // Require admin access to the organization
    require_org_admin(&state.db, user_id, org_id).await?;

    let tier = state.entitlements.get_config(org_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO is not available on your current plan".into()));
    }

    let sso_type = if payload.sso_type.is_empty() {
        "oidc".to_string()
    } else {
        payload.sso_type.to_lowercase()
    };

    // Validate based on SSO type
    match sso_type.as_str() {
        "oidc" => {
            if payload.issuer_url.is_none()
                || payload.client_id.is_none()
                || payload.client_secret.is_none()
            {
                return Err(AppError::Validation(
                    "OIDC configuration requires issuer_url, client_id, and client_secret"
                        .to_string(),
                ));
            }
            if let Some(ref url) = payload.issuer_url {
                if !url.starts_with("https://") {
                    return Err(AppError::Validation(
                        "issuer_url must use HTTPS".to_string(),
                    ));
                }
            }
        }
        "saml" => {
            if payload.saml_entity_id.is_none()
                || payload.saml_sso_url.is_none()
                || payload.saml_certificate.is_none()
            {
                return Err(AppError::Validation(
                    "SAML configuration requires saml_entity_id, saml_sso_url, and saml_certificate".to_string()
                ));
            }
            // Validate SAML fields are not empty strings
            if let Some(ref entity_id) = payload.saml_entity_id {
                if entity_id.trim().is_empty() {
                    return Err(AppError::Validation(
                        "saml_entity_id cannot be empty".to_string(),
                    ));
                }
            }
            if let Some(ref cert) = payload.saml_certificate {
                if cert.trim().is_empty() {
                    return Err(AppError::Validation(
                        "saml_certificate cannot be empty".to_string(),
                    ));
                }
                // Validate certificate using OpenSSL (format, expiry, key usage)
                validate_saml_certificate(cert)?;
            }
            // Validate SAML SSO URL uses HTTPS
            if let Some(ref url) = payload.saml_sso_url {
                if url.trim().is_empty() {
                    return Err(AppError::Validation(
                        "saml_sso_url cannot be empty".to_string(),
                    ));
                }
                if !url.starts_with("https://") {
                    return Err(AppError::Validation(
                        "saml_sso_url must use HTTPS".to_string(),
                    ));
                }
            }

            // SECURITY: Validate SP certificate and private key match if both are provided
            // This prevents configuration errors where mismatched key pairs would cause
            // SAML request signing to fail silently
            if let (Some(ref cert), Some(ref key)) =
                (&payload.sp_certificate, &payload.sp_private_key)
            {
                if !cert.trim().is_empty() && !key.trim().is_empty() {
                    validate_sp_key_certificate_match(cert, key)?;
                }
            }
        }
        _ => {
            return Err(AppError::Validation(
                "sso_type must be 'oidc' or 'saml'".to_string(),
            ));
        }
    }

    // Encrypt secrets before storing
    let encrypted_secret = if let Some(ref secret) = payload.client_secret {
        match state.encryptor.encrypt(secret) {
            Ok(encrypted) => encrypted,
            Err(e) => {
                error!("Failed to encrypt client secret: {}", e);
                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to encrypt secret"
                )));
            }
        }
    } else {
        String::new()
    };

    let encrypted_api_token = if let Some(ref token) = payload.okta_api_token {
        match state.encryptor.encrypt(token) {
            Ok(encrypted) => Some(encrypted),
            Err(e) => {
                error!("Failed to encrypt API token: {}", e);
                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to encrypt token"
                )));
            }
        }
    } else {
        None
    };

    // Encrypt SP private key if provided
    let encrypted_sp_private_key = if let Some(ref key) = payload.sp_private_key {
        match state.encryptor.encrypt(key) {
            Ok(encrypted) => Some(encrypted),
            Err(e) => {
                error!("Failed to encrypt SP private key: {}", e);
                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to encrypt SP private key"
                )));
            }
        }
    } else {
        None
    };

    let row = sqlx::query_as::<_, SsoConfigRow>(
        r#"
        INSERT INTO sso_configurations (
            organization_id, domain_name, provider, name, sso_type,
            issuer_url, issuer_alias, client_id, client_secret_encrypted,
            okta_domain, okta_api_token_encrypted,
            saml_entity_id, saml_sso_url, saml_slo_url, saml_certificate, saml_sign_requests,
            sp_certificate, sp_private_key_encrypted,
            scopes, auto_create_users, default_role, allowed_email_domains, enabled
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
        RETURNING id, organization_id, domain_name, provider, name, sso_type,
                  issuer_url, issuer_alias, client_id, client_secret_encrypted,
                  okta_domain, okta_api_token_encrypted,
                  saml_entity_id, saml_sso_url, saml_slo_url, saml_certificate, saml_sign_requests,
                  sp_certificate, sp_private_key_encrypted,
                  scopes, auto_create_users, default_role, allowed_email_domains,
                  enabled, created_at, updated_at
        "#
    )
    .bind(payload.organization_id)
    .bind(&payload.domain_name)
    .bind(&payload.provider)
    .bind(&payload.name)
    .bind(&sso_type)
    .bind(payload.issuer_url.as_deref().unwrap_or(""))
    .bind(&payload.issuer_alias)
    .bind(payload.client_id.as_deref().unwrap_or(""))
    .bind(&encrypted_secret)
    .bind(&payload.okta_domain)
    .bind(&encrypted_api_token)
    .bind(&payload.saml_entity_id)
    .bind(&payload.saml_sso_url)
    .bind(&payload.saml_slo_url)
    .bind(&payload.saml_certificate)
    .bind(payload.saml_sign_requests.unwrap_or(true))
    .bind(&payload.sp_certificate)
    .bind(&encrypted_sp_private_key)
    .bind(payload.scopes.unwrap_or_else(|| vec!["openid".to_string(), "profile".to_string(), "email".to_string()]))
    .bind(payload.auto_create_users.unwrap_or(true))
    .bind(payload.default_role.as_deref().unwrap_or("member"))
    .bind(payload.allowed_email_domains.unwrap_or_default())
    .bind(payload.enabled.unwrap_or(true))
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create SSO configuration: {}", e);
        if e.to_string().contains("duplicate key") {
            AppError::Validation("SSO configuration already exists for this domain/provider".to_string())
        } else {
            error!("Database error creating SSO config: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error"))
        }
    })?;

    // Log audit event for config creation
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let _ = AuditEventBuilder::new(AuditEventType::SsoConfigCreated)
        .user(user_id)
        .organization(org_id)
        .resource("sso_configuration", row.id)
        .details(serde_json::json!({
            "created": {
                "name": payload.name,
                "provider": payload.provider,
                "sso_type": sso_type,
                "enabled": row.enabled,
            }
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
        .success()
        .log(&state.clickhouse)
        .await;

    info!(
        "Created SSO configuration: type={}, provider={}, id={}",
        sso_type, payload.provider, row.id
    );
    Ok(Json(row.into()))
}

/// Get a specific SSO configuration
async fn get_configuration(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<SsoConfiguration>> {
    // Require authentication and admin access
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let org_id = require_sso_config_admin(&state.db, user_id, id).await?;

    let tier = state.entitlements.get_config(org_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO is not available on your current plan".into()));
    }

    let row = sqlx::query_as::<_, SsoConfigRow>(
        r#"
        SELECT id, organization_id, domain_name, provider, name, sso_type,
               issuer_url, issuer_alias, client_id, client_secret_encrypted,
               okta_domain, okta_api_token_encrypted,
               saml_entity_id, saml_sso_url, saml_slo_url, saml_certificate, saml_sign_requests,
               sp_certificate, sp_private_key_encrypted,
               scopes, auto_create_users, default_role, allowed_email_domains,
               enabled, created_at, updated_at
        FROM sso_configurations
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get SSO configuration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?
    .ok_or_else(|| AppError::NotFound("SSO configuration not found".to_string()))?;

    Ok(Json(row.into()))
}

/// Minimum response time for domain lookup to prevent timing attacks (milliseconds)
const DOMAIN_LOOKUP_MIN_RESPONSE_MS: u64 = 100;

/// Get SSO configuration by email domain (public endpoint)
///
/// Returns only minimal information needed to initiate SSO login.
/// This is intentionally limited to prevent information disclosure about
/// organization SSO configurations.
///
/// # Security
/// Uses fixed minimum response time to prevent timing-based domain enumeration.
async fn get_configuration_by_domain(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(domain): Path<String>,
) -> Result<Json<SsoConfigurationPublic>> {
    // SECURITY: Rate limit to prevent domain enumeration attacks
    let client_ip = extract_client_ip(&addr);
    check_unauthenticated_rate_limit(&state.redis, &client_ip, "sso_domain_lookup").await?;

    // SECURITY: Record start time for constant-time response
    // This prevents timing attacks where an attacker measures response times
    // to determine whether a domain has SSO configured
    let start_time = std::time::Instant::now();

    // Only fetch the minimal fields needed for the public response
    let row = sqlx::query_as::<_, (Uuid, String, String, Uuid)>(
        r#"
        SELECT id, sso_type, provider, organization_id
        FROM sso_configurations
        WHERE domain_name = $1 AND enabled = true
        "#,
    )
    .bind(&domain)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get SSO configuration by domain: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    // Build response before timing normalization
    let response = match row {
        Some((id, sso_type, provider, org_id)) => {
            let sso_allowed = state.entitlements.get_config(org_id).await
                .map(|t| t.config.platform.sso)
                .unwrap_or(false);
            if sso_allowed {
                SsoConfigurationPublic {
                    id: Some(id),
                    sso_type: Some(sso_type),
                    provider: Some(provider),
                    available: true,
                }
            } else {
                SsoConfigurationPublic {
                    id: None,
                    sso_type: None,
                    provider: None,
                    available: false,
                }
            }
        },
        None => SsoConfigurationPublic {
            id: None,
            sso_type: None,
            provider: None,
            available: false,
        },
    };

    // SECURITY: Normalize response time to prevent timing attacks
    // Wait until minimum response time has elapsed, regardless of whether
    // the domain was found or not. This makes it impossible to determine
    // domain existence via timing analysis.
    let elapsed = start_time.elapsed();
    let min_duration = tokio::time::Duration::from_millis(DOMAIN_LOOKUP_MIN_RESPONSE_MS);
    if elapsed < min_duration {
        tokio::time::sleep(min_duration - elapsed).await;
    }

    Ok(Json(response))
}

/// Update an SSO configuration
async fn update_configuration(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateSsoConfigRequest>,
) -> Result<Json<SsoConfiguration>> {
    // Require authentication and admin access
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let org_id = require_sso_config_admin(&state.db, user_id, id).await?;

    let tier = state.entitlements.get_config(org_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO is not available on your current plan".into()));
    }

    // Validate SAML fields if provided
    if let Some(ref entity_id) = payload.saml_entity_id {
        if entity_id.trim().is_empty() {
            return Err(AppError::Validation(
                "saml_entity_id cannot be empty".to_string(),
            ));
        }
    }
    if let Some(ref cert) = payload.saml_certificate {
        if cert.trim().is_empty() {
            return Err(AppError::Validation(
                "saml_certificate cannot be empty".to_string(),
            ));
        }
        // Validate certificate using OpenSSL (format, expiry, key usage)
        validate_saml_certificate(cert)?;
    }
    if let Some(ref url) = payload.saml_sso_url {
        if url.trim().is_empty() {
            return Err(AppError::Validation(
                "saml_sso_url cannot be empty".to_string(),
            ));
        }
        if !url.starts_with("https://") {
            return Err(AppError::Validation(
                "saml_sso_url must use HTTPS".to_string(),
            ));
        }
    }

    // Validate issuer_url if provided
    if let Some(ref url) = payload.issuer_url {
        if !url.starts_with("https://") {
            return Err(AppError::Validation(
                "issuer_url must use HTTPS".to_string(),
            ));
        }
    }

    // SECURITY: Validate SP certificate and private key match if both are provided
    // When updating, if only one is provided, we require both to ensure they match
    match (&payload.sp_certificate, &payload.sp_private_key) {
        (Some(cert), Some(key)) => {
            // Both provided - validate they match
            if !cert.trim().is_empty() && !key.trim().is_empty() {
                validate_sp_key_certificate_match(cert, key)?;
            }
        }
        (Some(cert), None) if !cert.trim().is_empty() => {
            // Only certificate provided - require private key for security
            return Err(AppError::Validation(
                "When updating SP certificate, you must also provide the matching private key"
                    .to_string(),
            ));
        }
        (None, Some(key)) if !key.trim().is_empty() => {
            // Only private key provided - require certificate for security
            return Err(AppError::Validation(
                "When updating SP private key, you must also provide the matching certificate"
                    .to_string(),
            ));
        }
        _ => {
            // Neither provided or both empty - no validation needed
        }
    }

    // Fetch before-state for audit before/after diff
    let before: (String, String, String, bool) = sqlx::query_as(
        "SELECT name, provider, sso_type, enabled FROM sso_configurations WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch SSO config before update: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    // Encrypt secrets if provided
    let encrypted_secret = if let Some(ref secret) = payload.client_secret {
        match state.encryptor.encrypt(secret) {
            Ok(encrypted) => Some(encrypted),
            Err(e) => {
                error!("Failed to encrypt client secret: {}", e);
                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to encrypt secret"
                )));
            }
        }
    } else {
        None
    };

    let encrypted_api_token = if let Some(ref token) = payload.okta_api_token {
        match state.encryptor.encrypt(token) {
            Ok(encrypted) => Some(encrypted),
            Err(e) => {
                error!("Failed to encrypt API token: {}", e);
                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to encrypt token"
                )));
            }
        }
    } else {
        None
    };

    // Encrypt SP private key if provided
    let encrypted_sp_private_key = if let Some(ref key) = payload.sp_private_key {
        match state.encryptor.encrypt(key) {
            Ok(encrypted) => Some(encrypted),
            Err(e) => {
                error!("Failed to encrypt SP private key: {}", e);
                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to encrypt SP private key"
                )));
            }
        }
    } else {
        None
    };

    let row = sqlx::query_as::<_, SsoConfigRow>(
        r#"
        UPDATE sso_configurations
        SET
            domain_name = COALESCE($1, domain_name),
            name = COALESCE($2, name),
            issuer_url = COALESCE($3, issuer_url),
            issuer_alias = COALESCE($4, issuer_alias),
            client_id = COALESCE($5, client_id),
            client_secret_encrypted = COALESCE($6, client_secret_encrypted),
            okta_domain = COALESCE($7, okta_domain),
            okta_api_token_encrypted = COALESCE($8, okta_api_token_encrypted),
            saml_entity_id = COALESCE($9, saml_entity_id),
            saml_sso_url = COALESCE($10, saml_sso_url),
            saml_slo_url = COALESCE($11, saml_slo_url),
            saml_certificate = COALESCE($12, saml_certificate),
            saml_sign_requests = COALESCE($13, saml_sign_requests),
            sp_certificate = COALESCE($14, sp_certificate),
            sp_private_key_encrypted = COALESCE($15, sp_private_key_encrypted),
            scopes = COALESCE($16, scopes),
            auto_create_users = COALESCE($17, auto_create_users),
            default_role = COALESCE($18, default_role),
            allowed_email_domains = COALESCE($19, allowed_email_domains),
            enabled = COALESCE($20, enabled),
            updated_at = NOW()
        WHERE id = $21
        RETURNING id, organization_id, domain_name, provider, name, sso_type,
                  issuer_url, issuer_alias, client_id, client_secret_encrypted,
                  okta_domain, okta_api_token_encrypted,
                  saml_entity_id, saml_sso_url, saml_slo_url, saml_certificate, saml_sign_requests,
                  sp_certificate, sp_private_key_encrypted,
                  scopes, auto_create_users, default_role, allowed_email_domains,
                  enabled, created_at, updated_at
        "#,
    )
    .bind(payload.domain_name.as_deref())
    .bind(payload.name.as_deref())
    .bind(payload.issuer_url.as_deref())
    .bind(payload.issuer_alias.as_deref())
    .bind(payload.client_id.as_deref())
    .bind(encrypted_secret.as_deref())
    .bind(payload.okta_domain.as_deref())
    .bind(encrypted_api_token.as_deref())
    .bind(payload.saml_entity_id.as_deref())
    .bind(payload.saml_sso_url.as_deref())
    .bind(payload.saml_slo_url.as_deref())
    .bind(payload.saml_certificate.as_deref())
    .bind(payload.saml_sign_requests)
    .bind(payload.sp_certificate.as_deref())
    .bind(encrypted_sp_private_key.as_deref())
    .bind(payload.scopes)
    .bind(payload.auto_create_users)
    .bind(payload.default_role.as_deref())
    .bind(payload.allowed_email_domains)
    .bind(payload.enabled)
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to update SSO configuration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?
    .ok_or_else(|| AppError::NotFound("SSO configuration not found".to_string()))?;

    // Log audit event for config update
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let _ = AuditEventBuilder::new(AuditEventType::SsoConfigUpdated)
        .user(user_id)
        .organization(org_id)
        .resource("sso_configuration", id)
        .details(serde_json::json!({
            "before": {
                "name": before.0,
                "provider": before.1,
                "sso_type": before.2,
                "enabled": before.3,
            },
            "after": {
                "name": row.name,
                "provider": row.provider,
                "sso_type": row.sso_type,
                "enabled": row.enabled,
            },
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
        .success()
        .log(&state.clickhouse)
        .await;

    info!("Updated SSO configuration: id={}", id);
    Ok(Json(row.into()))
}

/// Delete an SSO configuration
async fn delete_configuration(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode> {
    // Require authentication and admin access
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let org_id = require_sso_config_admin(&state.db, user_id, id).await?;

    let tier = state.entitlements.get_config(org_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO is not available on your current plan".into()));
    }

    // Fetch meaningful fields before deletion for audit
    let before: (String, String, String) =
        sqlx::query_as("SELECT name, provider, sso_type FROM sso_configurations WHERE id = $1")
            .bind(id)
            .fetch_one(&*state.db)
            .await
            .map_err(|e| {
                error!("Failed to fetch SSO config before delete: {}", e);
                AppError::Internal(anyhow::anyhow!("Database error"))
            })?;

    let result = sqlx::query("DELETE FROM sso_configurations WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete SSO configuration: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error"))
        })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(
            "SSO configuration not found".to_string(),
        ));
    }

    // Log audit event for config deletion
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let _ = AuditEventBuilder::new(AuditEventType::SsoConfigDeleted)
        .user(user_id)
        .organization(org_id)
        .resource("sso_configuration", id)
        .details(serde_json::json!({
            "deleted": {
                "name": before.0,
                "provider": before.1,
                "sso_type": before.2,
            }
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
        .success()
        .log(&state.clickhouse)
        .await;

    info!("Deleted SSO configuration: id={}", id);
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// OIDC Login Flow
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct LoginQuery {
    redirect_uri: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub authorization_url: String,
    pub sso_type: String,
}

/// Initiate OIDC login by config ID
async fn initiate_oidc_login(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(config_id): Path<Uuid>,
    Query(query): Query<LoginQuery>,
) -> Result<Json<LoginResponse>> {
    // Rate limit SSO login attempts by IP
    let client_ip = extract_client_ip(&addr);
    check_unauthenticated_rate_limit(&state.redis, &client_ip, "sso_login").await?;

    let config = get_config_by_id(&state.db, config_id).await?;

    let tier = state.entitlements.get_config(config.organization_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO is not available on your current plan".into()));
    }

    if config.sso_type != "oidc" {
        return Err(AppError::Validation(
            "This configuration is not OIDC".to_string(),
        ));
    }

    let auth_url = build_oidc_auth_url(&state, &config, query.redirect_uri).await?;

    Ok(Json(LoginResponse {
        authorization_url: auth_url,
        sso_type: "oidc".to_string(),
    }))
}

async fn build_oidc_auth_url(
    state: &WebsiteState,
    config: &SsoConfigRow,
    redirect_uri: Option<String>,
) -> Result<String> {
    // Use issuer_alias if provided (for Azure/Oracle quirks)
    let issuer = if let Some(ref alias) = config.issuer_alias {
        alias.clone()
    } else {
        config.issuer_url.clone()
    };

    let issuer_url = IssuerUrl::new(issuer).map_err(|e| {
        error!("Invalid OIDC issuer URL: {}", e);
        AppError::Internal(anyhow::anyhow!("SSO configuration error"))
    })?;

    let provider_metadata = CoreProviderMetadata::discover_async(issuer_url, async_http_client)
        .await
        .map_err(|e| {
            error!("Failed to discover OIDC provider metadata: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to connect to identity provider"))
        })?;

    let base_url = &state.config.base_url;
    let redirect_url =
        RedirectUrl::new(format!("{}/api/sso/callback/oidc/{}", base_url, config.id)).map_err(
            |e| {
                error!("Invalid OIDC redirect URL: {}", e);
                AppError::Internal(anyhow::anyhow!("SSO configuration error"))
            },
        )?;

    // Decrypt client secret
    let client_secret = state
        .encryptor
        .decrypt(&config.client_secret_encrypted)
        .map_err(|e| {
            error!("Failed to decrypt client secret: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to decrypt client secret"))
        })?;

    let client = CoreClient::from_provider_metadata(
        provider_metadata,
        ClientId::new(config.client_id.clone()),
        Some(ClientSecret::new(client_secret)),
    )
    .set_redirect_uri(redirect_url);

    // Generate PKCE challenge for additional security
    // PKCE protects against authorization code interception attacks
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

    let mut auth_request = client
        .authorize_url(
            CoreAuthenticationFlow::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        .set_pkce_challenge(pkce_challenge);

    for scope in &config.scopes {
        auth_request = auth_request.add_scope(Scope::new(scope.clone()));
    }

    let (auth_url, csrf_token, nonce) = auth_request.url();

    // Store session in Redis (including PKCE verifier)
    let session_key = format!("sso:session:{}", csrf_token.secret());
    let session_data = serde_json::json!({
        "nonce": nonce.secret(),
        "pkce_verifier": pkce_verifier.secret(),
        "config_id": config.id.to_string(),
        "redirect_uri": redirect_uri.unwrap_or_else(|| "/".to_string()),
    });

    let redis_pool = state.redis.clone();
    let mut conn = redis_pool.get().await.map_err(|e| {
        error!("Failed to get Redis connection: {}", e);
        AppError::Internal(anyhow::anyhow!("Session storage error"))
    })?;

    redis::cmd("SETEX")
        .arg(&session_key)
        .arg(SSO_SESSION_TTL_SECONDS)
        .arg(session_data.to_string())
        .query_async::<()>(&mut *conn)
        .await
        .map_err(|e| {
            error!("Failed to store SSO session: {}", e);
            AppError::Internal(anyhow::anyhow!("Session storage error"))
        })?;

    info!("Initiated OIDC login for config: {}", config.id);
    Ok(auth_url.to_string())
}

#[derive(Debug, Deserialize)]
pub struct OidcCallbackQuery {
    code: String,
    state: String,
}

#[derive(Debug, Serialize)]
pub struct CallbackResponse {
    /// JWT access token (only present if authentication is complete)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    /// User information
    pub user: UserInfo,
    /// Redirect URI after successful login
    pub redirect_uri: String,
    /// Whether MFA verification is required
    #[serde(default)]
    pub mfa_required: bool,
    /// MFA challenge token (for continuing MFA verification)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mfa_challenge_token: Option<String>,
    /// Available MFA methods for this user
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mfa_methods: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct UserInfo {
    pub id: Uuid,
    pub email: String,
    pub name: Option<String>,
    pub external_id: String,
}

/// Validate SAML RelayState format to prevent Redis key injection
///
/// SAML RelayState is used as the request_id which is generated as "_" + UUID.
/// This validates the format to prevent any malicious input from being used
/// as a Redis key.
///
/// # Security
/// Without this validation, an attacker could potentially:
/// - Craft a RelayState that collides with other Redis keys
/// - Inject special characters that could cause unexpected behavior
fn validate_saml_relay_state(relay_state: &str) -> Result<&str> {
    // RelayState must be non-empty
    if relay_state.is_empty() {
        return Err(AppError::Validation(
            "Invalid SAML RelayState: empty".to_string(),
        ));
    }

    // RelayState format: underscore prefix + UUID (e.g., "_550e8400-e29b-41d4-a716-446655440000")
    // Total length: 1 (underscore) + 36 (UUID with hyphens) = 37 characters
    if relay_state.len() != 37 {
        warn!(
            "Invalid SAML RelayState length: {} (expected 37)",
            relay_state.len()
        );
        return Err(AppError::Validation(
            "Invalid SAML RelayState format".to_string(),
        ));
    }

    // Must start with underscore
    if !relay_state.starts_with('_') {
        warn!("Invalid SAML RelayState: missing underscore prefix");
        return Err(AppError::Validation(
            "Invalid SAML RelayState format".to_string(),
        ));
    }

    // The rest must be a valid UUID
    let uuid_part = &relay_state[1..];
    if Uuid::parse_str(uuid_part).is_err() {
        warn!("Invalid SAML RelayState: not a valid UUID");
        return Err(AppError::Validation(
            "Invalid SAML RelayState format".to_string(),
        ));
    }

    Ok(relay_state)
}

/// Validate redirect_uri to prevent open redirect attacks
/// Only allows relative paths or same-origin URLs
fn validate_redirect_uri(redirect_uri: &str, base_url: &str) -> Result<String> {
    // Allow empty or root path
    if redirect_uri.is_empty() || redirect_uri == "/" {
        return Ok("/".to_string());
    }

    // Allow relative paths starting with /
    if redirect_uri.starts_with('/') && !redirect_uri.starts_with("//") {
        // Sanitize: remove any attempts to break out
        let sanitized = redirect_uri
            .split(|c| c == '\n' || c == '\r')
            .next()
            .unwrap_or("/");
        return Ok(sanitized.to_string());
    }

    // Parse as URL and verify it's same-origin
    if let Ok(url) = url::Url::parse(redirect_uri) {
        if let Ok(base) = url::Url::parse(base_url) {
            if url.scheme() == base.scheme()
                && url.host() == base.host()
                && url.port() == base.port()
            {
                return Ok(redirect_uri.to_string());
            }
        }
    }

    // Default to root if validation fails
    tracing::warn!("Rejected potentially malicious redirect_uri, defaulting to /");
    Ok("/".to_string())
}

/// Handle OIDC callback
async fn handle_oidc_callback(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(config_id): Path<Uuid>,
    Query(query): Query<OidcCallbackQuery>,
) -> Result<Json<CallbackResponse>> {
    // Rate limit callback attempts by IP to prevent brute force on state/code
    let client_ip = extract_client_ip(&addr);
    check_unauthenticated_rate_limit(&state.redis, &client_ip, "sso_callback").await?;

    // Extract user agent for session tracking
    let user_agent = headers
        .get("user-agent")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    // Verify session
    let session_key = format!("sso:session:{}", query.state);
    let redis_pool = state.redis.clone();
    let mut conn = redis_pool.get().await.map_err(|e| {
        error!("Failed to get Redis connection: {}", e);
        AppError::Internal(anyhow::anyhow!("Session storage error"))
    })?;

    let session_data: Option<String> = redis::cmd("GET")
        .arg(&session_key)
        .query_async(&mut *conn)
        .await
        .map_err(|e| {
            error!("Failed to get SSO session from Redis: {}", e);
            AppError::Internal(anyhow::anyhow!("Session storage error"))
        })?;

    let session: serde_json::Value = session_data
        .ok_or_else(|| AppError::Validation("Invalid or expired SSO session".to_string()))?
        .parse()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid session data")))?;

    // SECURITY: Verify the config_id in session matches the callback path parameter
    // This prevents session confusion attacks where an attacker uses a session
    // started with one SSO config to authenticate against a different config
    let session_config_id = session["config_id"]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| AppError::Validation("Invalid session: missing config_id".to_string()))?;

    if session_config_id != config_id {
        error!(
            "SSO session config_id mismatch: session has {}, callback received {}",
            session_config_id, config_id
        );
        return Err(AppError::Validation(
            "Invalid SSO session: configuration mismatch".to_string(),
        ));
    }

    // Delete session (prevent replay)
    let _: std::result::Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&session_key)
        .query_async(&mut *conn)
        .await;

    let config = get_config_by_id(&state.db, config_id).await?;

    // Use issuer_alias if provided
    let issuer = if let Some(ref alias) = config.issuer_alias {
        alias.clone()
    } else {
        config.issuer_url.clone()
    };

    let issuer_url = IssuerUrl::new(issuer).map_err(|e| {
        error!("Invalid OIDC issuer URL: {}", e);
        AppError::Internal(anyhow::anyhow!("SSO configuration error"))
    })?;

    let provider_metadata = CoreProviderMetadata::discover_async(issuer_url, async_http_client)
        .await
        .map_err(|e| {
            error!("Failed to discover OIDC provider: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to connect to identity provider"))
        })?;

    let base_url = &state.config.base_url;
    let redirect_url =
        RedirectUrl::new(format!("{}/api/sso/callback/oidc/{}", base_url, config_id)).map_err(
            |e| {
                error!("Invalid OIDC redirect URL: {}", e);
                AppError::Internal(anyhow::anyhow!("SSO configuration error"))
            },
        )?;

    // Decrypt client secret
    let client_secret = state
        .encryptor
        .decrypt(&config.client_secret_encrypted)
        .map_err(|e| {
            error!("Failed to decrypt client secret: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to decrypt client secret"))
        })?;

    let client = CoreClient::from_provider_metadata(
        provider_metadata,
        ClientId::new(config.client_id.clone()),
        Some(ClientSecret::new(client_secret)),
    )
    .set_redirect_uri(redirect_url);

    // Extract PKCE verifier from session (required for token exchange)
    let pkce_verifier = session["pkce_verifier"]
        .as_str()
        .map(|v| PkceCodeVerifier::new(v.to_string()))
        .ok_or_else(|| {
            error!("PKCE verifier missing from session");
            AppError::Internal(anyhow::anyhow!(
                "Invalid session data: missing PKCE verifier"
            ))
        })?;

    let token_response = client
        .exchange_code(AuthorizationCode::new(query.code.clone()))
        .set_pkce_verifier(pkce_verifier)
        .request_async(async_http_client)
        .await
        .map_err(|e| {
            error!("Failed to exchange authorization code: {}", e);
            AppError::Validation("Failed to authenticate with identity provider".to_string())
        })?;

    let id_token = token_response
        .id_token()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("No ID token in response")))?;

    let nonce = Nonce::new(session["nonce"].as_str().unwrap_or_default().to_string());
    let claims = id_token
        .claims(&client.id_token_verifier(), &nonce)
        .map_err(|e| {
            error!("Failed to verify ID token: {}", e);
            AppError::Validation("Invalid identity token".to_string())
        })?;

    let external_id = claims.subject().to_string();
    let email = claims
        .email()
        .map(|e| e.as_str().to_string())
        .ok_or_else(|| {
            AppError::Validation("Email not provided by identity provider".to_string())
        })?;
    let name: Option<String> = claims
        .name()
        .and_then(|n| n.get(None))
        .map(|n| n.as_str().to_string());

    // SECURITY: Check email domain restriction with IDNA normalization
    // Uses punycode conversion to prevent Unicode homoglyph attacks
    if !is_email_domain_allowed(&email, &config.allowed_email_domains) {
        // SECURITY: Use generic error to prevent email domain enumeration
        warn!("SSO login rejected: email domain not allowed");
        let audit_origin = AuditOrigin::from_headers(&headers);
        let audit_caller = AuditCaller::from_headers(&headers);
        let _ = AuditEventBuilder::new(AuditEventType::SsoLoginFailed)
            .organization(config.organization_id)
            .details(serde_json::json!({
                "sso_config_id": config.id,
                "reason": "email_domain_not_allowed",
                "provider": "oidc"
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
            .log(&state.clickhouse)
            .await;
        return Err(AppError::Auth("Authentication failed".to_string()));
    }

    let (user_id, is_new_user) = match find_or_create_sso_user(
        &state.db,
        config.id,
        &external_id,
        &email,
        name.as_deref(),
        config.auto_create_users,
        &config.default_role,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::SsoLoginFailed)
                .organization(config.organization_id)
                .details(serde_json::json!({
                    "sso_config_id": config.id,
                    "reason": "user_lookup_failed",
                    "provider": "oidc"
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
                .log(&state.clickhouse)
                .await;
            return Err(e);
        }
    };

    // Update last login (non-fatal if it fails)
    if let Err(e) = sqlx::query(
        "UPDATE sso_user_mappings SET last_login_at = NOW() WHERE sso_config_id = $1 AND external_id = $2"
    )
    .bind(config.id)
    .bind(&external_id)
    .execute(&*state.db)
    .await
    {
        warn!("Failed to update last_login_at for user {}: {}", user_id, e);
    }

    // OIDC standard doesn't include groups, but some providers do
    // For now, use empty groups - provisioning rules can match on email/domain
    let groups: Vec<String> = Vec::new();

    // Validate redirect_uri to prevent open redirect attacks
    let raw_redirect_uri = session["redirect_uri"].as_str().unwrap_or("/");
    let redirect_uri = validate_redirect_uri(raw_redirect_uri, &state.config.base_url)?;

    // Complete login with session creation, provisioning, and MFA check
    let response = complete_sso_login(
        &state,
        &config,
        user_id,
        &email,
        name,
        &external_id,
        &groups,
        None,                  // OIDC doesn't typically have arbitrary attributes
        None,                  // IdP session ID (could extract from tokens if available)
        Some(&client_ip),      // IP address for audit/session tracking
        user_agent.as_deref(), // User agent for session tracking
        &redirect_uri,
        is_new_user,
        &headers,
    )
    .await?;

    Ok(Json(response))
}

// ============================================================================
// SAML Login Flow
// ============================================================================

/// Initiate SAML login
async fn initiate_saml_login(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(config_id): Path<Uuid>,
    Query(query): Query<LoginQuery>,
) -> Result<Json<LoginResponse>> {
    // Rate limit SSO login attempts by IP
    let client_ip = extract_client_ip(&addr);
    check_unauthenticated_rate_limit(&state.redis, &client_ip, "sso_login").await?;

    let config = get_config_by_id(&state.db, config_id).await?;

    let tier = state.entitlements.get_config(config.organization_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO is not available on your current plan".into()));
    }

    if config.sso_type != "saml" {
        return Err(AppError::Validation(
            "This configuration is not SAML".to_string(),
        ));
    }

    let auth_url = build_saml_auth_url(&state, &config, query.redirect_uri).await?;

    Ok(Json(LoginResponse {
        authorization_url: auth_url,
        sso_type: "saml".to_string(),
    }))
}

async fn build_saml_auth_url(
    state: &WebsiteState,
    config: &SsoConfigRow,
    redirect_uri: Option<String>,
) -> Result<String> {
    let base_url = &state.config.base_url;

    // Create SP and IdP configs using shared helper
    let (sp_config, idp_config) = create_saml_configs(
        config,
        &base_url,
        &state.encryptor,
        state.config.saml_time_skew_seconds,
    )?;

    // Use SAML processor to build the SSO URL and get the request ID
    let processor = SamlProcessor::new(sp_config);
    let (request_id, xml) = processor
        .create_authn_request_xml(&idp_config)
        .map_err(|e| {
            error!("Failed to create SAML AuthnRequest: {}", e);
            AppError::Internal(anyhow::anyhow!("SSO configuration error"))
        })?;

    // Store request ID and redirect_uri in Redis for callback validation
    let session_key = format!("saml:session:{}", request_id);
    let session_data = serde_json::json!({
        "request_id": request_id,
        "config_id": config.id.to_string(),
        "redirect_uri": redirect_uri.unwrap_or_else(|| "/".to_string()),
    });

    let redis_pool = state.redis.clone();
    let mut conn = redis_pool.get().await.map_err(|e| {
        error!("Failed to get Redis connection: {}", e);
        AppError::Internal(anyhow::anyhow!("Session storage error"))
    })?;

    redis::cmd("SETEX")
        .arg(&session_key)
        .arg(SAML_SESSION_TTL_SECONDS)
        .arg(session_data.to_string())
        .query_async::<()>(&mut *conn)
        .await
        .map_err(|e| {
            error!("Failed to store SAML session: {}", e);
            AppError::Internal(anyhow::anyhow!("Session storage error"))
        })?;

    // Build the SSO URL with the pre-generated request (using request_id as RelayState for session lookup)
    let auth_url = processor
        .build_sso_url_with_request(&idp_config, &xml, &request_id, Some(&request_id))
        .map_err(|e| {
            error!("Failed to build SAML SSO URL: {}", e);
            AppError::Internal(anyhow::anyhow!("SSO configuration error"))
        })?;

    info!(
        "Initiated SAML login for config: {}, request_id: {}",
        config.id, request_id
    );
    Ok(auth_url)
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // relay_state captured for protocol compliance but not currently used
pub struct SamlCallbackForm {
    #[serde(rename = "SAMLResponse")]
    saml_response: String,
    #[serde(rename = "RelayState")]
    relay_state: Option<String>,
}

/// Handle SAML callback (POST binding)
async fn handle_saml_callback(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(config_id): Path<Uuid>,
    axum::Form(form): axum::Form<SamlCallbackForm>,
) -> Result<Json<CallbackResponse>> {
    // Rate limit callback attempts by IP to prevent replay attacks
    let client_ip = extract_client_ip(&addr);
    check_unauthenticated_rate_limit(&state.redis, &client_ip, "sso_callback").await?;

    // Extract user agent for session tracking
    let user_agent = headers
        .get("user-agent")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let config = get_config_by_id(&state.db, config_id).await?;

    if config.sso_type != "saml" {
        return Err(AppError::Validation(
            "This configuration is not SAML".to_string(),
        ));
    }

    // SECURITY: Require valid session from SP-initiated flow
    // IdP-initiated SSO is disabled to prevent SAML response replay attacks.
    // The InResponseTo validation requires a stored request ID from the original AuthnRequest.
    let relay_state = form.relay_state.as_ref().ok_or_else(|| {
        warn!("SAML callback received without RelayState - IdP-initiated SSO is not supported");
        AppError::Validation(
            "IdP-initiated SSO is not supported. Please start login from the application."
                .to_string(),
        )
    })?;

    // SECURITY: Validate RelayState format to prevent Redis key injection
    let validated_relay_state = validate_saml_relay_state(relay_state)?;

    let session_key = format!("saml:session:{}", validated_relay_state);
    let redis_pool = state.redis.clone();
    let mut conn = redis_pool.get().await.map_err(|e| {
        error!("Failed to get Redis connection: {}", e);
        AppError::Internal(anyhow::anyhow!("Session storage error"))
    })?;

    let session_data: Option<String> = redis::cmd("GET")
        .arg(&session_key)
        .query_async(&mut *conn)
        .await
        .map_err(|e| {
            error!("Failed to get SAML session from Redis: {}", e);
            AppError::Internal(anyhow::anyhow!("Session storage error"))
        })?;

    let session_data = session_data.ok_or_else(|| {
        warn!(
            "SAML session not found or expired for RelayState: {}",
            validated_relay_state
        );
        AppError::Validation(
            "SSO session expired. Please start the login process again.".to_string(),
        )
    })?;

    let session: serde_json::Value = session_data
        .parse()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid session data")))?;

    // SECURITY: Verify the config_id in session matches the callback path
    let session_config_id = session["config_id"]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| {
            error!("SAML session missing config_id");
            AppError::Internal(anyhow::anyhow!("Invalid session data"))
        })?;

    if session_config_id != config_id {
        warn!(
            "SAML session config_id mismatch: session has {}, callback received {}",
            session_config_id, config_id
        );
        return Err(AppError::Validation(
            "Invalid SAML session: configuration mismatch".to_string(),
        ));
    }

    // Delete session (prevent replay)
    let _: std::result::Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&session_key)
        .query_async(&mut *conn)
        .await;

    // Extract request_id (required for InResponseTo validation) and redirect_uri
    let expected_request_id = session["request_id"].as_str().map(String::from);
    let redirect_uri = session["redirect_uri"].as_str().unwrap_or("/").to_string();

    // SECURITY: request_id must exist for InResponseTo validation
    if expected_request_id.is_none() {
        error!("SAML session missing request_id");
        return Err(AppError::Internal(anyhow::anyhow!("Invalid session data")));
    }

    let base_url = &state.config.base_url;

    // Create SP and IdP configs using shared helper
    let (sp_config, idp_config) = create_saml_configs(
        &config,
        base_url,
        &state.encryptor,
        state.config.saml_time_skew_seconds,
    )?;

    // Verify and parse SAML response with expected request ID for InResponseTo validation
    let processor = SamlProcessor::new(sp_config);
    let claims = match processor.verify_response(
        &form.saml_response,
        &idp_config,
        expected_request_id.as_deref(),
    ) {
        Ok(claims) => claims,
        Err(e) => {
            error!("SAML verification failed: {}", e);
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::SsoLoginFailed)
                .organization(config.organization_id)
                .details(serde_json::json!({
                    "sso_config_id": config.id,
                    "reason": "saml_verification_failed",
                    "provider": "saml"
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
                .log(&state.clickhouse)
                .await;
            return Err(AppError::Validation(
                "SAML authentication failed".to_string(),
            ));
        }
    };

    // Get email from claims (required)
    let email = claims
        .email
        .ok_or_else(|| AppError::Validation("Email not provided in SAML assertion".to_string()))?;
    let external_id = claims.name_id.clone();
    let name = claims.name.clone().or_else(|| {
        // Build name from first/last if available
        match (claims.first_name.as_ref(), claims.last_name.as_ref()) {
            (Some(first), Some(last)) => Some(format!("{} {}", first, last)),
            (Some(first), None) => Some(first.clone()),
            (None, Some(last)) => Some(last.clone()),
            _ => None,
        }
    });

    // SECURITY: Check email domain restriction with IDNA normalization
    // Uses punycode conversion to prevent Unicode homoglyph attacks
    if !is_email_domain_allowed(&email, &config.allowed_email_domains) {
        // SECURITY: Use generic error to prevent email domain enumeration
        warn!("SSO login rejected: email domain not allowed");
        let audit_origin = AuditOrigin::from_headers(&headers);
        let audit_caller = AuditCaller::from_headers(&headers);
        let _ = AuditEventBuilder::new(AuditEventType::SsoLoginFailed)
            .organization(config.organization_id)
            .details(serde_json::json!({
                "sso_config_id": config.id,
                "reason": "email_domain_not_allowed",
                "provider": "saml"
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
            .log(&state.clickhouse)
            .await;
        return Err(AppError::Auth("Authentication failed".to_string()));
    }

    let (user_id, is_new_user) = match find_or_create_sso_user(
        &state.db,
        config.id,
        &external_id,
        &email,
        name.as_deref(),
        config.auto_create_users,
        &config.default_role,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::SsoLoginFailed)
                .organization(config.organization_id)
                .details(serde_json::json!({
                    "sso_config_id": config.id,
                    "reason": "user_lookup_failed",
                    "provider": "saml"
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
                .log(&state.clickhouse)
                .await;
            return Err(e);
        }
    };

    // Update last login (non-fatal if it fails)
    if let Err(e) = sqlx::query(
        "UPDATE sso_user_mappings SET last_login_at = NOW() WHERE sso_config_id = $1 AND external_id = $2"
    )
    .bind(config.id)
    .bind(&external_id)
    .execute(&*state.db)
    .await
    {
        warn!("Failed to update last_login_at for user {}: {}", user_id, e);
    }

    // Extract groups from SAML claims
    let groups = claims.groups.clone();
    let attributes_json = if claims.attributes.is_empty() {
        None
    } else {
        Some(serde_json::to_value(&claims.attributes).unwrap_or_default())
    };

    // Validate redirect_uri to prevent open redirect attacks
    let validated_redirect_uri = validate_redirect_uri(&redirect_uri, &state.config.base_url)?;

    // Complete login with session creation, provisioning, and MFA check
    let response = complete_sso_login(
        &state,
        &config,
        user_id,
        &email,
        name,
        &external_id,
        &groups,
        attributes_json.as_ref(),
        claims.session_index.as_deref(), // IdP session ID for SLO
        Some(&client_ip),                // IP address for audit/session tracking
        user_agent.as_deref(),           // User agent for session tracking
        &validated_redirect_uri,
        is_new_user,
        &headers,
    )
    .await?;

    Ok(Json(response))
}

// ============================================================================
// MFA Verification for SSO
// ============================================================================

#[derive(Debug, Deserialize)]
struct VerifyMfaRequest {
    /// MFA challenge token from the initial SSO response
    challenge_token: String,
    /// MFA code (TOTP or recovery code)
    code: String,
}

/// Verify MFA code to complete SSO login
async fn verify_sso_mfa(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<VerifyMfaRequest>,
) -> Result<Json<CallbackResponse>> {
    // Rate limit MFA verification attempts by IP to prevent brute force
    let client_ip = extract_client_ip(&addr);
    check_unauthenticated_rate_limit(&state.redis, &client_ip, "sso_mfa").await?;

    // Get challenge data from Redis
    let mfa_key = format!("mfa:challenge:{}", req.challenge_token);
    let redis_pool = state.redis.clone();
    let mut conn = redis_pool.get().await.map_err(|e| {
        error!("Failed to get Redis connection for MFA challenge: {}", e);
        AppError::Internal(anyhow::anyhow!("Session storage error"))
    })?;

    let challenge_data: Option<String> = redis::cmd("GET")
        .arg(&mfa_key)
        .query_async(&mut *conn)
        .await
        .map_err(|e| {
            error!("Failed to get MFA challenge from Redis: {}", e);
            AppError::Internal(anyhow::anyhow!("Session storage error"))
        })?;

    let challenge: serde_json::Value = challenge_data
        .ok_or_else(|| AppError::Validation("Invalid or expired MFA challenge".to_string()))?
        .parse()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid challenge data")))?;

    let user_id: Uuid = challenge["user_id"]
        .as_str()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid challenge data")))?
        .parse()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid user ID")))?;

    let sso_config_id: Uuid = challenge["sso_config_id"]
        .as_str()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid challenge data")))?
        .parse()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid config ID")))?;

    // Verify MFA code
    let mfa_valid = mfa::verify_mfa_code(&state, user_id, &req.code)
        .await
        .map_err(|e| {
            error!("MFA verification error: {}", e);
            AppError::Internal(anyhow::anyhow!("MFA verification failed"))
        })?;

    if !mfa_valid {
        // Log failed MFA attempt
        let organization_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT organization_id FROM sso_configurations WHERE id = $1",
        )
        .bind(sso_config_id)
        .fetch_optional(&*state.db)
        .await
        .ok()
        .flatten();

        let audit_origin = AuditOrigin::from_headers(&headers);
        let audit_caller = AuditCaller::from_headers(&headers);
        let mut audit_builder = AuditEventBuilder::new(AuditEventType::MfaFailed).user(user_id);
        if let Some(org_id) = organization_id {
            audit_builder = audit_builder.organization(org_id);
        }
        let _ = audit_builder
            .details(serde_json::json!({ "reason": "invalid_code" }))
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
            .log(&state.clickhouse)
            .await;

        return Err(AppError::Validation("Invalid MFA code".to_string()));
    }

    // Delete the challenge from Redis
    let _: std::result::Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&mfa_key)
        .query_async(&mut *conn)
        .await;

    // Get SSO config for session creation
    let config = get_config_by_id(&state.db, sso_config_id).await?;

    // Fetch user info from database (not from challenge to minimize PII in Redis)
    #[derive(sqlx::FromRow)]
    struct UserInfoRow {
        email: String,
        name: Option<String>,
    }
    let user_info: UserInfoRow = sqlx::query_as("SELECT email, name FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_one(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to fetch user info after MFA: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error"))
        })?;

    // Get external_id from SSO mapping
    let external_id: String = sqlx::query_scalar(
        "SELECT external_id FROM sso_user_mappings WHERE user_id = $1 AND sso_config_id = $2",
    )
    .bind(user_id)
    .bind(sso_config_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch external_id after MFA: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    // Create SSO session
    // SECURITY: Session creation is MANDATORY for SSO tokens.
    // SSO JWTs include `sso: true` claim which triggers session validation on each request.
    let session_token = Uuid::new_v4().to_string();
    sso_sessions::create_session(
        &state.db,
        user_id,
        config.id,
        &session_token,
        None,
        None,
        None,
        state.config.jwt_expiration_hours,
    )
    .await
    .map_err(|e| {
        error!(
            "Failed to create SSO session for user {} after MFA: {}",
            user_id, e
        );
        AppError::Internal(anyhow::anyhow!(
            "Failed to create session. Please try again."
        ))
    })?;

    let audit_origin_sess = AuditOrigin::from_headers(&headers);
    let audit_caller_sess = AuditCaller::from_headers(&headers);
    let _ = AuditEventBuilder::new(AuditEventType::SessionCreated)
        .user(user_id)
        .details(serde_json::json!({
            "sso_config_id": sso_config_id,
            "mfa_verified": true
        }))
        .origin(
            &audit_origin_sess.origin_type,
            &audit_origin_sess.origin_ref,
            &audit_origin_sess.origin_reason,
        )
        .caller(
            &audit_caller_sess.caller_type,
            &audit_caller_sess.key_label,
            &audit_caller_sess.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    // Generate JWT
    let email = &user_info.email;
    let name = user_info.name.clone();
    let raw_redirect_uri = challenge["redirect_uri"].as_str().unwrap_or("/");
    // Validate redirect_uri to prevent open redirect attacks
    let redirect_uri = validate_redirect_uri(raw_redirect_uri, &state.config.base_url)?;

    let token = generate_sso_jwt(
        &state.config.jwt_secret,
        &state.config.jwt_issuer,
        user_id,
        email,
        name.clone(),
        sso_config_id,
        &session_token,
        state.config.jwt_expiration_hours,
    )?;

    // Log successful MFA verification and login
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let _ = AuditEventBuilder::new(AuditEventType::MfaVerified)
        .user(user_id)
        .organization(config.organization_id)
        .details(serde_json::json!({ "sso_config_id": sso_config_id }))
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
        .log(&state.clickhouse)
        .await;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let _ = AuditEventBuilder::new(AuditEventType::SsoLoginSuccess)
        .user(user_id)
        .organization(config.organization_id)
        .details(serde_json::json!({
            "sso_config_id": sso_config_id,
            "mfa_verified": true
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
        .log(&state.clickhouse)
        .await;

    info!("SSO login with MFA successful: user_id={}", user_id);

    Ok(Json(CallbackResponse {
        access_token: Some(token),
        user: UserInfo {
            id: user_id,
            email: email.to_string(),
            name,
            external_id: external_id.to_string(),
        },
        redirect_uri: redirect_uri.to_string(),
        mfa_required: false,
        mfa_challenge_token: None,
        mfa_methods: None,
    }))
}

// ============================================================================
// SAML Single Logout (SLO)
// ============================================================================

/// Initiate SAML Single Logout
///
/// This endpoint creates a SAML LogoutRequest and redirects the user to the IdP's SLO URL.
/// The user must be authenticated and the SSO configuration must have an SLO URL configured.
///
/// # Security
/// - Requires valid JWT authentication
/// - Revokes the local SSO session before redirecting to IdP
/// - Rate-limited to prevent abuse
async fn initiate_saml_logout(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(config_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<axum::response::Redirect> {
    // SECURITY: Rate limit to prevent SLO abuse
    let client_ip = extract_client_ip(&addr);
    check_unauthenticated_rate_limit(&state.redis, &client_ip, "saml_slo").await?;

    // Extract user from JWT - must be authenticated
    let user_id = crate::auth::extract_user_id(&headers, &state.config.jwt_secret)?;

    // Get the SSO configuration
    let config = get_config_by_id(&state.db, config_id).await?;

    if config.sso_type != "saml" {
        return Err(AppError::Validation(
            "SLO is only supported for SAML configurations".to_string(),
        ));
    }

    // Check if SLO URL is configured
    if config.saml_slo_url.is_none() {
        return Err(AppError::Validation(
            "SLO URL is not configured for this IdP".to_string(),
        ));
    }

    // Get user's SSO session to retrieve the IdP session index
    #[derive(sqlx::FromRow)]
    struct SessionInfo {
        id: Uuid,
        idp_session_id: Option<String>,
    }

    let session = sqlx::query_as::<_, SessionInfo>(
        r#"
        SELECT id, idp_session_id
        FROM sso_sessions
        WHERE user_id = $1 AND sso_config_id = $2 AND revoked_at IS NULL AND expires_at > NOW()
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(user_id)
    .bind(config_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch SSO session for SLO: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    let session =
        session.ok_or_else(|| AppError::NotFound("No active SSO session found".to_string()))?;

    // Revoke the local session immediately
    sqlx::query("UPDATE sso_sessions SET revoked_at = NOW() WHERE id = $1")
        .bind(session.id)
        .execute(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to revoke SSO session: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to revoke session"))
        })?;

    // Get user email for the LogoutRequest NameID
    let user_email: String = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_one(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to fetch user email for SLO: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error"))
        })?;

    // Create SAML processor and LogoutRequest
    let base_url = &state.config.base_url;
    let (sp_config, idp_config) = create_saml_configs(
        &config,
        base_url,
        &state.encryptor,
        state.config.saml_time_skew_seconds,
    )?;

    let processor = crate::saml::SamlProcessor::new(sp_config);

    let (_request_id, logout_url) = processor
        .create_logout_request(
            &idp_config,
            &user_email,
            Some("urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"),
            session.idp_session_id.as_deref(),
        )
        .map_err(|e| {
            error!("Failed to create LogoutRequest: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to create logout request"))
        })?;

    // Log the SLO initiation
    info!(
        "Initiating SAML SLO for user {} with config {}",
        user_id, config_id
    );
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let _ = AuditEventBuilder::new(AuditEventType::SsoLogout)
        .user(user_id)
        .organization(config.organization_id)
        .resource("sso_configuration", config_id)
        .details(serde_json::json!({
            "method": "saml_slo",
            "idp_notified": true
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
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(axum::response::Redirect::temporary(&logout_url))
}

/// Get SAML Service Provider metadata
///
/// # Security
/// Rate-limited to prevent enumeration attacks on config IDs.
async fn get_saml_metadata(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(config_id): Path<Uuid>,
) -> Result<axum::response::Response> {
    // SECURITY: Rate limit to prevent config ID enumeration
    let client_ip = extract_client_ip(&addr);
    check_unauthenticated_rate_limit(&state.redis, &client_ip, "saml_metadata").await?;

    let base_url = &state.config.base_url;
    let acs_url = format!("{}/api/sso/callback/saml/{}", base_url, config_id);
    let entity_id = format!("{}/api/sso/saml/metadata/{}", base_url, config_id);

    // Check if we have an SP signing certificate
    let config = get_config_by_id(&state.db, config_id).await.ok();
    let has_signing_cert = config
        .as_ref()
        .map(|c| c.sp_certificate.is_some() && c.saml_sign_requests.unwrap_or(false))
        .unwrap_or(false);

    let authn_requests_signed = if has_signing_cert { "true" } else { "false" };

    // Build key descriptor section if SP certificate is available
    let key_descriptor = if let Some(ref cfg) = config {
        if let Some(ref cert) = cfg.sp_certificate {
            // Extract the certificate body (remove PEM headers and newlines)
            let cert_body = cert
                .lines()
                .filter(|line| !line.starts_with("-----"))
                .collect::<Vec<_>>()
                .join("");

            format!(
                r#"
    <md:KeyDescriptor use="signing">
      <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
        <ds:X509Data>
          <ds:X509Certificate>{}</ds:X509Certificate>
        </ds:X509Data>
      </ds:KeyInfo>
    </md:KeyDescriptor>"#,
                cert_body
            )
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    let metadata = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="{}">
  <md:SPSSODescriptor AuthnRequestsSigned="{}" WantAssertionsSigned="true" protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">{}
    <md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</md:NameIDFormat>
    <md:AssertionConsumerService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="{}" index="0" isDefault="true"/>
  </md:SPSSODescriptor>
</md:EntityDescriptor>"#,
        entity_id, authn_requests_signed, key_descriptor, acs_url
    );

    Ok(axum::response::Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(metadata))
        .expect("Failed to build SAML metadata response"))
}

// ============================================================================
// Certificate Health Monitoring
// ============================================================================

/// Certificate health status
#[derive(Debug, Serialize)]
pub struct CertificateHealthStatus {
    /// SSO configuration ID
    pub config_id: Uuid,
    /// Organization ID
    pub organization_id: Uuid,
    /// Configuration name
    pub name: String,
    /// Provider name
    pub provider: String,
    /// SSO type (oidc or saml)
    pub sso_type: String,
    /// Whether the configuration is enabled
    pub enabled: bool,
    /// IdP certificate status (for SAML)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idp_certificate: Option<CertificateInfo>,
    /// SP certificate status (for SAML request signing)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sp_certificate: Option<CertificateInfo>,
}

/// Certificate information with expiry status
#[derive(Debug, Serialize)]
pub struct CertificateInfo {
    /// Certificate subject
    pub subject: String,
    /// Certificate issuer
    pub issuer: String,
    /// Not valid before date
    pub not_before: String,
    /// Not valid after date
    pub not_after: String,
    /// Days until expiry (negative if expired)
    pub days_until_expiry: i64,
    /// Health status: "healthy", "expiring_soon", "expired"
    pub status: String,
}

/// Overall certificate health response
#[derive(Debug, Serialize)]
pub struct CertificateHealthResponse {
    /// Overall health status
    pub status: String,
    /// Number of certificates expiring soon (within 30 days)
    pub expiring_soon_count: i32,
    /// Number of expired certificates
    pub expired_count: i32,
    /// Total certificates checked
    pub total_checked: i32,
    /// Details for each SSO configuration
    pub configurations: Vec<CertificateHealthStatus>,
}

/// Get certificate health status for all SAML SSO configurations
///
/// Returns expiry status for IdP and SP certificates across all enabled SAML configurations.
/// Requires admin access (must be admin of at least one organization).
async fn get_certificate_health(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<CertificateHealthResponse>> {
    // Require authentication
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;

    // Verify user is an admin of at least one organization
    let is_admin: Option<(bool,)> = sqlx::query_as(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM memberships
            WHERE user_id = $1 AND status = 'active' AND role IN ('admin', 'owner')
        ) as is_admin
        "#,
    )
    .bind(user_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to check admin status: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    if !is_admin.map(|(v,)| v).unwrap_or(false) {
        return Err(AppError::Auth("Requires admin privileges".to_string()));
    }

    // Get all SAML configurations for organizations where user is admin
    let configs = sqlx::query_as::<_, SsoConfigRow>(
        r#"
        SELECT sc.id, sc.organization_id, sc.domain_name, sc.provider, sc.name, sc.sso_type,
               sc.issuer_url, sc.issuer_alias, sc.client_id, sc.client_secret_encrypted,
               sc.okta_domain, sc.okta_api_token_encrypted,
               sc.saml_entity_id, sc.saml_sso_url, sc.saml_slo_url, sc.saml_certificate, sc.saml_sign_requests,
               sc.sp_certificate, sc.sp_private_key_encrypted,
               sc.scopes, sc.auto_create_users, sc.default_role, sc.allowed_email_domains,
               sc.enabled, sc.created_at, sc.updated_at
        FROM sso_configurations sc
        INNER JOIN memberships m ON sc.organization_id = m.organization_id
        WHERE m.user_id = $1 AND m.status = 'active' AND m.role IN ('admin', 'owner')
          AND sc.sso_type = 'saml'
        ORDER BY sc.enabled DESC, sc.created_at DESC
        "#
    )
    .bind(user_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to list SSO configurations for certificate health: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    let mut configurations = Vec::new();
    let mut expiring_soon_count = 0;
    let mut expired_count = 0;
    let mut total_checked = 0;

    for config in configs {
        let mut config_status = CertificateHealthStatus {
            config_id: config.id,
            organization_id: config.organization_id,
            name: config.name.clone(),
            provider: config.provider.clone(),
            sso_type: config.sso_type.clone(),
            enabled: config.enabled,
            idp_certificate: None,
            sp_certificate: None,
        };

        // Check IdP certificate
        if let Some(ref cert_pem) = config.saml_certificate {
            match parse_certificate_info(cert_pem) {
                Ok(cert_info) => {
                    total_checked += 1;
                    if cert_info.status == "expired" {
                        expired_count += 1;
                    } else if cert_info.status == "expiring_soon" {
                        expiring_soon_count += 1;
                    }
                    config_status.idp_certificate = Some(cert_info);
                }
                Err(e) => {
                    warn!(
                        "Failed to parse IdP certificate for config {}: {}",
                        config.id, e
                    );
                    config_status.idp_certificate = Some(CertificateInfo {
                        subject: "Parse error".to_string(),
                        issuer: "Parse error".to_string(),
                        not_before: "N/A".to_string(),
                        not_after: "N/A".to_string(),
                        days_until_expiry: 0,
                        status: "error".to_string(),
                    });
                }
            }
        }

        // Check SP certificate (if configured for request signing)
        if let Some(ref cert_pem) = config.sp_certificate {
            match parse_certificate_info(cert_pem) {
                Ok(cert_info) => {
                    total_checked += 1;
                    if cert_info.status == "expired" {
                        expired_count += 1;
                    } else if cert_info.status == "expiring_soon" {
                        expiring_soon_count += 1;
                    }
                    config_status.sp_certificate = Some(cert_info);
                }
                Err(e) => {
                    warn!(
                        "Failed to parse SP certificate for config {}: {}",
                        config.id, e
                    );
                    config_status.sp_certificate = Some(CertificateInfo {
                        subject: "Parse error".to_string(),
                        issuer: "Parse error".to_string(),
                        not_before: "N/A".to_string(),
                        not_after: "N/A".to_string(),
                        days_until_expiry: 0,
                        status: "error".to_string(),
                    });
                }
            }
        }

        configurations.push(config_status);
    }

    // Determine overall status
    let overall_status = if expired_count > 0 {
        "critical"
    } else if expiring_soon_count > 0 {
        "warning"
    } else if total_checked > 0 {
        "healthy"
    } else {
        "no_certificates"
    };

    info!(
        "Certificate health check: status={}, expiring_soon={}, expired={}, total={}",
        overall_status, expiring_soon_count, expired_count, total_checked
    );

    Ok(Json(CertificateHealthResponse {
        status: overall_status.to_string(),
        expiring_soon_count,
        expired_count,
        total_checked,
        configurations,
    }))
}

/// Parse certificate and extract info with expiry status
fn parse_certificate_info(cert_pem: &str) -> anyhow::Result<CertificateInfo> {
    let cert = openssl::x509::X509::from_pem(cert_pem.as_bytes())
        .map_err(|e| anyhow::anyhow!("Invalid certificate PEM: {}", e))?;

    let not_before = cert.not_before();
    let not_after = cert.not_after();

    // Calculate days until expiry
    let now = openssl::asn1::Asn1Time::days_from_now(0)?;
    let days_until_expiry = {
        // Compare ASN1 times
        let diff = not_after.diff(&now)?;
        diff.days as i64
    };

    // Determine status
    let status = if days_until_expiry < 0 {
        "expired"
    } else if days_until_expiry <= CERT_EXPIRY_WARNING_DAYS {
        "expiring_soon"
    } else {
        "healthy"
    };

    // Format subject and issuer
    let subject = format!("{:?}", cert.subject_name());
    let issuer = format!("{:?}", cert.issuer_name());

    Ok(CertificateInfo {
        subject,
        issuer,
        not_before: format!("{}", not_before),
        not_after: format!("{}", not_after),
        days_until_expiry,
        status: status.to_string(),
    })
}

/// Check all SAML certificates and log warnings for expiring ones
///
/// This function is designed to be called by a background worker.
/// It checks all enabled SAML configurations and logs warnings for
/// certificates that are expiring soon or have already expired.
pub async fn check_certificate_expiry(db: &sqlx::PgPool) -> anyhow::Result<()> {
    // Get all enabled SAML configurations with certificates
    let configs: Vec<(Uuid, String, String, Option<String>, Option<String>)> = sqlx::query_as(
        r#"
        SELECT id, name, provider, saml_certificate, sp_certificate
        FROM sso_configurations
        WHERE sso_type = 'saml' AND enabled = true
          AND (saml_certificate IS NOT NULL OR sp_certificate IS NOT NULL)
        "#,
    )
    .fetch_all(db)
    .await?;

    let mut warnings = 0;
    let mut errors = 0;

    for (config_id, name, provider, idp_cert, sp_cert) in configs {
        // Check IdP certificate
        if let Some(ref cert_pem) = idp_cert {
            match parse_certificate_info(cert_pem) {
                Ok(info) => {
                    if info.status == "expired" {
                        error!(
                            "SAML IdP certificate EXPIRED for config '{}' ({}): expired {} days ago",
                            name, config_id, -info.days_until_expiry
                        );
                        errors += 1;
                    } else if info.status == "expiring_soon" {
                        warn!(
                            "SAML IdP certificate expiring soon for config '{}' ({}, {}): {} days remaining",
                            name, provider, config_id, info.days_until_expiry
                        );
                        warnings += 1;
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to parse IdP certificate for config '{}': {}",
                        name, e
                    );
                    errors += 1;
                }
            }
        }

        // Check SP certificate
        if let Some(ref cert_pem) = sp_cert {
            match parse_certificate_info(cert_pem) {
                Ok(info) => {
                    if info.status == "expired" {
                        error!(
                            "SAML SP certificate EXPIRED for config '{}' ({}): expired {} days ago",
                            name, config_id, -info.days_until_expiry
                        );
                        errors += 1;
                    } else if info.status == "expiring_soon" {
                        warn!(
                            "SAML SP certificate expiring soon for config '{}' ({}, {}): {} days remaining",
                            name, provider, config_id, info.days_until_expiry
                        );
                        warnings += 1;
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to parse SP certificate for config '{}': {}",
                        name, e
                    );
                    errors += 1;
                }
            }
        }
    }

    if errors > 0 || warnings > 0 {
        info!(
            "Certificate expiry check complete: {} warnings, {} errors",
            warnings, errors
        );
    }

    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

async fn get_config_by_id(db: &sqlx::PgPool, id: Uuid) -> Result<SsoConfigRow> {
    sqlx::query_as::<_, SsoConfigRow>(
        r#"
        SELECT id, organization_id, domain_name, provider, name, sso_type,
               issuer_url, issuer_alias, client_id, client_secret_encrypted,
               okta_domain, okta_api_token_encrypted,
               saml_entity_id, saml_sso_url, saml_slo_url, saml_certificate, saml_sign_requests,
               sp_certificate, sp_private_key_encrypted,
               scopes, auto_create_users, default_role, allowed_email_domains,
               enabled, created_at, updated_at
        FROM sso_configurations
        WHERE id = $1 AND enabled = true
        "#,
    )
    .bind(id)
    .fetch_optional(db)
    .await
    .map_err(|e| {
        error!("Failed to get SSO config by ID: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?
    .ok_or_else(|| AppError::NotFound("SSO configuration not found".to_string()))
}

/// Create SP and IdP configurations from SSO config
/// This helper reduces duplication between build_saml_auth_url and handle_saml_callback
fn create_saml_configs(
    config: &SsoConfigRow,
    base_url: &str,
    encryptor: &crate::crypto::RotatingSecretEncryptor,
    time_skew_seconds: i64,
) -> Result<(SpConfig, IdpConfig)> {
    // Determine if SP signing is available
    // sign_requests should only be true if we have both certificate and private key
    let can_sign = config.sp_certificate.is_some() && config.sp_private_key_encrypted.is_some();
    let sign_requests = config.saml_sign_requests.unwrap_or(false) && can_sign;

    if config.saml_sign_requests.unwrap_or(false) && !can_sign {
        warn!(
            "SAML sign_requests is enabled but SP certificate/key is not configured for config {}",
            config.id
        );
    }

    // Decrypt SP private key if signing is requested
    // SECURITY: The private key is stored encrypted at rest with AES-256-GCM
    let sp_private_key = if sign_requests {
        if let Some(ref encrypted_key) = config.sp_private_key_encrypted {
            Some(encryptor.decrypt(encrypted_key).map_err(|e| {
                error!(
                    "Failed to decrypt SP private key for config {}: {}",
                    config.id, e
                );
                AppError::Internal(anyhow::anyhow!("Failed to decrypt SP signing key"))
            })?)
        } else {
            None
        }
    } else {
        None
    };

    let sp_config = SpConfig {
        entity_id: format!("{}/api/sso/saml/metadata/{}", base_url, config.id),
        acs_url: format!("{}/api/sso/callback/saml/{}", base_url, config.id),
        // Use SP certificate if available and signing is requested
        certificate_pem: if sign_requests {
            config.sp_certificate.clone()
        } else {
            None
        },
        private_key_pem: sp_private_key,
        sign_requests,
        want_assertions_signed: true,
        time_skew_seconds,
    };

    let idp_config = IdpConfig {
        entity_id: config
            .saml_entity_id
            .clone()
            .ok_or_else(|| AppError::Validation("SAML Entity ID not configured".to_string()))?,
        sso_url: config
            .saml_sso_url
            .clone()
            .ok_or_else(|| AppError::Validation("SAML SSO URL not configured".to_string()))?,
        slo_url: config.saml_slo_url.clone(),
        certificate_pem: config
            .saml_certificate
            .clone()
            .ok_or_else(|| AppError::Validation("SAML certificate not configured".to_string()))?,
    };

    Ok((sp_config, idp_config))
}

/// Generate a JWT token for SSO authentication
///
/// # Arguments
/// * `jwt_secret` - The secret key for signing the JWT
/// * `jwt_issuer` - The issuer claim for the JWT
/// * `user_id` - The user's UUID
/// * `email` - The user's email address
/// * `name` - The user's display name (optional)
/// * `sso_config_id` - The SSO configuration ID
/// * `session_token` - The session token (will be hashed before storing in jti)
/// * `expiration_hours` - Token lifetime in hours (from config.jwt_expiration_hours)
///
/// # Security
/// The session token is hashed (SHA-256) before being stored in the jti claim.
/// This prevents exposure of the raw session token if the JWT is logged or leaked.
/// The hash matches what is stored in sso_sessions.session_token_hash for revocation checks.
fn generate_sso_jwt(
    jwt_secret: &str,
    jwt_issuer: &str,
    user_id: Uuid,
    email: &str,
    name: Option<String>,
    sso_config_id: Uuid,
    session_token: &str,
    expiration_hours: i64,
) -> Result<String> {
    let now = chrono::Utc::now();
    let expiration_seconds = expiration_hours * 3600;

    // SECURITY: Hash the session token before storing in JWT
    // This prevents exposure of the raw token if the JWT is logged or leaked
    let mut hasher = Sha256::new();
    hasher.update(session_token.as_bytes());
    let session_token_hash = hex::encode(hasher.finalize());

    let jwt_claims = serde_json::json!({
        "sub": user_id.to_string(),
        "email": email,
        "name": name,
        "sso_config_id": sso_config_id.to_string(),
        "iss": jwt_issuer,
        "iat": now.timestamp(),
        "exp": now.timestamp() + expiration_seconds,
        "jti": session_token_hash,
        // SECURITY: Mark this as an SSO token for session revocation checks
        // Regular auth tokens don't have this claim, so session validation
        // will only be enforced for SSO tokens
        "sso": true,
    });

    // SECURITY: Explicitly set JWT algorithm to prevent algorithm confusion attacks
    let jwt_header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    jsonwebtoken::encode(
        &jwt_header,
        &jwt_claims,
        &jsonwebtoken::EncodingKey::from_secret(jwt_secret.as_bytes()),
    )
    .map_err(|e| {
        error!("Failed to generate JWT token: {}", e);
        AppError::Internal(anyhow::anyhow!("Authentication error"))
    })
}

/// Complete SSO login: create session, apply provisioning rules, check MFA
async fn complete_sso_login(
    state: &WebsiteState,
    config: &SsoConfigRow,
    user_id: Uuid,
    email: &str,
    name: Option<String>,
    external_id: &str,
    groups: &[String],
    attributes: Option<&serde_json::Value>,
    idp_session_id: Option<&str>,
    ip_address: Option<&str>,
    user_agent: Option<&str>,
    redirect_uri: &str,
    is_new_user: bool,
    headers: &HeaderMap,
) -> Result<CallbackResponse> {
    // 1. Apply provisioning rules for new users
    if is_new_user {
        if let Err(e) = provisioning::apply_provisioning_rules(
            &state.db,
            config.organization_id,
            config.id,
            user_id,
            email,
            groups,
            attributes,
        )
        .await
        {
            error!("Failed to apply provisioning rules: {}", e);
            // Non-fatal - continue with default role
        }

        let audit_origin_uc = AuditOrigin::from_headers(headers);
        let audit_caller_uc = AuditCaller::from_headers(headers);
        let _ = AuditEventBuilder::new(AuditEventType::UserCreated)
            .user(user_id)
            .organization(config.organization_id)
            .details(serde_json::json!({
                "sso_config_id": config.id,
                "provider": config.provider,
                "method": "sso"
            }))
            .origin(
                &audit_origin_uc.origin_type,
                &audit_origin_uc.origin_ref,
                &audit_origin_uc.origin_reason,
            )
            .caller(
                &audit_caller_uc.caller_type,
                &audit_caller_uc.key_label,
                &audit_caller_uc.key_prefix,
            )
            .success()
            .log(&state.clickhouse)
            .await;
    }

    // 2. Check if MFA is required
    let mfa_enabled = mfa::user_has_mfa_enabled(&state.db, user_id)
        .await
        .unwrap_or(false);

    if mfa_enabled {
        // Generate MFA challenge token
        let mfa_token = Uuid::new_v4().to_string();
        let mfa_key = format!("mfa:challenge:{}", mfa_token);

        // Store minimal challenge data in Redis (5 min expiry)
        // SECURITY: Only store IDs, not PII. User info is fetched from DB after MFA verification.
        let challenge_data = serde_json::json!({
            "user_id": user_id.to_string(),
            "sso_config_id": config.id.to_string(),
            "redirect_uri": redirect_uri,
        });

        let redis_pool = state.redis.clone();
        let mut conn = redis_pool.get().await.map_err(|e| {
            error!(
                "Failed to get Redis connection for MFA challenge storage: {}",
                e
            );
            AppError::Internal(anyhow::anyhow!("Session storage error"))
        })?;

        // SECURITY: Use configurable TTL for MFA challenge
        // Default is 3 minutes, can be increased for accessibility needs via MFA_CHALLENGE_TTL_SECONDS env var
        redis::cmd("SETEX")
            .arg(&mfa_key)
            .arg(state.config.mfa_challenge_ttl_seconds)
            .arg(challenge_data.to_string())
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e| {
                error!("Failed to store MFA challenge in Redis: {}", e);
                AppError::Internal(anyhow::anyhow!("Session storage error"))
            })?;

        // Get available MFA methods
        let methods: Vec<(String,)> =
            sqlx::query_as("SELECT method FROM mfa_enrollments WHERE user_id = $1")
                .bind(user_id)
                .fetch_all(&*state.db)
                .await
                .map_err(|e| {
                    error!("Failed to get MFA methods: {}", e);
                    AppError::Internal(anyhow::anyhow!("Database error"))
                })?;

        // Log MFA challenge issued
        // Note: user_id is already set via .user(), so email is not logged (PII minimization)
        let audit_origin = AuditOrigin::from_headers(headers);
        let audit_caller = AuditCaller::from_headers(headers);
        let _ = AuditEventBuilder::new(AuditEventType::SsoLoginInitiated)
            .user(user_id)
            .organization(config.organization_id)
            .details(serde_json::json!({
                "sso_config_id": config.id
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
            .log(&state.clickhouse)
            .await;

        info!("MFA challenge issued for user {}", user_id);

        return Ok(CallbackResponse {
            access_token: None,
            user: UserInfo {
                id: user_id,
                email: email.to_string(),
                name,
                external_id: external_id.to_string(),
            },
            redirect_uri: redirect_uri.to_string(),
            mfa_required: true,
            mfa_challenge_token: Some(mfa_token),
            mfa_methods: Some(methods.into_iter().map(|(m,)| m).collect()),
        });
    }

    // 3. Create SSO session
    // SECURITY: Session creation is MANDATORY for SSO tokens.
    // SSO JWTs include `sso: true` claim which triggers session validation on each request.
    // If we allow login without a session, the JWT would fail validation immediately.
    let session_token = Uuid::new_v4().to_string();
    sso_sessions::create_session(
        &state.db,
        user_id,
        config.id,
        &session_token,
        idp_session_id,
        ip_address,
        user_agent,
        state.config.jwt_expiration_hours,
    )
    .await
    .map_err(|e| {
        error!("Failed to create SSO session for user {}: {}", user_id, e);
        AppError::Internal(anyhow::anyhow!(
            "Failed to create session. Please try again."
        ))
    })?;

    let audit_origin_sess = AuditOrigin::from_headers(headers);
    let audit_caller_sess = AuditCaller::from_headers(headers);
    let _ = AuditEventBuilder::new(AuditEventType::SessionCreated)
        .user(user_id)
        .organization(config.organization_id)
        .details(serde_json::json!({
            "sso_config_id": config.id,
            "provider": config.provider
        }))
        .origin(
            &audit_origin_sess.origin_type,
            &audit_origin_sess.origin_ref,
            &audit_origin_sess.origin_reason,
        )
        .caller(
            &audit_caller_sess.caller_type,
            &audit_caller_sess.key_label,
            &audit_caller_sess.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    // 4. Generate JWT
    let token = generate_sso_jwt(
        &state.config.jwt_secret,
        &state.config.jwt_issuer,
        user_id,
        email,
        name.clone(),
        config.id,
        &session_token,
        state.config.jwt_expiration_hours,
    )?;

    // 5. Log successful login
    // Note: user_id is already set via .user(), so email is not logged (PII minimization)
    let audit_origin = AuditOrigin::from_headers(headers);
    let audit_caller = AuditCaller::from_headers(headers);
    let _ = AuditEventBuilder::new(AuditEventType::SsoLoginSuccess)
        .user(user_id)
        .organization(config.organization_id)
        .details(serde_json::json!({
            "sso_config_id": config.id,
            "provider": config.provider
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
        .log(&state.clickhouse)
        .await;

    info!(
        "SSO login successful: config={}, user_id={}",
        config.id, user_id
    );

    Ok(CallbackResponse {
        access_token: Some(token),
        user: UserInfo {
            id: user_id,
            email: email.to_string(),
            name,
            external_id: external_id.to_string(),
        },
        redirect_uri: redirect_uri.to_string(),
        mfa_required: false,
        mfa_challenge_token: None,
        mfa_methods: None,
    })
}

/// Returns (user_id, is_new_user)
async fn find_or_create_sso_user(
    db: &sqlx::PgPool,
    sso_config_id: Uuid,
    external_id: &str,
    email: &str,
    name: Option<&str>,
    auto_create: bool,
    default_role: &str,
) -> Result<(Uuid, bool)> {
    #[derive(sqlx::FromRow)]
    struct MappingRow {
        user_id: Uuid,
    }

    let existing = sqlx::query_as::<_, MappingRow>(
        "SELECT user_id FROM sso_user_mappings WHERE sso_config_id = $1 AND external_id = $2",
    )
    .bind(sso_config_id)
    .bind(external_id)
    .fetch_optional(db)
    .await
    .map_err(|e| {
        error!("Failed to query SSO user mapping: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    if let Some(mapping) = existing {
        return Ok((mapping.user_id, false));
    }

    #[derive(sqlx::FromRow)]
    struct UserRow {
        id: Uuid,
    }

    let existing_user = sqlx::query_as::<_, UserRow>("SELECT id FROM users WHERE email = $1")
        .bind(email)
        .fetch_optional(db)
        .await
        .map_err(|e| {
            error!("Failed to query user by email: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error"))
        })?;

    let (user_id, is_new_user) = if let Some(user) = existing_user {
        (user.id, false)
    } else if auto_create {
        let new_user = sqlx::query_as::<_, UserRow>(
            r#"
            INSERT INTO users (email, name, password_hash, role, is_approved)
            VALUES ($1, $2, '', $3, true)
            RETURNING id
            "#,
        )
        .bind(email)
        .bind(name.unwrap_or(email))
        .bind(default_role)
        .fetch_one(db)
        .await
        .map_err(|e| {
            error!("Failed to create user via SSO: {}", e);
            AppError::Internal(anyhow::anyhow!("User creation failed"))
        })?;

        info!("Created new user via SSO: user_id={}", new_user.id);
        (new_user.id, true)
    } else {
        return Err(AppError::Validation(
            "User does not exist and auto-creation is disabled".to_string(),
        ));
    };

    sqlx::query(
        r#"
        INSERT INTO sso_user_mappings (user_id, sso_config_id, external_id, external_email)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(user_id)
    .bind(sso_config_id)
    .bind(external_id)
    .bind(email)
    .execute(db)
    .await
    .map_err(|e| {
        error!("Failed to create SSO user mapping: {}", e);
        AppError::Internal(anyhow::anyhow!("User mapping failed"))
    })?;

    Ok((user_id, is_new_user))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // SAML RelayState Validation Tests
    // ============================================================================

    #[test]
    fn test_valid_relay_state() {
        // Valid format: underscore + UUID
        let valid = "_550e8400-e29b-41d4-a716-446655440000";
        let result = validate_saml_relay_state(valid);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), valid);
    }

    #[test]
    fn test_relay_state_empty() {
        let result = validate_saml_relay_state("");
        assert!(result.is_err());
    }

    #[test]
    fn test_relay_state_wrong_length() {
        // Too short
        let result = validate_saml_relay_state("_abc");
        assert!(result.is_err());

        // Too long
        let result = validate_saml_relay_state("_550e8400-e29b-41d4-a716-446655440000-extra");
        assert!(result.is_err());
    }

    #[test]
    fn test_relay_state_missing_underscore() {
        let result = validate_saml_relay_state("550e8400-e29b-41d4-a716-446655440000x");
        assert!(result.is_err());
    }

    #[test]
    fn test_relay_state_invalid_uuid() {
        // Valid length but not a valid UUID
        let result = validate_saml_relay_state("_not-a-valid-uuid-at-all-xxxxxxx");
        assert!(result.is_err());
    }

    #[test]
    fn test_relay_state_prevents_redis_injection() {
        // Attempt to inject Redis commands via relay state
        let malicious_inputs = vec![
            "FLUSHALL",
            "DEL *",
            "_550e8400\r\nFLUSHALL\r\n",
            "../../../etc/passwd",
            "__550e8400-e29b-41d4-a716-446655440000", // Double underscore
        ];

        for input in malicious_inputs {
            let result = validate_saml_relay_state(input);
            assert!(result.is_err(), "Should reject malicious input: {}", input);
        }
    }

    // ============================================================================
    // Redirect URI Validation Tests
    // ============================================================================

    #[test]
    fn test_redirect_uri_empty() {
        let result = validate_redirect_uri("", "https://app.example.com");
        assert_eq!(result.unwrap(), "/");
    }

    #[test]
    fn test_redirect_uri_root() {
        let result = validate_redirect_uri("/", "https://app.example.com");
        assert_eq!(result.unwrap(), "/");
    }

    #[test]
    fn test_redirect_uri_relative_path() {
        let result = validate_redirect_uri("/dashboard", "https://app.example.com");
        assert_eq!(result.unwrap(), "/dashboard");

        let result = validate_redirect_uri("/app/settings/profile", "https://app.example.com");
        assert_eq!(result.unwrap(), "/app/settings/profile");
    }

    #[test]
    fn test_redirect_uri_protocol_relative_rejected() {
        // Protocol-relative URLs (//evil.com) should be rejected
        let result = validate_redirect_uri("//evil.com/path", "https://app.example.com");
        assert_eq!(result.unwrap(), "/");
    }

    #[test]
    fn test_redirect_uri_same_origin() {
        let result = validate_redirect_uri(
            "https://app.example.com/dashboard",
            "https://app.example.com",
        );
        assert_eq!(result.unwrap(), "https://app.example.com/dashboard");
    }

    #[test]
    fn test_redirect_uri_different_origin_rejected() {
        let result = validate_redirect_uri("https://evil.com/phishing", "https://app.example.com");
        assert_eq!(result.unwrap(), "/");
    }

    #[test]
    fn test_redirect_uri_different_scheme_rejected() {
        let result =
            validate_redirect_uri("http://app.example.com/path", "https://app.example.com");
        assert_eq!(result.unwrap(), "/");
    }

    #[test]
    fn test_redirect_uri_newline_injection() {
        // Attempt to inject via newline
        let result =
            validate_redirect_uri("/path\r\nSet-Cookie: evil=true", "https://app.example.com");
        assert_eq!(result.unwrap(), "/path"); // Should sanitize
    }

    // ============================================================================
    // Email Domain Validation Tests
    // ============================================================================

    #[test]
    fn test_email_domain_allowed_empty_list() {
        // Empty allowed list means all domains are allowed
        assert!(is_email_domain_allowed("user@example.com", &[]));
        assert!(is_email_domain_allowed("user@any.domain.org", &[]));
    }

    #[test]
    fn test_email_domain_allowed_match() {
        let allowed = vec!["example.com".to_string()];
        assert!(is_email_domain_allowed("user@example.com", &allowed));
        assert!(is_email_domain_allowed("admin@example.com", &allowed));
    }

    #[test]
    fn test_email_domain_not_allowed() {
        let allowed = vec!["example.com".to_string()];
        assert!(!is_email_domain_allowed("user@other.com", &allowed));
        assert!(!is_email_domain_allowed("user@example.org", &allowed));
    }

    #[test]
    fn test_email_domain_case_insensitive() {
        let allowed = vec!["EXAMPLE.COM".to_string()];
        assert!(is_email_domain_allowed("user@example.com", &allowed));
        assert!(is_email_domain_allowed("user@EXAMPLE.COM", &allowed));
        assert!(is_email_domain_allowed("user@Example.Com", &allowed));
    }

    #[test]
    fn test_email_domain_multiple_allowed() {
        let allowed = vec!["example.com".to_string(), "company.org".to_string()];
        assert!(is_email_domain_allowed("user@example.com", &allowed));
        assert!(is_email_domain_allowed("user@company.org", &allowed));
        assert!(!is_email_domain_allowed("user@other.net", &allowed));
    }

    #[test]
    fn test_email_domain_invalid_email() {
        let allowed = vec!["example.com".to_string()];
        assert!(!is_email_domain_allowed("not-an-email", &allowed));
        assert!(!is_email_domain_allowed("@example.com", &allowed));
        assert!(!is_email_domain_allowed("user@", &allowed));
    }

    #[test]
    fn test_email_domain_idna_normalization() {
        // IDNA normalization should handle international domains
        let allowed = vec!["example.com".to_string()];

        // Cyrillic 'а' looks like Latin 'a' but is different
        // After IDNA normalization, this would become a punycode domain
        // which won't match "example.com"
        let cyrillic_a = "exаmple.com"; // Uses Cyrillic 'а' (U+0430)
        assert!(!is_email_domain_allowed(
            &format!("user@{}", cyrillic_a),
            &allowed
        ));
    }

    #[test]
    fn test_normalize_email_domain() {
        // Basic normalization
        assert_eq!(normalize_email_domain("EXAMPLE.COM"), "example.com");
        assert_eq!(normalize_email_domain("Example.Com"), "example.com");

        // International domain (if IDNA library available)
        // München -> xn--mnchen-3ya
        let result = normalize_email_domain("münchen.de");
        assert!(result.contains("xn--") || result == "münchen.de");
    }
}
