//! SAML 2.0 authentication module
//!
//! Provides SAML AuthnRequest generation and Response verification for SSO.
//!
//! # Features
//! - AuthnRequest generation (HTTP-Redirect binding with DEFLATE compression)
//! - Response signature verification using IdP's X.509 certificate
//! - Response parsing and claim extraction
//! - SP metadata generation
//! - Attribute extraction (email, name, groups)
//! - Assertion time validation (NotBefore/NotOnOrAfter)
//!
//! # Security
//! This module uses the `samael` library which provides:
//! - Proper XML signature verification via xmlsec
//! - Protection against XML Signature Wrapping (XSW) attacks
//! - Correct XML canonicalization (C14N)
//! - XXE protection through secure XML parsing

use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
// Note: samael::service_provider is used internally for SAML response processing
use thiserror::Error;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Errors that can occur during SAML processing
#[derive(Error, Debug)]
pub enum SamlError {
    #[error("Invalid SAML response: {0}")]
    InvalidResponse(String),

    #[error("Signature verification failed: {0}")]
    SignatureVerificationFailed(String),

    #[error("Assertion validation failed: {0}")]
    AssertionValidationFailed(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Certificate error: {0}")]
    CertificateError(String),

    #[error("XML parsing error: {0}")]
    XmlError(String),
}

/// Claims extracted from a validated SAML assertion
#[derive(Debug, Clone)]
pub struct SamlClaims {
    /// Subject NameID (user identifier from IdP)
    pub name_id: String,
    /// NameID format
    pub name_id_format: Option<String>,
    /// Session index for SLO
    pub session_index: Option<String>,
    /// User email (from attributes)
    pub email: Option<String>,
    /// User's first name
    pub first_name: Option<String>,
    /// User's last name
    pub last_name: Option<String>,
    /// User's full name
    pub name: Option<String>,
    /// User's groups (from group attribute)
    pub groups: Vec<String>,
    /// All attributes from the assertion
    pub attributes: std::collections::HashMap<String, Vec<String>>,
}

/// SP (Service Provider) configuration
#[derive(Debug, Clone)]
pub struct SpConfig {
    /// SP Entity ID (unique identifier for our SP)
    pub entity_id: String,
    /// Assertion Consumer Service URL (where IdP sends responses)
    pub acs_url: String,
    /// SP certificate for signing (PEM format)
    pub certificate_pem: Option<String>,
    /// SP private key for signing (PEM format)
    pub private_key_pem: Option<String>,
    /// Whether to sign AuthnRequests
    pub sign_requests: bool,
    /// Whether to require signed assertions
    pub want_assertions_signed: bool,
    /// Clock skew tolerance in seconds for assertion time validation
    /// Default: 60 seconds
    pub time_skew_seconds: i64,
}

/// IdP (Identity Provider) configuration
#[derive(Debug, Clone)]
pub struct IdpConfig {
    /// IdP Entity ID
    pub entity_id: String,
    /// IdP SSO URL (where we send AuthnRequests)
    pub sso_url: String,
    /// IdP SLO URL (where we send LogoutRequests) - optional for SLO support
    pub slo_url: Option<String>,
    /// IdP certificate for signature verification (PEM format)
    pub certificate_pem: String,
}

/// SAML processor for handling authentication flows
pub struct SamlProcessor {
    sp_config: SpConfig,
}

impl SamlProcessor {
    /// Create a new SAML processor
    pub fn new(sp_config: SpConfig) -> Self {
        Self { sp_config }
    }

    /// Create a SAML AuthnRequest XML string
    /// Returns (request_id, base64_encoded_request)
    pub fn create_authn_request(
        &self,
        idp_config: &IdpConfig,
        _relay_state: Option<&str>,
    ) -> Result<(String, String)> {
        let request_id = format!("_{}", Uuid::new_v4());
        let issue_instant = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        // Build AuthnRequest XML
        let authn_request_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<samlp:AuthnRequest 
    xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
    ID="{}"
    Version="2.0"
    IssueInstant="{}"
    Destination="{}"
    AssertionConsumerServiceURL="{}"
    ProtocolBinding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST">
    <saml:Issuer>{}</saml:Issuer>
    <samlp:NameIDPolicy 
        Format="urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"
        AllowCreate="true"/>
</samlp:AuthnRequest>"#,
            request_id,
            issue_instant,
            idp_config.sso_url,
            self.sp_config.acs_url,
            self.sp_config.entity_id,
        );

        debug!("Created AuthnRequest with ID: {}", request_id);

        // Base64 encode for HTTP-POST binding
        let encoded = BASE64.encode(authn_request_xml.as_bytes());

        Ok((request_id, encoded))
    }

    /// Create AuthnRequest XML (not encoded)
    pub fn create_authn_request_xml(&self, idp_config: &IdpConfig) -> Result<(String, String)> {
        let request_id = format!("_{}", Uuid::new_v4());
        let issue_instant = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let authn_request_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<samlp:AuthnRequest 
    xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
    ID="{}"
    Version="2.0"
    IssueInstant="{}"
    Destination="{}"
    AssertionConsumerServiceURL="{}"
    ProtocolBinding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST">
    <saml:Issuer>{}</saml:Issuer>
    <samlp:NameIDPolicy 
        Format="urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"
        AllowCreate="true"/>
</samlp:AuthnRequest>"#,
            request_id,
            issue_instant,
            idp_config.sso_url,
            self.sp_config.acs_url,
            self.sp_config.entity_id,
        );

        Ok((request_id, authn_request_xml))
    }

    /// Build SSO URL for HTTP-Redirect binding
    pub fn build_sso_url(
        &self,
        idp_config: &IdpConfig,
        relay_state: Option<&str>,
    ) -> Result<String> {
        let (request_id, xml) = self.create_authn_request_xml(idp_config)?;
        self.build_sso_url_with_request(idp_config, &xml, &request_id, relay_state)
    }

    /// Build SSO URL for HTTP-Redirect binding using a pre-generated AuthnRequest XML
    ///
    /// This method should be used when you need to store the request ID for InResponseTo
    /// validation before building the URL.
    pub fn build_sso_url_with_request(
        &self,
        idp_config: &IdpConfig,
        authn_request_xml: &str,
        request_id: &str,
        relay_state: Option<&str>,
    ) -> Result<String> {
        // For HTTP-Redirect, we deflate then base64
        let encoded = deflate_and_encode(authn_request_xml)?;
        let encoded_url = urlencoding::encode(&encoded);

        let mut url = format!("{}?SAMLRequest={}", idp_config.sso_url, encoded_url);

        if let Some(state) = relay_state {
            url.push_str(&format!("&RelayState={}", urlencoding::encode(state)));
        }

        debug!("Built SSO URL with request ID: {}", request_id);

        Ok(url)
    }

    /// Create a SAML LogoutRequest for Single Logout (SLO)
    ///
    /// # Arguments
    /// * `idp_config` - IdP configuration (must have slo_url set)
    /// * `name_id` - The NameID value from the original assertion
    /// * `name_id_format` - The NameID format (optional)
    /// * `session_index` - The SessionIndex from the original assertion (optional)
    ///
    /// # Returns
    /// (request_id, logout_url) - The request ID and the full SLO URL to redirect to
    pub fn create_logout_request(
        &self,
        idp_config: &IdpConfig,
        name_id: &str,
        name_id_format: Option<&str>,
        session_index: Option<&str>,
    ) -> Result<(String, String)> {
        let slo_url = idp_config
            .slo_url
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("IdP does not have SLO URL configured"))?;

        let request_id = format!("_{}", Uuid::new_v4());
        let issue_instant = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        // Build SessionIndex element if provided
        let session_index_xml = session_index
            .map(|idx| {
                format!(
                    r#"
    <samlp:SessionIndex>{}</samlp:SessionIndex>"#,
                    idx
                )
            })
            .unwrap_or_default();

        // Use email format if not specified
        let format =
            name_id_format.unwrap_or("urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress");

        // Build LogoutRequest XML
        let logout_request_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<samlp:LogoutRequest 
    xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
    ID="{}"
    Version="2.0"
    IssueInstant="{}"
    Destination="{}">
    <saml:Issuer>{}</saml:Issuer>
    <saml:NameID Format="{}">{}</saml:NameID>{}
</samlp:LogoutRequest>"#,
            request_id,
            issue_instant,
            slo_url,
            self.sp_config.entity_id,
            format,
            name_id,
            session_index_xml,
        );

        debug!("Created LogoutRequest with ID: {}", request_id);

        // For HTTP-Redirect, we deflate then base64 encode
        let encoded = deflate_and_encode(&logout_request_xml)?;
        let encoded_url = urlencoding::encode(&encoded);

        let url = format!("{}?SAMLRequest={}", slo_url, encoded_url);

        Ok((request_id, url))
    }

    /// Verify and parse a SAML response
    ///
    /// This performs full signature verification using the IdP's X.509 certificate
    /// via the samael library, which uses xmlsec for proper XML signature verification.
    ///
    /// # Security Validations
    /// - Signature verification using IdP certificate
    /// - Response status check
    /// - Destination validation (ACS URL)
    /// - InResponseTo validation (replay protection)
    /// - Issuer validation (IdP entity ID)
    /// - Assertion time validation (NotBefore/NotOnOrAfter)
    /// - Audience restriction validation
    pub fn verify_response(
        &self,
        saml_response: &str,
        idp_config: &IdpConfig,
        expected_request_id: Option<&str>,
    ) -> Result<SamlClaims, SamlError> {
        // Decode base64
        let response_bytes = BASE64
            .decode(saml_response)
            .map_err(|e| SamlError::InvalidResponse(format!("Invalid base64: {}", e)))?;

        let response_str = String::from_utf8(response_bytes)
            .map_err(|e| SamlError::InvalidResponse(format!("Invalid UTF-8: {}", e)))?;

        debug!("Parsing SAML response with samael");

        // Parse the SAML response using samael
        let response: samael::schema::Response = response_str
            .parse()
            .map_err(|e| SamlError::XmlError(format!("Failed to parse SAML response: {}", e)))?;

        // SECURITY: Signature verification is mandatory
        // An empty certificate should never reach here due to API validation,
        // but we enforce it as defense-in-depth
        if idp_config.certificate_pem.trim().is_empty() {
            error!("SAML signature verification failed: IdP certificate is empty");
            return Err(SamlError::CertificateError(
                "IdP certificate is required for SAML signature verification".to_string(),
            ));
        }
        self.verify_response_signature(&response_str, &idp_config.certificate_pem)?;
        info!("SAML response signature verified successfully");

        // Validate Destination attribute (must match our ACS URL)
        if let Some(destination) = &response.destination {
            if destination != &self.sp_config.acs_url {
                warn!(
                    "SAML response Destination mismatch: expected {}, got {}",
                    self.sp_config.acs_url, destination
                );
                return Err(SamlError::AssertionValidationFailed(
                    "SAML response Destination does not match ACS URL".to_string(),
                ));
            }
            debug!("SAML Destination validated: {}", destination);
        }

        // Validate InResponseTo (replay protection for SP-initiated SSO)
        if let Some(expected_id) = expected_request_id {
            if let Some(in_response_to) = &response.in_response_to {
                if in_response_to != expected_id {
                    warn!(
                        "SAML InResponseTo mismatch: expected {}, got {}",
                        expected_id, in_response_to
                    );
                    return Err(SamlError::AssertionValidationFailed(
                        "SAML response InResponseTo does not match request ID".to_string(),
                    ));
                }
                debug!("SAML InResponseTo validated: {}", in_response_to);
            } else {
                // InResponseTo is absent - this would indicate IdP-initiated SSO
                // which is disabled for security (prevents replay attacks).
                // The API layer (sso.rs) enforces this by requiring a valid session
                // from the SP-initiated flow before calling verify_response.
                warn!("SAML response missing InResponseTo - IdP-initiated SSO not allowed");
                return Err(SamlError::AssertionValidationFailed(
                    "SAML response missing InResponseTo - IdP-initiated SSO is disabled"
                        .to_string(),
                ));
            }
        }

        // SECURITY: Validate Response-level Issuer (must be present and match expected IdP)
        let response_issuer = response.issuer.as_ref().ok_or_else(|| {
            warn!("SAML response missing Issuer element");
            SamlError::MissingField("SAML response must contain an Issuer element".to_string())
        })?;
        let response_issuer_value = response_issuer.value.as_ref().ok_or_else(|| {
            warn!("SAML response Issuer has no value");
            SamlError::MissingField("SAML response Issuer must have a value".to_string())
        })?;
        if response_issuer_value != &idp_config.entity_id {
            warn!(
                "SAML response Issuer mismatch: expected {}, got {}",
                idp_config.entity_id, response_issuer_value
            );
            return Err(SamlError::AssertionValidationFailed(
                "SAML response Issuer does not match expected IdP".to_string(),
            ));
        }
        debug!("SAML Response Issuer validated: {}", response_issuer_value);

        // SECURITY: Validate Assertion-level Issuer (prevents XSW attacks with assertions from different IdPs)
        let assertion = response
            .assertion
            .as_ref()
            .ok_or_else(|| SamlError::MissingField("No assertion found in response".to_string()))?;

        let assertion_issuer = assertion.issuer.value.as_ref().ok_or_else(|| {
            warn!("SAML assertion missing Issuer value");
            SamlError::MissingField("SAML assertion must contain an Issuer".to_string())
        })?;
        if assertion_issuer != &idp_config.entity_id {
            warn!(
                "SAML assertion Issuer mismatch: expected {}, got {}",
                idp_config.entity_id, assertion_issuer
            );
            return Err(SamlError::AssertionValidationFailed(
                "SAML assertion Issuer does not match expected IdP".to_string(),
            ));
        }
        debug!("SAML Assertion Issuer validated: {}", assertion_issuer);

        // Validate response status
        if let Some(status) = &response.status {
            if let Some(code) = &status.status_code.value {
                if code != "urn:oasis:names:tc:SAML:2.0:status:Success" {
                    return Err(SamlError::AssertionValidationFailed(format!(
                        "SAML response status: {}",
                        code
                    )));
                }
            }
        }

        // Validate assertion conditions (time and audience)
        self.validate_assertion_conditions(&response)?;

        // Extract claims from assertion
        let claims = self.extract_claims_from_response(&response)?;

        Ok(claims)
    }

    /// Validate assertion conditions including time and audience restrictions
    fn validate_assertion_conditions(
        &self,
        response: &samael::schema::Response,
    ) -> Result<(), SamlError> {
        let assertion = response
            .assertion
            .as_ref()
            .ok_or_else(|| SamlError::MissingField("No assertion found in response".to_string()))?;

        if let Some(conditions) = &assertion.conditions {
            let now = chrono::Utc::now();

            // Clock skew tolerance to account for clock synchronization issues between SP and IdP
            // Configurable via SAML_TIME_SKEW_SECONDS environment variable
            let skew_tolerance = chrono::Duration::seconds(self.sp_config.time_skew_seconds);

            // Validate NotBefore (with clock skew tolerance)
            if let Some(not_before) = &conditions.not_before {
                if now < (*not_before - skew_tolerance) {
                    warn!(
                        "SAML assertion not yet valid: NotBefore={}, now={}",
                        not_before, now
                    );
                    return Err(SamlError::AssertionValidationFailed(
                        "SAML assertion is not yet valid (NotBefore)".to_string(),
                    ));
                }
            }

            // Validate NotOnOrAfter (with clock skew tolerance)
            if let Some(not_on_or_after) = &conditions.not_on_or_after {
                if now >= (*not_on_or_after + skew_tolerance) {
                    warn!(
                        "SAML assertion expired: NotOnOrAfter={}, now={}",
                        not_on_or_after, now
                    );
                    return Err(SamlError::AssertionValidationFailed(
                        "SAML assertion has expired (NotOnOrAfter)".to_string(),
                    ));
                }
            }

            // Validate Audience Restriction
            if let Some(audience_restrictions) = &conditions.audience_restrictions {
                let mut audience_valid = false;
                for audience_restriction in audience_restrictions {
                    for audience in &audience_restriction.audience {
                        if audience == &self.sp_config.entity_id {
                            audience_valid = true;
                            break;
                        }
                    }
                    if audience_valid {
                        break;
                    }
                }

                if !audience_restrictions.is_empty() && !audience_valid {
                    warn!(
                        "SAML audience restriction failed: expected {}",
                        self.sp_config.entity_id
                    );
                    return Err(SamlError::AssertionValidationFailed(
                        "SAML assertion audience does not match SP entity ID".to_string(),
                    ));
                }
            }

            debug!("SAML assertion conditions validated successfully");
        }

        Ok(())
    }

    /// Verify the XML signature using samael/xmlsec
    fn verify_response_signature(
        &self,
        response_xml: &str,
        certificate_pem: &str,
    ) -> Result<(), SamlError> {
        // Parse the certificate
        let cert = openssl::x509::X509::from_pem(certificate_pem.as_bytes())
            .map_err(|e| SamlError::CertificateError(format!("Invalid certificate PEM: {}", e)))?;

        // Get the public key from the certificate
        let public_key = cert.public_key().map_err(|e| {
            SamlError::CertificateError(format!("Failed to extract public key: {}", e))
        })?;

        // Use samael's signature verification
        // The verify_signed_xml function returns () on success, or an error on failure
        samael::crypto::verify_signed_xml(
            response_xml.as_bytes(),
            public_key
                .public_key_to_der()
                .map_err(|e| {
                    SamlError::CertificateError(format!("Failed to encode public key: {}", e))
                })?
                .as_slice(),
            Some("urn:oasis:names:tc:SAML:2.0:protocol:Response"),
        )
        .map_err(|e| {
            SamlError::SignatureVerificationFailed(format!("Signature verification failed: {}", e))
        })?;

        debug!("SAML signature verified successfully using xmlsec");
        Ok(())
    }

    /// Extract claims from a parsed SAML response
    fn extract_claims_from_response(
        &self,
        response: &samael::schema::Response,
    ) -> Result<SamlClaims, SamlError> {
        // Get the assertion
        let assertion = response
            .assertion
            .as_ref()
            .ok_or_else(|| SamlError::MissingField("No assertion found in response".to_string()))?;

        // Extract NameID
        let name_id = assertion
            .subject
            .as_ref()
            .and_then(|s| s.name_id.as_ref())
            .map(|n| n.value.clone())
            .ok_or_else(|| SamlError::MissingField("NameID".to_string()))?;

        let name_id_format = assertion
            .subject
            .as_ref()
            .and_then(|s| s.name_id.as_ref())
            .and_then(|n| n.format.clone());

        // Extract session index from AuthnStatement
        let session_index = assertion
            .authn_statements
            .as_ref()
            .and_then(|stmts| stmts.first())
            .and_then(|stmt| stmt.session_index.clone());

        // Extract attributes
        let mut attributes: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        if let Some(attr_statements) = &assertion.attribute_statements {
            for attr_statement in attr_statements {
                for attr in &attr_statement.attributes {
                    let values: Vec<String> =
                        attr.values.iter().filter_map(|v| v.value.clone()).collect();
                    if let Some(name) = &attr.name {
                        attributes.insert(name.clone(), values);
                    }
                }
            }
        }

        // Extract common claims from attributes
        let email = get_attribute_value(
            &attributes,
            &[
                "email",
                "mail",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
                "http://schemas.xmlsoap.org/claims/EmailAddress",
            ],
        );

        let first_name = get_attribute_value(
            &attributes,
            &[
                "firstName",
                "givenName",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname",
                "http://schemas.xmlsoap.org/claims/GivenName",
            ],
        );

        let last_name = get_attribute_value(
            &attributes,
            &[
                "lastName",
                "surname",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname",
                "http://schemas.xmlsoap.org/claims/Surname",
            ],
        );

        let name = get_attribute_value(
            &attributes,
            &[
                "displayName",
                "name",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
            ],
        );

        // Extract groups
        let groups = get_groups_from_attributes(&attributes);

        debug!("Extracted claims from SAML response");

        Ok(SamlClaims {
            name_id,
            name_id_format,
            session_index,
            email,
            first_name,
            last_name,
            name,
            groups,
            attributes,
        })
    }

    /// Generate SP metadata XML
    ///
    /// Note: This generates basic SP metadata. For production use with SP signing,
    /// the metadata endpoint in sso.rs provides more complete metadata including
    /// KeyDescriptor elements.
    pub fn generate_metadata(&self) -> Result<String> {
        // Generate metadata XML directly since samael's EntityDescriptor
        // serialization is internal to the library
        let metadata_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="{}">
  <md:SPSSODescriptor AuthnRequestsSigned="{}" WantAssertionsSigned="{}" protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</md:NameIDFormat>
    <md:AssertionConsumerService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="{}" index="0" isDefault="true"/>
  </md:SPSSODescriptor>
</md:EntityDescriptor>"#,
            self.sp_config.entity_id,
            self.sp_config.sign_requests,
            self.sp_config.want_assertions_signed,
            self.sp_config.acs_url,
        );

        Ok(metadata_xml)
    }
}

/// Deflate and base64 encode for HTTP-Redirect binding
fn deflate_and_encode(data: &str) -> Result<String> {
    use flate2::write::DeflateEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data.as_bytes())?;
    let compressed = encoder.finish()?;

    Ok(BASE64.encode(compressed))
}

/// Get the first matching attribute value from a list of possible attribute names
fn get_attribute_value(
    attributes: &std::collections::HashMap<String, Vec<String>>,
    names: &[&str],
) -> Option<String> {
    for name in names {
        if let Some(values) = attributes.get(*name) {
            if let Some(value) = values.first() {
                if !value.is_empty() {
                    return Some(value.clone());
                }
            }
        }
    }
    None
}

/// Extract groups from SAML attributes
fn get_groups_from_attributes(
    attributes: &std::collections::HashMap<String, Vec<String>>,
) -> Vec<String> {
    let group_attrs = [
        "groups",
        "memberOf",
        "group",
        "http://schemas.microsoft.com/ws/2008/06/identity/claims/groups",
        "http://schemas.xmlsoap.org/claims/Group",
    ];

    let mut groups = Vec::new();

    for attr in &group_attrs {
        if let Some(values) = attributes.get(*attr) {
            for value in values {
                // Groups might be comma-separated or individual values
                for group in value.split(',') {
                    let g = group.trim();
                    if !g.is_empty() && !groups.contains(&g.to_string()) {
                        groups.push(g.to_string());
                    }
                }
            }
        }
    }

    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sp_config() -> SpConfig {
        SpConfig {
            entity_id: "https://reiver.example.com/saml/metadata".to_string(),
            acs_url: "https://reiver.example.com/api/sso/callback/saml".to_string(),
            certificate_pem: None,
            private_key_pem: None,
            sign_requests: false,
            want_assertions_signed: true,
            time_skew_seconds: 60,
        }
    }

    fn test_idp_config() -> IdpConfig {
        IdpConfig {
            entity_id: "https://idp.example.com".to_string(),
            sso_url: "https://idp.example.com/sso".to_string(),
            slo_url: Some("https://idp.example.com/slo".to_string()),
            certificate_pem: String::new(),
        }
    }

    #[test]
    fn test_create_authn_request() {
        let processor = SamlProcessor::new(test_sp_config());
        let idp = test_idp_config();

        let result = processor.create_authn_request(&idp, None);
        assert!(result.is_ok());

        let (request_id, encoded) = result.unwrap();
        assert!(request_id.starts_with("_"));
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_build_sso_url() {
        let processor = SamlProcessor::new(test_sp_config());
        let idp = test_idp_config();

        let result = processor.build_sso_url(&idp, Some("test_relay"));
        assert!(result.is_ok());

        let url = result.unwrap();
        assert!(url.starts_with("https://idp.example.com/sso?SAMLRequest="));
        assert!(url.contains("RelayState=test_relay"));
    }

    #[test]
    fn test_get_attribute_value() {
        let mut attrs = std::collections::HashMap::new();
        attrs.insert("email".to_string(), vec!["user@example.com".to_string()]);

        let result = get_attribute_value(&attrs, &["mail", "email"]);
        assert_eq!(result, Some("user@example.com".to_string()));

        let result = get_attribute_value(&attrs, &["nonexistent"]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_groups_from_attributes() {
        let mut attrs = std::collections::HashMap::new();
        attrs.insert("groups".to_string(), vec!["admin,users".to_string()]);

        let groups = get_groups_from_attributes(&attrs);
        assert!(groups.contains(&"admin".to_string()));
        assert!(groups.contains(&"users".to_string()));
    }

    // ============================================================================
    // Security Edge Case Tests
    // ============================================================================

    #[test]
    fn test_request_id_format() {
        // Request IDs should start with underscore followed by UUID
        let processor = SamlProcessor::new(test_sp_config());
        let idp = test_idp_config();

        let (request_id, _) = processor.create_authn_request(&idp, None).unwrap();

        assert!(
            request_id.starts_with("_"),
            "Request ID must start with underscore"
        );
        assert_eq!(
            request_id.len(),
            37,
            "Request ID must be 37 chars: _ + UUID"
        );

        // Verify the UUID part is valid
        let uuid_part = &request_id[1..];
        assert!(
            uuid::Uuid::parse_str(uuid_part).is_ok(),
            "UUID part must be valid"
        );
    }

    #[test]
    fn test_empty_attribute_values_ignored() {
        let mut attrs = std::collections::HashMap::new();
        attrs.insert("email".to_string(), vec!["".to_string()]);
        attrs.insert("name".to_string(), vec!["John Doe".to_string()]);

        // Empty email should be ignored
        let email = get_attribute_value(&attrs, &["email"]);
        assert_eq!(email, None, "Empty attribute values should be ignored");

        // Non-empty should work
        let name = get_attribute_value(&attrs, &["name"]);
        assert_eq!(name, Some("John Doe".to_string()));
    }

    #[test]
    fn test_groups_deduplication() {
        let mut attrs = std::collections::HashMap::new();
        // Same group appears in multiple places
        attrs.insert("groups".to_string(), vec!["admin,admin".to_string()]);
        attrs.insert("memberOf".to_string(), vec!["admin".to_string()]);

        let groups = get_groups_from_attributes(&attrs);

        // Should only have one "admin" entry
        let admin_count = groups.iter().filter(|g| *g == "admin").count();
        assert_eq!(admin_count, 1, "Groups should be deduplicated");
    }

    #[test]
    fn test_groups_whitespace_trimming() {
        let mut attrs = std::collections::HashMap::new();
        attrs.insert(
            "groups".to_string(),
            vec!["  admin  ,  users  ".to_string()],
        );

        let groups = get_groups_from_attributes(&attrs);

        assert!(
            groups.contains(&"admin".to_string()),
            "Whitespace should be trimmed"
        );
        assert!(
            groups.contains(&"users".to_string()),
            "Whitespace should be trimmed"
        );
    }

    #[test]
    fn test_empty_groups_ignored() {
        let mut attrs = std::collections::HashMap::new();
        attrs.insert("groups".to_string(), vec!["admin,,users,".to_string()]);

        let groups = get_groups_from_attributes(&attrs);

        // Should only have admin and users, not empty strings
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"admin".to_string()));
        assert!(groups.contains(&"users".to_string()));
    }

    #[test]
    fn test_logout_request_requires_slo_url() {
        let processor = SamlProcessor::new(test_sp_config());
        let mut idp = test_idp_config();
        idp.slo_url = None; // No SLO URL configured

        let result = processor.create_logout_request(&idp, "user@example.com", None, None);

        assert!(
            result.is_err(),
            "Logout request should fail without SLO URL"
        );
    }

    #[test]
    fn test_logout_request_format() {
        let processor = SamlProcessor::new(test_sp_config());
        let idp = test_idp_config();

        let result = processor.create_logout_request(
            &idp,
            "user@example.com",
            Some("urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"),
            Some("session123"),
        );

        assert!(result.is_ok());
        let (request_id, url) = result.unwrap();

        assert!(request_id.starts_with("_"));
        assert!(url.starts_with("https://idp.example.com/slo?SAMLRequest="));
    }
}
