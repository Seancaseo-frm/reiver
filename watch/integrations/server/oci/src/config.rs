//! Configuration for OCI integrations
//!
//! OCI uses API key authentication with request signing.
//! Authentication requires:
//! - Tenancy OCID
//! - User OCID  
//! - API Key fingerprint (public key fingerprint)
//! - Private Key (PEM format, RSA private key)
//! - Region

use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};
use rsa::RsaPrivateKey;
use std::collections::HashMap;
use url::Url;

use crate::signing::{sign_oci_request, parse_private_key};

/// OCI integration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciConfig {
    /// OCI tenancy OCID
    pub tenancy_ocid: String,
    /// OCI user OCID
    pub user_ocid: String,
    /// OCI API key fingerprint (public key fingerprint)
    pub fingerprint: String,
    /// OCI private key (PEM format, RSA private key)
    /// This should be the private key corresponding to the public key with the fingerprint
    pub private_key: String,
    /// OCI region (e.g., "us-ashburn-1", "us-phoenix-1")
    pub region: String,
    /// Optional: API key passphrase (if the private key is encrypted)
    pub passphrase: Option<String>,
}

impl Default for OciConfig {
    fn default() -> Self {
        Self {
            tenancy_ocid: String::new(),
            user_ocid: String::new(),
            fingerprint: String::new(),
            private_key: String::new(),
            region: String::new(),
            passphrase: None,
        }
    }
}

impl OciConfig {
    /// Parse and cache the private key
    fn get_private_key(&self) -> Result<RsaPrivateKey> {
        parse_private_key(&self.private_key)
            .context("Failed to parse OCI private key")
    }

    /// Get the key ID for signing (format: tenancy_ocid/user_ocid/fingerprint)
    fn get_key_id(&self) -> String {
        format!("{}/{}/{}", self.tenancy_ocid, self.user_ocid, self.fingerprint)
    }

    /// Sign an OCI API request
    /// 
    /// Implements proper OCI request signing based on HTTP Signatures (draft-cavage-http-signatures-08)
    /// Reference: oci-go-sdk/common/http_signer.go
    pub fn sign_request(
        &self,
        method: &str,
        url: &Url,
        headers: &mut HashMap<String, String>,
        body: Option<&[u8]>,
    ) -> Result<()> {
        let private_key = self.get_private_key()?;
        let key_id = self.get_key_id();

        // Add opc-request-id header
        if !headers.contains_key("opc-request-id") {
            headers.insert("opc-request-id".to_string(), uuid::Uuid::new_v4().to_string());
        }

        // Sign the request
        sign_oci_request(method, url, headers, body, &private_key, &key_id)?;

        Ok(())
    }
    
    /// Get the OCI API base URL for a service
    pub fn api_base_url(&self, service: &str) -> String {
        // OCI API endpoints follow the pattern: https://{service}.{region}.oci.oraclecloud.com
        format!("https://{}.{}.oci.oraclecloud.com", service, self.region)
    }
    
    /// Get the Monitoring API base URL
    pub fn monitoring_api_url(&self) -> String {
        self.api_base_url("telemetry")
    }
}
