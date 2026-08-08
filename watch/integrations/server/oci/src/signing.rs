//! OCI request signing implementation
//!
//! Implements OCI request signing based on HTTP Signatures (draft-cavage-http-signatures-08)
//! Reference: oci-go-sdk/common/http_signer.go

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use rsa::{RsaPrivateKey, pkcs1v15::{SigningKey, Signature}};
use rsa::signature::{Signer, SignatureEncoding};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::Utc;
use std::collections::HashMap;
use url::Url;

/// Sign an OCI HTTP request
///
/// Implements the OCI request signing algorithm:
/// 1. Build signing string from headers
/// 2. Hash signing string with SHA256
/// 3. Sign hash with RSA (PKCS1v15)
/// 4. Base64 encode signature
/// 5. Build Authorization header
pub fn sign_oci_request(
    method: &str,
    url: &Url,
    headers: &mut HashMap<String, String>,
    body: Option<&[u8]>,
    private_key: &RsaPrivateKey,
    key_id: &str,
) -> Result<()> {
    // Set Date header if not present
    if !headers.contains_key("date") {
        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        headers.insert("date".to_string(), date);
    }

    // Determine if we should hash the body (POST, PUT, PATCH)
    let should_hash_body = matches!(method.to_uppercase().as_str(), "POST" | "PUT" | "PATCH");

    // Generic headers (always included)
    let generic_headers = vec!["date", "(request-target)", "host"];

    // Body headers (only for POST/PUT/PATCH)
    let body_headers = if should_hash_body {
        vec!["content-length", "content-type", "x-content-sha256"]
    } else {
        vec![]
    };

    // Build signing headers list
    let mut signing_headers = generic_headers.clone();
    signing_headers.extend(body_headers);

    // Handle body hashing if needed
    if should_hash_body {
        let body_bytes = body.unwrap_or(&[]);
        let hash = Sha256::digest(body_bytes);
        let hash_base64 = BASE64.encode(&hash);
        
        // Set x-content-sha256 header
        headers.insert("x-content-sha256".to_string(), hash_base64);
        
        // Set content-length header
        if !headers.contains_key("content-length") {
            headers.insert("content-length".to_string(), body_bytes.len().to_string());
        }
        
        // Set content-type header if not present and we have a body
        if !headers.contains_key("content-type") && !body_bytes.is_empty() {
            headers.insert("content-type".to_string(), "application/json".to_string());
        }
    }

    // Build signing string
    let signing_string = build_signing_string(
        method,
        url,
        headers,
        &signing_headers,
    )?;

    // Sign with RSA-PKCS1v15
    // Note: SigningKey::<Sha256> automatically hashes with SHA256 before signing,
    // so we pass the signing string directly
    let signing_key = SigningKey::<Sha256>::new(private_key.clone());
    let signature: Signature = signing_key.sign(signing_string.as_bytes());
    
    // Base64 encode signature
    let signature_base64 = BASE64.encode(signature.to_bytes());

    // Build Authorization header
    let headers_list = signing_headers.join(" ");
    let auth_header = format!(
        r#"Signature version="1",headers="{}",keyId="{}",algorithm="rsa-sha256",signature="{}""#,
        headers_list, key_id, signature_base64
    );

    headers.insert("authorization".to_string(), auth_header);

    Ok(())
}

/// Build the signing string from headers
fn build_signing_string(
    method: &str,
    url: &Url,
    headers: &HashMap<String, String>,
    signing_headers: &[&str],
) -> Result<String> {
    let mut parts = Vec::new();

    for header_name in signing_headers {
        let header_name_lower = header_name.to_lowercase();
        let value = match header_name_lower.as_str() {
            "(request-target)" => {
                // Format: "{method lowercase} {path}{query}"
                let method_lower = method.to_lowercase();
                let path = url.path();
                let query_part = url.query().map(|q| format!("?{}", q)).unwrap_or_default();
                format!("{} {}{}", method_lower, path, query_part)
            }
            "host" => {
                url.host_str()
                    .map(|h| {
                        if let Some(port) = url.port() {
                            format!("{}:{}", h, port)
                        } else {
                            h.to_string()
                        }
                    })
                    .unwrap_or_else(|| {
                        headers.get("host")
                            .cloned()
                            .unwrap_or_else(|| url.to_string())
                    })
            }
            "x-content-sha256" => {
                headers.get("x-content-sha256")
                    .cloned()
                    .unwrap_or_default()
            }
            _ => {
                headers.get(&header_name_lower)
                    .cloned()
                    .unwrap_or_default()
            }
        };

        parts.push(format!("{}: {}", header_name_lower, value));
    }

    Ok(parts.join("\n"))
}

/// Parse RSA private key from PEM format
pub fn parse_private_key(pem: &str) -> Result<RsaPrivateKey> {
    use rsa::pkcs1::DecodeRsaPrivateKey;
    
    // Remove PEM headers/footers and whitespace
    let pem_lines: Vec<&str> = pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    
    let pem_content = pem_lines.join("");
    
    // Decode base64
    let key_bytes = BASE64.decode(pem_content.as_bytes())
        .context("Failed to decode private key from base64")?;

    // Parse as DER/PKCS#1
    let private_key = RsaPrivateKey::from_pkcs1_der(&key_bytes)
        .context("Failed to parse private key from PKCS#1 DER")?;

    Ok(private_key)
}
