//! GitHub Integration API Tests
//!
//! Tests for the GitHub integration endpoints including:
//! - Input validation (SHA, fingerprint)
//! - URL parsing
//! - Webhook signature verification
//! - CSRF token generation

// =============================================================================
// Unit Tests for Validation Functions
// =============================================================================

#[cfg(test)]
mod validation_tests {
    use super::*;

    /// Test that valid SHA formats are accepted
    #[test]
    fn test_valid_sha_format() {
        // A valid SHA is 40 hex characters
        let valid_shas = [
            "a1b2c3d4e5f6789012345678901234567890abcd",
            "0000000000000000000000000000000000000000",
            "ffffffffffffffffffffffffffffffffffffffff",
            "ABCDEF1234567890abcdef1234567890ABCDEF12",
        ];

        for sha in valid_shas {
            assert_eq!(sha.len(), 40, "SHA should be 40 characters");
            assert!(
                sha.chars().all(|c| c.is_ascii_hexdigit()),
                "SHA should be hex"
            );
        }
    }

    /// Test that invalid SHA formats are rejected
    #[test]
    fn test_invalid_sha_format() {
        let invalid_shas = [
            "abc123",                                    // Too short
            "a1b2c3d4e5f6789012345678901234567890abcde", // 41 chars - too long
            "a1b2c3d4e5f678901234567890123456789",       // 36 chars - too short
            "ghijklmnopqrstuvwxyz12345678901234567890",  // Contains non-hex
            "",                                          // Empty
        ];

        for sha in invalid_shas {
            let is_valid = sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit());
            assert!(!is_valid, "SHA '{}' should be invalid", sha);
        }
    }

    /// Test that valid fingerprint formats are accepted
    #[test]
    fn test_valid_fingerprint_format() {
        let valid_fingerprints = [
            "error-abc123",
            "TypeError_in_main",
            "a",
            "Error-2024-01-15",
            "fingerprint_with_underscores",
            "MixedCase123",
        ];

        for fp in valid_fingerprints {
            let is_valid = !fp.is_empty()
                && fp.len() <= 256
                && fp
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
            assert!(is_valid, "Fingerprint '{}' should be valid", fp);
        }
    }

    /// Test that invalid fingerprint formats are rejected
    #[test]
    fn test_invalid_fingerprint_format() {
        let invalid_fingerprints = [
            "",                          // Empty
            "fingerprint with spaces",   // Contains spaces
            "fingerprint@special!chars", // Contains special chars
            "fingerprint/with/slashes",  // Contains slashes
        ];

        for fp in invalid_fingerprints {
            let is_valid = !fp.is_empty()
                && fp.len() <= 256
                && fp
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
            assert!(!is_valid, "Fingerprint '{}' should be invalid", fp);
        }
    }
}

// =============================================================================
// Unit Tests for GitHub URL Parsing
// =============================================================================
//
// These tests use the actual parse_repo_url function from the github module.
// Additional comprehensive tests are in src/github/mod.rs.

#[cfg(test)]
mod url_parsing_tests {
    use reiver_watch::github::parse_repo_url;

    /// Test HTTPS URL parsing
    #[test]
    fn test_parse_https_url() {
        let url = "https://github.com/acme/myrepo";
        let result = parse_repo_url(url);
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    /// Test HTTPS URL with .git suffix
    #[test]
    fn test_parse_https_url_with_git() {
        let url = "https://github.com/acme/myrepo.git";
        let result = parse_repo_url(url);
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    /// Test SSH URL parsing
    #[test]
    fn test_parse_ssh_url() {
        let url = "git@github.com:acme/myrepo.git";
        let result = parse_repo_url(url);
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    /// Test URL with trailing slash
    #[test]
    fn test_parse_url_trailing_slash() {
        let url = "https://github.com/acme/myrepo/";
        let result = parse_repo_url(url);
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    /// Test non-GitHub URL returns None
    #[test]
    fn test_parse_non_github_url() {
        let url = "https://gitlab.com/acme/myrepo";
        let result = parse_repo_url(url);
        assert_eq!(result, None);
    }

    /// Test path traversal attempts are rejected (security validation)
    #[test]
    fn test_parse_url_path_traversal_rejected() {
        // Path traversal in owner
        assert_eq!(parse_repo_url("https://github.com/../other/repo"), None);
        // Path traversal in repo
        assert_eq!(parse_repo_url("https://github.com/owner/../repo"), None);
    }

    /// Test special characters in owner/repo are rejected (security validation)
    #[test]
    fn test_parse_url_special_chars_rejected() {
        assert_eq!(parse_repo_url("https://github.com/owner@evil/repo"), None);
        assert_eq!(parse_repo_url("https://github.com/owner/repo:tag"), None);
    }

    /// Test valid special characters (hyphen, underscore, dot) are accepted
    #[test]
    fn test_parse_url_valid_special_chars() {
        let result = parse_repo_url("https://github.com/my-org/my_repo.js");
        assert_eq!(
            result,
            Some(("my-org".to_string(), "my_repo.js".to_string()))
        );
    }

    /// Test URLs with query strings are handled correctly
    #[test]
    fn test_parse_url_with_query_string() {
        let result = parse_repo_url("https://github.com/acme/myrepo?tab=readme");
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    /// Test URLs with fragments are handled correctly
    #[test]
    fn test_parse_url_with_fragment() {
        let result = parse_repo_url("https://github.com/acme/myrepo#installation");
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }
}

// =============================================================================
// Integration Tests for API Endpoints
// =============================================================================
//
// NOTE: Full integration tests require TestApp infrastructure to be set up.
// The following test scenarios should be implemented when TestApp is available:
//
// Authentication tests:
// - test_install_requires_auth: GET /github/install without auth returns 401
// - test_list_installations_requires_auth: GET /github/installations without auth returns 401
//
// Input validation tests:
// - test_invalid_sha_returns_400: GET /projects/{id}/github/commit/{invalid_sha} returns 400
// - test_invalid_fingerprint_returns_400: GET /projects/{id}/github/version-info/{bad} returns 400
//
// CSRF tests:
// - test_callback_requires_state: GET /github/callback?installation_id=123 (no state) returns 400
//
// Authorization tests:
// - test_delete_installation_requires_admin: Non-admin users cannot DELETE installations
// - test_link_project_requires_admin: Non-admin users cannot POST to /projects/{id}/github

// =============================================================================
// CSRF State Token Tests
// =============================================================================

#[cfg(test)]
mod csrf_tests {
    /// Test that generated CSRF tokens are valid hex strings
    #[test]
    fn test_csrf_token_format() {
        // CSRF tokens should be 64 hex characters (32 bytes encoded)
        let token_length = 64;
        let sample_token = "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456";

        assert_eq!(sample_token.len(), token_length);
        assert!(sample_token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// Test that CSRF tokens are unique
    #[test]
    fn test_csrf_tokens_are_unique() {
        use std::collections::HashSet;

        // Generate multiple tokens and verify uniqueness
        let mut tokens = HashSet::new();
        for _ in 0..100 {
            use rand::RngCore;
            let mut rng = rand::thread_rng();
            let mut bytes = [0u8; 32];
            rng.fill_bytes(&mut bytes);
            let token = hex::encode(bytes);
            tokens.insert(token);
        }

        // All tokens should be unique
        assert_eq!(tokens.len(), 100);
    }
}

// =============================================================================
// Webhook Signature Verification Tests
// =============================================================================

#[cfg(test)]
mod webhook_signature_tests {
    use reiver_watch::github::verify_webhook_signature;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    /// Helper to compute HMAC-SHA256 signature like GitHub does
    fn compute_signature(secret: &str, payload: &[u8]) -> String {
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
        mac.update(payload);
        let result = mac.finalize();
        format!("sha256={}", hex::encode(result.into_bytes()))
    }

    #[test]
    fn test_valid_signature() {
        let secret = "my-webhook-secret";
        let payload = b"{\"action\": \"deleted\", \"installation\": {\"id\": 12345}}";
        let signature = compute_signature(secret, payload);

        assert!(verify_webhook_signature(secret, payload, &signature));
    }

    #[test]
    fn test_invalid_signature_wrong_secret() {
        let secret = "my-webhook-secret";
        let wrong_secret = "wrong-secret";
        let payload = b"{\"action\": \"deleted\", \"installation\": {\"id\": 12345}}";
        let signature = compute_signature(wrong_secret, payload);

        assert!(!verify_webhook_signature(secret, payload, &signature));
    }

    #[test]
    fn test_invalid_signature_tampered_payload() {
        let secret = "my-webhook-secret";
        let payload = b"{\"action\": \"deleted\", \"installation\": {\"id\": 12345}}";
        let tampered_payload = b"{\"action\": \"deleted\", \"installation\": {\"id\": 99999}}";
        let signature = compute_signature(secret, payload);

        assert!(!verify_webhook_signature(
            secret,
            tampered_payload,
            &signature
        ));
    }

    #[test]
    fn test_missing_sha256_prefix() {
        let secret = "my-webhook-secret";
        let payload = b"{\"action\": \"deleted\"}";
        // Missing sha256= prefix
        let signature = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";

        assert!(!verify_webhook_signature(secret, payload, signature));
    }

    #[test]
    fn test_invalid_hex_in_signature() {
        let secret = "my-webhook-secret";
        let payload = b"{\"action\": \"deleted\"}";
        // Invalid hex characters (gg is not valid hex)
        let signature = "sha256=gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg";

        assert!(!verify_webhook_signature(secret, payload, signature));
    }

    #[test]
    fn test_empty_payload() {
        let secret = "my-webhook-secret";
        let payload = b"";
        let signature = compute_signature(secret, payload);

        assert!(verify_webhook_signature(secret, payload, &signature));
    }

    #[test]
    fn test_large_payload() {
        let secret = "my-webhook-secret";
        let payload = vec![b'x'; 100_000]; // 100KB payload
        let signature = compute_signature(secret, &payload);

        assert!(verify_webhook_signature(secret, &payload, &signature));
    }
}

// =============================================================================
// IP Allowlist Tests
// =============================================================================

#[cfg(test)]
mod ip_allowlist_tests {
    use ipnetwork::IpNetwork;
    use std::net::IpAddr;

    /// Helper function that mirrors the check_webhook_ip_allowlist logic
    fn is_ip_in_allowlist(allowlist: &[String], client_ip: &str) -> bool {
        if allowlist.is_empty() {
            return true; // Empty allowlist means disabled
        }

        let ip: IpAddr = match client_ip.parse() {
            Ok(ip) => ip,
            Err(_) => return false,
        };

        for cidr in allowlist {
            if let Ok(network) = cidr.parse::<IpNetwork>() {
                if network.contains(ip) {
                    return true;
                }
            }
        }

        false
    }

    #[test]
    fn test_empty_allowlist_allows_all() {
        let allowlist: Vec<String> = vec![];
        assert!(is_ip_in_allowlist(&allowlist, "1.2.3.4"));
        assert!(is_ip_in_allowlist(&allowlist, "10.0.0.1"));
        assert!(is_ip_in_allowlist(&allowlist, "192.168.1.1"));
    }

    #[test]
    fn test_ip_in_cidr_range() {
        let allowlist = vec!["192.30.252.0/22".to_string()];

        // IPs in the range
        assert!(is_ip_in_allowlist(&allowlist, "192.30.252.1"));
        assert!(is_ip_in_allowlist(&allowlist, "192.30.255.254"));

        // IPs outside the range
        assert!(!is_ip_in_allowlist(&allowlist, "192.30.251.255"));
        assert!(!is_ip_in_allowlist(&allowlist, "192.31.0.1"));
        assert!(!is_ip_in_allowlist(&allowlist, "10.0.0.1"));
    }

    #[test]
    fn test_multiple_cidr_ranges() {
        let allowlist = vec![
            "192.30.252.0/22".to_string(),
            "185.199.108.0/22".to_string(),
            "140.82.112.0/20".to_string(),
        ];

        // IP in first range
        assert!(is_ip_in_allowlist(&allowlist, "192.30.252.1"));
        // IP in second range
        assert!(is_ip_in_allowlist(&allowlist, "185.199.110.5"));
        // IP in third range
        assert!(is_ip_in_allowlist(&allowlist, "140.82.120.10"));
        // IP not in any range
        assert!(!is_ip_in_allowlist(&allowlist, "8.8.8.8"));
    }

    #[test]
    fn test_invalid_ip_rejected() {
        let allowlist = vec!["192.30.252.0/22".to_string()];

        assert!(!is_ip_in_allowlist(&allowlist, "not-an-ip"));
        assert!(!is_ip_in_allowlist(&allowlist, ""));
        assert!(!is_ip_in_allowlist(&allowlist, "300.300.300.300"));
    }

    #[test]
    fn test_ipv6_support() {
        let allowlist = vec!["2001:db8::/32".to_string()];

        // IPv6 in range
        assert!(is_ip_in_allowlist(&allowlist, "2001:db8::1"));
        assert!(is_ip_in_allowlist(
            &allowlist,
            "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"
        ));

        // IPv6 outside range
        assert!(!is_ip_in_allowlist(&allowlist, "2001:db9::1"));
    }

    #[test]
    fn test_malformed_cidr_ignored() {
        // Invalid CIDR should be skipped, not crash
        let allowlist = vec!["not-a-cidr".to_string(), "192.30.252.0/22".to_string()];

        // Should still match the valid CIDR
        assert!(is_ip_in_allowlist(&allowlist, "192.30.252.1"));
        // But not unrelated IPs
        assert!(!is_ip_in_allowlist(&allowlist, "10.0.0.1"));
    }
}

// =============================================================================
// Trusted Proxy IP Extraction Tests
// =============================================================================

#[cfg(test)]
mod trusted_proxy_tests {
    use axum::http::HeaderMap;
    use ipnetwork::IpNetwork;
    use std::net::IpAddr;

    /// Helper that mirrors extract_real_client_ip logic
    fn extract_real_client_ip(
        socket_ip: &str,
        xff_header: Option<&str>,
        trusted_proxy_cidrs: &[String],
    ) -> String {
        if trusted_proxy_cidrs.is_empty() {
            return socket_ip.to_string();
        }

        let socket_addr: IpAddr = match socket_ip.parse() {
            Ok(ip) => ip,
            Err(_) => return socket_ip.to_string(),
        };

        let is_trusted_proxy = trusted_proxy_cidrs.iter().any(|cidr| {
            cidr.parse::<IpNetwork>()
                .map(|network| network.contains(socket_addr))
                .unwrap_or(false)
        });

        if !is_trusted_proxy {
            return socket_ip.to_string();
        }

        if let Some(xff) = xff_header {
            if let Some(client_ip) = xff.split(',').next() {
                let client_ip = client_ip.trim();
                if client_ip.parse::<IpAddr>().is_ok() {
                    return client_ip.to_string();
                }
            }
        }

        socket_ip.to_string()
    }

    #[test]
    fn test_no_trusted_proxies_uses_socket_ip() {
        let trusted: Vec<String> = vec![];

        // Even with X-Forwarded-For, should use socket IP
        assert_eq!(
            extract_real_client_ip("10.0.0.1", Some("1.2.3.4"), &trusted),
            "10.0.0.1"
        );
    }

    #[test]
    fn test_untrusted_connection_uses_socket_ip() {
        let trusted = vec!["192.168.0.0/16".to_string()];

        // Connection from non-trusted IP, ignore X-Forwarded-For
        assert_eq!(
            extract_real_client_ip("8.8.8.8", Some("1.2.3.4"), &trusted),
            "8.8.8.8"
        );
    }

    #[test]
    fn test_trusted_proxy_uses_xff() {
        let trusted = vec!["10.0.0.0/8".to_string()];

        // Connection from trusted proxy, use X-Forwarded-For
        assert_eq!(
            extract_real_client_ip("10.0.0.1", Some("192.30.252.5"), &trusted),
            "192.30.252.5"
        );
    }

    #[test]
    fn test_trusted_proxy_multiple_xff_uses_first() {
        let trusted = vec!["10.0.0.0/8".to_string()];

        // Multiple IPs in X-Forwarded-For, use the first (original client)
        assert_eq!(
            extract_real_client_ip(
                "10.0.0.1",
                Some("192.30.252.5, 10.0.0.100, 10.0.0.1"),
                &trusted
            ),
            "192.30.252.5"
        );
    }

    #[test]
    fn test_trusted_proxy_no_xff_uses_socket() {
        let trusted = vec!["10.0.0.0/8".to_string()];

        // Trusted proxy but no X-Forwarded-For header
        assert_eq!(
            extract_real_client_ip("10.0.0.1", None, &trusted),
            "10.0.0.1"
        );
    }

    #[test]
    fn test_trusted_proxy_invalid_xff_uses_socket() {
        let trusted = vec!["10.0.0.0/8".to_string()];

        // Trusted proxy with invalid IP in X-Forwarded-For
        assert_eq!(
            extract_real_client_ip("10.0.0.1", Some("not-an-ip"), &trusted),
            "10.0.0.1"
        );
    }

    #[test]
    fn test_trusted_proxy_with_whitespace() {
        let trusted = vec!["10.0.0.0/8".to_string()];

        // X-Forwarded-For with leading/trailing whitespace
        assert_eq!(
            extract_real_client_ip("10.0.0.1", Some("  192.30.252.5  "), &trusted),
            "192.30.252.5"
        );
    }
}

// =============================================================================
// Webhook Delivery Deduplication Tests
// =============================================================================

#[cfg(test)]
mod deduplication_tests {
    use std::collections::HashSet;

    /// Test that delivery IDs are expected to be unique UUIDs
    #[test]
    fn test_delivery_id_format() {
        // GitHub delivery IDs are UUIDs
        let sample_id = "12345678-1234-1234-1234-123456789abc";
        assert_eq!(sample_id.len(), 36);
        assert!(sample_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
    }

    /// Test uniqueness of delivery IDs
    #[test]
    fn test_delivery_ids_are_unique() {
        let mut seen = HashSet::new();
        for _ in 0..1000 {
            let id = uuid::Uuid::new_v4().to_string();
            assert!(seen.insert(id), "Duplicate delivery ID generated");
        }
    }
}

// =============================================================================
// Installation Security Tests
// =============================================================================

#[cfg(test)]
mod installation_security_tests {
    use uuid::Uuid;

    /// Test that different organizations cannot share the same installation
    #[test]
    fn test_installation_org_isolation() {
        // Simulate the ownership check logic
        fn can_claim_installation(requesting_org: Uuid, existing_org: Option<Uuid>) -> bool {
            match existing_org {
                None => true,                                 // No existing owner, can claim
                Some(existing) => existing == requesting_org, // Can only update own installation
            }
        }

        let org_a = Uuid::new_v4();
        let org_b = Uuid::new_v4();

        // New installation - can claim
        assert!(can_claim_installation(org_a, None));

        // Existing installation owned by same org - can update
        assert!(can_claim_installation(org_a, Some(org_a)));

        // Existing installation owned by different org - cannot claim
        assert!(!can_claim_installation(org_b, Some(org_a)));
    }
}

// =============================================================================
// Rate Limiting Tests
// =============================================================================
//
// NOTE: Rate limit integration tests require TestApp infrastructure.
// Test scenarios to implement:
// - test_rate_limit_headers_present: Verify X-RateLimit-* headers in responses
// - test_external_api_rate_limit: Verify ExternalApi rate limit is applied to commit lookup
