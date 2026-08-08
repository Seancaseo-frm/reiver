//! Trusted proxy enforcement middleware.
//!
//! Flow trusts `X-User-Id` and `X-Project-Id` headers forwarded by the
//! `website` gateway. If these headers can be spoofed by reaching Flow
//! directly (e.g. via a misconfigured port mapping), any caller can
//! impersonate any project.
//!
//! When `TRUSTED_PROXY_CIDRS` is set, this middleware rejects requests that
//! carry trusted headers from IPs outside the configured CIDR ranges.
//! If the env var is not set, the middleware logs a one-time warning at
//! startup and allows all requests (backwards-compatible default).
//!
//! # Configuration
//! Set `TRUSTED_PROXY_CIDRS` to a comma-separated list of CIDR blocks:
//! ```text
//! TRUSTED_PROXY_CIDRS=10.0.0.0/8,172.16.0.0/12,192.168.0.0/16
//! ```

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use ipnetwork::IpNetwork;

use crate::config::Config;

/// Check whether `ip` falls within the given CIDR block.
///
/// Uses the `ipnetwork` crate. Supports both IPv4 and IPv6, including
/// IPv4-mapped IPv6 addresses (e.g. `::ffff:10.0.0.1` matching `10.0.0.0/8`):
/// when the CIDR is IPv4, an IPv4-mapped IPv6 peer is normalized to IPv4 before checking.
/// Returns `false` for malformed CIDR strings.
pub fn ip_in_cidr(ip: &IpAddr, cidr: &str) -> bool {
    let network: IpNetwork = match cidr.parse() {
        Ok(n) => n,
        Err(_) => return false,
    };
    let check_ip = match (ip, &network) {
        (IpAddr::V6(v6), IpNetwork::V4(_)) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                IpAddr::V4(v4)
            } else {
                *ip
            }
        }
        _ => *ip,
    };
    network.contains(check_ip)
}

/// Axum middleware that enforces trusted proxy CIDR restrictions.
///
/// Requests carrying `X-User-Id` or `X-Project-Id` headers are only accepted
/// when the connecting IP is within one of the configured CIDR ranges.
pub async fn trusted_proxy_middleware(
    State(config): axum::extract::State<Arc<Config>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let cidrs = &config.trusted_proxy_cidrs;

    if cidrs.is_empty() {
        // No CIDRs configured — allow all (backwards-compatible mode).
        // A startup warning is logged in main.rs.
        return next.run(request).await;
    }

    let has_trusted_headers = request.headers().contains_key("x-user-id")
        || request.headers().contains_key("x-project-id");

    if !has_trusted_headers {
        return next.run(request).await;
    }

    // Extract peer IP from the ConnectInfo extension injected by axum
    let peer_ip: Option<IpAddr> = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());

    let peer_ip = match peer_ip {
        Some(ip) => ip,
        None => {
            // ConnectInfo not available (e.g. in tests) — skip enforcement
            return next.run(request).await;
        }
    };

    let is_loopback = peer_ip.is_loopback();
    let is_trusted = is_loopback || cidrs.iter().any(|cidr| ip_in_cidr(&peer_ip, cidr));

    if !is_trusted {
        tracing::warn!(
            peer_ip = %peer_ip,
            "Rejected request with trusted headers (X-User-Id/X-Project-Id) \
             from untrusted IP. Configure TRUSTED_PROXY_CIDRS to include this IP \
             if this is a legitimate proxy."
        );
        let body = serde_json::json!({
            "error": "Forbidden: request origin is not a trusted proxy"
        });
        return (StatusCode::FORBIDDEN, Json(body)).into_response();
    }

    next.run(request).await
}

// Re-export State extractor used in handler signature above
use axum::extract::State;

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_ip_in_cidr_exact() {
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 1, 5));
        assert!(ip_in_cidr(&ip, "10.0.0.0/8"));
        assert!(ip_in_cidr(&ip, "10.0.1.0/24"));
        assert!(!ip_in_cidr(&ip, "10.0.2.0/24"));
        assert!(!ip_in_cidr(&ip, "172.16.0.0/12"));
    }

    #[test]
    fn test_ip_in_cidr_host() {
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
        assert!(ip_in_cidr(&ip, "192.168.1.100/32"));
        assert!(!ip_in_cidr(&ip, "192.168.1.101/32"));
    }

    #[test]
    fn test_ip_in_cidr_any() {
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        assert!(ip_in_cidr(&ip, "0.0.0.0/0"));
    }

    #[test]
    fn test_ip_in_cidr_malformed() {
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        assert!(!ip_in_cidr(&ip, "not_a_cidr"));
        assert!(!ip_in_cidr(&ip, "10.0.0.0"));
        assert!(!ip_in_cidr(&ip, "10.0.0.0/33"));
    }

    /// IPv4-mapped IPv6 (::ffff:a.b.c.d) should match IPv4 CIDRs.
    /// Previously the hand-rolled implementation returned false, rejecting valid proxies.
    #[test]
    fn test_ip_in_cidr_ipv4_mapped_in_v4_cidr() {
        let ip: IpAddr = "::ffff:10.0.1.5".parse().expect("valid IPv4-mapped");
        assert!(ip_in_cidr(&ip, "10.0.0.0/8"));
        assert!(ip_in_cidr(&ip, "10.0.1.0/24"));
        assert!(!ip_in_cidr(&ip, "10.0.2.0/24"));
        assert!(!ip_in_cidr(&ip, "172.16.0.0/12"));
    }
}
