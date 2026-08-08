//! Integration tests for the trusted proxy middleware.
//!
//! Verifies that requests with X-Project-Id (or X-User-Id) are accepted when
//! the peer IP is in TRUSTED_PROXY_CIDRS and rejected with 403 when not.

mod test_support;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use axum::middleware;
use axum::routing::get;
use axum::Router;
use tower::util::ServiceExt;

use reiver_flow::config::Config;
use reiver_flow::trusted_proxy;

async fn ok_handler() -> StatusCode {
    StatusCode::OK
}

/// Build a minimal router with trusted_proxy_middleware and a single GET /ok route.
/// Uses the given config (trusted_proxy_cidrs must be set for enforcement).
fn app_with_trusted_proxy(config: Arc<Config>) -> Router {
    Router::new()
        .route("/ok", get(ok_handler))
        .layer(middleware::from_fn_with_state(
            config,
            trusted_proxy::trusted_proxy_middleware,
        ))
}

/// Build a request with the given peer IP and trusted headers.
fn request_with_peer(peer_ip: IpAddr, uri: &str) -> Request<Body> {
    let mut req = Request::builder()
        .uri(uri)
        .header("X-Project-Id", "00000000-0000-0000-0000-000000000001")
        .header("X-User-Id", "00000000-0000-0000-0000-000000000002")
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(ConnectInfo(SocketAddr::new(peer_ip, 0)));
    req
}

#[tokio::test]
async fn test_trusted_proxy_rejects_untrusted_ip_with_trusted_headers() {
    let mut config = test_support::test_config(None, None, None);
    config.trusted_proxy_cidrs = vec!["10.0.0.0/8".to_string()];
    let config = Arc::new(config);

    let app = app_with_trusted_proxy(config);
    let req = request_with_peer(IpAddr::V4(Ipv4Addr::new(192, 168, 99, 1)), "/ok");

    let response = app.oneshot(req).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "Request with trusted headers from IP outside CIDR must be rejected with 403"
    );
}

#[tokio::test]
async fn test_trusted_proxy_allows_trusted_ip_with_trusted_headers() {
    let mut config = test_support::test_config(None, None, None);
    config.trusted_proxy_cidrs = vec!["10.0.0.0/8".to_string()];
    let config = Arc::new(config);

    let app = app_with_trusted_proxy(config);
    let req = request_with_peer(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 5)), "/ok");

    let response = app.oneshot(req).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Request with trusted headers from IP inside CIDR must be allowed"
    );
}
