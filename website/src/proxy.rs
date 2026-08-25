//! Reverse proxy for backend services (Pond, Flow).
//!
//! The website acts as an API gateway: it authenticates every request,
//! verifies project access, and then forwards the request to the
//! appropriate backend service with a trusted `X-User-Id` header.
//! Backend services trust this header and skip their own auth.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use bb8_redis::redis::AsyncCommands;
use chrono::Utc;
use futures::TryStreamExt;
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::{RedisPool, WebsiteState};
use crate::auth::{authenticate_request, verify_project_access};
use crate::db::DbPool;
use crate::error::AppError;
use crate::rate_limit::RateLimitType;
use reiver_core::entitlements::types::Product;

/// Maximum request body size for proxied requests (50 MB).
const MAX_PROXY_BODY_SIZE: usize = 50 * 1024 * 1024;

/// Headers that must not be forwarded between hops (RFC 2616 hop-by-hop headers
/// plus auth/host which we replace).
const SKIP_HEADERS: &[&str] = &[
    "host",
    "authorization",
    "connection",
    "transfer-encoding",
    "upgrade",
    "te",
    "trailer",
    "keep-alive",
    "proxy-authorization",
    "proxy-connection",
    "x-project-id",
    "x-user-id",
    "x-user-jwt",
    "x-key-scopes",
    "x-organization-id",
    "x-billing-project-id",
    "x-creator-type",
    "x-creator-key-label",
    "x-creator-key-prefix",
    "x-audit-origin-type",
    "x-audit-origin-ref",
    "x-audit-origin-reason",
];

/// Extract the raw JWT from the Authorization header or token cookie.
fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    if let Some(token) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        return Some(token.to_string());
    }
    headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .and_then(|cookies| {
            cookies
                .split("; ")
                .find(|c| c.starts_with("token="))
                .and_then(|c| c.strip_prefix("token="))
                .map(|t| {
                    urlencoding::decode(t)
                        .map(|d| d.to_string())
                        .unwrap_or_else(|_| t.to_string())
                })
        })
}

/// Extract project_id from a URL path like `/api/projects/{project_id}/warehouse/...`.
///
/// Returns `None` if the path doesn't match the expected pattern.
fn extract_project_id_from_path(path: &str) -> Option<Uuid> {
    let segments: Vec<&str> = path.split('/').collect();
    for (i, seg) in segments.iter().enumerate() {
        if *seg == "projects" {
            if let Some(id_str) = segments.get(i + 1) {
                return Uuid::parse_str(id_str).ok();
            }
        }
    }
    None
}

/// Extract project_id from the 4th path segment in LLM routes.
///
/// Handles patterns like `/api/llm/metrics/{project_id}/summary` where the
/// project UUID appears right after the resource type.
///
/// Excludes `/api/llm/sessions/...` since session routes use project-prefixed
/// URLs (`/api/projects/{id}/llm/sessions/...`) and segment 4 there is a
/// session_id, not a project_id.
fn extract_project_id_from_llm_path(path: &str) -> Option<Uuid> {
    let segments: Vec<&str> = path.split('/').collect();
    // Exclude session paths -- segment 3 == "sessions"
    if segments.get(3) == Some(&"sessions") {
        return None;
    }
    // ["", "api", "llm", "metrics", "{uuid}", ...]
    segments.get(4).and_then(|s| Uuid::parse_str(s).ok())
}

/// Extract `project_id` from a URL query string (`?project_id={uuid}`).
fn extract_project_id_from_query(query: Option<&str>) -> Option<Uuid> {
    query.and_then(|q| {
        q.split('&').find_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let val = parts.next()?;
            if key == "project_id" {
                Uuid::parse_str(val).ok()
            } else {
                None
            }
        })
    })
}

/// Remove the `/projects/{uuid}` segment from a URL path.
///
/// Example: `/api/projects/abc-123/llm/sessions` -> `/api/llm/sessions`
///
/// Used for Flow proxying where the backend routes don't include the
/// `projects/{project_id}` prefix (project_id is sent via header instead).
fn strip_project_segment(path: &str) -> String {
    let segments: Vec<&str> = path.split('/').collect();
    let mut result = Vec::with_capacity(segments.len());
    let mut i = 0;
    while i < segments.len() {
        if segments[i] == "projects" && i + 1 < segments.len() {
            // Skip "projects" and the UUID that follows it
            i += 2;
        } else {
            result.push(segments[i]);
            i += 1;
        }
    }
    let joined = result.join("/");
    if joined.is_empty() {
        "/".to_string()
    } else {
        joined
    }
}

/// Determine the rate limit type from the downstream path.
fn rate_limit_for_path(path: &str) -> RateLimitType {
    // NL query is the most expensive endpoint (up to 3 LLM + 3 ClickHouse calls
    // per request), so it gets its own restrictive rate limit. Check before
    // the generic "/query" match.
    if path.contains("/natural-language") {
        RateLimitType::NlQuery
    } else if path.contains("/query")
        || path.contains("/stream")
        || path.contains("/usage")
        || path.contains("/freshness")
        || path.contains("/autocomplete")
        || path.contains("/search")
        || path.contains("/lineage")
    {
        RateLimitType::Analytics
    } else {
        RateLimitType::Crud
    }
}

/// Build a JSON error response body.
fn proxy_error(status: StatusCode, message: &str) -> Response {
    let body = serde_json::json!({ "error": message });
    let mut resp = Json(body).into_response();
    *resp.status_mut() = status;
    resp
}

/// Entitlement gate: block requests when the org's tier does not include the product.
///
/// Use `check_product_access_by_org` when the org_id is already known (avoids an
/// extra DB query). This variant looks up the org from the project.
async fn check_product_access(
    state: &WebsiteState,
    project_id: Uuid,
    product: Product,
) -> Result<(), Response> {
    let org_id: Uuid = sqlx::query_scalar("SELECT organization_id FROM projects WHERE id = $1")
        .bind(project_id)
        .fetch_optional(state.db.as_ref())
        .await
        .ok()
        .flatten()
        .ok_or_else(|| proxy_error(StatusCode::NOT_FOUND, "Project not found"))?;

    check_product_access_by_org(state, org_id, product).await
}

async fn check_product_access_by_org(
    state: &WebsiteState,
    org_id: Uuid,
    product: Product,
) -> Result<(), Response> {
    let tier = state.entitlements.get_config(org_id).await.map_err(|_| {
        proxy_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Entitlement check failed",
        )
    })?;

    if tier.config.is_product_enabled(product) {
        Ok(())
    } else {
        let product_name = match product {
            Product::Watch => "Observability",
            Product::PromptHub => "Prompt Hub",
            Product::Herd => "Herd",
        };
        Err(proxy_error(
            StatusCode::FORBIDDEN,
            &format!("{} is not available on your current plan", product_name),
        ))
    }
}

/// Billing gate for Watch ingestion: block when org has no payment method
/// and is past the 30-day grace period.
async fn check_watch_billing_gate(state: &WebsiteState, project_id: Uuid) -> Result<(), Response> {
    let org_id: Option<Uuid> =
        sqlx::query_scalar("SELECT organization_id FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(state.db.as_ref())
            .await
            .ok()
            .flatten();

    let Some(org_id) = org_id else { return Ok(()) };

    let cache_key = format!("billing:pm:{}", org_id);
    let mut has_pm_cached: Option<bool> = None;

    if let Ok(mut conn) = state.redis.get().await {
        if let Ok(val) = bb8_redis::redis::cmd("GET")
            .arg(&cache_key)
            .query_async::<Option<String>>(&mut *conn)
            .await
        {
            has_pm_cached = val.map(|v| v == "1");
        }
    }

    let has_pm = match has_pm_cached {
        Some(v) => v,
        None => {
            let v: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS(
                    SELECT 1 FROM payment_methods pm
                    JOIN stripe_customers sc ON sc.stripe_customer_id = pm.provider_customer_id
                    WHERE sc.organization_id = $1
                      AND pm.is_default = true
                      AND pm.status = 'active'
                )
                "#,
            )
            .bind(org_id)
            .fetch_one(state.db.as_ref())
            .await
            .unwrap_or(false);

            if let Ok(mut conn) = state.redis.get().await {
                let _ = bb8_redis::redis::cmd("SET")
                    .arg(&cache_key)
                    .arg(if v { "1" } else { "0" })
                    .arg("EX")
                    .arg(300)
                    .query_async::<()>(&mut *conn)
                    .await;
            }
            v
        }
    };

    if has_pm {
        return Ok(());
    }

    // Check grace period
    let created_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT created_at FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(state.db.as_ref())
            .await
            .ok()
            .flatten();

    let past_grace = created_at
        .map(|ca| chrono::Utc::now().signed_duration_since(ca).num_days() > 30)
        .unwrap_or(true);

    if past_grace {
        return Err(proxy_error(
            StatusCode::PAYMENT_REQUIRED,
            "Please add a payment method to continue using this service",
        ));
    }

    Ok(())
}

// ============================================================================
// Pond (Data Warehouse) Proxy — disabled, re-enable when Pond launches
// ============================================================================
//
// pub async fn proxy_to_pond(
//     State(state): State<Arc<WebsiteState>>,
//     method: Method,
//     uri: Uri,
//     headers: HeaderMap,
//     body: Body,
// ) -> Response {
//     match proxy_to_pond_inner(&state, method, uri, headers, body).await {
//         Ok(resp) => resp,
//         Err(resp) => resp,
//     }
// }
//
// async fn proxy_to_pond_inner(
//     state: &Arc<WebsiteState>,
//     method: Method,
//     uri: Uri,
//     headers: HeaderMap,
//     body: Body,
// ) -> Result<Response, Response> {
//     let path = uri.path();
//
//     let project_id = extract_project_id_from_path(path)
//         .ok_or_else(|| proxy_error(StatusCode::BAD_REQUEST, "Missing project ID in path"))?;
//
//     let rate_limit = rate_limit_for_path(path);
//     let user_id = authenticate_request(&headers, state.as_ref(), rate_limit)
//         .await
//         .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Authentication failed"))?;
//
//     verify_project_access(&state.db, project_id, user_id)
//         .await
//         .map_err(|_| proxy_error(StatusCode::FORBIDDEN, "Access denied to project"))?;
//
//     let downstream_path_and_query = uri
//         .path_and_query()
//         .map(|pq| pq.as_str())
//         .unwrap_or(path);
//
//     let downstream_url = format!("{}{}", state.pond_url, downstream_path_and_query);
//
//     forward_request(state, method, &downstream_url, &headers, body, |req| {
//         req.header("X-User-Id", user_id.to_string())
//     }).await
// }

// ============================================================================
// Flow (LLM Gateway) Proxy
// ============================================================================

/// Catch-all proxy handler for Flow management requests (`/api/projects/{id}/llm/*`).
pub async fn proxy_to_flow(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_flow_inner(&state, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

async fn proxy_to_flow_inner(
    state: &Arc<WebsiteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    let path = uri.path();

    let project_id = extract_project_id_from_path(path)
        .ok_or_else(|| proxy_error(StatusCode::BAD_REQUEST, "Missing project ID in path"))?;

    let rate_limit = rate_limit_for_path(path);
    let user_id = authenticate_request(&headers, state.as_ref(), rate_limit)
        .await
        .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Authentication failed"))?;

    let project = verify_project_access(&state.db, project_id, user_id)
        .await
        .map_err(|_| proxy_error(StatusCode::FORBIDDEN, "Access denied to project"))?;
    check_product_access_by_org(state, project.organization_id, Product::PromptHub).await?;

    // Flow's routes don't include `/projects/{project_id}` — strip it from the
    // path and forward project_id as a trusted header instead.
    let rewritten_path = strip_project_segment(path);
    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.flow_url, rewritten_path, q),
        None => format!("{}{}", state.flow_url, rewritten_path),
    };

    let user_jwt = extract_bearer_token(&headers);

    forward_request(state, method, &downstream_url, &headers, body, |req| {
        let req = req
            .header("X-User-Id", user_id.to_string())
            .header("X-Project-Id", project_id.to_string())
            .header("X-Organization-Id", project.organization_id.to_string());
        if let Some(ref jwt) = user_jwt {
            req.header("X-User-Jwt", jwt.as_str())
        } else {
            req
        }
    })
    .await
}

// ============================================================================
// Per-project usage limit helpers
// ============================================================================

/// Cached gateway usage limit settings for a project.
#[derive(Debug)]
struct ProjectUsageSettings {
    rate_limit_enabled: bool,
    rate_limit_rpm: i32,
}

/// Redis TTL for cached project usage settings (60 seconds).
const PROJECT_USAGE_SETTINGS_TTL_SECS: u64 = 60;

/// Fetch per-project gateway usage limit settings, with a 60-second Redis cache.
///
/// Returns `None` if both rate limiting is disabled or if the settings cannot
/// be read (in which case the caller skips enforcement rather than blocking).
async fn get_project_usage_settings(
    redis: &RedisPool,
    db: &DbPool,
    project_id: &Uuid,
) -> Option<ProjectUsageSettings> {
    let cache_key = format!("project_usage_settings:{}", project_id);

    // Try Redis cache first
    if let Ok(mut conn) = redis.get().await {
        let cached_result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            conn.get::<_, Option<String>>(cache_key.as_str()),
        )
        .await;
        if let Ok(Ok(Some(cached))) = cached_result {
            if let Ok(parts) = serde_json::from_str::<serde_json::Value>(&cached) {
                let enabled = parts
                    .get("enabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let rpm = parts.get("rpm").and_then(|v| v.as_i64()).unwrap_or(60) as i32;
                return Some(ProjectUsageSettings {
                    rate_limit_enabled: enabled,
                    rate_limit_rpm: rpm,
                });
            }
        }
    }

    // Cache miss: query the DB for the three relevant settings keys
    let rows: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT key, value
        FROM project_settings
        WHERE project_id = $1
          AND key IN ('gateway_rate_limit_enabled', 'gateway_rate_limit_rpm')
        "#,
    )
    .bind(project_id)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let mut enabled = false;
    let mut rpm: i32 = 300;

    for (key, value) in &rows {
        match key.as_str() {
            "gateway_rate_limit_enabled" => enabled = value == "true",
            "gateway_rate_limit_rpm" => rpm = value.parse().unwrap_or(300),
            _ => {}
        }
    }

    let settings = ProjectUsageSettings {
        rate_limit_enabled: enabled,
        rate_limit_rpm: rpm,
    };

    // Write back to Redis cache
    if let Ok(mut conn) = redis.get().await {
        let payload = serde_json::json!({ "enabled": enabled, "rpm": rpm }).to_string();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            conn.set_ex::<_, _, ()>(cache_key.as_str(), payload, PROJECT_USAGE_SETTINGS_TTL_SECS),
        )
        .await;
    }

    Some(settings)
}

/// Build an OpenAI-compatible rate limit exceeded response.
fn usage_limit_response(reset_at: &chrono::DateTime<Utc>, limit: i32) -> Response {
    let retry_after = (reset_at.timestamp() - Utc::now().timestamp()).max(1);
    let body = serde_json::json!({
        "error": {
            "message": "Your project's usage limit has been reached. Increase or disable the limit in your project settings.",
            "type": "rate_limit_error",
            "code": "project_usage_limit_exceeded"
        }
    });
    let mut resp = Json(body).into_response();
    *resp.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    resp.headers_mut().insert(
        "retry-after",
        HeaderValue::from_str(&retry_after.to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("60")),
    );
    resp.headers_mut().insert(
        "x-ratelimit-limit-requests",
        HeaderValue::from_str(&limit.to_string()).unwrap_or_else(|_| HeaderValue::from_static("0")),
    );
    resp
}

/// Proxy handler for Flow gateway requests (`/api/gateway/v1/*`).
///
/// SDK clients call this endpoint with API keys. The website validates the
/// API key, resolves the project, and forwards with `X-Project-Id`.
pub async fn proxy_to_flow_gateway(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_flow_gateway_inner(&state, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

async fn proxy_to_flow_gateway_inner(
    state: &Arc<WebsiteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    let path = uri.path();

    // Require Bearer prefix for API keys
    let api_key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .filter(|k| !k.is_empty())
        .ok_or_else(|| proxy_error(StatusCode::UNAUTHORIZED, "Missing or invalid Bearer token"))?;

    let project_id = crate::utils::validate_project_key_type_cached(
        &state.redis,
        state.db.as_ref(),
        api_key,
        "sdk",
    )
    .await
    .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Invalid SDK key"))?;

    check_product_access(state, project_id, Product::PromptHub).await?;

    // Enforce per-project usage limits (spend protection configured by the customer).
    // Only applies to the API key path — human UI traffic is exempt.
    if let Some(settings) = get_project_usage_settings(&state.redis, &state.db, &project_id).await {
        if settings.rate_limit_enabled {
            match crate::rate_limit::check_project_usage_limit(
                &state.redis,
                &project_id,
                settings.rate_limit_rpm,
            )
            .await
            {
                Ok(_) => {}
                Err(AppError::RateLimitExceeded(info)) => {
                    return Err(usage_limit_response(&info.reset_at, info.limit));
                }
                Err(_) => {
                    // Redis unavailable — fail open rather than blocking legitimate requests
                    tracing::warn!(project_id = %project_id, "Usage limit check failed, allowing request");
                }
            }
        }
    }

    let downstream_path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or(path);

    let downstream_url = format!("{}{}", state.flow_url, downstream_path_and_query);

    // Load scopes for the API key and forward them so downstream services
    // can enforce permissions without a second DB lookup.
    let scopes_header = load_key_scopes(&state.redis, &state.db, api_key).await;

    let scopes_vec: Vec<String> = scopes_header
        .as_ref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    if !reiver_mcp::scope::has_scope(&scopes_vec, reiver_mcp::scope::LLM_WRITE) {
        return Err(proxy_error(
            StatusCode::FORBIDDEN,
            "API key missing required scope: llm:write",
        ));
    }

    forward_request(state, method, &downstream_url, &headers, body, |req| {
        let req = req.header("X-Project-Id", project_id.to_string());
        if let Some(ref s) = scopes_header {
            req.header("X-Key-Scopes", s.as_str())
        } else {
            req
        }
    })
    .await
}

/// Load scopes for an API key and return as a JSON-encoded header value.
/// Caches in Redis for 300s to avoid hitting DB on every gateway request.
async fn load_key_scopes(redis: &RedisPool, db: &DbPool, api_key: &str) -> Option<String> {
    let key_hash = crate::utils::hash_api_key(api_key);
    let cache_key = format!("key_scopes:{}", key_hash);

    if let Ok(mut conn) = redis.get().await {
        let cached: Option<String> =
            tokio::time::timeout(std::time::Duration::from_secs(1), conn.get(&cache_key))
                .await
                .ok()
                .and_then(|r| r.ok())
                .flatten();

        if let Some(val) = cached {
            return Some(val);
        }
    }

    let scopes: Option<serde_json::Value> = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT scopes FROM project_keys WHERE key_hash = $1",
    )
    .bind(&key_hash)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    let result = scopes.map(|v: serde_json::Value| v.to_string());

    if let Some(ref json_str) = result {
        if let Ok(mut conn) = redis.get().await {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                conn.set_ex::<_, _, ()>(&cache_key, json_str.as_str(), 300),
            )
            .await;
        }
    }

    result
}

/// Catch-all proxy handler for Flow LLM requests without project prefix (`/api/llm/*`).
///
/// Supports two project_id extraction patterns:
/// - Path segment: `/api/llm/sessions/{project_id}`, `/api/llm/metrics/{project_id}/...`
/// - Query parameter: `/api/llm/prompts/configs?project_id={uuid}`
///
/// When project_id is found, project access is verified and `X-Project-Id` is
/// forwarded. The path is forwarded as-is to the Flow backend.
pub async fn proxy_to_flow_llm(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_flow_llm_inner(&state, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

async fn proxy_to_flow_llm_inner(
    state: &Arc<WebsiteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    let path = uri.path();

    let rate_limit = rate_limit_for_path(path);
    let user_id = authenticate_request(&headers, state.as_ref(), rate_limit)
        .await
        .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Authentication failed"))?;

    let project_id = extract_project_id_from_llm_path(path)
        .or_else(|| extract_project_id_from_query(uri.query()));

    let organization_id = if let Some(pid) = project_id {
        let project = verify_project_access(&state.db, pid, user_id)
            .await
            .map_err(|_| proxy_error(StatusCode::FORBIDDEN, "Access denied to project"))?;
        check_product_access_by_org(state, project.organization_id, Product::PromptHub).await?;
        Some(project.organization_id)
    } else {
        None
    };

    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.flow_url, path, q),
        None => format!("{}{}", state.flow_url, path),
    };

    let user_jwt = extract_bearer_token(&headers);

    forward_request(state, method, &downstream_url, &headers, body, |req| {
        let req = req.header("X-User-Id", user_id.to_string());
        let req = if let Some(pid) = project_id {
            req.header("X-Project-Id", pid.to_string())
        } else {
            req
        };
        let req = if let Some(oid) = organization_id {
            req.header("X-Organization-Id", oid.to_string())
        } else {
            req
        };
        if let Some(ref jwt) = user_jwt {
            req.header("X-User-Jwt", jwt.as_str())
        } else {
            req
        }
    })
    .await
}

// ============================================================================
// Watch (APM) Proxy
// ============================================================================

/// Proxy handler for Watch management requests (JWT auth, fallback).
///
/// The website authenticates the user via JWT, then forwards to Watch with
/// `X-User-Id` (and `X-Project-Id` when the path contains a project segment).
///
/// Because `.nest("/api/watch", ...)` strips the prefix, this handler receives
/// the remaining path (e.g., `/projects/{id}/exceptions`). The downstream URL
/// is built as `{watch_url}/api{remaining_path}`.
pub async fn proxy_to_watch(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_watch_inner(&state, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

/// Proxy handler for direct Watch routes (e.g. `/api/projects/{id}/traces`).
///
/// Unlike `proxy_to_watch` which is nested under `/api/watch`, these routes
/// are mounted directly at `/api/projects/{project_id}/...`. The path already
/// includes `/api/` so we forward it as-is to the Watch backend.
pub async fn proxy_to_watch_direct(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_watch_direct_inner(&state, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

async fn proxy_to_watch_direct_inner(
    state: &Arc<WebsiteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    let path = uri.path();

    let rate_limit = rate_limit_for_path(path);
    let user_id = authenticate_request(&headers, state.as_ref(), rate_limit)
        .await
        .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Authentication failed"))?;

    // Path already includes /api/projects/{id}/..., forward as-is to Watch backend
    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.watch_url, path, q),
        None => format!("{}{}", state.watch_url, path),
    };

    if let Some(project_id) = extract_project_id_from_path(path) {
        verify_project_access(&state.db, project_id, user_id)
            .await
            .map_err(|_| proxy_error(StatusCode::FORBIDDEN, "Access denied to project"))?;
        check_product_access(state, project_id, Product::Watch).await?;

        forward_request(state, method, &downstream_url, &headers, body, |req| {
            req.header("X-User-Id", user_id.to_string())
                .header("X-Project-Id", project_id.to_string())
        })
        .await
    } else {
        forward_request(state, method, &downstream_url, &headers, body, |req| {
            req.header("X-User-Id", user_id.to_string())
        })
        .await
    }
}

pub async fn proxy_to_watch_with_project(
    State(state): State<Arc<WebsiteState>>,
    Path(project_id): Path<Uuid>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_watch_with_project_inner(&state, project_id, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

async fn proxy_to_watch_with_project_inner(
    state: &Arc<WebsiteState>,
    project_id: Uuid,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    let path = uri.path();

    let rate_limit = rate_limit_for_path(path);
    let user_id = authenticate_request(&headers, state.as_ref(), rate_limit)
        .await
        .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Authentication failed"))?;

    verify_project_access(&state.db, project_id, user_id)
        .await
        .map_err(|_| proxy_error(StatusCode::FORBIDDEN, "Access denied to project"))?;
    check_product_access(state, project_id, Product::Watch).await?;

    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.watch_url, path, q),
        None => format!("{}{}", state.watch_url, path),
    };

    forward_request(state, method, &downstream_url, &headers, body, |req| {
        req.header("X-User-Id", user_id.to_string())
            .header("X-Project-Id", project_id.to_string())
    })
    .await
}

async fn proxy_to_watch_inner(
    state: &Arc<WebsiteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    let path = uri.path();

    let rate_limit = rate_limit_for_path(path);
    let user_id = authenticate_request(&headers, state.as_ref(), rate_limit)
        .await
        .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Authentication failed"))?;

    // Build downstream URL: prepend /api to the remaining path
    let downstream_path = format!("/api{}", path);
    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.watch_url, downstream_path, q),
        None => format!("{}{}", state.watch_url, downstream_path),
    };

    // If the path contains a project segment, extract and verify project access
    if let Some(project_id) = extract_project_id_from_path(path) {
        verify_project_access(&state.db, project_id, user_id)
            .await
            .map_err(|_| proxy_error(StatusCode::FORBIDDEN, "Access denied to project"))?;
        check_product_access(state, project_id, Product::Watch).await?;

        forward_request(state, method, &downstream_url, &headers, body, |req| {
            req.header("X-User-Id", user_id.to_string())
                .header("X-Project-Id", project_id.to_string())
        })
        .await
    } else {
        forward_request(state, method, &downstream_url, &headers, body, |req| {
            req.header("X-User-Id", user_id.to_string())
        })
        .await
    }
}

/// Proxy handler for Watch ingestion requests (API key auth).
///
/// SDK clients call endpoints under `/api/watch/ingest/...` with API keys.
/// The website validates the API key, resolves the project, and forwards with
/// `X-Project-Id`.
///
/// Because `.nest("/api/watch", ...)` strips the prefix, the route
/// `/ingest/{*rest}` captures `{rest}` (e.g., `v1/traces`). The downstream
/// URL is built as `{watch_url}/api/{rest}`.
pub async fn proxy_to_watch_ingest(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_watch_ingest_inner(&state, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

async fn proxy_to_watch_ingest_inner(
    state: &Arc<WebsiteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    // Require Bearer prefix for API keys
    let api_key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .filter(|k| !k.is_empty())
        .ok_or_else(|| proxy_error(StatusCode::UNAUTHORIZED, "Missing or invalid Bearer token"))?;

    let project_id = crate::utils::validate_project_key_type_cached(
        &state.redis,
        state.db.as_ref(),
        api_key,
        "sdk",
    )
    .await
    .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Invalid SDK key"))?;

    // Billing gate: block ingestion when org has no payment method and is past
    // the grace period or out of credits.
    if let Err(resp) = check_watch_billing_gate(state, project_id).await {
        return Err(resp);
    }

    check_product_access(state, project_id, Product::Watch).await?;

    let path = uri.path();
    let rest = path.strip_prefix("/ingest").unwrap_or(path);
    let downstream_path = format!("/api{}", rest);
    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.watch_url, downstream_path, q),
        None => format!("{}{}", state.watch_url, downstream_path),
    };

    forward_request(state, method, &downstream_url, &headers, body, |req| {
        req.header("X-Project-Id", project_id.to_string())
    })
    .await
}

/// Passthrough proxy for Watch endpoints that handle their own auth.
///
/// GitHub webhooks are signed with a secret that Watch validates. No JWT or
/// API key auth is applied by the website; the request is forwarded as-is with
/// the path rewritten to `/api/github/webhook`.
pub async fn proxy_to_watch_passthrough(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_watch_passthrough_inner(&state, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

async fn proxy_to_watch_passthrough_inner(
    state: &Arc<WebsiteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    // The nested router strips `/api/watch`, leaving `/github/webhook`.
    // Prepend `/api` to form the downstream path.
    let path = uri.path();
    let downstream_path = format!("/api{}", path);
    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.watch_url, downstream_path, q),
        None => format!("{}{}", state.watch_url, downstream_path),
    };

    forward_request(state, method, &downstream_url, &headers, body, |req| req).await
}

/// Proxy handler for Watch integration endpoints (slack, discord, github, etc.).
///
/// These endpoints live directly under `/api/{provider}/...` without a project
/// prefix. Authentication is via JWT; project_id is extracted from the
/// `project_id` query parameter when present.
pub async fn proxy_to_watch_integration(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_watch_integration_inner(&state, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

async fn proxy_to_watch_integration_inner(
    state: &Arc<WebsiteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    let path = uri.path();

    let rate_limit = rate_limit_for_path(path);
    let user_id = authenticate_request(&headers, state.as_ref(), rate_limit)
        .await
        .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Authentication failed"))?;

    let project_id = extract_project_id_from_query(uri.query());
    if let Some(pid) = project_id {
        verify_project_access(&state.db, pid, user_id)
            .await
            .map_err(|_| proxy_error(StatusCode::FORBIDDEN, "Access denied to project"))?;
        check_product_access(state, pid, Product::Watch).await?;
    }

    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.watch_url, path, q),
        None => format!("{}{}", state.watch_url, path),
    };

    forward_request(state, method, &downstream_url, &headers, body, |req| {
        let req = req.header("X-User-Id", user_id.to_string());
        if let Some(pid) = project_id {
            req.header("X-Project-Id", pid.to_string())
        } else {
            req
        }
    })
    .await
}

/// Passthrough proxy for Slack endpoints that handle their own auth.
///
/// Slack OAuth callback uses CSRF state (no JWT). Slack Events API uses
/// request signature verification. Both are forwarded as-is.
pub async fn proxy_to_watch_slack_passthrough(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    tracing::info!(method = %method, uri = %uri, "Slack passthrough proxy hit");
    let path = uri.path();
    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.watch_url, path, q),
        None => format!("{}{}", state.watch_url, path),
    };
    forward_request(&state, method, &downstream_url, &headers, body, |req| req)
        .await
        .unwrap_or_else(|resp| resp)
}

// ============================================================================
// Shared forwarding logic
// ============================================================================

/// Forward a request to a downstream service and stream the response back.
///
/// Streams the response body instead of buffering it, which is critical for
/// SSE endpoints like the LLM gateway's `/v1/chat/completions?stream=true`.
async fn forward_request<F>(
    state: &Arc<WebsiteState>,
    method: Method,
    downstream_url: &str,
    headers: &HeaderMap,
    body: Body,
    add_trusted_headers: F,
) -> Result<Response, Response>
where
    F: Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
{
    let body_bytes = axum::body::to_bytes(body, MAX_PROXY_BODY_SIZE)
        .await
        .map_err(|_| proxy_error(StatusCode::PAYLOAD_TOO_LARGE, "Request body too large"))?;

    let reqwest_method: reqwest::Method = method
        .as_str()
        .parse()
        .map_err(|_| proxy_error(StatusCode::BAD_REQUEST, "Invalid HTTP method"))?;

    let build_request = |body: bytes::Bytes| {
        let mut req = state
            .http_client
            .request(reqwest_method.clone(), downstream_url);
        req = add_trusted_headers(req);
        for (name, value) in headers.iter() {
            let name_str = name.as_str();
            if SKIP_HEADERS.contains(&name_str) {
                continue;
            }
            if let Ok(v) = reqwest::header::HeaderValue::from_bytes(value.as_bytes()) {
                if let Ok(n) = reqwest::header::HeaderName::from_bytes(name.as_ref()) {
                    req = req.header(n, v);
                }
            }
        }
        req.body(body)
    };

    // Retry once on connection-level errors (stale keep-alive connections).
    let downstream_resp = match build_request(body_bytes.clone()).send().await {
        Ok(resp) => resp,
        Err(e) if e.is_connect() || e.to_string().contains("connection closed") => {
            tracing::warn!(error = %e, url = %downstream_url, "Retrying proxy request after connection error");
            build_request(body_bytes).send().await.map_err(|e| {
                tracing::error!(error = %e, url = %downstream_url, "Proxy request to downstream failed on retry");
                proxy_error(StatusCode::BAD_GATEWAY, "Downstream service unavailable")
            })?
        }
        Err(e) => {
            tracing::error!(error = %e, url = %downstream_url, "Proxy request to downstream failed");
            return Err(proxy_error(
                StatusCode::BAD_GATEWAY,
                "Downstream service unavailable",
            ));
        }
    };

    let status = StatusCode::from_u16(downstream_resp.status().as_u16())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    let mut response_builder = Response::builder().status(status);
    for (name, value) in downstream_resp.headers().iter() {
        if let Ok(v) = HeaderValue::from_bytes(value.as_bytes()) {
            response_builder = response_builder.header(name.as_str(), v);
        }
    }

    // Stream the response body instead of buffering it.
    // This is critical for SSE endpoints (e.g., LLM gateway streaming).
    let body_stream = downstream_resp
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let streaming_body = Body::from_stream(body_stream);

    response_builder.body(streaming_body).map_err(|_| {
        proxy_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to build response",
        )
    })
}

// ============================================================================
// MCP proxy: passthrough to the MCP server
// ============================================================================

/// Forward `/mcp` requests directly to the MCP service. The Bearer token is
/// a project API key; we resolve the project to enforce product entitlements
/// before forwarding. We must re-inject the Authorization header because
/// `forward_request` strips it (it's in SKIP_HEADERS for other proxies).
pub async fn proxy_to_mcp(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let auth_value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if let Some(ref auth) = auth_value {
        if let Some(api_key) = auth.strip_prefix("Bearer ") {
            if let Ok(project_id) =
                crate::utils::validate_project_key_cached(&state.redis, state.db.as_ref(), api_key)
                    .await
            {
                if let Err(resp) =
                    check_product_access(&state, project_id, Product::PromptHub).await
                {
                    return resp;
                }
            }
        }
    }

    let downstream_url = format!("{}/mcp", state.mcp_url);
    forward_request(
        &state,
        method,
        &downstream_url,
        &headers,
        body,
        |mut req| {
            if let Some(ref v) = auth_value {
                req = req.header("authorization", v.as_str());
            }
            req
        },
    )
    .await
    .unwrap_or_else(|resp| resp)
}

// ============================================================================
// Herd (A2A Agent Registry) proxy
// ============================================================================

/// Forward `/a2a` JSON-RPC protocol requests to Herd. The Bearer token is
/// a project API key; we resolve the project to enforce product entitlements
/// before forwarding. Herd also validates the key by calling back to the
/// website's `/api/auth/validate-key` endpoint.
pub async fn proxy_to_herd_a2a(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let auth_value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if let Some(ref auth) = auth_value {
        if let Some(api_key) = auth.strip_prefix("Bearer ") {
            if let Ok(project_id) =
                crate::utils::validate_project_key_cached(&state.redis, state.db.as_ref(), api_key)
                    .await
            {
                if let Err(resp) = check_product_access(&state, project_id, Product::Herd).await {
                    return resp;
                }
            }
        }
    }

    let downstream_url = format!("{}/a2a", state.herd_url);
    forward_request(
        &state,
        method,
        &downstream_url,
        &headers,
        body,
        |mut req| {
            if let Some(ref v) = auth_value {
                req = req.header("authorization", v.as_str());
            }
            req
        },
    )
    .await
    .unwrap_or_else(|resp| resp)
}

/// Proxy `/api/herd/*` REST requests to the Herd backend.
/// Authenticated via JWT (UI/dashboard calls).
pub async fn proxy_to_herd(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_to_herd_inner(&state, method, uri, headers, body)
        .await
        .unwrap_or_else(|resp| resp)
}

async fn proxy_to_herd_inner(
    state: &Arc<WebsiteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Response> {
    let path = uri.path();

    let rate_limit = rate_limit_for_path(path);
    let user_id = authenticate_request(&headers, state.as_ref(), rate_limit)
        .await
        .map_err(|_| proxy_error(StatusCode::UNAUTHORIZED, "Authentication failed"))?;

    // Extract project_id from path (e.g. /api/projects/{id}/herd/...) or from
    // a project-scoped API key (resolved during authentication).
    let project_id = extract_project_id_from_path(path)
        .or_else(|| {
            headers
                .get("x-project-id")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| Uuid::parse_str(s).ok())
        })
        .ok_or_else(|| proxy_error(StatusCode::BAD_REQUEST, "Missing project ID"))?;

    let project = verify_project_access(&state.db, project_id, user_id)
        .await
        .map_err(|_| proxy_error(StatusCode::FORBIDDEN, "Access denied to project"))?;
    check_product_access_by_org(state, project.organization_id, Product::Herd).await?;

    let rewritten_path = strip_project_segment(path);
    let downstream_url = match uri.query() {
        Some(q) => format!("{}{}?{}", state.herd_url, rewritten_path, q),
        None => format!("{}{}", state.herd_url, rewritten_path),
    };

    forward_request(state, method, &downstream_url, &headers, body, |req| {
        req.header("X-User-Id", user_id.to_string())
            .header("X-Project-Id", project_id.to_string())
            .header("X-Organization-Id", project.organization_id.to_string())
    })
    .await
}

// ============================================================================
// Public (unauthenticated) proxy routes
// ============================================================================

/// Forward `GET /api/model-catalog` to Flow's model pricing endpoint.
/// This is the only unauthenticated Flow proxy route — it returns the public
/// model catalog with pricing, latency, error, and security stats.
pub async fn proxy_to_flow_model_catalog(
    State(state): State<Arc<WebsiteState>>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let downstream_url = format!("{}/api/llm/models/pricing", state.flow_url);
    forward_request(&state, method, &downstream_url, &headers, body, |req| req)
        .await
        .unwrap_or_else(|resp| resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_headers_includes_trusted_identity_and_audit_headers() {
        for header in [
            "x-project-id",
            "x-user-id",
            "x-user-jwt",
            "x-key-scopes",
            "x-organization-id",
            "x-billing-project-id",
            "x-creator-type",
            "x-creator-key-label",
            "x-creator-key-prefix",
            "x-audit-origin-type",
            "x-audit-origin-ref",
            "x-audit-origin-reason",
        ] {
            assert!(
                SKIP_HEADERS.contains(&header),
                "trusted header must be stripped before the proxy adds its own value: {header}"
            );
        }
    }

    #[test]
    fn test_extract_project_id() {
        let id = Uuid::new_v4();
        let path = format!("/api/projects/{}/warehouse/sources", id);
        assert_eq!(extract_project_id_from_path(&path), Some(id));

        let path2 = format!("/api/projects/{}/catalog/tables", id);
        assert_eq!(extract_project_id_from_path(&path2), Some(id));

        assert_eq!(extract_project_id_from_path("/api/auth/login"), None);
        assert_eq!(extract_project_id_from_path("/health"), None);
    }

    #[test]
    fn test_rate_limit_for_path() {
        assert!(matches!(
            rate_limit_for_path("/api/projects/abc/warehouse/query"),
            RateLimitType::Analytics
        ));
        assert!(matches!(
            rate_limit_for_path("/api/projects/abc/warehouse/sources"),
            RateLimitType::Crud
        ));
    }

    #[test]
    fn test_strip_project_segment() {
        let id = Uuid::new_v4();

        // Standard management path
        let path = format!("/api/projects/{}/llm/sessions", id);
        assert_eq!(strip_project_segment(&path), "/api/llm/sessions");

        // With sub-resource
        let path = format!("/api/projects/{}/llm/integrations/openai/test", id);
        assert_eq!(
            strip_project_segment(&path),
            "/api/llm/integrations/openai/test"
        );

        // No projects segment — pass through unchanged
        assert_eq!(
            strip_project_segment("/api/llm/sessions"),
            "/api/llm/sessions"
        );

        // Root path
        assert_eq!(strip_project_segment("/"), "/");
    }

    #[test]
    fn test_extract_project_id_from_llm_path() {
        let id = Uuid::new_v4();

        // sessions are excluded (use project-prefixed URLs instead)
        let path = format!("/api/llm/sessions/{}", id);
        assert_eq!(extract_project_id_from_llm_path(&path), None);

        // metrics pattern
        let path = format!("/api/llm/metrics/{}/summary", id);
        assert_eq!(extract_project_id_from_llm_path(&path), Some(id));

        // prompts pattern — no UUID in segment 4
        assert_eq!(
            extract_project_id_from_llm_path("/api/llm/prompts/configs"),
            None
        );

        // too few segments
        assert_eq!(extract_project_id_from_llm_path("/api/llm/sessions"), None);
    }

    #[test]
    fn test_extract_project_id_from_query() {
        let id = Uuid::new_v4();

        let q = format!("project_id={}&limit=10", id);
        assert_eq!(extract_project_id_from_query(Some(&q)), Some(id));

        let q2 = format!("limit=10&project_id={}", id);
        assert_eq!(extract_project_id_from_query(Some(&q2)), Some(id));

        assert_eq!(extract_project_id_from_query(Some("limit=10")), None);
        assert_eq!(extract_project_id_from_query(None), None);
    }

    // --- NL query rate limit path tests ---

    #[test]
    fn test_rate_limit_natural_language_path() {
        assert!(matches!(
            rate_limit_for_path("/api/projects/abc/warehouse/query/natural-language"),
            RateLimitType::NlQuery
        ));
    }

    #[test]
    fn test_rate_limit_natural_language_before_query() {
        // "/natural-language" path should match NlQuery, NOT Analytics
        // even though it also contains "query" (tested by ordering)
        let nl_path = "/api/projects/abc/warehouse/query/natural-language";
        assert!(matches!(
            rate_limit_for_path(nl_path),
            RateLimitType::NlQuery
        ));

        // Regular query path should still be Analytics
        let query_path = "/api/projects/abc/warehouse/query";
        assert!(matches!(
            rate_limit_for_path(query_path),
            RateLimitType::Analytics
        ));
    }

    #[test]
    fn test_rate_limit_non_matching_language_path() {
        // A settings path should map to Crud, not NlQuery
        assert!(matches!(
            rate_limit_for_path("/api/projects/abc/warehouse/settings"),
            RateLimitType::Crud
        ));
        // A sources path should also map to Crud
        assert!(matches!(
            rate_limit_for_path("/api/projects/abc/warehouse/sources"),
            RateLimitType::Crud
        ));
    }
}
