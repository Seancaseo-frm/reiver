//! HTTP/HTTPS Health Check Implementation

use anyhow::Result;
use reqwest::redirect::Policy;
use std::time::{Duration, Instant};
use tracing::{debug, error};

use super::{CheckStatus, HealthCheckConfig, HealthCheckResult};

pub struct HttpHealthCheck;

impl HttpHealthCheck {
    /// Perform an HTTP/HTTPS health check
    pub async fn check(config: &HealthCheckConfig) -> HealthCheckResult {
        let url = match &config.target_url {
            Some(url) => url.clone(),
            None => {
                return HealthCheckResult {
                    check_id: config.id.clone(),
                    check_type: "http".to_string(),
                    check_name: config.name.clone(),
                    target: "".to_string(),
                    status: CheckStatus::Error,
                    success: false,
                    response_time_ms: 0.0,
                    dns_time_ms: None,
                    connect_time_ms: None,
                    tls_time_ms: None,
                    first_byte_time_ms: None,
                    http_status_code: None,
                    http_response_size: None,
                    ssl_valid: false,
                    ssl_days_until_expiry: None,
                    ssl_issuer: None,
                    ssl_subject: None,
                    ssl_expires_at: None,
                    error_type: Some("configuration".to_string()),
                    error_message: Some("target_url is required for HTTP checks".to_string()),
                    response_body: None,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                };
            }
        };

        let start = Instant::now();
        
        // Build client
        let redirect_policy = if config.http_follow_redirects.unwrap_or(true) {
            Policy::limited(10)
        } else {
            Policy::none()
        };

        let client = match reqwest::Client::builder()
            .timeout(config.http_timeout())
            .redirect(redirect_policy)
            .danger_accept_invalid_certs(false)
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                return Self::error_result(config, &url, "client_build", &e.to_string());
            }
        };

        // Build request
        let method = config.http_method.as_deref().unwrap_or("GET");
        let mut request = match method.to_uppercase().as_str() {
            "GET" => client.get(&url),
            "POST" => client.post(&url),
            "PUT" => client.put(&url),
            "DELETE" => client.delete(&url),
            "HEAD" => client.head(&url),
            "PATCH" => client.patch(&url),
            _ => client.get(&url),
        };

        // Add headers
        if let Some(headers) = &config.http_headers {
            for (key, value) in headers {
                request = request.header(key, value);
            }
        }

        // Add body
        if let Some(body) = &config.http_body {
            request = request.body(body.clone());
        }

        // Execute request
        let response = match request.send().await {
            Ok(resp) => resp,
            Err(e) => {
                let error_type = if e.is_timeout() {
                    "timeout"
                } else if e.is_connect() {
                    "connection"
                } else if e.is_builder() {
                    "request_build"
                } else {
                    "request"
                };
                
                let status = if e.is_timeout() {
                    CheckStatus::Timeout
                } else {
                    CheckStatus::Unhealthy
                };

                return HealthCheckResult {
                    check_id: config.id.clone(),
                    check_type: "http".to_string(),
                    check_name: config.name.clone(),
                    target: url,
                    status,
                    success: false,
                    response_time_ms: start.elapsed().as_secs_f64() * 1000.0,
                    dns_time_ms: None,
                    connect_time_ms: None,
                    tls_time_ms: None,
                    first_byte_time_ms: None,
                    http_status_code: None,
                    http_response_size: None,
                    ssl_valid: false,
                    ssl_days_until_expiry: None,
                    ssl_issuer: None,
                    ssl_subject: None,
                    ssl_expires_at: None,
                    error_type: Some(error_type.to_string()),
                    error_message: Some(e.to_string()),
                    response_body: None,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                };
            }
        };

        let status_code = response.status().as_u16() as i32;
        let response_time = start.elapsed().as_secs_f64() * 1000.0;

        // Get response body
        let body = match response.bytes().await {
            Ok(b) => b,
            Err(e) => {
                return Self::error_result(config, &url, "response_body", &e.to_string());
            }
        };

        let body_size = body.len() as i64;
        let body_str = String::from_utf8_lossy(&body).to_string();
        let body_preview = if body_str.len() > 1000 {
            format!("{}...", &body_str[..1000])
        } else {
            body_str.clone()
        };

        // Check expected status
        let expected_statuses = config.http_expected_status.clone().unwrap_or_else(|| vec![200]);
        let status_ok = expected_statuses.contains(&status_code);

        // Check expected body
        let body_ok = match &config.http_expected_body {
            Some(expected) => body_str.contains(expected),
            None => true,
        };

        let success = status_ok && body_ok;
        let status = if success {
            CheckStatus::Healthy
        } else {
            CheckStatus::Unhealthy
        };

        let error_message = if !status_ok {
            Some(format!("Expected status {:?}, got {}", expected_statuses, status_code))
        } else if !body_ok {
            Some(format!("Expected body to contain '{}'", config.http_expected_body.as_ref().unwrap()))
        } else {
            None
        };

        debug!(
            "HTTP check {} -> {} ({}ms, status={})",
            config.name, status, response_time, status_code
        );

        HealthCheckResult {
            check_id: config.id.clone(),
            check_type: "http".to_string(),
            check_name: config.name.clone(),
            target: url,
            status,
            success,
            response_time_ms: response_time,
            dns_time_ms: None,
            connect_time_ms: None,
            tls_time_ms: None,
            first_byte_time_ms: None,
            http_status_code: Some(status_code),
            http_response_size: Some(body_size),
            ssl_valid: false,
            ssl_days_until_expiry: None,
            ssl_issuer: None,
            ssl_subject: None,
            ssl_expires_at: None,
            error_type: if !success { Some("assertion".to_string()) } else { None },
            error_message,
            response_body: Some(body_preview),
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    fn error_result(config: &HealthCheckConfig, url: &str, error_type: &str, message: &str) -> HealthCheckResult {
        HealthCheckResult {
            check_id: config.id.clone(),
            check_type: "http".to_string(),
            check_name: config.name.clone(),
            target: url.to_string(),
            status: CheckStatus::Error,
            success: false,
            response_time_ms: 0.0,
            dns_time_ms: None,
            connect_time_ms: None,
            tls_time_ms: None,
            first_byte_time_ms: None,
            http_status_code: None,
            http_response_size: None,
            ssl_valid: false,
            ssl_days_until_expiry: None,
            ssl_issuer: None,
            ssl_subject: None,
            ssl_expires_at: None,
            error_type: Some(error_type.to_string()),
            error_message: Some(message.to_string()),
            response_body: None,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }
}
