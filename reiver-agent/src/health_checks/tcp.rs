//! TCP/UDP Health Check Implementation

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::timeout;
use tracing::{debug, error};

use super::{CheckStatus, HealthCheckConfig, HealthCheckResult};

pub struct TcpHealthCheck;

impl TcpHealthCheck {
    /// Perform a TCP connectivity check
    pub async fn check(config: &HealthCheckConfig) -> HealthCheckResult {
        let host = match &config.target_host {
            Some(h) => h.clone(),
            None => {
                return Self::error_result(config, "", "configuration", "target_host is required for TCP checks");
            }
        };

        let port = match config.target_port {
            Some(p) => p,
            None => {
                return Self::error_result(config, &host, "configuration", "target_port is required for TCP checks");
            }
        };

        let target = format!("{}:{}", host, port);
        let start = Instant::now();
        let check_timeout = config.timeout();

        // Attempt TCP connection
        let stream = match timeout(check_timeout, TcpStream::connect(&target)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return HealthCheckResult {
                    check_id: config.id.clone(),
                    check_type: "tcp".to_string(),
                    check_name: config.name.clone(),
                    target: target.clone(),
                    status: CheckStatus::Unhealthy,
                    success: false,
                    response_time_ms: start.elapsed().as_secs_f64() * 1000.0,
                    dns_time_ms: None,
                    connect_time_ms: Some(start.elapsed().as_secs_f64() * 1000.0),
                    tls_time_ms: None,
                    first_byte_time_ms: None,
                    http_status_code: None,
                    http_response_size: None,
                    ssl_valid: false,
                    ssl_days_until_expiry: None,
                    ssl_issuer: None,
                    ssl_subject: None,
                    ssl_expires_at: None,
                    error_type: Some("connection".to_string()),
                    error_message: Some(e.to_string()),
                    response_body: None,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                };
            }
            Err(_) => {
                return HealthCheckResult {
                    check_id: config.id.clone(),
                    check_type: "tcp".to_string(),
                    check_name: config.name.clone(),
                    target: target.clone(),
                    status: CheckStatus::Timeout,
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
                    error_type: Some("timeout".to_string()),
                    error_message: Some(format!("Connection timeout after {}s", check_timeout.as_secs())),
                    response_body: None,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                };
            }
        };

        let connect_time = start.elapsed().as_secs_f64() * 1000.0;
        let mut stream = stream;

        // Optional: send data and check response
        let mut response_data = String::new();
        let mut success = true;
        let mut error_message = None;

        if let Some(send_data) = &config.tcp_send_data {
            if let Err(e) = stream.write_all(send_data.as_bytes()).await {
                return Self::error_result(config, &target, "write", &e.to_string());
            }

            // Read response if expected
            if config.tcp_expect_data.is_some() {
                let mut buf = vec![0u8; 4096];
                match timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
                    Ok(Ok(n)) => {
                        response_data = String::from_utf8_lossy(&buf[..n]).to_string();
                    }
                    Ok(Err(e)) => {
                        return Self::error_result(config, &target, "read", &e.to_string());
                    }
                    Err(_) => {
                        return Self::error_result(config, &target, "timeout", "Read timeout waiting for response");
                    }
                }
            }
        }

        // Check expected response
        if let Some(expected) = &config.tcp_expect_data {
            if !response_data.contains(expected) {
                success = false;
                error_message = Some(format!("Expected response to contain '{}'", expected));
            }
        }

        let response_time = start.elapsed().as_secs_f64() * 1000.0;
        let status = if success { CheckStatus::Healthy } else { CheckStatus::Unhealthy };

        debug!(
            "TCP check {} -> {} ({}ms)",
            config.name, status, response_time
        );

        HealthCheckResult {
            check_id: config.id.clone(),
            check_type: "tcp".to_string(),
            check_name: config.name.clone(),
            target,
            status,
            success,
            response_time_ms: response_time,
            dns_time_ms: None,
            connect_time_ms: Some(connect_time),
            tls_time_ms: None,
            first_byte_time_ms: None,
            http_status_code: None,
            http_response_size: None,
            ssl_valid: false,
            ssl_days_until_expiry: None,
            ssl_issuer: None,
            ssl_subject: None,
            ssl_expires_at: None,
            error_type: if !success { Some("assertion".to_string()) } else { None },
            error_message,
            response_body: if response_data.is_empty() { None } else { Some(response_data) },
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Perform a UDP check (send packet, optionally wait for response)
    pub async fn check_udp(config: &HealthCheckConfig) -> HealthCheckResult {
        let host = match &config.target_host {
            Some(h) => h.clone(),
            None => {
                return Self::error_result(config, "", "configuration", "target_host is required for UDP checks");
            }
        };

        let port = match config.target_port {
            Some(p) => p,
            None => {
                return Self::error_result(config, &host, "configuration", "target_port is required for UDP checks");
            }
        };

        let target = format!("{}:{}", host, port);
        let start = Instant::now();

        // Create UDP socket
        let socket = match UdpSocket::bind("0.0.0.0:0").await {
            Ok(s) => s,
            Err(e) => {
                return Self::error_result(config, &target, "bind", &e.to_string());
            }
        };

        // Connect to target
        if let Err(e) = socket.connect(&target).await {
            return Self::error_result(config, &target, "connect", &e.to_string());
        }

        // Send data
        let send_data = config.tcp_send_data.as_deref().unwrap_or("ping");
        if let Err(e) = socket.send(send_data.as_bytes()).await {
            return Self::error_result(config, &target, "send", &e.to_string());
        }

        let mut success = true;
        let mut response_data = String::new();
        let mut error_message = None;

        // Wait for response if expected
        if config.tcp_expect_data.is_some() {
            let mut buf = vec![0u8; 4096];
            match timeout(Duration::from_secs(5), socket.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    response_data = String::from_utf8_lossy(&buf[..n]).to_string();
                }
                Ok(Err(e)) => {
                    return Self::error_result(config, &target, "recv", &e.to_string());
                }
                Err(_) => {
                    // UDP timeout is not necessarily an error - packet might be lost
                    success = false;
                    error_message = Some("No response received".to_string());
                }
            }

            // Check expected response
            if let Some(expected) = &config.tcp_expect_data {
                if !response_data.contains(expected) {
                    success = false;
                    error_message = Some(format!("Expected response to contain '{}'", expected));
                }
            }
        }

        let response_time = start.elapsed().as_secs_f64() * 1000.0;
        let status = if success { CheckStatus::Healthy } else { CheckStatus::Unhealthy };

        debug!(
            "UDP check {} -> {} ({}ms)",
            config.name, status, response_time
        );

        HealthCheckResult {
            check_id: config.id.clone(),
            check_type: "udp".to_string(),
            check_name: config.name.clone(),
            target,
            status,
            success,
            response_time_ms: response_time,
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
            error_type: if !success { Some("assertion".to_string()) } else { None },
            error_message,
            response_body: if response_data.is_empty() { None } else { Some(response_data) },
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    fn error_result(config: &HealthCheckConfig, target: &str, error_type: &str, message: &str) -> HealthCheckResult {
        HealthCheckResult {
            check_id: config.id.clone(),
            check_type: "tcp".to_string(),
            check_name: config.name.clone(),
            target: target.to_string(),
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
