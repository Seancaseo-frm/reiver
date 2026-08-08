//! Health Checks Module
//!
//! Synthetic monitoring probes for verifying service availability:
//! - HTTP/HTTPS endpoint checks
//! - TCP/UDP port connectivity
//! - SSL/TLS certificate monitoring

mod http;
mod tcp;
mod ssl;
mod runner;

pub use http::HttpHealthCheck;
pub use tcp::TcpHealthCheck;
pub use ssl::SslCertificateCheck;
pub use runner::HealthCheckRunner;

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Health check result
#[derive(Debug, Clone, Serialize)]
pub struct HealthCheckResult {
    pub check_id: String,
    pub check_type: String,
    pub check_name: String,
    pub target: String,
    pub status: CheckStatus,
    pub success: bool,
    pub response_time_ms: f64,
    pub dns_time_ms: Option<f64>,
    pub connect_time_ms: Option<f64>,
    pub tls_time_ms: Option<f64>,
    pub first_byte_time_ms: Option<f64>,
    pub http_status_code: Option<i32>,
    pub http_response_size: Option<i64>,
    pub ssl_valid: bool,
    pub ssl_days_until_expiry: Option<i32>,
    pub ssl_issuer: Option<String>,
    pub ssl_subject: Option<String>,
    pub ssl_expires_at: Option<i64>,
    pub error_type: Option<String>,
    pub error_message: Option<String>,
    pub response_body: Option<String>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Healthy,
    Unhealthy,
    Timeout,
    Error,
}

impl std::fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckStatus::Healthy => write!(f, "healthy"),
            CheckStatus::Unhealthy => write!(f, "unhealthy"),
            CheckStatus::Timeout => write!(f, "timeout"),
            CheckStatus::Error => write!(f, "error"),
        }
    }
}

/// Configuration for a health check
#[derive(Debug, Clone, Deserialize)]
pub struct HealthCheckConfig {
    pub id: String,
    pub project_id: String,
    pub check_type: String,
    pub name: String,
    pub target_url: Option<String>,
    pub target_host: Option<String>,
    pub target_port: Option<u16>,
    
    // HTTP settings
    pub http_method: Option<String>,
    pub http_headers: Option<std::collections::HashMap<String, String>>,
    pub http_body: Option<String>,
    pub http_expected_status: Option<Vec<i32>>,
    pub http_expected_body: Option<String>,
    pub http_follow_redirects: Option<bool>,
    pub http_timeout_ms: Option<u64>,
    
    // TCP settings
    pub tcp_send_data: Option<String>,
    pub tcp_expect_data: Option<String>,
    
    // SSL settings
    pub ssl_check_expiry: Option<bool>,
    pub ssl_expiry_warning_days: Option<i32>,
    pub ssl_check_chain: Option<bool>,
    
    // Scheduling
    pub check_interval_seconds: u64,
    pub timeout_seconds: Option<u64>,
}

impl HealthCheckConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_seconds.unwrap_or(30))
    }
    
    pub fn http_timeout(&self) -> Duration {
        Duration::from_millis(self.http_timeout_ms.unwrap_or(10000))
    }
}
