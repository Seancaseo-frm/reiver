//! SSL/TLS Certificate Health Check Implementation

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_native_tls::TlsConnector;
use tracing::{debug, error};

use super::{CheckStatus, HealthCheckConfig, HealthCheckResult};

pub struct SslCertificateCheck;

impl SslCertificateCheck {
    /// Perform an SSL certificate check
    pub async fn check(config: &HealthCheckConfig) -> HealthCheckResult {
        // Determine target from URL or host:port
        let (host, port) = if let Some(url) = &config.target_url {
            match Self::parse_url(url) {
                Ok((h, p)) => (h, p),
                Err(e) => {
                    return Self::error_result(config, url, "configuration", &e);
                }
            }
        } else if let (Some(h), Some(p)) = (&config.target_host, config.target_port) {
            (h.clone(), p)
        } else {
            return Self::error_result(
                config,
                "",
                "configuration",
                "Either target_url or target_host+target_port is required for SSL checks",
            );
        };

        let target = format!("{}:{}", host, port);
        let start = Instant::now();
        let check_timeout = config.timeout();

        // Connect TCP
        let tcp_stream = match timeout(check_timeout, TcpStream::connect(&target)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Self::error_result(config, &target, "connection", &e.to_string());
            }
            Err(_) => {
                return HealthCheckResult {
                    check_id: config.id.clone(),
                    check_type: "ssl".to_string(),
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
                    error_message: Some("Connection timeout".to_string()),
                    response_body: None,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                };
            }
        };

        let connect_time = start.elapsed().as_secs_f64() * 1000.0;

        // Perform TLS handshake
        let connector = match native_tls::TlsConnector::builder()
            .danger_accept_invalid_certs(false)
            .build()
        {
            Ok(c) => TlsConnector::from(c),
            Err(e) => {
                return Self::error_result(config, &target, "tls_build", &e.to_string());
            }
        };

        let tls_stream = match timeout(Duration::from_secs(10), connector.connect(&host, tcp_stream)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return HealthCheckResult {
                    check_id: config.id.clone(),
                    check_type: "ssl".to_string(),
                    check_name: config.name.clone(),
                    target: target.clone(),
                    status: CheckStatus::Unhealthy,
                    success: false,
                    response_time_ms: start.elapsed().as_secs_f64() * 1000.0,
                    dns_time_ms: None,
                    connect_time_ms: Some(connect_time),
                    tls_time_ms: Some(start.elapsed().as_secs_f64() * 1000.0 - connect_time),
                    first_byte_time_ms: None,
                    http_status_code: None,
                    http_response_size: None,
                    ssl_valid: false,
                    ssl_days_until_expiry: None,
                    ssl_issuer: None,
                    ssl_subject: None,
                    ssl_expires_at: None,
                    error_type: Some("tls_handshake".to_string()),
                    error_message: Some(e.to_string()),
                    response_body: None,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                };
            }
            Err(_) => {
                return Self::error_result(config, &target, "timeout", "TLS handshake timeout");
            }
        };

        let tls_time = start.elapsed().as_secs_f64() * 1000.0 - connect_time;

        // Get certificate info from the DER-encoded certificate
        // Note: native_tls doesn't expose certificate details directly,
        // so we extract what we can from the connection
        let peer_cert = tls_stream.get_ref().peer_certificate();
        
        // For detailed cert info, we'll use the response from the server
        // or parse the DER bytes if available
        let (ssl_issuer, ssl_subject, ssl_expires_at, days_until_expiry) = match peer_cert {
            Ok(Some(cert)) => {
                // native_tls Certificate only gives us the DER bytes via to_der()
                // For detailed parsing, we'd need x509-parser crate
                // For now, just indicate the cert exists
                let der = cert.to_der().ok();
                
                // Parse certificate using simple DER extraction
                // (In production, use x509-parser crate for proper parsing)
                let (issuer, subject, expires) = if let Some(der_bytes) = der {
                    Self::parse_der_certificate(&der_bytes)
                } else {
                    (None, None, None)
                };
                
                let days = expires.map(|exp| {
                    let now = chrono::Utc::now();
                    ((exp - now.timestamp_millis()) / (24 * 60 * 60 * 1000)) as i32
                });
                
                (issuer, subject, expires, days)
            }
            _ => (None, None, None, None),
        };

        // Determine if certificate is valid
        let warning_days = config.ssl_expiry_warning_days.unwrap_or(30);
        let mut ssl_valid = true;
        let mut error_message = None;

        if let Some(days) = days_until_expiry {
            if days < 0 {
                ssl_valid = false;
                error_message = Some(format!("Certificate expired {} days ago", -days));
            } else if days < warning_days && config.ssl_check_expiry.unwrap_or(true) {
                ssl_valid = false;
                error_message = Some(format!("Certificate expires in {} days", days));
            }
        }

        let response_time = start.elapsed().as_secs_f64() * 1000.0;
        let status = if ssl_valid { CheckStatus::Healthy } else { CheckStatus::Unhealthy };

        debug!(
            "SSL check {} -> {} ({}ms, expires in {:?} days)",
            config.name, status, response_time, days_until_expiry
        );

        HealthCheckResult {
            check_id: config.id.clone(),
            check_type: "ssl".to_string(),
            check_name: config.name.clone(),
            target,
            status,
            success: ssl_valid,
            response_time_ms: response_time,
            dns_time_ms: None,
            connect_time_ms: Some(connect_time),
            tls_time_ms: Some(tls_time),
            first_byte_time_ms: None,
            http_status_code: None,
            http_response_size: None,
            ssl_valid,
            ssl_days_until_expiry: days_until_expiry,
            ssl_issuer,
            ssl_subject,
            ssl_expires_at,
            error_type: if !ssl_valid { Some("certificate".to_string()) } else { None },
            error_message,
            response_body: None,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    fn parse_url(url: &str) -> std::result::Result<(String, u16), String> {
        let url = if !url.starts_with("https://") && !url.starts_with("http://") {
            format!("https://{}", url)
        } else {
            url.to_string()
        };

        let parsed = url::Url::parse(&url).map_err(|e| e.to_string())?;
        let host = parsed.host_str().ok_or("No host in URL")?.to_string();
        let port = parsed.port().unwrap_or(443);
        Ok((host, port))
    }

    /// Simple DER certificate parser to extract basic info
    /// In production, use x509-parser crate for proper ASN.1 parsing
    fn parse_der_certificate(der: &[u8]) -> (Option<String>, Option<String>, Option<i64>) {
        // This is a simplified parser that looks for common patterns
        // For production, use a proper X.509 parser
        
        // Try to find the validity period (look for UTCTime or GeneralizedTime)
        // ASN.1 UTCTime tag is 0x17, GeneralizedTime is 0x18
        let mut expires_at = None;
        
        for i in 0..der.len().saturating_sub(15) {
            // Look for UTCTime (tag 0x17, length 13 for YYMMDDHHMMSSZ format)
            if der[i] == 0x17 && i + 1 < der.len() && der[i + 1] == 13 {
                if let Some(ts) = Self::parse_utc_time(&der[i + 2..i + 15]) {
                    // The second UTCTime in the sequence is the expiry
                    expires_at = Some(ts);
                }
            }
            // Look for GeneralizedTime (tag 0x18, length 15 for YYYYMMDDHHMMSSZ)
            if der[i] == 0x18 && i + 1 < der.len() && der[i + 1] == 15 {
                if let Some(ts) = Self::parse_generalized_time(&der[i + 2..i + 17]) {
                    expires_at = Some(ts);
                }
            }
        }
        
        // For issuer/subject, we'd need proper ASN.1 parsing
        // Return None for now - the connection success is the important part
        (None, None, expires_at)
    }

    fn parse_utc_time(bytes: &[u8]) -> Option<i64> {
        if bytes.len() < 13 {
            return None;
        }
        
        let s = std::str::from_utf8(bytes).ok()?;
        // Format: YYMMDDHHMMSSZ
        let year = s[0..2].parse::<i32>().ok()?;
        let year = if year >= 50 { 1900 + year } else { 2000 + year };
        let month = s[2..4].parse::<u32>().ok()?;
        let day = s[4..6].parse::<u32>().ok()?;
        let hour = s[6..8].parse::<u32>().ok()?;
        let min = s[8..10].parse::<u32>().ok()?;
        let sec = s[10..12].parse::<u32>().ok()?;
        
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|d| d.and_hms_opt(hour, min, sec))
            .map(|dt| dt.and_utc().timestamp_millis())
    }

    fn parse_generalized_time(bytes: &[u8]) -> Option<i64> {
        if bytes.len() < 15 {
            return None;
        }
        
        let s = std::str::from_utf8(bytes).ok()?;
        // Format: YYYYMMDDHHMMSSZ
        let year = s[0..4].parse::<i32>().ok()?;
        let month = s[4..6].parse::<u32>().ok()?;
        let day = s[6..8].parse::<u32>().ok()?;
        let hour = s[8..10].parse::<u32>().ok()?;
        let min = s[10..12].parse::<u32>().ok()?;
        let sec = s[12..14].parse::<u32>().ok()?;
        
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|d| d.and_hms_opt(hour, min, sec))
            .map(|dt| dt.and_utc().timestamp_millis())
    }

    fn error_result(config: &HealthCheckConfig, target: &str, error_type: &str, message: &str) -> HealthCheckResult {
        HealthCheckResult {
            check_id: config.id.clone(),
            check_type: "ssl".to_string(),
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
