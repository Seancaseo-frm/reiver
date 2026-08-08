use crate::error::{ReiverError, Result};
use serde::{Deserialize, Serialize};

/// Maximum batch size accepted by Watch.
pub const MAX_BATCH_SIZE: usize = 100;

/// Transport layer for sending events to Reiver API
pub struct Transport {
    client: reqwest::Client,
    api_url: String,
    api_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub api_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub level: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception: Option<ExceptionPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<serde_json::Value>,
    // Trace correlation (extracted automatically from OpenTelemetry context)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    // Service metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_url: Option<String>,
    // Infrastructure metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    // Request context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExceptionPayload {
    #[serde(rename = "type")]
    pub exception_type: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stacktrace: Option<Vec<StackFramePayload>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StackFramePayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineno: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub colno: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Whether this frame is from application code (true) or library code (false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_app: Option<bool>,
}

impl Transport {
    pub fn new(api_url: String, api_key: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .user_agent(format!("reiver-rust-sdk/{}", env!("CARGO_PKG_VERSION")))
                .build()
                .expect("Failed to create HTTP client"),
            api_url,
            api_key,
        }
    }

    pub async fn send(&self, payload: ErrorPayload) -> Result<()> {
        let url = format!(
            "{}/api/watch/ingest/exceptions",
            self.api_url.trim_end_matches('/')
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&payload)
            .send()
            .await
            .map_err(ReiverError::Http)?;

        Self::check_response(response).await
    }

    pub async fn send_batch(&self, payloads: Vec<ErrorPayload>) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        // Watch rejects batches larger than MAX_BATCH_SIZE.
        // Split into chunks and send each one.
        for chunk in payloads.chunks(MAX_BATCH_SIZE) {
            let url = format!(
                "{}/api/watch/ingest/exceptions/batch",
                self.api_url.trim_end_matches('/')
            );

            let response = self
                .client
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.api_key))
                .json(&chunk)
                .send()
                .await
                .map_err(ReiverError::Http)?;

            Self::check_response(response).await?;
        }

        Ok(())
    }

    async fn check_response(response: reqwest::Response) -> Result<()> {
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable body>".to_string());

        match status.as_u16() {
            401 | 403 => {
                tracing::error!(
                    status = %status,
                    "Reiver: authentication failed, check your project key"
                );
                Err(ReiverError::Auth(body))
            }
            429 => {
                tracing::warn!("Reiver: rate limited by server");
                Err(ReiverError::RateLimited)
            }
            _ => {
                tracing::warn!(
                    status = %status,
                    body = %body,
                    "Reiver: server returned non-success status"
                );
                Err(ReiverError::Server {
                    status: status.as_u16(),
                    body,
                })
            }
        }
    }
}
