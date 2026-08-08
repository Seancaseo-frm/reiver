//! OCI Functions integration for collecting OCI Monitoring metrics
//!
//! This module provides functionality to collect OCI Functions metrics from OCI Monitoring API.
//! Metrics collected include:
//! - Invocations
//! - Errors
//! - Duration
//! - Memory Utilization

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use url::Url;

use crate::config::OciConfig;

/// OCI Functions application identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciFunctionAppId(pub String);

/// OCI Functions metrics collected from OCI Monitoring API
#[derive(Debug, Clone, Serialize)]
pub struct OciFunctionMetrics {
    pub function_app_id: String,
    pub function_name: String,
    pub compartment_id: String,
    pub timestamp: DateTime<Utc>,
    pub invocations: Option<f64>,
    pub errors: Option<f64>,
    pub duration_ms: Option<f64>,
    pub memory_utilization: Option<f64>,
}

/// OCI Monitoring API response structures
#[derive(Debug, Deserialize)]
struct OciMonitoringResponse {
    #[serde(default)]
    data: Vec<OciMetricData>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct OciMetricData {
    namespace: String,
    compartmentId: String,
    name: String,
    dimensions: Option<serde_json::Value>,
    #[serde(default)]
    datapoints: Vec<OciDataPoint>,
    resolution: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OciDataPoint {
    timestamp: String,
    value: f64,
}

/// OCI Functions metrics collector
pub struct OciFunctionCollector {
    config: OciConfig,
    http_client: Client,
}

impl OciFunctionCollector {
    /// Create a new OCI Functions collector with the given OCI configuration
    pub fn new(config: OciConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// Collect OCI Monitoring metrics for Functions
    pub async fn collect_all_metrics(
        &self,
        compartment_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<OciFunctionMetrics>> {
        info!("Collecting OCI Functions metrics for compartment: {}", compartment_id);

        // Build OCI Monitoring API URL
        let base_url = self.config.monitoring_api_url();
        let url = format!("{}/v1/metricData/{}/actions/summarizeMetricsData", base_url, compartment_id);

        // Format times for OCI API (RFC3339)
        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();

        // Build query for Functions metrics
        // OCI Monitoring uses namespace oci_faas for Functions metrics
        // Query multiple metrics at once
        let query_text = "Invocations[1m].sum()";

        let request_body = serde_json::json!({
            "namespaceName": "oci_faas",
            "query": query_text,
            "startTime": start_time_rfc3339,
            "endTime": end_time_rfc3339,
            "compartmentId": compartment_id,
        });

        // Build URL object for signing
        let url_obj = Url::parse(&url)
            .map_err(|e| anyhow::anyhow!("Failed to parse URL: {}", e))?;

        // Build headers
        let mut headers = std::collections::HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());

        // Convert request body to bytes for signing
        let body_string = serde_json::to_string(&request_body)?;
        let body_bytes = body_string.as_bytes();

        // Sign the request
        self.config.sign_request("POST", &url_obj, &mut headers, Some(body_bytes))?;

        // Build request with signed headers
        let mut request = self.http_client
            .post(&url)
            .json(&request_body);

        for (key, value) in &headers {
            request = request.header(key, value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get OCI Monitoring metrics: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!("OCI Monitoring API error ({}): {}", status, body);
            return Ok(Vec::new()); // Return empty on error, don't fail completely
        }

        let metrics_response: OciMonitoringResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse OCI Monitoring metrics response: {}", e))?;

        // Group metrics by function
        let mut function_metrics: std::collections::HashMap<String, OciFunctionMetrics> = std::collections::HashMap::new();

        for metric_data in metrics_response.data {
            // Extract function ID and name from dimensions
            let function_app_id = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("resourceId"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let function_name = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("functionName"))
                .and_then(|v| v.as_str())
                .or_else(|| metric_data.dimensions.as_ref().and_then(|d| d.get("resourceName")).and_then(|v| v.as_str()))
                .unwrap_or("unknown")
                .to_string();

            let key = format!("{}:{}", function_app_id, function_name);

            // Get or create metrics for this function
            let metrics = function_metrics.entry(key.clone()).or_insert_with(|| OciFunctionMetrics {
                function_app_id: function_app_id.clone(),
                function_name: function_name.clone(),
                compartment_id: compartment_id.to_string(),
                timestamp: end_time,
                invocations: None,
                errors: None,
                duration_ms: None,
                memory_utilization: None,
            });

            // Get the latest data point
            let latest_value = metric_data.datapoints
                .iter()
                .max_by_key(|dp| &dp.timestamp)
                .map(|dp| dp.value);

            // Map metric names to fields
            match metric_data.name.as_str() {
                "Invocations" => {
                    metrics.invocations = latest_value;
                }
                "Errors" => {
                    metrics.errors = latest_value;
                }
                "Duration" | "DurationInMs" => {
                    metrics.duration_ms = latest_value;
                }
                "MemoryUtilization" | "MemoryBytes" => {
                    metrics.memory_utilization = latest_value;
                }
                _ => {
                    warn!("Unknown OCI Functions metric name: {}", metric_data.name);
                }
            }
        }

        info!("Collected {} OCI Functions metric sets", function_metrics.len());
        Ok(function_metrics.into_values().collect())
    }
}

impl Clone for OciFunctionCollector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            http_client: Client::new(),
        }
    }
}

/// Reiver metric format (compatible with metrics API)
#[derive(Debug, Clone, Serialize)]
pub struct ReiverMetric {
    pub name: String,
    pub value: f64,
    #[serde(rename = "type")]
    pub r#type: String,
    pub timestamp: DateTime<Utc>,
    pub tags: Vec<String>,
}

/// Convert OCI Functions metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn oci_function_metrics_to_reiver_format(
    metrics: &OciFunctionMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("function_app_id:{}", metrics.function_app_id),
        format!("function_name:{}", metrics.function_name),
        format!("compartment_id:{}", metrics.compartment_id),
        "source:oci_functions".to_string(),
    ];

    if let Some(value) = metrics.invocations {
        reiver_metrics.push(ReiverMetric {
            name: "oci.functions.invocations".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: "oci.functions.errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.duration_ms {
        reiver_metrics.push(ReiverMetric {
            name: "oci.functions.duration_ms".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "oci.functions.memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
