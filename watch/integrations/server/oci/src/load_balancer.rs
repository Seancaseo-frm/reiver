//! OCI Load Balancer integration for collecting OCI Monitoring metrics
//!
//! This module provides functionality to collect OCI Load Balancer metrics from OCI Monitoring API.
//! Metrics collected include:
//! - Request Count
//! - Request/Response Bytes
//! - Backend Latency
//! - Total Latency
//! - Backend Healthy/Unhealthy Instances
//! - Errors

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use url::Url;

use crate::config::OciConfig;

/// OCI Load Balancer identifier (load balancer OCID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciLoadBalancerId(pub String);

/// OCI Load Balancer metrics collected from OCI Monitoring API
#[derive(Debug, Clone, Serialize)]
pub struct OciLoadBalancerMetrics {
    pub load_balancer_id: String,
    pub load_balancer_name: String,
    pub compartment_id: String,
    pub timestamp: DateTime<Utc>,
    pub request_count: Option<f64>,
    pub request_bytes: Option<f64>,
    pub response_bytes: Option<f64>,
    pub backend_latency_ms: Option<f64>,
    pub total_latency_ms: Option<f64>,
    pub healthy_backend_count: Option<f64>,
    pub unhealthy_backend_count: Option<f64>,
    pub error_count: Option<f64>,
}

/// OCI Monitoring API response structures
#[derive(Debug, Deserialize)]
struct OciMonitoringResponse {
    #[serde(default)]
    data: Vec<OciMetricData>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OciMetricData {
    #[serde(rename = "namespace")]
    namespace_name: String,
    #[serde(rename = "compartmentId")]
    compartment_id: String,
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

/// OCI Load Balancer metrics collector
pub struct OciLoadBalancerCollector {
    config: OciConfig,
    http_client: Client,
}

impl OciLoadBalancerCollector {
    /// Create a new OCI Load Balancer collector with the given OCI configuration
    pub fn new(config: OciConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// Collect OCI Monitoring metrics for Load Balancers
    pub async fn collect_all_metrics(
        &self,
        compartment_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<OciLoadBalancerMetrics>> {
        info!("Collecting OCI Load Balancer metrics for compartment: {}", compartment_id);

        // Build OCI Monitoring API URL
        let base_url = self.config.monitoring_api_url();
        let url = format!("{}/v1/metricData/{}/actions/summarizeMetricsData", base_url, compartment_id);

        // Format times for OCI API (RFC3339)
        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();

        // Build query for Load Balancer metrics
        // OCI Monitoring uses namespace oci_lbaas for Load Balancer metrics
        let query_text = "RequestCount[1m].sum(), RequestBytes[1m].sum(), ResponseBytes[1m].sum(), BackendLatency[1m].mean(), TotalLatency[1m].mean(), HealthyBackendCount[1m].mean(), UnhealthyBackendCount[1m].mean(), ErrorCount[1m].sum()";

        let request_body = serde_json::json!({
            "namespaceName": "oci_lbaas",
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

        // Group metrics by load balancer
        let mut load_balancer_metrics: std::collections::HashMap<String, OciLoadBalancerMetrics> = std::collections::HashMap::new();

        for metric_data in metrics_response.data {
            // Extract load balancer ID and name from dimensions
            let load_balancer_id = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("resourceId"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let load_balancer_name = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("displayName"))
                .and_then(|v| v.as_str())
                .or_else(|| metric_data.dimensions.as_ref().and_then(|d| d.get("resourceName")).and_then(|v| v.as_str()))
                .unwrap_or("unknown")
                .to_string();

            let key = format!("{}:{}", compartment_id, load_balancer_id);

            // Get or create metrics for this load balancer
            let metrics = load_balancer_metrics.entry(key.clone()).or_insert_with(|| OciLoadBalancerMetrics {
                load_balancer_id: load_balancer_id.clone(),
                load_balancer_name: load_balancer_name.clone(),
                compartment_id: compartment_id.to_string(),
                timestamp: end_time,
                request_count: None,
                request_bytes: None,
                response_bytes: None,
                backend_latency_ms: None,
                total_latency_ms: None,
                healthy_backend_count: None,
                unhealthy_backend_count: None,
                error_count: None,
            });

            // Get the latest data point
            let latest_value = metric_data.datapoints
                .iter()
                .max_by_key(|dp| &dp.timestamp)
                .map(|dp| dp.value);

            // Map metric names to fields
            match metric_data.name.as_str() {
                "RequestCount" => {
                    metrics.request_count = latest_value;
                }
                "RequestBytes" => {
                    metrics.request_bytes = latest_value;
                }
                "ResponseBytes" => {
                    metrics.response_bytes = latest_value;
                }
                "BackendLatency" => {
                    metrics.backend_latency_ms = latest_value;
                }
                "TotalLatency" => {
                    metrics.total_latency_ms = latest_value;
                }
                "HealthyBackendCount" => {
                    metrics.healthy_backend_count = latest_value;
                }
                "UnhealthyBackendCount" => {
                    metrics.unhealthy_backend_count = latest_value;
                }
                "ErrorCount" => {
                    metrics.error_count = latest_value;
                }
                _ => {
                    warn!("Unknown OCI Load Balancer metric name: {}", metric_data.name);
                }
            }
        }

        info!("Collected {} OCI Load Balancer metric sets", load_balancer_metrics.len());
        Ok(load_balancer_metrics.into_values().collect())
    }
}

impl Clone for OciLoadBalancerCollector {
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

/// Convert OCI Load Balancer metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn oci_load_balancer_metrics_to_reiver_format(
    metrics: &OciLoadBalancerMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("load_balancer_id:{}", metrics.load_balancer_id),
        format!("load_balancer_name:{}", metrics.load_balancer_name),
        format!("compartment_id:{}", metrics.compartment_id),
        "source:oci_load_balancer".to_string(),
    ];

    if let Some(value) = metrics.request_count {
        reiver_metrics.push(ReiverMetric {
            name: "oci.load_balancer.request_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.request_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "oci.load_balancer.request_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.response_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "oci.load_balancer.response_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.backend_latency_ms {
        reiver_metrics.push(ReiverMetric {
            name: "oci.load_balancer.backend_latency_ms".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.total_latency_ms {
        reiver_metrics.push(ReiverMetric {
            name: "oci.load_balancer.total_latency_ms".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.healthy_backend_count {
        reiver_metrics.push(ReiverMetric {
            name: "oci.load_balancer.healthy_backend_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.unhealthy_backend_count {
        reiver_metrics.push(ReiverMetric {
            name: "oci.load_balancer.unhealthy_backend_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.error_count {
        reiver_metrics.push(ReiverMetric {
            name: "oci.load_balancer.error_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
