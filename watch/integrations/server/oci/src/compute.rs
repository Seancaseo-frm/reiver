//! OCI Compute integration for collecting OCI Monitoring metrics
//!
//! This module provides functionality to collect OCI Compute metrics from OCI Monitoring API.
//! Metrics collected include:
//! - CPU Utilization
//! - Memory Utilization
//! - Disk Read/Write Bytes
//! - Network In/Out Bytes

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use url::Url;

use crate::config::OciConfig;

/// OCI Compute instance identifier (instance OCID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciInstanceId(pub String);

/// OCI Compute metrics collected from OCI Monitoring API
#[derive(Debug, Clone, Serialize)]
pub struct OciInstanceMetrics {
    pub instance_id: String,
    pub instance_name: String,
    pub compartment_id: String,
    pub availability_domain: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub memory_utilization: Option<f64>,
    pub disk_read_bytes: Option<f64>,
    pub disk_write_bytes: Option<f64>,
    pub network_received_bytes: Option<f64>,
    pub network_sent_bytes: Option<f64>,
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

/// OCI Compute metrics collector
pub struct OciComputeCollector {
    config: OciConfig,
    http_client: Client,
}

impl OciComputeCollector {
    /// Create a new OCI Compute collector with the given OCI configuration
    pub fn new(config: OciConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// Collect OCI Monitoring metrics for Compute instances
    /// 
    /// Note: OCI uses request signing for authentication, which requires:
    /// 1. Building a signature string from request method, path, headers, body
    /// 2. Signing with RSA-SHA256 using the private key
    /// 3. Base64 encoding the signature
    /// 4. Building the Authorization header
    /// 
    /// This is a simplified implementation. For production, use the official OCI SDK
    /// or implement proper request signing.
    pub async fn collect_all_metrics(
        &self,
        compartment_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<OciInstanceMetrics>> {
        info!("Collecting OCI Compute metrics for compartment: {}", compartment_id);
        
        // Build OCI Monitoring API URL
        let base_url = self.config.monitoring_api_url();
        let url = format!("{}/v1/metricData/{}/actions/summarizeMetricsData", base_url, compartment_id);
        
        // Format times for OCI API (RFC3339)
        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();
        
        // Build query for Compute metrics
        // OCI Monitoring uses namespace oci_computeagent for Compute instance metrics
        let query_text = "CpuUtilization[1m].mean()";
        
        let request_body = serde_json::json!({
            "namespaceName": "oci_computeagent",
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
        
        // Group metrics by instance
        let mut instance_metrics: std::collections::HashMap<String, OciInstanceMetrics> = std::collections::HashMap::new();
        
        for metric_data in metrics_response.data {
            // Extract instance ID from dimensions
            let instance_id = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("resourceId"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            
            let instance_name = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("resourceName"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            
            let key = instance_id.clone();
            
            // Get or create metrics for this instance
            let metrics = instance_metrics.entry(key).or_insert_with(|| OciInstanceMetrics {
                instance_id: instance_id.clone(),
                instance_name: instance_name.clone(),
                compartment_id: compartment_id.to_string(),
                availability_domain: "unknown".to_string(), // Extract from dimensions if available
                timestamp: end_time,
                cpu_utilization: None,
                memory_utilization: None,
                disk_read_bytes: None,
                disk_write_bytes: None,
                network_received_bytes: None,
                network_sent_bytes: None,
            });
            
            // Get the latest data point
            let latest_value = metric_data.datapoints
                .iter()
                .max_by_key(|dp| &dp.timestamp)
                .map(|dp| dp.value);
            
            // Map metric names to fields
            match metric_data.name.as_str() {
                "CpuUtilization" => {
                    metrics.cpu_utilization = latest_value;
                }
                "MemoryUtilization" => {
                    metrics.memory_utilization = latest_value;
                }
                "DiskBytesRead" => {
                    metrics.disk_read_bytes = latest_value;
                }
                "DiskBytesWritten" => {
                    metrics.disk_write_bytes = latest_value;
                }
                "NetworkBytesIn" => {
                    metrics.network_received_bytes = latest_value;
                }
                "NetworkBytesOut" => {
                    metrics.network_sent_bytes = latest_value;
                }
                _ => {
                    warn!("Unknown metric name: {}", metric_data.name);
                }
            }
        }
        
        info!("Collected {} OCI Compute metric sets", instance_metrics.len());
        Ok(instance_metrics.into_values().collect())
    }
}

impl Clone for OciComputeCollector {
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

/// Convert OCI Compute metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn oci_compute_metrics_to_reiver_format(
    metrics: &OciInstanceMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("instance_id:{}", metrics.instance_id),
        format!("instance_name:{}", metrics.instance_name),
        format!("compartment_id:{}", metrics.compartment_id),
        format!("availability_domain:{}", metrics.availability_domain),
        "source:oci_compute".to_string(),
    ];

    if let Some(value) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "oci.compute.cpu_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "oci.compute.memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_read_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "oci.compute.disk_read_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_write_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "oci.compute.disk_write_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_received_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "oci.compute.network_received_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_sent_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "oci.compute.network_sent_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
