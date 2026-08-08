//! OCI Object Storage integration for collecting OCI Monitoring metrics
//!
//! This module provides functionality to collect OCI Object Storage metrics from OCI Monitoring API.
//! Metrics collected include:
//! - Object Count
//! - Total Object Storage Size
//! - Read/Write Requests
//! - Read/Write Bytes
//! - Errors

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use url::Url;

use crate::config::OciConfig;

/// OCI Object Storage bucket identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciBucketId(pub String);

/// OCI Object Storage metrics collected from OCI Monitoring API
#[derive(Debug, Clone, Serialize)]
pub struct OciObjectStorageMetrics {
    pub bucket_name: String,
    pub namespace: String,
    pub compartment_id: String,
    pub timestamp: DateTime<Utc>,
    pub object_count: Option<f64>,
    pub total_storage_bytes: Option<f64>,
    pub read_requests: Option<f64>,
    pub write_requests: Option<f64>,
    pub read_bytes: Option<f64>,
    pub write_bytes: Option<f64>,
    pub errors: Option<f64>,
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

/// OCI Object Storage metrics collector
pub struct OciObjectStorageCollector {
    config: OciConfig,
    http_client: Client,
}

impl OciObjectStorageCollector {
    /// Create a new OCI Object Storage collector with the given OCI configuration
    pub fn new(config: OciConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// Collect OCI Monitoring metrics for Object Storage buckets
    pub async fn collect_all_metrics(
        &self,
        compartment_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<OciObjectStorageMetrics>> {
        info!("Collecting OCI Object Storage metrics for compartment: {}", compartment_id);

        // Build OCI Monitoring API URL
        let base_url = self.config.monitoring_api_url();
        let url = format!("{}/v1/metricData/{}/actions/summarizeMetricsData", base_url, compartment_id);

        // Format times for OCI API (RFC3339)
        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();

        // Build query for Object Storage metrics
        // OCI Monitoring uses namespace oci_objectstorage for Object Storage metrics
        let query_text = "ObjectCount[1m].sum()";

        let request_body = serde_json::json!({
            "namespaceName": "oci_objectstorage",
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

        // Group metrics by bucket
        let mut bucket_metrics: std::collections::HashMap<String, OciObjectStorageMetrics> = std::collections::HashMap::new();

        for metric_data in metrics_response.data {
            // Extract bucket name and namespace from dimensions
            let bucket_name = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("bucketName"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let namespace = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("namespace"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let key = format!("{}:{}", namespace, bucket_name);

            // Get or create metrics for this bucket
            let metrics = bucket_metrics.entry(key.clone()).or_insert_with(|| OciObjectStorageMetrics {
                bucket_name: bucket_name.clone(),
                namespace: namespace.clone(),
                compartment_id: compartment_id.to_string(),
                timestamp: end_time,
                object_count: None,
                total_storage_bytes: None,
                read_requests: None,
                write_requests: None,
                read_bytes: None,
                write_bytes: None,
                errors: None,
            });

            // Get the latest data point
            let latest_value = metric_data.datapoints
                .iter()
                .max_by_key(|dp| &dp.timestamp)
                .map(|dp| dp.value);

            // Map metric names to fields
            match metric_data.name.as_str() {
                "ObjectCount" => {
                    metrics.object_count = latest_value;
                }
                "TotalObjectStorageSize" | "TotalStorageBytes" => {
                    metrics.total_storage_bytes = latest_value;
                }
                "ReadRequests" => {
                    metrics.read_requests = latest_value;
                }
                "WriteRequests" => {
                    metrics.write_requests = latest_value;
                }
                "ReadBytes" => {
                    metrics.read_bytes = latest_value;
                }
                "WriteBytes" => {
                    metrics.write_bytes = latest_value;
                }
                "Errors" | "RequestErrors" => {
                    metrics.errors = latest_value;
                }
                _ => {
                    warn!("Unknown OCI Object Storage metric name: {}", metric_data.name);
                }
            }
        }

        info!("Collected {} OCI Object Storage metric sets", bucket_metrics.len());
        Ok(bucket_metrics.into_values().collect())
    }
}

impl Clone for OciObjectStorageCollector {
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

/// Convert OCI Object Storage metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn oci_object_storage_metrics_to_reiver_format(
    metrics: &OciObjectStorageMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("bucket_name:{}", metrics.bucket_name),
        format!("namespace:{}", metrics.namespace),
        format!("compartment_id:{}", metrics.compartment_id),
        "source:oci_object_storage".to_string(),
    ];

    if let Some(value) = metrics.object_count {
        reiver_metrics.push(ReiverMetric {
            name: "oci.object_storage.object_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.total_storage_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "oci.object_storage.total_storage_bytes".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.read_requests {
        reiver_metrics.push(ReiverMetric {
            name: "oci.object_storage.read_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.write_requests {
        reiver_metrics.push(ReiverMetric {
            name: "oci.object_storage.write_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.read_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "oci.object_storage.read_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.write_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "oci.object_storage.write_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: "oci.object_storage.errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
