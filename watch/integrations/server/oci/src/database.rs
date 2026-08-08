//! OCI Database integration for collecting OCI Monitoring metrics
//!
//! This module provides functionality to collect OCI Database (Autonomous Database) metrics from OCI Monitoring API.
//! Metrics collected include:
//! - CPU Utilization
//! - Storage Utilization
//! - Memory Utilization
//! - Active Sessions
//! - Wait Time
//! - Transactions Per Second
//! - I/O Requests
//! - Network Throughput

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use url::Url;

use crate::config::OciConfig;

/// OCI Database identifier (Autonomous Database OCID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciDatabaseId(pub String);

/// OCI Database metrics collected from OCI Monitoring API
#[derive(Debug, Clone, Serialize)]
pub struct OciDatabaseMetrics {
    pub database_id: String,
    pub database_name: String,
    pub compartment_id: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub storage_utilization: Option<f64>,
    pub memory_utilization: Option<f64>,
    pub active_sessions: Option<f64>,
    pub wait_time: Option<f64>,
    pub transactions_per_second: Option<f64>,
    pub io_requests: Option<f64>,
    pub network_bytes_received: Option<f64>,
    pub network_bytes_sent: Option<f64>,
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

/// OCI Database metrics collector
pub struct OciDatabaseCollector {
    config: OciConfig,
    http_client: Client,
}

impl OciDatabaseCollector {
    /// Create a new OCI Database collector with the given OCI configuration
    pub fn new(config: OciConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// Collect OCI Monitoring metrics for Autonomous Databases
    pub async fn collect_all_metrics(
        &self,
        compartment_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<OciDatabaseMetrics>> {
        info!("Collecting OCI Database metrics for compartment: {}", compartment_id);

        // Build OCI Monitoring API URL
        let base_url = self.config.monitoring_api_url();
        let url = format!("{}/v1/metricData/{}/actions/summarizeMetricsData", base_url, compartment_id);

        // Format times for OCI API (RFC3339)
        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();

        // Build query for Database metrics
        // OCI Monitoring uses namespace oci_autonomous_database for Autonomous Database metrics
        let query_text = "CpuUtilization[1m].mean()";

        let request_body = serde_json::json!({
            "namespaceName": "oci_autonomous_database",
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

        // Group metrics by database
        let mut database_metrics: std::collections::HashMap<String, OciDatabaseMetrics> = std::collections::HashMap::new();

        for metric_data in metrics_response.data {
            // Extract database ID and name from dimensions
            let database_id = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("autonomousDatabaseId"))
                .and_then(|v| v.as_str())
                .or_else(|| metric_data.dimensions.as_ref().and_then(|d| d.get("resourceId")).and_then(|v| v.as_str()))
                .unwrap_or("unknown")
                .to_string();

            let database_name = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("autonomousDatabaseName"))
                .and_then(|v| v.as_str())
                .or_else(|| metric_data.dimensions.as_ref().and_then(|d| d.get("resourceName")).and_then(|v| v.as_str()))
                .unwrap_or("unknown")
                .to_string();

            let key = format!("{}:{}", compartment_id, database_id);

            // Get or create metrics for this database
            let metrics = database_metrics.entry(key.clone()).or_insert_with(|| OciDatabaseMetrics {
                database_id: database_id.clone(),
                database_name: database_name.clone(),
                compartment_id: compartment_id.to_string(),
                timestamp: end_time,
                cpu_utilization: None,
                storage_utilization: None,
                memory_utilization: None,
                active_sessions: None,
                wait_time: None,
                transactions_per_second: None,
                io_requests: None,
                network_bytes_received: None,
                network_bytes_sent: None,
            });

            // Get the latest data point
            let latest_value = metric_data.datapoints
                .iter()
                .max_by_key(|dp| &dp.timestamp)
                .map(|dp| dp.value);

            // Map metric names to fields
            match metric_data.name.as_str() {
                "CpuUtilization" | "CpuUsage" => {
                    metrics.cpu_utilization = latest_value;
                }
                "StorageUtilization" | "StorageUsed" => {
                    metrics.storage_utilization = latest_value;
                }
                "MemoryUtilization" | "MemoryUsage" => {
                    metrics.memory_utilization = latest_value;
                }
                "ActiveSessions" => {
                    metrics.active_sessions = latest_value;
                }
                "WaitTime" | "AverageWaitTime" => {
                    metrics.wait_time = latest_value;
                }
                "TransactionsPerSecond" | "TPS" => {
                    metrics.transactions_per_second = latest_value;
                }
                "IoRequests" | "IoRequestsPerSecond" => {
                    metrics.io_requests = latest_value;
                }
                "NetworkBytesReceived" | "NetworkInBytes" => {
                    metrics.network_bytes_received = latest_value;
                }
                "NetworkBytesSent" | "NetworkOutBytes" => {
                    metrics.network_bytes_sent = latest_value;
                }
                _ => {
                    warn!("Unknown OCI Database metric name: {}", metric_data.name);
                }
            }
        }

        info!("Collected {} OCI Database metric sets", database_metrics.len());
        Ok(database_metrics.into_values().collect())
    }
}

impl Clone for OciDatabaseCollector {
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

/// Convert OCI Database metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn oci_database_metrics_to_reiver_format(
    metrics: &OciDatabaseMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("database_id:{}", metrics.database_id),
        format!("database_name:{}", metrics.database_name),
        format!("compartment_id:{}", metrics.compartment_id),
        "source:oci_database".to_string(),
    ];

    if let Some(value) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "oci.database.cpu_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.storage_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "oci.database.storage_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "oci.database.memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.active_sessions {
        reiver_metrics.push(ReiverMetric {
            name: "oci.database.active_sessions".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.wait_time {
        reiver_metrics.push(ReiverMetric {
            name: "oci.database.wait_time".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.transactions_per_second {
        reiver_metrics.push(ReiverMetric {
            name: "oci.database.transactions_per_second".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.io_requests {
        reiver_metrics.push(ReiverMetric {
            name: "oci.database.io_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_bytes_received {
        reiver_metrics.push(ReiverMetric {
            name: "oci.database.network_bytes_received".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_bytes_sent {
        reiver_metrics.push(ReiverMetric {
            name: "oci.database.network_bytes_sent".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
