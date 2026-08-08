//! Google Compute Engine integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Compute Engine metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - CPU Utilization
//! - Memory Utilization
//! - Disk Read/Write Bytes
//! - Disk Read/Write Operations
//! - Network Received/Sent Bytes
//! - Instance Uptime

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google Compute Engine instance identifier (full resource name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GceInstanceId(pub String);

/// Google Compute Engine metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct GceInstanceMetrics {
    pub instance_id: String,
    pub instance_name: String,
    pub zone: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub memory_utilization: Option<f64>,
    pub disk_read_bytes: Option<f64>,
    pub disk_write_bytes: Option<f64>,
    pub disk_read_ops: Option<f64>,
    pub disk_write_ops: Option<f64>,
    pub network_received_bytes: Option<f64>,
    pub network_sent_bytes: Option<f64>,
    pub instance_uptime: Option<f64>,
}

/// Cloud Monitoring API response structures
#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct TimeSeriesResponse {
    timeSeries: Vec<TimeSeries>,
    #[serde(default)]
    nextPageToken: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct TimeSeries {
    metric: Metric,
    resource: Resource,
    points: Vec<Point>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Metric {
    #[serde(rename = "type")]
    metric_type: String,
    labels: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Resource {
    #[serde(rename = "type")]
    resource_type: String,
    labels: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Point {
    interval: Interval,
    value: Value,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Interval {
    endTime: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Value {
    doubleValue: Option<f64>,
    int64Value: Option<String>,
}

/// Google Compute Engine metrics collector
pub struct GceCollector {
    config: GcpConfig,
    http_client: Client,
}

impl GceCollector {
    /// Create a new GCE collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Compute Engine instances in the project
    pub async fn list_instances(&self) -> Result<Vec<GceInstanceId>> {
        info!("Listing Google Compute Engine instances...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all instances across all zones
        let url = format!(
            "https://compute.googleapis.com/compute/v1/projects/{}/aggregated/instances",
            self.config.project_id
        );
        
        let mut all_instances = Vec::new();
        let mut next_page_token: Option<String> = None;
        
        loop {
            let mut request_url = url.clone();
            if let Some(token) = &next_page_token {
                request_url = format!("{}?pageToken={}", url, token);
            }
            
            let response = self.http_client
                .get(&request_url)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list GCE instances: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("GCP API error ({}): {}", status, body));
            }
            
            let instances_response: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse GCP API response: {}", e))?;
            
            // Extract instance resource names
            if let Some(items) = instances_response.get("items").and_then(|v| v.as_object()) {
                for (_zone, zone_data) in items {
                    if let Some(instances) = zone_data.get("instances").and_then(|v| v.as_array()) {
                        for instance in instances {
                            if let Some(self_link) = instance.get("selfLink").and_then(|v| v.as_str()) {
                                // Convert selfLink to full resource name for Cloud Monitoring
                                // Format: projects/{project}/zones/{zone}/instances/{name}
                                all_instances.push(GceInstanceId(self_link.to_string()));
                            }
                        }
                    }
                }
            }
            
            // Check for next page
            next_page_token = instances_response.get("nextPageToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            
            if next_page_token.is_none() {
                break;
            }
        }
        
        info!("Found {} Google Compute Engine instances", all_instances.len());
        Ok(all_instances)
    }

    /// Extract instance name from GCE selfLink
    fn extract_instance_name(self_link: &str) -> String {
        self_link.split('/').last().unwrap_or(self_link).to_string()
    }

    /// Extract zone from GCE selfLink
    fn extract_zone(self_link: &str) -> String {
        let parts: Vec<&str> = self_link.split('/').collect();
        for (i, part) in parts.iter().enumerate() {
            if part == &"zones" && i + 1 < parts.len() {
                return parts[i + 1].to_string();
            }
        }
        "unknown".to_string()
    }

    /// Collect Cloud Monitoring metrics for a specific GCE instance
    pub async fn collect_metrics(
        &self,
        instance_id: &GceInstanceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<GceInstanceMetrics> {
        info!("Collecting GCE metrics for: {}", instance_id.0);
        
        let instance_name = Self::extract_instance_name(&instance_id.0);
        let zone = Self::extract_zone(&instance_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Convert selfLink to instance_id for Cloud Monitoring
        // selfLink: https://www.googleapis.com/compute/v1/projects/{project}/zones/{zone}/instances/{name}
        // We need to extract the numeric instance_id from Compute Engine API or use instance_name
        // For now, use instance_name as a filter (Cloud Monitoring uses instance_name, not numeric ID in filters)
        
        // Build filter for this specific instance using instance_name
        // Cloud Monitoring resource labels: project_id, zone, instance_id (numeric), instance_name
        let filter = format!(
            "resource.type = \"gce_instance\" AND resource.labels.instance_name = \"{}\" AND resource.labels.zone = \"{}\"",
            instance_name, zone
        );
        
        // Metrics to collect
        let metrics = vec![
            "compute.googleapis.com/instance/cpu/utilization",
            "compute.googleapis.com/instance/memory/utilization",
            "compute.googleapis.com/instance/disk/read_bytes_count",
            "compute.googleapis.com/instance/disk/write_bytes_count",
            "compute.googleapis.com/instance/disk/read_ops_count",
            "compute.googleapis.com/instance/disk/write_ops_count",
            "compute.googleapis.com/instance/network/received_bytes_count",
            "compute.googleapis.com/instance/network/sent_bytes_count",
            "compute.googleapis.com/instance/uptime",
        ];
        
        // Collect all metrics in a single request
        let metrics_str = metrics.iter().map(|m| format!("metric.type = \"{}\"", m)).collect::<Vec<_>>().join(" OR ");
        let full_filter = format!("{} AND ({})", filter, metrics_str);
        
        // Format times for Cloud Monitoring API (RFC3339)
        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();
        
        let request_body = serde_json::json!({
            "filter": full_filter,
            "interval": {
                "startTime": start_time_rfc3339,
                "endTime": end_time_rfc3339,
            },
            "aggregation": {
                "alignmentPeriod": "60s",
                "perSeriesAligner": "ALIGN_MEAN",
                "crossSeriesReducer": "REDUCE_MEAN",
            },
        });
        
        let response = self.http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Cloud Monitoring metrics: {}", e))?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Cloud Monitoring API error ({}): {}", status, body));
        }
        
        let metrics_response: TimeSeriesResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse Cloud Monitoring metrics response: {}", e))?;
        
        // Parse metrics from response
        let mut instance_metrics = GceInstanceMetrics {
            instance_id: instance_id.0.clone(),
            instance_name: instance_name.clone(),
            zone: zone.clone(),
            timestamp: end_time,
            cpu_utilization: None,
            memory_utilization: None,
            disk_read_bytes: None,
            disk_write_bytes: None,
            disk_read_ops: None,
            disk_write_ops: None,
            network_received_bytes: None,
            network_sent_bytes: None,
            instance_uptime: None,
        };
        
        for time_series in metrics_response.timeSeries {
            let metric_type = &time_series.metric.metric_type;
            
            // Get the latest data point
            let latest_value = time_series.points
                .iter()
                .filter_map(|point| {
                    point.value.doubleValue
                        .or_else(|| {
                            point.value.int64Value
                                .as_ref()
                                .and_then(|v| v.parse::<f64>().ok())
                        })
                })
                .last();
            
            match metric_type.as_str() {
                "compute.googleapis.com/instance/cpu/utilization" => {
                    instance_metrics.cpu_utilization = latest_value;
                }
                "compute.googleapis.com/instance/memory/utilization" => {
                    instance_metrics.memory_utilization = latest_value;
                }
                "compute.googleapis.com/instance/disk/read_bytes_count" => {
                    instance_metrics.disk_read_bytes = latest_value;
                }
                "compute.googleapis.com/instance/disk/write_bytes_count" => {
                    instance_metrics.disk_write_bytes = latest_value;
                }
                "compute.googleapis.com/instance/disk/read_ops_count" => {
                    instance_metrics.disk_read_ops = latest_value;
                }
                "compute.googleapis.com/instance/disk/write_ops_count" => {
                    instance_metrics.disk_write_ops = latest_value;
                }
                "compute.googleapis.com/instance/network/received_bytes_count" => {
                    instance_metrics.network_received_bytes = latest_value;
                }
                "compute.googleapis.com/instance/network/sent_bytes_count" => {
                    instance_metrics.network_sent_bytes = latest_value;
                }
                "compute.googleapis.com/instance/uptime" => {
                    instance_metrics.instance_uptime = latest_value;
                }
                _ => {
                    warn!("Unknown metric type: {}", metric_type);
                }
            }
        }
        
        Ok(instance_metrics)
    }

    /// Collect metrics for multiple GCE instances in parallel
    pub async fn collect_metrics_batch(
        &self,
        instances: &[GceInstanceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<GceInstanceMetrics>> {
        let mut tasks = Vec::new();
        for instance_id in instances {
            let collector = self.clone();
            let instance_id_clone = instance_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&instance_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for GCE instance: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for GceCollector {
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

/// Convert GCE metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn gce_metrics_to_reiver_format(
    metrics: &GceInstanceMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("instance_id:{}", metrics.instance_id),
        format!("instance_name:{}", metrics.instance_name),
        format!("zone:{}", metrics.zone),
        "source:gcp_compute_engine".to_string(),
    ];

    if let Some(value) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.compute_engine.cpu_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.compute_engine.memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_read_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.compute_engine.disk_read_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_write_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.compute_engine.disk_write_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_read_ops {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.compute_engine.disk_read_ops".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_write_ops {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.compute_engine.disk_write_ops".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_received_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.compute_engine.network_received_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_sent_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.compute_engine.network_sent_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.instance_uptime {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.compute_engine.instance_uptime".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
