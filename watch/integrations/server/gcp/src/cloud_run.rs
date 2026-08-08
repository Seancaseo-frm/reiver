//! Google Cloud Run integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud Run metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - Request Count
//! - Request Latency
//! - Instance Count
//! - CPU Utilization
//! - Memory Utilization
//! - Container Instance Time
//! - Errors

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google Cloud Run service identifier (service name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudRunServiceId(pub String);

/// Google Cloud Run metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct CloudRunMetrics {
    pub service_id: String,
    pub service_name: String,
    pub region: String,
    pub revision_name: String,
    pub timestamp: DateTime<Utc>,
    pub request_count: Option<f64>,
    pub request_latency: Option<f64>,
    pub instance_count: Option<f64>,
    pub cpu_utilization: Option<f64>,
    pub memory_utilization: Option<f64>,
    pub container_instance_time: Option<f64>,
    pub errors: Option<f64>,
}

/// Cloud Monitoring API response structures (reused from compute module)
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

/// Google Cloud Run metrics collector
pub struct CloudRunCollector {
    config: GcpConfig,
    http_client: Client,
}

impl CloudRunCollector {
    /// Create a new Cloud Run collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Cloud Run services in the project
    pub async fn list_services(&self) -> Result<Vec<CloudRunServiceId>> {
        info!("Listing Google Cloud Run services...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all services across all regions
        let url = format!(
            "https://run.googleapis.com/v1/projects/{}/locations/-/services",
            self.config.project_id
        );
        
        let mut all_services = Vec::new();
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
                .map_err(|e| anyhow::anyhow!("Failed to list Cloud Run services: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("GCP API error ({}): {}", status, body));
            }
            
            let services_response: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse GCP API response: {}", e))?;
            
            // Extract service names
            if let Some(services) = services_response.get("items").and_then(|v| v.as_array()) {
                for service in services {
                    if let Some(_name) = service.get("metadata").and_then(|m| m.get("name")).and_then(|v| v.as_str()) {
                        // Get full resource name for Cloud Monitoring
                        if let Some(self_link) = service.get("metadata").and_then(|m| m.get("selfLink")).and_then(|v| v.as_str()) {
                            all_services.push(CloudRunServiceId(self_link.to_string()));
                        } else if let Some(name_full) = service.get("metadata").and_then(|m| m.get("name")).and_then(|v| v.as_str()) {
                            all_services.push(CloudRunServiceId(name_full.to_string()));
                        }
                    }
                }
            }
            
            // Check for next page
            next_page_token = services_response.get("nextPageToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            
            if next_page_token.is_none() {
                break;
            }
        }
        
        info!("Found {} Google Cloud Run services", all_services.len());
        Ok(all_services)
    }

    /// Extract service name from full resource name
    /// Format: projects/{project}/locations/{region}/services/{service_name}
    fn extract_service_name(service_name: &str) -> String {
        service_name.split('/').last().unwrap_or(service_name).to_string()
    }

    /// Extract region from full resource name
    /// Format: projects/{project}/locations/{region}/services/{service_name}
    fn extract_region(service_name: &str) -> String {
        let parts: Vec<&str> = service_name.split('/').collect();
        for (i, part) in parts.iter().enumerate() {
            if part == &"locations" && i + 1 < parts.len() {
                return parts[i + 1].to_string();
            }
        }
        "unknown".to_string()
    }

    /// Extract revision name from resource labels (will be extracted from Cloud Monitoring)
    fn extract_revision_from_labels(labels: &Option<serde_json::Value>) -> String {
        if let Some(labels) = labels {
            if let Some(revision) = labels.get("revision_name").and_then(|v| v.as_str()) {
                return revision.to_string();
            }
        }
        "unknown".to_string()
    }

    /// Collect Cloud Monitoring metrics for a specific Cloud Run service
    pub async fn collect_metrics(
        &self,
        service_id: &CloudRunServiceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<CloudRunMetrics>> {
        info!("Collecting Cloud Run metrics for: {}", service_id.0);
        
        let service_name = Self::extract_service_name(&service_id.0);
        let region = Self::extract_region(&service_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for this specific service
        // Cloud Run resource format: projects/{project}/locations/{region}/services/{service_name}
        let filter = format!(
            "resource.type = \"cloud_run_revision\" AND resource.labels.service_name = \"{}\" AND resource.labels.location = \"{}\"",
            service_name, region
        );
        
        // Metrics to collect
        let metrics = vec![
            "run.googleapis.com/request_count",
            "run.googleapis.com/request_latencies",
            "run.googleapis.com/container/instance_count",
            "run.googleapis.com/container/cpu/utilizations",
            "run.googleapis.com/container/memory/utilizations",
            "run.googleapis.com/container/instance_up_time",
        ];
        
        // Build filter with metric types
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
        
        // Group metrics by revision (Cloud Run has multiple revisions)
        let mut revision_metrics: std::collections::HashMap<String, CloudRunMetrics> = std::collections::HashMap::new();
        
        // Parse main metrics
        for time_series in metrics_response.timeSeries {
            let metric_type = &time_series.metric.metric_type;
            
            // Extract revision name from resource labels
            let revision_name = Self::extract_revision_from_labels(&time_series.resource.labels);
            let revision_key = revision_name.clone();
            
            // Get or create metrics for this revision
            let metrics = revision_metrics.entry(revision_key).or_insert_with(|| CloudRunMetrics {
                service_id: service_id.0.clone(),
                service_name: service_name.clone(),
                region: region.clone(),
                revision_name: revision_name.clone(),
                timestamp: end_time,
                request_count: None,
                request_latency: None,
                instance_count: None,
                cpu_utilization: None,
                memory_utilization: None,
                container_instance_time: None,
                errors: None,
            });
            
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
                "run.googleapis.com/request_count" => {
                    metrics.request_count = latest_value;
                }
                "run.googleapis.com/request_latencies" => {
                    metrics.request_latency = latest_value;
                }
                "run.googleapis.com/container/instance_count" => {
                    metrics.instance_count = latest_value;
                }
                "run.googleapis.com/container/cpu/utilizations" => {
                    metrics.cpu_utilization = latest_value;
                }
                "run.googleapis.com/container/memory/utilizations" => {
                    metrics.memory_utilization = latest_value;
                }
                "run.googleapis.com/container/instance_up_time" => {
                    metrics.container_instance_time = latest_value;
                }
                _ => {
                    warn!("Unknown metric type: {}", metric_type);
                }
            }
        }
        
        // Separate request for errors
        let errors_filter = format!(
            "{} AND metric.type = \"run.googleapis.com/request_count\" AND metric.labels.response_code_class = \"5xx\"",
            filter
        );
        
        let errors_request_body = serde_json::json!({
            "filter": errors_filter,
            "interval": {
                "startTime": start_time_rfc3339,
                "endTime": end_time_rfc3339,
            },
            "aggregation": {
                "alignmentPeriod": "60s",
                "perSeriesAligner": "ALIGN_RATE",
                "crossSeriesReducer": "REDUCE_SUM",
            },
        });
        
        let errors_response = self.http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .json(&errors_request_body)
            .send()
            .await;
        
        // Parse errors if available
        if let Ok(errors_response) = errors_response {
            if errors_response.status().is_success() {
                if let Ok(errors_metrics) = errors_response.json::<TimeSeriesResponse>().await {
                    for time_series in errors_metrics.timeSeries {
                        let revision_name = Self::extract_revision_from_labels(&time_series.resource.labels);
                        
                        if let Some(metrics) = revision_metrics.get_mut(&revision_name) {
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
                            
                            if let Some(value) = latest_value {
                                metrics.errors = Some(
                                    metrics.errors.unwrap_or(0.0) + value
                                );
                            }
                        }
                    }
                }
            }
        }
        
        Ok(revision_metrics.into_values().collect())
    }

    /// Collect metrics for multiple Cloud Run services in parallel
    pub async fn collect_metrics_batch(
        &self,
        services: &[CloudRunServiceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<CloudRunMetrics>> {
        let mut tasks = Vec::new();
        for service_id in services {
            let collector = self.clone();
            let service_id_clone = service_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&service_id_clone, start_time, end_time).await
            }));
        }

        let mut all_metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metrics) => all_metrics.extend(metrics),
                Err(e) => {
                    error!("Failed to collect metrics for Cloud Run service: {}", e);
                }
            }
        }

        Ok(all_metrics)
    }
}

impl Clone for CloudRunCollector {
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

/// Convert Cloud Run metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn cloud_run_metrics_to_reiver_format(
    metrics: &CloudRunMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("service_id:{}", metrics.service_id),
        format!("service_name:{}", metrics.service_name),
        format!("region:{}", metrics.region),
        format!("revision_name:{}", metrics.revision_name),
        "source:gcp_cloud_run".to_string(),
    ];

    if let Some(value) = metrics.request_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_run.request_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.request_latency {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_run.request_latency".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.instance_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_run.instance_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_run.cpu_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_run.memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.container_instance_time {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_run.container_instance_time".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_run.errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
