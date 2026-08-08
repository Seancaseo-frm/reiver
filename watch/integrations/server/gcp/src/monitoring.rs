//! Google Cloud Monitoring integration for collecting generic metrics
//!
//! This module provides functionality to collect generic metrics from Cloud Monitoring API.
//! This integration allows querying any Cloud Monitoring metrics using filters.
//! Metrics collected include any metrics available via Cloud Monitoring API.

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::config::GcpConfig;

/// Google Cloud Monitoring metric identifier (metric filter)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringFilterId(pub String);

/// Google Cloud Monitoring metrics collected from Cloud Monitoring API
#[derive(Debug, Clone, Serialize)]
pub struct MonitoringMetrics {
    pub filter_id: String,
    pub metric_type: String,
    pub resource_type: String,
    pub resource_labels: Option<serde_json::Value>,
    pub metric_labels: Option<serde_json::Value>,
    pub timestamp: DateTime<Utc>,
    pub value: Option<f64>,
    pub count: Option<f64>,
    pub mean: Option<f64>,
    pub sum: Option<f64>,
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
    distributionValue: Option<serde_json::Value>,
}

/// Google Cloud Monitoring metrics collector
pub struct MonitoringCollector {
    config: GcpConfig,
    http_client: Client,
}

impl MonitoringCollector {
    /// Create a new Cloud Monitoring collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// Collect Cloud Monitoring metrics using a filter
    /// 
    /// The filter should follow Cloud Monitoring API filter syntax:
    /// - resource.type = "gce_instance"
    /// - metric.type = "compute.googleapis.com/instance/cpu/utilization"
    /// - resource.labels.instance_id = "instance-1"
    /// 
    /// Example filter: 'resource.type = "gce_instance" AND metric.type = "compute.googleapis.com/instance/cpu/utilization"'
    pub async fn collect_metrics(
        &self,
        filter: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<MonitoringMetrics>> {
        info!("Collecting Cloud Monitoring metrics with filter: {}", filter);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Format times for Cloud Monitoring API (RFC3339)
        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();
        
        let request_body = serde_json::json!({
            "filter": filter,
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
        
        let mut all_metrics = Vec::new();
        
        for time_series in metrics_response.timeSeries {
            let metric_type = time_series.metric.metric_type.clone();
            let resource_type = time_series.resource.resource_type.clone();
            let resource_labels = time_series.resource.labels.clone();
            let metric_labels = time_series.metric.labels.clone();
            
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
            
            // Create a unique identifier for this metric series
            let filter_id = format!("{}_{}", metric_type, resource_type);
            
            all_metrics.push(MonitoringMetrics {
                filter_id: filter_id.clone(),
                metric_type: metric_type.clone(),
                resource_type: resource_type.clone(),
                resource_labels: resource_labels.clone(),
                metric_labels: metric_labels.clone(),
                timestamp: end_time,
                value: latest_value,
                count: latest_value, // For simplicity, use value as count
                mean: latest_value,  // For simplicity, use value as mean
                sum: latest_value,   // For simplicity, use value as sum
            });
        }
        
        info!("Collected {} Cloud Monitoring metric series", all_metrics.len());
        Ok(all_metrics)
    }

    /// Collect metrics for multiple filters in parallel
    pub async fn collect_metrics_batch(
        &self,
        filters: &[String],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<MonitoringMetrics>> {
        let mut tasks = Vec::new();
        for filter in filters {
            let collector = self.clone();
            let filter_clone = filter.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&filter_clone, start_time, end_time).await
            }));
        }
        
        let mut all_metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metrics) => all_metrics.extend(metrics),
                Err(e) => {
                    error!("Failed to collect Cloud Monitoring metrics: {}", e);
                }
            }
        }
        
        Ok(all_metrics)
    }
}

impl Clone for MonitoringCollector {
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

/// Convert Cloud Monitoring metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn monitoring_metrics_to_reiver_format(
    metrics: &MonitoringMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    
    // Build tags from resource and metric labels
    let mut tags = Vec::new();
    tags.push(format!("metric_type:{}", metrics.metric_type));
    tags.push(format!("resource_type:{}", metrics.resource_type));
    tags.push("source:gcp_monitoring".to_string());
    
    // Add resource labels to tags
    if let Some(resource_labels) = &metrics.resource_labels {
        if let Some(obj) = resource_labels.as_object() {
            for (key, value) in obj {
                if let Some(val_str) = value.as_str() {
                    tags.push(format!("{}:{}", key, val_str));
                }
            }
        }
    }
    
    // Add metric labels to tags
    if let Some(metric_labels) = &metrics.metric_labels {
        if let Some(obj) = metric_labels.as_object() {
            for (key, value) in obj {
                if let Some(val_str) = value.as_str() {
                    tags.push(format!("{}:{}", key, val_str));
                }
            }
        }
    }
    
    // Convert metric type to a valid metric name
    let metric_name = metrics.metric_type
        .replace("/", ".")
        .replace(":", "_")
        .trim_start_matches(".")
        .to_string();
    
    if let Some(value) = metrics.value {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.monitoring.{}", metric_name),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }
    
    reiver_metrics
}
