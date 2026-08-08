//! Google Cloud Storage integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud Storage metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - Bucket Size (storage used)
//! - Object Count
//! - Request Counts (GET, PUT, DELETE, etc.)
//! - Bytes Sent/Received
//! - Error Rates

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google Cloud Storage bucket identifier (bucket name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsBucketId(pub String);

/// Google Cloud Storage metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct GcsBucketMetrics {
    pub bucket_id: String,
    pub bucket_name: String,
    pub location: String,
    pub timestamp: DateTime<Utc>,
    pub bucket_size: Option<f64>,
    pub object_count: Option<f64>,
    pub request_count: Option<f64>,
    pub bytes_sent: Option<f64>,
    pub bytes_received: Option<f64>,
    pub get_requests: Option<f64>,
    pub put_requests: Option<f64>,
    pub delete_requests: Option<f64>,
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

/// Google Cloud Storage metrics collector
pub struct GcsCollector {
    config: GcpConfig,
    http_client: Client,
}

impl GcsCollector {
    /// Create a new GCS collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Cloud Storage buckets in the project
    pub async fn list_buckets(&self) -> Result<Vec<GcsBucketId>> {
        info!("Listing Google Cloud Storage buckets...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all buckets in the project
        let url = format!(
            "https://storage.googleapis.com/storage/v1/b?project={}",
            self.config.project_id
        );
        
        let mut all_buckets = Vec::new();
        let mut next_page_token: Option<String> = None;
        
        loop {
            let mut request_url = url.clone();
            if let Some(token) = &next_page_token {
                request_url = format!("{}&pageToken={}", url, token);
            }
            
            let response = self.http_client
                .get(&request_url)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list GCS buckets: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("GCP API error ({}): {}", status, body));
            }
            
            let buckets_response: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse GCP API response: {}", e))?;
            
            // Extract bucket names
            if let Some(buckets) = buckets_response.get("items").and_then(|v| v.as_array()) {
                for bucket in buckets {
                    if let Some(name) = bucket.get("name").and_then(|v| v.as_str()) {
                        all_buckets.push(GcsBucketId(name.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_page_token = buckets_response.get("nextPageToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            
            if next_page_token.is_none() {
                break;
            }
        }
        
        info!("Found {} Google Cloud Storage buckets", all_buckets.len());
        Ok(all_buckets)
    }

    /// Extract bucket name (already just the name)
    fn extract_bucket_name(bucket_name: &str) -> String {
        bucket_name.to_string()
    }

    /// Extract location from bucket metadata (we'll get it from Cloud Monitoring resource labels)
    fn extract_location_from_labels(labels: &Option<serde_json::Value>) -> String {
        if let Some(labels) = labels {
            if let Some(location) = labels.get("location").and_then(|v| v.as_str()) {
                return location.to_string();
            }
        }
        "unknown".to_string()
    }

    /// Collect Cloud Monitoring metrics for a specific GCS bucket
    pub async fn collect_metrics(
        &self,
        bucket_id: &GcsBucketId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<GcsBucketMetrics> {
        info!("Collecting GCS metrics for: {}", bucket_id.0);
        
        let bucket_name = Self::extract_bucket_name(&bucket_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for this specific bucket
        let filter = format!(
            "resource.type = \"gcs_bucket\" AND resource.labels.bucket_name = \"{}\"",
            bucket_name
        );
        
        // Metrics to collect
        let metrics = vec![
            "storage.googleapis.com/storage/total_bytes",
            "storage.googleapis.com/storage/object_count",
            "storage.googleapis.com/api/request_count",
            "storage.googleapis.com/network/sent_bytes_count",
            "storage.googleapis.com/network/received_bytes_count",
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
        
        // Parse metrics from response
        let mut location = "unknown".to_string();
        let mut bucket_metrics = GcsBucketMetrics {
            bucket_id: bucket_id.0.clone(),
            bucket_name: bucket_name.clone(),
            location: location.clone(),
            timestamp: end_time,
            bucket_size: None,
            object_count: None,
            request_count: None,
            bytes_sent: None,
            bytes_received: None,
            get_requests: None,
            put_requests: None,
            delete_requests: None,
            errors: None,
        };
        
        // Parse main metrics
        for time_series in metrics_response.timeSeries {
            let metric_type = &time_series.metric.metric_type;
            
            // Extract location from resource labels (first time we see it)
            if location == "unknown" {
                location = Self::extract_location_from_labels(&time_series.resource.labels);
                bucket_metrics.location = location.clone();
            }
            
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
            
            // Check metric labels for request method
            let method_label = time_series.metric.labels
                .as_ref()
                .and_then(|l| l.get("method"))
                .and_then(|v| v.as_str());
            
            match metric_type.as_str() {
                "storage.googleapis.com/storage/total_bytes" => {
                    bucket_metrics.bucket_size = latest_value;
                }
                "storage.googleapis.com/storage/object_count" => {
                    bucket_metrics.object_count = latest_value;
                }
                "storage.googleapis.com/api/request_count" => {
                    match method_label {
                        Some("GET") => bucket_metrics.get_requests = latest_value,
                        Some("PUT") => bucket_metrics.put_requests = latest_value,
                        Some("DELETE") => bucket_metrics.delete_requests = latest_value,
                        _ => {
                            // Total request count (no method label or other method)
                            if bucket_metrics.request_count.is_none() {
                                bucket_metrics.request_count = latest_value;
                            }
                        }
                    }
                }
                "storage.googleapis.com/network/sent_bytes_count" => {
                    bucket_metrics.bytes_sent = latest_value;
                }
                "storage.googleapis.com/network/received_bytes_count" => {
                    bucket_metrics.bytes_received = latest_value;
                }
                _ => {
                    warn!("Unknown metric type: {}", metric_type);
                }
            }
        }
        
        // Separate request for errors
        let errors_filter = format!(
            "{} AND metric.type = \"storage.googleapis.com/api/request_count\" AND metric.labels.response_code >= \"400\"",
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
                            bucket_metrics.errors = Some(
                                bucket_metrics.errors.unwrap_or(0.0) + value
                            );
                        }
                    }
                }
            }
        }
        
        Ok(bucket_metrics)
    }

    /// Collect metrics for multiple GCS buckets in parallel
    pub async fn collect_metrics_batch(
        &self,
        buckets: &[GcsBucketId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<GcsBucketMetrics>> {
        let mut tasks = Vec::new();
        for bucket_id in buckets {
            let collector = self.clone();
            let bucket_id_clone = bucket_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&bucket_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for GCS bucket: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for GcsCollector {
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

/// Convert GCS metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn gcs_metrics_to_reiver_format(
    metrics: &GcsBucketMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("bucket_id:{}", metrics.bucket_id),
        format!("bucket_name:{}", metrics.bucket_name),
        format!("location:{}", metrics.location),
        "source:gcp_cloud_storage".to_string(),
    ];

    if let Some(value) = metrics.bucket_size {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_storage.bucket_size".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.object_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_storage.object_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.request_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_storage.request_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.bytes_sent {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_storage.bytes_sent".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.bytes_received {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_storage.bytes_received".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.get_requests {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_storage.get_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.put_requests {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_storage.put_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.delete_requests {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_storage.delete_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_storage.errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
