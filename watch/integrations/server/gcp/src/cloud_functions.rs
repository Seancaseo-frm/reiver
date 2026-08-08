//! Google Cloud Functions integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud Functions metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - Execution Count
//! - Execution Duration
//! - Memory Utilization
//! - Active Instances
//! - Errors
//! - Request Count

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google Cloud Function identifier (function name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudFunctionId(pub String);

/// Google Cloud Functions metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct CloudFunctionMetrics {
    pub function_id: String,
    pub function_name: String,
    pub region: String,
    pub timestamp: DateTime<Utc>,
    pub execution_count: Option<f64>,
    pub execution_duration: Option<f64>,
    pub memory_utilization: Option<f64>,
    pub active_instances: Option<f64>,
    pub errors: Option<f64>,
    pub request_count: Option<f64>,
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

/// Google Cloud Functions metrics collector
pub struct CloudFunctionsCollector {
    config: GcpConfig,
    http_client: Client,
}

impl CloudFunctionsCollector {
    /// Create a new Cloud Functions collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Cloud Functions in the project
    pub async fn list_functions(&self) -> Result<Vec<CloudFunctionId>> {
        info!("Listing Google Cloud Functions...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all functions across all regions
        let url = format!(
            "https://cloudfunctions.googleapis.com/v1/projects/{}/locations/-/functions",
            self.config.project_id
        );
        
        let mut all_functions = Vec::new();
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
                .map_err(|e| anyhow::anyhow!("Failed to list Cloud Functions: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("GCP API error ({}): {}", status, body));
            }
            
            let functions_response: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse GCP API response: {}", e))?;
            
            // Extract function names
            if let Some(functions) = functions_response.get("functions").and_then(|v| v.as_array()) {
                for function in functions {
                    if let Some(name) = function.get("name").and_then(|v| v.as_str()) {
                        all_functions.push(CloudFunctionId(name.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_page_token = functions_response.get("nextPageToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            
            if next_page_token.is_none() {
                break;
            }
        }
        
        info!("Found {} Google Cloud Functions", all_functions.len());
        Ok(all_functions)
    }

    /// Extract function name from full resource name
    /// Format: projects/{project}/locations/{region}/functions/{function_name}
    fn extract_function_name(function_name: &str) -> String {
        function_name.split('/').last().unwrap_or(function_name).to_string()
    }

    /// Extract region from full resource name
    /// Format: projects/{project}/locations/{region}/functions/{function_name}
    fn extract_region(function_name: &str) -> String {
        let parts: Vec<&str> = function_name.split('/').collect();
        for (i, part) in parts.iter().enumerate() {
            if part == &"locations" && i + 1 < parts.len() {
                return parts[i + 1].to_string();
            }
        }
        "unknown".to_string()
    }

    /// Collect Cloud Monitoring metrics for a specific Cloud Function
    pub async fn collect_metrics(
        &self,
        function_id: &CloudFunctionId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<CloudFunctionMetrics> {
        info!("Collecting Cloud Functions metrics for: {}", function_id.0);
        
        let function_name = Self::extract_function_name(&function_id.0);
        let region = Self::extract_region(&function_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for this specific function
        // Cloud Functions resource format: projects/{project}/locations/{region}/functions/{function_name}
        let filter = format!(
            "resource.type = \"cloud_function\" AND resource.labels.function_name = \"{}\" AND resource.labels.region = \"{}\"",
            function_name, region
        );
        
        // Metrics to collect
        let metrics = vec![
            "cloudfunctions.googleapis.com/function/execution_count",
            "cloudfunctions.googleapis.com/function/execution_times",
            "cloudfunctions.googleapis.com/function/memory_utilization",
            "cloudfunctions.googleapis.com/function/active_instances",
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
        let mut function_metrics = CloudFunctionMetrics {
            function_id: function_id.0.clone(),
            function_name: function_name.clone(),
            region: region.clone(),
            timestamp: end_time,
            execution_count: None,
            execution_duration: None,
            memory_utilization: None,
            active_instances: None,
            errors: None,
            request_count: None,
        };
        
        // Separate request for errors (filtered by severity)
        let errors_filter = format!(
            "{} AND metric.type = \"cloudfunctions.googleapis.com/function/execution_count\" AND metric.labels.severity = \"error\"",
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
        
        // Parse main metrics
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
                "cloudfunctions.googleapis.com/function/execution_count" => {
                    // Check if this is the total count (not error-specific)
                    if let Some(labels) = &time_series.metric.labels {
                        if labels.get("severity").is_none() {
                            function_metrics.execution_count = latest_value;
                        }
                    } else {
                        function_metrics.execution_count = latest_value;
                    }
                }
                "cloudfunctions.googleapis.com/function/execution_times" => {
                    function_metrics.execution_duration = latest_value;
                }
                "cloudfunctions.googleapis.com/function/memory_utilization" => {
                    function_metrics.memory_utilization = latest_value;
                }
                "cloudfunctions.googleapis.com/function/active_instances" => {
                    function_metrics.active_instances = latest_value;
                }
                _ => {
                    warn!("Unknown metric type: {}", metric_type);
                }
            }
        }
        
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
                        
                        function_metrics.errors = latest_value;
                    }
                }
            }
        }
        
        // Use execution_count as request_count if available
        if function_metrics.request_count.is_none() {
            function_metrics.request_count = function_metrics.execution_count;
        }
        
        Ok(function_metrics)
    }

    /// Collect metrics for multiple Cloud Functions in parallel
    pub async fn collect_metrics_batch(
        &self,
        functions: &[CloudFunctionId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<CloudFunctionMetrics>> {
        let mut tasks = Vec::new();
        for function_id in functions {
            let collector = self.clone();
            let function_id_clone = function_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&function_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Cloud Function: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for CloudFunctionsCollector {
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

/// Convert Cloud Functions metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn cloud_functions_metrics_to_reiver_format(
    metrics: &CloudFunctionMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("function_id:{}", metrics.function_id),
        format!("function_name:{}", metrics.function_name),
        format!("region:{}", metrics.region),
        "source:gcp_cloud_functions".to_string(),
    ];

    if let Some(value) = metrics.execution_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_functions.execution_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.execution_duration {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_functions.execution_duration".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_functions.memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.active_instances {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_functions.active_instances".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_functions.errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.request_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloud_functions.request_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
