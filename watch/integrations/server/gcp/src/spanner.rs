//! Google Cloud Spanner integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud Spanner metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - CPU Utilization
//! - Query Latency
//! - Query Throughput
//! - Node Count
//! - Storage Utilization
//! - Transaction Latency
//! - Errors

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google Cloud Spanner instance identifier (instance name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpannerInstanceId(pub String);

/// Google Cloud Spanner metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct SpannerMetrics {
    pub instance_id: String,
    pub instance_name: String,
    pub database_id: String,
    pub region: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub query_latency: Option<f64>,
    pub query_throughput: Option<f64>,
    pub transaction_latency: Option<f64>,
    pub node_count: Option<f64>,
    pub storage_utilization: Option<f64>,
    pub query_count: Option<f64>,
    pub transaction_count: Option<f64>,
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

/// Google Cloud Spanner metrics collector
pub struct SpannerCollector {
    config: GcpConfig,
    http_client: Client,
}

impl SpannerCollector {
    /// Create a new Spanner collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Spanner instances in the project
    pub async fn list_instances(&self) -> Result<Vec<SpannerInstanceId>> {
        info!("Listing Google Cloud Spanner instances...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all instances
        let url = format!(
            "https://spanner.googleapis.com/v1/projects/{}/instances",
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
                .map_err(|e| anyhow::anyhow!("Failed to list Spanner instances: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("GCP API error ({}): {}", status, body));
            }
            
            let instances_response: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse GCP API response: {}", e))?;
            
            // Extract instance names
            if let Some(instances) = instances_response.get("instances").and_then(|v| v.as_array()) {
                for instance in instances {
                    if let Some(name) = instance.get("name").and_then(|v| v.as_str()) {
                        all_instances.push(SpannerInstanceId(name.to_string()));
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
        
        info!("Found {} Google Cloud Spanner instances", all_instances.len());
        Ok(all_instances)
    }

    /// Extract instance name from full resource name
    /// Format: projects/{project}/instances/{instance_name}
    fn extract_instance_name(instance_name: &str) -> String {
        instance_name.split('/').last().unwrap_or(instance_name).to_string()
    }

    /// Extract metadata from resource labels
    fn extract_metadata_from_labels(labels: &Option<serde_json::Value>) -> (String, String) {
        let mut database_id = "unknown".to_string();
        let mut region = "unknown".to_string();
        
        if let Some(labels) = labels {
            if let Some(db_id) = labels.get("database_id").and_then(|v| v.as_str()) {
                database_id = db_id.to_string();
            }
            if let Some(reg) = labels.get("instance_id").and_then(|v| v.as_str()) {
                // Region might be in instance_id or separate label
                region = reg.to_string();
            }
            if let Some(reg) = labels.get("location").and_then(|v| v.as_str()) {
                region = reg.to_string();
            }
        }
        
        (database_id, region)
    }

    /// Collect Cloud Monitoring metrics for a specific Spanner instance
    /// Note: Spanner metrics are per-database, so we collect for all databases in the instance
    pub async fn collect_metrics(
        &self,
        instance_id: &SpannerInstanceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<SpannerMetrics>> {
        info!("Collecting Spanner metrics for: {}", instance_id.0);
        
        let instance_name = Self::extract_instance_name(&instance_id.0);
        
        // First, list all databases in this instance
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        let databases_url = format!(
            "https://spanner.googleapis.com/v1/{}",
            instance_id.0
        );
        
        // List databases
        let db_response = self.http_client
            .get(&format!("{}/databases", databases_url))
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list Spanner databases: {}", e))?;
        
        if !db_response.status().is_success() {
            let status = db_response.status();
            let body = db_response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("GCP API error listing databases ({}): {}", status, body));
        }
        
        let databases_response: serde_json::Value = db_response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse databases response: {}", e))?;
        
        let databases = databases_response.get("databases")
            .and_then(|v| v.as_array());
        
        let databases = match databases {
            Some(dbs) => dbs,
            None => {
                info!("No databases found in Spanner instance: {}", instance_name);
                return Ok(Vec::new());
            }
        };
        
        if databases.is_empty() {
            info!("No databases found in Spanner instance: {}", instance_name);
            return Ok(Vec::new());
        }
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        let mut all_metrics = Vec::new();
        
        // Collect metrics for each database
        for database in databases {
            let database_name = database.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            
            let database_id = database_name.split('/').last().unwrap_or(database_name);
            
            // Build filter for this specific database
            // Spanner resource format: projects/{project}/instances/{instance}/databases/{database}
            let filter = format!(
                "resource.type = \"spanner_database\" AND resource.labels.database = \"{}\" AND resource.labels.instance_id = \"{}\"",
                database_id, instance_name
            );
            
            // Metrics to collect
            let metrics = vec![
                "spanner.googleapis.com/instance/cpu/utilization",
                "spanner.googleapis.com/api/request/latencies",
                "spanner.googleapis.com/api/request_count",
                "spanner.googleapis.com/transaction/latencies",
                "spanner.googleapis.com/transaction/transactions",
                "spanner.googleapis.com/instance/cpu/utilization_by_priority",
                "spanner.googleapis.com/instance/storage/utilization",
                "spanner.googleapis.com/instance/num_nodes",
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
                warn!("Cloud Monitoring API error ({}): {} for database {}", status, body, database_id);
                continue;
            }
            
            let metrics_response: TimeSeriesResponse = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Cloud Monitoring metrics response: {}", e))?;
            
            // Extract metadata from first time series
            let mut region = "unknown".to_string();
            if let Some(first_ts) = metrics_response.timeSeries.first() {
                let (_, reg) = Self::extract_metadata_from_labels(&first_ts.resource.labels);
                region = reg;
            }
            
            // Parse metrics from response
            let mut db_metrics = SpannerMetrics {
                instance_id: instance_id.0.clone(),
                instance_name: instance_name.clone(),
                database_id: database_id.to_string(),
                region: region.clone(),
                timestamp: end_time,
                cpu_utilization: None,
                query_latency: None,
                query_throughput: None,
                transaction_latency: None,
                node_count: None,
                storage_utilization: None,
                query_count: None,
                transaction_count: None,
                errors: None,
            };
            
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
                    "spanner.googleapis.com/instance/cpu/utilization" => {
                        db_metrics.cpu_utilization = latest_value;
                    }
                    "spanner.googleapis.com/api/request/latencies" => {
                        // This is a distribution, we'll use p50 or mean if available
                        db_metrics.query_latency = latest_value;
                    }
                    "spanner.googleapis.com/api/request_count" => {
                        db_metrics.query_count = latest_value;
                        db_metrics.query_throughput = latest_value; // Can be used as throughput proxy
                    }
                    "spanner.googleapis.com/transaction/latencies" => {
                        db_metrics.transaction_latency = latest_value;
                    }
                    "spanner.googleapis.com/transaction/transactions" => {
                        db_metrics.transaction_count = latest_value;
                    }
                    "spanner.googleapis.com/instance/storage/utilization" => {
                        db_metrics.storage_utilization = latest_value;
                    }
                    "spanner.googleapis.com/instance/num_nodes" => {
                        db_metrics.node_count = latest_value;
                    }
                    _ => {
                        warn!("Unknown metric type: {}", metric_type);
                    }
                }
            }
            
            // Separate request for errors
            let errors_filter = format!(
                "{} AND metric.type = \"spanner.googleapis.com/api/request_count\" AND metric.labels.method = \"ExecuteSql\" AND metric.labels.code = \"ERROR\"",
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
                                db_metrics.errors = Some(
                                    db_metrics.errors.unwrap_or(0.0) + value
                                );
                            }
                        }
                    }
                }
            }
            
            all_metrics.push(db_metrics);
        }
        
        Ok(all_metrics)
    }

    /// Collect metrics for multiple Spanner instances in parallel
    pub async fn collect_metrics_batch(
        &self,
        instances: &[SpannerInstanceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<SpannerMetrics>> {
        let mut tasks = Vec::new();
        for instance_id in instances {
            let collector = self.clone();
            let instance_id_clone = instance_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&instance_id_clone, start_time, end_time).await
            }));
        }

        let mut all_metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metrics) => all_metrics.extend(metrics),
                Err(e) => {
                    error!("Failed to collect metrics for Spanner instance: {}", e);
                }
            }
        }

        Ok(all_metrics)
    }
}

impl Clone for SpannerCollector {
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

/// Convert Spanner metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn spanner_metrics_to_reiver_format(
    metrics: &SpannerMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("instance_id:{}", metrics.instance_id),
        format!("instance_name:{}", metrics.instance_name),
        format!("database_id:{}", metrics.database_id),
        format!("region:{}", metrics.region),
        "source:gcp_spanner".to_string(),
    ];

    if let Some(value) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.spanner.cpu_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.query_latency {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.spanner.query_latency".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.query_throughput {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.spanner.query_throughput".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.transaction_latency {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.spanner.transaction_latency".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.node_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.spanner.node_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.storage_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.spanner.storage_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.query_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.spanner.query_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.transaction_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.spanner.transaction_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.spanner.errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
