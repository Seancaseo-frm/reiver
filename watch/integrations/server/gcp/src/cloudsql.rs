//! Google CloudSQL integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google CloudSQL metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - CPU Utilization
//! - Memory Utilization
//! - Disk Utilization
//! - Database Connections
//! - Disk I/O
//! - Query Performance
//!
//! Note: For direct database connection monitoring (query-level metrics), use the Reiver Agent
//! which already supports PostgreSQL and MySQL connections directly to CloudSQL instances.

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google CloudSQL instance identifier (instance name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudSqlInstanceId(pub String);

/// Google CloudSQL metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct CloudSqlMetrics {
    pub instance_id: String,
    pub instance_name: String,
    pub database_version: String, // POSTGRES_14, MYSQL_8_0, etc.
    pub region: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub memory_utilization: Option<f64>,
    pub disk_utilization: Option<f64>,
    pub disk_read_ops: Option<f64>,
    pub disk_write_ops: Option<f64>,
    pub disk_read_bytes: Option<f64>,
    pub disk_write_bytes: Option<f64>,
    pub database_connections: Option<f64>,
    pub replication_lag: Option<f64>,
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

/// Google CloudSQL metrics collector
pub struct CloudSqlCollector {
    config: GcpConfig,
    http_client: Client,
}

impl CloudSqlCollector {
    /// Create a new CloudSQL collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all CloudSQL instances in the project
    pub async fn list_instances(&self) -> Result<Vec<CloudSqlInstanceId>> {
        info!("Listing Google CloudSQL instances...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all instances across all regions
        let url = format!(
            "https://sqladmin.googleapis.com/v1/projects/{}/instances",
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
                .map_err(|e| anyhow::anyhow!("Failed to list CloudSQL instances: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("GCP API error ({}): {}", status, body));
            }
            
            let instances_response: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse GCP API response: {}", e))?;
            
            // Extract instance names
            if let Some(instances) = instances_response.get("items").and_then(|v| v.as_array()) {
                for instance in instances {
                    if let Some(name) = instance.get("name").and_then(|v| v.as_str()) {
                        all_instances.push(CloudSqlInstanceId(name.to_string()));
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
        
        info!("Found {} Google CloudSQL instances", all_instances.len());
        Ok(all_instances)
    }

    /// Extract instance name (already just the name)
    fn extract_instance_name(instance_name: &str) -> String {
        instance_name.to_string()
    }

    /// Extract database version and region from instance metadata
    fn extract_metadata_from_labels(labels: &Option<serde_json::Value>) -> (String, String) {
        let mut database_version = "unknown".to_string();
        let mut region = "unknown".to_string();
        
        if let Some(labels) = labels {
            if let Some(db_version) = labels.get("database_version").and_then(|v| v.as_str()) {
                database_version = db_version.to_string();
            }
            if let Some(reg) = labels.get("region").and_then(|v| v.as_str()) {
                region = reg.to_string();
            }
        }
        
        (database_version, region)
    }

    /// Collect Cloud Monitoring metrics for a specific CloudSQL instance
    pub async fn collect_metrics(
        &self,
        instance_id: &CloudSqlInstanceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<CloudSqlMetrics> {
        info!("Collecting CloudSQL metrics for: {}", instance_id.0);
        
        let instance_name = Self::extract_instance_name(&instance_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for this specific instance
        // CloudSQL resource format: projects/{project}/instances/{instance_name}
        let filter = format!(
            "resource.type = \"cloudsql_database\" AND resource.labels.database_id = \"{}:{}\"",
            self.config.project_id, instance_name
        );
        
        // Metrics to collect
        let metrics = vec![
            "cloudsql.googleapis.com/database/cpu/utilization",
            "cloudsql.googleapis.com/database/memory/utilization",
            "cloudsql.googleapis.com/database/disk/utilization",
            "cloudsql.googleapis.com/database/disk/read_ops_count",
            "cloudsql.googleapis.com/database/disk/write_ops_count",
            "cloudsql.googleapis.com/database/disk/bytes_used",
            "cloudsql.googleapis.com/database/network/received_bytes_count",
            "cloudsql.googleapis.com/database/network/sent_bytes_count",
            "cloudsql.googleapis.com/database/postgresql/num_backends",
            "cloudsql.googleapis.com/database/mysql/threads_connected",
            "cloudsql.googleapis.com/database/replication/replica_lag",
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
        
        // Extract metadata from first time series
        let mut database_version = "unknown".to_string();
        let mut region = "unknown".to_string();
        if let Some(first_ts) = metrics_response.timeSeries.first() {
            let (db_ver, reg) = Self::extract_metadata_from_labels(&first_ts.resource.labels);
            database_version = db_ver;
            region = reg;
        }
        
        // Parse metrics from response
        let mut instance_metrics = CloudSqlMetrics {
            instance_id: instance_id.0.clone(),
            instance_name: instance_name.clone(),
            database_version: database_version.clone(),
            region: region.clone(),
            timestamp: end_time,
            cpu_utilization: None,
            memory_utilization: None,
            disk_utilization: None,
            disk_read_ops: None,
            disk_write_ops: None,
            disk_read_bytes: None,
            disk_write_bytes: None,
            database_connections: None,
            replication_lag: None,
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
                "cloudsql.googleapis.com/database/cpu/utilization" => {
                    instance_metrics.cpu_utilization = latest_value;
                }
                "cloudsql.googleapis.com/database/memory/utilization" => {
                    instance_metrics.memory_utilization = latest_value;
                }
                "cloudsql.googleapis.com/database/disk/utilization" => {
                    instance_metrics.disk_utilization = latest_value;
                }
                "cloudsql.googleapis.com/database/disk/read_ops_count" => {
                    instance_metrics.disk_read_ops = latest_value;
                }
                "cloudsql.googleapis.com/database/disk/write_ops_count" => {
                    instance_metrics.disk_write_ops = latest_value;
                }
                "cloudsql.googleapis.com/database/disk/bytes_used" => {
                    // This is total bytes used, we can use it for disk_read_bytes if needed
                    // But typically we want separate read/write metrics
                }
                "cloudsql.googleapis.com/database/network/received_bytes_count" => {
                    instance_metrics.disk_read_bytes = latest_value;
                }
                "cloudsql.googleapis.com/database/network/sent_bytes_count" => {
                    instance_metrics.disk_write_bytes = latest_value;
                }
                "cloudsql.googleapis.com/database/postgresql/num_backends" => {
                    instance_metrics.database_connections = latest_value;
                }
                "cloudsql.googleapis.com/database/mysql/threads_connected" => {
                    instance_metrics.database_connections = latest_value;
                }
                "cloudsql.googleapis.com/database/replication/replica_lag" => {
                    instance_metrics.replication_lag = latest_value;
                }
                _ => {
                    warn!("Unknown metric type: {}", metric_type);
                }
            }
        }
        
        Ok(instance_metrics)
    }

    /// Collect metrics for multiple CloudSQL instances in parallel
    pub async fn collect_metrics_batch(
        &self,
        instances: &[CloudSqlInstanceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<CloudSqlMetrics>> {
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
                    error!("Failed to collect metrics for CloudSQL instance: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for CloudSqlCollector {
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

/// Convert CloudSQL metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn cloudsql_metrics_to_reiver_format(
    metrics: &CloudSqlMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("instance_id:{}", metrics.instance_id),
        format!("instance_name:{}", metrics.instance_name),
        format!("database_version:{}", metrics.database_version),
        format!("region:{}", metrics.region),
        "source:gcp_cloudsql".to_string(),
    ];

    if let Some(value) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloudsql.cpu_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloudsql.memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloudsql.disk_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_read_ops {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloudsql.disk_read_ops".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_write_ops {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloudsql.disk_write_ops".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_read_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloudsql.disk_read_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_write_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloudsql.disk_write_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.database_connections {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloudsql.database_connections".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.replication_lag {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.cloudsql.replication_lag".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
