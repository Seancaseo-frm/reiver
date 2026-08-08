//! Google Cloud BigQuery integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud BigQuery metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - Query Count
//! - Query Execution Time
//! - Bytes Processed
//! - Slots Allocated
//! - Storage Used
//! - Table Count
//! - Dataset Count
//! - Job Failures

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google Cloud BigQuery project identifier (project ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BigQueryProjectId(pub String);

/// Google Cloud BigQuery metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct BigQueryMetrics {
    pub project_id: String,
    pub timestamp: DateTime<Utc>,
    pub query_count: Option<f64>,
    pub query_execution_time: Option<f64>,
    pub bytes_processed: Option<f64>,
    pub slots_allocated: Option<f64>,
    pub storage_used_bytes: Option<f64>,
    pub table_count: Option<f64>,
    pub dataset_count: Option<f64>,
    pub job_failures: Option<f64>,
    pub bytes_billed: Option<f64>,
    pub rows_scanned: Option<f64>,
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

/// Google Cloud BigQuery metrics collector
pub struct BigQueryCollector {
    config: GcpConfig,
    http_client: Client,
}

impl BigQueryCollector {
    /// Create a new BigQuery collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all BigQuery projects (typically just the configured project)
    /// Note: BigQuery metrics are scoped to projects
    pub async fn list_projects(&self) -> Result<Vec<BigQueryProjectId>> {
        info!("Listing Google Cloud BigQuery projects...");
        
        // For BigQuery, we typically monitor the configured project
        // We can verify it exists by checking for BigQuery metrics
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Use Cloud Monitoring API to verify BigQuery is enabled for this project
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Query for any BigQuery metric to verify the project has BigQuery enabled
        let filter = "resource.type = \"bigquery_project\" AND metric.type = \"bigquery.googleapis.com/job/num_in_flight\"";
        
        let end_time = Utc::now();
        let start_time = end_time - chrono::Duration::hours(1);
        
        let request_body = serde_json::json!({
            "filter": filter,
            "interval": {
                "startTime": start_time.to_rfc3339(),
                "endTime": end_time.to_rfc3339(),
            },
            "aggregation": {
                "alignmentPeriod": "3600s",
                "perSeriesAligner": "ALIGN_MEAN",
            },
        });
        
        let _response = self.http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to verify BigQuery project: {}", e))?;
        
        // Even if no metrics are found, we still return the project ID
        // BigQuery might be enabled but not have recent activity
        let projects = vec![BigQueryProjectId(self.config.project_id.clone())];
        
        info!("Found {} Google Cloud BigQuery project(s)", projects.len());
        Ok(projects)
    }

    /// Collect Cloud Monitoring metrics for a specific BigQuery project
    pub async fn collect_metrics(
        &self,
        project_id: &BigQueryProjectId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<BigQueryMetrics> {
        info!("Collecting BigQuery metrics for project: {}", project_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for BigQuery project metrics
        let filter = format!(
            "resource.type = \"bigquery_project\" AND resource.labels.project_id = \"{}\"",
            project_id.0
        );
        
        // Metrics to collect
        let metrics = vec![
            "bigquery.googleapis.com/job/num_in_flight", // Query count (in-flight jobs)
            "bigquery.googleapis.com/job/total_slot_ms", // Total slot milliseconds (execution time)
            "bigquery.googleapis.com/job/total_bytes_processed", // Bytes processed
            "bigquery.googleapis.com/job/total_bytes_billed", // Bytes billed
            "bigquery.googleapis.com/job/total_rows_scanned", // Rows scanned
            "bigquery.googleapis.com/slots/allocated", // Slots allocated
            "bigquery.googleapis.com/storage/table/size_bytes", // Storage used
            "bigquery.googleapis.com/storage/table/row_count", // Table row count (can be used to estimate table count)
            "bigquery.googleapis.com/job/failed", // Job failures
        ];
        
        let mut bigquery_metrics = BigQueryMetrics {
            project_id: project_id.0.clone(),
            timestamp: end_time,
            query_count: None,
            query_execution_time: None,
            bytes_processed: None,
            slots_allocated: None,
            storage_used_bytes: None,
            table_count: None,
            dataset_count: None,
            job_failures: None,
            bytes_billed: None,
            rows_scanned: None,
        };
        
        // Collect each metric
        for metric_type in metrics {
            let metric_filter = format!("{} AND metric.type = \"{}\"", filter, metric_type);
            
            let request_body = serde_json::json!({
                "filter": metric_filter,
                "interval": {
                    "startTime": start_time.to_rfc3339(),
                    "endTime": end_time.to_rfc3339(),
                },
                "aggregation": {
                    "alignmentPeriod": "60s",
                    "perSeriesAligner": "ALIGN_RATE", // Use RATE for counters, MEAN for gauges
                    "crossSeriesReducer": "REDUCE_SUM", // Sum across all series
                },
            });
            
            let response = self.http_client
                .post(&url)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .json(&request_body)
                .send()
                .await;
            
            match response {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        warn!("Failed to get BigQuery metric {}: {} - {}", metric_type, status, body);
                        continue;
                    }
                    
                    let metrics_response: TimeSeriesResponse = match resp.json().await {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("Failed to parse response for metric {}: {}", metric_type, e);
                            continue;
                        }
                    };
                    
                    // Extract the latest value from the time series
                    let mut latest_value: Option<f64> = None;
                    let mut latest_timestamp: Option<DateTime<Utc>> = None;
                    
                    for time_series in metrics_response.timeSeries {
                        for point in time_series.points {
                            // Parse timestamp
                            let timestamp_str = &point.interval.endTime;
                            if let Ok(timestamp) = DateTime::parse_from_rfc3339(timestamp_str) {
                                let timestamp_utc = timestamp.with_timezone(&Utc);
                                
                                if latest_timestamp.is_none() || timestamp_utc > latest_timestamp.unwrap() {
                                    latest_timestamp = Some(timestamp_utc);
                                    
                                    // Extract value
                                    if let Some(value) = point.value.doubleValue {
                                        latest_value = Some(value);
                                    } else if let Some(int_value_str) = &point.value.int64Value {
                                        if let Ok(int_value) = int_value_str.parse::<f64>() {
                                            latest_value = Some(int_value);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    // Map metric type to BigQueryMetrics field
                    match metric_type {
                        "bigquery.googleapis.com/job/num_in_flight" => {
                            bigquery_metrics.query_count = latest_value;
                        }
                        "bigquery.googleapis.com/job/total_slot_ms" => {
                            bigquery_metrics.query_execution_time = latest_value;
                        }
                        "bigquery.googleapis.com/job/total_bytes_processed" => {
                            bigquery_metrics.bytes_processed = latest_value;
                        }
                        "bigquery.googleapis.com/job/total_bytes_billed" => {
                            bigquery_metrics.bytes_billed = latest_value;
                        }
                        "bigquery.googleapis.com/job/total_rows_scanned" => {
                            bigquery_metrics.rows_scanned = latest_value;
                        }
                        "bigquery.googleapis.com/slots/allocated" => {
                            bigquery_metrics.slots_allocated = latest_value;
                        }
                        "bigquery.googleapis.com/storage/table/size_bytes" => {
                            bigquery_metrics.storage_used_bytes = latest_value;
                        }
                        "bigquery.googleapis.com/job/failed" => {
                            bigquery_metrics.job_failures = latest_value;
                        }
                        _ => {
                            // Unknown metric type, skip
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to get BigQuery metric {}: {}", metric_type, e);
                }
            }
        }
        
        // Note: Table count and dataset count are not directly available as metrics
        // They would require BigQuery API calls, which is beyond the scope of Cloud Monitoring
        
        Ok(bigquery_metrics)
    }

    /// Collect Cloud Monitoring metrics for multiple BigQuery projects
    pub async fn collect_metrics_batch(
        &self,
        projects: &[BigQueryProjectId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<BigQueryMetrics>> {
        let mut metrics = Vec::new();

        for project_id in projects {
            match self.collect_metrics(project_id, start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for BigQuery project {}: {}", project_id.0, e);
                }
            }
        }

        Ok(metrics)
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

/// Convert BigQuery metrics to Reiver format
pub fn bigquery_metrics_to_reiver_format(
    metrics: &BigQueryMetrics,
    project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let base_tags = vec![
        format!("project_id:{}", project_id),
        format!("bigquery_project_id:{}", metrics.project_id),
        "source:gcp_cloud_monitoring".to_string(),
        "service:bigquery".to_string(),
    ];

    let mut add_metric = |name: &str, value: Option<f64>, metric_type: &str| {
        if let Some(v) = value {
            reiver_metrics.push(ReiverMetric {
                name: format!("bigquery.{}", name),
                value: v,
                r#type: metric_type.to_string(),
                timestamp: metrics.timestamp,
                tags: base_tags.clone(),
            });
        }
    };

    add_metric("query_count", metrics.query_count, "gauge");
    add_metric("query_execution_time", metrics.query_execution_time, "gauge");
    add_metric("bytes_processed", metrics.bytes_processed, "gauge");
    add_metric("bytes_billed", metrics.bytes_billed, "gauge");
    add_metric("rows_scanned", metrics.rows_scanned, "gauge");
    add_metric("slots_allocated", metrics.slots_allocated, "gauge");
    add_metric("storage_used_bytes", metrics.storage_used_bytes, "gauge");
    add_metric("job_failures", metrics.job_failures, "counter");

    reiver_metrics
}
