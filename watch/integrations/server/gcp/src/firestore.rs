//! Google Cloud Firestore integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud Firestore metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - Document Count
//! - Document Reads
//! - Document Writes
//! - Document Deletes
//! - API Request Count
//! - Network Bytes Received
//! - Network Bytes Sent
//! - Active Connections

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google Cloud Firestore database identifier (database name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirestoreDatabaseId(pub String);

/// Google Cloud Firestore metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct FirestoreMetrics {
    pub database_id: String,
    pub database_name: String,
    pub project_id: String,
    pub location: String,
    pub timestamp: DateTime<Utc>,
    pub document_count: Option<f64>,
    pub document_reads: Option<f64>,
    pub document_writes: Option<f64>,
    pub document_deletes: Option<f64>,
    pub api_request_count: Option<f64>,
    pub network_bytes_received: Option<f64>,
    pub network_bytes_sent: Option<f64>,
    pub active_connections: Option<f64>,
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

/// Google Cloud Firestore metrics collector
pub struct FirestoreCollector {
    config: GcpConfig,
    http_client: Client,
}

impl FirestoreCollector {
    /// Create a new Firestore collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Firestore databases in the project
    /// Note: Firestore uses Cloud Monitoring to discover databases via resource labels
    pub async fn list_databases(&self) -> Result<Vec<FirestoreDatabaseId>> {
        info!("Listing Google Cloud Firestore databases...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Use Cloud Monitoring API to discover Firestore databases
        // We query for any Firestore metric to get the list of databases
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Query for document count metric to discover databases
        let filter = "resource.type = \"firestore_database\" AND metric.type = \"firestore.googleapis.com/document/count\"";
        
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
        
        let response = self.http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list Firestore databases: {}", e))?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Cloud Monitoring API error ({}): {}", status, body));
        }
        
        let metrics_response: TimeSeriesResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse Cloud Monitoring API response: {}", e))?;
        
        // Extract unique database IDs from resource labels
        let mut databases = std::collections::HashSet::new();
        
        for time_series in metrics_response.timeSeries {
            if let Some(labels) = &time_series.resource.labels {
                // Firestore database ID is typically in database_id or project_id label
                if let Some(db_id) = labels.get("database_id").and_then(|v| v.as_str()) {
                    databases.insert(db_id.to_string());
                } else if labels.get("project_id").is_some() {
                    // For default database, use project_id
                    databases.insert(format!("(default)"));
                }
            }
        }
        
        // If no databases found via monitoring, try to use the default database
        if databases.is_empty() {
            info!("No Firestore databases found via monitoring, using default database");
            databases.insert("(default)".to_string());
        }
        
        let database_ids: Vec<FirestoreDatabaseId> = databases
            .into_iter()
            .map(FirestoreDatabaseId)
            .collect();
        
        info!("Found {} Google Cloud Firestore databases", database_ids.len());
        Ok(database_ids)
    }

    /// Extract database name from database ID
    fn extract_database_name(database_id: &str) -> String {
        database_id.to_string()
    }

    /// Extract metadata from resource labels
    fn extract_metadata_from_labels(labels: &Option<serde_json::Value>) -> (String, String) {
        let mut location = "unknown".to_string();
        let mut project_id = "unknown".to_string();
        
        if let Some(labels) = labels {
            if let Some(loc) = labels.get("location").and_then(|v| v.as_str()) {
                location = loc.to_string();
            }
            if let Some(proj) = labels.get("project_id").and_then(|v| v.as_str()) {
                project_id = proj.to_string();
            }
        }
        
        (location, project_id)
    }

    /// Collect Cloud Monitoring metrics for a specific Firestore database
    pub async fn collect_metrics(
        &self,
        database_id: &FirestoreDatabaseId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<FirestoreMetrics> {
        info!("Collecting Firestore metrics for: {}", database_id.0);
        
        let database_name = Self::extract_database_name(&database_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for this specific database
        // Firestore uses database_id label, or project_id for default database
        let filter = if database_id.0 == "(default)" {
            format!(
                "resource.type = \"firestore_database\" AND resource.labels.project_id = \"{}\"",
                self.config.project_id
            )
        } else {
            format!(
                "resource.type = \"firestore_database\" AND resource.labels.database_id = \"{}\"",
                database_id.0
            )
        };
        
        // Metrics to collect
        let metrics = vec![
            "firestore.googleapis.com/document/count",
            "firestore.googleapis.com/document/read_count",
            "firestore.googleapis.com/document/write_count",
            "firestore.googleapis.com/document/delete_count",
            "firestore.googleapis.com/api/request_count",
            "firestore.googleapis.com/network/bytes_received",
            "firestore.googleapis.com/network/bytes_sent",
            "firestore.googleapis.com/api/active_connections",
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
                "perSeriesAligner": "ALIGN_RATE",
                "crossSeriesReducer": "REDUCE_SUM",
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
        let mut project_id = self.config.project_id.clone();
        let mut firestore_metrics = FirestoreMetrics {
            database_id: database_id.0.clone(),
            database_name: database_name.clone(),
            project_id: project_id.clone(),
            location: location.clone(),
            timestamp: end_time,
            document_count: None,
            document_reads: None,
            document_writes: None,
            document_deletes: None,
            api_request_count: None,
            network_bytes_received: None,
            network_bytes_sent: None,
            active_connections: None,
        };
        
        // Parse metrics from response
        for time_series in metrics_response.timeSeries {
            let metric_type = &time_series.metric.metric_type;
            
            // Extract location and project from resource labels (first time we see it)
            if location == "unknown" {
                let (loc, proj) = Self::extract_metadata_from_labels(&time_series.resource.labels);
                location = loc;
                project_id = proj;
                firestore_metrics.location = location.clone();
                firestore_metrics.project_id = project_id.clone();
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
            
            match metric_type.as_str() {
                "firestore.googleapis.com/document/count" => {
                    firestore_metrics.document_count = latest_value;
                }
                "firestore.googleapis.com/document/read_count" => {
                    firestore_metrics.document_reads = latest_value;
                }
                "firestore.googleapis.com/document/write_count" => {
                    firestore_metrics.document_writes = latest_value;
                }
                "firestore.googleapis.com/document/delete_count" => {
                    firestore_metrics.document_deletes = latest_value;
                }
                "firestore.googleapis.com/api/request_count" => {
                    firestore_metrics.api_request_count = latest_value;
                }
                "firestore.googleapis.com/network/bytes_received" => {
                    firestore_metrics.network_bytes_received = latest_value;
                }
                "firestore.googleapis.com/network/bytes_sent" => {
                    firestore_metrics.network_bytes_sent = latest_value;
                }
                "firestore.googleapis.com/api/active_connections" => {
                    firestore_metrics.active_connections = latest_value;
                }
                _ => {
                    warn!("Unknown Firestore metric: {}", metric_type);
                }
            }
        }
        
        Ok(firestore_metrics)
    }

    /// Collect metrics for multiple Firestore databases in parallel
    pub async fn collect_metrics_batch(
        &self,
        databases: &[FirestoreDatabaseId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<FirestoreMetrics>> {
        let mut tasks = Vec::new();
        for database_id in databases {
            let collector = self.clone();
            let database_id_clone = database_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&database_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Firestore database: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for FirestoreCollector {
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

/// Convert Firestore metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn firestore_metrics_to_reiver_format(
    metrics: &FirestoreMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("database_id:{}", metrics.database_id),
        format!("database_name:{}", metrics.database_name),
        format!("project_id:{}", metrics.project_id),
        format!("location:{}", metrics.location),
        "source:gcp_firestore".to_string(),
    ];

    if let Some(value) = metrics.document_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.firestore.document_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.document_reads {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.firestore.document_reads".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.document_writes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.firestore.document_writes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.document_deletes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.firestore.document_deletes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.api_request_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.firestore.api_request_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_bytes_received {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.firestore.network_bytes_received".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_bytes_sent {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.firestore.network_bytes_sent".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.active_connections {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.firestore.active_connections".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
