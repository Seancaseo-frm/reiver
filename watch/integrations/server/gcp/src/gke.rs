//! Google Kubernetes Engine (GKE) integration for collecting cluster info
//!
//! This module provides functionality to collect Google Kubernetes Engine cluster information
//! from the GKE API and cluster-level metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - Cluster Node Count
//! - Cluster Status
//! - Control Plane Metrics
//!
//! Note: For in-cluster metrics collection (node/pod metrics), deploy the Reiver Agent
//! as a DaemonSet in the cluster to collect detailed metrics directly from within the cluster.

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::config::GcpConfig;

/// Google Kubernetes Engine cluster identifier (cluster name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GkeClusterId(pub String);

/// Google Kubernetes Engine metrics collected from GKE API and Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct GkeClusterMetrics {
    pub cluster_id: String,
    pub cluster_name: String,
    pub location: String, // Region or zone
    pub cluster_status: String,
    pub timestamp: DateTime<Utc>,
    pub node_count: Option<f64>,
    pub node_pool_count: Option<f64>,
    pub control_plane_cpu_utilization: Option<f64>,
    pub control_plane_memory_utilization: Option<f64>,
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

/// Google Kubernetes Engine metrics collector
pub struct GkeCollector {
    config: GcpConfig,
    http_client: Client,
}

impl GkeCollector {
    /// Create a new GKE collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all GKE clusters in the project
    pub async fn list_clusters(&self) -> Result<Vec<GkeClusterId>> {
        info!("Listing Google Kubernetes Engine clusters...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all clusters across all locations
        let url = format!(
            "https://container.googleapis.com/v1/projects/{}/locations/-/clusters",
            self.config.project_id
        );
        
        let mut all_clusters = Vec::new();
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
                .map_err(|e| anyhow::anyhow!("Failed to list GKE clusters: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("GCP API error ({}): {}", status, body));
            }
            
            let clusters_response: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse GCP API response: {}", e))?;
            
            // Extract cluster names
            if let Some(clusters) = clusters_response.get("clusters").and_then(|v| v.as_array()) {
                for cluster in clusters {
                    if let Some(name) = cluster.get("name").and_then(|v| v.as_str()) {
                        all_clusters.push(GkeClusterId(name.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_page_token = clusters_response.get("nextPageToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            
            if next_page_token.is_none() {
                break;
            }
        }
        
        info!("Found {} Google Kubernetes Engine clusters", all_clusters.len());
        Ok(all_clusters)
    }

    /// Extract cluster name from full resource name
    /// Format: projects/{project}/locations/{location}/clusters/{cluster_name}
    fn extract_cluster_name(cluster_name: &str) -> String {
        cluster_name.split('/').last().unwrap_or(cluster_name).to_string()
    }

    /// Extract location from full resource name
    /// Format: projects/{project}/locations/{location}/clusters/{cluster_name}
    fn extract_location(cluster_name: &str) -> String {
        let parts: Vec<&str> = cluster_name.split('/').collect();
        for (i, part) in parts.iter().enumerate() {
            if part == &"locations" && i + 1 < parts.len() {
                return parts[i + 1].to_string();
            }
        }
        "unknown".to_string()
    }

    /// Collect cluster info and metrics for a specific GKE cluster
    pub async fn collect_metrics(
        &self,
        cluster_id: &GkeClusterId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<GkeClusterMetrics> {
        info!("Collecting GKE cluster info for: {}", cluster_id.0);
        
        let cluster_name = Self::extract_cluster_name(&cluster_id.0);
        let location = Self::extract_location(&cluster_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Get cluster details from GKE API
        let cluster_url = format!(
            "https://container.googleapis.com/v1/{}",
            cluster_id.0
        );
        
        let cluster_response = self.http_client
            .get(&cluster_url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GKE cluster details: {}", e))?;
        
        if !cluster_response.status().is_success() {
            let status = cluster_response.status();
            let body = cluster_response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("GKE API error ({}): {}", status, body));
        }
        
        let cluster_data: serde_json::Value = cluster_response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse GKE cluster response: {}", e))?;
        
        // Extract cluster status
        let cluster_status = cluster_data.get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        
        // Count nodes across all node pools
        let mut node_count = 0;
        let node_pool_count = cluster_data.get("nodePools")
            .and_then(|v| v.as_array())
            .map(|pools| pools.len() as f64)
            .unwrap_or(0.0);
        
        if let Some(node_pools) = cluster_data.get("nodePools").and_then(|v| v.as_array()) {
            for pool in node_pools {
                if let Some(count) = pool.get("initialNodeCount").and_then(|v| v.as_u64()) {
                    node_count += count;
                }
            }
        }
        
        // Build Cloud Monitoring API URL for control plane metrics
        let monitoring_url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for control plane metrics
        // GKE control plane resource format
        let filter = format!(
            "resource.type = \"k8s_cluster\" AND resource.labels.cluster_name = \"{}\" AND resource.labels.location = \"{}\"",
            cluster_name, location
        );
        
        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();
        
        // Metrics to collect
        let metrics = vec![
            "container.googleapis.com/cluster/cpu/utilization",
            "container.googleapis.com/cluster/memory/utilization",
        ];
        
        let metrics_str = metrics.iter().map(|m| format!("metric.type = \"{}\"", m)).collect::<Vec<_>>().join(" OR ");
        let full_filter = format!("{} AND ({})", filter, metrics_str);
        
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
        
        let metrics_response = self.http_client
            .post(&monitoring_url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await;
        
        let mut control_plane_cpu = None;
        let mut control_plane_memory = None;
        
        // Parse control plane metrics if available
        if let Ok(response) = metrics_response {
            if response.status().is_success() {
                if let Ok(metrics_data) = response.json::<TimeSeriesResponse>().await {
                    for time_series in metrics_data.timeSeries {
                        let metric_type = &time_series.metric.metric_type;
                        
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
                            "container.googleapis.com/cluster/cpu/utilization" => {
                                control_plane_cpu = latest_value;
                            }
                            "container.googleapis.com/cluster/memory/utilization" => {
                                control_plane_memory = latest_value;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        
        Ok(GkeClusterMetrics {
            cluster_id: cluster_id.0.clone(),
            cluster_name: cluster_name.clone(),
            location: location.clone(),
            cluster_status: cluster_status.clone(),
            timestamp: end_time,
            node_count: Some(node_count as f64),
            node_pool_count: Some(node_pool_count),
            control_plane_cpu_utilization: control_plane_cpu,
            control_plane_memory_utilization: control_plane_memory,
        })
    }

    /// Collect metrics for multiple GKE clusters in parallel
    pub async fn collect_metrics_batch(
        &self,
        clusters: &[GkeClusterId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<GkeClusterMetrics>> {
        let mut tasks = Vec::new();
        for cluster_id in clusters {
            let collector = self.clone();
            let cluster_id_clone = cluster_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&cluster_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for GKE cluster: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for GkeCollector {
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

/// Convert GKE metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn gke_metrics_to_reiver_format(
    metrics: &GkeClusterMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("cluster_id:{}", metrics.cluster_id),
        format!("cluster_name:{}", metrics.cluster_name),
        format!("location:{}", metrics.location),
        format!("cluster_status:{}", metrics.cluster_status),
        "source:gcp_gke".to_string(),
    ];

    if let Some(value) = metrics.node_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.gke.node_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.node_pool_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.gke.node_pool_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.control_plane_cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.gke.control_plane_cpu_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.control_plane_memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.gke.control_plane_memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
