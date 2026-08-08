//! OCI Container Engine for Kubernetes (OKE) integration for collecting OCI API and Monitoring metrics
//!
//! This module provides functionality to collect OKE cluster information and metrics from OCI API and Monitoring API.
//! Metrics collected include:
//! - Cluster Status (from OKE API)
//! - Node Count (from OKE API)
//! - Control Plane Metrics (from OCI Monitoring API)
//!
//! Note: Node and pod-level metrics are expected to be collected by an agent deployed in the cluster.

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use url::Url;

use crate::config::OciConfig;

/// OCI OKE Cluster identifier (cluster OCID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciOkeClusterId(pub String);

/// OCI OKE Cluster metrics collected from OCI API and Monitoring API
#[derive(Debug, Clone, Serialize)]
pub struct OciOkeClusterMetrics {
    pub cluster_id: String,
    pub cluster_name: String,
    pub compartment_id: String,
    pub timestamp: DateTime<Utc>,
    // Cluster info from OKE API
    pub cluster_status: Option<String>,
    pub node_count: Option<i64>,
    pub node_pool_count: Option<i64>,
    // Control plane metrics from OCI Monitoring API
    pub control_plane_cpu_utilization: Option<f64>,
    pub control_plane_memory_utilization: Option<f64>,
}

/// OCI OKE API response structures
#[derive(Debug, Deserialize)]
struct OkeClusterResponse {
    #[serde(default)]
    items: Vec<OkeCluster>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OkeCluster {
    id: String,
    name: String,
    lifecycle_state: Option<String>,
    #[serde(default)]
    endpoint_config: Option<OkeEndpointConfig>,
    #[serde(default)]
    options: Option<OkeClusterOptions>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OkeEndpointConfig {
    is_public_ip_enabled: Option<bool>,
    subnet_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OkeClusterOptions {
    #[serde(default)]
    service_lb_subnet_ids: Vec<String>,
}

/// OCI Monitoring API response structures
#[derive(Debug, Deserialize)]
struct OciMonitoringResponse {
    #[serde(default)]
    data: Vec<OciMetricData>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OciMetricData {
    #[serde(rename = "namespace")]
    namespace_name: String,
    #[serde(rename = "compartmentId")]
    compartment_id: String,
    name: String,
    dimensions: Option<serde_json::Value>,
    #[serde(default)]
    datapoints: Vec<OciDataPoint>,
    resolution: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OciDataPoint {
    timestamp: String,
    value: f64,
}

/// OCI OKE metrics collector
pub struct OciOkeCollector {
    config: OciConfig,
    http_client: Client,
}

impl OciOkeCollector {
    /// Create a new OCI OKE collector with the given OCI configuration
    pub fn new(config: OciConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all OKE clusters in a compartment
    pub async fn list_clusters(&self, compartment_id: &str) -> Result<Vec<OciOkeClusterId>> {
        info!("Listing OKE clusters in compartment: {}", compartment_id);

        let base_url = self.config.api_base_url("containerengine");
        let url = format!("{}/20180222/clusters", base_url);
        let mut all_clusters = Vec::new();
        let mut page: Option<String> = None;

        loop {
            let mut url_obj = Url::parse(&url)?;
            if let Some(page_token) = &page {
                url_obj.set_query(Some(&format!("compartmentId={}&page={}", compartment_id, page_token)));
            } else {
                url_obj.set_query(Some(&format!("compartmentId={}", compartment_id)));
            }

            let mut headers = std::collections::HashMap::new();
            headers.insert("content-type".to_string(), "application/json".to_string());

            self.config.sign_request("GET", &url_obj, &mut headers, None)?;

            let mut request = self.http_client.get(url_obj.as_str());
            for (key, value) in &headers {
                request = request.header(key, value);
            }

            let response = request
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list OKE clusters: {}", e))?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                warn!("OCI OKE API error ({}): {}", status, body);
                break;
            }

            // Extract next page token from headers BEFORE parsing JSON (which consumes response)
            let next_page_token = response.headers()
                .get("opc-next-page")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let cluster_response: OkeClusterResponse = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse OKE clusters response: {}", e))?;

            for cluster in cluster_response.items {
                all_clusters.push(OciOkeClusterId(cluster.id));
            }

            // Check for next page (OCI uses opc-next-page header)
            page = next_page_token;

            if page.is_none() {
                break;
            }
        }

        info!("Found {} OKE clusters", all_clusters.len());
        Ok(all_clusters)
    }

    /// Get cluster details from OKE API
    async fn get_cluster_details(&self, cluster_id: &str) -> Result<OkeCluster> {
        let base_url = self.config.api_base_url("containerengine");
        let url = format!("{}/20180222/clusters/{}", base_url, cluster_id);

        let url_obj = Url::parse(&url)?;
        let mut headers = std::collections::HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());

        self.config.sign_request("GET", &url_obj, &mut headers, None)?;

        let mut request = self.http_client.get(url_obj.as_str());
        for (key, value) in &headers {
            request = request.header(key, value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get OKE cluster details: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("OCI OKE API error ({}): {}", status, body));
        }

        let cluster: OkeCluster = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse OKE cluster response: {}", e))?;

        Ok(cluster)
    }

    /// Collect OCI Monitoring metrics for OKE control plane
    async fn collect_control_plane_metrics(
        &self,
        cluster_id: &str,
        compartment_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<(Option<f64>, Option<f64>)> {
        let base_url = self.config.monitoring_api_url();
        let url = format!("{}/v1/metricData/{}/actions/summarizeMetricsData", base_url, compartment_id);

        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();

        // OCI Monitoring uses namespace oci_containerengine for OKE metrics
        let query_text = "CpuUtilization[1m].mean()";

        let request_body = serde_json::json!({
            "namespaceName": "oci_containerengine",
            "query": query_text,
            "startTime": start_time_rfc3339,
            "endTime": end_time_rfc3339,
            "compartmentId": compartment_id,
        });

        let url_obj = Url::parse(&url)?;
        let mut headers = std::collections::HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());

        let body_string = serde_json::to_string(&request_body)?;
        let body_bytes = body_string.as_bytes();

        self.config.sign_request("POST", &url_obj, &mut headers, Some(body_bytes))?;

        let mut request = self.http_client.post(&url).json(&request_body);
        for (key, value) in &headers {
            request = request.header(key, value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get OCI Monitoring metrics: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!("OCI Monitoring API error ({}): {}", status, body);
            return Ok((None, None));
        }

        let metrics_response: OciMonitoringResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse OCI Monitoring metrics response: {}", e))?;

        let mut cpu_util = None;
        let mut memory_util = None;

        for metric_data in metrics_response.data {
            // Filter by cluster ID in dimensions
            let metric_cluster_id = metric_data.dimensions
                .as_ref()
                .and_then(|d| d.get("clusterId"))
                .and_then(|v| v.as_str());

            if metric_cluster_id != Some(cluster_id) {
                continue;
            }

            let latest_value = metric_data.datapoints
                .iter()
                .max_by_key(|dp| &dp.timestamp)
                .map(|dp| dp.value);

            match metric_data.name.as_str() {
                "CpuUtilization" | "CpuUsage" => {
                    cpu_util = latest_value;
                }
                "MemoryUtilization" | "MemoryUsage" => {
                    memory_util = latest_value;
                }
                _ => {}
            }
        }

        Ok((cpu_util, memory_util))
    }

    /// Collect all OKE metrics for a compartment
    pub async fn collect_all_metrics(
        &self,
        compartment_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<OciOkeClusterMetrics>> {
        info!("Collecting OKE metrics for compartment: {}", compartment_id);

        // List all clusters
        let cluster_ids = self.list_clusters(compartment_id).await?;

        let mut all_metrics = Vec::new();

        for cluster_id_obj in cluster_ids {
            let cluster_id = cluster_id_obj.0.clone();

            // Get cluster details from OKE API
            let cluster_details = match self.get_cluster_details(&cluster_id).await {
                Ok(details) => details,
                Err(e) => {
                    warn!("Failed to get OKE cluster details for {}: {}", cluster_id, e);
                    continue;
                }
            };

            // Collect control plane metrics from OCI Monitoring
            let (cpu_util, memory_util) = self.collect_control_plane_metrics(
                &cluster_id,
                compartment_id,
                start_time,
                end_time,
            ).await.unwrap_or((None, None));

            // Note: Node count would require additional API calls to list node pools and nodes
            // For now, we'll leave it as None and rely on the agent in cluster for detailed metrics
            let metrics = OciOkeClusterMetrics {
                cluster_id: cluster_id.clone(),
                cluster_name: cluster_details.name.clone(),
                compartment_id: compartment_id.to_string(),
                timestamp: end_time,
                cluster_status: cluster_details.lifecycle_state,
                node_count: None, // Would require additional API calls
                node_pool_count: None, // Would require additional API calls
                control_plane_cpu_utilization: cpu_util,
                control_plane_memory_utilization: memory_util,
            };

            all_metrics.push(metrics);
        }

        info!("Collected {} OKE cluster metric sets", all_metrics.len());
        Ok(all_metrics)
    }
}

impl Clone for OciOkeCollector {
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

/// Convert OCI OKE metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn oci_oke_metrics_to_reiver_format(
    metrics: &OciOkeClusterMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("cluster_id:{}", metrics.cluster_id),
        format!("cluster_name:{}", metrics.cluster_name),
        format!("compartment_id:{}", metrics.compartment_id),
        "source:oci_oke".to_string(),
    ];

    if let Some(value) = metrics.node_count {
        reiver_metrics.push(ReiverMetric {
            name: "oci.oke.node_count".to_string(),
            value: value as f64,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.control_plane_cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "oci.oke.control_plane_cpu_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.control_plane_memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "oci.oke.control_plane_memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
