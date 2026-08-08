//! Google Cloud Load Balancing integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud Load Balancing metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - Request Count
//! - Request/Response Bytes
//! - Backend Healthy/Unhealthy Instances
//! - Latency
//! - Errors

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::GcpConfig;

/// Google Cloud Load Balancer identifier (load balancer name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerId(pub String);

/// Google Cloud Load Balancing metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct LoadBalancingMetrics {
    pub load_balancer_id: String,
    pub load_balancer_name: String,
    pub load_balancer_type: String, // "https", "tcp_udp", "internal_https", etc.
    pub region: String,
    pub timestamp: DateTime<Utc>,
    pub request_count: Option<f64>,
    pub request_bytes: Option<f64>,
    pub response_bytes: Option<f64>,
    pub backend_latency: Option<f64>,
    pub total_latency: Option<f64>,
    pub backend_healthy_instances: Option<f64>,
    pub backend_unhealthy_instances: Option<f64>,
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

/// Google Cloud Load Balancing metrics collector
pub struct LoadBalancingCollector {
    config: GcpConfig,
    http_client: Client,
}

impl LoadBalancingCollector {
    /// Create a new Load Balancing collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// Collect Cloud Monitoring metrics for load balancers
    /// Note: We discover load balancers by querying Cloud Monitoring metrics directly
    pub async fn collect_all_metrics(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<LoadBalancingMetrics>> {
        info!("Collecting Google Cloud Load Balancing metrics...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Collect metrics for different load balancer types
        let mut all_metrics = Vec::new();
        
        // HTTP(S) Load Balancing
        if let Ok(metrics) = self.collect_https_lb_metrics(&url, &access_token, start_time, end_time).await {
            all_metrics.extend(metrics);
        }
        
        // TCP/UDP Load Balancing
        if let Ok(metrics) = self.collect_tcp_udp_lb_metrics(&url, &access_token, start_time, end_time).await {
            all_metrics.extend(metrics);
        }
        
        // Internal HTTP(S) Load Balancing
        if let Ok(metrics) = self.collect_internal_https_lb_metrics(&url, &access_token, start_time, end_time).await {
            all_metrics.extend(metrics);
        }
        
        info!("Collected {} load balancer metric sets", all_metrics.len());
        Ok(all_metrics)
    }

    /// Collect HTTP(S) Load Balancing metrics
    async fn collect_https_lb_metrics(
        &self,
        url: &str,
        access_token: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<LoadBalancingMetrics>> {
        let filter = "resource.type = \"https_lb_rule\"";
        
        let metrics = vec![
            "loadbalancing.googleapis.com/https/backend_request_count",
            "loadbalancing.googleapis.com/https/backend_request_bytes_count",
            "loadbalancing.googleapis.com/https/backend_response_bytes_count",
            "loadbalancing.googleapis.com/https/backend_latencies",
            "loadbalancing.googleapis.com/https/total_latencies",
            "loadbalancing.googleapis.com/https/backend_healthy_instances",
            "loadbalancing.googleapis.com/https/backend_unhealthy_instances",
        ];
        
        self.collect_lb_metrics(url, access_token, filter, &metrics, "https", start_time, end_time).await
    }

    /// Collect TCP/UDP Load Balancing metrics
    async fn collect_tcp_udp_lb_metrics(
        &self,
        url: &str,
        access_token: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<LoadBalancingMetrics>> {
        let filter = "resource.type = \"tcp_udp_lb_rule\"";
        
        let metrics = vec![
            "loadbalancing.googleapis.com/tcp_udp/backend_bytes_sent",
            "loadbalancing.googleapis.com/tcp_udp/backend_bytes_received",
            "loadbalancing.googleapis.com/tcp_udp/backend_healthy_instances",
            "loadbalancing.googleapis.com/tcp_udp/backend_unhealthy_instances",
        ];
        
        self.collect_lb_metrics(url, access_token, filter, &metrics, "tcp_udp", start_time, end_time).await
    }

    /// Collect Internal HTTP(S) Load Balancing metrics
    async fn collect_internal_https_lb_metrics(
        &self,
        url: &str,
        access_token: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<LoadBalancingMetrics>> {
        let filter = "resource.type = \"internal_https_lb_rule\"";
        
        let metrics = vec![
            "loadbalancing.googleapis.com/internal_https/backend_request_count",
            "loadbalancing.googleapis.com/internal_https/backend_request_bytes_count",
            "loadbalancing.googleapis.com/internal_https/backend_response_bytes_count",
            "loadbalancing.googleapis.com/internal_https/backend_latencies",
            "loadbalancing.googleapis.com/internal_https/total_latencies",
            "loadbalancing.googleapis.com/internal_https/backend_healthy_instances",
            "loadbalancing.googleapis.com/internal_https/backend_unhealthy_instances",
        ];
        
        self.collect_lb_metrics(url, access_token, filter, &metrics, "internal_https", start_time, end_time).await
    }

    /// Generic method to collect load balancing metrics
    async fn collect_lb_metrics(
        &self,
        url: &str,
        access_token: &str,
        filter: &str,
        metric_types: &[&str],
        lb_type: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<LoadBalancingMetrics>> {
        let metrics_str = metric_types.iter().map(|m| format!("metric.type = \"{}\"", m)).collect::<Vec<_>>().join(" OR ");
        let full_filter = format!("{} AND ({})", filter, metrics_str);
        
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
                "groupByFields": ["resource.labels.url_map_name", "resource.labels.region"],
            },
        });
        
        let response = self.http_client
            .post(url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Cloud Monitoring metrics: {}", e))?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!("Cloud Monitoring API error ({}): {}", status, body);
            return Ok(Vec::new()); // Return empty on error, don't fail completely
        }
        
        let metrics_response: TimeSeriesResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse Cloud Monitoring metrics response: {}", e))?;
        
        // Group metrics by load balancer (url_map_name + region)
        let mut lb_metrics: std::collections::HashMap<(String, String), LoadBalancingMetrics> = std::collections::HashMap::new();
        
        for time_series in metrics_response.timeSeries {
            let metric_type = &time_series.metric.metric_type;
            
            // Extract load balancer name and region from resource labels
            let (lb_name, region) = if let Some(labels) = &time_series.resource.labels {
                let name = labels.get("url_map_name")
                    .or_else(|| labels.get("forwarding_rule_name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let reg = labels.get("region")
                    .or_else(|| labels.get("zone"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("global")
                    .to_string();
                (name, reg)
            } else {
                ("unknown".to_string(), "global".to_string())
            };
            
            let key = (lb_name.clone(), region.clone());
            
            // Get or create metrics for this load balancer
            let metrics = lb_metrics.entry(key).or_insert_with(|| LoadBalancingMetrics {
                load_balancer_id: format!("{}:{}", lb_name, region),
                load_balancer_name: lb_name.clone(),
                load_balancer_type: lb_type.to_string(),
                region: region.clone(),
                timestamp: end_time,
                request_count: None,
                request_bytes: None,
                response_bytes: None,
                backend_latency: None,
                total_latency: None,
                backend_healthy_instances: None,
                backend_unhealthy_instances: None,
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
            
            // Map metric types to fields
            if metric_type.contains("request_count") {
                metrics.request_count = latest_value;
            } else if metric_type.contains("request_bytes") || metric_type.contains("bytes_sent") {
                metrics.request_bytes = latest_value;
            } else if metric_type.contains("response_bytes") || metric_type.contains("bytes_received") {
                metrics.response_bytes = latest_value;
            } else if metric_type.contains("backend_latencies") {
                metrics.backend_latency = latest_value;
            } else if metric_type.contains("total_latencies") {
                metrics.total_latency = latest_value;
            } else if metric_type.contains("backend_healthy_instances") {
                metrics.backend_healthy_instances = latest_value;
            } else if metric_type.contains("backend_unhealthy_instances") {
                metrics.backend_unhealthy_instances = latest_value;
            }
        }
        
        Ok(lb_metrics.into_values().collect())
    }
}

impl Clone for LoadBalancingCollector {
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

/// Convert Load Balancing metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn load_balancing_metrics_to_reiver_format(
    metrics: &LoadBalancingMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("load_balancer_id:{}", metrics.load_balancer_id),
        format!("load_balancer_name:{}", metrics.load_balancer_name),
        format!("load_balancer_type:{}", metrics.load_balancer_type),
        format!("region:{}", metrics.region),
        "source:gcp_load_balancing".to_string(),
    ];

    if let Some(value) = metrics.request_count {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.load_balancing.{}.request_count", metrics.load_balancer_type),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.request_bytes {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.load_balancing.{}.request_bytes", metrics.load_balancer_type),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.response_bytes {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.load_balancing.{}.response_bytes", metrics.load_balancer_type),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.backend_latency {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.load_balancing.{}.backend_latency", metrics.load_balancer_type),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.total_latency {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.load_balancing.{}.total_latency", metrics.load_balancer_type),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.backend_healthy_instances {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.load_balancing.{}.backend_healthy_instances", metrics.load_balancer_type),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.backend_unhealthy_instances {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.load_balancing.{}.backend_unhealthy_instances", metrics.load_balancer_type),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.load_balancing.{}.errors", metrics.load_balancer_type),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
