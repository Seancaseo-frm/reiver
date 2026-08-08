//! Google Cloud Redis (Memorystore) integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud Redis (Memorystore) metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - CPU Utilization
//! - Memory Utilization
//! - Cache Hits/Misses
//! - Connections
//! - Evictions
//! - Network I/O
//! - Operations per Second
//!
//! Note: For direct Redis connection monitoring (INFO command metrics), use the Reiver Agent
//! which already supports Redis connections directly to Memorystore instances.

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google Cloud Redis instance identifier (instance name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisInstanceId(pub String);

/// Google Cloud Redis metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct RedisMetrics {
    pub instance_id: String,
    pub instance_name: String,
    pub region: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub memory_utilization: Option<f64>,
    pub cache_hits: Option<f64>,
    pub cache_misses: Option<f64>,
    pub connected_clients: Option<f64>,
    pub evicted_keys: Option<f64>,
    pub network_bytes_received: Option<f64>,
    pub network_bytes_sent: Option<f64>,
    pub operations_per_second: Option<f64>,
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

/// Google Cloud Redis metrics collector
pub struct RedisCollector {
    config: GcpConfig,
    http_client: Client,
}

impl RedisCollector {
    /// Create a new Redis collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Redis instances in the project
    pub async fn list_instances(&self) -> Result<Vec<RedisInstanceId>> {
        info!("Listing Google Cloud Redis instances...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all instances across all regions
        let url = format!(
            "https://redis.googleapis.com/v1/projects/{}/locations/-/instances",
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
                .map_err(|e| anyhow::anyhow!("Failed to list Redis instances: {}", e))?;
            
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
                        all_instances.push(RedisInstanceId(name.to_string()));
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
        
        info!("Found {} Google Cloud Redis instances", all_instances.len());
        Ok(all_instances)
    }

    /// Extract instance name from full resource name
    /// Format: projects/{project}/locations/{region}/instances/{instance_name}
    fn extract_instance_name(instance_name: &str) -> String {
        instance_name.split('/').last().unwrap_or(instance_name).to_string()
    }

    /// Extract region from full resource name
    /// Format: projects/{project}/locations/{region}/instances/{instance_name}
    fn extract_region(instance_name: &str) -> String {
        let parts: Vec<&str> = instance_name.split('/').collect();
        for (i, part) in parts.iter().enumerate() {
            if part == &"locations" && i + 1 < parts.len() {
                return parts[i + 1].to_string();
            }
        }
        "unknown".to_string()
    }

    /// Collect Cloud Monitoring metrics for a specific Redis instance
    pub async fn collect_metrics(
        &self,
        instance_id: &RedisInstanceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<RedisMetrics> {
        info!("Collecting Redis metrics for: {}", instance_id.0);
        
        let instance_name = Self::extract_instance_name(&instance_id.0);
        let region = Self::extract_region(&instance_id.0);
        
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
        // Redis resource format: projects/{project}/locations/{region}/instances/{instance_name}
        let filter = format!(
            "resource.type = \"redis_instance\" AND resource.labels.instance_id = \"{}\" AND resource.labels.location = \"{}\"",
            instance_name, region
        );
        
        // Metrics to collect
        let metrics = vec![
            "redis.googleapis.com/stats/cpu/utilization",
            "redis.googleapis.com/stats/memory/utilization",
            "redis.googleapis.com/stats/cache_hits",
            "redis.googleapis.com/stats/cache_misses",
            "redis.googleapis.com/stats/connected_clients",
            "redis.googleapis.com/stats/evicted_keys",
            "redis.googleapis.com/stats/replication/network/received_bytes_count",
            "redis.googleapis.com/stats/replication/network/sent_bytes_count",
            "redis.googleapis.com/stats/commands/processed_total",
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
        let mut instance_metrics = RedisMetrics {
            instance_id: instance_id.0.clone(),
            instance_name: instance_name.clone(),
            region: region.clone(),
            timestamp: end_time,
            cpu_utilization: None,
            memory_utilization: None,
            cache_hits: None,
            cache_misses: None,
            connected_clients: None,
            evicted_keys: None,
            network_bytes_received: None,
            network_bytes_sent: None,
            operations_per_second: None,
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
                "redis.googleapis.com/stats/cpu/utilization" => {
                    instance_metrics.cpu_utilization = latest_value;
                }
                "redis.googleapis.com/stats/memory/utilization" => {
                    instance_metrics.memory_utilization = latest_value;
                }
                "redis.googleapis.com/stats/cache_hits" => {
                    instance_metrics.cache_hits = latest_value;
                }
                "redis.googleapis.com/stats/cache_misses" => {
                    instance_metrics.cache_misses = latest_value;
                }
                "redis.googleapis.com/stats/connected_clients" => {
                    instance_metrics.connected_clients = latest_value;
                }
                "redis.googleapis.com/stats/evicted_keys" => {
                    instance_metrics.evicted_keys = latest_value;
                }
                "redis.googleapis.com/stats/replication/network/received_bytes_count" => {
                    instance_metrics.network_bytes_received = latest_value;
                }
                "redis.googleapis.com/stats/replication/network/sent_bytes_count" => {
                    instance_metrics.network_bytes_sent = latest_value;
                }
                "redis.googleapis.com/stats/commands/processed_total" => {
                    // This is a counter, for ops/sec we'd need rate calculation
                    // For now, store as is
                    instance_metrics.operations_per_second = latest_value;
                }
                _ => {
                    warn!("Unknown metric type: {}", metric_type);
                }
            }
        }
        
        Ok(instance_metrics)
    }

    /// Collect metrics for multiple Redis instances in parallel
    pub async fn collect_metrics_batch(
        &self,
        instances: &[RedisInstanceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<RedisMetrics>> {
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
                    error!("Failed to collect metrics for Redis instance: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for RedisCollector {
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

/// Convert Redis metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn redis_metrics_to_reiver_format(
    metrics: &RedisMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("instance_id:{}", metrics.instance_id),
        format!("instance_name:{}", metrics.instance_name),
        format!("region:{}", metrics.region),
        "source:gcp_redis".to_string(),
    ];

    if let Some(value) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.redis.cpu_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.redis.memory_utilization".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.cache_hits {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.redis.cache_hits".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.cache_misses {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.redis.cache_misses".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.connected_clients {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.redis.connected_clients".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.evicted_keys {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.redis.evicted_keys".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_bytes_received {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.redis.network_bytes_received".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_bytes_sent {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.redis.network_bytes_sent".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.operations_per_second {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.redis.operations_per_second".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
