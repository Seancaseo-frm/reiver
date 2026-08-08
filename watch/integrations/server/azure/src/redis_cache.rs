//! Azure Redis Cache integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure Redis Cache metrics from Azure Monitor.
//! Metrics collected include:
//! - CPU Percentage
//! - Memory Usage
//! - Cache Hits
//! - Cache Misses
//! - Connected Clients
//! - Evictions
//! - Server Load
//! - Operations Per Second
//! - Network In/Out

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure Redis Cache identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureRedisCacheId(pub String);

/// Azure Redis Cache metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureRedisCacheMetrics {
    pub cache_id: String,
    pub cache_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_percentage: Option<f64>,
    pub memory_percentage: Option<f64>,
    pub cache_hits: Option<f64>,
    pub cache_misses: Option<f64>,
    pub connected_clients: Option<f64>,
    pub evicted_keys: Option<f64>,
    pub server_load: Option<f64>,
    pub operations_per_second: Option<f64>,
    pub network_in: Option<f64>,
    pub network_out: Option<f64>,
}

/// Azure Monitor Metrics API response structures (reused from compute module)
#[derive(Debug, Deserialize)]
struct AzureMonitorMetricsResponse {
    value: Vec<AzureMonitorMetric>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct AzureMonitorMetric {
    id: String,
    #[serde(rename = "type")]
    metric_type: String,
    name: AzureMonitorMetricName,
    displayDescription: Option<String>,
    unit: String,
    timeseries: Vec<AzureMonitorTimeSeries>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct AzureMonitorMetricName {
    value: String,
    localizedValue: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct AzureMonitorTimeSeries {
    #[serde(default)]
    metadatavalues: Vec<serde_json::Value>,
    data: Vec<AzureMonitorDataPoint>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct AzureMonitorDataPoint {
    timeStamp: String,
    average: Option<f64>,
    minimum: Option<f64>,
    maximum: Option<f64>,
    total: Option<f64>,
    count: Option<f64>,
}

/// Azure Redis Cache metrics collector
pub struct AzureRedisCacheCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureRedisCacheCollector {
    /// Create a new Azure Redis Cache collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Redis Cache instances in the subscription
    pub async fn list_caches(&self) -> Result<Vec<AzureRedisCacheId>> {
        info!("Listing Azure Redis Cache instances...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.Cache/redis'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_caches = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure Redis Cache instances: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract Redis Cache resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_caches.push(AzureRedisCacheId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure Redis Cache instances", all_caches.len());
        Ok(all_caches)
    }

    /// Extract resource group name from Azure resource ID
    fn extract_resource_group(resource_id: &str) -> Option<String> {
        let parts: Vec<&str> = resource_id.split('/').collect();
        if parts.len() >= 4 && parts[1] == "subscriptions" && parts[3] == "resourceGroups" {
            Some(parts[4].to_string())
        } else {
            None
        }
    }

    /// Extract Redis Cache name from Azure resource ID
    fn extract_cache_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific Redis Cache
    pub async fn collect_metrics(
        &self,
        cache_id: &AzureRedisCacheId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureRedisCacheMetrics> {
        info!("Collecting Azure Redis Cache metrics for: {}", cache_id.0);
        
        let cache_name = Self::extract_cache_name(&cache_id.0);
        let resource_group = Self::extract_resource_group(&cache_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for Redis Cache
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            cache_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "percentProcessorTime",   // CPU percentage
            "usedmemory",             // Memory usage
            "usedmemoryPercentage",   // Memory percentage
            "cachehits",              // Cache hits
            "cachemisses",            // Cache misses
            "connectedclients",       // Connected clients
            "evictedkeys",            // Evicted keys
            "serverLoad",             // Server load
            "operationsPerSecond",    // Operations per second
            "networkBytesIn",         // Network in
            "networkBytesOut",        // Network out
        ];
        let metric_names_str = metric_names.join(",");
        
        // Format times for Azure Monitor API (ISO 8601)
        let timespan = format!("{}/{}", start_time.to_rfc3339(), end_time.to_rfc3339());
        let interval = "PT1M"; // 1-minute interval
        
        let url = format!(
            "{}&metricnames={}&timespan={}&interval={}&aggregation=Average,Total",
            metrics_url, metric_names_str, timespan, interval
        );
        
        let response = self.http_client
            .get(&url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure Monitor metrics: {}", e))?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Azure Monitor API error ({}): {}", status, body));
        }
        
        let metrics_response: AzureMonitorMetricsResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse Azure Monitor metrics response: {}", e))?;
        
        // Parse metrics from response
        let mut redis_metrics = AzureRedisCacheMetrics {
            cache_id: cache_id.0.clone(),
            cache_name: cache_name.clone(),
            resource_group,
            timestamp: end_time,
            cpu_percentage: None,
            memory_percentage: None,
            cache_hits: None,
            cache_misses: None,
            connected_clients: None,
            evicted_keys: None,
            server_load: None,
            operations_per_second: None,
            network_in: None,
            network_out: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For percentages and averages, use average; for counts, use total
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    match metric.name.value.as_str() {
                        "percentProcessorTime" | "usedmemoryPercentage" | "serverLoad" | "operationsPerSecond" => dp.average,
                        "usedmemory" | "networkBytesIn" | "networkBytesOut" => dp.average,
                        "cachehits" | "cachemisses" | "connectedclients" | "evictedkeys" => dp.total,
                        _ => dp.average,
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "percentProcessorTime" => redis_metrics.cpu_percentage = latest_value,
                "usedmemoryPercentage" => redis_metrics.memory_percentage = latest_value,
                "cachehits" => redis_metrics.cache_hits = latest_value,
                "cachemisses" => redis_metrics.cache_misses = latest_value,
                "connectedclients" => redis_metrics.connected_clients = latest_value,
                "evictedkeys" => redis_metrics.evicted_keys = latest_value,
                "serverLoad" => redis_metrics.server_load = latest_value,
                "operationsPerSecond" => redis_metrics.operations_per_second = latest_value,
                "networkBytesIn" => redis_metrics.network_in = latest_value,
                "networkBytesOut" => redis_metrics.network_out = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(redis_metrics)
    }

    /// Collect metrics for multiple Redis Cache instances in parallel
    pub async fn collect_metrics_batch(
        &self,
        caches: &[AzureRedisCacheId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureRedisCacheMetrics>> {
        let mut tasks = Vec::new();
        for cache_id in caches {
            let collector = self.clone();
            let cache_id_clone = cache_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&cache_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure Redis Cache: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureRedisCacheCollector {
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

/// Convert Azure Redis Cache metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_redis_cache_metrics_to_reiver_format(
    metrics: &AzureRedisCacheMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("cache_id:{}", metrics.cache_id),
        format!("cache_name:{}", metrics.cache_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_redis_cache".to_string(),
    ];

    if let Some(value) = metrics.cpu_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.cpu_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.memory_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.cache_hits {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.cache_hits".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.cache_misses {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.cache_misses".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.connected_clients {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.connected_clients".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.evicted_keys {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.evicted_keys".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.server_load {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.server_load".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.operations_per_second {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.operations_per_second".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_in {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.network_in".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_out {
        reiver_metrics.push(ReiverMetric {
            name: "azure.redis_cache.network_out".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
