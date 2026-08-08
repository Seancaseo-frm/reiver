//! Azure Container Instances integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure Container Instances metrics from Azure Monitor.
//! Metrics collected include:
//! - CPU Percentage
//! - Memory Usage
//! - Network Bytes In
//! - Network Bytes Out

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure Container Group identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureContainerGroupId(pub String);

/// Azure Container Instances metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureContainerInstancesMetrics {
    pub container_group_id: String,
    pub container_group_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_percentage: Option<f64>,
    pub memory_percentage: Option<f64>,
    pub memory_usage_bytes: Option<f64>,
    pub network_bytes_received: Option<f64>,
    pub network_bytes_transmitted: Option<f64>,
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

/// Azure Container Instances metrics collector
pub struct AzureContainerInstancesCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureContainerInstancesCollector {
    /// Create a new Azure Container Instances collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Container Groups in the subscription
    pub async fn list_container_groups(&self) -> Result<Vec<AzureContainerGroupId>> {
        info!("Listing Azure Container Groups...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.ContainerInstance/containerGroups'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_groups = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure Container Groups: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract Container Group resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_groups.push(AzureContainerGroupId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure Container Groups", all_groups.len());
        Ok(all_groups)
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

    /// Extract Container Group name from Azure resource ID
    fn extract_container_group_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific Container Group
    pub async fn collect_metrics(
        &self,
        container_group_id: &AzureContainerGroupId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureContainerInstancesMetrics> {
        info!("Collecting Azure Container Instances metrics for: {}", container_group_id.0);
        
        let container_group_name = Self::extract_container_group_name(&container_group_id.0);
        let resource_group = Self::extract_resource_group(&container_group_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for Container Groups
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            container_group_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "CpuUsage",                    // CPU usage percentage
            "MemoryUsage",                 // Memory usage percentage
            "MemoryWorkingSet",            // Memory working set in bytes
            "NetworkBytesReceivedPerSec",  // Network bytes received per second
            "NetworkBytesTransmittedPerSec", // Network bytes transmitted per second
        ];
        let metric_names_str = metric_names.join(",");
        
        // Format times for Azure Monitor API (ISO 8601)
        let timespan = format!("{}/{}", start_time.to_rfc3339(), end_time.to_rfc3339());
        let interval = "PT1M"; // 1-minute interval
        
        let url = format!(
            "{}&metricnames={}&timespan={}&interval={}&aggregation=Average",
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
        let mut container_metrics = AzureContainerInstancesMetrics {
            container_group_id: container_group_id.0.clone(),
            container_group_name: container_group_name.clone(),
            resource_group,
            timestamp: end_time,
            cpu_percentage: None,
            memory_percentage: None,
            memory_usage_bytes: None,
            network_bytes_received: None,
            network_bytes_transmitted: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For percentages and rates, use average
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| dp.average)
                .last();
            
            match metric.name.value.as_str() {
                "CpuUsage" => container_metrics.cpu_percentage = latest_value,
                "MemoryUsage" => container_metrics.memory_percentage = latest_value,
                "MemoryWorkingSet" => container_metrics.memory_usage_bytes = latest_value,
                "NetworkBytesReceivedPerSec" => container_metrics.network_bytes_received = latest_value,
                "NetworkBytesTransmittedPerSec" => container_metrics.network_bytes_transmitted = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(container_metrics)
    }

    /// Collect metrics for multiple Container Groups in parallel
    pub async fn collect_metrics_batch(
        &self,
        container_groups: &[AzureContainerGroupId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureContainerInstancesMetrics>> {
        let mut tasks = Vec::new();
        for container_group_id in container_groups {
            let collector = self.clone();
            let container_group_id_clone = container_group_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&container_group_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure Container Group: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureContainerInstancesCollector {
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

/// Convert Azure Container Instances metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_container_instances_metrics_to_reiver_format(
    metrics: &AzureContainerInstancesMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("container_group_id:{}", metrics.container_group_id),
        format!("container_group_name:{}", metrics.container_group_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_container_instances".to_string(),
    ];

    if let Some(value) = metrics.cpu_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.container_instances.cpu_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.container_instances.memory_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_usage_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "azure.container_instances.memory_usage_bytes".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_bytes_received {
        reiver_metrics.push(ReiverMetric {
            name: "azure.container_instances.network_bytes_received".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_bytes_transmitted {
        reiver_metrics.push(ReiverMetric {
            name: "azure.container_instances.network_bytes_transmitted".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
