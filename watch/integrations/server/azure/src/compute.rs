//! Azure Virtual Machines integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure VM metrics from Azure Monitor.
//! Metrics collected include:
//! - Percentage CPU
//! - Network In/Out
//! - Disk Read/Write Operations
//! - Disk Read/Write Bytes
//! - Available Memory Bytes

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure VM identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureVmId(pub String);

/// Azure VM metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureVmMetrics {
    pub vm_id: String,
    pub vm_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub percentage_cpu: Option<f64>,
    pub network_in_total: Option<f64>,
    pub network_out_total: Option<f64>,
    pub disk_read_bytes: Option<f64>,
    pub disk_write_bytes: Option<f64>,
    pub disk_read_operations: Option<f64>,
    pub disk_write_operations: Option<f64>,
    pub available_memory_bytes: Option<f64>,
}

/// Azure Monitor Metrics API response structures
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

/// Azure VM metrics collector
pub struct AzureVmCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureVmCollector {
    /// Create a new Azure VM collector with the given Azure configuration
    /// 
    /// Supports both Service Principal (preferred) and Default Azure Credential
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Virtual Machines in the subscription
    pub async fn list_vms(&self) -> Result<Vec<AzureVmId>> {
        info!("Listing Azure Virtual Machines...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.Compute/virtualMachines'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_vms = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure VMs: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract VM resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_vms.push(AzureVmId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure Virtual Machines", all_vms.len());
        Ok(all_vms)
    }

    /// Extract resource group name from Azure resource ID
    /// Format: /subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/{provider}/{type}/{name}
    fn extract_resource_group(resource_id: &str) -> Option<String> {
        let parts: Vec<&str> = resource_id.split('/').collect();
        if parts.len() >= 4 && parts[1] == "subscriptions" && parts[3] == "resourceGroups" {
            Some(parts[4].to_string())
        } else {
            None
        }
    }

    /// Extract VM name from Azure resource ID
    fn extract_vm_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific VM
    pub async fn collect_metrics(
        &self,
        vm_id: &AzureVmId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureVmMetrics> {
        info!("Collecting Azure VM metrics for: {}", vm_id.0);
        
        let vm_name = Self::extract_vm_name(&vm_id.0);
        let resource_group = Self::extract_resource_group(&vm_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format:
        // /subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/{provider}/{type}/{name}/providers/microsoft.insights/metrics
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            vm_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "Percentage CPU",
            "Network In Total",
            "Network Out Total",
            "Disk Read Bytes",
            "Disk Write Bytes",
            "Disk Read Operations/Sec",
            "Disk Write Operations/Sec",
            "Available Memory Bytes",
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
        let mut vm_metrics = AzureVmMetrics {
            vm_id: vm_id.0.clone(),
            vm_name: vm_name.clone(),
            resource_group,
            timestamp: end_time,
            percentage_cpu: None,
            network_in_total: None,
            network_out_total: None,
            disk_read_bytes: None,
            disk_write_bytes: None,
            disk_read_operations: None,
            disk_write_operations: None,
            available_memory_bytes: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| dp.average)
                .last();
            
            match metric.name.value.as_str() {
                "Percentage CPU" => vm_metrics.percentage_cpu = latest_value,
                "Network In Total" => vm_metrics.network_in_total = latest_value,
                "Network Out Total" => vm_metrics.network_out_total = latest_value,
                "Disk Read Bytes" => vm_metrics.disk_read_bytes = latest_value,
                "Disk Write Bytes" => vm_metrics.disk_write_bytes = latest_value,
                "Disk Read Operations/Sec" => vm_metrics.disk_read_operations = latest_value,
                "Disk Write Operations/Sec" => vm_metrics.disk_write_operations = latest_value,
                "Available Memory Bytes" => vm_metrics.available_memory_bytes = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(vm_metrics)
    }

    /// Collect metrics for multiple VMs in parallel
    pub async fn collect_metrics_batch(
        &self,
        vms: &[AzureVmId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureVmMetrics>> {
        let mut tasks = Vec::new();
        for vm_id in vms {
            let collector = self.clone();
            let vm_id_clone = vm_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&vm_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure VM: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureVmCollector {
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

/// Convert Azure VM metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_vm_metrics_to_reiver_format(
    metrics: &AzureVmMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("vm_id:{}", metrics.vm_id),
        format!("vm_name:{}", metrics.vm_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_vm".to_string(),
    ];

    if let Some(value) = metrics.percentage_cpu {
        reiver_metrics.push(ReiverMetric {
            name: "azure.vm.percentage_cpu".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_in_total {
        reiver_metrics.push(ReiverMetric {
            name: "azure.vm.network_in_total".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.network_out_total {
        reiver_metrics.push(ReiverMetric {
            name: "azure.vm.network_out_total".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_read_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "azure.vm.disk_read_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_write_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "azure.vm.disk_write_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_read_operations {
        reiver_metrics.push(ReiverMetric {
            name: "azure.vm.disk_read_operations".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.disk_write_operations {
        reiver_metrics.push(ReiverMetric {
            name: "azure.vm.disk_write_operations".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.available_memory_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "azure.vm.available_memory_bytes".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
