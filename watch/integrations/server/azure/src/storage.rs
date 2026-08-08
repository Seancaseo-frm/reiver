//! Azure Blob Storage integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure Blob Storage metrics from Azure Monitor.
//! Metrics collected include:
//! - Capacity (storage used)
//! - Transactions (read/write operations)
//! - Ingress (data incoming)
//! - Egress (data outgoing)
//! - Availability
//! - SuccessE2ELatency
//! - SuccessServerLatency

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure Storage Account identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureStorageAccountId(pub String);

/// Azure Blob Storage metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureBlobStorageMetrics {
    pub storage_account_id: String,
    pub storage_account_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub capacity: Option<f64>,
    pub transactions: Option<f64>,
    pub ingress: Option<f64>,
    pub egress: Option<f64>,
    pub availability: Option<f64>,
    pub success_e2e_latency: Option<f64>,
    pub success_server_latency: Option<f64>,
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

/// Azure Blob Storage metrics collector
pub struct AzureBlobStorageCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureBlobStorageCollector {
    /// Create a new Azure Blob Storage collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Storage Accounts in the subscription
    pub async fn list_storage_accounts(&self) -> Result<Vec<AzureStorageAccountId>> {
        info!("Listing Azure Storage Accounts...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.Storage/storageAccounts'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_storage_accounts = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure Storage Accounts: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract Storage Account resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_storage_accounts.push(AzureStorageAccountId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure Storage Accounts", all_storage_accounts.len());
        Ok(all_storage_accounts)
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

    /// Extract Storage Account name from Azure resource ID
    fn extract_storage_account_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific Storage Account
    pub async fn collect_metrics(
        &self,
        storage_account_id: &AzureStorageAccountId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureBlobStorageMetrics> {
        info!("Collecting Azure Blob Storage metrics for: {}", storage_account_id.0);
        
        let storage_account_name = Self::extract_storage_account_name(&storage_account_id.0);
        let resource_group = Self::extract_resource_group(&storage_account_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for Storage Accounts
        // Note: Blob-specific metrics require filtering by blob service
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            storage_account_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        // For Blob Storage, we need to filter by service (Blob) using $filter parameter
        let metric_names = vec![
            "UsedCapacity",           // Capacity
            "Transactions",            // Total transactions
            "Ingress",                // Data incoming
            "Egress",                 // Data outgoing
            "Availability",           // Availability percentage
            "SuccessE2ELatency",      // End-to-end latency
            "SuccessServerLatency",    // Server latency
        ];
        let metric_names_str = metric_names.join(",");
        
        // Format times for Azure Monitor API (ISO 8601)
        let timespan = format!("{}/{}", start_time.to_rfc3339(), end_time.to_rfc3339());
        let interval = "PT1H"; // 1-hour interval (Blob Storage metrics are typically hourly)
        
        // Filter by Blob service: $filter=ApiName eq 'GetBlob' or ApiName eq 'PutBlob' etc.
        // For aggregate metrics, we can get all blob operations
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
        let mut storage_metrics = AzureBlobStorageMetrics {
            storage_account_id: storage_account_id.0.clone(),
            storage_account_name: storage_account_name.clone(),
            resource_group,
            timestamp: end_time,
            capacity: None,
            transactions: None,
            ingress: None,
            egress: None,
            availability: None,
            success_e2e_latency: None,
            success_server_latency: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For capacity, use average; for transactions/ingress/egress, use total; for latency/availability, use average
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    match metric.name.value.as_str() {
                        "UsedCapacity" | "Availability" | "SuccessE2ELatency" | "SuccessServerLatency" => dp.average,
                        "Transactions" | "Ingress" | "Egress" => dp.total,
                        _ => dp.average,
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "UsedCapacity" => storage_metrics.capacity = latest_value,
                "Transactions" => storage_metrics.transactions = latest_value,
                "Ingress" => storage_metrics.ingress = latest_value,
                "Egress" => storage_metrics.egress = latest_value,
                "Availability" => storage_metrics.availability = latest_value,
                "SuccessE2ELatency" => storage_metrics.success_e2e_latency = latest_value,
                "SuccessServerLatency" => storage_metrics.success_server_latency = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(storage_metrics)
    }

    /// Collect metrics for multiple Storage Accounts in parallel
    pub async fn collect_metrics_batch(
        &self,
        storage_accounts: &[AzureStorageAccountId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureBlobStorageMetrics>> {
        let mut tasks = Vec::new();
        for storage_account_id in storage_accounts {
            let collector = self.clone();
            let storage_account_id_clone = storage_account_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&storage_account_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure Storage Account: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureBlobStorageCollector {
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

/// Convert Azure Blob Storage metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_blob_storage_metrics_to_reiver_format(
    metrics: &AzureBlobStorageMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("storage_account_id:{}", metrics.storage_account_id),
        format!("storage_account_name:{}", metrics.storage_account_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_blob_storage".to_string(),
    ];

    if let Some(value) = metrics.capacity {
        reiver_metrics.push(ReiverMetric {
            name: "azure.blob_storage.capacity".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.transactions {
        reiver_metrics.push(ReiverMetric {
            name: "azure.blob_storage.transactions".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.ingress {
        reiver_metrics.push(ReiverMetric {
            name: "azure.blob_storage.ingress".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.egress {
        reiver_metrics.push(ReiverMetric {
            name: "azure.blob_storage.egress".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.availability {
        reiver_metrics.push(ReiverMetric {
            name: "azure.blob_storage.availability".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.success_e2e_latency {
        reiver_metrics.push(ReiverMetric {
            name: "azure.blob_storage.success_e2e_latency".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.success_server_latency {
        reiver_metrics.push(ReiverMetric {
            name: "azure.blob_storage.success_server_latency".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
