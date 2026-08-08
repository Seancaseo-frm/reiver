//! Azure CosmosDB integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect CosmosDB database account metrics from Azure Monitor.
//! Metrics collected include:
//! - TotalRequestUnits (RU consumption)
//! - TotalRequests (request count)
//! - DataUsage (storage used)
//! - IndexUsage (index storage)
//! - DocumentCount (number of documents)
//! - Availability (service availability)
//! - ServiceAvailability (service availability percentage)
//! - DocumentQuota (document quota)
//! - IndexQuota (index quota)

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure CosmosDB account identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureCosmosDbAccountId(pub String);

/// Azure CosmosDB metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureCosmosDbMetrics {
    pub account_id: String,
    pub account_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub total_request_units: Option<f64>,
    pub total_requests: Option<f64>,
    pub data_usage: Option<f64>,
    pub index_usage: Option<f64>,
    pub document_count: Option<f64>,
    pub availability: Option<f64>,
    pub service_availability: Option<f64>,
    pub document_quota: Option<f64>,
    pub index_quota: Option<f64>,
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

/// Azure CosmosDB metrics collector
pub struct AzureCosmosDbCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureCosmosDbCollector {
    /// Create a new Azure CosmosDB collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all CosmosDB accounts in the subscription
    pub async fn list_accounts(&self) -> Result<Vec<AzureCosmosDbAccountId>> {
        info!("Listing Azure CosmosDB accounts...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.DocumentDB/databaseAccounts'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_accounts = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure CosmosDB accounts: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract CosmosDB account resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_accounts.push(AzureCosmosDbAccountId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure CosmosDB accounts", all_accounts.len());
        Ok(all_accounts)
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

    /// Extract CosmosDB account name from Azure resource ID
    fn extract_account_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific CosmosDB account
    pub async fn collect_metrics(
        &self,
        account_id: &AzureCosmosDbAccountId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureCosmosDbMetrics> {
        info!("Collecting Azure CosmosDB metrics for: {}", account_id.0);
        
        let account_name = Self::extract_account_name(&account_id.0);
        let resource_group = Self::extract_resource_group(&account_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for CosmosDB
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            account_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "TotalRequestUnits",      // Total Request Units consumed
            "TotalRequests",           // Total requests
            "DataUsage",               // Data storage usage
            "IndexUsage",              // Index storage usage
            "DocumentCount",           // Document count
            "Availability",           // Availability
            "ServiceAvailability",     // Service availability percentage
            "DocumentQuota",          // Document quota
            "IndexQuota",             // Index quota
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
        let mut cosmosdb_metrics = AzureCosmosDbMetrics {
            account_id: account_id.0.clone(),
            account_name: account_name.clone(),
            resource_group,
            timestamp: end_time,
            total_request_units: None,
            total_requests: None,
            data_usage: None,
            index_usage: None,
            document_count: None,
            availability: None,
            service_availability: None,
            document_quota: None,
            index_quota: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For totals (RequestUnits, Requests), use total; for usage/availability, use average
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    match metric.name.value.as_str() {
                        "TotalRequestUnits" | "TotalRequests" => dp.total,
                        "DataUsage" | "IndexUsage" | "DocumentCount" | "Availability" 
                        | "ServiceAvailability" | "DocumentQuota" | "IndexQuota" => dp.average,
                        _ => dp.average,
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "TotalRequestUnits" => cosmosdb_metrics.total_request_units = latest_value,
                "TotalRequests" => cosmosdb_metrics.total_requests = latest_value,
                "DataUsage" => cosmosdb_metrics.data_usage = latest_value,
                "IndexUsage" => cosmosdb_metrics.index_usage = latest_value,
                "DocumentCount" => cosmosdb_metrics.document_count = latest_value,
                "Availability" => cosmosdb_metrics.availability = latest_value,
                "ServiceAvailability" => cosmosdb_metrics.service_availability = latest_value,
                "DocumentQuota" => cosmosdb_metrics.document_quota = latest_value,
                "IndexQuota" => cosmosdb_metrics.index_quota = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(cosmosdb_metrics)
    }

    /// Collect metrics for multiple CosmosDB accounts in parallel
    pub async fn collect_metrics_batch(
        &self,
        accounts: &[AzureCosmosDbAccountId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureCosmosDbMetrics>> {
        let mut tasks = Vec::new();
        for account_id in accounts {
            let collector = self.clone();
            let account_id_clone = account_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&account_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure CosmosDB account: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureCosmosDbCollector {
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

/// Convert Azure CosmosDB metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_cosmosdb_metrics_to_reiver_format(
    metrics: &AzureCosmosDbMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("account_id:{}", metrics.account_id),
        format!("account_name:{}", metrics.account_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_cosmosdb".to_string(),
    ];

    if let Some(request_units) = metrics.total_request_units {
        reiver_metrics.push(ReiverMetric {
            name: "azure.cosmosdb.total_request_units".to_string(),
            value: request_units,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(requests) = metrics.total_requests {
        reiver_metrics.push(ReiverMetric {
            name: "azure.cosmosdb.total_requests".to_string(),
            value: requests,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(data_usage) = metrics.data_usage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.cosmosdb.data_usage".to_string(),
            value: data_usage,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(index_usage) = metrics.index_usage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.cosmosdb.index_usage".to_string(),
            value: index_usage,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(document_count) = metrics.document_count {
        reiver_metrics.push(ReiverMetric {
            name: "azure.cosmosdb.document_count".to_string(),
            value: document_count,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(availability) = metrics.availability {
        reiver_metrics.push(ReiverMetric {
            name: "azure.cosmosdb.availability".to_string(),
            value: availability,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(service_availability) = metrics.service_availability {
        reiver_metrics.push(ReiverMetric {
            name: "azure.cosmosdb.service_availability".to_string(),
            value: service_availability,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(document_quota) = metrics.document_quota {
        reiver_metrics.push(ReiverMetric {
            name: "azure.cosmosdb.document_quota".to_string(),
            value: document_quota,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(index_quota) = metrics.index_quota {
        reiver_metrics.push(ReiverMetric {
            name: "azure.cosmosdb.index_quota".to_string(),
            value: index_quota,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
