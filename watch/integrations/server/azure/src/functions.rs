//! Azure Functions integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure Functions metrics from Azure Monitor.
//! Metrics collected include:
//! - Function Execution Count
//! - Function Execution Units
//! - Function Execution Duration
//! - Function Errors
//! - Function Throttles

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure Function App identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureFunctionAppId(pub String);

/// Azure Functions metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureFunctionsMetrics {
    pub function_app_id: String,
    pub function_app_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub function_execution_count: Option<f64>,
    pub function_execution_units: Option<f64>,
    pub function_execution_duration: Option<f64>,
    pub function_errors: Option<f64>,
    pub function_throttles: Option<f64>,
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

/// Azure Functions metrics collector
pub struct AzureFunctionsCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureFunctionsCollector {
    /// Create a new Azure Functions collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Function Apps in the subscription
    pub async fn list_function_apps(&self) -> Result<Vec<AzureFunctionAppId>> {
        info!("Listing Azure Function Apps...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.Web/sites' and kind eq 'functionapp'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_function_apps = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure Function Apps: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract Function App resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_function_apps.push(AzureFunctionAppId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure Function Apps", all_function_apps.len());
        Ok(all_function_apps)
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

    /// Extract Function App name from Azure resource ID
    fn extract_function_app_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific Function App
    pub async fn collect_metrics(
        &self,
        function_app_id: &AzureFunctionAppId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureFunctionsMetrics> {
        info!("Collecting Azure Functions metrics for: {}", function_app_id.0);
        
        let function_app_name = Self::extract_function_app_name(&function_app_id.0);
        let resource_group = Self::extract_resource_group(&function_app_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for Function Apps
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            function_app_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "FunctionExecutionCount",
            "FunctionExecutionUnits",
            "FunctionExecutionDuration",
            "FunctionErrors",
            "FunctionThrottles",
        ];
        let metric_names_str = metric_names.join(",");
        
        // Format times for Azure Monitor API (ISO 8601)
        let timespan = format!("{}/{}", start_time.to_rfc3339(), end_time.to_rfc3339());
        let interval = "PT1M"; // 1-minute interval
        
        let url = format!(
            "{}&metricnames={}&timespan={}&interval={}&aggregation=Total",
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
        let mut function_metrics = AzureFunctionsMetrics {
            function_app_id: function_app_id.0.clone(),
            function_app_name: function_app_name.clone(),
            resource_group,
            timestamp: end_time,
            function_execution_count: None,
            function_execution_units: None,
            function_execution_duration: None,
            function_errors: None,
            function_throttles: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For counters, use total; for durations, use average
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    // Use total for counters, average for durations
                    if metric.name.value.contains("Duration") {
                        dp.average
                    } else {
                        dp.total
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "FunctionExecutionCount" => function_metrics.function_execution_count = latest_value,
                "FunctionExecutionUnits" => function_metrics.function_execution_units = latest_value,
                "FunctionExecutionDuration" => function_metrics.function_execution_duration = latest_value,
                "FunctionErrors" => function_metrics.function_errors = latest_value,
                "FunctionThrottles" => function_metrics.function_throttles = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(function_metrics)
    }

    /// Collect metrics for multiple Function Apps in parallel
    pub async fn collect_metrics_batch(
        &self,
        function_apps: &[AzureFunctionAppId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureFunctionsMetrics>> {
        let mut tasks = Vec::new();
        for function_app_id in function_apps {
            let collector = self.clone();
            let function_app_id_clone = function_app_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&function_app_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure Function App: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureFunctionsCollector {
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

/// Convert Azure Functions metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_functions_metrics_to_reiver_format(
    metrics: &AzureFunctionsMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("function_app_id:{}", metrics.function_app_id),
        format!("function_app_name:{}", metrics.function_app_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_functions".to_string(),
    ];

    if let Some(value) = metrics.function_execution_count {
        reiver_metrics.push(ReiverMetric {
            name: "azure.functions.execution_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.function_execution_units {
        reiver_metrics.push(ReiverMetric {
            name: "azure.functions.execution_units".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.function_execution_duration {
        reiver_metrics.push(ReiverMetric {
            name: "azure.functions.execution_duration".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.function_errors {
        reiver_metrics.push(ReiverMetric {
            name: "azure.functions.errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.function_throttles {
        reiver_metrics.push(ReiverMetric {
            name: "azure.functions.throttles".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
