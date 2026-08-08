//! Azure App Services integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure App Services metrics from Azure Monitor.
//! Metrics collected include:
//! - CPU Percentage
//! - Memory Percentage
//! - HTTP Server Errors
//! - HTTP Requests
//! - Average Response Time
//! - Data In
//! - Data Out
//! - Working Set

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure App Service identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureAppServiceId(pub String);

/// Azure App Services metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureAppServicesMetrics {
    pub app_service_id: String,
    pub app_service_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_percentage: Option<f64>,
    pub memory_percentage: Option<f64>,
    pub http_server_errors: Option<f64>,
    pub http_requests: Option<f64>,
    pub average_response_time: Option<f64>,
    pub bytes_received: Option<f64>,
    pub bytes_sent: Option<f64>,
    pub working_set: Option<f64>,
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

/// Azure App Services metrics collector
pub struct AzureAppServicesCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureAppServicesCollector {
    /// Create a new Azure App Services collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all App Services in the subscription
    pub async fn list_app_services(&self) -> Result<Vec<AzureAppServiceId>> {
        info!("Listing Azure App Services...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Filter for Microsoft.Web/sites (App Services)
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.Web/sites'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_app_services = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure App Services: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract App Service resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_app_services.push(AzureAppServiceId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure App Services", all_app_services.len());
        Ok(all_app_services)
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

    /// Extract App Service name from Azure resource ID
    fn extract_app_service_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific App Service
    pub async fn collect_metrics(
        &self,
        app_service_id: &AzureAppServiceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureAppServicesMetrics> {
        info!("Collecting Azure App Services metrics for: {}", app_service_id.0);
        
        let app_service_name = Self::extract_app_service_name(&app_service_id.0);
        let resource_group = Self::extract_resource_group(&app_service_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for App Services
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            app_service_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "CpuTime",              // CPU percentage (average)
            "MemoryPercentage",     // Memory percentage
            "Http5xx",              // HTTP 5xx server errors
            "Http4xx",              // HTTP 4xx client errors
            "Requests",             // HTTP requests
            "AverageResponseTime",  // Average response time
            "BytesReceived",        // Data in
            "BytesSent",            // Data out
            "WorkingSet",           // Working set memory
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
        let mut app_service_metrics = AzureAppServicesMetrics {
            app_service_id: app_service_id.0.clone(),
            app_service_name: app_service_name.clone(),
            resource_group,
            timestamp: end_time,
            cpu_percentage: None,
            memory_percentage: None,
            http_server_errors: None,
            http_requests: None,
            average_response_time: None,
            bytes_received: None,
            bytes_sent: None,
            working_set: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For percentages and averages, use average; for counts, use total
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    match metric.name.value.as_str() {
                        "CpuTime" | "MemoryPercentage" | "AverageResponseTime" | "WorkingSet" => dp.average,
                        "Http5xx" | "Http4xx" | "Requests" => dp.total,
                        "BytesReceived" | "BytesSent" => dp.total,
                        _ => dp.average,
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "CpuTime" => app_service_metrics.cpu_percentage = latest_value,
                "MemoryPercentage" => app_service_metrics.memory_percentage = latest_value,
                "Http5xx" => {
                    // Combine 4xx and 5xx errors as server errors
                    if let Some(existing) = app_service_metrics.http_server_errors {
                        app_service_metrics.http_server_errors = Some(existing + latest_value.unwrap_or(0.0));
                    } else {
                        app_service_metrics.http_server_errors = latest_value;
                    }
                }
                "Http4xx" => {
                    // Add 4xx to server errors count
                    if let Some(existing) = app_service_metrics.http_server_errors {
                        app_service_metrics.http_server_errors = Some(existing + latest_value.unwrap_or(0.0));
                    } else {
                        app_service_metrics.http_server_errors = latest_value;
                    }
                }
                "Requests" => app_service_metrics.http_requests = latest_value,
                "AverageResponseTime" => app_service_metrics.average_response_time = latest_value,
                "BytesReceived" => app_service_metrics.bytes_received = latest_value,
                "BytesSent" => app_service_metrics.bytes_sent = latest_value,
                "WorkingSet" => app_service_metrics.working_set = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(app_service_metrics)
    }

    /// Collect metrics for multiple App Services in parallel
    pub async fn collect_metrics_batch(
        &self,
        app_services: &[AzureAppServiceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureAppServicesMetrics>> {
        let mut tasks = Vec::new();
        for app_service_id in app_services {
            let collector = self.clone();
            let app_service_id_clone = app_service_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&app_service_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure App Service: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureAppServicesCollector {
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

/// Convert Azure App Services metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_app_services_metrics_to_reiver_format(
    metrics: &AzureAppServicesMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("app_service_id:{}", metrics.app_service_id),
        format!("app_service_name:{}", metrics.app_service_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_app_services".to_string(),
    ];

    if let Some(value) = metrics.cpu_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.app_services.cpu_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.memory_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.app_services.memory_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.http_server_errors {
        reiver_metrics.push(ReiverMetric {
            name: "azure.app_services.http_server_errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.http_requests {
        reiver_metrics.push(ReiverMetric {
            name: "azure.app_services.http_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.average_response_time {
        reiver_metrics.push(ReiverMetric {
            name: "azure.app_services.average_response_time".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.bytes_received {
        reiver_metrics.push(ReiverMetric {
            name: "azure.app_services.bytes_received".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.bytes_sent {
        reiver_metrics.push(ReiverMetric {
            name: "azure.app_services.bytes_sent".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.working_set {
        reiver_metrics.push(ReiverMetric {
            name: "azure.app_services.working_set".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
