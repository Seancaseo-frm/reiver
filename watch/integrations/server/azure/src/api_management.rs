//! Azure API Management integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure API Management metrics from Azure Monitor.
//! Metrics collected include:
//! - Requests
//! - Gateway Response Time
//! - Backend Duration
//! - Duration
//! - Capacity
//! - Event Hub Events
//! - Event Hub Failed Events
//! - Event Hub Dropped Events

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure API Management Service identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureApiManagementServiceId(pub String);

/// Azure API Management metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureApiManagementMetrics {
    pub service_id: String,
    pub service_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub requests: Option<f64>,
    pub gateway_response_time: Option<f64>,
    pub backend_duration: Option<f64>,
    pub duration: Option<f64>,
    pub capacity: Option<f64>,
    pub event_hub_events: Option<f64>,
    pub event_hub_failed_events: Option<f64>,
    pub event_hub_dropped_events: Option<f64>,
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

/// Azure API Management metrics collector
pub struct AzureApiManagementCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureApiManagementCollector {
    /// Create a new Azure API Management collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all API Management Services in the subscription
    pub async fn list_services(&self) -> Result<Vec<AzureApiManagementServiceId>> {
        info!("Listing Azure API Management Services...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.ApiManagement/service'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_services = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure API Management Services: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract API Management Service resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_services.push(AzureApiManagementServiceId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure API Management Services", all_services.len());
        Ok(all_services)
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

    /// Extract API Management Service name from Azure resource ID
    fn extract_service_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific API Management Service
    pub async fn collect_metrics(
        &self,
        service_id: &AzureApiManagementServiceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureApiManagementMetrics> {
        info!("Collecting Azure API Management metrics for: {}", service_id.0);
        
        let service_name = Self::extract_service_name(&service_id.0);
        let resource_group = Self::extract_resource_group(&service_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for API Management Services
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            service_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "Requests",                // Total requests count
            "GatewayResponseTime",     // Gateway response time in milliseconds
            "BackendDuration",         // Backend duration in milliseconds
            "Duration",                // Total duration in milliseconds
            "Capacity",                // Capacity percentage
            "EventHubEvents",          // Event Hub events count
            "EventHubFailedEvents",    // Event Hub failed events count
            "EventHubDroppedEvents",   // Event Hub dropped events count
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
        let mut api_management_metrics = AzureApiManagementMetrics {
            service_id: service_id.0.clone(),
            service_name: service_name.clone(),
            resource_group,
            timestamp: end_time,
            requests: None,
            gateway_response_time: None,
            backend_duration: None,
            duration: None,
            capacity: None,
            event_hub_events: None,
            event_hub_failed_events: None,
            event_hub_dropped_events: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For counts, use total; for durations/capacity, use average
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    match metric.name.value.as_str() {
                        "Requests" | "EventHubEvents" | "EventHubFailedEvents" | "EventHubDroppedEvents" => dp.total,
                        "GatewayResponseTime" | "BackendDuration" | "Duration" | "Capacity" => dp.average,
                        _ => dp.average,
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "Requests" => api_management_metrics.requests = latest_value,
                "GatewayResponseTime" => api_management_metrics.gateway_response_time = latest_value,
                "BackendDuration" => api_management_metrics.backend_duration = latest_value,
                "Duration" => api_management_metrics.duration = latest_value,
                "Capacity" => api_management_metrics.capacity = latest_value,
                "EventHubEvents" => api_management_metrics.event_hub_events = latest_value,
                "EventHubFailedEvents" => api_management_metrics.event_hub_failed_events = latest_value,
                "EventHubDroppedEvents" => api_management_metrics.event_hub_dropped_events = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(api_management_metrics)
    }

    /// Collect metrics for multiple API Management Services in parallel
    pub async fn collect_metrics_batch(
        &self,
        services: &[AzureApiManagementServiceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureApiManagementMetrics>> {
        let mut tasks = Vec::new();
        for service_id in services {
            let collector = self.clone();
            let service_id_clone = service_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&service_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure API Management Service: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureApiManagementCollector {
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

/// Convert Azure API Management metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_api_management_metrics_to_reiver_format(
    metrics: &AzureApiManagementMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("service_id:{}", metrics.service_id),
        format!("service_name:{}", metrics.service_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_api_management".to_string(),
    ];

    if let Some(value) = metrics.requests {
        reiver_metrics.push(ReiverMetric {
            name: "azure.api_management.requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.gateway_response_time {
        reiver_metrics.push(ReiverMetric {
            name: "azure.api_management.gateway_response_time".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.backend_duration {
        reiver_metrics.push(ReiverMetric {
            name: "azure.api_management.backend_duration".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.duration {
        reiver_metrics.push(ReiverMetric {
            name: "azure.api_management.duration".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.capacity {
        reiver_metrics.push(ReiverMetric {
            name: "azure.api_management.capacity".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.event_hub_events {
        reiver_metrics.push(ReiverMetric {
            name: "azure.api_management.event_hub_events".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.event_hub_failed_events {
        reiver_metrics.push(ReiverMetric {
            name: "azure.api_management.event_hub_failed_events".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.event_hub_dropped_events {
        reiver_metrics.push(ReiverMetric {
            name: "azure.api_management.event_hub_dropped_events".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
