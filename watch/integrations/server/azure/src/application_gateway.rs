//! Azure Application Gateway integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure Application Gateway metrics from Azure Monitor.
//! Metrics collected include:
//! - Throughput
//! - Total Requests
//! - Healthy Host Count
//! - Unhealthy Host Count
//! - Current Connections
//! - Failed Requests
//! - Response Status
//! - Backend Connect Time

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure Application Gateway identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureApplicationGatewayId(pub String);

/// Azure Application Gateway metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureApplicationGatewayMetrics {
    pub gateway_id: String,
    pub gateway_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub throughput: Option<f64>,
    pub total_requests: Option<f64>,
    pub healthy_host_count: Option<f64>,
    pub unhealthy_host_count: Option<f64>,
    pub current_connections: Option<f64>,
    pub failed_requests: Option<f64>,
    pub response_status: Option<f64>,
    pub backend_connect_time: Option<f64>,
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

/// Azure Application Gateway metrics collector
pub struct AzureApplicationGatewayCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureApplicationGatewayCollector {
    /// Create a new Azure Application Gateway collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Application Gateways in the subscription
    pub async fn list_gateways(&self) -> Result<Vec<AzureApplicationGatewayId>> {
        info!("Listing Azure Application Gateways...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.Network/applicationGateways'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_gateways = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure Application Gateways: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract Application Gateway resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_gateways.push(AzureApplicationGatewayId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure Application Gateways", all_gateways.len());
        Ok(all_gateways)
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

    /// Extract Application Gateway name from Azure resource ID
    fn extract_gateway_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific Application Gateway
    pub async fn collect_metrics(
        &self,
        gateway_id: &AzureApplicationGatewayId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureApplicationGatewayMetrics> {
        info!("Collecting Azure Application Gateway metrics for: {}", gateway_id.0);
        
        let gateway_name = Self::extract_gateway_name(&gateway_id.0);
        let resource_group = Self::extract_resource_group(&gateway_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for Application Gateways
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            gateway_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "Throughput",              // Throughput in bytes per second
            "TotalRequests",           // Total requests count
            "HealthyHostCount",        // Healthy host count
            "UnhealthyHostCount",      // Unhealthy host count
            "CurrentConnections",      // Current connections count
            "FailedRequests",          // Failed requests count
            "ResponseStatus",          // Response status (aggregated)
            "BackendConnectTime",      // Backend connect time in milliseconds
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
        let mut gateway_metrics = AzureApplicationGatewayMetrics {
            gateway_id: gateway_id.0.clone(),
            gateway_name: gateway_name.clone(),
            resource_group,
            timestamp: end_time,
            throughput: None,
            total_requests: None,
            healthy_host_count: None,
            unhealthy_host_count: None,
            current_connections: None,
            failed_requests: None,
            response_status: None,
            backend_connect_time: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For counts, use total; for throughput/durations, use average
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    match metric.name.value.as_str() {
                        "TotalRequests" | "FailedRequests" => dp.total,
                        "Throughput" | "HealthyHostCount" | "UnhealthyHostCount" 
                        | "CurrentConnections" | "ResponseStatus" | "BackendConnectTime" => dp.average,
                        _ => dp.average,
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "Throughput" => gateway_metrics.throughput = latest_value,
                "TotalRequests" => gateway_metrics.total_requests = latest_value,
                "HealthyHostCount" => gateway_metrics.healthy_host_count = latest_value,
                "UnhealthyHostCount" => gateway_metrics.unhealthy_host_count = latest_value,
                "CurrentConnections" => gateway_metrics.current_connections = latest_value,
                "FailedRequests" => gateway_metrics.failed_requests = latest_value,
                "ResponseStatus" => gateway_metrics.response_status = latest_value,
                "BackendConnectTime" => gateway_metrics.backend_connect_time = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(gateway_metrics)
    }

    /// Collect metrics for multiple Application Gateways in parallel
    pub async fn collect_metrics_batch(
        &self,
        gateways: &[AzureApplicationGatewayId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureApplicationGatewayMetrics>> {
        let mut tasks = Vec::new();
        for gateway_id in gateways {
            let collector = self.clone();
            let gateway_id_clone = gateway_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&gateway_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure Application Gateway: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureApplicationGatewayCollector {
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

/// Convert Azure Application Gateway metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_application_gateway_metrics_to_reiver_format(
    metrics: &AzureApplicationGatewayMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("gateway_id:{}", metrics.gateway_id),
        format!("gateway_name:{}", metrics.gateway_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_application_gateway".to_string(),
    ];

    if let Some(value) = metrics.throughput {
        reiver_metrics.push(ReiverMetric {
            name: "azure.application_gateway.throughput".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.total_requests {
        reiver_metrics.push(ReiverMetric {
            name: "azure.application_gateway.total_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.healthy_host_count {
        reiver_metrics.push(ReiverMetric {
            name: "azure.application_gateway.healthy_host_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.unhealthy_host_count {
        reiver_metrics.push(ReiverMetric {
            name: "azure.application_gateway.unhealthy_host_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.current_connections {
        reiver_metrics.push(ReiverMetric {
            name: "azure.application_gateway.current_connections".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.failed_requests {
        reiver_metrics.push(ReiverMetric {
            name: "azure.application_gateway.failed_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.response_status {
        reiver_metrics.push(ReiverMetric {
            name: "azure.application_gateway.response_status".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.backend_connect_time {
        reiver_metrics.push(ReiverMetric {
            name: "azure.application_gateway.backend_connect_time".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
