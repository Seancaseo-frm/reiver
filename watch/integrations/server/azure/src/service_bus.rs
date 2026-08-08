//! Azure Service Bus integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure Service Bus metrics from Azure Monitor.
//! Metrics collected include:
//! - Active Messages
//! - Dead Letter Messages
//! - Incoming Messages
//! - Outgoing Messages
//! - Size
//! - User Errors
//! - Server Errors
//! - Complete Messages
//! - Abandon Messages

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure Service Bus Namespace identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureServiceBusNamespaceId(pub String);

/// Azure Service Bus metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureServiceBusMetrics {
    pub namespace_id: String,
    pub namespace_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub active_messages: Option<f64>,
    pub dead_letter_messages: Option<f64>,
    pub incoming_messages: Option<f64>,
    pub outgoing_messages: Option<f64>,
    pub size: Option<f64>,
    pub user_errors: Option<f64>,
    pub server_errors: Option<f64>,
    pub complete_messages: Option<f64>,
    pub abandon_messages: Option<f64>,
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

/// Azure Service Bus metrics collector
pub struct AzureServiceBusCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureServiceBusCollector {
    /// Create a new Azure Service Bus collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Service Bus Namespaces in the subscription
    pub async fn list_namespaces(&self) -> Result<Vec<AzureServiceBusNamespaceId>> {
        info!("Listing Azure Service Bus Namespaces...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.ServiceBus/namespaces'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_namespaces = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure Service Bus Namespaces: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract Service Bus Namespace resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_namespaces.push(AzureServiceBusNamespaceId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure Service Bus Namespaces", all_namespaces.len());
        Ok(all_namespaces)
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

    /// Extract Service Bus Namespace name from Azure resource ID
    fn extract_namespace_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific Service Bus Namespace
    pub async fn collect_metrics(
        &self,
        namespace_id: &AzureServiceBusNamespaceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureServiceBusMetrics> {
        info!("Collecting Azure Service Bus metrics for: {}", namespace_id.0);
        
        let namespace_name = Self::extract_namespace_name(&namespace_id.0);
        let resource_group = Self::extract_resource_group(&namespace_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for Service Bus Namespaces
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            namespace_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "ActiveMessages",          // Active messages count
            "DeadletterMessages",      // Dead letter messages count
            "IncomingMessages",        // Incoming messages count
            "OutgoingMessages",        // Outgoing messages count
            "Size",                    // Size of messages
            "UserErrors",              // User errors count
            "ServerErrors",            // Server errors count
            "CompleteMessages",        // Complete messages count
            "AbandonMessages",         // Abandon messages count
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
        let mut service_bus_metrics = AzureServiceBusMetrics {
            namespace_id: namespace_id.0.clone(),
            namespace_name: namespace_name.clone(),
            resource_group,
            timestamp: end_time,
            active_messages: None,
            dead_letter_messages: None,
            incoming_messages: None,
            outgoing_messages: None,
            size: None,
            user_errors: None,
            server_errors: None,
            complete_messages: None,
            abandon_messages: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For counts, use total; for sizes, use average
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    match metric.name.value.as_str() {
                        "ActiveMessages" | "DeadletterMessages" | "IncomingMessages" 
                        | "OutgoingMessages" | "UserErrors" | "ServerErrors" 
                        | "CompleteMessages" | "AbandonMessages" => dp.total,
                        "Size" => dp.average,
                        _ => dp.average,
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "ActiveMessages" => service_bus_metrics.active_messages = latest_value,
                "DeadletterMessages" => service_bus_metrics.dead_letter_messages = latest_value,
                "IncomingMessages" => service_bus_metrics.incoming_messages = latest_value,
                "OutgoingMessages" => service_bus_metrics.outgoing_messages = latest_value,
                "Size" => service_bus_metrics.size = latest_value,
                "UserErrors" => service_bus_metrics.user_errors = latest_value,
                "ServerErrors" => service_bus_metrics.server_errors = latest_value,
                "CompleteMessages" => service_bus_metrics.complete_messages = latest_value,
                "AbandonMessages" => service_bus_metrics.abandon_messages = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(service_bus_metrics)
    }

    /// Collect metrics for multiple Service Bus Namespaces in parallel
    pub async fn collect_metrics_batch(
        &self,
        namespaces: &[AzureServiceBusNamespaceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureServiceBusMetrics>> {
        let mut tasks = Vec::new();
        for namespace_id in namespaces {
            let collector = self.clone();
            let namespace_id_clone = namespace_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&namespace_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure Service Bus Namespace: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureServiceBusCollector {
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

/// Convert Azure Service Bus metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_service_bus_metrics_to_reiver_format(
    metrics: &AzureServiceBusMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("namespace_id:{}", metrics.namespace_id),
        format!("namespace_name:{}", metrics.namespace_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_service_bus".to_string(),
    ];

    if let Some(value) = metrics.active_messages {
        reiver_metrics.push(ReiverMetric {
            name: "azure.service_bus.active_messages".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.dead_letter_messages {
        reiver_metrics.push(ReiverMetric {
            name: "azure.service_bus.dead_letter_messages".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.incoming_messages {
        reiver_metrics.push(ReiverMetric {
            name: "azure.service_bus.incoming_messages".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.outgoing_messages {
        reiver_metrics.push(ReiverMetric {
            name: "azure.service_bus.outgoing_messages".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.size {
        reiver_metrics.push(ReiverMetric {
            name: "azure.service_bus.size".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.user_errors {
        reiver_metrics.push(ReiverMetric {
            name: "azure.service_bus.user_errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.server_errors {
        reiver_metrics.push(ReiverMetric {
            name: "azure.service_bus.server_errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.complete_messages {
        reiver_metrics.push(ReiverMetric {
            name: "azure.service_bus.complete_messages".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.abandon_messages {
        reiver_metrics.push(ReiverMetric {
            name: "azure.service_bus.abandon_messages".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
