//! Azure Event Hub integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure Event Hub metrics from Azure Monitor.
//! Metrics collected include:
//! - Incoming Messages
//! - Outgoing Messages
//! - Incoming Bytes
//! - Outgoing Bytes
//! - Captured Bytes
//! - Throttled Requests
//! - Server Errors
//! - User Errors
//! - Quota Exceeded Errors
//! - Active Connections

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure Event Hub Namespace identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureEventHubNamespaceId(pub String);

/// Azure Event Hub metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureEventHubMetrics {
    pub namespace_id: String,
    pub namespace_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub incoming_messages: Option<f64>,
    pub outgoing_messages: Option<f64>,
    pub incoming_bytes: Option<f64>,
    pub outgoing_bytes: Option<f64>,
    pub captured_bytes: Option<f64>,
    pub throttled_requests: Option<f64>,
    pub server_errors: Option<f64>,
    pub user_errors: Option<f64>,
    pub quota_exceeded_errors: Option<f64>,
    pub active_connections: Option<f64>,
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

/// Azure Event Hub metrics collector
pub struct AzureEventHubCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureEventHubCollector {
    /// Create a new Azure Event Hub collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Event Hub Namespaces in the subscription
    pub async fn list_namespaces(&self) -> Result<Vec<AzureEventHubNamespaceId>> {
        info!("Listing Azure Event Hub Namespaces...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.EventHub/namespaces'&api-version=2021-04-01",
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
                .map_err(|e| anyhow::anyhow!("Failed to list Azure Event Hub Namespaces: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract Event Hub Namespace resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_namespaces.push(AzureEventHubNamespaceId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure Event Hub Namespaces", all_namespaces.len());
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

    /// Extract Event Hub Namespace name from Azure resource ID
    fn extract_namespace_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Collect Azure Monitor metrics for a specific Event Hub Namespace
    pub async fn collect_metrics(
        &self,
        namespace_id: &AzureEventHubNamespaceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureEventHubMetrics> {
        info!("Collecting Azure Event Hub metrics for: {}", namespace_id.0);
        
        let namespace_name = Self::extract_namespace_name(&namespace_id.0);
        let resource_group = Self::extract_resource_group(&namespace_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for Event Hub Namespaces
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            namespace_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "IncomingMessages",        // Incoming messages count
            "OutgoingMessages",        // Outgoing messages count
            "IncomingBytes",           // Incoming bytes
            "OutgoingBytes",           // Outgoing bytes
            "CapturedBytes",           // Captured bytes (for capture feature)
            "ThrottledRequests",       // Throttled requests count
            "ServerErrors",            // Server errors count
            "UserErrors",              // User errors count
            "QuotaExceededErrors",     // Quota exceeded errors count
            "ActiveConnections",       // Active connections count
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
        let mut event_hub_metrics = AzureEventHubMetrics {
            namespace_id: namespace_id.0.clone(),
            namespace_name: namespace_name.clone(),
            resource_group,
            timestamp: end_time,
            incoming_messages: None,
            outgoing_messages: None,
            incoming_bytes: None,
            outgoing_bytes: None,
            captured_bytes: None,
            throttled_requests: None,
            server_errors: None,
            user_errors: None,
            quota_exceeded_errors: None,
            active_connections: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For counts, use total; for bytes/connections, use average
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    match metric.name.value.as_str() {
                        "IncomingMessages" | "OutgoingMessages" | "ThrottledRequests" 
                        | "ServerErrors" | "UserErrors" | "QuotaExceededErrors" => dp.total,
                        "IncomingBytes" | "OutgoingBytes" | "CapturedBytes" => dp.total,
                        "ActiveConnections" => dp.average,
                        _ => dp.average,
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "IncomingMessages" => event_hub_metrics.incoming_messages = latest_value,
                "OutgoingMessages" => event_hub_metrics.outgoing_messages = latest_value,
                "IncomingBytes" => event_hub_metrics.incoming_bytes = latest_value,
                "OutgoingBytes" => event_hub_metrics.outgoing_bytes = latest_value,
                "CapturedBytes" => event_hub_metrics.captured_bytes = latest_value,
                "ThrottledRequests" => event_hub_metrics.throttled_requests = latest_value,
                "ServerErrors" => event_hub_metrics.server_errors = latest_value,
                "UserErrors" => event_hub_metrics.user_errors = latest_value,
                "QuotaExceededErrors" => event_hub_metrics.quota_exceeded_errors = latest_value,
                "ActiveConnections" => event_hub_metrics.active_connections = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(event_hub_metrics)
    }

    /// Collect metrics for multiple Event Hub Namespaces in parallel
    pub async fn collect_metrics_batch(
        &self,
        namespaces: &[AzureEventHubNamespaceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureEventHubMetrics>> {
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
                    error!("Failed to collect metrics for Azure Event Hub Namespace: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureEventHubCollector {
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

/// Convert Azure Event Hub metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_event_hub_metrics_to_reiver_format(
    metrics: &AzureEventHubMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("namespace_id:{}", metrics.namespace_id),
        format!("namespace_name:{}", metrics.namespace_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_event_hub".to_string(),
    ];

    if let Some(value) = metrics.incoming_messages {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.incoming_messages".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.outgoing_messages {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.outgoing_messages".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.incoming_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.incoming_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.outgoing_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.outgoing_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.captured_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.captured_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.throttled_requests {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.throttled_requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.server_errors {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.server_errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.user_errors {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.user_errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.quota_exceeded_errors {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.quota_exceeded_errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.active_connections {
        reiver_metrics.push(ReiverMetric {
            name: "azure.event_hub.active_connections".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
