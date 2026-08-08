//! Azure Synapse Analytics integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure Synapse Analytics metrics from Azure Monitor.
//! Metrics collected include:
//! - SQL Requests (ended, succeeded, failed, canceled)
//! - Query Duration
//! - Data Processed (bytes)
//! - SQL Pool Active Queries
//! - SQL Pool Queued Queries
//! - SQL Pool Active Requests
//! - SQL Pool Queued Requests
//! - SQL Pool DWU Used
//! - SQL Pool DWU Percentage

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure Synapse Workspace identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureSynapseWorkspaceId(pub String);

/// Azure Synapse Analytics metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureSynapseAnalyticsMetrics {
    pub workspace_id: String,
    pub workspace_name: String,
    pub resource_group: String,
    pub sql_pool_name: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub sql_requests_ended: Option<f64>,
    pub sql_requests_succeeded: Option<f64>,
    pub sql_requests_failed: Option<f64>,
    pub sql_requests_canceled: Option<f64>,
    pub query_duration: Option<f64>,
    pub data_processed_bytes: Option<f64>,
    pub sql_pool_active_queries: Option<f64>,
    pub sql_pool_queued_queries: Option<f64>,
    pub sql_pool_active_requests: Option<f64>,
    pub sql_pool_queued_requests: Option<f64>,
    pub sql_pool_dwu_used: Option<f64>,
    pub sql_pool_dwu_percentage: Option<f64>,
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

/// Azure Synapse Analytics metrics collector
pub struct AzureSynapseAnalyticsCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureSynapseAnalyticsCollector {
    /// Create a new Azure Synapse Analytics collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Synapse Workspaces in the subscription
    pub async fn list_workspaces(&self) -> Result<Vec<(AzureSynapseWorkspaceId, String, String)>> {
        info!("Listing Azure Synapse workspaces...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Use Azure Resource Manager API to list Synapse workspaces
        let url = format!(
            "https://management.azure.com/subscriptions/{}/providers/Microsoft.Synapse/workspaces?api-version=2021-06-01",
            self.config.subscription_id
        );
        
        let response = self.http_client
            .get(&url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list Synapse workspaces: {}", e))?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
        }
        
        #[derive(Debug, Deserialize)]
        struct AzureResourceListResponse {
            value: Vec<AzureResource>,
        }
        
        #[derive(Debug, Deserialize)]
        struct AzureResource {
            id: String,
            name: String,
            #[serde(rename = "resourceGroup")]
            resource_group: String,
        }
        
        let resource_list: AzureResourceListResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
        
        let workspaces: Vec<(AzureSynapseWorkspaceId, String, String)> = resource_list.value
            .into_iter()
            .map(|resource| {
                (AzureSynapseWorkspaceId(resource.id.clone()), resource.name, resource.resource_group)
            })
            .collect();
        
        info!("Found {} Azure Synapse workspaces", workspaces.len());
        Ok(workspaces)
    }

    /// Collect Azure Monitor metrics for a specific Synapse workspace
    pub async fn collect_metrics(
        &self,
        workspace_id: &AzureSynapseWorkspaceId,
        workspace_name: &str,
        resource_group: &str,
        sql_pool_name: Option<&str>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureSynapseAnalyticsMetrics> {
        info!("Collecting Synapse Analytics metrics for workspace: {}", workspace_name);
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build Azure Monitor Metrics API URL
        let url = format!(
            "https://management.azure.com{}/providers/Microsoft.Insights/metrics?api-version=2018-01-01",
            workspace_id.0
        );
        
        // Metrics to collect for Synapse Analytics
        let metrics_to_collect = vec![
            "sql_requests_ended",
            "sql_requests_succeeded",
            "sql_requests_failed",
            "sql_requests_canceled",
            "query_duration",
            "data_processed_bytes",
            "sql_pool_active_queries",
            "sql_pool_queued_queries",
            "sql_pool_active_requests",
            "sql_pool_queued_requests",
            "sql_pool_dwu_used",
            "sql_pool_dwu_percentage",
        ];
        
        let mut synapse_metrics = AzureSynapseAnalyticsMetrics {
            workspace_id: workspace_id.0.clone(),
            workspace_name: workspace_name.to_string(),
            resource_group: resource_group.to_string(),
            sql_pool_name: sql_pool_name.map(|s| s.to_string()),
            timestamp: end_time,
            sql_requests_ended: None,
            sql_requests_succeeded: None,
            sql_requests_failed: None,
            sql_requests_canceled: None,
            query_duration: None,
            data_processed_bytes: None,
            sql_pool_active_queries: None,
            sql_pool_queued_queries: None,
            sql_pool_active_requests: None,
            sql_pool_queued_requests: None,
            sql_pool_dwu_used: None,
            sql_pool_dwu_percentage: None,
        };
        
        // Build metric filter
        let metric_names = metrics_to_collect.join(",");
        let timespan = format!("{}/{}", start_time.to_rfc3339(), end_time.to_rfc3339());
        let interval = "PT1M"; // 1-minute intervals
        
        let request_url = format!(
            "{}&timespan={}&interval={}&metricnames={}&aggregation=Average,Total,Count",
            url, timespan, interval, metric_names
        );
        
        let response = self.http_client
            .get(&request_url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .send()
            .await;
        
        match response {
            Ok(resp) => {
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!("Failed to get Synapse Analytics metrics: {} - {}", status, body);
                    return Ok(synapse_metrics);
                }
                
                let metrics_response: AzureMonitorMetricsResponse = match resp.json().await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Failed to parse Synapse Analytics metrics response: {}", e);
                        return Ok(synapse_metrics);
                    }
                };
                
                // Extract metric values
                for metric in metrics_response.value {
                    let metric_name = metric.name.value.as_str();
                    let value = self.extract_latest_metric_value(&metric.timeseries);
                    
                    match metric_name {
                        "sql_requests_ended" => synapse_metrics.sql_requests_ended = value,
                        "sql_requests_succeeded" => synapse_metrics.sql_requests_succeeded = value,
                        "sql_requests_failed" => synapse_metrics.sql_requests_failed = value,
                        "sql_requests_canceled" => synapse_metrics.sql_requests_canceled = value,
                        "query_duration" => synapse_metrics.query_duration = value,
                        "data_processed_bytes" => synapse_metrics.data_processed_bytes = value,
                        "sql_pool_active_queries" => synapse_metrics.sql_pool_active_queries = value,
                        "sql_pool_queued_queries" => synapse_metrics.sql_pool_queued_queries = value,
                        "sql_pool_active_requests" => synapse_metrics.sql_pool_active_requests = value,
                        "sql_pool_queued_requests" => synapse_metrics.sql_pool_queued_requests = value,
                        "sql_pool_dwu_used" => synapse_metrics.sql_pool_dwu_used = value,
                        "sql_pool_dwu_percentage" => synapse_metrics.sql_pool_dwu_percentage = value,
                        _ => {
                            // Unknown metric, skip
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to get Synapse Analytics metrics: {}", e);
            }
        }
        
        Ok(synapse_metrics)
    }

    /// Extract the latest metric value from time series
    fn extract_latest_metric_value(&self, timeseries: &[AzureMonitorTimeSeries]) -> Option<f64> {
        let mut latest_value: Option<f64> = None;
        let mut latest_timestamp: Option<DateTime<Utc>> = None;
        
        for series in timeseries {
            for data_point in &series.data {
                if let Ok(timestamp) = DateTime::parse_from_rfc3339(&data_point.timeStamp) {
                    let timestamp_utc = timestamp.with_timezone(&Utc);
                    
                    if latest_timestamp.is_none() || timestamp_utc > latest_timestamp.unwrap() {
                        latest_timestamp = Some(timestamp_utc);
                        
                        // Prefer average, fall back to total or count
                        if let Some(avg) = data_point.average {
                            latest_value = Some(avg);
                        } else if let Some(total) = data_point.total {
                            latest_value = Some(total);
                        } else if let Some(count) = data_point.count {
                            latest_value = Some(count);
                        }
                    }
                }
            }
        }
        
        latest_value
    }

    /// Collect Azure Monitor metrics for multiple Synapse workspaces
    pub async fn collect_metrics_batch(
        &self,
        workspaces: &[(AzureSynapseWorkspaceId, String, String)],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureSynapseAnalyticsMetrics>> {
        let mut metrics = Vec::new();

        for (workspace_id, workspace_name, resource_group) in workspaces {
            match self.collect_metrics(workspace_id, workspace_name, resource_group, None, start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Synapse workspace {}: {}", workspace_name, e);
                }
            }
        }

        Ok(metrics)
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

/// Convert Azure Synapse Analytics metrics to Reiver format
pub fn azure_synapse_analytics_metrics_to_reiver_format(
    metrics: &AzureSynapseAnalyticsMetrics,
    project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let mut base_tags = vec![
        format!("project_id:{}", project_id),
        format!("workspace_id:{}", metrics.workspace_id),
        format!("workspace_name:{}", metrics.workspace_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_monitor".to_string(),
        "service:synapse_analytics".to_string(),
    ];
    
    if let Some(ref sql_pool) = metrics.sql_pool_name {
        base_tags.push(format!("sql_pool:{}", sql_pool));
    }

    let mut add_metric = |name: &str, value: Option<f64>, metric_type: &str| {
        if let Some(v) = value {
            reiver_metrics.push(ReiverMetric {
                name: format!("azure.synapse_analytics.{}", name),
                value: v,
                r#type: metric_type.to_string(),
                timestamp: metrics.timestamp,
                tags: base_tags.clone(),
            });
        }
    };

    add_metric("sql_requests_ended", metrics.sql_requests_ended, "counter");
    add_metric("sql_requests_succeeded", metrics.sql_requests_succeeded, "counter");
    add_metric("sql_requests_failed", metrics.sql_requests_failed, "counter");
    add_metric("sql_requests_canceled", metrics.sql_requests_canceled, "counter");
    add_metric("query_duration", metrics.query_duration, "gauge");
    add_metric("data_processed_bytes", metrics.data_processed_bytes, "gauge");
    add_metric("sql_pool_active_queries", metrics.sql_pool_active_queries, "gauge");
    add_metric("sql_pool_queued_queries", metrics.sql_pool_queued_queries, "gauge");
    add_metric("sql_pool_active_requests", metrics.sql_pool_active_requests, "gauge");
    add_metric("sql_pool_queued_requests", metrics.sql_pool_queued_requests, "gauge");
    add_metric("sql_pool_dwu_used", metrics.sql_pool_dwu_used, "gauge");
    add_metric("sql_pool_dwu_percentage", metrics.sql_pool_dwu_percentage, "gauge");

    reiver_metrics
}
