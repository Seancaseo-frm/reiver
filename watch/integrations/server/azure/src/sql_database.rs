//! Azure SQL Database integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect Azure SQL Database metrics from Azure Monitor.
//! Metrics collected include:
//! - CPU Percentage (vCore-based)
//! - DTU Percentage (DTU-based)
//! - Database Connections
//! - Storage Percentage
//! - Data IO Percentage
//! - Log IO Percentage
//! - Deadlocks
//! - Failed Connections

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure SQL Database identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureSqlDatabaseId(pub String);

/// Azure SQL Database metrics collected from Azure Monitor
#[derive(Debug, Clone, Serialize)]
pub struct AzureSqlDatabaseMetrics {
    pub database_id: String,
    pub database_name: String,
    pub server_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_percentage: Option<f64>,
    pub dtu_percentage: Option<f64>,
    pub storage_percentage: Option<f64>,
    pub database_connections: Option<f64>,
    pub data_io_percentage: Option<f64>,
    pub log_io_percentage: Option<f64>,
    pub deadlocks: Option<f64>,
    pub failed_connections: Option<f64>,
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

/// Azure SQL Database metrics collector
pub struct AzureSqlDatabaseCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureSqlDatabaseCollector {
    /// Create a new Azure SQL Database collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all SQL Databases in the subscription
    pub async fn list_databases(&self) -> Result<Vec<AzureSqlDatabaseId>> {
        info!("Listing Azure SQL Databases...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.Sql/servers/databases'&api-version=2021-04-01",
            self.config.subscription_id
        );
        
        let mut all_databases = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure SQL Databases: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract SQL Database resource IDs
            // Filter out system databases (master, tempdb, etc.)
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        // Skip system databases (they typically don't have useful metrics)
                        let database_name = Self::extract_database_name(id);
                        if !Self::is_system_database(&database_name) {
                            all_databases.push(AzureSqlDatabaseId(id.to_string()));
                        }
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure SQL Databases", all_databases.len());
        Ok(all_databases)
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

    /// Extract database name from Azure resource ID
    /// Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Sql/servers/{server}/databases/{database}
    fn extract_database_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Extract server name from Azure resource ID
    /// Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Sql/servers/{server}/databases/{database}
    fn extract_server_name(resource_id: &str) -> Option<String> {
        let parts: Vec<&str> = resource_id.split('/').collect();
        // Find the index of "servers" and return the next part
        for (i, part) in parts.iter().enumerate() {
            if part == &"servers" && i + 1 < parts.len() {
                return Some(parts[i + 1].to_string());
            }
        }
        None
    }

    /// Check if database is a system database
    fn is_system_database(database_name: &str) -> bool {
        let name_lower = database_name.to_lowercase();
        name_lower == "master"
            || name_lower == "tempdb"
            || name_lower == "model"
            || name_lower == "msdb"
    }

    /// Collect Azure Monitor metrics for a specific SQL Database
    pub async fn collect_metrics(
        &self,
        database_id: &AzureSqlDatabaseId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureSqlDatabaseMetrics> {
        info!("Collecting Azure SQL Database metrics for: {}", database_id.0);
        
        let database_name = Self::extract_database_name(&database_id.0);
        let server_name = Self::extract_server_name(&database_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        let resource_group = Self::extract_resource_group(&database_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // Build metrics API URL
        // Azure Monitor Metrics API format for SQL Databases
        let metrics_url = format!(
            "https://management.azure.com{}/providers/microsoft.insights/metrics?api-version=2021-05-01",
            database_id.0
        );
        
        // Build metric names list (Azure Monitor supports multiple metrics in one call)
        let metric_names = vec![
            "cpu_percent",                    // CPU percentage
            "dtu_consumption_percent",        // DTU percentage
            "storage_percent",                // Storage percentage
            "connection_successful",          // Successful connections
            "connection_failed",              // Failed connections
            "data_io_percent",                // Data IO percentage
            "log_write_percent",              // Log IO percentage
            "deadlock",                       // Deadlocks
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
        let mut database_metrics = AzureSqlDatabaseMetrics {
            database_id: database_id.0.clone(),
            database_name: database_name.clone(),
            server_name: server_name.clone(),
            resource_group,
            timestamp: end_time,
            cpu_percentage: None,
            dtu_percentage: None,
            storage_percentage: None,
            database_connections: None,
            data_io_percentage: None,
            log_io_percentage: None,
            deadlocks: None,
            failed_connections: None,
        };
        
        for metric in metrics_response.value {
            // Get the latest data point from the timeseries
            // For percentages, use average; for counts (deadlocks, connections), use total
            let latest_value = metric.timeseries
                .iter()
                .flat_map(|ts| ts.data.iter())
                .filter_map(|dp| {
                    match metric.name.value.as_str() {
                        "cpu_percent" | "dtu_consumption_percent" | "storage_percent" 
                        | "data_io_percent" | "log_write_percent" => dp.average,
                        "connection_successful" | "connection_failed" | "deadlock" => dp.total,
                        _ => dp.average,
                    }
                })
                .last();
            
            match metric.name.value.as_str() {
                "cpu_percent" => database_metrics.cpu_percentage = latest_value,
                "dtu_consumption_percent" => database_metrics.dtu_percentage = latest_value,
                "storage_percent" => database_metrics.storage_percentage = latest_value,
                "connection_successful" => database_metrics.database_connections = latest_value,
                "connection_failed" => database_metrics.failed_connections = latest_value,
                "data_io_percent" => database_metrics.data_io_percentage = latest_value,
                "log_write_percent" => database_metrics.log_io_percentage = latest_value,
                "deadlock" => database_metrics.deadlocks = latest_value,
                _ => {
                    warn!("Unknown metric: {}", metric.name.value);
                }
            }
        }
        
        Ok(database_metrics)
    }

    /// Collect metrics for multiple SQL Databases in parallel
    pub async fn collect_metrics_batch(
        &self,
        databases: &[AzureSqlDatabaseId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureSqlDatabaseMetrics>> {
        let mut tasks = Vec::new();
        for database_id in databases {
            let collector = self.clone();
            let database_id_clone = database_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&database_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure SQL Database: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureSqlDatabaseCollector {
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

/// Convert Azure SQL Database metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_sql_database_metrics_to_reiver_format(
    metrics: &AzureSqlDatabaseMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("database_id:{}", metrics.database_id),
        format!("database_name:{}", metrics.database_name),
        format!("server_name:{}", metrics.server_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_sql_database".to_string(),
    ];

    if let Some(value) = metrics.cpu_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.sql_database.cpu_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.dtu_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.sql_database.dtu_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.storage_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.sql_database.storage_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.database_connections {
        reiver_metrics.push(ReiverMetric {
            name: "azure.sql_database.connections".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.data_io_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.sql_database.data_io_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.log_io_percentage {
        reiver_metrics.push(ReiverMetric {
            name: "azure.sql_database.log_io_percentage".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.deadlocks {
        reiver_metrics.push(ReiverMetric {
            name: "azure.sql_database.deadlocks".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.failed_connections {
        reiver_metrics.push(ReiverMetric {
            name: "azure.sql_database.failed_connections".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
