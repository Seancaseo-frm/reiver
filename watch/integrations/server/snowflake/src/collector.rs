//! Snowflake integration for collecting metrics from system tables
//!
//! This module provides functionality to collect Snowflake metrics from system tables.
//! Metrics collected include:
//! - Warehouse usage (running, queued, blocked)
//! - Query execution metrics
//! - Credit consumption
//! - Storage usage
//! - Warehouse metering history

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snowflake_connector_rs::{SnowflakeClient, SnowflakeAuthMethod, SnowflakeClientConfig};
use std::time::Duration;
use tracing::{error, info, warn};

use crate::config::SnowflakeConfig;

/// Snowflake account identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnowflakeAccountId(pub String);

/// Snowflake metrics collected from system tables
#[derive(Debug, Clone, Serialize)]
pub struct SnowflakeMetrics {
    pub account_id: String,
    pub timestamp: DateTime<Utc>,
    pub warehouse_name: Option<String>,
    pub warehouse_running: Option<f64>,
    pub warehouse_queued_load: Option<f64>,
    pub warehouse_queued_provisioning: Option<f64>,
    pub warehouse_blocked: Option<f64>,
    pub credits_used_compute: Option<f64>,
    pub credits_used_cloud_services: Option<f64>,
    pub queries_executed: Option<f64>,
    pub queries_failed: Option<f64>,
    pub storage_used_bytes: Option<f64>,
    pub storage_used_tables: Option<f64>,
    pub storage_used_stages: Option<f64>,
}

/// Snowflake metrics collector
pub struct SnowflakeCollector {
    config: SnowflakeConfig,
}

impl SnowflakeCollector {
    /// Create a new Snowflake collector with the given configuration
    pub fn new(config: SnowflakeConfig) -> Self {
        Self { config }
    }

    /// Create a Snowflake client and session
    async fn create_session(&self) -> Result<snowflake_connector_rs::SnowflakeSession> {
        let account = &self.config.account;
        let username = &self.config.username;
        let password = &self.config.password;
        
        // Build client config
        let client_config = SnowflakeClientConfig {
            account: account.clone(),
            role: self.config.role.clone(),
            warehouse: self.config.warehouse.clone(),
            database: self.config.database.clone(),
            schema: self.config.schema.clone(),
            timeout: Some(Duration::from_secs(30)),
        };
        
        info!("Connecting to Snowflake account: {}", account);
        
        let client = SnowflakeClient::new(
            username,
            SnowflakeAuthMethod::Password(password.clone()),
            client_config,
        )
        .map_err(|e| anyhow::anyhow!("Failed to create Snowflake client: {}", e))?;
        
        let session = client
            .create_session()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create Snowflake session: {}", e))?;
        
        info!("Connected to Snowflake successfully");
        Ok(session)
    }

    /// List all warehouses in the account
    pub async fn list_warehouses(&self) -> Result<Vec<String>> {
        let session = self.create_session().await?;
        
        // Query to list all warehouses
        let query = "SHOW WAREHOUSES";
        
        let rows = session
            .query(query)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list warehouses: {}", e))?;
        
        let mut warehouses = Vec::new();
        for row in rows {
            // SHOW WAREHOUSES returns a result set with a "name" column
            // Try to extract warehouse names from different possible column names
            if let Ok(name) = row.get::<String>("name") {
                warehouses.push(name);
            } else if let Ok(name) = row.get::<String>("WAREHOUSE_NAME") {
                warehouses.push(name);
            } else if let Ok(name) = row.get::<String>("NAME") {
                warehouses.push(name);
            }
        }
        
        if warehouses.is_empty() {
            warn!("No warehouses found in Snowflake account");
        } else {
            info!("Found {} warehouses", warehouses.len());
        }
        
        Ok(warehouses)
    }

    /// Collect metrics for a specific warehouse or all warehouses
    pub async fn collect_metrics(
        &self,
        warehouse_name: Option<&str>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<SnowflakeMetrics> {
        let session = self.create_session().await?;
        
        let account_id = self.config.account.clone();
        let timestamp = end_time;
        
        let mut metrics = SnowflakeMetrics {
            account_id: account_id.clone(),
            timestamp,
            warehouse_name: warehouse_name.map(|s| s.to_string()),
            warehouse_running: None,
            warehouse_queued_load: None,
            warehouse_queued_provisioning: None,
            warehouse_blocked: None,
            credits_used_compute: None,
            credits_used_cloud_services: None,
            queries_executed: None,
            queries_failed: None,
            storage_used_bytes: None,
            storage_used_tables: None,
            storage_used_stages: None,
        };
        
        // Query warehouse load history from INFORMATION_SCHEMA
        if let Some(warehouse) = warehouse_name {
            let warehouse_filter = format!("WAREHOUSE_NAME = '{}'", warehouse);
            metrics.warehouse_running = self.query_warehouse_load_history(
                &session,
                &warehouse_filter,
                "AVG_RUNNING",
                start_time,
                end_time,
            ).await?;
            
            metrics.warehouse_queued_load = self.query_warehouse_load_history(
                &session,
                &warehouse_filter,
                "AVG_QUEUED_LOAD",
                start_time,
                end_time,
            ).await?;
            
            metrics.warehouse_queued_provisioning = self.query_warehouse_load_history(
                &session,
                &warehouse_filter,
                "AVG_QUEUED_PROVISIONING",
                start_time,
                end_time,
            ).await?;
            
            metrics.warehouse_blocked = self.query_warehouse_load_history(
                &session,
                &warehouse_filter,
                "AVG_BLOCKED",
                start_time,
                end_time,
            ).await?;
        }
        
        // Query warehouse metering history from ACCOUNT_USAGE
        if let Some(warehouse) = warehouse_name {
            let warehouse_filter = format!("WAREHOUSE_NAME = '{}'", warehouse);
            metrics.credits_used_compute = self.query_warehouse_metering(
                &session,
                &warehouse_filter,
                "CREDITS_USED_COMPUTE",
                start_time,
                end_time,
            ).await?;
            
            metrics.credits_used_cloud_services = self.query_warehouse_metering(
                &session,
                &warehouse_filter,
                "CREDITS_USED_CLOUD_SERVICES",
                start_time,
                end_time,
            ).await?;
        }
        
        // Query query history from ACCOUNT_USAGE
        if let Some(warehouse) = warehouse_name {
            let warehouse_filter = format!("WAREHOUSE_NAME = '{}'", warehouse);
            metrics.queries_executed = self.query_query_history(
                &session,
                &warehouse_filter,
                start_time,
                end_time,
                false,
            ).await?;
            
            metrics.queries_failed = self.query_query_history(
                &session,
                &warehouse_filter,
                start_time,
                end_time,
                true,
            ).await?;
        }
        
        // Query storage usage
        metrics.storage_used_bytes = self.query_storage_usage(&session).await?;
        metrics.storage_used_tables = self.query_storage_tables(&session).await?;
        metrics.storage_used_stages = self.query_storage_stages(&session).await?;
        
        Ok(metrics)
    }

    /// Query warehouse load history from INFORMATION_SCHEMA
    async fn query_warehouse_load_history(
        &self,
        session: &snowflake_connector_rs::SnowflakeSession,
        warehouse_filter: &str,
        metric_column: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Option<f64>> {
        // Use WAREHOUSE_LOAD_HISTORY table function
        let query = format!(
            "SELECT AVG({}) as avg_value
             FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY(
                 DATE_RANGE_START => '{}',
                 DATE_RANGE_END => '{}'
             ))
             WHERE {}",
            metric_column,
            start_time.format("%Y-%m-%d %H:%M:%S"),
            end_time.format("%Y-%m-%d %H:%M:%S"),
            warehouse_filter
        );
        
        match session.query(query.as_str()).await {
            Ok(rows) => {
                if let Some(row) = rows.first() {
                    if let Ok(value) = row.get::<f64>("avg_value") {
                        return Ok(Some(value));
                    } else if let Ok(value_str) = row.get::<String>("avg_value") {
                        if let Ok(value) = value_str.parse::<f64>() {
                            return Ok(Some(value));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to query warehouse load history: {}", e);
                Ok(None)
            }
        }
    }

    /// Query warehouse metering history from ACCOUNT_USAGE
    async fn query_warehouse_metering(
        &self,
        session: &snowflake_connector_rs::SnowflakeSession,
        warehouse_filter: &str,
        metric_column: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Option<f64>> {
        let query = format!(
            "SELECT SUM({}) as total_value
             FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
             WHERE START_TIME >= '{}' AND END_TIME <= '{}'
             AND {}",
            metric_column,
            start_time.format("%Y-%m-%d %H:%M:%S"),
            end_time.format("%Y-%m-%d %H:%M:%S"),
            warehouse_filter
        );
        
        match session.query(query.as_str()).await {
            Ok(rows) => {
                if let Some(row) = rows.first() {
                    if let Ok(value) = row.get::<f64>("total_value") {
                        return Ok(Some(value));
                    } else if let Ok(value_str) = row.get::<String>("total_value") {
                        if let Ok(value) = value_str.parse::<f64>() {
                            return Ok(Some(value));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to query warehouse metering: {}", e);
                Ok(None)
            }
        }
    }

    /// Query query history from ACCOUNT_USAGE
    async fn query_query_history(
        &self,
        session: &snowflake_connector_rs::SnowflakeSession,
        warehouse_filter: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        failed_only: bool,
    ) -> Result<Option<f64>> {
        let status_filter = if failed_only {
            "AND ERROR_CODE IS NOT NULL"
        } else {
            ""
        };
        
        let query = format!(
            "SELECT COUNT(*) as query_count
             FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
             WHERE START_TIME >= '{}' AND END_TIME <= '{}'
             AND {}
             {}",
            start_time.format("%Y-%m-%d %H:%M:%S"),
            end_time.format("%Y-%m-%d %H:%M:%S"),
            warehouse_filter,
            status_filter
        );
        
        match session.query(query.as_str()).await {
            Ok(rows) => {
                if let Some(row) = rows.first() {
                    if let Ok(value) = row.get::<i64>("query_count") {
                        return Ok(Some(value as f64));
                    } else if let Ok(value_str) = row.get::<String>("query_count") {
                        if let Ok(value) = value_str.parse::<f64>() {
                            return Ok(Some(value));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to query query history: {}", e);
                Ok(None)
            }
        }
    }

    /// Query storage usage
    async fn query_storage_usage(&self, session: &snowflake_connector_rs::SnowflakeSession) -> Result<Option<f64>> {
        let query = "SELECT SUM(AVERAGE_DATABASE_BYTES + AVERAGE_FAILSAFE_BYTES) as total_bytes
                      FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
                      WHERE USAGE_DATE = CURRENT_DATE()";
        
        match session.query(query).await {
            Ok(rows) => {
                if let Some(row) = rows.first() {
                    if let Ok(value) = row.get::<f64>("total_bytes") {
                        return Ok(Some(value));
                    } else if let Ok(value_str) = row.get::<String>("total_bytes") {
                        if let Ok(value) = value_str.parse::<f64>() {
                            return Ok(Some(value));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to query storage usage: {}", e);
                Ok(None)
            }
        }
    }

    /// Query storage used by tables
    async fn query_storage_tables(&self, session: &snowflake_connector_rs::SnowflakeSession) -> Result<Option<f64>> {
        let query = "SELECT SUM(BYTES) as total_bytes
                      FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
                      WHERE DELETED IS NULL";
        
        match session.query(query).await {
            Ok(rows) => {
                if let Some(row) = rows.first() {
                    if let Ok(value) = row.get::<f64>("total_bytes") {
                        return Ok(Some(value));
                    } else if let Ok(value_str) = row.get::<String>("total_bytes") {
                        if let Ok(value) = value_str.parse::<f64>() {
                            return Ok(Some(value));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to query table storage: {}", e);
                Ok(None)
            }
        }
    }

    /// Query storage used by stages
    async fn query_storage_stages(&self, session: &snowflake_connector_rs::SnowflakeSession) -> Result<Option<f64>> {
        let query = "SELECT SUM(BYTES) as total_bytes
                      FROM SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY
                      WHERE USAGE_DATE = CURRENT_DATE()";
        
        match session.query(query).await {
            Ok(rows) => {
                if let Some(row) = rows.first() {
                    if let Ok(value) = row.get::<f64>("total_bytes") {
                        return Ok(Some(value));
                    } else if let Ok(value_str) = row.get::<String>("total_bytes") {
                        if let Ok(value) = value_str.parse::<f64>() {
                            return Ok(Some(value));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to query stage storage: {}", e);
                Ok(None)
            }
        }
    }

    /// Collect metrics for multiple warehouses
    pub async fn collect_metrics_batch(
        &self,
        warehouses: &[String],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<SnowflakeMetrics>> {
        let mut metrics = Vec::new();

        for warehouse in warehouses {
            match self.collect_metrics(Some(warehouse), start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for warehouse {}: {}", warehouse, e);
                }
            }
        }

        // Also collect account-level metrics (without warehouse filter)
        match self.collect_metrics(None, start_time, end_time).await {
            Ok(metric) => metrics.push(metric),
            Err(e) => {
                error!("Failed to collect account-level metrics: {}", e);
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

/// Convert Snowflake metrics to Reiver format
pub fn snowflake_metrics_to_reiver_format(
    metrics: &SnowflakeMetrics,
    project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let mut base_tags = vec![
        format!("project_id:{}", project_id),
        format!("account_id:{}", metrics.account_id),
        "source:snowflake".to_string(),
        "service:snowflake".to_string(),
    ];
    
    if let Some(ref warehouse) = metrics.warehouse_name {
        base_tags.push(format!("warehouse:{}", warehouse));
    }

    let mut add_metric = |name: &str, value: Option<f64>, metric_type: &str| {
        if let Some(v) = value {
            reiver_metrics.push(ReiverMetric {
                name: format!("snowflake.{}", name),
                value: v,
                r#type: metric_type.to_string(),
                timestamp: metrics.timestamp,
                tags: base_tags.clone(),
            });
        }
    };

    add_metric("warehouse.running", metrics.warehouse_running, "gauge");
    add_metric("warehouse.queued_load", metrics.warehouse_queued_load, "gauge");
    add_metric("warehouse.queued_provisioning", metrics.warehouse_queued_provisioning, "gauge");
    add_metric("warehouse.blocked", metrics.warehouse_blocked, "gauge");
    add_metric("credits.used_compute", metrics.credits_used_compute, "gauge");
    add_metric("credits.used_cloud_services", metrics.credits_used_cloud_services, "gauge");
    add_metric("queries.executed", metrics.queries_executed, "counter");
    add_metric("queries.failed", metrics.queries_failed, "counter");
    add_metric("storage.used_bytes", metrics.storage_used_bytes, "gauge");
    add_metric("storage.used_tables", metrics.storage_used_tables, "gauge");
    add_metric("storage.used_stages", metrics.storage_used_stages, "gauge");

    reiver_metrics
}
