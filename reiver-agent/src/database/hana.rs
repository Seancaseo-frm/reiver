//! SAP HANA database monitoring collector
//!
//! This collector uses the `hdbconnect_async` crate for SAP HANA connectivity.
//! For SAP HANA performance monitoring, the collector queries:
//! - M_SQL_PLAN_CACHE_STATISTICS (SQL plan cache statistics) - primary source
//!
//! Metrics collected include:
//! - Query execution counts
//! - Total/mean/min/max execution times
//! - Statement text for fingerprinting

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use md5;
use serde::Deserialize;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

/// Structure to hold query metric data for observation
struct QueryMetricRow {
    calls: i64,
    total_time_ms: f64,
    mean_time_ms: f64,
    min_time_ms: f64,
    max_time_ms: f64,
    tags: Vec<KeyValue>,
}

/// Row structure for SAP HANA M_SQL_PLAN_CACHE_STATISTICS query result
#[derive(Debug, Deserialize)]
struct HanaQueryRow {
    #[serde(rename = "STATEMENT_STRING")]
    statement_string: Option<String>,
    calls: i64,
    total_time_ms: f64,
    mean_time_ms: f64,
    min_time_ms: f64,
    max_time_ms: f64,
}

pub struct HanaCollector {
    config: Arc<DatabaseConfig>,
    // Store connection string for on-demand connection
    connection_string: String,
}

impl HanaCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection string for SAP HANA
        let env_key = format!("DB_MONITORING_{}_PASSWORD", config.name.to_uppercase().replace("-", "_"));
        let password = std::env::var(&env_key)
            .or_else(|_| {
                if !config.password.is_empty() {
                    Ok(config.password.clone())
                } else {
                    Err(std::env::VarError::NotPresent)
                }
            })
            .unwrap_or_default();

        // SAP HANA connection string format: hdbsql://USER:PASSWORD@HOST:PORT
        let conn_str = format!("hdbsql://{}:{}@{}:{}", config.username, password, config.host, config.port);

        info!("Testing SAP HANA connection...");
        let start = std::time::Instant::now();
        
        // Test connection using hdbconnect_async
        let mut conn = hdbconnect_async::Connection::new(conn_str.clone())
            .await
            .context(format!("Failed to connect to SAP HANA database: {}. Note: SAP HANA support requires the hdbclient library to be available.", config.name))?;
        
        // Test query to verify connection
        let _: Vec<(i32,)> = conn.query("SELECT 1 FROM DUMMY")
            .await
            .context("Failed to execute test query")?
            .try_into()
            .await
            .context("Failed to parse test query result")?;
        
        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to SAP HANA successfully");

        Ok(Self {
            config,
            connection_string: conn_str,
        })
    }

    /// Query M_SQL_PLAN_CACHE_STATISTICS and return structured data
    /// Uses hdbconnect_async crate for SAP HANA connectivity
    fn query_sql_plan_cache_statistics(config: Arc<DatabaseConfig>, conn_str: String) -> Result<Vec<QueryMetricRow>> {
        // hdbconnect_async uses async API, so we need to run it in a runtime
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| anyhow::anyhow!("No tokio runtime available"))?;
        
        rt.block_on(async {
            let config_clone_inner = config.clone();
            
            // Create connection
            let mut conn = hdbconnect_async::Connection::new(conn_str.clone())
                .await
                .context("Failed to connect to SAP HANA")?;
            
            // Query M_SQL_PLAN_CACHE_STATISTICS for query performance metrics
            // This is the recommended view for monitoring SQL statements in SAP HANA
            // Note: Requires appropriate privileges (SELECT on M_SQL_PLAN_CACHE_STATISTICS)
            let limit = config.performance_schema.limit;
            let query = format!(
                "SELECT 
                    STATEMENT_STRING,
                    EXECUTION_COUNT as calls,
                    TOTAL_EXECUTION_TIME / 1000000.0 as total_time_ms,
                    CASE 
                        WHEN EXECUTION_COUNT > 0 
                        THEN TOTAL_EXECUTION_TIME / EXECUTION_COUNT / 1000000.0 
                        ELSE 0 
                    END as mean_time_ms,
                    MIN_EXECUTION_TIME / 1000000.0 as min_time_ms,
                    MAX_EXECUTION_TIME / 1000000.0 as max_time_ms
                FROM M_SQL_PLAN_CACHE_STATISTICS
                WHERE EXECUTION_COUNT > 0
                  AND STATEMENT_STRING IS NOT NULL
                ORDER BY mean_time_ms DESC
                LIMIT {}",
                limit
            );
            
            let base_tags = vec![
                KeyValue::new("host", config_clone_inner.host.clone()),
                KeyValue::new("source", "remote"),
                KeyValue::new("database", config_clone_inner.name.clone()),
                KeyValue::new("db_name", config_clone_inner.database.clone()),
                KeyValue::new("db_type", "hana"),
            ];
            
            // Execute query and convert to Vec<HanaQueryRow>
            let rows: Vec<HanaQueryRow> = conn.query(&query)
                .await
                .context("Failed to execute query")?
                .try_into()
                .await
                .context("Failed to parse query results")?;
            
            let mut result = Vec::new();
            
            for row in rows {
                let query_text = row.statement_string.unwrap_or_default();
                if query_text.is_empty() {
                    continue;
                }
                
                // Extract trace_id from SQL comment if present
                let trace_id = extract_trace_id_from_query(&query_text);
                
                // Normalize query for fingerprinting
                let query_template = normalize_query(&query_text);
                let query_fingerprint = format!("{:x}", md5::compute(&query_template));
                
                let mut query_tags = base_tags.clone();
                query_tags.push(KeyValue::new("query_fingerprint", query_fingerprint.clone()));
                if let Some(tid) = trace_id {
                    query_tags.push(KeyValue::new("trace_id", tid));
                }
                
                result.push(QueryMetricRow {
                    calls: row.calls,
                    total_time_ms: row.total_time_ms,
                    mean_time_ms: row.mean_time_ms,
                    min_time_ms: row.min_time_ms,
                    max_time_ms: row.max_time_ms,
                    tags: query_tags,
                });
            }
            
            Ok::<_, anyhow::Error>(result)
        })
    }
}

impl Collector for HanaCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled || !self.config.performance_schema.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let conn_str = self.connection_string.clone();
        
        // Query calls counter
        let _query_calls = meter
            .u64_observable_counter("database.hana.queries.calls")
            .with_description("Number of query executions")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_sql_plan_cache_statistics(config.clone(), conn_str.clone()) {
                        for row_data in rows {
                            let tags_slice: &[KeyValue] = &row_data.tags;
                            observer.observe(row_data.calls as u64, tags_slice);
                        }
                    }
                }
            })
            .build();
        
        // Mean execution time gauge
        let _query_mean_time = meter
            .f64_observable_gauge("database.hana.queries.mean_time_ms")
            .with_description("Mean execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_sql_plan_cache_statistics(config.clone(), conn_str.clone()) {
                        for row_data in rows {
                            let tags_slice: &[KeyValue] = &row_data.tags;
                            observer.observe(row_data.mean_time_ms, tags_slice);
                        }
                    }
                }
            })
            .build();
        
        // Total execution time gauge
        let _query_total_time = meter
            .f64_observable_gauge("database.hana.queries.total_time_ms")
            .with_description("Total execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_sql_plan_cache_statistics(config.clone(), conn_str.clone()) {
                        for row_data in rows {
                            let tags_slice: &[KeyValue] = &row_data.tags;
                            observer.observe(row_data.total_time_ms, tags_slice);
                        }
                    }
                }
            })
            .build();
        
        // Min execution time gauge
        let _query_min_time = meter
            .f64_observable_gauge("database.hana.queries.min_time_ms")
            .with_description("Minimum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_sql_plan_cache_statistics(config.clone(), conn_str.clone()) {
                        for row_data in rows {
                            let tags_slice: &[KeyValue] = &row_data.tags;
                            observer.observe(row_data.min_time_ms, tags_slice);
                        }
                    }
                }
            })
            .build();
        
        // Max execution time gauge
        let _query_max_time = meter
            .f64_observable_gauge("database.hana.queries.max_time_ms")
            .with_description("Maximum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_sql_plan_cache_statistics(config.clone(), conn_str.clone()) {
                        for row_data in rows {
                            let tags_slice: &[KeyValue] = &row_data.tags;
                            observer.observe(row_data.max_time_ms, tags_slice);
                        }
                    }
                }
            })
            .build();
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled && self.config.query_metrics.enabled && self.config.performance_schema.enabled
    }
}

/// Extract trace_id from SQL comment
fn extract_trace_id_from_query(query: &str) -> Option<String> {
    let patterns = [
        r"(?i)/\*\s*trace_id\s*:\s*([a-f0-9]{32,})\s*\*/",
        r"(?i)/\*\s*trace_id\s*=\s*([a-f0-9]{32,})\s*\*/",
        r"(?i)--\s*trace_id\s*:\s*([a-f0-9]{32,})",
    ];

    for pattern in &patterns {
        if let Some(captures) = regex::Regex::new(pattern)
            .ok()
            .and_then(|re| re.captures(query))
        {
            if let Some(trace_id) = captures.get(1) {
                return Some(trace_id.as_str().to_string());
            }
        }
    }

    None
}

/// Normalize query for fingerprinting
fn normalize_query(query: &str) -> String {
    let mut normalized = query.to_string();
    
    // Remove SQL comments (but preserve trace_id comments)
    normalized = regex::Regex::new(r"--.*").unwrap().replace_all(&normalized, "").to_string();
    normalized = regex::Regex::new(r"/\*(?!.*trace_id).*?\*/")
        .unwrap()
        .replace_all(&normalized, "")
        .to_string();
    
    // Normalize whitespace
    normalized = regex::Regex::new(r"\s+").unwrap().replace_all(&normalized, " ").to_string();
    normalized = normalized.trim().to_string();
    
    // Replace SAP HANA parameter markers (?, :1, :2, etc.)
    normalized = regex::Regex::new(r":\w+").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace string literals with ?
    normalized = regex::Regex::new(r"'([^']|'')*'").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace numeric literals with ?
    normalized = regex::Regex::new(r"\b\d+\b").unwrap().replace_all(&normalized, "?").to_string();
    
    normalized
}
