//! IBM Db2 database monitoring collector
//!
//! **IMPORTANT**: IBM Db2 support requires the IBM Db2 ODBC driver to be installed on the system.
//! This collector uses the `ibm_db` crate which wraps ODBC for database connectivity.
//! Requirements:
//! 1. IBM Db2 ODBC driver installation
//! 2. Connection string configuration
//! 3. Appropriate database permissions (SELECT on MON_GET_PKG_CACHE_STMT)
//!
//! For IBM Db2 performance monitoring, the collector queries:
//! - MON_GET_PKG_CACHE_STMT (package cache statement metrics) - primary source
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

pub struct Db2Collector {
    config: Arc<DatabaseConfig>,
    // Store connection parameters for on-demand connection
    // ibm_db crate uses synchronous connections, so we'll create them in the runtime
    connection_string: String,
}

impl Db2Collector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection string for Db2
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

        // Db2 connection string format (ibm_db crate uses ODBC connection strings)
        // Format: "DATABASE=database;HOSTNAME=host;PORT=port;UID=username;PWD=password"
        let conn_str = format!(
            "DATABASE={};HOSTNAME={};PORT={};UID={};PWD={}",
            config.database,
            config.host,
            config.port,
            config.username,
            password
        );

        info!("Testing Db2 connection...");
        let start = std::time::Instant::now();
        
        // Clone values needed after the move
        let conn_str_test = conn_str.clone();
        let conn_str_final = conn_str.clone();
        let config_name = config.name.clone();
        
        // Test connection in a blocking context (ibm_db uses synchronous API)
        let rt = tokio::runtime::Handle::current();
        
        rt.spawn_blocking(move || -> Result<()> {
            use ibm_db::{create_environment_v3, safe::AutocommitOn};
            use std::error::Error;
            
            let env = create_environment_v3()
                .map_err(|e| anyhow::anyhow!("Failed to create ODBC environment"))?;
            let _conn = env.connect_with_connection_string(&conn_str_test)
                .map_err(|e| anyhow::anyhow!("Failed to connect to Db2: {:?}", e))?;
            
            Ok(())
        })
        .await
        .context("Failed to spawn blocking task for Db2 connection")?
        .context(format!("Failed to connect to Db2 database: {}. Note: Db2 support requires IBM Db2 ODBC driver to be installed on the system.", config_name))?;
        
        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to Db2 successfully");

        Ok(Self {
            config,
            connection_string: conn_str_final,
        })
    }

    /// Query MON_GET_PKG_CACHE_STMT and return structured data
    /// Uses ibm_db crate for Db2 connectivity
    fn query_mon_get_pkg_cache_stmt(config: Arc<DatabaseConfig>, conn_str: String) -> Result<Vec<QueryMetricRow>> {
        // ibm_db crate uses blocking I/O, so we run it in a blocking task
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| anyhow::anyhow!("No tokio runtime available"))?;
        
        rt.block_on(async {
            let conn_str_clone = conn_str.clone();
            let config_clone = config.clone();
            
            tokio::task::spawn_blocking(move || -> Result<Vec<QueryMetricRow>, anyhow::Error> {
                use ibm_db::{create_environment_v3, safe::AutocommitOn, Statement, ResultSetState::{Data, NoData}};
                use std::error::Error;
                
                let env = create_environment_v3()
                    .map_err(|_| anyhow::anyhow!("Failed to create ODBC environment"))?;
                
                let conn = env.connect_with_connection_string(&conn_str_clone)
                    .map_err(|e| anyhow::anyhow!("Failed to connect to Db2: {:?}", e))?;
                
                let config_clone_inner = config_clone.clone();
                
                // Query MON_GET_PKG_CACHE_STMT for query performance metrics
                // This is the recommended table for monitoring SQL statements in Db2
                // Note: Requires appropriate privileges (SELECT on MON_GET_PKG_CACHE_STMT)
                let limit = config_clone.performance_schema.limit;
                let query = format!(
                    "SELECT 
                        STMT_TEXT,
                        NUM_EXECUTIONS as calls,
                        TOTAL_EXEC_TIME / 1000000.0 as total_time_ms,
                        CASE 
                            WHEN NUM_EXECUTIONS > 0 
                            THEN TOTAL_EXEC_TIME / NUM_EXECUTIONS / 1000000.0 
                            ELSE 0 
                        END as mean_time_ms,
                        MIN_EXEC_TIME / 1000000.0 as min_time_ms,
                        MAX_EXEC_TIME / 1000000.0 as max_time_ms
                    FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2)) AS T
                    WHERE NUM_EXECUTIONS > 0
                      AND STMT_TEXT IS NOT NULL
                    ORDER BY mean_time_ms DESC
                    FETCH FIRST {} ROWS ONLY",
                    limit
                );
                
                let base_tags = vec![
                    KeyValue::new("host", config_clone_inner.host.clone()),
                    KeyValue::new("source", "remote"),
                    KeyValue::new("database", config_clone_inner.name.clone()),
                    KeyValue::new("db_name", config_clone_inner.database.clone()),
                    KeyValue::new("db_type", "db2"),
                ];
                
                let mut result = Vec::new();
                
                // Execute query using ibm_db
                let stmt = Statement::with_parent(&conn)
                    .map_err(|e| anyhow::anyhow!("Failed to create statement: {:?}", e))?;
                
                match stmt.exec_direct(&query)
                    .map_err(|e| anyhow::anyhow!("Failed to execute query: {:?}", e))? {
                    Data(mut stmt) => {
                        let _cols = stmt.num_result_cols()
                            .map_err(|e| anyhow::anyhow!("Failed to get number of result columns: {:?}", e))?;
                        
                        // Fetch rows - column order: STMT_TEXT, calls, total_time_ms, mean_time_ms, min_time_ms, max_time_ms
                        while let Some(mut cursor) = stmt.fetch()
                            .map_err(|e| anyhow::anyhow!("Failed to fetch row: {:?}", e))? {
                            
                            // Get column values (1-indexed) - get_data returns Result<Option<T>, Box<dyn Error>>
                            // Extract values one at a time to avoid multiple mutable borrows
                            let query_text = cursor.get_data::<&str>(1)
                                .map_err(|e| anyhow::anyhow!("Failed to get STMT_TEXT: {:?}", e))?
                                .unwrap_or_default()
                                .to_string();
                            
                            if query_text.is_empty() {
                                continue;
                            }
                            
                            let calls = cursor.get_data::<i64>(2)
                                .map_err(|e| anyhow::anyhow!("Failed to get calls: {:?}", e))?
                                .unwrap_or(0);
                            let total_time_ms = cursor.get_data::<f64>(3)
                                .map_err(|e| anyhow::anyhow!("Failed to get total_time_ms: {:?}", e))?
                                .unwrap_or(0.0);
                            let mean_time_ms = cursor.get_data::<f64>(4)
                                .map_err(|e| anyhow::anyhow!("Failed to get mean_time_ms: {:?}", e))?
                                .unwrap_or(0.0);
                            let min_time_ms = cursor.get_data::<f64>(5)
                                .map_err(|e| anyhow::anyhow!("Failed to get min_time_ms: {:?}", e))?
                                .unwrap_or(0.0);
                            let max_time_ms = cursor.get_data::<f64>(6)
                                .map_err(|e| anyhow::anyhow!("Failed to get max_time_ms: {:?}", e))?
                                .unwrap_or(0.0);
                            
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
                                calls,
                                total_time_ms,
                                mean_time_ms,
                                min_time_ms,
                                max_time_ms,
                                tags: query_tags,
                            });
                        }
                    }
                    NoData(_) => {
                        // Query executed but no data returned - this is fine
                    }
                }
                
                Ok(result)
            }).await.map_err(|e| anyhow::anyhow!("Blocking task failed: {}", e))?
        })
    }
}

impl Collector for Db2Collector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled || !self.config.performance_schema.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let conn_str = self.connection_string.clone();
        
        // Query calls counter
        let _query_calls = meter
            .u64_observable_counter("database.db2.queries.calls")
            .with_description("Number of query executions")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_mon_get_pkg_cache_stmt(config.clone(), conn_str.clone()) {
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
            .f64_observable_gauge("database.db2.queries.mean_time_ms")
            .with_description("Mean execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_mon_get_pkg_cache_stmt(config.clone(), conn_str.clone()) {
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
            .f64_observable_gauge("database.db2.queries.total_time_ms")
            .with_description("Total execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_mon_get_pkg_cache_stmt(config.clone(), conn_str.clone()) {
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
            .f64_observable_gauge("database.db2.queries.min_time_ms")
            .with_description("Minimum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_mon_get_pkg_cache_stmt(config.clone(), conn_str.clone()) {
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
            .f64_observable_gauge("database.db2.queries.max_time_ms")
            .with_description("Maximum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_mon_get_pkg_cache_stmt(config.clone(), conn_str.clone()) {
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
    
    // Replace Db2 parameter markers (?, :1, :2, etc.)
    normalized = regex::Regex::new(r":\w+").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace string literals with ?
    normalized = regex::Regex::new(r"'([^']|'')*'").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace numeric literals with ?
    normalized = regex::Regex::new(r"\b\d+\b").unwrap().replace_all(&normalized, "?").to_string();
    
    normalized
}
