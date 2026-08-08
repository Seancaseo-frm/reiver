use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
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

pub struct OracleCollector {
    config: Arc<DatabaseConfig>,
    // Store connection string for on-demand connection
    // Oracle crate uses synchronous connections, so we'll create them in the runtime
    connection_string: String,
}

impl OracleCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection string for Oracle
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

        // Oracle connection string format: //host:port/service_name
        let conn_str = format!(
            "//{}:{}/{}",
            config.host,
            config.port,
            config.database
        );

        info!("Testing Oracle connection...");
        let start = std::time::Instant::now();
        
        // Clone values needed after the move
        let username = config.username.clone();
        let password_clone = password.clone();
        let conn_str_test = conn_str.clone();
        let conn_str_final = conn_str.clone();
        let config_name = config.name.clone();
        let username_final = username.clone();
        let password_final = password.clone();
        
        // Test connection in a blocking context
        let rt = tokio::runtime::Handle::current();
        
        rt.spawn_blocking(move || {
            oracle::Connection::connect(&username, &password_clone, &conn_str_test)
        })
        .await
        .context("Failed to spawn blocking task for Oracle connection")?
        .context(format!("Failed to connect to Oracle database: {}. Note: Oracle support requires Oracle Instant Client libraries to be installed on the system.", config_name))?;
        
        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to Oracle successfully");

        Ok(Self {
            config,
            connection_string: format!("{}:{}@{}", username_final, password_final, conn_str_final),
        })
    }

    /// Query V$SQLSTATS and return structured data
    /// Uses oracle crate for Oracle connectivity
    fn query_v_sqlstats(config: Arc<DatabaseConfig>, conn_str: String) -> Result<Vec<QueryMetricRow>> {
        // Oracle crate uses blocking I/O, so we run it in a blocking task
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| anyhow::anyhow!("No tokio runtime available"))?;
        
        rt.block_on(async {
                let conn_str_clone = conn_str.clone();
            let config_clone = config.clone();
            
            tokio::task::spawn_blocking(move || {
                // Parse connection string (format: "username:password@//host:port/service")
                let (auth_part, db_part) = conn_str_clone.split_once('@')
                    .ok_or_else(|| anyhow::anyhow!("Invalid Oracle connection string format"))?;
                let (username, password) = auth_part.split_once(':')
                    .ok_or_else(|| anyhow::anyhow!("Invalid Oracle auth format"))?;
                
                let conn = oracle::Connection::connect(username, password, db_part)
                    .context("Failed to connect to Oracle")?;
                
                let config_clone_inner = config_clone.clone();
                
                // Query V$SQLSTATS for query performance metrics
                // Note: Requires appropriate privileges (SELECT on V$SQLSTATS)
                let limit = config_clone.performance_schema.limit;
                let query = format!(
                    "SELECT 
                        sql_id,
                        executions as calls,
                        elapsed_time / 1000000.0 as total_time_ms,
                        CASE 
                            WHEN executions > 0 
                            THEN elapsed_time / executions / 1000000.0 
                            ELSE 0 
                        END as mean_time_ms,
                        elapsed_time / 1000000.0 as min_time_ms,
                        elapsed_time / 1000000.0 as max_time_ms,
                        sql_text as query_text
                    FROM (
                        SELECT 
                            sql_id,
                            executions,
                            elapsed_time,
                            sql_text,
                            ROW_NUMBER() OVER (ORDER BY elapsed_time / NULLIF(executions, 0) DESC) as rn
                        FROM V$SQLSTATS
                        WHERE executions > 0
                          AND sql_text IS NOT NULL
                          AND sql_text NOT LIKE '%V$SQLSTATS%'
                    )
                    WHERE rn <= :limit"
                );
                
                let mut base_tags = vec![
                    KeyValue::new("host", config_clone_inner.host.clone()),
                    KeyValue::new("source", "remote"),
                    KeyValue::new("database", config_clone_inner.name.clone()),
                    KeyValue::new("db_name", config_clone_inner.database.clone()),
                    KeyValue::new("db_type", "oracle"),
                ];
                
                let mut result = Vec::new();
                
                // Execute query with parameter
                let rows = conn.query(&query, &[&limit])
                    .context("Failed to execute query on Oracle")?;
                
                for row_result in rows {
                    let row = row_result.context("Failed to fetch row")?;
                    
                    let sql_id: Option<String> = row.get(0)?;
                    let calls: i64 = row.get(1)?;
                    let total_time_ms: f64 = row.get(2)?;
                    let mean_time_ms: f64 = row.get(3)?;
                    let min_time_ms: f64 = row.get(4)?;
                    let max_time_ms: f64 = row.get(5)?;
                    let query_text: Option<String> = row.get(6)?;
                    
                    let query_text = query_text.unwrap_or_default();
                    if query_text.is_empty() {
                        continue;
                    }
                    
                    // Extract trace_id from SQL comment if present
                    let trace_id = extract_trace_id_from_query(&query_text);
                    
                    // Normalize query for fingerprinting
                    let query_template = normalize_query(&query_text);
                    let query_fingerprint = if let Some(id) = sql_id.as_ref() {
                        id.chars().take(16).collect::<String>() // Use first 16 chars of sql_id
                    } else {
                        format!("{:x}", md5::compute(&query_template))
                    };
                    
                    // Build tags for this query
                    let mut query_tags = base_tags.clone();
                    if let Some(id) = sql_id {
                        query_tags.push(KeyValue::new("sql_id", id));
                    }
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
                
                Ok::<_, anyhow::Error>(result)
            }).await.context("Blocking task failed")?
        })
    }
}

impl Collector for OracleCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled || !self.config.performance_schema.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let conn_str = self.connection_string.clone();
        
        // Query calls counter
        let _query_calls = meter
            .u64_observable_counter("database.oracle.queries.calls")
            .with_description("Number of query executions")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_v_sqlstats(config.clone(), conn_str.clone()) {
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
            .f64_observable_gauge("database.oracle.queries.mean_time_ms")
            .with_description("Mean execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_v_sqlstats(config.clone(), conn_str.clone()) {
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
            .f64_observable_gauge("database.oracle.queries.total_time_ms")
            .with_description("Total execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_v_sqlstats(config.clone(), conn_str.clone()) {
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
            .f64_observable_gauge("database.oracle.queries.min_time_ms")
            .with_description("Minimum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_v_sqlstats(config.clone(), conn_str.clone()) {
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
            .f64_observable_gauge("database.oracle.queries.max_time_ms")
            .with_description("Maximum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_str = conn_str.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_v_sqlstats(config.clone(), conn_str.clone()) {
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
    
    // Replace Oracle bind variables (:1, :2, :name, etc.)
    normalized = regex::Regex::new(r":\w+").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace string literals with ?
    normalized = regex::Regex::new(r"'([^']|'')*'").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace numeric literals with ?
    normalized = regex::Regex::new(r"\b\d+\b").unwrap().replace_all(&normalized, "?").to_string();
    
    normalized
}
