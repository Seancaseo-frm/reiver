//! CockroachDB database monitoring collector
//!
//! This collector uses the `tokio-postgres` crate for CockroachDB connectivity.
//! CockroachDB is PostgreSQL-compatible, so we can use similar queries.
//! For CockroachDB performance monitoring, the collector queries:
//! - pg_stat_statements (if available, PostgreSQL compatibility feature)
//! - crdb_internal.statement_statistics (CockroachDB-specific, if available)
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

pub struct CockroachDBCollector {
    config: Arc<DatabaseConfig>,
    // Store connection parameters for on-demand connection
    connection_config: (String, String, u16, String, String), // (host, database, port, username, password)
}

impl CockroachDBCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection parameters for CockroachDB
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

        info!("Testing CockroachDB connection...");
        let start = std::time::Instant::now();
        
        // Build connection string for tokio-postgres
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            config.host,
            config.port,
            config.username,
            password,
            config.database
        );
        
        // Test connection using tokio-postgres
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .context(format!("Failed to connect to CockroachDB database: {}. Note: CockroachDB uses PostgreSQL protocol.", config.name))?;
        
        // Spawn connection task (required by tokio-postgres)
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!("CockroachDB connection error: {}", e);
            }
        });
        
        // Test query to verify connection
        let _: Vec<tokio_postgres::Row> = client
            .query("SELECT 1", &[])
            .await
            .context("Failed to execute test query")?;
        
        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to CockroachDB successfully");

        // Clone values before moving config
        let host = config.host.clone();
        let database = config.database.clone();
        let port = config.port;
        let username = config.username.clone();
        
        Ok(Self {
            config,
            connection_config: (host, database, port, username, password),
        })
    }

    /// Query pg_stat_statements or crdb_internal.statement_statistics and return structured data
    /// Uses tokio-postgres crate for CockroachDB connectivity
    fn query_statement_statistics(config: Arc<DatabaseConfig>, conn_config: (String, String, u16, String, String)) -> Result<Vec<QueryMetricRow>> {
        // tokio-postgres uses async API, so we need to run it in a runtime
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| anyhow::anyhow!("No tokio runtime available"))?;
        
        rt.block_on(async {
            let config_clone_inner = config.clone();
            let (host, database, port, username, password) = conn_config;
            
            // Build connection string
            let conn_str = format!(
                "host={} port={} user={} password={} dbname={}",
                host, port, username, password, database
            );
            
            // Create connection
            let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                .await
                .context("Failed to connect to CockroachDB")?;
            
            // Spawn connection task
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    warn!("CockroachDB connection error: {}", e);
                }
            });
            
            // Try pg_stat_statements first (PostgreSQL compatibility feature)
            // If that fails, try crdb_internal.statement_statistics (CockroachDB-specific)
            let query = format!(
                "SELECT 
                    query,
                    calls,
                    total_exec_time as total_time_ms,
                    mean_exec_time as mean_time_ms,
                    min_exec_time as min_time_ms,
                    max_exec_time as max_time_ms
                FROM pg_stat_statements
                WHERE query NOT LIKE '%pg_stat_statements%'
                  AND query NOT LIKE '%crdb_internal%'
                ORDER BY mean_exec_time DESC
                LIMIT $1"
            );
            
            let base_tags = vec![
                KeyValue::new("host", config_clone_inner.host.clone()),
                KeyValue::new("source", "remote"),
                KeyValue::new("database", config_clone_inner.name.clone()),
                KeyValue::new("db_name", config_clone_inner.database.clone()),
                KeyValue::new("db_type", "cockroachdb"),
            ];
            
            let mut result = Vec::new();
            
            // Execute query
            match client.query(&query, &[&(config_clone_inner.performance_schema.limit as i64)]).await {
                Ok(rows) => {
                    for row in rows {
                        let query_text: String = row.get("query");
                        let calls: i64 = row.get("calls");
                        let total_time_ms: f64 = row.get("total_time_ms");
                        let mean_time_ms: f64 = row.get("mean_time_ms");
                        let min_time_ms: f64 = row.get("min_time_ms");
                        let max_time_ms: f64 = row.get("max_time_ms");
                        
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
                            calls,
                            total_time_ms,
                            mean_time_ms,
                            min_time_ms,
                            max_time_ms,
                            tags: query_tags,
                        });
                    }
                }
                Err(e) => {
                    // pg_stat_statements might not be available, that's okay
                    warn!("Failed to query pg_stat_statements: {}. CockroachDB may not have pg_stat_statements enabled.", e);
                }
            }
            
            Ok::<_, anyhow::Error>(result)
        })
    }
}

impl Collector for CockroachDBCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled || !self.config.performance_schema.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let conn_config = self.connection_config.clone();
        
        // Query calls counter
        let _query_calls = meter
            .u64_observable_counter("database.cockroachdb.queries.calls")
            .with_description("Number of query executions")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_statement_statistics(config.clone(), conn_config.clone()) {
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
            .f64_observable_gauge("database.cockroachdb.queries.mean_time_ms")
            .with_description("Mean execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_statement_statistics(config.clone(), conn_config.clone()) {
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
            .f64_observable_gauge("database.cockroachdb.queries.total_time_ms")
            .with_description("Total execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_statement_statistics(config.clone(), conn_config.clone()) {
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
            .f64_observable_gauge("database.cockroachdb.queries.min_time_ms")
            .with_description("Minimum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_statement_statistics(config.clone(), conn_config.clone()) {
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
            .f64_observable_gauge("database.cockroachdb.queries.max_time_ms")
            .with_description("Maximum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_statement_statistics(config.clone(), conn_config.clone()) {
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
    
    // Replace PostgreSQL parameter markers ($1, $2, etc.)
    normalized = regex::Regex::new(r"\$\d+").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace string literals with ?
    normalized = regex::Regex::new(r"'([^']|'')*'").unwrap().replace_all(&normalized, "?").to_string();
    normalized = regex::Regex::new(r"\$\$[^$]*\$\$").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace numeric literals with ?
    normalized = regex::Regex::new(r"\b\d+\b").unwrap().replace_all(&normalized, "?").to_string();
    
    normalized
}
