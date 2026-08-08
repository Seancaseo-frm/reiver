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

pub struct SQLServerCollector {
    config: Arc<DatabaseConfig>,
    // Tiberius requires a connection config - we'll store it and create connections on demand
    connection_config: Arc<tiberius::Config>,
}

impl SQLServerCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection config for Tiberius
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

        info!("Configuring SQL Server connection...");
        
        let mut connection_config = tiberius::Config::new();
        connection_config.host(&config.host);
        connection_config.port(config.port);
        connection_config.database(&config.database);
        connection_config.authentication(tiberius::AuthMethod::sql_server(&config.username, &password));
        connection_config.trust_cert(); // Trust server certificate for now
        
        // Test connection
        info!("Testing SQL Server connection...");
        let start = std::time::Instant::now();
        
        let tcp = tokio::net::TcpStream::connect(connection_config.get_addr())
            .await
            .context(format!("Failed to connect to SQL Server {}:{}", config.host, config.port))?;
        tcp.set_nodelay(true)?;
        
        // Create a client to test the connection
        let tcp = tokio_util::compat::TokioAsyncReadCompatExt::compat(tcp);
        let mut client = tiberius::Client::connect(connection_config.clone(), tcp)
            .await
            .context(format!("Failed to authenticate to SQL Server database: {}", config.name))?;
        
        // Test with a simple query
        let test_stream = client.simple_query("SELECT 1")
            .await
            .context("Failed to execute test query")?;
        let test_results: Vec<Vec<tiberius::Row>> = test_stream.into_results().await?;
        // Just verify we got results (even if empty)
        let _ = test_results;
        
        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to SQL Server successfully");

        Ok(Self {
            config,
            connection_config: Arc::new(connection_config),
        })
    }

    /// Query sys.dm_exec_query_stats and return structured data
    /// Uses Tiberius for SQL Server connectivity
    fn query_dm_exec_query_stats(config: Arc<DatabaseConfig>, conn_config: Arc<tiberius::Config>) -> Result<Vec<QueryMetricRow>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to create tokio runtime for SQL Server query")?;
        
        rt.block_on(async {
            // Establish connection
            let tcp = tokio::net::TcpStream::connect(conn_config.get_addr())
                .await
                .context("Failed to connect to SQL Server")?;
            tcp.set_nodelay(true)?;
            
            let tcp = tokio_util::compat::TokioAsyncReadCompatExt::compat(tcp);
            let mut client = tiberius::Client::connect(conn_config.as_ref().clone(), tcp)
                .await
                .context("Failed to authenticate to SQL Server")?;
            
            // Query sys.dm_exec_query_stats for query performance metrics
            let limit = config.performance_schema.limit as i32;
            let query = "
                SELECT TOP (@P1)
                    qs.execution_count as calls,
                    qs.total_elapsed_time / 1000000.0 as total_time_ms,
                    CASE 
                        WHEN qs.execution_count > 0 
                        THEN qs.total_elapsed_time / qs.execution_count / 1000000.0 
                        ELSE 0 
                    END as mean_time_ms,
                    qs.min_elapsed_time / 1000000.0 as min_time_ms,
                    qs.max_elapsed_time / 1000000.0 as max_time_ms,
                    SUBSTRING(qt.text, 
                        (qs.statement_start_offset/2)+1,
                        ((CASE qs.statement_end_offset 
                            WHEN -1 THEN DATALENGTH(qt.text) 
                            ELSE qs.statement_end_offset 
                        END - qs.statement_start_offset)/2)+1
                    ) as query_text
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
                WHERE qt.text NOT LIKE '%sys.dm_exec_query_stats%'
                  AND qt.text NOT LIKE '%sys.dm_exec_sql_text%'
                ORDER BY mean_time_ms DESC
            ";
            
            let mut base_tags = vec![
                KeyValue::new("host", config.host.clone()),
                KeyValue::new("source", "remote"),
                KeyValue::new("database", config.name.clone()),
                KeyValue::new("db_name", config.database.clone()),
                KeyValue::new("db_type", "sqlserver"),
            ];
            
            let mut result = Vec::new();
            
            // Execute parameterized query
            let stream = client.query(query, &[&limit]).await
                .context("Failed to execute query on SQL Server")?;
            
            // Process results - into_results returns Vec<Vec<Row>>
            let results: Vec<Vec<tiberius::Row>> = stream.into_results().await
                .context("Failed to fetch results")?;
            
            // Get rows from first result set (if any)
            let rows = results.into_iter().next().unwrap_or_default();
            
            for row in rows {
                // Tiberius uses indexed access (0-based) - get returns Option
                let calls: i64 = row.get::<i64, _>(0)
                    .ok_or_else(|| anyhow::anyhow!("Failed to get calls"))?;
                let total_time_ms: f64 = row.get::<f64, _>(1)
                    .ok_or_else(|| anyhow::anyhow!("Failed to get total_time_ms"))?;
                let mean_time_ms: f64 = row.get::<f64, _>(2)
                    .ok_or_else(|| anyhow::anyhow!("Failed to get mean_time_ms"))?;
                let min_time_ms: f64 = row.get::<f64, _>(3)
                    .ok_or_else(|| anyhow::anyhow!("Failed to get min_time_ms"))?;
                let max_time_ms: f64 = row.get::<f64, _>(4)
                    .ok_or_else(|| anyhow::anyhow!("Failed to get max_time_ms"))?;
                let query_text: String = row.get::<&str, _>(5)
                    .ok_or_else(|| anyhow::anyhow!("Failed to get query_text"))?
                    .to_string();
                
                // Extract trace_id from SQL comment if present
                let trace_id = extract_trace_id_from_query(&query_text);
                
                // Normalize query for fingerprinting
                let query_template = normalize_query(&query_text);
                let query_fingerprint = format!("{:x}", md5::compute(&query_template));
                
                // Build tags for this query
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
            
            Ok::<_, anyhow::Error>(result)
        })
    }
}

impl Collector for SQLServerCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled || !self.config.performance_schema.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let conn_config = self.connection_config.clone();
        
        // Query calls counter
        let _query_calls = meter
            .u64_observable_counter("database.sqlserver.queries.calls")
            .with_description("Number of query executions")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_dm_exec_query_stats(config.clone(), conn_config.clone()) {
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
            .f64_observable_gauge("database.sqlserver.queries.mean_time_ms")
            .with_description("Mean execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_dm_exec_query_stats(config.clone(), conn_config.clone()) {
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
            .f64_observable_gauge("database.sqlserver.queries.total_time_ms")
            .with_description("Total execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_dm_exec_query_stats(config.clone(), conn_config.clone()) {
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
            .f64_observable_gauge("database.sqlserver.queries.min_time_ms")
            .with_description("Minimum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_dm_exec_query_stats(config.clone(), conn_config.clone()) {
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
            .f64_observable_gauge("database.sqlserver.queries.max_time_ms")
            .with_description("Maximum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let conn_config = conn_config.clone();
                move |observer| {
                    if let Ok(rows) = Self::query_dm_exec_query_stats(config.clone(), conn_config.clone()) {
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
    
    // Replace parameter placeholders (@p1, @p2, etc.)
    normalized = regex::Regex::new(r"@\w+").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace string literals with ?
    normalized = regex::Regex::new(r"'([^']|'')*'").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace numeric literals with ?
    normalized = regex::Regex::new(r"\b\d+\b").unwrap().replace_all(&normalized, "?").to_string();
    
    normalized
}
