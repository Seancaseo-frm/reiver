use anyhow::{Context, Result};
use sqlx::{mysql::MySqlPoolOptions, MySqlPool, Row};
use std::sync::Arc;
use std::time::Duration;
use tracing::{instrument, info, warn, error};
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

pub struct MySQLCollector {
    config: Arc<DatabaseConfig>,
    pool: Option<MySqlPool>,
}

impl MySQLCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection string
        // Try environment variable first, then fall back to config password
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
        
        let conn_str = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.username,
            password,
            config.host,
            config.port,
            config.database
        );

        info!("Connecting to MySQL database...");
        let start = std::time::Instant::now();
        
        // Try to create connection pool (will connect on first use)
        let pool = MySqlPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&conn_str)
            .await
            .context(format!("Failed to connect to MySQL database: {}", config.name))?;

        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to MySQL database");

        Ok(Self {
            config,
            pool: Some(pool),
        })
    }

    /// Query performance_schema.events_statements_summary_by_digest and return structured data
    /// This handles async-to-sync conversion for observable callbacks
    fn query_performance_schema(pool: &MySqlPool, config: &DatabaseConfig) -> Result<Vec<QueryMetricRow>> {
        // Check if performance_schema is enabled
        let performance_schema_enabled: bool = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.block_on(async {
                    sqlx::query_scalar::<_, u8>(
                        "SELECT @@performance_schema"
                    )
                    .fetch_one(pool)
                    .await
                })
                .context("Failed to check performance_schema")? == 1
            }
            Err(_) => {
                // No current runtime, create a new one
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("Failed to create tokio runtime for database query")?;
                
                rt.block_on(async {
                    sqlx::query_scalar::<_, u8>(
                        "SELECT @@performance_schema"
                    )
                    .fetch_one(pool)
                    .await
                })
                .context("Failed to check performance_schema")? == 1
            }
        };

        if !performance_schema_enabled {
            warn!("performance_schema is not enabled. Please enable it in MySQL configuration (performance_schema=ON)");
            return Ok(Vec::new()); // Don't error, just skip
        }

        // Query events_statements_summary_by_digest for query metrics
        // This table is similar to pg_stat_statements in PostgreSQL
        let query = format!(
            "SELECT 
                SCHEMA_NAME,
                DIGEST,
                DIGEST_TEXT,
                COUNT_STAR as calls,
                SUM_TIMER_WAIT / 1000000000000 as total_time_ms, -- Convert from picoseconds to milliseconds
                AVG_TIMER_WAIT / 1000000000000 as mean_time_ms,
                MIN_TIMER_WAIT / 1000000000000 as min_time_ms,
                MAX_TIMER_WAIT / 1000000000000 as max_time_ms,
                SUM_ROWS_EXAMINED as rows_examined,
                SUM_ROWS_SENT as rows_returned,
                SUM_ROWS_AFFECTED as rows_affected,
                SUM_SELECT_SCAN as select_scan,
                SUM_SELECT_FULL_JOIN as full_join
            FROM performance_schema.events_statements_summary_by_digest
            WHERE DIGEST_TEXT IS NOT NULL
              AND DIGEST_TEXT NOT LIKE '%performance_schema%'
              AND DIGEST_TEXT NOT LIKE '%information_schema%'
            ORDER BY AVG_TIMER_WAIT DESC
            LIMIT ?"
        );

        let rows = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.block_on(async {
                    sqlx::query(&query)
                        .bind(config.performance_schema.limit)
                        .fetch_all(pool)
                        .await
                })
            }
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("Failed to create tokio runtime for database query")?;
                rt.block_on(async {
                    sqlx::query(&query)
                        .bind(config.performance_schema.limit)
                        .fetch_all(pool)
                        .await
                })
            }
        }
        .context("Failed to query performance_schema.events_statements_summary_by_digest")?;

        // Build base tags
        let mut base_tags = vec![
            KeyValue::new("host", config.host.clone()),
            KeyValue::new("source", "remote"),
            KeyValue::new("database", config.name.clone()),
            KeyValue::new("db_name", config.database.clone()),
            KeyValue::new("db_type", config.r#type.clone()),
        ];

        let mut result = Vec::new();

        // Process each query row
        for row in rows {
            let digest_text: Option<String> = row.try_get("DIGEST_TEXT")?;
            let digest_text = digest_text.unwrap_or_default();
            
            if digest_text.is_empty() {
                continue;
            }
            
            let schema_name: Option<String> = row.try_get("SCHEMA_NAME").ok().flatten();
            let digest: Option<String> = row.try_get("DIGEST").ok().flatten();
            
            // Extract trace_id from SQL comment if present
            let trace_id = extract_trace_id_from_query(&digest_text);
            
            // Normalize query for fingerprinting (digest_text is already normalized, but we can use digest as fingerprint)
            let query_template = normalize_mysql_query(&digest_text);
            let query_fingerprint = if let Some(d) = digest {
                d.chars().take(16).collect::<String>() // Use first 16 chars of digest as fingerprint
            } else {
                format!("{:x}", md5::compute(&query_template))
            };

            let calls: i64 = row.try_get("calls")?;
            let total_time_ms: f64 = row.try_get::<f64, _>("total_time_ms")?;
            let mean_time_ms: f64 = row.try_get::<f64, _>("mean_time_ms")?;
            let min_time_ms: f64 = row.try_get::<f64, _>("min_time_ms")?;
            let max_time_ms: f64 = row.try_get::<f64, _>("max_time_ms")?;

            // Build tags for this query
            let mut query_tags = base_tags.clone();
            if let Some(schema) = schema_name {
                query_tags.push(KeyValue::new("schema", schema));
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

        Ok(result)
    }
}

impl Collector for MySQLCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled || !self.config.performance_schema.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let pool = self.pool.clone();
        
        // Create observable instruments with per-instrument callbacks
        // Note: Each callback queries the database separately. This is acceptable
        // since queries are fast and only happen periodically (e.g., every 60s)
        
        // Query calls counter
        let _query_calls = meter
            .u64_observable_counter("database.mysql.queries.calls")
            .with_description("Number of query executions")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_performance_schema(pool, &config) {
                            for row_data in rows {
                                let tags_slice: &[KeyValue] = &row_data.tags;
                                observer.observe(row_data.calls as u64, tags_slice);
                            }
                        }
                    }
                }
            })
            .build();
        
        // Total execution time gauge
        let _query_total_time = meter
            .f64_observable_gauge("database.mysql.queries.total_time_ms")
            .with_description("Total execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_performance_schema(pool, &config) {
                            for row_data in rows {
                                let tags_slice: &[KeyValue] = &row_data.tags;
                                observer.observe(row_data.total_time_ms, tags_slice);
                            }
                        }
                    }
                }
            })
            .build();
        
        // Mean execution time gauge
        let _query_mean_time = meter
            .f64_observable_gauge("database.mysql.queries.mean_time_ms")
            .with_description("Mean execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_performance_schema(pool, &config) {
                            for row_data in rows {
                                let tags_slice: &[KeyValue] = &row_data.tags;
                                observer.observe(row_data.mean_time_ms, tags_slice);
                            }
                        }
                    }
                }
            })
            .build();
        
        // Min execution time gauge
        let _query_min_time = meter
            .f64_observable_gauge("database.mysql.queries.min_time_ms")
            .with_description("Minimum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_performance_schema(pool, &config) {
                            for row_data in rows {
                                let tags_slice: &[KeyValue] = &row_data.tags;
                                observer.observe(row_data.min_time_ms, tags_slice);
                            }
                        }
                    }
                }
            })
            .build();
        
        // Max execution time gauge
        let _query_max_time = meter
            .f64_observable_gauge("database.mysql.queries.max_time_ms")
            .with_description("Maximum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_performance_schema(pool, &config) {
                            for row_data in rows {
                                let tags_slice: &[KeyValue] = &row_data.tags;
                                observer.observe(row_data.max_time_ms, tags_slice);
                            }
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
/// Format: /* trace_id: abc123 */ or /*trace_id=abc123*/
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

/// Normalize MySQL query for fingerprinting (digest_text is already normalized, but clean it further)
fn normalize_mysql_query(query: &str) -> String {
    let mut normalized = query.to_string();
    
    // Remove SQL comments (but preserve trace_id comments for correlation)
    // Remove single-line comments
    normalized = regex::Regex::new(r"--.*").unwrap().replace_all(&normalized, "").to_string();
    
    // Remove multi-line comments (but preserve trace_id comments)
    normalized = regex::Regex::new(r"/\*(?!.*trace_id).*?\*/")
        .unwrap()
        .replace_all(&normalized, "")
        .to_string();
    
    // Normalize whitespace
    normalized = regex::Regex::new(r"\s+").unwrap().replace_all(&normalized, " ").to_string();
    normalized = normalized.trim().to_string();
    
    normalized
}

