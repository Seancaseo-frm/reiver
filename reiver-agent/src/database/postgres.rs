use anyhow::{Context, Result};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
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

pub struct PostgreSQLCollector {
    config: Arc<DatabaseConfig>,
    pool: Option<PgPool>,
}

impl PostgreSQLCollector {
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
            "postgresql://{}:{}@{}:{}/{}",
            config.username,
            password,
            config.host,
            config.port,
            config.database
        );

        info!("Connecting to PostgreSQL database...");
        let start = std::time::Instant::now();
        
        // Try to create connection pool (will connect on first use)
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&conn_str)
            .await
            .context(format!("Failed to connect to PostgreSQL database: {}", config.name))?;

        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to PostgreSQL database");

        Ok(Self {
            config,
            pool: Some(pool),
        })
    }

    #[instrument(skip(self), fields(database = %self.config.name))]
    #[allow(dead_code)]
    async fn _old_collect_query_metrics(&self) -> Result<()> {
        let pool = self.pool.as_ref().ok_or_else(|| anyhow::anyhow!("No database connection"))?;
        
        // Check if pg_stat_statements extension is enabled
        let check_span = tracing::span!(tracing::Level::DEBUG, "db.check_extension");
        let _check_guard = check_span.enter();
        
        let extension_enabled: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')"
        )
        .fetch_one(pool)
        .await
        .context("Failed to check pg_stat_statements extension")?;

        drop(_check_guard);

        if !extension_enabled {
            warn!("pg_stat_statements extension is not enabled. Please run: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;");
            anyhow::bail!("pg_stat_statements extension is not enabled");
        }

        // Query pg_stat_statements for query metrics
        let query_span = tracing::span!(tracing::Level::DEBUG, "db.query.pg_stat_statements");
        let _query_guard = query_span.enter();
        
        let query = format!(
            "SELECT 
                pg_stat_statements.queryid,
                pg_stat_statements.query,
                pg_stat_statements.calls,
                pg_stat_statements.total_exec_time as total_time_ms,
                pg_stat_statements.mean_exec_time as mean_time_ms,
                pg_stat_statements.min_exec_time as min_time_ms,
                pg_stat_statements.max_exec_time as max_time_ms,
                pg_stat_statements.stddev_exec_time as stddev_time_ms,
                pg_stat_statements.rows,
                pg_stat_statements.shared_blks_hit,
                pg_stat_statements.shared_blks_read,
                pg_stat_statements.temp_blks_read,
                pg_stat_statements.temp_blks_written,
                pg_stat_statements.blk_read_time,
                pg_stat_statements.blk_write_time
            FROM pg_stat_statements
            WHERE pg_stat_statements.query NOT LIKE '%pg_stat_statements%'
            ORDER BY pg_stat_statements.mean_exec_time DESC
            LIMIT $1"
        );

        let start = std::time::Instant::now();
        let rows = sqlx::query(&query)
            .bind(self.config.pg_stat_statements.limit)
            .fetch_all(pool)
            .await
            .context("Failed to query pg_stat_statements")?;
        
        let query_duration_ms = start.elapsed().as_millis();
        info!(query_duration_ms, rows_count = rows.len(), "Query completed");
        
        drop(_query_guard);

        // TODO: Convert to OpenTelemetry observable callbacks
        // For now, this method is deprecated - metrics will be collected via observable callbacks
        // This code is kept for reference but will be removed
        
        Ok(())
    }
    
    /// Query pg_stat_statements and return structured data
    /// This handles async-to-sync conversion for observable callbacks
    fn query_pg_stat_statements(pool: &PgPool, config: &DatabaseConfig) -> Result<Vec<QueryMetricRow>> {
        // Check if pg_stat_statements extension is enabled
        let extension_enabled: bool = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.block_on(async {
                    sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')"
                    )
                    .fetch_one(pool)
                    .await
                })
                .context("Failed to check pg_stat_statements extension")?
            }
            Err(_) => {
                // No current runtime, create a new one
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("Failed to create tokio runtime for database query")?;
                
                rt.block_on(async {
                    sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')"
                    )
                    .fetch_one(pool)
                    .await
                })
                .context("Failed to check pg_stat_statements extension")?
            }
        };

        if !extension_enabled {
            warn!("pg_stat_statements extension is not enabled. Please run: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;");
            return Ok(Vec::new()); // Don't error, just return empty
        }

        // Query pg_stat_statements for query metrics
        let query = format!(
            "SELECT 
                pg_stat_statements.queryid,
                pg_stat_statements.query,
                pg_stat_statements.calls,
                pg_stat_statements.total_exec_time as total_time_ms,
                pg_stat_statements.mean_exec_time as mean_time_ms,
                pg_stat_statements.min_exec_time as min_time_ms,
                pg_stat_statements.max_exec_time as max_time_ms,
                pg_stat_statements.stddev_exec_time as stddev_time_ms,
                pg_stat_statements.rows,
                pg_stat_statements.shared_blks_hit,
                pg_stat_statements.shared_blks_read,
                pg_stat_statements.temp_blks_read,
                pg_stat_statements.temp_blks_written,
                pg_stat_statements.blk_read_time,
                pg_stat_statements.blk_write_time
            FROM pg_stat_statements
            WHERE pg_stat_statements.query NOT LIKE '%pg_stat_statements%'
            ORDER BY pg_stat_statements.mean_exec_time DESC
            LIMIT $1"
        );

        let rows = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.block_on(async {
                    sqlx::query(&query)
                        .bind(config.pg_stat_statements.limit)
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
                        .bind(config.pg_stat_statements.limit)
                        .fetch_all(pool)
                        .await
                })
            }
        }
        .context("Failed to query pg_stat_statements")?;

        // Build base tags
        let base_tags = vec![
            KeyValue::new("host", config.host.clone()),
            KeyValue::new("source", "remote"),
            KeyValue::new("database", config.name.clone()),
            KeyValue::new("db_name", config.database.clone()),
            KeyValue::new("db_type", config.r#type.clone()),
        ];

        let mut result = Vec::new();

        // Process each query row
        for row in rows {
            let query_text: String = row.try_get("query")?;
            
            // Extract trace_id from SQL comment if present
            let trace_id = extract_trace_id_from_query(&query_text);
            
            // Normalize query for fingerprinting
            let query_template = normalize_query(&query_text);
            let query_fingerprint = format!("{:x}", md5::compute(&query_template));

            let calls: i64 = row.try_get("calls")?;
            let total_time_ms: f64 = row.try_get::<f64, _>("total_time_ms")?;
            let mean_time_ms: f64 = row.try_get::<f64, _>("mean_time_ms")?;
            let min_time_ms: f64 = row.try_get::<f64, _>("min_time_ms")?;
            let max_time_ms: f64 = row.try_get::<f64, _>("max_time_ms")?;
            let stddev_time_ms: Option<f64> = row.try_get::<Option<f64>, _>("stddev_time_ms").ok().flatten();
            let rows_returned: Option<i64> = row.try_get::<Option<i64>, _>("rows").ok().flatten();
            let shared_blks_hit: Option<i64> = row.try_get::<Option<i64>, _>("shared_blks_hit").ok().flatten();
            let shared_blks_read: Option<i64> = row.try_get::<Option<i64>, _>("shared_blks_read").ok().flatten();
            let temp_blks_read: Option<i64> = row.try_get::<Option<i64>, _>("temp_blks_read").ok().flatten();
            let temp_blks_written: Option<i64> = row.try_get::<Option<i64>, _>("temp_blks_written").ok().flatten();
            let blk_read_time_ms: Option<f64> = row.try_get::<Option<f64>, _>("blk_read_time").ok().flatten();
            let blk_write_time_ms: Option<f64> = row.try_get::<Option<f64>, _>("blk_write_time").ok().flatten();

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

        Ok(result)
    }
}

impl Collector for PostgreSQLCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let pool = self.pool.clone();
        
        // Create observable instruments with per-instrument callbacks
        // Note: Each callback queries the database separately. This is acceptable
        // since queries are fast and only happen periodically (e.g., every 60s)
        
        // Query calls counter
        let _query_calls = meter
            .u64_observable_counter("database.postgresql.queries.calls")
            .with_description("Number of query executions")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_pg_stat_statements(pool, &config) {
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
            .f64_observable_gauge("database.postgresql.queries.total_time_ms")
            .with_description("Total execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_pg_stat_statements(pool, &config) {
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
            .f64_observable_gauge("database.postgresql.queries.mean_time_ms")
            .with_description("Mean execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_pg_stat_statements(pool, &config) {
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
            .f64_observable_gauge("database.postgresql.queries.min_time_ms")
            .with_description("Minimum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_pg_stat_statements(pool, &config) {
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
            .f64_observable_gauge("database.postgresql.queries.max_time_ms")
            .with_description("Maximum execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref pool) = pool {
                        if let Ok(rows) = Self::query_pg_stat_statements(pool, &config) {
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
        self.config.enabled && self.config.query_metrics.enabled
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

/// Normalize query for fingerprinting (remove parameters, normalize whitespace)
fn normalize_query(query: &str) -> String {
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
    
    // Replace parameter placeholders ($1, $2, ?, etc.)
    normalized = regex::Regex::new(r"\$\d+").unwrap().replace_all(&normalized, "?").to_string();
    normalized = regex::Regex::new(r"\?").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace string literals with ?
    normalized = regex::Regex::new(r"'([^']|'')*'").unwrap().replace_all(&normalized, "?").to_string();
    normalized = regex::Regex::new(r"\$\$[^$]*\$\$").unwrap().replace_all(&normalized, "?").to_string();
    
    // Replace numeric literals with ?
    normalized = regex::Regex::new(r"\b\d+\b").unwrap().replace_all(&normalized, "?").to_string();
    
    normalized
}

