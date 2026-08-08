//! TimescaleDB database monitoring collector
//!
//! This collector uses PostgreSQL connectivity (TimescaleDB is a PostgreSQL extension).
//! TimescaleDB extends PostgreSQL with time-series capabilities including hypertables and chunks.
//!
//! Metrics collected include:
//! - All standard PostgreSQL metrics (via pg_stat_statements)
//! - TimescaleDB-specific metrics:
//!   - Number of hypertables
//!   - Number of chunks per hypertable
//!   - Chunk sizes and compression status
//!   - Hypertable statistics

use anyhow::{Context, Result};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::Arc;
use std::time::Duration;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

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

pub struct TimescaleDBCollector {
    config: Arc<DatabaseConfig>,
    pool: Option<PgPool>,
}

impl TimescaleDBCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection string (same as PostgreSQL)
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

        info!("Connecting to TimescaleDB database...");
        let start = std::time::Instant::now();
        
        // Try to create connection pool
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&conn_str)
            .await
            .context(format!("Failed to connect to TimescaleDB database: {}", config.name))?;

        // Verify TimescaleDB extension is installed
        let extension_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')"
        )
        .fetch_one(&pool)
        .await
        .context("Failed to check TimescaleDB extension")?;

        if !extension_exists {
            warn!("TimescaleDB extension not found. This database may not have TimescaleDB installed.");
        } else {
            info!("TimescaleDB extension detected");
        }

        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to TimescaleDB database");

        Ok(Self {
            config,
            pool: Some(pool),
        })
    }

    /// Query pg_stat_statements and return structured data (same as PostgreSQL)
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
                .map_err(|e| anyhow::anyhow!("Failed to check pg_stat_statements: {}", e))?
            }
            Err(_) => {
                let rt = tokio::runtime::Runtime::new()
                    .context("Failed to create tokio runtime")?;
                rt.block_on(async {
                    sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')"
                    )
                    .fetch_one(pool)
                    .await
                })
                .map_err(|e| anyhow::anyhow!("Failed to check pg_stat_statements: {}", e))?
            }
        };

        if !extension_enabled {
            return Ok(Vec::new()); // Return empty if extension not enabled
        }

        let query = format!(
            "SELECT 
                pg_stat_statements.queryid,
                LEFT(pg_stat_statements.query, 200) as query,
                pg_stat_statements.calls,
                pg_stat_statements.total_exec_time as total_time_ms,
                pg_stat_statements.mean_exec_time as mean_time_ms,
                pg_stat_statements.min_exec_time as min_time_ms,
                pg_stat_statements.max_exec_time as max_time_ms
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
                .map_err(|e| anyhow::anyhow!("Failed to query pg_stat_statements: {}", e))?
            }
            Err(_) => {
                let rt = tokio::runtime::Runtime::new()
                    .context("Failed to create tokio runtime")?;
                rt.block_on(async {
                    sqlx::query(&query)
                        .bind(config.pg_stat_statements.limit)
                        .fetch_all(pool)
                        .await
                })
                .map_err(|e| anyhow::anyhow!("Failed to query pg_stat_statements: {}", e))?
            }
        };

        let mut metrics = Vec::new();
        for row in rows {
            let query_hash = format!("{:x}", md5::compute(row.get::<String, _>("query")));
            let tags = vec![
                KeyValue::new("host", config.host.clone()),
                KeyValue::new("source", "remote"),
                KeyValue::new("database", config.name.clone()),
                KeyValue::new("db_type", "timescaledb"),
                KeyValue::new("query_hash", query_hash),
            ];

            metrics.push(QueryMetricRow {
                calls: row.get("calls"),
                total_time_ms: row.get::<f64, _>("total_time_ms"),
                mean_time_ms: row.get("mean_time_ms"),
                min_time_ms: row.get("min_time_ms"),
                max_time_ms: row.get("max_time_ms"),
                tags,
            });
        }

        Ok(metrics)
    }

    /// Query TimescaleDB-specific metrics (hypertables, chunks, etc.)
    fn query_timescaledb_metrics(pool: &PgPool, config: &DatabaseConfig) -> Result<Vec<(String, f64, Vec<KeyValue>)>> {
        let base_tags = vec![
            KeyValue::new("host", config.host.clone()),
            KeyValue::new("source", "remote"),
            KeyValue::new("database", config.name.clone()),
            KeyValue::new("db_type", "timescaledb"),
        ];

        let mut metrics = Vec::new();

        // Helper function to execute async queries returning i64
        let execute_query_i64 = |query: &str| -> Result<i64> {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.block_on(async {
                        sqlx::query_scalar::<_, i64>(query)
                            .fetch_one(pool)
                            .await
                    })
                    .map_err(|e| anyhow::anyhow!("Query failed: {}", e))
                }
                Err(_) => {
                    let rt = tokio::runtime::Runtime::new()
                        .context("Failed to create tokio runtime")?;
                    rt.block_on(async {
                        sqlx::query_scalar::<_, i64>(query)
                            .fetch_one(pool)
                            .await
                    })
                    .map_err(|e| anyhow::anyhow!("Query failed: {}", e))
                }
            }
        };

        // Helper function to execute async queries returning bool
        let execute_query_bool = |query: &str| -> Result<bool> {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.block_on(async {
                        sqlx::query_scalar::<_, bool>(query)
                            .fetch_one(pool)
                            .await
                    })
                    .map_err(|e| anyhow::anyhow!("Query failed: {}", e))
                }
                Err(_) => {
                    let rt = tokio::runtime::Runtime::new()
                        .context("Failed to create tokio runtime")?;
                    rt.block_on(async {
                        sqlx::query_scalar::<_, bool>(query)
                            .fetch_one(pool)
                            .await
                    })
                    .map_err(|e| anyhow::anyhow!("Query failed: {}", e))
                }
            }
        };

        // Check if TimescaleDB extension exists
        let extension_exists: bool = execute_query_bool(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')"
        ).unwrap_or(false);

        if !extension_exists {
            return Ok(metrics); // Return empty if TimescaleDB not installed
        }

        // Query hypertables count
        let hypertables_count: i64 = execute_query_i64(
            "SELECT COUNT(*) FROM timescaledb_information.hypertables"
        ).unwrap_or(0);
        metrics.push(("timescaledb.hypertables.count".to_string(), hypertables_count as f64, base_tags.clone()));

        // Query total chunks count
        let chunks_count: i64 = execute_query_i64(
            "SELECT COUNT(*) FROM timescaledb_information.chunks"
        ).unwrap_or(0);
        metrics.push(("timescaledb.chunks.total_count".to_string(), chunks_count as f64, base_tags.clone()));

        // Query compressed chunks count
        let compressed_chunks: i64 = execute_query_i64(
            "SELECT COUNT(*) FROM timescaledb_information.chunks WHERE is_compressed = true"
        ).unwrap_or(0);
        metrics.push(("timescaledb.chunks.compressed_count".to_string(), compressed_chunks as f64, base_tags.clone()));

        // Query chunk statistics per hypertable
        let chunk_stats_query = "
            SELECT 
                hypertable_name,
                COUNT(*) as chunk_count,
                COUNT(*) FILTER (WHERE is_compressed = true) as compressed_count
            FROM timescaledb_information.chunks
            GROUP BY hypertable_name
        ";

        let chunk_stats: Vec<(String, i64, i64)> = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.block_on(async {
                    sqlx::query(chunk_stats_query)
                        .map(|row: sqlx::postgres::PgRow| {
                            (
                                row.get::<String, _>("hypertable_name"),
                                row.get::<i64, _>("chunk_count"),
                                row.get::<i64, _>("compressed_count"),
                            )
                        })
                        .fetch_all(pool)
                        .await
                })
                .unwrap_or_default()
            }
            Err(_) => {
                let rt = tokio::runtime::Runtime::new()
                    .context("Failed to create tokio runtime")?;
                rt.block_on(async {
                    sqlx::query(chunk_stats_query)
                        .map(|row: sqlx::postgres::PgRow| {
                            (
                                row.get::<String, _>("hypertable_name"),
                                row.get::<i64, _>("chunk_count"),
                                row.get::<i64, _>("compressed_count"),
                            )
                        })
                        .fetch_all(pool)
                        .await
                })
                .unwrap_or_default()
            }
        };

        for (hypertable_name, chunk_count, compressed_count) in chunk_stats {
            let mut tags = base_tags.clone();
            tags.push(KeyValue::new("hypertable", hypertable_name.clone()));
            metrics.push(("timescaledb.hypertable.chunk_count".to_string(), chunk_count as f64, tags.clone()));
            
            let mut tags_compressed = base_tags.clone();
            tags_compressed.push(KeyValue::new("hypertable", hypertable_name));
            metrics.push(("timescaledb.hypertable.compressed_chunk_count".to_string(), compressed_count as f64, tags_compressed));
        }

        Ok(metrics)
    }
}

impl Collector for TimescaleDBCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let pool = self.pool.clone();

        // Register PostgreSQL query metrics (same as PostgreSQL collector)
        let _query_calls = meter
            .u64_observable_counter("timescaledb.query.calls")
            .with_description("Number of times a query was executed")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_pg_stat_statements(p, &config) {
                            for metric in metrics {
                                observer.observe(metric.calls as u64, &metric.tags);
                            }
                        }
                    }
                }
            })
            .build();

        let _query_total_time = meter
            .f64_observable_gauge("timescaledb.query.total_time_ms")
            .with_description("Total execution time for queries in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_pg_stat_statements(p, &config) {
                            for metric in metrics {
                                observer.observe(metric.total_time_ms, &metric.tags);
                            }
                        }
                    }
                }
            })
            .build();

        let _query_mean_time = meter
            .f64_observable_gauge("timescaledb.query.mean_time_ms")
            .with_description("Mean execution time for queries in milliseconds")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_pg_stat_statements(p, &config) {
                            for metric in metrics {
                                observer.observe(metric.mean_time_ms, &metric.tags);
                            }
                        }
                    }
                }
            })
            .build();

        // Register TimescaleDB-specific metrics
        let _hypertables_count = meter
            .u64_observable_gauge("timescaledb.hypertables.count")
            .with_description("Number of hypertables in the database")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_timescaledb_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "timescaledb.hypertables.count" {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();

        let _chunks_count = meter
            .u64_observable_gauge("timescaledb.chunks.total_count")
            .with_description("Total number of chunks across all hypertables")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_timescaledb_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "timescaledb.chunks.total_count" {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();

        let _compressed_chunks = meter
            .u64_observable_gauge("timescaledb.chunks.compressed_count")
            .with_description("Number of compressed chunks")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_timescaledb_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "timescaledb.chunks.compressed_count" {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();

        // Register per-hypertable chunk metrics
        let _hypertable_chunks = meter
            .u64_observable_gauge("timescaledb.hypertable.chunk_count")
            .with_description("Number of chunks per hypertable")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_timescaledb_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "timescaledb.hypertable.chunk_count" {
                                    observer.observe(value as u64, &tags);
                                }
                            }
                        }
                    }
                }
            })
            .build();

        let _hypertable_compressed_chunks = meter
            .u64_observable_gauge("timescaledb.hypertable.compressed_chunk_count")
            .with_description("Number of compressed chunks per hypertable")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_timescaledb_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "timescaledb.hypertable.compressed_chunk_count" {
                                    observer.observe(value as u64, &tags);
                                }
                            }
                        }
                    }
                }
            })
            .build();

        // Register general TimescaleDB metrics gauge
        let _timescaledb_metrics = meter
            .f64_observable_gauge("timescaledb.metrics")
            .with_description("TimescaleDB-specific metrics")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_timescaledb_metrics(p, &config) {
                            for (_metric_name, value, tags) in metrics {
                                observer.observe(value, &tags);
                            }
                        }
                    }
                }
            })
            .build();

        info!("TimescaleDB collector initialized with PostgreSQL and TimescaleDB-specific metrics");
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled && self.config.query_metrics.enabled
    }
}
