//! Amazon Redshift database monitoring collector
//!
//! This collector connects directly to Redshift clusters (PostgreSQL-compatible)
//! and collects metrics from system tables and views.
//!
//! Redshift is PostgreSQL-compatible, so we use the PostgreSQL driver (sqlx).
//! However, Redshift has its own system tables and views for monitoring:
//! - stl_query (query history)
//! - stv_recents (recent queries)
//! - stl_wlm_query (WLM queue information)
//! - svv_table_info (table information)
//! - stl_scan (scan statistics)
//!
//! Metrics collected include:
//! - Standard PostgreSQL metrics (via pg_stat_statements if available)
//! - Redshift-specific metrics:
//!   - Query execution statistics
//!   - WLM queue length
//!   - Table scan statistics
//!   - Connection count

use anyhow::{Context, Result};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
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

pub struct RedshiftCollector {
    config: Arc<DatabaseConfig>,
    pool: Option<PgPool>,
}

impl RedshiftCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection string (Redshift uses PostgreSQL protocol)
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

        info!("Connecting to Redshift database...");
        let start = std::time::Instant::now();
        
        // Try to create connection pool
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&conn_str)
            .await
            .context(format!("Failed to connect to Redshift database: {}", config.name))?;

        // Verify it's actually Redshift by checking version
        let version: String = sqlx::query_scalar("SELECT version()")
            .fetch_one(&pool)
            .await
            .context("Failed to query Redshift version")?;

        if !version.to_lowercase().contains("redshift") {
            warn!("Connected database may not be Redshift. Version: {}", version);
        } else {
            info!("Redshift detected: {}", version);
        }

        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to Redshift database");

        Ok(Self {
            config,
            pool: Some(pool),
        })
    }

    /// Query pg_stat_statements and return structured data (if available)
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
                KeyValue::new("db_type", "redshift"),
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

    /// Query Redshift-specific metrics from system tables
    fn query_redshift_metrics(pool: &PgPool, config: &DatabaseConfig) -> Result<Vec<(String, f64, Vec<KeyValue>)>> {
        let base_tags = vec![
            KeyValue::new("host", config.host.clone()),
            KeyValue::new("source", "remote"),
            KeyValue::new("database", config.name.clone()),
            KeyValue::new("db_type", "redshift"),
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

        // Query active connections count
        let active_connections: i64 = execute_query_i64(
            "SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active'"
        ).unwrap_or(0);
        metrics.push(("redshift.connections.active".to_string(), active_connections as f64, base_tags.clone()));

        // Query total connections count
        let total_connections: i64 = execute_query_i64(
            "SELECT COUNT(*) FROM pg_stat_activity"
        ).unwrap_or(0);
        metrics.push(("redshift.connections.total".to_string(), total_connections as f64, base_tags.clone()));

        // Query WLM queue length (queries waiting in queue)
        // Note: This query may not work on all Redshift versions, so we handle errors gracefully
        let wlm_queue_length: i64 = execute_query_i64(
            "SELECT COUNT(*) FROM stv_wlm_query_state WHERE queue_start_time > CURRENT_TIMESTAMP - INTERVAL '1 hour'"
        ).unwrap_or(0);
        metrics.push(("redshift.wlm.queue_length".to_string(), wlm_queue_length as f64, base_tags.clone()));

        // Query recent query count (last hour)
        let recent_queries: i64 = execute_query_i64(
            "SELECT COUNT(*) FROM stl_query WHERE starttime > CURRENT_TIMESTAMP - INTERVAL '1 hour'"
        ).unwrap_or(0);
        metrics.push(("redshift.queries.recent_count".to_string(), recent_queries as f64, base_tags.clone()));

        // Query table count
        let table_count: i64 = execute_query_i64(
            "SELECT COUNT(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"
        ).unwrap_or(0);
        metrics.push(("redshift.tables.count".to_string(), table_count as f64, base_tags.clone()));

        Ok(metrics)
    }
}

impl Collector for RedshiftCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let pool = self.pool.clone();

        // Register PostgreSQL query metrics (if pg_stat_statements is available)
        let _query_calls = meter
            .u64_observable_counter("redshift.query.calls")
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
            .f64_observable_gauge("redshift.query.total_time_ms")
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
            .f64_observable_gauge("redshift.query.mean_time_ms")
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

        // Register Redshift-specific metrics
        let _connections_active = meter
            .u64_observable_gauge("redshift.connections.active")
            .with_description("Number of active connections")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_redshift_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "redshift.connections.active" {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();

        let _connections_total = meter
            .u64_observable_gauge("redshift.connections.total")
            .with_description("Total number of connections")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_redshift_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "redshift.connections.total" {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();

        let _wlm_queue_length = meter
            .u64_observable_gauge("redshift.wlm.queue_length")
            .with_description("Number of queries waiting in WLM queue")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_redshift_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "redshift.wlm.queue_length" {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();

        let _recent_queries = meter
            .u64_observable_gauge("redshift.queries.recent_count")
            .with_description("Number of queries executed in the last hour")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_redshift_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "redshift.queries.recent_count" {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();

        let _table_count = meter
            .u64_observable_gauge("redshift.tables.count")
            .with_description("Number of user tables")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_redshift_metrics(p, &config) {
                            for (name, value, tags) in metrics {
                                if name == "redshift.tables.count" {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();

        // Register general Redshift metrics gauge
        let _redshift_metrics = meter
            .f64_observable_gauge("redshift.metrics")
            .with_description("Redshift-specific metrics")
            .with_callback({
                let config = config.clone();
                let pool = pool.clone();
                move |observer| {
                    if let Some(ref p) = pool {
                        if let Ok(metrics) = Self::query_redshift_metrics(p, &config) {
                            for (_metric_name, value, tags) in metrics {
                                observer.observe(value, &tags);
                            }
                        }
                    }
                }
            })
            .build();

        info!("Redshift collector initialized with PostgreSQL and Redshift-specific metrics");
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled && self.config.query_metrics.enabled
    }
}
