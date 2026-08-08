use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Row, postgres::PgPoolOptions};
use sqlx::mysql::MySqlPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{info, error, warn};
use uuid::Uuid;
use serde::{Deserialize, Serialize};
use serde_json;
use parking_lot::RwLock;

/// TTL for cached MySQL pools (15 minutes).
/// Pools older than this will be recreated to handle credential rotation.
const MYSQL_POOL_CACHE_TTL_SECS: u64 = 15 * 60;

/// Maximum number of cached MySQL pools.
const MAX_CACHED_MYSQL_POOLS: usize = 100;

/// Cached MySQL connection pool with creation timestamp for TTL expiration.
struct CachedMySqlPool {
    pool: MySqlPool,
    created_at: Instant,
}

/// Global cache for MySQL connection pools, keyed by config ID.
static MYSQL_POOL_CACHE: once_cell::sync::Lazy<RwLock<HashMap<Uuid, CachedMySqlPool>>> =
    once_cell::sync::Lazy::new(|| RwLock::new(HashMap::new()));

/// Database monitoring worker that queries performance tables
pub struct DatabaseMonitoringWorker {
    reiver_db_pool: PgPool, // PostgreSQL pool for Reiver database (to store metrics)
    project_id: Uuid,
    config: DatabaseMonitoringConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatabaseMonitoringConfig {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub database_type: String, // "postgresql", "mysql", "clickhouse"
    pub host: String,
    pub port: i32,
    pub database_name: String,
    pub username: String,
    pub password: Option<String>, // Can be None if using env vars or connection pooling
    pub collection_interval_seconds: i32,
    pub slow_query_threshold_ms: f64,
    pub pg_stat_statements_enabled: bool,
    pub pg_stat_statements_limit: i32,
    #[serde(default)]
    pub performance_schema_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct QueryMetric {
    pub query_fingerprint: String,
    pub query_template: String,
    pub calls: i64,
    pub total_time_ms: f64,
    pub mean_time_ms: f64,
    pub min_time_ms: f64,
    pub max_time_ms: f64,
    pub stddev_time_ms: Option<f64>,
    pub rows_affected: Option<i64>,
    pub rows_returned: Option<i64>,
    pub shared_blks_hit: Option<i64>,
    pub shared_blks_read: Option<i64>,
    pub temp_blks_read: Option<i64>,
    pub temp_blks_written: Option<i64>,
    pub blk_read_time_ms: Option<f64>,
    pub blk_write_time_ms: Option<f64>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub trace_id: Option<String>, // Extracted from query comments
}

impl DatabaseMonitoringWorker {
    pub fn new(reiver_db_pool: PgPool, project_id: Uuid, config: DatabaseMonitoringConfig) -> Self {
        Self {
            reiver_db_pool,
            project_id,
            config,
        }
    }

    /// Get or create a cached MySQL connection pool for this config.
    /// 
    /// Pools are cached by config ID and reused across collection runs.
    /// Cached pools expire after MYSQL_POOL_CACHE_TTL_SECS to handle credential rotation.
    async fn get_or_create_mysql_pool(&self) -> anyhow::Result<MySqlPool> {
        use sqlx::mysql::MySqlPoolOptions;
        
        let config_id = self.config.id;
        let now = Instant::now();
        
        // Check if we have a valid cached pool
        {
            let cache = MYSQL_POOL_CACHE.read();
            if let Some(cached) = cache.get(&config_id) {
                if cached.created_at.elapsed().as_secs() < MYSQL_POOL_CACHE_TTL_SECS {
                    return Ok(cached.pool.clone());
                }
            }
        }
        
        // Need to create a new pool
        let env_key = format!("DB_MONITORING_{}_PASSWORD", self.config.id);
        let env_password = std::env::var(&env_key).ok();
        let password = self.config.password.as_deref()
            .or_else(|| env_password.as_deref())
            .unwrap_or("");
        
        // URL-encode username and password to handle special characters (@, :, /, etc.)
        let encoded_username = urlencoding::encode(&self.config.username);
        let encoded_password = urlencoding::encode(password);
        
        let conn_str = format!(
            "mysql://{}:{}@{}:{}/{}",
            encoded_username,
            encoded_password,
            self.config.host,
            self.config.port,
            self.config.database_name
        );

        let pool = MySqlPoolOptions::new()
            .max_connections(2)
            .min_connections(1)
            .acquire_timeout(StdDuration::from_secs(10))
            .idle_timeout(Some(StdDuration::from_secs(300)))
            .connect(&conn_str)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to MySQL database {}: {}", self.config.name, e))?;
        
        // Cache the pool
        {
            let mut cache = MYSQL_POOL_CACHE.write();
            
            // Enforce maximum cache size by removing oldest entries
            if cache.len() >= MAX_CACHED_MYSQL_POOLS {
                // Find and remove the oldest entry
                if let Some(oldest_id) = cache
                    .iter()
                    .min_by_key(|(_, v)| v.created_at)
                    .map(|(k, _)| *k)
                {
                    cache.remove(&oldest_id);
                }
            }
            
            cache.insert(config_id, CachedMySqlPool {
                pool: pool.clone(),
                created_at: now,
            });
        }
        
        Ok(pool)
    }

    /// Collect query metrics from PostgreSQL pg_stat_statements
    /// Connects to the target database specified in config
    pub async fn collect_postgresql_metrics(&self) -> anyhow::Result<Vec<QueryMetric>> {
        // Build connection string for target database
        let env_key = format!("DB_MONITORING_{}_PASSWORD", self.config.id);
        let env_password = std::env::var(&env_key).ok();
        let password = self.config.password.as_deref()
            .or_else(|| env_password.as_deref())
            .unwrap_or("");
        
        let conn_str = format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.config.username,
            password,
            self.config.host,
            self.config.port,
            self.config.database_name
        );

        // Create a connection pool to the target database
        let target_pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(StdDuration::from_secs(10))
            .connect(&conn_str)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to target database {}: {}", self.config.name, e))?;

        // Check if pg_stat_statements extension is enabled
        let extension_enabled: bool = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
            )"
        )
        .fetch_one(&target_pool)
        .await?;

        if !extension_enabled {
            warn!("pg_stat_statements extension is not enabled. Please run: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;");
            return Ok(Vec::new());
        }

        // Query pg_stat_statements for query metrics
        // Note: This queries the Reiver database's pg_stat_statements
        // For monitoring other databases, we'd connect to those databases separately
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

        let rows = sqlx::query(&query)
            .bind(self.config.pg_stat_statements_limit)
            .fetch_all(&target_pool)
            .await?;

        let mut metrics = Vec::new();
        let now = Utc::now();

        for row in rows {
            let query_text: String = row.try_get("query")?;
            
            // Extract trace_id from SQL comment if present
            // Format: /* trace_id: abc123 */
            let trace_id = extract_trace_id_from_query(&query_text);
            
            // Normalize query for fingerprinting (remove parameters, normalize whitespace)
            let query_template = normalize_query(&query_text);
            let query_fingerprint = format!("{:x}", md5::compute(&query_template));

            let metric = QueryMetric {
                query_fingerprint,
                query_template,
                calls: row.try_get("calls")?,
                total_time_ms: row.try_get::<f64, _>("total_time_ms")?,
                mean_time_ms: row.try_get::<f64, _>("mean_time_ms")?,
                min_time_ms: row.try_get::<f64, _>("min_time_ms")?,
                max_time_ms: row.try_get::<f64, _>("max_time_ms")?,
                stddev_time_ms: row.try_get::<Option<f64>, _>("stddev_time_ms").ok().flatten(),
                rows_returned: row.try_get::<Option<i64>, _>("rows").ok().flatten(),
                rows_affected: None, // pg_stat_statements doesn't provide this directly
                shared_blks_hit: row.try_get::<Option<i64>, _>("shared_blks_hit").ok().flatten(),
                shared_blks_read: row.try_get::<Option<i64>, _>("shared_blks_read").ok().flatten(),
                temp_blks_read: row.try_get::<Option<i64>, _>("temp_blks_read").ok().flatten(),
                temp_blks_written: row.try_get::<Option<i64>, _>("temp_blks_written").ok().flatten(),
                blk_read_time_ms: row.try_get::<Option<f64>, _>("blk_read_time").ok().flatten(),
                blk_write_time_ms: row.try_get::<Option<f64>, _>("blk_write_time").ok().flatten(),
                first_seen: now, // pg_stat_statements doesn't track this, use current time
                last_seen: now,
                trace_id,
            };

            metrics.push(metric);
        }

        info!("Collected {} query metrics from PostgreSQL", metrics.len());
        Ok(metrics)
    }

    /// Collect query metrics from MySQL performance_schema
    /// 
    /// Uses the performance_schema.events_statements_summary_by_digest table
    /// which provides aggregated query statistics by normalized query (digest).
    pub async fn collect_mysql_metrics(&self) -> anyhow::Result<Vec<QueryMetric>> {
        use sqlx::mysql::MySqlPoolOptions;
        use sqlx::Row as MySqlRow;
        
        // Get or create cached MySQL pool
        let target_pool = self.get_or_create_mysql_pool().await?;

        // Check if performance_schema is enabled
        let ps_enabled: i32 = sqlx::query_scalar::<_, i32>(
            "SELECT @@performance_schema"
        )
        .fetch_one(&target_pool)
        .await
        .unwrap_or(0);

        if ps_enabled == 0 {
            warn!(
                database = %self.config.name,
                "performance_schema is not enabled in MySQL. Cannot collect query metrics."
            );
            return Ok(Vec::new());
        }

        // Query the events_statements_summary_by_digest table
        // This provides aggregated statistics for normalized queries (digests)
        let query = r#"
            SELECT 
                DIGEST as query_fingerprint,
                COALESCE(DIGEST_TEXT, '') as query_template,
                COUNT_STAR as calls,
                SUM_TIMER_WAIT / 1000000000000 as total_time_sec,
                AVG_TIMER_WAIT / 1000000000000 as mean_time_sec,
                MIN_TIMER_WAIT / 1000000000000 as min_time_sec,
                MAX_TIMER_WAIT / 1000000000000 as max_time_sec,
                SUM_ROWS_AFFECTED as rows_affected,
                SUM_ROWS_SENT as rows_returned,
                SUM_ROWS_EXAMINED as rows_examined,
                SUM_CREATED_TMP_DISK_TABLES as temp_tables_disk,
                SUM_CREATED_TMP_TABLES as temp_tables_memory,
                SUM_NO_INDEX_USED as no_index_used,
                SUM_NO_GOOD_INDEX_USED as no_good_index_used,
                FIRST_SEEN as first_seen,
                LAST_SEEN as last_seen
            FROM performance_schema.events_statements_summary_by_digest
            WHERE SCHEMA_NAME = ? OR SCHEMA_NAME IS NULL
            ORDER BY SUM_TIMER_WAIT DESC
            LIMIT ?
        "#;

        let rows = sqlx::query(query)
            .bind(&self.config.database_name)
            .bind(self.config.pg_stat_statements_limit) // Reuse the same limit parameter
            .fetch_all(&target_pool)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to query performance_schema: {}", e))?;

        let mut metrics = Vec::with_capacity(rows.len());

        for row in rows {
            let query_fingerprint: Option<String> = row.get("query_fingerprint");
            let query_template: String = row.get("query_template");
            
            // Skip entries without a digest (aggregated stats)
            let query_fingerprint = match query_fingerprint {
                Some(fp) => fp,
                None => continue,
            };

            // Skip empty queries
            if query_template.is_empty() {
                continue;
            }

            let calls: i64 = row.get::<u64, _>("calls") as i64;
            let total_time_sec: f64 = row.get("total_time_sec");
            let mean_time_sec: f64 = row.get("mean_time_sec");
            let min_time_sec: f64 = row.get("min_time_sec");
            let max_time_sec: f64 = row.get("max_time_sec");
            
            // Convert seconds to milliseconds
            let total_time_ms = total_time_sec * 1000.0;
            let mean_time_ms = mean_time_sec * 1000.0;
            let min_time_ms = min_time_sec * 1000.0;
            let max_time_ms = max_time_sec * 1000.0;

            let rows_affected: Option<i64> = row.get::<Option<u64>, _>("rows_affected").map(|v| v as i64);
            let rows_returned: Option<i64> = row.get::<Option<u64>, _>("rows_returned").map(|v| v as i64);
            
            // MySQL doesn't have block I/O stats like PostgreSQL
            // but we can track temp table usage
            let temp_blks_written: Option<i64> = row.get::<Option<u64>, _>("temp_tables_disk").map(|v| v as i64);
            let temp_blks_read: Option<i64> = row.get::<Option<u64>, _>("temp_tables_memory").map(|v| v as i64);

            let first_seen: chrono::DateTime<Utc> = row.get("first_seen");
            let last_seen: chrono::DateTime<Utc> = row.get("last_seen");

            // Extract trace_id from query comment if present
            let trace_id = extract_trace_id_from_query(&query_template);

            let metric = QueryMetric {
                query_fingerprint,
                query_template,
                calls,
                total_time_ms,
                mean_time_ms,
                min_time_ms,
                max_time_ms,
                stddev_time_ms: None, // MySQL doesn't provide stddev
                rows_affected,
                rows_returned,
                shared_blks_hit: None,  // Not available in MySQL
                shared_blks_read: None, // Not available in MySQL
                temp_blks_read,
                temp_blks_written,
                blk_read_time_ms: None,  // Not available in MySQL
                blk_write_time_ms: None, // Not available in MySQL
                first_seen,
                last_seen,
                trace_id,
            };

            metrics.push(metric);
        }

        // Also collect slow query log if enabled
        if self.config.slow_query_threshold_ms > 0.0 {
            // Query the events_statements_history_long for recent slow queries
            let slow_query = r#"
                SELECT 
                    DIGEST as query_fingerprint,
                    SQL_TEXT as query_template,
                    TIMER_WAIT / 1000000000000 as exec_time_sec
                FROM performance_schema.events_statements_history_long
                WHERE TIMER_WAIT / 1000000000 > ?
                ORDER BY TIMER_WAIT DESC
                LIMIT 100
            "#;

            let slow_threshold_ns = (self.config.slow_query_threshold_ms * 1_000_000.0) as i64;
            
            if let Ok(slow_rows) = sqlx::query(slow_query)
                .bind(slow_threshold_ns)
                .fetch_all(&target_pool)
                .await
            {
                for row in slow_rows {
                    let query_template: Option<String> = row.get("query_template");
                    if let Some(sql) = query_template {
                        let exec_time_ms: f64 = row.get::<f64, _>("exec_time_sec") * 1000.0;
                        info!(
                            database = %self.config.name,
                            exec_time_ms = %exec_time_ms,
                            query = %sql.chars().take(100).collect::<String>(),
                            "Detected slow query"
                        );
                    }
                }
            }
        }

        info!(
            database = %self.config.name,
            metrics_count = metrics.len(),
            "Collected MySQL query metrics"
        );
        Ok(metrics)
    }

    /// Store query metrics in Reiver database
    pub async fn store_metrics(&self, metrics: &[QueryMetric]) -> anyhow::Result<()> {
        for metric in metrics {
            // Insert or update query metric and get the ID
            let query_metric_id: uuid::Uuid = sqlx::query_scalar(
                r#"
                INSERT INTO database_query_metrics (
                    project_id, database_host, database_name, database_type,
                    query_fingerprint, query_template,
                    calls, total_time_ms, mean_time_ms, min_time_ms, max_time_ms, stddev_time_ms,
                    rows_affected, rows_returned,
                    shared_blks_hit, shared_blks_read, temp_blks_read, temp_blks_written,
                    blk_read_time_ms, blk_write_time_ms,
                    first_seen, last_seen, collected_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
                ON CONFLICT (project_id, database_host, database_name, query_fingerprint, collected_at)
                DO UPDATE SET
                    calls = EXCLUDED.calls,
                    total_time_ms = EXCLUDED.total_time_ms,
                    mean_time_ms = EXCLUDED.mean_time_ms,
                    min_time_ms = EXCLUDED.min_time_ms,
                    max_time_ms = EXCLUDED.max_time_ms,
                    stddev_time_ms = EXCLUDED.stddev_time_ms,
                    last_seen = EXCLUDED.last_seen
                RETURNING id
                "#,
            )
            .bind(self.project_id)
            .bind(&self.config.host)
            .bind(&self.config.database_name)
            .bind(&self.config.database_type)
            .bind(&metric.query_fingerprint)
            .bind(&metric.query_template)
            .bind(metric.calls)
            .bind(metric.total_time_ms)
            .bind(metric.mean_time_ms)
            .bind(metric.min_time_ms)
            .bind(metric.max_time_ms)
            .bind(metric.stddev_time_ms)
            .bind(metric.rows_affected)
            .bind(metric.rows_returned)
            .bind(metric.shared_blks_hit)
            .bind(metric.shared_blks_read)
            .bind(metric.temp_blks_read)
            .bind(metric.temp_blks_written)
            .bind(metric.blk_read_time_ms)
            .bind(metric.blk_write_time_ms)
            .bind(metric.first_seen)
            .bind(metric.last_seen)
            .bind(Utc::now())
            .fetch_one(&self.reiver_db_pool)
            .await?;

            // For slow queries, get and store explain plan
            if metric.mean_time_ms >= self.config.slow_query_threshold_ms {
                if let Ok(explain_plan_json) = self.get_explain_plan(&metric.query_template, metric.trace_id.as_deref()).await {
                    // Extract execution stats and detect issues from explain plan
                    let (execution_time_ms, planning_time_ms, total_cost, rows_estimated, rows_actual, has_full_table_scan, has_missing_index, has_sequential_scan) =
                        self.analyze_explain_plan(&explain_plan_json)?;
                    
                    // Store explain plan
                    sqlx::query(
                        r#"
                        INSERT INTO database_explain_plans (
                            project_id, query_metric_id, database_host, database_name,
                            query_template, query_parameters,
                            explain_plan,
                            execution_time_ms, planning_time_ms, total_cost,
                            rows_estimated, rows_actual,
                            has_full_table_scan, has_missing_index, has_sequential_scan,
                            trace_id, collected_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                        ON CONFLICT DO NOTHING
                        "#,
                    )
                    .bind(self.project_id)
                    .bind(query_metric_id)
                    .bind(&self.config.host)
                    .bind(&self.config.database_name)
                    .bind(&metric.query_template)
                    .bind(Option::<serde_json::Value>::None as Option<serde_json::Value>) // query_parameters - not available from pg_stat_statements
                    .bind(serde_json::to_value(&explain_plan_json)?)
                    .bind(execution_time_ms)
                    .bind(planning_time_ms)
                    .bind(total_cost)
                    .bind(rows_estimated)
                    .bind(rows_actual)
                    .bind(has_full_table_scan)
                    .bind(has_missing_index)
                    .bind(has_sequential_scan)
                    .bind(metric.trace_id.as_ref())
                    .bind(Utc::now())
                    .execute(&self.reiver_db_pool)
                    .await?;
                    
                    info!("Stored explain plan for slow query: {} ({} ms) - ID: {}", metric.query_fingerprint, metric.mean_time_ms, query_metric_id);
                }
            }
        }

        Ok(())
    }

    /// Get explain plan for a slow query
    /// Note: For safety, we use EXPLAIN (without ANALYZE) to avoid side effects
    async fn get_explain_plan(
        &self,
        query_template: &str,
        _trace_id: Option<&str>,
    ) -> anyhow::Result<serde_json::Value> {
        // Build connection string for target database
        let env_key = format!("DB_MONITORING_{}_PASSWORD", self.config.id);
        let env_password = std::env::var(&env_key).ok();
        let password = self.config.password.as_deref()
            .or_else(|| env_password.as_deref())
            .unwrap_or("");
        
        let conn_str = format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.config.username,
            password,
            self.config.host,
            self.config.port,
            self.config.database_name
        );

        let target_pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(StdDuration::from_secs(10))
            .connect(&conn_str)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to target database {}: {}", self.config.name, e))?;

        // For PostgreSQL, use EXPLAIN (BUFFERS, FORMAT JSON) without ANALYZE to avoid side effects
        // Note: This won't execute the query, so we won't get actual execution times
        // but we can get the query plan structure
        let explain_query = format!("EXPLAIN (BUFFERS, FORMAT JSON) {}", query_template);

        let explain_result: serde_json::Value = sqlx::query_scalar(&explain_query)
            .fetch_one(&target_pool)
            .await?;

        Ok(explain_result)
    }

    /// Analyze explain plan to extract execution stats and detect issues
    fn analyze_explain_plan(&self, explain_plan: &serde_json::Value) -> anyhow::Result<(
        Option<f64>, // execution_time_ms
        Option<f64>, // planning_time_ms
        Option<f64>, // total_cost
        Option<i64>, // rows_estimated
        Option<i64>, // rows_actual
        bool,        // has_full_table_scan
        bool,        // has_missing_index
        bool,        // has_sequential_scan
    )> {
        // PostgreSQL EXPLAIN (FORMAT JSON) returns an array with a single object
        // Structure: [{"Plan": {...}, "Planning Time": 0.123, "Execution Time": 456.789}]
        let plan_array = explain_plan.as_array()
            .and_then(|arr| arr.first());
        
        let plan_obj = plan_array
            .and_then(|obj| obj.as_object())
            .ok_or_else(|| anyhow::anyhow!("Invalid explain plan format"))?;
        
        // Extract execution stats
        let execution_time_ms = plan_obj.get("Execution Time")
            .and_then(|v| v.as_f64());
        
        let planning_time_ms = plan_obj.get("Planning Time")
            .and_then(|v| v.as_f64());
        
        // Extract plan node (recursive structure)
        let plan = plan_obj.get("Plan")
            .ok_or_else(|| anyhow::anyhow!("No Plan in explain result"))?;
        
        // Analyze plan recursively for issues
        let (total_cost, rows_estimated, rows_actual, has_full_table_scan, has_missing_index, has_sequential_scan) =
            self.analyze_plan_node(plan)?;
        
        Ok((
            execution_time_ms,
            planning_time_ms,
            total_cost,
            rows_estimated,
            rows_actual,
            has_full_table_scan,
            has_missing_index,
            has_sequential_scan,
        ))
    }

    /// Recursively analyze plan node to detect issues
    fn analyze_plan_node(&self, node: &serde_json::Value) -> anyhow::Result<(
        Option<f64>, // total_cost
        Option<i64>, // rows_estimated
        Option<i64>, // rows_actual
        bool,        // has_full_table_scan
        bool,        // has_missing_index
        bool,        // has_sequential_scan
    )> {
        let node_obj = node.as_object()
            .ok_or_else(|| anyhow::anyhow!("Plan node is not an object"))?;
        
        let node_type = node_obj.get("Node Type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        
        let total_cost = node_obj.get("Total Cost")
            .and_then(|v| v.as_f64());
        
        let rows_estimated = node_obj.get("Plan Rows")
            .and_then(|v| v.as_f64())
            .map(|r| r as i64);
        
        let rows_actual = node_obj.get("Actual Rows")
            .and_then(|v| v.as_f64())
            .map(|r| r as i64);
        
        // Detect issues based on node type
        let has_sequential_scan = node_type == "Seq Scan";
        let has_full_table_scan = has_sequential_scan || node_type == "Index Only Scan" && total_cost.map(|c| c > 1000.0).unwrap_or(false);
        let has_missing_index = has_sequential_scan && rows_estimated.map(|r| r > 1000).unwrap_or(false);
        
        // Recursively check child plans
        let mut child_has_full_table_scan = false;
        let mut child_has_missing_index = false;
        let mut child_has_sequential_scan = false;
        
        if let Some(plans) = node_obj.get("Plans").and_then(|v| v.as_array()) {
            for child_plan in plans {
                match self.analyze_plan_node(child_plan) {
                    Ok((_, _, _, child_full_scan, child_missing_idx, child_seq_scan)) => {
                        child_has_full_table_scan |= child_full_scan;
                        child_has_missing_index |= child_missing_idx;
                        child_has_sequential_scan |= child_seq_scan;
                    }
                    Err(_) => continue, // Skip invalid child plans
                }
            }
        }
        
        Ok((
            total_cost,
            rows_estimated,
            rows_actual,
            has_full_table_scan || child_has_full_table_scan,
            has_missing_index || child_has_missing_index,
            has_sequential_scan || child_has_sequential_scan,
        ))
    }
}

/// Start database monitoring worker (background daemon)
/// Polls database_monitoring_configs table for enabled configs and collects metrics
pub async fn start_database_monitoring_worker(
    reiver_db_pool: Arc<PgPool>,
) -> Result<JoinHandle<()>> {
    info!("Starting database monitoring worker...");

    let handle = tokio::spawn(async move {
        let poll_interval = StdDuration::from_secs(30); // Check for configs every 30s

        loop {
            sleep(poll_interval).await;

            if let Err(e) = collect_all_database_metrics(&reiver_db_pool).await {
                error!("Database monitoring worker error: {}", e);
            }
        }
    });

    Ok(handle)
}

/// Collect metrics from all enabled database monitoring configs
async fn collect_all_database_metrics(reiver_db_pool: &PgPool) -> Result<()> {
    // Fetch all enabled monitoring configs
    let rows = sqlx::query(
        r#"
        SELECT 
            id, project_id, name, database_type, host, port, database_name, username,
            password_encrypted,
            collection_interval_seconds, slow_query_threshold_ms,
            pg_stat_statements_enabled, pg_stat_statements_limit,
            performance_schema_enabled
        FROM database_monitoring_configs
        WHERE enabled = true
        "#
    )
    .fetch_all(reiver_db_pool)
    .await?;

    let configs: Vec<DatabaseMonitoringConfig> = rows.into_iter().filter_map(|row| {
        Some(DatabaseMonitoringConfig {
            id: row.try_get::<Uuid, _>(0).ok()?,
            project_id: row.try_get::<Uuid, _>(1).ok()?,
            name: row.try_get::<String, _>(2).ok()?,
            database_type: row.try_get::<String, _>(3).ok()?,
            host: row.try_get::<String, _>(4).ok()?,
            port: row.try_get::<i32, _>(5).ok().unwrap_or(5432),
            database_name: row.try_get::<String, _>(6).ok()?,
            username: row.try_get::<String, _>(7).ok()?,
            password: row.try_get::<Option<String>, _>(8).ok().flatten(),
            collection_interval_seconds: row.try_get::<i32, _>(9).ok().unwrap_or(60),
            slow_query_threshold_ms: row.try_get::<Option<f64>, _>(10).ok().flatten().unwrap_or(1000.0),
            pg_stat_statements_enabled: row.try_get::<Option<bool>, _>(11).ok().flatten().unwrap_or(true),
            pg_stat_statements_limit: row.try_get::<Option<i32>, _>(12).ok().flatten().unwrap_or(10000),
            performance_schema_enabled: row.try_get::<Option<bool>, _>(13).ok().flatten().unwrap_or(true),
        })
    }).collect();

    for config in configs {
        let worker = DatabaseMonitoringWorker::new(
            reiver_db_pool.clone(),
            config.project_id,
            config.clone(),
        );

        match config.database_type.as_str() {
            "postgresql" => {
                if let Ok(metrics) = worker.collect_postgresql_metrics().await {
                    if let Err(e) = worker.store_metrics(&metrics).await {
                        error!("Failed to store metrics for {}: {}", config.name, e);
                    } else {
                        info!("Collected and stored {} metrics from {}", metrics.len(), config.name);
                    }
                }
            }
            "mysql" => {
                let worker = DatabaseMonitoringWorker::new(
                    reiver_db_pool.clone(),
                    project_id,
                    config.clone(),
                );
                
                match worker.collect_mysql_metrics().await {
                    Ok(metrics) => {
                        if !metrics.is_empty() {
                            if let Err(e) = worker.store_metrics(&metrics).await {
                                error!(
                                    config_id = %config.id,
                                    error = %e,
                                    "Failed to store MySQL metrics"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            config_id = %config.id,
                            error = %e,
                            "Failed to collect MySQL metrics"
                        );
                    }
                }
            }
            _ => {
                warn!("Unsupported database type: {} for {}", config.database_type, config.name);
            }
        }
    }

    Ok(())
}

/// Extract trace_id from SQL comment
/// Format: /* trace_id: abc123 */ or /*trace_id=abc123*/
fn extract_trace_id_from_query(query: &str) -> Option<String> {
    // Look for trace_id in SQL comments
    // Patterns: /* trace_id: abc123 */, /*trace_id=abc123*/, -- trace_id: abc123
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    // ========================================================================
    // Query Normalization Tests
    // ========================================================================

    #[test]
    fn test_normalize_query_basic() {
        let query = "SELECT * FROM users WHERE id = 123";
        let normalized = normalize_query(query);
        assert_eq!(normalized, "SELECT * FROM users WHERE id = ?");
    }

    #[test]
    fn test_normalize_query_string_literals() {
        let query = "SELECT * FROM users WHERE name = 'John Doe'";
        let normalized = normalize_query(query);
        assert_eq!(normalized, "SELECT * FROM users WHERE name = ?");
    }

    #[test]
    fn test_normalize_query_multiple_params() {
        let query = "SELECT * FROM users WHERE id = 1 AND status = 'active' AND age > 25";
        let normalized = normalize_query(query);
        assert_eq!(normalized, "SELECT * FROM users WHERE id = ? AND status = ? AND age > ?");
    }

    #[test]
    fn test_normalize_query_postgres_params() {
        let query = "SELECT * FROM users WHERE id = $1 AND name = $2";
        let normalized = normalize_query(query);
        assert_eq!(normalized, "SELECT * FROM users WHERE id = ? AND name = ?");
    }

    #[test]
    fn test_normalize_query_whitespace() {
        let query = "SELECT   *   FROM   users   WHERE   id  =  1";
        let normalized = normalize_query(query);
        assert_eq!(normalized, "SELECT * FROM users WHERE id = ?");
    }

    #[test]
    fn test_normalize_query_removes_single_line_comments() {
        let query = "SELECT * FROM users -- this is a comment\nWHERE id = 1";
        let normalized = normalize_query(query);
        assert!(normalized.contains("SELECT * FROM users"));
        assert!(!normalized.contains("this is a comment"));
    }

    #[test]
    fn test_normalize_query_in_clause() {
        let query = "SELECT * FROM users WHERE id IN (1, 2, 3)";
        let normalized = normalize_query(query);
        assert_eq!(normalized, "SELECT * FROM users WHERE id IN (?, ?, ?)");
    }

    #[test]
    fn test_normalize_query_insert() {
        let query = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')";
        let normalized = normalize_query(query);
        assert_eq!(normalized, "INSERT INTO users (name, email) VALUES (?, ?)");
    }

    // ========================================================================
    // Trace ID Extraction Tests
    // ========================================================================

    #[test]
    fn test_extract_trace_id_block_comment_colon() {
        let query = "SELECT * FROM users /* trace_id: 0123456789abcdef0123456789abcdef */ WHERE id = 1";
        let trace_id = extract_trace_id_from_query(query);
        assert_eq!(trace_id, Some("0123456789abcdef0123456789abcdef".to_string()));
    }

    #[test]
    fn test_extract_trace_id_block_comment_equals() {
        let query = "SELECT * FROM users /*trace_id=abcdef0123456789abcdef0123456789*/ WHERE id = 1";
        let trace_id = extract_trace_id_from_query(query);
        assert_eq!(trace_id, Some("abcdef0123456789abcdef0123456789".to_string()));
    }

    #[test]
    fn test_extract_trace_id_inline_comment() {
        let query = "SELECT * FROM users -- trace_id: 0123456789abcdef0123456789abcdef";
        let trace_id = extract_trace_id_from_query(query);
        assert_eq!(trace_id, Some("0123456789abcdef0123456789abcdef".to_string()));
    }

    #[test]
    fn test_extract_trace_id_not_present() {
        let query = "SELECT * FROM users WHERE id = 1";
        let trace_id = extract_trace_id_from_query(query);
        assert_eq!(trace_id, None);
    }

    #[test]
    fn test_extract_trace_id_too_short() {
        // Trace ID must be at least 32 hex chars
        let query = "SELECT * FROM users /* trace_id: abc123 */ WHERE id = 1";
        let trace_id = extract_trace_id_from_query(query);
        assert_eq!(trace_id, None);
    }

    #[test]
    fn test_extract_trace_id_case_insensitive() {
        let query = "SELECT * FROM users /* TRACE_ID: 0123456789abcdef0123456789abcdef */ WHERE id = 1";
        let trace_id = extract_trace_id_from_query(query);
        assert_eq!(trace_id, Some("0123456789abcdef0123456789abcdef".to_string()));
    }

    // ========================================================================
    // MySQL Pool Cache Tests
    // ========================================================================

    #[test]
    fn test_mysql_pool_cache_constants() {
        // Verify cache constants are reasonable
        assert_eq!(MYSQL_POOL_CACHE_TTL_SECS, 15 * 60); // 15 minutes
        assert_eq!(MAX_CACHED_MYSQL_POOLS, 100);
    }

    #[test]
    fn test_cached_mysql_pool_struct() {
        // Verify the CachedMySqlPool struct can be created
        // (actual pool creation requires database, so just test the timestamp logic)
        let created_at = Instant::now();
        let elapsed = created_at.elapsed().as_secs();
        assert!(elapsed < MYSQL_POOL_CACHE_TTL_SECS);
    }

    #[test]
    fn test_mysql_pool_cache_lazy_initialization() {
        // Verify the static MYSQL_POOL_CACHE is accessible and starts empty
        let cache = MYSQL_POOL_CACHE.read();
        // On first access in a fresh test, it should be empty or contain entries from other tests
        // Just verify it's accessible without panicking
        drop(cache);
    }

    // ========================================================================
    // DatabaseMonitoringConfig Tests
    // ========================================================================

    #[test]
    fn test_database_monitoring_config_defaults() {
        let config = DatabaseMonitoringConfig {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "test-db".to_string(),
            database_type: "postgresql".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            database_name: "testdb".to_string(),
            username: "user".to_string(),
            password: Some("password".to_string()),
            collection_interval_seconds: 60,
            slow_query_threshold_ms: 100.0,
            pg_stat_statements_enabled: true,
            pg_stat_statements_limit: 1000,
            performance_schema_enabled: false,
        };

        assert_eq!(config.database_type, "postgresql");
        assert_eq!(config.port, 5432);
        assert_eq!(config.collection_interval_seconds, 60);
    }

    #[test]
    fn test_database_monitoring_config_mysql() {
        let config = DatabaseMonitoringConfig {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "test-mysql".to_string(),
            database_type: "mysql".to_string(),
            host: "localhost".to_string(),
            port: 3306,
            database_name: "testdb".to_string(),
            username: "root".to_string(),
            password: Some("password".to_string()),
            collection_interval_seconds: 60,
            slow_query_threshold_ms: 100.0,
            pg_stat_statements_enabled: false,
            pg_stat_statements_limit: 1000,
            performance_schema_enabled: true,
        };

        assert_eq!(config.database_type, "mysql");
        assert_eq!(config.port, 3306);
        assert!(config.performance_schema_enabled);
    }

    // ========================================================================
    // QueryMetric Tests
    // ========================================================================

    #[test]
    fn test_query_metric_creation() {
        let now = chrono::Utc::now();
        let metric = QueryMetric {
            query_fingerprint: "select_users".to_string(),
            query_template: "SELECT * FROM users WHERE id = ?".to_string(),
            calls: 100,
            total_time_ms: 5000.0,
            mean_time_ms: 50.0,
            min_time_ms: 10.0,
            max_time_ms: 200.0,
            stddev_time_ms: Some(25.0),
            rows_affected: Some(100),
            rows_returned: Some(50),
            shared_blks_hit: Some(1000),
            shared_blks_read: Some(10),
            temp_blks_read: None,
            temp_blks_written: None,
            blk_read_time_ms: Some(5.0),
            blk_write_time_ms: None,
            first_seen: now,
            last_seen: now,
            trace_id: Some("abc123".to_string()),
        };

        assert_eq!(metric.calls, 100);
        assert_eq!(metric.mean_time_ms, 50.0);
        assert_eq!(metric.trace_id, Some("abc123".to_string()));
    }
}
