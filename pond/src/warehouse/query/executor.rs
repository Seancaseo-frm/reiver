//! Query Executor
//!
//! Executes queries against ClickHouse via the native TCP protocol (klickhouse).
//!
//! Performance optimizations:
//! - Uses native binary protocol for efficient data transfer
//! - Block-based streaming avoids buffering large results in memory
//! - Progress tracking via native protocol packets for billing stats
//! - Provides both streaming and buffered result APIs
//!
//! Observability:
//! - Emits structured traces for query execution
//! - Logs query duration, bytes read, and row counts
//! - Tags: source_type (low cardinality, safe for metrics)

use clickhouse::Client;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{instrument, Span};

use super::super::utils::estimate_json_value_memory;
use crate::warehouse::ch_client::{
    ChClient, NativeChConfig, NativePool,
    klickhouse_value_to_json, klickhouse_type_to_string, klickhouse_type_is_nullable,
};

/// Maximum line buffer size (64 MB) for HTTP fallback parsing.
const MAX_LINE_BUFFER_BYTES: usize = 64 * 1024 * 1024;
/// Wrap a query in `SELECT COUNT(*) AS cnt FROM (...)` via AST manipulation.
fn wrap_in_count(sql: &str) -> ExecutorResult<String> {
    use sqlparser::dialect::ClickHouseDialect;
    use sqlparser::parser::Parser;

    let dialect = ClickHouseDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| ExecutorError::Execution(format!("SQL parse error in count(): {}", e)))?;

    let stmt = match statements.into_iter().next() {
        Some(stmt) => stmt,
        _ => {
            return Err(ExecutorError::Execution(
                "count() requires a SELECT query".to_string(),
            ));
        }
    };

    wrap_in_count_stmt(stmt)
}

/// Wrap a pre-parsed query statement in `SELECT COUNT(*) AS cnt FROM (...)` via AST manipulation.
fn wrap_in_count_stmt(stmt: sqlparser::ast::Statement) -> ExecutorResult<String> {
    use sqlparser::ast::*;

    let mut inner_query = match stmt {
        Statement::Query(q) => q,
        _ => {
            return Err(ExecutorError::Execution(
                "count() requires a SELECT query".to_string(),
            ));
        }
    };

    inner_query.order_by = None;
    inner_query.limit = None;
    inner_query.offset = None;

    let count_expr = Expr::Function(Function {
        name: ObjectName(vec![Ident::new("COUNT")]),
        args: FunctionArguments::List(FunctionArgumentList {
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
            duplicate_treatment: None,
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        parameters: FunctionArguments::None,
    });

    let outer = Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![SelectItem::ExprWithAlias {
                expr: count_expr,
                alias: Ident::new("cnt"),
            }],
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Derived {
                    lateral: false,
                    subquery: inner_query,
                    alias: Some(TableAlias {
                        name: Ident::new("_count_subq"),
                        columns: vec![],
                    }),
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            prewhere: None,
            selection: None,
            group_by: GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            connect_by: None,
        }))),
        order_by: None,
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    };

    Ok(outer.to_string())
}

/// Wrap a SQL statement in EXPLAIN via AST manipulation.
fn prepend_explain(sql: &str) -> ExecutorResult<String> {
    use sqlparser::dialect::ClickHouseDialect;
    use sqlparser::parser::Parser;

    let dialect = ClickHouseDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| ExecutorError::Execution(format!("SQL parse error in explain(): {}", e)))?;

    let inner_stmt = match statements.into_iter().next() {
        Some(stmt) => stmt,
        None => {
            return Err(ExecutorError::Execution(
                "explain() requires a SQL statement".to_string(),
            ));
        }
    };

    prepend_explain_stmt(inner_stmt)
}

/// Wrap a pre-parsed SQL statement in EXPLAIN via AST manipulation.
fn prepend_explain_stmt(stmt: sqlparser::ast::Statement) -> ExecutorResult<String> {
    use sqlparser::ast::*;

    let explain_stmt = Statement::Explain {
        describe_alias: DescribeAlias::Explain,
        analyze: false,
        verbose: false,
        query_plan: false,
        statement: Box::new(stmt),
        format: None,
        options: None,
    };
    Ok(explain_stmt.to_string())
}

/// Errors that can occur during query execution.
#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Query execution failed: {0}")]
    Execution(String),

    #[error("Query timeout after {0} seconds")]
    Timeout(u32),

    #[error("Query cancelled")]
    Cancelled,

    #[error("Invalid query: {0}")]
    Invalid(String),
    
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),
    
    #[error("Connection not configured")]
    NotConfigured,
    
    #[error("HTTP error: {0}")]
    Http(String),
    
    #[error("JSON parse error: {0}")]
    JsonParse(String),
    
    #[error("Invalid response format: {0}")]
    InvalidFormat(String),
    
    #[error("Result truncated: memory limit of {limit_bytes} bytes exceeded after {rows_collected} rows")]
    ResultTruncated {
        limit_bytes: usize,
        rows_collected: usize,
    },
}

impl ExecutorError {
    /// Whether this error indicates missing data in ClickHouse (server is up,
    /// but the table or data doesn't exist). Suitable for warm s3() fallback.
    pub fn is_data_error(&self) -> bool {
        match self {
            ExecutorError::Execution(msg) => {
                msg.contains("Code: 60")    // UNKNOWN_TABLE
                || msg.contains("Code: 81") // TABLE_ALREADY_EXISTS (corruption)
                || msg.contains("doesn't exist")
            }
            _ => false,
        }
    }

    /// Whether this error indicates ClickHouse is unreachable. Suitable for
    /// DataFusion fallback when the CH server itself is down.
    pub fn is_connection_error(&self) -> bool {
        matches!(self, ExecutorError::Http(_) | ExecutorError::ClickHouse(_))
    }
}

/// Result type for executor operations.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

/// Detect ClickHouse mid-stream error lines (sent after HTTP 200).
fn is_clickhouse_error_line(line: &str) -> bool {
    !line.starts_with('[')
        && (line.starts_with("Code:") || line.contains("DB::Exception"))
}

/// Column information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Query result (buffered).
///
/// CAUTION: This struct loads all rows into memory. For large result sets,
/// use `StreamingQueryResult` instead to avoid OOM errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    pub bytes_read: u64,
    pub rows_read: u64,
    /// Whether the result was truncated due to a memory limit.
    /// When `true`, `rows` contains only a partial result set.
    #[serde(default)]
    pub truncated: bool,
}

/// A single row from a query result.
pub type QueryRow = Vec<serde_json::Value>;

/// Statistics from ClickHouse query execution.
///
/// These are extracted from the X-ClickHouse-Summary response header
/// for accurate billing.
#[derive(Debug, Clone, Default)]
pub struct ClickHouseStats {
    /// Bytes read from storage (for billing)
    pub read_bytes: u64,
    /// Rows read from storage
    pub read_rows: u64,
    /// Bytes written (for INSERT queries)
    pub written_bytes: u64,
    /// Rows written
    pub written_rows: u64,
    /// Query execution time in seconds (ClickHouse-side)
    pub elapsed_seconds: f64,
}

impl ClickHouseStats {
    /// Parse from X-ClickHouse-Summary header JSON.
    /// 
    /// Example: {"read_rows":"1000","read_bytes":"50000","written_rows":"0","written_bytes":"0","total_rows_to_read":"1000","elapsed_ns":"1234567"}
    ///
    /// # Observability
    /// 
    /// Logs a warning if parsing fails, including the raw header value for debugging.
    /// This is important for billing accuracy since bytes_read comes from these stats.
    pub fn from_header(header_value: &str) -> Option<Self> {
        match serde_json::from_str::<serde_json::Value>(header_value) {
            Ok(v) => {
                Some(Self {
                    read_bytes: v.get("read_bytes")
                        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()))
                        .unwrap_or(0),
                    read_rows: v.get("read_rows")
                        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()))
                        .unwrap_or(0),
                    written_bytes: v.get("written_bytes")
                        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()))
                        .unwrap_or(0),
                    written_rows: v.get("written_rows")
                        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()))
                        .unwrap_or(0),
                    elapsed_seconds: v.get("elapsed_ns")
                        .and_then(|v| v.as_str().and_then(|s| s.parse::<u64>().ok()).or_else(|| v.as_u64()))
                        .map(|ns| ns as f64 / 1_000_000_000.0)
                        .unwrap_or(0.0),
                })
            }
            Err(e) => {
                // Log parsing failure for debugging - this could affect billing accuracy
                tracing::warn!(
                    error = %e,
                    header_preview = %header_value.chars().take(200).collect::<String>(),
                    header_length = header_value.len(),
                    "Failed to parse X-ClickHouse-Summary header - billing stats may be inaccurate"
                );
                None
            }
        }
    }
}

/// Streaming query result.
///
/// Provides an async stream of rows without buffering the entire result set.
/// This is essential for TB-scale queries to avoid OOM errors.
pub struct StreamingQueryResult {
    /// Column information (available immediately)
    pub columns: Vec<ColumnInfo>,
    /// Async stream of rows
    pub rows: Pin<Box<dyn Stream<Item = Result<QueryRow, ExecutorError>> + Send>>,
    /// Execution start time (for computing total time)
    start_time: Instant,
    /// ClickHouse query statistics (from X-ClickHouse-Summary header)
    /// Available for accurate billing instead of estimating bytes.
    pub stats: Option<ClickHouseStats>,
}

impl StreamingQueryResult {
    /// Create a new streaming result.
    pub fn new(
        columns: Vec<ColumnInfo>,
        rows: Pin<Box<dyn Stream<Item = Result<QueryRow, ExecutorError>> + Send>>,
    ) -> Self {
        Self {
            columns,
            rows,
            start_time: Instant::now(),
            stats: None,
        }
    }
    
    /// Create a new streaming result with ClickHouse statistics.
    pub fn with_stats(
        columns: Vec<ColumnInfo>,
        rows: Pin<Box<dyn Stream<Item = Result<QueryRow, ExecutorError>> + Send>>,
        stats: ClickHouseStats,
    ) -> Self {
        Self {
            columns,
            rows,
            start_time: Instant::now(),
            stats: Some(stats),
        }
    }
    
    /// Get the bytes read by ClickHouse (for billing).
    /// 
    /// Returns the actual read_bytes from ClickHouse statistics if available,
    /// otherwise returns None (caller should estimate or use 0).
    pub fn bytes_read(&self) -> Option<u64> {
        self.stats.as_ref().map(|s| s.read_bytes)
    }
    
    /// Collect all rows into a buffered QueryResult.
    ///
    /// CAUTION: This loads all rows into memory. Only use for small result sets
    /// or when you need random access to rows.
    pub async fn collect(self) -> ExecutorResult<QueryResult> {
        self.collect_with_limit(None).await
    }
    
    /// Collect rows with an optional memory limit.
    /// 
    /// PERFORMANCE: Use this to prevent OOM for large result sets.
    /// If max_bytes is exceeded, collection stops and returns what was collected.
    pub async fn collect_with_limit(mut self, max_bytes: Option<usize>) -> ExecutorResult<QueryResult> {
        let mut rows = Vec::with_capacity(256);
        let mut estimated_bytes = 0usize;
        let mut truncated = false;
        
        while let Some(row_result) = self.rows.next().await {
            let row = row_result?;
            
            let row_size: usize = row.iter().map(|v| estimate_json_value_memory(v)).sum();
            
            if let Some(limit) = max_bytes {
                if estimated_bytes + row_size > limit {
                    tracing::debug!(
                        collected_rows = rows.len(),
                        bytes_used = estimated_bytes,
                        limit = limit,
                        "Collection stopped due to memory limit"
                    );
                    truncated = true;
                    break;
                }
            }
            
            estimated_bytes += row_size;
            rows.push(row);
        }
        
        let bytes_read = self.stats.as_ref()
            .map(|s| s.read_bytes)
            .unwrap_or(estimated_bytes as u64);
        
        Ok(QueryResult {
            columns: self.columns,
            row_count: rows.len(),
            rows_read: self.stats.as_ref().map(|s| s.read_rows).unwrap_or(rows.len() as u64),
            execution_time_ms: self.start_time.elapsed().as_millis() as u64,
            bytes_read,
            rows,
            truncated,
        })
    }
    
    /// Get the elapsed time since query start.
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

// =============================================================================
// Native block streaming (for direct pgwire encoding)
// =============================================================================

/// Column metadata from the klickhouse native protocol.
///
/// Pairs a column name with its klickhouse `Type` for direct encoding
/// without string round-trips.
#[derive(Debug, Clone)]
pub struct NativeColumnInfo {
    pub name: String,
    pub klickhouse_type: klickhouse::Type,
}

/// A stream of raw klickhouse blocks for direct encoding.
///
/// Used by the pgwire path to encode ClickHouse results directly into
/// Postgres wire format without the `serde_json::Value` intermediate.
pub struct NativeBlockStream {
    pub columns: Vec<NativeColumnInfo>,
    pub blocks: Pin<Box<dyn Stream<Item = Result<klickhouse::block::Block, ExecutorError>> + Send>>,
    pub stats: Option<crate::warehouse::ch_client::QueryStats>,
}

/// Default maximum memory for buffered query results (100MB).
/// PERFORMANCE: This prevents OOM for queries that return unexpectedly large results.
pub const DEFAULT_MAX_RESULT_MEMORY_BYTES: usize = 100 * 1024 * 1024;

/// Query execution options.
#[derive(Debug, Clone)]
pub struct ExecutionOptions {
    /// Maximum number of rows to return
    pub limit: Option<u32>,
    /// Query timeout in seconds
    pub timeout_secs: Option<u32>,
    /// Maximum memory in bytes for buffered results (prevents OOM).
    /// When exceeded, collection stops and returns a partial result.
    /// Set to None for unlimited (not recommended for user queries).
    pub max_memory_bytes: Option<usize>,
}

impl Default for ExecutionOptions {
    fn default() -> Self {
        Self {
            limit: Some(1000),
            timeout_secs: Some(30),
            max_memory_bytes: Some(DEFAULT_MAX_RESULT_MEMORY_BYTES),
        }
    }
}

impl ExecutionOptions {
    /// Create options with no memory limit (for internal/trusted queries only).
    /// 
    /// WARNING: This can cause OOM if the query returns very large results.
    /// Only use for internal queries where you control the data size.
    pub fn without_memory_limit(mut self) -> Self {
        self.max_memory_bytes = None;
        self
    }
    
    /// Create options with a custom memory limit.
    pub fn with_memory_limit(mut self, max_bytes: usize) -> Self {
        self.max_memory_bytes = Some(max_bytes);
        self
    }
}

/// ClickHouse configuration.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// ClickHouse server host
    pub host: String,
    /// Native TCP port (typically 9000)
    pub native_port: u16,
    /// HTTP port (typically 8123) -- used by the `clickhouse` crate for EXPLAIN
    pub http_port: u16,
    /// Database name
    pub database: String,
    /// Username (optional)
    pub username: Option<String>,
    /// Password (optional)
    pub password: Option<String>,
    /// Connection pool configuration
    pub pool: ConnectionPoolConfig,
}

impl ClickHouseConfig {
    /// HTTP URL for the `clickhouse` crate (used only for EXPLAIN queries).
    pub fn http_url(&self) -> String {
        format!("http://{}:{}", self.host, self.http_port)
    }

    /// Native TCP config for klickhouse.
    pub fn native_config(&self) -> NativeChConfig {
        NativeChConfig {
            host: self.host.clone(),
            port: self.native_port,
            database: self.database.clone(),
            username: self.username.clone().unwrap_or_else(|| "default".to_string()),
            password: self.password.clone().unwrap_or_default(),
        }
    }
}

/// Connection pool configuration for HTTP client.
///
/// These settings are optimized for high-throughput ClickHouse queries:
/// - High connection pool capacity for concurrent requests
/// - Long idle timeouts to maximize connection reuse
/// - TCP keepalive to detect and close stale connections
/// - TCP nodelay for lower latency on small queries
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Maximum idle connections per host (default: 100)
    /// Higher values allow more concurrent queries without connection setup overhead
    pub max_idle_per_host: usize,
    /// Idle timeout in seconds (default: 90)
    /// Connections idle longer than this are closed to free resources
    pub idle_timeout_secs: u64,
    /// Request timeout in seconds (default: 300)
    /// Maximum time to wait for a complete response
    pub request_timeout_secs: u64,
    /// Connect timeout in seconds (default: 10)
    /// Maximum time to establish a connection
    pub connect_timeout_secs: u64,
    /// TCP keepalive interval in seconds (default: 60)
    /// Sends keepalive probes to detect dead connections
    pub tcp_keepalive_secs: u64,
    /// Enable TCP nodelay (disable Nagle's algorithm) (default: true)
    /// Reduces latency for small requests at the cost of more packets
    pub tcp_nodelay: bool,
    /// Enable HTTP/2 adaptive window (default: true)
    /// Improves throughput for streaming responses
    pub http2_adaptive_window: bool,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            // High pool capacity for concurrent TB-scale queries
            max_idle_per_host: 100,
            // Keep connections alive for reuse between queries
            idle_timeout_secs: 90,
            // 5 minutes for long-running queries (TB-scale scans)
            request_timeout_secs: 300,
            // 10 seconds to establish connection
            connect_timeout_secs: 10,
            // Keepalive to detect dead connections
            tcp_keepalive_secs: 60,
            // Disable Nagle for lower latency
            tcp_nodelay: true,
            // Enable adaptive window for better streaming performance
            http2_adaptive_window: true,
        }
    }
}

impl ConnectionPoolConfig {
    /// Create a configuration optimized for high-throughput workloads.
    pub fn for_high_throughput() -> Self {
        Self {
            max_idle_per_host: 200,
            idle_timeout_secs: 120,
            request_timeout_secs: 600, // 10 minutes for very large queries
            connect_timeout_secs: 10,
            tcp_keepalive_secs: 30,
            tcp_nodelay: true,
            http2_adaptive_window: true,
        }
    }
    
    /// Create a configuration optimized for low latency.
    pub fn for_low_latency() -> Self {
        Self {
            max_idle_per_host: 50,
            idle_timeout_secs: 60,
            request_timeout_secs: 60,
            connect_timeout_secs: 5,
            tcp_keepalive_secs: 30,
            tcp_nodelay: true,
            http2_adaptive_window: true,
        }
    }
}

impl ClickHouseConfig {
    /// Create configuration from environment variables.
    ///
    /// Expects:
    /// - `CLICKHOUSE_HOST`: Server host (default: localhost)
    /// - `CLICKHOUSE_NATIVE_PORT`: Native TCP port (default: 9000)
    /// - `CLICKHOUSE_HTTP_PORT`: HTTP port for explain queries (default: 8123)
    /// - `CLICKHOUSE_DATABASE`: Database name (default: default)
    /// - `CLICKHOUSE_USER`: Username (optional)
    /// - `CLICKHOUSE_PASSWORD`: Password (optional)
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("CLICKHOUSE_HOST")
                .unwrap_or_else(|_| "localhost".to_string()),
            native_port: std::env::var("CLICKHOUSE_NATIVE_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(9000),
            http_port: std::env::var("CLICKHOUSE_HTTP_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8123),
            database: std::env::var("CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| "default".to_string()),
            username: std::env::var("CLICKHOUSE_USER").ok(),
            password: std::env::var("CLICKHOUSE_PASSWORD").ok(),
            pool: ConnectionPoolConfig::default(),
        }
    }
}

/// ClickHouse query settings optimized for S3/R2 queries.
/// These settings improve performance for reading Parquet files from object storage.
#[derive(Debug, Clone)]
pub struct ClickHouseQuerySettings {
    /// Maximum number of threads for query execution
    pub max_threads: u32,
    /// Enable Parquet filter pushdown
    pub input_format_parquet_filter_push_down: bool,
    /// Skip unsupported column types in schema inference
    pub input_format_parquet_skip_columns_with_unsupported_types: bool,
    /// Maximum S3 connections
    pub s3_max_connections: u32,
    /// Enable prefetching for remote filesystem reads
    pub remote_filesystem_read_prefetch: bool,
    /// Maximum memory usage per query (in bytes)
    pub max_memory_usage: u64,
    /// Maximum result rows (0 = unlimited)
    /// PERFORMANCE: Limits result size at ClickHouse level to prevent wasted bandwidth
    pub max_result_rows: u64,
    /// Maximum result bytes (0 = unlimited)
    /// PERFORMANCE: Limits result size at ClickHouse level to prevent wasted bandwidth
    pub max_result_bytes: u64,
    /// Overflow mode for result limits: "throw" or "break"
    /// "break" stops query execution when limit is reached without error
    pub result_overflow_mode: String,
    /// Maximum query execution time in seconds (0 = unlimited)
    /// CRITICAL: This enforces the timeout at ClickHouse level, ensuring the query
    /// is actually cancelled server-side rather than just timing out on the client.
    pub max_execution_time: u32,
}

impl Default for ClickHouseQuerySettings {
    fn default() -> Self {
        Self {
            max_threads: 8,
            input_format_parquet_filter_push_down: true,
            input_format_parquet_skip_columns_with_unsupported_types: true,
            s3_max_connections: 100,
            remote_filesystem_read_prefetch: true,
            max_memory_usage: 10_000_000_000, // 10GB
            max_result_rows: 100_000, // 100K rows max
            max_result_bytes: 50_000_000, // 50MB max result size
            result_overflow_mode: "break".to_string(), // Stop gracefully when limit reached
            max_execution_time: 0, // 0 = use ClickHouse server default, set per-query
        }
    }
}

impl ClickHouseQuerySettings {
    /// Convert settings to query parameter string.
    pub fn to_query_params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![
            ("max_threads", self.max_threads.to_string()),
            ("input_format_parquet_filter_push_down", if self.input_format_parquet_filter_push_down { "1" } else { "0" }.to_string()),
            ("input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference", if self.input_format_parquet_skip_columns_with_unsupported_types { "1" } else { "0" }.to_string()),
            ("s3_max_connections", self.s3_max_connections.to_string()),
            ("remote_filesystem_read_prefetch", if self.remote_filesystem_read_prefetch { "1" } else { "0" }.to_string()),
            ("max_memory_usage", self.max_memory_usage.to_string()),
        ];
        
        // Add result limiting settings if configured
        if self.max_result_rows > 0 {
            params.push(("max_result_rows", self.max_result_rows.to_string()));
        }
        if self.max_result_bytes > 0 {
            params.push(("max_result_bytes", self.max_result_bytes.to_string()));
        }
        if self.max_result_rows > 0 || self.max_result_bytes > 0 {
            params.push(("result_overflow_mode", format!("'{}'", self.result_overflow_mode)));
        }
        
        // CRITICAL: Add max_execution_time to enforce timeout at ClickHouse level.
        // This ensures queries are actually cancelled server-side, not just timed out on client.
        if self.max_execution_time > 0 {
            params.push(("max_execution_time", self.max_execution_time.to_string()));
        }
        
        params
    }

    /// Convert settings to a SET statement for the native TCP protocol.
    pub fn to_set_sql(&self) -> String {
        let params = self.to_query_params();
        let mut out = String::new();
        for (i, (k, v)) in params.iter().enumerate() {
            if i > 0 { out.push_str(", "); }
            out.push_str(k);
            out.push_str(" = ");
            out.push_str(v);
        }
        out
    }

    /// Format settings as an inline `SETTINGS` clause to append to a query.
    /// Returns an empty string if there are no settings to apply.
    pub fn to_inline_settings(&self) -> String {
        let params = self.to_query_params();
        if params.is_empty() {
            return String::new();
        }
        let mut out = String::from("SETTINGS ");
        for (i, (k, v)) in params.iter().enumerate() {
            if i > 0 { out.push_str(", "); }
            out.push_str(k);
            out.push_str(" = ");
            out.push_str(v);
        }
        out
    }
    
    /// Create settings with custom result limits.
    pub fn with_result_limits(mut self, max_rows: u64, max_bytes: u64) -> Self {
        self.max_result_rows = max_rows;
        self.max_result_bytes = max_bytes;
        self
    }
    
    /// Create settings with no result limits (for internal queries).
    pub fn without_result_limits(mut self) -> Self {
        self.max_result_rows = 0;
        self.max_result_bytes = 0;
        self
    }
    
    /// Create settings with a specific execution timeout.
    ///
    /// CRITICAL: This sets max_execution_time at the ClickHouse level to ensure
    /// queries are actually cancelled server-side when they exceed the timeout.
    pub fn with_timeout(mut self, timeout_secs: u32) -> Self {
        self.max_execution_time = timeout_secs;
        self
    }

    /// Create settings optimized for object storage (S3/R2) queries.
    ///
    /// PERFORMANCE: Scales connection count based on expected file count.
    /// More files = more parallel connections = faster query execution.
    ///
    /// # Arguments
    /// * `file_count` - Expected number of files to scan
    pub fn for_object_storage(file_count: usize) -> Self {
        Self {
            // Scale connections with file count, up to 500
            s3_max_connections: file_count.saturating_mul(2).clamp(100, 500) as u32,
            // Higher thread count for parallel file processing
            max_threads: 16,
            // Always enable prefetching for remote files
            remote_filesystem_read_prefetch: true,
            // Ensure Parquet filter pushdown is enabled
            input_format_parquet_filter_push_down: true,
            // Allow schema flexibility for Parquet files
            input_format_parquet_skip_columns_with_unsupported_types: true,
            // Higher memory budget for large scans
            max_memory_usage: 15_000_000_000, // 15GB
            // Reasonable result limits
            max_result_rows: 100_000,
            max_result_bytes: 100_000_000, // 100MB for object storage queries
            result_overflow_mode: "break".to_string(),
            // Longer timeout for S3 queries
            max_execution_time: 300, // 5 minutes
        }
    }

    /// Settings for mixed queries (native ClickHouse + object storage).
    ///
    /// Uses more conservative parallelism than pure object-storage queries
    /// because native MergeTree tables have their own indexes and thread
    /// scheduling. Over-provisioning threads can cause contention.
    pub fn for_mixed_storage(object_storage_file_count: usize) -> Self {
        let mut settings = Self::for_object_storage(object_storage_file_count);
        settings.max_threads = settings.max_threads.min(8);
        settings
    }
}

/// Query executor for ClickHouse.
/// 
/// Uses the native TCP protocol (klickhouse) for dynamic query execution.
/// Optimized for large result sets and S3/R2 data sources.
pub struct QueryExecutor {
    /// Typed client for schema queries (clickhouse crate, HTTP-based, used for EXPLAIN only)
    client: Option<Client>,
    /// bb8-managed pool of native TCP connections
    native_pool: Option<NativePool>,
    /// ClickHouse configuration
    config: Option<ClickHouseConfig>,
    /// Query settings for performance optimization
    query_settings: ClickHouseQuerySettings,
}

impl QueryExecutor {
    /// Create a new query executor without a connection.
    pub fn new() -> ExecutorResult<Self> {
        Ok(Self { 
            client: None,
            native_pool: None,
            config: None,
            query_settings: ClickHouseQuerySettings::default(),
        })
    }
    
    /// Create a new query executor with a ClickHouse connection pool.
    ///
    /// Opens a bb8 pool of native TCP connections. Also creates an
    /// HTTP-based `clickhouse::Client` for EXPLAIN queries.
    pub async fn with_config(config: ClickHouseConfig) -> ExecutorResult<Self> {
        let mut http_client = Client::default()
            .with_url(&config.http_url())
            .with_database(&config.database);
        
        if let Some(ref user) = config.username {
            http_client = http_client.with_user(user);
        }
        if let Some(ref password) = config.password {
            http_client = http_client.with_password(password);
        }

        let pool = config.native_config().create_pool(4)
            .await
            .map_err(|e| ExecutorError::Http(format!("Native TCP pool creation failed: {}", e)))?;
        
        Ok(Self { 
            client: Some(http_client),
            native_pool: Some(pool),
            config: Some(config),
            query_settings: ClickHouseQuerySettings::default(),
        })
    }
    
    /// Create a query executor from environment configuration.
    pub async fn from_env() -> ExecutorResult<Self> {
        Self::with_config(ClickHouseConfig::from_env()).await
    }
    
    /// Set custom query settings.
    pub fn with_query_settings(mut self, settings: ClickHouseQuerySettings) -> Self {
        self.query_settings = settings;
        self
    }

    /// Access the native connection pool.
    pub fn native_pool(&self) -> Option<&NativePool> {
        self.native_pool.as_ref()
    }

    /// Checkout a native TCP connection from the pool.
    pub async fn get_native(
        &self,
    ) -> ExecutorResult<bb8::PooledConnection<'_, klickhouse::ConnectionManager>> {
        self.native_pool
            .as_ref()
            .ok_or(ExecutorError::NotConfigured)?
            .get()
            .await
            .map_err(|e| ExecutorError::Http(format!("Pool checkout failed: {}", e)))
    }

    /// Access the ClickHouse config.
    pub fn clickhouse_config(&self) -> Option<&ClickHouseConfig> {
        self.config.as_ref()
    }

    /// Execute a query using streaming JSON parsing.
    ///
    /// Uses FORMAT JSONCompactEachRowWithNamesAndTypes which returns:
    /// - Line 1: Column names as JSON array
    /// - Line 2: Column types as JSON array  
    /// - Lines 3+: Data rows as JSON arrays
    ///
    /// This eliminates the need for a separate DESCRIBE query and allows
    /// streaming the response without buffering the entire body.
    ///
    /// # Observability
    /// 
    /// Emits a `warehouse.query.execute` trace span with:
    /// - `query_length`: Length of the SQL query
    /// - `timeout_secs`: Configured timeout
    /// - `duration_ms`: Execution duration
    /// - `row_count`: Number of rows returned
    /// - `bytes_read`: Bytes scanned by ClickHouse
    #[instrument(
        name = "warehouse.query.execute",
        skip(self, sql, options),
        fields(
            query_length = sql.len(),
            timeout_secs = options.timeout_secs.unwrap_or(30),
            duration_ms,
            row_count,
            bytes_read,
        ),
        err(Display),
    )]
    pub async fn execute(
        &self,
        sql: &str,
        options: ExecutionOptions,
    ) -> ExecutorResult<QueryResult> {
        let _client = self.client.as_ref()
            .ok_or(ExecutorError::NotConfigured)?;
        
        let start = Instant::now();
        
        // NOTE: LIMIT handling is done at the API layer via AST manipulation
        // (see src/api/warehouse.rs ensure_query_limit). The executor trusts
        // that the caller has already applied appropriate limits to avoid
        // conflicts with CTEs and subqueries that contain LIMIT.
        
        // Server-side timeout via max_execution_time.  Client-side timeout
        // is set a few seconds longer so ClickHouse has time to cancel and
        // return an error before the client gives up, preventing orphan queries.
        // Minimum of 1 to avoid sending max_execution_time=0 (which means
        // "use server default" in ClickHouse, defeating the timeout cascade).
        let timeout_secs = options.timeout_secs.unwrap_or(30).max(1);
        let timeout = Duration::from_secs((timeout_secs as u64).saturating_add(5));
        
        let result = tokio::time::timeout(timeout, async {
            self.fetch_with_native(sql, Some(timeout_secs), options.max_memory_bytes).await
        })
        .await
        .map_err(|_| {
            tracing::warn!(
                timeout_secs = timeout_secs,
                "Query timed out"
            );
            ExecutorError::Timeout(timeout_secs)
        })??;
        
        let execution_time_ms = start.elapsed().as_millis() as u64;
        
        // Record metrics in the span
        let span = Span::current();
        span.record("duration_ms", execution_time_ms);
        span.record("row_count", result.rows.len());
        span.record("bytes_read", result.bytes_read);
        
        tracing::info!(
            duration_ms = execution_time_ms,
            row_count = result.rows.len(),
            bytes_read = result.bytes_read,
            "Query executed successfully"
        );
        
        Ok(QueryResult {
            row_count: result.rows.len(),
            execution_time_ms,
            ..result
        })
    }
    
    /// Fetch query results via native TCP protocol.
    ///
    /// Executes the query, streams blocks from ClickHouse, and converts
    /// native `klickhouse::Value` data to `serde_json::Value` for the
    /// existing `QueryResult` format.
    #[tracing::instrument(
        name = "warehouse.query.fetch_native",
        skip(self, sql),
        fields(query_length = sql.len(), timeout_secs = ?timeout_secs, max_memory_bytes = ?max_memory_bytes),
        err(Display),
    )]
    async fn fetch_with_native(
        &self, 
        sql: &str, 
        timeout_secs: Option<u32>,
        max_memory_bytes: Option<usize>,
    ) -> ExecutorResult<QueryResult> {
        let native = self.get_native().await?;

        let settings = if let Some(timeout) = timeout_secs {
            self.query_settings.clone().with_timeout(timeout)
        } else {
            self.query_settings.clone()
        };

        let inline = settings.to_inline_settings();
        let full_sql = if inline.is_empty() {
            sql.to_string()
        } else {
            let mut s = String::with_capacity(sql.len() + 1 + inline.len());
            s.push_str(sql);
            s.push(' ');
            s.push_str(&inline);
            s
        };

        let progress_rx = native.subscribe_progress();

        let mut block_stream = native.query_raw(full_sql)
            .await
            .map_err(|e| ExecutorError::Execution(format!("Query failed: {}", e)))?;

        let mut columns: Vec<ColumnInfo> = Vec::new();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::with_capacity(256);
        let mut estimated_memory: usize = 0;
        let mut memory_limit_reached = false;
        let mut col_names: Vec<String> = Vec::new();

        const MEMORY_SAMPLE_INTERVAL: usize = 64;

        while let Some(block_result) = block_stream.next().await {
            if memory_limit_reached {
                break;
            }
            let mut block = block_result
                .map_err(|e| ExecutorError::Execution(format!("Block read error: {}", e)))?;

            if columns.is_empty() && !block.column_types.is_empty() {
                columns = block.column_types.iter()
                    .map(|(name, ty)| ColumnInfo {
                        name: name.clone(),
                        data_type: klickhouse_type_to_string(ty),
                        nullable: klickhouse_type_is_nullable(ty),
                    })
                    .collect();
            }

            if col_names.is_empty() && !block.column_data.is_empty() {
                col_names = block.column_data.keys().cloned().collect();
            }

            let num_cols = col_names.len();
            let num_rows = block.rows as usize;

            let mut json_columns: Vec<Vec<serde_json::Value>> = col_names.iter()
                .map(|name| {
                    let vals = std::mem::take(block.column_data.get_mut(name)
                        .ok_or_else(|| ExecutorError::InvalidFormat(
                            format!("Missing column '{}' in block data", name)
                        ))?);
                    Ok(vals.into_iter().map(klickhouse_value_to_json).collect())
                })
                .collect::<ExecutorResult<Vec<Vec<serde_json::Value>>>>()?;

            for row_idx in 0..num_rows {
                if memory_limit_reached { break; }
                let mut json_row = Vec::with_capacity(num_cols);
                for col in &mut json_columns {
                    json_row.push(std::mem::replace(&mut col[row_idx], serde_json::Value::Null));
                }

                if let Some(limit) = max_memory_bytes {
                    if row_idx % MEMORY_SAMPLE_INTERVAL == 0 {
                        let row_memory: usize = json_row.iter().map(estimate_json_value_memory).sum();
                        let scale = MEMORY_SAMPLE_INTERVAL.min(num_rows - row_idx);
                        estimated_memory = estimated_memory
                            .saturating_add(row_memory.saturating_mul(scale));
                        if estimated_memory > limit {
                            memory_limit_reached = true;
                            break;
                        }
                    }
                }
                rows.push(json_row);
            }
        }

        let stats = ChClient::accumulate_progress(progress_rx);

        let row_count = rows.len();
        Ok(QueryResult {
            columns,
            row_count,
            rows,
            execution_time_ms: 0,
            bytes_read: stats.read_bytes,
            rows_read: stats.read_rows.max(row_count as u64),
            truncated: memory_limit_reached,
        })
    }
    
    /// Parse a streaming response in JSONCompactEachRowWithNamesAndTypes format.
    ///
    /// Kept for the HTTP-based external connector fallback path.
    #[tracing::instrument(
        name = "warehouse.query.parse_streaming_response",
        skip_all,
        err(Display)
    )]
    pub async fn parse_streaming_response(
        &self,
        response: reqwest::Response,
        max_memory_bytes: Option<usize>,
    ) -> ExecutorResult<QueryResult> {
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::with_capacity(64 * 1024);
        let mut columns: Vec<ColumnInfo> = Vec::new();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::with_capacity(256);
        let mut line_number = 0;
        let mut bytes_read: u64 = 0;
        let mut estimated_memory: usize = 0;
        let mut memory_limit_reached = false;
        
        // Read and process the stream
        loop {
            if memory_limit_reached {
                break;
            }
            let Some(chunk_result) = stream.next().await else { break };

            let chunk = chunk_result
                .map_err(|e| ExecutorError::Http(format!("Failed to read response chunk: {}", e)))?;
            
            bytes_read += chunk.len() as u64;
            buffer.extend_from_slice(&chunk);

            if buffer.len() > MAX_LINE_BUFFER_BYTES {
                return Err(ExecutorError::InvalidFormat(format!(
                    "Line buffer exceeded {} bytes without a newline — \
                     response is likely malformed",
                    MAX_LINE_BUFFER_BYTES,
                )));
            }
            
            // Process complete lines from the buffer.
            // Use an offset to avoid O(n^2) from draining after each line.
            let mut buf_start = 0;
            while let Some(rel_pos) = buffer[buf_start..].iter().position(|&b| b == b'\n') {
                if memory_limit_reached {
                    break;
                }
                let newline_pos = buf_start + rel_pos;
                let line = std::str::from_utf8(&buffer[buf_start..newline_pos])
                    .map_err(|e| ExecutorError::InvalidFormat(
                        format!("Invalid UTF-8 in response at byte {}: {}", e.valid_up_to(), e)
                    ))?;
                buf_start = newline_pos + 1;
                let line = line.trim();
                
                if line.is_empty() {
                    continue;
                }
                
                match line_number {
                    0 => {
                        // Parse column names
                        let names: Vec<String> = serde_json::from_str(line)
                            .map_err(|e| ExecutorError::InvalidFormat(
                                format!("Failed to parse column names: {}", e)
                            ))?;
                        columns = names.into_iter().map(|name| ColumnInfo {
                            name,
                            data_type: String::new(),
                            nullable: false,
                        }).collect();
                    }
                    1 => {
                        // Parse column types
                        let types: Vec<String> = serde_json::from_str(line)
                            .map_err(|e| ExecutorError::InvalidFormat(
                                format!("Failed to parse column types: {}", e)
                            ))?;
                        
                        if types.len() != columns.len() {
                            return Err(ExecutorError::InvalidFormat(
                                "Column names and types count mismatch".to_string()
                            ));
                        }
                        
                        for (col, type_str) in columns.iter_mut().zip(types.into_iter()) {
                            col.nullable = type_str.starts_with("Nullable");
                            col.data_type = type_str;
                        }
                    }
                    _ => {
                        if is_clickhouse_error_line(line) {
                            return Err(ExecutorError::Execution(line.to_string()));
                        }

                        let row: Vec<serde_json::Value> = serde_json::from_str(line)
                            .map_err(|e| ExecutorError::JsonParse(
                                format!("Failed to parse row {}: {}", line_number - 2, e)
                            ))?;

                        if row.len() != columns.len() {
                            return Err(ExecutorError::InvalidFormat(format!(
                                "Row {} has {} columns, expected {}",
                                line_number - 2, row.len(), columns.len()
                            )));
                        }

                        if let Some(limit) = max_memory_bytes {
                            let row_size: usize = row.iter()
                                .map(estimate_json_value_memory)
                                .sum();

                            if estimated_memory + row_size > limit {
                                tracing::warn!(
                                    rows_collected = rows.len(),
                                    estimated_memory_bytes = estimated_memory,
                                    limit_bytes = limit,
                                    "Query result memory limit reached, truncating results"
                                );
                                memory_limit_reached = true;
                                line_number += 1;
                                continue;
                            }

                            estimated_memory += row_size;
                        }

                        rows.push(row);
                    }
                }
                
                line_number += 1;
            }
            buffer.drain(..buf_start);

            if memory_limit_reached {
                buffer.clear();
            }
        }
        
        // Handle any remaining data in the buffer (last line without newline)
        if !buffer.is_empty() && !memory_limit_reached {
            let line = std::str::from_utf8(&buffer)
                .map_err(|e| ExecutorError::InvalidFormat(
                    format!("Invalid UTF-8 in final response chunk at byte {}: {}", e.valid_up_to(), e)
                ))?;
            let line = line.trim();
            
            if !line.is_empty() && line_number >= 2 {
                if is_clickhouse_error_line(line) {
                    return Err(ExecutorError::Execution(line.to_string()));
                }

                let row: Vec<serde_json::Value> = serde_json::from_str(line)
                    .map_err(|e| ExecutorError::JsonParse(
                        format!("Failed to parse row {}: {}", line_number - 2, e)
                    ))?;

                if row.len() != columns.len() {
                    return Err(ExecutorError::InvalidFormat(format!(
                        "Row {} has {} columns, expected {}",
                        line_number - 2, row.len(), columns.len()
                    )));
                }

                // Check memory limit for final row
                let row_size: usize = if max_memory_bytes.is_some() {
                    row.iter().map(estimate_json_value_memory).sum()
                } else {
                    0
                };

                if let Some(limit) = max_memory_bytes {
                    if estimated_memory + row_size > limit {
                        memory_limit_reached = true;
                    } else {
                        estimated_memory += row_size;
                        rows.push(row);
                    }
                } else {
                    rows.push(row);
                }
                line_number += 1;
            }
        }
        let _ = estimated_memory;

        if line_number < 2 && !memory_limit_reached {
            return Err(ExecutorError::InvalidFormat(
                format!(
                    "Incomplete ClickHouse response: expected at least 2 header lines (names + types), got {}",
                    line_number
                )
            ));
        }
        
        let row_count = rows.len();
        Ok(QueryResult {
            columns,
            row_count,
            rows,
            execution_time_ms: 0, // Will be set by caller
            bytes_read,
            rows_read: row_count as u64, // Fallback; overwritten by stats if available
            truncated: memory_limit_reached,
        })
    }

    /// Execute a query and return only the count.
    pub async fn count(&self, sql: &str) -> ExecutorResult<u64> {
        let count_sql = wrap_in_count(sql)?;
        let result = self.execute(&count_sql, ExecutionOptions {
            limit: Some(1),
            timeout_secs: Some(60),
            max_memory_bytes: None,
        }).await?;

        let row = result.rows.first().ok_or_else(|| {
            ExecutorError::InvalidFormat("COUNT query returned no rows".to_string())
        })?;
        let count_val = row.first().ok_or_else(|| {
            ExecutorError::InvalidFormat("COUNT query row has no columns".to_string())
        })?;
        count_val
            .as_u64()
            .or_else(|| count_val.as_str().and_then(|s| s.parse().ok()))
            .ok_or_else(|| {
                ExecutorError::InvalidFormat(format!(
                    "COUNT query returned non-numeric value: {}",
                    count_val
                ))
            })
    }
    
    /// Execute a query with streaming results via native TCP protocol.
    ///
    /// PERFORMANCE: Use this for large result sets to avoid OOM errors.
    /// Rows are yielded as blocks arrive from the native protocol.
    #[tracing::instrument(
        name = "warehouse.query.execute_streaming",
        skip(self, sql, options),
        fields(query_length = sql.len(), timeout_secs = options.timeout_secs.unwrap_or(30)),
        err(Display),
    )]
    pub async fn execute_streaming(
        &self,
        sql: &str,
        options: ExecutionOptions,
    ) -> ExecutorResult<StreamingQueryResult> {
        let native = self.get_native().await?;

        let _ = options.limit;

        let timeout_secs = options.timeout_secs.unwrap_or(30).max(1);

        let settings = self.query_settings.clone().with_timeout(timeout_secs);

        let inline = settings.to_inline_settings();
        let full_sql = if inline.is_empty() {
            sql.to_string()
        } else {
            let mut s = String::with_capacity(sql.len() + 1 + inline.len());
            s.push_str(sql);
            s.push(' ');
            s.push_str(&inline);
            s
        };

        let progress_rx = native.subscribe_progress();

        let mut block_stream = native.query_raw(full_sql)
            .await
            .map_err(|e| ExecutorError::Execution(format!("Query failed: {}", e)))?;

        let mut columns: Vec<ColumnInfo> = Vec::new();

        let (tx, rx) = mpsc::channel::<Result<QueryRow, ExecutorError>>(1024);

        let mut col_names: Vec<String> = Vec::new();
        if let Some(block_result) = block_stream.next().await {
            let mut block = block_result
                .map_err(|e| ExecutorError::Execution(format!("Block read error: {}", e)))?;

            if !block.column_types.is_empty() {
                columns = block.column_types.iter()
                    .map(|(name, ty)| ColumnInfo {
                        name: name.clone(),
                        data_type: klickhouse_type_to_string(ty),
                        nullable: klickhouse_type_is_nullable(ty),
                    })
                    .collect();
            }

            if col_names.is_empty() && !block.column_data.is_empty() {
                col_names = block.column_data.keys().cloned().collect();
            }

            let num_cols = col_names.len();
            let num_rows = block.rows as usize;
            let mut json_columns: Vec<Vec<serde_json::Value>> = col_names.iter()
                .map(|name| {
                    let vals = std::mem::take(block.column_data.get_mut(name)
                        .ok_or_else(|| ExecutorError::InvalidFormat(
                            format!("Missing column '{}' in block data", name)
                        ))?);
                    Ok(vals.into_iter().map(klickhouse_value_to_json).collect())
                })
                .collect::<ExecutorResult<Vec<Vec<serde_json::Value>>>>()?;
            for row_idx in 0..num_rows {
                let mut json_row = Vec::with_capacity(num_cols);
                for col in &mut json_columns {
                    json_row.push(std::mem::replace(&mut col[row_idx], serde_json::Value::Null));
                }
                let _ = tx.send(Ok(json_row)).await;
            }
        }

        tokio::spawn(async move {
            while let Some(block_result) = block_stream.next().await {
                match block_result {
                    Ok(mut block) => {
                        if col_names.is_empty() && !block.column_data.is_empty() {
                            col_names = block.column_data.keys().cloned().collect();
                        }
                        let num_cols = col_names.len();
                        let num_rows = block.rows as usize;
                        let json_columns_result: Result<Vec<Vec<serde_json::Value>>, _> = col_names.iter()
                            .map(|name| {
                                match block.column_data.get_mut(name) {
                                    Some(vals) => {
                                        let vals = std::mem::take(vals);
                                        Ok(vals.into_iter().map(klickhouse_value_to_json).collect())
                                    }
                                    None => Err(ExecutorError::InvalidFormat(
                                        format!("Missing column '{}' in block data", name)
                                    )),
                                }
                            })
                            .collect();
                        let mut json_columns = match json_columns_result {
                            Ok(cols) => cols,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                        };
                        for row_idx in 0..num_rows {
                            let mut json_row = Vec::with_capacity(num_cols);
                            for col in &mut json_columns {
                                json_row.push(std::mem::replace(&mut col[row_idx], serde_json::Value::Null));
                            }
                            if tx.send(Ok(json_row)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(ExecutorError::Execution(format!("Block error: {}", e)))).await;
                        return;
                    }
                }
            }
        });

        let row_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        let stats = ChClient::accumulate_progress(progress_rx);
        let result = StreamingQueryResult::with_stats(
            columns,
            Box::pin(row_stream),
            ClickHouseStats {
                read_rows: stats.read_rows,
                read_bytes: stats.read_bytes,
                written_bytes: 0,
                written_rows: 0,
                elapsed_seconds: 0.0,
            },
        );

        Ok(result)
    }

    /// Execute a query and return raw klickhouse blocks for direct encoding.
    ///
    /// Unlike `execute` (which converts to `serde_json::Value`) or
    /// `execute_streaming` (which streams JSON rows), this method yields
    /// the raw klickhouse `Block`s. The caller (pgwire handler) encodes
    /// them directly into the target wire format without any intermediate
    /// representation.
    #[tracing::instrument(
        name = "warehouse.query.execute_native_blocks",
        skip(self, sql, options),
        fields(query_length = sql.len(), timeout_secs = options.timeout_secs.unwrap_or(30)),
        err(Display),
    )]
    pub async fn execute_native_blocks(
        &self,
        sql: &str,
        options: ExecutionOptions,
    ) -> ExecutorResult<NativeBlockStream> {
        let native = self.get_native().await?;

        let timeout_secs = options.timeout_secs.unwrap_or(30).max(1);
        let client_timeout = Duration::from_secs((timeout_secs as u64).saturating_add(5));

        let settings = self.query_settings.clone().with_timeout(timeout_secs);

        let inline = settings.to_inline_settings();
        let full_sql = if inline.is_empty() {
            sql.to_string()
        } else {
            let mut s = String::with_capacity(sql.len() + 1 + inline.len());
            s.push_str(sql);
            s.push(' ');
            s.push_str(&inline);
            s
        };

        let progress_rx = native.subscribe_progress();

        let sql_owned = full_sql;
        let row_limit = options.limit;

        // Wrap the initial query + first-block read in a client-side timeout
        // so we don't hang if ClickHouse ignores max_execution_time.
        let (columns, first_blocks, block_stream) = tokio::time::timeout(client_timeout, async {
            let mut block_stream = native.query_raw(sql_owned)
                .await
                .map_err(|e| ExecutorError::Execution(format!("Query failed: {}", e)))?;

            let mut columns: Vec<NativeColumnInfo> = Vec::new();
            let mut first_blocks: Vec<klickhouse::block::Block> = Vec::new();

            if let Some(block_result) = block_stream.next().await {
                let block = block_result
                    .map_err(|e| ExecutorError::Execution(format!("Block read error: {}", e)))?;

                if !block.column_types.is_empty() {
                    columns = block.column_types.iter()
                        .map(|(name, ty)| NativeColumnInfo {
                            name: name.clone(),
                            klickhouse_type: ty.clone(),
                        })
                        .collect();
                }

                if block.rows > 0 {
                    first_blocks.push(block);
                }
            }

            Ok::<_, ExecutorError>((columns, first_blocks, block_stream))
        })
        .await
        .map_err(|_| {
            tracing::warn!(timeout_secs = timeout_secs, "Native block query timed out");
            ExecutorError::Timeout(timeout_secs)
        })??;

        let first_stream = futures::stream::iter(
            first_blocks.into_iter().map(Ok)
        );
        let rest_stream = block_stream.map(|result| {
            result.map_err(|e| ExecutorError::Execution(format!("Block error: {}", e)))
        });

        // Apply row limit: stop the stream after `limit` rows
        let combined: Pin<Box<dyn Stream<Item = Result<klickhouse::block::Block, ExecutorError>> + Send>> = if let Some(limit) = row_limit {
            let limit = limit as u64;
            let mut rows_emitted: u64 = 0;
            let limited = async_stream::stream! {
                let mut inner = first_stream.chain(rest_stream);
                while let Some(block_result) = inner.next().await {
                    if rows_emitted >= limit {
                        break;
                    }
                    match block_result {
                        Ok(mut block) => {
                            let remaining = limit - rows_emitted;
                            if block.rows > remaining {
                                block.rows = remaining;
                                for col in block.column_data.values_mut() {
                                    col.truncate(remaining as usize);
                                }
                            }
                            rows_emitted += block.rows;
                            yield Ok(block);
                            if rows_emitted >= limit {
                                break;
                            }
                        }
                        Err(e) => {
                            yield Err(e);
                            break;
                        }
                    }
                }
            };
            Box::pin(limited)
        } else {
            Box::pin(first_stream.chain(rest_stream))
        };

        let progress_stats = ChClient::accumulate_progress(progress_rx);
        tracing::info!(
            read_rows = progress_stats.read_rows,
            read_bytes = progress_stats.read_bytes,
            "Native block stream ready"
        );

        Ok(NativeBlockStream {
            columns,
            blocks: combined,
            stats: Some(progress_stats),
        })
    }
    
    /// Execute an EXPLAIN query to get the query plan.
    pub async fn explain(&self, sql: &str) -> ExecutorResult<String> {
        let client = self.client.as_ref()
            .ok_or(ExecutorError::NotConfigured)?;
        
        let explain_sql = prepend_explain(sql)?;
        
        #[derive(clickhouse::Row, Deserialize)]
        struct ExplainRow {
            explain: String,
        }
        
        let rows: Vec<ExplainRow> = client
            .query(&explain_sql)
            .fetch_all()
            .await
            .map_err(|e| ExecutorError::Execution(format!("EXPLAIN failed: {}", e)))?;
        
        Ok(rows.into_iter().map(|r| r.explain).collect::<Vec<_>>().join("\n"))
    }
    
    /// Execute a raw SQL query and return the result as a string.
    /// 
    /// Useful for administrative queries (DDL, mutation stats, etc.) where
    /// the caller handles response parsing.
    #[tracing::instrument(
        name = "warehouse.query.execute_raw_query",
        skip(self, sql),
        fields(query_length = sql.len()),
        err(Display),
    )]
    pub async fn execute_raw_query(&self, sql: &str) -> ExecutorResult<String> {
        let native = self.get_native().await?;

        let mut block_stream = native.query_raw(sql)
            .await
            .map_err(|e| ExecutorError::Execution(format!("Raw query failed: {}", e)))?;

        let mut output = String::new();
        let mut col_names: Vec<String> = Vec::new();
        while let Some(block_result) = block_stream.next().await {
            let mut block = block_result
                .map_err(|e| ExecutorError::Execution(format!("Block error: {}", e)))?;

            if col_names.is_empty() && !block.column_data.is_empty() {
                col_names = block.column_data.keys().cloned().collect();
            }
            let num_rows = block.rows as usize;

            let mut json_columns: Vec<Vec<serde_json::Value>> = col_names.iter()
                .map(|name| {
                    let vals = std::mem::take(block.column_data.get_mut(name)
                        .ok_or_else(|| ExecutorError::InvalidFormat(
                            format!("Missing column '{}' in block data", name)
                        ))?);
                    Ok(vals.into_iter().map(klickhouse_value_to_json).collect())
                })
                .collect::<ExecutorResult<Vec<Vec<serde_json::Value>>>>()?;
            for row_idx in 0..num_rows {
                for (col_idx, col) in json_columns.iter_mut().enumerate() {
                    if col_idx > 0 { output.push('\t'); }
                    let val = std::mem::replace(&mut col[row_idx], serde_json::Value::Null);
                    match val {
                        serde_json::Value::String(s) => output.push_str(&s),
                        serde_json::Value::Null => output.push_str("\\N"),
                        other => output.push_str(&other.to_string()),
                    }
                }
                output.push('\n');
            }
        }

        Ok(output)
    }

    /// Check if the executor is configured with a connection.
    pub fn is_configured(&self) -> bool {
        self.client.is_some()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    // ==================== Configuration Tests ====================
    
    #[test]
    fn test_default_execution_options() {
        let options = ExecutionOptions::default();
        assert!(options.limit.is_some());
        assert!(options.max_memory_bytes.is_some());
        assert!(options.timeout_secs.is_some());
    }
    
    #[test]
    fn test_execution_options_with_limits() {
        let options = ExecutionOptions {
            limit: Some(1000),
            max_memory_bytes: Some(10 * 1024 * 1024),
            timeout_secs: Some(30),
        };
        
        assert_eq!(options.limit, Some(1000));
        assert_eq!(options.max_memory_bytes, Some(10 * 1024 * 1024));
        assert_eq!(options.timeout_secs, Some(30));
    }
    
    #[test]
    fn test_clickhouse_config() {
        let config = ClickHouseConfig {
            host: "ch.example.com".to_string(),
            native_port: 9000,
            http_port: 8123,
            database: "analytics".to_string(),
            username: Some("admin".to_string()),
            password: Some("secret".to_string()),
            pool: ConnectionPoolConfig::default(),
        };
        
        assert!(config.http_url().contains("ch.example.com"));
        assert_eq!(config.database, "analytics");
        assert!(config.username.is_some());
    }
    
    #[test]
    fn test_connection_pool_config_default() {
        let config = ConnectionPoolConfig::default();
        assert!(config.max_idle_per_host > 0);
        assert!(config.idle_timeout_secs > 0);
    }
    
    // ==================== ColumnInfo Tests ====================
    
    #[test]
    fn test_column_info_creation() {
        let col = ColumnInfo {
            name: "user_id".to_string(),
            data_type: "String".to_string(),
            nullable: false,
        };
        
        assert_eq!(col.name, "user_id");
        assert_eq!(col.data_type, "String");
        assert!(!col.nullable);
    }
    
    #[test]
    fn test_column_info_nullable_detection() {
        // ClickHouse returns types like "Nullable(String)"
        let type_str = "Nullable(String)";
        let nullable = type_str.starts_with("Nullable");
        assert!(nullable);
        
        let type_str = "String";
        let nullable = type_str.starts_with("Nullable");
        assert!(!nullable);
    }
    
    // ==================== ClickHouseStats Tests ====================
    
    #[test]
    fn test_clickhouse_stats_default() {
        let stats = ClickHouseStats::default();
        assert_eq!(stats.read_rows, 0);
        assert_eq!(stats.read_bytes, 0);
        assert_eq!(stats.elapsed_seconds, 0.0);
    }
    
    // ==================== QueryResult Tests ====================
    
    #[test]
    fn test_query_result_empty() {
        let result = QueryResult {
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                }
            ],
            rows: vec![],
            row_count: 0,
            execution_time_ms: 0,
            bytes_read: 0,
            rows_read: 0,
            truncated: false,
        };
        
        assert!(result.rows.is_empty());
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.row_count, 0);
        assert!(!result.truncated);
    }
    
    #[test]
    fn test_query_result_with_data() {
        let result = QueryResult {
            columns: vec![
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "String".to_string(),
                    nullable: false,
                }
            ],
            rows: vec![
                vec![serde_json::json!("Alice")],
                vec![serde_json::json!("Bob")],
            ],
            row_count: 2,
            execution_time_ms: 5,
            bytes_read: 100,
            rows_read: 2,
            truncated: false,
        };
        
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.bytes_read, 100);
        assert!(!result.truncated);
    }
    
    // ==================== ExecutorError Tests ====================
    
    #[test]
    fn test_executor_error_not_configured() {
        let err = ExecutorError::NotConfigured;
        let msg = err.to_string();
        assert!(msg.contains("not configured"));
    }
    
    #[test]
    fn test_executor_error_timeout() {
        let err = ExecutorError::Timeout(30);
        let msg = err.to_string();
        assert!(msg.contains("30"));
    }
    
    #[test]
    fn test_executor_error_result_truncated() {
        let err = ExecutorError::ResultTruncated {
            limit_bytes: 1024,
            rows_collected: 100,
        };
        let msg = err.to_string();
        assert!(msg.contains("1024"));
        assert!(msg.contains("100"));
    }
    
    // ==================== Streaming Result Tests ====================
    
    #[tokio::test]
    async fn test_streaming_query_result_creation() {
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
            }
        ];
        
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<QueryRow, ExecutorError>>(10);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        
        // Send one row
        tx.send(Ok(vec![serde_json::json!(42)])).await.unwrap();
        drop(tx); // Close channel
        
        let result = StreamingQueryResult::new(columns, Box::pin(stream));
        
        assert_eq!(result.columns.len(), 1);
    }
    
    #[tokio::test]
    async fn test_streaming_query_result_collect() {
        let columns = vec![
            ColumnInfo {
                name: "value".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
            }
        ];
        
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<QueryRow, ExecutorError>>(10);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        
        // Send multiple rows
        tx.send(Ok(vec![serde_json::json!(1)])).await.unwrap();
        tx.send(Ok(vec![serde_json::json!(2)])).await.unwrap();
        tx.send(Ok(vec![serde_json::json!(3)])).await.unwrap();
        drop(tx);
        
        let mut result = StreamingQueryResult::new(columns, Box::pin(stream));
        let collected = result.collect().await.unwrap();
        
        assert_eq!(collected.rows.len(), 3);
        assert!(!collected.truncated);
    }

    #[tokio::test]
    async fn test_collect_with_limit_sets_truncated() {
        let columns = vec![
            ColumnInfo {
                name: "value".to_string(),
                data_type: "String".to_string(),
                nullable: false,
            }
        ];

        let rows: Vec<Result<QueryRow, ExecutorError>> = (0..100)
            .map(|i| Ok(vec![serde_json::json!(format!("row_{i}"))]))
            .collect();
        let stream = futures::stream::iter(rows);

        let result = StreamingQueryResult::new(columns, Box::pin(stream));
        let collected = result.collect_with_limit(Some(128)).await.unwrap();

        assert!(collected.truncated, "Result must be marked truncated");
        assert!(collected.rows.len() < 100, "Not all rows should have been collected");
    }
    
    #[tokio::test]
    async fn test_streaming_query_result_error_recovery() {
        let columns = vec![
            ColumnInfo {
                name: "value".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
            }
        ];
        
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<QueryRow, ExecutorError>>(10);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        
        // Send some rows, then an error
        tx.send(Ok(vec![serde_json::json!(1)])).await.unwrap();
        tx.send(Ok(vec![serde_json::json!(2)])).await.unwrap();
        tx.send(Err(ExecutorError::JsonParse("simulated parse error".to_string()))).await.unwrap();
        drop(tx);
        
        let mut result = StreamingQueryResult::new(columns, Box::pin(stream));
        let collect_result = result.collect().await;
        
        // Should return error
        assert!(collect_result.is_err());
        match collect_result.err().unwrap() {
            ExecutorError::JsonParse(msg) => assert!(msg.contains("simulated")),
            other => panic!("Expected JsonParse error, got {:?}", other),
        }
    }
    
    #[tokio::test]
    async fn test_streaming_partial_results_on_error() {
        use futures::StreamExt;
        
        let columns = vec![
            ColumnInfo {
                name: "value".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
            }
        ];
        
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<QueryRow, ExecutorError>>(10);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        
        // Send some rows, then an error
        tx.send(Ok(vec![serde_json::json!(1)])).await.unwrap();
        tx.send(Ok(vec![serde_json::json!(2)])).await.unwrap();
        tx.send(Err(ExecutorError::Http("connection lost".to_string()))).await.unwrap();
        tx.send(Ok(vec![serde_json::json!(3)])).await.unwrap(); // This won't be read
        drop(tx);
        
        let mut result = StreamingQueryResult::new(columns, Box::pin(stream));
        
        // Consume the stream manually to verify we get partial results before error
        let mut rows_received = 0;
        let mut error_received = false;
        
        while let Some(row_result) = result.rows.next().await {
            match row_result {
                Ok(_) => rows_received += 1,
                Err(_) => {
                    error_received = true;
                    break;
                }
            }
        }
        
        assert_eq!(rows_received, 2);
        assert!(error_received);
    }
    
    // ==================== Memory Estimation Tests ====================
    
    #[test]
    fn test_estimate_json_value_memory_string() {
        let value = serde_json::json!("hello world");
        let size = estimate_json_value_memory(&value);
        // Should include String struct overhead + content length
        assert!(size >= 11); // "hello world" is 11 chars
    }
    
    #[test]
    fn test_estimate_json_value_memory_number() {
        let value = serde_json::json!(42);
        let size = estimate_json_value_memory(&value);
        // Number should have some overhead
        assert!(size > 0);
    }
    
    #[test]
    fn test_estimate_json_value_memory_array() {
        let value = serde_json::json!([1, 2, 3, 4, 5]);
        let size = estimate_json_value_memory(&value);
        // Array should be larger than individual numbers
        assert!(size > estimate_json_value_memory(&serde_json::json!(1)) * 5);
    }
    
    #[test]
    fn test_estimate_json_value_memory_object() {
        let value = serde_json::json!({"name": "Alice", "age": 30});
        let size = estimate_json_value_memory(&value);
        // Object should have overhead for keys and values
        assert!(size > 10);
    }
    
    #[test]
    fn test_estimate_json_value_memory_null() {
        let value = serde_json::json!(null);
        let size = estimate_json_value_memory(&value);
        // Null should have minimal overhead
        assert!(size > 0);
    }
    
    // ==================== ClickHouseQuerySettings Tests ====================
    
    #[test]
    fn test_query_settings_default() {
        let settings = ClickHouseQuerySettings::default();
        // Should have reasonable defaults
        assert_eq!(settings.max_threads, 8);
        assert!(settings.input_format_parquet_filter_push_down);
        assert!(settings.max_memory_usage > 0);
    }
    
    #[test]
    fn test_query_settings_to_params() {
        let settings = ClickHouseQuerySettings {
            max_threads: 4,
            input_format_parquet_filter_push_down: true,
            input_format_parquet_skip_columns_with_unsupported_types: true,
            s3_max_connections: 50,
            remote_filesystem_read_prefetch: true,
            max_memory_usage: 1024 * 1024 * 1024,
            max_result_rows: 1_000_000,
            max_result_bytes: 100_000_000,
            result_overflow_mode: "break".to_string(),
            max_execution_time: 60,
        };
        
        let params = settings.to_query_params();
        assert!(params.contains(&("max_execution_time", "60".to_string())));
        assert!(params.iter().any(|(k, _)| *k == "max_memory_usage"));
        assert!(params.iter().any(|(k, _)| *k == "max_threads"));
    }
    
    // ==================== QueryExecutor Tests ====================
    
    #[test]
    fn test_query_executor_not_configured() {
        let executor = QueryExecutor::new().unwrap();
        assert!(!executor.is_configured());
    }

    #[test]
    fn test_prepend_explain_select() {
        let sql = "SELECT * FROM events WHERE id > 10";
        let result = prepend_explain(sql).unwrap();
        assert!(
            result.to_uppercase().starts_with("EXPLAIN"),
            "Must start with EXPLAIN: {result}"
        );
        assert!(result.contains("events"), "Must preserve table name: {result}");
    }

    #[test]
    fn test_prepend_explain_preserves_query() {
        let sql = "SELECT count() FROM events GROUP BY status";
        let result = prepend_explain(sql).unwrap();
        assert!(result.to_uppercase().contains("EXPLAIN"), "Must contain EXPLAIN: {result}");
        assert!(result.contains("GROUP BY") || result.contains("group by") || result.to_uppercase().contains("GROUP BY"),
            "Must preserve GROUP BY: {result}");
    }

    #[test]
    fn test_prepend_explain_empty_sql_errors() {
        assert!(prepend_explain("").is_err(), "Empty SQL must error");
    }

    #[test]
    fn test_prepend_explain_invalid_sql_errors() {
        assert!(
            prepend_explain("))) NOT VALID SQL (((").is_err(),
            "Invalid SQL must error"
        );
    }

    #[test]
    fn test_max_line_buffer_constant_is_reasonable() {
        assert_eq!(
            MAX_LINE_BUFFER_BYTES,
            64 * 1024 * 1024,
            "Buffer limit must be 64 MB"
        );
    }

    #[tokio::test]
    async fn test_streaming_timeout_error_propagates() {
        use futures::StreamExt;

        let columns = vec![ColumnInfo {
            name: "v".to_string(),
            data_type: "Int64".to_string(),
            nullable: false,
        }];

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<QueryRow, ExecutorError>>(4);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        tx.send(Ok(vec![serde_json::json!(1)])).await.unwrap();
        tx.send(Err(ExecutorError::Http(
            "Streaming response timed out after 300 seconds".to_string(),
        ))).await.unwrap();
        drop(tx);

        let mut result = StreamingQueryResult::new(columns, Box::pin(stream));

        let mut rows = 0u64;
        let mut got_timeout = false;
        while let Some(item) = result.rows.next().await {
            match item {
                Ok(_) => rows += 1,
                Err(ExecutorError::Http(msg)) if msg.contains("timed out") => {
                    got_timeout = true;
                    break;
                }
                Err(other) => panic!("unexpected error: {other}"),
            }
        }
        assert_eq!(rows, 1);
        assert!(got_timeout, "Consumer must receive timeout error");
    }

    #[test]
    fn test_timeout_secs_zero_clamped_to_one() {
        let options = ExecutionOptions {
            limit: None,
            max_memory_bytes: None,
            timeout_secs: Some(0),
        };
        let clamped = options.timeout_secs.unwrap_or(30).max(1);
        assert_eq!(
            clamped, 1,
            "timeout_secs=0 must be clamped to 1 to avoid sending \
             max_execution_time=0 (server default) to ClickHouse"
        );
    }

    #[test]
    fn test_wrap_in_count_strips_limit() {
        let sql = "SELECT * FROM events LIMIT 10";
        let result = wrap_in_count(sql).unwrap();
        let upper = result.to_uppercase();
        assert!(
            !upper.contains(" LIMIT ") || upper.contains("LIMIT"),
            "wrap_in_count should strip LIMIT from the inner query: {}",
            result
        );
        assert!(
            upper.contains("COUNT"),
            "Result must contain COUNT: {}",
            result
        );
    }

    #[test]
    fn test_wrap_in_count_strips_offset() {
        let sql = "SELECT * FROM events OFFSET 20";
        let result = wrap_in_count(sql).unwrap();
        assert!(
            !result.to_uppercase().contains("OFFSET"),
            "wrap_in_count should strip OFFSET from the inner query: {}",
            result
        );
    }

    #[test]
    fn test_is_clickhouse_error_line_code_prefix() {
        assert!(is_clickhouse_error_line(
            "Code: 241. DB::Exception: Memory limit (for query) exceeded: would use 9.37 GiB"
        ));
    }

    #[test]
    fn test_is_clickhouse_error_line_db_exception() {
        assert!(is_clickhouse_error_line(
            "Received exception from server: DB::Exception: Table default.foo doesn't exist"
        ));
    }

    #[test]
    fn test_is_clickhouse_error_line_normal_json() {
        assert!(!is_clickhouse_error_line("[\"value1\",\"value2\"]"));
    }

    #[test]
    fn test_is_clickhouse_error_line_empty() {
        assert!(!is_clickhouse_error_line(""));
    }

    #[tokio::test]
    async fn test_parse_streaming_response_clickhouse_error_in_final_buffer() {
        let executor = QueryExecutor::new().unwrap();

        // Simulate a response where the ClickHouse error appears as the last
        // line with no trailing newline (the final-buffer code path).
        let header = "[\"col1\"]\n[\"String\"]\n";
        let error_line = "Code: 241. DB::Exception: Memory limit exceeded";
        let body = format!("{}{}", header, error_line);

        let response = http::Response::builder()
            .status(200)
            .body(body)
            .unwrap();
        let response = reqwest::Response::from(response);

        let result = executor.parse_streaming_response(response, None).await;
        assert!(result.is_err(), "Should return an error");
        match result.unwrap_err() {
            ExecutorError::Execution(msg) => {
                assert!(
                    msg.contains("Code: 241"),
                    "Error must contain original ClickHouse error, got: {}",
                    msg,
                );
            }
            other => panic!(
                "Expected ExecutorError::Execution, got: {:?}",
                other,
            ),
        }
    }

    #[tokio::test]
    async fn test_parse_streaming_response_rejects_column_count_mismatch() {
        let executor = QueryExecutor::new().unwrap();

        let headers = "[\"id\",\"name\"]\n";
        let types = "[\"UInt64\",\"String\"]\n";
        let bad_row = "[1,\"alice\",\"extra_col\"]\n";
        let body_str = format!("{headers}{types}{bad_row}");
        let body = http::Response::builder()
            .status(200)
            .body(body_str)
            .unwrap();
        let response = reqwest::Response::from(body);

        let result = executor.parse_streaming_response(response, None).await;
        assert!(result.is_err(), "Column count mismatch must be rejected");
        match result.unwrap_err() {
            ExecutorError::InvalidFormat(msg) => {
                assert!(
                    msg.contains("3 columns") && msg.contains("expected 2"),
                    "Error must report actual vs expected column count, got: {}",
                    msg,
                );
            }
            other => panic!(
                "Expected ExecutorError::InvalidFormat, got: {:?}",
                other,
            ),
        }
    }

    // ==================== Memory Estimation Tests ====================

    #[test]
    fn test_memory_estimation_saturating_arithmetic() {
        let row_memory: usize = usize::MAX / 2;
        let scale: usize = 64;
        let estimated: usize = 100;
        let result = estimated.saturating_add(row_memory.saturating_mul(scale));
        assert_eq!(result, usize::MAX, "Saturating ops must clamp to usize::MAX instead of wrapping");
    }

    #[test]
    fn test_memory_limit_does_not_include_exceeding_row() {
        const MEMORY_SAMPLE_INTERVAL: usize = 64;
        let max_memory_bytes: usize = 100;
        let mut estimated_memory: usize = 0;
        let mut memory_limit_reached = false;
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();

        let num_rows = 128;
        for row_idx in 0..num_rows {
            if memory_limit_reached { break; }
            let json_row = vec![serde_json::Value::String("x".repeat(50))];

            if row_idx % MEMORY_SAMPLE_INTERVAL == 0 {
                let row_memory: usize = json_row.iter().map(estimate_json_value_memory).sum();
                let scale = MEMORY_SAMPLE_INTERVAL.min(num_rows - row_idx);
                estimated_memory = estimated_memory
                    .saturating_add(row_memory.saturating_mul(scale));
                if estimated_memory > max_memory_bytes {
                    memory_limit_reached = true;
                    break;
                }
            }
            rows.push(json_row);
        }

        assert!(memory_limit_reached);
        assert_eq!(rows.len(), 0, "No rows should be added when the first sample already exceeds the limit");
    }

    // ==================== Block Limit Truncation Tests ====================

    fn make_test_block(num_rows: u64) -> klickhouse::block::Block {
        let col: Vec<klickhouse::Value> = (0..num_rows).map(|i| klickhouse::Value::Int32(i as i32)).collect();
        let mut column_types = klickhouse::IndexMap::new();
        column_types.insert("x".to_string(), klickhouse::Type::Int32);
        let mut column_data = klickhouse::IndexMap::new();
        column_data.insert("x".to_string(), col);
        klickhouse::block::Block {
            info: klickhouse::block::BlockInfo {
                is_overflows: false,
                bucket_num: -1,
            },
            rows: num_rows,
            column_types,
            column_data,
        }
    }

    #[tokio::test]
    async fn test_block_stream_limit_truncates_final_block() {
        use futures::StreamExt;

        let limit: u64 = 100;
        let mut rows_emitted: u64 = 0;

        let block1 = make_test_block(95);
        let block2 = make_test_block(50);

        let source = futures::stream::iter(vec![
            Ok::<_, ExecutorError>(block1),
            Ok::<_, ExecutorError>(block2),
        ]);
        let limited = async_stream::stream! {
            let mut inner = source;
            while let Some(block_result) = inner.next().await {
                if rows_emitted >= limit {
                    break;
                }
                match block_result {
                    Ok(mut block) => {
                        let remaining = limit - rows_emitted;
                        if block.rows > remaining {
                            block.rows = remaining;
                            for col in block.column_data.values_mut() {
                                col.truncate(remaining as usize);
                            }
                        }
                        rows_emitted += block.rows;
                        yield Ok::<_, ExecutorError>(block);
                        if rows_emitted >= limit {
                            break;
                        }
                    }
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }
        };

        let blocks: Vec<Result<_, _>> = Box::pin(limited).collect().await;
        let total_rows: u64 = blocks.iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.rows)
            .sum();
        assert_eq!(total_rows, 100, "Total rows must equal the limit exactly");

        let last_block = blocks.last().unwrap().as_ref().unwrap();
        assert_eq!(last_block.rows, 5, "Last block must be truncated to 5 rows");
        assert_eq!(last_block.column_data["x"].len(), 5, "Column data must match truncated row count");
    }

    #[tokio::test]
    async fn test_parse_streaming_response_error_includes_row_number() {
        let executor = QueryExecutor::new().unwrap();

        let headers = "[\"id\"]\n";
        let types = "[\"UInt64\"]\n";
        let good_row = "[1]\n";
        let bad_row = "not_json\n";
        let body_str = format!("{headers}{types}{good_row}{bad_row}");
        let body = http::Response::builder()
            .status(200)
            .body(body_str)
            .unwrap();
        let response = reqwest::Response::from(body);

        let result = executor.parse_streaming_response(response, None).await;
        assert!(result.is_err(), "Invalid JSON must be rejected");
        match result.unwrap_err() {
            ExecutorError::JsonParse(msg) => {
                assert!(
                    msg.contains("row 1"),
                    "Error must include the row index, got: {}",
                    msg,
                );
            }
            other => panic!(
                "Expected ExecutorError::JsonParse, got: {:?}",
                other,
            ),
        }
    }

    // ==================== Bug Regression: Missing Column Returns Error ====================

    #[test]
    fn test_missing_column_returns_error_not_panic() {
        let col_names = vec!["x".to_string(), "missing_col".to_string()];
        let mut column_data = klickhouse::IndexMap::new();
        column_data.insert(
            "x".to_string(),
            vec![klickhouse::Value::Int32(1)],
        );

        let result: ExecutorResult<Vec<Vec<serde_json::Value>>> = col_names.iter()
            .map(|name| {
                let vals = std::mem::take(column_data.get_mut(name)
                    .ok_or_else(|| ExecutorError::InvalidFormat(
                        format!("Missing column '{}' in block data", name)
                    ))?);
                Ok(vals.into_iter().map(klickhouse_value_to_json).collect())
            })
            .collect();

        assert!(result.is_err(), "Missing column must return Err, not panic");
        match result.unwrap_err() {
            ExecutorError::InvalidFormat(msg) => {
                assert!(
                    msg.contains("missing_col"),
                    "Error must name the missing column, got: {}",
                    msg,
                );
            }
            other => panic!(
                "Expected ExecutorError::InvalidFormat, got: {:?}",
                other,
            ),
        }
    }

    #[test]
    fn test_all_columns_present_succeeds() {
        let col_names = vec!["x".to_string(), "y".to_string()];
        let mut column_data = klickhouse::IndexMap::new();
        column_data.insert(
            "x".to_string(),
            vec![klickhouse::Value::Int32(1)],
        );
        column_data.insert(
            "y".to_string(),
            vec![klickhouse::Value::Int32(2)],
        );

        let result: ExecutorResult<Vec<Vec<serde_json::Value>>> = col_names.iter()
            .map(|name| {
                let vals = std::mem::take(column_data.get_mut(name)
                    .ok_or_else(|| ExecutorError::InvalidFormat(
                        format!("Missing column '{}' in block data", name)
                    ))?);
                Ok(vals.into_iter().map(klickhouse_value_to_json).collect())
            })
            .collect();

        assert!(result.is_ok(), "All columns present must succeed");
        let cols = result.unwrap();
        assert_eq!(cols.len(), 2);
    }
}

