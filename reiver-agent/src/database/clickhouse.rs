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

pub struct ClickHouseCollector {
    config: Arc<DatabaseConfig>,
    client: Option<clickhouse::Client>,
}

impl ClickHouseCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection string
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
        
        // ClickHouse connection string format: http://[username:password@]host:port/database
        let conn_str = if password.is_empty() && config.username.is_empty() {
            format!("http://{}:{}/{}", config.host, config.port, config.database)
        } else {
            format!("http://{}:{}@{}:{}/{}", config.username, password, config.host, config.port, config.database)
        };

        info!("Connecting to ClickHouse...");
        let start = std::time::Instant::now();
        
        let client = clickhouse::Client::default()
            .with_url(&conn_str);
        
        // Test connection with a simple query
        let _: Vec<u8> = client
            .query("SELECT 1")
            .fetch_all()
            .await
            .context("Failed to ping ClickHouse")?;

        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to ClickHouse");

        Ok(Self {
            config,
            client: Some(client),
        })
    }

    /// Query ClickHouse system.query_log and return structured data
    fn query_query_log(client: &clickhouse::Client, config: &DatabaseConfig) -> Result<Vec<QueryMetricRow>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to create tokio runtime for ClickHouse query")?;
        
        rt.block_on(async {
            // Query system.query_log for recent queries
            // Note: query_log must be enabled in ClickHouse config (query_log table)
            let query = format!(
                "SELECT 
                    query,
                    count() as calls,
                    sum(query_duration_ms) as total_time_ms,
                    avg(query_duration_ms) as mean_time_ms,
                    min(query_duration_ms) as min_time_ms,
                    max(query_duration_ms) as max_time_ms
                FROM system.query_log
                WHERE type = 2 -- Finished queries only
                  AND query_start_time > now() - INTERVAL 1 HOUR
                  AND query NOT LIKE '%system.query_log%'
                  AND query NOT LIKE '%system.metrics%'
                GROUP BY query
                ORDER BY mean_time_ms DESC
                LIMIT ?"
            );
            
            #[derive(clickhouse::Row, serde::Deserialize)]
            struct QueryLogRow {
                query: String,
                calls: u64,
                total_time_ms: f64,
                mean_time_ms: f64,
                min_time_ms: f64,
                max_time_ms: f64,
            }
            
            // ClickHouse query API - use .query().bind().fetch_all() for SELECT queries
            let query = "SELECT query, count() as calls, sum(query_duration_ms) as total_time_ms, avg(query_duration_ms) as mean_time_ms, min(query_duration_ms) as min_time_ms, max(query_duration_ms) as max_time_ms FROM system.query_log WHERE type = 2 AND query_start_time > now() - INTERVAL 1 HOUR AND query NOT LIKE '%system.query_log%' AND query NOT LIKE '%system.metrics%' GROUP BY query ORDER BY mean_time_ms DESC LIMIT ?";
            
            let rows: Vec<QueryLogRow> = client
                .query(query)
                .bind(config.performance_schema.limit)
                .fetch_all()
                .await
                .context("Failed to query system.query_log")?;
            
            let base_tags = vec![
                KeyValue::new("host", config.host.clone()),
                KeyValue::new("source", "remote"),
                KeyValue::new("database", config.name.clone()),
                KeyValue::new("db_name", config.database.clone()),
                KeyValue::new("db_type", "clickhouse"),
            ];
            
            let mut result = Vec::new();
            
            for row in rows {
                let query_text = row.query;
                
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
                    calls: row.calls as i64,
                    total_time_ms: row.total_time_ms,
                    mean_time_ms: row.mean_time_ms,
                    min_time_ms: row.min_time_ms,
                    max_time_ms: row.max_time_ms,
                    tags: query_tags,
                });
            }
            
            Ok::<_, anyhow::Error>(result)
        })
    }
}

impl Collector for ClickHouseCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled || !self.config.performance_schema.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let client = self.client.clone();
        
        // Query calls counter
        let _query_calls = meter
            .u64_observable_counter("database.clickhouse.queries.calls")
            .with_description("Number of query executions")
            .with_callback({
                let config = config.clone();
                let client = client.clone();
                move |observer| {
                    if let Some(ref client) = client {
                        if let Ok(rows) = Self::query_query_log(client, &config) {
                            for row_data in rows {
                                let tags_slice: &[KeyValue] = &row_data.tags;
                                observer.observe(row_data.calls as u64, tags_slice);
                            }
                        }
                    }
                }
            })
            .build();
        
        // Mean execution time gauge
        let _query_mean_time = meter
            .f64_observable_gauge("database.clickhouse.queries.mean_time_ms")
            .with_description("Mean execution time in milliseconds")
            .with_callback({
                let config = config.clone();
                let client = client.clone();
                move |observer| {
                    if let Some(ref client) = client {
                        if let Ok(rows) = Self::query_query_log(client, &config) {
                            for row_data in rows {
                                let tags_slice: &[KeyValue] = &row_data.tags;
                                observer.observe(row_data.mean_time_ms, tags_slice);
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
    
    normalized
}

