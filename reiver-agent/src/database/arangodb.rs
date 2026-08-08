//! ArangoDB database monitoring collector
//!
//! This collector uses the `arangors` crate for ArangoDB connectivity.
//! ArangoDB is a multi-model database supporting document, graph, and key-value data models.
//!
//! Metrics collected include:
//! - Connection health
//! - Request statistics (HTTP requests, AQL queries)
//! - Connection counts
//! - Memory usage
//! - Database operations (reads, writes, deletes)
//! - Storage engine statistics

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use arangors::Connection;
use reqwest::Client as HttpClient;
use serde_json::Value;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

pub struct ArangoDBCollector {
    config: Arc<DatabaseConfig>,
    connection: Option<Arc<Connection>>,
    http_client: HttpClient,
    base_url: String,
    username: String,
    password: String,
}

impl ArangoDBCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection parameters
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
        
        // ArangoDB connection URL format: http://host:port
        let arango_url = format!("http://{}:{}", config.host, config.port);
        let base_url = arango_url.clone();

        info!("Connecting to ArangoDB...");
        let start = std::time::Instant::now();
        
        // Establish connection with basic authentication
        let connection = if password.is_empty() && config.username.is_empty() {
            Connection::establish_without_auth(&arango_url)
                .await
                .context(format!("Failed to connect to ArangoDB without auth: {}", config.name))?
        } else {
            Connection::establish_basic_auth(
                &arango_url,
                &config.username,
                &password,
            )
            .await
            .context(format!("Failed to connect to ArangoDB: {}", config.name))?
        };
        
        // Test connection by accessing the database
        let db = connection
            .db(&config.database)
            .await
            .context(format!("Failed to access ArangoDB database '{}': {}", config.database, config.name))?;
        
        // Verify connection by getting database info
        let _info = db.info().await
            .context("Failed to get ArangoDB database info")?;

        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to ArangoDB successfully");

        let username = config.username.clone();
        
        Ok(Self {
            config,
            connection: Some(Arc::new(connection)),
            http_client: HttpClient::new(),
            base_url,
            username,
            password,
        })
    }

    /// Query ArangoDB metrics API and return structured metrics (internal helper)
    fn query_metrics_internal(
        base_url: &str,
        username: &str,
        password: &str,
        http_client: &HttpClient,
        config_host: &str,
        config_name: &str,
    ) -> Result<Vec<(String, f64, Vec<KeyValue>)>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to create tokio runtime for ArangoDB query")?;
        
        let mut metrics = Vec::new();
        let base_url = base_url.to_string();
        let username = username.to_string();
        let password = password.to_string();
        let http_client = http_client.clone();
        let config_host = config_host.to_string();
        let config_name = config_name.to_string();
        
        rt.block_on(async {
            // Try to get metrics from /_admin/metrics/v2 (Prometheus format)
            // If that fails, fall back to /_api/engine/stats
            let metrics_url = format!("{}/_admin/metrics/v2", base_url);
            let mut request = http_client.get(&metrics_url);
            
            if !username.is_empty() {
                request = request.basic_auth(&username, Some(&password));
            }
            
            let response = request.send().await;
            
            if let Ok(resp) = response {
                if resp.status().is_success() {
                    if let Ok(text) = resp.text().await {
                        // Parse Prometheus format metrics
                        let base_tags = vec![
                            KeyValue::new("host", config_host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config_name.clone()),
                            KeyValue::new("db_type", "arangodb"),
                        ];
                        Self::parse_prometheus_metrics(&text, &base_tags, &mut metrics);
                        return Ok::<_, anyhow::Error>(());
                    }
                }
            }
            
            // Fallback to engine stats
            let stats_url = format!("{}/_api/engine/stats", base_url);
            let mut request = http_client.get(&stats_url);
            
            if !username.is_empty() {
                request = request.basic_auth(&username, Some(&password));
            }
            
            let response = request.send().await?;
            
            if response.status().is_success() {
                let stats: Value = response.json().await?;
                let base_tags = vec![
                    KeyValue::new("host", config_host.clone()),
                    KeyValue::new("source", "remote"),
                    KeyValue::new("database", config_name.clone()),
                    KeyValue::new("db_type", "arangodb"),
                ];
                Self::parse_engine_stats(&stats, &base_tags, &mut metrics);
            }
            
            Ok::<_, anyhow::Error>(())
        })?;
        
        Ok(metrics)
    }

    /// Parse Prometheus format metrics from ArangoDB
    fn parse_prometheus_metrics(text: &str, base_tags: &[KeyValue], metrics: &mut Vec<(String, f64, Vec<KeyValue>)>) {
        for line in text.lines() {
            // Skip comments and empty lines
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            
            // Prometheus format: metric_name{labels} value
            if let Some(space_idx) = line.rfind(' ') {
                let (metric_part, value_str) = line.split_at(space_idx);
                let value_str = value_str.trim();
                
                // Extract metric name and labels
                if let Some(brace_idx) = metric_part.find('{') {
                    let metric_name = &metric_part[..brace_idx];
                    // Parse value
                    if let Ok(value) = value_str.parse::<f64>() {
                        // Convert Prometheus metric names to our format
                        let reiver_name = format!("arangodb.{}", metric_name.replace("arangodb_", ""));
                        metrics.push((reiver_name, value, base_tags.to_vec()));
                    }
                } else {
                    // No labels
                    let metric_name = metric_part.trim();
                    if let Ok(value) = value_str.parse::<f64>() {
                        let reiver_name = format!("arangodb.{}", metric_name.replace("arangodb_", ""));
                        metrics.push((reiver_name, value, base_tags.to_vec()));
                    }
                }
            }
        }
    }

    /// Parse engine stats JSON from ArangoDB
    fn parse_engine_stats(stats: &Value, base_tags: &[KeyValue], metrics: &mut Vec<(String, f64, Vec<KeyValue>)>) {
        // Extract common metrics from engine stats
        if let Some(obj) = stats.as_object() {
            // Memory metrics
            if let Some(memory) = obj.get("memory") {
                if let Some(used) = memory.get("used").and_then(|v| v.as_f64()) {
                    metrics.push(("arangodb.memory.used_bytes".to_string(), used, base_tags.to_vec()));
                }
                if let Some(allocated) = memory.get("allocated").and_then(|v| v.as_f64()) {
                    metrics.push(("arangodb.memory.allocated_bytes".to_string(), allocated, base_tags.to_vec()));
                }
            }
            
            // Threads
            if let Some(threads) = obj.get("threads").and_then(|v| v.as_f64()) {
                metrics.push(("arangodb.threads.count".to_string(), threads, base_tags.to_vec()));
            }
            
            // Transactions
            if let Some(transactions) = obj.get("transactions") {
                if let Some(committed) = transactions.get("committed").and_then(|v| v.as_f64()) {
                    metrics.push(("arangodb.transactions.committed".to_string(), committed, base_tags.to_vec()));
                }
                if let Some(aborted) = transactions.get("aborted").and_then(|v| v.as_f64()) {
                    metrics.push(("arangodb.transactions.aborted".to_string(), aborted, base_tags.to_vec()));
                }
            }
            
            // RocksDB metrics (if available)
            if let Some(rocksdb) = obj.get("rocksdb") {
                if let Some(block_cache_size) = rocksdb.get("blockCacheSize").and_then(|v| v.as_f64()) {
                    metrics.push(("arangodb.rocksdb.block_cache_size_bytes".to_string(), block_cache_size, base_tags.to_vec()));
                }
                if let Some(estimate_num_keys) = rocksdb.get("estimateNumKeys").and_then(|v| v.as_f64()) {
                    metrics.push(("arangodb.rocksdb.estimate_num_keys".to_string(), estimate_num_keys, base_tags.to_vec()));
                }
            }
        }
    }

    /// Test connection health by accessing the database
    fn test_connection(connection: &Arc<Connection>, database_name: &str) -> bool {
        // Try to create a runtime if we don't have one
        let rt = tokio::runtime::Handle::try_current();
        if rt.is_err() {
            // If we can't get runtime, create a new one
            let rt = tokio::runtime::Runtime::new();
            if rt.is_err() {
                return false;
            }
            let rt = rt.unwrap();
            return rt.block_on(async {
                match connection.db(database_name).await {
                    Ok(db) => db.info().await.is_ok(),
                    Err(_) => false,
                }
            });
        }
        
        let rt = rt.unwrap();
        rt.block_on(async {
            match connection.db(database_name).await {
                Ok(db) => db.info().await.is_ok(),
                Err(_) => false,
            }
        })
    }
}

impl Collector for ArangoDBCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let base_url = self.base_url.clone();
        let username = self.username.clone();
        let password = self.password.clone();
        let http_client = self.http_client.clone();
        let config_host = self.config.host.clone();
        let config_name = self.config.name.clone();
        let database_name = self.config.database.clone();
        let connection = self.connection.clone();

        // Register connection health metric
        let _connection_gauge = meter
            .f64_observable_gauge("arangodb.connection.healthy")
            .with_description("ArangoDB connection health (1 = healthy, 0 = unhealthy)")
            .with_callback({
                let config = config.clone();
                let connection = connection.clone();
                let database_name = database_name.clone();
                move |observer| {
                    if let Some(ref conn) = connection {
                        let is_healthy = Self::test_connection(conn, &database_name);
                        observer.observe(
                            if is_healthy { 1.0 } else { 0.0 },
                            &[
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("host", config.host.clone()),
                                KeyValue::new("db_name", database_name.clone()),
                            ],
                        );
                    } else {
                        observer.observe(
                            0.0,
                            &[
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("host", config.host.clone()),
                                KeyValue::new("db_name", database_name.clone()),
                            ],
                        );
                    }
                }
            })
            .build();

        // Register memory usage metric
        let _memory_used = meter
            .u64_observable_gauge("arangodb.memory.used_bytes")
            .with_description("Memory used by ArangoDB in bytes")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_metrics_internal(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "arangodb"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "arangodb.memory.used_bytes" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        // Register transaction metrics
        let _transactions_committed = meter
            .u64_observable_counter("arangodb.transactions.committed")
            .with_description("Number of committed transactions")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_metrics_internal(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "arangodb"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "arangodb.transactions.committed" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        let _transactions_aborted = meter
            .u64_observable_counter("arangodb.transactions.aborted")
            .with_description("Number of aborted transactions")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_metrics_internal(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "arangodb"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "arangodb.transactions.aborted" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        // Register general metrics gauge for all other metrics
        let _arangodb_metrics = meter
            .f64_observable_gauge("arangodb.metrics")
            .with_description("ArangoDB metrics from metrics API")
            .with_callback({
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_metrics_internal(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        for (_metric_name, value, tags) in metrics {
                            // Observe all metrics with their tags
                            observer.observe(value, &tags);
                        }
                    }
                }
            })
            .build();

        info!("ArangoDB collector initialized with comprehensive metrics collection");
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled && self.config.query_metrics.enabled
    }
}
