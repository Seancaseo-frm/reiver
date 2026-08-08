//! InfluxDB database monitoring collector
//!
//! This collector uses the `influxdb` crate for InfluxDB connectivity.
//! InfluxDB is a time-series database designed for high-write and query workloads.
//!
//! Metrics collected include:
//! - Connection health
//! - Database statistics (series count, measurements, points)
//! - Query performance metrics
//! - Write performance metrics
//! - Memory usage
//! - HTTP request statistics

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use influxdb::Client;
use reqwest::Client as HttpClient;
use serde_json::Value;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

pub struct InfluxDBCollector {
    config: Arc<DatabaseConfig>,
    client: Option<Client>,
    http_client: HttpClient,
    base_url: String,
    username: String,
    password: String,
}

impl InfluxDBCollector {
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
        
        // InfluxDB connection URL format: http://host:port
        let base_url = format!("http://{}:{}", config.host, config.port);

        info!("Connecting to InfluxDB...");
        let start = std::time::Instant::now();
        
        // Create InfluxDB client
        let client = if !config.username.is_empty() && !password.is_empty() {
            Client::new(&base_url, &config.database)
                .with_auth(&config.username, &password)
        } else {
            Client::new(&base_url, &config.database)
        };
        
        // Test connection by querying database info
        // Use a simple query to verify connectivity
        use influxdb::ReadQuery;
        let test_query = ReadQuery::new("SHOW DATABASES");
        let _result = client.query(test_query).await
            .context(format!("Failed to query InfluxDB: {}", config.name))?;

        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to InfluxDB successfully");

        let username = config.username.clone();
        
        Ok(Self {
            config,
            client: Some(client),
            http_client: HttpClient::new(),
            base_url,
            username,
            password,
        })
    }

    /// Query InfluxDB metrics API and return structured metrics
    fn query_influxdb_metrics(
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
            .context("Failed to create tokio runtime for InfluxDB query")?;
        
        let mut metrics = Vec::new();
        let base_url = base_url.to_string();
        let username = username.to_string();
        let password = password.to_string();
        let http_client = http_client.clone();
        let config_host = config_host.to_string();
        let config_name = config_name.to_string();
        
        rt.block_on(async {
            // Try to get metrics from /metrics endpoint (Prometheus format)
            // If that fails, try /debug/vars (JSON format)
            let metrics_url = format!("{}/metrics", base_url);
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
                            KeyValue::new("db_type", "influxdb"),
                        ];
                        Self::parse_prometheus_metrics(&text, &base_tags, &mut metrics);
                        return Ok::<_, anyhow::Error>(());
                    }
                }
            }
            
            // Fallback to /debug/vars (JSON format)
            let vars_url = format!("{}/debug/vars", base_url);
            let mut request = http_client.get(&vars_url);
            
            if !username.is_empty() {
                request = request.basic_auth(&username, Some(&password));
            }
            
            let response = request.send().await?;
            
            if response.status().is_success() {
                let vars: Value = response.json().await?;
                let base_tags = vec![
                    KeyValue::new("host", config_host.clone()),
                    KeyValue::new("source", "remote"),
                    KeyValue::new("database", config_name.clone()),
                    KeyValue::new("db_type", "influxdb"),
                ];
                Self::parse_debug_vars(&vars, &base_tags, &mut metrics);
            }
            
            Ok::<_, anyhow::Error>(())
        })?;
        
        Ok(metrics)
    }

    /// Parse Prometheus format metrics from InfluxDB
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
                        let reiver_name = format!("influxdb.{}", metric_name.replace("influxdb_", ""));
                        metrics.push((reiver_name, value, base_tags.to_vec()));
                    }
                } else {
                    // No labels
                    let metric_name = metric_part.trim();
                    if let Ok(value) = value_str.parse::<f64>() {
                        let reiver_name = format!("influxdb.{}", metric_name.replace("influxdb_", ""));
                        metrics.push((reiver_name, value, base_tags.to_vec()));
                    }
                }
            }
        }
    }

    /// Parse debug/vars JSON from InfluxDB
    fn parse_debug_vars(vars: &Value, base_tags: &[KeyValue], metrics: &mut Vec<(String, f64, Vec<KeyValue>)>) {
        // Extract common metrics from debug/vars
        if let Some(obj) = vars.as_object() {
            // System metrics
            if let Some(cmdline) = obj.get("cmdline") {
                // cmdline contains command-line arguments, skip
            }
            
            // Database statistics
            if let Some(database) = obj.get("database") {
                if let Some(db_obj) = database.as_object() {
                    // Number of series
                    if let Some(num_series) = db_obj.get("numSeries").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.database.num_series".to_string(), num_series, base_tags.to_vec()));
                    }
                    // Number of measurements
                    if let Some(num_measurements) = db_obj.get("numMeasurements").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.database.num_measurements".to_string(), num_measurements, base_tags.to_vec()));
                    }
                }
            }
            
            // HTTP statistics
            if let Some(httpd) = obj.get("httpd") {
                if let Some(httpd_obj) = httpd.as_object() {
                    // Query requests
                    if let Some(query_req) = httpd_obj.get("queryReq").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.httpd.query_requests".to_string(), query_req, base_tags.to_vec()));
                    }
                    // Write requests
                    if let Some(write_req) = httpd_obj.get("writeReq").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.httpd.write_requests".to_string(), write_req, base_tags.to_vec()));
                    }
                    // Points written
                    if let Some(points_written) = httpd_obj.get("pointsWrittenOK").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.httpd.points_written_ok".to_string(), points_written, base_tags.to_vec()));
                    }
                    // Query duration
                    if let Some(query_duration) = httpd_obj.get("queryReqDurationNs").and_then(|v| v.as_f64()) {
                        // Convert nanoseconds to milliseconds
                        metrics.push(("influxdb.httpd.query_duration_ms".to_string(), query_duration / 1_000_000.0, base_tags.to_vec()));
                    }
                }
            }
            
            // Memory statistics
            if let Some(memstats) = obj.get("memstats") {
                if let Some(mem_obj) = memstats.as_object() {
                    // Alloc (bytes allocated)
                    if let Some(alloc) = mem_obj.get("Alloc").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.memory.alloc_bytes".to_string(), alloc, base_tags.to_vec()));
                    }
                    // Sys (bytes obtained from system)
                    if let Some(sys) = mem_obj.get("Sys").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.memory.sys_bytes".to_string(), sys, base_tags.to_vec()));
                    }
                    // HeapAlloc (bytes allocated and not yet freed)
                    if let Some(heap_alloc) = mem_obj.get("HeapAlloc").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.memory.heap_alloc_bytes".to_string(), heap_alloc, base_tags.to_vec()));
                    }
                }
            }
            
            // Query executor statistics
            if let Some(query_executor) = obj.get("queryExecutor") {
                if let Some(qe_obj) = query_executor.as_object() {
                    // Queries executed
                    if let Some(queries_executed) = qe_obj.get("queriesExecuted").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.query_executor.queries_executed".to_string(), queries_executed, base_tags.to_vec()));
                    }
                    // Query duration
                    if let Some(query_duration) = qe_obj.get("queryDurationNs").and_then(|v| v.as_f64()) {
                        metrics.push(("influxdb.query_executor.query_duration_ms".to_string(), query_duration / 1_000_000.0, base_tags.to_vec()));
                    }
                }
            }
        }
    }

    /// Test connection health by querying the database
    fn test_connection(client: &Client) -> bool {
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
                use influxdb::ReadQuery;
                let test_query = ReadQuery::new("SHOW DATABASES");
                client.query(test_query).await.is_ok()
            });
        }
        
        let rt = rt.unwrap();
        rt.block_on(async {
            use influxdb::ReadQuery;
            let test_query = ReadQuery::new("SHOW DATABASES");
            client.query(test_query).await.is_ok()
        })
    }
}

impl Collector for InfluxDBCollector {
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
        let client = self.client.clone();

        // Register connection health metric
        let _connection_gauge = meter
            .f64_observable_gauge("influxdb.connection.healthy")
            .with_description("InfluxDB connection health (1 = healthy, 0 = unhealthy)")
            .with_callback({
                let config = config.clone();
                let client = client.clone();
                move |observer| {
                    if let Some(ref c) = client {
                        let is_healthy = Self::test_connection(c);
                        observer.observe(
                            if is_healthy { 1.0 } else { 0.0 },
                            &[
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("host", config.host.clone()),
                                KeyValue::new("db_name", config.database.clone()),
                            ],
                        );
                    } else {
                        observer.observe(
                            0.0,
                            &[
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("host", config.host.clone()),
                                KeyValue::new("db_name", config.database.clone()),
                            ],
                        );
                    }
                }
            })
            .build();

        // Register memory usage metric
        let _memory_alloc = meter
            .u64_observable_gauge("influxdb.memory.alloc_bytes")
            .with_description("Memory allocated by InfluxDB in bytes")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_influxdb_metrics(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "influxdb"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "influxdb.memory.alloc_bytes" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        // Register HTTP request metrics
        let _query_requests = meter
            .u64_observable_counter("influxdb.httpd.query_requests")
            .with_description("Number of query requests")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_influxdb_metrics(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "influxdb"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "influxdb.httpd.query_requests" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        let _write_requests = meter
            .u64_observable_counter("influxdb.httpd.write_requests")
            .with_description("Number of write requests")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_influxdb_metrics(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "influxdb"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "influxdb.httpd.write_requests" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        let _points_written = meter
            .u64_observable_counter("influxdb.httpd.points_written_ok")
            .with_description("Number of points written successfully")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_influxdb_metrics(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "influxdb"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "influxdb.httpd.points_written_ok" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        // Register database statistics
        let _num_series = meter
            .u64_observable_gauge("influxdb.database.num_series")
            .with_description("Number of series in the database")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_influxdb_metrics(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "influxdb"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "influxdb.database.num_series" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        // Register general metrics gauge for all other metrics
        let _influxdb_metrics = meter
            .f64_observable_gauge("influxdb.metrics")
            .with_description("InfluxDB metrics from metrics API")
            .with_callback({
                let base_url = base_url.clone();
                let username = username.clone();
                let password = password.clone();
                let http_client = http_client.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_influxdb_metrics(&base_url, &username, &password, &http_client, &config_host, &config_name) {
                        for (_metric_name, value, tags) in metrics {
                            // Observe all metrics with their tags
                            observer.observe(value, &tags);
                        }
                    }
                }
            })
            .build();

        info!("InfluxDB collector initialized with comprehensive metrics collection");
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled && self.config.query_metrics.enabled
    }
}
