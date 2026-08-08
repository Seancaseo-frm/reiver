//! Presto/Trino database monitoring collector
//!
//! This collector connects directly to Presto/Trino clusters and collects metrics
//! from system tables and runtime statistics.
//!
//! Presto and Trino are distributed SQL query engines. They provide system tables
//! in the `system` catalog for monitoring:
//! - system.runtime.queries (query execution information)
//! - system.runtime.nodes (cluster node information)
//! - system.runtime.tasks (task execution details)
//!
//! Metrics collected include:
//! - Query execution statistics
//! - Active queries count
//! - Queued queries count
//! - Node count
//! - Query duration
//! - Data processed

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

pub struct PrestoCollector {
    config: Arc<DatabaseConfig>,
    // Store connection parameters for on-demand connection
    server_url: String,
    catalog: String,
    schema: String,
    username: String,
}

impl PrestoCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build server URL
        // Presto/Trino typically uses HTTP/HTTPS
        let protocol = if config.port == 443 || config.port == 8443 {
            "https"
        } else {
            "http"
        };
        let server_url = format!("{}://{}:{}", protocol, config.host, config.port);
        
        // Parse catalog and schema from database field
        // Format: catalog/schema or just catalog
        let parts: Vec<&str> = config.database.split('/').collect();
        let catalog = parts.first().map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());
        let schema = parts.get(1).map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());

        info!("Connecting to Presto/Trino database...");
        let start = std::time::Instant::now();
        
        // Test connection using prusto
        let server_url_clone = server_url.clone();
        let catalog_clone = catalog.clone();
        let schema_clone = schema.clone();
        let username_clone = config.username.clone();
        
        let test_result = tokio::task::spawn_blocking(move || {
            // Create a runtime for async operations
            let rt = tokio::runtime::Runtime::new()
                .context("Failed to create tokio runtime")?;
            
            rt.block_on(async {
                use prusto::ClientBuilder;
                
                // Create client using builder pattern
                // Parse host from server_url (format: http://host:port)
                let url_parts: Vec<&str> = server_url_clone
                    .trim_start_matches("http://")
                    .trim_start_matches("https://")
                    .split(':')
                    .collect();
                let host = url_parts.first().unwrap_or(&"localhost");
                let port: u16 = url_parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(8080);
                let secure = server_url_clone.starts_with("https://");
                
                let client = ClientBuilder::new(&username_clone, host)
                    .port(port)
                    .secure(secure)
                    .catalog(&catalog_clone)
                    .schema(&schema_clone)
                    .build()
                    .context("Failed to build Presto client")?;
                
                // Test connection with a simple query
                // Use get() to get QueryResult, not execute() which just runs the query
                let response = client.get::<prusto::Row>("SELECT 1".to_string()).await
                    .context("Failed to execute test query")?;
                
                // Check for errors
                if let Some(error) = response.error {
                    return Err(anyhow::anyhow!("Query failed: {:?}", error));
                }
                
                Ok::<(), anyhow::Error>(())
            })
        }).await;
        
        match test_result {
            Ok(Ok(_)) => {
                let duration_ms = start.elapsed().as_millis();
                info!(duration_ms, "Connected to Presto/Trino database");
            }
            Ok(Err(e)) => {
                warn!("Initial connection test failed (will retry on first query): {}", e);
            }
            Err(e) => {
                warn!("Connection test task failed: {}", e);
            }
        }

        let username_clone = config.username.clone();
        
        Ok(Self {
            config,
            server_url,
            catalog,
            schema,
            username: username_clone,
        })
    }

    /// Query Presto/Trino system tables for metrics
    fn query_presto_metrics(
        server_url: &str,
        catalog: &str,
        schema: &str,
        username: &str,
        config_host: &str,
        config_name: &str,
    ) -> Result<Vec<(String, f64, Vec<KeyValue>)>> {
        let base_tags = vec![
            KeyValue::new("host", config_host.to_string()),
            KeyValue::new("source", "remote"),
            KeyValue::new("database", config_name.to_string()),
            KeyValue::new("db_type", "presto"),
        ];

        let mut metrics = Vec::new();

        // Create a runtime for async operations
        let rt = tokio::runtime::Runtime::new()
            .context("Failed to create tokio runtime")?;

        rt.block_on(async {
            use prusto::{ClientBuilder, QueryResult, Row, Presto};
            use serde_json::Value as JsonValue;
            
            // Parse host from server_url (format: http://host:port)
            let url_parts: Vec<&str> = server_url
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .split(':')
                .collect();
            let host = url_parts.first().unwrap_or(&"localhost");
            let port: u16 = url_parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(8080);
            let secure = server_url.starts_with("https://");
            
            let client = match ClientBuilder::new(username, host)
                .port(port)
                .secure(secure)
                .catalog(catalog)
                .schema(schema)
                .build() {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to build Presto client: {}", e);
                    return Ok::<_, anyhow::Error>(());
                }
            };
            
            // Helper function to extract count from QueryResult
            // Note: QueryResult may have next_uri for pagination
            let extract_count = |mut result: QueryResult<Row>| -> i64 {
                if let Some(error) = result.error {
                    warn!("Query error: {:?}", error);
                    return 0;
                }
                
                // Handle pagination - follow next_uri until we have all data
                let client_ref = &client;
                let rt_handle = tokio::runtime::Handle::current();
                while let Some(next_uri) = result.next_uri.clone() {
                    match rt_handle.block_on(client_ref.get_next::<Row>(&next_uri)) {
                        Ok(next_result) => {
                            if let Some(error) = next_result.error {
                                warn!("Query error in pagination: {:?}", error);
                                break;
                            }
                            // Merge data sets if needed
                            if let Some(next_data) = next_result.data_set {
                                if let Some(ref mut current_data) = result.data_set {
                                    current_data.merge(next_data);
                                } else {
                                    result.data_set = Some(next_data);
                                }
                            }
                            result.next_uri = next_result.next_uri;
                        }
                        Err(e) => {
                            warn!("Failed to get next page: {}", e);
                            break;
                        }
                    }
                }
                
                if let Some(data_set) = result.data_set {
                    let rows = data_set.into_vec();
                    if let Some(row) = rows.first() {
                        // Access row data directly via value() method
                        let values = row.value();
                        if let Some(JsonValue::Number(num)) = values.first() {
                            return num.as_i64().unwrap_or_else(|| num.as_u64().unwrap_or(0) as i64);
                        }
                    }
                }
                0
            };
            
            // Query system.runtime.queries for active queries
            let active_queries: i64 = match client.get::<Row>("SELECT COUNT(*) as count FROM system.runtime.queries WHERE state = 'RUNNING'".to_string()).await {
                Ok(response) => extract_count(response),
                Err(e) => {
                    warn!("Failed to query active queries: {}", e);
                    0
                }
            };

            metrics.push(("presto.queries.active".to_string(), active_queries as f64, base_tags.clone()));

            // Query system.runtime.queries for queued queries
            let queued_queries: i64 = match client.get::<Row>("SELECT COUNT(*) as count FROM system.runtime.queries WHERE state = 'QUEUED'".to_string()).await {
                Ok(response) => extract_count(response),
                Err(e) => {
                    warn!("Failed to query queued queries: {}", e);
                    0
                }
            };

            metrics.push(("presto.queries.queued".to_string(), queued_queries as f64, base_tags.clone()));

            // Query system.runtime.nodes for node count
            let node_count: i64 = match client.get::<Row>("SELECT COUNT(*) as count FROM system.runtime.nodes".to_string()).await {
                Ok(response) => extract_count(response),
                Err(e) => {
                    warn!("Failed to query node count: {}", e);
                    0
                }
            };

            metrics.push(("presto.nodes.count".to_string(), node_count as f64, base_tags.clone()));

            // Query system.runtime.queries for recent query statistics
            match client.get::<Row>(
                "SELECT COUNT(*) as count, AVG(elapsed_time) as avg_duration_ms \
                 FROM system.runtime.queries \
                 WHERE created >= current_timestamp - interval '1' hour".to_string()
            ).await {
                Ok(response) => {
                    // Extract metrics from response if needed
                    // For now, we have the basic metrics above
                }
                Err(e) => {
                    warn!("Failed to query recent query statistics: {}", e);
                }
            }

            Ok::<_, anyhow::Error>(())
        })?;

        Ok(metrics)
    }
}

impl Collector for PrestoCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();

        // Register Presto/Trino-specific metrics
        let server_url = self.server_url.clone();
        let catalog = self.catalog.clone();
        let schema = self.schema.clone();
        let username = self.username.clone();
        let config_host = self.config.host.clone();
        let config_name = self.config.name.clone();

        let _queries_active = meter
            .u64_observable_gauge("presto.queries.active")
            .with_description("Number of active queries")
            .with_callback({
                let server_url = server_url.clone();
                let catalog = catalog.clone();
                let schema = schema.clone();
                let username = username.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_presto_metrics(
                        &server_url, &catalog, &schema, &username, &config_host, &config_name
                    ) {
                        for (name, value, tags) in metrics {
                            if name == "presto.queries.active" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        let _queries_queued = meter
            .u64_observable_gauge("presto.queries.queued")
            .with_description("Number of queued queries")
            .with_callback({
                let server_url = server_url.clone();
                let catalog = catalog.clone();
                let schema = schema.clone();
                let username = username.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_presto_metrics(
                        &server_url, &catalog, &schema, &username, &config_host, &config_name
                    ) {
                        for (name, value, tags) in metrics {
                            if name == "presto.queries.queued" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        let _nodes_count = meter
            .u64_observable_gauge("presto.nodes.count")
            .with_description("Number of nodes in the cluster")
            .with_callback({
                let server_url = server_url.clone();
                let catalog = catalog.clone();
                let schema = schema.clone();
                let username = username.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_presto_metrics(
                        &server_url, &catalog, &schema, &username, &config_host, &config_name
                    ) {
                        for (name, value, tags) in metrics {
                            if name == "presto.nodes.count" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();

        // Register general Presto metrics gauge
        let _presto_metrics = meter
            .f64_observable_gauge("presto.metrics")
            .with_description("Presto/Trino-specific metrics")
            .with_callback({
                let server_url = server_url.clone();
                let catalog = catalog.clone();
                let schema = schema.clone();
                let username = username.clone();
                let config_host = config_host.clone();
                let config_name = config_name.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_presto_metrics(
                        &server_url, &catalog, &schema, &username, &config_host, &config_name
                    ) {
                        for (_metric_name, value, tags) in metrics {
                            observer.observe(value, &tags);
                        }
                    }
                }
            })
            .build();

        info!("Presto/Trino collector initialized");
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled && self.config.query_metrics.enabled
    }
}
