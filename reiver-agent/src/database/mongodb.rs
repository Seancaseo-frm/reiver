use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

pub struct MongoDBCollector {
    config: Arc<DatabaseConfig>,
    client: Option<mongodb::Client>,
}

impl MongoDBCollector {
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
        
        // MongoDB connection string format: mongodb://[username:password@]host[:port][/database][?options]
        let conn_str = if password.is_empty() && config.username.is_empty() {
            format!("mongodb://{}:{}", config.host, config.port)
        } else {
            format!("mongodb://{}:{}@{}:{}/{}", config.username, password, config.host, config.port, config.database)
        };

        info!("Connecting to MongoDB...");
        let start = std::time::Instant::now();
        
        let client_options = mongodb::options::ClientOptions::parse(&conn_str).await
            .context(format!("Failed to parse MongoDB connection string: {}", config.name))?;
        
        let client = mongodb::Client::with_options(client_options)
            .context(format!("Failed to create MongoDB client: {}", config.name))?;
        
        // Test connection
        client.database("admin")
            .run_command(mongodb::bson::doc! { "ping": 1 })
            .await
            .context("Failed to ping MongoDB")?;

        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to MongoDB");

        Ok(Self {
            config,
            client: Some(client),
        })
    }

    /// Query MongoDB serverStatus() and return structured metrics
    fn query_server_status(client: &mongodb::Client, config: &DatabaseConfig) -> Result<Vec<(String, f64, Vec<KeyValue>)>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to create tokio runtime for MongoDB query")?;
        
        let mut metrics = Vec::new();
        
        rt.block_on(async {
            let db = client.database("admin");
            
            // Execute serverStatus command
            let status: mongodb::bson::Document = db
                .run_command(mongodb::bson::doc! { "serverStatus": 1 })
                .await
                .context("Failed to execute serverStatus")?;
            
            let mut base_tags = vec![
                KeyValue::new("host", config.host.clone()),
                KeyValue::new("source", "remote"),
                KeyValue::new("database", config.name.clone()),
                KeyValue::new("db_type", "mongodb"),
            ];
            
            // Extract metrics from serverStatus document
            // This is a simplified version - MongoDB serverStatus has many nested fields
            Self::extract_metrics_from_doc(&status, "mongodb".to_string(), &base_tags, &mut metrics);
            
            Ok::<_, anyhow::Error>(())
        })?;
        
        Ok(metrics)
    }

    /// Recursively extract metrics from MongoDB BSON document
    fn extract_metrics_from_doc(
        doc: &mongodb::bson::Document,
        prefix: String,
        base_tags: &[KeyValue],
        metrics: &mut Vec<(String, f64, Vec<KeyValue>)>,
    ) {
        for (key, value) in doc {
            let metric_name = format!("{}.{}", prefix, key);
            
            match value {
                mongodb::bson::Bson::Double(v) => {
                    metrics.push((metric_name, *v, base_tags.to_vec()));
                }
                mongodb::bson::Bson::Int32(v) => {
                    metrics.push((metric_name, *v as f64, base_tags.to_vec()));
                }
                mongodb::bson::Bson::Int64(v) => {
                    metrics.push((metric_name, *v as f64, base_tags.to_vec()));
                }
                mongodb::bson::Bson::Document(nested) => {
                    Self::extract_metrics_from_doc(nested, metric_name, base_tags, metrics);
                }
                _ => {} // Skip other types
            }
        }
    }
}

impl Collector for MongoDBCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let client = self.client.clone();
        
        // Common MongoDB metrics as observables
        let _connections = meter
            .u64_observable_gauge("mongodb.connections.current")
            .with_description("Current number of connections")
            .with_callback({
                let config = config.clone();
                let client = client.clone();
                move |observer| {
                    if let Some(ref client) = client {
                        if let Ok(metrics) = Self::query_server_status(client, &config) {
                            let tags = vec![
                                KeyValue::new("host", config.host.clone()),
                                KeyValue::new("source", "remote"),
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("db_type", "mongodb"),
                            ];
                            for (name, value, _) in metrics {
                                if name.contains("connections.current") {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();
        
        let _operations = meter
            .u64_observable_counter("mongodb.opcounters.command")
            .with_description("Total number of commands executed")
            .with_callback({
                let config = config.clone();
                let client = client.clone();
                move |observer| {
                    if let Some(ref client) = client {
                        if let Ok(metrics) = Self::query_server_status(client, &config) {
                            let tags = vec![
                                KeyValue::new("host", config.host.clone()),
                                KeyValue::new("source", "remote"),
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("db_type", "mongodb"),
                            ];
                            for (name, value, _) in metrics {
                                if name.contains("opcounters.command") {
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
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

