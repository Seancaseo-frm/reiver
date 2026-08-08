use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{instrument, info};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use redis::AsyncCommands;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

pub struct RedisCollector {
    config: Arc<DatabaseConfig>,
    client: Option<redis::Client>,
}

impl RedisCollector {
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
        
        // Redis connection string format: redis://[:password@]host[:port][/database]
        let conn_str = if password.is_empty() {
            format!("redis://{}:{}", config.host, config.port)
        } else {
            format!("redis://:{}@{}:{}", password, config.host, config.port)
        };

        info!("Connecting to Redis...");
        let start = std::time::Instant::now();
        
        let client = redis::Client::open(conn_str.as_str())
            .context(format!("Failed to create Redis client: {}", config.name))?;
        
        // Test connection
        let mut conn = client.get_async_connection().await
            .context(format!("Failed to connect to Redis: {}", config.name))?;
        
        let _: String = redis::cmd("PING").query_async(&mut conn).await
            .context("Failed to ping Redis")?;

        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to Redis");

        Ok(Self {
            config,
            client: Some(client),
        })
    }

    /// Query Redis INFO command and return structured metrics
    fn query_redis_info(client: &redis::Client, config: &DatabaseConfig) -> Result<Vec<(String, f64, Vec<KeyValue>)>> {
        use redis::aio::ConnectionManager;
        
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to create tokio runtime for Redis query")?;
        
        let mut metrics = Vec::new();
        
        rt.block_on(async {
            let mut conn = client.get_async_connection().await?;
            
            // Execute INFO command (returns all sections)
            let info: String = redis::cmd("INFO").query_async(&mut conn).await?;
            
            // Parse INFO output (key:value format, sections separated by #)
            let mut base_tags = vec![
                KeyValue::new("host", config.host.clone()),
                KeyValue::new("source", "remote"),
                KeyValue::new("database", config.name.clone()),
                KeyValue::new("db_type", "redis"),
            ];
            
            // Parse INFO sections
            let mut current_section = String::new();
            for line in info.lines() {
                if line.starts_with('#') {
                    current_section = line.trim_start_matches('#').trim().to_lowercase().replace(' ', "_");
                    continue;
                }
                
                if line.is_empty() || !line.contains(':') {
                    continue;
                }
                
                let parts: Vec<&str> = line.splitn(2, ':').collect();
                if parts.len() != 2 {
                    continue;
                }
                
                let key = parts[0].trim();
                let value_str = parts[1].trim();
                
                // Try to parse as f64
                if let Ok(value) = value_str.parse::<f64>() {
                    let tags = {
                        let mut t = base_tags.clone();
                        t.push(KeyValue::new("section", current_section.clone()));
                        t
                    };
                    
                    let metric_name = format!("redis.{}.{}", current_section, key);
                    metrics.push((metric_name, value, tags));
                }
            }
            
            Ok::<_, anyhow::Error>(())
        })?;
        
        Ok(metrics)
    }
}

impl Collector for RedisCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled {
            return Ok(());
        }
        
        let config = self.config.clone();
        let client = self.client.clone();
        
        // Create observable gauge for all Redis INFO metrics
        // Redis doesn't have query-level metrics like SQL databases, so we collect general stats
        let _redis_metrics = meter
            .f64_observable_gauge("redis.info")
            .with_description("Redis INFO metrics")
            .with_callback({
                let config = config.clone();
                let client = client.clone();
                move |observer| {
                    if let Some(ref client) = client {
                        if let Ok(metrics) = Self::query_redis_info(client, &config) {
                            for (_metric_name, value, tags) in metrics {
                                // For now, observe all metrics with the same gauge name but different tags
                                // In a production system, you might want separate gauges for different metric types
                                observer.observe(value, &tags);
                            }
                        }
                    }
                }
            })
            .build();
        
        // Common Redis metrics as separate observables for better UX
        let _connected_clients = meter
            .u64_observable_gauge("redis.clients.connected")
            .with_description("Number of connected clients")
            .with_callback({
                let config = config.clone();
                let client = client.clone();
                move |observer| {
                    if let Some(ref client) = client {
                        if let Ok(metrics) = Self::query_redis_info(client, &config) {
                            let mut tags = vec![
                                KeyValue::new("host", config.host.clone()),
                                KeyValue::new("source", "remote"),
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("db_type", "redis"),
                            ];
                            for (name, value, _) in metrics {
                                if name.contains("connected_clients") {
                                    let tags = vec![
                                        KeyValue::new("host", config.host.clone()),
                                        KeyValue::new("source", "remote"),
                                        KeyValue::new("database", config.name.clone()),
                                        KeyValue::new("db_type", "redis"),
                                    ];
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();
        
        let _used_memory = meter
            .u64_observable_gauge("redis.memory.used_bytes")
            .with_description("Memory used by Redis in bytes")
            .with_callback({
                let config = config.clone();
                let client = client.clone();
                move |observer| {
                    if let Some(ref client) = client {
                        if let Ok(metrics) = Self::query_redis_info(client, &config) {
                            let mut tags = vec![
                                KeyValue::new("host", config.host.clone()),
                                KeyValue::new("source", "remote"),
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("db_type", "redis"),
                            ];
                            for (name, value, _) in metrics {
                                if name.contains("used_memory") {
                                    let tags = vec![
                                        KeyValue::new("host", config.host.clone()),
                                        KeyValue::new("source", "remote"),
                                        KeyValue::new("database", config.name.clone()),
                                        KeyValue::new("db_type", "redis"),
                                    ];
                                    observer.observe(value as u64, &tags);
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .build();
        
        let _commands_processed = meter
            .u64_observable_counter("redis.stats.total_commands_processed")
            .with_description("Total number of commands processed")
            .with_callback({
                let config = config.clone();
                let client = client.clone();
                move |observer| {
                    if let Some(ref client) = client {
                        if let Ok(metrics) = Self::query_redis_info(client, &config) {
                            let mut tags = vec![
                                KeyValue::new("host", config.host.clone()),
                                KeyValue::new("source", "remote"),
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("db_type", "redis"),
                            ];
                            for (name, value, _) in metrics {
                                if name.contains("total_commands_processed") {
                                    let tags = vec![
                                        KeyValue::new("host", config.host.clone()),
                                        KeyValue::new("source", "remote"),
                                        KeyValue::new("database", config.name.clone()),
                                        KeyValue::new("db_type", "redis"),
                                    ];
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

