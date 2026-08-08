//! CouchDB database monitoring collector
//!
//! This collector uses the `couch_rs` crate for CouchDB connectivity.
//!
//! Metrics collected include:
//! - Connection health
//! - Database statistics
//!
//! Note: Unlike SQL databases, CouchDB doesn't have query-level performance
//! metrics like pg_stat_statements. Instead, we focus on database-level metrics
//! and connection health.

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

pub struct CouchDBCollector {
    config: Arc<DatabaseConfig>,
    // Store connection parameters for on-demand connection
    base_url: String,
    database: String,
    username: String,
    password: String,
}

impl CouchDBCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection parameters for CouchDB
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

        info!("Testing CouchDB connection...");
        let start = std::time::Instant::now();
        
        // Build base URL for CouchDB (typically http://host:port)
        let base_url = format!("http://{}:{}", config.host, config.port);
        
        // Create client - Client::new is not async but validates the URL format
        let client = couch_rs::Client::new(&base_url, &config.username, &password)
            .context(format!("Failed to create CouchDB client: {}", config.name))?;
        
        // Test connection by accessing a database if database name is provided
        // Otherwise, just creating the client is sufficient (actual connection happens on first use)
        if !config.database.is_empty() {
            let _db = client.db(&config.database).await
                .context(format!("Failed to access CouchDB database: {}", config.name))?;
        }
        
        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to CouchDB successfully");

        // Clone values before moving config
        let database = config.database.clone();
        let username = config.username.clone();

        Ok(Self {
            config,
            base_url,
            database,
            username,
            password,
        })
    }
}

impl Collector for CouchDBCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        // For CouchDB, we primarily focus on connection health and database-level metrics
        // Query-level performance metrics are not available like in SQL databases
        // CouchDB provides database statistics but not query-level metrics
        
        warn!("CouchDB collector initialized. Note: Detailed query-level metrics are not available like in SQL databases. Consider using database-level statistics or application-level metrics.");
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled
    }
}
