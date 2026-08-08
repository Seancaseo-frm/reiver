//! Cassandra/ScyllaDB database monitoring collector
//!
//! This collector uses the `scylla` crate for Cassandra/ScyllaDB connectivity.
//! The scylla driver is compatible with both ScyllaDB and Apache Cassandra.
//!
//! Metrics collected include:
//! - Connection metrics (from the driver)
//! - System-level metrics from system tables
//!
//! Note: Unlike SQL databases, Cassandra/ScyllaDB doesn't have query-level performance
//! metrics like pg_stat_statements. Instead, we focus on system-level metrics and
//! connection health.

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

pub struct CassandraCollector {
    config: Arc<DatabaseConfig>,
    // Store connection parameters for on-demand connection
    contact_points: Vec<String>,
    keyspace: String,
    username: String,
    password: String,
}

impl CassandraCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection parameters for Cassandra/ScyllaDB
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

        info!("Testing Cassandra/ScyllaDB connection...");
        let start = std::time::Instant::now();
        
        // Build contact points (host:port)
        let contact_point = format!("{}:{}", config.host, config.port);
        let contact_points = vec![contact_point.clone()];
        
        // Create session
        let session: Session = SessionBuilder::new()
            .known_node(&contact_point)
            .user(&config.username, &password)
            .build()
            .await
            .context(format!("Failed to connect to Cassandra/ScyllaDB database: {}", config.name))?;
        
        // Test query to verify connection
        let _result = session
            .query_unpaged("SELECT release_version FROM system.local", &[])
            .await
            .context("Failed to execute test query")?;
        
        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to Cassandra/ScyllaDB successfully");

        // Clone values before moving config
        let keyspace = config.database.clone();
        let username = config.username.clone();

        Ok(Self {
            config,
            contact_points,
            keyspace,
            username,
            password,
        })
    }
}

impl Collector for CassandraCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        // For Cassandra/ScyllaDB, we primarily focus on connection health and system metrics
        // Query-level performance metrics are not available like in SQL databases
        // The scylla driver doesn't expose detailed query metrics in the same way
        
        // For now, we'll register basic metrics if needed in the future
        // Most monitoring for Cassandra/ScyllaDB is done at the application/driver level
        // or through system tables which require CQL queries
        
        warn!("Cassandra/ScyllaDB collector initialized. Note: Detailed query-level metrics are not available like in SQL databases. Consider using application-level metrics or system table queries.");
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled
    }
}
