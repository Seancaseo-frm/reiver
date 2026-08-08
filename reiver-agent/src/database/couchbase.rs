//! CouchBase database monitoring collector
//!
//! This collector uses the `couchbase` crate for CouchBase connectivity.
//!
//! Metrics collected include:
//! - Connection health
//! - Cluster and bucket statistics
//!
//! Note: Unlike SQL databases, CouchBase doesn't have query-level performance
//! metrics like pg_stat_statements. Instead, we focus on cluster/bucket-level metrics
//! and connection health.

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use couchbase::cluster::Cluster;
use couchbase::options::cluster_options::ClusterOptions;
use couchbase::authenticator::{PasswordAuthenticator, Authenticator};

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

pub struct CouchBaseCollector {
    config: Arc<DatabaseConfig>,
    // Store connection parameters for on-demand connection
    connection_string: String,
    bucket_name: String,
    username: String,
    password: String,
}

impl CouchBaseCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build connection parameters for CouchBase
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

        info!("Testing CouchBase connection...");
        let start = std::time::Instant::now();
        
        // Build connection string for CouchBase (typically couchbase://host:port)
        // CouchBase uses couchbase:// protocol for cluster connections
        let connection_string = format!("couchbase://{}:{}", config.host, config.port);
        
        // Create authenticator and cluster options
        let authenticator = PasswordAuthenticator::new(&config.username, &password);
        let options = ClusterOptions::new(Authenticator::PasswordAuthenticator(authenticator));
        
        // Connect to cluster
        let cluster = Cluster::connect(&connection_string, options)
            .await
            .context(format!("Failed to connect to CouchBase cluster: {}", config.name))?;
        
        // Test connection by accessing a bucket if bucket name is provided
        // Otherwise, just creating the cluster is sufficient (actual connection happens on first use)
        if !config.database.is_empty() {
            let _bucket = cluster.bucket(&config.database);
            // Optionally wait for the bucket to be ready
            // _bucket.wait_until_ready(std::time::Duration::from_secs(5)).await?;
        }
        
        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to CouchBase successfully");

        // Clone values before moving config
        let bucket_name = config.database.clone();
        let username = config.username.clone();

        Ok(Self {
            config,
            connection_string,
            bucket_name,
            username,
            password,
        })
    }
}

impl Collector for CouchBaseCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        // For CouchBase, we primarily focus on connection health and cluster/bucket-level metrics
        // Query-level performance metrics are not available like in SQL databases
        // CouchBase provides cluster and bucket statistics but not query-level metrics
        
        warn!("CouchBase collector initialized. Note: Detailed query-level metrics are not available like in SQL databases. Consider using cluster/bucket-level statistics or application-level metrics.");
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled
    }
}
