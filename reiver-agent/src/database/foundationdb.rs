//! FoundationDB database monitoring collector
//!
//! This collector uses the `foundationdb` crate for FoundationDB connectivity.
//! FoundationDB is a distributed key-value database.
//!
//! Metrics collected include:
//! - Connection health
//! - Basic cluster information
//!
//! Note: FoundationDB requires initialization with `foundationdb::boot()` which can
//! only be called once per process lifetime. The collector handles this initialization.

use anyhow::{Context, Result};
use std::sync::Arc;
use std::sync::Once;
use tracing::{instrument, info, warn};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use foundationdb::Database;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

// Global initialization flag - FoundationDB can only be initialized once per process
static FDB_INIT: Once = Once::new();

pub struct FoundationDBCollector {
    config: Arc<DatabaseConfig>,
    database: Option<Arc<Database>>,
}

impl FoundationDBCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        info!("Initializing FoundationDB connection...");
        
        // FoundationDB must be initialized once per process
        // We use a Once to ensure it's only initialized once
        FDB_INIT.call_once(|| {
            unsafe {
                let _network = foundationdb::boot();
                // NetworkAutoStop will automatically stop the network when dropped
            }
            info!("FoundationDB network initialized");
        });

        let start = std::time::Instant::now();
        
        // Connect to FoundationDB using default configuration
        // FoundationDB uses cluster file (typically /etc/foundationdb/fdb.cluster)
        // or FDB_CLUSTER_FILE environment variable
        let database = Database::default()
            .context(format!("Failed to connect to FoundationDB: {}", config.name))?;
        
        // Test connection by performing a simple read operation
        let test_key = b"__reiver_health_check__";
        let _result = database
            .run(|trx, _| async move {
                trx.get(test_key, false).await
                    .map_err(|e| foundationdb::FdbBindingError::from(e))
            })
            .await
            .context("Failed to execute test query on FoundationDB")?;
        
        let duration_ms = start.elapsed().as_millis();
        info!(duration_ms, "Connected to FoundationDB successfully");

        Ok(Self {
            config,
            database: Some(Arc::new(database)),
        })
    }

    /// Test connection health by performing a simple read
    fn test_connection(database: &Arc<Database>) -> bool {
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
                database
                    .run(|trx, _| async move {
                        trx.get(b"__health_check__", false).await
                            .map_err(|e| foundationdb::FdbBindingError::from(e))
                    })
                    .await
                    .is_ok()
            });
        }
        
        let rt = rt.unwrap();
        rt.block_on(async {
            database
                .run(|trx, _| async move {
                    trx.get(b"__health_check__", false).await
                        .map_err(|e| foundationdb::FdbBindingError::from(e))
                })
                .await
                .is_ok()
        })
    }
}

impl Collector for FoundationDBCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        let config = self.config.clone();
        let database = self.database.clone();

        // Register connection health metric
        let _connection_gauge = meter
            .f64_observable_gauge("foundationdb.connection.healthy")
            .with_description("FoundationDB connection health (1 = healthy, 0 = unhealthy)")
            .with_callback({
                let config = config.clone();
                move |observer| {
                    if let Some(ref db) = database {
                        let is_healthy = Self::test_connection(db);
                        observer.observe(
                            if is_healthy { 1.0 } else { 0.0 },
                            &[
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("host", config.host.clone()),
                            ],
                        );
                    } else {
                        observer.observe(
                            0.0,
                            &[
                                KeyValue::new("database", config.name.clone()),
                                KeyValue::new("host", config.host.clone()),
                            ],
                        );
                    }
                }
            })
            .build();

        info!("FoundationDB collector initialized with connection health metrics");
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled
    }
}
