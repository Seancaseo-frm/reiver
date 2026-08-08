use anyhow::Result;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, error, warn, Instrument};

use crate::config::Config;
use crate::database::{
    PostgreSQLCollector, MySQLCollector, RedisCollector, MongoDBCollector,
    ClickHouseCollector, MariaDBCollector, SQLServerCollector,
    OracleCollector, ElasticsearchCollector, Db2Collector, HanaCollector,
    CockroachDBCollector, TiDBCollector, YugabyteDBCollector, SingleStoreCollector,
    CassandraCollector, ScyllaCollector, FoundationDBCollector, ArangoDBCollector, InfluxDBCollector, TimescaleDBCollector, RedshiftCollector, PrestoCollector, CouchDBCollector, CouchBaseCollector
};
use crate::metrics::system::SystemMetricsCollector;
use crate::metrics::Collector;

pub struct Agent {
    config: Arc<Config>,
    collectors: Vec<Box<dyn crate::metrics::Collector>>,
}

impl Agent {
    pub async fn new(config: Config, meter: opentelemetry::metrics::Meter) -> Result<Self> {
        let config = Arc::new(config);
        
        let mut collectors: Vec<Box<dyn crate::metrics::Collector>> = Vec::new();
        
        // Add system metrics collector if enabled
        if config.system_metrics.enabled {
            info!("Initializing system metrics collector");
            let system_collector = SystemMetricsCollector::new(config.clone());
            // Register observable callbacks with the meter
            system_collector.register_observables(meter.clone())?;
            collectors.push(Box::new(system_collector));
        }
        
        // Add database collectors
        for db_config in &config.databases {
            if !db_config.enabled {
                continue;
            }
            
            match db_config.r#type.to_lowercase().as_str() {
                "postgresql" | "postgres" => {
                    info!("Initializing PostgreSQL collector for database: {}", db_config.name);
                    match PostgreSQLCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize PostgreSQL collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "mysql" => {
                    info!("Initializing MySQL collector for database: {}", db_config.name);
                    match MySQLCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize MySQL collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "mariadb" => {
                    info!("Initializing MariaDB collector for database: {}", db_config.name);
                    // MariaDB uses MySQL collector (they're compatible)
                    match MySQLCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize MariaDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "redis" => {
                    info!("Initializing Redis collector for database: {}", db_config.name);
                    match RedisCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize Redis collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "mongodb" | "mongo" => {
                    info!("Initializing MongoDB collector for database: {}", db_config.name);
                    match MongoDBCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize MongoDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "clickhouse" => {
                    info!("Initializing ClickHouse collector for database: {}", db_config.name);
                    match ClickHouseCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize ClickHouse collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "sqlserver" | "mssql" | "sql-server" => {
                    info!("Initializing SQL Server collector for database: {}", db_config.name);
                    match SQLServerCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize SQL Server collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "oracle" => {
                    info!("Initializing Oracle collector for database: {}", db_config.name);
                    match OracleCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize Oracle collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "elasticsearch" | "es" => {
                    info!("Initializing Elasticsearch collector for database: {}", db_config.name);
                    match ElasticsearchCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize Elasticsearch collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "db2" | "ibmdb2" | "ibm_db2" => {
                    info!("Initializing IBM Db2 collector for database: {}", db_config.name);
                    match Db2Collector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize IBM Db2 collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "hana" | "sap_hana" | "saphana" | "sap-hana" => {
                    info!("Initializing SAP HANA collector for database: {}", db_config.name);
                    match HanaCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize SAP HANA collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "cockroachdb" | "cockroach" | "crdb" => {
                    info!("Initializing CockroachDB collector for database: {}", db_config.name);
                    match CockroachDBCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize CockroachDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "tidb" => {
                    info!("Initializing TiDB collector for database: {}", db_config.name);
                    match TiDBCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize TiDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "yugabytedb" | "yugabyte" | "yugabyte-db" => {
                    info!("Initializing YugabyteDB collector for database: {}", db_config.name);
                    match YugabyteDBCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize YugabyteDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "singlestore" | "memsql" | "single-store" => {
                    info!("Initializing SingleStore collector for database: {}", db_config.name);
                    match SingleStoreCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize SingleStore collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "cassandra" => {
                    info!("Initializing Cassandra collector for database: {}", db_config.name);
                    match CassandraCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize Cassandra collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "scylla" | "scylladb" => {
                    info!("Initializing ScyllaDB collector for database: {}", db_config.name);
                    match ScyllaCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize ScyllaDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "foundationdb" | "fdb" => {
                    info!("Initializing FoundationDB collector for database: {}", db_config.name);
                    match FoundationDBCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize FoundationDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "arangodb" | "arango" => {
                    info!("Initializing ArangoDB collector for database: {}", db_config.name);
                    match ArangoDBCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize ArangoDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "influxdb" | "influx" => {
                    info!("Initializing InfluxDB collector for database: {}", db_config.name);
                    match InfluxDBCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize InfluxDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "timescaledb" | "timescale" => {
                    info!("Initializing TimescaleDB collector for database: {}", db_config.name);
                    match TimescaleDBCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize TimescaleDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "redshift" => {
                    info!("Initializing Redshift collector for database: {}", db_config.name);
                    match RedshiftCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize Redshift collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "presto" | "trino" => {
                    info!("Initializing Presto/Trino collector for database: {}", db_config.name);
                    match PrestoCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize Presto/Trino collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                "couchdb" | "couch" => {
                    info!("Initializing CouchDB collector for database: {}", db_config.name);
                    match CouchDBCollector::new(Arc::new(db_config.clone())).await {
                        Ok(collector) => {
                            collector.register_observables(meter.clone())?;
                            collectors.push(Box::new(collector));
                        }
                        Err(e) => {
                            error!("Failed to initialize CouchDB collector for {}: {}", db_config.name, e);
                        }
                    }
                }
                _ => {
                    warn!("Unknown database type: {} for database: {}", db_config.r#type, db_config.name);
                }
            }
        }
        
        info!("Agent initialized with {} collector(s)", collectors.len());
        info!("Metrics will be automatically exported via OpenTelemetry OTLP");
        
        Ok(Agent {
            config,
            collectors,
        })
    }
    
    pub async fn run(&mut self) -> Result<()> {
        let span = tracing::span!(tracing::Level::INFO, "agent.startup");
        let _guard = span.enter();
        
        info!("Starting Reiver Agent...");
        info!("Metrics are being exported automatically via OpenTelemetry OTLP");
        info!("No manual collection loop needed - SDK handles metric export");
        
        drop(_guard); // Exit startup span
        
        // With OpenTelemetry observable callbacks, metrics are exported automatically
        // by the SDK when export intervals occur. We just need to keep the agent running.
        // Periodically refresh system state for observable callbacks to read fresh data.
        loop {
            // Refresh system state periodically so observable callbacks read current values
            // The actual export happens asynchronously by the OpenTelemetry SDK
            tokio::time::sleep(Duration::from_secs(1)).await;
            
            // The OpenTelemetry SDK will call our observable callbacks
            // when it needs to export metrics (based on batch configuration)
        }
    }
}

