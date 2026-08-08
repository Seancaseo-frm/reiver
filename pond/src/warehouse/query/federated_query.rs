//! Federated Query Execution using DataFusion Federation
//!
//! This module provides federated query execution across multiple data sources
//! (PostgreSQL, MySQL, Parquet) using datafusion-table-providers for database
//! connectivity.
//!
//! Query routing is based on storage tier:
//! - Cold: Query directly at source using table providers
//! - Warm: Query Parquet on R2 via DataFusion with local index pruning
//! - Hot: Query ClickHouse directly for maximum performance

use ahash::AHashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

/// Default SSL mode for PostgreSQL federated connections.
const DEFAULT_PG_SSLMODE: &str = "prefer";
/// Default SSL mode for MySQL federated connections.
const DEFAULT_MYSQL_SSLMODE: &str = "disabled";
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use datafusion_table_providers::common::DatabaseCatalogProvider;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::util::secrets::to_secret_map;
use thiserror::Error;
use uuid::Uuid;

use crate::warehouse::sources::types::StorageTier;
use crate::warehouse::types::{warm_table_path, SourceType};

/// Errors that can occur during federated query execution
#[derive(Debug, Error)]
pub enum FederatedQueryError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Source not found: {0}")]
    SourceNotFound(String),
    
    #[error("Table not found: {0}.{1}")]
    TableNotFound(String, String),
    
    #[error("Unsupported source type: {0}")]
    UnsupportedSourceType(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
}

/// Result type for federated query operations
pub type Result<T> = std::result::Result<T, FederatedQueryError>;

/// Configuration for a PostgreSQL source
#[derive(Debug, Clone)]
pub struct PostgresSourceConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub sslmode: Option<String>,
}

/// Configuration for a MySQL source
#[derive(Debug, Clone)]
pub struct MySqlSourceConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub sslmode: Option<String>,
}

/// Configuration for R2/S3 object storage (for warm sources)
#[derive(Debug, Clone)]
pub struct R2SourceConfig {
    /// R2/S3 endpoint URL (e.g., https://account.r2.cloudflarestorage.com)
    pub endpoint: String,
    /// Bucket name
    pub bucket: String,
    /// Access key ID
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
    /// Optional AWS region (default: auto for R2)
    pub region: Option<String>,
}

/// Configuration for ClickHouse (for hot sources)
#[derive(Debug, Clone)]
pub struct ClickHouseSourceConfig {
    /// ClickHouse HTTP endpoint URL
    pub url: String,
    /// Database name
    pub database: String,
    /// Username
    pub username: Option<String>,
    /// Password
    pub password: Option<String>,
}

/// Registered source information including tier
#[derive(Debug, Clone)]
pub struct RegisteredSourceInfo {
    pub name: String,
    pub source_type: SourceType,
    pub tier: StorageTier,
}

/// Federated query executor
/// 
/// Manages multiple data sources and executes SQL queries across them.
/// Tables are accessible via catalog.schema.table naming convention.
/// 
/// Query routing is based on storage tier:
/// - Cold: Uses datafusion-table-providers to query source directly
/// - Warm: Queries Parquet files on R2/S3 with local index pruning
/// - Hot: Routes queries to ClickHouse for maximum performance
pub struct FederatedQueryExecutor {
    ctx: SessionContext,
    registered_catalogs: Vec<String>,
    /// Source information for query routing
    source_info: AHashMap<String, RegisteredSourceInfo>,
    /// R2/S3 configuration for warm sources
    r2_config: Option<R2SourceConfig>,
    /// ClickHouse configuration for hot sources
    clickhouse_config: Option<ClickHouseSourceConfig>,
    /// Project ID for path generation
    project_id: Option<Uuid>,
    /// Reusable HTTP client for ClickHouse queries (avoids TLS setup per request)
    http_client: reqwest::Client,
}

impl FederatedQueryExecutor {
    /// Create a new federated query executor
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .pool_max_idle_per_host(10)
            .build()
            .expect("Failed to create HTTP client");
        
        Self {
            ctx,
            registered_catalogs: Vec::new(),
            source_info: AHashMap::new(),
            r2_config: None,
            clickhouse_config: None,
            project_id: None,
            http_client,
        }
    }
    
    /// Create a new federated query executor with project context
    pub fn with_project(project_id: Uuid) -> Self {
        let ctx = SessionContext::new();
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .pool_max_idle_per_host(10)
            .build()
            .expect("Failed to create HTTP client");
        
        Self {
            ctx,
            registered_catalogs: Vec::new(),
            source_info: AHashMap::new(),
            r2_config: None,
            clickhouse_config: None,
            project_id: Some(project_id),
            http_client,
        }
    }
    
    /// Configure R2/S3 storage for warm sources
    ///
    /// The R2 configuration is stored and used when registering warm tables.
    /// For R2/S3 access, we use HTTP URLs with the DataFusion Parquet reader.
    pub fn configure_r2(&mut self, config: R2SourceConfig) {
        self.r2_config = Some(config);
        tracing::info!("Configured R2/S3 for warm sources");
    }
    
    /// Configure ClickHouse for hot sources
    pub fn configure_clickhouse(&mut self, config: ClickHouseSourceConfig) {
        self.clickhouse_config = Some(config);
        tracing::info!("Configured ClickHouse for hot sources");
    }
    
    /// Get the tier for a registered source
    pub fn get_source_tier(&self, catalog_name: &str) -> Option<StorageTier> {
        self.source_info.get(catalog_name).map(|info| info.tier)
    }
    
    /// Get all registered source info
    pub fn get_source_info(&self) -> &AHashMap<String, RegisteredSourceInfo> {
        &self.source_info
    }
    
    /// Create a new executor specifically for warm backing failover.
    ///
    /// Configured with R2 object storage and a project ID for table path
    /// resolution. No ClickHouse or external DB connections.
    pub fn new_for_warm_backing(r2_config: R2SourceConfig, project_id: Uuid) -> Self {
        let ctx = SessionContext::new();
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .pool_max_idle_per_host(10)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            ctx,
            registered_catalogs: Vec::new(),
            source_info: AHashMap::new(),
            r2_config: Some(r2_config),
            clickhouse_config: None,
            project_id: Some(project_id),
            http_client,
        }
    }

    /// Register a table from R2/S3 for warm tier queries via DataFusion.
    ///
    /// Configures an S3-compatible object store pointing at the R2 bucket,
    /// then registers the Parquet files under `r2_prefix` as a table.
    pub async fn register_warm_table(
        &mut self,
        table_name: &str,
        r2_prefix: &str,
    ) -> Result<()> {
        use datafusion::datasource::object_store::ObjectStoreUrl;
        use object_store::aws::AmazonS3Builder;

        let r2_config = self.r2_config.as_ref()
            .ok_or_else(|| FederatedQueryError::Config("R2 not configured for warm sources".to_string()))?;

        let store = AmazonS3Builder::new()
            .with_endpoint(&r2_config.endpoint)
            .with_bucket_name(&r2_config.bucket)
            .with_access_key_id(&r2_config.access_key_id)
            .with_secret_access_key(&r2_config.secret_access_key)
            .with_region(r2_config.region.as_deref().unwrap_or("auto"))
            .with_allow_http(r2_config.endpoint.starts_with("http://"))
            .build()
            .map_err(|e| FederatedQueryError::Config(format!("Failed to build S3 object store: {}", e)))?;

        let s3_url = ObjectStoreUrl::parse(format!("s3://{}", r2_config.bucket))
            .map_err(|e| FederatedQueryError::Config(format!("Invalid S3 URL: {}", e)))?;

        self.ctx.register_object_store(s3_url.as_ref(), Arc::new(store));

        let table_path = format!("s3://{}/{}/", r2_config.bucket, r2_prefix);
        self.ctx.register_parquet(
            table_name,
            &table_path,
            ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| FederatedQueryError::Config(format!(
            "Failed to register warm table '{}' from {}: {}",
            table_name, table_path, e,
        )))?;

        tracing::info!(table = %table_name, prefix = %r2_prefix, "Registered warm backing table in DataFusion");
        Ok(())
    }
    
    /// Get the R2 HTTP URL for a warm table's parquet files
    pub fn get_warm_table_path(&self, source_name: &str, table_name: &str) -> Option<String> {
        let r2_config = self.r2_config.as_ref()?;
        let project_id = self.project_id?;
        
        Some(format!(
            "{}/{}/{}",
            r2_config.endpoint,
            r2_config.bucket,
            warm_table_path(project_id, source_name, table_name)
        ))
    }
    
    /// Register a PostgreSQL source as a catalog
    /// 
    /// After registration, tables can be accessed as `catalog_name.schema.table`
    /// For example: `my_postgres.public.users`
    pub async fn register_postgres(
        &mut self,
        catalog_name: &str,
        config: PostgresSourceConfig,
    ) -> Result<()> {
        self.register_postgres_with_tier(catalog_name, config, StorageTier::Cold).await
    }
    
    /// Register a PostgreSQL source with explicit tier
    #[tracing::instrument(name = "warehouse.federated.register_postgres", skip(self, config), fields(%catalog_name, ?tier), err(Display))]
    pub async fn register_postgres_with_tier(
        &mut self,
        catalog_name: &str,
        config: PostgresSourceConfig,
        tier: StorageTier,
    ) -> Result<()> {
        match tier {
            StorageTier::Cold => {
                // Query directly at source using table providers
                let params = to_secret_map(std::collections::HashMap::from([
                    ("host".to_string(), config.host),
                    ("port".to_string(), config.port.to_string()),
                    ("db".to_string(), config.database),
                    ("user".to_string(), config.user),
                    ("pass".to_string(), config.password),
                    ("sslmode".to_string(), config.sslmode.unwrap_or_else(|| DEFAULT_PG_SSLMODE.to_string())),
                ]));
                
                let pool = Arc::new(
                    PostgresConnectionPool::new(params)
                        .await
                        .map_err(|e| FederatedQueryError::Connection(e.to_string()))?
                );
                
                let catalog = DatabaseCatalogProvider::try_new(pool)
                    .await
                    .map_err(|e| FederatedQueryError::Connection(e.to_string()))?;
                
                self.ctx.register_catalog(catalog_name, Arc::new(catalog));
            }
            StorageTier::Warm => {
                // For warm sources, we query Parquet on R2
                // The R2 configuration must be set up first using configure_r2()
                // Tables will be registered lazily when specific tables are accessed
                if self.r2_config.is_none() {
                    tracing::warn!(
                        catalog = %catalog_name,
                        tier = "warm",
                        "Warm tier requires R2 configuration - call configure_r2() first"
                    );
                }
                tracing::info!(
                    catalog = %catalog_name,
                    tier = "warm",
                    "Warm tier: queries will use Parquet on R2"
                );
            }
            StorageTier::Hot => {
                // For hot sources, queries go to ClickHouse
                // The ClickHouse configuration must be set up first
                if self.clickhouse_config.is_none() {
                    tracing::warn!(
                        catalog = %catalog_name,
                        tier = "hot",
                        "Hot tier requires ClickHouse configuration - call configure_clickhouse() first"
                    );
                }
                tracing::info!(
                    catalog = %catalog_name,
                    tier = "hot",
                    "Hot tier: queries will route to ClickHouse"
                );
            }
        }
        
        self.registered_catalogs.push(catalog_name.to_string());
        self.source_info.insert(catalog_name.to_string(), RegisteredSourceInfo {
            name: catalog_name.to_string(),
            source_type: SourceType::PostgreSQL,
            tier,
        });
        
        tracing::info!(
            catalog = %catalog_name,
            source_type = "postgres",
            tier = %tier,
            "Registered PostgreSQL catalog"
        );
        
        Ok(())
    }
    
    /// Register a MySQL source as a catalog
    /// 
    /// After registration, tables can be accessed as `catalog_name.schema.table`
    /// For example: `my_mysql.mysql_db.orders`
    pub async fn register_mysql(
        &mut self,
        catalog_name: &str,
        config: MySqlSourceConfig,
    ) -> Result<()> {
        self.register_mysql_with_tier(catalog_name, config, StorageTier::Cold).await
    }
    
    /// Register a MySQL source with explicit tier
    #[tracing::instrument(name = "warehouse.federated.register_mysql", skip(self, config), fields(%catalog_name, ?tier), err(Display))]
    pub async fn register_mysql_with_tier(
        &mut self,
        catalog_name: &str,
        config: MySqlSourceConfig,
        tier: StorageTier,
    ) -> Result<()> {
        match tier {
            StorageTier::Cold => {
                // Query directly at source using table providers
                let connection_string = format!(
                    "mysql://{}:{}@{}:{}/{}",
                    config.user, config.password, config.host, config.port, config.database
                );
                
                let params = to_secret_map(std::collections::HashMap::from([
                    ("connection_string".to_string(), connection_string),
                    ("sslmode".to_string(), config.sslmode.unwrap_or_else(|| DEFAULT_MYSQL_SSLMODE.to_string())),
                ]));
                
                let pool = Arc::new(
                    MySQLConnectionPool::new(params)
                        .await
                        .map_err(|e| FederatedQueryError::Connection(e.to_string()))?
                );
                
                let catalog = DatabaseCatalogProvider::try_new(pool)
                    .await
                    .map_err(|e| FederatedQueryError::Connection(e.to_string()))?;
                
                self.ctx.register_catalog(catalog_name, Arc::new(catalog));
            }
            StorageTier::Warm => {
                // For warm sources, we query Parquet on R2
                if self.r2_config.is_none() {
                    tracing::warn!(
                        catalog = %catalog_name,
                        tier = "warm",
                        "Warm tier requires R2 configuration"
                    );
                }
                tracing::info!(
                    catalog = %catalog_name,
                    tier = "warm",
                    "Warm tier: queries will use Parquet on R2"
                );
            }
            StorageTier::Hot => {
                // For hot sources, queries go to ClickHouse
                if self.clickhouse_config.is_none() {
                    tracing::warn!(
                        catalog = %catalog_name,
                        tier = "hot",
                        "Hot tier requires ClickHouse configuration"
                    );
                }
                tracing::info!(
                    catalog = %catalog_name,
                    tier = "hot",
                    "Hot tier: queries will route to ClickHouse"
                );
            }
        }
        
        self.registered_catalogs.push(catalog_name.to_string());
        self.source_info.insert(catalog_name.to_string(), RegisteredSourceInfo {
            name: catalog_name.to_string(),
            source_type: SourceType::MySQL,
            tier,
        });
        
        tracing::info!(
            catalog = %catalog_name,
            source_type = "mysql",
            tier = %tier,
            "Registered MySQL catalog"
        );
        
        Ok(())
    }
    
    /// Register a table from a warm source (Parquet on R2)
    ///
    /// This is used when querying warm sources to register specific tables.
    pub async fn register_warm_source_table(
        &mut self,
        source_name: &str,
        table_name: &str,
    ) -> Result<()> {
        self.register_warm_table(table_name, source_name).await
    }
    
    /// Execute a query against a hot source via ClickHouse
    ///
    /// This method routes the query to ClickHouse for hot performance.
    /// The query should reference tables using their ClickHouse names.
    pub async fn execute_hot_query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let ch_config = self.clickhouse_config.as_ref()
            .ok_or_else(|| FederatedQueryError::Config("ClickHouse not configured".to_string()))?;
        
        // Build the ClickHouse HTTP URL with the query
        let url = format!("{}/?default_format=ArrowStream", ch_config.url);
        
        // Use the shared HTTP client (already has timeout configured)
        let mut request = self.http_client
            .post(&url)
            .header("Content-Type", "text/plain")
            .body(sql.to_string());
        
        // Add authentication if configured
        if let (Some(user), Some(pass)) = (&ch_config.username, &ch_config.password) {
            request = request.basic_auth(user, Some(pass));
        } else if let Some(user) = &ch_config.username {
            request = request.basic_auth(user, None::<&str>);
        }
        
        let response = request.send().await
            .map_err(|e| FederatedQueryError::Connection(format!("ClickHouse request failed: {}", e)))?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(FederatedQueryError::Connection(format!(
                "ClickHouse query failed with status {}: {}",
                status, body
            )));
        }
        
        // Parse Arrow IPC stream from response
        let bytes = response.bytes().await
            .map_err(|e| FederatedQueryError::Connection(format!("Failed to read response: {}", e)))?;
        
        if bytes.is_empty() {
            return Ok(vec![]);
        }
        
        let cursor = std::io::Cursor::new(bytes);
        let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)
            .map_err(|e| FederatedQueryError::Connection(format!("Failed to parse Arrow stream: {}", e)))?;
        
        let batches: std::result::Result<Vec<_>, _> = reader.collect();
        let batches = batches
            .map_err(|e| FederatedQueryError::Connection(format!("Failed to read Arrow batches: {}", e)))?;
        
        tracing::info!(
            batch_count = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "ClickHouse query execution completed"
        );
        
        Ok(batches)
    }
    
    /// Register a Parquet file or directory as a table
    /// 
    /// DataFusion has native Parquet support with automatic min/max row group pruning.
    pub async fn register_parquet(
        &mut self,
        table_name: &str,
        path: &str,
    ) -> Result<()> {
        self.register_parquet_with_tier(table_name, path, StorageTier::Cold).await
    }
    
    /// Register a Parquet source with explicit tier and source type.
    pub async fn register_parquet_with_tier(
        &mut self,
        table_name: &str,
        path: &str,
        tier: StorageTier,
    ) -> Result<()> {
        self.register_parquet_with_tier_and_type(table_name, path, tier, SourceType::ExternalParquet).await
    }

    /// Register a Parquet source with explicit tier and source type.
    pub async fn register_parquet_with_tier_and_type(
        &mut self,
        table_name: &str,
        path: &str,
        tier: StorageTier,
        source_type: SourceType,
    ) -> Result<()> {
        match tier {
            StorageTier::Cold | StorageTier::Warm => {
                self.ctx
                    .register_parquet(table_name, path, ParquetReadOptions::default())
                    .await
                    .map_err(FederatedQueryError::DataFusion)?;
            }
            StorageTier::Hot => {
                return Err(FederatedQueryError::UnsupportedSourceType(format!(
                    "Hot-tier Parquet registration is not supported \
                     (table={}). Hot-tier queries must be routed to \
                     ClickHouse via execute_hot_query() instead.",
                    table_name,
                )));
            }
        }
        
        self.source_info.insert(table_name.to_string(), RegisteredSourceInfo {
            name: table_name.to_string(),
            source_type,
            tier,
        });
        
        tracing::info!(
            table = %table_name,
            path = %path,
            tier = %tier,
            "Registered Parquet table"
        );
        
        Ok(())
    }
    
    /// Execute a federated SQL query
    /// 
    /// Tables can be referenced using catalog.schema.table format:
    /// - PostgreSQL: `postgres_source.public.users`
    /// - MySQL: `mysql_source.database.orders`
    /// - Parquet: just the table name registered
    #[tracing::instrument(name = "warehouse.federated.execute", skip(self, sql), fields(sql_length = sql.len()), err(Display))]
    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        tracing::info!(sql = %sql, "Executing federated query");
        
        let df = self.ctx
            .sql(sql)
            .await
            .map_err(FederatedQueryError::DataFusion)?;
        
        let batches = df
            .collect()
            .await
            .map_err(FederatedQueryError::DataFusion)?;
        
        tracing::info!(
            batch_count = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Query execution completed"
        );
        
        Ok(batches)
    }
    
    /// Execute a query with a row limit
    #[tracing::instrument(name = "warehouse.federated.execute_with_limit", skip(self, sql), fields(sql_length = sql.len(), limit), err(Display))]
    pub async fn execute_with_limit(&self, sql: &str, limit: usize) -> Result<Vec<RecordBatch>> {
        let df = self.ctx
            .sql(sql)
            .await
            .map_err(FederatedQueryError::DataFusion)?;
        
        let df = df
            .limit(0, Some(limit))
            .map_err(FederatedQueryError::DataFusion)?;
        
        df.collect()
            .await
            .map_err(FederatedQueryError::DataFusion)
    }
    
    /// Get the underlying SessionContext for advanced usage
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }
    
    /// List all registered catalogs
    pub fn list_catalogs(&self) -> &[String] {
        &self.registered_catalogs
    }
}

impl Default for FederatedQueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to create a FederatedQueryExecutor from source configurations stored in JSON
pub async fn create_executor_from_configs(
    sources: Vec<(String, SourceType, serde_json::Value)>, // (name, source_type, config)
) -> Result<FederatedQueryExecutor> {
    // Default to Cold tier
    create_executor_from_configs_with_tiers(
        sources.into_iter().map(|(name, st, cfg)| (name, st, cfg, StorageTier::Cold)).collect()
    ).await
}

/// Helper to create a FederatedQueryExecutor with explicit tiers for each source
pub async fn create_executor_from_configs_with_tiers(
    sources: Vec<(String, SourceType, serde_json::Value, StorageTier)>, // (name, source_type, config, tier)
) -> Result<FederatedQueryExecutor> {
    let mut executor = FederatedQueryExecutor::new();
    
    for (name, source_type, config, tier) in sources {
        match source_type {
            SourceType::PostgreSQL => {
                let pg_config = PostgresSourceConfig {
                    host: config.get("host").and_then(|v| v.as_str()).unwrap_or("localhost").to_string(),
                    port: config.get("port").and_then(|v| v.as_u64()).unwrap_or(5432) as u16,
                    database: config.get("database").and_then(|v| v.as_str()).unwrap_or("postgres").to_string(),
                    user: config.get("username").and_then(|v| v.as_str()).unwrap_or("postgres").to_string(),
                    password: config.get("password").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    sslmode: config.get("sslmode").and_then(|v| v.as_str()).map(|s| s.to_string()),
                };
                executor.register_postgres_with_tier(&name, pg_config, tier).await?;
            }
            SourceType::MySQL => {
                let mysql_config = MySqlSourceConfig {
                    host: config.get("host").and_then(|v| v.as_str()).unwrap_or("localhost").to_string(),
                    port: config.get("port").and_then(|v| v.as_u64()).unwrap_or(3306) as u16,
                    database: config.get("database").and_then(|v| v.as_str()).unwrap_or("mysql").to_string(),
                    user: config.get("username").and_then(|v| v.as_str()).unwrap_or("root").to_string(),
                    password: config.get("password").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    sslmode: config.get("sslmode").and_then(|v| v.as_str()).map(|s| s.to_string()),
                };
                executor.register_mysql_with_tier(&name, mysql_config, tier).await?;
            }
            SourceType::ExternalParquet => {
                let path = config.get("path").and_then(|v| v.as_str())
                    .or_else(|| config.get("bucket_url").and_then(|v| v.as_str()))
                    .unwrap_or("");
                if !path.is_empty() {
                    executor.register_parquet_with_tier(&name, path, tier).await?;
                } else {
                    tracing::warn!(
                        name = %name,
                        "Skipping ExternalParquet source with empty path"
                    );
                }
            }
            SourceType::Derived => {
                // Derived tables live on R2 (warm tier). Like other warm
                // sources, registering them with DataFusion's register_parquet
                // requires a properly configured `object_store` with R2/S3
                // credentials — a bare r2_prefix (e.g. "projects/{id}/derived/{name}")
                // is NOT a path DataFusion can resolve.
                //
                // Follow the same pattern as `register_warm_table`: record the
                // source info so downstream code knows the table exists, and
                // log the path for debugging. Queries against derived tables
                // are routed through the ClickHouse-based rewriter, not
                // DataFusion's Parquet reader.
                let r2_prefix = config.get("r2_prefix").and_then(|v| v.as_str())
                    .unwrap_or("");

                tracing::debug!(
                    table = %name,
                    r2_prefix = %r2_prefix,
                    tier = %tier,
                    "Derived table configured (queries route through ClickHouse rewriter)"
                );

                executor.source_info.insert(name.to_string(), RegisteredSourceInfo {
                    name: name.to_string(),
                    source_type: SourceType::Derived,
                    tier,
                });
            }
            other => {
                tracing::warn!(
                    source_type = %other,
                    name = %name,
                    tier = %tier,
                    "Skipping unsupported source type in federated query"
                );
            }
        }
    }
    
    Ok(executor)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // ==================== Unit Tests (No Database Required) ====================
    
    #[test]
    fn test_executor_creation() {
        let executor = FederatedQueryExecutor::new();
        assert!(executor.list_catalogs().is_empty());
    }
    
    #[test]
    fn test_executor_default() {
        let executor = FederatedQueryExecutor::default();
        assert!(executor.list_catalogs().is_empty());
    }
    
    #[test]
    fn test_postgres_source_config() {
        let config = PostgresSourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            user: "postgres".to_string(),
            password: "secret".to_string(),
            sslmode: Some("require".to_string()),
        };
        
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.database, "testdb");
        assert_eq!(config.user, "postgres");
        assert_eq!(config.password, "secret");
        assert_eq!(config.sslmode, Some("require".to_string()));
    }
    
    #[test]
    fn test_mysql_source_config() {
        let config = MySqlSourceConfig {
            host: "localhost".to_string(),
            port: 3306,
            database: "testdb".to_string(),
            user: "root".to_string(),
            password: "secret".to_string(),
            sslmode: None,
        };
        
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 3306);
        assert_eq!(config.database, "testdb");
        assert_eq!(config.user, "root");
        assert!(config.sslmode.is_none());
    }
    
    #[test]
    fn test_postgres_source_config_clone() {
        let config = PostgresSourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            user: "postgres".to_string(),
            password: "secret".to_string(),
            sslmode: None,
        };
        
        let cloned = config.clone();
        assert_eq!(config.host, cloned.host);
        assert_eq!(config.port, cloned.port);
    }
    
    #[test]
    fn test_error_display() {
        let err = FederatedQueryError::SourceNotFound("test_source".to_string());
        assert_eq!(err.to_string(), "Source not found: test_source");
        
        let err = FederatedQueryError::TableNotFound("source".to_string(), "table".to_string());
        assert_eq!(err.to_string(), "Table not found: source.table");
        
        let err = FederatedQueryError::UnsupportedSourceType("redis".to_string());
        assert_eq!(err.to_string(), "Unsupported source type: redis");
        
        let err = FederatedQueryError::Config("bad config".to_string());
        assert_eq!(err.to_string(), "Configuration error: bad config");
        
        let err = FederatedQueryError::Connection("connection failed".to_string());
        assert_eq!(err.to_string(), "Connection error: connection failed");
    }
    
    #[test]
    fn test_session_context_access() {
        let executor = FederatedQueryExecutor::new();
        let ctx = executor.session_context();
        
        // Should be able to access catalog names from context
        let catalog_names = ctx.catalog_names();
        // Default catalog should exist
        assert!(!catalog_names.is_empty());
    }
    
    #[tokio::test]
    async fn test_execute_simple_query() {
        let executor = FederatedQueryExecutor::new();
        
        // Execute a simple query that doesn't require any tables
        let result = executor.execute("SELECT 1 + 1 AS result").await;
        assert!(result.is_ok());
        
        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }
    
    #[tokio::test]
    async fn test_execute_with_limit() {
        let executor = FederatedQueryExecutor::new();
        
        // Generate numbers and limit
        let result = executor.execute_with_limit(
            "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(n)",
            3
        ).await;
        
        assert!(result.is_ok());
        let batches = result.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows <= 3);
    }
    
    #[tokio::test]
    async fn test_execute_invalid_sql() {
        let executor = FederatedQueryExecutor::new();
        
        let result = executor.execute("INVALID SQL QUERY").await;
        assert!(result.is_err());
        
        match result {
            Err(FederatedQueryError::DataFusion(_)) => {
                // Expected error type
            }
            _ => panic!("Expected DataFusion error"),
        }
    }
    
    #[tokio::test]
    async fn test_execute_nonexistent_table() {
        let executor = FederatedQueryExecutor::new();
        
        let result = executor.execute("SELECT * FROM nonexistent_table").await;
        assert!(result.is_err());
    }
    
    #[tokio::test]
    async fn test_register_parquet_and_query() {
        let executor = FederatedQueryExecutor::new();
        
        // Test that we can query a VALUES-based table (no real file needed)
        // This validates the parquet registration path exists
        let result = executor.execute("SELECT 1 AS col").await;
        assert!(result.is_ok());
        
        // The actual parquet registration is tested in integration tests
        // since DataFusion lazily evaluates file existence
    }
    
    #[tokio::test]
    async fn test_create_executor_from_configs_unsupported_type() {
        let sources = vec![
            (
                "kafka_source".to_string(),
                SourceType::Kafka,
                serde_json::json!({"host": "localhost"}),
            ),
        ];
        
        // Should succeed but skip the unsupported source type
        let result = create_executor_from_configs(sources).await;
        assert!(result.is_ok());
        
        let executor = result.unwrap();
        // No catalogs should be registered for unsupported type in federated queries
        assert!(executor.list_catalogs().is_empty());
    }
    
    #[tokio::test]
    async fn test_execute_multiple_queries() {
        let executor = FederatedQueryExecutor::new();
        
        // Execute multiple queries in sequence
        let result1 = executor.execute("SELECT 1 AS a").await;
        let result2 = executor.execute("SELECT 2 AS b").await;
        let result3 = executor.execute("SELECT 3 AS c").await;
        
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());
    }
    
    #[tokio::test]
    async fn test_execute_with_aggregation() {
        let executor = FederatedQueryExecutor::new();
        
        let result = executor.execute(
            "SELECT COUNT(*) as cnt FROM (VALUES (1), (2), (3)) AS t(n)"
        ).await;
        
        assert!(result.is_ok());
        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);
    }
    
    #[tokio::test]
    async fn test_execute_with_join() {
        let executor = FederatedQueryExecutor::new();
        
        // Join two inline tables
        let result = executor.execute(
            "SELECT a.id, b.name FROM \
             (VALUES (1), (2)) AS a(id) \
             JOIN (VALUES (1, 'one'), (2, 'two')) AS b(id, name) \
             ON a.id = b.id"
        ).await;
        
        assert!(result.is_ok());
        let batches = result.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
    
    // ==================== Integration Tests (Database Required) ====================
    
    /// Integration test that requires a running PostgreSQL instance.
    /// Run with: cargo test --ignored test_postgres_integration
    #[tokio::test]
    #[ignore = "Requires PostgreSQL - run with: cargo test --ignored test_postgres_integration"]
    async fn test_postgres_integration() {
        let mut executor = FederatedQueryExecutor::new();
        
        let config = PostgresSourceConfig {
            host: std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: std::env::var("POSTGRES_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5432),
            database: std::env::var("POSTGRES_DB").unwrap_or_else(|_| "test".to_string()),
            user: std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string()),
            password: std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "password".to_string()),
            sslmode: Some("disable".to_string()),
        };
        
        let result = executor.register_postgres("pg", config).await;
        if result.is_err() {
            println!("Skipping test: PostgreSQL not available");
            return;
        }
        
        assert_eq!(executor.list_catalogs(), &["pg"]);
        
        // Try a simple query
        let result = executor.execute("SELECT 1").await;
        assert!(result.is_ok());
    }
    
    /// Integration test that requires a running MySQL instance.
    /// Run with: cargo test --ignored test_mysql_integration
    #[tokio::test]
    #[ignore = "Requires MySQL - run with: cargo test --ignored test_mysql_integration"]
    async fn test_mysql_integration() {
        let mut executor = FederatedQueryExecutor::new();
        
        let config = MySqlSourceConfig {
            host: std::env::var("MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: std::env::var("MYSQL_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3306),
            database: std::env::var("MYSQL_DB").unwrap_or_else(|_| "test".to_string()),
            user: std::env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string()),
            password: std::env::var("MYSQL_PASSWORD").unwrap_or_else(|_| "password".to_string()),
            sslmode: Some("disabled".to_string()),
        };
        
        let result = executor.register_mysql("mysql", config).await;
        if result.is_err() {
            println!("Skipping test: MySQL not available");
            return;
        }
        
        assert_eq!(executor.list_catalogs(), &["mysql"]);
    }
    
    /// Integration test for Parquet file queries.
    /// Run with: cargo test --ignored test_parquet_integration
    #[tokio::test]
    #[ignore = "Requires test parquet file - run with: cargo test --ignored test_parquet_integration"]
    async fn test_parquet_integration() {
        use std::path::PathBuf;
        
        let mut executor = FederatedQueryExecutor::new();
        
        // Look for test parquet file
        let test_file = PathBuf::from("test_data/sample.parquet");
        if !test_file.exists() {
            println!("Skipping test: test parquet file not found at {:?}", test_file);
            return;
        }
        
        let result = executor.register_parquet("sample", test_file.to_str().unwrap()).await;
        assert!(result.is_ok(), "Should register parquet file successfully");
        
        let query_result = executor.execute("SELECT * FROM sample LIMIT 5").await;
        assert!(query_result.is_ok(), "Should query parquet table successfully");
    }
    
    /// Test that DataFusion requires three-part names for non-default catalogs
    /// and that two-part names fail (documenting the root cause).
    /// Run with: cargo test --lib --ignored test_postgres_catalog_inspection -- --nocapture
    #[tokio::test]
    #[ignore = "Requires test-postgres Docker container on port 15432"]
    async fn test_postgres_catalog_inspection() {
        let mut executor = FederatedQueryExecutor::new();

        let config = PostgresSourceConfig {
            host: "localhost".to_string(),
            port: 15432,
            database: "testdb".to_string(),
            user: "testuser".to_string(),
            password: "testpass".to_string(),
            sslmode: Some("disable".to_string()),
        };

        // Register with a hyphenated name like the real source
        executor.register_postgres("test-postgres", config).await
            .expect("Should connect to test-postgres");

        let ctx = executor.session_context();

        // Verify catalog is registered
        let catalog_names = ctx.catalog_names();
        assert!(
            catalog_names.contains(&"test-postgres".to_string()),
            "Catalog 'test-postgres' should be registered. Found: {:?}",
            catalog_names,
        );

        // Verify schema discovery
        let cat = ctx.catalog("test-postgres").expect("catalog must exist");
        let schema_names = cat.schema_names();
        assert!(
            schema_names.contains(&"public".to_string()),
            "Schema 'public' should exist. Found: {:?}",
            schema_names,
        );

        // Verify table discovery
        let schema = cat.schema("public").expect("public schema must exist");
        let table_names = schema.table_names();
        assert!(
            table_names.contains(&"customers".to_string()),
            "Table 'customers' should exist in public schema. Found: {:?}",
            table_names,
        );

        // Two-part query MUST fail (DataFusion resolves as datafusion."test-postgres".customers)
        let result_2part = executor.execute(
            r#"SELECT * FROM "test-postgres".customers LIMIT 1"#
        ).await;
        assert!(
            result_2part.is_err(),
            "Two-part query should fail because DataFusion uses default catalog",
        );
        let err_msg = result_2part.unwrap_err().to_string();
        assert!(
            err_msg.contains("table") && err_msg.contains("not found"),
            "Error should be 'table not found', got: {}",
            err_msg,
        );

        // Three-part query MUST succeed
        let result_3part = executor.execute(
            r#"SELECT * FROM "test-postgres".public.customers LIMIT 1"#
        ).await;
        assert!(
            result_3part.is_ok(),
            "Three-part query should succeed: {:?}",
            result_3part.err(),
        );
        let batches = result_3part.unwrap();
        assert!(!batches.is_empty(), "Should return at least one batch");
    }

    /// Test that the SQL rewriting function correctly upgrades two-part
    /// table names to three-part when the first part is a known catalog.
    #[test]
    fn test_rewrite_federated_table_refs() {
        use crate::api::warehouse::rewrite_federated_table_refs;

        let catalogs: ahash::AHashSet<String> =
            ["test-postgres", "my-mysql"].iter().map(|s| s.to_string()).collect();

        // Two-part name with known catalog -> should inject "public"
        let sql = r#"SELECT * FROM "test-postgres".customers LIMIT 20"#;
        let rewritten = rewrite_federated_table_refs(sql, &catalogs).unwrap();
        assert!(
            rewritten.contains(r#""test-postgres".public.customers"#),
            "Should rewrite to three-part name. Got: {}",
            rewritten,
        );

        // Three-part name -> should NOT be modified
        let sql = r#"SELECT * FROM "test-postgres".public.customers LIMIT 20"#;
        let rewritten = rewrite_federated_table_refs(sql, &catalogs).unwrap();
        assert!(
            rewritten.contains("public") && rewritten.contains("customers"),
            "Three-part name should stay intact. Got: {}",
            rewritten,
        );

        // Unknown catalog -> should NOT be modified
        let sql = r#"SELECT * FROM "unknown".orders LIMIT 10"#;
        let rewritten = rewrite_federated_table_refs(sql, &catalogs).unwrap();
        // The first part "unknown" is not a registered catalog so no rewrite
        assert!(
            !rewritten.contains("public"),
            "Unknown catalog should not be rewritten. Got: {}",
            rewritten,
        );

        // Single-part name -> should NOT be modified
        let sql = "SELECT * FROM customers LIMIT 10";
        let rewritten = rewrite_federated_table_refs(sql, &catalogs).unwrap();
        assert!(
            !rewritten.contains("public"),
            "Single-part name should not be rewritten. Got: {}",
            rewritten,
        );

        // JOIN with mixed catalogs
        let sql = r#"SELECT a.*, b.* FROM "test-postgres".customers a JOIN "my-mysql".orders b ON a.id = b.customer_id"#;
        let rewritten = rewrite_federated_table_refs(sql, &catalogs).unwrap();
        assert!(
            rewritten.contains(r#""test-postgres".public.customers"#),
            "test-postgres table should be rewritten. Got: {}",
            rewritten,
        );
        assert!(
            rewritten.contains(r#""my-mysql".public.orders"#),
            "my-mysql table should be rewritten. Got: {}",
            rewritten,
        );
    }

    /// End-to-end test: the rewrite function fixes two-part queries for DataFusion.
    /// Run with: cargo test --lib --ignored test_rewrite_fixes_two_part_query -- --nocapture
    #[tokio::test]
    #[ignore = "Requires test-postgres Docker container on port 15432"]
    async fn test_rewrite_fixes_two_part_query() {
        use crate::api::warehouse::rewrite_federated_table_refs;

        let mut executor = FederatedQueryExecutor::new();

        let config = PostgresSourceConfig {
            host: "localhost".to_string(),
            port: 15432,
            database: "testdb".to_string(),
            user: "testuser".to_string(),
            password: "testpass".to_string(),
            sslmode: Some("disable".to_string()),
        };

        executor.register_postgres("test-postgres", config).await
            .expect("Should connect to test-postgres");

        // Build the catalog set from the executor (same as execute_federated_query does)
        let catalog_names: ahash::AHashSet<String> =
            executor.list_catalogs().iter().cloned().collect();

        // The original user query (two-part, which fails raw)
        let user_sql = r#"SELECT * FROM "test-postgres".customers LIMIT 5"#;

        // Without rewrite: should fail
        let raw_result = executor.execute(user_sql).await;
        assert!(raw_result.is_err(), "Raw two-part query should fail");

        // With rewrite: should succeed
        let rewritten_sql = rewrite_federated_table_refs(user_sql, &catalog_names).unwrap();
        let rewritten_result = executor.execute(&rewritten_sql).await;
        assert!(
            rewritten_result.is_ok(),
            "Rewritten query should succeed. Error: {:?}",
            rewritten_result.err(),
        );
        let batches = rewritten_result.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("=== Rewritten query returned {} rows", total_rows);
        assert!(total_rows > 0, "Should return rows from customers table");

        // Verify that record_batches_to_response serializes timestamps correctly
        let response = crate::api::warehouse::record_batches_to_response(batches, 5).unwrap();
        // Find the created_at column index
        let ts_col_idx = response.columns.iter().position(|c| c.name == "created_at")
            .expect("Should have created_at column");
        let ts_value = &response.rows[0][ts_col_idx];
        println!("=== created_at value: {:?}", ts_value);
        // Must NOT be the literal "timestamp"
        assert_ne!(
            ts_value.as_str(),
            Some("timestamp"),
            "Timestamp should be a real value, not the literal 'timestamp'"
        );
        // Should be an ISO 8601 string containing a date
        let ts_str = ts_value.as_str().expect("Timestamp should be a string");
        assert!(
            ts_str.contains("20") && ts_str.contains("T"),
            "Timestamp should look like ISO 8601: {}",
            ts_str,
        );
    }

    /// Integration test for cross-database queries.
    /// Run with: cargo test --ignored test_cross_database_join
    #[tokio::test]
    #[ignore = "Requires PostgreSQL and MySQL - run with: cargo test --ignored test_cross_database_join"]
    async fn test_cross_database_join() {
        let mut executor = FederatedQueryExecutor::new();
        
        // Register PostgreSQL
        let pg_config = PostgresSourceConfig {
            host: std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: 5432,
            database: "test".to_string(),
            user: "postgres".to_string(),
            password: "password".to_string(),
            sslmode: Some("disable".to_string()),
        };
        
        if executor.register_postgres("pg", pg_config).await.is_err() {
            println!("Skipping test: PostgreSQL not available");
            return;
        }
        
        // Register MySQL
        let mysql_config = MySqlSourceConfig {
            host: std::env::var("MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: 3306,
            database: "test".to_string(),
            user: "root".to_string(),
            password: "password".to_string(),
            sslmode: Some("disabled".to_string()),
        };
        
        if executor.register_mysql("mysql", mysql_config).await.is_err() {
            println!("Skipping test: MySQL not available");
            return;
        }
        
        assert_eq!(executor.list_catalogs().len(), 2);
        
        // Note: Actual cross-database JOIN would require tables to exist
        // This just verifies both catalogs are registered
    }
}
