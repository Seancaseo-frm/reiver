//! Connector Registry Service
//!
//! Manages runtime connector instances for cold (federated) data sources.
//! This service bridges the database source configuration with the runtime
//! ConnectorRegistry used by the FederationExecutor.
//!
//! # Responsibilities
//!
//! - Load all cold sources from the database on startup
//! - Instantiate appropriate connectors for each source type
//! - Handle dynamic registration when new sources are added
//! - Handle cleanup when sources are removed
//!
//! # Architecture
//!
//! ```text
//! warehouse_sources (DB)
//!         │
//!         ▼
//! ConnectorRegistryService
//!         │
//!         ▼  (instantiates connectors)
//! ConnectorRegistry
//!         │
//!         ▼  (used by)
//! FederationExecutor
//! ```

use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::warehouse::connectors::{
    Connector, ConnectorError,
    MongoDBConfig, MongoDBConnector,
    MySqlConfig, MySqlConnector,
    RedshiftConfig, RedshiftConnector, RedshiftSslMode,
    SnowflakeConfig, SnowflakeConnector,
    SqlServerConfig, SqlServerConnector,
};
use crate::warehouse::connectors::databases::{
    BigQueryConfig, BigQueryConnector,
    SQLiteConfig, SQLiteConnector,
};
use crate::warehouse::connectors::postgres::{PostgresConfig, PostgresConnector};
use crate::warehouse::query::ConnectorRegistry;
use crate::warehouse::sources::{DataSourceRegistry, RegisteredSource};
use crate::warehouse::types::SourceType;

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during connector registry service operations.
#[derive(Debug, Error)]
pub enum RegistryServiceError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Connector initialization failed for source '{source_name}': {message}")]
    ConnectorInit {
        source_name: String,
        message: String,
    },

    #[error("Source not found: {0}")]
    SourceNotFound(Uuid),

    #[error("Unsupported source type for cold queries: {0}")]
    UnsupportedSourceType(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

impl From<crate::warehouse::sources::registry::RegistryError> for RegistryServiceError {
    fn from(e: crate::warehouse::sources::registry::RegistryError) -> Self {
        Self::Database(e.to_string())
    }
}

impl From<ConnectorError> for RegistryServiceError {
    fn from(e: ConnectorError) -> Self {
        Self::ConnectorInit {
            source_name: "unknown".to_string(),
            message: e.to_string(),
        }
    }
}

/// Result type for registry service operations.
pub type RegistryServiceResult<T> = Result<T, RegistryServiceError>;

// ============================================================================
// Connector Registry Service
// ============================================================================

/// Service for managing runtime connectors for cold (federated) data sources.
///
/// This service is responsible for:
/// - Loading all cold sources from the database on startup
/// - Creating connector instances for each source
/// - Maintaining the runtime ConnectorRegistry
/// - Handling dynamic source additions/removals
pub struct ConnectorRegistryService {
    /// The runtime connector registry used by FederationExecutor.
    registry: Arc<ConnectorRegistry>,
    /// Data source registry for database operations.
    source_registry: Arc<DataSourceRegistry>,
}

impl ConnectorRegistryService {
    /// Create a new connector registry service.
    pub fn new(
        registry: Arc<ConnectorRegistry>,
        source_registry: Arc<DataSourceRegistry>,
    ) -> Self {
        Self {
            registry,
            source_registry,
        }
    }

    /// Get the underlying connector registry.
    pub fn registry(&self) -> Arc<ConnectorRegistry> {
        Arc::clone(&self.registry)
    }

    /// Initialize the service by loading all cold sources from the database.
    ///
    /// This should be called during application startup.
    pub async fn initialize(&self) -> RegistryServiceResult<InitializeResult> {
        info!("Initializing connector registry service");

        let sources = self.source_registry.list_all_cold().await?;
        let total = sources.len();
        
        let mut loaded = 0;
        let mut failed = 0;
        let mut errors = Vec::new();

        for source in sources {
            match self.register_source_internal(&source).await {
                Ok(()) => {
                    loaded += 1;
                    debug!(
                        source_id = %source.id,
                        source_name = %source.name,
                        source_type = ?source.source_type,
                        "Registered connector for cold source"
                    );
                }
                Err(e) => {
                    failed += 1;
                    let error_msg = format!(
                        "Failed to register connector for source '{}' ({}): {}",
                        source.name, source.id, e
                    );
                    error!("{}", error_msg);
                    errors.push(error_msg);
                }
            }
        }

        info!(
            total = total,
            loaded = loaded,
            failed = failed,
            "Connector registry initialization complete"
        );

        Ok(InitializeResult {
            total,
            loaded,
            failed,
            errors,
        })
    }

    /// Register a connector for a source.
    ///
    /// This is called when a new cold source is added via the API.
    pub async fn register_source(&self, source: &RegisteredSource) -> RegistryServiceResult<()> {
        if !source.tier.is_cold() {
            return Err(RegistryServiceError::InvalidConfig(format!(
                "Cannot register sync source '{}' as cold connector",
                source.name
            )));
        }

        self.register_source_internal(source).await
    }

    /// Internal method to register a source.
    async fn register_source_internal(&self, source: &RegisteredSource) -> RegistryServiceResult<()> {
        let connector = self.create_connector(source).await?;
        
        // Use project_id + source_name as the key to avoid conflicts
        let registry_key = format!("{}:{}", source.project_id, source.name);
        
        self.registry.register(registry_key, connector);
        
        Ok(())
    }

    /// Create a connector instance for a source.
    async fn create_connector(
        &self,
        source: &RegisteredSource,
    ) -> RegistryServiceResult<Arc<dyn Connector>> {
        match source.source_type {
            SourceType::MongoDB => {
                let mongo_config = self.parse_mongodb_config(source)?;
                let connector = MongoDBConnector::new(mongo_config);
                Ok(Arc::new(connector))
            }
            
            SourceType::BigQuery => {
                let bq_config = self.parse_bigquery_config(source)?;
                let connector = BigQueryConnector::new(bq_config);
                Ok(Arc::new(connector))
            }
            
            SourceType::SqlServer => {
                let ss_config = self.parse_sqlserver_config(source)?;
                let connector = SqlServerConnector::new(ss_config);
                Ok(Arc::new(connector))
            }
            
            SourceType::SQLite => {
                let sqlite_config = self.parse_sqlite_config(source)?;
                let connector = SQLiteConnector::new(sqlite_config);
                Ok(Arc::new(connector))
            }
            
            SourceType::PostgreSQL => {
                let pg_config = self.parse_postgres_config(source)?;
                let connector = PostgresConnector::new(pg_config);
                Ok(Arc::new(connector))
            }
            
            SourceType::MySQL => {
                let mysql_config = self.parse_mysql_config(source)?;
                let connector = MySqlConnector::new(mysql_config);
                Ok(Arc::new(connector))
            }
            
            SourceType::Redshift => {
                let redshift_config = self.parse_redshift_config(source)?;
                let connector = RedshiftConnector::new(redshift_config);
                Ok(Arc::new(connector))
            }
            
            SourceType::Snowflake => {
                let snowflake_config = self.parse_snowflake_config(source)?;
                let connector = SnowflakeConnector::new(snowflake_config);
                Ok(Arc::new(connector))
            }
            
            _ => Err(RegistryServiceError::UnsupportedSourceType(
                format!("{:?}", source.source_type)
            )),
        }
    }

    /// Parse MongoDB configuration from a source.
    fn parse_mongodb_config(&self, source: &RegisteredSource) -> RegistryServiceResult<MongoDBConfig> {
        match &source.backend {
            crate::warehouse::sources::SourceBackend::ExternalDatabase {
                host, port, database, username, password, ..
            } => {
                let connection_string = format!(
                    "mongodb://{}:{}@{}:{}/{}",
                    urlencoding::encode(username),
                    urlencoding::encode(password.as_deref().unwrap_or("")),
                    host,
                    port,
                    database
                );
                Ok(MongoDBConfig::new(connection_string, database.clone()))
            }
            crate::warehouse::sources::SourceBackend::ExternalApi { config_json, .. } => {
                #[derive(serde::Deserialize)]
                struct MongoConfigJson {
                    connection_string: String,
                    database: String,
                }
                
                let parsed: MongoConfigJson = serde_json::from_str(config_json).map_err(|e| {
                    RegistryServiceError::InvalidConfig(format!(
                        "Invalid MongoDB config JSON: {}",
                        e
                    ))
                })?;
                Ok(MongoDBConfig::new(parsed.connection_string, parsed.database))
            }
            _ => Err(RegistryServiceError::InvalidConfig(
                "MongoDB source requires ExternalDatabase or ExternalApi backend".to_string()
            )),
        }
    }

    /// Parse BigQuery configuration from a source.
    fn parse_bigquery_config(&self, source: &RegisteredSource) -> RegistryServiceResult<BigQueryConfig> {
        match &source.backend {
            crate::warehouse::sources::SourceBackend::ExternalApi { config_json, .. } => {
                #[derive(serde::Deserialize)]
                struct BqConfigJson {
                    project_id: String,
                    dataset: String,
                    #[serde(default)]
                    credentials_json: Option<String>,
                    #[serde(default)]
                    credentials_path: Option<String>,
                }
                
                let parsed: BqConfigJson = serde_json::from_str(config_json).map_err(|e| {
                    RegistryServiceError::InvalidConfig(format!(
                        "Invalid BigQuery config JSON: {}",
                        e
                    ))
                })?;
                let mut config = BigQueryConfig::new(parsed.project_id, parsed.dataset);
                if let Some(creds_json) = parsed.credentials_json {
                    config = config.with_credentials_json(creds_json);
                }
                if let Some(creds_path) = parsed.credentials_path {
                    config = config.with_credentials_path(creds_path);
                }
                Ok(config)
            }
            crate::warehouse::sources::SourceBackend::ExternalDatabase {
                database, ..
            } => {
                // Minimal config - would need credentials from secure storage
                Ok(BigQueryConfig::new(database.clone(), "default"))
            }
            _ => Err(RegistryServiceError::InvalidConfig(
                "BigQuery source requires ExternalApi or ExternalDatabase backend".to_string()
            )),
        }
    }

    /// Parse SQL Server configuration from a source.
    fn parse_sqlserver_config(&self, source: &RegisteredSource) -> RegistryServiceResult<SqlServerConfig> {
        match &source.backend {
            crate::warehouse::sources::SourceBackend::ExternalDatabase {
                host, port, database, username, password, ..
            } => {
                let config = SqlServerConfig::new(
                    host.clone(),
                    database.clone(),
                    username.clone(),
                    password.clone().unwrap_or_default(),
                ).with_port(*port)
                 .with_trust_server_certificate(true);
                Ok(config)
            }
            crate::warehouse::sources::SourceBackend::ExternalApi { config_json, .. } => {
                #[derive(serde::Deserialize)]
                struct SqlServerConfigJson {
                    host: String,
                    database: String,
                    username: String,
                    password: String,
                    #[serde(default = "default_port")]
                    port: u16,
                }
                fn default_port() -> u16 { 1433 }
                
                let parsed: SqlServerConfigJson = serde_json::from_str(config_json).map_err(|e| {
                    RegistryServiceError::InvalidConfig(format!(
                        "Invalid SQL Server config JSON: {}",
                        e
                    ))
                })?;
                Ok(SqlServerConfig::new(
                    parsed.host,
                    parsed.database,
                    parsed.username,
                    parsed.password,
                ).with_port(parsed.port))
            }
            _ => Err(RegistryServiceError::InvalidConfig(
                "SQL Server source requires ExternalDatabase or ExternalApi backend".to_string()
            )),
        }
    }

    /// Parse SQLite configuration from a source.
    fn parse_sqlite_config(&self, source: &RegisteredSource) -> RegistryServiceResult<SQLiteConfig> {
        match &source.backend {
            crate::warehouse::sources::SourceBackend::ExternalDatabase {
                database, ..
            } => {
                Ok(SQLiteConfig::new(database.clone()).with_read_only(true))
            }
            crate::warehouse::sources::SourceBackend::ExternalApi { config_json, .. } => {
                #[derive(serde::Deserialize)]
                struct SqliteConfigJson {
                    database_path: String,
                    #[serde(default)]
                    read_only: bool,
                }
                
                let parsed: SqliteConfigJson = serde_json::from_str(config_json).map_err(|e| {
                    RegistryServiceError::InvalidConfig(format!(
                        "Invalid SQLite config JSON: {}",
                        e
                    ))
                })?;
                let mut config = SQLiteConfig::new(parsed.database_path);
                if parsed.read_only {
                    config = config.with_read_only(true);
                }
                Ok(config)
            }
            _ => Err(RegistryServiceError::InvalidConfig(
                "SQLite source requires ExternalDatabase or ExternalApi backend".to_string()
            )),
        }
    }

    /// Parse PostgreSQL configuration from a source.
    fn parse_postgres_config(&self, source: &RegisteredSource) -> RegistryServiceResult<PostgresConfig> {
        match &source.backend {
            crate::warehouse::sources::SourceBackend::ExternalDatabase {
                host, port, database, username, password, schema, ..
            } => {
                let connection_string = format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    urlencoding::encode(username),
                    urlencoding::encode(password.as_deref().unwrap_or("")),
                    host,
                    port,
                    database
                );
                let mut config = PostgresConfig::new(connection_string);
                if let Some(s) = schema {
                    config = config.with_schema(s.clone());
                }
                Ok(config)
            }
            crate::warehouse::sources::SourceBackend::ExternalApi { config_json, .. } => {
                // Parse as connection string
                #[derive(serde::Deserialize)]
                struct PgConfigJson {
                    connection_string: String,
                    #[serde(default = "default_schema")]
                    schema: String,
                }
                fn default_schema() -> String { "public".to_string() }
                
                let parsed: PgConfigJson = serde_json::from_str(config_json).map_err(|e| {
                    RegistryServiceError::InvalidConfig(format!(
                        "Invalid PostgreSQL config JSON: {}",
                        e
                    ))
                })?;
                Ok(PostgresConfig::new(parsed.connection_string).with_schema(parsed.schema))
            }
            _ => Err(RegistryServiceError::InvalidConfig(
                "PostgreSQL source requires ExternalDatabase or ExternalApi backend".to_string()
            )),
        }
    }

    /// Parse MySQL configuration from a source.
    fn parse_mysql_config(&self, source: &RegisteredSource) -> RegistryServiceResult<MySqlConfig> {
        match &source.backend {
            crate::warehouse::sources::SourceBackend::ExternalDatabase {
                host, port, database, username, password, ..
            } => {
                let connection_string = format!(
                    "mysql://{}:{}@{}:{}/{}",
                    urlencoding::encode(username),
                    urlencoding::encode(password.as_deref().unwrap_or("")),
                    host,
                    port,
                    database
                );
                Ok(MySqlConfig::new(connection_string))
            }
            crate::warehouse::sources::SourceBackend::ExternalApi { config_json, .. } => {
                #[derive(serde::Deserialize)]
                struct MysqlConfigJson {
                    connection_string: String,
                }
                
                let parsed: MysqlConfigJson = serde_json::from_str(config_json).map_err(|e| {
                    RegistryServiceError::InvalidConfig(format!(
                        "Invalid MySQL config JSON: {}",
                        e
                    ))
                })?;
                Ok(MySqlConfig::new(parsed.connection_string))
            }
            _ => Err(RegistryServiceError::InvalidConfig(
                "MySQL source requires ExternalDatabase or ExternalApi backend".to_string()
            )),
        }
    }

    /// Parse Redshift configuration from a source.
    fn parse_redshift_config(&self, source: &RegisteredSource) -> RegistryServiceResult<RedshiftConfig> {
        match &source.backend {
            crate::warehouse::sources::SourceBackend::ExternalDatabase {
                host, port, database, username, password, schema, ..
            } => {
                let mut config = RedshiftConfig::new(
                    host.clone(),
                    database.clone(),
                    username.clone(),
                    password.clone().unwrap_or_default(),
                );
                
                // Redshift default port is 5439
                if *port != 5439 {
                    config = config.with_port(*port);
                }
                
                if let Some(s) = schema {
                    config = config.with_schema(s.clone());
                }
                
                Ok(config)
            }
            crate::warehouse::sources::SourceBackend::ExternalApi { config_json, .. } => {
                #[derive(serde::Deserialize)]
                struct RedshiftConfigJson {
                    host: String,
                    database: String,
                    username: String,
                    password: String,
                    #[serde(default = "default_port")]
                    port: u16,
                    #[serde(default = "default_schema")]
                    schema: String,
                    #[serde(default = "default_ssl_mode")]
                    ssl_mode: String,
                }
                fn default_port() -> u16 { 5439 }
                fn default_schema() -> String { "public".to_string() }
                fn default_ssl_mode() -> String { "require".to_string() }
                
                let parsed: RedshiftConfigJson = serde_json::from_str(config_json).map_err(|e| {
                    RegistryServiceError::InvalidConfig(format!(
                        "Invalid Redshift config JSON: {}",
                        e
                    ))
                })?;
                
                let ssl_mode: RedshiftSslMode = parsed.ssl_mode.parse()
                    .unwrap_or_default();
                
                Ok(RedshiftConfig::new(
                    parsed.host,
                    parsed.database,
                    parsed.username,
                    parsed.password,
                ).with_port(parsed.port)
                 .with_schema(parsed.schema)
                 .with_ssl_mode(ssl_mode))
            }
            _ => Err(RegistryServiceError::InvalidConfig(
                "Redshift source requires ExternalDatabase or ExternalApi backend".to_string()
            )),
        }
    }

    /// Parse Snowflake configuration from a source.
    fn parse_snowflake_config(&self, source: &RegisteredSource) -> RegistryServiceResult<SnowflakeConfig> {
        match &source.backend {
            crate::warehouse::sources::SourceBackend::ExternalDatabase {
                host, database, username, password, schema, ..
            } => {
                // For Snowflake, 'host' is the account identifier and we need warehouse from extra config
                // This is a simplified case - in practice, Snowflake config often comes via ExternalApi
                let mut config = SnowflakeConfig::new(
                    host.clone(),       // account
                    "COMPUTE_WH",       // default warehouse
                    database.clone(),
                    username.clone(),
                    password.clone().unwrap_or_default(),
                );
                
                if let Some(s) = schema {
                    config = config.with_schema(s.clone());
                }
                
                Ok(config)
            }
            crate::warehouse::sources::SourceBackend::ExternalApi { config_json, .. } => {
                #[derive(serde::Deserialize)]
                struct SnowflakeConfigJson {
                    account: String,
                    warehouse: String,
                    database: String,
                    username: String,
                    password: String,
                    #[serde(default = "default_schema")]
                    schema: String,
                    #[serde(default)]
                    role: Option<String>,
                }
                fn default_schema() -> String { "PUBLIC".to_string() }
                
                let parsed: SnowflakeConfigJson = serde_json::from_str(config_json).map_err(|e| {
                    RegistryServiceError::InvalidConfig(format!(
                        "Invalid Snowflake config JSON: {}",
                        e
                    ))
                })?;
                
                let mut config = SnowflakeConfig::new(
                    parsed.account,
                    parsed.warehouse,
                    parsed.database,
                    parsed.username,
                    parsed.password,
                ).with_schema(parsed.schema);
                
                if let Some(role) = parsed.role {
                    config = config.with_role(role);
                }
                
                Ok(config)
            }
            _ => Err(RegistryServiceError::InvalidConfig(
                "Snowflake source requires ExternalDatabase or ExternalApi backend".to_string()
            )),
        }
    }

    /// Unregister a source connector.
    ///
    /// This is called when a source is deleted or disabled.
    pub async fn unregister_source(
        &self,
        project_id: Uuid,
        source_name: &str,
    ) -> RegistryServiceResult<()> {
        let registry_key = format!("{}:{}", project_id, source_name);
        
        if self.registry.remove(&registry_key).is_some() {
            info!(
                project_id = %project_id,
                source_name = %source_name,
                "Unregistered connector for source"
            );
        } else {
            warn!(
                project_id = %project_id,
                source_name = %source_name,
                "Attempted to unregister non-existent connector"
            );
        }

        Ok(())
    }

    /// Get a connector by project ID and source name.
    pub async fn get_connector(
        &self,
        project_id: Uuid,
        source_name: &str,
    ) -> Option<Arc<dyn Connector>> {
        let registry_key = format!("{}:{}", project_id, source_name);
        self.registry.get(&registry_key)
    }

    /// Check if a connector is registered for a source.
    pub async fn has_connector(&self, project_id: Uuid, source_name: &str) -> bool {
        let registry_key = format!("{}:{}", project_id, source_name);
        self.registry.get(&registry_key).is_some()
    }

    /// List all registered connectors for a project.
    pub async fn list_connectors(&self, project_id: Uuid) -> Vec<String> {
        let all_sources = self.registry.list_sources();
        let prefix = format!("{}:", project_id);
        
        all_sources
            .into_iter()
            .filter_map(|key| {
                if key.starts_with(&prefix) {
                    Some(key.strip_prefix(&prefix).unwrap().to_string())
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Result of initializing the connector registry service.
#[derive(Debug)]
pub struct InitializeResult {
    /// Total number of cold sources found.
    pub total: usize,
    /// Number of successfully loaded connectors.
    pub loaded: usize,
    /// Number of failed connector initializations.
    pub failed: usize,
    /// Error messages for failed initializations.
    pub errors: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_key_format() {
        let project_id = Uuid::parse_str("12345678-1234-1234-1234-123456789012").unwrap();
        let source_name = "my_source";
        let key = format!("{}:{}", project_id, source_name);
        assert_eq!(key, "12345678-1234-1234-1234-123456789012:my_source");
    }
}
