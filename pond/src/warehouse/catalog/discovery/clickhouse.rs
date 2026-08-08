//! ClickHouse Schema Discovery
//!
//! Discovers schema information from ClickHouse databases by querying `system.columns`.

use async_trait::async_trait;
use clickhouse::Client;
use quick_cache::sync::Cache;
use serde::Deserialize;
use tracing::{debug, info, instrument};
use uuid::Uuid;

use super::{DiscoveryError, DiscoveryResult, SchemaDiscovery};
use crate::warehouse::sources::types::{RegisteredSource, SourceConfig};
use crate::warehouse::types::{TypedColumn, TypedSchema};

/// Client cache capacity.
const CLIENT_CACHE_CAPACITY: usize = 50;

// ============================================================================
// ClickHouse Schema Discovery
// ============================================================================

/// Row returned by querying `system.columns`.
#[derive(Debug, Deserialize, clickhouse::Row)]
#[allow(dead_code)]
struct ColumnRow {
    table: String,
    name: String,
    #[serde(rename = "type")]
    col_type: String,
    default_kind: String,
    default_expression: String,
    comment: String,
}

/// Row returned by querying `system.tables`.
#[derive(Debug, Deserialize, clickhouse::Row)]
struct TableRow {
    name: String,
    comment: String,
}

/// ClickHouse schema discovery implementation.
///
/// Uses a client cache to reuse HTTP connections across discovery calls.
pub struct ClickHouseSchemaDiscovery {
    /// Query timeout in seconds.
    query_timeout_secs: u64,
    /// Client cache: source_id -> clickhouse::Client.
    client_cache: Cache<Uuid, Client>,
}

impl ClickHouseSchemaDiscovery {
    /// Create a new ClickHouse schema discovery.
    pub fn new() -> Self {
        Self {
            query_timeout_secs: 30,
            client_cache: Cache::new(CLIENT_CACHE_CAPACITY),
        }
    }

    /// Set query timeout.
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.query_timeout_secs = timeout_secs;
        self
    }

    /// Get or create a ClickHouse client for the source.
    ///
    /// Caches clients by source ID to reuse connections across discovery calls.
    fn get_or_create_client(&self, source: &RegisteredSource) -> DiscoveryResult<Client> {
        // Check cache first
        if let Some(client) = self.client_cache.get(&source.id) {
            debug!("Reusing cached ClickHouse client for source {}", source.name);
            return Ok(client);
        }

        // Build client from source config
        let client = self.build_client(source)?;

        // Cache it
        self.client_cache.insert(source.id, client.clone());
        debug!(
            "Created and cached ClickHouse client for source {}",
            source.name
        );

        Ok(client)
    }

    /// Build a ClickHouse client from the source configuration.
    fn build_client(&self, source: &RegisteredSource) -> DiscoveryResult<Client> {
        match &source.config {
            SourceConfig::ClickHouseDatabase {
                host,
                port,
                database,
                username,
                password,
                ..
            } => {
                let url = format!("http://{}:{}", host, port);
                let mut client = Client::default()
                    .with_url(&url)
                    .with_database(database)
                    .with_user(username);

                if let Some(pw) = password {
                    client = client.with_password(pw);
                }

                Ok(client)
            }
            _ => Err(DiscoveryError::NotConfigured(format!(
                "Source '{}' is not configured as a ClickHouse database",
                source.name
            ))),
        }
    }

    /// Get the database name from the source configuration.
    fn get_database_name(&self, source: &RegisteredSource) -> DiscoveryResult<String> {
        match &source.config {
            SourceConfig::ClickHouseDatabase { database, .. } => Ok(database.clone()),
            _ => Err(DiscoveryError::NotConfigured(format!(
                "Source '{}' is not configured as a ClickHouse database",
                source.name
            ))),
        }
    }

    /// Get the list of tables to discover (if specified in config).
    fn get_table_filter(&self, source: &RegisteredSource) -> Option<Vec<String>> {
        match &source.config {
            SourceConfig::ClickHouseDatabase { tables, .. } if !tables.is_empty() => {
                Some(tables.clone())
            }
            _ => None,
        }
    }

    /// Query table list from `system.tables`.
    async fn discover_tables_internal(
        &self,
        client: &Client,
        database: &str,
        table_filter: Option<&[String]>,
    ) -> DiscoveryResult<Vec<(String, String)>> {
        let tables: Vec<TableRow> = if let Some(filter) = table_filter {
            // Build a comma-separated list of quoted table names for the IN clause
            let in_list: String = filter
                .iter()
                .map(|t| format!("'{}'", t.replace("'", "''")))
                .collect::<Vec<_>>()
                .join(", ");

            let query = format!(
                "SELECT name, comment FROM system.tables WHERE database = '{}' AND name IN ({}) ORDER BY name",
                database.replace("'", "''"),
                in_list,
            );

            client
                .query(&query)
                .fetch_all()
                .await
                .map_err(|e| DiscoveryError::Query(format!("Failed to query system.tables: {}", e)))?
        } else {
            let query = format!(
                "SELECT name, comment FROM system.tables WHERE database = '{}' AND engine NOT IN ('System') ORDER BY name",
                database.replace("'", "''"),
            );

            client
                .query(&query)
                .fetch_all()
                .await
                .map_err(|e| DiscoveryError::Query(format!("Failed to query system.tables: {}", e)))?
        };

        Ok(tables.into_iter().map(|t| (t.name, t.comment)).collect())
    }

    /// Query column metadata from `system.columns` for a specific table.
    async fn discover_columns_internal(
        &self,
        client: &Client,
        database: &str,
        table_name: &str,
        source_name: &str,
    ) -> DiscoveryResult<Vec<TypedColumn>> {
        let query = format!(
            "SELECT table, name, type, default_kind, default_expression, comment \
             FROM system.columns \
             WHERE database = '{}' AND table = '{}' \
             ORDER BY position",
            database.replace("'", "''"),
            table_name.replace("'", "''"),
        );

        let rows: Vec<ColumnRow> = client
            .query(&query)
            .fetch_all()
            .await
            .map_err(|e| {
                DiscoveryError::Query(format!(
                    "Failed to query system.columns for table '{}': {}",
                    table_name, e
                ))
            })?;

        let columns = rows
            .into_iter()
            .map(|row| {
                let (arrow_type, nullable) =
                    crate::warehouse::ch_type_parser::ch_type_to_arrow(&row.col_type);

                let mut col = TypedColumn::new(
                    &row.name,
                    &arrow_type,
                    nullable,
                    &row.col_type,
                    source_name,
                );

                if !row.comment.is_empty() {
                    col = col.with_description(&row.comment);
                }

                col
            })
            .collect();

        Ok(columns)
    }
}

impl Default for ClickHouseSchemaDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaDiscovery for ClickHouseSchemaDiscovery {
    #[instrument(skip(self, source))]
    async fn discover_schemas(
        &self,
        source: &RegisteredSource,
    ) -> DiscoveryResult<Vec<TypedSchema>> {
        info!(
            "Discovering ClickHouse schemas for source: {}",
            source.name
        );

        let database = self.get_database_name(source)?;
        let table_filter = self.get_table_filter(source);

        // Get or create client (cached)
        let client = self.get_or_create_client(source)?;

        // Discover tables
        let tables = self
            .discover_tables_internal(&client, &database, table_filter.as_deref())
            .await?;

        info!(
            "Found {} tables in ClickHouse database '{}'",
            tables.len(),
            database
        );

        // Discover columns for each table
        let mut schemas = Vec::new();
        for (table_name, _description) in tables {
            let columns = self
                .discover_columns_internal(&client, &database, &table_name, &source.name)
                .await?;

            let mut schema = TypedSchema::new(&table_name, &source.name);
            for col in columns {
                schema = schema.with_column(col);
            }

            debug!(
                "Discovered table '{}' with {} columns",
                table_name,
                schema.columns.len()
            );
            schemas.push(schema);
        }

        Ok(schemas)
    }

    #[instrument(skip(self, source))]
    async fn discover_table_schema(
        &self,
        source: &RegisteredSource,
        table_name: &str,
    ) -> DiscoveryResult<Option<TypedSchema>> {
        info!(
            "Discovering ClickHouse schema for table: {}.{}",
            source.name, table_name
        );

        let database = self.get_database_name(source)?;

        // Get or create client (cached)
        let client = self.get_or_create_client(source)?;

        // Discover columns for the table
        let columns = self
            .discover_columns_internal(&client, &database, table_name, &source.name)
            .await?;

        // Return None if table doesn't exist (no columns found)
        if columns.is_empty() {
            return Ok(None);
        }

        let mut schema = TypedSchema::new(table_name, &source.name);
        for col in columns {
            schema = schema.with_column(col);
        }

        Ok(Some(schema))
    }
}
