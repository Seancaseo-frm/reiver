//! Data Source Registry
//!
//! Manages the registry of data sources for each project.
//! Provides caching and resolution of sources by name for query processing.

use chrono::Utc;
use quick_cache::sync::Cache;
use sqlx::Row;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::warehouse::types::{SourceType, StorageType, TypedColumn, TypedSchema};
use reiver_core::crypto::RotatingSecretEncryptor;

use super::types::{
    ConsistencyLevel, ExternalDbType, RegisteredSource, SourceBackend, SourceColumnInfo,
    SourceConfig, SourceTableInfo, StorageTier, StorageTierPolicy, SyncScope,
};

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during source registry operations.
#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("Source not found: {0}")]
    SourceNotFound(String),

    #[error("Source name already exists: {0}")]
    NameAlreadyExists(String),

    #[error("Invalid source name: {0}")]
    InvalidName(String),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type for registry operations.
pub type RegistryResult<T> = Result<T, RegistryError>;

// ============================================================================
// Constants
// ============================================================================

/// Maximum source name length.
const MAX_SOURCE_NAME_LENGTH: usize = 64;

/// Cache capacity for source lookups.
const SOURCE_CACHE_CAPACITY: usize = 1000;

// ============================================================================
// Registry
// ============================================================================

/// Registry of data sources for projects.
///
/// Provides:
/// - Source resolution by name (for query processing)
/// - CRUD operations for sources
/// - Caching for frequently accessed sources
pub struct DataSourceRegistry {
    db: Arc<sqlx::PgPool>,
    encryptor: Arc<RotatingSecretEncryptor>,
    /// Cache: (project_id, source_name) -> RegisteredSource
    cache: Cache<(Uuid, String), RegisteredSource>,
}

impl DataSourceRegistry {
    /// Create a new data source registry.
    pub fn new(db: Arc<sqlx::PgPool>, encryptor: Arc<RotatingSecretEncryptor>) -> Self {
        Self {
            db,
            encryptor,
            cache: Cache::new(SOURCE_CACHE_CAPACITY),
        }
    }

    /// Resolve a source by name for a project.
    ///
    /// This is the primary lookup method used during query processing.
    /// Results are cached for performance.
    pub async fn resolve(
        &self,
        project_id: Uuid,
        source_name: &str,
    ) -> RegistryResult<RegisteredSource> {
        let cache_key = (project_id, source_name.to_lowercase());

        // Check cache first
        if let Some(source) = self.cache.get(&cache_key) {
            debug!(
                project_id = %project_id,
                source_name = source_name,
                "Source cache hit"
            );
            return Ok(source);
        }

        // Query database
        let row = sqlx::query(
            r#"
            SELECT 
                id, project_id, name, source_type, storage_type,
                config, enabled, created_at, updated_at,
                COALESCE(tier, 'cold') as tier,
                COALESCE(supports_cdc, true) as supports_cdc,
                COALESCE(consistency_level, 'eventual') as consistency_level,
                COALESCE(sync_scope, 'full') as sync_scope,
                sync_scope_older_than_days,
                COALESCE(storage_tier_policy, '{"type": "fixed"}'::jsonb) as storage_tier_policy,
                backs_source_id
            FROM warehouse_sources
            WHERE project_id = $1 AND LOWER(name) = LOWER($2)
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .fetch_optional(self.db.as_ref())
        .await?;

        let row = row.ok_or_else(|| RegistryError::SourceNotFound(source_name.to_string()))?;

        let source = self.row_to_registered_source(row)?;

        // Cache the result
        self.cache.insert(cache_key, source.clone());

        Ok(source)
    }

    /// Resolve a source by ID.
    pub async fn resolve_by_id(
        &self,
        project_id: Uuid,
        source_id: Uuid,
    ) -> RegistryResult<RegisteredSource> {
        let row = sqlx::query(
            r#"
            SELECT 
                id, project_id, name, source_type, storage_type,
                config, enabled, created_at, updated_at,
                COALESCE(tier, 'cold') as tier,
                COALESCE(supports_cdc, true) as supports_cdc,
                COALESCE(consistency_level, 'eventual') as consistency_level,
                COALESCE(sync_scope, 'full') as sync_scope,
                sync_scope_older_than_days,
                COALESCE(storage_tier_policy, '{"type": "fixed"}'::jsonb) as storage_tier_policy,
                backs_source_id
            FROM warehouse_sources
            WHERE project_id = $1 AND id = $2
            "#,
        )
        .bind(project_id)
        .bind(source_id)
        .fetch_optional(self.db.as_ref())
        .await?;

        let row = row.ok_or_else(|| RegistryError::SourceNotFound(source_id.to_string()))?;

        self.row_to_registered_source(row)
    }

    /// List all sources for a project.
    pub async fn list(&self, project_id: Uuid) -> RegistryResult<Vec<RegisteredSource>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, project_id, name, source_type, storage_type,
                config, enabled, created_at, updated_at,
                COALESCE(tier, 'cold') as tier,
                COALESCE(supports_cdc, true) as supports_cdc,
                COALESCE(consistency_level, 'eventual') as consistency_level,
                COALESCE(sync_scope, 'full') as sync_scope,
                sync_scope_older_than_days,
                COALESCE(storage_tier_policy, '{"type": "fixed"}'::jsonb) as storage_tier_policy,
                backs_source_id
            FROM warehouse_sources
            WHERE project_id = $1
            ORDER BY name
            "#,
        )
        .bind(project_id)
        .fetch_all(self.db.as_ref())
        .await?;

        let mut sources = Vec::with_capacity(rows.len());
        for row in rows {
            match self.row_to_registered_source(row) {
                Ok(source) => sources.push(source),
                Err(e) => {
                    warn!("Failed to parse source from database: {}", e);
                }
            }
        }

        Ok(sources)
    }

    /// List all source names for a project (lightweight version for autocomplete).
    pub async fn list_names(&self, project_id: Uuid) -> RegistryResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT name FROM warehouse_sources WHERE project_id = $1 ORDER BY name",
        )
        .bind(project_id)
        .fetch_all(self.db.as_ref())
        .await?;

        Ok(rows)
    }

    /// Check if a source name exists for a project.
    pub async fn name_exists(&self, project_id: Uuid, name: &str) -> RegistryResult<bool> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM warehouse_sources WHERE project_id = $1 AND LOWER(name) = LOWER($2)",
        )
        .bind(project_id)
        .bind(name)
        .fetch_one(self.db.as_ref())
        .await?;

        Ok(count > 0)
    }

    /// Update the configuration for a source.
    pub async fn update_config(
        &self,
        project_id: Uuid,
        source_id: Uuid,
        config: &SourceConfig,
    ) -> RegistryResult<()> {
        let config_json = serde_json::to_value(config)?;

        let result = sqlx::query(
            r#"
            UPDATE warehouse_sources
            SET config = $1, updated_at = NOW()
            WHERE project_id = $2 AND id = $3
            "#,
        )
        .bind(config_json)
        .bind(project_id)
        .bind(source_id)
        .execute(self.db.as_ref())
        .await?;

        if result.rows_affected() == 0 {
            return Err(RegistryError::SourceNotFound(source_id.to_string()));
        }

        // Invalidate cache for this source
        self.invalidate_source_cache(project_id, source_id).await;

        Ok(())
    }

    /// Invalidate cache entries for a source.
    async fn invalidate_source_cache(&self, project_id: Uuid, source_id: Uuid) {
        let name: Option<String> = sqlx::query_scalar(
            "SELECT name FROM warehouse_sources WHERE project_id = $1 AND id = $2",
        )
        .bind(project_id)
        .bind(source_id)
        .fetch_optional(self.db.as_ref())
        .await
        .ok()
        .flatten();

        if let Some(name) = name {
            let cache_key = (project_id, name.to_lowercase());
            self.cache.remove(&cache_key);
        }
    }

    /// Get tables available in a source.
    ///
    /// For synced sources, this queries the warehouse_tables table.
    /// For external sources, this may need to introspect the data.
    pub async fn list_tables(
        &self,
        project_id: Uuid,
        source_id: Uuid,
    ) -> RegistryResult<Vec<SourceTableInfo>> {
        // Query warehouse_tables for this source
        let rows = sqlx::query(
            r#"
            SELECT 
                name, schema_json, row_count, last_synced_at
            FROM warehouse_tables
            WHERE project_id = $1 AND source_id = $2
            ORDER BY name
            "#,
        )
        .bind(project_id)
        .bind(source_id)
        .fetch_all(self.db.as_ref())
        .await?;

        // Get source name for qualified names
        let source = self.resolve_by_id(project_id, source_id).await?;

        let tables = rows
            .into_iter()
            .map(|row| {
                let name: String = row.get("name");
                let qualified_name = format!("{}.{}", source.name, name);
                let estimated_rows: Option<i64> = row.get("row_count");
                let last_synced_at: Option<chrono::DateTime<Utc>> = row.get("last_synced_at");
                let schema_json: Option<serde_json::Value> = row.get("schema_json");

                let columns = schema_json
                    .as_ref()
                    .map(|json| Self::parse_column_infos(json))
                    .unwrap_or_default();

                SourceTableInfo {
                    name,
                    qualified_name,
                    columns,
                    estimated_rows: estimated_rows.map(|r| r as u64),
                    last_synced_at,
                }
            })
            .collect();

        Ok(tables)
    }

    /// Get the typed schema for a specific table.
    ///
    /// This retrieves the full schema with Arrow types and semantic metadata
    /// for use in schema reconciliation during cross-source JOINs.
    ///
    /// # Arguments
    /// * `project_id` - The project ID
    /// * `source_name` - The source name (e.g., "stripe")
    /// * `table_name` - The table name within the source (e.g., "customers")
    ///
    /// # Returns
    /// A `TypedSchema` with full column type information, or an error if not found.
    pub async fn get_typed_schema(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> RegistryResult<TypedSchema> {
        // First resolve the source to get its ID and type
        let source = self.resolve(project_id, source_name).await?;

        // Query the table schema from warehouse_tables
        let row = sqlx::query(
            r#"
            SELECT 
                name, schema_json, source_id
            FROM warehouse_tables
            WHERE project_id = $1 AND source_id = $2 AND LOWER(name) = LOWER($3)
            "#,
        )
        .bind(project_id)
        .bind(source.id)
        .bind(table_name)
        .fetch_optional(self.db.as_ref())
        .await?;

        let row = row.ok_or_else(|| {
            RegistryError::SourceNotFound(format!("{}.{}", source_name, table_name))
        })?;

        let name: String = row.get("name");
        let schema_json: Option<serde_json::Value> = row.get("schema_json");

        // Parse the schema JSON into TypedSchema
        let typed_schema = match schema_json {
            Some(json) => self.parse_typed_schema(&json, &name, &source.name)?,
            None => {
                // Return empty schema if no schema_json is stored
                TypedSchema::new(&name, &source.name)
            }
        };

        Ok(typed_schema)
    }

    /// Get typed schemas for all tables in a source.
    ///
    /// This is useful for schema discovery and validation.
    pub async fn get_all_typed_schemas(
        &self,
        project_id: Uuid,
        source_name: &str,
    ) -> RegistryResult<Vec<TypedSchema>> {
        let source = self.resolve(project_id, source_name).await?;

        let rows = sqlx::query(
            r#"
            SELECT 
                name, schema_json
            FROM warehouse_tables
            WHERE project_id = $1 AND source_id = $2
            ORDER BY name
            "#,
        )
        .bind(project_id)
        .bind(source.id)
        .fetch_all(self.db.as_ref())
        .await?;

        let mut schemas = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.get("name");
            let schema_json: Option<serde_json::Value> = row.get("schema_json");

            let typed_schema = match schema_json {
                Some(json) => match self.parse_typed_schema(&json, &name, &source.name) {
                    Ok(schema) => schema,
                    Err(e) => {
                        warn!(
                            table = %name,
                            source = %source.name,
                            error = %e,
                            "Failed to parse typed schema, using empty schema"
                        );
                        TypedSchema::new(&name, &source.name)
                    }
                },
                None => TypedSchema::new(&name, &source.name),
            };

            schemas.push(typed_schema);
        }

        Ok(schemas)
    }

    /// Parse schema_json into a list of `SourceColumnInfo`.
    ///
    /// Handles both legacy format (simple `columns` array with name/data_type/nullable)
    /// and new format (`typed_columns` array with full Arrow type information).
    fn parse_column_infos(json: &serde_json::Value) -> Vec<SourceColumnInfo> {
        // Try new format first
        if let Some(typed_columns) = json.get("typed_columns").and_then(|v| v.as_array()) {
            return typed_columns
                .iter()
                .filter_map(|col| {
                    let name = col.get("name").and_then(|v| v.as_str())?;
                    let data_type = col
                        .get("source_type")
                        .or_else(|| col.get("data_type"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("string");
                    let nullable = col.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
                    let description = col.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());

                    Some(SourceColumnInfo {
                        name: name.to_string(),
                        data_type: data_type.to_string(),
                        nullable,
                        description,
                    })
                })
                .collect();
        }

        // Fall back to legacy format
        if let Some(columns) = json.get("columns").and_then(|v| v.as_array()) {
            return columns
                .iter()
                .filter_map(|col| {
                    let name = col.get("name").and_then(|v| v.as_str())?;
                    let data_type = col
                        .get("data_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("string");
                    let nullable = col.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
                    let description = col.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());

                    Some(SourceColumnInfo {
                        name: name.to_string(),
                        data_type: data_type.to_string(),
                        nullable,
                        description,
                    })
                })
                .collect();
        }

        Vec::new()
    }

    /// Parse schema_json into a TypedSchema.
    ///
    /// The schema_json format supports both legacy (simple columns array)
    /// and new format (with full Arrow type information).
    fn parse_typed_schema(
        &self,
        json: &serde_json::Value,
        table_name: &str,
        source_name: &str,
    ) -> RegistryResult<TypedSchema> {
        let mut typed_schema = TypedSchema::new(table_name, source_name);

        // Check for new format with "typed_columns" array
        if let Some(typed_columns) = json.get("typed_columns").and_then(|v| v.as_array()) {
            for col_json in typed_columns {
                if let Ok(column) = serde_json::from_value::<TypedColumn>(col_json.clone()) {
                    typed_schema = typed_schema.with_column(column);
                }
            }
            return Ok(typed_schema);
        }

        // Fall back to legacy format with simple "columns" array
        if let Some(columns) = json.get("columns").and_then(|v| v.as_array()) {
            for col_json in columns {
                let name = col_json
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let data_type_str = col_json
                    .get("data_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("string");
                let nullable = col_json
                    .get("nullable")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);

                // Convert simple type string to Arrow DataType
                let arrow_type = self.simple_type_to_arrow(data_type_str);
                let column = TypedColumn::new(
                    name,
                    &arrow_type,
                    nullable,
                    data_type_str,
                    source_name,
                );
                typed_schema = typed_schema.with_column(column);
            }
        }

        Ok(typed_schema)
    }

    /// Convert a simple type string to Arrow DataType.
    ///
    /// This handles the legacy schema format where types are stored as strings
    /// like "string", "int64", "timestamp", etc.
    fn simple_type_to_arrow(&self, type_str: &str) -> arrow::datatypes::DataType {
        use arrow::datatypes::{DataType, TimeUnit};

        match type_str.to_lowercase().as_str() {
            "string" | "text" | "varchar" => DataType::Utf8,
            "int" | "integer" | "int32" => DataType::Int32,
            "bigint" | "int64" | "long" => DataType::Int64,
            "smallint" | "int16" | "short" => DataType::Int16,
            "tinyint" | "int8" | "byte" => DataType::Int8,
            "float" | "float32" | "real" => DataType::Float32,
            "double" | "float64" => DataType::Float64,
            "boolean" | "bool" => DataType::Boolean,
            "date" | "date32" => DataType::Date32,
            "timestamp" | "datetime" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "decimal" | "numeric" => DataType::Decimal128(38, 18),
            "uuid" => DataType::FixedSizeBinary(16),
            "json" | "jsonb" => DataType::Utf8,
            "binary" | "bytes" | "blob" => DataType::Binary,
            _ => DataType::Utf8, // Default to string
        }
    }

    /// Convert a database row to a RegisteredSource.
    fn row_to_registered_source(
        &self,
        row: sqlx::postgres::PgRow,
    ) -> RegistryResult<RegisteredSource> {
        let id: Uuid = row.get("id");
        let project_id: Uuid = row.get("project_id");
        let name: String = row.get("name");
        let source_type_str: String = row.get("source_type");
        let storage_type_str: String = row.get("storage_type");
        let config_json: serde_json::Value = row.get("config");
        let enabled: bool = row.get("enabled");
        let created_at: chrono::DateTime<Utc> = row.get("created_at");
        let updated_at: chrono::DateTime<Utc> = row.get("updated_at");
        let tier_str: String = row.get("tier");
        let supports_cdc: bool = row.get("supports_cdc");
        let consistency_level_str: String = row.get("consistency_level");
        let sync_scope_str: String = row.get("sync_scope");
        let sync_scope_older_than_days: Option<i32> = row.get("sync_scope_older_than_days");
        let storage_tier_policy_json: serde_json::Value = row.get("storage_tier_policy");
        let backs_source_id: Option<Uuid> = row.get("backs_source_id");

        // Parse source type
        let source_type = parse_source_type(&source_type_str);
        
        // Parse tier
        let tier = tier_str.parse::<StorageTier>().unwrap_or_default();
        
        // Parse consistency level
        let consistency_level = consistency_level_str.parse::<ConsistencyLevel>().unwrap_or_default();
        
        // Parse sync scope
        let sync_scope = match sync_scope_str.as_str() {
            "time_based" => {
                let days = sync_scope_older_than_days.unwrap_or(0) as u32;
                SyncScope::TimeBased { older_than_days: days }
            }
            _ => SyncScope::Full,
        };
        
        // Parse storage tier policy
        let storage_tier_policy: StorageTierPolicy =
            serde_json::from_value(storage_tier_policy_json).unwrap_or_default();

        // Parse backend and config from the stored JSON
        let (backend, config) = parse_backend_and_config(
            &storage_type_str,
            &config_json,
            source_type,
            &self.encryptor,
        )?;

        Ok(RegisteredSource {
            id,
            project_id,
            name,
            source_type,
            tier,
            backend,
            config,
            enabled,
            supports_cdc,
            consistency_level,
            sync_scope,
            storage_tier_policy,
            created_at,
            updated_at,
            backs_source_id,
        })
    }
    
    /// List all cold tier sources for a project.
    /// Used by the ConnectorRegistryService to initialize runtime connectors.
    pub async fn list_cold(&self, project_id: Uuid) -> RegistryResult<Vec<RegisteredSource>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, project_id, name, source_type, storage_type,
                config, enabled, created_at, updated_at,
                COALESCE(tier, 'cold') as tier,
                COALESCE(supports_cdc, true) as supports_cdc,
                COALESCE(consistency_level, 'eventual') as consistency_level,
                COALESCE(sync_scope, 'full') as sync_scope,
                sync_scope_older_than_days,
                COALESCE(storage_tier_policy, '{"type": "fixed"}'::jsonb) as storage_tier_policy,
                backs_source_id
            FROM warehouse_sources
            WHERE project_id = $1 AND COALESCE(tier, 'cold') = 'cold' AND enabled = TRUE
            ORDER BY name
            "#,
        )
        .bind(project_id)
        .fetch_all(self.db.as_ref())
        .await?;

        let mut sources = Vec::with_capacity(rows.len());
        for row in rows {
            match self.row_to_registered_source(row) {
                Ok(source) => sources.push(source),
                Err(e) => {
                    warn!("Failed to parse cold source from database: {}", e);
                }
            }
        }

        Ok(sources)
    }
    
    /// List all cold tier sources across all projects.
    /// Used during startup to initialize all runtime connectors.
    pub async fn list_all_cold(&self) -> RegistryResult<Vec<RegisteredSource>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, project_id, name, source_type, storage_type,
                config, enabled, created_at, updated_at,
                COALESCE(tier, 'cold') as tier,
                COALESCE(supports_cdc, true) as supports_cdc,
                COALESCE(consistency_level, 'eventual') as consistency_level,
                COALESCE(sync_scope, 'full') as sync_scope,
                sync_scope_older_than_days,
                COALESCE(storage_tier_policy, '{"type": "fixed"}'::jsonb) as storage_tier_policy,
                backs_source_id
            FROM warehouse_sources
            WHERE COALESCE(tier, 'cold') = 'cold' AND enabled = TRUE
            ORDER BY project_id, name
            "#,
        )
        .fetch_all(self.db.as_ref())
        .await?;

        let mut sources = Vec::with_capacity(rows.len());
        for row in rows {
            match self.row_to_registered_source(row) {
                Ok(source) => sources.push(source),
                Err(e) => {
                    warn!("Failed to parse cold source from database: {}", e);
                }
            }
        }

        Ok(sources)
    }
}

/// Validate a source name.
pub fn validate_source_name(name: &str) -> RegistryResult<()> {
    if name.is_empty() {
        return Err(RegistryError::InvalidName("Source name cannot be empty".to_string()));
    }

    if name.len() > MAX_SOURCE_NAME_LENGTH {
        return Err(RegistryError::InvalidName(format!(
            "Source name exceeds maximum length of {} characters",
            MAX_SOURCE_NAME_LENGTH
        )));
    }

    // Must start with a letter
    if !name.chars().next().map(|c| c.is_ascii_alphabetic()).unwrap_or(false) {
        return Err(RegistryError::InvalidName(
            "Source name must start with a letter".to_string(),
        ));
    }

    // Only alphanumeric and underscores
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(RegistryError::InvalidName(
            "Source name can only contain letters, numbers, and underscores".to_string(),
        ));
    }

    // Check for reserved names
    let reserved = ["system", "default", "information_schema", "pg_catalog"];
    if reserved.contains(&name.to_lowercase().as_str()) {
        return Err(RegistryError::InvalidName(format!(
            "'{}' is a reserved name",
            name
        )));
    }

    Ok(())
}

/// Parse source type from string, falling back to ExternalParquet.
fn parse_source_type(s: &str) -> SourceType {
    s.parse().unwrap_or(SourceType::ExternalParquet)
}

/// Check if a source type represents an external database.
fn is_external_database_type(source_type: SourceType) -> bool {
    matches!(
        source_type,
        SourceType::PostgreSQL
            | SourceType::MySQL
            | SourceType::MongoDB
            | SourceType::SqlServer
            | SourceType::SQLite
            | SourceType::Redshift
            | SourceType::Snowflake
            | SourceType::ClickHouse
            | SourceType::BigQuery
    )
}

/// Map a SourceType to the corresponding ExternalDbType.
fn source_type_to_db_type(source_type: SourceType) -> Option<ExternalDbType> {
    match source_type {
        SourceType::PostgreSQL => Some(ExternalDbType::PostgreSQL),
        SourceType::MySQL => Some(ExternalDbType::MySQL),
        SourceType::MongoDB => Some(ExternalDbType::MongoDB),
        SourceType::SqlServer => Some(ExternalDbType::SqlServer),
        SourceType::SQLite => Some(ExternalDbType::SQLite),
        SourceType::Redshift => Some(ExternalDbType::Redshift),
        SourceType::Snowflake => Some(ExternalDbType::Snowflake),
        SourceType::BigQuery => Some(ExternalDbType::BigQuery),
        _ => None,
    }
}

/// Decrypt config JSON if it is encrypted, returning the plaintext JSON.
fn decrypt_config(
    config_json: &serde_json::Value,
    encryptor: &RotatingSecretEncryptor,
) -> RegistryResult<serde_json::Value> {
    if let Some(encrypted_str) = config_json.get("encrypted").and_then(|v| v.as_str()) {
        let decrypted = encryptor.decrypt(encrypted_str)
            .map_err(|e| RegistryError::Config(format!("Failed to decrypt config: {}", e)))?;
        serde_json::from_str(&decrypted)
            .map_err(|e| RegistryError::Config(format!("Invalid decrypted config JSON: {}", e)))
    } else {
        // Not encrypted (legacy or already plaintext)
        Ok(config_json.clone())
    }
}

/// Parse an ExternalDatabase backend from decrypted config JSON.
fn parse_external_database_backend(
    decrypted: &serde_json::Value,
    source_type: SourceType,
) -> RegistryResult<SourceBackend> {
    let db_type = source_type_to_db_type(source_type)
        .ok_or_else(|| RegistryError::Config(format!("{:?} is not an external database type", source_type)))?;

    let host = decrypted["host"].as_str().unwrap_or("localhost").to_string();
    let port_u64 = decrypted["port"].as_u64().unwrap_or_else(|| {
        match source_type {
            SourceType::PostgreSQL => 5432,
            SourceType::MySQL => 3306,
            SourceType::MongoDB => 27017,
            SourceType::SqlServer => 1433,
            SourceType::Redshift => 5439,
            SourceType::ClickHouse => 8123,
            _ => 0,
        }
    });
    let port = u16::try_from(port_u64)
        .map_err(|_| RegistryError::Config(format!("port {} out of valid range (0-65535)", port_u64)))?;
    let database = decrypted["database"].as_str().unwrap_or("").to_string();
    let username = decrypted["username"].as_str().unwrap_or("").to_string();
    let password = decrypted["password"].as_str().map(|s| s.to_string());
    let schema = decrypted["schema"].as_str().map(|s| s.to_string());

    Ok(SourceBackend::ExternalDatabase {
        db_type,
        host,
        port,
        database,
        username,
        password,
        schema,
    })
}

/// Parse backend and config from stored JSON.
fn parse_backend_and_config(
    storage_type: &str,
    config_json: &serde_json::Value,
    source_type: SourceType,
    encryptor: &RotatingSecretEncryptor,
) -> RegistryResult<(SourceBackend, SourceConfig)> {
    let parsed_storage: StorageType = storage_type.parse().unwrap_or_default();

    // For external database source types, decrypt config and build ExternalDatabase backend
    let backend = if is_external_database_type(source_type) {
        let decrypted = decrypt_config(config_json, encryptor)?;
        parse_external_database_backend(&decrypted, source_type)?
    } else {
        match parsed_storage {
            StorageType::NativeClickHouse => {
                let database = config_json["clickhouse_database"]
                    .as_str()
                    .unwrap_or("reiver")
                    .to_string();
                let table_prefix = config_json["table_prefix"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                SourceBackend::ClickHouseNative { database, table_prefix }
            }
            StorageType::ObjectStorage | StorageType::External => {
                let bucket_url = config_json["bucket_url"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                let prefix = config_json["prefix"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                SourceBackend::ObjectStorage {
                    bucket_url,
                    prefix,
                    access_key_id: config_json["access_key_id"].as_str().map(|s| s.to_string()),
                    secret_access_key: None, // Not loaded from JSON for security
                }
            }
        }
    };

    // Parse config based on source type
    let config = match source_type {
        SourceType::ExternalParquet => {
            // Try to parse ExternalSourceConfig from the config JSON
            if let Ok(parquet_config) = serde_json::from_value(config_json.clone()) {
                SourceConfig::Parquet { config: parquet_config }
            } else {
                SourceConfig::Parquet {
                    config: crate::warehouse::types::ExternalSourceConfig::default(),
                }
            }
        }
        _ => {
            // Synced source
            let sync_interval = config_json["sync_interval_secs"]
                .as_u64()
                .unwrap_or(3600);
            let tables = config_json["tables"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            SourceConfig::Synced {
                sync_interval_secs: sync_interval,
                tables,
            }
        }
    };

    Ok((backend, config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_source_name_valid() {
        assert!(validate_source_name("stripe").is_ok());
        assert!(validate_source_name("my_data").is_ok());
        assert!(validate_source_name("events2024").is_ok());
        assert!(validate_source_name("S3_bucket").is_ok());
    }

    #[test]
    fn test_validate_source_name_invalid() {
        assert!(validate_source_name("").is_err());
        assert!(validate_source_name("123abc").is_err()); // Starts with number
        assert!(validate_source_name("my-data").is_err()); // Contains hyphen
        assert!(validate_source_name("my.data").is_err()); // Contains dot
        assert!(validate_source_name("system").is_err()); // Reserved
    }

    #[test]
    fn test_parse_source_type() {
        assert_eq!(parse_source_type("stripe"), SourceType::Stripe);
        assert_eq!(parse_source_type("PostgreSQL"), SourceType::PostgreSQL);
        assert_eq!(parse_source_type("postgres"), SourceType::PostgreSQL);
        assert_eq!(parse_source_type("external_parquet"), SourceType::ExternalParquet);
    }
}
