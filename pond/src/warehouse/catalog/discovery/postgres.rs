//! PostgreSQL Schema Discovery
//!
//! Discovers schema information from PostgreSQL databases.

use arrow::datatypes::DataType;
use async_trait::async_trait;
use quick_cache::sync::Cache;
use sqlx::PgPool;
use tracing::{debug, info, instrument};
use uuid::Uuid;

use super::{DiscoveryError, DiscoveryResult, SchemaDiscovery};
use crate::warehouse::sources::types::{RegisteredSource, SourceBackend, SourceConfig};
use crate::warehouse::types::{TypedColumn, TypedSchema};

/// Connection pool cache capacity.
const POOL_CACHE_CAPACITY: usize = 50;

// ============================================================================
// PostgreSQL Type Mapping
// ============================================================================

/// Map PostgreSQL type names to Arrow data types.
fn pg_type_to_arrow(pg_type: &str, udt_name: &str) -> DataType {
    let lower = pg_type.to_lowercase();
    let udt_lower = udt_name.to_lowercase();

    // Check UDT name first for specific types
    match udt_lower.as_str() {
        // Integer types
        "int2" | "smallint" => DataType::Int16,
        "int4" | "integer" | "int" => DataType::Int32,
        "int8" | "bigint" => DataType::Int64,
        "oid" => DataType::UInt32,

        // Serial types (auto-increment integers)
        "smallserial" | "serial2" => DataType::Int16,
        "serial" | "serial4" => DataType::Int32,
        "bigserial" | "serial8" => DataType::Int64,

        // Floating point
        "float4" | "real" => DataType::Float32,
        "float8" | "double precision" => DataType::Float64,

        // Fixed point (map to string to preserve precision)
        "numeric" | "decimal" => DataType::Utf8, // Could use Decimal128 if precision is known
        "money" => DataType::Utf8,

        // Boolean
        "bool" | "boolean" => DataType::Boolean,

        // Text types
        "char" | "bpchar" | "character" => DataType::Utf8,
        "varchar" | "character varying" | "text" => DataType::Utf8,
        "name" => DataType::Utf8,

        // Binary
        "bytea" => DataType::Binary,

        // Date/Time
        "date" => DataType::Date32,
        "time" | "timetz" => DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        "timestamp" => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        "timestamptz" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        }
        "interval" => DataType::Duration(arrow::datatypes::TimeUnit::Microsecond),

        // UUID
        "uuid" => DataType::Utf8, // Arrow doesn't have native UUID, use string

        // JSON
        "json" | "jsonb" => DataType::Utf8,

        // Network types
        "inet" | "cidr" | "macaddr" | "macaddr8" => DataType::Utf8,

        // Geometric types
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => DataType::Utf8,

        // Range types
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            DataType::Utf8
        }

        // Array types - treat as string for simplicity
        _ if udt_lower.starts_with('_') => DataType::Utf8,

        // Fall back to checking the general type
        _ => match lower.as_str() {
            "array" => DataType::Utf8,
            "user-defined" => DataType::Utf8,
            _ => {
                debug!(
                    "Unknown PostgreSQL type: {} ({}), defaulting to Utf8",
                    pg_type, udt_name
                );
                DataType::Utf8
            }
        },
    }
}

// ============================================================================
// PostgreSQL Schema Discovery
// ============================================================================

/// PostgreSQL schema discovery implementation.
///
/// Uses a connection pool cache to reuse database connections across discovery calls.
pub struct PostgresSchemaDiscovery {
    /// Query timeout in seconds
    query_timeout_secs: u64,
    /// Connection pool cache: source_id -> PgPool
    pool_cache: Cache<Uuid, PgPool>,
}

impl PostgresSchemaDiscovery {
    /// Create a new PostgreSQL schema discovery.
    pub fn new() -> Self {
        Self {
            query_timeout_secs: 30,
            pool_cache: Cache::new(POOL_CACHE_CAPACITY),
        }
    }

    /// Set query timeout.
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.query_timeout_secs = timeout_secs;
        self
    }

    /// Get or create a connection pool for the source.
    ///
    /// Caches pools by source ID to reuse connections across discovery calls.
    async fn get_or_create_pool(&self, source: &RegisteredSource) -> DiscoveryResult<PgPool> {
        // Check cache first
        if let Some(pool) = self.pool_cache.get(&source.id) {
            debug!("Reusing cached connection pool for source {}", source.name);
            return Ok(pool);
        }

        // Create new pool
        let conn_string = self.build_connection_string(source)?;
        let pool = PgPool::connect(&conn_string)
            .await
            .map_err(|e| DiscoveryError::Connection(e.to_string()))?;

        // Cache it
        self.pool_cache.insert(source.id, pool.clone());
        debug!(
            "Created and cached connection pool for source {}",
            source.name
        );

        Ok(pool)
    }

    /// Build a connection string from source configuration.
    fn build_connection_string(&self, source: &RegisteredSource) -> DiscoveryResult<String> {
        match &source.backend {
            SourceBackend::ExternalDatabase {
                host,
                port,
                database,
                username,
                password,
                schema: _,
                ..
            } => {
                let password_part = password
                    .as_ref()
                    .map(|p| format!(":{}", p))
                    .unwrap_or_default();

                Ok(format!(
                    "postgres://{}{}@{}:{}/{}",
                    username, password_part, host, port, database
                ))
            }
            _ => Err(DiscoveryError::NotConfigured(format!(
                "Source {} is not configured as an external PostgreSQL database",
                source.name
            ))),
        }
    }

    /// Get the schema name to query.
    fn get_schema_name(&self, source: &RegisteredSource) -> String {
        match &source.backend {
            SourceBackend::ExternalDatabase { schema, .. } => {
                schema.clone().unwrap_or_else(|| "public".to_string())
            }
            _ => "public".to_string(),
        }
    }

    /// Get the list of tables to discover (if specified in config).
    fn get_table_filter(&self, source: &RegisteredSource) -> Option<Vec<String>> {
        match &source.config {
            SourceConfig::Database { tables, .. } if !tables.is_empty() => Some(tables.clone()),
            _ => None,
        }
    }

    /// Query table metadata from information_schema.
    async fn discover_tables_internal(
        &self,
        pool: &sqlx::PgPool,
        schema_name: &str,
        table_filter: Option<&[String]>,
    ) -> DiscoveryResult<Vec<(String, String)>> {
        // Build query for table list
        let base_query = r#"
            SELECT table_name, COALESCE(obj_description((quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass), '') as description
            FROM information_schema.tables
            WHERE table_schema = $1
              AND table_type = 'BASE TABLE'
        "#;

        let tables: Vec<(String, String)> = if let Some(filter) = table_filter {
            // Filter to specific tables
            let query = format!(
                "{} AND table_name = ANY($2) ORDER BY table_name",
                base_query
            );

            sqlx::query_as::<_, (String, String)>(&query)
                .bind(schema_name)
                .bind(filter)
                .fetch_all(pool)
                .await
                .map_err(|e| DiscoveryError::Query(e.to_string()))?
        } else {
            // Get all tables
            let query = format!("{} ORDER BY table_name", base_query);

            sqlx::query_as::<_, (String, String)>(&query)
                .bind(schema_name)
                .fetch_all(pool)
                .await
                .map_err(|e| DiscoveryError::Query(e.to_string()))?
        };

        Ok(tables)
    }

    /// Query column metadata for a table.
    async fn discover_columns_internal(
        &self,
        pool: &sqlx::PgPool,
        schema_name: &str,
        table_name: &str,
        source_name: &str,
    ) -> DiscoveryResult<Vec<TypedColumn>> {
        let query = r#"
            SELECT 
                c.column_name,
                c.data_type,
                c.udt_name,
                c.is_nullable,
                c.column_default,
                COALESCE(pgd.description, '') as description,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_statio_all_tables st 
                ON st.schemaname = c.table_schema AND st.relname = c.table_name
            LEFT JOIN pg_catalog.pg_description pgd 
                ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
            WHERE c.table_schema = $1 AND c.table_name = $2
            ORDER BY c.ordinal_position
        "#;

        let rows = sqlx::query_as::<
            _,
            (
                String,         // column_name
                String,         // data_type
                String,         // udt_name
                String,         // is_nullable
                Option<String>, // column_default
                String,         // description
                Option<i32>,    // character_maximum_length
                Option<i32>,    // numeric_precision
                Option<i32>,    // numeric_scale
            ),
        >(query)
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(pool)
        .await
        .map_err(|e| DiscoveryError::Query(e.to_string()))?;

        let columns = rows
            .into_iter()
            .map(
                |(
                    name,
                    data_type,
                    udt_name,
                    is_nullable,
                    default,
                    desc,
                    max_len,
                    precision,
                    scale,
                )| {
                    let arrow_type = pg_type_to_arrow(&data_type, &udt_name);
                    let nullable = is_nullable.to_lowercase() == "yes";

                    // Build source type name with full details
                    let source_type_name = if let Some(p) = precision {
                        if let Some(s) = scale {
                            format!("{}({},{})", udt_name, p, s)
                        } else {
                            format!("{}({})", udt_name, p)
                        }
                    } else if let Some(len) = max_len {
                        format!("{}({})", udt_name, len)
                    } else {
                        udt_name.clone()
                    };

                    let mut col = TypedColumn::new(
                        &name,
                        &arrow_type,
                        nullable,
                        &source_type_name,
                        source_name,
                    );

                    if !desc.is_empty() {
                        col = col.with_description(&desc);
                    }

                    col
                },
            )
            .collect();

        Ok(columns)
    }
}

impl Default for PostgresSchemaDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaDiscovery for PostgresSchemaDiscovery {
    #[instrument(skip(self, source))]
    async fn discover_schemas(
        &self,
        source: &RegisteredSource,
    ) -> DiscoveryResult<Vec<TypedSchema>> {
        info!("Discovering PostgreSQL schemas for source: {}", source.name);

        let schema_name = self.get_schema_name(source);
        let table_filter = self.get_table_filter(source);

        // Get or create connection pool (cached)
        let pool = self.get_or_create_pool(source).await?;

        // Discover tables
        let tables = self
            .discover_tables_internal(&pool, &schema_name, table_filter.as_deref())
            .await?;

        info!("Found {} tables in schema {}", tables.len(), schema_name);

        // Discover columns for each table
        let mut schemas = Vec::new();
        for (table_name, description) in tables {
            let columns = self
                .discover_columns_internal(&pool, &schema_name, &table_name, &source.name)
                .await?;

            let mut schema = TypedSchema::new(&table_name, &source.name);
            for col in columns {
                schema = schema.with_column(col);
            }

            // Note: TypedSchema doesn't have a description field currently
            // but we could extend it if needed

            debug!(
                "Discovered table {} with {} columns",
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
            "Discovering PostgreSQL schema for table: {}.{}",
            source.name, table_name
        );

        let schema_name = self.get_schema_name(source);

        // Get or create connection pool (cached)
        let pool = self.get_or_create_pool(source).await?;

        // Discover columns for the table
        let columns = self
            .discover_columns_internal(&pool, &schema_name, table_name, &source.name)
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_mapping_integers() {
        assert_eq!(pg_type_to_arrow("int2", "int2"), DataType::Int16);
        assert_eq!(pg_type_to_arrow("integer", "int4"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("bigint", "int8"), DataType::Int64);
    }

    #[test]
    fn test_pg_type_mapping_floats() {
        assert_eq!(pg_type_to_arrow("real", "float4"), DataType::Float32);
        assert_eq!(
            pg_type_to_arrow("double precision", "float8"),
            DataType::Float64
        );
    }

    #[test]
    fn test_pg_type_mapping_text() {
        assert_eq!(pg_type_to_arrow("varchar", "varchar"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("text", "text"), DataType::Utf8);
        assert_eq!(
            pg_type_to_arrow("character varying", "varchar"),
            DataType::Utf8
        );
    }

    #[test]
    fn test_pg_type_mapping_boolean() {
        assert_eq!(pg_type_to_arrow("boolean", "bool"), DataType::Boolean);
    }

    #[test]
    fn test_pg_type_mapping_timestamps() {
        assert_eq!(
            pg_type_to_arrow("timestamp", "timestamp"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            pg_type_to_arrow("timestamp with time zone", "timestamptz"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_pg_type_mapping_date() {
        assert_eq!(pg_type_to_arrow("date", "date"), DataType::Date32);
    }

    #[test]
    fn test_pg_type_mapping_binary() {
        assert_eq!(pg_type_to_arrow("bytea", "bytea"), DataType::Binary);
    }

    #[test]
    fn test_pg_type_mapping_arrays() {
        // Arrays (prefixed with _) map to Utf8
        assert_eq!(pg_type_to_arrow("ARRAY", "_int4"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("ARRAY", "_text"), DataType::Utf8);
    }

    #[test]
    fn test_pg_type_mapping_unknown() {
        // Unknown types default to Utf8
        assert_eq!(
            pg_type_to_arrow("custom_type", "custom_type"),
            DataType::Utf8
        );
    }
}
