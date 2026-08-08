//! Catalog Repository
//!
//! Database access layer for the unified catalog system.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

use super::types::{
    Cardinality, CatalogEntry, ColumnLineage, ColumnRef, CrossSourceRelationship,
    FreshnessInfo, LineageDiscoveryMethod, LineageSource, RelationshipType,
    SearchResult, SearchResultType, SourceSummary, SyncStatus, TableRef, TableSummary,
    TransformationType,
};
use crate::warehouse::types::{TypedColumn, TypedSchema};

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during catalog operations.
#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Catalog entry not found: {source_name}.{table_name}")]
    EntryNotFound {
        source_name: String,
        table_name: String,
    },

    #[error("Relationship not found: {id}")]
    RelationshipNotFound { id: Uuid },

    #[error("Invalid catalog data: {0}")]
    InvalidData(String),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type for catalog operations.
pub type CatalogResult<T> = Result<T, CatalogError>;

// ============================================================================
// Row Types for Database Mapping
// ============================================================================

#[derive(Debug, FromRow)]
struct CatalogRow {
    id: Uuid,
    project_id: Uuid,
    source_id: Option<Uuid>,
    source_name: String,
    table_name: String,
    schema: serde_json::Value,
    description: Option<String>,
    tags: serde_json::Value,
    last_sync_at: Option<DateTime<Utc>>,
    sync_status: Option<String>,
    row_count_estimate: Option<i64>,
    size_bytes_estimate: Option<i64>,
    fulltext_columns: serde_json::Value,
    discovered_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
struct LineageRow {
    id: Uuid,
    source_source: String,
    source_table: String,
    source_column: String,
    transformation_type: String,
    transformation_sql: Option<String>,
    confidence: f32,
    discovered_by: String,
}

#[derive(Debug, FromRow)]
struct RelationshipRow {
    id: Uuid,
    project_id: Uuid,
    name: Option<String>,
    from_source: String,
    from_table: String,
    from_columns: serde_json::Value,
    to_source: String,
    to_table: String,
    to_columns: serde_json::Value,
    relationship_type: String,
    cardinality: String,
    confidence: f32,
    is_validated: bool,
    last_validated_at: Option<DateTime<Utc>>,
    violation_count: i32,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

// ============================================================================
// Catalog Repository
// ============================================================================

/// Repository for catalog database operations.
pub struct CatalogRepository {
    db: Arc<PgPool>,
}

impl CatalogRepository {
    /// Create a new catalog repository.
    pub fn new(db: Arc<PgPool>) -> Self {
        Self { db }
    }

    // ========================================================================
    // Catalog Entry Operations
    // ========================================================================

    /// Get a catalog entry by source and table name.
    pub async fn get_entry(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> CatalogResult<CatalogEntry> {
        let row = sqlx::query_as::<_, CatalogRow>(
            r#"
            SELECT 
                id, project_id, source_id, source_name, table_name,
                schema, description, tags, last_sync_at, sync_status,
                row_count_estimate, size_bytes_estimate, fulltext_columns,
                discovered_at, updated_at
            FROM warehouse_catalog
            WHERE project_id = $1 AND source_name = $2 AND table_name = $3
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .bind(table_name)
        .fetch_optional(&*self.db)
        .await?
        .ok_or_else(|| CatalogError::EntryNotFound {
            source_name: source_name.to_string(),
            table_name: table_name.to_string(),
        })?;

        self.row_to_entry(row)
    }

    /// Get a catalog entry by ID.
    pub async fn get_entry_by_id(
        &self,
        id: Uuid,
    ) -> CatalogResult<CatalogEntry> {
        let row = sqlx::query_as::<_, CatalogRow>(
            r#"
            SELECT 
                id, project_id, source_id, source_name, table_name,
                schema, description, tags, last_sync_at, sync_status,
                row_count_estimate, size_bytes_estimate, fulltext_columns,
                discovered_at, updated_at
            FROM warehouse_catalog
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&*self.db)
        .await?
        .ok_or_else(|| CatalogError::InvalidData(format!("Entry not found: {}", id)))?;

        self.row_to_entry(row)
    }

    /// List all catalog entries for a project.
    pub async fn list_entries(
        &self,
        project_id: Uuid,
    ) -> CatalogResult<Vec<CatalogEntry>> {
        let rows = sqlx::query_as::<_, CatalogRow>(
            r#"
            SELECT 
                id, project_id, source_id, source_name, table_name,
                schema, description, tags, last_sync_at, sync_status,
                row_count_estimate, size_bytes_estimate, fulltext_columns,
                discovered_at, updated_at
            FROM warehouse_catalog
            WHERE project_id = $1
            ORDER BY source_name, table_name
            "#,
        )
        .bind(project_id)
        .fetch_all(&*self.db)
        .await?;

        rows.into_iter().map(|r| self.row_to_entry(r)).collect()
    }

    /// List catalog entries for a specific source.
    pub async fn list_entries_for_source(
        &self,
        project_id: Uuid,
        source_name: &str,
    ) -> CatalogResult<Vec<CatalogEntry>> {
        let rows = sqlx::query_as::<_, CatalogRow>(
            r#"
            SELECT 
                id, project_id, source_id, source_name, table_name,
                schema, description, tags, last_sync_at, sync_status,
                row_count_estimate, size_bytes_estimate, fulltext_columns,
                discovered_at, updated_at
            FROM warehouse_catalog
            WHERE project_id = $1 AND source_name = $2
            ORDER BY table_name
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .fetch_all(&*self.db)
        .await?;

        rows.into_iter().map(|r| self.row_to_entry(r)).collect()
    }

    /// Insert or update a catalog entry.
    pub async fn upsert_entry(&self, entry: &CatalogEntry) -> CatalogResult<Uuid> {
        let schema_json = self.schema_to_json(&entry.schema)?;
        let tags_json = serde_json::to_value(&entry.tags)?;
        let fulltext_json = serde_json::to_value(&entry.fulltext_columns)?;

        let result = sqlx::query(
            r#"
            INSERT INTO warehouse_catalog (
                id, project_id, source_id, source_name, table_name,
                schema, description, tags, last_sync_at, sync_status,
                row_count_estimate, size_bytes_estimate, fulltext_columns,
                discovered_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (project_id, source_name, table_name)
            DO UPDATE SET
                source_id = EXCLUDED.source_id,
                schema = EXCLUDED.schema,
                description = EXCLUDED.description,
                tags = EXCLUDED.tags,
                last_sync_at = EXCLUDED.last_sync_at,
                sync_status = EXCLUDED.sync_status,
                row_count_estimate = EXCLUDED.row_count_estimate,
                size_bytes_estimate = EXCLUDED.size_bytes_estimate,
                fulltext_columns = EXCLUDED.fulltext_columns,
                updated_at = now()
            RETURNING id
            "#,
        )
        .bind(entry.id)
        .bind(entry.project_id)
        .bind(entry.source_id)
        .bind(&entry.source_name)
        .bind(&entry.table_name)
        .bind(&schema_json)
        .bind(&entry.description)
        .bind(&tags_json)
        .bind(entry.freshness.last_sync_at)
        .bind(entry.freshness.sync_status.as_str())
        .bind(entry.freshness.row_count_estimate)
        .bind(entry.freshness.size_bytes_estimate)
        .bind(&fulltext_json)
        .bind(entry.discovered_at)
        .bind(entry.updated_at)
        .fetch_one(&*self.db)
        .await?;

        Ok(result.get("id"))
    }

    /// Batch insert or update catalog entries.
    ///
    /// Uses a single INSERT statement with multiple values for better performance.
    /// This is significantly faster than calling upsert_entry in a loop.
    pub async fn batch_upsert_entries(&self, entries: &[CatalogEntry]) -> CatalogResult<Vec<Uuid>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // For small batches, use individual inserts (not transactional)
        if entries.len() <= 5 {
            let mut ids = Vec::with_capacity(entries.len());
            for entry in entries {
                ids.push(self.upsert_entry(entry).await?);
            }
            return Ok(ids);
        }

        // PostgreSQL has a 65,535 parameter limit; with 15 params per entry
        // we can fit at most 4,369 entries per statement.
        const PARAMS_PER_ENTRY: usize = 15;
        const MAX_ENTRIES_PER_BATCH: usize = 65_535 / PARAMS_PER_ENTRY;

        if entries.len() > MAX_ENTRIES_PER_BATCH {
            let mut all_ids = Vec::with_capacity(entries.len());
            for chunk in entries.chunks(MAX_ENTRIES_PER_BATCH) {
                all_ids.extend(Box::pin(self.batch_upsert_entries(chunk)).await?);
            }
            return Ok(all_ids);
        }

        // Build the VALUES clause dynamically for larger batches
        let mut values_clauses = Vec::with_capacity(entries.len());
        let mut param_idx = 1;

        for (i, _) in entries.iter().enumerate() {
            let placeholders: Vec<String> = (0..15)
                .map(|j| format!("${}", param_idx + j))
                .collect();
            values_clauses.push(format!("({})", placeholders.join(", ")));
            param_idx += 15;
        }

        let query = format!(
            r#"
            INSERT INTO warehouse_catalog (
                id, project_id, source_id, source_name, table_name,
                schema, description, tags, last_sync_at, sync_status,
                row_count_estimate, size_bytes_estimate, fulltext_columns,
                discovered_at, updated_at
            ) VALUES {}
            ON CONFLICT (project_id, source_name, table_name)
            DO UPDATE SET
                source_id = EXCLUDED.source_id,
                schema = EXCLUDED.schema,
                description = EXCLUDED.description,
                tags = EXCLUDED.tags,
                last_sync_at = EXCLUDED.last_sync_at,
                sync_status = EXCLUDED.sync_status,
                row_count_estimate = EXCLUDED.row_count_estimate,
                size_bytes_estimate = EXCLUDED.size_bytes_estimate,
                fulltext_columns = EXCLUDED.fulltext_columns,
                updated_at = now()
            RETURNING id
            "#,
            values_clauses.join(", ")
        );

        // Build the query with all bindings
        let mut query_builder = sqlx::query(&query);

        for entry in entries {
            let schema_json = self.schema_to_json(&entry.schema)?;
            let tags_json = serde_json::to_value(&entry.tags)?;
            let fulltext_json = serde_json::to_value(&entry.fulltext_columns)?;

            query_builder = query_builder
                .bind(entry.id)
                .bind(entry.project_id)
                .bind(entry.source_id)
                .bind(&entry.source_name)
                .bind(&entry.table_name)
                .bind(schema_json)
                .bind(&entry.description)
                .bind(tags_json)
                .bind(entry.freshness.last_sync_at)
                .bind(entry.freshness.sync_status.as_str())
                .bind(entry.freshness.row_count_estimate)
                .bind(entry.freshness.size_bytes_estimate)
                .bind(fulltext_json)
                .bind(entry.discovered_at)
                .bind(entry.updated_at);
        }

        let rows = query_builder.fetch_all(&*self.db).await?;
        let ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();

        Ok(ids)
    }

    /// Delete a catalog entry.
    pub async fn delete_entry(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> CatalogResult<bool> {
        let result = sqlx::query(
            r#"
            DELETE FROM warehouse_catalog
            WHERE project_id = $1 AND source_name = $2 AND table_name = $3
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .bind(table_name)
        .execute(&*self.db)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Update fulltext columns for a catalog entry.
    pub async fn update_fulltext_columns(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        fulltext_columns: &[String],
    ) -> CatalogResult<()> {
        let json = serde_json::to_value(fulltext_columns)?;
        sqlx::query(
            r#"
            UPDATE warehouse_catalog
            SET fulltext_columns = $4, updated_at = now()
            WHERE project_id = $1 AND source_name = $2 AND table_name = $3
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .bind(table_name)
        .bind(&json)
        .execute(&*self.db)
        .await
        .map(|result| {
            if result.rows_affected() == 0 {
                tracing::warn!(
                    project_id = %project_id,
                    source_name = source_name,
                    table_name = table_name,
                    "update_fulltext_columns matched no rows"
                );
            }
        })?;

        Ok(())
    }

    /// Update sync status for a catalog entry.
    pub async fn update_sync_status(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        status: SyncStatus,
        row_count: Option<i64>,
    ) -> CatalogResult<()> {
        sqlx::query(
            r#"
            UPDATE warehouse_catalog
            SET 
                sync_status = $4,
                last_sync_at = CASE WHEN $4 = 'synced' THEN now() ELSE last_sync_at END,
                row_count_estimate = COALESCE($5, row_count_estimate),
                updated_at = now()
            WHERE project_id = $1 AND source_name = $2 AND table_name = $3
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .bind(table_name)
        .bind(status.as_str())
        .bind(row_count)
        .execute(&*self.db)
        .await
        .map(|result| {
            if result.rows_affected() == 0 {
                tracing::warn!(
                    project_id = %project_id,
                    source_name = source_name,
                    table_name = table_name,
                    "update_sync_status matched no rows"
                );
            }
        })?;

        Ok(())
    }

    // ========================================================================
    // Source Summary Operations
    // ========================================================================

    /// List all sources with summary information.
    pub async fn list_sources(&self, project_id: Uuid) -> CatalogResult<Vec<SourceSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                c.source_name,
                COALESCE(s.source_type, 'unknown') as source_type,
                COUNT(*) as table_count,
                SUM(c.row_count_estimate) as total_rows,
                MAX(c.last_sync_at) as last_sync_at,
                MAX(CASE c.sync_status
                    WHEN 'error' THEN 4
                    WHEN 'syncing' THEN 3
                    WHEN 'unknown' THEN 2
                    WHEN 'synced' THEN 1
                    ELSE 0
                END) as sync_status_priority,
                CASE MAX(CASE c.sync_status
                    WHEN 'error' THEN 4
                    WHEN 'syncing' THEN 3
                    WHEN 'unknown' THEN 2
                    WHEN 'synced' THEN 1
                    ELSE 0
                END)
                    WHEN 4 THEN 'error'
                    WHEN 3 THEN 'syncing'
                    WHEN 2 THEN 'unknown'
                    WHEN 1 THEN 'synced'
                    ELSE 'unknown'
                END as sync_status
            FROM warehouse_catalog c
            LEFT JOIN warehouse_sources s ON c.source_id = s.id
            WHERE c.project_id = $1
            GROUP BY c.source_name, s.source_type
            ORDER BY c.source_name
            "#,
        )
        .bind(project_id)
        .fetch_all(&*self.db)
        .await?;

        let summaries = rows
            .iter()
            .map(|row| {
                let source_type_str: String = row.get("source_type");
                let source_type = source_type_str.parse()
                    .unwrap_or(crate::warehouse::types::SourceType::Csv); // fallback
                SourceSummary {
                    name: row.get("source_name"),
                    source_type,
                    table_count: row.get::<i64, _>("table_count"),
                    total_rows: row.get("total_rows"),
                    last_sync_at: row.get("last_sync_at"),
                    sync_status: SyncStatus::from_str(
                        row.get::<Option<String>, _>("sync_status")
                            .as_deref()
                            .unwrap_or("unknown"),
                    ),
                }
            })
            .collect();

        Ok(summaries)
    }

    /// List tables with summary information.
    pub async fn list_tables(
        &self,
        project_id: Uuid,
        source_name: &str,
    ) -> CatalogResult<Vec<TableSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                source_name,
                table_name,
                COALESCE(
                    jsonb_array_length(schema),
                    jsonb_array_length(schema->'typed_columns'),
                    jsonb_array_length(schema->'columns'),
                    0
                ) as column_count,
                row_count_estimate,
                size_bytes_estimate,
                sync_status,
                last_sync_at,
                description
            FROM warehouse_catalog
            WHERE project_id = $1 AND source_name = $2
            ORDER BY table_name
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .fetch_all(&*self.db)
        .await?;

        let summaries = rows
            .iter()
            .map(|row| TableSummary {
                source_name: row.get("source_name"),
                table_name: row.get("table_name"),
                column_count: row.get::<i32, _>("column_count"),
                row_count_estimate: row.get("row_count_estimate"),
                size_bytes_estimate: row.get("size_bytes_estimate"),
                sync_status: SyncStatus::from_str(
                    row.get::<Option<String>, _>("sync_status")
                        .as_deref()
                        .unwrap_or("unknown"),
                ),
                last_sync_at: row.get("last_sync_at"),
                description: row.get("description"),
            })
            .collect();

        Ok(summaries)
    }

    // ========================================================================
    // Search Operations
    // ========================================================================

    /// Search the catalog for matching entries.
    pub async fn search(
        &self,
        project_id: Uuid,
        query: &str,
        limit: i32,
    ) -> CatalogResult<Vec<SearchResult>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                'table' as result_type,
                table_name as name,
                source_name || '.' || table_name as fqn,
                description,
                ts_rank(
                    to_tsvector('english', table_name || ' ' || COALESCE(description, '')),
                    plainto_tsquery('english', $2)
                ) as score
            FROM warehouse_catalog
            WHERE project_id = $1
              AND to_tsvector('english', table_name || ' ' || COALESCE(description, '')) 
                  @@ plainto_tsquery('english', $2)
            ORDER BY score DESC
            LIMIT $3
            "#,
        )
        .bind(project_id)
        .bind(query)
        .bind(limit)
        .fetch_all(&*self.db)
        .await?;

        let results = rows
            .iter()
            .map(|row| SearchResult {
                result_type: SearchResultType::Table,
                name: row.get("name"),
                fqn: row.get("fqn"),
                description: row.get("description"),
                score: row.get("score"),
            })
            .collect();

        Ok(results)
    }

    // ========================================================================
    // Lineage Operations
    // ========================================================================

    /// Get lineage for a column (upstream sources).
    pub async fn get_column_lineage(
        &self,
        project_id: Uuid,
        target: &ColumnRef,
    ) -> CatalogResult<ColumnLineage> {
        let rows = sqlx::query_as::<_, LineageRow>(
            r#"
            SELECT 
                id, source_source, source_table, source_column,
                transformation_type::text as transformation_type,
                transformation_sql, confidence, discovered_by::text as discovered_by
            FROM warehouse_lineage
            WHERE project_id = $1 
              AND target_source = $2 
              AND target_table = $3 
              AND target_column = $4
            "#,
        )
        .bind(project_id)
        .bind(&target.source)
        .bind(&target.table)
        .bind(&target.column)
        .fetch_all(&*self.db)
        .await?;

        let mut lineage = ColumnLineage::new(target.clone());

        for row in rows {
            lineage.add_source(LineageSource {
                id: Some(row.id),
                column: ColumnRef::new(
                    &row.source_source,
                    &row.source_table,
                    &row.source_column,
                ),
                transformation_type: TransformationType::from_str(&row.transformation_type),
                transformation_sql: row.transformation_sql,
                confidence: row.confidence,
                discovered_by: LineageDiscoveryMethod::from_str(&row.discovered_by),
            });
        }

        Ok(lineage)
    }

    /// Get downstream dependencies for a column.
    pub async fn get_downstream_dependencies(
        &self,
        project_id: Uuid,
        source: &ColumnRef,
    ) -> CatalogResult<Vec<ColumnRef>> {
        let rows = sqlx::query(
            r#"
            SELECT target_source, target_table, target_column
            FROM warehouse_lineage
            WHERE project_id = $1 
              AND source_source = $2 
              AND source_table = $3 
              AND source_column = $4
            "#,
        )
        .bind(project_id)
        .bind(&source.source)
        .bind(&source.table)
        .bind(&source.column)
        .fetch_all(&*self.db)
        .await?;

        let deps = rows
            .iter()
            .map(|row| {
                ColumnRef::new(
                    row.get::<String, _>("target_source"),
                    row.get::<String, _>("target_table"),
                    row.get::<String, _>("target_column"),
                )
            })
            .collect();

        Ok(deps)
    }

    /// Add a lineage relationship.
    pub async fn add_lineage(
        &self,
        project_id: Uuid,
        target: &ColumnRef,
        source: &LineageSource,
    ) -> CatalogResult<Uuid> {
        let result = sqlx::query(
            r#"
            INSERT INTO warehouse_lineage (
                project_id, target_source, target_table, target_column,
                source_source, source_table, source_column,
                transformation_type, transformation_sql, confidence, discovered_by
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::lineage_transformation_type, $9, $10, $11::lineage_discovery_method)
            ON CONFLICT (project_id, target_source, target_table, target_column,
                        source_source, source_table, source_column)
            DO UPDATE SET
                transformation_type = EXCLUDED.transformation_type,
                transformation_sql = EXCLUDED.transformation_sql,
                confidence = EXCLUDED.confidence,
                discovered_by = EXCLUDED.discovered_by,
                updated_at = now()
            RETURNING id
            "#,
        )
        .bind(project_id)
        .bind(&target.source)
        .bind(&target.table)
        .bind(&target.column)
        .bind(&source.column.source)
        .bind(&source.column.table)
        .bind(&source.column.column)
        .bind(source.transformation_type.as_str())
        .bind(&source.transformation_sql)
        .bind(source.confidence)
        .bind(source.discovered_by.as_str())
        .fetch_one(&*self.db)
        .await?;

        Ok(result.get("id"))
    }

    /// Delete lineage for a target column.
    pub async fn delete_lineage_for_target(
        &self,
        project_id: Uuid,
        target: &ColumnRef,
    ) -> CatalogResult<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM warehouse_lineage
            WHERE project_id = $1 
              AND target_source = $2 
              AND target_table = $3 
              AND target_column = $4
            "#,
        )
        .bind(project_id)
        .bind(&target.source)
        .bind(&target.table)
        .bind(&target.column)
        .execute(&*self.db)
        .await?;

        Ok(result.rows_affected())
    }

    // ========================================================================
    // Relationship Operations
    // ========================================================================

    /// List all relationships for a project.
    pub async fn list_relationships(
        &self,
        project_id: Uuid,
    ) -> CatalogResult<Vec<CrossSourceRelationship>> {
        let rows = sqlx::query_as::<_, RelationshipRow>(
            r#"
            SELECT 
                id, project_id, name,
                from_source, from_table, from_columns,
                to_source, to_table, to_columns,
                relationship_type::text as relationship_type,
                cardinality::text as cardinality,
                confidence, is_validated, last_validated_at, violation_count,
                created_at, updated_at
            FROM warehouse_relationships
            WHERE project_id = $1
            ORDER BY from_source, from_table, to_source, to_table
            "#,
        )
        .bind(project_id)
        .fetch_all(&*self.db)
        .await?;

        rows.into_iter().map(|r| self.row_to_relationship(r)).collect()
    }

    /// Get relationships for a table.
    pub async fn get_relationships_for_table(
        &self,
        project_id: Uuid,
        table: &TableRef,
    ) -> CatalogResult<Vec<CrossSourceRelationship>> {
        let rows = sqlx::query_as::<_, RelationshipRow>(
            r#"
            SELECT 
                id, project_id, name,
                from_source, from_table, from_columns,
                to_source, to_table, to_columns,
                relationship_type::text as relationship_type,
                cardinality::text as cardinality,
                confidence, is_validated, last_validated_at, violation_count,
                created_at, updated_at
            FROM warehouse_relationships
            WHERE project_id = $1
              AND ((from_source = $2 AND from_table = $3)
                   OR (to_source = $2 AND to_table = $3))
            "#,
        )
        .bind(project_id)
        .bind(&table.source)
        .bind(&table.table)
        .fetch_all(&*self.db)
        .await?;

        rows.into_iter().map(|r| self.row_to_relationship(r)).collect()
    }

    /// Get a relationship by ID.
    pub async fn get_relationship(
        &self,
        id: Uuid,
    ) -> CatalogResult<CrossSourceRelationship> {
        let row = sqlx::query_as::<_, RelationshipRow>(
            r#"
            SELECT 
                id, project_id, name,
                from_source, from_table, from_columns,
                to_source, to_table, to_columns,
                relationship_type::text as relationship_type,
                cardinality::text as cardinality,
                confidence, is_validated, last_validated_at, violation_count,
                created_at, updated_at
            FROM warehouse_relationships
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&*self.db)
        .await?
        .ok_or(CatalogError::RelationshipNotFound { id })?;

        self.row_to_relationship(row)
    }

    /// Add or update a relationship.
    pub async fn upsert_relationship(
        &self,
        rel: &CrossSourceRelationship,
    ) -> CatalogResult<Uuid> {
        let from_columns = serde_json::to_value(&rel.from_columns)?;
        let to_columns = serde_json::to_value(&rel.to_columns)?;

        let result = sqlx::query(
            r#"
            INSERT INTO warehouse_relationships (
                id, project_id, name,
                from_source, from_table, from_columns,
                to_source, to_table, to_columns,
                relationship_type, cardinality, confidence,
                is_validated, last_validated_at, violation_count,
                created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10::relationship_type, $11::relationship_cardinality, $12,
                $13, $14, $15, $16, $17
            )
            ON CONFLICT (project_id, from_source, from_table, to_source, to_table, from_columns, to_columns)
            DO UPDATE SET
                name = EXCLUDED.name,
                relationship_type = EXCLUDED.relationship_type,
                cardinality = EXCLUDED.cardinality,
                confidence = EXCLUDED.confidence,
                is_validated = EXCLUDED.is_validated,
                last_validated_at = EXCLUDED.last_validated_at,
                violation_count = EXCLUDED.violation_count,
                updated_at = now()
            RETURNING id
            "#,
        )
        .bind(rel.id)
        .bind(rel.project_id)
        .bind(&rel.name)
        .bind(&rel.from.source)
        .bind(&rel.from.table)
        .bind(&from_columns)
        .bind(&rel.to.source)
        .bind(&rel.to.table)
        .bind(&to_columns)
        .bind(rel.relationship_type.as_str())
        .bind(rel.cardinality.as_str())
        .bind(rel.confidence)
        .bind(rel.is_validated)
        .bind(rel.last_validated_at)
        .bind(rel.violation_count)
        .bind(rel.created_at)
        .bind(rel.updated_at)
        .fetch_one(&*self.db)
        .await?;

        Ok(result.get("id"))
    }

    /// Delete a relationship.
    pub async fn delete_relationship(&self, id: Uuid) -> CatalogResult<bool> {
        let result = sqlx::query(
            r#"
            DELETE FROM warehouse_relationships
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&*self.db)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Update validation status for a relationship.
    pub async fn update_relationship_validation(
        &self,
        id: Uuid,
        is_validated: bool,
        violation_count: i32,
    ) -> CatalogResult<()> {
        sqlx::query(
            r#"
            UPDATE warehouse_relationships
            SET 
                is_validated = $2,
                violation_count = $3,
                last_validated_at = now(),
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(is_validated)
        .bind(violation_count)
        .execute(&*self.db)
        .await?;

        Ok(())
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /// Convert a database row to a CatalogEntry.
    fn row_to_entry(&self, row: CatalogRow) -> CatalogResult<CatalogEntry> {
        let schema = self.json_to_schema(&row.schema, &row.source_name, &row.table_name, row.updated_at)?;
        let tags: Vec<String> = serde_json::from_value(row.tags)?;
        let fulltext_columns: Vec<String> = serde_json::from_value(row.fulltext_columns)?;

        Ok(CatalogEntry {
            id: row.id,
            project_id: row.project_id,
            source_id: row.source_id,
            source_name: row.source_name,
            table_name: row.table_name,
            schema,
            description: row.description,
            tags,
            freshness: FreshnessInfo {
                last_sync_at: row.last_sync_at,
                sync_status: SyncStatus::from_str(
                    row.sync_status.as_deref().unwrap_or("unknown"),
                ),
                row_count_estimate: row.row_count_estimate,
                size_bytes_estimate: row.size_bytes_estimate,
            },
            fulltext_columns,
            discovered_at: row.discovered_at,
            updated_at: row.updated_at,
        })
    }

    /// Convert a database row to a CrossSourceRelationship.
    fn row_to_relationship(&self, row: RelationshipRow) -> CatalogResult<CrossSourceRelationship> {
        let from_columns: Vec<String> = serde_json::from_value(row.from_columns)?;
        let to_columns: Vec<String> = serde_json::from_value(row.to_columns)?;

        Ok(CrossSourceRelationship {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            from: TableRef::new(&row.from_source, &row.from_table),
            from_columns,
            to: TableRef::new(&row.to_source, &row.to_table),
            to_columns,
            relationship_type: RelationshipType::from_str(&row.relationship_type),
            cardinality: Cardinality::from_str(&row.cardinality),
            confidence: row.confidence,
            is_validated: row.is_validated,
            last_validated_at: row.last_validated_at,
            violation_count: row.violation_count,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }

    /// Convert TypedSchema to JSON for database storage.
    fn schema_to_json(&self, schema: &TypedSchema) -> CatalogResult<serde_json::Value> {
        Ok(serde_json::to_value(&schema.columns)?)
    }

    /// Convert JSON to TypedSchema, preserving the original `updated_at` timestamp.
    fn json_to_schema(
        &self,
        json: &serde_json::Value,
        source_name: &str,
        table_name: &str,
        updated_at: DateTime<Utc>,
    ) -> CatalogResult<TypedSchema> {
        let columns: Vec<TypedColumn> = serde_json::from_value(json.clone())?;

        Ok(TypedSchema {
            table_name: table_name.to_string(),
            columns,
            source_name: source_name.to_string(),
            updated_at: Some(updated_at),
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_error_display() {
        let err = CatalogError::EntryNotFound {
            source_name: "stripe".to_string(),
            table_name: "customers".to_string(),
        };
        assert!(err.to_string().contains("stripe.customers"));
    }

    #[test]
    fn test_relationship_error_display() {
        let id = Uuid::new_v4();
        let err = CatalogError::RelationshipNotFound { id };
        assert!(err.to_string().contains(&id.to_string()));
    }
}
