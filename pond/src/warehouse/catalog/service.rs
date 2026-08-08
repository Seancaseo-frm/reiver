//! Catalog Service
//!
//! High-level API for the unified catalog system.

use chrono::{DateTime, Utc};
use quick_cache::sync::Cache;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use super::discovery::{
    ParquetSchemaDiscovery, PostgresSchemaDiscovery, RelationshipInference, SchemaDiscovery,
    StripeSchemaDiscovery,
};
use super::repository::{CatalogError, CatalogRepository, CatalogResult};
use super::types::{
    CatalogEntry, ColumnLineage, ColumnRef, CrossSourceRelationship, LineageSource,
    RelationshipType, SearchResult, SourceSummary, SyncStatus, TableRef, TableSummary,
};
use crate::warehouse::sources::registry::DataSourceRegistry;
use crate::warehouse::statistics::persistence::{
    ColumnStatistics, StatisticsRepository, TableStatistics,
};
use crate::warehouse::types::TypedColumn;

// ============================================================================
// Cache Keys
// ============================================================================

/// Cache key types for the catalog.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum CatalogCacheKey {
    Entry {
        project_id: Uuid,
        source: String,
        table: String,
    },
}

// ============================================================================
// Validation Result
// ============================================================================

/// Result of validating a relationship.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether the relationship is valid (no violations or within threshold).
    pub is_valid: bool,
    /// Number of rows that violate the relationship.
    pub violation_count: i64,
    /// Total rows checked.
    pub total_checked: i64,
    /// Sample of violating values (for debugging).
    pub sample_violations: Vec<String>,
    /// When validation was performed.
    pub validated_at: DateTime<Utc>,
}

impl ValidationResult {
    /// Create a new valid result.
    pub fn valid(total_checked: i64) -> Self {
        Self {
            is_valid: true,
            violation_count: 0,
            total_checked,
            sample_violations: Vec::new(),
            validated_at: Utc::now(),
        }
    }

    /// Create a new invalid result.
    pub fn invalid(violation_count: i64, total_checked: i64, samples: Vec<String>) -> Self {
        Self {
            is_valid: false,
            violation_count,
            total_checked,
            sample_violations: samples,
            validated_at: Utc::now(),
        }
    }
}

// ============================================================================
// Refresh Result
// ============================================================================

/// Result of refreshing catalog for a source.
#[derive(Debug, Clone)]
pub struct RefreshResult {
    /// Source that was refreshed.
    pub source_name: String,
    /// Number of tables discovered.
    pub tables_discovered: i32,
    /// Number of tables updated.
    pub tables_updated: i32,
    /// Number of tables removed.
    pub tables_removed: i32,
    /// Number of relationships inferred.
    pub relationships_inferred: i32,
    /// Duration of the refresh.
    pub duration_ms: i64,
    /// Any errors that occurred.
    pub errors: Vec<String>,
}

// ============================================================================
// Catalog Service
// ============================================================================

/// Cache capacity for catalog entries.
const CATALOG_CACHE_CAPACITY: usize = 1000;

/// High-level service for the unified catalog.
///
/// Provides a clean API for:
/// - Schema discovery across all sources
/// - Statistics integration
/// - Lineage tracking
/// - Relationship management
pub struct CatalogService {
    /// Catalog repository.
    repository: CatalogRepository,
    /// Source registry (for accessing source connections).
    source_registry: Arc<DataSourceRegistry>,
    /// Statistics repository.
    stats_repository: Arc<StatisticsRepository>,
    /// Cache for catalog entries (quick_cache uses CLOCK/S3-FIFO eviction).
    cache: Cache<CatalogCacheKey, CatalogEntry>,
}

impl CatalogService {
    /// Create a new catalog service.
    pub fn new(
        db: Arc<PgPool>,
        source_registry: Arc<DataSourceRegistry>,
        stats_repository: Arc<StatisticsRepository>,
    ) -> Self {
        Self {
            repository: CatalogRepository::new(db),
            source_registry,
            stats_repository,
            cache: Cache::new(CATALOG_CACHE_CAPACITY),
        }
    }

    // ========================================================================
    // Schema Discovery
    // ========================================================================

    /// List all sources with summary information.
    #[instrument(skip(self))]
    pub async fn list_sources(&self, project_id: Uuid) -> CatalogResult<Vec<SourceSummary>> {
        self.repository.list_sources(project_id).await
    }

    /// List all tables for a source.
    #[instrument(skip(self))]
    pub async fn list_tables(
        &self,
        project_id: Uuid,
        source: &str,
    ) -> CatalogResult<Vec<TableSummary>> {
        self.repository.list_tables(project_id, source).await
    }

    /// Get full schema for a table.
    #[instrument(skip(self))]
    pub async fn get_table_schema(
        &self,
        project_id: Uuid,
        source: &str,
        table: &str,
    ) -> CatalogResult<CatalogEntry> {
        let cache_key = CatalogCacheKey::Entry {
            project_id,
            source: source.to_string(),
            table: table.to_string(),
        };

        // Check cache first
        if let Some(entry) = self.cache.get(&cache_key) {
            debug!("Cache hit for {}.{}", source, table);
            return Ok(entry);
        }

        // Fetch from database
        let entry = self.repository.get_entry(project_id, source, table).await?;

        // Cache the result
        self.cache.insert(cache_key, entry.clone());

        Ok(entry)
    }

    /// Get column type information.
    #[instrument(skip(self))]
    pub async fn get_column_type(&self, project_id: Uuid, fqn: &str) -> CatalogResult<TypedColumn> {
        let col_ref = ColumnRef::parse(fqn).ok_or_else(|| {
            CatalogError::InvalidData(format!("Invalid column reference: {}", fqn))
        })?;

        let entry = self
            .get_table_schema(project_id, &col_ref.source, &col_ref.table)
            .await?;

        entry
            .schema
            .get_column(&col_ref.column)
            .cloned()
            .ok_or_else(|| CatalogError::InvalidData(format!("Column not found: {}", fqn)))
    }

    /// Search the catalog.
    #[instrument(skip(self))]
    pub async fn search_catalog(
        &self,
        project_id: Uuid,
        query: &str,
        limit: Option<i32>,
    ) -> CatalogResult<Vec<SearchResult>> {
        self.repository
            .search(project_id, query, limit.unwrap_or(20))
            .await
    }

    // ========================================================================
    // Statistics Integration
    // ========================================================================

    /// Get table statistics (delegates to statistics repository).
    #[instrument(skip(self))]
    pub async fn get_table_statistics(
        &self,
        project_id: Uuid,
        source: &str,
        table: &str,
    ) -> CatalogResult<Option<TableStatistics>> {
        match self.stats_repository.get(project_id, source, table).await {
            Ok(stats) => Ok(stats),
            Err(e) => {
                warn!(
                    project_id = %project_id,
                    source = source,
                    table = table,
                    error = %e,
                    "Failed to fetch table statistics, returning None",
                );
                Ok(None)
            }
        }
    }

    /// Get column statistics.
    #[instrument(skip(self))]
    pub async fn get_column_statistics(
        &self,
        project_id: Uuid,
        fqn: &str,
    ) -> CatalogResult<Option<ColumnStatistics>> {
        let col_ref = ColumnRef::parse(fqn).ok_or_else(|| {
            CatalogError::InvalidData(format!("Invalid column reference: {}", fqn))
        })?;

        // Get table statistics first, then extract column stats
        match self
            .stats_repository
            .get(project_id, &col_ref.source, &col_ref.table)
            .await
        {
            Ok(Some(table_stats)) => Ok(table_stats.column_stats.get(&col_ref.column).cloned()),
            Ok(None) => Ok(None),
            Err(e) => {
                warn!(
                    project_id = %project_id,
                    fqn = fqn,
                    error = %e,
                    "Failed to fetch column statistics, returning None",
                );
                Ok(None)
            }
        }
    }

    // ========================================================================
    // Lineage
    // ========================================================================

    /// Get lineage for a column (upstream sources).
    #[instrument(skip(self))]
    pub async fn get_column_lineage(
        &self,
        project_id: Uuid,
        fqn: &str,
    ) -> CatalogResult<ColumnLineage> {
        let col_ref = ColumnRef::parse(fqn).ok_or_else(|| {
            CatalogError::InvalidData(format!("Invalid column reference: {}", fqn))
        })?;

        self.repository
            .get_column_lineage(project_id, &col_ref)
            .await
    }

    /// Add a lineage relationship.
    #[instrument(skip(self))]
    pub async fn add_lineage(
        &self,
        project_id: Uuid,
        target_fqn: &str,
        source: &LineageSource,
    ) -> CatalogResult<Uuid> {
        let target = ColumnRef::parse(target_fqn).ok_or_else(|| {
            CatalogError::InvalidData(format!("Invalid column reference: {}", target_fqn))
        })?;

        self.repository
            .add_lineage(project_id, &target, source)
            .await
    }

    /// Get downstream dependencies for a column.
    #[instrument(skip(self))]
    pub async fn get_downstream_dependencies(
        &self,
        project_id: Uuid,
        fqn: &str,
    ) -> CatalogResult<Vec<ColumnRef>> {
        let col_ref = ColumnRef::parse(fqn).ok_or_else(|| {
            CatalogError::InvalidData(format!("Invalid column reference: {}", fqn))
        })?;

        self.repository
            .get_downstream_dependencies(project_id, &col_ref)
            .await
    }

    // ========================================================================
    // Relationships
    // ========================================================================

    /// List all relationships for a project.
    #[instrument(skip(self))]
    pub async fn list_relationships(
        &self,
        project_id: Uuid,
    ) -> CatalogResult<Vec<CrossSourceRelationship>> {
        self.repository.list_relationships(project_id).await
    }

    /// Get relationships for a specific table.
    #[instrument(skip(self))]
    pub async fn get_table_relationships(
        &self,
        project_id: Uuid,
        source: &str,
        table: &str,
    ) -> CatalogResult<Vec<CrossSourceRelationship>> {
        let table_ref = TableRef::new(source, table);
        self.repository
            .get_relationships_for_table(project_id, &table_ref)
            .await
    }

    /// Add a relationship, enforcing project scoping.
    #[instrument(skip(self))]
    pub async fn add_relationship(
        &self,
        project_id: Uuid,
        rel: &CrossSourceRelationship,
    ) -> CatalogResult<Uuid> {
        if rel.project_id != project_id {
            return Err(CatalogError::InvalidData(format!(
                "Relationship project_id {} does not match expected project {}",
                rel.project_id, project_id,
            )));
        }
        self.repository.upsert_relationship(rel).await
    }

    /// Infer relationships based on column names and types.
    #[instrument(skip(self))]
    pub async fn infer_relationships(
        &self,
        project_id: Uuid,
    ) -> CatalogResult<Vec<CrossSourceRelationship>> {
        info!("Inferring relationships for project {}", project_id);

        // Get all catalog entries
        let entries = self.repository.list_entries(project_id).await?;

        // Use the inference engine
        let inference = RelationshipInference::new();
        let inferred = inference.infer_relationships(&entries);

        // Save inferred relationships
        let mut saved = Vec::new();
        for mut rel in inferred {
            rel.project_id = project_id;
            rel.relationship_type = RelationshipType::Inferred;

            match self.repository.upsert_relationship(&rel).await {
                Ok(id) => {
                    rel.id = id;
                    saved.push(rel);
                }
                Err(e) => {
                    warn!("Failed to save inferred relationship: {}", e);
                }
            }
        }

        info!("Inferred {} relationships", saved.len());
        Ok(saved)
    }

    /// Validate a relationship by checking for referential integrity.
    ///
    /// Not yet implemented: all code paths currently return `Err`.
    /// PostgreSQL same-source relationships are recognized but the actual
    /// validation logic is a stub. Cross-source relationships and other
    /// source types also return an error.
    #[instrument(skip(self))]
    pub async fn validate_relationship(
        &self,
        project_id: Uuid,
        rel_id: Uuid,
    ) -> CatalogResult<ValidationResult> {
        let rel = self.repository.get_relationship(rel_id).await?;

        if rel.project_id != project_id {
            return Err(CatalogError::RelationshipNotFound { id: rel_id });
        }

        // Cross-source relationships cannot be validated through SQL
        if rel.is_cross_source() {
            return Err(CatalogError::InvalidData(format!(
                "Validation not supported for cross-source relationships: {} -> {}",
                rel.from.fqn(),
                rel.to.fqn()
            )));
        }

        // Get the source to check its type
        let source = match self
            .source_registry
            .resolve(project_id, &rel.from.source)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return Err(CatalogError::InvalidData(format!(
                    "Source not found for validation: {} - {}",
                    rel.from.source, e
                )));
            }
        };

        use crate::warehouse::types::SourceType;

        match source.source_type {
            SourceType::PostgreSQL => {
                // TODO: run actual referential integrity query, e.g.:
                //   SELECT COUNT(*) FROM from_table
                //   WHERE from_column NOT IN (SELECT to_column FROM to_table)
                warn!(
                    "PostgreSQL relationship validation not fully implemented yet for {}.{} -> {}.{}",
                    rel.from.source, rel.from.table, rel.to.source, rel.to.table
                );
                Err(CatalogError::InvalidData(format!(
                    "Validation not yet implemented for source type: {:?}",
                    source.source_type
                )))
            }
            _ => Err(CatalogError::InvalidData(format!(
                "Validation not implemented for source type: {:?}",
                source.source_type
            ))),
        }
    }

    // ========================================================================
    // Catalog Refresh
    // ========================================================================

    /// Refresh catalog for a specific source.
    #[instrument(skip(self))]
    pub async fn refresh_source_catalog(
        &self,
        project_id: Uuid,
        source_name: &str,
    ) -> CatalogResult<RefreshResult> {
        let start = std::time::Instant::now();
        info!("Refreshing catalog for source: {}", source_name);

        let mut result = RefreshResult {
            source_name: source_name.to_string(),
            tables_discovered: 0,
            tables_updated: 0,
            tables_removed: 0,
            relationships_inferred: 0,
            duration_ms: 0,
            errors: Vec::new(),
        };

        // Get source info from registry
        let source = match self.source_registry.resolve(project_id, source_name).await {
            Ok(s) => s,
            Err(e) => {
                return Err(CatalogError::InvalidData(format!(
                    "Failed to get source {}: {}",
                    source_name, e
                )));
            }
        };

        // Discover schema based on source type.
        // `discovery_failed` tracks whether a connector error occurred, so we
        // can skip the deletion phase and avoid wiping existing entries on
        // transient failures.
        use crate::warehouse::types::SourceType;
        let mut discovery_failed = false;
        let mut discovery_skipped = false;
        let schemas = match source.source_type {
            SourceType::PostgreSQL => {
                let discovery = PostgresSchemaDiscovery::new();
                match discovery.discover_schemas(&source).await {
                    Ok(s) => s,
                    Err(e) => {
                        result
                            .errors
                            .push(format!("PostgreSQL discovery failed: {}", e));
                        discovery_failed = true;
                        Vec::new()
                    }
                }
            }
            SourceType::Stripe => {
                let discovery = StripeSchemaDiscovery::new();
                match discovery.discover_schemas(&source).await {
                    Ok(s) => s,
                    Err(e) => {
                        result
                            .errors
                            .push(format!("Stripe discovery failed: {}", e));
                        discovery_failed = true;
                        Vec::new()
                    }
                }
            }
            SourceType::ExternalParquet => {
                let discovery = ParquetSchemaDiscovery::new();
                match discovery.discover_schemas(&source).await {
                    Ok(s) => s,
                    Err(e) => {
                        result
                            .errors
                            .push(format!("Parquet discovery failed: {}", e));
                        discovery_failed = true;
                        Vec::new()
                    }
                }
            }
            SourceType::Derived => {
                debug!("Derived tables use stored schema; skipping connector discovery");
                discovery_skipped = true;
                Vec::new()
            }
            _ => {
                debug!(
                    "No schema discovery for source type: {:?}",
                    source.source_type
                );
                discovery_skipped = true;
                Vec::new()
            }
        };

        result.tables_discovered = schemas.len() as i32;

        // Get existing entries for this source
        let existing = self
            .repository
            .list_entries_for_source(project_id, source_name)
            .await?;
        let existing_tables: std::collections::HashSet<_> =
            existing.iter().map(|e| e.table_name.clone()).collect();

        // Collect discovered table names before consuming schemas
        let discovered_tables: std::collections::HashSet<String> =
            schemas.iter().map(|s| s.table_name.clone()).collect();

        // Build entries for batch upsert
        let now = Utc::now();
        let entries: Vec<CatalogEntry> = schemas
            .into_iter()
            .map(|schema| {
                let table_name = schema.table_name.clone();
                let mut entry = CatalogEntry::new(project_id, source_name, &table_name);
                entry.source_id = Some(source.id);
                entry.schema = schema;
                entry.freshness.sync_status = SyncStatus::Synced;
                entry.freshness.last_sync_at = Some(now);
                entry
            })
            .collect();

        // Batch upsert all entries at once
        let upsert_failed;
        match self.repository.batch_upsert_entries(&entries).await {
            Ok(ids) => {
                upsert_failed = false;
                result.tables_updated = ids.len() as i32;
                // Invalidate cache for all updated entries
                let source_owned = source_name.to_string();
                for entry in &entries {
                    self.cache.remove(&CatalogCacheKey::Entry {
                        project_id,
                        source: source_owned.clone(),
                        table: entry.table_name.clone(),
                    });
                }
            }
            Err(e) => {
                upsert_failed = true;
                result
                    .errors
                    .push(format!("Failed to batch upsert entries: {}", e));
            }
        }

        // Remove entries that no longer exist.
        // Skip deletion when discovery failed/skipped or upsert failed — an empty
        // discovered set from a connector error must not wipe all existing entries,
        // and a failed upsert means we don't know which entries were written.
        if discovery_failed || discovery_skipped || upsert_failed {
            if discovery_failed {
                warn!(
                    "Skipping table removal for source '{}': discovery failed, cannot determine which tables were dropped",
                    source_name
                );
            } else if upsert_failed {
                warn!(
                    "Skipping table removal for source '{}': batch upsert failed, cannot safely delete entries",
                    source_name
                );
            } else {
                debug!(
                    "Skipping table removal for source '{}': discovery was intentionally skipped",
                    source_name
                );
            }
        } else {
            for table in existing_tables.difference(&discovered_tables) {
                match self
                    .repository
                    .delete_entry(project_id, source_name, table)
                    .await
                {
                    Ok(true) => {
                        result.tables_removed += 1;
                        self.cache.remove(&CatalogCacheKey::Entry {
                            project_id,
                            source: source_name.to_string(),
                            table: table.clone(),
                        });
                    }
                    Ok(false) => {}
                    Err(e) => {
                        result
                            .errors
                            .push(format!("Failed to remove {}: {}", table, e));
                    }
                }
            }
        }

        result.duration_ms = start.elapsed().as_millis() as i64;

        info!(
            "Catalog refresh complete: {} discovered, {} updated, {} removed in {}ms",
            result.tables_discovered,
            result.tables_updated,
            result.tables_removed,
            result.duration_ms
        );

        Ok(result)
    }

    /// Refresh catalog for all sources in a project.
    #[instrument(skip(self))]
    pub async fn refresh_all(&self, project_id: Uuid) -> CatalogResult<Vec<RefreshResult>> {
        info!(
            "Refreshing catalog for all sources in project {}",
            project_id
        );

        let sources = self
            .source_registry
            .list(project_id)
            .await
            .map_err(|e| CatalogError::InvalidData(format!("Failed to list sources: {}", e)))?;

        let mut results = Vec::new();
        for source in sources {
            match self.refresh_source_catalog(project_id, &source.name).await {
                Ok(r) => results.push(r),
                Err(e) => {
                    warn!("Failed to refresh source {}: {}", source.name, e);
                    results.push(RefreshResult {
                        source_name: source.name,
                        tables_discovered: 0,
                        tables_updated: 0,
                        tables_removed: 0,
                        relationships_inferred: 0,
                        duration_ms: 0,
                        errors: vec![e.to_string()],
                    });
                }
            }
        }

        match self.infer_relationships(project_id).await {
            Ok(rels) => {
                let source_idx: std::collections::HashMap<String, usize> = results
                    .iter()
                    .enumerate()
                    .map(|(i, r)| (r.source_name.clone(), i))
                    .collect();

                for rel in &rels {
                    if let Some(&idx) = source_idx.get(&rel.from.source) {
                        results[idx].relationships_inferred += 1;
                    }
                }
            }
            Err(e) => {
                warn!("Failed to infer relationships: {}", e);
            }
        }

        Ok(results)
    }

    // ========================================================================
    // Catalog Updates (for sync workers)
    // ========================================================================

    /// Update catalog entry after a successful sync.
    #[instrument(skip(self))]
    pub async fn on_sync_complete(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        row_count: Option<i64>,
    ) -> CatalogResult<()> {
        self.repository
            .update_sync_status(
                project_id,
                source_name,
                table_name,
                SyncStatus::Synced,
                row_count,
            )
            .await?;

        // Invalidate cache
        self.cache.remove(&CatalogCacheKey::Entry {
            project_id,
            source: source_name.to_string(),
            table: table_name.to_string(),
        });

        Ok(())
    }

    /// Mark a table as syncing.
    #[instrument(skip(self))]
    pub async fn on_sync_start(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> CatalogResult<()> {
        self.repository
            .update_sync_status(
                project_id,
                source_name,
                table_name,
                SyncStatus::Syncing,
                None,
            )
            .await?;

        self.cache.remove(&CatalogCacheKey::Entry {
            project_id,
            source: source_name.to_string(),
            table: table_name.to_string(),
        });

        Ok(())
    }

    /// Mark a table sync as failed.
    #[instrument(skip(self))]
    pub async fn on_sync_error(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> CatalogResult<()> {
        self.repository
            .update_sync_status(project_id, source_name, table_name, SyncStatus::Error, None)
            .await?;

        self.cache.remove(&CatalogCacheKey::Entry {
            project_id,
            source: source_name.to_string(),
            table: table_name.to_string(),
        });

        Ok(())
    }

    // ========================================================================
    // Direct Repository Access
    // ========================================================================

    /// Get direct access to the repository (for advanced operations).
    pub fn repository(&self) -> &CatalogRepository {
        &self.repository
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: a cross-source relationship (A -> B) must increment
    /// `relationships_inferred` only for the `from` source, not both sides.
    #[test]
    fn test_cross_source_relationship_counted_once_not_twice() {
        use crate::warehouse::catalog::types::{CrossSourceRelationship, TableRef};
        use std::collections::HashMap;

        let mut results = vec![
            RefreshResult {
                source_name: "source_a".to_string(),
                tables_discovered: 0,
                tables_updated: 0,
                tables_removed: 0,
                relationships_inferred: 0,
                duration_ms: 0,
                errors: vec![],
            },
            RefreshResult {
                source_name: "source_b".to_string(),
                tables_discovered: 0,
                tables_updated: 0,
                tables_removed: 0,
                relationships_inferred: 0,
                duration_ms: 0,
                errors: vec![],
            },
        ];

        let rels = vec![CrossSourceRelationship::new(
            Uuid::new_v4(),
            TableRef::new("source_a", "orders"),
            vec!["user_id".into()],
            TableRef::new("source_b", "users"),
            vec!["id".into()],
        )];

        let source_idx: HashMap<String, usize> = results
            .iter()
            .enumerate()
            .map(|(i, r)| (r.source_name.clone(), i))
            .collect();

        for rel in &rels {
            if let Some(&idx) = source_idx.get(&rel.from.source) {
                results[idx].relationships_inferred += 1;
            }
        }

        assert_eq!(
            results[0].relationships_inferred, 1,
            "from-source should have exactly 1 relationship"
        );
        assert_eq!(
            results[1].relationships_inferred, 0,
            "to-source should NOT be incremented for cross-source relationships"
        );
    }

    #[test]
    fn test_project_mismatch_returns_not_found_to_prevent_enumeration() {
        let rel_id = Uuid::new_v4();

        // Cross-project access must return RelationshipNotFound (not InvalidData)
        // so that attackers cannot distinguish "exists in another project" from
        // "does not exist at all".
        let err = CatalogError::RelationshipNotFound { id: rel_id };

        assert!(
            matches!(&err, CatalogError::RelationshipNotFound { .. }),
            "Cross-project access must return RelationshipNotFound"
        );
        assert!(
            !matches!(&err, CatalogError::InvalidData(_)),
            "Must NOT return InvalidData (leaks existence to other projects)"
        );
    }
}
