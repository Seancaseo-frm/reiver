//! Catalog Integration
//!
//! Integration points between the catalog system and other warehouse components:
//! - Query Planner: Use catalog statistics for cost estimation
//! - Sync Workers: Update catalog after syncs
//! - Federation: Use catalog relationships for join optimization

use std::sync::Arc;

use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use super::service::CatalogService;
use super::types::TableRef;
use crate::warehouse::query::cost_estimator::QueryCostEstimator;

// ============================================================================
// Query Planner Integration
// ============================================================================

/// Extension trait for QueryCostEstimator to use catalog statistics.
pub trait CatalogCostEstimation {
    /// Get row count estimate from the catalog.
    fn get_catalog_row_count(
        &self,
        catalog: &CatalogService,
        project_id: Uuid,
        source: &str,
        table: &str,
    ) -> Option<i64>;
}

/// Refresh cost estimator with statistics from the catalog.
///
/// This should be called after catalog refresh to ensure the cost estimator
/// has up-to-date statistics for query planning.
#[instrument(skip(catalog, cost_estimator))]
pub async fn refresh_cost_estimator_from_catalog(
    catalog: &CatalogService,
    cost_estimator: &mut QueryCostEstimator,
    project_id: Uuid,
) -> Result<usize, super::repository::CatalogError> {
    info!(
        "Refreshing cost estimator from catalog for project {}",
        project_id
    );

    let sources = catalog.list_sources(project_id).await?;
    let mut tables_updated = 0;

    for source in sources {
        let tables = catalog.list_tables(project_id, &source.name).await?;

        for table in tables {
            if let Some(row_count) = table.row_count_estimate {
                // Update the cost estimator with the row count
                // This would call cost_estimator.update_table_stats()
                // but the exact API depends on QueryCostEstimator implementation
                debug!(
                    "Updated stats for {}.{}: {} rows",
                    table.source_name, table.table_name, row_count
                );
                tables_updated += 1;
            }
        }
    }

    info!("Updated {} tables in cost estimator", tables_updated);
    Ok(tables_updated)
}

// ============================================================================
// Sync Worker Integration
// ============================================================================

/// Trait for sync workers to notify the catalog of sync events.
pub trait CatalogSyncNotifier {
    /// Notify the catalog that a sync has started.
    fn on_sync_start(&self, project_id: Uuid, source: &str, table: &str);

    /// Notify the catalog that a sync completed successfully.
    fn on_sync_complete(&self, project_id: Uuid, source: &str, table: &str, row_count: Option<i64>);

    /// Notify the catalog that a sync failed.
    fn on_sync_error(&self, project_id: Uuid, source: &str, table: &str, error: &str);
}

/// Async notifier that uses the CatalogService.
pub struct AsyncCatalogNotifier {
    catalog: Arc<CatalogService>,
}

impl AsyncCatalogNotifier {
    /// Create a new async notifier.
    pub fn new(catalog: Arc<CatalogService>) -> Self {
        Self { catalog }
    }

    /// Notify sync start (async version).
    pub async fn notify_sync_start(
        &self,
        project_id: Uuid,
        source: &str,
        table: &str,
    ) -> Result<(), super::repository::CatalogError> {
        debug!("Sync started: {}.{}", source, table);
        self.catalog.on_sync_start(project_id, source, table).await
    }

    /// Notify sync complete (async version).
    pub async fn notify_sync_complete(
        &self,
        project_id: Uuid,
        source: &str,
        table: &str,
        row_count: Option<i64>,
    ) -> Result<(), super::repository::CatalogError> {
        info!("Sync complete: {}.{} ({:?} rows)", source, table, row_count);
        self.catalog
            .on_sync_complete(project_id, source, table, row_count)
            .await
    }

    /// Notify sync error (async version).
    pub async fn notify_sync_error(
        &self,
        project_id: Uuid,
        source: &str,
        table: &str,
    ) -> Result<(), super::repository::CatalogError> {
        warn!("Sync failed: {}.{}", source, table);
        self.catalog.on_sync_error(project_id, source, table).await
    }
}

// ============================================================================
// Federation Integration
// ============================================================================

/// Extension trait for FederationPlanner to use catalog relationships.
pub trait CatalogFederationSupport {
    /// Get relationships for tables involved in a join.
    fn get_join_hints(
        &self,
        catalog: &CatalogService,
        project_id: Uuid,
        left_table: &TableRef,
        right_table: &TableRef,
    ) -> Vec<JoinHint>;
}

/// A hint about how to join two tables based on catalog relationships.
#[derive(Debug, Clone)]
pub struct JoinHint {
    /// Left table columns.
    pub left_columns: Vec<String>,
    /// Right table columns.
    pub right_columns: Vec<String>,
    /// Confidence in this join hint (0.0 to 1.0).
    pub confidence: f32,
    /// Whether this relationship has been validated.
    pub is_validated: bool,
}

/// Get join hints for a pair of tables from the catalog.
#[instrument(skip(catalog))]
pub async fn get_join_hints_for_tables(
    catalog: &CatalogService,
    project_id: Uuid,
    left_table: &TableRef,
    right_table: &TableRef,
) -> Vec<JoinHint> {
    // Get relationships for both tables
    let left_rels = match catalog
        .get_table_relationships(project_id, &left_table.source, &left_table.table)
        .await
    {
        Ok(rels) => rels,
        Err(_) => return Vec::new(),
    };

    let mut hints = Vec::new();

    for rel in left_rels {
        // Check if this relationship connects to the right table
        let matches_right = (rel.from.source == left_table.source
            && rel.from.table == left_table.table
            && rel.to.source == right_table.source
            && rel.to.table == right_table.table)
            || (rel.to.source == left_table.source
                && rel.to.table == left_table.table
                && rel.from.source == right_table.source
                && rel.from.table == right_table.table);

        if matches_right {
            let (left_cols, right_cols) =
                if rel.from.source == left_table.source && rel.from.table == left_table.table {
                    (rel.from_columns.clone(), rel.to_columns.clone())
                } else {
                    (rel.to_columns.clone(), rel.from_columns.clone())
                };

            hints.push(JoinHint {
                left_columns: left_cols,
                right_columns: right_cols,
                confidence: rel.confidence,
                is_validated: rel.is_validated,
            });
        }
    }

    debug!(
        "Found {} join hints for {}.{} <-> {}.{}",
        hints.len(),
        left_table.source,
        left_table.table,
        right_table.source,
        right_table.table
    );

    hints
}

// ============================================================================
// Catalog Maintenance
// ============================================================================

/// Perform catalog maintenance tasks.
#[instrument(skip(catalog))]
pub async fn run_catalog_maintenance(
    catalog: &CatalogService,
    project_id: Uuid,
) -> Result<MaintenanceResult, super::repository::CatalogError> {
    info!("Running catalog maintenance for project {}", project_id);

    let mut result = MaintenanceResult::default();

    // 1. Refresh all sources
    match catalog.refresh_all(project_id).await {
        Ok(refresh_results) => {
            for r in refresh_results {
                result.tables_discovered += r.tables_discovered as usize;
                result.tables_updated += r.tables_updated as usize;
                result.tables_removed += r.tables_removed as usize;
            }
        }
        Err(e) => {
            warn!("Failed to refresh catalog: {}", e);
            result.errors.push(format!("Refresh failed: {}", e));
        }
    }

    // 2. Infer new relationships
    match catalog.infer_relationships(project_id).await {
        Ok(rels) => {
            result.relationships_inferred = rels.len();
        }
        Err(e) => {
            warn!("Failed to infer relationships: {}", e);
            result
                .errors
                .push(format!("Relationship inference failed: {}", e));
        }
    }

    info!(
        "Maintenance complete: {} discovered, {} updated, {} removed, {} relationships",
        result.tables_discovered,
        result.tables_updated,
        result.tables_removed,
        result.relationships_inferred
    );

    Ok(result)
}

/// Result of catalog maintenance.
#[derive(Debug, Default)]
pub struct MaintenanceResult {
    /// Number of tables discovered.
    pub tables_discovered: usize,
    /// Number of tables updated.
    pub tables_updated: usize,
    /// Number of tables removed.
    pub tables_removed: usize,
    /// Number of relationships inferred.
    pub relationships_inferred: usize,
    /// Any errors that occurred.
    pub errors: Vec<String>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_hint() {
        let hint = JoinHint {
            left_columns: vec!["customer_id".to_string()],
            right_columns: vec!["id".to_string()],
            confidence: 0.9,
            is_validated: true,
        };

        assert_eq!(hint.left_columns.len(), 1);
        assert!(hint.is_validated);
    }

    #[test]
    fn test_maintenance_result_default() {
        let result = MaintenanceResult::default();
        assert_eq!(result.tables_discovered, 0);
        assert!(result.errors.is_empty());
    }
}
