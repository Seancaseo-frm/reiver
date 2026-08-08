//! Unified Catalog REST API
//!
//! Provides endpoints for accessing catalog metadata, lineage, and relationships.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::PondState;
use crate::error::{AppError, Result};
use crate::warehouse::catalog::types::{
    Cardinality, ColumnRef, RelationshipType, SearchResult, SourceSummary,
    TableRef, TableSummary, TransformationType,
};
use crate::warehouse::catalog::{
    CatalogEntry, ColumnLineage, CrossSourceRelationship, LineageSource,
};

/// Create catalog routes.
/// All routes are nested under /projects/:project_id/catalog
pub fn routes() -> Router<Arc<PondState>> {
    Router::new()
        // Sources
        .route("/projects/{project_id}/catalog/sources", get(list_sources))
        // Tables
        .route("/projects/{project_id}/catalog/sources/{source}/tables", get(list_tables))
        .route("/projects/{project_id}/catalog/sources/{source}/tables/{table}", get(get_table_schema))
        // Columns
        .route("/projects/{project_id}/catalog/sources/{source}/tables/{table}/columns/{column}", get(get_column))
        // Search
        .route("/projects/{project_id}/catalog/search", get(search_catalog))
        // Relationships
        .route("/projects/{project_id}/catalog/relationships", get(list_relationships).post(create_relationship))
        .route("/projects/{project_id}/catalog/relationships/{rel_id}", get(get_relationship).delete(delete_relationship))
        .route("/projects/{project_id}/catalog/relationships/{rel_id}/validate", post(validate_relationship))
        .route("/projects/{project_id}/catalog/relationships/infer", post(infer_relationships))
        // Lineage
        .route("/projects/{project_id}/catalog/sources/{source}/tables/{table}/columns/{column}/lineage", get(get_column_lineage))
        .route("/projects/{project_id}/catalog/lineage", post(add_lineage))
        .route("/projects/{project_id}/catalog/sources/{source}/tables/{table}/columns/{column}/downstream", get(get_downstream))
        // Refresh
        .route("/projects/{project_id}/catalog/refresh", post(refresh_all))
        .route("/projects/{project_id}/catalog/refresh/{source}", post(refresh_source))
}

// ============================================================================
// Request/Response Types
// ============================================================================

/// Response for a source summary.
#[derive(Debug, Serialize)]
pub struct SourceSummaryResponse {
    pub name: String,
    pub source_type: String,
    pub table_count: i64,
    pub total_rows: Option<i64>,
    pub last_sync_at: Option<String>,
    pub sync_status: String,
}

impl From<SourceSummary> for SourceSummaryResponse {
    fn from(s: SourceSummary) -> Self {
        Self {
            name: s.name,
            source_type: s.source_type.to_string(),
            table_count: s.table_count,
            total_rows: s.total_rows,
            last_sync_at: s.last_sync_at.map(|t| t.to_rfc3339()),
            sync_status: s.sync_status.as_str().to_string(),
        }
    }
}

/// Response for a table summary.
#[derive(Debug, Serialize)]
pub struct TableSummaryResponse {
    pub source_name: String,
    pub table_name: String,
    pub column_count: i32,
    pub row_count_estimate: Option<i64>,
    pub size_bytes_estimate: Option<i64>,
    pub sync_status: String,
    pub last_sync_at: Option<String>,
    pub description: Option<String>,
}

impl From<TableSummary> for TableSummaryResponse {
    fn from(t: TableSummary) -> Self {
        Self {
            source_name: t.source_name,
            table_name: t.table_name,
            column_count: t.column_count,
            row_count_estimate: t.row_count_estimate,
            size_bytes_estimate: t.size_bytes_estimate,
            sync_status: t.sync_status.as_str().to_string(),
            last_sync_at: t.last_sync_at.map(|t| t.to_rfc3339()),
            description: t.description,
        }
    }
}

/// Response for a catalog entry (table schema).
#[derive(Debug, Serialize)]
pub struct CatalogEntryResponse {
    pub id: Uuid,
    pub source_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnResponse>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub row_count_estimate: Option<i64>,
    pub sync_status: String,
    pub last_sync_at: Option<String>,
    pub discovered_at: String,
    pub updated_at: String,
}

impl From<CatalogEntry> for CatalogEntryResponse {
    fn from(e: CatalogEntry) -> Self {
        Self {
            id: e.id,
            source_name: e.source_name,
            table_name: e.table_name,
            columns: e.schema.columns.iter().map(|c| ColumnResponse {
                name: c.name.clone(),
                data_type: c.source_type_name.clone(),
                nullable: c.nullable,
                description: c.description.clone(),
            }).collect(),
            description: e.description,
            tags: e.tags,
            row_count_estimate: e.freshness.row_count_estimate,
            sync_status: e.freshness.sync_status.as_str().to_string(),
            last_sync_at: e.freshness.last_sync_at.map(|t| t.to_rfc3339()),
            discovered_at: e.discovered_at.to_rfc3339(),
            updated_at: e.updated_at.to_rfc3339(),
        }
    }
}

/// Response for a column.
#[derive(Debug, Serialize)]
pub struct ColumnResponse {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub description: Option<String>,
}

/// Search query parameters.
#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: String,
    #[serde(default = "default_limit")]
    pub limit: i32,
}

fn default_limit() -> i32 {
    20
}

/// Response for a relationship.
#[derive(Debug, Serialize)]
pub struct RelationshipResponse {
    pub id: Uuid,
    pub name: Option<String>,
    pub from_source: String,
    pub from_table: String,
    pub from_columns: Vec<String>,
    pub to_source: String,
    pub to_table: String,
    pub to_columns: Vec<String>,
    pub relationship_type: String,
    pub cardinality: String,
    pub confidence: f32,
    pub is_validated: bool,
    pub last_validated_at: Option<String>,
    pub violation_count: i32,
}

impl From<CrossSourceRelationship> for RelationshipResponse {
    fn from(r: CrossSourceRelationship) -> Self {
        Self {
            id: r.id,
            name: r.name,
            from_source: r.from.source,
            from_table: r.from.table,
            from_columns: r.from_columns,
            to_source: r.to.source,
            to_table: r.to.table,
            to_columns: r.to_columns,
            relationship_type: r.relationship_type.as_str().to_string(),
            cardinality: r.cardinality.as_str().to_string(),
            confidence: r.confidence,
            is_validated: r.is_validated,
            last_validated_at: r.last_validated_at.map(|t| t.to_rfc3339()),
            violation_count: r.violation_count,
        }
    }
}

/// Request to create a relationship.
#[derive(Debug, Deserialize)]
pub struct CreateRelationshipRequest {
    pub name: Option<String>,
    pub from_source: String,
    pub from_table: String,
    pub from_columns: Vec<String>,
    pub to_source: String,
    pub to_table: String,
    pub to_columns: Vec<String>,
    #[serde(default = "default_rel_type")]
    pub relationship_type: String,
    #[serde(default = "default_cardinality")]
    pub cardinality: String,
}

fn default_rel_type() -> String {
    "manual".to_string()
}

fn default_cardinality() -> String {
    "unknown".to_string()
}

/// Response for column lineage.
#[derive(Debug, Serialize)]
pub struct LineageResponse {
    pub target: String,
    pub sources: Vec<LineageSourceResponse>,
}

impl From<ColumnLineage> for LineageResponse {
    fn from(l: ColumnLineage) -> Self {
        Self {
            target: l.target.fqn(),
            sources: l.sources.into_iter().map(LineageSourceResponse::from).collect(),
        }
    }
}

/// Response for a lineage source.
#[derive(Debug, Serialize)]
pub struct LineageSourceResponse {
    pub column: String,
    pub transformation_type: String,
    pub transformation_sql: Option<String>,
    pub confidence: f32,
}

impl From<LineageSource> for LineageSourceResponse {
    fn from(s: LineageSource) -> Self {
        Self {
            column: s.column.fqn(),
            transformation_type: s.transformation_type.as_str().to_string(),
            transformation_sql: s.transformation_sql,
            confidence: s.confidence,
        }
    }
}

/// Request to add lineage.
#[derive(Debug, Deserialize)]
pub struct AddLineageRequest {
    pub target: String,  // source.table.column
    pub source_column: String,  // source.table.column
    #[serde(default = "default_transformation")]
    pub transformation_type: String,
    pub transformation_sql: Option<String>,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
}

fn default_transformation() -> String {
    "direct".to_string()
}

fn default_confidence() -> f32 {
    1.0
}

/// Response for a refresh operation.
#[derive(Debug, Serialize)]
pub struct RefreshResponse {
    pub source_name: String,
    pub tables_discovered: i32,
    pub tables_updated: i32,
    pub tables_removed: i32,
    pub relationships_inferred: i32,
    pub duration_ms: i64,
    pub errors: Vec<String>,
}

/// Response for validation.
#[derive(Debug, Serialize)]
pub struct ValidationResponse {
    pub is_valid: bool,
    pub violation_count: i64,
    pub total_checked: i64,
    pub sample_violations: Vec<String>,
}

// ============================================================================
// Path Parameters
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ProjectPath {
    pub project_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct SourcePath {
    pub project_id: Uuid,
    pub source: String,
}

#[derive(Debug, Deserialize)]
pub struct TablePath {
    pub project_id: Uuid,
    pub source: String,
    pub table: String,
}

#[derive(Debug, Deserialize)]
pub struct ColumnPath {
    pub project_id: Uuid,
    pub source: String,
    pub table: String,
    pub column: String,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipPath {
    pub project_id: Uuid,
    pub rel_id: Uuid,
}

// ============================================================================
// Handlers
// ============================================================================

/// List all sources with summary information.
#[tracing::instrument(name = "catalog.api.list_sources", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn list_sources(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ProjectPath>,
) -> Result<Json<Vec<SourceSummaryResponse>>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let sources = catalog.list_sources(path.project_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to list sources: {}", e)))?;

    Ok(Json(sources.into_iter().map(SourceSummaryResponse::from).collect()))
}

/// List tables for a source.
#[tracing::instrument(name = "catalog.api.list_tables", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn list_tables(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<Vec<TableSummaryResponse>>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let tables = catalog.list_tables(path.project_id, &path.source).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to list tables: {}", e)))?;

    Ok(Json(tables.into_iter().map(TableSummaryResponse::from).collect()))
}

/// Get table schema and metadata.
#[tracing::instrument(name = "catalog.api.get_table_schema", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn get_table_schema(
    State(state): State<Arc<PondState>>,
    Path(path): Path<TablePath>,
) -> Result<Json<CatalogEntryResponse>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let entry = catalog.get_table_schema(path.project_id, &path.source, &path.table).await
        .map_err(|e| AppError::NotFound(format!("Table not found: {}", e)))?;

    Ok(Json(CatalogEntryResponse::from(entry)))
}

/// Get column details.
#[tracing::instrument(name = "catalog.api.get_column", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn get_column(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ColumnPath>,
) -> Result<Json<ColumnResponse>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let fqn = format!("{}.{}.{}", path.source, path.table, path.column);
    let column = catalog.get_column_type(path.project_id, &fqn).await
        .map_err(|e| AppError::NotFound(format!("Column not found: {}", e)))?;

    Ok(Json(ColumnResponse {
        name: column.name,
        data_type: column.source_type_name,
        nullable: column.nullable,
        description: column.description,
    }))
}

/// Search the catalog.
#[tracing::instrument(name = "catalog.api.search_catalog", skip(state, query), fields(project_id = %path.project_id), err(Display))]
async fn search_catalog(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ProjectPath>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<Vec<SearchResult>>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let results = catalog.search_catalog(path.project_id, &query.q, Some(query.limit)).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Search failed: {}", e)))?;

    Ok(Json(results))
}

/// List all relationships.
#[tracing::instrument(name = "catalog.api.list_relationships", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn list_relationships(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ProjectPath>,
) -> Result<Json<Vec<RelationshipResponse>>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let relationships = catalog.list_relationships(path.project_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to list relationships: {}", e)))?;

    Ok(Json(relationships.into_iter().map(RelationshipResponse::from).collect()))
}

/// Get a relationship by ID.
#[tracing::instrument(name = "catalog.api.get_relationship", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn get_relationship(
    State(state): State<Arc<PondState>>,
    Path(path): Path<RelationshipPath>,
) -> Result<Json<RelationshipResponse>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let rel = catalog.repository().get_relationship(path.rel_id).await
        .map_err(|e| AppError::NotFound(format!("Relationship not found: {}", e)))?;

    // Verify the relationship belongs to this project to prevent cross-project access
    if rel.project_id != path.project_id {
        return Err(AppError::NotFound(format!(
            "Relationship {} not found in project {}",
            path.rel_id, path.project_id
        )));
    }

    Ok(Json(RelationshipResponse::from(rel)))
}

/// Create a new relationship.
#[tracing::instrument(name = "catalog.api.create_relationship", skip(state, req), fields(project_id = %path.project_id), err(Display))]
async fn create_relationship(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ProjectPath>,
    Json(req): Json<CreateRelationshipRequest>,
) -> Result<Json<RelationshipResponse>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let mut rel = CrossSourceRelationship::new(
        path.project_id,
        TableRef::new(&req.from_source, &req.from_table),
        req.from_columns,
        TableRef::new(&req.to_source, &req.to_table),
        req.to_columns,
    );
    
    rel.name = req.name;
    rel.relationship_type = RelationshipType::from_str(&req.relationship_type);
    rel.cardinality = Cardinality::from_str(&req.cardinality);

    let id = catalog.add_relationship(path.project_id, &rel).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create relationship: {}", e)))?;

    rel.id = id;

    Ok(Json(RelationshipResponse::from(rel)))
}

/// Delete a relationship.
#[tracing::instrument(name = "catalog.api.delete_relationship", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn delete_relationship(
    State(state): State<Arc<PondState>>,
    Path(path): Path<RelationshipPath>,
) -> Result<StatusCode> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    // First verify the relationship belongs to this project
    let rel = catalog.repository().get_relationship(path.rel_id).await
        .map_err(|e| AppError::NotFound(format!("Relationship not found: {}", e)))?;
    
    if rel.project_id != path.project_id {
        return Err(AppError::NotFound(format!(
            "Relationship {} not found in project {}",
            path.rel_id, path.project_id
        )));
    }

    catalog.repository().delete_relationship(path.rel_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to delete relationship: {}", e)))?;

    Ok(StatusCode::NO_CONTENT)
}

/// Validate a relationship.
#[tracing::instrument(name = "catalog.api.validate_relationship", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn validate_relationship(
    State(state): State<Arc<PondState>>,
    Path(path): Path<RelationshipPath>,
) -> Result<Json<ValidationResponse>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    // Verify ownership before validation to prevent cross-project access
    let rel = catalog.repository().get_relationship(path.rel_id).await
        .map_err(|e| AppError::NotFound(format!("Relationship not found: {}", e)))?;
    if rel.project_id != path.project_id {
        return Err(AppError::NotFound(format!(
            "Relationship {} not found in project {}",
            path.rel_id, path.project_id
        )));
    }

    let result = catalog.validate_relationship(path.project_id, path.rel_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Validation failed: {}", e)))?;

    Ok(Json(ValidationResponse {
        is_valid: result.is_valid,
        violation_count: result.violation_count,
        total_checked: result.total_checked,
        sample_violations: result.sample_violations,
    }))
}

/// Infer relationships automatically.
#[tracing::instrument(name = "catalog.api.infer_relationships", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn infer_relationships(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ProjectPath>,
) -> Result<Json<Vec<RelationshipResponse>>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let relationships = catalog.infer_relationships(path.project_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Inference failed: {}", e)))?;

    Ok(Json(relationships.into_iter().map(RelationshipResponse::from).collect()))
}

/// Get column lineage.
#[tracing::instrument(name = "catalog.api.get_column_lineage", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn get_column_lineage(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ColumnPath>,
) -> Result<Json<LineageResponse>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let fqn = format!("{}.{}.{}", path.source, path.table, path.column);
    let lineage = catalog.get_column_lineage(path.project_id, &fqn).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to get lineage: {}", e)))?;

    Ok(Json(LineageResponse::from(lineage)))
}

/// Add lineage relationship.
#[tracing::instrument(name = "catalog.api.add_lineage", skip(state, req), fields(project_id = %path.project_id), err(Display))]
async fn add_lineage(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ProjectPath>,
    Json(req): Json<AddLineageRequest>,
) -> Result<StatusCode> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let source_col = ColumnRef::parse(&req.source_column)
        .ok_or_else(|| AppError::BadRequest("Invalid source column reference".to_string()))?;

    let source = LineageSource::new(source_col, TransformationType::from_str(&req.transformation_type))
        .with_sql(req.transformation_sql.unwrap_or_default())
        .with_confidence(req.confidence);

    catalog.add_lineage(path.project_id, &req.target, &source).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to add lineage: {}", e)))?;

    Ok(StatusCode::CREATED)
}

/// Get downstream dependencies.
#[tracing::instrument(name = "catalog.api.get_downstream", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn get_downstream(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ColumnPath>,
) -> Result<Json<Vec<String>>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let fqn = format!("{}.{}.{}", path.source, path.table, path.column);
    let deps = catalog.get_downstream_dependencies(path.project_id, &fqn).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to get downstream: {}", e)))?;

    Ok(Json(deps.iter().map(|c| c.fqn()).collect()))
}

/// Refresh catalog for all sources.
#[tracing::instrument(name = "catalog.api.refresh_all", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn refresh_all(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ProjectPath>,
) -> Result<Json<Vec<RefreshResponse>>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let results = catalog.refresh_all(path.project_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Refresh failed: {}", e)))?;

    Ok(Json(results.into_iter().map(|r| RefreshResponse {
        source_name: r.source_name,
        tables_discovered: r.tables_discovered,
        tables_updated: r.tables_updated,
        tables_removed: r.tables_removed,
        relationships_inferred: r.relationships_inferred,
        duration_ms: r.duration_ms,
        errors: r.errors,
    }).collect()))
}

/// Refresh catalog for a specific source.
#[tracing::instrument(name = "catalog.api.refresh_source", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn refresh_source(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<RefreshResponse>> {

    let catalog = state.catalog_service.as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Catalog service not initialized")))?;

    let result = catalog.refresh_source_catalog(path.project_id, &path.source).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Refresh failed: {}", e)))?;

    Ok(Json(RefreshResponse {
        source_name: result.source_name,
        tables_discovered: result.tables_discovered,
        tables_updated: result.tables_updated,
        tables_removed: result.tables_removed,
        relationships_inferred: result.relationships_inferred,
        duration_ms: result.duration_ms,
        errors: result.errors,
    }))
}
