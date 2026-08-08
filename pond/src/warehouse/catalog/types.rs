//! Unified Catalog Types
//!
//! Core data structures for the catalog system.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::warehouse::types::{SourceType, TypedSchema};

// ============================================================================
// Sync Status
// ============================================================================

/// Sync status for a catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncStatus {
    /// Data is up to date with the source.
    Synced,
    /// Sync is currently in progress.
    Syncing,
    /// Data is outdated and needs refresh.
    Stale,
    /// Last sync failed with an error.
    Error,
    /// Status is unknown (never synced).
    Unknown,
}

impl SyncStatus {
    /// Convert to database string.
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncStatus::Synced => "synced",
            SyncStatus::Syncing => "syncing",
            SyncStatus::Stale => "stale",
            SyncStatus::Error => "error",
            SyncStatus::Unknown => "unknown",
        }
    }

    /// Parse from database string (backwards compat helper).
    pub fn from_str(s: &str) -> Self {
        s.parse().unwrap_or_default()
    }
}

impl std::fmt::Display for SyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for SyncStatus {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "synced" => SyncStatus::Synced,
            "syncing" => SyncStatus::Syncing,
            "stale" => SyncStatus::Stale,
            "error" => SyncStatus::Error,
            _ => SyncStatus::Unknown,
        })
    }
}

impl Default for SyncStatus {
    fn default() -> Self {
        SyncStatus::Unknown
    }
}

// ============================================================================
// Freshness Info
// ============================================================================

/// Information about data freshness for a catalog entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessInfo {
    /// When the data was last synced.
    pub last_sync_at: Option<DateTime<Utc>>,
    /// Current sync status.
    pub sync_status: SyncStatus,
    /// Estimated row count.
    pub row_count_estimate: Option<i64>,
    /// Estimated size in bytes.
    pub size_bytes_estimate: Option<i64>,
}

impl FreshnessInfo {
    /// Create new freshness info.
    pub fn new() -> Self {
        Self {
            last_sync_at: None,
            sync_status: SyncStatus::Unknown,
            row_count_estimate: None,
            size_bytes_estimate: None,
        }
    }

    /// Calculate staleness duration.
    pub fn staleness(&self) -> Option<Duration> {
        self.last_sync_at.map(|sync_at| Utc::now().signed_duration_since(sync_at))
    }

    /// Check if data is considered stale (older than threshold).
    pub fn is_stale(&self, threshold: Duration) -> bool {
        match self.staleness() {
            Some(age) => age > threshold,
            None => true, // Never synced is considered stale
        }
    }
}

impl Default for FreshnessInfo {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Catalog Entry
// ============================================================================

/// A table entry in the unified catalog.
///
/// Represents a single table from any data source with full schema,
/// metadata, and freshness information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    /// Unique identifier for this catalog entry.
    pub id: Uuid,
    /// Project this entry belongs to.
    pub project_id: Uuid,
    /// Source ID (if registered in warehouse_sources).
    pub source_id: Option<Uuid>,
    /// Source name (e.g., "stripe", "postgres", "events").
    pub source_name: String,
    /// Table name.
    pub table_name: String,
    /// Full schema with typed columns.
    pub schema: TypedSchema,
    /// Human-readable description.
    pub description: Option<String>,
    /// Tags for categorization and search.
    pub tags: Vec<String>,
    /// Freshness information.
    pub freshness: FreshnessInfo,
    /// Column names marked for full-text (substring) search.
    pub fulltext_columns: Vec<String>,
    /// When this entry was first discovered.
    pub discovered_at: DateTime<Utc>,
    /// When this entry was last updated.
    pub updated_at: DateTime<Utc>,
}

impl CatalogEntry {
    /// Create a new catalog entry.
    pub fn new(
        project_id: Uuid,
        source_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        let source_name = source_name.into();
        let table_name = table_name.into();
        let now = Utc::now();

        Self {
            id: Uuid::new_v4(),
            project_id,
            source_id: None,
            source_name: source_name.clone(),
            table_name: table_name.clone(),
            schema: TypedSchema::new(&table_name, &source_name),
            description: None,
            tags: Vec::new(),
            freshness: FreshnessInfo::new(),
            fulltext_columns: Vec::new(),
            discovered_at: now,
            updated_at: now,
        }
    }

    /// Get the fully qualified table name (source.table).
    pub fn fqn(&self) -> String {
        format!("{}.{}", self.source_name, self.table_name)
    }

    /// Get column count.
    pub fn column_count(&self) -> usize {
        self.schema.columns.len()
    }

    /// Set the schema.
    pub fn with_schema(mut self, schema: TypedSchema) -> Self {
        self.schema = schema;
        self
    }

    /// Set the description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add a tag.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Set freshness info.
    pub fn with_freshness(mut self, freshness: FreshnessInfo) -> Self {
        self.freshness = freshness;
        self
    }
}

// ============================================================================
// References
// ============================================================================

/// Reference to a column (source.table.column).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Source name.
    pub source: String,
    /// Table name.
    pub table: String,
    /// Column name.
    pub column: String,
}

impl ColumnRef {
    /// Create a new column reference.
    pub fn new(
        source: impl Into<String>,
        table: impl Into<String>,
        column: impl Into<String>,
    ) -> Self {
        Self {
            source: source.into(),
            table: table.into(),
            column: column.into(),
        }
    }

    /// Parse from fully qualified name (source.table.column).
    pub fn parse(fqn: &str) -> Option<Self> {
        let parts: Vec<&str> = fqn.split('.').collect();
        if parts.len() == 3 {
            Some(Self::new(parts[0], parts[1], parts[2]))
        } else {
            None
        }
    }

    /// Get fully qualified name.
    pub fn fqn(&self) -> String {
        format!("{}.{}.{}", self.source, self.table, self.column)
    }

    /// Get table reference.
    pub fn table_ref(&self) -> TableRef {
        TableRef::new(&self.source, &self.table)
    }
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.fqn())
    }
}

/// Reference to a table (source.table).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    /// Source name.
    pub source: String,
    /// Table name.
    pub table: String,
}

impl TableRef {
    /// Create a new table reference.
    pub fn new(source: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            table: table.into(),
        }
    }

    /// Parse from fully qualified name (source.table).
    pub fn parse(fqn: &str) -> Option<Self> {
        let parts: Vec<&str> = fqn.split('.').collect();
        if parts.len() == 2 {
            Some(Self::new(parts[0], parts[1]))
        } else {
            None
        }
    }

    /// Get fully qualified name.
    pub fn fqn(&self) -> String {
        format!("{}.{}", self.source, self.table)
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.fqn())
    }
}

// ============================================================================
// Lineage
// ============================================================================

/// How a lineage relationship was discovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineageDiscoveryMethod {
    /// Manually defined by a user.
    Manual,
    /// Inferred from column names and types.
    Inferred,
    /// Extracted from SQL query analysis.
    QueryAnalysis,
    /// Detected during data sync.
    Sync,
}

impl LineageDiscoveryMethod {
    /// Convert to database string.
    pub fn as_str(&self) -> &'static str {
        match self {
            LineageDiscoveryMethod::Manual => "manual",
            LineageDiscoveryMethod::Inferred => "inferred",
            LineageDiscoveryMethod::QueryAnalysis => "query_analysis",
            LineageDiscoveryMethod::Sync => "sync",
        }
    }

    /// Parse from database string (backwards compat helper).
    pub fn from_str(s: &str) -> Self {
        s.parse().unwrap_or(LineageDiscoveryMethod::Manual)
    }
}

impl std::fmt::Display for LineageDiscoveryMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for LineageDiscoveryMethod {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "manual" => LineageDiscoveryMethod::Manual,
            "inferred" => LineageDiscoveryMethod::Inferred,
            "query_analysis" => LineageDiscoveryMethod::QueryAnalysis,
            "sync" => LineageDiscoveryMethod::Sync,
            _ => LineageDiscoveryMethod::Manual,
        })
    }
}

/// Type of transformation in a lineage relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformationType {
    /// Column copied directly (SELECT col FROM ...).
    Direct,
    /// Derived via expression (SELECT col * 2 FROM ...).
    Derived,
    /// Result of aggregation (SELECT SUM(col) FROM ...).
    Aggregated,
    /// Result of join operation.
    Joined,
    /// Column used in filter condition.
    Filtered,
    /// Source unknown or not analyzed.
    Unknown,
}

impl TransformationType {
    /// Convert to database string.
    pub fn as_str(&self) -> &'static str {
        match self {
            TransformationType::Direct => "direct",
            TransformationType::Derived => "derived",
            TransformationType::Aggregated => "aggregated",
            TransformationType::Joined => "joined",
            TransformationType::Filtered => "filtered",
            TransformationType::Unknown => "unknown",
        }
    }

    /// Parse from database string (backwards compat helper).
    pub fn from_str(s: &str) -> Self {
        s.parse().unwrap_or(TransformationType::Unknown)
    }
}

impl std::fmt::Display for TransformationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for TransformationType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "direct" => TransformationType::Direct,
            "derived" => TransformationType::Derived,
            "aggregated" => TransformationType::Aggregated,
            "joined" => TransformationType::Joined,
            "filtered" => TransformationType::Filtered,
            _ => TransformationType::Unknown,
        })
    }
}

/// A source column in a lineage relationship.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageSource {
    /// Unique identifier.
    pub id: Option<Uuid>,
    /// The source column.
    pub column: ColumnRef,
    /// Type of transformation applied.
    pub transformation_type: TransformationType,
    /// SQL expression if available.
    pub transformation_sql: Option<String>,
    /// Confidence level (0.0 to 1.0).
    pub confidence: f32,
    /// How this lineage was discovered.
    pub discovered_by: LineageDiscoveryMethod,
}

impl LineageSource {
    /// Create a new lineage source.
    pub fn new(column: ColumnRef, transformation_type: TransformationType) -> Self {
        Self {
            id: None,
            column,
            transformation_type,
            transformation_sql: None,
            confidence: 1.0,
            discovered_by: LineageDiscoveryMethod::Manual,
        }
    }

    /// Set the transformation SQL.
    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.transformation_sql = Some(sql.into());
        self
    }

    /// Set the confidence level.
    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    /// Set the discovery method.
    pub fn with_discovery(mut self, method: LineageDiscoveryMethod) -> Self {
        self.discovered_by = method;
        self
    }
}

/// Lineage information for a column.
///
/// Tracks where a column's data comes from (upstream dependencies).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineage {
    /// The target column (where data flows to).
    pub target: ColumnRef,
    /// Source columns (where data flows from).
    pub sources: Vec<LineageSource>,
}

impl ColumnLineage {
    /// Create new column lineage.
    pub fn new(target: ColumnRef) -> Self {
        Self {
            target,
            sources: Vec::new(),
        }
    }

    /// Add a source.
    pub fn add_source(&mut self, source: LineageSource) {
        self.sources.push(source);
    }

    /// Check if this column has any known lineage.
    pub fn has_lineage(&self) -> bool {
        !self.sources.is_empty()
    }
}

// ============================================================================
// Relationships
// ============================================================================

/// Type of relationship between tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipType {
    /// Explicit foreign key from database schema.
    ForeignKey,
    /// Inferred from column names and value matching.
    Inferred,
    /// User-defined relationship.
    Manual,
}

impl RelationshipType {
    /// Convert to database string.
    pub fn as_str(&self) -> &'static str {
        match self {
            RelationshipType::ForeignKey => "foreign_key",
            RelationshipType::Inferred => "inferred",
            RelationshipType::Manual => "manual",
        }
    }

    /// Parse from database string (backwards compat helper).
    pub fn from_str(s: &str) -> Self {
        s.parse().unwrap_or(RelationshipType::Manual)
    }
}

impl std::fmt::Display for RelationshipType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for RelationshipType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "foreign_key" => RelationshipType::ForeignKey,
            "inferred" => RelationshipType::Inferred,
            _ => RelationshipType::Manual,
        })
    }
}

/// Cardinality of a relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    /// One-to-one relationship.
    OneToOne,
    /// One-to-many relationship (one parent, many children).
    OneToMany,
    /// Many-to-one relationship (many children, one parent).
    ManyToOne,
    /// Many-to-many relationship.
    ManyToMany,
    /// Cardinality not determined.
    Unknown,
}

impl Cardinality {
    /// Convert to database string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Cardinality::OneToOne => "one_to_one",
            Cardinality::OneToMany => "one_to_many",
            Cardinality::ManyToOne => "many_to_one",
            Cardinality::ManyToMany => "many_to_many",
            Cardinality::Unknown => "unknown",
        }
    }

    /// Parse from database string (backwards compat helper).
    pub fn from_str(s: &str) -> Self {
        s.parse().unwrap_or(Cardinality::Unknown)
    }
}

impl std::fmt::Display for Cardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for Cardinality {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "one_to_one" => Cardinality::OneToOne,
            "one_to_many" => Cardinality::OneToMany,
            "many_to_one" => Cardinality::ManyToOne,
            "many_to_many" => Cardinality::ManyToMany,
            _ => Cardinality::Unknown,
        })
    }
}

/// A cross-source relationship (foreign key).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossSourceRelationship {
    /// Unique identifier.
    pub id: Uuid,
    /// Project this relationship belongs to.
    pub project_id: Uuid,
    /// Optional name for the relationship.
    pub name: Option<String>,
    /// The referencing table (has the FK column).
    pub from: TableRef,
    /// Columns in the referencing table.
    pub from_columns: Vec<String>,
    /// The referenced table (has the PK).
    pub to: TableRef,
    /// Columns in the referenced table.
    pub to_columns: Vec<String>,
    /// Type of relationship.
    pub relationship_type: RelationshipType,
    /// Cardinality of the relationship.
    pub cardinality: Cardinality,
    /// Confidence level (0.0 to 1.0).
    pub confidence: f32,
    /// Whether the relationship has been validated.
    pub is_validated: bool,
    /// When the relationship was last validated.
    pub last_validated_at: Option<DateTime<Utc>>,
    /// Number of violations found during validation.
    pub violation_count: i32,
    /// When this relationship was created.
    pub created_at: DateTime<Utc>,
    /// When this relationship was last updated.
    pub updated_at: DateTime<Utc>,
}

impl CrossSourceRelationship {
    /// Create a new relationship.
    pub fn new(
        project_id: Uuid,
        from: TableRef,
        from_columns: Vec<String>,
        to: TableRef,
        to_columns: Vec<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            project_id,
            name: None,
            from,
            from_columns,
            to,
            to_columns,
            relationship_type: RelationshipType::Manual,
            cardinality: Cardinality::Unknown,
            confidence: 1.0,
            is_validated: false,
            last_validated_at: None,
            violation_count: 0,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a simple foreign key relationship (single column).
    pub fn foreign_key(project_id: Uuid, from: ColumnRef, to: ColumnRef) -> Self {
        let mut rel = Self::new(
            project_id,
            from.table_ref(),
            vec![from.column],
            to.table_ref(),
            vec![to.column],
        );
        rel.relationship_type = RelationshipType::ForeignKey;
        rel.cardinality = Cardinality::ManyToOne;
        rel
    }

    /// Set the name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the relationship type.
    pub fn with_type(mut self, rel_type: RelationshipType) -> Self {
        self.relationship_type = rel_type;
        self
    }

    /// Set the cardinality.
    pub fn with_cardinality(mut self, cardinality: Cardinality) -> Self {
        self.cardinality = cardinality;
        self
    }

    /// Set the confidence.
    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    /// Check if this is a cross-source relationship.
    pub fn is_cross_source(&self) -> bool {
        self.from.source != self.to.source
    }

    /// Get a descriptive string for the relationship.
    pub fn description(&self) -> String {
        format!(
            "{}.{} ({}) -> {}.{} ({})",
            self.from.source,
            self.from.table,
            self.from_columns.join(", "),
            self.to.source,
            self.to.table,
            self.to_columns.join(", ")
        )
    }
}

// ============================================================================
// Summary Types (for listing)
// ============================================================================

/// Summary of a data source for listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSummary {
    /// Source name.
    pub name: String,
    /// Source type.
    pub source_type: SourceType,
    /// Number of tables in this source.
    pub table_count: i64,
    /// Total row count across all tables.
    pub total_rows: Option<i64>,
    /// When the source was last synced.
    pub last_sync_at: Option<DateTime<Utc>>,
    /// Current sync status.
    pub sync_status: SyncStatus,
}

/// Summary of a table for listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSummary {
    /// Source name.
    pub source_name: String,
    /// Table name.
    pub table_name: String,
    /// Number of columns.
    pub column_count: i32,
    /// Estimated row count.
    pub row_count_estimate: Option<i64>,
    /// Estimated size in bytes.
    pub size_bytes_estimate: Option<i64>,
    /// Current sync status.
    pub sync_status: SyncStatus,
    /// When the table was last synced.
    pub last_sync_at: Option<DateTime<Utc>>,
    /// Description if available.
    pub description: Option<String>,
}

impl TableSummary {
    /// Get fully qualified name.
    pub fn fqn(&self) -> String {
        format!("{}.{}", self.source_name, self.table_name)
    }
}

// ============================================================================
// Search
// ============================================================================

/// Result of a catalog search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Type of result (table, column, relationship).
    pub result_type: SearchResultType,
    /// Matched name.
    pub name: String,
    /// Fully qualified name.
    pub fqn: String,
    /// Description if available.
    pub description: Option<String>,
    /// Relevance score (higher is better).
    pub score: f32,
}

/// Type of search result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchResultType {
    /// A table match.
    Table,
    /// A column match.
    Column,
    /// A relationship match.
    Relationship,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_ref_parse() {
        let col = ColumnRef::parse("stripe.customers.id").unwrap();
        assert_eq!(col.source, "stripe");
        assert_eq!(col.table, "customers");
        assert_eq!(col.column, "id");
        assert_eq!(col.fqn(), "stripe.customers.id");
    }

    #[test]
    fn test_column_ref_parse_invalid() {
        assert!(ColumnRef::parse("stripe.customers").is_none());
        assert!(ColumnRef::parse("id").is_none());
        assert!(ColumnRef::parse("").is_none());
    }

    #[test]
    fn test_table_ref_parse() {
        let table = TableRef::parse("postgres.users").unwrap();
        assert_eq!(table.source, "postgres");
        assert_eq!(table.table, "users");
        assert_eq!(table.fqn(), "postgres.users");
    }

    #[test]
    fn test_catalog_entry_fqn() {
        let entry = CatalogEntry::new(Uuid::new_v4(), "stripe", "customers");
        assert_eq!(entry.fqn(), "stripe.customers");
    }

    #[test]
    fn test_freshness_staleness() {
        let mut freshness = FreshnessInfo::new();
        
        // No sync time = always stale
        assert!(freshness.is_stale(Duration::hours(1)));
        
        // Recent sync = not stale
        freshness.last_sync_at = Some(Utc::now());
        assert!(!freshness.is_stale(Duration::hours(1)));
    }

    #[test]
    fn test_relationship_description() {
        let rel = CrossSourceRelationship::foreign_key(
            Uuid::new_v4(),
            ColumnRef::new("stripe", "charges", "customer_id"),
            ColumnRef::new("stripe", "customers", "id"),
        );
        
        let desc = rel.description();
        assert!(desc.contains("charges"));
        assert!(desc.contains("customers"));
        assert!(desc.contains("customer_id"));
    }

    #[test]
    fn test_relationship_is_cross_source() {
        let same_source = CrossSourceRelationship::foreign_key(
            Uuid::new_v4(),
            ColumnRef::new("postgres", "orders", "user_id"),
            ColumnRef::new("postgres", "users", "id"),
        );
        assert!(!same_source.is_cross_source());
        
        let cross_source = CrossSourceRelationship::foreign_key(
            Uuid::new_v4(),
            ColumnRef::new("stripe", "charges", "customer_id"),
            ColumnRef::new("postgres", "users", "stripe_customer_id"),
        );
        assert!(cross_source.is_cross_source());
    }

    #[test]
    fn test_sync_status_conversions() {
        for status in [
            SyncStatus::Synced,
            SyncStatus::Syncing,
            SyncStatus::Stale,
            SyncStatus::Error,
            SyncStatus::Unknown,
        ] {
            let s = status.as_str();
            let parsed = SyncStatus::from_str(s);
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_lineage_source() {
        let source = LineageSource::new(
            ColumnRef::new("stripe", "charges", "amount"),
            TransformationType::Direct,
        )
        .with_confidence(0.95)
        .with_discovery(LineageDiscoveryMethod::QueryAnalysis);
        
        assert_eq!(source.confidence, 0.95);
        assert_eq!(source.discovered_by, LineageDiscoveryMethod::QueryAnalysis);
    }
}
