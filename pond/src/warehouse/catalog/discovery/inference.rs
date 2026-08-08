//! Relationship Inference
//!
//! Automatically infers foreign key relationships between tables based on:
//! - Column naming patterns (e.g., customer_id -> customers.id)
//! - Data type matching
//! - Value sampling (optional)

use std::collections::HashMap;
use tracing::{debug, info, instrument};
use uuid::Uuid;

use crate::warehouse::catalog::types::{
    CatalogEntry, Cardinality, CrossSourceRelationship, RelationshipType, TableRef,
};

// ============================================================================
// Inference Configuration
// ============================================================================

/// Configuration for relationship inference.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    /// Minimum confidence threshold for inferred relationships.
    pub min_confidence: f32,
    /// Common ID column suffixes to look for.
    pub id_suffixes: Vec<String>,
    /// Common primary key column names.
    pub pk_names: Vec<String>,
    /// Whether to infer cross-source relationships.
    pub cross_source: bool,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            min_confidence: 0.5,
            id_suffixes: vec![
                "_id".to_string(),
                "_uuid".to_string(),
                "_key".to_string(),
                "Id".to_string(),
                "ID".to_string(),
            ],
            pk_names: vec![
                "id".to_string(),
                "uuid".to_string(),
                "pk".to_string(),
                "key".to_string(),
            ],
            cross_source: true,
        }
    }
}

// ============================================================================
// Candidate Relationship
// ============================================================================

/// A candidate relationship before confidence scoring.
#[derive(Debug)]
struct CandidateRelationship {
    from_source: String,
    from_table: String,
    from_column: String,
    to_source: String,
    to_table: String,
    to_column: String,
    confidence: f32,
}

// ============================================================================
// Relationship Inference
// ============================================================================

/// Infers relationships between catalog entries.
pub struct RelationshipInference {
    config: InferenceConfig,
}

impl RelationshipInference {
    /// Create a new relationship inference engine.
    pub fn new() -> Self {
        Self {
            config: InferenceConfig::default(),
        }
    }

    /// Create with custom configuration.
    pub fn with_config(config: InferenceConfig) -> Self {
        Self { config }
    }

    /// Infer relationships from a set of catalog entries.
    #[instrument(skip(self, entries))]
    pub fn infer_relationships(&self, entries: &[CatalogEntry]) -> Vec<CrossSourceRelationship> {
        info!("Inferring relationships from {} catalog entries", entries.len());

        // Build lookup maps
        let table_map = self.build_table_map(entries);
        let pk_map = self.identify_primary_keys(entries);

        // Find candidate relationships
        let candidates = self.find_candidates(entries, &table_map, &pk_map);

        debug!("Found {} candidate relationships", candidates.len());

        // Convert candidates to relationships
        let relationships: Vec<_> = candidates
            .into_iter()
            .filter(|c| c.confidence >= self.config.min_confidence)
            .map(|c| self.candidate_to_relationship(c, entries))
            .collect();

        info!("Inferred {} relationships above threshold", relationships.len());
        relationships
    }

    /// Build a map of source.table -> entry for quick lookup.
    fn build_table_map<'a>(&self, entries: &'a [CatalogEntry]) -> HashMap<String, &'a CatalogEntry> {
        entries
            .iter()
            .map(|e| (format!("{}.{}", e.source_name, e.table_name), e))
            .collect()
    }

    /// Identify likely primary key columns for each table.
    fn identify_primary_keys(&self, entries: &[CatalogEntry]) -> HashMap<String, String> {
        let mut pk_map = HashMap::new();

        for entry in entries {
            let key = format!("{}.{}", entry.source_name, entry.table_name);

            // Look for common PK names
            for col in &entry.schema.columns {
                let lower_name = col.name.to_lowercase();
                
                // Check for exact matches first
                if self.config.pk_names.iter().any(|pk| lower_name == pk.to_lowercase()) {
                    pk_map.insert(key.clone(), col.name.clone());
                    break;
                }
                
                // Check for identifier semantic type
                if col.is_identifier() {
                    pk_map.insert(key.clone(), col.name.clone());
                    break;
                }
            }
        }

        pk_map
    }

    /// Find candidate relationships by analyzing column names.
    fn find_candidates(
        &self,
        entries: &[CatalogEntry],
        table_map: &HashMap<String, &CatalogEntry>,
        pk_map: &HashMap<String, String>,
    ) -> Vec<CandidateRelationship> {
        let mut candidates = Vec::new();

        for entry in entries {
            for col in &entry.schema.columns {
                // Look for columns that end with ID suffixes
                let lower_name = col.name.to_lowercase();
                
                for suffix in &self.config.id_suffixes {
                    let lower_suffix = suffix.to_lowercase();
                    
                    if lower_name.ends_with(&lower_suffix) {
                        // Extract the table name part
                        let table_hint = lower_name
                            .strip_suffix(&lower_suffix)
                            .unwrap_or("");

                        if table_hint.is_empty() {
                            continue;
                        }

                        // Look for matching tables
                        let target_matches = self.find_matching_tables(
                            table_hint,
                            &entry.source_name,
                            entries,
                            table_map,
                            pk_map,
                        );

                        for (target_key, target_col, confidence) in target_matches {
                            // Skip self-references
                            let from_key = format!("{}.{}", entry.source_name, entry.table_name);
                            if from_key == target_key {
                                continue;
                            }

                            // Check if cross-source relationships are allowed
                            let target_source = target_key.split('.').next().unwrap_or("");
                            if !self.config.cross_source && target_source != entry.source_name {
                                continue;
                            }

                            let parts: Vec<&str> = target_key.split('.').collect();
                            if parts.len() == 2 {
                                candidates.push(CandidateRelationship {
                                    from_source: entry.source_name.clone(),
                                    from_table: entry.table_name.clone(),
                                    from_column: col.name.clone(),
                                    to_source: parts[0].to_string(),
                                    to_table: parts[1].to_string(),
                                    to_column: target_col,
                                    confidence,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Deduplicate candidates (keep highest confidence)
        self.deduplicate_candidates(candidates)
    }

    /// Find tables that match a given hint (e.g., "customer" -> "customers").
    fn find_matching_tables(
        &self,
        table_hint: &str,
        source_hint: &str,
        _entries: &[CatalogEntry],
        table_map: &HashMap<String, &CatalogEntry>,
        pk_map: &HashMap<String, String>,
    ) -> Vec<(String, String, f32)> {
        let mut matches = Vec::new();

        // Common pluralization patterns
        let singular = table_hint.to_lowercase();
        let plurals = vec![
            singular.clone(),
            format!("{}s", singular),
            format!("{}es", singular),
            format!("{}ies", singular.strip_suffix('y').unwrap_or(&singular)),
        ];

        for (key, entry) in table_map {
            let table_lower = entry.table_name.to_lowercase();

            // Check for exact or plural matches
            let is_match = plurals.iter().any(|p| p == &table_lower);
            
            if !is_match {
                continue;
            }

            // Get the PK column for this table
            let pk_col = pk_map.get(key).cloned().unwrap_or_else(|| "id".to_string());

            // Calculate confidence (capped at 0.95 for standard relationships,
            // reserving 0.95+ for verified/explicit relationships)
            let mut confidence: f32 = 0.5;

            // Boost for same source
            if entry.source_name == source_hint {
                confidence += 0.15;
            }

            // Boost for exact singular match
            if table_lower == singular || table_lower == format!("{}s", singular) {
                confidence += 0.15;
            }

            // Boost if we found the PK column
            if pk_map.contains_key(key) {
                confidence += 0.1;
            }

            matches.push((key.clone(), pk_col, confidence.min(1.0_f32)));
        }

        matches
    }

    /// Deduplicate candidates, keeping the highest confidence.
    fn deduplicate_candidates(&self, candidates: Vec<CandidateRelationship>) -> Vec<CandidateRelationship> {
        let mut best: HashMap<String, CandidateRelationship> = HashMap::new();

        for c in candidates {
            let key = format!(
                "{}_{}_{}_{}_{}",
                c.from_source, c.from_table, c.from_column, c.to_source, c.to_table
            );

            if let Some(existing) = best.get(&key) {
                if c.confidence > existing.confidence {
                    best.insert(key, c);
                }
            } else {
                best.insert(key, c);
            }
        }

        best.into_values().collect()
    }

    /// Convert a candidate to a CrossSourceRelationship.
    fn candidate_to_relationship(
        &self,
        candidate: CandidateRelationship,
        entries: &[CatalogEntry],
    ) -> CrossSourceRelationship {
        // Get project_id from the first matching entry
        let project_id = entries
            .iter()
            .find(|e| e.source_name == candidate.from_source && e.table_name == candidate.from_table)
            .map(|e| e.project_id)
            .unwrap_or_else(Uuid::nil);

        CrossSourceRelationship {
            id: Uuid::new_v4(),
            project_id,
            name: Some(format!(
                "{}_{}_fk",
                candidate.from_table, candidate.to_table
            )),
            from: TableRef::new(&candidate.from_source, &candidate.from_table),
            from_columns: vec![candidate.from_column],
            to: TableRef::new(&candidate.to_source, &candidate.to_table),
            to_columns: vec![candidate.to_column],
            relationship_type: RelationshipType::Inferred,
            cardinality: Cardinality::ManyToOne,
            confidence: candidate.confidence,
            is_validated: false,
            last_validated_at: None,
            violation_count: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }
}

impl Default for RelationshipInference {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::types::TypedSchema;

    fn make_entry(source: &str, table: &str, columns: &[&str]) -> CatalogEntry {
        use arrow::datatypes::DataType;
        use crate::warehouse::types::TypedColumn;

        let mut schema = TypedSchema::new(table, source);
        for col_name in columns {
            let col = TypedColumn::new(*col_name, &DataType::Utf8, true, "string", source);
            schema = schema.with_column(col);
        }

        let mut entry = CatalogEntry::new(Uuid::new_v4(), source, table);
        entry.schema = schema;
        entry
    }

    #[test]
    fn test_infer_customer_relationship() {
        let entries = vec![
            make_entry("db", "customers", &["id", "name", "email"]),
            make_entry("db", "orders", &["id", "customer_id", "amount"]),
        ];

        let inference = RelationshipInference::new();
        let relationships = inference.infer_relationships(&entries);

        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].from.table, "orders");
        assert_eq!(relationships[0].to.table, "customers");
        assert_eq!(relationships[0].from_columns, vec!["customer_id"]);
    }

    #[test]
    fn test_infer_multiple_relationships() {
        let entries = vec![
            make_entry("db", "users", &["id", "name"]),
            make_entry("db", "products", &["id", "name", "price"]),
            make_entry("db", "orders", &["id", "user_id", "product_id", "quantity"]),
        ];

        let inference = RelationshipInference::new();
        let relationships = inference.infer_relationships(&entries);

        assert_eq!(relationships.len(), 2);
        
        let rel_tables: Vec<_> = relationships.iter().map(|r| r.to.table.as_str()).collect();
        assert!(rel_tables.contains(&"users"));
        assert!(rel_tables.contains(&"products"));
    }

    #[test]
    fn test_no_self_reference() {
        let entries = vec![
            make_entry("db", "users", &["id", "name", "user_id"]),
        ];

        let inference = RelationshipInference::new();
        let relationships = inference.infer_relationships(&entries);

        // Should not create a self-reference from user_id to users
        assert!(relationships.is_empty());
    }

    #[test]
    fn test_cross_source_relationships() {
        let entries = vec![
            make_entry("stripe", "customers", &["id", "email"]),
            make_entry("postgres", "orders", &["id", "customer_id", "amount"]),
        ];

        let inference = RelationshipInference::new();
        let relationships = inference.infer_relationships(&entries);

        // Should find cross-source relationship
        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].from.source, "postgres");
        assert_eq!(relationships[0].to.source, "stripe");
    }

    #[test]
    fn test_cross_source_disabled() {
        let entries = vec![
            make_entry("stripe", "customers", &["id", "email"]),
            make_entry("postgres", "orders", &["id", "customer_id", "amount"]),
        ];

        let config = InferenceConfig {
            cross_source: false,
            ..Default::default()
        };
        let inference = RelationshipInference::with_config(config);
        let relationships = inference.infer_relationships(&entries);

        // Should not find cross-source relationship
        assert!(relationships.is_empty());
    }

    #[test]
    fn test_confidence_threshold() {
        let entries = vec![
            make_entry("db", "customers", &["id", "name"]),
            make_entry("db", "orders", &["id", "customer_id"]),
        ];

        let config = InferenceConfig {
            min_confidence: 0.99, // Very high threshold
            ..Default::default()
        };
        let inference = RelationshipInference::with_config(config);
        let relationships = inference.infer_relationships(&entries);

        // High threshold should filter out lower-confidence relationships
        assert!(relationships.is_empty());
    }

    #[test]
    fn test_plural_table_matching() {
        let entries = vec![
            // Singular table name
            make_entry("db", "customer", &["id", "name"]),
            make_entry("db", "order", &["id", "customer_id"]),
        ];

        let inference = RelationshipInference::new();
        let relationships = inference.infer_relationships(&entries);

        // Should still find the relationship with singular table names
        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].to.table, "customer");
    }
}
