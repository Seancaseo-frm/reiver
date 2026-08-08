//! Schema-based query suggestion engine.
//!
//! Generates natural language question suggestions from catalog metadata
//! without requiring an LLM call.

use serde::Serialize;

use crate::warehouse::catalog::types::CatalogEntry;

/// A suggested natural language question.
#[derive(Debug, Clone, Serialize)]
pub struct QuerySuggestion {
    pub question: String,
    pub category: SuggestionCategory,
}

/// Category of suggestion, derived from column semantics.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionCategory {
    Trend,
    Aggregation,
    TopK,
    Relationship,
    Overview,
}

/// Generate deterministic query suggestions based on catalog metadata.
pub fn generate_suggestions(entries: &[CatalogEntry], limit: usize) -> Vec<QuerySuggestion> {
    let mut suggestions: Vec<(i64, QuerySuggestion)> = Vec::new();

    for entry in entries {
        let table = &entry.table_name;
        let row_count = entry.freshness.row_count_estimate.unwrap_or(0);

        let has_timestamp = entry.schema.columns.iter().any(|c| is_timestamp_type(&c.source_type_name));
        let numeric_cols: Vec<&str> = entry.schema.columns.iter()
            .filter(|c| is_numeric_type(&c.source_type_name))
            .map(|c| c.name.as_str())
            .collect();
        let string_cols: Vec<&str> = entry.schema.columns.iter()
            .filter(|c| is_string_type(&c.source_type_name))
            .map(|c| c.name.as_str())
            .collect();

        if has_timestamp {
            suggestions.push((row_count, QuerySuggestion {
                question: format!("What was the trend of {} over the last month?", table),
                category: SuggestionCategory::Trend,
            }));
        }

        if let Some(col) = numeric_cols.first() {
            suggestions.push((row_count, QuerySuggestion {
                question: format!("What is the average {} in {}?", col, table),
                category: SuggestionCategory::Aggregation,
            }));
        }

        if let Some(col) = string_cols.first() {
            suggestions.push((row_count, QuerySuggestion {
                question: format!("Show the top 10 {} by count in {}", col, table),
                category: SuggestionCategory::TopK,
            }));
        }

        if entry.schema.columns.len() > 1 {
            suggestions.push((row_count, QuerySuggestion {
                question: format!("How many rows are in {}?", table),
                category: SuggestionCategory::Overview,
            }));
        }
    }

    // Check for FK-like relationships between tables
    for (i, a) in entries.iter().enumerate() {
        for b in entries.iter().skip(i + 1) {
            if has_fk_relationship(a, b) {
                let row_count = a.freshness.row_count_estimate
                    .unwrap_or(0)
                    .max(b.freshness.row_count_estimate.unwrap_or(0));
                suggestions.push((row_count, QuerySuggestion {
                    question: format!("How do {} and {} relate?", a.table_name, b.table_name),
                    category: SuggestionCategory::Relationship,
                }));
            }
        }
    }

    suggestions.sort_by(|a, b| b.0.cmp(&a.0));
    suggestions.into_iter().map(|(_, s)| s).take(limit).collect()
}

fn is_timestamp_type(source_type: &str) -> bool {
    let lower = source_type.to_lowercase();
    lower.contains("date") || lower.contains("time") || lower.contains("timestamp")
}

fn is_numeric_type(source_type: &str) -> bool {
    let lower = source_type.to_lowercase();
    lower.contains("int")
        || lower.contains("float")
        || lower.contains("double")
        || lower.contains("decimal")
        || lower.contains("numeric")
        || lower.contains("real")
        || lower.contains("bigint")
}

fn is_string_type(source_type: &str) -> bool {
    let lower = source_type.to_lowercase();
    lower.contains("varchar")
        || lower.contains("text")
        || lower.contains("string")
        || lower.contains("char")
        || lower == "utf8"
}

/// Heuristic: table A has a column named `{B.table}_id` or vice versa.
fn has_fk_relationship(a: &CatalogEntry, b: &CatalogEntry) -> bool {
    let a_fk = format!("{}_id", b.table_name);
    let b_fk = format!("{}_id", a.table_name);

    let a_has_ref = a.schema.columns.iter().any(|c| c.name == a_fk);
    let b_has_ref = b.schema.columns.iter().any(|c| c.name == b_fk);
    a_has_ref || b_has_ref
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::catalog::types::CatalogEntry;
    use crate::warehouse::types::{TypedColumn, TypedSchema};
    use uuid::Uuid;

    fn make_entry(table: &str, cols: &[(&str, &str)]) -> CatalogEntry {
        let columns = cols.iter().map(|(name, ty)| {
            TypedColumn::new(
                *name,
                &arrow::datatypes::DataType::Utf8,
                true,
                *ty,
                "test",
            )
        }).collect();

        let mut entry = CatalogEntry::new(Uuid::new_v4(), "db", table);
        entry.schema = TypedSchema {
            table_name: table.to_string(),
            columns,
            source_name: "db".to_string(),
            updated_at: None,
        };
        entry
    }

    #[test]
    fn test_empty_catalog() {
        let suggestions = generate_suggestions(&[], 5);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_timestamp_generates_trend() {
        let entry = make_entry("events", &[
            ("id", "Int64"),
            ("created_at", "DateTime"),
            ("name", "String"),
        ]);
        let suggestions = generate_suggestions(&[entry], 10);
        assert!(suggestions.iter().any(|s| matches!(s.category, SuggestionCategory::Trend)));
    }

    #[test]
    fn test_numeric_generates_aggregation() {
        let entry = make_entry("orders", &[
            ("id", "Int64"),
            ("amount", "Decimal(18,4)"),
        ]);
        let suggestions = generate_suggestions(&[entry], 10);
        assert!(suggestions.iter().any(|s| matches!(s.category, SuggestionCategory::Aggregation)));
    }

    #[test]
    fn test_string_generates_topk() {
        let entry = make_entry("users", &[
            ("id", "Int64"),
            ("name", "String"),
        ]);
        let suggestions = generate_suggestions(&[entry], 10);
        assert!(suggestions.iter().any(|s| matches!(s.category, SuggestionCategory::TopK)));
    }

    #[test]
    fn test_fk_generates_relationship() {
        let orders = make_entry("orders", &[
            ("id", "Int64"),
            ("customers_id", "Int64"),
        ]);
        let customers = make_entry("customers", &[
            ("id", "Int64"),
            ("name", "String"),
        ]);
        let suggestions = generate_suggestions(&[orders, customers], 10);
        assert!(suggestions.iter().any(|s| matches!(s.category, SuggestionCategory::Relationship)));
    }

    #[test]
    fn test_limit_respected() {
        let entry = make_entry("events", &[
            ("id", "Int64"),
            ("created_at", "DateTime"),
            ("name", "String"),
            ("amount", "Decimal(18,4)"),
        ]);
        let suggestions = generate_suggestions(&[entry], 2);
        assert!(suggestions.len() <= 2);
    }

    #[test]
    fn test_sorted_by_row_count() {
        use crate::warehouse::catalog::types::FreshnessInfo;

        let mut small = make_entry("small_table", &[("id", "Int64"), ("name", "String")]);
        small.freshness = FreshnessInfo {
            row_count_estimate: Some(100),
            ..Default::default()
        };

        let mut large = make_entry("large_table", &[("id", "Int64"), ("name", "String")]);
        large.freshness = FreshnessInfo {
            row_count_estimate: Some(1_000_000),
            ..Default::default()
        };

        let suggestions = generate_suggestions(&[small, large], 10);
        assert!(!suggestions.is_empty());
        assert!(suggestions[0].question.contains("large_table"));
    }
}
