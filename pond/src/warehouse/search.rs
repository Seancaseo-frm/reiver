//! Cross-table full-text search over warehouse data content.
//!
//! Provides a single search endpoint that fans out `hasToken()` queries
//! across all string columns in all tables, merges results, and returns
//! matching rows. Index-accelerated on both hot (ClickHouse tokenbf) and
//! warm (skip index token FST) tiers.

use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::app_state::PondState;
use crate::warehouse::query::executor::ColumnInfo;
use crate::warehouse::catalog::CatalogRepository;
use crate::warehouse::catalog::types::CatalogEntry;

/// Default result limit per table.
const DEFAULT_LIMIT: u32 = 50;

/// Maximum result limit.
const MAX_LIMIT: u32 = 500;

/// Maximum concurrent table queries.
const MAX_CONCURRENT_QUERIES: usize = 5;

/// Request for a full-text search across warehouse data.
#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    /// The search query (e.g., "timeout error").
    pub query: String,
    /// Optional: restrict search to specific tables.
    pub tables: Option<Vec<String>>,
    /// Maximum results per table (default 50, max 500).
    pub limit: Option<u32>,
}

/// Response from a full-text search.
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    /// Results grouped by table and column.
    pub results: Vec<TableSearchResult>,
    /// Total search time in milliseconds.
    pub execution_time_ms: u64,
}

/// Search results from a single table/column.
#[derive(Debug, Serialize)]
pub struct TableSearchResult {
    pub source_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
    pub match_count: usize,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Check if a column type name represents a string type.
pub fn is_string_column(source_type_name: &str) -> bool {
    let lower = source_type_name.to_lowercase();
    lower.contains("varchar")
        || lower.contains("text")
        || lower.contains("string")
        || lower.contains("char")
        || lower == "utf8"
        || lower == "largeutf8"
        || lower == "json"
}

/// Tokenize a search query into individual search terms.
///
/// Splits on non-alphanumeric characters, lowercases, deduplicates,
/// and filters out tokens shorter than 2 characters.
pub fn tokenize(query: &str) -> Vec<String> {
    let mut tokens: Vec<String> = query
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| s.len() >= 2)
        .map(|s| s.to_lowercase())
        .collect();
    tokens.sort();
    tokens.dedup();
    tokens
}

/// Collect the names of all string columns from a catalog entry.
pub fn string_columns_from_entry(entry: &CatalogEntry) -> Vec<String> {
    entry
        .schema
        .columns
        .iter()
        .filter(|c| is_string_column(&c.source_type_name))
        .map(|c| c.name.clone())
        .collect()
}

/// Build a SQL WHERE clause for full-text search using `hasToken()`.
///
/// Multi-token queries use AND within a column (all tokens must match).
/// Multi-column tables use OR across columns (any column can match).
pub fn build_search_where(string_cols: &[String], tokens: &[String]) -> String {
    if string_cols.is_empty() || tokens.is_empty() {
        return "1 = 0".to_string();
    }

    let col_clauses: Vec<String> = string_cols
        .iter()
        .map(|col| {
            let token_preds: Vec<String> = tokens
                .iter()
                .map(|tok| {
                    let escaped_tok = tok.replace('\'', "''");
                    let escaped_col = col.replace('`', "``");
                    format!("hasToken(`{}`, '{}')", escaped_col, escaped_tok)
                })
                .collect();
            if token_preds.len() == 1 {
                token_preds[0].clone()
            } else {
                format!("({})", token_preds.join(" AND "))
            }
        })
        .collect();

    if col_clauses.len() == 1 {
        col_clauses[0].clone()
    } else {
        format!("({})", col_clauses.join(" OR "))
    }
}

/// Build a full SELECT query for searching a table.
pub fn build_search_query(
    source_name: &str,
    table_name: &str,
    string_cols: &[String],
    tokens: &[String],
    limit: u32,
) -> String {
    let where_clause = build_search_where(string_cols, tokens);
    let escaped_source = source_name.replace('`', "``");
    let escaped_table = table_name.replace('`', "``");
    format!(
        "SELECT * FROM `{}`.`{}` WHERE {} LIMIT {}",
        escaped_source, escaped_table, where_clause, limit
    )
}

/// Execute a full-text search across all tables in a project.
#[tracing::instrument(
    name = "warehouse.search.execute",
    skip_all,
    err(Display)
)]
pub async fn execute_search(
    state: &Arc<PondState>,
    project_id: Uuid,
    request: &SearchRequest,
) -> Result<SearchResponse> {
    let start = std::time::Instant::now();
    let limit = request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let tokens = tokenize(&request.query);

    if tokens.is_empty() {
        return Ok(SearchResponse {
            results: vec![],
            execution_time_ms: 0,
        });
    }

    let catalog_repo = CatalogRepository::new(state.db.clone());
    let entries = catalog_repo
        .list_entries(project_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load catalog: {}", e))?;

    // Build list of (entry, string_columns) pairs to search
    let search_targets: Vec<(&CatalogEntry, Vec<String>)> = entries
        .iter()
        .filter(|e| {
            if let Some(ref tables) = request.tables {
                tables.iter().any(|t| t == &e.table_name)
            } else {
                true
            }
        })
        .filter_map(|e| {
            let cols = string_columns_from_entry(e);
            if cols.is_empty() {
                None
            } else {
                Some((e, cols))
            }
        })
        .collect();

    if search_targets.is_empty() {
        return Ok(SearchResponse {
            results: vec![],
            execution_time_ms: start.elapsed().as_millis() as u64,
        });
    }

    // Fan out queries with bounded concurrency
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES));
    let mut handles = Vec::new();

    for (entry, string_cols) in &search_targets {
        let sql = build_search_query(
            &entry.source_name,
            &entry.table_name,
            string_cols,
            &tokens,
            limit,
        );

        let state = state.clone();
        let source_name = entry.source_name.clone();
        let table_name = entry.table_name.clone();
        let sem = semaphore.clone();

        handles.push(tokio::spawn(async move {
            let _permit = match sem.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    tracing::warn!(
                        table = %table_name,
                        "Search semaphore closed, skipping table"
                    );
                    return None;
                }
            };

            let rewritten = crate::api::warehouse::validate_and_rewrite_nl_query(
                &state, project_id, &sql,
            )
            .await;

            let rewritten_sql = match rewritten {
                Ok(sql) => sql,
                Err(e) => {
                    tracing::warn!(
                        table = %table_name,
                        error = %e,
                        "Search query rewrite failed, skipping table"
                    );
                    return None;
                }
            };

            let options = crate::warehouse::query::executor::ExecutionOptions {
                limit: Some(limit),
                timeout_secs: Some(15),
                max_memory_bytes: Some(50 * 1024 * 1024),
            };

            match state.warehouse_query_executor.execute(&rewritten_sql, options).await {
                Ok(result) => {
                    let columns: Vec<ColumnInfo> = result
                        .columns
                        .clone();
                    let match_count = result.rows.len();
                    if match_count > 0 {
                        Some(TableSearchResult {
                            source_name,
                            table_name,
                            columns,
                            match_count,
                            rows: result.rows,
                        })
                    } else {
                        None
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        table = %table_name,
                        error = %e,
                        "Search query execution failed, skipping table"
                    );
                    None
                }
            }
        }));
    }

    let mut results = Vec::new();
    for handle in handles {
        if let Ok(Some(result)) = handle.await {
            results.push(result);
        }
    }

    // Sort by match count descending
    results.sort_by(|a, b| b.match_count.cmp(&a.match_count));

    Ok(SearchResponse {
        results,
        execution_time_ms: start.elapsed().as_millis() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_basic() {
        let tokens = tokenize("timeout error");
        assert_eq!(tokens, vec!["error", "timeout"]);
    }

    #[test]
    fn test_tokenize_dedup() {
        let tokens = tokenize("error error Error");
        assert_eq!(tokens, vec!["error"]);
    }

    #[test]
    fn test_tokenize_short_tokens_filtered() {
        let tokens = tokenize("a is timeout");
        assert_eq!(tokens, vec!["is", "timeout"]);
    }

    #[test]
    fn test_tokenize_special_chars() {
        let tokens = tokenize("user@example.com");
        assert_eq!(tokens, vec!["com", "example", "user"]);
    }

    #[test]
    fn test_tokenize_empty() {
        let tokens = tokenize("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_tokenize_underscores_preserved() {
        let tokens = tokenize("user_id timeout_error");
        assert_eq!(tokens, vec!["timeout_error", "user_id"]);
    }

    #[test]
    fn test_build_search_where_single_col_single_token() {
        let clause = build_search_where(
            &["message".to_string()],
            &["timeout".to_string()],
        );
        assert_eq!(clause, "hasToken(`message`, 'timeout')");
    }

    #[test]
    fn test_build_search_where_single_col_multi_token() {
        let clause = build_search_where(
            &["message".to_string()],
            &["error".to_string(), "timeout".to_string()],
        );
        assert_eq!(
            clause,
            "(hasToken(`message`, 'error') AND hasToken(`message`, 'timeout'))"
        );
    }

    #[test]
    fn test_build_search_where_multi_col_single_token() {
        let clause = build_search_where(
            &["message".to_string(), "body".to_string()],
            &["timeout".to_string()],
        );
        assert_eq!(
            clause,
            "(hasToken(`message`, 'timeout') OR hasToken(`body`, 'timeout'))"
        );
    }

    #[test]
    fn test_build_search_where_multi_col_multi_token() {
        let clause = build_search_where(
            &["message".to_string(), "body".to_string()],
            &["error".to_string(), "timeout".to_string()],
        );
        assert_eq!(
            clause,
            "((hasToken(`message`, 'error') AND hasToken(`message`, 'timeout')) OR (hasToken(`body`, 'error') AND hasToken(`body`, 'timeout')))"
        );
    }

    #[test]
    fn test_build_search_where_empty_cols() {
        let clause = build_search_where(&[], &["timeout".to_string()]);
        assert_eq!(clause, "1 = 0");
    }

    #[test]
    fn test_build_search_where_empty_tokens() {
        let clause = build_search_where(&["message".to_string()], &[]);
        assert_eq!(clause, "1 = 0");
    }

    #[test]
    fn test_build_search_where_sql_injection() {
        let clause = build_search_where(
            &["message".to_string()],
            &["test'; DROP TABLE--".to_string()],
        );
        assert!(clause.contains("test''; DROP TABLE--"));
    }

    #[test]
    fn test_build_search_where_backtick_in_column_name() {
        let clause = build_search_where(
            &["col`; DROP TABLE x--".to_string()],
            &["hello".to_string()],
        );
        assert_eq!(clause, "hasToken(`col``; DROP TABLE x--`, 'hello')");
        assert!(!clause.contains("col`; DROP"), "Backtick must be escaped to prevent SQL injection");
    }

    #[test]
    fn test_build_search_query_escapes_identifiers() {
        let sql = build_search_query(
            "db`injection",
            "tbl`name",
            &["col`x".to_string()],
            &["tok".to_string()],
            10,
        );
        assert!(sql.contains("`db``injection`"), "Source name backticks must be escaped");
        assert!(sql.contains("`tbl``name`"), "Table name backticks must be escaped");
        assert!(sql.contains("`col``x`"), "Column name backticks must be escaped");
    }

    #[test]
    fn test_build_search_query() {
        let sql = build_search_query(
            "db",
            "events",
            &["message".to_string()],
            &["timeout".to_string()],
            50,
        );
        assert_eq!(
            sql,
            "SELECT * FROM `db`.`events` WHERE hasToken(`message`, 'timeout') LIMIT 50"
        );
    }

    #[test]
    fn test_is_string_column() {
        assert!(is_string_column("String"));
        assert!(is_string_column("Nullable(String)"));
        assert!(is_string_column("varchar(255)"));
        assert!(is_string_column("text"));
        assert!(is_string_column("Utf8"));
        assert!(is_string_column("LargeUtf8"));
        assert!(is_string_column("char(10)"));
        assert!(is_string_column("Json"));
        assert!(!is_string_column("Int64"));
        assert!(!is_string_column("Float64"));
        assert!(!is_string_column("DateTime"));
        assert!(!is_string_column("Boolean"));
    }

    #[test]
    fn test_string_columns_from_entry() {
        use crate::warehouse::catalog::types::CatalogEntry;
        use crate::warehouse::types::{TypedColumn, TypedSchema};

        let mut entry = CatalogEntry::new(Uuid::new_v4(), "db", "events");
        entry.schema = TypedSchema {
            table_name: "events".to_string(),
            columns: vec![
                TypedColumn::new("id", &arrow::datatypes::DataType::Int64, false, "Int64", "db"),
                TypedColumn::new("message", &arrow::datatypes::DataType::Utf8, true, "String", "db"),
                TypedColumn::new("level", &arrow::datatypes::DataType::Utf8, true, "String", "db"),
                TypedColumn::new("timestamp", &arrow::datatypes::DataType::Utf8, false, "DateTime", "db"),
            ],
            source_name: "db".to_string(),
            updated_at: None,
        };

        let cols = string_columns_from_entry(&entry);
        assert_eq!(cols, vec!["message", "level"]);
    }

    #[tokio::test]
    async fn test_closed_semaphore_returns_none() {
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        sem.close();

        let result = match sem.acquire().await {
            Ok(_permit) => Some("acquired"),
            Err(_) => None,
        };

        assert!(result.is_none(), "Closed semaphore must return None, not panic");
    }

    #[test]
    fn test_multi_column_search_has_outer_parentheses() {
        let cols = vec!["name".to_string(), "email".to_string()];
        let tokens = vec!["alice".to_string()];
        let clause = build_search_where(&cols, &tokens);

        assert!(
            clause.starts_with('(') && clause.ends_with(')'),
            "Multi-column OR clause must be wrapped in parentheses to prevent \
             operator precedence bugs when AND conditions are appended. Got: {}",
            clause
        );
        assert!(clause.contains(" OR "), "Must contain OR between columns");
    }

    #[test]
    fn test_single_column_search_no_extra_parentheses() {
        let cols = vec!["name".to_string()];
        let tokens = vec!["alice".to_string()];
        let clause = build_search_where(&cols, &tokens);

        assert!(
            !clause.starts_with('('),
            "Single-column clause should not have redundant outer parentheses. Got: {}",
            clause
        );
    }
}
