//! Query Explain Plans
//!
//! Shows query execution plans with scan steps and optimizations.

use serde::{Deserialize, Serialize};
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use ahash::AHashMap;

use super::cost_estimator::{QueryCostEstimate, QueryCostEstimator};
use super::rewriter::TableRewriter;

/// Step types in an execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepType {
    /// Scan files from a table
    TableScan { table: String, files: Vec<String> },
    /// Index lookup
    IndexLookup { index: String, key: String },
    /// Filter rows
    Filter { predicate: String },
    /// Join operation
    Join {
        join_type: String,
        left: String,
        right: String,
    },
    /// Aggregate rows
    Aggregate { columns: Vec<String> },
    /// Sort rows
    Sort { columns: Vec<String> },
    /// Result from cache
    CacheHit { query_hash: String },
}

/// A step in the execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainStep {
    pub step_type: StepType,
    pub description: String,
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
}

/// Query explain result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExplain {
    pub plan: Vec<ExplainStep>,
    pub estimated_cost: QueryCostEstimate,
    pub optimizations_applied: Vec<String>,
    pub warnings: Vec<String>,
}

impl Default for QueryExplain {
    fn default() -> Self {
        Self {
            plan: vec![],
            estimated_cost: QueryCostEstimate::default(),
            optimizations_applied: vec![],
            warnings: vec![],
        }
    }
}

/// File information for explain.
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
}

/// Query explainer.
pub struct QueryExplainer {
    /// Cost estimator for estimates
    cost_estimator: QueryCostEstimator,
    /// File information per table
    table_files: AHashMap<String, Vec<FileInfo>>,
}

impl QueryExplainer {
    /// Create a new query explainer.
    pub fn new(cost_estimator: QueryCostEstimator) -> Self {
        Self {
            cost_estimator,
            table_files: AHashMap::new(),
        }
    }

    /// Add file information for a table.
    pub fn add_table_files(&mut self, table: &str, files: Vec<FileInfo>) {
        self.table_files.insert(table.to_string(), files);
    }

    /// Explain a query.
    pub fn explain(&mut self, sql: &str) -> QueryExplain {
        let mut explain = QueryExplain::default();

        if let Ok(cost) = self.cost_estimator.estimate(sql) {
            explain.estimated_cost = cost.clone();

            if cost.cache_hit {
                explain.plan.push(ExplainStep {
                    step_type: StepType::CacheHit {
                        query_hash: "cached".to_string(),
                    },
                    description: "Result served from cache".to_string(),
                    estimated_rows: 0,
                    estimated_bytes: 0,
                });
                explain
                    .optimizations_applied
                    .push("Query cache hit".to_string());
                return explain;
            }

            for warning in &cost.warnings {
                explain.warnings.push(format!("{:?}", warning));
            }
        }

        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap_or_default();

        let tables = TableRewriter::extract_tables_from_ast(&statements);

        for table in &tables {
            let files: Vec<String> = self
                .table_files
                .get(table)
                .map(|f| f.iter().map(|fi| fi.path.clone()).collect())
                .unwrap_or_default();

            let total_files = files.len();
            let estimated_rows = self
                .table_files
                .get(table)
                .map(|f| f.iter().map(|fi| fi.row_count).sum())
                .unwrap_or(0);
            let estimated_bytes = self
                .table_files
                .get(table)
                .map(|f| f.iter().map(|fi| fi.size_bytes).sum())
                .unwrap_or(0);

            explain.plan.push(ExplainStep {
                step_type: StepType::TableScan {
                    table: table.clone(),
                    files: files.clone(),
                },
                description: format!("Scan {} files from {}", total_files, table),
                estimated_rows,
                estimated_bytes,
            });

            if !files.is_empty() {
                explain.optimizations_applied.push(format!(
                    "Reading {} files for {}",
                    total_files, table
                ));
            }
        }

        let (has_where, has_group_by, has_order_by, has_join) =
            analyze_query_structure(&statements);

        if has_where {
            explain.plan.push(ExplainStep {
                step_type: StepType::Filter {
                    predicate: "WHERE clause".to_string(),
                },
                description: "Filter rows based on WHERE clause".to_string(),
                estimated_rows: 0,
                estimated_bytes: 0,
            });
        }

        if has_group_by {
            explain.plan.push(ExplainStep {
                step_type: StepType::Aggregate {
                    columns: vec!["(aggregation columns)".to_string()],
                },
                description: "Aggregate rows".to_string(),
                estimated_rows: 0,
                estimated_bytes: 0,
            });
        }

        if has_order_by {
            explain.plan.push(ExplainStep {
                step_type: StepType::Sort {
                    columns: vec!["(sort columns)".to_string()],
                },
                description: "Sort results".to_string(),
                estimated_rows: 0,
                estimated_bytes: 0,
            });
        }

        if has_join {
            explain.plan.push(ExplainStep {
                step_type: StepType::Join {
                    join_type: "INNER".to_string(),
                    left: "left_table".to_string(),
                    right: "right_table".to_string(),
                },
                description: "Join tables".to_string(),
                estimated_rows: 0,
                estimated_bytes: 0,
            });
        }

        explain
    }
}

/// Analyze parsed SQL statements for structural features (WHERE, GROUP BY, ORDER BY, JOIN).
fn analyze_query_structure(statements: &[Statement]) -> (bool, bool, bool, bool) {
    let mut has_where = false;
    let mut has_group_by = false;
    let mut has_order_by = false;
    let mut has_join = false;

    for stmt in statements {
        if let Statement::Query(query) = stmt {
            if query.order_by.is_some() {
                has_order_by = true;
            }
            if let SetExpr::Select(select) = query.body.as_ref() {
                if select.selection.is_some() {
                    has_where = true;
                }
                if !matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref v, _) if v.is_empty())
                    && !matches!(select.group_by, sqlparser::ast::GroupByExpr::All(ref v) if v.is_empty())
                {
                    has_group_by = true;
                }
                for table_with_joins in &select.from {
                    if !table_with_joins.joins.is_empty() {
                        has_join = true;
                    }
                }
            }
        }
    }

    (has_where, has_group_by, has_order_by, has_join)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explain_simple_query() {
        let cost_estimator = QueryCostEstimator::new();
        let mut explainer = QueryExplainer::new(cost_estimator);

        let sql = "SELECT * FROM customers WHERE id = 1";
        let explain = explainer.explain(sql);

        // Should have a filter step
        assert!(explain
            .plan
            .iter()
            .any(|s| matches!(s.step_type, StepType::Filter { .. })));
    }

    #[test]
    fn test_explain_with_aggregation() {
        let cost_estimator = QueryCostEstimator::new();
        let mut explainer = QueryExplainer::new(cost_estimator);

        let sql = "SELECT status, COUNT(*) FROM orders GROUP BY status";
        let explain = explainer.explain(sql);

        // Should have an aggregate step
        assert!(explain
            .plan
            .iter()
            .any(|s| matches!(s.step_type, StepType::Aggregate { .. })));
    }
}
