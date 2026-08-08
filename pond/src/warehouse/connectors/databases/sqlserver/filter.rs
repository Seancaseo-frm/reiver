//! SQL Server Filter Builder
//!
//! Validates and builds SQL WHERE clauses for predicate pushdown.
//! Unlike MongoDB, SQL Server already uses SQL so this module focuses on:
//! - Column name validation (prevent injection)
//! - Parameterized query building
//! - Type-safe value handling

use once_cell::sync::Lazy;
use regex::Regex;

use super::utils::escape_sqlserver_string;
use crate::warehouse::connectors::{ConnectorError, ConnectorResult};

/// SQL comparison operator.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlOperator {
    Eq,          // =
    NotEq,       // != or <>
    Lt,          // <
    LtEq,        // <=
    Gt,          // >
    GtEq,        // >=
    Like,        // LIKE
    NotLike,     // NOT LIKE
    In,          // IN
    NotIn,       // NOT IN
    IsNull,      // IS NULL
    IsNotNull,   // IS NOT NULL
    Between,     // BETWEEN
}

impl SqlOperator {
    /// Convert to SQL string representation.
    pub fn to_sql(&self) -> &'static str {
        match self {
            SqlOperator::Eq => "=",
            SqlOperator::NotEq => "<>",
            SqlOperator::Lt => "<",
            SqlOperator::LtEq => "<=",
            SqlOperator::Gt => ">",
            SqlOperator::GtEq => ">=",
            SqlOperator::Like => "LIKE",
            SqlOperator::NotLike => "NOT LIKE",
            SqlOperator::In => "IN",
            SqlOperator::NotIn => "NOT IN",
            SqlOperator::IsNull => "IS NULL",
            SqlOperator::IsNotNull => "IS NOT NULL",
            SqlOperator::Between => "BETWEEN",
        }
    }
}

/// A SQL predicate for filtering.
#[derive(Debug, Clone)]
pub struct SqlPredicate {
    /// Column name
    pub column: String,
    /// Comparison operator
    pub operator: SqlOperator,
    /// Value(s) for comparison
    pub value: PredicateValue,
}

/// Value for a SQL predicate.
#[derive(Debug, Clone)]
pub enum PredicateValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<PredicateValue>),
    Range(Box<PredicateValue>, Box<PredicateValue>), // For BETWEEN
}

impl PredicateValue {
    /// Convert to SQL literal string.
    pub fn to_sql_literal(&self) -> String {
        match self {
            PredicateValue::Null => "NULL".to_string(),
            PredicateValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            PredicateValue::Int(i) => i.to_string(),
            PredicateValue::Float(f) => f.to_string(),
            PredicateValue::String(s) => format!("'{}'", escape_sqlserver_string(s)),
            PredicateValue::List(items) => {
                let values: Vec<String> = items.iter().map(|v| v.to_sql_literal()).collect();
                format!("({})", values.join(", "))
            }
            PredicateValue::Range(low, high) => {
                format!("{} AND {}", low.to_sql_literal(), high.to_sql_literal())
            }
        }
    }
}

/// Regex for validating column names.
static COLUMN_NAME_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap());

/// Regex for detecting SQL injection patterns.
/// Uses word boundaries for SQL keywords to avoid false positives (e.g., "updated_at" matching "update").
static INJECTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(--|;|/\*|\*/|xp_|sp_|\bexec\b|\bexecute\b|\binsert\b|\bupdate\b|\bdelete\b|\bdrop\b|\bcreate\b|\balter\b|\btruncate\b|\bunion\b|\bdeclare\b)")
        .unwrap()
});

/// Builder for SQL Server WHERE clauses.
pub struct SqlServerFilterBuilder {
    schema: String,
    /// Collected predicates
    predicates: Vec<SqlPredicate>,
}

impl SqlServerFilterBuilder {
    /// Create a new filter builder.
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            predicates: Vec::new(),
        }
    }

    /// Add a predicate to the filter.
    pub fn add_predicate(&mut self, predicate: SqlPredicate) -> ConnectorResult<&mut Self> {
        // Validate column name
        validate_column_name(&predicate.column)?;
        self.predicates.push(predicate);
        Ok(self)
    }

    /// Build the WHERE clause.
    ///
    /// Returns None if no predicates are added.
    pub fn build_where_clause(&self) -> Option<String> {
        if self.predicates.is_empty() {
            return None;
        }

        let conditions: Vec<String> = self
            .predicates
            .iter()
            .map(|p| predicate_to_sql(p))
            .collect();

        Some(format!("WHERE {}", conditions.join(" AND ")))
    }

    /// Build a complete SELECT query with the filter.
    pub fn build_select_query(&self, table: &str, columns: Option<&[&str]>) -> ConnectorResult<String> {
        validate_identifier(table)?;

        let column_list = match columns {
            Some(cols) => {
                for col in cols {
                    validate_column_name(col)?;
                }
                cols.iter()
                    .map(|c| format!("[{}]", c))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
            None => "*".to_string(),
        };

        let mut query = format!(
            "SELECT {} FROM [{}].[{}]",
            column_list, self.schema, table
        );

        if let Some(where_clause) = self.build_where_clause() {
            query.push(' ');
            query.push_str(&where_clause);
        }

        Ok(query)
    }

    /// Build a query to fetch by IDs (for index-accelerated queries).
    pub fn build_id_query(
        &self,
        table: &str,
        id_column: &str,
        ids: &[String],
    ) -> ConnectorResult<String> {
        validate_identifier(table)?;
        validate_column_name(id_column)?;

        if ids.is_empty() {
            return Ok(format!(
                "SELECT * FROM [{}].[{}] WHERE 1=0",
                self.schema, table
            ));
        }

        const MAX_IN_CLAUSE_SIZE: usize = 10000;

        if ids.len() > MAX_IN_CLAUSE_SIZE {
            return Err(ConnectorError::Validation(format!(
                "ID list size {} exceeds maximum IN clause size of {}; \
                 callers must chunk the request to avoid incomplete results",
                ids.len(),
                MAX_IN_CLAUSE_SIZE,
            )));
        }

        let id_list: Vec<String> = ids
            .iter()
            .map(|id| format!("'{}'", escape_sqlserver_string(id)))
            .collect();

        Ok(format!(
            "SELECT * FROM [{}].[{}] WHERE [{}] IN ({})",
            self.schema,
            table,
            id_column,
            id_list.join(", ")
        ))
    }
}

/// Validate a column name.
pub fn validate_column_name(name: &str) -> ConnectorResult<()> {
    if name.is_empty() {
        return Err(ConnectorError::Validation(
            "Column name cannot be empty".to_string(),
        ));
    }

    if name.len() > 128 {
        return Err(ConnectorError::Validation(format!(
            "Column name too long: {} characters (max 128)",
            name.len()
        )));
    }

    if !COLUMN_NAME_REGEX.is_match(name) {
        return Err(ConnectorError::Validation(format!(
            "Invalid column name: {}",
            name
        )));
    }

    if INJECTION_PATTERN.is_match(name) {
        return Err(ConnectorError::Validation(format!(
            "Column name contains forbidden pattern: {}",
            name
        )));
    }

    Ok(())
}

/// Validate a SQL identifier (table/schema name).
pub fn validate_identifier(name: &str) -> ConnectorResult<()> {
    if name.is_empty() {
        return Err(ConnectorError::Validation(
            "Identifier cannot be empty".to_string(),
        ));
    }

    if name.len() > 128 {
        return Err(ConnectorError::Validation(format!(
            "Identifier too long: {} characters (max 128)",
            name.len()
        )));
    }

    if !COLUMN_NAME_REGEX.is_match(name) {
        return Err(ConnectorError::Validation(format!(
            "Invalid identifier: {}",
            name
        )));
    }

    if INJECTION_PATTERN.is_match(name) {
        return Err(ConnectorError::Validation(format!(
            "Identifier contains forbidden pattern: {}",
            name
        )));
    }

    Ok(())
}

/// Validate a SQL filter string.
pub fn validate_sql_filter(filter: &str) -> ConnectorResult<()> {
    // Check for dangerous patterns
    if INJECTION_PATTERN.is_match(filter) {
        return Err(ConnectorError::Validation(
            "SQL filter contains forbidden pattern".to_string(),
        ));
    }

    // Check balanced parentheses
    let mut depth = 0i32;
    for c in filter.chars() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return Err(ConnectorError::Validation(
                        "Unbalanced parentheses in SQL filter".to_string(),
                    ));
                }
            }
            _ => {}
        }
    }

    if depth != 0 {
        return Err(ConnectorError::Validation(
            "Unbalanced parentheses in SQL filter".to_string(),
        ));
    }

    Ok(())
}

/// Convert a predicate to SQL.
fn predicate_to_sql(predicate: &SqlPredicate) -> String {
    let column = format!("[{}]", predicate.column);

    match predicate.operator {
        SqlOperator::IsNull => format!("{} IS NULL", column),
        SqlOperator::IsNotNull => format!("{} IS NOT NULL", column),
        SqlOperator::In | SqlOperator::NotIn => {
            format!(
                "{} {} {}",
                column,
                predicate.operator.to_sql(),
                predicate.value.to_sql_literal()
            )
        }
        SqlOperator::Between => {
            if let PredicateValue::Range(low, high) = &predicate.value {
                format!(
                    "{} BETWEEN {} AND {}",
                    column,
                    low.to_sql_literal(),
                    high.to_sql_literal()
                )
            } else {
                // Fallback - shouldn't happen with proper usage
                format!(
                    "{} {} {}",
                    column,
                    predicate.operator.to_sql(),
                    predicate.value.to_sql_literal()
                )
            }
        }
        _ => {
            format!(
                "{} {} {}",
                column,
                predicate.operator.to_sql(),
                predicate.value.to_sql_literal()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_column_name_valid() {
        assert!(validate_column_name("id").is_ok());
        assert!(validate_column_name("user_id").is_ok());
        assert!(validate_column_name("Column1").is_ok());
        assert!(validate_column_name("_private").is_ok());
    }

    #[test]
    fn test_validate_column_name_invalid() {
        assert!(validate_column_name("").is_err());
        assert!(validate_column_name("123abc").is_err());
        assert!(validate_column_name("col-name").is_err());
        assert!(validate_column_name("col;drop").is_err());
        assert!(validate_column_name(&"x".repeat(200)).is_err());
    }

    #[test]
    fn test_predicate_to_sql() {
        let pred = SqlPredicate {
            column: "status".to_string(),
            operator: SqlOperator::Eq,
            value: PredicateValue::String("active".to_string()),
        };
        assert_eq!(predicate_to_sql(&pred), "[status] = 'active'");

        let pred_int = SqlPredicate {
            column: "age".to_string(),
            operator: SqlOperator::GtEq,
            value: PredicateValue::Int(18),
        };
        assert_eq!(predicate_to_sql(&pred_int), "[age] >= 18");

        let pred_null = SqlPredicate {
            column: "deleted_at".to_string(),
            operator: SqlOperator::IsNull,
            value: PredicateValue::Null,
        };
        assert_eq!(predicate_to_sql(&pred_null), "[deleted_at] IS NULL");

        let pred_in = SqlPredicate {
            column: "country".to_string(),
            operator: SqlOperator::In,
            value: PredicateValue::List(vec![
                PredicateValue::String("US".to_string()),
                PredicateValue::String("UK".to_string()),
            ]),
        };
        assert_eq!(predicate_to_sql(&pred_in), "[country] IN ('US', 'UK')");
    }

    #[test]
    fn test_build_where_clause() {
        let mut builder = SqlServerFilterBuilder::new("dbo");
        
        builder.add_predicate(SqlPredicate {
            column: "status".to_string(),
            operator: SqlOperator::Eq,
            value: PredicateValue::String("active".to_string()),
        }).unwrap();

        builder.add_predicate(SqlPredicate {
            column: "age".to_string(),
            operator: SqlOperator::GtEq,
            value: PredicateValue::Int(18),
        }).unwrap();

        let where_clause = builder.build_where_clause().unwrap();
        assert!(where_clause.contains("[status] = 'active'"));
        assert!(where_clause.contains("[age] >= 18"));
        assert!(where_clause.contains(" AND "));
    }

    #[test]
    fn test_build_select_query() {
        let mut builder = SqlServerFilterBuilder::new("dbo");
        
        builder.add_predicate(SqlPredicate {
            column: "active".to_string(),
            operator: SqlOperator::Eq,
            value: PredicateValue::Bool(true),
        }).unwrap();

        let query = builder.build_select_query("users", None).unwrap();
        assert!(query.contains("SELECT * FROM [dbo].[users]"));
        assert!(query.contains("WHERE [active] = 1"));
    }

    #[test]
    fn test_build_id_query() {
        let builder = SqlServerFilterBuilder::new("dbo");
        
        let ids = vec!["1".to_string(), "2".to_string(), "3".to_string()];
        let query = builder.build_id_query("users", "id", &ids).unwrap();
        
        assert!(query.contains("SELECT * FROM [dbo].[users]"));
        assert!(query.contains("WHERE [id] IN ('1', '2', '3')"));
    }

    #[test]
    fn test_build_id_query_empty() {
        let builder = SqlServerFilterBuilder::new("dbo");
        let query = builder.build_id_query("users", "id", &[]).unwrap();
        assert_eq!(query, "SELECT * FROM [dbo].[users] WHERE 1=0");
    }

    #[test]
    fn test_build_id_query_exceeds_limit_returns_error() {
        let builder = SqlServerFilterBuilder::new("dbo");
        let ids: Vec<String> = (0..10_001).map(|i| i.to_string()).collect();
        let result = builder.build_id_query("users", "id", &ids);
        assert!(result.is_err(), "Must return error when IDs exceed limit");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("10001") && err_msg.contains("10000"),
            "Error should mention both the actual size and the limit: {}",
            err_msg,
        );
    }

    #[test]
    fn test_build_id_query_at_limit_succeeds() {
        let builder = SqlServerFilterBuilder::new("dbo");
        let ids: Vec<String> = (0..10_000).map(|i| i.to_string()).collect();
        let result = builder.build_id_query("users", "id", &ids);
        assert!(result.is_ok(), "Exactly 10000 IDs should succeed");
    }

    #[test]
    fn test_validate_sql_filter() {
        assert!(validate_sql_filter("status = 'active'").is_ok());
        assert!(validate_sql_filter("(a = 1) AND (b = 2)").is_ok());
        
        // Injection attempts
        assert!(validate_sql_filter("1; DROP TABLE users").is_err());
        assert!(validate_sql_filter("1 UNION SELECT * FROM passwords").is_err());
        
        // Unbalanced parentheses
        assert!(validate_sql_filter("(a = 1").is_err());
        assert!(validate_sql_filter("a = 1)").is_err());
    }
}
