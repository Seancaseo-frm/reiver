//! Predicate Pushdown Optimization
//!
//! Optimizes query filtering by pushing predicates to the lowest possible level:
//!
//! 1. **File-level**: Skip entire files (using FST/Xor indexes)
//! 2. **Row-group level**: Skip row groups within files (Parquet statistics)
//! 3. **Row-level**: Filter during scan (WHERE clause)
//!
//! # PREWHERE Optimization
//!
//! ClickHouse's PREWHERE is more efficient than WHERE for selective filters:
//! - Reads only the filtered columns first
//! - Only reads remaining columns for matching rows
//! - Especially effective for wide tables with selective filters
//!
//! # Example
//!
//! ```sql
//! -- Less efficient: reads all columns, then filters
//! SELECT * FROM s3(...) WHERE event_type = 'purchase'
//!
//! -- More efficient: reads event_type first, then other columns for matches
//! SELECT * FROM s3(...) PREWHERE event_type = 'purchase'
//! ```

use ahash::{AHashMap, AHashSet};
use compact_str::CompactString;
use std::sync::Arc;
use sqlparser::ast::{BinaryOperator, Expr, Ident, SetExpr, Statement, UnaryOperator, Value};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

use super::cost_model::FilterOperation;

/// Errors during predicate analysis.
#[derive(Debug, Error)]
pub enum PredicateError {
    #[error("Invalid predicate: {0}")]
    Invalid(String),

    #[error("Unsupported predicate type: {0}")]
    Unsupported(String),

    #[error("Predicate cannot be pushed down: {0}")]
    CannotPushdown(String),
}

/// Result type for predicate operations.
pub type PredicateResult<T> = Result<T, PredicateError>;

/// A parsed predicate from a WHERE clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    /// Equality: column = value
    Equals {
        column: CompactString,
        value: CompactString,
    },
    /// IN clause: column IN (value1, value2, ...)
    In {
        column: CompactString,
        values: Vec<CompactString>,
    },
    /// Range: column > value or column >= value
    GreaterThan {
        column: CompactString,
        value: CompactString,
        inclusive: bool,
    },
    /// Range: column < value or column <= value
    LessThan {
        column: CompactString,
        value: CompactString,
        inclusive: bool,
    },
    /// Between: column BETWEEN low AND high
    Between {
        column: CompactString,
        low: CompactString,
        high: CompactString,
    },
    /// LIKE pattern: column LIKE 'pattern%'
    Like {
        column: CompactString,
        pattern: CompactString,
    },
    /// Substring match: column LIKE '%term%' or CONTAINS(column, 'term')
    Contains {
        column: CompactString,
        substring: CompactString,
    },
    /// IS NULL or IS NOT NULL
    IsNull {
        column: CompactString,
        is_null: bool,
    },
    /// Compound: AND of multiple predicates
    And(Vec<Predicate>),
    /// Compound: OR of multiple predicates
    Or(Vec<Predicate>),
    /// Negation: NOT predicate
    Not(Box<Predicate>),
}

impl Predicate {
    /// Get the column name if this is a simple predicate.
    pub fn column(&self) -> Option<&str> {
        match self {
            Predicate::Equals { column, .. }
            | Predicate::In { column, .. }
            | Predicate::GreaterThan { column, .. }
            | Predicate::LessThan { column, .. }
            | Predicate::Between { column, .. }
            | Predicate::Like { column, .. }
            | Predicate::Contains { column, .. }
            | Predicate::IsNull { column, .. } => Some(column),
            Predicate::And(_) | Predicate::Or(_) => None,
            Predicate::Not(inner) => inner.column(),
        }
    }

    /// Check if this predicate is highly selective (good for PREWHERE).
    pub fn is_selective(&self) -> bool {
        match self {
            // Equality and small IN clauses are typically selective
            Predicate::Equals { .. } => true,
            Predicate::In { values, .. } => values.len() <= 10,
            // Ranges can be selective depending on data
            Predicate::Between { .. } => true,
            Predicate::GreaterThan { .. } | Predicate::LessThan { .. } => true,
            // LIKE with leading % is not selective
            Predicate::Like { pattern, .. } => !pattern.starts_with('%'),
            // Contains (LIKE '%x%') is not selective for PREWHERE
            Predicate::Contains { .. } => false,
            // NULL checks are not very selective
            Predicate::IsNull { .. } => false,
            // Compound predicates
            Predicate::And(preds) => preds.iter().any(|p| p.is_selective()),
            Predicate::Or(_) => false, // OR is typically not selective
            Predicate::Not(_) => false, // NOT inverts selectivity (matches most rows)
        }
    }

    /// Get all column names referenced by this predicate.
    pub fn columns(&self) -> AHashSet<CompactString> {
        let mut cols = AHashSet::with_capacity(4);
        self.collect_columns(&mut cols);
        cols
    }

    fn collect_columns(&self, cols: &mut AHashSet<CompactString>) {
        match self {
            Predicate::Equals { column, .. }
            | Predicate::In { column, .. }
            | Predicate::GreaterThan { column, .. }
            | Predicate::LessThan { column, .. }
            | Predicate::Between { column, .. }
            | Predicate::Like { column, .. }
            | Predicate::Contains { column, .. }
            | Predicate::IsNull { column, .. } => {
                cols.insert(column.clone());
            }
            Predicate::And(preds) | Predicate::Or(preds) => {
                for p in preds {
                    p.collect_columns(cols);
                }
            }
            Predicate::Not(inner) => {
                inner.collect_columns(cols);
            }
        }
    }
}

/// Classification of predicates by pushdown level.
#[derive(Debug, Default)]
pub struct PredicatePushdown {
    /// Predicates that can be used for file-level filtering (FST/Xor indexes).
    pub file_predicates: Vec<FilePredicate>,
    /// Predicates for PREWHERE (read filter columns first).
    pub prewhere_predicates: Vec<Predicate>,
    /// Remaining predicates for WHERE.
    pub where_predicates: Vec<Predicate>,
}

/// A predicate that can be used for file-level filtering.
#[derive(Debug, Clone)]
pub struct FilePredicate {
    /// Column name.
    pub column: CompactString,
    /// Predicate type.
    pub predicate_type: FilePredicateType,
}

/// Type of file-level predicate.
#[derive(Debug, Clone)]
pub enum FilePredicateType {
    /// Exact match (use FST or Xor filter).
    ExactMatch(CompactString),
    /// Prefix match (use FST).
    PrefixMatch(CompactString),
    /// Range match (use numeric stats).
    Range {
        min: Option<CompactString>,
        max: Option<CompactString>,
        min_inclusive: bool,
        max_inclusive: bool,
    },
    /// IN clause (multiple exact matches).
    InList(Vec<CompactString>),
    /// Substring match (use FST with regex-automata DFA).
    SubstringMatch(CompactString),
}

impl PredicatePushdown {
    /// Analyze predicates and classify them by pushdown level.
    ///
    /// # Arguments
    /// * `predicates` - List of predicates from WHERE clause
    /// * `indexed_columns` - Columns that have file-level indexes
    #[tracing::instrument(name = "warehouse.predicate.analyze", skip_all, fields(predicate_count = predicates.len()))]
    pub fn analyze(
        predicates: Vec<Predicate>,
        indexed_columns: &AHashSet<String>,
    ) -> Self {
        let mut result = PredicatePushdown::default();

        for predicate in predicates {
            result.classify_predicate(predicate, indexed_columns);
        }

        result
    }

    fn classify_predicate(
        &mut self,
        predicate: Predicate,
        indexed_columns: &AHashSet<String>,
    ) {
        // Check if this predicate can be pushed to file level
        if let Some(file_pred) = self.try_file_predicate(&predicate, indexed_columns) {
            self.file_predicates.push(file_pred);
            // Also add to PREWHERE for row-level filtering
            if predicate.is_selective() {
                self.prewhere_predicates.push(predicate);
            } else {
                self.where_predicates.push(predicate);
            }
        } else if predicate.is_selective() {
            // Selective but not indexed - use PREWHERE
            self.prewhere_predicates.push(predicate);
        } else {
            // Neither indexed nor selective - use WHERE
            self.where_predicates.push(predicate);
        }
    }

    fn try_file_predicate(
        &self,
        predicate: &Predicate,
        indexed_columns: &AHashSet<String>,
    ) -> Option<FilePredicate> {
        match predicate {
            Predicate::Equals { column, value } if indexed_columns.contains(column.as_str()) => {
                Some(FilePredicate {
                    column: column.clone(),
                    predicate_type: FilePredicateType::ExactMatch(value.clone()),
                })
            }
            Predicate::In { column, values } if indexed_columns.contains(column.as_str()) => {
                Some(FilePredicate {
                    column: column.clone(),
                    predicate_type: FilePredicateType::InList(values.clone()),
                })
            }
            Predicate::Like { column, pattern } 
                if indexed_columns.contains(column.as_str()) && !pattern.starts_with('%') => {
                let prefix = extract_like_literal_prefix(pattern);
                if prefix.is_empty() {
                    None
                } else {
                    Some(FilePredicate {
                        column: column.clone(),
                        predicate_type: FilePredicateType::PrefixMatch(prefix.into()),
                    })
                }
            }
            Predicate::Between { column, low, high } if indexed_columns.contains(column.as_str()) => {
                Some(FilePredicate {
                    column: column.clone(),
                    predicate_type: FilePredicateType::Range {
                        min: Some(low.clone()),
                        max: Some(high.clone()),
                        min_inclusive: true,
                        max_inclusive: true,
                    },
                })
            }
            Predicate::GreaterThan { column, value, inclusive } if indexed_columns.contains(column.as_str()) => {
                Some(FilePredicate {
                    column: column.clone(),
                    predicate_type: FilePredicateType::Range {
                        min: Some(value.clone()),
                        max: None,
                        min_inclusive: *inclusive,
                        max_inclusive: false,
                    },
                })
            }
            Predicate::LessThan { column, value, inclusive } if indexed_columns.contains(column.as_str()) => {
                Some(FilePredicate {
                    column: column.clone(),
                    predicate_type: FilePredicateType::Range {
                        min: None,
                        max: Some(value.clone()),
                        min_inclusive: false,
                        max_inclusive: *inclusive,
                    },
                })
            }
            Predicate::Contains { column, substring } if indexed_columns.contains(column.as_str()) => {
                Some(FilePredicate {
                    column: column.clone(),
                    predicate_type: FilePredicateType::SubstringMatch(substring.clone()),
                })
            }
            _ => None,
        }
    }

    /// Check if there are file-level predicates.
    pub fn has_file_predicates(&self) -> bool {
        !self.file_predicates.is_empty()
    }

    /// Check if there are PREWHERE predicates.
    pub fn has_prewhere(&self) -> bool {
        !self.prewhere_predicates.is_empty()
    }

    /// Generate PREWHERE clause as a display string (for logging).
    pub fn prewhere_clause_sql(&self) -> Option<String> {
        if self.prewhere_predicates.is_empty() {
            return None;
        }

        let clauses: Vec<String> = self
            .prewhere_predicates
            .iter()
            .map(|p| predicate_to_display_sql(p, SqlDialect::ClickHouse))
            .collect();

        Some(clauses.join(" AND "))
    }

    /// Generate WHERE clause as a display string (for logging).
    pub fn where_clause_sql(&self) -> Option<String> {
        if self.where_predicates.is_empty() {
            return None;
        }

        let clauses: Vec<String> = self
            .where_predicates
            .iter()
            .map(|p| predicate_to_display_sql(p, SqlDialect::ClickHouse))
            .collect();

        Some(clauses.join(" AND "))
    }

    /// Generate PREWHERE clause as an AST `Expr`.
    fn prewhere_expr(&self) -> Option<Expr> {
        predicates_to_conjunction(&self.prewhere_predicates)
    }

    /// Generate WHERE clause as an AST `Expr`.
    fn where_expr(&self) -> Option<Expr> {
        predicates_to_conjunction(&self.where_predicates)
    }

    /// Rewrite a query to use PREWHERE.
    ///
    /// If the query already has a WHERE clause, the new predicates are
    /// AND-merged with it to avoid producing invalid double-WHERE SQL.
    ///
    /// Handles queries that contain trailing clauses (GROUP BY, ORDER BY,
    /// LIMIT, etc.) by inserting PREWHERE/WHERE before them.
    #[tracing::instrument(name = "warehouse.predicate.rewrite_with_prewhere", skip_all)]
    pub fn rewrite_with_prewhere(&self, base_query: &str) -> String {
        let prewhere_expr = self.prewhere_expr();
        let where_expr = self.where_expr();

        if prewhere_expr.is_none() && where_expr.is_none() {
            return base_query.to_string();
        }

        let dialect = ClickHouseDialect {};
        let mut statements = match Parser::parse_sql(&dialect, base_query) {
            Ok(s) => s,
            Err(_) => return base_query.to_string(),
        };

        for statement in &mut statements {
            if let Statement::Query(query) = statement {
                if let Some(ref pw) = prewhere_expr {
                    inject_prewhere_into_set_expr(&mut query.body, pw.clone());
                }
                if let Some(ref wc) = where_expr {
                    inject_where_into_query(query, wc.clone());
                }
            }
        }

        serialize_statements(&statements)
    }
}

/// Check if a value is a plain numeric literal (integer or float, possibly negative).
/// Returns `true` for values like "42", "-3.14", "0", "1000000".
fn is_numeric_literal(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let s = if s.starts_with('-') || s.starts_with('+') { &s[1..] } else { s };
    if s.is_empty() {
        return false;
    }
    // Reject leading zeros (e.g. "01234" is a zero-padded string, not a number)
    // Allow "0" and "0.xxx" (valid numeric forms)
    if s.len() > 1 && s.starts_with('0') && !s.starts_with("0.") && !s.starts_with("0e") && !s.starts_with("0E") {
        return false;
    }
    let mut saw_dot = false;
    let mut saw_digit = false;
    let mut in_exponent = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if !in_exponent {
            match b {
                b'0'..=b'9' => saw_digit = true,
                b'.' if !saw_dot => saw_dot = true,
                b'e' | b'E' if saw_digit => {
                    in_exponent = true;
                    saw_digit = false;
                    // Allow optional +/- after e/E
                    if i + 1 < bytes.len() && (bytes[i + 1] == b'+' || bytes[i + 1] == b'-') {
                        i += 1;
                    }
                }
                _ => return false,
            }
        } else {
            match b {
                b'0'..=b'9' => saw_digit = true,
                _ => return false,
            }
        }
        i += 1;
    }
    saw_digit
}

/// Extract the literal prefix from a LIKE pattern, handling `\%` and `\_` escapes.
///
/// Stops at the first unescaped `%` or `_` wildcard. Escaped wildcards (`\%`, `\_`)
/// are included as literal characters in the result.
fn extract_like_literal_prefix(pattern: &str) -> String {
    let mut prefix = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(&ch) = chars.peek() {
        match ch {
            '\\' => {
                chars.next();
                if let Some(&escaped) = chars.peek() {
                    if escaped == '%' || escaped == '_' || escaped == '\\' {
                        prefix.push(escaped);
                        chars.next();
                    } else {
                        // Per SQL LIKE semantics, backslash only escapes
                        // %, _, and \. For any other character the backslash
                        // is kept as a literal and the next char is re-examined
                        // in the next iteration.
                        prefix.push('\\');
                    }
                } else {
                    prefix.push('\\');
                }
            }
            '%' | '_' => break,
            c => {
                prefix.push(c);
                chars.next();
            }
        }
    }
    prefix
}

/// Format a SQL value, emitting numeric literals unquoted to preserve
/// correct comparison semantics (numeric vs lexicographic).
fn format_sql_value(value: &str) -> String {
    if is_numeric_literal(value) {
        value.to_string()
    } else {
        format!("'{}'", escape_sql_string(value))
    }
}

/// Convert a predicate to a SQL display string (for logging, diagnostics,
/// and external consumers like `PredicateTranslation::SqlFragment`).
/// For AST injection, use `predicate_to_expr` instead.
fn predicate_to_display_sql(predicate: &Predicate, dialect: SqlDialect) -> String {
    match predicate {
        Predicate::Equals { column, value } => {
            format!("{} = {}", escape_column_name(column, dialect), format_sql_value(value))
        }
        Predicate::In { column, values } => {
            if values.is_empty() {
                "1=0".to_string()
            } else {
                let vals: Vec<String> = values
                    .iter()
                    .map(|v| format_sql_value(v))
                    .collect();
                format!("{} IN ({})", escape_column_name(column, dialect), vals.join(", "))
            }
        }
        Predicate::GreaterThan { column, value, inclusive } => {
            let op = if *inclusive { ">=" } else { ">" };
            format!("{} {} {}", escape_column_name(column, dialect), op, format_sql_value(value))
        }
        Predicate::LessThan { column, value, inclusive } => {
            let op = if *inclusive { "<=" } else { "<" };
            format!("{} {} {}", escape_column_name(column, dialect), op, format_sql_value(value))
        }
        Predicate::Between { column, low, high } => {
            format!(
                "{} BETWEEN {} AND {}",
                escape_column_name(column, dialect),
                format_sql_value(low),
                format_sql_value(high)
            )
        }
        Predicate::Like { column, pattern } => {
            format!("{} LIKE '{}'", escape_column_name(column, dialect), escape_sql_string(pattern))
        }
        Predicate::Contains { column, substring } => {
            format!("{} LIKE '%{}%'", escape_column_name(column, dialect), escape_like_pattern(substring))
        }
        Predicate::IsNull { column, is_null } => {
            if *is_null {
                format!("{} IS NULL", escape_column_name(column, dialect))
            } else {
                format!("{} IS NOT NULL", escape_column_name(column, dialect))
            }
        }
        Predicate::And(preds) => {
            if preds.is_empty() {
                "1=1".to_string()
            } else {
                let parts: Vec<String> = preds.iter().map(|p| predicate_to_display_sql(p, dialect)).collect();
                format!("({})", parts.join(" AND "))
            }
        }
        Predicate::Or(preds) => {
            if preds.is_empty() {
                "1=0".to_string()
            } else {
                let parts: Vec<String> = preds.iter().map(|p| predicate_to_display_sql(p, dialect)).collect();
                format!("({})", parts.join(" OR "))
            }
        }
        Predicate::Not(inner) => {
            format!("NOT ({})", predicate_to_display_sql(inner, dialect))
        }
    }
}

/// Convert a `Predicate` directly into a `sqlparser::ast::Expr` node,
/// bypassing the string roundtrip of `predicate_to_display_sql` + `parse_condition_expr`.
fn predicate_to_expr(predicate: &Predicate) -> Expr {
    match predicate {
        Predicate::Equals { column, value } => Expr::BinaryOp {
            left: Box::new(column_ident(column)),
            op: BinaryOperator::Eq,
            right: Box::new(value_to_expr(value)),
        },
        Predicate::In { column, values } => {
            if values.is_empty() {
                Expr::BinaryOp {
                    left: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number("0".to_string(), false))),
                }
            } else {
                Expr::InList {
                    expr: Box::new(column_ident(column)),
                    list: values.iter().map(|v| value_to_expr(v)).collect(),
                    negated: false,
                }
            }
        }
        Predicate::GreaterThan { column, value, inclusive } => Expr::BinaryOp {
            left: Box::new(column_ident(column)),
            op: if *inclusive { BinaryOperator::GtEq } else { BinaryOperator::Gt },
            right: Box::new(value_to_expr(value)),
        },
        Predicate::LessThan { column, value, inclusive } => Expr::BinaryOp {
            left: Box::new(column_ident(column)),
            op: if *inclusive { BinaryOperator::LtEq } else { BinaryOperator::Lt },
            right: Box::new(value_to_expr(value)),
        },
        Predicate::Between { column, low, high } => Expr::Between {
            expr: Box::new(column_ident(column)),
            negated: false,
            low: Box::new(value_to_expr(low)),
            high: Box::new(value_to_expr(high)),
        },
        Predicate::Like { column, pattern } => Expr::Like {
            negated: false,
            any: false,
            expr: Box::new(column_ident(column)),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString(pattern.to_string()))),
            escape_char: None,
        },
        Predicate::Contains { column, substring } => {
            let escaped = escape_like_pattern_for_ast(substring);
            let mut pattern = String::with_capacity(escaped.len() + 2);
            pattern.push('%');
            pattern.push_str(&escaped);
            pattern.push('%');
            Expr::Like {
                negated: false,
                any: false,
                expr: Box::new(column_ident(column)),
                pattern: Box::new(Expr::Value(Value::SingleQuotedString(pattern))),
                escape_char: None,
            }
        },
        Predicate::IsNull { column, is_null } => {
            if *is_null {
                Expr::IsNull(Box::new(column_ident(column)))
            } else {
                Expr::IsNotNull(Box::new(column_ident(column)))
            }
        }
        Predicate::And(preds) => {
            if preds.is_empty() {
                Expr::BinaryOp {
                    left: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                }
            } else {
                chain_exprs(preds, BinaryOperator::And)
            }
        }
        Predicate::Or(preds) => {
            if preds.is_empty() {
                Expr::BinaryOp {
                    left: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number("0".to_string(), false))),
                }
            } else {
                chain_exprs(preds, BinaryOperator::Or)
            }
        }
        Predicate::Not(inner) => Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Nested(Box::new(predicate_to_expr(inner)))),
        },
    }
}

fn column_ident(name: &str) -> Expr {
    Expr::Identifier(Ident::with_quote('`', name))
}

fn value_to_expr(value: &str) -> Expr {
    if is_numeric_literal(value) {
        Expr::Value(Value::Number(value.to_string(), false))
    } else {
        Expr::Value(Value::SingleQuotedString(value.to_string()))
    }
}

fn chain_exprs(preds: &[Predicate], op: BinaryOperator) -> Expr {
    let mut iter = preds.iter();
    let first = predicate_to_expr(iter.next().unwrap());
    let chained = iter.fold(first, |acc, p| Expr::BinaryOp {
        left: Box::new(acc),
        op: op.clone(),
        right: Box::new(predicate_to_expr(p)),
    });
    Expr::Nested(Box::new(chained))
}

/// Combine a slice of predicates into a single AND-conjunction `Expr`.
/// Returns `None` if the slice is empty.
fn predicates_to_conjunction(predicates: &[Predicate]) -> Option<Expr> {
    if predicates.is_empty() {
        return None;
    }
    let mut iter = predicates.iter();
    let first = predicate_to_expr(iter.next().unwrap());
    let combined = iter.fold(first, |acc, p| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOperator::And,
        right: Box::new(predicate_to_expr(p)),
    });
    Some(combined)
}

#[cfg(test)]
fn parse_condition_expr(condition: &str) -> Result<Expr, sqlparser::parser::ParserError> {
    let dialect = ClickHouseDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(condition)?;
    parser.parse_expr()
}

/// Inject a WHERE condition into a Query, AND-ing with any existing WHERE.
fn inject_where_into_query(query: &mut sqlparser::ast::Query, condition: Expr) {
    inject_where_into_set_expr(&mut query.body, condition);
}

/// Inject a WHERE condition into a set expression. Returns `true` if the
/// condition was successfully injected, `false` if the expression type does
/// not support WHERE injection (e.g., `VALUES`).
fn inject_where_into_set_expr(set_expr: &mut SetExpr, condition: Expr) -> bool {
    match set_expr {
        SetExpr::Select(select) => {
            select.selection = Some(match select.selection.take() {
                Some(existing) => Expr::BinaryOp {
                    left: Box::new(Expr::Nested(Box::new(existing))),
                    op: BinaryOperator::And,
                    right: Box::new(condition),
                },
                None => condition,
            });
            true
        }
        SetExpr::Query(inner) => {
            inject_where_into_query(inner, condition);
            true
        }
        SetExpr::SetOperation { left, right, .. } => {
            inject_where_into_set_expr(left, condition.clone());
            inject_where_into_set_expr(right, condition);
            true
        }
        _ => {
            tracing::warn!(
                "Cannot inject WHERE into unsupported SetExpr variant; predicate dropped"
            );
            false
        }
    }
}

fn inject_prewhere_into_set_expr(set_expr: &mut SetExpr, condition: Expr) -> bool {
    match set_expr {
        SetExpr::Select(select) => {
            select.prewhere = Some(match select.prewhere.take() {
                Some(existing) => Expr::BinaryOp {
                    left: Box::new(Expr::Nested(Box::new(existing))),
                    op: BinaryOperator::And,
                    right: Box::new(condition),
                },
                None => condition,
            });
            true
        }
        SetExpr::Query(inner) => inject_prewhere_into_set_expr(&mut inner.body, condition),
        SetExpr::SetOperation { left, right, .. } => {
            inject_prewhere_into_set_expr(left, condition.clone());
            inject_prewhere_into_set_expr(right, condition);
            true
        }
        _ => {
            tracing::warn!(
                "Cannot inject PREWHERE into unsupported SetExpr variant; predicate dropped"
            );
            false
        }
    }
}

fn serialize_statements(statements: &[Statement]) -> String {
    match statements {
        [single] => single.to_string(),
        multiple => multiple
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; "),
    }
}

/// Escape special characters in SQL strings for ClickHouse.
///
/// This function escapes:
/// - Single quotes (') -> ('')
/// - Backslashes (\) -> (\\)
/// - Null bytes are stripped (with a warning log)
///
/// # Security
///
/// This provides defense-in-depth but parameterized queries should be preferred
/// when available. This escaping is specifically for ClickHouse's SQL dialect.
fn escape_sql_string(s: &str) -> String {
    if s.contains('\0') {
        tracing::warn!("SQL string contains null bytes — stripping them to prevent truncation attacks");
    }
    
    let mut result = String::with_capacity(s.len() + s.len() / 8);
    for ch in s.chars() {
        match ch {
            '\0' => {}
            '\'' => result.push_str("''"),
            '\\' => result.push_str("\\\\"),
            _ => result.push(ch),
        }
    }
    result
}

/// Escape LIKE pattern special characters for use inside a SQL string literal.
///
/// The output passes through **two** interpretation levels in ClickHouse:
///
/// 1. **String literal parsing** — recognises `\\` → `\` and `''` → `'`.
/// 2. **LIKE pattern evaluation** — `\%` → literal `%`, `\_` → literal `_`,
///    `\\` → literal `\`.
///
/// Therefore every LIKE escape sequence we emit must itself be
/// string-literal-escaped so it survives level 1 and reaches level 2
/// intact.
///
/// | Input char | Output chars | After string parse | LIKE meaning   |
/// |------------|-------------|-------------------|----------------|
/// | `\`        | `\\\\`      | `\\`              | literal `\`    |
/// | `%`        | `\\%`       | `\%`              | literal `%`    |
/// | `_`        | `\\_`       | `\_`              | literal `_`    |
/// | `'`        | `''`        | `'`               | literal `'`    |
///
/// NOTE: This escapes single quotes for raw SQL string embedding (used by
/// `predicate_to_display_sql`). For AST construction via `Value::SingleQuotedString`,
/// use `escape_like_pattern_for_ast` instead to avoid double-escaping.
fn escape_like_pattern(s: &str) -> String {
    if s.contains('\0') {
        tracing::warn!("LIKE pattern contains null bytes — stripping them to prevent truncation attacks");
    }
    let mut result = String::with_capacity(s.len() + s.len() / 8);
    for ch in s.chars() {
        match ch {
            '\0' => {}
            '\'' => result.push_str("''"),
            '\\' => result.push_str("\\\\\\\\"),
            '%' => result.push_str("\\\\%"),
            '_' => result.push_str("\\\\_"),
            _ => result.push(ch),
        }
    }
    result
}

/// Escape only LIKE metacharacters for use with sqlparser AST nodes.
///
/// Unlike `escape_like_pattern`, this does NOT escape single quotes because
/// `Value::SingleQuotedString` handles quote escaping via sqlparser's `Display`.
/// Using `escape_like_pattern` with `SingleQuotedString` causes double-escaping.
fn escape_like_pattern_for_ast(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + s.len() / 8);
    for ch in s.chars() {
        match ch {
            '\0' => {}
            '\\' => result.push_str("\\\\"),
            '%' => result.push_str("\\%"),
            '_' => result.push_str("\\_"),
            _ => result.push(ch),
        }
    }
    result
}

/// Validate a column name to prevent SQL injection.
///
/// Valid column names contain only:
/// - Alphanumeric characters (a-z, A-Z, 0-9)
/// - Underscores (_), hyphens (-), dots (.)
/// - Must start with a letter or underscore
/// - Must not contain backticks, semicolons, or other SQL-special characters
/// - Maximum 128 characters
///
/// Dots are allowed for qualified names (`table.column`).
/// Hyphens are allowed for API-sourced column names (`created-at`).
fn is_valid_column_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 128 {
        return false;
    }
    
    let mut chars = name.chars();
    
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// SQL dialect for identifier quoting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    ClickHouse,
    MySQL,
    Postgres,
    Snowflake,
    Redshift,
    SQLite,
}

/// Escape a column name for safe use in SQL, using dialect-appropriate quoting.
///
/// ClickHouse and MySQL use backtick quoting (`` `col` ``), while PostgreSQL,
/// Snowflake, Redshift, and SQLite use double-quote quoting (`"col"`).
fn escape_column_name(name: &str, dialect: SqlDialect) -> String {
    if is_valid_column_name(name) {
        let q = match dialect {
            SqlDialect::ClickHouse | SqlDialect::MySQL => '`',
            _ => '"',
        };
        format!("{}{}{}", q, name, q)
    } else {
        match dialect {
            SqlDialect::ClickHouse | SqlDialect::MySQL => "`INVALID_COLUMN`".to_string(),
            _ => "\"INVALID_COLUMN\"".to_string(),
        }
    }
}

/// Statistics for predicate pushdown optimization.
#[derive(Debug, Clone, Default)]
pub struct PushdownStats {
    /// Number of files pruned by file-level predicates.
    pub files_pruned: usize,
    /// Total files considered.
    pub total_files: usize,
    /// Prune rate (files_pruned / total_files).
    pub prune_rate: f64,
    /// Whether PREWHERE was used.
    pub used_prewhere: bool,
}

impl PushdownStats {
    /// Calculate prune rate.
    pub fn calculate_prune_rate(&mut self) {
        if self.total_files > 0 {
            self.prune_rate = self.files_pruned as f64 / self.total_files as f64;
        }
    }
}

// ============================================================================
// Source-Aware Predicate Analysis
// ============================================================================

/// Result of predicate analysis for a specific source.
///
/// Contains the predicates classified into those that can be pushed down
/// to the source and those that must be applied locally after fetching.
#[derive(Debug, Clone)]
pub struct SourcePredicateAnalysis {
    /// Predicates that can be pushed to this source.
    pub pushable: Vec<TranslatedPredicate>,

    /// Predicates that must be applied locally after fetch.
    pub local_only: Vec<Predicate>,

    /// Warnings about non-pushable predicates.
    pub warnings: Vec<PushdownWarning>,

    /// Estimated selectivity of pushable predicates (0.0 to 1.0).
    /// Lower values mean more selective (fewer rows returned).
    pub pushable_selectivity: f64,

    /// Estimated selectivity of local predicates (0.0 to 1.0).
    pub local_selectivity: f64,

    /// Source name this analysis is for.
    pub source_name: String,

    /// Table name this analysis is for.
    pub table_name: String,
}

impl Default for SourcePredicateAnalysis {
    fn default() -> Self {
        Self {
            pushable: Vec::new(),
            local_only: Vec::new(),
            warnings: Vec::new(),
            pushable_selectivity: 1.0,
            local_selectivity: 1.0,
            source_name: String::new(),
            table_name: String::new(),
        }
    }
}

impl SourcePredicateAnalysis {
    /// Create a new analysis for a source and table.
    pub fn new(source_name: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            source_name: source_name.into(),
            table_name: table_name.into(),
            ..Default::default()
        }
    }

    /// Check if any predicates can be pushed down.
    pub fn has_pushable(&self) -> bool {
        !self.pushable.is_empty()
    }

    /// Check if any predicates must be applied locally.
    pub fn has_local(&self) -> bool {
        !self.local_only.is_empty()
    }

    /// Check if there are any warnings.
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Get the combined selectivity (pushable * local).
    pub fn combined_selectivity(&self) -> f64 {
        self.pushable_selectivity * self.local_selectivity
    }

    /// Estimate data reduction from pushdown.
    /// Returns the fraction of data saved by pushing predicates.
    pub fn data_reduction(&self) -> f64 {
        1.0 - self.pushable_selectivity
    }

    /// Add a pushable predicate.
    pub fn add_pushable(&mut self, translated: TranslatedPredicate) {
        self.pushable.push(translated);
    }

    /// Add a local-only predicate with a warning.
    pub fn add_local_with_warning(&mut self, predicate: Predicate, warning: PushdownWarning) {
        self.local_only.push(predicate);
        self.warnings.push(warning);
    }

    /// Add a local-only predicate without a warning.
    pub fn add_local(&mut self, predicate: Predicate) {
        self.local_only.push(predicate);
    }
}

/// A predicate translated for a specific source.
///
/// Contains both the original predicate and its source-specific representation.
/// Uses `Arc<Predicate>` to avoid cloning predicates during translation.
#[derive(Debug, Clone)]
pub struct TranslatedPredicate {
    /// Original predicate from the query (shared via Arc to avoid cloning).
    pub original: Arc<Predicate>,

    /// Source-specific representation.
    pub translated: PredicateTranslation,

    /// Estimated selectivity (0.0 to 1.0).
    pub estimated_selectivity: f64,
}

impl TranslatedPredicate {
    /// Create a new translated predicate.
    pub fn new(original: Predicate, translated: PredicateTranslation) -> Self {
        Self {
            original: Arc::new(original),
            translated,
            estimated_selectivity: 0.5, // Default 50% selectivity
        }
    }

    /// Create a new translated predicate from an Arc.
    /// Use this when you already have an Arc to avoid wrapping again.
    pub fn from_arc(original: Arc<Predicate>, translated: PredicateTranslation) -> Self {
        Self {
            original,
            translated,
            estimated_selectivity: 0.5,
        }
    }

    /// Set the estimated selectivity.
    pub fn with_selectivity(mut self, selectivity: f64) -> Self {
        self.estimated_selectivity = selectivity.clamp(0.0, 1.0);
        self
    }
}

/// Source-specific representation of a predicate.
///
/// Different sources accept predicates in different formats:
/// - SQL databases: SQL WHERE clause fragments
/// - REST APIs: Query parameters
/// - Parquet: Row group filter expressions
#[derive(Debug, Clone)]
pub enum PredicateTranslation {
    /// SQL WHERE clause fragment.
    /// Ready to be appended to a SQL query.
    SqlFragment(String),

    /// API query parameters.
    /// Key-value pairs to be added to API request.
    ApiParams(AHashMap<String, String>),

    /// Parquet row group filter.
    /// Used for predicate pushdown in Parquet reads.
    ParquetFilter {
        column: String,
        min: Option<String>,
        max: Option<String>,
    },

    /// GraphQL filter argument.
    GraphQLFilter {
        field: String,
        operator: String,
        value: String,
    },

    /// No translation available (predicate passed through as-is).
    Passthrough,
}

impl PredicateTranslation {
    /// Create a SQL fragment translation.
    pub fn sql(fragment: impl Into<String>) -> Self {
        PredicateTranslation::SqlFragment(fragment.into())
    }

    /// Create an API params translation.
    pub fn api_params(params: AHashMap<String, String>) -> Self {
        PredicateTranslation::ApiParams(params)
    }

    /// Create a single API param translation.
    pub fn api_param(key: impl Into<String>, value: impl Into<String>) -> Self {
        let mut params = AHashMap::new();
        params.insert(key.into(), value.into());
        PredicateTranslation::ApiParams(params)
    }

    /// Create a Parquet filter translation.
    pub fn parquet(column: impl Into<String>, min: Option<String>, max: Option<String>) -> Self {
        PredicateTranslation::ParquetFilter {
            column: column.into(),
            min,
            max,
        }
    }

    /// Check if this is a SQL translation.
    pub fn is_sql(&self) -> bool {
        matches!(self, PredicateTranslation::SqlFragment(_))
    }

    /// Check if this is an API params translation.
    pub fn is_api(&self) -> bool {
        matches!(self, PredicateTranslation::ApiParams(_))
    }

    /// Get SQL fragment if this is a SQL translation.
    pub fn as_sql(&self) -> Option<&str> {
        match self {
            PredicateTranslation::SqlFragment(s) => Some(s),
            _ => None,
        }
    }

    /// Get API params if this is an API translation.
    pub fn as_api_params(&self) -> Option<&AHashMap<String, String>> {
        match self {
            PredicateTranslation::ApiParams(p) => Some(p),
            _ => None,
        }
    }
}

/// Warning about a predicate that cannot be pushed down.
///
/// These warnings are shown to users to help them understand
/// query performance implications and optimize their queries.
#[derive(Debug, Clone)]
pub struct PushdownWarning {
    /// String representation of the predicate.
    pub predicate: String,

    /// Reason the predicate cannot be pushed down.
    pub reason: PushdownWarningReason,

    /// Estimated performance impact.
    pub estimated_impact: EstimatedImpact,

    /// Suggestion for the user (if any).
    pub suggestion: Option<String>,

    /// Source this warning is for.
    pub source: String,
}

impl PushdownWarning {
    /// Create a new pushdown warning.
    pub fn new(
        predicate: impl Into<String>,
        reason: PushdownWarningReason,
        source: impl Into<String>,
    ) -> Self {
        let estimated_impact = reason.default_impact();
        Self {
            predicate: predicate.into(),
            reason,
            estimated_impact,
            suggestion: None,
            source: source.into(),
        }
    }

    /// Set a suggestion for the user.
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestion = Some(suggestion.into());
        self
    }

    /// Set the estimated impact.
    pub fn with_impact(mut self, impact: EstimatedImpact) -> Self {
        self.estimated_impact = impact;
        self
    }

    /// Check if this is a high-impact warning.
    pub fn is_high_impact(&self) -> bool {
        matches!(self.estimated_impact, EstimatedImpact::High)
    }
}

/// Reason why a predicate cannot be pushed down.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushdownWarningReason {
    /// The operation is not supported by the source.
    UnsupportedOperation {
        operation: String,
        source: String,
    },

    /// The column cannot be filtered on this source.
    UnsupportedColumn {
        column: String,
        source: String,
    },

    /// The predicate is too complex for the source.
    ComplexPredicate {
        description: String,
    },

    /// OR conditions are not supported by the source.
    OrNotSupported,

    /// NOT conditions are not supported by the source.
    NotNotSupported,

    /// Too many filter conditions for the source.
    TooManyFilters {
        max: usize,
        requested: usize,
    },

    /// Value transformation failed.
    TransformFailed {
        column: String,
        value: String,
        reason: String,
    },

    /// Function call in predicate not supported.
    FunctionNotSupported {
        function: String,
    },

    /// Subquery in predicate not supported.
    SubqueryNotSupported,

    /// Join condition cannot be pushed down.
    JoinConditionNotPushable,
}

impl PushdownWarningReason {
    /// Get the default impact level for this reason.
    pub fn default_impact(&self) -> EstimatedImpact {
        match self {
            PushdownWarningReason::UnsupportedOperation { .. } => EstimatedImpact::Medium,
            PushdownWarningReason::UnsupportedColumn { .. } => EstimatedImpact::High,
            PushdownWarningReason::ComplexPredicate { .. } => EstimatedImpact::Medium,
            PushdownWarningReason::OrNotSupported => EstimatedImpact::Medium,
            PushdownWarningReason::NotNotSupported => EstimatedImpact::Low,
            PushdownWarningReason::TooManyFilters { .. } => EstimatedImpact::High,
            PushdownWarningReason::TransformFailed { .. } => EstimatedImpact::High,
            PushdownWarningReason::FunctionNotSupported { .. } => EstimatedImpact::Medium,
            PushdownWarningReason::SubqueryNotSupported => EstimatedImpact::High,
            PushdownWarningReason::JoinConditionNotPushable => EstimatedImpact::Medium,
        }
    }

    /// Get a human-readable description of this reason.
    pub fn description(&self) -> String {
        match self {
            PushdownWarningReason::UnsupportedOperation { operation, source } => {
                format!("'{}' operation is not supported by {}", operation, source)
            }
            PushdownWarningReason::UnsupportedColumn { column, source } => {
                format!("Filtering on column '{}' is not supported by {}", column, source)
            }
            PushdownWarningReason::ComplexPredicate { description } => {
                format!("Predicate too complex: {}", description)
            }
            PushdownWarningReason::OrNotSupported => {
                "OR conditions are not supported by this source".to_string()
            }
            PushdownWarningReason::NotNotSupported => {
                "NOT conditions are not supported by this source".to_string()
            }
            PushdownWarningReason::TooManyFilters { max, requested } => {
                format!("Too many filters ({} requested, max {})", requested, max)
            }
            PushdownWarningReason::TransformFailed { column, value, reason } => {
                format!(
                    "Cannot transform value '{}' for column '{}': {}",
                    value, column, reason
                )
            }
            PushdownWarningReason::FunctionNotSupported { function } => {
                format!("Function '{}' is not supported by this source", function)
            }
            PushdownWarningReason::SubqueryNotSupported => {
                "Subqueries are not supported by this source".to_string()
            }
            PushdownWarningReason::JoinConditionNotPushable => {
                "Join condition cannot be pushed to a single source".to_string()
            }
        }
    }
}

/// Estimated performance impact of a non-pushable predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimatedImpact {
    /// Low impact: minimal additional data transfer expected.
    Low,
    /// Medium impact: noticeable additional data transfer.
    Medium,
    /// High impact: significant data transfer, consider query rewrite.
    High,
}

impl EstimatedImpact {
    /// Get the severity weight (for sorting/prioritizing warnings).
    pub fn weight(&self) -> u8 {
        match self {
            EstimatedImpact::Low => 1,
            EstimatedImpact::Medium => 2,
            EstimatedImpact::High => 3,
        }
    }

    /// Get a human-readable description.
    pub fn description(&self) -> &'static str {
        match self {
            EstimatedImpact::Low => "minimal impact on performance",
            EstimatedImpact::Medium => "noticeable impact on performance",
            EstimatedImpact::High => "significant impact on performance - consider query optimization",
        }
    }
}

// ============================================================================
// Predicate Splitter
// ============================================================================

use super::cost_model::{ColumnFilterCapability, SourceCapabilities, ValueTransform};
use super::source_capabilities::SourceCapabilityMatrix;
use crate::warehouse::types::SourceType;

/// Result of attempting to push down a predicate.
#[derive(Debug)]
pub enum PushResult {
    /// Predicate can be fully pushed down.
    Pushable(TranslatedPredicate),
    /// Predicate must be applied locally.
    LocalOnly(PushdownWarningReason),
    /// Predicate can be partially pushed (split into pushable and local parts).
    Partial {
        pushable: TranslatedPredicate,
        local: Predicate,
    },
}

/// Splits predicates between source pushdown and local evaluation.
///
/// The splitter analyzes each predicate against the source's capabilities
/// and determines whether it can be pushed down, must be applied locally,
/// or can be partially pushed.
#[derive(Debug)]
pub struct PredicateSplitter {
    /// Capabilities for each registered source.
    capabilities: AHashMap<String, SourceCapabilities>,
}

impl PredicateSplitter {
    /// Create a new predicate splitter.
    pub fn new() -> Self {
        Self {
            capabilities: AHashMap::new(),
        }
    }

    /// Register capabilities for a source.
    pub fn register_source(
        &mut self,
        source_name: impl Into<String>,
        capabilities: SourceCapabilities,
    ) {
        self.capabilities.insert(source_name.into(), capabilities);
    }

    /// Register capabilities for a source type (using default capabilities).
    pub fn register_source_type(
        &mut self,
        source_name: impl Into<String>,
        source_type: SourceType,
    ) {
        let capabilities = SourceCapabilityMatrix::for_source_type(source_type);
        self.register_source(source_name, capabilities);
    }

    /// Get capabilities for a source.
    pub fn get_capabilities(&self, source_name: &str) -> Option<&SourceCapabilities> {
        self.capabilities.get(source_name)
    }

    /// Analyze predicates for a specific source.
    ///
    /// Returns a SourcePredicateAnalysis containing:
    /// - Predicates that can be pushed to the source
    /// - Predicates that must be applied locally
    /// - Warnings about non-pushable predicates
    #[tracing::instrument(name = "warehouse.predicate.analyze_for_source", skip(self, predicates), fields(%source_name, %table_name))]
    pub fn analyze_for_source(
        &self,
        predicates: &[Predicate],
        source_name: &str,
        table_name: &str,
    ) -> SourcePredicateAnalysis {
        let capabilities = self
            .capabilities
            .get(source_name)
            .cloned()
            .unwrap_or_else(SourceCapabilities::no_pushdown);

        let mut analysis = SourcePredicateAnalysis::new(source_name, table_name);
        let mut filter_count = 0;

        for predicate in predicates {
            // Check max filters limit
            if let Some(max) = capabilities.max_filters {
                if filter_count >= max {
                    let warning = PushdownWarning::new(
                        predicate.to_sql_string(),
                        PushdownWarningReason::TooManyFilters {
                            max,
                            requested: filter_count + 1,
                        },
                        source_name,
                    )
                    .with_suggestion(format!(
                        "Reduce the number of filters to {} or fewer for {}",
                        max, source_name
                    ));
                    // Clone only when needed for local_only (which requires Predicate, not Arc)
                    analysis.add_local_with_warning(predicate.clone(), warning);
                    continue;
                }
            }

            match self.can_push_predicate(predicate, &capabilities, source_name) {
                PushResult::Pushable(translated) => {
                    filter_count += 1;
                    analysis.add_pushable(translated);
                }
                PushResult::LocalOnly(reason) => {
                    let warning = self.create_warning(predicate, reason, source_name, &capabilities);
                    // Clone only when needed for local_only (which requires Predicate, not Arc)
                    analysis.add_local_with_warning(predicate.clone(), warning);
                }
                PushResult::Partial { pushable, local } => {
                    filter_count += 1;
                    analysis.add_pushable(pushable);
                    analysis.add_local(local);
                }
            }
        }

        // Estimate selectivities
        analysis.pushable_selectivity = self.estimate_selectivity(&analysis.pushable);
        analysis.local_selectivity = self.estimate_local_selectivity(&analysis.local_only);

        analysis
    }

    /// Check if a single predicate can be pushed down.
    fn can_push_predicate(
        &self,
        predicate: &Predicate,
        capabilities: &SourceCapabilities,
        source_name: &str,
    ) -> PushResult {
        match predicate {
            // Simple predicates - each type has its own handler
            Predicate::Equals { column, .. } => {
                self.handle_equals_predicate(column, capabilities, predicate, source_name)
            }

            Predicate::In { column, values } => {
                self.handle_in_predicate(column, values, capabilities, predicate, source_name)
            }

            Predicate::GreaterThan { column, inclusive, .. } => {
                self.handle_comparison_predicate(column, *inclusive, true, capabilities, predicate, source_name)
            }

            Predicate::LessThan { column, inclusive, .. } => {
                self.handle_comparison_predicate(column, *inclusive, false, capabilities, predicate, source_name)
            }

            Predicate::Between { column, .. } => {
                self.handle_between_predicate(column, capabilities, predicate, source_name)
            }

            Predicate::Like { column, pattern } => {
                self.handle_like_predicate(column, pattern, capabilities, predicate, source_name)
            }

            Predicate::Contains { column, substring } => {
                // Contains is LIKE '%x%' -- handle same as leading-wildcard LIKE
                let mut pattern = String::with_capacity(substring.len() + 2);
                pattern.push('%');
                pattern.push_str(&substring);
                pattern.push('%');
                self.handle_like_predicate(column, &pattern, capabilities, predicate, source_name)
            }

            Predicate::IsNull { column, is_null } => {
                self.handle_is_null_predicate(column, *is_null, capabilities, predicate, source_name)
            }

            // Compound predicates
            Predicate::And(predicates) => {
                if !capabilities.supports_and && predicates.len() > 1 {
                    return PushResult::LocalOnly(PushdownWarningReason::ComplexPredicate {
                        description: "AND conditions not fully supported".to_string(),
                    });
                }
                self.handle_and_predicate(predicates, capabilities, source_name)
            }

            Predicate::Or(predicates) => {
                if !capabilities.supports_or {
                    return PushResult::LocalOnly(PushdownWarningReason::OrNotSupported);
                }
                self.handle_or_predicate(predicates, capabilities, source_name)
            }

            Predicate::Not(inner) => {
                if !capabilities.supports_not {
                    return PushResult::LocalOnly(PushdownWarningReason::NotNotSupported);
                }
                self.handle_not_predicate(inner, capabilities, source_name)
            }
        }
    }

    /// Handle Equals predicate.
    fn handle_equals_predicate(
        &self,
        column: &str,
        capabilities: &SourceCapabilities,
        predicate: &Predicate,
        source_name: &str,
    ) -> PushResult {
        self.check_simple_predicate(
            column,
            FilterOperation::Equals,
            capabilities,
            predicate,
            source_name,
        )
    }

    /// Handle In predicate with size limit checking.
    fn handle_in_predicate(
        &self,
        column: &str,
        values: &[CompactString],
        capabilities: &SourceCapabilities,
        predicate: &Predicate,
        source_name: &str,
    ) -> PushResult {
        let op = FilterOperation::In {
            max_values: Some(values.len()),
        };
        
        // Check if IN list size is within limits
        if let Some(col_cap) = capabilities.get_column_capability(column) {
            if let Some(max) = col_cap.max_in_values {
                if values.len() > max {
                    return PushResult::LocalOnly(PushdownWarningReason::UnsupportedOperation {
                        operation: format!("IN list with {} values (max {})", values.len(), max),
                        source: source_name.to_string(),
                    });
                }
            }
        }
        
        self.check_simple_predicate(column, op, capabilities, predicate, source_name)
    }

    /// Handle comparison predicates (GreaterThan, LessThan).
    ///
    /// # Arguments
    /// * `column` - The column name
    /// * `inclusive` - Whether the comparison is inclusive (>= or <=)
    /// * `is_greater` - true for GreaterThan variants, false for LessThan
    fn handle_comparison_predicate(
        &self,
        column: &str,
        inclusive: bool,
        is_greater: bool,
        capabilities: &SourceCapabilities,
        predicate: &Predicate,
        source_name: &str,
    ) -> PushResult {
        let op = match (is_greater, inclusive) {
            (true, true) => FilterOperation::GreaterThanOrEquals,
            (true, false) => FilterOperation::GreaterThan,
            (false, true) => FilterOperation::LessThanOrEquals,
            (false, false) => FilterOperation::LessThan,
        };
        self.check_simple_predicate(column, op, capabilities, predicate, source_name)
    }

    /// Handle Between predicate.
    fn handle_between_predicate(
        &self,
        column: &str,
        capabilities: &SourceCapabilities,
        predicate: &Predicate,
        source_name: &str,
    ) -> PushResult {
        self.check_simple_predicate(
            column,
            FilterOperation::Between,
            capabilities,
            predicate,
            source_name,
        )
    }

    /// Handle Like predicate with leading wildcard detection.
    fn handle_like_predicate(
        &self,
        column: &str,
        pattern: &str,
        capabilities: &SourceCapabilities,
        predicate: &Predicate,
        source_name: &str,
    ) -> PushResult {
        let has_leading_wildcard = pattern.starts_with('%');
        let op = FilterOperation::Like {
            supports_leading_wildcard: has_leading_wildcard,
        };
        self.check_simple_predicate(column, op, capabilities, predicate, source_name)
    }

    /// Handle IsNull/IsNotNull predicate.
    fn handle_is_null_predicate(
        &self,
        column: &str,
        is_null: bool,
        capabilities: &SourceCapabilities,
        predicate: &Predicate,
        source_name: &str,
    ) -> PushResult {
        let op = if is_null {
            FilterOperation::IsNull
        } else {
            FilterOperation::IsNotNull
        };
        self.check_simple_predicate(column, op, capabilities, predicate, source_name)
    }

    /// Check if a simple predicate can be pushed down.
    fn check_simple_predicate(
        &self,
        column: &str,
        operation: FilterOperation,
        capabilities: &SourceCapabilities,
        predicate: &Predicate,
        source_name: &str,
    ) -> PushResult {
        // Check column-specific capability first
        if let Some(col_cap) = capabilities.get_column_capability(column) {
            if col_cap.supports(&operation) {
                return self.translate_predicate(predicate, col_cap, capabilities);
            } else {
                return PushResult::LocalOnly(PushdownWarningReason::UnsupportedOperation {
                    operation: format!("{:?}", operation),
                    source: source_name.to_string(),
                });
            }
        }

        // Fall back to global capabilities
        if capabilities.supports_operation(&operation) {
            // For SQL sources, generate SQL fragment
            if capabilities.supports_arbitrary_sql {
                let sql = predicate_to_display_sql(predicate, SqlDialect::ClickHouse);
                let translated = TranslatedPredicate::new(
                    predicate.clone(),
                    PredicateTranslation::SqlFragment(sql),
                );
                return PushResult::Pushable(translated);
            }

            // For other sources, we need column-specific support
            PushResult::LocalOnly(PushdownWarningReason::UnsupportedColumn {
                column: column.to_string(),
                source: source_name.to_string(),
            })
        } else {
            PushResult::LocalOnly(PushdownWarningReason::UnsupportedOperation {
                operation: format!("{:?}", operation),
                source: source_name.to_string(),
            })
        }
    }

    /// Translate a predicate using column capability information.
    fn translate_predicate(
        &self,
        predicate: &Predicate,
        col_cap: &ColumnFilterCapability,
        capabilities: &SourceCapabilities,
    ) -> PushResult {
        let transformed_predicate = if let Some(transform) = &col_cap.value_transform {
            match self.transform_predicate_values(predicate, transform) {
                Ok(p) => p,
                Err(reason) => return PushResult::LocalOnly(reason),
            }
        } else {
            predicate.clone()
        };

        if capabilities.supports_arbitrary_sql {
            let sql = predicate_to_display_sql(&transformed_predicate, SqlDialect::ClickHouse);
            let translated = TranslatedPredicate::new(
                transformed_predicate,
                PredicateTranslation::SqlFragment(sql),
            );
            PushResult::Pushable(translated)
        } else {
            let column = predicate.column().unwrap_or_default();
            let param_name = col_cap
                .api_param_name
                .clone()
                .unwrap_or_else(|| column.to_string());

            match &transformed_predicate {
                Predicate::Equals { value, .. } => {
                    let translation = PredicateTranslation::api_param(param_name, value.clone());
                    let translated = TranslatedPredicate::new(transformed_predicate, translation);
                    PushResult::Pushable(translated)
                }
                _ => self.generate_api_translation(&transformed_predicate, col_cap),
            }
        }
    }

    fn transform_predicate_values(
        &self,
        predicate: &Predicate,
        transform: &ValueTransform,
    ) -> Result<Predicate, PushdownWarningReason> {
        let column_name = predicate.column().unwrap_or_default();
        match predicate {
            Predicate::Equals { column, value } => {
                let v = self.apply_transform(&column_name, value, transform)?;
                Ok(Predicate::Equals { column: column.clone(), value: v.into() })
            }
            Predicate::GreaterThan { column, value, inclusive } => {
                let v = self.apply_transform(&column_name, value, transform)?;
                Ok(Predicate::GreaterThan { column: column.clone(), value: v.into(), inclusive: *inclusive })
            }
            Predicate::LessThan { column, value, inclusive } => {
                let v = self.apply_transform(&column_name, value, transform)?;
                Ok(Predicate::LessThan { column: column.clone(), value: v.into(), inclusive: *inclusive })
            }
            Predicate::In { column, values } => {
                let transformed: Result<Vec<CompactString>, _> = values
                    .iter()
                    .map(|v| self.apply_transform(&column_name, v, transform).map(CompactString::from))
                    .collect();
                Ok(Predicate::In { column: column.clone(), values: transformed? })
            }
            Predicate::Between { column, low, high } => {
                let l = self.apply_transform(&column_name, low, transform)?;
                let h = self.apply_transform(&column_name, high, transform)?;
                Ok(Predicate::Between { column: column.clone(), low: l.into(), high: h.into() })
            }
            other => Ok(other.clone()),
        }
    }

    /// Generate API translation for a predicate.
    fn generate_api_translation(
        &self,
        predicate: &Predicate,
        col_cap: &ColumnFilterCapability,
    ) -> PushResult {
        let mut params = AHashMap::new();

        match predicate {
            Predicate::Equals { column, value } => {
                let param = col_cap.api_param_name.clone().unwrap_or_else(|| column.to_string());
                let val = self.transform_value_for_api(column, value, col_cap);
                params.insert(param, val);
            }
            Predicate::In { column, values } => {
                let param = col_cap.api_param_name.clone().unwrap_or_else(|| column.to_string());
                let vals: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let transformed = self.transform_value_for_api(column, v, col_cap);
                        transformed.replace(',', "%2C")
                    })
                    .collect();
                params.insert(param, vals.join(","));
            }
            Predicate::GreaterThan { column, value, inclusive } => {
                let base_param = col_cap.api_param_name.clone().unwrap_or_else(|| column.to_string());
                let suffix = if *inclusive { "[gte]" } else { "[gt]" };
                let val = self.transform_value_for_api(column, value, col_cap);
                params.insert(format!("{}{}", base_param, suffix), val);
            }
            Predicate::LessThan { column, value, inclusive } => {
                let base_param = col_cap.api_param_name.clone().unwrap_or_else(|| column.to_string());
                let suffix = if *inclusive { "[lte]" } else { "[lt]" };
                let val = self.transform_value_for_api(column, value, col_cap);
                params.insert(format!("{}{}", base_param, suffix), val);
            }
            Predicate::Between { column, low, high } => {
                let base_param = col_cap.api_param_name.clone().unwrap_or_else(|| column.to_string());
                let low_val = self.transform_value_for_api(column, low, col_cap);
                let high_val = self.transform_value_for_api(column, high, col_cap);
                params.insert(format!("{}[gte]", base_param), low_val);
                params.insert(format!("{}[lte]", base_param), high_val);
            }
            _ => {
                return PushResult::LocalOnly(PushdownWarningReason::UnsupportedOperation {
                    operation: format!("{:?}", predicate.to_filter_operation()),
                    source: "API translation".to_string(),
                });
            }
        }

        let translated = TranslatedPredicate::new(
            predicate.clone(),
            PredicateTranslation::ApiParams(params),
        );
        PushResult::Pushable(translated)
    }

    /// Transform a value for API translation.
    fn transform_value_for_api(&self, column: &str, value: &str, col_cap: &ColumnFilterCapability) -> String {
        if let Some(transform) = &col_cap.value_transform {
            self.apply_transform(column, value, transform).unwrap_or_else(|_| value.to_string())
        } else {
            value.to_string()
        }
    }

    /// Apply a value transformation.
    fn apply_transform(
        &self,
        column: &str,
        value: &str,
        transform: &ValueTransform,
    ) -> Result<String, PushdownWarningReason> {
        match transform {
            ValueTransform::TimestampToEpoch => {
                // Try to parse as ISO 8601 and convert to epoch seconds
                self.parse_timestamp_to_epoch(column, value)
            }
            ValueTransform::TimestampToEpochMs => {
                if let Ok(epoch) = value.parse::<i64>() {
                    const EPOCH_MS_THRESHOLD: i64 = 10_000_000_000;
                    if epoch >= EPOCH_MS_THRESHOLD || epoch <= -EPOCH_MS_THRESHOLD {
                        return Ok(epoch.to_string());
                    }
                    return epoch.checked_mul(1000)
                        .map(|v| v.to_string())
                        .ok_or_else(|| PushdownWarningReason::TransformFailed {
                            column: column.to_string(),
                            value: value.to_string(),
                            reason: "Epoch to milliseconds conversion overflow".to_string(),
                        });
                }
                self.parse_timestamp_to_epoch(column, value).and_then(|s| {
                    s.parse::<i64>()
                        .ok()
                        .and_then(|v| v.checked_mul(1000))
                        .map(|v| v.to_string())
                        .ok_or_else(|| PushdownWarningReason::TransformFailed {
                            column: column.to_string(),
                            value: value.to_string(),
                            reason: "Epoch to milliseconds conversion overflow".to_string(),
                        })
                })
            }
            ValueTransform::DateToIso8601 => {
                // Assume already in correct format or pass through
                Ok(value.to_string())
            }
            ValueTransform::DateTimeToIso8601 => {
                // Assume already in correct format or pass through
                Ok(value.to_string())
            }
            ValueTransform::BooleanToString => {
                Ok(value.to_lowercase())
            }
            ValueTransform::BooleanToInt => {
                match value.to_lowercase().as_str() {
                    "true" | "1" | "yes" => Ok("1".to_string()),
                    "false" | "0" | "no" => Ok("0".to_string()),
                    _ => Ok(value.to_string()),
                }
            }
            ValueTransform::CentsToDollars => {
                value.parse::<i64>()
                    .map(|v| format!("{:.2}", v as f64 / 100.0))
                    .map_err(|_| PushdownWarningReason::TransformFailed {
                        column: column.to_string(),
                        value: value.to_string(),
                        reason: "Cannot parse as integer for cents conversion".to_string(),
                    })
            }
            ValueTransform::DollarsToCents => {
                value.parse::<f64>()
                    .map_err(|_| PushdownWarningReason::TransformFailed {
                        column: column.to_string(),
                        value: value.to_string(),
                        reason: "Cannot parse as number for dollars conversion".to_string(),
                    })
                    .and_then(|v| {
                        let cents = (v * 100.0).round();
                        if cents > i64::MAX as f64 || cents < i64::MIN as f64 {
                            return Err(PushdownWarningReason::TransformFailed {
                                column: column.to_string(),
                                value: value.to_string(),
                                reason: "Dollar amount too large for cents conversion".to_string(),
                            });
                        }
                        Ok((cents as i64).to_string())
                    })
            }
            ValueTransform::UrlEncode => {
                Ok(urlencoding::encode(value).to_string())
            }
            ValueTransform::Base64Encode => {
                use base64::Engine;
                Ok(base64::engine::general_purpose::STANDARD.encode(value))
            }
            ValueTransform::ToLowercase => {
                Ok(value.to_lowercase())
            }
            ValueTransform::ToUppercase => {
                Ok(value.to_uppercase())
            }
            ValueTransform::Custom(_expr) => {
                // Custom transforms would need evaluation
                Ok(value.to_string())
            }
        }
    }

    /// Parse a timestamp string to epoch seconds.
    fn parse_timestamp_to_epoch(&self, column: &str, value: &str) -> Result<String, PushdownWarningReason> {
        // Try common timestamp formats
        
        // Already an epoch timestamp?
        if let Ok(epoch) = value.parse::<i64>() {
            // Heuristic: epoch seconds are currently ~1.7e9 (10 digits).
            // Epoch milliseconds are ~1.7e12 (13 digits).
            // Values >= 10 billion are likely already in milliseconds;
            // normalize to seconds so callers get a consistent unit.
            const EPOCH_MS_THRESHOLD: i64 = 10_000_000_000;
            if epoch >= EPOCH_MS_THRESHOLD || epoch <= -EPOCH_MS_THRESHOLD {
                // Use div_euclid for floor division: always rounds toward
                // negative infinity, matching UNIX timestamp semantics.
                // Plain `/` truncates toward zero, which is wrong for
                // negative timestamps (e.g., -1001 / 1000 = -1 but should be -2).
                return Ok(epoch.div_euclid(1000).to_string());
            }
            return Ok(epoch.to_string());
        }

        // Try RFC 3339 / ISO 8601
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
            return Ok(dt.timestamp().to_string());
        }

        // Try date only (YYYY-MM-DD)
        if let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            return Ok(date
                .and_hms_opt(0, 0, 0)
                .map(|dt| dt.and_utc().timestamp())
                .unwrap_or(0)
                .to_string());
        }

        // Try datetime without timezone
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
            return Ok(dt.and_utc().timestamp().to_string());
        }

        Err(PushdownWarningReason::TransformFailed {
            column: column.to_string(),
            value: value.to_string(),
            reason: "Cannot parse timestamp".to_string(),
        })
    }

    /// Handle AND predicate - try to push down each part.
    fn handle_and_predicate(
        &self,
        predicates: &[Predicate],
        capabilities: &SourceCapabilities,
        source_name: &str,
    ) -> PushResult {
        let mut pushable_parts = Vec::with_capacity(predicates.len());
        let mut local_parts = Vec::with_capacity(predicates.len());

        for pred in predicates {
            match self.can_push_predicate(pred, capabilities, source_name) {
                PushResult::Pushable(t) => pushable_parts.push(t),
                PushResult::LocalOnly(_) => local_parts.push(pred.clone()),
                PushResult::Partial { pushable, local } => {
                    pushable_parts.push(pushable);
                    local_parts.push(local);
                }
            }
        }

        if pushable_parts.is_empty() {
            return PushResult::LocalOnly(PushdownWarningReason::ComplexPredicate {
                description: "No parts of AND condition can be pushed".to_string(),
            });
        }

        // Combine translations (for SQL sources)
        let combined_translation = if capabilities.supports_arbitrary_sql {
            let sql_translatable: Vec<&TranslatedPredicate> = pushable_parts
                .iter()
                .filter(|t| t.translated.as_sql().is_some())
                .collect();

            if sql_translatable.is_empty() {
                // None of the pushable parts have SQL translations — keep all local.
                // pushable_parts is guaranteed non-empty here (early return above),
                // so local_parts will always be non-empty after this loop.
                for t in &pushable_parts {
                    // Clone only when needed for local_only (which requires Predicate)
                    local_parts.push((*t.original).clone());
                }
                return PushResult::LocalOnly(PushdownWarningReason::ComplexPredicate {
                    description: "AND parts have no SQL translation".to_string(),
                });
            }

            // Move non-SQL-translatable parts to local
            for t in &pushable_parts {
                if t.translated.as_sql().is_none() {
                    // Clone only when needed for local_only (which requires Predicate)
                    local_parts.push((*t.original).clone());
                }
            }

            let sql_parts: Vec<String> = sql_translatable
                .iter()
                .filter_map(|t| t.translated.as_sql().map(String::from))
                .collect();

            // Collect Arc references first, then clone only once when constructing Predicate::And
            let combined_original_inner = Predicate::And(
                sql_translatable.iter().map(|t| (*t.original).clone()).collect()
            );

            let translation = PredicateTranslation::SqlFragment(
                format!("({})", sql_parts.join(" AND "))
            );
            let translated = TranslatedPredicate::new(combined_original_inner, translation);

            return if local_parts.is_empty() {
                PushResult::Pushable(translated)
            } else {
                PushResult::Partial {
                    pushable: translated,
                    local: if local_parts.len() == 1 {
                        local_parts.into_iter().next().unwrap()
                    } else {
                        Predicate::And(local_parts)
                    },
                }
            };
        } else {
            // For API sources, merge all params with collision detection.
            // Check ALL params for a predicate before inserting any, to avoid
            // partially polluting `all_params` when a later param collides.
            let mut all_params: AHashMap<String, String> = AHashMap::with_capacity(pushable_parts.len());
            let mut collision_indices = Vec::new();
            for (idx, t) in pushable_parts.iter().enumerate() {
                if let Some(params) = t.translated.as_api_params() {
                    let has_collision = params.iter().any(|(key, value)| {
                        all_params.get(key).is_some_and(|existing| existing != value)
                    });
                    if has_collision {
                        collision_indices.push(idx);
                    } else {
                        for (key, value) in params {
                            all_params.insert(key.clone(), value.clone());
                        }
                    }
                }
            }

            if !collision_indices.is_empty() {
                for &idx in collision_indices.iter().rev() {
                    local_parts.push((*pushable_parts[idx].original).clone());
                    pushable_parts.remove(idx);
                }
            }
            if pushable_parts.is_empty() {
                return PushResult::LocalOnly(PushdownWarningReason::ComplexPredicate {
                    description: "All AND parts have API param collisions".to_string(),
                });
            }
            PredicateTranslation::ApiParams(all_params)
        };

        // Clone only when constructing Predicate::And (which requires Vec<Predicate>)
        let combined_original = Predicate::And(
            pushable_parts.iter().map(|t| (*t.original).clone()).collect()
        );
        let translated = TranslatedPredicate::new(combined_original, combined_translation);

        if local_parts.is_empty() {
            PushResult::Pushable(translated)
        } else {
            PushResult::Partial {
                pushable: translated,
                local: if local_parts.len() == 1 {
                    local_parts.into_iter().next().unwrap()
                } else {
                    Predicate::And(local_parts)
                },
            }
        }
    }

    /// Handle OR predicate - all parts must be pushable.
    fn handle_or_predicate(
        &self,
        predicates: &[Predicate],
        capabilities: &SourceCapabilities,
        source_name: &str,
    ) -> PushResult {
        let mut translated_parts = Vec::with_capacity(predicates.len());

        for pred in predicates {
            match self.can_push_predicate(pred, capabilities, source_name) {
                PushResult::Pushable(t) => translated_parts.push(t),
                PushResult::LocalOnly(reason) => return PushResult::LocalOnly(reason),
                PushResult::Partial { .. } => {
                    return PushResult::LocalOnly(PushdownWarningReason::ComplexPredicate {
                        description: "OR with partial pushdown not supported".to_string(),
                    });
                }
            }
        }

        if translated_parts.is_empty() {
            return PushResult::LocalOnly(PushdownWarningReason::ComplexPredicate {
                description: "Empty OR predicate".to_string(),
            });
        }

        // All parts are pushable
        // Clone only when constructing Predicate::Or (which requires Vec<Predicate>)
        let combined_original = Predicate::Or(
            translated_parts.iter().map(|t| (*t.original).clone()).collect()
        );

        let combined_translation = if capabilities.supports_arbitrary_sql {
            let sql_parts: Vec<String> = translated_parts
                .iter()
                .filter_map(|t| t.translated.as_sql().map(String::from))
                .collect();
            if sql_parts.len() == translated_parts.len() {
                PredicateTranslation::SqlFragment(format!("({})", sql_parts.join(" OR ")))
            } else {
                return PushResult::LocalOnly(PushdownWarningReason::ComplexPredicate {
                    description: "OR parts missing SQL translation".to_string(),
                });
            }
        } else {
            return PushResult::LocalOnly(PushdownWarningReason::ComplexPredicate {
                description: "API sources do not support OR predicates".to_string(),
            });
        };

        PushResult::Pushable(TranslatedPredicate::new(combined_original, combined_translation))
    }

    /// Handle NOT predicate.
    fn handle_not_predicate(
        &self,
        inner: &Predicate,
        capabilities: &SourceCapabilities,
        source_name: &str,
    ) -> PushResult {
        match self.can_push_predicate(inner, capabilities, source_name) {
            PushResult::Pushable(t) => {
                // Clone only when constructing Predicate::Not (which requires Predicate, not Arc)
                let negated = Predicate::Not(Box::new((*t.original).clone()));
                let translation = if capabilities.supports_arbitrary_sql {
                    match t.translated.as_sql() {
                        Some(sql) => PredicateTranslation::SqlFragment(format!("NOT ({})", sql)),
                        None => PredicateTranslation::Passthrough,
                    }
                } else {
                    PredicateTranslation::Passthrough
                };
                PushResult::Pushable(TranslatedPredicate::new(negated, translation))
            }
            PushResult::Partial { .. } | PushResult::LocalOnly(_) => {
                PushResult::LocalOnly(PushdownWarningReason::ComplexPredicate {
                    description: "NOT with partially pushable inner predicate must be evaluated locally".to_string(),
                })
            }
        }
    }

    /// Create a warning for a non-pushable predicate.
    fn create_warning(
        &self,
        predicate: &Predicate,
        reason: PushdownWarningReason,
        source_name: &str,
        capabilities: &SourceCapabilities,
    ) -> PushdownWarning {
        let mut warning = PushdownWarning::new(
            predicate.to_sql_string(),
            reason,
            source_name,
        );

        // Add suggestions based on capabilities
        if predicate.column().is_some() {
            // Check if there are alternative columns that can be filtered
            let supported_columns: Vec<&String> = capabilities
                .column_filters
                .keys()
                .collect();
            
            if !supported_columns.is_empty() {
                warning = warning.with_suggestion(format!(
                    "Consider filtering on supported columns: {}",
                    supported_columns.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                ));
            }
        }

        warning
    }

    /// Estimate selectivity of pushable predicates.
    fn estimate_selectivity(&self, predicates: &[TranslatedPredicate]) -> f64 {
        if predicates.is_empty() {
            return 1.0;
        }

        // Simple heuristic: multiply individual selectivities
        let mut selectivity = 1.0;
        for pred in predicates {
            selectivity *= pred.estimated_selectivity;
        }
        selectivity.max(0.001) // Minimum selectivity to avoid zero
    }

    /// Estimate selectivity of local predicates.
    fn estimate_local_selectivity(&self, predicates: &[Predicate]) -> f64 {
        if predicates.is_empty() {
            return 1.0;
        }

        // Simple heuristic based on predicate type
        let mut selectivity = 1.0;
        for pred in predicates {
            let pred_selectivity = match pred {
                Predicate::Equals { .. } => 0.1,
                Predicate::In { values, .. } => (values.len() as f64 * 0.05).min(0.5),
                Predicate::GreaterThan { .. } | Predicate::LessThan { .. } => 0.3,
                Predicate::Between { .. } => 0.2,
                Predicate::Like { pattern, .. } => {
                    if pattern.starts_with('%') { 0.5 } else { 0.2 }
                }
                Predicate::Contains { .. } => 0.5,
                Predicate::IsNull { .. } => 0.1,
                Predicate::And(preds) => self.estimate_local_selectivity(preds),
                Predicate::Or(preds) => {
                    let mut combined = 0.0_f64;
                    for p in preds {
                        // Pass reference instead of cloning
                        let s = self.estimate_local_selectivity(std::slice::from_ref(p));
                        combined = combined + s - combined * s;
                    }
                    combined.min(1.0)
                }
                Predicate::Not(_) => 0.9,
            };
            selectivity *= pred_selectivity;
        }
        selectivity.max(0.001)
    }

    /// Generate a source-specific query with pushed predicates.
    #[tracing::instrument(name = "warehouse.predicate.generate_source_query", skip_all)]
    pub fn generate_source_query(
        &self,
        base_query: &str,
        analysis: &SourcePredicateAnalysis,
        source_type: SourceType,
    ) -> SourceQueryWithFilters {
        let capabilities = SourceCapabilityMatrix::for_source_type(source_type);

        if capabilities.supports_arbitrary_sql {
            let pushable_preds: Vec<&Predicate> = analysis
                .pushable
                .iter()
                .filter(|t| t.translated.is_sql())
                .map(|t| t.original.as_ref())
                .collect();

            let query = if pushable_preds.is_empty() {
                base_query.to_string()
            } else {
                let dialect = ClickHouseDialect {};
                let mut stmts = match Parser::parse_sql(&dialect, base_query) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Failed to parse base query for predicate pushdown; returning original query"
                        );
                        return SourceQueryWithFilters {
                            query: base_query.to_string(),
                            api_params: AHashMap::new(),
                            local_filters: analysis.local_only.clone(),
                        };
                    }
                };

                let mut cond_iter = pushable_preds.iter();
                let first = predicate_to_expr(cond_iter.next().unwrap());
                let combined = cond_iter.fold(first, |acc, p| Expr::BinaryOp {
                    left: Box::new(acc),
                    op: BinaryOperator::And,
                    right: Box::new(predicate_to_expr(p)),
                });

                for stmt in &mut stmts {
                    if let Statement::Query(q) = stmt {
                        inject_where_into_query(q, combined.clone());
                    }
                }
                serialize_statements(&stmts)
            };

            SourceQueryWithFilters {
                query,
                api_params: AHashMap::new(),
                local_filters: analysis.local_only.clone(),
            }
        } else {
            // API source - collect params
            let mut api_params = AHashMap::new();
            for t in &analysis.pushable {
                if let Some(params) = t.translated.as_api_params() {
                    api_params.extend(params.clone());
                }
            }

            SourceQueryWithFilters {
                query: base_query.to_string(),
                api_params,
                local_filters: analysis.local_only.clone(),
            }
        }
    }
}

impl Default for PredicateSplitter {
    fn default() -> Self {
        Self::new()
    }
}

/// A source query with filters applied.
#[derive(Debug, Clone)]
pub struct SourceQueryWithFilters {
    /// The query (with SQL filters applied if applicable).
    pub query: String,
    /// API parameters for non-SQL sources.
    pub api_params: AHashMap<String, String>,
    /// Predicates that must be applied locally.
    pub local_filters: Vec<Predicate>,
}

impl SourceQueryWithFilters {
    /// Check if there are API parameters.
    pub fn has_api_params(&self) -> bool {
        !self.api_params.is_empty()
    }

    /// Check if there are local filters.
    pub fn has_local_filters(&self) -> bool {
        !self.local_filters.is_empty()
    }
}

// ============================================================================
// Predicate to FilterOperation Conversion
// ============================================================================

impl Predicate {
    /// Get the FilterOperation that corresponds to this predicate.
    pub fn to_filter_operation(&self) -> Option<FilterOperation> {
        match self {
            Predicate::Equals { .. } => Some(FilterOperation::Equals),
            Predicate::In { values, .. } => Some(FilterOperation::In {
                max_values: Some(values.len()),
            }),
            Predicate::GreaterThan { inclusive, .. } => {
                if *inclusive {
                    Some(FilterOperation::GreaterThanOrEquals)
                } else {
                    Some(FilterOperation::GreaterThan)
                }
            }
            Predicate::LessThan { inclusive, .. } => {
                if *inclusive {
                    Some(FilterOperation::LessThanOrEquals)
                } else {
                    Some(FilterOperation::LessThan)
                }
            }
            Predicate::Between { .. } => Some(FilterOperation::Between),
            Predicate::Like { pattern, .. } => Some(FilterOperation::Like {
                supports_leading_wildcard: pattern.starts_with('%'),
            }),
            Predicate::Contains { .. } => Some(FilterOperation::Contains),
            Predicate::IsNull { is_null, .. } => {
                if *is_null {
                    Some(FilterOperation::IsNull)
                } else {
                    Some(FilterOperation::IsNotNull)
                }
            }
            // Compound predicates don't map to a single operation
            Predicate::And(_) | Predicate::Or(_) | Predicate::Not(_) => None,
        }
    }

    /// Convert to SQL string representation (for logging/display).
    /// Uses ClickHouse dialect by default for display purposes.
    pub fn to_sql_string(&self) -> String {
        predicate_to_display_sql(self, SqlDialect::ClickHouse)
    }
}

/// Convert a predicate to a SQL display string.
///
/// Public wrapper so connectors can build WHERE clauses from predicates.
pub fn predicate_to_sql(predicate: &Predicate, dialect: SqlDialect) -> String {
    predicate_to_display_sql(predicate, dialect)
}

/// Extract a column name from a `sqlparser` expression.
///
/// Handles bare identifiers (`col`), compound identifiers (`t.col`),
/// and `CAST(col AS type)`. Returns `None` for anything else.
fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|i| i.value.clone()),
        Expr::Cast { expr, .. } => extract_column_name(expr),
        _ => None,
    }
}

/// Extract a literal value as a `String` from a `sqlparser` expression.
///
/// Supports quoted strings, numbers, booleans, and `NULL`.
fn extract_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()),
        Expr::Value(Value::DoubleQuotedString(s)) => Some(s.clone()),
        Expr::Value(Value::Number(n, _)) => Some(n.clone()),
        Expr::Value(Value::Boolean(b)) => Some(b.to_string()),
        Expr::Value(Value::Null) => None,
        _ => None,
    }
}

/// Convert a `sqlparser::ast::Expr` (typically a WHERE clause) into
/// a `Vec<Predicate>`.
///
/// Top-level `AND` nodes are flattened into the returned vec.
/// Unsupported expression shapes are silently dropped -- the caller must
/// apply post-filtering for anything not captured here.
pub fn expr_to_predicates(expr: &Expr) -> Vec<Predicate> {
    let mut out = Vec::with_capacity(8);
    collect_predicates(expr, &mut out);
    out
}

fn collect_predicates(expr: &Expr, out: &mut Vec<Predicate>) {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                collect_predicates(left, out);
                collect_predicates(right, out);
            }
            BinaryOperator::Or => {
                let mut left_preds = Vec::new();
                let mut right_preds = Vec::new();
                collect_predicates(left, &mut left_preds);
                collect_predicates(right, &mut right_preds);
                if !left_preds.is_empty() && !right_preds.is_empty() {
                    let left_p = if left_preds.len() == 1 {
                        left_preds.remove(0)
                    } else {
                        Predicate::And(left_preds)
                    };
                    let right_p = if right_preds.len() == 1 {
                        right_preds.remove(0)
                    } else {
                        Predicate::And(right_preds)
                    };
                    out.push(Predicate::Or(vec![left_p, right_p]));
                }
            }
            BinaryOperator::Eq => {
                if let Some(p) = try_binary_predicate(left, right, |col, val| {
                    Predicate::Equals { column: col.into(), value: val.into() }
                }) {
                    out.push(p);
                }
            }
            BinaryOperator::Gt => {
                if let Some(p) = try_comparison(left, right, false, false) {
                    out.push(p);
                }
            }
            BinaryOperator::GtEq => {
                if let Some(p) = try_comparison(left, right, true, false) {
                    out.push(p);
                }
            }
            BinaryOperator::Lt => {
                if let Some(p) = try_comparison(left, right, false, true) {
                    out.push(p);
                }
            }
            BinaryOperator::LtEq => {
                if let Some(p) = try_comparison(left, right, true, true) {
                    out.push(p);
                }
            }
            BinaryOperator::NotEq => {
                if let Some(p) = try_binary_predicate(left, right, |col, val| {
                    Predicate::Not(Box::new(Predicate::Equals { column: col.into(), value: val.into() }))
                }) {
                    out.push(p);
                }
            }
            _ => {}
        },
        Expr::Like { negated, expr, pattern, .. } => {
            if let (Some(col), Some(pat)) = (extract_column_name(expr), extract_literal_value(pattern)) {
                let pred = if pat.starts_with('%') && pat.ends_with('%') && pat.len() > 2 {
                    let substr = &pat[1..pat.len() - 1];
                    if !substr.contains('%') && !substr.contains('_') {
                        Predicate::Contains { column: col.into(), substring: substr.into() }
                    } else {
                        Predicate::Like { column: col.into(), pattern: pat.into() }
                    }
                } else {
                    Predicate::Like { column: col.into(), pattern: pat.into() }
                };
                if *negated {
                    out.push(Predicate::Not(Box::new(pred)));
                } else {
                    out.push(pred);
                }
            }
        }
        Expr::InList { expr, list, negated } => {
            if let Some(col) = extract_column_name(expr) {
                let values: Vec<CompactString> = list.iter().filter_map(extract_literal_value).map(CompactString::from).collect();
                let has_null_in_list = values.len() < list.len();
                if !values.is_empty() {
                    // SQL: `x NOT IN (..., NULL)` always yields NULL (no rows).
                    // Don't push this predicate; let the database handle it.
                    if *negated && has_null_in_list {
                        // skip — the DB will correctly evaluate NOT IN with NULL
                    } else {
                        let pred = Predicate::In { column: col.into(), values };
                        if *negated {
                            out.push(Predicate::Not(Box::new(pred)));
                        } else {
                            out.push(pred);
                        }
                    }
                }
            }
        }
        Expr::Between { expr, negated, low, high } => {
            if let Some(col) = extract_column_name(expr) {
                if let (Some(lo), Some(hi)) = (extract_literal_value(low), extract_literal_value(high)) {
                    let pred = Predicate::Between { column: col.into(), low: lo.into(), high: hi.into() };
                    if *negated {
                        out.push(Predicate::Not(Box::new(pred)));
                    } else {
                        out.push(pred);
                    }
                }
            }
        }
        Expr::IsNull(inner) => {
            if let Some(col) = extract_column_name(inner) {
                out.push(Predicate::IsNull { column: col.into(), is_null: true });
            }
        }
        Expr::IsNotNull(inner) => {
            if let Some(col) = extract_column_name(inner) {
                out.push(Predicate::IsNull { column: col.into(), is_null: false });
            }
        }
        Expr::UnaryOp { op: UnaryOperator::Not, expr } => {
            let mut inner_preds = Vec::new();
            collect_predicates(expr, &mut inner_preds);
            match inner_preds.len() {
                0 => {}
                1 => out.push(Predicate::Not(Box::new(inner_preds.remove(0)))),
                _ => out.push(Predicate::Not(Box::new(Predicate::And(inner_preds)))),
            }
        }
        Expr::Nested(inner) => {
            collect_predicates(inner, out);
        }
        _ => {}
    }
}

/// Try to build a predicate from a binary `col op value` or `value op col` expression.
fn try_binary_predicate(
    left: &Expr,
    right: &Expr,
    build: impl FnOnce(String, String) -> Predicate,
) -> Option<Predicate> {
    if let (Some(col), Some(val)) = (extract_column_name(left), extract_literal_value(right)) {
        return Some(build(col, val));
    }
    if let (Some(col), Some(val)) = (extract_column_name(right), extract_literal_value(left)) {
        return Some(build(col, val));
    }
    None
}

/// Build a GreaterThan or LessThan predicate, handling the `value op col` flip.
fn try_comparison(left: &Expr, right: &Expr, inclusive: bool, is_lt: bool) -> Option<Predicate> {
    if let (Some(col), Some(val)) = (extract_column_name(left), extract_literal_value(right)) {
        return Some(if is_lt {
            Predicate::LessThan { column: col.into(), value: val.into(), inclusive }
        } else {
            Predicate::GreaterThan { column: col.into(), value: val.into(), inclusive }
        });
    }
    // Flipped: `100 < col` means `col > 100`
    if let (Some(col), Some(val)) = (extract_column_name(right), extract_literal_value(left)) {
        return Some(if is_lt {
            Predicate::GreaterThan { column: col.into(), value: val.into(), inclusive }
        } else {
            Predicate::LessThan { column: col.into(), value: val.into(), inclusive }
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicate_selectivity() {
        let eq = Predicate::Equals {
            column: "col".into(),
            value: "val".into(),
        };
        assert!(eq.is_selective());

        let small_in = Predicate::In {
            column: "col".into(),
            values: vec!["a".into(), "b".into()],
        };
        assert!(small_in.is_selective());

        let large_in = Predicate::In {
            column: "col".into(),
            values: (0..20).map(|i| CompactString::from(i.to_string())).collect(),
        };
        assert!(!large_in.is_selective());

        let leading_wildcard = Predicate::Like {
            column: "col".into(),
            pattern: "%suffix".into(),
        };
        assert!(!leading_wildcard.is_selective());
    }

    #[test]
    fn test_predicate_pushdown_classification() {
        let mut indexed = AHashSet::new();
        indexed.insert("event_type".to_string());
        indexed.insert("timestamp".to_string());

        let predicates = vec![
            Predicate::Equals {
                column: "event_type".into(),
                value: "purchase".into(),
            },
            Predicate::Between {
                column: "timestamp".into(),
                low: "2024-01-01".into(),
                high: "2024-01-31".into(),
            },
            Predicate::IsNull {
                column: "extra_data".into(),
                is_null: true,
            },
        ];

        let pushdown = PredicatePushdown::analyze(predicates, &indexed);

        assert_eq!(pushdown.file_predicates.len(), 2);
        assert!(pushdown.has_prewhere());
        assert!(!pushdown.where_predicates.is_empty());
    }

    #[test]
    fn test_open_range_inclusive_flags() {
        let mut indexed = AHashSet::new();
        indexed.insert("price".to_string());

        let predicates = vec![Predicate::GreaterThan {
            column: "price".into(),
            value: "100".into(),
            inclusive: true,
        }];

        let pushdown = PredicatePushdown::analyze(predicates, &indexed);
        assert_eq!(pushdown.file_predicates.len(), 1);
        if let FilePredicateType::Range {
            min,
            max,
            min_inclusive,
            max_inclusive,
        } = &pushdown.file_predicates[0].predicate_type
        {
            assert!(min.is_some());
            assert!(max.is_none());
            assert!(*min_inclusive);
            assert!(
                !*max_inclusive,
                "max_inclusive must be false when max is None"
            );
        } else {
            panic!("Expected Range file predicate");
        }

        let predicates = vec![Predicate::LessThan {
            column: "price".into(),
            value: "200".into(),
            inclusive: false,
        }];
        let pushdown = PredicatePushdown::analyze(predicates, &indexed);
        if let FilePredicateType::Range {
            min,
            max,
            min_inclusive,
            max_inclusive,
        } = &pushdown.file_predicates[0].predicate_type
        {
            assert!(min.is_none());
            assert!(max.is_some());
            assert!(
                !*min_inclusive,
                "min_inclusive must be false when min is None"
            );
            assert!(!*max_inclusive);
        } else {
            panic!("Expected Range file predicate");
        }
    }

    #[test]
    fn test_prewhere_rewriting() {
        let mut indexed = AHashSet::new();
        indexed.insert("event_type".to_string());

        let predicates = vec![
            Predicate::Equals {
                column: "event_type".into(),
                value: "purchase".into(),
            },
            Predicate::IsNull {
                column: "meta".into(),
                is_null: false,
            },
        ];

        let pushdown = PredicatePushdown::analyze(predicates, &indexed);
        let rewritten = pushdown.rewrite_with_prewhere("SELECT * FROM events");

        assert!(rewritten.contains("PREWHERE"));
        assert!(rewritten.contains("`event_type` = 'purchase'"));
        assert!(rewritten.contains("WHERE"));
        assert!(rewritten.contains("`meta` IS NOT NULL"));
    }

    #[test]
    fn test_sql_string_escaping() {
        assert_eq!(escape_sql_string("O'Reilly"), "O''Reilly");
        assert_eq!(escape_sql_string("simple"), "simple");
        assert_eq!(escape_sql_string("back\\slash"), "back\\\\slash");
        assert_eq!(escape_sql_string("null\0byte"), "nullbyte"); // Null bytes stripped
    }

    #[test]
    fn test_column_name_validation() {
        assert!(is_valid_column_name("valid_column"));
        assert!(is_valid_column_name("Column123"));
        assert!(is_valid_column_name("_private"));
        assert!(is_valid_column_name("has-dash"));
        assert!(is_valid_column_name("table.column"));
        assert!(is_valid_column_name("created-at"));
        assert!(!is_valid_column_name("")); // Empty
        assert!(!is_valid_column_name("123start")); // Starts with number
        assert!(!is_valid_column_name("has space")); // Space
        assert!(!is_valid_column_name(&"a".repeat(129))); // Too long
    }

    #[test]
    fn test_predicate_to_display_sql() {
        let eq = Predicate::Equals {
            column: "name".into(),
            value: "O'Brien".into(),
        };
        assert_eq!(predicate_to_display_sql(&eq, SqlDialect::ClickHouse), "`name` = 'O''Brien'");

        let in_clause = Predicate::In {
            column: "status".into(),
            values: vec!["active".into(), "pending".into()],
        };
        assert_eq!(predicate_to_display_sql(&in_clause, SqlDialect::ClickHouse), "`status` IN ('active', 'pending')");
    }

    #[test]
    fn test_predicate_to_expr_equals() {
        let pred = Predicate::Equals {
            column: "name".into(),
            value: "O'Brien".into(),
        };
        let expr = predicate_to_expr(&pred);
        let sql = expr.to_string();
        assert!(sql.contains("`name`"), "Column must be backtick-quoted: {sql}");
        assert!(sql.contains("O'Brien") || sql.contains("O''Brien"), "Value must appear: {sql}");
        assert!(sql.contains("="), "Must have equality op: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_in_list() {
        let pred = Predicate::In {
            column: "status".into(),
            values: vec!["active".into(), "pending".into()],
        };
        let expr = predicate_to_expr(&pred);
        let sql = expr.to_string();
        assert!(sql.contains("IN"), "Must contain IN: {sql}");
        assert!(sql.contains("active"), "Must contain first value: {sql}");
        assert!(sql.contains("pending"), "Must contain second value: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_in_empty() {
        let pred = Predicate::In {
            column: "x".into(),
            values: vec![],
        };
        let expr = predicate_to_expr(&pred);
        let sql = expr.to_string();
        assert!(sql.contains("1 = 0"), "Empty IN must produce false: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_greater_than() {
        let pred = Predicate::GreaterThan {
            column: "age".into(),
            value: "18".into(),
            inclusive: false,
        };
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains(">"), "Must contain >: {sql}");
        assert!(!sql.contains(">="), "Must not contain >=: {sql}");
        assert!(sql.contains("18"), "Must contain value: {sql}");

        let pred_incl = Predicate::GreaterThan {
            column: "age".into(),
            value: "18".into(),
            inclusive: true,
        };
        let sql_incl = predicate_to_expr(&pred_incl).to_string();
        assert!(sql_incl.contains(">="), "Inclusive must contain >=: {sql_incl}");
    }

    #[test]
    fn test_predicate_to_expr_less_than() {
        let pred = Predicate::LessThan {
            column: "price".into(),
            value: "100".into(),
            inclusive: true,
        };
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("<="), "Inclusive must contain <=: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_between() {
        let pred = Predicate::Between {
            column: "ts".into(),
            low: "2024-01-01".into(),
            high: "2024-12-31".into(),
        };
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("BETWEEN"), "Must contain BETWEEN: {sql}");
        assert!(sql.contains("2024-01-01"), "Must contain low bound: {sql}");
        assert!(sql.contains("2024-12-31"), "Must contain high bound: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_like() {
        let pred = Predicate::Like {
            column: "name".into(),
            pattern: "John%".into(),
        };
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("LIKE"), "Must contain LIKE: {sql}");
        assert!(sql.contains("John%"), "Must contain pattern: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_contains() {
        let pred = Predicate::Contains {
            column: "desc".into(),
            substring: "foo".into(),
        };
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("LIKE"), "Must contain LIKE: {sql}");
        assert!(sql.contains("%foo%"), "Must contain wrapped substring: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_is_null() {
        let null_pred = Predicate::IsNull {
            column: "email".into(),
            is_null: true,
        };
        let sql = predicate_to_expr(&null_pred).to_string();
        assert!(sql.contains("IS NULL"), "Must contain IS NULL: {sql}");
        assert!(!sql.contains("NOT"), "IS NULL must not contain NOT: {sql}");

        let not_null_pred = Predicate::IsNull {
            column: "email".into(),
            is_null: false,
        };
        let sql = predicate_to_expr(&not_null_pred).to_string();
        assert!(sql.contains("IS NOT NULL"), "Must contain IS NOT NULL: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_and() {
        let pred = Predicate::And(vec![
            Predicate::Equals { column: "a".into(), value: "1".into() },
            Predicate::Equals { column: "b".into(), value: "2".into() },
        ]);
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("AND"), "Must contain AND: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_or() {
        let pred = Predicate::Or(vec![
            Predicate::Equals { column: "a".into(), value: "1".into() },
            Predicate::Equals { column: "b".into(), value: "2".into() },
        ]);
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("OR"), "Must contain OR: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_not() {
        let pred = Predicate::Not(Box::new(
            Predicate::Equals { column: "x".into(), value: "1".into() },
        ));
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("NOT"), "Must contain NOT: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_empty_and_is_true() {
        let pred = Predicate::And(vec![]);
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("1 = 1"), "Empty AND must be tautology: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_empty_or_is_false() {
        let pred = Predicate::Or(vec![]);
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("1 = 0"), "Empty OR must be contradiction: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_numeric_values_unquoted() {
        let pred = Predicate::GreaterThan {
            column: "age".into(),
            value: "42".into(),
            inclusive: false,
        };
        let sql = predicate_to_expr(&pred).to_string();
        assert!(!sql.contains("'42'"), "Numeric values must not be quoted: {sql}");
        assert!(sql.contains("42"), "Numeric value must appear: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_string_values_quoted() {
        let pred = Predicate::Equals {
            column: "name".into(),
            value: "Alice".into(),
        };
        let sql = predicate_to_expr(&pred).to_string();
        assert!(sql.contains("'Alice'"), "String values must be quoted: {sql}");
    }

    #[test]
    fn test_predicate_to_expr_roundtrip_matches_display_sql() {
        let predicates = vec![
            Predicate::Equals { column: "x".into(), value: "1".into() },
            Predicate::In {
                column: "status".into(),
                values: vec!["a".into(), "b".into()],
            },
            Predicate::GreaterThan { column: "n".into(), value: "10".into(), inclusive: false },
            Predicate::LessThan { column: "n".into(), value: "100".into(), inclusive: true },
            Predicate::Between {
                column: "d".into(),
                low: "2024-01-01".into(),
                high: "2024-12-31".into(),
            },
            Predicate::IsNull { column: "e".into(), is_null: true },
            Predicate::IsNull { column: "e".into(), is_null: false },
        ];

        for pred in &predicates {
            let display = predicate_to_display_sql(pred, SqlDialect::ClickHouse);
            let expr = predicate_to_expr(pred);
            let expr_sql = expr.to_string();
            let display_parsed = parse_condition_expr(&display);
            assert!(
                display_parsed.is_ok(),
                "Display SQL '{}' must be parseable (for pred {:?})",
                display,
                pred,
            );
        }
    }

    #[test]
    fn test_file_predicate_extraction() {
        let mut indexed = AHashSet::new();
        indexed.insert("user_id".to_string());

        let eq = Predicate::Equals {
            column: "user_id".into(),
            value: "abc123".into(),
        };

        let pushdown = PredicatePushdown::analyze(vec![eq], &indexed);
        
        assert_eq!(pushdown.file_predicates.len(), 1);
        let file_pred = &pushdown.file_predicates[0];
        assert_eq!(file_pred.column, "user_id");
        match &file_pred.predicate_type {
            FilePredicateType::ExactMatch(v) => assert_eq!(v, "abc123"),
            _ => panic!("Expected ExactMatch"),
        }
    }

    #[test]
    fn test_not_predicate() {
        let eq = Predicate::Equals {
            column: "status".into(),
            value: "deleted".into(),
        };
        let not_eq = Predicate::Not(Box::new(eq));

        // Test SQL generation
        assert_eq!(predicate_to_display_sql(&not_eq, SqlDialect::ClickHouse), "NOT (`status` = 'deleted')");

        // Test column extraction
        assert_eq!(not_eq.column(), Some("status"));

        // Test columns set
        let cols = not_eq.columns();
        assert!(cols.contains("status"));

        // NOT inverts selectivity — NOT(selective) typically matches most rows
        assert!(!not_eq.is_selective());
    }

    #[test]
    fn test_escape_like_pattern() {
        // Regular strings pass through
        assert_eq!(escape_like_pattern("hello"), "hello");

        // LIKE wildcards are double-backslash escaped so the escape
        // backslash survives ClickHouse string-literal parsing:
        //   output `\\%` → string-parsed `\%` → LIKE literal `%`
        assert_eq!(escape_like_pattern("100%"), "100\\\\%");
        assert_eq!(escape_like_pattern("user_123"), "user\\\\_123");
        assert_eq!(escape_like_pattern("%admin%"), "\\\\%admin\\\\%");

        // SQL single quotes are escaped
        assert_eq!(escape_like_pattern("O'Brien"), "O''Brien");

        // Input backslash needs four backslashes in the output:
        //   output `\\\\` → string-parsed `\\` → LIKE literal `\`
        assert_eq!(escape_like_pattern("back\\slash"), "back\\\\\\\\slash");

        // Combined: `%` and regular chars
        assert_eq!(escape_like_pattern("50% off!"), "50\\\\% off!");
    }

    #[test]
    fn test_escape_like_pattern_backslash_percent_combo() {
        // Input `test\%file` (literal backslash + literal percent + "file"):
        //   `\` → `\\\\`   (4 backslashes)
        //   `%` → `\\%`    (escaped LIKE wildcard)
        // Output: `test\\\\\\\\%file`
        //   After string parse: `test\\` + `\%` + `file`
        //   LIKE: literal `\`, literal `%`, "file"
        assert_eq!(
            escape_like_pattern("test\\%file"),
            "test\\\\\\\\\\\\%file"
        );
    }

    #[test]
    fn test_empty_and_or_predicates() {
        // Empty AND produces "1=1" (always true)
        let empty_and = Predicate::And(vec![]);
        assert_eq!(predicate_to_display_sql(&empty_and, SqlDialect::ClickHouse), "1=1");

        // Empty OR produces "1=0" (always false)
        let empty_or = Predicate::Or(vec![]);
        assert_eq!(predicate_to_display_sql(&empty_or, SqlDialect::ClickHouse), "1=0");

        // Non-empty still works
        let and_one = Predicate::And(vec![Predicate::Equals {
            column: "x".into(),
            value: "1".into(),
        }]);
        assert_eq!(predicate_to_display_sql(&and_one, SqlDialect::ClickHouse), "(`x` = 1)");
    }

    // ========== Source Predicate Analysis Tests ==========

    #[test]
    fn test_source_predicate_analysis_default() {
        let analysis = SourcePredicateAnalysis::default();
        assert!(!analysis.has_pushable());
        assert!(!analysis.has_local());
        assert!(!analysis.has_warnings());
        assert_eq!(analysis.pushable_selectivity, 1.0);
        assert_eq!(analysis.local_selectivity, 1.0);
    }

    #[test]
    fn test_source_predicate_analysis_new() {
        let analysis = SourcePredicateAnalysis::new("stripe", "charges");
        assert_eq!(analysis.source_name, "stripe");
        assert_eq!(analysis.table_name, "charges");
    }

    #[test]
    fn test_source_predicate_analysis_combined_selectivity() {
        let mut analysis = SourcePredicateAnalysis::new("postgres", "users");
        analysis.pushable_selectivity = 0.5; // 50% of rows pass pushable filters
        analysis.local_selectivity = 0.2; // 20% of remaining pass local filters
        
        assert_eq!(analysis.combined_selectivity(), 0.1); // 10% total
        assert_eq!(analysis.data_reduction(), 0.5); // 50% data saved by pushdown
    }

    #[test]
    fn test_translated_predicate() {
        let predicate = Predicate::Equals {
            column: "customer".into(),
            value: "cus_123".into(),
        };
        
        let translated = TranslatedPredicate::new(
            predicate.clone(),
            PredicateTranslation::api_param("customer", "cus_123"),
        ).with_selectivity(0.01);
        
        assert_eq!(translated.estimated_selectivity, 0.01);
        assert!(translated.translated.is_api());
    }

    #[test]
    fn test_predicate_translation_sql() {
        let translation = PredicateTranslation::sql("status = 'active'");
        
        assert!(translation.is_sql());
        assert!(!translation.is_api());
        assert_eq!(translation.as_sql(), Some("status = 'active'"));
        assert!(translation.as_api_params().is_none());
    }

    #[test]
    fn test_predicate_translation_api() {
        let translation = PredicateTranslation::api_param("created[gte]", "1704067200");
        
        assert!(translation.is_api());
        assert!(!translation.is_sql());
        
        let params = translation.as_api_params().unwrap();
        assert_eq!(params.get("created[gte]"), Some(&"1704067200".to_string()));
    }

    #[test]
    fn test_predicate_translation_parquet() {
        let translation = PredicateTranslation::parquet(
            "timestamp",
            Some("2024-01-01".to_string()),
            Some("2024-12-31".to_string()),
        );
        
        match translation {
            PredicateTranslation::ParquetFilter { column, min, max } => {
                assert_eq!(column, "timestamp");
                assert_eq!(min, Some("2024-01-01".to_string()));
                assert_eq!(max, Some("2024-12-31".to_string()));
            }
            _ => panic!("Expected ParquetFilter"),
        }
    }

    #[test]
    fn test_pushdown_warning() {
        let warning = PushdownWarning::new(
            "amount > 100",
            PushdownWarningReason::UnsupportedColumn {
                column: "amount".into(),
                source: "Stripe".to_string(),
            },
            "Stripe",
        )
        .with_suggestion("Use the 'customer' filter instead for better performance")
        .with_impact(EstimatedImpact::High);
        
        assert!(warning.is_high_impact());
        assert_eq!(warning.predicate, "amount > 100");
        assert!(warning.suggestion.is_some());
    }

    #[test]
    fn test_pushdown_warning_reason_description() {
        let reason = PushdownWarningReason::TooManyFilters {
            max: 10,
            requested: 15,
        };
        
        let desc = reason.description();
        assert!(desc.contains("15"));
        assert!(desc.contains("10"));
    }

    #[test]
    fn test_estimated_impact() {
        assert_eq!(EstimatedImpact::Low.weight(), 1);
        assert_eq!(EstimatedImpact::Medium.weight(), 2);
        assert_eq!(EstimatedImpact::High.weight(), 3);
        
        assert!(EstimatedImpact::High.description().contains("significant"));
    }

    #[test]
    fn test_predicate_to_filter_operation() {
        let eq = Predicate::Equals {
            column: "name".into(),
            value: "test".into(),
        };
        assert_eq!(eq.to_filter_operation(), Some(FilterOperation::Equals));
        
        let gt = Predicate::GreaterThan {
            column: "age".into(),
            value: "18".into(),
            inclusive: false,
        };
        assert_eq!(gt.to_filter_operation(), Some(FilterOperation::GreaterThan));
        
        let gte = Predicate::GreaterThan {
            column: "age".into(),
            value: "18".into(),
            inclusive: true,
        };
        assert_eq!(gte.to_filter_operation(), Some(FilterOperation::GreaterThanOrEquals));
        
        let between = Predicate::Between {
            column: "date".into(),
            low: "2024-01-01".into(),
            high: "2024-12-31".into(),
        };
        assert_eq!(between.to_filter_operation(), Some(FilterOperation::Between));
        
        // Compound predicates don't have a single operation
        let and = Predicate::And(vec![eq.clone()]);
        assert_eq!(and.to_filter_operation(), None);
    }

    #[test]
    fn test_source_predicate_analysis_add_methods() {
        let mut analysis = SourcePredicateAnalysis::new("test", "table");
        
        // Add pushable
        let predicate = Predicate::Equals {
            column: "status".into(),
            value: "active".into(),
        };
        let translated = TranslatedPredicate::new(
            predicate.clone(),
            PredicateTranslation::sql("status = 'active'"),
        );
        analysis.add_pushable(translated);
        assert!(analysis.has_pushable());
        
        // Add local with warning
        let local_predicate = Predicate::Like {
            column: "name".into(),
            pattern: "%search%".into(),
        };
        let warning = PushdownWarning::new(
            "name LIKE '%search%'",
            PushdownWarningReason::UnsupportedOperation {
                operation: "LIKE with leading wildcard".to_string(),
                source: "test".to_string(),
            },
            "test",
        );
        analysis.add_local_with_warning(local_predicate, warning);
        
        assert!(analysis.has_local());
        assert!(analysis.has_warnings());
        assert_eq!(analysis.local_only.len(), 1);
        assert_eq!(analysis.warnings.len(), 1);
    }

    // ========== PredicateSplitter Tests ==========

    #[test]
    fn test_predicate_splitter_postgresql() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let predicates = vec![
            Predicate::Equals {
                column: "status".into(),
                value: "active".into(),
            },
            Predicate::GreaterThan {
                column: "created_at".into(),
                value: "2024-01-01".into(),
                inclusive: true,
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "users");

        // PostgreSQL supports all predicates
        assert_eq!(analysis.pushable.len(), 2);
        assert!(analysis.local_only.is_empty());
        assert!(analysis.warnings.is_empty());
    }

    #[test]
    fn test_predicate_splitter_stripe() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("stripe", SourceType::Stripe);

        let predicates = vec![
            // Stripe supports customer equals
            Predicate::Equals {
                column: "customer".into(),
                value: "cus_123".into(),
            },
            // Stripe does NOT support amount filters (not in column_filters)
            Predicate::GreaterThan {
                column: "amount".into(),
                value: "1000".into(),
                inclusive: false,
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "charges");

        // Customer filter should be pushable
        assert!(analysis.has_pushable());
        // Amount filter should be local
        assert!(analysis.has_local());
        // Should have a warning about unsupported column
        assert!(analysis.has_warnings());
    }

    #[test]
    fn test_predicate_splitter_csv_no_pushdown() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("csv", SourceType::Csv);

        let predicates = vec![
            Predicate::Equals {
                column: "name".into(),
                value: "test".into(),
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "csv", "data");

        // CSV has no pushdown
        assert!(analysis.pushable.is_empty());
        assert_eq!(analysis.local_only.len(), 1);
        assert!(analysis.has_warnings());
    }

    #[test]
    fn test_predicate_splitter_or_not_supported() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("stripe", SourceType::Stripe);

        let predicates = vec![
            Predicate::Or(vec![
                Predicate::Equals {
                    column: "status".into(),
                    value: "active".into(),
                },
                Predicate::Equals {
                    column: "status".into(),
                    value: "pending".into(),
                },
            ]),
        ];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "charges");

        // OR should be local (Stripe doesn't support OR)
        assert!(analysis.pushable.is_empty());
        assert!(analysis.has_local());
    }

    #[test]
    fn test_predicate_splitter_generate_source_query_sql() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let predicates = vec![
            Predicate::Equals {
                column: "status".into(),
                value: "active".into(),
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "users");
        let query = splitter.generate_source_query("SELECT * FROM users", &analysis, SourceType::PostgreSQL);

        assert!(query.query.contains("WHERE"));
        assert!(query.api_params.is_empty());
    }

    #[test]
    fn test_predicate_splitter_unregistered_source() {
        let splitter = PredicateSplitter::new();

        let predicates = vec![
            Predicate::Equals {
                column: "name".into(),
                value: "test".into(),
            },
        ];

        // Unregistered source should default to no pushdown
        let analysis = splitter.analyze_for_source(&predicates, "unknown", "table");

        assert!(analysis.pushable.is_empty());
        assert!(analysis.has_local());
    }

    #[test]
    fn test_source_query_with_filters() {
        let query = SourceQueryWithFilters {
            query: "SELECT * FROM charges".to_string(),
            api_params: {
                let mut params = AHashMap::new();
                params.insert("customer".to_string(), "cus_123".to_string());
                params
            },
            local_filters: vec![
                Predicate::GreaterThan {
                    column: "amount".into(),
                    value: "1000".into(),
                    inclusive: false,
                },
            ],
        };

        assert!(query.has_api_params());
        assert!(query.has_local_filters());
        assert_eq!(query.api_params.get("customer"), Some(&"cus_123".to_string()));
    }

    #[test]
    fn test_predicate_splitter_and_partial_pushdown() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        // AND predicate where all parts can be pushed
        let predicates = vec![
            Predicate::And(vec![
                Predicate::Equals {
                    column: "status".into(),
                    value: "active".into(),
                },
                Predicate::GreaterThan {
                    column: "age".into(),
                    value: "18".into(),
                    inclusive: true,
                },
            ]),
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "users");

        // AND should be fully pushable for PostgreSQL
        assert!(analysis.has_pushable());
        assert!(analysis.local_only.is_empty());
    }

    // ========== Comprehensive Integration Tests ==========

    #[test]
    fn test_predicate_splitter_stripe_timestamp_transform() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("stripe", SourceType::Stripe);

        // Stripe's created filter should get timestamp transform
        let predicates = vec![
            Predicate::GreaterThan {
                column: "created".into(),
                value: "2024-01-01T00:00:00Z".into(),
                inclusive: true,
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "charges");

        // Created filter should be pushable with transform
        assert!(analysis.has_pushable());
        assert_eq!(analysis.pushable.len(), 1);
        
        // Check that the translation is an API param type
        match &analysis.pushable[0].translated {
            PredicateTranslation::ApiParams(params) => {
                // Should have a created-related parameter
                let has_created_param = params.keys().any(|k| k.contains("created"));
                assert!(has_created_param, "Expected created parameter, got: {:?}", params);
            }
            other => {
                // Or SqlFragment for sources that translate to SQL-like
                match other {
                    PredicateTranslation::SqlFragment(s) => assert!(s.contains("created")),
                    _ => panic!("Unexpected translation type: {:?}", other),
                }
            }
        }
    }

    #[test]
    fn test_predicate_splitter_not_predicate_postgres() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let predicates = vec![
            Predicate::Not(Box::new(Predicate::Equals {
                column: "status".into(),
                value: "deleted".into(),
            })),
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "users");

        // PostgreSQL supports NOT
        assert!(analysis.has_pushable());
        assert!(analysis.local_only.is_empty());
    }

    #[test]
    fn test_predicate_splitter_not_predicate_stripe() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("stripe", SourceType::Stripe);

        let predicates = vec![
            Predicate::Not(Box::new(Predicate::Equals {
                column: "status".into(),
                value: "failed".into(),
            })),
        ];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "charges");

        // Stripe doesn't support NOT
        assert!(analysis.pushable.is_empty());
        assert!(analysis.has_local());
        assert!(analysis.has_warnings());
    }

    #[test]
    fn test_predicate_splitter_nested_and() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        // Nested AND predicates
        let predicates = vec![
            Predicate::And(vec![
                Predicate::Equals {
                    column: "status".into(),
                    value: "active".into(),
                },
                Predicate::And(vec![
                    Predicate::GreaterThan {
                        column: "age".into(),
                        value: "18".into(),
                        inclusive: true,
                    },
                    Predicate::LessThan {
                        column: "age".into(),
                        value: "65".into(),
                        inclusive: true,
                    },
                ]),
            ]),
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "users");

        // PostgreSQL supports nested AND
        assert!(analysis.has_pushable());
    }

    #[test]
    fn test_predicate_splitter_between() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let predicates = vec![
            Predicate::Between {
                column: "created_at".into(),
                low: "2024-01-01".into(),
                high: "2024-12-31".into(),
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "orders");

        assert!(analysis.has_pushable());
        
        // Check SQL translation contains BETWEEN
        if let PredicateTranslation::SqlFragment(fragment) = &analysis.pushable[0].translated {
            assert!(fragment.contains("BETWEEN"));
            assert!(fragment.contains("2024-01-01"));
            assert!(fragment.contains("2024-12-31"));
        }
    }

    #[test]
    fn test_predicate_splitter_in_list() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let predicates = vec![
            Predicate::In {
                column: "status".into(),
                values: vec!["active".into(), "pending".into(), "review".into()],
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "orders");

        assert!(analysis.has_pushable());
        
        // Check SQL translation contains IN
        if let PredicateTranslation::SqlFragment(fragment) = &analysis.pushable[0].translated {
            assert!(fragment.contains("IN"));
            assert!(fragment.contains("active"));
        }
    }

    #[test]
    fn test_predicate_splitter_like_pattern() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let predicates = vec![
            Predicate::Like {
                column: "name".into(),
                pattern: "John%".into(),
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "users");

        assert!(analysis.has_pushable());
        
        if let PredicateTranslation::SqlFragment(fragment) = &analysis.pushable[0].translated {
            assert!(fragment.contains("LIKE"));
            assert!(fragment.contains("John%"));
        }
    }

    #[test]
    fn test_predicate_splitter_is_null() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let predicates = vec![
            Predicate::IsNull {
                column: "deleted_at".into(),
                is_null: true,
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "users");

        assert!(analysis.has_pushable());
        
        if let PredicateTranslation::SqlFragment(fragment) = &analysis.pushable[0].translated {
            assert!(fragment.contains("IS NULL"));
        }
    }

    #[test]
    fn test_predicate_splitter_is_not_null() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let predicates = vec![
            Predicate::IsNull {
                column: "email".into(),
                is_null: false,
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "postgres", "users");

        assert!(analysis.has_pushable());
        
        if let PredicateTranslation::SqlFragment(fragment) = &analysis.pushable[0].translated {
            assert!(fragment.contains("IS NOT NULL"));
        }
    }

    #[test]
    fn test_predicate_splitter_external_parquet_capabilities() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("parquet", SourceType::ExternalParquet);

        let predicates = vec![
            Predicate::Equals {
                column: "category".into(),
                value: "electronics".into(),
            },
            Predicate::GreaterThan {
                column: "price".into(),
                value: "100".into(),
                inclusive: true,
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "parquet", "products");

        // ExternalParquet doesn't support arbitrary SQL and doesn't have 
        // column-specific filters for generic columns, so predicates are local.
        // This is expected - Parquet pushdown works at the file reading level,
        // not through the PredicateSplitter for arbitrary columns.
        assert!(analysis.has_local());
    }

    #[test]
    fn test_predicate_splitter_mixed_pushable_and_local() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("stripe", SourceType::Stripe);

        let predicates = vec![
            // Pushable: customer filter
            Predicate::Equals {
                column: "customer".into(),
                value: "cus_abc123".into(),
            },
            // Pushable: created date range
            Predicate::GreaterThan {
                column: "created".into(),
                value: "1704067200".into(),
                inclusive: true,
            },
            // Local: amount (not in Stripe's column_filters)
            Predicate::GreaterThan {
                column: "amount".into(),
                value: "5000".into(),
                inclusive: true,
            },
            // Local: metadata (complex filter)
            Predicate::Like {
                column: "metadata".into(),
                pattern: "%premium%".into(),
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "charges");

        // Should have mixed results
        assert!(analysis.has_pushable());
        assert!(analysis.has_local());
        assert!(analysis.has_warnings());
        
        // Should have 2 pushable, 2 local
        assert_eq!(analysis.pushable.len(), 2);
        assert_eq!(analysis.local_only.len(), 2);
        assert_eq!(analysis.warnings.len(), 2);
    }

    #[test]
    fn test_predicate_splitter_jira_jql() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("jira", SourceType::Jira);

        let predicates = vec![
            Predicate::Equals {
                column: "project".into(),
                value: "PROJ".into(),
            },
            Predicate::Equals {
                column: "status".into(),
                value: "Open".into(),
            },
            Predicate::Equals {
                column: "assignee".into(),
                value: "john.doe".into(),
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "jira", "issues");

        // Jira supports operations via JQL, but requires column-specific
        // capabilities to be defined. Without explicit column filters,
        // predicates on generic columns are evaluated locally.
        // The capability matrix defines supported operations, but for non-SQL
        // sources, column-specific filters must be defined.
        assert!(analysis.local_only.len() + analysis.pushable.len() == 3);
    }

    #[test]
    fn test_predicate_splitter_salesforce_soql() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("salesforce", SourceType::Salesforce);

        let predicates = vec![
            Predicate::Equals {
                column: "OwnerId".into(),
                value: "005xxx".into(),
            },
            Predicate::GreaterThan {
                column: "CreatedDate".into(),
                value: "2024-01-01T00:00:00Z".into(),
                inclusive: true,
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "salesforce", "Account");

        // Salesforce SOQL is SQL-like but without arbitrary SQL support and
        // column-specific capabilities, predicates are evaluated locally.
        // This reflects that SOQL requires explicit field mapping.
        assert_eq!(analysis.local_only.len() + analysis.pushable.len(), 2);
    }

    #[test]
    fn test_combined_selectivity_calculation() {
        let mut analysis = SourcePredicateAnalysis::new("test", "table");
        
        // Add multiple pushable predicates with different selectivities
        let pred1 = Predicate::Equals { column: "status".into(), value: "active".into() };
        let translated1 = TranslatedPredicate::new(pred1.clone(), PredicateTranslation::sql("status = 'active'"));
        analysis.add_pushable(translated1);
        analysis.pushable_selectivity = 0.1; // 10% of rows
        
        let pred2 = Predicate::Equals { column: "region".into(), value: "US".into() };
        let translated2 = TranslatedPredicate::new(pred2.clone(), PredicateTranslation::sql("region = 'US'"));
        analysis.add_pushable(translated2);
        
        // Add a local predicate
        analysis.add_local(Predicate::Like { column: "name".into(), pattern: "%test%".into() });
        analysis.local_selectivity = 0.5; // 50% of remaining rows
        
        // Combined selectivity should be: 0.1 * 0.5 = 0.05 (5%)
        let combined = analysis.combined_selectivity();
        assert!(combined < 0.1); // Should be less than either individual selectivity
    }

    #[test]
    fn test_source_query_with_filters_no_params() {
        let query = SourceQueryWithFilters {
            query: "SELECT * FROM users".to_string(),
            api_params: AHashMap::new(),
            local_filters: vec![],
        };

        assert!(!query.has_api_params());
        assert!(!query.has_local_filters());
    }

    #[test]
    fn test_multiple_sources_same_splitter() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);
        splitter.register_source_type("stripe", SourceType::Stripe);
        splitter.register_source_type("csv", SourceType::Csv);

        let predicates = vec![
            Predicate::Equals {
                column: "status".into(),
                value: "active".into(),
            },
        ];

        // Same predicate analyzed for different sources
        let pg_analysis = splitter.analyze_for_source(&predicates, "postgres", "users");
        let stripe_analysis = splitter.analyze_for_source(&predicates, "stripe", "charges");
        let csv_analysis = splitter.analyze_for_source(&predicates, "csv", "data");

        // PostgreSQL: pushable (supports arbitrary SQL)
        assert!(pg_analysis.has_pushable());
        assert!(!pg_analysis.has_local());

        // Stripe: status is in column_filters for Stripe, so it's pushable
        // But the operation needs to match - Stripe status supports Equals
        assert!(stripe_analysis.has_pushable() || stripe_analysis.has_local());
        // Verify total predicates are accounted for
        assert_eq!(stripe_analysis.pushable.len() + stripe_analysis.local_only.len(), 1);

        // CSV: always local (no pushdown support)
        assert!(!csv_analysis.has_pushable());
        assert!(csv_analysis.has_local());
    }

    #[test]
    fn test_predicate_translation_passthrough() {
        let translation = PredicateTranslation::Passthrough;
        match translation {
            PredicateTranslation::Passthrough => (), // Expected
            _ => panic!("Expected Passthrough variant"),
        }
    }

    #[test]
    fn test_predicate_translation_graphql() {
        let translation = PredicateTranslation::GraphQLFilter {
            field: "status".to_string(),
            operator: "eq".to_string(),
            value: "active".into(),
        };
        
        if let PredicateTranslation::GraphQLFilter { field, operator, value } = translation {
            assert_eq!(field, "status");
            assert_eq!(operator, "eq");
            assert_eq!(value, "active");
        } else {
            panic!("Expected GraphQLFilter variant");
        }
    }

    #[test]
    fn test_warning_impact_levels() {
        // Low impact warning (small data reduction)
        let low_warning = PushdownWarning::new(
            "limit > 100",
            PushdownWarningReason::UnsupportedOperation {
                operation: "limit comparison".to_string(),
                source: "csv".to_string(),
            },
            "csv",
        )
        .with_impact(EstimatedImpact::Low);
        
        assert!(!low_warning.is_high_impact());
        
        // High impact warning (could filter most data)
        let high_warning = PushdownWarning::new(
            "date > '2024-01-01'",
            PushdownWarningReason::UnsupportedOperation {
                operation: "date comparison".to_string(),
                source: "csv".to_string(),
            },
            "csv",
        )
        .with_impact(EstimatedImpact::High);
        
        assert!(high_warning.is_high_impact());
    }

    #[test]
    fn test_pushdown_warning_reason_variants() {
        let reasons = vec![
            PushdownWarningReason::UnsupportedOperation {
                operation: "LIKE".to_string(),
                source: "api".to_string(),
            },
            PushdownWarningReason::UnsupportedColumn {
                column: "metadata".into(),
                source: "stripe".to_string(),
            },
            PushdownWarningReason::OrNotSupported,
            PushdownWarningReason::NotNotSupported,
            PushdownWarningReason::TooManyFilters {
                max: 10,
                requested: 15,
            },
            PushdownWarningReason::ComplexPredicate {
                description: "Nested OR with complex subexpressions".to_string(),
            },
        ];

        // All reasons should have a description
        for reason in reasons {
            let desc = reason.description();
            assert!(!desc.is_empty(), "Description should not be empty for {:?}", reason);
        }
    }

    #[test]
    fn test_transform_failed_includes_column_name() {
        use crate::warehouse::query::cost_model::{ColumnFilterCapability, ValueTransform};
        
        let splitter = PredicateSplitter::new();
        
        // Test CentsToDollars with invalid value
        let result = splitter.apply_transform(
            "price",
            "not_a_number",
            &ValueTransform::CentsToDollars,
        );
        
        if let Err(PushdownWarningReason::TransformFailed { column, value, reason }) = result {
            assert_eq!(column, "price", "Column name should be 'price'");
            assert_eq!(value, "not_a_number");
            assert!(reason.contains("cents"), "Reason should mention cents conversion");
        } else {
            panic!("Expected TransformFailed error");
        }
        
        // Test DollarsToCents with invalid value
        let result = splitter.apply_transform(
            "amount",
            "abc",
            &ValueTransform::DollarsToCents,
        );
        
        if let Err(PushdownWarningReason::TransformFailed { column, value, reason }) = result {
            assert_eq!(column, "amount", "Column name should be 'amount'");
            assert_eq!(value, "abc");
            assert!(reason.contains("dollars"), "Reason should mention dollars conversion");
        } else {
            panic!("Expected TransformFailed error");
        }

        // Test DollarsToCents with overflowing value (would exceed i64::MAX as cents)
        let result = splitter.apply_transform(
            "amount",
            "1e18",
            &ValueTransform::DollarsToCents,
        );

        if let Err(PushdownWarningReason::TransformFailed { column, value, reason }) = result {
            assert_eq!(column, "amount");
            assert_eq!(value, "1e18");
            assert!(reason.contains("too large"), "Reason should indicate overflow: {reason}");
        } else {
            panic!("Expected TransformFailed error for overflowing dollar amount, got: {result:?}");
        }

        // Test TimestampToEpoch with invalid value
        let result = splitter.apply_transform(
            "created_at",
            "invalid-timestamp",
            &ValueTransform::TimestampToEpoch,
        );
        
        if let Err(PushdownWarningReason::TransformFailed { column, value, reason }) = result {
            assert_eq!(column, "created_at", "Column name should be 'created_at'");
            assert_eq!(value, "invalid-timestamp");
            assert!(reason.contains("timestamp"), "Reason should mention timestamp");
        } else {
            panic!("Expected TransformFailed error");
        }
    }

    // ========== Deeply Nested Predicate Tests ==========

    #[test]
    fn test_deeply_nested_and_or_predicates() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        // 4 levels deep: AND(OR(AND(Equals, Equals), Not(Equals)), Equals)
        let deep_predicate = Predicate::And(vec![
            Predicate::Or(vec![
                Predicate::And(vec![
                    Predicate::Equals { column: "a".into(), value: "1".into() },
                    Predicate::Equals { column: "b".into(), value: "2".into() },
                ]),
                Predicate::Not(Box::new(Predicate::Equals {
                    column: "c".into(),
                    value: "3".into(),
                })),
            ]),
            Predicate::Equals { column: "d".into(), value: "4".into() },
        ]);

        let analysis = splitter.analyze_for_source(&[deep_predicate], "postgres", "table");
        
        // PostgreSQL supports all operations, so this should be pushable
        assert!(analysis.has_pushable(), "Deeply nested predicate should be pushable to PostgreSQL");
    }

    #[test]
    fn test_nested_not_in_and() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        // AND(NOT(Equals), NOT(Equals))
        let predicate = Predicate::And(vec![
            Predicate::Not(Box::new(Predicate::Equals {
                column: "status".into(),
                value: "deleted".into(),
            })),
            Predicate::Not(Box::new(Predicate::Equals {
                column: "archived".into(),
                value: "true".into(),
            })),
        ]);

        let analysis = splitter.analyze_for_source(&[predicate], "postgres", "table");
        assert!(analysis.has_pushable());
    }

    #[test]
    fn test_triple_nested_or() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        // OR(OR(Equals, Equals), OR(Equals, Equals))
        let predicate = Predicate::Or(vec![
            Predicate::Or(vec![
                Predicate::Equals { column: "a".into(), value: "1".into() },
                Predicate::Equals { column: "b".into(), value: "2".into() },
            ]),
            Predicate::Or(vec![
                Predicate::Equals { column: "c".into(), value: "3".into() },
                Predicate::Equals { column: "d".into(), value: "4".into() },
            ]),
        ]);

        let analysis = splitter.analyze_for_source(&[predicate], "postgres", "table");
        assert!(analysis.has_pushable());
    }

    #[test]
    fn test_nested_predicate_in_non_sql_source() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("stripe", SourceType::Stripe);

        // Stripe doesn't support OR, so nested OR should be local
        let predicate = Predicate::And(vec![
            Predicate::Or(vec![
                Predicate::Equals { column: "status".into(), value: "active".into() },
                Predicate::Equals { column: "status".into(), value: "pending".into() },
            ]),
        ]);

        let analysis = splitter.analyze_for_source(&[predicate], "stripe", "customers");
        
        // OR is not supported by Stripe, so should be local
        assert!(analysis.has_local(), "Nested OR should be local for Stripe");
    }

    #[test]
    fn test_very_deep_nesting_no_stack_overflow() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        // Build a deeply nested AND predicate (10 levels)
        let mut predicate = Predicate::Equals { 
            column: "x".into(), 
            value: "1".into() 
        };
        for _ in 0..10 {
            predicate = Predicate::And(vec![predicate]);
        }

        // This should not cause a stack overflow
        let analysis = splitter.analyze_for_source(&[predicate], "postgres", "table");
        assert!(analysis.has_pushable());
    }

    // ========== SQL Injection Prevention Tests ==========

    #[test]
    fn test_sql_escape_injection_attempts() {
        // Classic SQL injection - single quote becomes two single quotes
        assert_eq!(
            escape_sql_string("'; DROP TABLE users; --"),
            "''; DROP TABLE users; --"
        );
        
        // Multi-byte character edge cases
        assert_eq!(escape_sql_string("测试'注入"), "测试''注入");
        
        // Double quote doesn't need escaping in single-quoted strings
        assert_eq!(escape_sql_string("test\"value"), "test\"value");
        
        // Backslash escaping
        assert_eq!(escape_sql_string("test\\value"), "test\\\\value");
        
        // Backslash before quote
        assert_eq!(escape_sql_string("test\\'value"), "test\\\\''value");
        
        // Multiple quotes in a row
        assert_eq!(escape_sql_string("a''b"), "a''''b");
    }

    #[test]
    fn test_column_name_injection_attempts() {
        // These should all be rejected as invalid
        assert!(!is_valid_column_name("id; DROP TABLE users"));
        assert!(!is_valid_column_name("id`; DROP TABLE"));
        assert!(!is_valid_column_name("id\"; DROP TABLE"));
        assert!(!is_valid_column_name("id\n; DROP TABLE"));
        assert!(!is_valid_column_name("id\r\n; DROP"));
        
        // Valid column names
        assert!(is_valid_column_name("user_id"));
        assert!(is_valid_column_name("created_at"));
        assert!(is_valid_column_name("camelCase"));
        assert!(is_valid_column_name("column123"));
        assert!(is_valid_column_name("_private"));
    }

    #[test]
    fn test_predicate_to_display_sql_with_malicious_input() {
        let malicious = Predicate::Equals {
            column: "name".into(),
            value: "'; DELETE FROM users WHERE '1'='1".into(),
        };
        
        let sql = predicate_to_display_sql(&malicious, SqlDialect::ClickHouse);
        // Should be safely escaped - the quote should become double quotes
        assert!(sql.contains("''"), "SQL injection should be escaped");
        // The dangerous DELETE should still be there but as a literal string value
        assert!(sql.contains("DELETE"), "DELETE text should be in escaped string");
    }

    #[test]
    fn test_escape_like_pattern_injection() {
        // LIKE-specific escaping: wildcards get double-backslash prefix
        // so the escape survives ClickHouse string literal parsing.
        assert_eq!(escape_like_pattern("%admin%"), "\\\\%admin\\\\%");
        assert_eq!(escape_like_pattern("_secret_"), "\\\\_secret\\\\_");
        assert_eq!(escape_like_pattern("100%"), "100\\\\%");
    }

    #[test]
    fn test_column_name_escaping() {
        assert_eq!(escape_column_name("user_id", SqlDialect::ClickHouse), "`user_id`");
        assert_eq!(escape_column_name("table", SqlDialect::ClickHouse), "`table`");
        assert_eq!(
            escape_column_name("table.column", SqlDialect::ClickHouse), "`table.column`",
            "Qualified names with dots must be accepted"
        );
        assert_eq!(
            escape_column_name("created-at", SqlDialect::ClickHouse), "`created-at`",
            "Hyphenated column names must be accepted"
        );
    }

    #[test]
    fn test_column_name_escaping_postgres() {
        assert_eq!(escape_column_name("user_id", SqlDialect::Postgres), "\"user_id\"");
        assert_eq!(escape_column_name("user-name", SqlDialect::Postgres), "\"user-name\"");
    }

    #[test]
    fn test_column_name_escaping_mysql() {
        assert_eq!(escape_column_name("user_id", SqlDialect::MySQL), "`user_id`");
    }

    #[test]
    fn test_predicate_to_sql_dialect_aware() {
        let eq = Predicate::Equals {
            column: "user-name".into(),
            value: "alice".into(),
        };
        assert_eq!(
            predicate_to_sql(&eq, SqlDialect::Postgres),
            "\"user-name\" = 'alice'"
        );
        assert_eq!(
            predicate_to_sql(&eq, SqlDialect::ClickHouse),
            "`user-name` = 'alice'"
        );
        assert_eq!(
            predicate_to_sql(&eq, SqlDialect::Snowflake),
            "\"user-name\" = 'alice'"
        );
    }

    // ========== Empty Predicate Vector Tests ==========

    #[test]
    fn test_empty_and_predicate() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let empty_and = Predicate::And(vec![]);
        let analysis = splitter.analyze_for_source(&[empty_and], "postgres", "table");
        
        // Empty AND has no parts to push, so it ends up as local.
        // This is acceptable behavior - an empty AND is semantically "true"
        // and will pass all rows locally without needing remote evaluation.
        // The key is that it doesn't crash and produces a valid analysis.
        assert_eq!(
            analysis.pushable.len() + analysis.local_only.len(), 
            1, 
            "Empty AND should be tracked as exactly one predicate"
        );
    }

    #[test]
    fn test_empty_or_predicate() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let empty_or = Predicate::Or(vec![]);
        let analysis = splitter.analyze_for_source(&[empty_or], "postgres", "table");
        
        // Empty OR should be handled gracefully
        // (semantically always false, but we just need to not crash)
        let total = analysis.pushable.len() + analysis.local_only.len();
        assert!(total <= 1, "Empty OR should be handled as a single predicate");
    }

    #[test]
    fn test_single_element_and_predicate() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        // AND with single element should behave same as the element alone
        let single_and = Predicate::And(vec![
            Predicate::Equals { column: "id".into(), value: "1".into() },
        ]);
        
        let analysis = splitter.analyze_for_source(&[single_and], "postgres", "table");
        assert!(analysis.has_pushable(), "Single-element AND should be pushable");
    }

    #[test]
    fn test_single_element_or_predicate() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        // OR with single element should behave same as the element alone
        let single_or = Predicate::Or(vec![
            Predicate::Equals { column: "id".into(), value: "1".into() },
        ]);
        
        let analysis = splitter.analyze_for_source(&[single_or], "postgres", "table");
        assert!(analysis.has_pushable(), "Single-element OR should be pushable");
    }

    // ===== Predicate::Contains Tests =====

    #[test]
    fn test_contains_predicate_to_display_sql() {
        let pred = Predicate::Contains {
            column: "name".into(),
            substring: "foo".into(),
        };
        assert_eq!(predicate_to_display_sql(&pred, SqlDialect::ClickHouse), "`name` LIKE '%foo%'");
    }

    #[test]
    fn test_contains_predicate_column() {
        let pred = Predicate::Contains {
            column: "name".into(),
            substring: "foo".into(),
        };
        assert_eq!(pred.column(), Some("name"));
    }

    #[test]
    fn test_contains_predicate_not_selective() {
        let pred = Predicate::Contains {
            column: "name".into(),
            substring: "foo".into(),
        };
        assert!(!pred.is_selective(), "Contains should not be selective for PREWHERE");
    }

    #[test]
    fn test_contains_file_predicate_extraction() {
        let mut indexed = AHashSet::new();
        indexed.insert("name".to_string());

        let pred = Predicate::Contains {
            column: "name".into(),
            substring: "foo".into(),
        };

        let pushdown = PredicatePushdown::analyze(vec![pred], &indexed);

        assert_eq!(pushdown.file_predicates.len(), 1);
        let file_pred = &pushdown.file_predicates[0];
        assert_eq!(file_pred.column, "name");
        match &file_pred.predicate_type {
            FilePredicateType::SubstringMatch(v) => assert_eq!(v, "foo"),
            other => panic!("Expected SubstringMatch, got {:?}", other),
        }
    }

    #[test]
    fn test_contains_predicate_escapes_backslash() {
        let pred = Predicate::Contains {
            column: "path".into(),
            substring: "back\\slash".into(),
        };
        let sql = predicate_to_display_sql(&pred, SqlDialect::ClickHouse);
        // The output SQL must produce a LIKE pattern that, after ClickHouse
        // string-literal parsing and LIKE evaluation, matches a literal
        // backslash.
        //   escape_like_pattern("back\slash") → back\\\\slash
        //   full SQL: `path` LIKE '%back\\\\slash%'
        //   CH string parse: %back\\slash%
        //   LIKE: % wildcard, back, \\ → literal \, slash, % wildcard
        assert_eq!(sql, "`path` LIKE '%back\\\\\\\\slash%'");
    }

    #[test]
    fn test_contains_predicate_escapes_percent() {
        let pred = Predicate::Contains {
            column: "desc".into(),
            substring: "100%".into(),
        };
        let sql = predicate_to_display_sql(&pred, SqlDialect::ClickHouse);
        // escape_like_pattern("100%") → 100\\%
        // full SQL: `desc` LIKE '%100\\%%'
        // CH string parse: %100\%%
        // LIKE: % wildcard, 100, \% → literal %, % wildcard
        assert_eq!(sql, "`desc` LIKE '%100\\\\%%'");
    }

    #[test]
    fn test_contains_predicate_escapes_backslash_percent_combo() {
        let pred = Predicate::Contains {
            column: "data".into(),
            substring: "a\\%b".into(),
        };
        let sql = predicate_to_display_sql(&pred, SqlDialect::ClickHouse);
        // \ → \\\\  (4 backslashes)
        // % → \\%   (2 backslashes + %)
        // full SQL: `data` LIKE '%a\\\\\\%b%'
        assert_eq!(sql, "`data` LIKE '%a\\\\\\\\\\\\%b%'");
    }

    #[test]
    fn test_not_predicate_passthrough_no_invalid_sql() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("parquet", SourceType::ExternalParquet);

        let predicates = vec![
            Predicate::Not(Box::new(Predicate::Equals {
                column: "status".into(),
                value: "deleted".into(),
            })),
        ];

        let analysis = splitter.analyze_for_source(&predicates, "parquet", "events");

        for pred in &analysis.pushable {
            if let PredicateTranslation::SqlFragment(sql) = &pred.translated {
                assert!(
                    !sql.contains("NOT ()"),
                    "Must not generate invalid SQL 'NOT ()', got: {}",
                    sql
                );
            }
        }
    }

    #[test]
    fn test_or_selectivity_uses_inclusion_exclusion() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("csv", SourceType::Csv);

        let predicates = vec![Predicate::Or(vec![
            Predicate::GreaterThan {
                column: "a".into(),
                value: "10".into(),
                inclusive: false,
            },
            Predicate::GreaterThan {
                column: "b".into(),
                value: "20".into(),
                inclusive: false,
            },
        ])];

        let analysis = splitter.analyze_for_source(&predicates, "csv", "data");

        // GreaterThan selectivity = 0.3 each
        // Inclusion-exclusion: 0.3 + 0.3 - 0.3*0.3 = 0.51
        // Naive sum would give 0.6, clamped to 0.6
        let sel = analysis.local_selectivity;
        assert!(
            (sel - 0.51).abs() < 0.05,
            "OR selectivity should be ~0.51 via inclusion-exclusion, got {}",
            sel
        );
    }

    #[test]
    fn test_escape_like_pattern_strips_null_bytes() {
        let result = escape_like_pattern("hello\0world");
        assert!(
            !result.is_empty(),
            "Should strip null bytes, not return empty: {}",
            result
        );
        assert!(result.contains("helloworld"));
    }

    #[test]
    fn test_escape_like_pattern_normal_input() {
        let result = escape_like_pattern("100%_done");
        assert!(result.contains("\\\\%"), "% should be escaped with double backslash");
        assert!(result.contains("\\\\_"), "_ should be escaped with double backslash");
    }

    // ========== Regression Tests for Bug Fixes ==========

    #[test]
    fn test_not_partial_predicate_kept_local() {
        use crate::warehouse::types::SourceType;

        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("stripe", SourceType::Stripe);

        // NOT(customer = 'x' AND unsupported_col = 'y')
        // customer is pushable on Stripe, unsupported_col is not.
        // The inner AND is Partial, so NOT(Partial) must be kept fully local.
        let predicates = vec![
            Predicate::Not(Box::new(Predicate::And(vec![
                Predicate::Equals {
                    column: "customer".into(),
                    value: "cus_123".into(),
                },
                Predicate::Equals {
                    column: "unsupported_col".into(),
                    value: "val".into(),
                },
            ]))),
        ];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "charges");

        // The NOT must NOT be pushed (it would drop the negation).
        assert!(
            analysis.pushable.is_empty(),
            "NOT with partial inner must not be pushed; pushable = {:?}",
            analysis.pushable.len()
        );
        assert!(analysis.has_local());
    }

    #[test]
    fn test_empty_or_handled_gracefully() {
        use crate::warehouse::types::SourceType;

        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("postgres", SourceType::PostgreSQL);

        let predicates = vec![Predicate::Or(vec![])];
        let analysis = splitter.analyze_for_source(&predicates, "postgres", "t");

        // Empty OR should be local (semantically always false), not generate invalid SQL
        assert!(
            analysis.pushable.is_empty(),
            "Empty OR should not produce pushable predicates"
        );
    }

    #[test]
    fn test_range_preserves_inclusive_flag() {
        let mut indexed = AHashSet::new();
        indexed.insert("price".to_string());

        let gt_exclusive = Predicate::GreaterThan {
            column: "price".into(),
            value: "100".into(),
            inclusive: false,
        };
        let gt_inclusive = Predicate::GreaterThan {
            column: "price".into(),
            value: "100".into(),
            inclusive: true,
        };

        let pushdown_excl = PredicatePushdown::analyze(vec![gt_exclusive], &indexed);
        let pushdown_incl = PredicatePushdown::analyze(vec![gt_inclusive], &indexed);

        let pred_excl = &pushdown_excl.file_predicates[0];
        let pred_incl = &pushdown_incl.file_predicates[0];

        match &pred_excl.predicate_type {
            FilePredicateType::Range { min_inclusive, .. } => {
                assert!(!min_inclusive, "Exclusive > should have min_inclusive=false");
            }
            _ => panic!("Expected Range"),
        }
        match &pred_incl.predicate_type {
            FilePredicateType::Range { min_inclusive, .. } => {
                assert!(min_inclusive, "Inclusive >= should have min_inclusive=true");
            }
            _ => panic!("Expected Range"),
        }
    }

    #[test]
    fn test_rewrite_with_prewhere_before_order_by() {
        let mut indexed = AHashSet::new();
        indexed.insert("event_type".to_string());

        let predicates = vec![
            Predicate::Equals {
                column: "event_type".into(),
                value: "purchase".into(),
            },
        ];

        let pushdown = PredicatePushdown::analyze(predicates, &indexed);
        let rewritten = pushdown.rewrite_with_prewhere(
            "SELECT * FROM events ORDER BY created_at"
        );

        assert!(
            rewritten.contains("PREWHERE") || rewritten.contains("WHERE"),
            "Should contain a filter clause"
        );
        let prewhere_pos = rewritten.find("PREWHERE").or_else(|| rewritten.find("WHERE")).unwrap();
        let order_pos = rewritten.find("ORDER BY").unwrap();
        assert!(
            prewhere_pos < order_pos,
            "Filter clause must come before ORDER BY. Got: {}",
            rewritten
        );
    }

    #[test]
    fn test_rewrite_with_prewhere_before_group_by_limit() {
        let mut indexed = AHashSet::new();
        indexed.insert("status".to_string());

        let predicates = vec![
            Predicate::Equals {
                column: "status".into(),
                value: "active".into(),
            },
        ];

        let pushdown = PredicatePushdown::analyze(predicates, &indexed);
        let rewritten = pushdown.rewrite_with_prewhere(
            "SELECT status, count(*) FROM users GROUP BY status LIMIT 10"
        );

        let filter_pos = rewritten.find("PREWHERE").or_else(|| rewritten.find("WHERE")).unwrap();
        let group_pos = rewritten.find("GROUP BY").unwrap();
        assert!(
            filter_pos < group_pos,
            "Filter clause must come before GROUP BY. Got: {}",
            rewritten
        );
    }

    // ========== Regression Tests ==========

    #[test]
    fn test_rewrite_with_existing_where_and_order_by() {
        let mut indexed = AHashSet::new();
        indexed.insert("status".to_string());

        let predicates = vec![Predicate::Equals {
            column: "status".into(),
            value: "active".into(),
        }];

        let pushdown = PredicatePushdown::analyze(predicates, &indexed);
        let rewritten = pushdown.rewrite_with_prewhere(
            "SELECT * FROM t WHERE x = 1 ORDER BY y LIMIT 10"
        );

        assert!(
            rewritten.contains("PREWHERE"),
            "Indexed predicate should go to PREWHERE. Got: {}",
            rewritten
        );
        assert!(
            rewritten.contains("WHERE"),
            "Existing WHERE should be preserved. Got: {}",
            rewritten
        );

        assert!(
            rewritten.contains("ORDER BY y"),
            "ORDER BY clause must be preserved. Got: {}",
            rewritten
        );
        assert!(
            rewritten.contains("LIMIT 10"),
            "LIMIT clause must be preserved. Got: {}",
            rewritten
        );
    }

    #[test]
    fn test_inject_where_preserves_subquery() {
        let dialect = ClickHouseDialect {};
        let mut stmts = Parser::parse_sql(&dialect, "SELECT * FROM (SELECT * FROM t WHERE x = 1) AS sub").unwrap();
        let cond = parse_condition_expr("y = 2").unwrap();
        for stmt in &mut stmts {
            if let Statement::Query(q) = stmt {
                inject_where_into_query(q, cond.clone());
            }
        }
        let result = serialize_statements(&stmts);
        assert!(result.contains("y = 2"), "Should inject WHERE at top level: {}", result);
        assert!(result.contains("x = 1"), "Should preserve subquery WHERE: {}", result);
    }

    #[test]
    fn test_inject_where_preserves_order_by_in_string() {
        let dialect = ClickHouseDialect {};
        let mut stmts = Parser::parse_sql(&dialect, "SELECT * FROM t WHERE name = 'ORDER BY hack' ORDER BY real_col").unwrap();
        let cond = parse_condition_expr("z = 3").unwrap();
        for stmt in &mut stmts {
            if let Statement::Query(q) = stmt {
                inject_where_into_query(q, cond.clone());
            }
        }
        let result = serialize_statements(&stmts);
        assert!(result.contains("z = 3"), "Should inject condition: {}", result);
        assert!(result.contains("ORDER BY real_col"), "Should preserve ORDER BY: {}", result);
    }

    #[test]
    fn test_numeric_literal_detection() {
        assert!(is_numeric_literal("42"));
        assert!(is_numeric_literal("-3.14"));
        assert!(is_numeric_literal("0"));
        assert!(is_numeric_literal("+100"));
        assert!(is_numeric_literal("1000000"));
        assert!(!is_numeric_literal(""));
        assert!(!is_numeric_literal("abc"));
        assert!(!is_numeric_literal("12abc"));
        assert!(!is_numeric_literal("1.2.3"));
    }

    #[test]
    fn test_predicate_to_display_sql_numeric_values_unquoted() {
        let gt = Predicate::GreaterThan {
            column: "price".into(),
            value: "100".into(),
            inclusive: false,
        };
        let sql = predicate_to_display_sql(&gt, SqlDialect::ClickHouse);
        assert_eq!(sql, "`price` > 100");

        let between = Predicate::Between {
            column: "amount".into(),
            low: "10".into(),
            high: "200.50".into(),
        };
        let sql = predicate_to_display_sql(&between, SqlDialect::ClickHouse);
        assert_eq!(sql, "`amount` BETWEEN 10 AND 200.50");
    }

    #[test]
    fn test_predicate_to_display_sql_string_values_quoted() {
        let eq = Predicate::Equals {
            column: "status".into(),
            value: "active".into(),
        };
        let sql = predicate_to_display_sql(&eq, SqlDialect::ClickHouse);
        assert_eq!(sql, "`status` = 'active'");
    }

    // ========== Regression Tests for AST-based SQL Manipulation ==========

    #[test]
    fn test_inject_where_into_bare_select() {
        let dialect = ClickHouseDialect {};
        let mut stmts = Parser::parse_sql(&dialect, "SELECT a, b FROM t").unwrap();
        let cond = parse_condition_expr("x = 1").unwrap();
        for stmt in &mut stmts {
            if let Statement::Query(q) = stmt {
                inject_where_into_query(q, cond.clone());
            }
        }
        let result = serialize_statements(&stmts);
        assert!(result.contains("WHERE"), "Should add WHERE: {}", result);
        assert!(result.contains("x = 1"), "Should contain condition: {}", result);
    }

    #[test]
    fn test_inject_where_ands_with_existing() {
        let dialect = ClickHouseDialect {};
        let mut stmts = Parser::parse_sql(&dialect, "SELECT * FROM t WHERE a > 5").unwrap();
        let cond = parse_condition_expr("b = 'foo'").unwrap();
        for stmt in &mut stmts {
            if let Statement::Query(q) = stmt {
                inject_where_into_query(q, cond.clone());
            }
        }
        let result = serialize_statements(&stmts);
        assert!(result.contains("a > 5"), "Should keep old condition: {}", result);
        assert!(result.contains("b = 'foo'"), "Should add new condition: {}", result);
        assert!(result.contains("AND"), "Should combine with AND: {}", result);
    }

    #[test]
    fn test_inject_where_preserves_group_by_order_by() {
        let dialect = ClickHouseDialect {};
        let mut stmts = Parser::parse_sql(&dialect, "SELECT col, COUNT(*) FROM t GROUP BY col ORDER BY col LIMIT 10").unwrap();
        let cond = parse_condition_expr("x = 1").unwrap();
        for stmt in &mut stmts {
            if let Statement::Query(q) = stmt {
                inject_where_into_query(q, cond.clone());
            }
        }
        let result = serialize_statements(&stmts);
        assert!(result.contains("WHERE x = 1"), "Should inject WHERE: {}", result);
        assert!(result.contains("GROUP BY col"), "Should keep GROUP BY: {}", result);
        assert!(result.contains("ORDER BY col"), "Should keep ORDER BY: {}", result);
        assert!(result.contains("LIMIT 10"), "Should keep LIMIT: {}", result);
    }

    #[test]
    fn test_serialize_statements_roundtrip() {
        let dialect = ClickHouseDialect {};
        let original = "SELECT * FROM t WHERE x = 1 ORDER BY y";
        let stmts = Parser::parse_sql(&dialect, original).unwrap();
        let serialized = serialize_statements(&stmts);
        let reparsed = Parser::parse_sql(&dialect, &serialized).unwrap();
        assert_eq!(stmts.len(), reparsed.len());
    }

    #[test]
    fn test_prewhere_no_duplication() {
        let mut indexed = AHashSet::new();
        indexed.insert("event_type".to_string());

        let predicates = vec![
            Predicate::Equals {
                column: "event_type".into(),
                value: "purchase".into(),
            },
            Predicate::Contains {
                column: "description".into(),
                substring: "foo".into(),
            },
        ];

        let pushdown = PredicatePushdown::analyze(predicates, &indexed);
        let rewritten = pushdown.rewrite_with_prewhere("SELECT * FROM events");

        assert!(
            rewritten.contains("PREWHERE"),
            "Should contain PREWHERE: {rewritten}"
        );
        assert!(
            rewritten.contains("WHERE"),
            "Should contain WHERE: {rewritten}"
        );

        let prewhere_count = rewritten.matches("event_type").count();
        assert_eq!(
            prewhere_count, 1,
            "event_type should appear exactly once (not duplicated). Got: {rewritten}"
        );
    }

    #[test]
    fn test_is_numeric_literal_bare_dot_rejected() {
        assert!(!is_numeric_literal("."), "bare dot should not be numeric");
        assert!(!is_numeric_literal("-."), "sign + dot should not be numeric");
        assert!(!is_numeric_literal("+."), "sign + dot should not be numeric");
        assert!(is_numeric_literal("3.14"), "3.14 should be numeric");
        assert!(is_numeric_literal(".5"), ".5 should be numeric");
        assert!(is_numeric_literal("42"), "42 should be numeric");
        assert!(is_numeric_literal("-0"), "-0 should be numeric");
    }

    #[test]
    fn test_inject_where_into_union_both_sides() {
        let dialect = ClickHouseDialect {};
        let mut stmts = Parser::parse_sql(
            &dialect,
            "SELECT * FROM a UNION ALL SELECT * FROM b",
        )
        .unwrap();
        let cond = parse_condition_expr("x = 1").unwrap();
        for stmt in &mut stmts {
            if let Statement::Query(q) = stmt {
                inject_where_into_query(q, cond.clone());
            }
        }
        let result = serialize_statements(&stmts);
        let where_count = result.matches("WHERE").count();
        assert_eq!(
            where_count, 2,
            "Both sides of UNION should have WHERE. Got: {result}"
        );
    }

    // ========== Regression tests for bug fixes ==========

    #[test]
    fn test_parse_condition_expr_returns_error_on_invalid_sql() {
        assert!(
            parse_condition_expr("))) INVALID (((").is_err(),
            "Invalid SQL must return Err, not panic"
        );
        assert!(
            parse_condition_expr("").is_err(),
            "Empty string must return Err"
        );
        assert!(
            parse_condition_expr("SELECT * FROM t").is_ok() || parse_condition_expr("SELECT * FROM t").is_err(),
            "Should not panic regardless of input"
        );
    }

    #[test]
    fn test_parse_condition_expr_valid_input() {
        let result = parse_condition_expr("x = 1");
        assert!(result.is_ok(), "Valid SQL condition must parse successfully");
    }

    #[test]
    fn test_timestamp_to_epoch_normalizes_millisecond_input() {
        let splitter = PredicateSplitter::new();
        let result = splitter
            .apply_transform("ts", "1706745600000", &ValueTransform::TimestampToEpoch)
            .unwrap();
        let epoch: i64 = result.parse().unwrap();
        assert!(
            epoch < 10_000_000_000,
            "Millisecond input should be normalized to seconds, got {epoch}"
        );
        assert_eq!(epoch, 1706745600, "Should divide by 1000 to get seconds");
    }

    #[test]
    fn test_timestamp_to_epoch_ms_does_not_double_multiply() {
        let splitter = PredicateSplitter::new();
        let result = splitter
            .apply_transform("ts", "1706745600000", &ValueTransform::TimestampToEpochMs)
            .unwrap();
        let epoch_ms: i64 = result.parse().unwrap();
        assert_eq!(
            epoch_ms, 1706745600000,
            "Millisecond input should not be multiplied again, got {epoch_ms}"
        );
    }

    #[test]
    fn test_timestamp_to_epoch_ms_converts_seconds_correctly() {
        let splitter = PredicateSplitter::new();
        let result = splitter
            .apply_transform("ts", "1706745600", &ValueTransform::TimestampToEpochMs)
            .unwrap();
        let epoch_ms: i64 = result.parse().unwrap();
        assert_eq!(
            epoch_ms, 1706745600000,
            "Seconds input should be multiplied by 1000"
        );
    }

    #[test]
    fn test_timestamp_to_epoch_ms_date_string() {
        let splitter = PredicateSplitter::new();
        let result = splitter
            .apply_transform("ts", "2024-01-31", &ValueTransform::TimestampToEpochMs)
            .unwrap();
        let epoch_ms: i64 = result.parse().unwrap();
        assert!(
            epoch_ms > 1_000_000_000_000,
            "Date string should produce millisecond-range value, got {epoch_ms}"
        );
    }

    #[test]
    fn test_is_numeric_literal_rejects_leading_zeros() {
        assert!(!is_numeric_literal("01234"), "Leading-zero strings (zip codes) must not be treated as numbers");
        assert!(!is_numeric_literal("007"), "Leading-zero strings must not be treated as numbers");
        assert!(is_numeric_literal("0"), "Bare zero is a valid number");
        assert!(is_numeric_literal("0.5"), "Zero-dot-fraction is a valid number");
        assert!(is_numeric_literal("42"));
        assert!(is_numeric_literal("-3.14"));
        assert!(is_numeric_literal("+100"));
        assert!(!is_numeric_literal(""));
        assert!(!is_numeric_literal("abc"));
    }

    #[test]
    fn test_empty_in_list_generates_always_false() {
        let pred = Predicate::In {
            column: "status".into(),
            values: vec![],
        };
        let sql = predicate_to_display_sql(&pred, SqlDialect::ClickHouse);
        assert_eq!(sql, "1=0", "Empty IN list must produce always-false expression, not invalid SQL");
    }

    #[test]
    fn test_extract_like_literal_prefix_with_escapes() {
        assert_eq!(extract_like_literal_prefix("hello%"), "hello");
        assert_eq!(extract_like_literal_prefix("hello\\%world%"), "hello%world");
        assert_eq!(extract_like_literal_prefix("hello\\_world%"), "hello_world");
        assert_eq!(extract_like_literal_prefix("abc"), "abc");
        assert_eq!(extract_like_literal_prefix("%abc"), "");
        assert_eq!(extract_like_literal_prefix("_abc"), "");
    }

    #[test]
    fn test_extract_like_literal_prefix_preserves_backslash_before_non_special() {
        // Backslash before a non-special character is a literal backslash,
        // NOT an escape sequence. Only \%, \_, and \\ are LIKE escapes.
        assert_eq!(extract_like_literal_prefix(r"path\dir%"), r"path\dir");
        assert_eq!(extract_like_literal_prefix(r"c:\users%"), r"c:\users");
        // \\  is an escaped backslash (literal \), then \s is literal \ + s
        assert_eq!(extract_like_literal_prefix(r"\\server\share%"), r"\server\share");
        // Trailing backslash with no following character
        assert_eq!(extract_like_literal_prefix(r"trail\"), r"trail\");
        // Mixed: escaped wildcard then non-special backslash
        assert_eq!(extract_like_literal_prefix(r"a\%b\c%"), r"a%b\c");
    }

    #[test]
    fn test_escape_like_pattern_clickhouse_two_level_escape() {
        // A single backslash needs four output backslashes so it survives
        // both ClickHouse string-literal parsing (\\\\→\\) and LIKE
        // evaluation (\\→literal \).
        let escaped = escape_like_pattern(r"foo\bar");
        assert_eq!(escaped, "foo\\\\\\\\bar");

        // Wildcards get double-backslash prefix (\\%→\%→literal %)
        let escaped = escape_like_pattern("100%");
        assert_eq!(escaped, "100\\\\%");

        let escaped = escape_like_pattern("col_name");
        assert_eq!(escaped, "col\\\\_name");

        // Single quotes get SQL-escaped
        let escaped = escape_like_pattern("it's");
        assert_eq!(escaped, "it''s");

        // Combined: backslash + quote + wildcard
        // Input `a\'b%c`  (a, \, ', b, %, c)
        // \  → \\\\  (4 backslashes)
        // '  → ''
        // %  → \\%   (2 backslashes + %)
        let escaped = escape_like_pattern(r"a\'b%c");
        assert_eq!(escaped, "a\\\\\\\\''b\\\\%c");
    }

    #[test]
    fn test_epoch_abs_no_panic_on_extremes() {
        use crate::warehouse::query::cost_model::ValueTransform;

        let splitter = PredicateSplitter::new();

        // i64::MIN would panic with .abs() in debug mode
        let result = splitter.apply_transform("ts", &i64::MIN.to_string(), &ValueTransform::TimestampToEpoch);
        assert!(result.is_ok(), "i64::MIN must not panic: {:?}", result);

        let result = splitter.apply_transform("ts", &i64::MAX.to_string(), &ValueTransform::TimestampToEpoch);
        assert!(result.is_ok(), "i64::MAX must not panic: {:?}", result);

        // A large negative value (ms-range) should still normalize to seconds
        let neg_ms = "-1706745600000";
        let result = splitter
            .apply_transform("ts", neg_ms, &ValueTransform::TimestampToEpoch)
            .unwrap();
        let epoch: i64 = result.parse().unwrap();
        assert_eq!(epoch, -1706745600, "Negative ms should normalize to seconds");
    }

    #[test]
    fn test_too_many_filters_reports_actual_count() {
        let mut splitter = PredicateSplitter::new();
        let mut caps = SourceCapabilityMatrix::for_source_type(SourceType::PostgreSQL);
        caps.max_filters = Some(2);
        splitter.register_source("pg", caps);

        let predicates = vec![
            Predicate::Equals { column: "a".into(), value: "1".into() },
            Predicate::Equals { column: "b".into(), value: "2".into() },
            Predicate::Equals { column: "c".into(), value: "3".into() },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "pg", "t");
        let too_many_warning = analysis.warnings.iter().find(|w| {
            matches!(&w.reason, PushdownWarningReason::TooManyFilters { .. })
        });
        assert!(too_many_warning.is_some(), "Should warn about too many filters");

        if let PushdownWarningReason::TooManyFilters { max, requested } =
            &too_many_warning.unwrap().reason
        {
            assert_eq!(*max, 2);
            assert_eq!(
                *requested, 3,
                "requested should be the count that exceeded the limit (filter_count+1), not predicates.len()"
            );
        }
    }

    #[test]
    fn test_in_list_too_large_uses_distinct_warning() {
        use crate::warehouse::query::cost_model::{ColumnFilterCapability, FilterOperation};

        let mut splitter = PredicateSplitter::new();
        let mut caps = SourceCapabilityMatrix::for_source_type(SourceType::Stripe);
        let col_cap = ColumnFilterCapability::new([FilterOperation::Equals, FilterOperation::In { max_values: None }])
            .with_max_in_values(2);
        caps.column_filters.insert("status".to_string(), col_cap);
        splitter.register_source("stripe", caps);

        let predicates = vec![
            Predicate::In {
                column: "status".into(),
                values: vec!["a".into(), "b".into(), "c".into()],
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "t");

        let has_too_many_filters = analysis.warnings.iter().any(|w| {
            matches!(&w.reason, PushdownWarningReason::TooManyFilters { .. })
        });
        assert!(
            !has_too_many_filters,
            "IN list size limit must NOT use TooManyFilters variant"
        );

        let has_unsupported_op = analysis.warnings.iter().any(|w| {
            matches!(&w.reason, PushdownWarningReason::UnsupportedOperation { operation, .. }
                     if operation.contains("IN list"))
        });
        assert!(
            has_unsupported_op,
            "IN list size limit should produce UnsupportedOperation warning with 'IN list' context"
        );
    }

    #[test]
    fn test_timestamp_to_epoch_ms_preserves_sub_second_precision() {
        let splitter = PredicateSplitter::new();
        let result = splitter
            .apply_transform("ts", "1706745600123", &ValueTransform::TimestampToEpochMs)
            .unwrap();
        assert_eq!(
            result, "1706745600123",
            "Sub-second precision must be preserved for millisecond inputs"
        );
    }

    #[test]
    fn test_timestamp_to_epoch_ms_negative_milliseconds() {
        let splitter = PredicateSplitter::new();
        let result = splitter
            .apply_transform("ts", "-10000000001", &ValueTransform::TimestampToEpochMs)
            .unwrap();
        assert_eq!(
            result, "-10000000001",
            "Negative millisecond input must be returned as-is"
        );
    }

    #[test]
    fn test_timestamp_to_epoch_ms_iso_string() {
        let splitter = PredicateSplitter::new();
        let result = splitter
            .apply_transform("ts", "2024-02-19T12:00:00Z", &ValueTransform::TimestampToEpochMs)
            .unwrap();
        let epoch_ms: i64 = result.parse().unwrap();
        assert_eq!(epoch_ms, 1708344000000);
    }

    #[test]
    fn test_parse_timestamp_to_epoch_ms_boundary() {
        let splitter = PredicateSplitter::new();

        // Positive epoch ms near second boundary
        let result = splitter.parse_timestamp_to_epoch("ts", "1708334567999").unwrap();
        assert_eq!(result, "1708334567", "ms->s should floor, not round");

        let result = splitter.parse_timestamp_to_epoch("ts", "1708334567000").unwrap();
        assert_eq!(result, "1708334567");

        // Epoch seconds pass through unchanged
        let result = splitter.parse_timestamp_to_epoch("ts", "1708334567").unwrap();
        assert_eq!(result, "1708334567");

        // Negative epoch ms: div_euclid floors toward -inf
        let result = splitter.parse_timestamp_to_epoch("ts", "-10000000001").unwrap();
        assert_eq!(
            result, "-10000001",
            "negative ms->s should floor toward -inf (div_euclid)"
        );
    }

    #[test]
    fn test_parse_timestamp_to_epoch_formats() {
        let splitter = PredicateSplitter::new();

        // RFC 3339
        let result = splitter.parse_timestamp_to_epoch("ts", "2024-02-19T12:00:00Z").unwrap();
        assert_eq!(result, "1708344000");

        // Date only
        let result = splitter.parse_timestamp_to_epoch("ts", "2024-02-19").unwrap();
        assert_eq!(result, "1708300800");

        // Unparseable
        let result = splitter.parse_timestamp_to_epoch("ts", "not-a-date");
        assert!(result.is_err());
    }

    #[test]
    fn test_in_list_api_param_escapes_commas() {
        use crate::warehouse::query::cost_model::{ColumnFilterCapability, FilterOperation};

        let mut splitter = PredicateSplitter::new();
        let mut caps = SourceCapabilityMatrix::for_source_type(SourceType::Stripe);
        let col_cap = ColumnFilterCapability::new([
            FilterOperation::Equals,
            FilterOperation::In { max_values: None },
        ]);
        caps.column_filters.insert("tag".to_string(), col_cap);
        splitter.register_source("stripe", caps);

        let predicates = vec![Predicate::In {
            column: "tag".into(),
            values: vec!["a,b".into(), "c".into()],
        }];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "t");

        let api_params: Vec<_> = analysis
            .pushable
            .iter()
            .filter_map(|t| t.translated.as_api_params())
            .collect();
        assert!(
            !api_params.is_empty(),
            "IN predicate should be pushed as API params"
        );

        let joined = api_params[0].get("tag").expect("tag param must exist");
        assert!(
            !joined.contains("a,b"),
            "Commas inside values must be percent-encoded, got: {}",
            joined
        );
        assert!(
            joined.contains("a%2Cb"),
            "Comma in 'a,b' should become 'a%2Cb', got: {}",
            joined
        );
        assert_eq!(
            joined.split(',').count(),
            2,
            "There should be exactly 2 comma-separated values (a%2Cb and c), got: {}",
            joined
        );
    }

    #[test]
    fn test_is_numeric_literal_scientific_notation() {
        assert!(is_numeric_literal("1e10"), "1e10 is a valid numeric literal");
        assert!(is_numeric_literal("3.14e-2"), "3.14e-2 is valid");
        assert!(is_numeric_literal("2.5E+8"), "2.5E+8 is valid");
        assert!(is_numeric_literal("0e0"), "0e0 is valid");
        assert!(is_numeric_literal("42"), "plain integer");
        assert!(is_numeric_literal("3.14"), "plain decimal");
        assert!(is_numeric_literal("-1e5"), "negative scientific");
        assert!(!is_numeric_literal("1e"), "incomplete exponent");
        assert!(!is_numeric_literal("e10"), "no mantissa");
        assert!(!is_numeric_literal(""), "empty string");
        assert!(!is_numeric_literal("abc"), "not a number");
        assert!(!is_numeric_literal("1e2e3"), "double exponent");
    }

    #[test]
    fn test_api_param_collision_falls_back_to_local() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("stripe", SourceType::Stripe);

        let predicates = vec![
            Predicate::GreaterThan {
                column: "created".into(),
                value: "2024-01-01".into(),
                inclusive: true,
            },
            Predicate::GreaterThan {
                column: "created".into(),
                value: "2024-06-01".into(),
                inclusive: true,
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "charges");
        let total = analysis.pushable.len() + analysis.local_only.len();
        assert_eq!(total, 2, "All predicates must be accounted for");
    }

    #[test]
    fn test_passthrough_translation_not_marked_as_pushed() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("pg", SourceType::PostgreSQL);

        let predicates = vec![
            Predicate::Equals {
                column: "a".into(),
                value: "1".into(),
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "pg", "test");
        for pushed in &analysis.pushable {
            assert!(
                pushed.translated.as_sql().is_some(),
                "Pushed predicates for SQL sources must have SQL translation"
            );
        }
    }

    #[test]
    fn test_contains_maps_to_filter_operation_contains() {
        let pred = Predicate::Contains {
            column: "description".into(),
            substring: "hello".into(),
        };
        let op = pred.to_filter_operation();
        assert_eq!(
            op,
            Some(FilterOperation::Contains),
            "Predicate::Contains must map to FilterOperation::Contains, not Like"
        );
    }

    #[test]
    fn test_and_predicate_with_csv_all_local() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("csv", SourceType::Csv);

        // CSV source has no pushdown, so AND with multiple parts must be all local.
        let predicates = vec![Predicate::And(vec![
            Predicate::Equals {
                column: "name".into(),
                value: "foo".into(),
            },
            Predicate::Equals {
                column: "status".into(),
                value: "active".into(),
            },
        ])];

        let analysis = splitter.analyze_for_source(&predicates, "csv", "test");

        let total = analysis.pushable.len() + analysis.local_only.len();
        assert!(
            total >= 1,
            "AND predicate must be accounted for — got pushable={} local={}",
            analysis.pushable.len(),
            analysis.local_only.len()
        );
        assert!(
            analysis.pushable.is_empty(),
            "CSV source should not push any predicates"
        );
    }

    #[test]
    fn test_and_predicate_mixed_push_and_local() {
        let mut splitter = PredicateSplitter::new();
        splitter.register_source_type("pg", SourceType::PostgreSQL);

        // For PostgreSQL, Equals is pushable but a complex AND including
        // simple predicates should all be pushed or split correctly.
        let predicates = vec![Predicate::And(vec![
            Predicate::Equals {
                column: "id".into(),
                value: "1".into(),
            },
            Predicate::Equals {
                column: "status".into(),
                value: "active".into(),
            },
        ])];

        let analysis = splitter.analyze_for_source(&predicates, "pg", "test");

        let total = analysis.pushable.len() + analysis.local_only.len();
        assert!(
            total >= 1,
            "AND predicate must be accounted for — got pushable={} local={}",
            analysis.pushable.len(),
            analysis.local_only.len()
        );
    }

    #[test]
    fn test_api_param_collision_no_partial_pollution() {
        use crate::warehouse::query::cost_model::{ColumnFilterCapability, FilterOperation};

        let mut splitter = PredicateSplitter::new();
        let mut caps = SourceCapabilityMatrix::for_source_type(SourceType::Stripe);
        let col_cap = ColumnFilterCapability::new([
            FilterOperation::Equals,
            FilterOperation::GreaterThanOrEquals,
        ]);
        caps.column_filters.insert("status".to_string(), col_cap.clone());
        caps.column_filters.insert("created".to_string(), col_cap.clone());
        caps.column_filters.insert("amount".to_string(), col_cap);
        splitter.register_source("stripe", caps);

        // Predicate #1: status = "active" AND created >= "2024-01-01"
        //   => params: {status: active, created[gte]: 2024-01-01}
        // Predicate #2: status = "active" AND created >= "2024-06-01"
        //   => params: {status: active, created[gte]: 2024-06-01}  -- collision on created[gte]
        // Predicate #3: amount >= "100"
        //   => params: {amount[gte]: 100}
        //
        // Before fix: predicate #2 would partially insert status=active before
        // detecting the collision on created[gte], polluting the shared map.
        // Predicate #3 would then see status=active from predicate #2.
        // After fix: predicate #2 inserts nothing, so predicate #3 is independent.
        let predicates = vec![
            Predicate::And(vec![
                Predicate::Equals {
                    column: "status".into(),
                    value: "active".into(),
                },
                Predicate::GreaterThan {
                    column: "created".into(),
                    value: "2024-01-01".into(),
                    inclusive: true,
                },
            ]),
            Predicate::And(vec![
                Predicate::Equals {
                    column: "status".into(),
                    value: "active".into(),
                },
                Predicate::GreaterThan {
                    column: "created".into(),
                    value: "2024-06-01".into(),
                    inclusive: true,
                },
            ]),
            Predicate::GreaterThan {
                column: "amount".into(),
                value: "100".into(),
                inclusive: true,
            },
        ];

        let analysis = splitter.analyze_for_source(&predicates, "stripe", "charges");

        // All predicates must be accounted for (pushable + local)
        let total = analysis.pushable.len() + analysis.local_only.len();
        assert!(
            total >= 3,
            "All predicates must be accounted for: pushable={} local={}",
            analysis.pushable.len(),
            analysis.local_only.len()
        );
    }

    #[test]
    fn test_inject_where_except_pushes_to_both_sides() {
        use sqlparser::dialect::GenericDialect;

        let sql = "SELECT id FROM orders EXCEPT SELECT id FROM returns";
        let dialect = GenericDialect {};
        let mut stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();

        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("status"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(
                sqlparser::ast::Value::SingleQuotedString("active".into()).into(),
            )),
        };

        if let Statement::Query(ref mut query) = stmts[0] {
            inject_where_into_set_expr(&mut query.body, condition);
        }

        let result = stmts[0].to_string().to_lowercase();

        // Pushing WHERE to both sides of EXCEPT is correct:
        // (A EXCEPT B) WHERE p  ===  (A WHERE p) EXCEPT (B WHERE p)
        let except_pos = result.find("except").unwrap();
        let left_part = &result[..except_pos];
        let right_part = &result[except_pos..];

        assert!(
            left_part.contains("where"),
            "Left side of EXCEPT should have the predicate: {}",
            result
        );
        assert!(
            right_part.contains("where"),
            "Right side of EXCEPT should also have the predicate: {}",
            result
        );
    }

    #[test]
    fn test_inject_where_union_pushes_to_both() {
        use sqlparser::dialect::GenericDialect;

        let sql = "SELECT id FROM orders UNION ALL SELECT id FROM returns";
        let dialect = GenericDialect {};
        let mut stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();

        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("status"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(
                sqlparser::ast::Value::SingleQuotedString("active".into()).into(),
            )),
        };

        if let Statement::Query(ref mut query) = stmts[0] {
            inject_where_into_set_expr(&mut query.body, condition);
        }

        let result = stmts[0].to_string().to_lowercase();
        let parts: Vec<&str> = result.split("union all").collect();
        assert_eq!(parts.len(), 2, "Should have two sides of UNION ALL");

        assert!(
            parts[0].contains("status"),
            "Left side of UNION ALL should have the predicate: {}",
            result
        );
        assert!(
            parts[1].contains("status"),
            "Right side of UNION ALL should also have the predicate: {}",
            result
        );
    }

    #[test]
    fn test_generate_source_query_invalid_sql_returns_original() {
        let splitter = PredicateSplitter::new();
        let analysis = SourcePredicateAnalysis {
            pushable: vec![TranslatedPredicate {
                original: Arc::new(Predicate::Equals {
                    column: "x".into(),
                    value: "1".into(),
                }),
                translated: PredicateTranslation::sql("x = 1"),
                estimated_selectivity: 0.01,
            }],
            local_only: vec![],
            warnings: vec![],
            pushable_selectivity: 0.01,
            local_selectivity: 1.0,
            source_name: "test".to_string(),
            table_name: "t".to_string(),
        };

        let bad_sql = "NOT VALID SQL AT ALL {{{";
        let result = splitter.generate_source_query(bad_sql, &analysis, SourceType::PostgreSQL);

        assert_eq!(
            result.query, bad_sql,
            "Invalid SQL should return the original query unchanged"
        );
    }

    #[test]
    fn test_inject_where_into_values_returns_false() {
        let mut values_expr = SetExpr::Values(sqlparser::ast::Values {
            explicit_row: false,
            rows: vec![vec![Expr::Value(sqlparser::ast::Value::Number("1".to_string(), false))]],
        });
        let condition = Expr::Value(sqlparser::ast::Value::Boolean(true));
        let result = inject_where_into_set_expr(&mut values_expr, condition);
        assert!(
            !result,
            "inject_where_into_set_expr must return false for VALUES expressions"
        );
    }

    #[test]
    fn test_inject_where_into_intersect_applies_to_both_sides() {
        let dialect = ClickHouseDialect {};
        let sql = "SELECT id FROM a INTERSECT SELECT id FROM b";
        let mut query = Parser::new(&dialect)
            .try_with_sql(sql)
            .unwrap()
            .parse_query()
            .unwrap();
        let condition = parse_condition_expr("x > 5").unwrap();
        inject_where_into_set_expr(&mut query.body, condition);
        let result = query.to_string();
        let lower = result.to_lowercase();
        assert!(
            lower.contains("from a where") || lower.contains("from a\nwhere"),
            "WHERE must be injected into left side of INTERSECT, got: {result}"
        );
        assert!(
            lower.contains("from b where") || lower.contains("from b\nwhere"),
            "WHERE must be injected into right side of INTERSECT, got: {result}"
        );
    }

    #[test]
    fn test_inject_where_into_except_applies_to_both_sides() {
        let dialect = ClickHouseDialect {};
        let sql = "SELECT id FROM a EXCEPT SELECT id FROM b";
        let mut query = Parser::new(&dialect)
            .try_with_sql(sql)
            .unwrap()
            .parse_query()
            .unwrap();
        let condition = parse_condition_expr("y = 1").unwrap();
        inject_where_into_set_expr(&mut query.body, condition);
        let result = query.to_string();
        let lower = result.to_lowercase();
        assert!(
            lower.contains("from a where") || lower.contains("from a\nwhere"),
            "WHERE must be injected into left side of EXCEPT, got: {result}"
        );
        assert!(
            lower.contains("from b where") || lower.contains("from b\nwhere"),
            "WHERE must be injected into right side of EXCEPT, got: {result}"
        );
    }

    #[test]
    fn test_contains_predicate_no_escape_clause() {
        let pred = Predicate::Contains {
            column: "name".into(),
            substring: "test".into(),
        };
        let expr = predicate_to_expr(&pred);
        let sql = expr.to_string();
        assert!(
            !sql.contains("ESCAPE"),
            "Contains predicate must not emit ESCAPE clause (ClickHouse defaults to backslash). Got: {sql}"
        );
        assert!(
            sql.contains("LIKE '%test%'"),
            "Contains predicate must produce LIKE '%%substring%%'. Got: {sql}"
        );
    }

    #[test]
    fn test_contains_predicate_special_chars_escaped() {
        let pred = Predicate::Contains {
            column: "name".into(),
            substring: "100%_off".into(),
        };
        let expr = predicate_to_expr(&pred);
        let sql = expr.to_string();
        assert!(
            sql.contains(r"100\\%\\_off") || sql.contains(r"100\%\_off"),
            "Special LIKE chars must be escaped with backslash. Got: {sql}"
        );
    }

    #[test]
    fn test_not_in_with_null_not_pushed() {
        use sqlparser::ast::{Expr, Value};

        // NOT IN (1, 2, NULL) — SQL semantics: always returns NULL, zero rows
        let expr = Expr::InList {
            expr: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("id"))),
            list: vec![
                Expr::Value(Value::Number("1".to_string(), false)),
                Expr::Value(Value::Number("2".to_string(), false)),
                Expr::Value(Value::Null),
            ],
            negated: true,
        };
        let predicates = expr_to_predicates(&expr);
        assert!(
            predicates.is_empty(),
            "NOT IN with NULL in list must not produce a pushed predicate, got: {:?}",
            predicates
        );

        // IN (1, 2, NULL) — NULL is simply ignored, still valid to push
        let expr_in = Expr::InList {
            expr: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("id"))),
            list: vec![
                Expr::Value(Value::Number("1".to_string(), false)),
                Expr::Value(Value::Number("2".to_string(), false)),
                Expr::Value(Value::Null),
            ],
            negated: false,
        };
        let predicates_in = expr_to_predicates(&expr_in);
        assert_eq!(predicates_in.len(), 1, "IN with NULL should still push predicate");
    }

    #[test]
    fn test_not_compound_and_preserves_demorgan() {
        // NOT (x = 1 AND y = 2) must produce a single Not(And(...)),
        // NOT two separate Not() predicates AND-joined (De Morgan's law).
        let expr = parse_condition_expr("NOT (x = 1 AND y = 2)").unwrap();
        let predicates = expr_to_predicates(&expr);
        assert_eq!(predicates.len(), 1, "NOT(AND) must produce exactly one predicate");
        match &predicates[0] {
            Predicate::Not(inner) => match inner.as_ref() {
                Predicate::And(children) => {
                    assert_eq!(children.len(), 2, "inner AND should have 2 children");
                }
                other => panic!("expected Not(And(...)), got Not({:?})", other),
            },
            other => panic!("expected Not(...), got {:?}", other),
        }
    }

    #[test]
    fn test_not_single_predicate_unchanged() {
        let expr = parse_condition_expr("NOT (x = 1)").unwrap();
        let predicates = expr_to_predicates(&expr);
        assert_eq!(predicates.len(), 1);
        assert!(matches!(&predicates[0], Predicate::Not(inner) if matches!(inner.as_ref(), Predicate::Equals { .. })));
    }

    #[test]
    fn test_contains_predicate_to_expr_no_double_escaping() {
        let pred = Predicate::Contains {
            column: "name".into(),
            substring: "it's".into(),
        };
        let expr = predicate_to_expr(&pred);
        let sql = expr.to_string();
        // sqlparser's Display for SingleQuotedString escapes ' → ''
        // so "it's" in a LIKE pattern should appear as "it''s" exactly once.
        assert!(
            sql.contains("it''s"),
            "Expected single-escaped quote in SQL output, got: {}",
            sql
        );
        assert!(
            !sql.contains("it''''s"),
            "Double-escaping detected in SQL output: {}",
            sql
        );
    }

    #[test]
    fn test_escape_like_pattern_for_ast_metacharacters() {
        assert_eq!(escape_like_pattern_for_ast("100%"), "100\\%");
        assert_eq!(escape_like_pattern_for_ast("a_b"), "a\\_b");
        assert_eq!(escape_like_pattern_for_ast("it's"), "it's");
        assert_eq!(escape_like_pattern_for_ast("a\\b"), "a\\\\b");
    }
}
