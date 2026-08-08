//! SQL to MongoDB Filter Translation
//!
//! Translates SQL WHERE clause predicates to MongoDB query documents
//! for predicate pushdown optimization.

use bson::{doc, Bson, Document};
use sqlparser::ast::{BinaryOperator, Expr, Value};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

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
}

/// A SQL predicate that can be pushed down to MongoDB.
#[derive(Debug, Clone)]
pub struct SqlPredicate {
    /// Column name (may include flattened path like "user__address__city")
    pub column: String,
    /// Comparison operator
    pub operator: SqlOperator,
    /// Value(s) for comparison
    pub value: PredicateValue,
}

/// Value for a SQL predicate.
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<PredicateValue>),
}

impl PredicateValue {
    /// Convert to BSON value.
    fn to_bson(&self) -> Bson {
        match self {
            PredicateValue::Null => Bson::Null,
            PredicateValue::Bool(b) => Bson::Boolean(*b),
            PredicateValue::Int(i) => Bson::Int64(*i),
            PredicateValue::Float(f) => Bson::Double(*f),
            PredicateValue::String(s) => Bson::String(s.clone()),
            PredicateValue::List(items) => {
                Bson::Array(items.iter().map(|v| v.to_bson()).collect())
            }
        }
    }
}

/// Translator for converting SQL predicates to MongoDB query documents.
pub struct MongoFilterTranslator {
    /// Field separator used in flattened field names
    field_separator: String,
}

impl Default for MongoFilterTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl MongoFilterTranslator {
    /// Create a new filter translator.
    pub fn new() -> Self {
        Self {
            field_separator: "__".to_string(),
        }
    }

    /// Create a translator with a custom field separator.
    pub fn with_separator(separator: impl Into<String>) -> Self {
        Self {
            field_separator: separator.into(),
        }
    }

    /// Translate a list of SQL predicates to a MongoDB query document.
    ///
    /// Predicates are combined with AND logic.
    pub fn translate(&self, predicates: &[SqlPredicate]) -> ConnectorResult<Document> {
        if predicates.is_empty() {
            return Ok(doc! {});
        }

        let mut conditions = Vec::new();

        for predicate in predicates {
            let condition = self.translate_predicate(predicate)?;
            conditions.push(condition);
        }

        if conditions.len() == 1 {
            Ok(conditions.into_iter().next().unwrap())
        } else {
            Ok(doc! { "$and": conditions })
        }
    }

    /// Translate a single predicate to a MongoDB query condition.
    fn translate_predicate(&self, predicate: &SqlPredicate) -> ConnectorResult<Document> {
        let field_path = self.convert_field_path(&predicate.column);

        match &predicate.operator {
            SqlOperator::Eq => {
                let bson_value = predicate.value.to_bson();
                Ok(doc! { &field_path: bson_value })
            }
            SqlOperator::NotEq => {
                let bson_value = predicate.value.to_bson();
                Ok(doc! { &field_path: { "$ne": bson_value } })
            }
            SqlOperator::Lt => {
                let bson_value = predicate.value.to_bson();
                Ok(doc! { &field_path: { "$lt": bson_value } })
            }
            SqlOperator::LtEq => {
                let bson_value = predicate.value.to_bson();
                Ok(doc! { &field_path: { "$lte": bson_value } })
            }
            SqlOperator::Gt => {
                let bson_value = predicate.value.to_bson();
                Ok(doc! { &field_path: { "$gt": bson_value } })
            }
            SqlOperator::GtEq => {
                let bson_value = predicate.value.to_bson();
                Ok(doc! { &field_path: { "$gte": bson_value } })
            }
            SqlOperator::Like => {
                let pattern = self.like_to_regex(&predicate.value)?;
                Ok(doc! { &field_path: Bson::RegularExpression(bson::Regex { pattern, options: String::new() }) })
            }
            SqlOperator::NotLike => {
                let pattern = self.like_to_regex(&predicate.value)?;
                Ok(doc! { &field_path: { "$not": Bson::RegularExpression(bson::Regex { pattern, options: String::new() }) } })
            }
            SqlOperator::In => {
                if let PredicateValue::List(items) = &predicate.value {
                    let values: Vec<Bson> = items.iter().map(|v| v.to_bson()).collect();
                    Ok(doc! { &field_path: { "$in": values } })
                } else {
                    Err(ConnectorError::Validation(
                        "IN operator requires a list value".to_string(),
                    ))
                }
            }
            SqlOperator::NotIn => {
                if let PredicateValue::List(items) = &predicate.value {
                    let values: Vec<Bson> = items.iter().map(|v| v.to_bson()).collect();
                    Ok(doc! { &field_path: { "$nin": values } })
                } else {
                    Err(ConnectorError::Validation(
                        "NOT IN operator requires a list value".to_string(),
                    ))
                }
            }
            SqlOperator::IsNull => {
                Ok(doc! { &field_path: { "$eq": Bson::Null } })
            }
            SqlOperator::IsNotNull => {
                Ok(doc! { &field_path: { "$ne": Bson::Null } })
            }
        }
    }

    /// Convert a flattened field path back to MongoDB dot notation.
    ///
    /// e.g., "user__address__city" -> "user.address.city"
    fn convert_field_path(&self, flattened: &str) -> String {
        flattened.replace(&self.field_separator, ".")
    }

    /// Convert a SQL LIKE pattern to a MongoDB regex pattern.
    ///
    /// Handles LIKE escape sequences: `\%` -> literal `%`, `\_` -> literal `_`,
    /// `\\` -> literal `\`. Unescaped `%` becomes `.*` and `_` becomes `.`.
    fn like_to_regex(&self, value: &PredicateValue) -> ConnectorResult<String> {
        let pattern = match value {
            PredicateValue::String(s) => s.clone(),
            _ => {
                return Err(ConnectorError::Validation(
                    "LIKE pattern must be a string".to_string(),
                ))
            }
        };

        const MULTI_WILDCARD: &str = "\x00MULTI\x00";
        const SINGLE_WILDCARD: &str = "\x00SINGLE\x00";

        // Process escape sequences character by character before
        // replacing wildcards. `\%`, `\_`, `\\` are treated as literals.
        let mut processed = String::with_capacity(pattern.len());
        let mut chars = pattern.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                if let Some(&next) = chars.peek() {
                    if next == '%' || next == '_' || next == '\\' {
                        processed.push(next);
                        chars.next();
                        continue;
                    }
                }
            }
            match ch {
                '%' => processed.push_str(MULTI_WILDCARD),
                '_' => processed.push_str(SINGLE_WILDCARD),
                _ => processed.push(ch),
            }
        }

        let escaped = regex::escape(&processed);

        let regex_pattern = escaped
            .replace(MULTI_WILDCARD, ".*")
            .replace(SINGLE_WILDCARD, ".");

        let starts_wild = has_unescaped_leading_wildcard(&pattern);
        let ends_wild = has_unescaped_trailing_wildcard(&pattern);

        let anchored = if starts_wild && ends_wild {
            regex_pattern
        } else if starts_wild {
            format!("{}$", regex_pattern)
        } else if ends_wild {
            format!("^{}", regex_pattern)
        } else {
            format!("^{}$", regex_pattern)
        };

        Ok(anchored)
    }

    /// Build a MongoDB filter for fetching documents by IDs.
    ///
    /// Used for semi-join optimization where we fetch only documents
    /// whose IDs match join keys from another table.
    pub fn build_id_filter(&self, ids: &[String]) -> Document {
        let values: Vec<Bson> = ids
            .iter()
            .map(|id| {
                bson::oid::ObjectId::parse_str(id)
                    .map(Bson::ObjectId)
                    .unwrap_or_else(|_| Bson::String(id.clone()))
            })
            .collect();

        doc! { "_id": { "$in": values } }
    }

    /// Build a MongoDB filter for semi-join optimization.
    ///
    /// Creates an $in filter using join keys from the smaller table.
    pub fn build_semi_join_filter(
        &self,
        join_column: &str,
        join_values: &[PredicateValue],
        max_values: usize,
    ) -> ConnectorResult<Option<Document>> {
        if join_values.is_empty() {
            return Ok(None);
        }

        if join_values.len() > max_values {
            // Too many values for efficient $in query
            return Ok(None);
        }

        let field_path = self.convert_field_path(join_column);
        let bson_values: Vec<Bson> = join_values.iter().map(|v| v.to_bson()).collect();

        Ok(Some(doc! { &field_path: { "$in": bson_values } }))
    }
}

/// Result of parsing a SQL WHERE clause into pushable predicates.
#[derive(Debug)]
pub struct ParsedWhereClause {
    /// Predicates that can be pushed down to MongoDB.
    pub predicates: Vec<SqlPredicate>,
    /// `true` when the WHERE clause contained OR conditions that could not be
    /// translated.  Callers must NOT treat an empty `predicates` vec as "no
    /// filter needed" when this flag is set -- the original query still has
    /// filtering intent that must be applied elsewhere.
    pub has_unsupported_or: bool,
}

/// Parse a simple SQL WHERE clause into predicates.
///
/// This is a simplified parser for common patterns. For complex queries,
/// use the full SQL parser from sqlparser crate.
pub fn parse_simple_where_clause(sql_where: &str) -> ParsedWhereClause {
    let dialect = ClickHouseDialect {};
    let expr = match Parser::new(&dialect)
        .try_with_sql(sql_where.trim())
        .and_then(|mut p| p.parse_expr())
    {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(
                sql_where = %sql_where.trim(),
                error = %e,
                "Failed to parse WHERE clause for MongoDB filter pushdown; falling back to full scan"
            );
            return ParsedWhereClause { predicates: Vec::new(), has_unsupported_or: true };
        }
    };

    let mut predicates = Vec::new();
    let mut has_unsupported_or = false;
    collect_predicates_from_expr(&expr, &mut predicates, &mut has_unsupported_or);
    ParsedWhereClause { predicates, has_unsupported_or }
}

fn collect_predicates_from_expr(expr: &Expr, out: &mut Vec<SqlPredicate>, has_or: &mut bool) {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            collect_predicates_from_expr(left, out, has_or);
            collect_predicates_from_expr(right, out, has_or);
        }
        Expr::BinaryOp { op: BinaryOperator::Or, .. } => {
            *has_or = true;
            tracing::warn!(
                expr = %expr,
                "OR conditions in MongoDB filter are not supported and will be skipped"
            );
        }
        Expr::Nested(inner) => collect_predicates_from_expr(inner, out, has_or),
        other => {
            if let Some(pred) = expr_to_sql_predicate(other) {
                out.push(pred);
            }
        }
    }
}

fn expr_to_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|i| i.value.as_str())
            .collect::<Vec<_>>()
            .join("__"),
        other => other.to_string(),
    }
}

fn sql_value_to_predicate_value(val: &Value) -> PredicateValue {
    match val {
        Value::Null => PredicateValue::Null,
        Value::Boolean(b) => PredicateValue::Bool(*b),
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                PredicateValue::Int(i)
            } else if let Ok(f) = n.parse::<f64>() {
                PredicateValue::Float(f)
            } else {
                PredicateValue::String(n.clone())
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            PredicateValue::String(s.clone())
        }
        other => PredicateValue::String(other.to_string()),
    }
}

fn expr_to_predicate_value(expr: &Expr) -> PredicateValue {
    match expr {
        Expr::Value(v) => sql_value_to_predicate_value(v),
        Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Minus, expr } => {
            if let Expr::Value(Value::Number(n, _)) = expr.as_ref() {
                let negated = format!("-{}", n);
                if let Ok(i) = negated.parse::<i64>() {
                    PredicateValue::Int(i)
                } else if let Ok(f) = negated.parse::<f64>() {
                    PredicateValue::Float(f)
                } else {
                    PredicateValue::String(negated)
                }
            } else {
                PredicateValue::String(expr.to_string())
            }
        }
        other => PredicateValue::String(other.to_string()),
    }
}

fn expr_to_sql_predicate(expr: &Expr) -> Option<SqlPredicate> {
    match expr {
        Expr::IsNull(inner) => {
            let column = expr_to_column_name(inner);
            if !is_valid_column_name(&column) { return None; }
            Some(SqlPredicate { column, operator: SqlOperator::IsNull, value: PredicateValue::Null })
        }
        Expr::IsNotNull(inner) => {
            let column = expr_to_column_name(inner);
            if !is_valid_column_name(&column) { return None; }
            Some(SqlPredicate { column, operator: SqlOperator::IsNotNull, value: PredicateValue::Null })
        }
        Expr::BinaryOp { left, op, right } => {
            let sql_op = match op {
                BinaryOperator::Eq => SqlOperator::Eq,
                BinaryOperator::NotEq => SqlOperator::NotEq,
                BinaryOperator::Lt => SqlOperator::Lt,
                BinaryOperator::LtEq => SqlOperator::LtEq,
                BinaryOperator::Gt => SqlOperator::Gt,
                BinaryOperator::GtEq => SqlOperator::GtEq,
                _ => return None,
            };

            let (col_expr, val_expr, final_op) = if is_literal_expr(left) && !is_literal_expr(right) {
                (right.as_ref(), left.as_ref(), flip_comparison(sql_op))
            } else {
                (left.as_ref(), right.as_ref(), sql_op)
            };

            let column = expr_to_column_name(col_expr);
            if !is_valid_column_name(&column) { return None; }
            Some(SqlPredicate { column, operator: final_op, value: expr_to_predicate_value(val_expr) })
        }
        Expr::Like { negated, expr: col, pattern, .. } => {
            let column = expr_to_column_name(col);
            if !is_valid_column_name(&column) { return None; }
            let op = if *negated { SqlOperator::NotLike } else { SqlOperator::Like };
            Some(SqlPredicate { column, operator: op, value: expr_to_predicate_value(pattern) })
        }
        Expr::InList { expr: col, list, negated } => {
            let column = expr_to_column_name(col);
            if !is_valid_column_name(&column) { return None; }
            let values: Vec<PredicateValue> = list.iter().map(expr_to_predicate_value).collect();
            let op = if *negated { SqlOperator::NotIn } else { SqlOperator::In };
            Some(SqlPredicate { column, operator: op, value: PredicateValue::List(values) })
        }
        _ => None,
    }
}

/// Check if a LIKE pattern starts with an unescaped `%` wildcard.
///
/// Leading backslashes are counted: an even number means the `%` is unescaped
/// (e.g. `\\%` = literal backslash + wildcard), while an odd number means
/// the `%` is escaped as a literal (e.g. `\%` = literal percent).
fn has_unescaped_leading_wildcard(pattern: &str) -> bool {
    let leading_backslashes = pattern.chars().take_while(|&c| c == '\\').count();
    pattern.as_bytes().get(leading_backslashes) == Some(&b'%')
        && leading_backslashes % 2 == 0
}

/// Check if a LIKE pattern ends with an unescaped `%` wildcard.
/// A trailing `\%` (preceded by an odd number of backslashes) is an escaped literal.
fn has_unescaped_trailing_wildcard(pattern: &str) -> bool {
    if !pattern.ends_with('%') {
        return false;
    }
    let before = &pattern[..pattern.len() - 1];
    let trailing_backslashes = before.chars().rev().take_while(|&c| c == '\\').count();
    trailing_backslashes % 2 == 0
}

fn is_literal_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => matches!(expr.as_ref(), Expr::Value(_)),
        _ => false,
    }
}

fn flip_comparison(op: SqlOperator) -> SqlOperator {
    match op {
        SqlOperator::Lt => SqlOperator::Gt,
        SqlOperator::LtEq => SqlOperator::GtEq,
        SqlOperator::Gt => SqlOperator::Lt,
        SqlOperator::GtEq => SqlOperator::LtEq,
        other => other,
    }
}

/// Validate a column name to prevent injection attacks.
///
/// Returns true if the column name is safe:
/// - Not empty and at most 256 characters
/// - Starts with a letter or underscore
/// - Contains only alphanumeric characters and underscores (individual `_`)
/// - Does not contain SQL keywords or injection patterns
///
/// Flattened nested-field separators (`__`) pass validation naturally because
/// each `_` is allowed individually.
fn is_valid_column_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 256 {
        return false;
    }
    
    // Check for dangerous patterns
    let lower = name.to_lowercase();
    const FORBIDDEN_PATTERNS: &[&str] = &[
        ";", "--", "/*", "*/", "'", "\"", "\\",
        "drop ", "delete ", "insert ", "update ", "select ",
        "union ", "exec ", "execute ",
    ];
    for pattern in FORBIDDEN_PATTERNS {
        if lower.contains(pattern) {
            return false;
        }
    }
    
    // First character must be letter or underscore
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    
    // Remaining characters must be alphanumeric or underscore
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_translate_eq() {
        let translator = MongoFilterTranslator::new();
        let predicates = vec![SqlPredicate {
            column: "status".to_string(),
            operator: SqlOperator::Eq,
            value: PredicateValue::String("active".to_string()),
        }];

        let result = translator.translate(&predicates).unwrap();
        assert_eq!(result.get_str("status").unwrap(), "active");
    }

    #[test]
    fn test_translate_gt() {
        let translator = MongoFilterTranslator::new();
        let predicates = vec![SqlPredicate {
            column: "amount".to_string(),
            operator: SqlOperator::Gt,
            value: PredicateValue::Int(100),
        }];

        let result = translator.translate(&predicates).unwrap();
        let amount_doc = result.get_document("amount").unwrap();
        assert_eq!(amount_doc.get_i64("$gt").unwrap(), 100);
    }

    #[test]
    fn test_translate_in() {
        let translator = MongoFilterTranslator::new();
        let predicates = vec![SqlPredicate {
            column: "status".to_string(),
            operator: SqlOperator::In,
            value: PredicateValue::List(vec![
                PredicateValue::String("active".to_string()),
                PredicateValue::String("pending".to_string()),
            ]),
        }];

        let result = translator.translate(&predicates).unwrap();
        let status_doc = result.get_document("status").unwrap();
        let in_array = status_doc.get_array("$in").unwrap();
        assert_eq!(in_array.len(), 2);
    }

    #[test]
    fn test_translate_nested_field() {
        let translator = MongoFilterTranslator::new();
        let predicates = vec![SqlPredicate {
            column: "user__address__city".to_string(),
            operator: SqlOperator::Eq,
            value: PredicateValue::String("NYC".to_string()),
        }];

        let result = translator.translate(&predicates).unwrap();
        assert_eq!(result.get_str("user.address.city").unwrap(), "NYC");
    }

    #[test]
    fn test_translate_like_prefix() {
        let translator = MongoFilterTranslator::new();
        let predicates = vec![SqlPredicate {
            column: "name".to_string(),
            operator: SqlOperator::Like,
            value: PredicateValue::String("John%".to_string()),
        }];

        let result = translator.translate(&predicates).unwrap();
        let name_val = result.get("name").expect("must have name key");
        match name_val {
            Bson::RegularExpression(re) => {
                assert!(re.pattern.starts_with("^"), "LIKE prefix pattern must start with ^, got: {}", re.pattern);
            }
            other => panic!("LIKE must produce RegularExpression, got: {:?}", other),
        }
    }

    #[test]
    fn test_translate_multiple_predicates() {
        let translator = MongoFilterTranslator::new();
        let predicates = vec![
            SqlPredicate {
                column: "status".to_string(),
                operator: SqlOperator::Eq,
                value: PredicateValue::String("active".to_string()),
            },
            SqlPredicate {
                column: "amount".to_string(),
                operator: SqlOperator::Gt,
                value: PredicateValue::Int(100),
            },
        ];

        let result = translator.translate(&predicates).unwrap();
        let and_array = result.get_array("$and").unwrap();
        assert_eq!(and_array.len(), 2);
    }

    #[test]
    fn test_parse_simple_eq() {
        let parsed = parse_simple_where_clause("status = 'active'");
        assert!(!parsed.has_unsupported_or);
        assert_eq!(parsed.predicates.len(), 1);
        assert_eq!(parsed.predicates[0].column, "status");
        assert_eq!(parsed.predicates[0].operator, SqlOperator::Eq);
    }

    #[test]
    fn test_parse_multiple_conditions() {
        let parsed = parse_simple_where_clause("status = 'active' AND amount > 100");
        assert!(!parsed.has_unsupported_or);
        assert_eq!(parsed.predicates.len(), 2);
    }

    #[test]
    fn test_parse_is_null() {
        let parsed = parse_simple_where_clause("deleted_at IS NULL");
        assert!(!parsed.has_unsupported_or);
        assert_eq!(parsed.predicates.len(), 1);
        assert_eq!(parsed.predicates[0].operator, SqlOperator::IsNull);
    }

    #[test]
    fn test_parse_in_list_via_where_clause() {
        let parsed = parse_simple_where_clause("col IN (1, 2, 3)");
        assert_eq!(parsed.predicates.len(), 1);
        assert_eq!(parsed.predicates[0].operator, SqlOperator::In);
        if let PredicateValue::List(ref values) = parsed.predicates[0].value {
            assert_eq!(values.len(), 3);
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_parse_failure_signals_unsupported() {
        // Use a fragment that cannot be parsed as any SQL expression.
        let parsed = parse_simple_where_clause("))) INVALID ((( = =");
        assert!(
            parsed.has_unsupported_or,
            "Parse failure must set has_unsupported_or so callers don't skip filtering"
        );
        assert!(
            parsed.predicates.is_empty(),
            "No predicates should be extracted from an unparseable clause"
        );
    }

    #[test]
    fn test_parse_valid_clause_no_unsupported_flag() {
        let parsed = parse_simple_where_clause("status = 'active' AND amount > 100");
        assert!(
            !parsed.has_unsupported_or,
            "A fully parseable AND clause must not set has_unsupported_or"
        );
    }

    #[test]
    fn test_build_id_filter() {
        let translator = MongoFilterTranslator::new();
        // Use valid ObjectId format (24 hex characters)
        let ids = vec![
            "507f1f77bcf86cd799439011".to_string(),
            "507f1f77bcf86cd799439012".to_string(),
        ];
        let filter = translator.build_id_filter(&ids);
        
        let id_doc = filter.get_document("_id").unwrap();
        let in_array = id_doc.get_array("$in").unwrap();
        assert_eq!(in_array.len(), 2);
    }

    #[test]
    fn test_is_valid_column_name_valid() {
        // Simple names
        assert!(is_valid_column_name("name"));
        assert!(is_valid_column_name("user_id"));
        assert!(is_valid_column_name("_private"));
        assert!(is_valid_column_name("col123"));
        
        // Nested fields with separator
        assert!(is_valid_column_name("user__address__city"));
        
        // Case variations
        assert!(is_valid_column_name("UserName"));
        assert!(is_valid_column_name("COLUMN"));
    }

    #[test]
    fn test_is_valid_column_name_invalid() {
        // Empty
        assert!(!is_valid_column_name(""));
        
        // Starts with number
        assert!(!is_valid_column_name("123col"));
        
        // Contains special characters
        assert!(!is_valid_column_name("col-name"));
        assert!(!is_valid_column_name("col@name"));
        assert!(!is_valid_column_name("col.name"));
        
        // Too long
        assert!(!is_valid_column_name(&"x".repeat(300)));
        
        // Contains SQL injection patterns
        assert!(!is_valid_column_name("col; DROP"));
        assert!(!is_valid_column_name("col--comment"));
    }

    #[test]
    fn test_like_is_case_sensitive() {
        let translator = MongoFilterTranslator::new();
        let predicates = vec![SqlPredicate {
            column: "name".to_string(),
            operator: SqlOperator::Like,
            value: PredicateValue::String("John%".to_string()),
        }];

        let result = translator.translate(&predicates).unwrap();
        let name_val = result.get("name").expect("must have name key");
        match name_val {
            Bson::RegularExpression(re) => {
                assert!(
                    re.options.is_empty(),
                    "LIKE should be case-sensitive (empty options), got: {:?}",
                    re.options
                );
            }
            other => panic!("LIKE must produce RegularExpression, got: {:?}", other),
        }
    }

    #[test]
    fn test_or_conditions_not_silently_dropped() {
        let parsed = parse_simple_where_clause("a = 1 AND (b = 2 OR c = 3)");
        assert!(parsed.has_unsupported_or, "should flag unsupported OR");
        assert_eq!(
            parsed.predicates.len(),
            1,
            "Only the AND-able predicate 'a = 1' should be kept; the OR subtree should be excluded"
        );
        assert_eq!(parsed.predicates[0].column, "a");
    }

    #[test]
    fn test_pure_or_flags_unsupported() {
        let parsed = parse_simple_where_clause("a = 1 OR b = 2");
        assert!(parsed.has_unsupported_or, "pure OR must set the flag");
        assert!(
            parsed.predicates.is_empty(),
            "no predicates can be safely pushed from a top-level OR"
        );
    }

    #[test]
    fn test_build_id_filter_non_objectid() {
        let translator = MongoFilterTranslator::new();
        let ids = vec![
            "not-an-objectid".to_string(),
            "some-uuid-value".to_string(),
        ];
        let filter = translator.build_id_filter(&ids);

        let id_doc = filter.get_document("_id").unwrap();
        let in_array = id_doc.get_array("$in").unwrap();
        assert_eq!(in_array.len(), 2, "Non-ObjectId strings should not be dropped");
        assert!(
            matches!(&in_array[0], Bson::String(s) if s == "not-an-objectid"),
            "Should fall back to Bson::String"
        );
    }

    #[test]
    fn test_build_id_filter_mixed_objectid_and_string() {
        let translator = MongoFilterTranslator::new();
        let ids = vec![
            "507f1f77bcf86cd799439011".to_string(),
            "not-an-objectid".to_string(),
        ];
        let filter = translator.build_id_filter(&ids);

        let id_doc = filter.get_document("_id").unwrap();
        let in_array = id_doc.get_array("$in").unwrap();
        assert_eq!(in_array.len(), 2);
        assert!(matches!(&in_array[0], Bson::ObjectId(_)));
        assert!(matches!(&in_array[1], Bson::String(_)));
    }

    #[test]
    fn test_literal_on_left_side_swapped() {
        let parsed = parse_simple_where_clause("100 < amount");
        assert_eq!(parsed.predicates.len(), 1);
        assert_eq!(parsed.predicates[0].column, "amount");
        assert_eq!(parsed.predicates[0].operator, SqlOperator::Gt);
        assert_eq!(parsed.predicates[0].value, PredicateValue::Int(100));
    }

    #[test]
    fn test_literal_eq_on_left_side_swapped() {
        let parsed = parse_simple_where_clause("'active' = status");
        assert_eq!(parsed.predicates.len(), 1);
        assert_eq!(parsed.predicates[0].column, "status");
        assert_eq!(parsed.predicates[0].operator, SqlOperator::Eq);
        assert_eq!(
            parsed.predicates[0].value,
            PredicateValue::String("active".to_string())
        );
    }

    #[test]
    fn test_like_to_regex_escaped_percent() {
        let translator = MongoFilterTranslator::new();
        // `\%` in LIKE means a literal percent sign
        let regex = translator
            .like_to_regex(&PredicateValue::String(r"hello\%world".to_string()))
            .unwrap();
        let re = regex::Regex::new(&regex).unwrap();
        assert!(re.is_match("hello%world"), "should match literal percent: {}", regex);
        assert!(!re.is_match("helloXworld"), "must not match arbitrary char: {}", regex);
    }

    #[test]
    fn test_like_to_regex_escaped_underscore() {
        let translator = MongoFilterTranslator::new();
        let regex = translator
            .like_to_regex(&PredicateValue::String(r"test\_value".to_string()))
            .unwrap();
        let re = regex::Regex::new(&regex).unwrap();
        assert!(re.is_match("test_value"), "should match literal underscore: {}", regex);
        assert!(!re.is_match("testXvalue"), "must not match arbitrary char: {}", regex);
    }

    #[test]
    fn test_like_to_regex_escaped_backslash() {
        let translator = MongoFilterTranslator::new();
        let regex = translator
            .like_to_regex(&PredicateValue::String(r"path\\dir".to_string()))
            .unwrap();
        let re = regex::Regex::new(&regex).unwrap();
        assert!(re.is_match(r"path\dir"), "should match literal backslash: {}", regex);
    }

    #[test]
    fn test_like_to_regex_unescaped_wildcards_still_work() {
        let translator = MongoFilterTranslator::new();
        let regex = translator
            .like_to_regex(&PredicateValue::String("%hello_world%".to_string()))
            .unwrap();
        let re = regex::Regex::new(&regex).unwrap();
        assert!(re.is_match("XXhelloXworldYY"), "% and _ should still be wildcards: {}", regex);
        assert!(re.is_match("helloXworld"), "single char wildcard: {}", regex);
        assert!(!re.is_match("helloworld"), "_ requires exactly one char: {}", regex);
    }

    #[test]
    fn test_like_to_regex_escaped_percent_at_end() {
        let translator = MongoFilterTranslator::new();
        let regex = translator
            .like_to_regex(&PredicateValue::String(r"hello\%".to_string()))
            .unwrap();
        let re = regex::Regex::new(&regex).unwrap();
        assert!(re.is_match("hello%"), "should match literal percent: {}", regex);
        assert!(!re.is_match("hello%anything"), "must NOT match extra characters after literal %: {}", regex);
        assert!(!re.is_match("hello"), "must require literal %: {}", regex);
    }

    #[test]
    fn test_like_to_regex_escaped_percent_at_start_and_end() {
        let translator = MongoFilterTranslator::new();
        let regex = translator
            .like_to_regex(&PredicateValue::String(r"%\%".to_string()))
            .unwrap();
        let re = regex::Regex::new(&regex).unwrap();
        assert!(re.is_match("hello%"), "should match anything ending with literal %: {}", regex);
        assert!(!re.is_match("hello%x"), "must not match characters after literal %: {}", regex);
    }

    #[test]
    fn test_has_unescaped_trailing_wildcard() {
        assert!(has_unescaped_trailing_wildcard("hello%"));
        assert!(!has_unescaped_trailing_wildcard(r"hello\%"));
        assert!(has_unescaped_trailing_wildcard(r"hello\\%"));
        assert!(!has_unescaped_trailing_wildcard(r"hello\\\%"));
        assert!(!has_unescaped_trailing_wildcard("hello"));
    }

    #[test]
    fn test_has_unescaped_leading_wildcard() {
        assert!(has_unescaped_leading_wildcard("%hello"));
        assert!(!has_unescaped_leading_wildcard(r"\%hello"));
        assert!(has_unescaped_leading_wildcard(r"\\%hello"));
        assert!(!has_unescaped_leading_wildcard(r"\\\%hello"));
        assert!(!has_unescaped_leading_wildcard("hello"));
        assert!(!has_unescaped_leading_wildcard(""));
    }

    #[test]
    fn test_like_escaped_leading_percent_is_anchored() {
        let translator = MongoFilterTranslator::new();
        let regex = translator
            .like_to_regex(&PredicateValue::String(r"\%hello".to_string()))
            .unwrap();
        assert!(
            regex.starts_with('^'),
            r"Pattern '\%hello' has escaped leading %%, regex should be anchored at start: {}",
            regex,
        );
        let re = regex::Regex::new(&regex).unwrap();
        assert!(re.is_match("%hello"));
        assert!(!re.is_match("Xhello"));
    }

    #[test]
    fn test_like_double_backslash_percent_is_wildcard() {
        let translator = MongoFilterTranslator::new();
        let regex = translator
            .like_to_regex(&PredicateValue::String(r"\\%hello".to_string()))
            .unwrap();
        assert!(
            !regex.starts_with('^'),
            r"Pattern '\\%hello' has unescaped %% after literal backslash, regex should NOT be anchored at start: {}",
            regex,
        );
    }

    #[test]
    fn test_translate_not_like_uses_regex_object() {
        let translator = MongoFilterTranslator::new();
        let predicates = vec![SqlPredicate {
            column: "name".to_string(),
            operator: SqlOperator::NotLike,
            value: PredicateValue::String("John%".to_string()),
        }];

        let result = translator.translate(&predicates).unwrap();
        let name_doc = result.get_document("name").unwrap();
        let not_val = name_doc.get("$not").expect("$not key must exist");
        assert!(
            matches!(not_val, Bson::RegularExpression(_)),
            "$not must wrap a Bson::RegularExpression, got: {:?}",
            not_val
        );
    }

    #[test]
    fn test_is_literal_expr_rejects_complex_negation() {
        use sqlparser::ast::{UnaryOperator, BinaryOperator};
        let complex = Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Value(Value::Number("2".to_string(), false))),
            }),
        };
        assert!(!is_literal_expr(&complex), "-(1+2) must not be classified as a literal");

        let simple_neg = Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::Value(Value::Number("42".to_string(), false))),
        };
        assert!(is_literal_expr(&simple_neg), "-42 should be a literal");
    }

    #[test]
    fn test_malformed_where_clause_returns_empty_predicates() {
        // "NOT VALID SQL @@#$%" partially parses (NOT <identifier>), so use a
        // truly unparseable fragment to exercise the Err branch of parse_expr.
        let parsed = parse_simple_where_clause("@@#$%");
        assert!(
            parsed.predicates.is_empty(),
            "Malformed SQL should return empty predicates, not panic"
        );
        assert!(parsed.has_unsupported_or,
            "Parse failure must set has_unsupported_or so callers do not treat empty predicates as 'no filter'");
    }

    #[test]
    fn test_like_and_not_like_produce_bson_regex() {
        let translator = MongoFilterTranslator::new();
        let like_pred = SqlPredicate {
            column: "name".to_string(),
            operator: SqlOperator::Like,
            value: PredicateValue::String("%test%".to_string()),
        };
        let not_like_pred = SqlPredicate {
            column: "name".to_string(),
            operator: SqlOperator::NotLike,
            value: PredicateValue::String("%test%".to_string()),
        };

        let like_doc = translator.translate_predicate(&like_pred).unwrap();
        let not_like_doc = translator.translate_predicate(&not_like_pred).unwrap();

        // LIKE must produce a Bson::RegularExpression, not a string "$regex"
        let like_val = like_doc.get("name").expect("LIKE doc must have 'name' key");
        assert!(
            matches!(like_val, Bson::RegularExpression(_)),
            "LIKE must produce a Bson::RegularExpression, got {:?}",
            like_val,
        );

        // NOT LIKE must wrap the regex in $not
        let not_like_val = not_like_doc.get("name").expect("NOT LIKE doc must have 'name' key");
        if let Bson::Document(inner) = not_like_val {
            let not_val = inner.get("$not").expect("NOT LIKE must have $not key");
            assert!(
                matches!(not_val, Bson::RegularExpression(_)),
                "NOT LIKE $not must contain a RegularExpression, got {:?}",
                not_val,
            );
        } else {
            panic!("NOT LIKE doc value should be a Document, got {:?}", not_like_val);
        }
    }
}
