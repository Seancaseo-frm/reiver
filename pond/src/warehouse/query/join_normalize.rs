//! CROSS JOIN to INNER JOIN normalization.
//!
//! Detects `CROSS JOIN ... WHERE table1.col = table2.col` patterns and
//! rewrites them as `INNER JOIN ... ON table1.col = table2.col`, removing
//! the equi-join predicate from the WHERE clause.  This enables downstream
//! semi-join optimization and produces more accurate cost estimates.

use ahash::AHashMap;
use sqlparser::ast::{
    BinaryOperator, Expr, JoinConstraint, JoinOperator, Query, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

/// Rewrite CROSS JOINs with equi-join WHERE predicates into INNER JOINs.
///
/// Returns the rewritten SQL string unchanged if no transformation applies.
pub fn normalize_cross_joins(sql: &str) -> Result<String, String> {
    let dialect = ClickHouseDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("SQL parse error: {e}"))?;

    let mut changed = false;
    for stmt in &mut statements {
        if let Statement::Query(query) = stmt {
            changed |= normalize_query(query);
        }
    }

    if !changed {
        return Ok(sql.to_string());
    }

    let parts: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
    Ok(parts.join("; "))
}

/// Walk a `Query` recursively and normalize its body.
fn normalize_query(query: &mut Query) -> bool {
    normalize_set_expr(query.body.as_mut())
}

fn normalize_set_expr(body: &mut SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => {
            let alias_map = build_alias_map(&select.from);

            let mut changed = false;
            for twj in &mut select.from {
                changed |= normalize_table_with_joins(twj, &mut select.selection, &alias_map);
            }
            changed
        }
        SetExpr::Query(q) => normalize_query(q),
        SetExpr::SetOperation { left, right, .. } => {
            let a = normalize_set_expr(left.as_mut());
            let b = normalize_set_expr(right.as_mut());
            a || b
        }
        _ => false,
    }
}

/// Build a map of alias -> table name for every table in the FROM clause.
fn build_alias_map(from: &[TableWithJoins]) -> AHashMap<String, String> {
    let mut map = AHashMap::new();
    for twj in from {
        collect_alias(&twj.relation, &mut map);
        for join in &twj.joins {
            collect_alias(&join.relation, &mut map);
        }
    }
    map
}

fn collect_alias(factor: &TableFactor, map: &mut AHashMap<String, String>) {
    if let TableFactor::Table { name, alias, .. } = factor {
        let table_name = name.0.last().map(|i| i.value.clone()).unwrap_or_default();
        if let Some(a) = alias {
            map.insert(a.name.value.clone(), table_name.clone());
        }
        map.insert(table_name.clone(), table_name);
    }
}

/// Get the name (or alias) that a table factor is referenceable by in
/// the WHERE clause.
fn table_ref_name(factor: &TableFactor) -> Option<String> {
    if let TableFactor::Table { name, alias, .. } = factor {
        if let Some(a) = alias {
            return Some(a.name.value.clone());
        }
        return name.0.last().map(|i| i.value.clone());
    }
    None
}

/// For each CROSS JOIN in `twj.joins`, check if the WHERE clause contains
/// an equi-join predicate referencing the left relation and the cross-joined
/// table.  If so, convert the CROSS JOIN to INNER JOIN ON and remove the
/// predicate from WHERE.
fn normalize_table_with_joins(
    twj: &mut TableWithJoins,
    selection: &mut Option<Expr>,
    alias_map: &AHashMap<String, String>,
) -> bool {
    let mut changed = false;

    // Collect the names of all tables on the "left side" of the join chain.
    // As we process joins left-to-right, tables already joined are available.
    let mut left_tables: Vec<String> = Vec::new();
    if let Some(name) = table_ref_name(&twj.relation) {
        left_tables.push(name);
    }

    for join in &mut twj.joins {
        if !matches!(join.join_operator, JoinOperator::CrossJoin) {
            if let Some(name) = table_ref_name(&join.relation) {
                left_tables.push(name);
            }
            continue;
        }

        let right_name = match table_ref_name(&join.relation) {
            Some(n) => n,
            None => {
                left_tables.push("".to_string());
                continue;
            }
        };

        if let Some(where_expr) = selection.as_ref() {
            if let Some((on_expr, remaining)) =
                extract_equi_join_predicate(where_expr, &left_tables, &right_name, alias_map)
            {
                join.join_operator = JoinOperator::Inner(JoinConstraint::On(on_expr));
                *selection = remaining;
                changed = true;
            }
        }

        left_tables.push(right_name);
    }

    changed
}

/// Try to extract an equi-join predicate from `expr` that references one of
/// `left_tables` and `right_table`.
///
/// Returns `Some((on_condition, remaining_where))` if found.
/// The `remaining_where` is the WHERE clause with the extracted predicate
/// removed, or `None` if nothing remains.
fn extract_equi_join_predicate(
    expr: &Expr,
    left_tables: &[String],
    right_table: &str,
    alias_map: &AHashMap<String, String>,
) -> Option<(Expr, Option<Expr>)> {
    // Flatten the AND chain so we can pick out the equi-join predicate.
    let mut conjuncts = Vec::new();
    flatten_and(expr, &mut conjuncts);

    let mut found_idx = None;
    for (i, conj) in conjuncts.iter().enumerate() {
        if is_cross_table_eq(conj, left_tables, right_table, alias_map) {
            found_idx = Some(i);
            break;
        }
    }

    let idx = found_idx?;
    let on_expr = conjuncts.remove(idx);
    let remaining = rebuild_and(conjuncts);
    Some((on_expr, remaining))
}

/// Flatten an AND-chain into a vec of conjuncts.  Parenthesized `(a AND b)`
/// is flattened the same way.
fn flatten_and(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        Expr::Nested(inner) => flatten_and(inner, out),
        other => out.push(other.clone()),
    }
}

/// Rebuild an AND-chain from a vec of conjuncts.
fn rebuild_and(mut parts: Vec<Expr>) -> Option<Expr> {
    if parts.is_empty() {
        return None;
    }
    let mut result = parts.remove(0);
    for part in parts {
        result = Expr::BinaryOp {
            left: Box::new(result),
            op: BinaryOperator::And,
            right: Box::new(part),
        };
    }
    Some(result)
}

/// Check whether `expr` is an equality comparison between a column from one
/// of `left_tables` and a column from `right_table`.
fn is_cross_table_eq(
    expr: &Expr,
    left_tables: &[String],
    right_table: &str,
    alias_map: &AHashMap<String, String>,
) -> bool {
    let (left, right) = match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => (left.as_ref(), right.as_ref()),
        _ => return false,
    };

    let l_ref = extract_column_ref(left);
    let r_ref = extract_column_ref(right);

    let (l_table, r_table) = match (l_ref, r_ref) {
        (Some((lt, _)), Some((rt, _))) => (lt, rt),
        _ => return false,
    };

    if l_table.is_empty() || r_table.is_empty() {
        return false;
    }

    let l_resolved = alias_map
        .get(&l_table)
        .map(|s| s.as_str())
        .unwrap_or(&l_table);
    let r_resolved = alias_map
        .get(&r_table)
        .map(|s| s.as_str())
        .unwrap_or(&r_table);

    let right_resolved = alias_map
        .get(right_table)
        .map(|s| s.as_str())
        .unwrap_or(right_table);

    let l_on_left = left_tables.iter().any(|lt| {
        let lt_resolved = alias_map.get(lt.as_str()).map(|s| s.as_str()).unwrap_or(lt);
        l_resolved.eq_ignore_ascii_case(lt_resolved) || l_table.eq_ignore_ascii_case(lt)
    });
    let r_on_right = r_resolved.eq_ignore_ascii_case(right_resolved)
        || r_table.eq_ignore_ascii_case(right_table);

    let r_on_left = left_tables.iter().any(|lt| {
        let lt_resolved = alias_map.get(lt.as_str()).map(|s| s.as_str()).unwrap_or(lt);
        r_resolved.eq_ignore_ascii_case(lt_resolved) || r_table.eq_ignore_ascii_case(lt)
    });
    let l_on_right = l_resolved.eq_ignore_ascii_case(right_resolved)
        || l_table.eq_ignore_ascii_case(right_table);

    (l_on_left && r_on_right) || (r_on_left && l_on_right)
}

/// Extract `(table, column)` from a compound identifier expression.
fn extract_column_ref(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let table = parts[parts.len() - 2].value.clone();
            let column = parts[parts.len() - 1].value.clone();
            Some((table, column))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(sql: &str) -> String {
        normalize_cross_joins(sql).expect("should not fail")
    }

    #[test]
    fn test_basic_cross_to_inner() {
        let sql = "SELECT * FROM a CROSS JOIN b WHERE a.id = b.a_id";
        let result = roundtrip(sql);
        assert!(
            result.contains("JOIN") && !result.contains("CROSS"),
            "Expected INNER JOIN, got: {result}"
        );
        assert!(
            result.contains("a.id = b.a_id"),
            "ON condition missing: {result}"
        );
        // No WHERE clause should remain
        assert!(
            !result.to_uppercase().contains("WHERE"),
            "WHERE should be removed: {result}"
        );
    }

    #[test]
    fn test_partial_where_preserved() {
        let sql = "SELECT * FROM a CROSS JOIN b WHERE a.id = b.a_id AND a.status = 'active'";
        let result = roundtrip(sql);
        assert!(
            !result.contains("CROSS"),
            "CROSS JOIN should be converted: {result}"
        );
        assert!(
            result.contains("a.id = b.a_id"),
            "ON condition missing: {result}"
        );
        let upper = result.to_uppercase();
        assert!(
            upper.contains("WHERE"),
            "WHERE clause for remaining predicate should stay: {result}"
        );
        assert!(
            result.contains("status") && result.contains("active"),
            "Remaining predicate lost: {result}"
        );
    }

    #[test]
    fn test_multiple_cross_joins() {
        let sql = "SELECT * FROM a CROSS JOIN b CROSS JOIN c WHERE a.id = b.a_id AND b.id = c.b_id";
        let result = roundtrip(sql);
        assert!(
            !result.contains("CROSS"),
            "All CROSS JOINs should be converted: {result}"
        );
        assert!(
            result.contains("a.id = b.a_id"),
            "First ON condition missing: {result}"
        );
        assert!(
            result.contains("b.id = c.b_id"),
            "Second ON condition missing: {result}"
        );
    }

    #[test]
    fn test_no_conversion_without_equi_join() {
        let sql = "SELECT * FROM a CROSS JOIN b WHERE a.x > 10";
        let result = roundtrip(sql);
        assert!(
            result.contains("CROSS"),
            "Should stay as CROSS JOIN: {result}"
        );
    }

    #[test]
    fn test_alias_handling() {
        let sql = "SELECT * FROM users u CROSS JOIN orders o WHERE u.id = o.user_id";
        let result = roundtrip(sql);
        assert!(
            !result.contains("CROSS"),
            "CROSS JOIN should be converted: {result}"
        );
        assert!(
            result.contains("u.id = o.user_id"),
            "ON condition missing: {result}"
        );
    }

    #[test]
    fn test_passthrough_inner_join() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id";
        let result = roundtrip(sql);
        assert!(
            result.contains("a.id = b.a_id"),
            "INNER JOIN should be unchanged: {result}"
        );
    }

    #[test]
    fn test_passthrough_no_join() {
        let sql = "SELECT * FROM a WHERE a.x = 1";
        let result = roundtrip(sql);
        assert!(
            result.contains("a.x") && result.contains("1"),
            "Simple query should be unchanged: {result}"
        );
    }

    #[test]
    fn test_unqualified_columns_not_matched() {
        // Unqualified columns can't be reliably attributed to a table
        let sql = "SELECT * FROM a CROSS JOIN b WHERE id = 5";
        let result = roundtrip(sql);
        assert!(
            result.contains("CROSS"),
            "Unqualified column should not trigger conversion: {result}"
        );
    }
}
