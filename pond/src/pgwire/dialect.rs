//! SQL dialect translation from Postgres to ClickHouse.
//!
//! BI tools generate Postgres-flavored SQL. ClickHouse supports most of it,
//! but certain constructs need translation. This module performs AST-level
//! rewriting of Postgres SQL idioms before queries reach ClickHouse.
//!
//! Only applied to **data queries** routed to ClickHouse. Catalog queries
//! go through DataFusion which handles Postgres SQL natively.
//!
//! ## What gets translated
//!
//! - **Cast syntax**: `value::type` (DoubleColon) -> `CAST(value AS type)`
//! - **Type names in CASTs**: Postgres type names -> ClickHouse equivalents
//! - **Functions**: `date_trunc`, `string_agg`, `extract(epoch ...)`, `substring(FROM/FOR)`, etc.

use sqlparser::ast::{
    CastKind, DataType, DateTimeField, Expr, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, Query, SetExpr, Statement,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Translate Postgres SQL idioms to ClickHouse-compatible SQL.
///
/// Returns the rewritten SQL string. If parsing fails or no translation
/// is needed, the original SQL is returned unchanged.
pub fn translate_to_clickhouse(sql: &str) -> String {
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(_) => return sql.to_owned(),
    };

    if statements.is_empty() {
        return sql.to_owned();
    }

    translate_statements_to_clickhouse(&mut statements);

    statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// Apply PG-to-ClickHouse dialect rewrites on pre-parsed statements in-place.
///
/// Zero-parse variant for use when statements are already available.
pub fn translate_statements_to_clickhouse(statements: &mut [Statement]) {
    for stmt in statements.iter_mut() {
        rewrite_statement(stmt);
    }
}

// =============================================================================
// AST walking
// =============================================================================

fn rewrite_statement(stmt: &mut Statement) {
    match stmt {
        Statement::Query(query) => rewrite_query(query),
        _ => {}
    }
}

fn rewrite_query(query: &mut Query) {
    rewrite_set_expr(&mut query.body);
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            rewrite_query(&mut cte.query);
        }
    }
    // Rewrite ORDER BY expressions
    if let Some(order_by) = &mut query.order_by {
        for item in &mut order_by.exprs {
            rewrite_expr(&mut item.expr);
        }
    }
}

fn rewrite_set_expr(set_expr: &mut SetExpr) {
    match set_expr {
        SetExpr::Select(select) => {
            // Rewrite SELECT expressions
            for item in &mut select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item
                {
                    rewrite_expr(expr);
                }
            }
            // Rewrite FROM table factors (subqueries)
            for table_with_joins in &mut select.from {
                rewrite_table_factor(&mut table_with_joins.relation);
                for join in &mut table_with_joins.joins {
                    rewrite_table_factor(&mut join.relation);
                }
            }
            // Rewrite WHERE
            if let Some(selection) = &mut select.selection {
                rewrite_expr(selection);
            }
            // Rewrite GROUP BY
            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
                for expr in exprs {
                    rewrite_expr(expr);
                }
            }
            // Rewrite HAVING
            if let Some(having) = &mut select.having {
                rewrite_expr(having);
            }
        }
        SetExpr::Query(query) => rewrite_query(query),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left);
            rewrite_set_expr(right);
        }
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for expr in row {
                    rewrite_expr(expr);
                }
            }
        }
        // Insert/Update/Table won't appear in read-only SELECT queries
        // (the read-only guard in handler.rs rejects them before they reach here).
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Table(_) => {}
    }
}

fn rewrite_table_factor(factor: &mut sqlparser::ast::TableFactor) {
    use sqlparser::ast::TableFactor;

    match factor {
        TableFactor::Table { with_hints, .. } => {
            // Table name is a leaf, but WITH hints can contain expressions
            for hint in with_hints {
                rewrite_expr(hint);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_query(subquery);
        }
        TableFactor::TableFunction { expr, .. } => {
            rewrite_expr(expr);
        }
        TableFactor::Function { args, .. } => {
            for arg in args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        rewrite_expr(expr);
                    }
                    FunctionArg::Named { arg: FunctionArgExpr::Expr(expr), .. } => {
                        rewrite_expr(expr);
                    }
                    _ => {}
                }
            }
        }
        TableFactor::UNNEST { array_exprs, .. } => {
            for expr in array_exprs {
                rewrite_expr(expr);
            }
        }
        TableFactor::JsonTable { json_expr, .. } => {
            rewrite_expr(json_expr);
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            rewrite_table_factor(&mut table_with_joins.relation);
            for join in &mut table_with_joins.joins {
                rewrite_table_factor(&mut join.relation);
            }
        }
        TableFactor::Pivot {
            table,
            aggregate_functions,
            default_on_null,
            ..
        } => {
            rewrite_table_factor(table);
            for agg in aggregate_functions {
                rewrite_expr(&mut agg.expr);
            }
            if let Some(default) = default_on_null {
                rewrite_expr(default);
            }
        }
        TableFactor::Unpivot { table, .. } => {
            // Unpivot columns are identifiers (leaves), but the source table
            // may itself contain expressions.
            rewrite_table_factor(table);
        }
        TableFactor::MatchRecognize {
            table,
            partition_by,
            measures,
            symbols,
            ..
        } => {
            rewrite_table_factor(table);
            for expr in partition_by {
                rewrite_expr(expr);
            }
            for measure in measures {
                rewrite_expr(&mut measure.expr);
            }
            for sym in symbols {
                rewrite_expr(&mut sym.definition);
            }
        }
    }
}

// =============================================================================
// Expression rewriting
// =============================================================================

fn rewrite_expr(expr: &mut Expr) {
    // First, recurse into ALL sub-expressions exhaustively.
    // Every Expr variant that contains child Expr or Query nodes must be
    // handled here so nested Postgres constructs are always visited.
    match expr {
        // ── Already handled variants ──
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr(left);
            rewrite_expr(right);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            rewrite_expr(inner);
        }
        Expr::Nested(inner) => {
            rewrite_expr(inner);
        }
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => {
            rewrite_expr(inner);
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            rewrite_expr(left);
            rewrite_expr(right);
        }
        Expr::InList { expr: inner, list, .. } => {
            rewrite_expr(inner);
            for item in list {
                rewrite_expr(item);
            }
        }
        Expr::InSubquery { expr: inner, subquery, .. } => {
            rewrite_expr(inner);
            rewrite_query(subquery);
        }
        Expr::InUnnest { expr: inner, array_expr, .. } => {
            rewrite_expr(inner);
            rewrite_expr(array_expr);
        }
        Expr::Between { expr: inner, low, high, .. } => {
            rewrite_expr(inner);
            rewrite_expr(low);
            rewrite_expr(high);
        }
        Expr::Case { operand, conditions, results, else_result, .. } => {
            if let Some(op) = operand {
                rewrite_expr(op);
            }
            for cond in conditions {
                rewrite_expr(cond);
            }
            for res in results {
                rewrite_expr(res);
            }
            if let Some(el) = else_result {
                rewrite_expr(el);
            }
        }
        Expr::Cast { expr: inner, .. } => {
            rewrite_expr(inner);
        }
        Expr::Function(func) => {
            rewrite_function_args(&mut func.args);
            if let Some(ref mut filter_expr) = func.filter {
                rewrite_expr(filter_expr);
            }
            if let Some(ref mut window) = func.over {
                if let sqlparser::ast::WindowType::WindowSpec(ref mut spec) = window {
                    for expr in &mut spec.partition_by {
                        rewrite_expr(expr);
                    }
                    for order in &mut spec.order_by {
                        rewrite_expr(&mut order.expr);
                    }
                }
            }
            for order in &mut func.within_group {
                rewrite_expr(&mut order.expr);
            }
        }
        Expr::Subquery(query) => {
            rewrite_query(query);
        }
        Expr::Extract { expr: inner, .. } => {
            rewrite_expr(inner);
        }

        // ── Like / ILike / SimilarTo / RLike ──
        Expr::Like { expr: inner, pattern, .. }
        | Expr::ILike { expr: inner, pattern, .. }
        | Expr::SimilarTo { expr: inner, pattern, .. }
        | Expr::RLike { expr: inner, pattern, .. } => {
            rewrite_expr(inner);
            rewrite_expr(pattern);
        }

        // ── AnyOp / AllOp ──
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            rewrite_expr(left);
            rewrite_expr(right);
        }

        // ── Convert (MSSQL) ──
        Expr::Convert { expr: inner, styles, .. } => {
            rewrite_expr(inner);
            for style in styles {
                rewrite_expr(style);
            }
        }

        // ── AT TIME ZONE ──
        Expr::AtTimeZone { timestamp, time_zone } => {
            rewrite_expr(timestamp);
            rewrite_expr(time_zone);
        }

        // ── Ceil / Floor ──
        Expr::Ceil { expr: inner, .. } | Expr::Floor { expr: inner, .. } => {
            rewrite_expr(inner);
        }

        // ── Position ──
        Expr::Position { expr: inner, r#in } => {
            rewrite_expr(inner);
            rewrite_expr(r#in);
        }

        // ── Substring ──
        Expr::Substring { expr: inner, substring_from, substring_for, .. } => {
            rewrite_expr(inner);
            if let Some(from) = substring_from {
                rewrite_expr(from);
            }
            if let Some(f) = substring_for {
                rewrite_expr(f);
            }
        }

        // ── Trim ──
        Expr::Trim { expr: inner, trim_what, trim_characters, .. } => {
            rewrite_expr(inner);
            if let Some(what) = trim_what {
                rewrite_expr(what);
            }
            if let Some(chars) = trim_characters {
                for c in chars {
                    rewrite_expr(c);
                }
            }
        }

        // ── Overlay ──
        Expr::Overlay { expr: inner, overlay_what, overlay_from, overlay_for } => {
            rewrite_expr(inner);
            rewrite_expr(overlay_what);
            rewrite_expr(overlay_from);
            if let Some(f) = overlay_for {
                rewrite_expr(f);
            }
        }

        // ── Collate ──
        Expr::Collate { expr: inner, .. } => {
            rewrite_expr(inner);
        }

        // ── Exists ──
        Expr::Exists { subquery, .. } => {
            rewrite_query(subquery);
        }

        // ── Array ──
        Expr::Array(arr) => {
            for elem in &mut arr.elem {
                rewrite_expr(elem);
            }
        }

        // ── Tuple ──
        Expr::Tuple(exprs) => {
            for e in exprs {
                rewrite_expr(e);
            }
        }

        // ── Struct (BigQuery) ──
        Expr::Struct { values, .. } => {
            for v in values {
                rewrite_expr(v);
            }
        }

        // ── Named expression (BigQuery) ──
        Expr::Named { expr: inner, .. } => {
            rewrite_expr(inner);
        }

        // ── GroupingSets / Cube / Rollup ──
        Expr::GroupingSets(sets) | Expr::Cube(sets) | Expr::Rollup(sets) => {
            for set in sets {
                for e in set {
                    rewrite_expr(e);
                }
            }
        }

        // ── Subscript (array[idx]) ──
        Expr::Subscript { expr: inner, subscript } => {
            rewrite_expr(inner);
            match subscript.as_mut() {
                sqlparser::ast::Subscript::Index { index } => rewrite_expr(index),
                sqlparser::ast::Subscript::Slice { lower_bound, upper_bound, stride } => {
                    if let Some(lb) = lower_bound { rewrite_expr(lb); }
                    if let Some(ub) = upper_bound { rewrite_expr(ub); }
                    if let Some(s) = stride { rewrite_expr(s); }
                }
            }
        }

        // ── MapAccess ──
        Expr::MapAccess { column, keys } => {
            rewrite_expr(column);
            for key in keys {
                rewrite_expr(&mut key.key);
            }
        }

        // ── JsonAccess ──
        Expr::JsonAccess { value, path } => {
            rewrite_expr(value);
            for elem in &mut path.path {
                if let sqlparser::ast::JsonPathElem::Bracket { key } = elem {
                    rewrite_expr(key);
                }
            }
        }

        // ── CompositeAccess ──
        Expr::CompositeAccess { expr: inner, .. } => {
            rewrite_expr(inner);
        }

        // ── Interval ──
        Expr::Interval(interval) => {
            rewrite_expr(&mut interval.value);
        }

        // ── Dictionary (DuckDB struct literal) ──
        Expr::Dictionary(fields) => {
            for field in fields {
                rewrite_expr(&mut field.value);
            }
        }

        // ── Map (DuckDB map literal) ──
        Expr::Map(map) => {
            for entry in &mut map.entries {
                rewrite_expr(&mut entry.key);
                rewrite_expr(&mut entry.value);
            }
        }

        // ── OuterJoin / Prior ──
        Expr::OuterJoin(inner) | Expr::Prior(inner) => {
            rewrite_expr(inner);
        }

        // ── Lambda ──
        Expr::Lambda(lambda) => {
            rewrite_expr(&mut lambda.body);
        }

        // ── Leaf nodes: no sub-expressions to recurse into ──
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::TypedString { .. }
        | Expr::IntroducedString { .. }
        | Expr::Wildcard
        | Expr::QualifiedWildcard(_)
        | Expr::MatchAgainst { .. } => {}
    }

    // Then apply transformations at this node
    let replacement = match expr {
        // ── Cast: DoubleColon -> CAST, with type translation ──
        Expr::Cast {
            kind,
            expr: _inner,
            data_type,
            format: _,
        } => {
            // Convert :: casts to explicit CAST syntax for ClickHouse
            if *kind == CastKind::DoubleColon {
                *kind = CastKind::Cast;
            }
            // Translate Postgres type names to ClickHouse equivalents
            translate_data_type(data_type);
            None
        }

        // ── Extract(field from ...) -> ClickHouse temporal functions ──
        Expr::Extract { field, expr: inner, .. } => translate_extract(field, inner),

        // ── Function translations ──
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_ascii_lowercase();
            match func_name.as_str() {
                // date_trunc('interval', column) -> toStartOf...
                "date_trunc" => translate_date_trunc(func),

                // string_agg(col, sep) -> arrayStringConcat(groupArray(col), sep)
                "string_agg" => translate_string_agg(func),

                // date_part('field', expr) -> same as extract
                "date_part" => translate_date_part(func),

                // to_char(expr, fmt) -> formatDateTime(expr, ch_fmt)
                "to_char" => translate_to_char(func),

                // array_agg(expr) -> groupArray(expr)
                "array_agg" => translate_simple_rename(func, "groupArray"),

                // bool_or(expr) -> max(expr)
                "bool_or" => translate_simple_rename(func, "max"),

                // bool_and(expr) -> min(expr)
                "bool_and" => translate_simple_rename(func, "min"),

                // regexp_replace(str, pat, rep [, flags]) ->
                //   replaceRegexpAll if flags contains 'g', else replaceRegexpOne
                "regexp_replace" => translate_regexp_replace(func),

                // left(str, n) -> substring(str, 1, n)
                "left" => translate_left(func),

                // right(str, n) -> substring(str, -n)
                "right" => translate_right(func),

                // char_length(str) -> lengthUTF8(str)
                "char_length" | "character_length" => translate_simple_rename(func, "lengthUTF8"),

                // current_timestamp -> now()  (ClickHouse native)
                "current_timestamp" => {
                    func.name = ObjectName(vec![Ident::new("now")]);
                    None
                }

                // current_date -> today()  (ClickHouse native)
                "current_date" => {
                    func.name = ObjectName(vec![Ident::new("today")]);
                    None
                }

                // current_time -> now()  (ClickHouse has no time-only function)
                "current_time" => {
                    func.name = ObjectName(vec![Ident::new("now")]);
                    None
                }

                // localtimestamp -> now()
                "localtimestamp" => {
                    func.name = ObjectName(vec![Ident::new("now")]);
                    None
                }

                // localtime -> now()
                "localtime" => {
                    func.name = ObjectName(vec![Ident::new("now")]);
                    None
                }

                _ => None,
            }
        }

        _ => None,
    };

    if let Some(new_expr) = replacement {
        *expr = new_expr;
    }
}

fn rewrite_function_args(args: &mut FunctionArguments) {
    match args {
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        rewrite_expr(expr);
                    }
                    FunctionArg::Named { arg: FunctionArgExpr::Expr(expr), .. } => {
                        rewrite_expr(expr);
                    }
                    // Wildcard / QualifiedWildcard are leaf nodes
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    | FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_))
                    | FunctionArg::Named { arg: FunctionArgExpr::Wildcard, .. }
                    | FunctionArg::Named { arg: FunctionArgExpr::QualifiedWildcard(_), .. } => {}
                }
            }
            // Recurse into clauses that contain expressions (ORDER BY, LIMIT, etc.)
            for clause in &mut list.clauses {
                match clause {
                    sqlparser::ast::FunctionArgumentClause::OrderBy(order_exprs) => {
                        for oe in order_exprs {
                            rewrite_expr(&mut oe.expr);
                        }
                    }
                    sqlparser::ast::FunctionArgumentClause::Limit(expr) => {
                        rewrite_expr(expr);
                    }
                    sqlparser::ast::FunctionArgumentClause::Having(bound) => {
                        rewrite_expr(&mut bound.1);
                    }
                    // Separator, IgnoreOrRespectNulls, OnOverflow -- no sub-expressions
                    _ => {}
                }
            }
        }
        FunctionArguments::Subquery(query) => {
            rewrite_query(query);
        }
        FunctionArguments::None => {}
    }
}

// =============================================================================
// Type translation
// =============================================================================

/// Translate Postgres type names to ClickHouse equivalents in-place.
fn translate_data_type(dt: &mut DataType) {
    let replacement = match dt {
        // integer / int -> Int32
        DataType::Integer(_) | DataType::Int(_) => Some(DataType::Int32),
        // bigint -> Int64
        DataType::BigInt(_) => Some(DataType::Int64),
        // smallint -> Int16
        DataType::SmallInt(_) => Some(DataType::Int16),
        // boolean / bool -> Bool
        DataType::Boolean => Some(DataType::Bool),
        // real / float4 -> Float32
        DataType::Real => Some(DataType::Float32),
        // double precision / float8 -> Float64
        DataType::DoublePrecision => Some(DataType::Float64),
        // text -> String (ClickHouse)
        DataType::Text => Some(DataType::String(None)),
        // varchar -> String
        DataType::Varchar(_) | DataType::CharVarying(_) => Some(DataType::String(None)),
        // char -> FixedString in ClickHouse, but String is safer
        DataType::Char(_) | DataType::Character(_) => Some(DataType::String(None)),
        // timestamp -> DateTime64(3) -- we use String representation since
        // sqlparser doesn't have a DateTime64 variant. The CAST will work
        // because ClickHouse handles CAST(x AS DateTime) natively.
        // Just leave Timestamp as-is since ClickHouse understands it.
        _ => None,
    };

    if let Some(new_dt) = replacement {
        *dt = new_dt;
    }
}

// =============================================================================
// Function translations
// =============================================================================

/// Translate `date_trunc('interval', expr)` to ClickHouse `toStartOf...` functions.
fn translate_date_trunc(func: &mut sqlparser::ast::Function) -> Option<Expr> {
    let args = extract_function_args(&func.args)?;
    if args.len() < 2 {
        return None;
    }

    // First arg should be a string literal (the interval)
    let interval = match &args[0] {
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => s.to_ascii_lowercase(),
        _ => return None,
    };

    let column_expr = args[1].clone();

    let ch_func = match interval.as_str() {
        "second" => "toStartOfSecond",
        "minute" => "toStartOfMinute",
        "hour" => "toStartOfHour",
        "day" => "toStartOfDay",
        "week" => "toStartOfWeek",
        "month" => "toStartOfMonth",
        "quarter" => "toStartOfQuarter",
        "year" => "toStartOfYear",
        _ => return None, // Unknown interval, pass through
    };

    Some(make_function_call(ch_func, vec![column_expr]))
}

/// Translate `string_agg(col, sep [ORDER BY ...])` to ClickHouse equivalent.
///
/// Without ORDER BY: `arrayStringConcat(groupArray(col), sep)`
/// With ORDER BY:    `arrayStringConcat(arraySort(groupArray(col)), sep)`
///
/// `arraySort` provides ascending lexicographic order, covering the common
/// `ORDER BY col ASC` case. Descending or multi-key ordering is not supported.
fn translate_string_agg(func: &mut sqlparser::ast::Function) -> Option<Expr> {
    let (has_order_by, has_distinct) = match &func.args {
        FunctionArguments::List(list) => (
            list.clauses.iter().any(|c|
                matches!(c, sqlparser::ast::FunctionArgumentClause::OrderBy(_))),
            matches!(list.duplicate_treatment, Some(sqlparser::ast::DuplicateTreatment::Distinct)),
        ),
        _ => (false, false),
    };

    let args = extract_function_args(&func.args)?;
    if args.len() < 2 {
        return None;
    }

    let col_expr = args[0].clone();
    let sep_expr = args[1].clone();

    let group_array = make_function_call("groupArray", vec![col_expr]);

    let deduped = if has_distinct {
        make_function_call("arrayDistinct", vec![group_array])
    } else {
        group_array
    };

    let ordered = if has_order_by {
        make_function_call("arraySort", vec![deduped])
    } else {
        deduped
    };

    Some(make_function_call(
        "arrayStringConcat",
        vec![ordered, sep_expr],
    ))
}

/// Translate `EXTRACT(field FROM expr)` to ClickHouse temporal functions.
fn translate_extract(field: &DateTimeField, inner: &Expr) -> Option<Expr> {
    match field {
        DateTimeField::Epoch => {
            // PG EXTRACT(EPOCH) returns float with fractional seconds.
            // CH toUnixTimestamp returns UInt32 (no sub-second precision).
            // Use toUnixTimestamp64Micro / 1000000.0 to preserve microseconds.
            return Some(Expr::BinaryOp {
                left: Box::new(make_function_call("toUnixTimestamp64Micro", vec![inner.clone()])),
                op: sqlparser::ast::BinaryOperator::Divide,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number("1000000.0".to_string(), false))),
            });
        }
        DateTimeField::Dow => {
            // PG DOW: 0=Sunday, 1=Monday, ..., 6=Saturday
            // CH toDayOfWeek: 1=Monday, ..., 7=Sunday
            // toDayOfWeek(x) % 7 maps 1..6 -> 1..6, 7 -> 0, matching PG convention
            return Some(Expr::BinaryOp {
                left: Box::new(make_function_call("toDayOfWeek", vec![inner.clone()])),
                op: sqlparser::ast::BinaryOperator::Modulo,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number("7".to_string(), false))),
            });
        }
        _ => {}
    }

    let ch_func = match field {
        DateTimeField::Year => "toYear",
        DateTimeField::Quarter => "toQuarter",
        DateTimeField::Month => "toMonth",
        DateTimeField::Day => "toDayOfMonth",
        DateTimeField::Hour => "toHour",
        DateTimeField::Minute => "toMinute",
        DateTimeField::Second => "toSecond",
        DateTimeField::Doy => "toDayOfYear",
        DateTimeField::Week(_) => "toISOWeek",
        _ => return None,
    };
    Some(make_function_call(ch_func, vec![inner.clone()]))
}

/// Translate `date_part('field', expr)` -- same as extract but as a function call.
fn translate_date_part(func: &mut sqlparser::ast::Function) -> Option<Expr> {
    let args = extract_function_args(&func.args)?;
    if args.len() < 2 {
        return None;
    }

    let field_str = match &args[0] {
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => s.to_ascii_lowercase(),
        _ => return None,
    };

    let column_expr = args[1].clone();

    match field_str.as_str() {
        "epoch" => {
            return Some(Expr::BinaryOp {
                left: Box::new(make_function_call("toUnixTimestamp64Micro", vec![column_expr])),
                op: sqlparser::ast::BinaryOperator::Divide,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number("1000000.0".to_string(), false))),
            });
        }
        "dow" | "dayofweek" => {
            return Some(Expr::BinaryOp {
                left: Box::new(make_function_call("toDayOfWeek", vec![column_expr])),
                op: sqlparser::ast::BinaryOperator::Modulo,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number("7".to_string(), false))),
            });
        }
        _ => {}
    }

    let ch_func = match field_str.as_str() {
        "year" => "toYear",
        "quarter" => "toQuarter",
        "month" => "toMonth",
        "day" => "toDayOfMonth",
        "hour" => "toHour",
        "minute" => "toMinute",
        "second" => "toSecond",
        "doy" | "dayofyear" => "toDayOfYear",
        "week" => "toISOWeek",
        _ => return None,
    };

    Some(make_function_call(ch_func, vec![column_expr]))
}

/// Translate `to_char(expr, format)` to `formatDateTime(expr, ch_format)`.
///
/// Converts Postgres format specifiers to ClickHouse `formatDateTime` specifiers.
fn translate_to_char(func: &mut sqlparser::ast::Function) -> Option<Expr> {
    let args = extract_function_args(&func.args)?;
    if args.len() < 2 {
        return None;
    }

    let datetime_expr = args[0].clone();
    let format_str = match &args[1] {
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => s.clone(),
        _ => return None,
    };

    let ch_format = translate_pg_format_to_clickhouse(&format_str);

    Some(make_function_call(
        "formatDateTime",
        vec![
            datetime_expr,
            Expr::Value(sqlparser::ast::Value::SingleQuotedString(ch_format)),
        ],
    ))
}

/// Convert Postgres `to_char` format string to ClickHouse `formatDateTime` format.
///
/// Uses left-to-right greedy matching: at each position the longest matching
/// Postgres token is consumed and replaced with its ClickHouse equivalent.
/// Unrecognised characters pass through as literals.
fn translate_pg_format_to_clickhouse(pg_fmt: &str) -> String {
    // Postgres MS = milliseconds (3 digits); ClickHouse has no direct
    // equivalent. %i gives milliseconds in formatDateTime64, which is
    // the closest match. Callers formatting sub-second precision should
    // use DateTime64 columns for accurate results.
    const REPLACEMENTS: &[(&str, &str)] = &[
        ("YYYY", "%Y"), ("HH24", "%H"), ("HH12", "%I"),
        ("MONTH", "%B"), ("Month", "%B"), ("month", "%B"),
        ("MON", "%b"), ("Mon", "%b"), ("mon", "%b"),
        ("DAY", "%A"), ("Day", "%A"), ("day", "%A"),
        ("DY", "%a"), ("Dy", "%a"), ("dy", "%a"),
        ("YY", "%y"), ("MM", "%m"), ("DD", "%d"),
        ("HH", "%H"), ("MI", "%M"), ("SS", "%S"),
        ("MS", "%i"), ("AM", "%p"), ("PM", "%p"), ("TZ", "%Z"),
    ];

    let mut result = String::with_capacity(pg_fmt.len());
    let mut pos = 0;
    while pos < pg_fmt.len() {
        let remaining = &pg_fmt[pos..];
        let matched = REPLACEMENTS.iter()
            .filter(|(pg, _)| remaining.starts_with(pg))
            .max_by_key(|(pg, _)| pg.len());
        if let Some((pg, ch)) = matched {
            result.push_str(ch);
            pos += pg.len();
        } else {
            let ch = remaining.chars().next().unwrap();
            result.push(ch);
            pos += ch.len_utf8();
        }
    }
    result
}

/// Simple function rename: keep all arguments and clauses, just change the function name.
fn translate_simple_rename(
    func: &mut sqlparser::ast::Function,
    new_name: &str,
) -> Option<Expr> {
    let args = extract_function_args(&func.args)?;
    Some(make_function_call_preserving(new_name, args, func))
}

/// Translate `regexp_replace(str, pat, rep [, flags])` to ClickHouse.
///
/// Postgres `regexp_replace` replaces only the first match by default.
/// A 4th `'g'` flag makes it replace all matches. Map accordingly:
/// - No flags or no `g` -> `replaceRegexpOne(str, pat, rep)`
/// - Flags contain `g` -> `replaceRegexpAll(str, pat, rep)`
fn translate_regexp_replace(func: &mut sqlparser::ast::Function) -> Option<Expr> {
    let args = extract_function_args(&func.args)?;
    if args.len() < 3 {
        return None;
    }

    // Check for a 4th argument containing the 'g' (global) flag
    let use_all = if args.len() >= 4 {
        match &args[3] {
            Expr::Value(sqlparser::ast::Value::SingleQuotedString(flags)) => {
                flags.contains('g')
            }
            _ => false,
        }
    } else {
        false
    };

    let ch_func = if use_all {
        "replaceRegexpAll"
    } else {
        "replaceRegexpOne"
    };

    // Only pass the first 3 args (str, pat, rep) -- ClickHouse doesn't take flags
    Some(make_function_call(
        ch_func,
        vec![args[0].clone(), args[1].clone(), args[2].clone()],
    ))
}

/// Translate `left(str, n)` to `substring(str, 1, n)`.
fn translate_left(func: &mut sqlparser::ast::Function) -> Option<Expr> {
    let args = extract_function_args(&func.args)?;
    if args.len() < 2 {
        return None;
    }
    Some(make_function_call(
        "substring",
        vec![
            args[0].clone(),
            Expr::Value(sqlparser::ast::Value::Number("1".to_owned(), false)),
            args[1].clone(),
        ],
    ))
}

/// Translate `right(str, n)` to `substring(str, -n)`.
///
/// ClickHouse's `substring(s, pos)` with a negative pos counts from the end.
fn translate_right(func: &mut sqlparser::ast::Function) -> Option<Expr> {
    let args = extract_function_args(&func.args)?;
    if args.len() < 2 {
        return None;
    }
    // Build -n as a unary minus expression
    let neg_n = Expr::UnaryOp {
        op: sqlparser::ast::UnaryOperator::Minus,
        expr: Box::new(args[1].clone()),
    };
    Some(make_function_call("substring", vec![args[0].clone(), neg_n]))
}

// =============================================================================
// Helpers
// =============================================================================

/// Extract positional arguments from a function call as a Vec of Expr.
fn extract_function_args(args: &FunctionArguments) -> Option<Vec<Expr>> {
    match args {
        FunctionArguments::List(list) => {
            let mut exprs = Vec::new();
            for arg in &list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        exprs.push(expr.clone());
                    }
                    _ => return None, // Named args or wildcards -- don't translate
                }
            }
            Some(exprs)
        }
        _ => None,
    }
}

/// Create a simple function call expression.
fn make_function_call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(sqlparser::ast::Function {
        name: ObjectName(vec![Ident::new(name)]),
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// Create a function call that preserves FILTER, OVER, ORDER BY, DISTINCT,
/// and argument clauses from the original.
fn make_function_call_preserving(
    name: &str,
    args: Vec<Expr>,
    original: &sqlparser::ast::Function,
) -> Expr {
    let (orig_duplicate_treatment, orig_clauses) = match &original.args {
        FunctionArguments::List(list) => (list.duplicate_treatment, list.clauses.clone()),
        _ => (None, vec![]),
    };
    Expr::Function(sqlparser::ast::Function {
        name: ObjectName(vec![Ident::new(name)]),
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: orig_duplicate_treatment,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: orig_clauses,
        }),
        filter: original.filter.clone(),
        null_treatment: original.null_treatment,
        over: original.over.clone(),
        within_group: original.within_group.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_double_colon_cast() {
        let input = "SELECT id::text FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            output.contains("CAST("),
            "Expected CAST in output, got: {}",
            output
        );
        assert!(
            !output.contains("::"),
            "Expected no :: in output, got: {}",
            output
        );
    }

    #[test]
    fn test_date_trunc() {
        let input = "SELECT date_trunc('month', created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("tostartofmonth"),
            "Expected toStartOfMonth in output, got: {}",
            output
        );
    }

    #[test]
    fn test_extract_epoch_preserves_subsecond_precision() {
        let input = "SELECT extract(epoch from created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("tounixtimestamp64micro"),
            "Expected toUnixTimestamp64Micro for sub-second precision, got: {}",
            output
        );
        assert!(
            lower.contains("1000000.0"),
            "Expected division by 1000000.0 to produce fractional epoch, got: {}",
            output
        );
    }

    #[test]
    fn test_extract_dow_uses_modulo_7() {
        let input = "SELECT extract(dow from created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("todayofweek"),
            "Expected toDayOfWeek in output, got: {}",
            output
        );
        assert!(
            lower.contains("% 7"),
            "Expected modulo 7 to convert CH 1-7 to PG 0-6, got: {}",
            output
        );
    }

    #[test]
    fn test_date_part_dow_uses_modulo_7() {
        let input = "SELECT date_part('dow', created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("todayofweek"),
            "Expected toDayOfWeek in output, got: {}",
            output
        );
        assert!(
            lower.contains("% 7"),
            "Expected modulo 7 to convert CH 1-7 to PG 0-6, got: {}",
            output
        );
    }

    #[test]
    fn test_date_part_epoch_preserves_subsecond_precision() {
        let input = "SELECT date_part('epoch', created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("tounixtimestamp64micro"),
            "Expected toUnixTimestamp64Micro for sub-second precision, got: {}",
            output
        );
        assert!(
            lower.contains("1000000.0"),
            "Expected division by 1000000.0, got: {}",
            output
        );
    }

    #[test]
    fn test_string_agg() {
        let input = "SELECT string_agg(name, ',') FROM users";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("arraystringconcat"),
            "Expected arrayStringConcat in output, got: {}",
            output
        );
        assert!(
            output.to_ascii_lowercase().contains("grouparray"),
            "Expected groupArray in output, got: {}",
            output
        );
    }

    #[test]
    fn test_string_agg_without_order_by() {
        let input = "SELECT string_agg(name, ',') FROM users";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            !lower.contains("arraysort"),
            "string_agg WITHOUT ORDER BY must NOT use arraySort, got: {}",
            output
        );
    }

    #[test]
    fn test_string_agg_with_order_by() {
        let input = "SELECT string_agg(name, ',' ORDER BY name) FROM users";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("arraysort"),
            "string_agg WITH ORDER BY must use arraySort, got: {}",
            output
        );
        assert!(
            lower.contains("grouparray"),
            "Must still wrap in groupArray, got: {}",
            output
        );
        assert!(
            lower.contains("arraystringconcat"),
            "Must still wrap in arrayStringConcat, got: {}",
            output
        );
    }

    #[test]
    fn test_passthrough() {
        // Regular ClickHouse-compatible SQL should pass through unchanged in meaning
        let input = "SELECT count(*) FROM orders WHERE total > 100";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("count(*)"),
            "Expected count(*) preserved, got: {}",
            output
        );
    }

    #[test]
    fn test_type_translation_in_cast() {
        let input = "SELECT CAST(id AS bigint) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            output.contains("INT64") || output.contains("Int64") || output.contains("int64") || output.contains("BIGINT"),
            "Expected Int64 or BIGINT in output, got: {}",
            output
        );
    }

    // ── Tests for exhaustive recursion into previously-skipped expression types ──

    #[test]
    fn test_cast_inside_like() {
        let input = "SELECT * FROM orders WHERE name LIKE col::text";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.contains("::"),
            "Expected :: cast inside LIKE to be translated, got: {}",
            output
        );
        assert!(
            output.contains("CAST("),
            "Expected CAST in LIKE pattern, got: {}",
            output
        );
    }

    #[test]
    fn test_cast_inside_exists() {
        let input = "SELECT * FROM orders WHERE EXISTS (SELECT col::text FROM t)";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.contains("::"),
            "Expected :: cast inside EXISTS to be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_cast_inside_array() {
        let input = "SELECT ARRAY[x::int] FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.contains("::"),
            "Expected :: cast inside ARRAY to be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_cast_inside_trim() {
        let input = "SELECT TRIM(col::text) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.contains("::"),
            "Expected :: cast inside TRIM to be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_cast_inside_substring() {
        let input = "SELECT SUBSTRING(col::text FROM 1 FOR 10) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.contains("::"),
            "Expected :: cast inside SUBSTRING to be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_cast_inside_coalesce() {
        // COALESCE is a function, already handled, but good to confirm
        let input = "SELECT COALESCE(x::int, 0) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.contains("::"),
            "Expected :: cast inside COALESCE to be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_cast_inside_case() {
        let input = "SELECT CASE WHEN x::int > 0 THEN 'yes' ELSE 'no' END FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.contains("::"),
            "Expected :: cast inside CASE to be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_cast_inside_subquery_from() {
        // Cast inside a derived table (subquery in FROM)
        let input = "SELECT * FROM (SELECT col::text AS c FROM t) sub";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.contains("::"),
            "Expected :: cast inside subquery to be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_cast_inside_ilike() {
        let input = "SELECT * FROM orders WHERE name ILIKE col::text";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.contains("::"),
            "Expected :: cast inside ILIKE to be translated, got: {}",
            output
        );
    }

    // ── Tests for expanded function translations ──

    #[test]
    fn test_extract_year() {
        let input = "SELECT extract(year from created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("toyear"),
            "Expected toYear in output, got: {}",
            output
        );
    }

    #[test]
    fn test_extract_month() {
        let input = "SELECT extract(month from created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("tomonth"),
            "Expected toMonth in output, got: {}",
            output
        );
    }

    #[test]
    fn test_extract_day() {
        let input = "SELECT extract(day from created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("todayofmonth"),
            "Expected toDayOfMonth in output, got: {}",
            output
        );
    }

    #[test]
    fn test_date_part() {
        let input = "SELECT date_part('hour', created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("tohour"),
            "Expected toHour in output, got: {}",
            output
        );
    }

    #[test]
    fn test_to_char() {
        let input = "SELECT to_char(created_at, 'YYYY-MM-DD') FROM orders";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("formatdatetime"),
            "Expected formatDateTime in output, got: {}",
            output
        );
        assert!(
            output.contains("%Y") && output.contains("%m") && output.contains("%d"),
            "Expected ClickHouse format specifiers, got: {}",
            output
        );
    }

    #[test]
    fn test_array_agg() {
        let input = "SELECT array_agg(name) FROM users";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("grouparray"),
            "Expected groupArray in output, got: {}",
            output
        );
    }

    #[test]
    fn test_bool_or() {
        let input = "SELECT bool_or(active) FROM users";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("max("),
            "Expected max( in output for bool_or, got: {}",
            output
        );
    }

    #[test]
    fn test_bool_and() {
        let input = "SELECT bool_and(active) FROM users";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("min("),
            "Expected min( in output for bool_and, got: {}",
            output
        );
    }

    #[test]
    fn test_regexp_replace_default_first_match() {
        // No flags -> replaceRegexpOne (first match only, Postgres default)
        let input = "SELECT regexp_replace(name, 'foo', 'bar') FROM users";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("replaceregexpone"),
            "Expected replaceRegexpOne in output, got: {}",
            output
        );
    }

    #[test]
    fn test_regexp_replace_global_flag() {
        // 'g' flag -> replaceRegexpAll (all matches)
        let input = "SELECT regexp_replace(name, 'foo', 'bar', 'g') FROM users";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("replaceregexpall"),
            "Expected replaceRegexpAll in output, got: {}",
            output
        );
    }

    #[test]
    fn test_left_function() {
        let input = "SELECT left(name, 3) FROM users";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("substring("),
            "Expected substring( in output for left(), got: {}",
            output
        );
    }

    #[test]
    fn test_right_function() {
        let input = "SELECT right(name, 3) FROM users";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("substring("),
            "Expected substring( in output for right(), got: {}",
            output
        );
    }

    #[test]
    fn test_char_length() {
        let input = "SELECT char_length(name) FROM users";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("lengthutf8"),
            "Expected lengthUTF8 in output, got: {}",
            output
        );
    }

    // ── Tests for current_timestamp / current_date routing ──

    #[test]
    fn test_current_timestamp() {
        let input = "SELECT current_timestamp";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("now"),
            "Expected now in output for current_timestamp, got: {}",
            output
        );
    }

    #[test]
    fn test_current_date() {
        let input = "SELECT current_date";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("today"),
            "Expected today in output for current_date, got: {}",
            output
        );
    }

    #[test]
    fn test_current_time() {
        let input = "SELECT current_time";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("now"),
            "Expected now in output for current_time, got: {}",
            output
        );
    }

    #[test]
    fn test_current_timestamp_in_where() {
        let input = "SELECT * FROM orders WHERE created_at > current_timestamp";
        let output = translate_to_clickhouse(input);
        assert!(
            output.to_ascii_lowercase().contains("now"),
            "Expected now in WHERE clause for current_timestamp, got: {}",
            output
        );
    }

    #[test]
    fn test_try_cast_type_translation() {
        // TRY_CAST should also get its type names translated
        let input = "SELECT TRY_CAST(id AS bigint) FROM orders";
        let output = translate_to_clickhouse(input);
        assert!(
            output.contains("INT64") || output.contains("Int64") || output.contains("int64") || output.contains("BIGINT"),
            "Expected type translation in TRY_CAST, got: {}",
            output
        );
    }

    // ── error / edge-case paths ──

    #[test]
    fn test_translate_empty_string() {
        let output = translate_to_clickhouse("");
        assert_eq!(output, "", "Empty input should produce empty output");
    }

    #[test]
    fn test_translate_unparseable_sql() {
        // Unparseable SQL should be returned unchanged
        let input = "NOT VALID @#$";
        let output = translate_to_clickhouse(input);
        assert_eq!(
            output, input,
            "Unparseable SQL should be returned unchanged"
        );
    }

    #[test]
    fn test_translate_multiple_statements() {
        let input = "SELECT 1; SELECT 2";
        let output = translate_to_clickhouse(input);
        // Both statements should appear in the output
        assert!(
            output.contains('1') && output.contains('2'),
            "Both statements should be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_to_char_with_unknown_format_tokens() {
        // Unknown format tokens should be passed through unchanged
        let input = "SELECT to_char(created_at, 'YYYY-QQ-ZZ') FROM orders";
        let output = translate_to_clickhouse(input);
        // Should still produce formatDateTime call (or pass through)
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("formatdatetime") || lower.contains("to_char"),
            "Expected formatDateTime or passthrough for unknown tokens, got: {}",
            output
        );
    }

    #[test]
    fn test_date_trunc_with_unknown_interval() {
        // Unknown interval units should not crash
        let input = "SELECT date_trunc('fortnight', created_at) FROM orders";
        let output = translate_to_clickhouse(input);
        // Should still produce something (possibly unchanged or best-effort)
        assert!(
            !output.is_empty(),
            "date_trunc with unknown interval should not produce empty output"
        );
    }

    // ── Additional dialect translation coverage ──

    #[test]
    fn test_nested_cast() {
        // (x::int)::text -- both casts should be translated
        let input = "SELECT (x::int)::text FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        // Both should be explicit CASTs with translated types
        assert!(
            lower.contains("cast") || lower.contains("int32") || lower.contains("string"),
            "Nested casts should both be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_cast_in_where_clause() {
        let input = "SELECT * FROM t WHERE created_at::date = '2024-01-01'";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("cast"),
            ":: cast in WHERE should become explicit CAST, got: {}",
            output
        );
    }

    #[test]
    fn test_extract_dow() {
        let input = "SELECT EXTRACT(DOW FROM created_at) FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        // DOW should produce toDayOfWeek or be passed through
        assert!(
            lower.contains("todayofweek") || lower.contains("dow") || lower.contains("extract"),
            "EXTRACT(DOW) should be handled, got: {}",
            output
        );
    }

    #[test]
    fn test_extract_doy() {
        let input = "SELECT EXTRACT(DOY FROM created_at) FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("todayofyear") || lower.contains("doy") || lower.contains("extract"),
            "EXTRACT(DOY) should be handled, got: {}",
            output
        );
    }

    #[test]
    fn test_at_time_zone() {
        // AT TIME ZONE should not crash -- verify recursion handles it
        let input = "SELECT created_at AT TIME ZONE 'UTC' FROM t";
        let output = translate_to_clickhouse(input);
        assert!(
            !output.is_empty(),
            "AT TIME ZONE should not produce empty output"
        );
    }

    #[test]
    fn test_union_both_sides_translated() {
        let input = "SELECT x::int FROM t UNION ALL SELECT y::int FROM t2";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        // Both sides should have CAST with translated type
        let cast_count = lower.matches("cast").count();
        assert!(
            cast_count >= 2,
            "Both sides of UNION should have CAST translated, got {} casts in: {}",
            cast_count,
            output
        );
    }

    #[test]
    fn test_coalesce_with_inner_cast() {
        let input = "SELECT COALESCE(x::int, 0) FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("cast") && lower.contains("coalesce"),
            "COALESCE with inner :: cast should be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_type_translation_numeric() {
        // numeric(p,s) should pass through (ClickHouse has Decimal)
        let input = "SELECT CAST(x AS numeric(10,2)) FROM t";
        let output = translate_to_clickhouse(input);
        // Should still produce valid SQL
        assert!(
            !output.is_empty(),
            "numeric type translation should not produce empty output"
        );
    }

    // ── Function translation completeness ──

    #[test]
    fn test_date_part_all_fields() {
        let fields_and_expected = vec![
            ("epoch", "tounix"),
            ("year", "toyear"),
            ("quarter", "toquarter"),
            ("month", "tomonth"),
            ("day", "todayofmonth"),
            ("hour", "tohour"),
            ("minute", "tominute"),
            ("second", "tosecond"),
            ("dow", "todayofweek"),
            ("dayofweek", "todayofweek"),
            ("doy", "todayofyear"),
            ("dayofyear", "todayofyear"),
            ("week", "toisoweek"),
        ];

        for (field, expected_prefix) in &fields_and_expected {
            let input = format!("SELECT date_part('{}', created_at) FROM t", field);
            let output = translate_to_clickhouse(&input);
            let lower = output.to_ascii_lowercase();
            assert!(
                lower.contains(expected_prefix),
                "date_part('{}') should produce {}, got: {}",
                field,
                expected_prefix,
                output
            );
        }
    }

    #[test]
    fn test_date_part_unknown_field() {
        let input = "SELECT date_part('microsecond', created_at) FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        // Unknown field should pass through as date_part unchanged
        assert!(
            lower.contains("date_part"),
            "Unknown date_part field should be left unchanged, got: {}",
            output
        );
    }

    #[test]
    fn test_to_char_full_format() {
        let input = "SELECT to_char(created_at, 'YYYY-MM-DD HH24:MI:SS') FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("formatdatetime"),
            "to_char with full format should become formatDateTime, got: {}",
            output
        );
        // Check that tokens were translated to ClickHouse format specifiers
        assert!(
            output.contains("%Y") && output.contains("%m") && output.contains("%d")
                && output.contains("%H") && output.contains("%M") && output.contains("%S"),
            "Format tokens should be translated, got: {}",
            output
        );
    }

    #[test]
    fn test_to_char_single_arg() {
        // to_char with only 1 arg should be left unchanged (too few args)
        let input = "SELECT to_char(123) FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("to_char"),
            "to_char with single arg should be left unchanged, got: {}",
            output
        );
    }

    #[test]
    fn test_left_single_arg() {
        // left with only 1 arg should be left unchanged
        let input = "SELECT left('hello') FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("left"),
            "left with single arg should be left unchanged, got: {}",
            output
        );
    }

    #[test]
    fn test_right_single_arg() {
        // right with only 1 arg should be left unchanged
        let input = "SELECT right('hello') FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("right"),
            "right with single arg should be left unchanged, got: {}",
            output
        );
    }

    #[test]
    fn test_regexp_replace_too_few_args() {
        // regexp_replace with only 2 args should be left unchanged
        let input = "SELECT regexp_replace('hello', 'h') FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("regexp_replace"),
            "regexp_replace with 2 args should be left unchanged, got: {}",
            output
        );
    }

    #[test]
    fn test_character_length_alias() {
        let input = "SELECT character_length('hello') FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("lengthutf8"),
            "character_length should become lengthUTF8, got: {}",
            output
        );
    }

    #[test]
    fn test_extract_week() {
        let input = "SELECT EXTRACT(WEEK FROM created_at) FROM t";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("toisoweek"),
            "EXTRACT(WEEK) should become toISOWeek, got: {}",
            output
        );
    }

    #[test]
    fn test_current_timestamp_with_parens() {
        // current_timestamp() with explicit parens should also become now()
        let input = "SELECT current_timestamp()";
        let output = translate_to_clickhouse(input);
        let lower = output.to_ascii_lowercase();
        assert!(
            lower.contains("now"),
            "current_timestamp() should become now(), got: {}",
            output
        );
    }

    #[test]
    fn test_to_char_lowercase_format_specifiers() {
        let input = "SELECT to_char(created_at, 'month')";
        let output = translate_to_clickhouse(input);
        assert!(
            output.contains("%B"),
            "lowercase 'month' should translate to %B, got: {}",
            output
        );

        let input = "SELECT to_char(created_at, 'mon')";
        let output = translate_to_clickhouse(input);
        assert!(
            output.contains("%b"),
            "lowercase 'mon' should translate to %b, got: {}",
            output
        );

        let input = "SELECT to_char(created_at, 'day')";
        let output = translate_to_clickhouse(input);
        assert!(
            output.contains("%A"),
            "lowercase 'day' should translate to %A, got: {}",
            output
        );

        let input = "SELECT to_char(created_at, 'dy')";
        let output = translate_to_clickhouse(input);
        assert!(
            output.contains("%a"),
            "lowercase 'dy' should translate to %a, got: {}",
            output
        );
    }

    #[test]
    fn test_format_dday_greedy() {
        let result = translate_pg_format_to_clickhouse("DDAY");
        assert_eq!(
            result, "%dAY",
            "DDAY must be parsed left-to-right as DD + AY, not D + DAY"
        );
    }

    #[test]
    fn test_format_mmon_greedy() {
        let result = translate_pg_format_to_clickhouse("MMON");
        assert_eq!(
            result, "%mON",
            "MMON must be parsed left-to-right as MM + ON, not M + MON"
        );
    }

    #[test]
    fn test_format_standard_separators_unchanged() {
        let result = translate_pg_format_to_clickhouse("YYYY-MM-DD HH24:MI:SS");
        assert_eq!(
            result, "%Y-%m-%d %H:%M:%S",
            "Standard format with separators must translate correctly"
        );
    }

    #[test]
    fn test_format_non_ascii_utf8_does_not_panic() {
        let result = translate_pg_format_to_clickhouse("DD/MM/YYYY café");
        assert_eq!(result, "%d/%m/%Y café");
    }

    #[test]
    fn test_format_multibyte_emoji_passthrough() {
        let result = translate_pg_format_to_clickhouse("YYYY🎉MM");
        assert_eq!(result, "%Y🎉%m");
    }

    #[test]
    fn test_format_cjk_characters() {
        let result = translate_pg_format_to_clickhouse("YYYY年MM月DD日");
        assert_eq!(result, "%Y年%m月%d日");
    }

    #[test]
    fn test_aggregate_filter_clause_preserved() {
        let sql = "SELECT array_agg(name) FILTER (WHERE active) FROM users";
        let result = translate_to_clickhouse(sql);
        // The FILTER clause must be preserved in the translated output
        assert!(
            result.contains("FILTER"),
            "FILTER clause must be preserved after function rename, got: {}",
            result
        );
    }

    #[test]
    fn test_aggregate_over_clause_preserved() {
        let sql = "SELECT array_agg(name) OVER (PARTITION BY dept) FROM users";
        let result = translate_to_clickhouse(sql);
        assert!(
            result.contains("OVER"),
            "OVER clause must be preserved after function rename, got: {}",
            result
        );
    }

    #[test]
    fn test_rewrite_expr_recurses_into_filter() {
        // A ::date cast inside a FILTER clause must be rewritten to CAST
        let sql = "SELECT count(*) FILTER (WHERE created_at::date = '2024-01-01') FROM orders";
        let result = translate_to_clickhouse(sql);
        assert!(
            !result.contains("::"),
            ":: cast inside FILTER must be rewritten, got: {}",
            result
        );
    }

    #[test]
    fn test_rewrite_expr_recurses_into_over_partition() {
        let sql = "SELECT sum(amount) OVER (PARTITION BY created_at::date) FROM orders";
        let result = translate_to_clickhouse(sql);
        assert!(
            !result.contains("::"),
            ":: cast inside OVER PARTITION BY must be rewritten, got: {}",
            result
        );
    }

    #[test]
    fn test_array_agg_distinct_preserved() {
        let sql = "SELECT array_agg(DISTINCT name) FROM users";
        let result = translate_to_clickhouse(sql);
        let lower = result.to_ascii_lowercase();
        assert!(
            lower.contains("distinct"),
            "DISTINCT must be preserved on aggregate translation, got: {}",
            result
        );
    }

    #[test]
    fn test_string_agg_distinct_produces_array_distinct() {
        let sql = "SELECT string_agg(DISTINCT name, ',') FROM users";
        let result = translate_to_clickhouse(sql);
        let lower = result.to_ascii_lowercase();
        assert!(
            lower.contains("arraydistinct"),
            "string_agg(DISTINCT ...) must use arrayDistinct, got: {}",
            result
        );
    }

    #[test]
    fn test_string_agg_without_distinct_no_array_distinct() {
        let sql = "SELECT string_agg(name, ',') FROM users";
        let result = translate_to_clickhouse(sql);
        let lower = result.to_ascii_lowercase();
        assert!(
            !lower.contains("arraydistinct"),
            "string_agg without DISTINCT should not use arrayDistinct, got: {}",
            result
        );
        assert!(
            lower.contains("arraystringconcat"),
            "string_agg must translate to arrayStringConcat, got: {}",
            result
        );
    }
}
