// PromQL AST to DataFusion LogicalPlan planner.
// Inspired by GreptimeDB's planner, but simplified for our schema (samples_v1 + time_series_v1).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{
    self, col, lit, Extension, LogicalPlan, LogicalPlanBuilder, SortExpr,
};
use datafusion::prelude::*;
use futures_util::FutureExt;
use promql_parser::label::{MatchOp, Matcher, METRIC_NAME};
use promql_parser::parser::{self as prom, token, Expr as PromExpr};

use super::error::{EvalError, EvalResult};
use super::extension_plan::instant_manipulate::InstantManipulate;
use super::extension_plan::range_manipulate::{Millisecond, RangeManipulate};
use super::extension_plan::series_divide::SeriesDivide;
use super::extension_plan::series_normalize::SeriesNormalize;
use super::functions;

/// Column names in our schema
pub const COL_TIMESTAMP: &str = "unix_milli";
pub const COL_VALUE: &str = "value";
pub const COL_FINGERPRINT: &str = "fingerprint";
pub const COL_LABELS: &str = "labels";

/// Default Prometheus lookback delta (5 minutes) — used for instant-selector
/// staleness and as the minimum ClickHouse fetch extension.
pub const DEFAULT_LOOKBACK_DELTA_MS: i64 = 5 * 60 * 1000;

const DEFAULT_LOOKBACK_DELTA: i64 = DEFAULT_LOOKBACK_DELTA_MS;

/// Evaluation context — holds the time range and step info for the query.
#[derive(Debug, Clone)]
pub struct EvalContext {
    pub start: Millisecond,
    pub end: Millisecond,
    pub interval: Millisecond,
    pub lookback_delta: Millisecond,
}

impl EvalContext {
    pub fn new(start: Millisecond, end: Millisecond, step_ms: Millisecond) -> Self {
        Self {
            start,
            end,
            interval: step_ms,
            lookback_delta: DEFAULT_LOOKBACK_DELTA,
        }
    }
}

/// The PromQL planner converts a parsed PromQL AST into a DataFusion LogicalPlan.
pub struct PromPlanner {
    ctx: EvalContext,
}

impl PromPlanner {
    pub fn new(ctx: EvalContext) -> Self {
        Self { ctx }
    }

    /// Main entry point: plan a PromQL expression into a DataFusion LogicalPlan.
    pub fn plan(&self, expr: &PromExpr, session: &SessionContext) -> EvalResult<LogicalPlan> {
        match expr {
            PromExpr::VectorSelector(vs) => {
                let offset = vs.offset.as_ref().map(|d| duration_to_ms(d));
                self.plan_vector_selector(vs, session, offset)
            }
            PromExpr::MatrixSelector(ms) => self.plan_matrix_selector(ms, session),
            PromExpr::Call(call) => self.plan_call(call, session),
            PromExpr::Aggregate(agg) => self.plan_aggregate(agg, session),
            PromExpr::Binary(bin) => self.plan_binary(bin, session),
            PromExpr::Paren(paren) => self.plan(&paren.expr, session),
            PromExpr::NumberLiteral(n) => self.plan_number_literal(n.val),
            PromExpr::StringLiteral(_) => Err(EvalError::Unsupported("string literals".into())),
            PromExpr::Unary(u) => self.plan_unary(u, session),
            PromExpr::Subquery(sq) => self.plan_subquery(sq, session),
            PromExpr::Extension(_) => Err(EvalError::Unsupported("extension expressions".into())),
        }
    }

    /// Plan a VectorSelector: scan → filter → sort → SeriesDivide → InstantManipulate
    fn plan_vector_selector(
        &self,
        vs: &prom::VectorSelector,
        session: &SessionContext,
        offset: Option<Millisecond>,
    ) -> EvalResult<LogicalPlan> {
        let metric_names = self.resolve_metric_names(vs)?;
        let (_matcher_tags, filters) = self.build_label_filters(&vs.matchers.matchers);
        let offset_ms = offset.unwrap_or(0);

        let mut sub_plans = Vec::new();
        for name in &metric_names {
            let scan = match self.build_table_scan(session, Some(name.as_str()), &filters) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(metric = %name, error = %e, "build_table_scan failed for vector selector");
                    continue;
                }
            };
            let divide_columns = self.series_divide_columns(&scan);
            let sorted = self.apply_sort(scan, &divide_columns)?;

            let series_divide = LogicalPlan::Extension(Extension {
                node: Arc::new(SeriesDivide::new(
                    divide_columns.clone(),
                    COL_TIMESTAMP.to_string(),
                    sorted,
                )),
            });

            let normalize = if offset_ms != 0 {
                LogicalPlan::Extension(Extension {
                    node: Arc::new(SeriesNormalize::new(
                        offset_ms,
                        COL_TIMESTAMP.to_string(),
                        Some(COL_VALUE.to_string()),
                        series_divide,
                    )),
                })
            } else {
                series_divide
            };

            let instant = LogicalPlan::Extension(Extension {
                node: Arc::new(InstantManipulate::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.lookback_delta,
                    self.ctx.interval,
                    COL_TIMESTAMP.to_string(),
                    Some(COL_VALUE.to_string()),
                    normalize,
                )),
            });
            sub_plans.push(instant);
        }

        self.union_plans(sub_plans)
    }

    /// Plan a MatrixSelector: scan → filter → sort → SeriesDivide → RangeManipulate
    fn plan_matrix_selector(
        &self,
        ms: &prom::MatrixSelector,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        let vs = &ms.vs;
        let range_ms = ms.range.as_millis() as i64;
        let metric_names = self.resolve_metric_names(vs)?;
        let (_matcher_tags, filters) = self.build_label_filters(&vs.matchers.matchers);
        let offset_ms = vs.offset.as_ref().map(|d| duration_to_ms(d)).unwrap_or(0);

        let mut sub_plans = Vec::new();
        for name in &metric_names {
            let scan = match self.build_table_scan(session, Some(name.as_str()), &filters) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(metric = %name, error = %e, "build_table_scan failed for matrix selector");
                    continue;
                }
            };
            let divide_columns = self.series_divide_columns(&scan);
            let sorted = self.apply_sort(scan, &divide_columns)?;

            let series_divide = LogicalPlan::Extension(Extension {
                node: Arc::new(SeriesDivide::new(
                    divide_columns.clone(),
                    COL_TIMESTAMP.to_string(),
                    sorted,
                )),
            });

            let normalize = if offset_ms != 0 {
                LogicalPlan::Extension(Extension {
                    node: Arc::new(SeriesNormalize::new(
                        offset_ms,
                        COL_TIMESTAMP.to_string(),
                        Some(COL_VALUE.to_string()),
                        series_divide,
                    )),
                })
            } else {
                series_divide
            };

            let range_manipulate = RangeManipulate::new(
                self.ctx.start,
                self.ctx.end,
                self.ctx.interval,
                range_ms,
                COL_TIMESTAMP.to_string(),
                vec![COL_VALUE.to_string()],
                normalize,
            )
            .map_err(|e| EvalError::DataFusion(e))?;

            sub_plans.push(LogicalPlan::Extension(Extension {
                node: Arc::new(range_manipulate),
            }));
        }

        self.union_plans(sub_plans)
    }

    /// Plan a function call.
    fn plan_call(&self, call: &prom::Call, session: &SessionContext) -> EvalResult<LogicalPlan> {
        let func_name = call.func.name;

        match func_name {
            // Range functions (operate on matrix selectors / range vectors)
            "rate" | "increase" | "delta" | "irate" | "idelta" | "deriv" | "changes" | "resets"
            | "avg_over_time" | "min_over_time" | "max_over_time" | "sum_over_time"
            | "count_over_time" | "last_over_time" | "stddev_over_time" | "stdvar_over_time"
            | "absent_over_time" | "present_over_time" | "quantile_over_time" | "mad_over_time"
            | "predict_linear" => self.plan_range_function(call, session),
            // Constant-producing functions (no args or special handling)
            "vector" => self.plan_vector_function(call),
            "pi" => self.plan_number_literal(std::f64::consts::PI),
            "time" | "timestamp" => self.plan_time_function(),
            // Passthrough: scalar(vector) just returns the value
            "scalar" => {
                if call.args.args.is_empty() {
                    return Err(EvalError::Invalid("scalar() requires 1 argument".into()));
                }
                self.plan(call.args.args[0].as_ref(), session)
            }
            // absent(vector) — returns 1 if no series match, empty otherwise
            "absent" => self.plan_absent(call, session),
            // histogram_quantile(scalar, vector)
            "histogram_quantile" => self.plan_histogram_quantile(call, session),
            // Label manipulation
            "label_replace" => self.plan_label_replace(call, session),
            "label_join" => self.plan_label_join(call, session),
            // Sort functions — plan inner, the sort is a presentation detail
            "sort" | "sort_desc" | "sort_by_label" | "sort_by_label_desc" => {
                self.plan_sort_function(call, session)
            }
            // Math / trig instant functions
            "abs" | "ceil" | "floor" | "exp" | "ln" | "log2" | "log10" | "sqrt" | "cbrt"
            | "sgn" | "round" | "acos" | "acosh" | "asin" | "asinh" | "atan" | "atanh" | "cos"
            | "cosh" | "sin" | "sinh" | "tan" | "tanh" | "deg" | "rad" | "clamp" | "clamp_min"
            | "clamp_max" => self.plan_instant_function(call, session),
            _ => Err(EvalError::Unsupported(format!("function {}()", func_name))),
        }
    }

    /// Plan range functions (rate, increase, etc.): plan inner matrix → apply UDF.
    fn plan_range_function(
        &self,
        call: &prom::Call,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        if call.args.args.is_empty() {
            return Err(EvalError::Invalid(format!(
                "{}() requires at least 1 argument",
                call.func.name
            )));
        }

        // For subquery arguments like max_over_time((irate(m[2m]))[60s:15s]),
        // the inner plan is an already-evaluated instant vector — it has no
        // RangeManipulate and thus no unix_milli_range column. We need to:
        //   1. Evaluate the inner expression with an extended start time so that
        //      early windows of the outer RangeManipulate have sufficient data.
        //   2. Add sort → SeriesDivide → RangeManipulate so the outer range
        //      function gets proper time windows.
        let first_arg_unwrapped = Self::unwrap_paren(call.args.args[0].as_ref());
        let mut inner_plan = if let PromExpr::Subquery(sq) = first_arg_unwrapped {
            let range_ms = sq.range.as_millis() as i64;
            let sub_step = sq
                .step
                .as_ref()
                .map(|d| d.as_millis() as i64)
                .filter(|&s| s > 0)
                .unwrap_or(self.ctx.interval);

            // Extend start backwards by the subquery range so the outer
            // RangeManipulate has data for its earliest windows.
            let extended_start = self.ctx.start - range_ms;
            let sub_ctx = EvalContext {
                start: extended_start,
                end: self.ctx.end,
                interval: sub_step,
                lookback_delta: self.ctx.lookback_delta,
            };
            let sub_planner = PromPlanner::new(sub_ctx);
            let mut sub_plan = sub_planner.plan(&sq.expr, session)?;

            // Apply offset if present on the subquery
            let offset_ms = sq.offset.as_ref().map(|d| duration_to_ms(d)).unwrap_or(0);
            if offset_ms != 0 {
                let divide_columns = self.series_divide_columns(&sub_plan);
                let sorted = self.apply_sort(sub_plan, &divide_columns)?;
                let series_divide = LogicalPlan::Extension(Extension {
                    node: Arc::new(SeriesDivide::new(
                        divide_columns,
                        COL_TIMESTAMP.to_string(),
                        sorted,
                    )),
                });
                sub_plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(SeriesNormalize::new(
                        offset_ms,
                        COL_TIMESTAMP.to_string(),
                        Some(COL_VALUE.to_string()),
                        series_divide,
                    )),
                });
            }

            let divide_columns = self.series_divide_columns(&sub_plan);
            let sorted = self.apply_sort(sub_plan, &divide_columns)?;

            let series_divide = LogicalPlan::Extension(Extension {
                node: Arc::new(SeriesDivide::new(
                    divide_columns,
                    COL_TIMESTAMP.to_string(),
                    sorted,
                )),
            });

            let range_manipulate = RangeManipulate::new(
                self.ctx.start,
                self.ctx.end,
                self.ctx.interval,
                range_ms,
                COL_TIMESTAMP.to_string(),
                vec![COL_VALUE.to_string()],
                series_divide,
            )
            .map_err(EvalError::DataFusion)?;

            LogicalPlan::Extension(Extension {
                node: Arc::new(range_manipulate),
            })
        } else {
            self.plan(call.args.args[0].as_ref(), session)?
        };

        let ts_range_col = RangeManipulate::build_timestamp_range_name(COL_TIMESTAMP);

        let udf = self.get_range_udf(call.func.name)?;

        let mut udf_args = vec![
            logical_expr::Expr::Column(datafusion::common::Column::from_name(&ts_range_col)),
            logical_expr::Expr::Column(datafusion::common::Column::from_name(COL_VALUE)),
        ];

        match call.func.name {
            "rate" | "increase" | "delta" => {
                udf_args.push(logical_expr::Expr::Column(
                    datafusion::common::Column::from_name(COL_TIMESTAMP),
                ));
                let range_ms = self.extract_range_from_first_arg(call.args.args[0].as_ref());
                udf_args.push(lit(range_ms));
            }
            "quantile_over_time" => {
                if call.args.args.len() >= 2 {
                    if let PromExpr::NumberLiteral(n) = call.args.args[1].as_ref() {
                        udf_args.push(lit(n.val));
                    }
                }
            }
            "predict_linear" => {
                if call.args.args.len() >= 2 {
                    if let PromExpr::NumberLiteral(n) = call.args.args[1].as_ref() {
                        udf_args.push(lit(n.val as i64));
                    }
                }
            }
            _ => {}
        }

        let udf_expr = logical_expr::Expr::ScalarFunction(
            datafusion_expr::expr::ScalarFunction::new_udf(Arc::new(udf), udf_args),
        );

        let mut projections = vec![
            col(COL_TIMESTAMP),
            col(COL_FINGERPRINT),
            udf_expr.alias(COL_VALUE),
        ];

        for field in inner_plan.schema().fields() {
            if field.name().starts_with("lbl_") {
                projections.push(col(field.name().as_str()));
            }
        }

        let plan = LogicalPlanBuilder::from(inner_plan)
            .project(projections)?
            .build()?;

        Ok(plan)
    }

    fn get_range_udf(&self, name: &str) -> EvalResult<datafusion::logical_expr::ScalarUDF> {
        let udf = match name {
            "rate" => functions::Rate::scalar_udf(),
            "increase" => functions::Increase::scalar_udf(),
            "delta" => functions::Delta::scalar_udf(),
            "irate" => functions::IDelta::<true>::scalar_udf(),
            "idelta" => functions::IDelta::<false>::scalar_udf(),
            "deriv" => functions::Deriv::scalar_udf(),
            "changes" => functions::Changes::scalar_udf(),
            "resets" => functions::Resets::scalar_udf(),
            "avg_over_time" => functions::AvgOverTime::scalar_udf(),
            "min_over_time" => functions::MinOverTime::scalar_udf(),
            "max_over_time" => functions::MaxOverTime::scalar_udf(),
            "sum_over_time" => functions::SumOverTime::scalar_udf(),
            "count_over_time" => functions::CountOverTime::scalar_udf(),
            "last_over_time" => functions::LastOverTime::scalar_udf(),
            "stddev_over_time" => functions::StddevOverTime::scalar_udf(),
            "stdvar_over_time" => functions::StdvarOverTime::scalar_udf(),
            "absent_over_time" => functions::AbsentOverTime::scalar_udf(),
            "present_over_time" => functions::PresentOverTime::scalar_udf(),
            "quantile_over_time" => functions::QuantileOverTime::scalar_udf(),
            "mad_over_time" => functions::MadOverTime::scalar_udf(),
            "predict_linear" => functions::PredictLinear::scalar_udf(),
            _ => return Err(EvalError::Unsupported(format!("range function: {}", name))),
        };
        Ok(udf)
    }

    fn extract_range_from_first_arg(&self, expr: &PromExpr) -> i64 {
        match expr {
            PromExpr::MatrixSelector(ms) => ms.range.as_millis() as i64,
            PromExpr::Subquery(sq) => sq.range.as_millis() as i64,
            PromExpr::Paren(p) => self.extract_range_from_first_arg(p.expr.as_ref()),
            _ => self.ctx.interval,
        }
    }

    /// Plan instant functions (math, trig, round, clamp, sgn, etc.)
    fn plan_instant_function(
        &self,
        call: &prom::Call,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        if call.args.args.is_empty() {
            return Err(EvalError::Invalid(format!(
                "{}() requires at least 1 argument",
                call.func.name
            )));
        }

        let inner_plan = self.plan(call.args.args[0].as_ref(), session)?;

        let func_expr = match call.func.name {
            // Basic math
            "abs" => datafusion::functions::math::abs().call(vec![col(COL_VALUE)]),
            "ceil" => datafusion::functions::math::ceil().call(vec![col(COL_VALUE)]),
            "floor" => datafusion::functions::math::floor().call(vec![col(COL_VALUE)]),
            "exp" => datafusion::functions::math::exp().call(vec![col(COL_VALUE)]),
            "ln" => datafusion::functions::math::ln().call(vec![col(COL_VALUE)]),
            "log2" => datafusion::functions::math::log2().call(vec![col(COL_VALUE)]),
            "log10" => datafusion::functions::math::log10().call(vec![col(COL_VALUE)]),
            "sqrt" => datafusion::functions::math::sqrt().call(vec![col(COL_VALUE)]),
            "cbrt" => datafusion::functions::math::cbrt().call(vec![col(COL_VALUE)]),
            "sgn" => datafusion::functions::math::signum().call(vec![col(COL_VALUE)]),
            // Trig
            "acos" => datafusion::functions::math::acos().call(vec![col(COL_VALUE)]),
            "acosh" => datafusion::functions::math::acosh().call(vec![col(COL_VALUE)]),
            "asin" => datafusion::functions::math::asin().call(vec![col(COL_VALUE)]),
            "asinh" => datafusion::functions::math::asinh().call(vec![col(COL_VALUE)]),
            "atan" => datafusion::functions::math::atan().call(vec![col(COL_VALUE)]),
            "atanh" => datafusion::functions::math::atanh().call(vec![col(COL_VALUE)]),
            "cos" => datafusion::functions::math::cos().call(vec![col(COL_VALUE)]),
            "cosh" => datafusion::functions::math::cosh().call(vec![col(COL_VALUE)]),
            "sin" => datafusion::functions::math::sin().call(vec![col(COL_VALUE)]),
            "sinh" => datafusion::functions::math::sinh().call(vec![col(COL_VALUE)]),
            "tan" => datafusion::functions::math::tan().call(vec![col(COL_VALUE)]),
            "tanh" => datafusion::functions::math::tanh().call(vec![col(COL_VALUE)]),
            // Angle conversions
            "deg" => datafusion::functions::math::degrees().call(vec![col(COL_VALUE)]),
            "rad" => datafusion::functions::math::radians().call(vec![col(COL_VALUE)]),
            // Clamp family — CASE-based since DataFusion doesn't have greatest/least as UDFs
            "clamp" => {
                let min_val = self
                    .extract_number_arg(call, 1)
                    .unwrap_or(f64::NEG_INFINITY);
                let max_val = self.extract_number_arg(call, 2).unwrap_or(f64::INFINITY);
                // CASE WHEN value < min THEN min WHEN value > max THEN max ELSE value END
                datafusion_expr::expr::Expr::Case(datafusion_expr::expr::Case {
                    expr: None,
                    when_then_expr: vec![
                        (
                            Box::new(col(COL_VALUE).lt(lit(min_val))),
                            Box::new(lit(min_val)),
                        ),
                        (
                            Box::new(col(COL_VALUE).gt(lit(max_val))),
                            Box::new(lit(max_val)),
                        ),
                    ],
                    else_expr: Some(Box::new(col(COL_VALUE))),
                })
            }
            "clamp_min" => {
                let min_val = self
                    .extract_number_arg(call, 1)
                    .unwrap_or(f64::NEG_INFINITY);
                datafusion_expr::expr::Expr::Case(datafusion_expr::expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(col(COL_VALUE).lt(lit(min_val))),
                        Box::new(lit(min_val)),
                    )],
                    else_expr: Some(Box::new(col(COL_VALUE))),
                })
            }
            "clamp_max" => {
                let max_val = self.extract_number_arg(call, 1).unwrap_or(f64::INFINITY);
                datafusion_expr::expr::Expr::Case(datafusion_expr::expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(col(COL_VALUE).gt(lit(max_val))),
                        Box::new(lit(max_val)),
                    )],
                    else_expr: Some(Box::new(col(COL_VALUE))),
                })
            }
            // Round with optional nearest parameter
            "round" => {
                let nearest = self.extract_number_arg(call, 1).unwrap_or(1.0);
                let udf = functions::Round::scalar_udf();
                logical_expr::Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                    Arc::new(udf),
                    vec![col(COL_VALUE), lit(nearest)],
                ))
            }
            name => {
                return Err(EvalError::Unsupported(format!(
                    "instant function: {}",
                    name
                )));
            }
        };

        let mut projections = vec![
            col(COL_TIMESTAMP),
            col(COL_FINGERPRINT),
            func_expr.alias(COL_VALUE),
        ];

        for field in inner_plan.schema().fields() {
            if field.name().starts_with("lbl_") {
                projections.push(col(field.name().as_str()));
            }
        }

        let plan = LogicalPlanBuilder::from(inner_plan)
            .project(projections)?
            .build()?;

        Ok(plan)
    }

    fn extract_number_arg(&self, call: &prom::Call, index: usize) -> Option<f64> {
        call.args.args.get(index).and_then(|arg| {
            if let PromExpr::NumberLiteral(n) = arg.as_ref() {
                Some(n.val)
            } else {
                None
            }
        })
    }

    /// Plan vector(N) — generates a constant value at each evaluation timestamp.
    fn plan_vector_function(&self, call: &prom::Call) -> EvalResult<LogicalPlan> {
        let val = call
            .args
            .args
            .first()
            .and_then(|arg| match arg.as_ref() {
                PromExpr::NumberLiteral(n) => Some(n.val),
                _ => None,
            })
            .unwrap_or(0.0);
        self.plan_number_literal(val)
    }

    /// Plan a number literal — produces a single-series result with constant value.
    fn plan_number_literal(&self, val: f64) -> EvalResult<LogicalPlan> {
        let num_steps = ((self.ctx.end - self.ctx.start) / self.ctx.interval.max(1) + 1) as usize;

        let timestamps: Vec<i64> = (0..num_steps)
            .map(|i| self.ctx.start + i as i64 * self.ctx.interval)
            .collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                COL_TIMESTAMP,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(COL_VALUE, DataType::Float64, false),
            Field::new(COL_FINGERPRINT, DataType::Utf8, false),
        ]));

        let ts_array =
            arrow_array::TimestampMillisecondArray::from_iter(timestamps.iter().map(|&t| Some(t)));
        let val_array = arrow_array::Float64Array::from(vec![val; num_steps]);
        let fp_array = arrow_array::StringArray::from(vec!["__literal__"; num_steps]);

        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ts_array), Arc::new(val_array), Arc::new(fp_array)],
        )
        .map_err(|e| EvalError::Arrow(e))?;

        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| EvalError::DataFusion(e))?;

        let plan =
            LogicalPlanBuilder::scan("__literal__", provider_as_source(Arc::new(mem_table)), None)?
                .build()?;

        Ok(plan)
    }

    /// Plan an aggregation expression (sum, avg, count, etc.)
    fn plan_aggregate(
        &self,
        agg: &prom::AggregateExpr,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        let inner_plan = self.plan(&agg.expr, session)?;

        let group_cols = self.extract_group_columns_with_schema(&agg.modifier, &inner_plan);

        let mut group_exprs: Vec<logical_expr::Expr> = vec![col(COL_TIMESTAMP)];
        for label in &group_cols {
            group_exprs.push(col(label.as_str()));
        }

        let agg_expr = match agg.op.id() {
            token::T_SUM => datafusion::functions_aggregate::sum::sum(col(COL_VALUE)),
            token::T_AVG => datafusion::functions_aggregate::average::avg(col(COL_VALUE)),
            token::T_MIN => datafusion::functions_aggregate::min_max::min(col(COL_VALUE)),
            token::T_MAX => datafusion::functions_aggregate::min_max::max(col(COL_VALUE)),
            token::T_COUNT => datafusion::functions_aggregate::count::count(col(COL_VALUE)),
            token::T_STDDEV => datafusion::functions_aggregate::stddev::stddev_pop(col(COL_VALUE)),
            token::T_STDVAR => datafusion::functions_aggregate::variance::var_pop(col(COL_VALUE)),
            token::T_GROUP => lit(1.0_f64).alias(COL_VALUE),
            token::T_TOPK | token::T_BOTTOMK => {
                return self.plan_topk_bottomk(agg, session);
            }
            token::T_QUANTILE => {
                return self.plan_quantile_agg(agg, session);
            }
            token::T_COUNT_VALUES => {
                return self.plan_count_values_agg(agg, session);
            }
            _ => {
                return Err(EvalError::Unsupported(format!(
                    "aggregation operator: {:?}",
                    agg.op
                )));
            }
        };

        let agg_plan = LogicalPlanBuilder::from(inner_plan)
            .aggregate(group_exprs.clone(), vec![agg_expr.alias(COL_VALUE)])?
            .build()?;

        // Re-project to ensure consistent schema: timestamp, value, fingerprint, [labels].
        let mut proj = vec![
            col(COL_TIMESTAMP),
            col(COL_VALUE),
            lit("").alias(COL_FINGERPRINT),
        ];
        for label in &group_cols {
            proj.push(col(label.as_str()));
        }

        let projected = LogicalPlanBuilder::from(agg_plan).project(proj)?.build()?;
        self.sort_aggregate_output(projected, &group_cols)
    }

    /// topk/bottomk: partition by timestamp (+ by/without labels), rank by
    /// value, and keep only the top/bottom K rows per partition.
    fn plan_topk_bottomk(
        &self,
        agg: &prom::AggregateExpr,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        let k = self.extract_param_scalar(agg)?.max(0.0) as usize;
        let inner_plan = self.plan(&agg.expr, session)?;
        let is_topk = agg.op.id() == token::T_TOPK;

        let group_cols = self.extract_group_columns_with_schema(&agg.modifier, &inner_plan);

        let node = super::extension_plan::topk_bottomk::TopkBottomk::new(
            k,
            is_topk,
            group_cols,
            inner_plan,
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// quantile aggregation: compute exact quantile via sort+interpolate
    fn plan_quantile_agg(
        &self,
        agg: &prom::AggregateExpr,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        let q = self.extract_param_scalar(agg)?;
        let inner_plan = self.plan(&agg.expr, session)?;
        let group_cols = self.extract_group_columns_with_schema(&agg.modifier, &inner_plan);

        let mut group_exprs: Vec<logical_expr::Expr> = vec![col(COL_TIMESTAMP)];
        for label in &group_cols {
            group_exprs.push(col(label.as_str()));
        }

        let quantile_udaf = Arc::new(functions::ExactQuantileUdaf::udaf(q));
        let agg_expr = datafusion_expr::expr::Expr::AggregateFunction(
            datafusion_expr::expr::AggregateFunction::new_udf(
                quantile_udaf,
                vec![col(COL_VALUE), lit(q)],
                false,
                None,
                vec![],
                None,
            ),
        )
        .alias(COL_VALUE);

        let agg_plan = LogicalPlanBuilder::from(inner_plan)
            .aggregate(group_exprs, vec![agg_expr])?
            .build()?;

        let mut proj = vec![
            col(COL_TIMESTAMP),
            col(COL_VALUE),
            lit("").alias(COL_FINGERPRINT),
        ];
        for label in &group_cols {
            proj.push(col(label.as_str()));
        }

        let projected = LogicalPlanBuilder::from(agg_plan).project(proj)?.build()?;
        self.sort_aggregate_output(projected, &group_cols)
    }

    /// count_values("label_name", vector): group by (timestamp, value, grouping labels),
    /// count occurrences, and emit the original sample value as a new label.
    fn plan_count_values_agg(
        &self,
        agg: &prom::AggregateExpr,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        let label_name = match agg.param.as_ref() {
            Some(expr) => match expr.as_ref() {
                PromExpr::StringLiteral(s) => s.val.clone(),
                _ => {
                    return Err(EvalError::Invalid(
                        "count_values() first arg must be a string".into(),
                    ))
                }
            },
            None => {
                return Err(EvalError::Invalid(
                    "count_values() requires a label name parameter".into(),
                ))
            }
        };

        let inner_plan = self.plan(&agg.expr, session)?;
        let group_cols = self.extract_group_columns_with_schema(&agg.modifier, &inner_plan);

        let output_label_col = format!("lbl_{label_name}");

        // Exclude the output label from group columns to avoid duplicate columns
        let group_cols: Vec<String> = group_cols
            .into_iter()
            .filter(|c| *c != output_label_col)
            .collect();

        let mut group_exprs: Vec<logical_expr::Expr> = vec![col(COL_TIMESTAMP), col(COL_VALUE)];
        for label in &group_cols {
            group_exprs.push(col(label.as_str()));
        }

        let agg_expr =
            datafusion::functions_aggregate::count::count(lit(1i64)).alias("count_val");

        let agg_plan = LogicalPlanBuilder::from(inner_plan)
            .aggregate(group_exprs, vec![agg_expr])?
            .build()?;

        let mut proj = vec![
            col(COL_TIMESTAMP),
            cast(col("count_val"), DataType::Float64).alias(COL_VALUE),
            lit("").alias(COL_FINGERPRINT),
            cast(col(COL_VALUE), DataType::Utf8).alias(&output_label_col),
        ];
        for label in &group_cols {
            proj.push(col(label.as_str()));
        }

        let projected = LogicalPlanBuilder::from(agg_plan).project(proj)?.build()?;
        let mut out_cols = group_cols.clone();
        out_cols.push(output_label_col);
        self.sort_aggregate_output(projected, &out_cols)
    }

    /// Extract the scalar parameter from an aggregation (e.g. topk(5, ...) -> 5.0)
    fn extract_param_scalar(&self, agg: &prom::AggregateExpr) -> EvalResult<f64> {
        match agg.param.as_ref() {
            Some(expr) => match expr.as_ref() {
                PromExpr::NumberLiteral(n) => Ok(n.val),
                _ => Err(EvalError::Invalid("expected numeric parameter".into())),
            },
            None => Err(EvalError::Invalid(
                "missing parameter for aggregation".into(),
            )),
        }
    }

    /// Plan a binary expression.
    fn plan_binary(
        &self,
        bin: &prom::BinaryExpr,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        let return_bool = bin.return_bool();

        let is_comparison = matches!(
            bin.op.id(),
            token::T_EQLC
                | token::T_NEQ
                | token::T_LSS
                | token::T_GTR
                | token::T_LTE
                | token::T_GTE
        );
        let lhs_is_scalar = matches!(bin.lhs.as_ref(), PromExpr::NumberLiteral(_));
        let rhs_is_scalar = matches!(bin.rhs.as_ref(), PromExpr::NumberLiteral(_));

        // Scalar-scalar comparison with bool: produce literal 0/1
        if is_comparison && return_bool && lhs_is_scalar && rhs_is_scalar {
            if let (PromExpr::NumberLiteral(l), PromExpr::NumberLiteral(r)) =
                (bin.lhs.as_ref(), bin.rhs.as_ref())
            {
                let result = if cmp_op_matches(bin.op.id(), l.val, r.val) {
                    1.0
                } else {
                    0.0
                };
                return self.plan_number_literal(result);
            }
        }

        // Set operators
        match bin.op.id() {
            token::T_LOR => {
                let lhs = self.plan(&bin.lhs, session)?;
                let rhs = self.plan(&bin.rhs, session)?;
                return self.plan_set_or(lhs, rhs, bin);
            }
            token::T_LAND => {
                let lhs = self.plan(&bin.lhs, session)?;
                let rhs = self.plan(&bin.rhs, session)?;
                return self.plan_set_and(lhs, rhs, bin);
            }
            token::T_LUNLESS => {
                let lhs = self.plan(&bin.lhs, session)?;
                let rhs = self.plan(&bin.rhs, session)?;
                return self.plan_set_unless(lhs, rhs, bin);
            }
            _ => {}
        }

        let lhs = self.plan(&bin.lhs, session)?;
        let rhs = self.plan(&bin.rhs, session)?;

        // Comparison with scalar RHS and no `bool`: filter (PromQL semantics)
        if is_comparison && !return_bool && rhs_is_scalar {
            if let PromExpr::NumberLiteral(n) = bin.rhs.as_ref() {
                let filter_op = cmp_to_df_op(bin.op.id());
                let filter_expr =
                    logical_expr::Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr::new(
                        Box::new(col(COL_VALUE)),
                        filter_op,
                        Box::new(lit(n.val)),
                    ));
                return Ok(LogicalPlanBuilder::from(lhs).filter(filter_expr)?.build()?);
            }
        }

        // Comparison with scalar LHS and no `bool`: filter
        if is_comparison && !return_bool && lhs_is_scalar {
            if let PromExpr::NumberLiteral(n) = bin.lhs.as_ref() {
                let filter_op = cmp_to_df_op(bin.op.id());
                let filter_expr =
                    logical_expr::Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr::new(
                        Box::new(lit(n.val)),
                        filter_op,
                        Box::new(col(COL_VALUE)),
                    ));
                return Ok(LogicalPlanBuilder::from(rhs).filter(filter_expr)?.build()?);
            }
        }

        // Determine the value expression for the join output
        let value_expr = |lhs_val: logical_expr::Expr,
                          rhs_val: logical_expr::Expr|
         -> logical_expr::Expr {
            // atan2 uses the DataFusion built-in
            if bin.op.id() == token::T_ATAN2 {
                return datafusion::functions::math::atan2().call(vec![lhs_val, rhs_val]);
            }
            // pow uses the power function
            if bin.op.id() == token::T_POW {
                return datafusion::functions::math::power().call(vec![lhs_val, rhs_val]);
            }

            let op = match bin.op.id() {
                token::T_ADD => datafusion_expr::Operator::Plus,
                token::T_SUB => datafusion_expr::Operator::Minus,
                token::T_MUL => datafusion_expr::Operator::Multiply,
                token::T_DIV => datafusion_expr::Operator::Divide,
                token::T_MOD => datafusion_expr::Operator::Modulo,
                token::T_EQLC => datafusion_expr::Operator::Eq,
                token::T_NEQ => datafusion_expr::Operator::NotEq,
                token::T_LSS => datafusion_expr::Operator::Lt,
                token::T_GTR => datafusion_expr::Operator::Gt,
                token::T_LTE => datafusion_expr::Operator::LtEq,
                token::T_GTE => datafusion_expr::Operator::GtEq,
                _ => datafusion_expr::Operator::Plus,
            };

            let bin_expr = logical_expr::Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr::new(
                Box::new(lhs_val.clone()),
                op,
                Box::new(rhs_val.clone()),
            ));

            if return_bool && is_comparison {
                datafusion_expr::expr::Expr::Case(datafusion_expr::expr::Case {
                    expr: None,
                    when_then_expr: vec![(Box::new(bin_expr), Box::new(lit(1.0_f64)))],
                    else_expr: Some(Box::new(lit(0.0_f64))),
                })
            } else if is_comparison {
                // vector-vector comparison without `bool`: filter (keep LHS value or NaN)
                datafusion_expr::expr::Expr::Case(datafusion_expr::expr::Case {
                    expr: None,
                    when_then_expr: vec![(Box::new(bin_expr), Box::new(lhs_val))],
                    else_expr: Some(Box::new(lit(f64::NAN))),
                })
            } else {
                bin_expr
            }
        };

        // Build join conditions from on()/ignoring() modifiers
        let (matching_labels, is_on) = self.extract_matching_labels(bin);

        let lhs_aliased = LogicalPlanBuilder::from(lhs).alias("__lhs__")?.build()?;
        let rhs_aliased = LogicalPlanBuilder::from(rhs).alias("__rhs__")?.build()?;

        let qcol = |table: &str, name: &str| -> logical_expr::Expr {
            logical_expr::Expr::Column(datafusion::common::Column::new(
                Some(table.to_string()),
                name.to_string(),
            ))
        };

        let has_field = |plan: &LogicalPlan, name: &str| -> bool {
            plan.schema().field_with_unqualified_name(name).is_ok()
        };

        let lhs_ts = qcol("__lhs__", COL_TIMESTAMP);
        let rhs_ts = qcol("__rhs__", COL_TIMESTAMP);
        let lhs_val = qcol("__lhs__", COL_VALUE);
        let rhs_val = qcol("__rhs__", COL_VALUE);

        // Build join conditions: always join on timestamp + matching labels.
        // Prometheus default (no on/ignoring): match on ALL shared labels.
        let mut join_conds = vec![lhs_ts.clone().eq(rhs_ts)];

        if is_on {
            if matching_labels.is_empty() {
                // on() with no labels — match on timestamp only (no label conditions)
            } else {
                // on(label1, label2): join on those specific label columns
                for label in &matching_labels {
                    let col_name = format!("lbl_{label}");
                    if has_field(&lhs_aliased, &col_name) && has_field(&rhs_aliased, &col_name) {
                        join_conds.push(qcol("__lhs__", &col_name).eq(qcol("__rhs__", &col_name)));
                    } else {
                        // Label absent on one/both sides → no match possible
                        join_conds.push(lit(false));
                    }
                }
            }
        } else if !matching_labels.is_empty() {
            // ignoring(label1, label2): join on all shared labels EXCEPT ignored ones
            let ignore_set: std::collections::HashSet<String> =
                matching_labels.iter().map(|l| format!("lbl_{l}")).collect();
            for field in lhs_aliased.schema().fields() {
                let name = field.name();
                if name.starts_with("lbl_")
                    && !ignore_set.contains(name.as_str())
                    && has_field(&rhs_aliased, name)
                {
                    join_conds.push(qcol("__lhs__", name).eq(qcol("__rhs__", name)));
                }
            }
        } else {
            // No modifier: match on all shared lbl_* columns (Prometheus default)
            for field in lhs_aliased.schema().fields() {
                let name = field.name();
                if name.starts_with("lbl_") && has_field(&rhs_aliased, name) {
                    join_conds.push(qcol("__lhs__", name).eq(qcol("__rhs__", name)));
                }
            }
        }

        // Determine join type for group_left/group_right
        let join_type = self.extract_join_type(bin);

        let fp_expr = if has_field(&lhs_aliased, COL_FINGERPRINT) {
            qcol("__lhs__", COL_FINGERPRINT).alias(COL_FINGERPRINT)
        } else if has_field(&rhs_aliased, COL_FINGERPRINT) {
            qcol("__rhs__", COL_FINGERPRINT).alias(COL_FINGERPRINT)
        } else {
            lit("").alias(COL_FINGERPRINT)
        };

        let val_result = value_expr(lhs_val.clone(), rhs_val);

        let mut proj = vec![
            lhs_ts.alias(COL_TIMESTAMP),
            fp_expr,
            val_result.alias(COL_VALUE),
        ];

        // For group_left/group_right, include extra label columns from the "one" side
        if let Some(modifier) = &bin.modifier {
            if let Some(extra_labels) = modifier.card.labels() {
                for label in &extra_labels.labels {
                    let col_name = format!("lbl_{label}");
                    let source = match &modifier.card {
                        prom::VectorMatchCardinality::ManyToOne(_) => "__rhs__",
                        prom::VectorMatchCardinality::OneToMany(_) => "__lhs__",
                        _ => continue,
                    };
                    if has_field(
                        if source == "__rhs__" {
                            &rhs_aliased
                        } else {
                            &lhs_aliased
                        },
                        &col_name,
                    ) {
                        proj.push(qcol(source, &col_name).alias(&col_name));
                    }
                }
            }
        }

        // Carry forward lbl_* columns from the "many" side of the join.
        // group_left  → left is many  → carry LHS labels
        // group_right → right is many → carry RHS labels
        // scalar LHS  → carry RHS labels (scalar has no labels)
        // default     → carry LHS labels
        let is_group_right = bin.modifier.as_ref().map_or(false, |m| {
            matches!(m.card, prom::VectorMatchCardinality::OneToMany(_))
        });
        let (many_plan, many_alias) = if is_group_right {
            (&rhs_aliased, "__rhs__")
        } else if lhs_is_scalar {
            (&rhs_aliased, "__rhs__")
        } else {
            (&lhs_aliased, "__lhs__")
        };
        for field in many_plan.schema().fields() {
            let name = field.name();
            if name.starts_with("lbl_")
                && !proj.iter().any(|p| format!("{p}").contains(name.as_str()))
            {
                proj.push(qcol(many_alias, name).alias(name.as_str()));
            }
        }

        let join_plan = LogicalPlanBuilder::from(lhs_aliased)
            .join_on(rhs_aliased, join_type, join_conds)?
            .project(proj)?
            .build()?;

        // Prometheus enforces one-to-one cardinality at runtime: without
        // group_left/group_right, a many-to-one join is an error. We enforce
        // this pragmatically by deduplicating to at most one output row per
        // (timestamp, join-key-labels) when cardinality is OneToOne.
        let is_one_to_one = match &bin.modifier {
            Some(m) => matches!(m.card, prom::VectorMatchCardinality::OneToOne),
            None => true,
        };
        let join_plan = if is_one_to_one && !lhs_is_scalar && !rhs_is_scalar {
            // Group by timestamp + the join-key labels only.
            // This ensures at most one row per join-key combination per timestamp.
            let mut group_exprs: Vec<logical_expr::Expr> = vec![col(COL_TIMESTAMP)];
            if is_on && !matching_labels.is_empty() {
                // on(label1, label2): join key is exactly those labels
                for label in &matching_labels {
                    let col_name = format!("lbl_{label}");
                    if join_plan.schema().field_with_unqualified_name(&col_name).is_ok() {
                        group_exprs.push(col(col_name.as_str()));
                    }
                }
            } else if !is_on && !matching_labels.is_empty() {
                // ignoring(label1, label2): join key is all shared labels EXCEPT ignored
                let ignore_set: std::collections::HashSet<String> =
                    matching_labels.iter().map(|l| format!("lbl_{l}")).collect();
                for field in join_plan.schema().fields() {
                    let name = field.name();
                    if name.starts_with("lbl_") && !ignore_set.contains(name.as_str()) {
                        group_exprs.push(col(name.as_str()));
                    }
                }
            } else {
                // No modifier: join key is all shared labels → dedup by all
                for field in join_plan.schema().fields() {
                    let name = field.name();
                    if name.starts_with("lbl_") {
                        group_exprs.push(col(name.as_str()));
                    }
                }
            }

            let agg_expr = datafusion::functions_aggregate::first_last::first_value(
                col(COL_VALUE),
                vec![],
            )
            .alias(COL_VALUE);
            let fp_agg = datafusion::functions_aggregate::first_last::first_value(
                col(COL_FINGERPRINT),
                vec![],
            )
            .alias(COL_FINGERPRINT);

            // Carry forward non-group label columns via first_value
            let group_col_names: std::collections::HashSet<&str> = group_exprs
                .iter()
                .filter_map(|e| match e {
                    logical_expr::Expr::Column(c) => Some(c.name.as_str()),
                    _ => None,
                })
                .collect();
            let mut extra_aggs: Vec<logical_expr::Expr> = Vec::new();
            for field in join_plan.schema().fields() {
                let name = field.name();
                if name.starts_with("lbl_") && !group_col_names.contains(name.as_str()) {
                    extra_aggs.push(
                        datafusion::functions_aggregate::first_last::first_value(
                            col(name.as_str()),
                            vec![],
                        )
                        .alias(name.as_str()),
                    );
                }
            }

            let mut all_aggs = vec![agg_expr, fp_agg];
            all_aggs.extend(extra_aggs);

            let agg_plan = LogicalPlanBuilder::from(join_plan)
                .aggregate(group_exprs.clone(), all_aggs)?
                .build()?;

            let mut out_proj = vec![
                col(COL_TIMESTAMP),
                col(COL_VALUE),
                col(COL_FINGERPRINT),
            ];
            for field in agg_plan.schema().fields() {
                let name = field.name();
                if name.starts_with("lbl_") {
                    out_proj.push(col(name.as_str()));
                }
            }
            LogicalPlanBuilder::from(agg_plan)
                .project(out_proj)?
                .build()?
        } else {
            join_plan
        };

        // For vector-vector comparison without bool: filter out NaN results
        if is_comparison && !return_bool && !rhs_is_scalar && !lhs_is_scalar {
            let filter = datafusion::functions::math::isnan()
                .call(vec![col(COL_VALUE)])
                .not();
            return Ok(LogicalPlanBuilder::from(join_plan)
                .filter(filter)?
                .build()?);
        }

        Ok(join_plan)
    }

    fn extract_matching_labels(&self, bin: &prom::BinaryExpr) -> (Vec<String>, bool) {
        match &bin.modifier {
            Some(modifier) => match &modifier.matching {
                Some(prom::LabelModifier::Include(labels)) => {
                    (labels.labels.clone(), true) // on(...)
                }
                Some(prom::LabelModifier::Exclude(labels)) => {
                    (labels.labels.clone(), false) // ignoring(...)
                }
                // Modifier present but no matching clause (set ops: card=ManyToMany)
                None => (vec![], false),
            },
            // No modifier at all — match on all shared labels
            None => (vec![], false),
        }
    }

    /// Build join conditions for set operators (and/unless/or).
    /// Matches on timestamp + label columns per on()/ignoring() modifiers.
    /// Without modifiers, matches on all shared lbl_* columns (Prometheus default).
    fn build_set_op_join_conds(
        &self,
        bin: &prom::BinaryExpr,
        lhs: &LogicalPlan,
        rhs: &LogicalPlan,
    ) -> Vec<logical_expr::Expr> {
        let qcol = |table: &str, name: &str| -> logical_expr::Expr {
            logical_expr::Expr::Column(datafusion::common::Column::new(
                Some(table.to_string()),
                name.to_string(),
            ))
        };
        let has_field = |plan: &LogicalPlan, name: &str| -> bool {
            plan.schema().field_with_unqualified_name(name).is_ok()
        };

        let (matching_labels, is_on) = self.extract_matching_labels(bin);
        let mut conds = vec![qcol("__lhs__", COL_TIMESTAMP).eq(qcol("__rhs__", COL_TIMESTAMP))];

        if is_on {
            if matching_labels.is_empty() {
                // on() with no labels — match on timestamp only
            } else {
                for label in &matching_labels {
                    let col_name = format!("lbl_{label}");
                    if has_field(lhs, &col_name) && has_field(rhs, &col_name) {
                        conds.push(qcol("__lhs__", &col_name).eq(qcol("__rhs__", &col_name)));
                    } else {
                        conds.push(lit(false));
                    }
                }
            }
        } else if !matching_labels.is_empty() {
            // ignoring(...): join on all shared labels EXCEPT ignored ones
            let ignore_set: std::collections::HashSet<String> =
                matching_labels.iter().map(|l| format!("lbl_{l}")).collect();
            for field in lhs.schema().fields() {
                let name = field.name();
                if name.starts_with("lbl_")
                    && !ignore_set.contains(name.as_str())
                    && has_field(rhs, name)
                {
                    conds.push(qcol("__lhs__", name).eq(qcol("__rhs__", name)));
                }
            }
        } else {
            // No modifier: match on all shared lbl_* columns
            for field in lhs.schema().fields() {
                let name = field.name();
                if name.starts_with("lbl_") && has_field(rhs, name) {
                    conds.push(qcol("__lhs__", name).eq(qcol("__rhs__", name)));
                }
            }
        }

        conds
    }

    fn extract_join_type(&self, bin: &prom::BinaryExpr) -> datafusion::logical_expr::JoinType {
        match &bin.modifier {
            Some(modifier) => match &modifier.card {
                prom::VectorMatchCardinality::ManyToOne(_) => {
                    datafusion::logical_expr::JoinType::Inner
                }
                prom::VectorMatchCardinality::OneToMany(_) => {
                    datafusion::logical_expr::JoinType::Inner
                }
                _ => datafusion::logical_expr::JoinType::Inner,
            },
            None => datafusion::logical_expr::JoinType::Inner,
        }
    }

    fn plan_set_or(
        &self,
        lhs: LogicalPlan,
        rhs: LogicalPlan,
        bin: &prom::BinaryExpr,
    ) -> EvalResult<LogicalPlan> {
        // Align schemas: both sides must have the same columns for UNION.
        let lhs_fields: Vec<String> = lhs
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let rhs_fields: Vec<String> = rhs
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let mut all_fields: Vec<String> = lhs_fields.clone();
        for f in &rhs_fields {
            if !all_fields.contains(f) {
                all_fields.push(f.clone());
            }
        }

        let project_aligned = |plan: LogicalPlan, existing: &[String]| -> EvalResult<LogicalPlan> {
            let mut proj: Vec<logical_expr::Expr> = Vec::new();
            for name in &all_fields {
                if existing.contains(name) {
                    proj.push(col(name.as_str()));
                } else {
                    proj.push(lit("").alias(name.as_str()));
                }
            }
            Ok(LogicalPlanBuilder::from(plan).project(proj)?.build()?)
        };

        let lhs_aligned = if lhs_fields.len() == all_fields.len() && lhs_fields == all_fields {
            lhs.clone()
        } else {
            project_aligned(lhs.clone(), &lhs_fields)?
        };
        let rhs_aligned = if rhs_fields.len() == all_fields.len() && rhs_fields == all_fields {
            rhs.clone()
        } else {
            project_aligned(rhs.clone(), &rhs_fields)?
        };

        // Anti-join RHS against LHS to remove duplicates before union.
        // Alias both for the anti-join condition building.
        let lhs_for_anti = LogicalPlanBuilder::from(lhs_aligned.clone()).alias("__lhs__")?.build()?;
        let rhs_for_anti = LogicalPlanBuilder::from(rhs_aligned.clone()).alias("__rhs__")?.build()?;
        let anti_conds = self.build_set_op_join_conds(bin, &lhs_for_anti, &rhs_for_anti);

        let qcol = |table: &str, name: &str| -> logical_expr::Expr {
            logical_expr::Expr::Column(datafusion::common::Column::new(
                Some(table.to_string()),
                name.to_string(),
            ))
        };

        // Project the anti-join result back to unqualified column names
        let mut anti_proj: Vec<logical_expr::Expr> = Vec::new();
        for field in rhs_for_anti.schema().fields() {
            let name = field.name();
            anti_proj.push(qcol("__rhs__", name).alias(name.as_str()));
        }

        let rhs_deduped = LogicalPlanBuilder::from(rhs_for_anti)
            .join_on(lhs_for_anti, datafusion::logical_expr::JoinType::LeftAnti, anti_conds)?
            .project(anti_proj)?
            .build()?;

        let plan = LogicalPlanBuilder::from(lhs_aligned)
            .union(rhs_deduped)?
            .build()?;
        Ok(plan)
    }

    fn plan_set_and(
        &self,
        lhs: LogicalPlan,
        rhs: LogicalPlan,
        bin: &prom::BinaryExpr,
    ) -> EvalResult<LogicalPlan> {
        let lhs_aliased = LogicalPlanBuilder::from(lhs).alias("__lhs__")?.build()?;
        let rhs_aliased = LogicalPlanBuilder::from(rhs).alias("__rhs__")?.build()?;
        let join_conds = self.build_set_op_join_conds(bin, &lhs_aliased, &rhs_aliased);

        let qcol = |table: &str, name: &str| -> logical_expr::Expr {
            logical_expr::Expr::Column(datafusion::common::Column::new(
                Some(table.to_string()),
                name.to_string(),
            ))
        };

        let mut proj: Vec<logical_expr::Expr> = Vec::new();
        for field in lhs_aliased.schema().fields() {
            let name = field.name();
            proj.push(qcol("__lhs__", name).alias(name.as_str()));
        }

        let plan = LogicalPlanBuilder::from(lhs_aliased)
            .join_on(rhs_aliased, datafusion::logical_expr::JoinType::LeftSemi, join_conds)?
            .project(proj)?
            .build()?;
        Ok(plan)
    }

    fn plan_set_unless(
        &self,
        lhs: LogicalPlan,
        rhs: LogicalPlan,
        bin: &prom::BinaryExpr,
    ) -> EvalResult<LogicalPlan> {
        let lhs_aliased = LogicalPlanBuilder::from(lhs).alias("__lhs__")?.build()?;
        let rhs_aliased = LogicalPlanBuilder::from(rhs).alias("__rhs__")?.build()?;
        let join_conds = self.build_set_op_join_conds(bin, &lhs_aliased, &rhs_aliased);

        let qcol = |table: &str, name: &str| -> logical_expr::Expr {
            logical_expr::Expr::Column(datafusion::common::Column::new(
                Some(table.to_string()),
                name.to_string(),
            ))
        };

        let mut proj: Vec<logical_expr::Expr> = Vec::new();
        for field in lhs_aliased.schema().fields() {
            let name = field.name();
            proj.push(qcol("__lhs__", name).alias(name.as_str()));
        }

        let plan = LogicalPlanBuilder::from(lhs_aliased)
            .join_on(rhs_aliased, datafusion::logical_expr::JoinType::LeftAnti, join_conds)?
            .project(proj)?
            .build()?;
        Ok(plan)
    }

    /// Plan time() — returns the evaluation timestamp as the value.
    fn plan_time_function(&self) -> EvalResult<LogicalPlan> {
        let num_steps = ((self.ctx.end - self.ctx.start) / self.ctx.interval.max(1) + 1) as usize;

        let timestamps: Vec<i64> = (0..num_steps)
            .map(|i| self.ctx.start + i as i64 * self.ctx.interval)
            .collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                COL_TIMESTAMP,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(COL_VALUE, DataType::Float64, false),
            Field::new(COL_FINGERPRINT, DataType::Utf8, false),
        ]));

        let ts_array =
            arrow_array::TimestampMillisecondArray::from_iter(timestamps.iter().map(|&t| Some(t)));
        // time() returns seconds since epoch
        let val_array = arrow_array::Float64Array::from(
            timestamps
                .iter()
                .map(|&t| t as f64 / 1000.0)
                .collect::<Vec<_>>(),
        );
        let fp_array = arrow_array::StringArray::from(vec!["__time__"; num_steps]);

        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ts_array), Arc::new(val_array), Arc::new(fp_array)],
        )
        .map_err(|e| EvalError::Arrow(e))?;

        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| EvalError::DataFusion(e))?;

        Ok(
            LogicalPlanBuilder::scan("__time__", provider_as_source(Arc::new(mem_table)), None)?
                .build()?,
        )
    }

    /// absent(vector) — returns empty result if series exist, or a single 1 if they don't.
    /// We approximate by planning the inner and wrapping with a count check.
    fn plan_absent(&self, call: &prom::Call, session: &SessionContext) -> EvalResult<LogicalPlan> {
        if call.args.args.is_empty() {
            return Err(EvalError::Invalid("absent() requires 1 argument".into()));
        }
        // Plan the inner expression; if it produces rows, absent() should be empty.
        // If it produces no rows, absent() should return 1.
        // We use an anti-join approach: generate all timestamps, then anti-join with inner.
        let inner = self.plan(call.args.args[0].as_ref(), session)?;
        let all_ts = self.plan_number_literal(1.0)?;

        let plan = LogicalPlanBuilder::from(all_ts)
            .alias("__absent_all__")?
            .build()?;

        let inner_aliased = LogicalPlanBuilder::from(inner)
            .alias("__absent_inner__")?
            .build()?;

        let qcol = |table: &str, name: &str| -> logical_expr::Expr {
            logical_expr::Expr::Column(datafusion::common::Column::new(
                Some(table.to_string()),
                name.to_string(),
            ))
        };

        let result = LogicalPlanBuilder::from(plan)
            .join_on(
                inner_aliased,
                datafusion::logical_expr::JoinType::LeftAnti,
                vec![qcol("__absent_all__", COL_TIMESTAMP)
                    .eq(qcol("__absent_inner__", COL_TIMESTAMP))],
            )?
            .build()?;

        Ok(result)
    }

    /// histogram_quantile(scalar, vector) — computes quantile from histogram buckets.
    /// Groups by timestamp + labels (excluding "le"), then interpolates.
    fn plan_histogram_quantile(
        &self,
        call: &prom::Call,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        if call.args.args.len() < 2 {
            return Err(EvalError::Invalid(
                "histogram_quantile() requires 2 arguments".into(),
            ));
        }

        let q = match call.args.args[0].as_ref() {
            PromExpr::NumberLiteral(n) => n.val,
            _ => {
                return Err(EvalError::Invalid(
                    "histogram_quantile() first arg must be a number".into(),
                ))
            }
        };

        let inner_plan = self.plan(call.args.args[1].as_ref(), session)?;

        // histogram_quantile operates on {le="..."} labeled data.
        // We need to group by timestamp + all labels except "le", sort by le, and interpolate.
        // Use our custom UDF for this.
        let udf = functions::histogram_quantile_udf();

        // Group by timestamp + all non-le label columns
        let mut group_exprs: Vec<logical_expr::Expr> = vec![col(COL_TIMESTAMP)];
        let mut label_cols = Vec::new();
        for field in inner_plan.schema().fields() {
            if field.name().starts_with("lbl_") && field.name() != "lbl_le" {
                group_exprs.push(col(field.name().as_str()));
                label_cols.push(field.name().clone());
            }
        }

        // Collect le values and bucket counts using array_agg
        let le_agg =
            datafusion::functions_aggregate::array_agg::array_agg(col("lbl_le")).alias("le_values");
        let count_agg = datafusion::functions_aggregate::array_agg::array_agg(col(COL_VALUE))
            .alias("bucket_counts");

        let agg_plan = LogicalPlanBuilder::from(inner_plan)
            .aggregate(group_exprs, vec![le_agg, count_agg])?
            .build()?;

        let udf_expr =
            logical_expr::Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                Arc::new(udf),
                vec![lit(q), col("le_values"), col("bucket_counts")],
            ));

        let mut proj = vec![
            col(COL_TIMESTAMP),
            lit("").alias(COL_FINGERPRINT),
            udf_expr.alias(COL_VALUE),
        ];
        for lbl in &label_cols {
            proj.push(col(lbl.as_str()));
        }

        Ok(LogicalPlanBuilder::from(agg_plan).project(proj)?.build()?)
    }

    /// label_replace(v, dst, replacement, src, regex) — replace label values via regex.
    fn plan_label_replace(
        &self,
        call: &prom::Call,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        if call.args.args.len() < 5 {
            return Err(EvalError::Invalid(
                "label_replace() requires 5 arguments".into(),
            ));
        }
        let inner = self.plan(call.args.args[0].as_ref(), session)?;

        let dst_label = Self::extract_string_arg(&call.args.args[1], "label_replace dst_label")?;
        let replacement = Self::extract_string_arg(&call.args.args[2], "label_replace replacement")?;
        let src_label = Self::extract_string_arg(&call.args.args[3], "label_replace src_label")?;
        let regex = Self::extract_string_arg(&call.args.args[4], "label_replace regex")?;

        let src_col = format!("lbl_{src_label}");
        let dst_col = format!("lbl_{dst_label}");
        let anchored = format!("^(?:{regex})$");

        let has_src = inner.schema().field_with_unqualified_name(&src_col).is_ok();
        let has_dst = inner.schema().field_with_unqualified_name(&dst_col).is_ok();

        // Prometheus treats missing labels as empty string for regex matching
        let src_expr: logical_expr::Expr = if has_src {
            col(src_col.as_str())
        } else {
            lit("")
        };

        let fallback: logical_expr::Expr = if has_dst && dst_col != src_col {
            col(dst_col.as_str())
        } else if has_src && dst_col == src_col {
            col(src_col.as_str())
        } else {
            lit("")
        };

        let new_val = when(
            regexp_like(src_expr.clone(), lit(anchored.clone()), None),
            regexp_replace(src_expr, lit(anchored), lit(replacement), None),
        )
        .otherwise(fallback)?;

        // Build projection: all existing columns, replacing/adding lbl_<dst>
        let mut proj: Vec<logical_expr::Expr> = Vec::new();
        let mut dst_added = false;
        for field in inner.schema().fields() {
            let name = field.name();
            if name == &dst_col {
                proj.push(new_val.clone().alias(dst_col.as_str()));
                dst_added = true;
            } else {
                proj.push(col(name.as_str()));
            }
        }
        if !dst_added {
            proj.push(new_val.alias(dst_col.as_str()));
        }

        Ok(LogicalPlanBuilder::from(inner).project(proj)?.build()?)
    }

    fn extract_string_arg(expr: &PromExpr, context: &str) -> EvalResult<String> {
        match expr {
            PromExpr::StringLiteral(s) => Ok(s.val.clone()),
            _ => Err(EvalError::Invalid(format!(
                "{context}: expected string literal"
            ))),
        }
    }

    fn unwrap_paren(expr: &PromExpr) -> &PromExpr {
        match expr {
            PromExpr::Paren(p) => Self::unwrap_paren(p.expr.as_ref()),
            other => other,
        }
    }

    /// label_join(v, dst, separator, src1, src2, ...) — concatenate label values.
    fn plan_label_join(
        &self,
        call: &prom::Call,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        if call.args.args.len() < 4 {
            return Err(EvalError::Invalid(
                "label_join() requires at least 4 arguments".into(),
            ));
        }
        let inner = self.plan(call.args.args[0].as_ref(), session)?;

        let dst_label = Self::extract_string_arg(&call.args.args[1], "label_join dst_label")?;
        let separator = Self::extract_string_arg(&call.args.args[2], "label_join separator")?;

        let mut src_cols: Vec<logical_expr::Expr> = Vec::new();
        for arg in &call.args.args[3..] {
            let src_name = Self::extract_string_arg(arg, "label_join src_label")?;
            let src_col = format!("lbl_{src_name}");
            if inner.schema().field_with_unqualified_name(&src_col).is_ok() {
                src_cols.push(col(src_col.as_str()));
            } else {
                src_cols.push(lit(""));
            }
        }

        let dst_col = format!("lbl_{dst_label}");
        let concat_expr = concat_ws(lit(separator), src_cols);

        let mut proj: Vec<logical_expr::Expr> = Vec::new();
        let mut dst_added = false;
        for field in inner.schema().fields() {
            let name = field.name();
            if name == &dst_col {
                proj.push(concat_expr.clone().alias(dst_col.as_str()));
                dst_added = true;
            } else {
                proj.push(col(name.as_str()));
            }
        }
        if !dst_added {
            proj.push(concat_expr.alias(dst_col.as_str()));
        }

        Ok(LogicalPlanBuilder::from(inner).project(proj)?.build()?)
    }

    /// sort/sort_desc: plan inner + apply sort on value column.
    fn plan_sort_function(
        &self,
        call: &prom::Call,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        if call.args.args.is_empty() {
            return Err(EvalError::Invalid(format!(
                "{}() requires at least 1 argument",
                call.func.name
            )));
        }
        let inner = self.plan(call.args.args[0].as_ref(), session)?;
        let ascending = matches!(call.func.name, "sort" | "sort_by_label");

        let plan = LogicalPlanBuilder::from(inner)
            .sort(vec![
                SortExpr::new(col(COL_TIMESTAMP), true, true),
                SortExpr::new(col(COL_VALUE), ascending, true),
            ])?
            .build()?;
        Ok(plan)
    }

    /// Plan a unary expression (-expr).
    fn plan_unary(&self, u: &prom::UnaryExpr, session: &SessionContext) -> EvalResult<LogicalPlan> {
        let inner = self.plan(&u.expr, session)?;
        let mut proj = vec![
            col(COL_TIMESTAMP),
            col(COL_FINGERPRINT),
            (lit(0.0_f64) - col(COL_VALUE)).alias(COL_VALUE),
        ];
        for field in inner.schema().fields() {
            let name = field.name();
            if name.starts_with("lbl_") {
                proj.push(col(name.as_str()));
            }
        }
        let plan = LogicalPlanBuilder::from(inner).project(proj)?.build()?;
        Ok(plan)
    }

    /// Plan a subquery expression.
    fn plan_subquery(
        &self,
        sq: &prom::SubqueryExpr,
        session: &SessionContext,
    ) -> EvalResult<LogicalPlan> {
        // Use subquery step if specified, otherwise fall back to outer interval
        let sub_step = sq
            .step
            .as_ref()
            .map(|d| d.as_millis() as i64)
            .filter(|&s| s > 0)
            .unwrap_or(self.ctx.interval);

        let sub_ctx = EvalContext {
            start: self.ctx.start,
            end: self.ctx.end,
            interval: sub_step,
            lookback_delta: self.ctx.lookback_delta,
        };

        let sub_planner = PromPlanner::new(sub_ctx);
        let mut plan = sub_planner.plan(&sq.expr, session)?;

        // Apply offset if present
        let offset_ms = sq.offset.as_ref().map(|d| duration_to_ms(d)).unwrap_or(0);
        if offset_ms != 0 {
            let divide_columns = self.series_divide_columns(&plan);
            let sorted = self.apply_sort(plan, &divide_columns)?;
            let series_divide = LogicalPlan::Extension(Extension {
                node: Arc::new(SeriesDivide::new(
                    divide_columns,
                    COL_TIMESTAMP.to_string(),
                    sorted,
                )),
            });
            plan = LogicalPlan::Extension(Extension {
                node: Arc::new(SeriesNormalize::new(
                    offset_ms,
                    COL_TIMESTAMP.to_string(),
                    Some(COL_VALUE.to_string()),
                    series_divide,
                )),
            });
        }

        Ok(plan)
    }

    // ── Helper functions ──

    fn extract_metric_name(&self, vs: &prom::VectorSelector) -> Option<String> {
        vs.name.clone().or_else(|| {
            vs.matchers.matchers.iter().find_map(|m| {
                if m.name == METRIC_NAME && m.op == MatchOp::Equal {
                    Some(m.value.clone())
                } else {
                    None
                }
            })
        })
    }

    fn build_label_filters(&self, matchers: &[Matcher]) -> (Vec<String>, Vec<logical_expr::Expr>) {
        let mut tag_columns = Vec::new();
        let mut filters = Vec::new();

        for m in matchers {
            if m.name == METRIC_NAME {
                continue;
            }
            let col_name = format!("lbl_{}", m.name);
            tag_columns.push(col_name.clone());

            let filter = match m.op {
                MatchOp::Equal => col(col_name).eq(lit(m.value.clone())),
                MatchOp::NotEqual => col(col_name).not_eq(lit(m.value.clone())),
                MatchOp::Re(_) => {
                    let stripped = m.value.strip_prefix('^').unwrap_or(&m.value);
                    let stripped = stripped.strip_suffix('$').unwrap_or(stripped);
                    if stripped == ".*" {
                        // =~".*" matches everything — skip
                        continue;
                    }
                    if stripped == ".+" {
                        // =~".+" matches any non-empty string
                        col(col_name).not_eq(lit(""))
                    } else if can_use_like(&m.value) {
                        col(col_name).like(lit(regex_to_like(&m.value)))
                    } else {
                        regex_filter_expr(col_name, &m.value, false)
                    }
                }
                MatchOp::NotRe(_) => {
                    let stripped = m.value.strip_prefix('^').unwrap_or(&m.value);
                    let stripped = stripped.strip_suffix('$').unwrap_or(stripped);
                    if stripped == ".*" {
                        // !~".*" matches nothing — nonsensical, skip
                        continue;
                    }
                    if stripped == ".+" {
                        // !~".+" matches only empty string
                        col(col_name).eq(lit(""))
                    } else if can_use_like(&m.value) {
                        col(col_name).not_like(lit(regex_to_like(&m.value)))
                    } else {
                        regex_filter_expr(col_name, &m.value, true)
                    }
                }
            };
            filters.push(filter);
        }

        tag_columns.sort();
        tag_columns.dedup();
        (tag_columns, filters)
    }

    fn build_table_scan(
        &self,
        session: &SessionContext,
        metric_name: Option<&str>,
        filters: &[logical_expr::Expr],
    ) -> EvalResult<LogicalPlan> {
        let table_name = metric_name
            .map(|m| metric_table_name(m))
            .ok_or_else(|| EvalError::Invalid("metric name is required for table scan".into()))?;

        // Use table_name.as_str() so DataFusion applies the same identifier
        // normalization (lowercasing) as register_table does. TableReference::bare()
        // bypasses normalization, causing case-mismatch lookups.
        let source = session
            .table(table_name.as_str())
            .now_or_never()
            .ok_or_else(|| {
                EvalError::Internal(format!(
                    "table provider for metric '{}' not available synchronously",
                    table_name,
                ))
            })?
            .map_err(|e| EvalError::DataFusion(e))?;

        let plan = source.into_unoptimized_plan();

        // Strip table qualifiers from column names. TableScan qualifies every
        // field as "prom_xxx.col", but our extension nodes (SeriesDivide,
        // RangeManipulate, etc.) use unqualified col() refs in expressions().
        // We must project using QUALIFIED Expr::Column refs (so DataFusion's
        // optimizer can match them to the scan schema) with .alias() to produce
        // unqualified output names. Using unqualified col() here would cause
        // the projection-pushdown optimizer to fail matching columns — see
        // https://github.com/apache/arrow-datafusion/issues/617
        let projections: Vec<logical_expr::Expr> = (0..plan.schema().fields().len())
            .map(|i| {
                let (qualifier, field) = plan.schema().qualified_field(i);
                let column = datafusion::common::Column::new(qualifier.cloned(), field.name());
                logical_expr::Expr::Column(column).alias(field.name().as_str())
            })
            .collect();
        let plan = LogicalPlanBuilder::from(plan)
            .project(projections)?
            .build()?;

        let mut builder = LogicalPlanBuilder::from(plan);

        for filter in filters {
            builder = builder.filter(filter.clone())?;
        }

        Ok(builder.build()?)
    }

    /// Resolve metric names from a VectorSelector — returns one name for exact matches,
    /// or multiple names for `__name__=~"a|b|c"` regex alternation patterns.
    fn resolve_metric_names(&self, vs: &prom::VectorSelector) -> EvalResult<Vec<String>> {
        if let Some(name) = self.extract_metric_name(vs) {
            return Ok(vec![name]);
        }
        let names = extract_metric_names_from_regex(vs);
        if names.is_empty() {
            return Err(EvalError::Invalid(
                "metric name is required for table scan".into(),
            ));
        }
        Ok(names)
    }

    /// Union multiple sub-plans into a single plan. Returns error if empty.
    fn union_plans(&self, plans: Vec<LogicalPlan>) -> EvalResult<LogicalPlan> {
        if plans.is_empty() {
            return Err(EvalError::Invalid("no matching metrics found".into()));
        }
        if plans.len() == 1 {
            return Ok(plans.into_iter().next().unwrap());
        }
        let mut iter = plans.into_iter();
        let first = iter.next().unwrap();
        let mut builder = LogicalPlanBuilder::from(first);
        for plan in iter {
            builder = builder.union(plan).map_err(EvalError::DataFusion)?;
        }
        Ok(builder.build().map_err(EvalError::DataFusion)?)
    }

    /// Extract all `lbl_*` columns from a plan's schema to use as tag columns.
    /// This ensures SeriesDivide separates every unique label combination
    /// (e.g. different `le` buckets) into its own series.
    fn all_label_columns(&self, plan: &LogicalPlan) -> Vec<String> {
        let mut cols: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .filter(|f| f.name().starts_with("lbl_"))
            .map(|f| f.name().clone())
            .collect();
        cols.sort();
        cols.dedup();
        cols
    }

    /// Label columns plus fingerprint — used for Sort + SeriesDivide so that
    /// distinct fingerprints are never merged into one series even when they
    /// share the same label values.  Without this, pre-aggregated counters from
    /// different sources interleave within a single series, causing rate() to
    /// see fake counter resets and produce wildly inflated values.
    fn series_divide_columns(&self, plan: &LogicalPlan) -> Vec<String> {
        let mut cols = self.all_label_columns(plan);
        if plan.schema().has_column_with_unqualified_name(COL_FINGERPRINT) {
            cols.push(COL_FINGERPRINT.to_string());
        }
        cols
    }

    fn apply_sort(&self, plan: LogicalPlan, tag_columns: &[String]) -> EvalResult<LogicalPlan> {
        let mut sort_exprs: Vec<SortExpr> = tag_columns
            .iter()
            .map(|c| SortExpr::new(col(c.as_str()), true, true))
            .collect();
        sort_exprs.push(SortExpr::new(col(COL_TIMESTAMP), true, true));

        let plan = LogicalPlanBuilder::from(plan).sort(sort_exprs)?.build()?;
        Ok(plan)
    }

    /// Restore chronological order after a DataFusion hash aggregation.
    fn sort_aggregate_output(
        &self,
        plan: LogicalPlan,
        group_cols: &[String],
    ) -> EvalResult<LogicalPlan> {
        let mut sort_exprs: Vec<SortExpr> = group_cols
            .iter()
            .map(|c| SortExpr::new(col(c.as_str()), true, true))
            .collect();
        sort_exprs.push(SortExpr::new(col(COL_TIMESTAMP), true, true));

        let plan = LogicalPlanBuilder::from(plan)
            .sort(sort_exprs)?
            .build()?;
        Ok(plan)
    }

    /// Extract group-by columns from aggregation modifier, handling both `by()` and `without()`.
    fn extract_group_columns_with_schema(
        &self,
        modifier: &Option<prom::LabelModifier>,
        plan: &LogicalPlan,
    ) -> Vec<String> {
        match modifier {
            Some(prom::LabelModifier::Include(labels)) => {
                labels.labels.iter().map(|l| format!("lbl_{l}")).collect()
            }
            Some(prom::LabelModifier::Exclude(labels)) => {
                // `without(l1, l2)`: group by all label columns EXCEPT the excluded ones
                let exclude_set: std::collections::HashSet<String> =
                    labels.labels.iter().map(|l| format!("lbl_{l}")).collect();
                plan.schema()
                    .fields()
                    .iter()
                    .filter_map(|f| {
                        let name = f.name();
                        if name.starts_with("lbl_") && !exclude_set.contains(name.as_str()) {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            None => Vec::new(),
        }
    }
}

fn cmp_op_matches(op: u8, l: f64, r: f64) -> bool {
    match op {
        token::T_EQLC => l == r,
        token::T_NEQ => l != r,
        token::T_LSS => l < r,
        token::T_GTR => l > r,
        token::T_LTE => l <= r,
        token::T_GTE => l >= r,
        _ => false,
    }
}

fn cmp_to_df_op(op: u8) -> datafusion_expr::Operator {
    match op {
        token::T_EQLC => datafusion_expr::Operator::Eq,
        token::T_NEQ => datafusion_expr::Operator::NotEq,
        token::T_LSS => datafusion_expr::Operator::Lt,
        token::T_GTR => datafusion_expr::Operator::Gt,
        token::T_LTE => datafusion_expr::Operator::LtEq,
        token::T_GTE => datafusion_expr::Operator::GtEq,
        _ => datafusion_expr::Operator::Eq,
    }
}

/// Convert a PromQL regex to a SQL LIKE pattern (simplified).
///
/// Strips `^` / `$` anchors that Grafana's variable templating adds
/// (e.g. `^.*$`, `^/dev/.*$`). LIKE already matches the full string,
/// so anchors are implicit.
/// Returns true if the regex pattern can be safely converted to a SQL LIKE pattern.
/// Only patterns using literal chars, `.*`, `.+`, or lone `.` qualify.
/// Returns true if the regex pattern can be safely converted to a SQL LIKE pattern.
/// Only patterns using literal chars (no `_` or `%` which are LIKE wildcards),
/// `.*`, `.+`, or lone `.` qualify.
fn can_use_like(regex: &str) -> bool {
    let s = regex.strip_prefix('^').unwrap_or(regex);
    let s = s.strip_suffix('$').unwrap_or(s);
    // Characters that disqualify LIKE: regex metacharacters + LIKE wildcards
    let complex_meta = ['|', '[', ']', '(', ')', '?', '+', '*', '{', '}', '\\', '_', '%'];
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '.' && i + 1 < chars.len() && (chars[i + 1] == '*' || chars[i + 1] == '+')
        {
            i += 2; // `.*` and `.+` are valid LIKE-convertible sequences
        } else if complex_meta.contains(&chars[i]) {
            return false;
        } else {
            i += 1;
        }
    }
    true
}

fn regex_to_like(regex: &str) -> String {
    let s = regex.strip_prefix('^').unwrap_or(regex);
    let s = s.strip_suffix('$').unwrap_or(s);

    if s == ".*" || s == ".+" {
        return "%".to_string();
    }

    // At this point can_use_like guarantees no `_`, `%`, or complex metacharacters.
    // Only `.*`, `.+`, and lone `.` need conversion.
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '.' && i + 1 < chars.len() && chars[i + 1] == '*' {
            result.push('%');
            i += 2;
        } else if chars[i] == '.' && i + 1 < chars.len() && chars[i + 1] == '+' {
            result.push('_');
            result.push('%');
            i += 2;
        } else if chars[i] == '.' {
            result.push('_');
            i += 1;
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result
}

/// Build a filter expression for a regex label matcher using DataFusion's regexp_like.
/// Prometheus =~ is implicitly anchored (full-string match).
fn regex_filter_expr(col_name: String, pattern: &str, negate: bool) -> logical_expr::Expr {
    let anchored = format!("^(?:{pattern})$");
    let expr = regexp_like(col(col_name), lit(anchored), None);
    if negate { expr.not() } else { expr }
}

fn duration_to_ms(d: &prom::ast::Offset) -> i64 {
    match d {
        prom::ast::Offset::Pos(d) => d.as_millis() as i64,
        prom::ast::Offset::Neg(d) => -(d.as_millis() as i64),
    }
}

/// Sanitize a Prometheus metric name into a valid DataFusion table name.
pub fn metric_table_name(metric: &str) -> String {
    format!("prom_{}", crate::promql::sanitize_metric_identifier(metric))
}

/// A metric reference discovered from a PromQL AST.
#[derive(Debug, Clone)]
pub struct MetricRef {
    pub metric_name: String,
    pub label_names: Vec<String>,
    /// Exact equality matchers (`label = "value"`) that can be pushed down to storage.
    /// Regex, not-equal, and `__name__` matchers are excluded.
    pub equality_matchers: Vec<(String, String)>,
}

/// Maximum range-vector width, subquery range, and selector offset in a query.
///
/// Used to extend the ClickHouse fetch window so `rate(foo[1h])` has a full hour
/// of prior samples at the first evaluation step.
pub fn collect_query_fetch_lookback(expr: &PromExpr) -> i64 {
    let mut max_ms = 0i64;
    walk_fetch_lookback(expr, &mut max_ms);
    max_ms
}

fn walk_fetch_lookback(expr: &PromExpr, max_ms: &mut i64) {
    match expr {
        PromExpr::VectorSelector(vs) => {
            if let Some(offset) = &vs.offset {
                *max_ms = (*max_ms).max(duration_to_ms(offset).unsigned_abs() as i64);
            }
        }
        PromExpr::MatrixSelector(ms) => {
            *max_ms = (*max_ms).max(ms.range.as_millis() as i64);
            if let Some(offset) = &ms.vs.offset {
                *max_ms = (*max_ms).max(duration_to_ms(offset).unsigned_abs() as i64);
            }
        }
        PromExpr::Subquery(sq) => {
            *max_ms = (*max_ms).max(sq.range.as_millis() as i64);
            if let Some(offset) = &sq.offset {
                *max_ms = (*max_ms).max(duration_to_ms(offset).unsigned_abs() as i64);
            }
            walk_fetch_lookback(&sq.expr, max_ms);
        }
        PromExpr::Call(call) => {
            for arg in &call.args.args {
                walk_fetch_lookback(arg.as_ref(), max_ms);
            }
        }
        PromExpr::Aggregate(agg) => walk_fetch_lookback(&agg.expr, max_ms),
        PromExpr::Binary(bin) => {
            walk_fetch_lookback(&bin.lhs, max_ms);
            walk_fetch_lookback(&bin.rhs, max_ms);
        }
        PromExpr::Paren(p) => walk_fetch_lookback(&p.expr, max_ms),
        PromExpr::Unary(u) => walk_fetch_lookback(&u.expr, max_ms),
        _ => {}
    }
}

/// Walk a PromQL AST and collect all metric references (metric names and label names).
///
/// When the same metric appears multiple times (e.g. `sum(m{x="a"}) / sum(m)`),
/// only equality matchers common to ALL occurrences are pushed down to storage.
/// This prevents incorrectly restricting data fetched for unfiltered references.
pub fn collect_metric_refs(expr: &PromExpr) -> Vec<MetricRef> {
    let mut refs: HashMap<
        String,
        (
            std::collections::HashSet<String>,
            Vec<std::collections::HashSet<(String, String)>>,
        ),
    > = HashMap::new();
    walk_expr(expr, &mut refs);
    refs.into_iter()
        .map(|(metric_name, (labels, per_occurrence_matchers))| {
            let mut label_names: Vec<String> = labels.into_iter().collect();
            label_names.sort();

            // Only push down matchers present in EVERY occurrence.
            let equality_matchers = if per_occurrence_matchers.is_empty() {
                vec![]
            } else {
                let mut intersection = per_occurrence_matchers[0].clone();
                for occ in &per_occurrence_matchers[1..] {
                    intersection = intersection.intersection(occ).cloned().collect();
                }
                let mut matchers: Vec<(String, String)> = intersection.into_iter().collect();
                matchers.sort();
                matchers
            };

            MetricRef {
                metric_name,
                label_names,
                equality_matchers,
            }
        })
        .collect()
}

/// Accumulated state per metric: (label_names, per-occurrence equality matchers).
type MetricAccum = HashMap<
    String,
    (
        std::collections::HashSet<String>,
        Vec<std::collections::HashSet<(String, String)>>,
    ),
>;

fn walk_expr(expr: &PromExpr, refs: &mut MetricAccum) {
    match expr {
        PromExpr::VectorSelector(vs) => {
            let names = if let Some(name) = extract_metric_from_vs(vs) {
                vec![name]
            } else {
                extract_metric_names_from_regex(vs)
            };
            for name in names {
                let (labels, occurrences) = refs.entry(name).or_default();
                let mut this_occurrence = std::collections::HashSet::new();
                for m in &vs.matchers.matchers {
                    if m.name != METRIC_NAME {
                        labels.insert(m.name.clone());
                        if m.op == MatchOp::Equal {
                            this_occurrence.insert((m.name.clone(), m.value.clone()));
                        }
                    }
                }
                occurrences.push(this_occurrence);
            }
        }
        PromExpr::MatrixSelector(ms) => {
            let names = if let Some(name) = extract_metric_from_vs(&ms.vs) {
                vec![name]
            } else {
                extract_metric_names_from_regex(&ms.vs)
            };
            for name in names {
                let (labels, occurrences) = refs.entry(name).or_default();
                let mut this_occurrence = std::collections::HashSet::new();
                for m in &ms.vs.matchers.matchers {
                    if m.name != METRIC_NAME {
                        labels.insert(m.name.clone());
                        if m.op == MatchOp::Equal {
                            this_occurrence.insert((m.name.clone(), m.value.clone()));
                        }
                    }
                }
                occurrences.push(this_occurrence);
            }
        }
        PromExpr::Call(call) => {
            for arg in &call.args.args {
                walk_expr(arg.as_ref(), refs);
            }
        }
        PromExpr::Aggregate(agg) => {
            walk_expr(&agg.expr, refs);
            if let Some(modifier) = &agg.modifier {
                match modifier {
                    prom::LabelModifier::Include(labels) | prom::LabelModifier::Exclude(labels) => {
                        for (metric_labels, _) in refs.values_mut() {
                            for l in &labels.labels {
                                metric_labels.insert(l.clone());
                            }
                        }
                    }
                }
            }
        }
        PromExpr::Binary(bin) => {
            walk_expr(&bin.lhs, refs);
            walk_expr(&bin.rhs, refs);
            if let Some(modifier) = &bin.modifier {
                let extra_labels: Vec<String> = match &modifier.matching {
                    Some(prom::LabelModifier::Include(labels)) => labels.labels.clone(),
                    Some(prom::LabelModifier::Exclude(labels)) => labels.labels.clone(),
                    None => vec![],
                };
                let card_labels: Vec<String> = modifier
                    .card
                    .labels()
                    .map(|l| l.labels.clone())
                    .unwrap_or_default();
                let all_extra: Vec<String> = extra_labels.into_iter().chain(card_labels).collect();
                if !all_extra.is_empty() {
                    for (metric_labels, _) in refs.values_mut() {
                        for l in &all_extra {
                            metric_labels.insert(l.clone());
                        }
                    }
                }
            }
        }
        PromExpr::Paren(p) => walk_expr(&p.expr, refs),
        PromExpr::Unary(u) => walk_expr(&u.expr, refs),
        PromExpr::Subquery(sq) => walk_expr(&sq.expr, refs),
        _ => {}
    }
}

fn extract_metric_from_vs(vs: &prom::VectorSelector) -> Option<String> {
    vs.name.clone().or_else(|| {
        vs.matchers.matchers.iter().find_map(|m| {
            if m.name == METRIC_NAME && m.op == MatchOp::Equal {
                Some(m.value.clone())
            } else {
                None
            }
        })
    })
}

/// Extract metric names from `__name__=~"a|b|c"` regex alternation patterns.
fn extract_metric_names_from_regex(vs: &prom::VectorSelector) -> Vec<String> {
    for m in &vs.matchers.matchers {
        if m.name == METRIC_NAME {
            if let MatchOp::Re(_) = &m.op {
                let pattern = m.value.trim_start_matches('^').trim_end_matches('$');
                if !pattern.is_empty()
                    && pattern
                        .chars()
                        .all(|c| c.is_alphanumeric() || c == '_' || c == '|' || c == ':')
                {
                    return pattern
                        .split('|')
                        .filter(|s| !s.is_empty())
                        .map(String::from)
                        .collect();
                }
            }
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_to_like_wildcard_all() {
        assert_eq!(regex_to_like(".*"), "%");
        assert_eq!(regex_to_like(".+"), "%");
    }

    #[test]
    fn regex_to_like_anchored_wildcard() {
        assert_eq!(regex_to_like("^.*$"), "%");
        assert_eq!(regex_to_like("^.+$"), "%");
    }

    #[test]
    fn regex_to_like_exact_value() {
        assert_eq!(regex_to_like("argocd"), "argocd");
        assert_eq!(regex_to_like("Healthy"), "Healthy");
        assert_eq!(regex_to_like("Synced"), "Synced");
    }

    #[test]
    fn regex_to_like_prefix_wildcard() {
        assert_eq!(regex_to_like("argocd.*"), "argocd%");
        assert_eq!(regex_to_like("^argocd.*"), "argocd%");
        assert_eq!(regex_to_like("^argocd.*$"), "argocd%");
    }

    #[test]
    fn regex_to_like_dot_becomes_single_char_wildcard() {
        assert_eq!(regex_to_like("v1.2.3"), "v1_2_3");
    }

    #[test]
    fn can_use_like_rejects_alternation() {
        assert!(!can_use_like("Error|Failed"));
    }

    #[test]
    fn can_use_like_rejects_underscore() {
        // Underscore is a LIKE wildcard — must use regexp_like
        assert!(!can_use_like("app_exporter"));
        assert!(!can_use_like("node_exporter.*"));
    }

    #[test]
    fn can_use_like_accepts_simple_patterns() {
        assert!(can_use_like("argocd.*"));
        assert!(can_use_like("^Healthy$"));
        assert!(can_use_like("v1.2.3"));
        assert!(can_use_like(".*"));
    }

    #[test]
    fn can_use_like_rejects_quantifiers() {
        assert!(!can_use_like("foo+bar"));
        assert!(!can_use_like("ba*r"));
        assert!(!can_use_like("x?y"));
    }

    #[test]
    fn collect_query_fetch_lookback_range_vector() {
        let ast = crate::promql::parse("rate(http_requests_total[30m])").unwrap();
        assert_eq!(collect_query_fetch_lookback(&ast), 30 * 60 * 1000);
    }

    #[test]
    fn collect_query_fetch_lookback_nested_and_subquery() {
        let ast = crate::promql::parse("max_over_time((rate(m[5m]))[1h:5m])").unwrap();
        assert_eq!(collect_query_fetch_lookback(&ast), 60 * 60 * 1000);
    }

    #[test]
    fn collect_query_fetch_lookback_offset() {
        let ast = crate::promql::parse("http_requests_total offset 1h").unwrap();
        assert_eq!(collect_query_fetch_lookback(&ast), 60 * 60 * 1000);
    }

    #[test]
    fn collect_query_fetch_lookback_instant_only() {
        let ast = crate::promql::parse("sum(up)").unwrap();
        assert_eq!(collect_query_fetch_lookback(&ast), 0);
    }

    #[test]
    fn metric_table_names_distinct_for_dotted_literal_collision_query() {
        let (sanitized, _) =
            crate::promql::sanitize_otel_names("system.network.io + system_network_io");
        let ast = crate::promql::parse(&sanitized).unwrap();
        let refs = collect_metric_refs(&ast);
        assert_eq!(refs.len(), 2);
        let names: std::collections::HashSet<String> =
            refs.iter().map(|r| metric_table_name(&r.metric_name)).collect();
        assert_eq!(names.len(), 2, "expected distinct table names, got {:?}", names);
    }

    #[test]
    fn metric_table_name_single_otel_metric() {
        let (sanitized, _) = crate::promql::sanitize_otel_names("rate(system.network.io[5m])");
        let ast = crate::promql::parse(&sanitized).unwrap();
        let refs = collect_metric_refs(&ast);
        assert_eq!(refs.len(), 1);
        assert_eq!(metric_table_name(&refs[0].metric_name), "prom_system_network_io");
    }
}
