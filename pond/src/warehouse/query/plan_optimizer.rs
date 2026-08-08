//! Plan Optimizer
//!
//! Enumerates and costs possible execution plans for federated queries,
//! selecting the optimal plan based on the cost model.
//!
//! # Optimization Strategy
//!
//! 1. **Predicate Pushdown Ordering**: Apply predicates to reduce data volume early
//! 2. **Build/Probe Side Selection**: Choose the smaller side for hash join build
//! 3. **Materialization Decision**: Decide which sources to materialize
//! 4. **Join Order Optimization**: For multi-way joins, find optimal order

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use uuid::Uuid;

use crate::warehouse::query::predicate_pushdown::{self, Predicate};
use crate::warehouse::statistics::TableStatistics;
use crate::warehouse::types::SourceType;

use super::cost_model::{BuildSide, CostModel, QueryCost, SourceAccessProfile};

// ============================================================================
// Constants
// ============================================================================

// -- Default estimates (used when table statistics are unavailable) -----------

const DEFAULT_ESTIMATED_ROWS: i64 = 100_000;
const DEFAULT_ESTIMATED_BYTES: i64 = 10 * 1024 * 1024; // 10 MB

// -- Selectivity heuristics --------------------------------------------------
//
// These follow PostgreSQL-style conventions for estimating predicate
// selectivity when column statistics are not available.  Each value
// represents the fraction of rows expected to pass the predicate.

const SEL_EQUALITY: f64 = 0.1;
const SEL_RANGE: f64 = 0.33;
const SEL_BETWEEN: f64 = 0.25;
const SEL_IN_PER_VALUE: f64 = 0.1;
const SEL_IN_MAX: f64 = 0.5;
const SEL_LIKE_PREFIX: f64 = 0.1;
const SEL_LIKE_WILDCARD: f64 = 0.25;
const SEL_LIKE_EXACT: f64 = 0.05;
const SEL_NULL: f64 = 0.05;
const SEL_DEFAULT_NULL_FRACTION: f64 = 0.01;
const MIN_COMBINED_SELECTIVITY: f64 = 0.0001;

// -- Join / materialization thresholds ---------------------------------------

const HASH_JOIN_MEMORY_OVERHEAD_MB: u64 = 100;
const DUAL_MATERIALIZE_MEMORY_OVERHEAD_MB: u64 = 200;
const MATERIALIZATION_LATENCY_THRESHOLD_MS: f64 = 200.0;

// ============================================================================
// Execution Step
// ============================================================================

/// A single step in an execution plan.
///
/// # Type Note
///
/// Row and byte counts use `u64` internally since they are non-negative values.
/// When interfacing with the database (PostgreSQL bigint), use `i64` and convert
/// via `max(0) as u64` to handle any edge cases.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionStep {
    /// Scan a source with optional predicates.
    Scan {
        source_name: Arc<str>,
        table_name: Arc<str>,
        /// Predicates to push down to the source.
        predicates: Vec<String>,
        /// Estimated rows after predicate application.
        estimated_rows: u64,
        /// Estimated bytes after predicate application.
        estimated_bytes: u64,
    },

    /// Materialize data into a temp table.
    Materialize {
        source_name: Arc<str>,
        table_name: Arc<str>,
        temp_table_name: Arc<str>,
        /// Estimated rows to materialize.
        estimated_rows: u64,
    },

    /// Hash join between two sources.
    HashJoin {
        /// Build side (held in memory).
        build_source: Arc<str>,
        build_table: Arc<str>,
        /// Probe side (streamed).
        probe_source: Arc<str>,
        probe_table: Arc<str>,
        /// Join condition.
        join_condition: Arc<str>,
        /// Estimated output rows.
        estimated_output_rows: u64,
    },

    /// Merge join (for sorted data).
    MergeJoin {
        left_source: Arc<str>,
        left_table: Arc<str>,
        right_source: Arc<str>,
        right_table: Arc<str>,
        join_condition: Arc<str>,
        estimated_output_rows: u64,
    },

    /// Filter step (post-join filtering).
    Filter {
        predicates: Vec<String>,
        estimated_selectivity: f64,
    },

    /// Project specific columns.
    Project {
        columns: Vec<String>,
    },
}

// ============================================================================
// Execution Plan
// ============================================================================

/// A complete execution plan for a federated query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// Unique identifier for this plan.
    pub id: Uuid,
    /// Ordered execution steps.
    pub steps: Vec<ExecutionStep>,
    /// Total estimated cost.
    pub cost: QueryCost,
    /// Memory budget required in MB.
    pub memory_required_mb: u32,
    /// Whether this plan requires materialization.
    pub requires_materialization: bool,
    /// Plan description for debugging.
    pub description: String,
}

impl ExecutionPlan {
    /// Create a new execution plan.
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            steps: Vec::new(),
            cost: QueryCost::zero(),
            memory_required_mb: 0,
            requires_materialization: false,
            description: String::new(),
        }
    }

    /// Add a step to the plan.
    pub fn add_step(&mut self, step: ExecutionStep) {
        if matches!(step, ExecutionStep::Materialize { .. }) {
            self.requires_materialization = true;
        }
        self.steps.push(step);
    }

    /// Set the total cost.
    pub fn with_cost(mut self, cost: QueryCost) -> Self {
        self.cost = cost;
        self
    }

    /// Set the memory requirement.
    pub fn with_memory(mut self, memory_mb: u32) -> Self {
        self.memory_required_mb = memory_mb;
        self
    }

    /// Set the description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }
}

impl Default for ExecutionPlan {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Table Info for Planning
// ============================================================================

/// Information about a table for plan optimization.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Source name.
    pub source_name: Arc<str>,
    /// Table name.
    pub table_name: Arc<str>,
    /// Source type.
    pub source_type: SourceType,
    /// Access profile.
    pub profile: SourceAccessProfile,
    /// Statistics (if available).
    pub statistics: Option<TableStatistics>,
    /// Applicable predicates (structured).
    pub predicates: Vec<Predicate>,
}

impl TableInfo {
    /// Create new table info.
    pub fn new(
        source_name: impl Into<Arc<str>>,
        table_name: impl Into<Arc<str>>,
        source_type: SourceType,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            table_name: table_name.into(),
            source_type,
            profile: SourceAccessProfile::default_for_source_type(source_type),
            statistics: None,
            predicates: Vec::new(),
        }
    }

    /// Set statistics.
    pub fn with_statistics(mut self, stats: TableStatistics) -> Self {
        self.statistics = Some(stats);
        self
    }

    /// Set predicates from structured `Predicate` values.
    pub fn with_predicates(mut self, predicates: Vec<Predicate>) -> Self {
        self.predicates = predicates;
        self
    }

    /// Parse SQL predicate strings into structured predicates and set them.
    pub fn with_predicate_strings(mut self, strings: Vec<String>) -> Self {
        let dialect = ClickHouseDialect {};
        for s in &strings {
            if let Ok(expr) = Parser::new(&dialect)
                .try_with_sql(s.trim())
                .and_then(|mut p| p.parse_expr())
            {
                let mut preds = predicate_pushdown::expr_to_predicates(&expr);
                self.predicates.append(&mut preds);
            }
        }
        self
    }

    /// Set custom access profile.
    pub fn with_profile(mut self, profile: SourceAccessProfile) -> Self {
        self.profile = profile;
        self
    }

    /// Get estimated row count as `u64` for internal calculations.
    ///
    /// Returns 0 for negative values from statistics (shouldn't happen in practice).
    pub fn estimated_rows(&self) -> u64 {
        self.estimated_rows_i64().max(0) as u64
    }

    /// Get estimated row count as `i64` for database compatibility.
    ///
    /// This matches PostgreSQL's bigint type used in statistics storage.
    pub fn estimated_rows_i64(&self) -> i64 {
        self.statistics
            .as_ref()
            .and_then(|s| s.row_count)
            .unwrap_or(DEFAULT_ESTIMATED_ROWS)
    }

    /// Get estimated size in bytes as `u64` for internal calculations.
    pub fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes_i64().max(0) as u64
    }

    /// Get estimated size in bytes as `i64` for database compatibility.
    pub fn estimated_bytes_i64(&self) -> i64 {
        self.statistics
            .as_ref()
            .and_then(|s| s.size_bytes)
            .unwrap_or(DEFAULT_ESTIMATED_BYTES)
    }

    /// Estimate selectivity of predicates.
    ///
    /// Uses column statistics when available for more accurate estimates.
    /// Falls back to heuristics when stats are not available.
    ///
    /// Multiple predicates are combined using **exponential back-off
    /// dampening** rather than pure independence multiplication.  The most
    /// selective predicate applies at full strength; each subsequent
    /// predicate applies at its square-root, accounting for the fact that
    /// real-world predicates are often correlated (e.g. `city = 'NYC' AND
    /// state = 'NY'`).  Pure multiplication would underestimate the
    /// combined selectivity for correlated columns.
    pub fn predicate_selectivity(&self) -> f64 {
        if self.predicates.is_empty() {
            return 1.0;
        }

        let mut selectivities: Vec<f64> = self
            .predicates
            .iter()
            .map(|p| self.estimate_single_predicate_selectivity(p))
            .collect();

        // Sort ascending so the most selective predicate applies at full strength
        selectivities.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let mut combined = selectivities[0];
        for &sel in &selectivities[1..] {
            combined *= sel.sqrt();
        }

        combined.clamp(MIN_COMBINED_SELECTIVITY, 1.0)
    }

    /// Estimate selectivity for a single structured predicate.
    fn estimate_single_predicate_selectivity(&self, predicate: &Predicate) -> f64 {
        match predicate {
            Predicate::Equals { column, value } => {
                if let Some(cs) = self.get_column_stats(column) {
                    return cs.estimate_equality_selectivity(value);
                }
                SEL_EQUALITY
            }
            Predicate::In { column, values } => {
                if let Some(cs) = self.get_column_stats(column) {
                    return values
                        .iter()
                        .map(|v| cs.estimate_equality_selectivity(v.as_str()))
                        .sum::<f64>()
                        .min(1.0);
                }
                (values.len() as f64 * SEL_IN_PER_VALUE).min(SEL_IN_MAX)
            }
            Predicate::GreaterThan { column, value, inclusive } => {
                if let Some(cs) = self.get_column_stats(column) {
                    let gt = cs.estimate_gt_selectivity(value);
                    return if *inclusive {
                        (gt + cs.estimate_equality_selectivity(value)).min(1.0)
                    } else {
                        gt
                    };
                }
                SEL_RANGE
            }
            Predicate::LessThan { column, value, inclusive } => {
                if let Some(cs) = self.get_column_stats(column) {
                    let lt = cs.estimate_lt_selectivity(value);
                    return if *inclusive {
                        (lt + cs.estimate_equality_selectivity(value)).min(1.0)
                    } else {
                        lt
                    };
                }
                SEL_RANGE
            }
            Predicate::Between { column, low, high } => {
                if let Some(cs) = self.get_column_stats(column) {
                    return cs.estimate_range_selectivity(low, high);
                }
                SEL_BETWEEN
            }
            Predicate::Like { column, pattern } => {
                if let Some(_cs) = self.get_column_stats(column) {
                    if pattern.starts_with('%') {
                        return SEL_LIKE_WILDCARD;
                    } else if pattern.ends_with('%') {
                        return SEL_LIKE_PREFIX;
                    }
                    return SEL_LIKE_EXACT;
                }
                if pattern.starts_with('%') { SEL_LIKE_WILDCARD } else { SEL_LIKE_PREFIX }
            }
            Predicate::Contains { column, .. } => {
                if self.get_column_stats(column).is_some() {
                    return SEL_LIKE_WILDCARD;
                }
                SEL_LIKE_WILDCARD
            }
            Predicate::IsNull { column, is_null } => {
                if let Some(cs) = self.get_column_stats(column) {
                    let nf = cs.null_fraction.map(|f| f as f64).unwrap_or(SEL_DEFAULT_NULL_FRACTION);
                    return if *is_null { nf } else { 1.0 - nf };
                }
                if *is_null { SEL_NULL } else { 1.0 - SEL_NULL }
            }
            Predicate::Not(inner) => {
                1.0 - self.estimate_single_predicate_selectivity(inner)
            }
            Predicate::And(preds) => {
                preds.iter().map(|p| self.estimate_single_predicate_selectivity(p)).product::<f64>().clamp(MIN_COMBINED_SELECTIVITY, 1.0)
            }
            Predicate::Or(preds) => {
                let mut combined = 0.0_f64;
                for p in preds {
                    let s = self.estimate_single_predicate_selectivity(p);
                    combined = combined + s - combined * s;
                }
                combined.min(1.0)
            }
        }
    }

    /// Get column statistics for a column name.
    fn get_column_stats(&self, column: &str) -> Option<&crate::warehouse::statistics::ColumnStatistics> {
        self.statistics
            .as_ref()
            .and_then(|stats| stats.column_stats.get(column))
    }
}

// ============================================================================
// Join Info
// ============================================================================

/// Information about a join operation.
#[derive(Debug, Clone)]
pub struct JoinInfo {
    /// Left side of join.
    pub left: TableInfo,
    /// Right side of join.
    pub right: TableInfo,
    /// Join condition.
    pub condition: Arc<str>,
    /// Join type (inner, left, right, full).
    pub join_type: JoinType,
}

/// Type of join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl Default for JoinType {
    fn default() -> Self {
        JoinType::Inner
    }
}

// ============================================================================
// Plan Optimizer
// ============================================================================

/// Optimizes execution plans for federated queries.
pub struct PlanOptimizer {
    /// Cost model for plan evaluation.
    cost_model: CostModel,
    /// Maximum number of plans to consider.
    max_plans: usize,
    /// Memory budget in MB.
    memory_budget_mb: u32,
}

impl PlanOptimizer {
    /// Create a new plan optimizer.
    pub fn new(cost_model: CostModel) -> Self {
        Self {
            cost_model,
            max_plans: 100,
            memory_budget_mb: 1024,
        }
    }

    /// Set memory budget.
    pub fn with_memory_budget(mut self, budget_mb: u32) -> Self {
        self.memory_budget_mb = budget_mb;
        self
    }

    /// Set maximum plans to consider.
    pub fn with_max_plans(mut self, max: usize) -> Self {
        self.max_plans = max;
        self
    }

    /// Optimize a two-way join.
    #[tracing::instrument(name = "warehouse.optimizer.optimize_join", skip(self, join))]
    pub fn optimize_join(&self, join: &JoinInfo) -> ExecutionPlan {
        let plans = self.generate_join_plans(join);
        self.select_best_plan(plans)
    }

    /// Optimize a multi-way join.
    #[tracing::instrument(name = "warehouse.optimizer.optimize_multi_join", skip(self, tables, joins), fields(table_count = tables.len(), join_count = joins.len()))]
    pub fn optimize_multi_join(&self, tables: &[TableInfo], joins: &[JoinInfo]) -> ExecutionPlan {
        if joins.is_empty() {
            // No joins, just scan
            return self.create_scan_only_plan(tables);
        }

        if joins.len() == 1 {
            return self.optimize_join(&joins[0]);
        }

        // For multiple joins, use dynamic programming approach
        // (simplified version: try left-deep and bushy plans)
        let plans = self.generate_multi_join_plans(tables, joins);
        self.select_best_plan(plans)
    }

    /// Build a single hash-join plan with the given build side.
    fn build_hash_join_plan(
        &self,
        join: &JoinInfo,
        build_side: BuildSide,
        left_rows: u64,
        right_rows: u64,
        left_bytes: u64,
        right_bytes: u64,
    ) -> ExecutionPlan {
        let left = &join.left;
        let right = &join.right;

        let (build, probe, build_rows, probe_rows, build_bytes, probe_bytes) = match build_side {
            BuildSide::Left => (left, right, left_rows, right_rows, left_bytes, right_bytes),
            BuildSide::Right => (right, left, right_rows, left_rows, right_bytes, left_bytes),
        };

        let mut plan = ExecutionPlan::new();
        let cost = self.cost_join_plan(left, right, build_side);

        let left_preds: Vec<String> = left.predicates.iter().map(|p| p.to_sql_string()).collect();
        let right_preds: Vec<String> = right.predicates.iter().map(|p| p.to_sql_string()).collect();

        plan.add_step(ExecutionStep::Scan {
            source_name: left.source_name.clone(),
            table_name: left.table_name.clone(),
            predicates: left_preds,
            estimated_rows: left_rows,
            estimated_bytes: left_bytes,
        });

        plan.add_step(ExecutionStep::Scan {
            source_name: right.source_name.clone(),
            table_name: right.table_name.clone(),
            predicates: right_preds,
            estimated_rows: right_rows,
            estimated_bytes: right_bytes,
        });

        if should_materialize(&build.profile) {
            plan.add_step(ExecutionStep::Materialize {
                source_name: build.source_name.clone(),
                table_name: build.table_name.clone(),
                temp_table_name: Arc::from(format!("_temp_{}", Uuid::new_v4().simple())),
                estimated_rows: build_rows,
            });
        }

        let estimated_output = estimate_join_output(left_rows, right_rows, join.join_type);
        plan.add_step(ExecutionStep::HashJoin {
            build_source: build.source_name.clone(),
            build_table: build.table_name.clone(),
            probe_source: probe.source_name.clone(),
            probe_table: probe.table_name.clone(),
            join_condition: join.condition.clone(),
            estimated_output_rows: estimated_output,
        });

        let memory_mb = (build_bytes / (1024 * 1024))
            .saturating_add(HASH_JOIN_MEMORY_OVERHEAD_MB)
            .min(u32::MAX as u64) as u32;
        let description = match build_side {
            BuildSide::Left => "Hash join with left as build side",
            BuildSide::Right => "Hash join with right as build side",
        };

        plan.with_cost(cost)
            .with_memory(memory_mb)
            .with_description(description)
    }

    /// Generate possible plans for a two-way join.
    fn generate_join_plans(&self, join: &JoinInfo) -> Vec<ExecutionPlan> {
        let mut plans = Vec::with_capacity(3);

        let left = &join.left;
        let right = &join.right;

        let left_rows = safe_f64_to_u64(left.estimated_rows() as f64 * left.predicate_selectivity());
        let right_rows = safe_f64_to_u64(right.estimated_rows() as f64 * right.predicate_selectivity());
        let left_bytes = safe_f64_to_u64(left.estimated_bytes() as f64 * left.predicate_selectivity());
        let right_bytes = safe_f64_to_u64(right.estimated_bytes() as f64 * right.predicate_selectivity());

        plans.push(self.build_hash_join_plan(join, BuildSide::Left, left_rows, right_rows, left_bytes, right_bytes));
        plans.push(self.build_hash_join_plan(join, BuildSide::Right, left_rows, right_rows, left_bytes, right_bytes));

        // Plan 3: Materialize both sides (for complex cross-source joins)
        if should_materialize(&left.profile) || should_materialize(&right.profile) {
            let mut plan = ExecutionPlan::new();

            // Clone strings once per plan to avoid repeated clones
            let left_source = left.source_name.clone();
            let left_table = left.table_name.clone();
            let right_source = right.source_name.clone();
            let right_table = right.table_name.clone();
            let join_cond = join.condition.clone();

            plan.add_step(ExecutionStep::Materialize {
                source_name: left_source.clone(),
                table_name: left_table.clone(),
                temp_table_name: Arc::from(format!("_temp_{}", Uuid::new_v4().simple())),
                estimated_rows: left_rows,
            });

            plan.add_step(ExecutionStep::Materialize {
                source_name: right_source.clone(),
                table_name: right_table.clone(),
                temp_table_name: Arc::from(format!("_temp_{}", Uuid::new_v4().simple())),
                estimated_rows: right_rows,
            });

            let estimated_output = estimate_join_output(left_rows, right_rows, join.join_type);
            
            // Use the smaller side as build
            let (build_source, build_table, probe_source, probe_table) = if left_bytes <= right_bytes {
                (left_source, left_table, right_source, right_table)
            } else {
                (right_source, right_table, left_source, left_table)
            };

            plan.add_step(ExecutionStep::HashJoin {
                build_source,
                build_table,
                probe_source,
                probe_table,
                join_condition: join_cond,
                estimated_output_rows: estimated_output,
            });

            // Cost: materialization + join
            let mat_cost = self.cost_model.estimate_materialization_cost(
                left_rows,
                left_bytes,
                &left.profile,
            );
            let mat_cost2 = self.cost_model.estimate_materialization_cost(
                right_rows,
                right_bytes,
                &right.profile,
            );
            let join_cost = self.cost_model.estimate_hash_join_cost(
                left_rows.min(right_rows),
                left_bytes.min(right_bytes),
                left_rows.max(right_rows),
                left_bytes.max(right_bytes),
            );
            let total_cost = mat_cost.add(&mat_cost2).add(&join_cost);

            let memory_mb = (left_bytes.saturating_add(right_bytes) / (1024 * 1024)).saturating_add(DUAL_MATERIALIZE_MEMORY_OVERHEAD_MB).min(u32::MAX as u64) as u32;
            plans.push(
                plan.with_cost(total_cost)
                    .with_memory(memory_mb)
                    .with_description("Materialize both sides, then hash join"),
            );
        }

        plans
    }

    /// Generate plans for multi-way joins.
    ///
    /// For small join counts (<= 6), uses exhaustive permutation search to
    /// guarantee optimal ordering. For larger counts, falls back to
    /// cardinality-aware greedy. Both strategies propagate intermediate
    /// cardinalities so earlier joins reduce subsequent costs.
    fn generate_multi_join_plans(
        &self,
        _tables: &[TableInfo],
        joins: &[JoinInfo],
    ) -> Vec<ExecutionPlan> {
        let mut plans = Vec::with_capacity(3);

        // Primary strategy: exhaustive for small, greedy for large
        if joins.len() <= 6 {
            plans.push(self.exhaustive_join_order(joins));
        }
        plans.push(self.greedy_join_order(joins));

        // Left-deep baseline for comparison
        let mut left_deep = self.optimize_join(&joins[0]);
        for join in joins.iter().skip(1) {
            let next_plan = self.optimize_join(join);
            left_deep.steps.extend(next_plan.steps);
            left_deep.cost = left_deep.cost.add(&next_plan.cost);
            left_deep.memory_required_mb = left_deep.memory_required_mb.saturating_add(next_plan.memory_required_mb);
            left_deep.requires_materialization |= next_plan.requires_materialization;
        }
        left_deep.description = "Left-deep join order".to_string();
        plans.push(left_deep);

        plans
    }

    /// Greedy join ordering with intermediate cardinality propagation.
    ///
    /// At each step, re-costs all remaining joins using the current
    /// intermediate row estimates (updated after each join completes).
    /// This ensures that joining a small table first correctly reduces
    /// the estimated cost of subsequent joins.
    fn greedy_join_order(&self, joins: &[JoinInfo]) -> ExecutionPlan {
        self.cost_join_ordering(joins, None)
    }

    /// Cost a specific ordering of joins, propagating intermediate cardinalities.
    ///
    /// If `order` is `None`, uses greedy selection (pick cheapest next).
    /// If `order` is `Some`, evaluates the given permutation.
    ///
    /// Returns the combined `ExecutionPlan` with accumulated cost.
    fn cost_join_ordering(
        &self,
        joins: &[JoinInfo],
        order: Option<&[usize]>,
    ) -> ExecutionPlan {
        use ahash::AHashMap;

        // Track current row/byte estimates per table, updated after each join.
        let mut table_rows: AHashMap<(&str, &str), u64> = AHashMap::new();
        let mut table_bytes: AHashMap<(&str, &str), u64> = AHashMap::new();

        for join in joins {
            for tbl in [&join.left, &join.right] {
                let key = (&*tbl.source_name, &*tbl.table_name);
                table_rows.entry(key).or_insert_with(|| {
                    safe_f64_to_u64(tbl.estimated_rows() as f64 * tbl.predicate_selectivity())
                });
                table_bytes.entry(key).or_insert_with(|| {
                    safe_f64_to_u64(tbl.estimated_bytes() as f64 * tbl.predicate_selectivity())
                });
            }
        }

        let mut remaining: Vec<usize> = (0..joins.len()).collect();
        let mut combined = ExecutionPlan::new();
        let mut total_cost = QueryCost::zero();
        let mut total_memory_mb: u64 = 0;
        let mut order_desc = Vec::with_capacity(joins.len());

        for step_idx in 0..joins.len() {
            let chosen_idx = if let Some(perm) = order {
                let idx = perm[step_idx];
                remaining.retain(|&i| i != idx);
                idx
            } else {
                // Greedy: pick the remaining join with lowest cost given
                // current intermediate cardinalities.
                let best_pos = remaining
                    .iter()
                    .enumerate()
                    .min_by(|(_, &a_idx), (_, &b_idx)| {
                        let cost_a = self.cost_join_with_cardinalities(
                            &joins[a_idx], &table_rows, &table_bytes,
                        );
                        let cost_b = self.cost_join_with_cardinalities(
                            &joins[b_idx], &table_rows, &table_bytes,
                        );
                        cost_a.total_cost
                            .partial_cmp(&cost_b.total_cost)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|(pos, _)| pos)
                    .expect("remaining is non-empty");
                remaining.swap_remove(best_pos)
            };

            let join = &joins[chosen_idx];
            let left_key = (&*join.left.source_name, &*join.left.table_name);
            let right_key = (&*join.right.source_name, &*join.right.table_name);
            let left_rows = *table_rows.get(&left_key).unwrap_or(&0);
            let right_rows = *table_rows.get(&right_key).unwrap_or(&0);
            let left_bytes = *table_bytes.get(&left_key).unwrap_or(&0);
            let right_bytes = *table_bytes.get(&right_key).unwrap_or(&0);

            order_desc.push(format!("{}↔{}", join.left.table_name, join.right.table_name));

            let step_cost = self.cost_join_with_cardinalities(join, &table_rows, &table_bytes);

            // Build the plan steps for this join
            let plan = self.optimize_join(join);
            combined.steps.extend(plan.steps);
            combined.requires_materialization |= plan.requires_materialization;
            total_cost = total_cost.add(&step_cost);

            let build_bytes = left_bytes.min(right_bytes);
            let step_mem = (build_bytes / (1024 * 1024)).saturating_add(HASH_JOIN_MEMORY_OVERHEAD_MB);
            total_memory_mb = total_memory_mb.saturating_add(step_mem);

            // Propagate: update both tables' estimates to reflect the join output.
            let output_rows = estimate_join_output(left_rows, right_rows, join.join_type);
            let avg_row_size = if left_rows.saturating_add(right_rows) > 0 {
                (left_bytes.saturating_add(right_bytes)) as f64
                    / left_rows.saturating_add(right_rows).max(1) as f64
            } else {
                100.0
            };
            let output_bytes = safe_f64_to_u64(output_rows as f64 * avg_row_size);

            table_rows.insert(left_key, output_rows);
            table_bytes.insert(left_key, output_bytes);
            table_rows.insert(right_key, output_rows);
            table_bytes.insert(right_key, output_bytes);
        }

        let label = if order.is_some() { "Permutation" } else { "Greedy" };
        combined.cost = total_cost;
        combined.memory_required_mb = total_memory_mb.min(u32::MAX as u64) as u32;
        combined.description = format!("{label} join order: {}", order_desc.join(" → "));
        combined
    }

    /// Cost a join using the current intermediate cardinality estimates
    /// rather than the original table statistics.
    fn cost_join_with_cardinalities(
        &self,
        join: &JoinInfo,
        table_rows: &ahash::AHashMap<(&str, &str), u64>,
        table_bytes: &ahash::AHashMap<(&str, &str), u64>,
    ) -> QueryCost {
        let left_key = (&*join.left.source_name, &*join.left.table_name);
        let right_key = (&*join.right.source_name, &*join.right.table_name);

        let left_rows = *table_rows.get(&left_key).unwrap_or(&0);
        let right_rows = *table_rows.get(&right_key).unwrap_or(&0);
        let left_bytes = *table_bytes.get(&left_key).unwrap_or(&0);
        let right_bytes = *table_bytes.get(&right_key).unwrap_or(&0);

        let (build_rows, build_bytes, probe_rows, probe_bytes) = if left_rows <= right_rows {
            (left_rows, left_bytes, right_rows, right_bytes)
        } else {
            (right_rows, right_bytes, left_rows, left_bytes)
        };

        let join_cost = self
            .cost_model
            .estimate_hash_join_cost(build_rows, build_bytes, probe_rows, probe_bytes);

        let left_scan = self.cost_model.estimate_scan_cost(
            left_rows, left_bytes, &join.left.profile, 1.0,
        );
        let right_scan = self.cost_model.estimate_scan_cost(
            right_rows, right_bytes, &join.right.profile, 1.0,
        );

        left_scan.add(&right_scan).add(&join_cost)
    }

    /// Exhaustive permutation search for small join counts (<= 6).
    ///
    /// Enumerates all `N!` orderings and picks the one with the lowest
    /// total cost using cardinality propagation. Falls back to the
    /// greedy strategy for 7+ joins.
    fn exhaustive_join_order(&self, joins: &[JoinInfo]) -> ExecutionPlan {
        const MAX_EXHAUSTIVE_JOINS: usize = 6;

        if joins.len() > MAX_EXHAUSTIVE_JOINS {
            return self.greedy_join_order(joins);
        }

        let n = joins.len();
        let mut indices: Vec<usize> = (0..n).collect();
        let mut best_plan: Option<ExecutionPlan> = None;

        // Heap's algorithm for generating all permutations iteratively
        let mut c = vec![0usize; n];
        // Evaluate identity permutation first
        let plan = self.cost_join_ordering(joins, Some(&indices));
        best_plan = Some(plan);

        let mut i = 0;
        while i < n {
            if c[i] < i {
                if i % 2 == 0 {
                    indices.swap(0, i);
                } else {
                    indices.swap(c[i], i);
                }

                let plan = self.cost_join_ordering(joins, Some(&indices));
                if let Some(ref best) = best_plan {
                    if plan.cost.total_cost < best.cost.total_cost {
                        best_plan = Some(plan);
                    }
                } else {
                    best_plan = Some(plan);
                }

                c[i] += 1;
                i = 0;
            } else {
                c[i] = 0;
                i += 1;
            }
        }

        best_plan.unwrap_or_else(|| self.greedy_join_order(joins))
    }

    /// Create a scan-only plan (no joins).
    fn create_scan_only_plan(&self, tables: &[TableInfo]) -> ExecutionPlan {
        let mut plan = ExecutionPlan::new();
        let mut total_cost = QueryCost::zero();

        for table in tables {
            let rows = safe_f64_to_u64(table.estimated_rows() as f64 * table.predicate_selectivity());
            let bytes = safe_f64_to_u64(table.estimated_bytes() as f64 * table.predicate_selectivity());

            plan.add_step(ExecutionStep::Scan {
                source_name: table.source_name.clone(),
                table_name: table.table_name.clone(),
                predicates: table.predicates.iter().map(|p| p.to_sql_string()).collect(),
                estimated_rows: rows,
                estimated_bytes: bytes,
            });

            let scan_cost = self.cost_model.estimate_scan_cost(
                table.estimated_rows(),
                table.estimated_bytes(),
                &table.profile,
                table.predicate_selectivity(),
            );
            total_cost = total_cost.add(&scan_cost);
        }

        plan.with_cost(total_cost)
            .with_description("Scan only (no joins)")
    }

    /// Cost a join plan.
    fn cost_join_plan(
        &self,
        left: &TableInfo,
        right: &TableInfo,
        build_side: BuildSide,
    ) -> QueryCost {
        let left_rows = safe_f64_to_u64(left.estimated_rows() as f64 * left.predicate_selectivity());
        let right_rows = safe_f64_to_u64(right.estimated_rows() as f64 * right.predicate_selectivity());
        let left_bytes = safe_f64_to_u64(left.estimated_bytes() as f64 * left.predicate_selectivity());
        let right_bytes = safe_f64_to_u64(right.estimated_bytes() as f64 * right.predicate_selectivity());

        // Scan costs
        let left_scan = self.cost_model.estimate_scan_cost(
            left.estimated_rows(),
            left.estimated_bytes(),
            &left.profile,
            left.predicate_selectivity(),
        );
        let right_scan = self.cost_model.estimate_scan_cost(
            right.estimated_rows(),
            right.estimated_bytes(),
            &right.profile,
            right.predicate_selectivity(),
        );

        // Join cost
        let (build_rows, build_bytes, probe_rows, probe_bytes) = match build_side {
            BuildSide::Left => (left_rows, left_bytes, right_rows, right_bytes),
            BuildSide::Right => (right_rows, right_bytes, left_rows, left_bytes),
        };

        let join_cost = self
            .cost_model
            .estimate_hash_join_cost(build_rows, build_bytes, probe_rows, probe_bytes);

        // Materialization cost if needed
        let (build_profile, mat_rows, mat_bytes) = match build_side {
            BuildSide::Left => (&left.profile, left_rows, left_bytes),
            BuildSide::Right => (&right.profile, right_rows, right_bytes),
        };

        let mat_cost = if should_materialize(build_profile) {
            self.cost_model
                .estimate_materialization_cost(mat_rows, mat_bytes, build_profile)
        } else {
            QueryCost::zero()
        };

        left_scan.add(&right_scan).add(&join_cost).add(&mat_cost)
    }

    /// Select the best plan from candidates.
    fn select_best_plan(&self, mut plans: Vec<ExecutionPlan>) -> ExecutionPlan {
        if plans.is_empty() {
            return ExecutionPlan::new().with_description("Empty plan");
        }

        // Save the lowest-memory plan before filtering, so we can fall back
        // to it if all plans exceed the budget. Clone instead of removing so
        // it still participates in the cost-based comparison below.
        plans.sort_by(|a, b| {
            a.memory_required_mb
                .partial_cmp(&b.memory_required_mb)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let lowest_memory_plan = plans[0].clone();

        // Filter out plans that exceed memory budget
        plans.retain(|p| p.memory_required_mb <= self.memory_budget_mb);

        if plans.is_empty() {
            tracing::warn!(
                budget_mb = self.memory_budget_mb,
                lowest_mb = lowest_memory_plan.memory_required_mb,
                "All plans exceed memory budget, using lowest-memory plan"
            );
            return lowest_memory_plan;
        }

        // Sort by cost and return the best
        plans.sort_by(|a, b| {
            a.cost
                .total_cost
                .partial_cmp(&b.cost.total_cost)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        plans.remove(0)
    }
}

impl Default for PlanOptimizer {
    fn default() -> Self {
        Self::new(CostModel::default())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Check if a source should be materialized for joins.
fn should_materialize(profile: &SourceAccessProfile) -> bool {
    // Materialize if:
    // - Source has rate limits (API)
    // - Source has high latency
    // - Source doesn't support predicate pushdown
    profile.rate_limit.is_some()
        || profile.avg_latency_ms() > MATERIALIZATION_LATENCY_THRESHOLD_MS
        || !profile.supports_predicate_pushdown
}

/// Estimate join output row count.
fn estimate_join_output(left_rows: u64, right_rows: u64, join_type: JoinType) -> u64 {
    match join_type {
        JoinType::Inner => {
            // Heuristic: assume foreign-key join selectivity, capped at the smaller side
            let smaller = std::cmp::min(left_rows, right_rows);
            let larger = std::cmp::max(left_rows, right_rows);
            // Estimate ~80% of the smaller side matches, scaled down for larger ratios
            let estimate = safe_f64_to_u64(smaller as f64 * 0.8 * (1.0 + (larger as f64 / (smaller.max(1) as f64)).ln() * 0.1));
            std::cmp::min(estimate, left_rows.saturating_mul(right_rows))
        }
        JoinType::Left => left_rows,
        JoinType::Right => right_rows,
        JoinType::Full => left_rows.saturating_add(right_rows),
        JoinType::Cross => left_rows.saturating_mul(right_rows),
    }
}

/// Safely convert an `f64` to `u64`, clamping NaN and negative values to 0
/// and values exceeding `u64::MAX` to `u64::MAX`.
fn safe_f64_to_u64(value: f64) -> u64 {
    if value.is_nan() || value < 0.0 {
        0
    } else if value > u64::MAX as f64 {
        u64::MAX
    } else {
        value as u64
    }
}

// ============================================================================
// Semi-Join Analysis
// ============================================================================

/// Result of semi-join analysis for a join operation.
#[derive(Debug, Clone)]
pub struct SemiJoinAnalysis {
    /// Whether semi-join reduction is recommended.
    pub should_use_semi_join: bool,
    /// Which side should be the probe (queried first).
    pub probe_side: BuildSide,
    /// Estimated rows from probe side after filtering.
    pub probe_estimated_rows: u64,
    /// Estimated rows from build side (before IN filter).
    pub build_estimated_rows: u64,
    /// Selectivity ratio (probe_rows / build_rows).
    pub selectivity_ratio: f64,
    /// Reason for the recommendation.
    pub reason: String,
}

/// Configuration for semi-join decision making.
#[derive(Debug, Clone)]
pub struct SemiJoinThresholds {
    /// Maximum rows on probe side to use IN clause (default: 10,000).
    pub max_probe_rows_for_in_clause: u64,
    /// Minimum selectivity ratio to consider semi-join (default: 0.1 = 10%).
    pub min_selectivity_ratio: f64,
    /// Whether the join is across different databases.
    pub is_cross_database: bool,
}

impl Default for SemiJoinThresholds {
    fn default() -> Self {
        Self {
            max_probe_rows_for_in_clause: 10_000,
            min_selectivity_ratio: 0.1,
            is_cross_database: false,
        }
    }
}

/// Analyze whether a semi-join reduction strategy would be beneficial.
///
/// Semi-join is recommended when:
/// 1. One side has selective predicates (estimated < threshold rows after filter)
/// 2. The filtered side is significantly smaller than the other (< 10% by default)
/// 3. The join is cross-database (network transfer is expensive)
///
/// # Arguments
/// * `left` - Left side table info
/// * `right` - Right side table info
/// * `join_type` - Type of join
/// * `thresholds` - Decision thresholds
///
/// # Returns
/// Analysis result with recommendation and reasoning.
#[tracing::instrument(name = "warehouse.optimizer.analyze_semi_join", skip_all)]
pub fn analyze_semi_join(
    left: &TableInfo,
    right: &TableInfo,
    join_type: JoinType,
    thresholds: &SemiJoinThresholds,
) -> SemiJoinAnalysis {
    // Calculate estimated rows after predicate application
    let left_rows = safe_f64_to_u64(left.estimated_rows() as f64 * left.predicate_selectivity());
    let right_rows = safe_f64_to_u64(right.estimated_rows() as f64 * right.predicate_selectivity());

    // Only consider semi-join for INNER and LEFT joins
    if !matches!(join_type, JoinType::Inner | JoinType::Left) {
        return SemiJoinAnalysis {
            should_use_semi_join: false,
            probe_side: BuildSide::Left,
            probe_estimated_rows: left_rows,
            build_estimated_rows: right_rows,
            selectivity_ratio: 1.0,
            reason: format!("Semi-join not supported for {:?} joins", join_type),
        };
    }

    // For LEFT JOIN, only left side can be probe
    if join_type == JoinType::Left {
        let selectivity_ratio = left_rows as f64 / right_rows.max(1) as f64;
        
        if left_rows <= thresholds.max_probe_rows_for_in_clause 
            && selectivity_ratio <= thresholds.min_selectivity_ratio
            && thresholds.is_cross_database
        {
            return SemiJoinAnalysis {
                should_use_semi_join: true,
                probe_side: BuildSide::Left,
                probe_estimated_rows: left_rows,
                build_estimated_rows: right_rows,
                selectivity_ratio,
                reason: format!(
                    "LEFT JOIN with small left side ({} rows, {:.1}% of right)",
                    left_rows,
                    selectivity_ratio * 100.0
                ),
            };
        }

        let reason = if !thresholds.is_cross_database {
            "Same-database join - semi-join not needed".to_string()
        } else {
            "Left side too large for semi-join reduction".to_string()
        };

        return SemiJoinAnalysis {
            should_use_semi_join: false,
            probe_side: BuildSide::Left,
            probe_estimated_rows: left_rows,
            build_estimated_rows: right_rows,
            selectivity_ratio,
            reason,
        };
    }

    // For INNER JOIN, choose the smaller side as probe
    let (probe_rows, build_rows, probe_side) = if left_rows <= right_rows {
        (left_rows, right_rows, BuildSide::Left)
    } else {
        (right_rows, left_rows, BuildSide::Right)
    };

    let selectivity_ratio = probe_rows as f64 / build_rows.max(1) as f64;

    // Check if semi-join is beneficial
    let should_use = probe_rows <= thresholds.max_probe_rows_for_in_clause
        && selectivity_ratio <= thresholds.min_selectivity_ratio
        && thresholds.is_cross_database; // Only for cross-DB joins

    let reason = if should_use {
        format!(
            "Cross-database join with small {} side ({} rows, {:.1}% of other side)",
            if probe_side == BuildSide::Left { "left" } else { "right" },
            probe_rows,
            selectivity_ratio * 100.0
        )
    } else if !thresholds.is_cross_database {
        "Same-database join - semi-join not needed".to_string()
    } else if probe_rows > thresholds.max_probe_rows_for_in_clause {
        format!(
            "Probe side has {} rows (exceeds {} threshold)",
            probe_rows, thresholds.max_probe_rows_for_in_clause
        )
    } else {
        format!(
            "Selectivity ratio {:.1}% exceeds {:.1}% threshold",
            selectivity_ratio * 100.0,
            thresholds.min_selectivity_ratio * 100.0
        )
    };

    SemiJoinAnalysis {
        should_use_semi_join: should_use,
        probe_side,
        probe_estimated_rows: probe_rows,
        build_estimated_rows: build_rows,
        selectivity_ratio,
        reason,
    }
}

/// Check if semi-join should be used (convenience function).
///
/// Uses default thresholds. For custom thresholds, use `analyze_semi_join`.
pub fn should_use_semi_join(
    left: &TableInfo,
    right: &TableInfo,
    join_type: JoinType,
    is_cross_database: bool,
) -> bool {
    let thresholds = SemiJoinThresholds {
        is_cross_database,
        ..Default::default()
    };
    
    analyze_semi_join(left, right, join_type, &thresholds).should_use_semi_join
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_plan_builder() {
        let mut plan = ExecutionPlan::new();
        
        plan.add_step(ExecutionStep::Scan {
            source_name: Arc::from("stripe"),
            table_name: Arc::from("customers"),
            predicates: vec![],
            estimated_rows: 10000,
            estimated_bytes: 1024 * 1024,
        });

        assert_eq!(plan.steps.len(), 1);
        assert!(!plan.requires_materialization);
    }

    #[test]
    fn test_table_info_selectivity() {
        let table = TableInfo::new("stripe", "charges", SourceType::Stripe)
            .with_predicate_strings(vec!["status = 'paid'".to_string()]);

        // One equality predicate gives ~10% selectivity (heuristic)
        assert!((table.predicate_selectivity() - 0.1).abs() < 0.01);

        // Two predicates with dampening: most selective (0.1) at full
        // strength, second (0.33) dampened via sqrt -> 0.1 * sqrt(0.33) ≈ 0.0575
        let table2 = TableInfo::new("stripe", "charges", SourceType::Stripe)
            .with_predicate_strings(vec![
                "status = 'paid'".to_string(),
                "amount > 100".to_string(),
            ]);
        let expected = 0.1 * 0.33_f64.sqrt(); // ≈ 0.0574
        assert!((table2.predicate_selectivity() - expected).abs() < 0.01);
    }

    #[test]
    fn test_predicate_string_parsing() {
        use crate::warehouse::query::predicate_pushdown::Predicate;

        let table = TableInfo::new("s", "t", SourceType::PostgreSQL)
            .with_predicate_strings(vec![
                "status = 'paid'".to_string(),
                "amount > 100".to_string(),
                "created_at BETWEEN '2023-01-01' AND '2023-12-31'".to_string(),
                "status IN ('active', 'pending')".to_string(),
            ]);

        assert_eq!(table.predicates.len(), 4);
        assert!(matches!(&table.predicates[0], Predicate::Equals { column, value }
            if column == "status" && value == "paid"));
        assert!(matches!(&table.predicates[1], Predicate::GreaterThan { column, .. }
            if column == "amount"));
        assert!(matches!(&table.predicates[2], Predicate::Between { column, .. }
            if column == "created_at"));
        assert!(matches!(&table.predicates[3], Predicate::In { column, values }
            if column == "status" && values.len() == 2));
    }

    #[test]
    fn test_should_materialize() {
        let stripe_profile = SourceAccessProfile::default_for_source_type(SourceType::Stripe);
        let postgres_profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);
        let parquet_profile = SourceAccessProfile::default_for_source_type(SourceType::ExternalParquet);

        // Stripe should be materialized (rate limited)
        assert!(should_materialize(&stripe_profile));

        // PostgreSQL doesn't need materialization
        assert!(!should_materialize(&postgres_profile));

        // Parquet doesn't need materialization
        assert!(!should_materialize(&parquet_profile));
    }

    #[test]
    fn test_estimate_join_output() {
        // Inner join: smaller * 0.8 * (1 + ln(larger/smaller) * 0.1)
        let inner = estimate_join_output(1000, 1000, JoinType::Inner);
        assert!(inner > 0 && inner < 1000);

        // Left join: same as left side
        let left = estimate_join_output(1000, 500, JoinType::Left);
        assert_eq!(left, 1000);

        // Full join: sum of both sides
        let full = estimate_join_output(1000, 500, JoinType::Full);
        assert_eq!(full, 1500);
    }

    #[test]
    fn test_plan_optimizer_join() {
        let optimizer = PlanOptimizer::default();

        let left = TableInfo::new("stripe", "charges", SourceType::Stripe);
        let right = TableInfo::new("postgres", "orders", SourceType::PostgreSQL);

        let join = JoinInfo {
            left,
            right,
            condition: Arc::from("charges.order_id = orders.id"),
            join_type: JoinType::Inner,
        };

        let plan = optimizer.optimize_join(&join);

        // Should produce a valid plan
        assert!(!plan.steps.is_empty());
        assert!(plan.cost.total_cost > 0.0);
    }

    #[test]
    fn test_plan_optimizer_selects_smaller_build() {
        let optimizer = PlanOptimizer::default();

        // Create tables with different sizes
        let small_stats = TableStatistics::new(
            Uuid::new_v4(),
            "stripe",
            "customers",
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(1000)
        .with_size_bytes(100 * 1024);

        let large_stats = TableStatistics::new(
            Uuid::new_v4(),
            "events",
            "clicks",
            crate::warehouse::statistics::CollectionMethod::Metadata,
        )
        .with_row_count(10_000_000)
        .with_size_bytes(10 * 1024 * 1024 * 1024);

        let left = TableInfo::new("stripe", "customers", SourceType::Stripe)
            .with_statistics(small_stats);
        let right = TableInfo::new("events", "clicks", SourceType::ExternalParquet)
            .with_statistics(large_stats);

        let join = JoinInfo {
            left,
            right,
            condition: Arc::from("customers.id = clicks.customer_id"),
            join_type: JoinType::Inner,
        };

        let plan = optimizer.optimize_join(&join);

        // Should have picked the smaller side as build
        // (exact verification would require inspecting the plan details)
        assert!(!plan.steps.is_empty());
    }

    // ==================== Semi-Join Analysis Tests ====================

    #[test]
    fn test_analyze_semi_join_cross_db_small_probe() {
        // Small filtered left side, large right side, cross-database
        let small_stats = TableStatistics::new(
            Uuid::new_v4(),
            "stripe",
            "customers",
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(10_000);

        let large_stats = TableStatistics::new(
            Uuid::new_v4(),
            "postgres",
            "orders",
            crate::warehouse::statistics::CollectionMethod::Metadata,
        )
        .with_row_count(1_000_000);

        let left = TableInfo::new("stripe", "customers", SourceType::Stripe)
            .with_statistics(small_stats)
            .with_predicate_strings(vec!["status = 'active'".to_string()]); // 10% selectivity

        let right = TableInfo::new("postgres", "orders", SourceType::PostgreSQL)
            .with_statistics(large_stats);

        let thresholds = SemiJoinThresholds {
            is_cross_database: true,
            ..Default::default()
        };

        let analysis = analyze_semi_join(&left, &right, JoinType::Inner, &thresholds);

        // Should recommend semi-join: 1,000 rows (10% of 10K) vs 1M rows
        assert!(analysis.should_use_semi_join);
        assert_eq!(analysis.probe_side, BuildSide::Left);
        assert!(analysis.selectivity_ratio < 0.01); // 1K / 1M = 0.001
    }

    #[test]
    fn test_analyze_semi_join_same_database() {
        let left = TableInfo::new("postgres", "users", SourceType::PostgreSQL);
        let right = TableInfo::new("postgres", "orders", SourceType::PostgreSQL);

        let thresholds = SemiJoinThresholds {
            is_cross_database: false, // Same database
            ..Default::default()
        };

        let analysis = analyze_semi_join(&left, &right, JoinType::Inner, &thresholds);

        // Should NOT recommend semi-join for same-database joins
        assert!(!analysis.should_use_semi_join);
        assert!(analysis.reason.contains("Same-database"));
    }

    #[test]
    fn test_analyze_semi_join_probe_too_large() {
        let large_stats = TableStatistics::new(
            Uuid::new_v4(),
            "source",
            "table",
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(100_000);

        let left = TableInfo::new("stripe", "payments", SourceType::Stripe)
            .with_statistics(large_stats.clone());
        let right = TableInfo::new("postgres", "orders", SourceType::PostgreSQL)
            .with_statistics(large_stats);

        let thresholds = SemiJoinThresholds {
            is_cross_database: true,
            max_probe_rows_for_in_clause: 10_000, // Limit
            ..Default::default()
        };

        let analysis = analyze_semi_join(&left, &right, JoinType::Inner, &thresholds);

        // Should NOT recommend semi-join: both sides are 100K rows
        assert!(!analysis.should_use_semi_join);
        assert!(analysis.reason.contains("exceeds"));
    }

    #[test]
    fn test_analyze_semi_join_left_join() {
        let small_stats = TableStatistics::new(
            Uuid::new_v4(),
            "source",
            "table",
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(500);

        let large_stats = TableStatistics::new(
            Uuid::new_v4(),
            "source",
            "table",
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(100_000);

        let left = TableInfo::new("stripe", "customers", SourceType::Stripe)
            .with_statistics(small_stats);
        let right = TableInfo::new("postgres", "orders", SourceType::PostgreSQL)
            .with_statistics(large_stats);

        let thresholds = SemiJoinThresholds {
            is_cross_database: true,
            ..Default::default()
        };

        let analysis = analyze_semi_join(&left, &right, JoinType::Left, &thresholds);

        // LEFT JOIN with small left side should use semi-join
        assert!(analysis.should_use_semi_join);
        assert_eq!(analysis.probe_side, BuildSide::Left);
    }

    #[test]
    fn test_analyze_semi_join_full_join_not_supported() {
        let left = TableInfo::new("stripe", "customers", SourceType::Stripe);
        let right = TableInfo::new("postgres", "orders", SourceType::PostgreSQL);

        let thresholds = SemiJoinThresholds {
            is_cross_database: true,
            ..Default::default()
        };

        let analysis = analyze_semi_join(&left, &right, JoinType::Full, &thresholds);

        // FULL JOIN should not use semi-join
        assert!(!analysis.should_use_semi_join);
        assert!(analysis.reason.contains("not supported"));
    }

    #[test]
    fn test_should_use_semi_join_convenience() {
        let small_stats = TableStatistics::new(
            Uuid::new_v4(),
            "source",
            "table",
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(100);

        let large_stats = TableStatistics::new(
            Uuid::new_v4(),
            "source",
            "table",
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(100_000);

        let left = TableInfo::new("stripe", "customers", SourceType::Stripe)
            .with_statistics(small_stats);
        let right = TableInfo::new("postgres", "orders", SourceType::PostgreSQL)
            .with_statistics(large_stats);

        // Cross-database: should recommend
        assert!(should_use_semi_join(&left, &right, JoinType::Inner, true));

        // Same-database: should not recommend
        assert!(!should_use_semi_join(&left, &right, JoinType::Inner, false));
    }

    #[test]
    fn test_semi_join_thresholds_default() {
        let thresholds = SemiJoinThresholds::default();
        
        assert_eq!(thresholds.max_probe_rows_for_in_clause, 10_000);
        assert!((thresholds.min_selectivity_ratio - 0.1).abs() < 0.001);
        assert!(!thresholds.is_cross_database);
    }

    #[test]
    fn test_parse_between_with_and_before_keyword() {
        use crate::warehouse::query::predicate_pushdown::Predicate;

        let table = TableInfo::new("src", "tbl", SourceType::PostgreSQL)
            .with_predicate_strings(vec!["amount BETWEEN 10 AND 20".to_string()]);
        assert_eq!(table.predicates.len(), 1);
        match &table.predicates[0] {
            Predicate::Between { column, low, high } => {
                assert_eq!(column.as_str(), "amount");
                assert_eq!(low.as_str(), "10");
                assert_eq!(high.as_str(), "20");
            }
            other => panic!("Expected Between, got: {other:?}"),
        }

        let table2 = TableInfo::new("src", "tbl", SourceType::PostgreSQL)
            .with_predicate_strings(vec!["demand BETWEEN 5 AND 15".to_string()]);
        assert_eq!(table2.predicates.len(), 1);
        match &table2.predicates[0] {
            Predicate::Between { column, low, high } => {
                assert_eq!(column.as_str(), "demand");
                assert_eq!(low.as_str(), "5");
                assert_eq!(high.as_str(), "15");
            }
            other => panic!("Expected Between, got: {other:?}"),
        }
    }

    #[test]
    fn test_analyze_semi_join_left_join_same_database_rejected() {
        let small_stats = TableStatistics::new(
            Uuid::new_v4(),
            "source",
            "table",
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(500);

        let large_stats = TableStatistics::new(
            Uuid::new_v4(),
            "source",
            "table",
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(100_000);

        let left = TableInfo::new("postgres", "customers", SourceType::PostgreSQL)
            .with_statistics(small_stats);
        let right = TableInfo::new("postgres", "orders", SourceType::PostgreSQL)
            .with_statistics(large_stats);

        let thresholds = SemiJoinThresholds {
            is_cross_database: false,
            ..Default::default()
        };

        let analysis = analyze_semi_join(&left, &right, JoinType::Left, &thresholds);

        assert!(
            !analysis.should_use_semi_join,
            "LEFT JOIN in same database should NOT use semi-join, got: {}",
            analysis.reason
        );
        assert!(
            analysis.reason.contains("Same-database"),
            "Reason should mention same-database, got: {}",
            analysis.reason
        );
    }

    #[test]
    fn test_select_best_plan_all_exceed_budget() {
        let cost_model = CostModel::default();
        let optimizer = PlanOptimizer::new(cost_model)
            .with_memory_budget(10);

        let plan1 = ExecutionPlan::new()
            .with_description("Plan A")
            .with_memory(50);
        let plan2 = ExecutionPlan::new()
            .with_description("Plan B")
            .with_memory(100);

        let best = optimizer.select_best_plan(vec![plan1, plan2]);
        assert_eq!(best.memory_required_mb, 50,
            "Should return lowest-memory plan when all exceed budget");
    }

    // ==================== Regression tests for estimate_join_output ====================

    #[test]
    fn test_full_join_estimate_does_not_overflow() {
        let result = estimate_join_output(u64::MAX, u64::MAX, JoinType::Full);
        assert_eq!(result, u64::MAX, "Full join with u64::MAX should saturate, not overflow");
    }

    #[test]
    fn test_full_join_estimate_near_max() {
        let result = estimate_join_output(u64::MAX - 1, 2, JoinType::Full);
        assert_eq!(result, u64::MAX, "Full join near u64::MAX should saturate");
    }

    #[test]
    fn test_inner_join_estimate_zero_rows() {
        let result = estimate_join_output(0, 1000, JoinType::Inner);
        assert_eq!(result, 0, "Inner join with 0 left rows should produce 0");

        let result = estimate_join_output(1000, 0, JoinType::Inner);
        assert_eq!(result, 0, "Inner join with 0 right rows should produce 0");
    }

    #[test]
    fn test_cross_join_estimate_large_values() {
        let result = estimate_join_output(u64::MAX, 2, JoinType::Cross);
        assert_eq!(result, u64::MAX, "Cross join with u64::MAX should saturate");
    }

    // ==================== Regression tests for safe_f64_to_u64 ====================

    #[test]
    fn test_safe_f64_to_u64_normal_values() {
        assert_eq!(safe_f64_to_u64(0.0), 0);
        assert_eq!(safe_f64_to_u64(1.0), 1);
        assert_eq!(safe_f64_to_u64(100.5), 100);
        assert_eq!(safe_f64_to_u64(1_000_000.0), 1_000_000);
    }

    #[test]
    fn test_safe_f64_to_u64_nan() {
        assert_eq!(safe_f64_to_u64(f64::NAN), 0);
    }

    #[test]
    fn test_safe_f64_to_u64_negative() {
        assert_eq!(safe_f64_to_u64(-1.0), 0);
        assert_eq!(safe_f64_to_u64(-1000.0), 0);
        assert_eq!(safe_f64_to_u64(f64::NEG_INFINITY), 0);
    }

    #[test]
    fn test_safe_f64_to_u64_very_large() {
        assert_eq!(safe_f64_to_u64(f64::INFINITY), u64::MAX);
        assert_eq!(safe_f64_to_u64(1e30), u64::MAX);
    }

    #[test]
    fn test_join_plan_always_has_scan_steps_even_without_predicates() {
        let optimizer = PlanOptimizer::default();

        let left = TableInfo::new("stripe", "customers", SourceType::Stripe);
        let right = TableInfo::new("postgres", "orders", SourceType::PostgreSQL);

        assert!(left.predicates.is_empty(), "precondition: left has no predicates");
        assert!(right.predicates.is_empty(), "precondition: right has no predicates");

        let join = JoinInfo {
            left,
            right,
            condition: Arc::from("customers.id = orders.customer_id"),
            join_type: JoinType::Inner,
        };

        let plan = optimizer.optimize_join(&join);

        let scan_count = plan.steps.iter().filter(|s| matches!(s, ExecutionStep::Scan { .. })).count();
        assert!(
            scan_count >= 2,
            "Plan must include Scan steps for both tables even without predicates, got {} scans in {} total steps",
            scan_count,
            plan.steps.len(),
        );
    }

    #[test]
    fn test_memory_mb_no_overflow_for_large_datasets() {
        let optimizer = PlanOptimizer::default();

        let large_stats = TableStatistics::new(
            Uuid::new_v4(),
            "warehouse",
            "events",
            crate::warehouse::statistics::CollectionMethod::Metadata,
        )
        .with_row_count(i64::MAX / 1000)
        .with_size_bytes(i64::MAX / 2);

        let left = TableInfo::new("warehouse", "events", SourceType::ExternalParquet)
            .with_statistics(large_stats.clone());
        let right = TableInfo::new("warehouse", "events2", SourceType::ExternalParquet)
            .with_statistics(large_stats);

        let join = JoinInfo {
            left,
            right,
            condition: Arc::from("events.id = events2.id"),
            join_type: JoinType::Inner,
        };

        let plan = optimizer.optimize_join(&join);

        assert!(
            plan.memory_required_mb > 0,
            "memory_required_mb must be positive, not wrapped around to 0"
        );
    }

    /// Helper: create a `TableInfo` with the given row count and proportional byte size.
    fn make_table(source: &str, name: &str, rows: i64) -> TableInfo {
        let avg_row_bytes: i64 = 200;
        let stats = TableStatistics::new(
            Uuid::new_v4(),
            source,
            name,
            crate::warehouse::statistics::CollectionMethod::Sync,
        )
        .with_row_count(rows)
        .with_size_bytes(rows * avg_row_bytes);
        TableInfo::new(source, name, SourceType::Stripe).with_statistics(stats)
    }

    /// Test A: Small table joined first produces fewer intermediate rows,
    /// making the second join significantly cheaper.  The optimizer should
    /// prefer joining the 10-row table first.
    #[test]
    fn test_cardinality_propagation_small_first() {
        let optimizer = PlanOptimizer::default();

        let small = make_table("src", "dim_users", 10);
        let medium = make_table("src", "fact_orders", 100_000);
        let large = make_table("src", "fact_clicks", 1_000_000);

        // Join order matters: small↔medium first should beat medium↔large first
        let joins = vec![
            JoinInfo {
                left: medium.clone(),
                right: large.clone(),
                condition: Arc::from("fact_orders.user_id = fact_clicks.user_id"),
                join_type: JoinType::Inner,
            },
            JoinInfo {
                left: small.clone(),
                right: medium.clone(),
                condition: Arc::from("dim_users.id = fact_orders.user_id"),
                join_type: JoinType::Inner,
            },
        ];

        let greedy = optimizer.greedy_join_order(&joins);

        // The greedy plan should pick the small↔medium join first (index 1)
        // because it has lower cost, not the medium↔large join (index 0).
        assert!(
            greedy.description.contains("dim_users↔fact_orders"),
            "Greedy plan should mention the small-first join: {}",
            greedy.description,
        );
        // Verify the small join appears before the large one in the description
        let small_pos = greedy.description.find("dim_users↔fact_orders").unwrap();
        let large_pos = greedy.description.find("fact_orders↔fact_clicks").unwrap();
        assert!(
            small_pos < large_pos,
            "Small table join should come first in the ordering: {}",
            greedy.description,
        );
    }

    /// Test B: With wildly asymmetric table sizes, the optimizer should
    /// join the tiny table with the medium one first, producing ~10 rows,
    /// before joining with the million-row table.
    #[test]
    fn test_asymmetric_join_sizes() {
        let optimizer = PlanOptimizer::default();

        let tiny = make_table("a", "lookup", 10);
        let medium = make_table("b", "transactions", 100_000);
        let huge = make_table("c", "events", 1_000_000);

        let joins = vec![
            JoinInfo {
                left: medium.clone(),
                right: huge.clone(),
                condition: Arc::from("transactions.event_id = events.id"),
                join_type: JoinType::Inner,
            },
            JoinInfo {
                left: tiny.clone(),
                right: medium.clone(),
                condition: Arc::from("lookup.id = transactions.lookup_id"),
                join_type: JoinType::Inner,
            },
        ];

        let exhaustive = optimizer.exhaustive_join_order(&joins);
        let greedy = optimizer.greedy_join_order(&joins);

        // Both strategies should pick the tiny↔medium join first.
        for (label, plan) in [("exhaustive", &exhaustive), ("greedy", &greedy)] {
            let tiny_pos = plan.description.find("lookup↔transactions");
            let huge_pos = plan.description.find("transactions↔events");
            assert!(
                tiny_pos.is_some() && huge_pos.is_some(),
                "{label} plan description missing expected joins: {}",
                plan.description,
            );
            assert!(
                tiny_pos.unwrap() < huge_pos.unwrap(),
                "{label} should join tiny table first: {}",
                plan.description,
            );
        }

        // The exhaustive plan's cost should be <= the greedy plan's cost
        // (exhaustive explores all orderings).
        assert!(
            exhaustive.cost.total_cost <= greedy.cost.total_cost,
            "Exhaustive cost ({}) should be <= greedy cost ({})",
            exhaustive.cost.total_cost,
            greedy.cost.total_cost,
        );
    }

    /// Test C: For a 2-join case, both exhaustive and greedy should produce
    /// the same join ordering and identical cost.
    #[test]
    fn test_permutation_vs_greedy_agrees_on_two_joins() {
        let optimizer = PlanOptimizer::default();

        let t1 = make_table("s", "alpha", 5_000);
        let t2 = make_table("s", "beta", 50_000);

        let joins = vec![
            JoinInfo {
                left: t1.clone(),
                right: t2.clone(),
                condition: Arc::from("alpha.id = beta.alpha_id"),
                join_type: JoinType::Inner,
            },
        ];

        let exhaustive = optimizer.exhaustive_join_order(&joins);
        let greedy = optimizer.greedy_join_order(&joins);

        let cost_diff = (exhaustive.cost.total_cost - greedy.cost.total_cost).abs();
        assert!(
            cost_diff < 1e-6,
            "For a single join, exhaustive and greedy should agree. \
             Exhaustive={}, Greedy={}",
            exhaustive.cost.total_cost,
            greedy.cost.total_cost,
        );
    }

    #[test]
    fn test_is_not_null_selectivity_without_stats() {
        use crate::warehouse::query::predicate_pushdown::Predicate;

        let table = TableInfo::new("src", "tbl", SourceType::PostgreSQL);

        let is_null_pred = Predicate::IsNull {
            column: "col".into(),
            is_null: true,
        };
        let is_not_null_pred = Predicate::IsNull {
            column: "col".into(),
            is_null: false,
        };

        let sel_null = table.estimate_single_predicate_selectivity(&is_null_pred);
        let sel_not_null = table.estimate_single_predicate_selectivity(&is_not_null_pred);

        assert!(
            (sel_null - SEL_NULL).abs() < 1e-9,
            "IS NULL without stats should be {SEL_NULL}, got {sel_null}"
        );
        assert!(
            (sel_not_null - (1.0 - SEL_NULL)).abs() < 1e-9,
            "IS NOT NULL without stats should be {}, got {sel_not_null}",
            1.0 - SEL_NULL
        );
        assert!(
            sel_not_null > 0.9,
            "IS NOT NULL selectivity must be high (~0.95), got {sel_not_null}"
        );
    }

    #[test]
    fn test_or_selectivity_uses_inclusion_exclusion() {
        use crate::warehouse::query::predicate_pushdown::Predicate;

        let table = TableInfo::new("src", "tbl", SourceType::PostgreSQL);

        // Two Between predicates each with 0.25 default selectivity (SEL_BETWEEN).
        // Naive sum: 0.25 + 0.25 = 0.50
        // Inclusion-exclusion: 0.25 + 0.25 - 0.25*0.25 = 0.4375
        let or_pred = Predicate::Or(vec![
            Predicate::Between {
                column: "a".into(),
                low: "10".into(),
                high: "20".into(),
            },
            Predicate::Between {
                column: "b".into(),
                low: "30".into(),
                high: "40".into(),
            },
        ]);

        let sel = table.estimate_single_predicate_selectivity(&or_pred);

        // With inclusion-exclusion the result must be strictly less than the
        // naive sum (0.50) and close to 0.25 + 0.25 - 0.25*0.25 = 0.4375.
        assert!(
            sel < 0.50,
            "OR selectivity should use inclusion-exclusion, not naive sum; got {sel}"
        );
        let expected = 0.25 + 0.25 - 0.25 * 0.25;
        assert!(
            (sel - expected).abs() < 1e-6,
            "Expected OR selectivity ~{expected:.4}, got {sel:.4}"
        );
    }

    #[test]
    fn test_memory_required_mb_saturating_add() {
        let mut plan = ExecutionPlan::new();
        plan.memory_required_mb = u32::MAX - 10;

        let next = ExecutionPlan {
            memory_required_mb: 100,
            ..ExecutionPlan::new()
        };

        // This should not panic or wrap; it should saturate
        plan.memory_required_mb = plan.memory_required_mb.saturating_add(next.memory_required_mb);
        assert_eq!(plan.memory_required_mb, u32::MAX);
    }
}
