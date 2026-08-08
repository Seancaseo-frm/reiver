# Query Module Investigation Report

Investigation of 20 concerns raised during the code review of `pond/src/warehouse/query/` (~30,000 lines, 20 files).

---

## Correctness Concerns

### C1. Predicate independence assumption done

**Evidence**: `plan_optimizer.rs` lines 252-265. The `predicate_selectivity` method multiplies individual predicate selectivities:

```rust
// Line 257-263
let mut combined_selectivity = 1.0;
for predicate in &self.predicates {
    let sel = self.estimate_single_predicate_selectivity(predicate);
    combined_selectivity *= sel;
}
combined_selectivity.clamp(0.0001, 1.0)
```

There is no dampening factor or correlation handling. For correlated predicates like `city = 'NYC' AND state = 'NY'`, each has ~0.1 selectivity, so the combined estimate is 0.01 (1%) when the true answer is ~0.1 (10%). The `clamp(0.0001, 1.0)` floor prevents extreme underestimation but does not address moderate cases.

The selectivity feeds into `estimated_rows()` which drives join ordering, build/probe side selection, semi-join decisions, and memory budget calculations. Underestimating rows by 10x can lead to suboptimal plan choices (e.g., choosing a hash join build side that is actually larger than expected).

**Severity**: Medium

**Verdict**: Confirmed. The independence assumption is a known limitation in query optimizers, but there is no mitigation here. Systems like PostgreSQL use multi-column statistics or apply a dampening factor.

**Recommendation**: Add a dampening factor for multi-predicate selectivity. A simple approach: `combined = combined.max(individual_selectivities.iter().min())` or apply a square-root dampening after the first predicate. Document the limitation in the method's doc comment.

---

### C2. Greedy join ordering done

**Evidence**: `plan_optimizer.rs` lines 765-827. `generate_multi_join_plans` generates exactly 2 plans:
1. Greedy: at each step pick the cheapest remaining join (lines 792-827)
2. Left-deep: process joins in input order (lines 777-786)

The greedy approach evaluates each join independently (`self.optimize_join(j)` on line 793) and then sorts by cost. It does **not** account for how the output of one join affects the cost of the next -- intermediate cardinalities are not propagated.

Callers: `optimize_multi_join` is only re-exported via `mod.rs` but has **zero call sites** outside the module itself. The API is public but unused in production code currently. The `federation.rs` planner does not call it.

**Severity**: Low

**Verdict**: Partially confirmed. The greedy ordering is simplistic and would miss optimal plans for 4+ table joins. However, the function is currently not called in production, making this a theoretical concern for future use.

**Recommendation**: No immediate action needed. When this function is integrated into the federation planner, consider: (a) propagating intermediate cardinalities through the greedy loop, and (b) for join counts <= 6, enumerate all permutations (6! = 720 is tractable).

---

### C3. Semi-join CROSS join handling done

**Evidence**: `plan_optimizer.rs` lines 1069-1079. `analyze_semi_join` rejects non-INNER/LEFT joins with a format string:

```rust
if !matches!(join_type, JoinType::Inner | JoinType::Left) {
    return SemiJoinAnalysis {
        should_use_semi_join: false,
        // ...
        reason: format!("Semi-join not supported for {:?} joins", join_type),
    };
}
```

This correctly rejects CROSS, RIGHT, and FULL joins. Semi-join reduction requires an equi-join condition to extract keys, which CROSS joins lack by definition.

Tracing upstream: the `FederationPlanner` in `federation.rs` constructs `JoinCondition` objects from parsed SQL, and CROSS joins would have no condition to extract keys from. The CROSS+filter pattern (where a filter acts as an implicit join condition) is not rewritten to INNER join upstream -- it arrives as `JoinType::Cross`.

**Severity**: Low

**Verdict**: Partially confirmed. The rejection is correct -- semi-join cannot work without an equi-join condition. The gap is that CROSS+filter patterns are not transformed to INNER joins upstream, but this is a federation planner concern, not a plan_optimizer concern. The reason message is clear enough.

**Recommendation**: No action needed in `analyze_semi_join`. If CROSS+filter optimization is desired, it should be handled in the federation planner by detecting implicit equi-join conditions in WHERE clauses and converting to INNER joins before plan optimization.

---

### C4. Bloom filter duplicate insertion

**Evidence**: `bloom_pushdown.rs` lines 96-101 document the contract:

```rust
/// Callers **must not** insert duplicate values.  The `num_items` counter
/// is incremented unconditionally, so duplicates would inflate it and skew
/// the estimated false-positive probability.
```

However, the primary call site `BloomFilterPushdown::from_keys` (line 282-297) **already deduplicates** keys before insertion:

```rust
pub fn from_keys(keys: &[String], false_positive_rate: f64) -> BloomResult<Self> {
    let mut unique_keys: std::collections::HashSet<&String> = std::collections::HashSet::with_capacity(keys.len());
    unique_keys.extend(keys.iter());
    let filter = BloomFilter::new(unique_keys.len(), false_positive_rate);
    // ...
    for key in &unique_keys { pushdown.filter.insert(*key); }
}
```

And the keys passed to `from_keys` come from `SemiJoinExecutor::extract_keys` (semi_join.rs line 362), which uses an `AHashSet` for deduplication:

```rust
let mut keys = AHashSet::with_capacity(result.rows.len());
for row in &result.rows {
    if let Some(key) = value_to_raw_key(value) { keys.insert(key); }
}
```

So keys are deduplicated twice before reaching `BloomFilter::insert`.

**Severity**: Non-issue

**Verdict**: Dismissed. The doc comment warning on `insert` is correct API documentation, but all production call paths already deduplicate keys. The `from_keys` constructor even uses its own `HashSet` as a safety net. `num_items` will be accurate in practice.

**Recommendation**: No action needed. The defensive deduplication in `from_keys` is appropriate.

---

## Performance Concerns

### P1. Predicate parsing redundancy done

**Evidence**: `plan_optimizer.rs` lines 334-344. `parse_predicate` creates a new `ClickHouseDialect` and `Parser` for each predicate string, parsing it into an AST expression:

```rust
fn parse_predicate(predicate: &str) -> Option<(String, PredicateOp, String)> {
    let dialect = ClickHouseDialect {};
    let mut parser = match Parser::new(&dialect).try_with_sql(predicate.trim()) {
        Ok(p) => p,
        Err(_) => return None,
    };
    let expr = match parser.parse_expr() { Ok(e) => e, Err(_) => return None };
    Self::extract_predicate_parts(&expr)
}
```

This is called from `estimate_single_predicate_selectivity` (line 273), which is called once per predicate per `predicate_selectivity()` call. Additionally, `heuristic_selectivity` (line 384) parses the same predicate again if `get_column_stats` returns `None`.

The `predicate_pushdown.rs` module already has a structured `Predicate` enum (line 54) with variants like `Equals`, `In`, `Between`, etc. However, `TableInfo.predicates` is `Vec<String>`, so the structured form is not passed through.

**Severity**: Low

**Verdict**: Confirmed. Each predicate is parsed 1-2 times (once in `parse_predicate`, potentially again in `heuristic_selectivity`). However, the total number of predicates per query is typically small (1-5), and SQL parsing is fast for single expressions. The cost is measured in microseconds, not milliseconds.

**Recommendation**: Low priority. If `TableInfo` is refactored to carry structured `Predicate` enums instead of strings, parsing would be eliminated. This is a cleanliness improvement more than a performance fix.

---

### P2. `estimate_json_value_memory` per-row overhead

**Evidence**: `utils.rs` lines 183-204 and `executor.rs` line 382. The function is recursive:

```rust
pub fn estimate_json_value_memory(v: &serde_json::Value) -> usize {
    match v {
        Value::Null => 24,
        Value::Bool(_) => 24,
        Value::Number(_) => 32,
        Value::String(s) => 24 + 24 + s.len(),
        Value::Array(arr) => 24 + 24 + arr.iter().map(estimate_json_value_memory).sum::<usize>(),
        Value::Object(map) => 24 + 24 + map.iter().map(|(k, v)| 24 + k.len() + estimate_json_value_memory(v)).sum::<usize>(),
    }
}
```

For typical query results (flat rows of scalars), this is O(columns) per row -- each value is a `Null`, `Bool`, `Number`, or `String` (one match arm, no recursion). The function is only recursive for nested `Array`/`Object` values, which are uncommon in tabular results.

In `collect_with_limit` (executor.rs line 374-414), this is called for every row:
```rust
let row_size: usize = row.iter().map(|v| estimate_json_value_memory(v)).sum();
```

For a query returning 1M rows with 10 columns, this is ~10M match operations. Each is a single branch with no allocation -- roughly 10-50ns per call, so ~100-500ms total for 1M rows. This is non-trivial but likely dwarfed by the network I/O time.

**Severity**: Low

**Verdict**: Partially confirmed. The per-row cost is real but small for flat tabular data. Sampling every Nth row would introduce inaccuracy in the memory limit check -- a row that exceeds the remaining budget could be missed. The current approach is safer.

**Recommendation**: No action needed for correctness. If profiling shows this as a bottleneck, consider: (a) amortizing by computing row size on the first row and multiplying by row count, or (b) checking every 64th row and using 64x the single-row estimate. Both trade accuracy for speed.

---

### P3. BloomFilter uses `DefaultHasher`

**Evidence**: `bloom_pushdown.rs` lines 19, 105-111. Uses `std::collections::hash_map::DefaultHasher` with a double-hashing scheme:

```rust
let mut h1_hasher = DefaultHasher::new();
value.hash(&mut h1_hasher);
let h1 = h1_hasher.finish();

let mut h2_hasher = DefaultHasher::new();
(value, 0x517cc1b727220a95u64).hash(&mut h2_hasher);
let h2 = h2_hasher.finish();
```

`DefaultHasher` uses SipHash-1-3 (since Rust 1.36), which provides DoS resistance at the cost of throughput. For Bloom filters where DoS resistance is irrelevant (the keys come from trusted query results, not user input), `ahash` would be ~3-5x faster.

The double-hashing optimization (computing k hash indices from 2 hash values) is well-implemented and reduces the per-lookup cost from k hash computations to 2.

Bloom filters are used for key sets between 10K-1M. At 1M keys with k=7, that's 2M hash operations for insertion. With `DefaultHasher` at ~10ns per hash, that's ~20ms. With `ahash` at ~2ns, it would be ~4ms. The 16ms savings is small relative to the network I/O of the semi-join.

**Severity**: Low

**Verdict**: Confirmed but low impact. The hasher choice adds ~16ms overhead for 1M-key Bloom filters, which is negligible compared to the network transfer time of the semi-join operation (typically 100ms-10s).

**Recommendation**: Low priority. Switching to `ahash::AHasher` is a simple internal change (replace `DefaultHasher::new()` with `AHasher::default()`) that does not affect the API. Worth doing in a cleanup pass but not urgent.

---

### P4. Source capabilities reconstructed per-call

**Evidence**: `source_capabilities.rs` lines 41-119. `for_source_type` is a large match statement that calls builder functions like `stripe_capabilities()` (line 210-263). Each builder allocates `AHashMap` and `AHashSet` instances:

```rust
fn stripe_capabilities() -> SourceCapabilities {
    let mut column_filters = AHashMap::new();
    column_filters.insert("created".to_string(), ColumnFilterCapability::new([...]));
    column_filters.insert("customer".to_string(), ...);
    // ... 5 more inserts
    SourceCapabilities { column_filters, ... }
}
```

Call frequency: 6 call sites in `predicate_pushdown.rs`, 4 in `plan_optimizer.rs`, 10 in `source_capabilities.rs` (tests), 11 in `cost_model.rs` (mostly tests). In production, this is called once per source per query plan -- typically 1-3 times per query.

The cost is: 1 `AHashMap` allocation + 5-10 `String` allocations + `AHashSet` allocation per call. Total ~500ns-1us per call.

**Severity**: Non-issue

**Verdict**: Dismissed. The construction cost (~1us) is trivial compared to query execution time (100ms+). Caching with `LazyLock` would save a few microseconds per query but add complexity (static mutable state, 40+ `LazyLock` instances for each source type). The current approach is simpler and correct.

**Recommendation**: No action needed. The allocation cost is negligible relative to I/O.

---

## Code Quality Concerns done

### Q1. Magic numbers in plan_optimizer.rs

**Evidence**: The file has 66 numeric literal occurrences. Cataloging the ones that are not test values or struct field assignments:

| Line | Value | Context | Named constant? |
|------|-------|---------|-----------------|
| 232 | `100_000` | Default estimated rows when stats missing | No |
| 245 | `10 * 1024 * 1024` | Default estimated bytes (10MB) | No |
| 265 | `0.0001` | Minimum combined selectivity floor | No |
| 303 | `0.25` | LIKE leading wildcard selectivity | No |
| 305 | `0.1` | LIKE prefix match selectivity | No |
| 307 | `0.05` | LIKE specific pattern selectivity | No |
| 399 | `0.1` | Equality heuristic selectivity | No |
| 400 | `0.9` | Not-equals heuristic selectivity | No |
| 402 | `0.33` | Range heuristic selectivity | No |
| 406 | `0.1` / `0.5` | IN-list heuristic selectivity | No |
| 408 | `0.25` | BETWEEN heuristic selectivity | No |
| 413 | `0.05` | IS NULL/IS NOT NULL heuristic selectivity | No |
| 444 | `0.3` | `DEFAULT_PREDICATE_SELECTIVITY` | **Yes** (named) |
| 622 | `100` | Memory overhead MB added to build side | No |
| 679 | `100` | Memory overhead MB added to build side | No |
| 749 | `200` | Memory overhead MB for dual materialization | No |
| 967 | `200.0` | Latency threshold for materialization decision | No |
| 979 | `0.8` / `0.1` | Inner join output heuristic multipliers | No |

Only 1 of ~18 magic numbers has a named constant (`DEFAULT_PREDICATE_SELECTIVITY`). The selectivity heuristics (0.1, 0.33, 0.25, etc.) are standard database optimizer values but are undocumented.

**Severity**: Medium

**Verdict**: Confirmed. The heuristic selectivity values are defensible choices but should be named constants for maintainability and documentation. The `100_000` default row count and `200.0ms` latency threshold are particularly opaque.

**Recommendation**: Extract to named constants at the top of the file:
- `DEFAULT_ESTIMATED_ROWS: u64 = 100_000`
- `DEFAULT_ESTIMATED_BYTES: u64 = 10 * 1024 * 1024`
- `MIN_COMBINED_SELECTIVITY: f64 = 0.0001`
- `MATERIALIZATION_LATENCY_THRESHOLD_MS: f64 = 200.0`
- `HASH_JOIN_MEMORY_OVERHEAD_MB: u32 = 100`
- Group the heuristic selectivities together with a doc comment explaining they follow PostgreSQL-style conventions.

---

### Q2. String cloning in hot paths done

**Evidence**: `plan_optimizer.rs` lines 569-757. In `generate_join_plans`, strings are cloned per plan:

- Plan 1 (lines 574-581): 6 `String::clone()` calls + 1 `Vec::clone()` for predicates
- Plan 2 (lines 635-642): Same 6+1 clones
- Plan 3 (lines 691-696): 5 `String::clone()` calls

Within each plan, some strings are cloned again when passed to `ExecutionStep` variants (e.g., `left_source.clone()` on line 586 after already cloning on line 575).

Total: ~24 `String::clone()` calls for a single 2-table join. Each clone involves a heap allocation + memcpy. For source/table names (typically 5-30 bytes), this is ~10-50ns per clone, so ~240-1200ns total.

`generate_join_plans` is called once per join in the plan, typically 1-3 times per query.

**Severity**: Low

**Verdict**: Confirmed but low impact. The total overhead is ~1-3us per query, negligible compared to I/O. Using `Arc<str>` would eliminate the copies but require changing `ExecutionStep` field types from `String` to `Arc<str>`, which propagates through serialization.

**Recommendation**: Low priority. Not worth the refactoring effort unless `ExecutionStep` is already being refactored for other reasons.

---

### Q3. Repetitive code in `generate_join_plans`

**Evidence**: Plan 1 (lines 569-628) and Plan 2 (lines 630-685) are nearly identical. The differences are:

1. Cost calculation: `BuildSide::Left` vs `BuildSide::Right` (lines 572 vs 633)
2. Materialization target: left profile vs right profile (lines 602 vs 660)
3. HashJoin field assignment: `build_source`/`probe_source` are swapped (lines 613-618 vs 670-675)
4. Memory calculation: `left_bytes` vs `right_bytes` (lines 622 vs 679)
5. Description string (lines 626 vs 683)

The 5 differences span ~60 lines of identical code each. A helper function `fn build_plan(&self, join: &JoinInfo, build_side: BuildSide, left_rows: u64, ...) -> ExecutionPlan` could unify them, reducing ~120 lines to ~70 lines.

**Severity**: Low

**Verdict**: Confirmed. The duplication is real and a maintainability concern -- a bug fix in one block could be missed in the other. However, the code is straightforward and the two blocks are adjacent, reducing the risk.

**Recommendation**: Extract a `build_hash_join_plan(&self, join: &JoinInfo, build_side: BuildSide, left_rows: u64, right_rows: u64, left_bytes: u64, right_bytes: u64) -> ExecutionPlan` helper to eliminate the duplication.

---

### Q4. Mixed HashMap implementations done

**Evidence**: Searching for `std::collections::HashMap` and `std::collections::HashSet` across all 20 query module files:

- `bloom_pushdown.rs` line 283: `std::collections::HashSet` used in `from_keys` for deduplication
- No other files use `std::collections::HashMap` or `std::collections::HashSet`

All other files consistently use `ahash::AHashMap` and `ahash::AHashSet`. The single `std::collections::HashSet` usage in `bloom_pushdown.rs` is a minor inconsistency -- it could use `AHashSet` instead.

**Severity**: Non-issue

**Verdict**: Dismissed. The inconsistency is limited to a single occurrence in a non-hot path (deduplication before Bloom filter construction). Using `std::collections::HashSet` here has no functional impact -- it's slightly slower than `AHashSet` but the deduplication happens once per semi-join operation.

**Recommendation**: Optionally change `std::collections::HashSet` to `AHashSet` in `bloom_pushdown.rs` line 283 for consistency, but this is purely cosmetic.

---

## Architecture Concerns

### A1. Large file sizes

**Evidence**: Natural split boundaries identified:

**`rewriter.rs` (5,908 lines)**:
| Lines | Component | Self-contained? |
|-------|-----------|-----------------|
| 1-157 | Utility functions (`serialize_statements`, `add_months_to_date`, `date_range_to_partition_keys`) | Yes |
| 158-211 | `RewriteError`, `PruningStats`, `RewriteOutput` | Yes (types) |
| 212-555 | `QueryPlanCache` and related structs | Yes |
| 556-882 | `TableTransformer` trait + `AstVisitor` | Yes |
| 883-914 | `BasicTableTransformer` | Yes |
| 915-1010 | `PartitionPruningTransformer` | Yes |
| 1011-1162 | `ColumnPruningTransformer` | Yes |
| 1163-1412 | `HierarchicalSkipIndexTransformer` | Yes |
| 1413-3586 | `TableRewriter` (main rewriter) | Depends on transformers |
| 3587-3856 | `TypeChecker` | Yes |
| 3857-5908 | Tests | N/A |

Splitting suggestion: `rewriter/cache.rs`, `rewriter/transformers.rs`, `rewriter/type_checker.rs`, `rewriter/mod.rs` (main `TableRewriter`).

**`predicate_pushdown.rs` (5,642 lines)**:
| Lines | Component | Self-contained? |
|-------|-----------|-----------------|
| 1-177 | `PredicateError`, `Predicate` enum with methods | Yes |
| 178-718 | `PredicatePushdown`, `FilePredicate` and related | Yes |
| 719-924 | First test module | N/A |
| 925-1372 | `PushdownStats`, `SourcePredicateAnalysis`, `TranslatedPredicate`, `PushdownWarning`, `EstimatedImpact` | Yes (types) |
| 1373-2425 | `PredicateSplitter` with all analysis logic | Yes |
| 2426-2706 | `SourceQueryWithFilters`, helper functions | Yes |
| 2707-5642 | Second test module | N/A |

Splitting suggestion: `predicate_pushdown/types.rs`, `predicate_pushdown/splitter.rs`, `predicate_pushdown/mod.rs` (core `Predicate` enum + `PredicatePushdown`).

No circular dependencies would be introduced by these splits -- each component has a clear dependency direction.

**Severity**: Medium

**Verdict**: Confirmed. Both files have clear, self-contained sections that could be extracted into submodules. The `rewriter.rs` has 4 independent `TableTransformer` implementations that are natural split candidates.

**Recommendation**: Split both files into submodules. Start with `rewriter.rs` since it has the most clearly delineated components (cache, transformers, type checker).

---

### A2. Federation planner/executor coupling

**Evidence**: Cross-reference analysis:

**`federation_executor.rs` imports from `federation.rs`**:
```rust
use super::federation::{CombinationStrategy, ExternalSourceInfo, FederatedPlan, MongoDBSourceInfo, SourceQuery};
```

**`federation.rs` imports from `federation_executor.rs`**: **None**. No `use super::federation_executor` found.

**`semi_join.rs` imports from `federation.rs`**:
```rust
use super::federation::{CombinationStrategy, FederationConfig};
```

The dependency is **strictly one-directional**: `federation_executor.rs` and `semi_join.rs` depend on types from `federation.rs`, but `federation.rs` has no dependency on either. This is the correct architecture -- the planner defines plan types, and executors consume them.

**Severity**: Non-issue

**Verdict**: Dismissed. The coupling is one-directional and well-structured. The planner (`federation.rs`) defines the plan types, and the executors (`federation_executor.rs`, `semi_join.rs`) consume them. There is no bidirectional dependency.

**Recommendation**: No action needed. The architecture is clean.

---

### A3. Aggressive glob re-exports in mod.rs

**Evidence**: `mod.rs` (65 lines) has 5 glob re-exports:
- `pub use cache::*` -- re-exports ~8 public symbols
- `pub use cost_estimator::*` -- re-exports ~7 public symbols
- `pub use executor::*` -- re-exports ~15 public symbols
- `pub use explain::*` -- re-exports ~5 public symbols
- `pub use limiter::*` -- re-exports ~6 public symbols
- `pub use rewriter::*` -- re-exports ~19 public symbols
- `pub use router::*` -- re-exports ~6 public symbols

Total via globs: ~66 symbols. Additionally, ~40 symbols are re-exported explicitly (lines 26-52). Grand total: ~106 public symbols from this module.

Name collision check: `federated_query.rs` defines `pub type Result<T>` (line 56), but this is not glob-re-exported (it uses explicit re-exports on lines 57-60). No actual collisions found among the glob-exported symbols.

**Severity**: Low

**Verdict**: Partially confirmed. The glob re-exports do not cause name collisions in practice. However, they export 66 symbols implicitly, making it hard to determine where a type originates when reading consumer code. The explicit re-exports (lines 26-52) are well-curated.

**Recommendation**: Replace `pub use executor::*` (the largest glob at ~15 symbols) with explicit re-exports as a first step. The other globs are smaller and less problematic.

---

## Test Coverage Concerns

### T1. cache.rs has only 3 tests

**Evidence**: `cache.rs` lines 684-727, three tests:
1. `test_cache_key_with_generation_format` -- verifies key format string
2. `test_query_hash_consistency` -- verifies query normalization hashing
3. `test_generation_key_format` -- verifies generation key format

All three tests are format/string verification tests. None test actual cache behavior (get/set/invalidation) because the `QueryCache` requires a Redis connection pool.

**Integration test coverage in `warehouse_tests.rs`**:
- `test_default_cache_config` (line 322) -- config defaults
- `test_cache_config_disabled` (line 330) -- disabled flag
- `test_query_plan_cache_performance` (line 747) -- plan cache (not result cache)
- `test_query_plan_cache_hit_miss` (line 1246) -- plan cache hit/miss
- `test_query_plan_cache_generation_invalidation` (line 1272) -- plan cache invalidation
- `test_query_plan_cache_memory_limit` (line 1293) -- plan cache limits
- `test_concurrent_read_with_invalidation` (line 1470) -- async, tests concurrent invalidation
- `test_generation_invalidation_during_concurrent_writes` (line 1526) -- async, concurrent writes

The integration tests cover the `QueryPlanCache` (in `rewriter.rs`), not the Redis-based `QueryCache` (in `cache.rs`). The actual `QueryCache` get/set/invalidation logic is **untested** beyond format checks.

Untested code paths:
- `QueryCache::get()` -- actual Redis retrieval and deserialization
- `QueryCache::set()` -- serialization and Redis storage with TTL
- `QueryCache::set_with_tier()` -- tiered TTL selection
- `QueryCache::increment_generation()` -- generation counter logic
- `QueryCache::determine_tier()` -- tier selection logic
- Error paths (pool errors, serialization errors, timeouts)

**Severity**: Medium

**Verdict**: Confirmed. The `QueryCache` is a billing-critical component (cached queries avoid byte-scanned charges) with zero behavioral tests. All 3 unit tests are format-only. The integration tests cover a different cache (`QueryPlanCache`).

**Recommendation**: Add unit tests using a mock Redis or an in-memory Redis implementation (e.g., `mini-redis` or `fakeredis`). Priority test cases: tier selection logic (`determine_tier`), generation-based invalidation (set with gen N, increment to N+1, verify get misses), and serialization round-trip.

---

### T2. explain.rs has only 2 tests

**Evidence**: `explain.rs` lines 252-285, two tests:
1. `test_explain_simple_query` -- verifies a `Filter` step exists for `WHERE id = 1`
2. `test_explain_with_aggregation` -- verifies an `Aggregate` step exists for `GROUP BY`

Both tests only check for the presence of a step type, not the step contents (estimated rows, description, warnings, etc.).

The `QueryExplainer` also has:
- Cost estimation integration (via `QueryCostEstimator`)
- Warning generation (large table scan, missing WHERE, SELECT *)
- Step description formatting

These are all untested.

Indirect testing: BI compat tests (`bi_compat_tests.rs` line 361-363) test that `EXPLAIN SELECT ...` is accepted as valid SQL but do not test the explain output content.

**Severity**: Low

**Verdict**: Confirmed. The explain module has minimal test coverage. However, explain is a read-only diagnostic feature -- incorrect output does not affect query correctness or billing. It's a UX concern.

**Recommendation**: Add tests for warning generation (e.g., `SELECT * FROM large_table` should produce a `SelectStar` warning, a query without WHERE should produce `MissingWhereClause`). These are cheap to write and improve confidence in user-facing output.

---

### T3. No fuzz testing for SQL parsing/rewriting

**Evidence**: No `fuzz/` directory exists in the repo. No `cargo-fuzz` or `afl` configuration found.

`proptest` is used in `warehouse_tests.rs` with 4 `proptest!` blocks:
1. Query normalization idempotency (line 1754)
2. Query hash determinism (line 1760+)
3. Skip index property tests (line 1873)
4. R2TablePath property tests (line 1949)

The `proptest` tests cover utility functions (normalization, hashing, skip indexes) but **do not** test the SQL rewriter or parser. The rewriter is tested with ~150 hand-written unit tests, which cover common SQL patterns but not adversarial/random inputs.

The `sqlparser` crate itself has its own fuzz testing, so panics from parsing are unlikely. The risk is more in the AST transformation logic (`AstVisitor`, `TableTransformer` implementations) where unexpected AST shapes could cause panics or incorrect rewrites.

**Severity**: Low

**Verdict**: Confirmed. There is no fuzz testing for the rewriter. The risk is mitigated by: (a) `sqlparser` handling parsing robustly, (b) the rewriter using pattern matching with catch-all arms, and (c) 150+ hand-written test cases. Fuzz testing would still be valuable for finding edge cases in the AST transformation logic.

**Recommendation**: Low priority. If adding fuzz testing, start with a proptest strategy that generates random SQL strings (or random `sqlparser::ast::Statement` trees) and feeds them through `TableRewriter::rewrite`. The assertion would be: no panics, and the output is valid SQL (re-parseable).

---

### T4. MergeJoin/Project/Filter steps untested

**Evidence**: Searching for `ExecutionStep::MergeJoin`, `ExecutionStep::Project`, and `ExecutionStep::Filter` across the entire codebase: **zero results**.

These three variants are defined in `plan_optimizer.rs` lines 72-93:
```rust
MergeJoin { left_source, left_table, right_source, right_table, join_condition, estimated_output_rows },
Filter { predicate, estimated_output_rows },
Project { columns, estimated_output_rows },
```

They are never constructed anywhere -- not in the optimizer, not in the executor, not in the federation planner. They exist only as type definitions. The `explain.rs` module references `Filter` as a `StepType` variant for explain output but uses a separate `StepType` enum, not `ExecutionStep`.

**Severity**: Medium

**Verdict**: Confirmed -- these are dead code. They were likely added as forward declarations for planned features (merge join for sorted data, filter/project pushdown) but never implemented. They add 22 lines of unused type definitions and require `Clone`, `Serialize`, `Deserialize` derives.

**Recommendation**: Either (a) remove the dead variants and add them back when implementing the features, or (b) add `#[allow(dead_code)]` with a comment explaining they're reserved for future use. Option (a) is preferred since dead code creates false expectations about module capabilities.

---

### T5. Integration tests are all `#[ignore]`

**Evidence**: `pond/tests/warehouse_tests.rs` has:
- 89 total test functions (`#[test]` or `#[tokio::test]`)
- 5 marked with `#[ignore]` (requiring ClickHouse/PostgreSQL/MySQL)
- 84 non-ignored tests

CI configuration (`.github/workflows/test.yml` line 165-167):
```yaml
- name: Test
  run: cargo test --all-features
  working-directory: pond
```

`cargo test --all-features` runs all non-ignored tests. There is **no separate CI stage** for ignored tests with real infrastructure (no Docker compose, no service containers in the workflow).

The 5 ignored tests are:
- ClickHouse integration tests (basic executor, system tables, streaming, timeout, LIMIT)
- PostgreSQL/MySQL federated query tests

**Severity**: Low

**Verdict**: Partially confirmed. The original claim that "many integration tests are all `#[ignore]`" overstated the issue. Only 5 of 89 tests are ignored (5.6%). The vast majority (84 tests) run in CI without external dependencies. The ignored tests cover ClickHouse/PostgreSQL/MySQL connectivity which genuinely requires infrastructure.

**Recommendation**: Consider adding a Docker Compose file and a separate CI job that runs ignored tests with containerized ClickHouse. PostgreSQL/MySQL tests can use the GitHub Actions `services` feature. This would cover the 5 remaining tests.

---

*Report generated from code review investigation. All 20 items have been investigated with specific code evidence.*
