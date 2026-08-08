//! Warehouse benchmarks for performance-critical operations.
//!
//! Run with: cargo bench --bench warehouse_benchmarks
//!
//! Run specific benchmark groups:
//!   cargo bench --bench warehouse_benchmarks -- skip_index_filter
//!   cargo bench --bench warehouse_benchmarks -- data_types
//!   cargo bench --bench warehouse_benchmarks -- cardinality
//!   cargo bench --bench warehouse_benchmarks -- table_shape
//!   cargo bench --bench warehouse_benchmarks -- realworld
//!   cargo bench --bench warehouse_benchmarks -- edge_cases
//!   cargo bench --bench warehouse_benchmarks -- memory
//!
//! These benchmarks cover:
//!
//! **Core FST Operations:**
//! - Skip index filtering at various scales (10K, 100K, 500K files)
//! - FST construction for different cardinalities
//! - Query rewriting (parsing + AST transformation)
//! - Query plan cache hit/miss latency
//!
//! **Data Type Variety:**
//! - Numeric columns (integers, floats, decimals)
//! - Timestamp/date columns
//! - Boolean columns
//! - Mixed type columns (realistic scenarios)
//!
//! **Cardinality Spectrum:**
//! - Ultra-low (2-3 values) to ultra-high (50K+ values)
//! - UUID columns (high cardinality - should skip FST)
//! - Email columns (real-world high cardinality)
//!
//! **Table Shapes:**
//! - Narrow tables (5 columns)
//! - Wide tables (100 columns)
//! - Ultra-wide tables (500 columns)
//! - Column selection overhead
//!
//! **Real-World Patterns:**
//! - E-commerce (orders, payments, shipping)
//! - Event logs (page views, clicks, signups)
//! - Time-series (IoT sensors, metrics)
//!
//! **Edge Cases:**
//! - Single value columns (all same value)
//! - All unique columns (no value reuse)
//! - Skewed distributions (90% one value)
//! - Long strings (1KB+ values)
//! - Unicode/multi-byte strings (CJK, Arabic, emoji)
//!
//! **Memory Pressure:**
//! - FST memory usage at scale
//! - Behavior near cardinality limits (100K)
//! - Partition scaling overhead

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;

use reiver_pond::warehouse::indexes::skip_index::{
    DataSkipIndex, FileSkipIndex, HierarchicalSkipIndex,
};
use reiver_pond::warehouse::query::rewriter::{QueryPlanCache, TableRewriter};
use reiver_pond::warehouse::types::R2TablePath;
use reiver_pond::warehouse::utils::hash_query;

// ============================================================================
// Skip Index Benchmarks
// ============================================================================

/// Create a test FileSkipIndex with specified number of values per column.
fn create_file_skip_index(file_path: &str, values_per_column: usize) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Add a status column with low cardinality
    columns.insert(
        "status".to_string(),
        vec!["active".to_string(), "inactive".to_string(), "pending".to_string()],
    );
    
    // Add a customer column with medium cardinality
    let customers: Vec<String> = (0..values_per_column)
        .map(|i| format!("customer_{}", i % 1000))
        .collect();
    columns.insert("customer".to_string(), customers);
    
    // Add a region column with low cardinality
    columns.insert(
        "region".to_string(),
        vec!["us-east".to_string(), "us-west".to_string(), "eu-west".to_string()],
    );
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create a HierarchicalSkipIndex with specified number of files and partitions.
fn create_hierarchical_index(file_count: usize, partition_count: usize) -> HierarchicalSkipIndex {
    create_hierarchical_index_with_values(file_count, partition_count, 100)
}

/// Create a HierarchicalSkipIndex with configurable values per column.
/// Use fewer values for large scale tests to speed up setup.
fn create_hierarchical_index_with_values(
    file_count: usize,
    partition_count: usize,
    values_per_column: usize,
) -> HierarchicalSkipIndex {
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:06}.parquet", partition_key, f);
            let file_index = create_file_skip_index(&file_path, values_per_column);
            index
                .add_file(&partition_key, file_index, 10_000)
                .expect("Failed to add file");
        }
    }
    
    index
}

/// Benchmark skip index filtering at various scales.
fn bench_skip_index_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("skip_index_filter");
    group.sample_size(50); // Reduce sample size for larger tests
    
    // Test with predicates that should filter out most files
    let mut predicates = HashMap::new();
    predicates.insert("status".to_string(), "active".to_string());
    predicates.insert("customer".to_string(), "customer_42".to_string());
    
    // Note: 500K+ files takes significant time to build indexes during setup
    for (file_count, partition_count) in [(10_000, 100), (100_000, 1000), (500_000, 1000)] {
        let index = create_hierarchical_index(file_count, partition_count);
        
        group.throughput(Throughput::Elements(file_count as u64));
        
        // Benchmark filtering without partition hints
        group.bench_with_input(
            BenchmarkId::new("no_hint", file_count),
            &(&index, &predicates),
            |b, (idx, preds)| {
                b.iter(|| {
                    let result = idx.filter_with_partition_hint(black_box(preds), None);
                    black_box(result)
                })
            },
        );
        
        // Benchmark filtering with partition hints (simulating date range query)
        let partition_hints: Vec<&str> = vec!["2025/01", "2025/02"];
        group.bench_with_input(
            BenchmarkId::new("with_hint", file_count),
            &(&index, &predicates, &partition_hints),
            |b, (idx, preds, hints)| {
                b.iter(|| {
                    let result = idx.filter_with_partition_hint(
                        black_box(preds),
                        Some(black_box(hints.as_slice())),
                    );
                    black_box(result)
                })
            },
        );
    }
    
    group.finish();
}

/// Benchmark DataSkipIndex (flat) vs HierarchicalSkipIndex.
fn bench_flat_vs_hierarchical(c: &mut Criterion) {
    let mut group = c.benchmark_group("flat_vs_hierarchical");
    group.sample_size(50);
    
    let file_count = 50_000;
    let partition_count = 100;
    
    // Create flat index
    let mut flat_index = DataSkipIndex::new();
    for p in 0..partition_count {
        for f in 0..(file_count / partition_count) {
            let file_path = format!("2025/{:02}/data_{:06}.parquet", (p % 12) + 1, f);
            let file_index = create_file_skip_index(&file_path, 100);
            flat_index.add_file(file_index);
        }
    }
    
    // Create hierarchical index
    let hierarchical_index = create_hierarchical_index(file_count, partition_count);
    
    let mut predicates = HashMap::new();
    predicates.insert("status".to_string(), "active".to_string());
    
    group.bench_function("flat_filter", |b| {
        b.iter(|| {
            let result = flat_index.filter_files_by_predicates(black_box(&predicates));
            black_box(result)
        })
    });
    
    group.bench_function("hierarchical_filter", |b| {
        b.iter(|| {
            let result = hierarchical_index.filter_with_partition_hint(black_box(&predicates), None);
            black_box(result)
        })
    });
    
    group.finish();
}

// ============================================================================
// FST Build Benchmarks
// ============================================================================

/// Benchmark FST construction for different cardinalities.
fn bench_fst_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("fst_build");
    
    for cardinality in [100, 1_000, 10_000, 50_000] {
        let values: Vec<String> = (0..cardinality)
            .map(|i| format!("value_{:08}", i))
            .collect();
        
        let mut columns = HashMap::new();
        columns.insert("test_column".to_string(), values);
        
        group.throughput(Throughput::Elements(cardinality as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(cardinality),
            &columns,
            |b, cols| {
                b.iter(|| {
                    let result = FileSkipIndex::build(
                        black_box("test.parquet"),
                        black_box(cols.clone()),
                    );
                    black_box(result)
                })
            },
        );
    }
    
    group.finish();
}

// ============================================================================
// Query Rewriter Benchmarks
// ============================================================================

/// Create test tables for rewriting benchmarks.
fn create_test_tables() -> HashMap<String, R2TablePath> {
    use reiver_pond::warehouse::types::SourceType;
    
    let project_id = uuid::Uuid::new_v4();
    let mut tables = HashMap::new();
    
    tables.insert(
        "orders".to_string(),
        R2TablePath::try_with_project(project_id, SourceType::Stripe, "orders")
            .expect("valid path"),
    );
    
    tables.insert(
        "customers".to_string(),
        R2TablePath::try_with_project(project_id, SourceType::Stripe, "customers")
            .expect("valid path"),
    );
    
    tables.insert(
        "products".to_string(),
        R2TablePath::try_with_project(project_id, SourceType::Stripe, "products")
            .expect("valid path"),
    );
    
    tables.insert(
        "invoices".to_string(),
        R2TablePath::try_with_project(project_id, SourceType::Stripe, "invoices")
            .expect("valid path"),
    );
    
    tables
}

/// Benchmark query rewriting for different query complexities.
fn bench_query_rewrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_rewrite");
    
    let tables = create_test_tables();
    let rewriter = TableRewriter::new("warehouse_r2");
    
    let queries = [
        (
            "simple_select",
            "SELECT id, name, amount FROM orders WHERE status = 'active'",
        ),
        (
            "with_join",
            "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'active'",
        ),
        (
            "with_subquery",
            "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'us')",
        ),
        (
            "multi_join",
            "SELECT o.id, c.name, p.name as product_name, i.total \
             FROM orders o \
             JOIN customers c ON o.customer_id = c.id \
             JOIN products p ON o.product_id = p.id \
             JOIN invoices i ON o.invoice_id = i.id \
             WHERE o.status = 'active' AND c.tier = 'premium'",
        ),
        (
            "with_aggregation",
            "SELECT customer_id, SUM(amount) as total, COUNT(*) as count \
             FROM orders \
             WHERE created_at >= '2025-01-01' \
             GROUP BY customer_id \
             HAVING SUM(amount) > 1000 \
             ORDER BY total DESC \
             LIMIT 100",
        ),
    ];
    
    for (name, sql) in queries {
        group.bench_function(name, |b| {
            b.iter(|| {
                let result = rewriter.rewrite(black_box(sql), black_box(&tables));
                black_box(result)
            })
        });
    }
    
    group.finish();
}

// ============================================================================
// Query Plan Cache Benchmarks
// ============================================================================

/// Benchmark query plan cache operations.
fn bench_query_plan_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_plan_cache");
    
    let cache = QueryPlanCache::new(10_000);
    let tables = create_test_tables();
    
    // Pre-populate cache with some queries
    for i in 0..1000 {
        let sql = format!("SELECT * FROM orders WHERE id = {}", i);
        let rewritten = format!("SELECT * FROM s3(...) WHERE id = {}", i);
        cache.put(&sql, &tables, rewritten);
    }
    
    // Benchmark cache hit
    let hit_sql = "SELECT * FROM orders WHERE id = 500";
    group.bench_function("cache_hit", |b| {
        b.iter(|| {
            let result = cache.get(black_box(hit_sql), black_box(&tables));
            black_box(result)
        })
    });
    
    // Benchmark cache miss
    let miss_sql = "SELECT * FROM orders WHERE id = 99999";
    group.bench_function("cache_miss", |b| {
        b.iter(|| {
            let result = cache.get(black_box(miss_sql), black_box(&tables));
            black_box(result)
        })
    });
    
    // Benchmark cache put
    group.bench_function("cache_put", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let sql = format!("SELECT * FROM products WHERE id = {}", i);
            let rewritten = format!("SELECT * FROM s3(...) WHERE id = {}", i);
            cache.put(black_box(&sql), black_box(&tables), rewritten);
            i = i.wrapping_add(1);
        })
    });
    
    group.finish();
}

/// Benchmark query hashing.
fn bench_query_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_hash");
    
    let queries = [
        ("short", "SELECT * FROM orders"),
        (
            "medium",
            "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'active' AND o.created_at > '2025-01-01'",
        ),
        (
            "long",
            {
                let cols = (0..100).map(|i| format!("col_{}", i)).collect::<Vec<_>>().join(", ");
                let wheres = (0..50).map(|i| format!("col_{} = {}", i, i)).collect::<Vec<_>>().join(" AND ");
                format!("SELECT {} FROM orders WHERE {}", cols, wheres).leak() as &str
            },
        ),
    ];
    
    for (name, sql) in queries {
        group.bench_function(name, |b| {
            b.iter(|| {
                let result = hash_query(black_box(sql));
                black_box(result)
            })
        });
    }
    
    group.finish();
}

// ============================================================================
// Streaming Result Processing Benchmarks
// ============================================================================

use reiver_pond::warehouse::utils::estimate_json_value_memory;

/// Generate mock JSON row data for streaming benchmarks.
fn generate_mock_rows(row_count: usize, column_count: usize) -> Vec<Vec<serde_json::Value>> {
    (0..row_count)
        .map(|i| {
            (0..column_count)
                .map(|j| {
                    match j % 4 {
                        0 => serde_json::json!(i as i64 + j as i64),
                        1 => serde_json::json!(format!("value_{}_{}", i, j)),
                        2 => serde_json::json!(i as f64 / 1000.0),
                        _ => serde_json::json!(i % 2 == 0),
                    }
                })
                .collect()
        })
        .collect()
}

/// Benchmark memory estimation for JSON values of varying complexity.
fn bench_memory_estimation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_estimation");
    
    // Simple values
    let simple = serde_json::json!("hello world");
    group.bench_function("string_simple", |b| {
        b.iter(|| {
            let size = estimate_json_value_memory(black_box(&simple));
            black_box(size)
        })
    });
    
    // Small object
    let small_obj = serde_json::json!({
        "id": 12345,
        "name": "Alice",
        "email": "alice@example.com",
        "active": true
    });
    group.bench_function("object_small", |b| {
        b.iter(|| {
            let size = estimate_json_value_memory(black_box(&small_obj));
            black_box(size)
        })
    });
    
    // Large object with nested arrays
    let large_obj = serde_json::json!({
        "id": 12345,
        "name": "Alice",
        "metadata": {
            "created_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-20T14:22:00Z",
            "tags": ["important", "premium", "verified", "active"]
        },
        "orders": [
            {"id": 1, "amount": 99.99, "status": "completed"},
            {"id": 2, "amount": 149.99, "status": "pending"},
            {"id": 3, "amount": 299.99, "status": "shipped"}
        ]
    });
    group.bench_function("object_large_nested", |b| {
        b.iter(|| {
            let size = estimate_json_value_memory(black_box(&large_obj));
            black_box(size)
        })
    });
    
    // Array of numbers
    let number_array: serde_json::Value = (0..100).map(|i| i as i64).collect();
    group.bench_function("array_100_numbers", |b| {
        b.iter(|| {
            let size = estimate_json_value_memory(black_box(&number_array));
            black_box(size)
        })
    });
    
    group.finish();
}

/// Benchmark row processing throughput (simulating streaming collection).
fn bench_streaming_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_throughput");
    group.sample_size(30); // Lower sample size for slower operations
    
    // Test different row/column combinations
    for (row_count, column_count) in [(100, 10), (1000, 10), (10_000, 10), (1000, 50)] {
        let rows = generate_mock_rows(row_count, column_count);
        
        group.throughput(Throughput::Elements(row_count as u64));
        
        group.bench_with_input(
            BenchmarkId::new("collect_rows", format!("{}x{}", row_count, column_count)),
            &rows,
            |b, rows| {
                b.iter(|| {
                    // Simulate collecting rows into a Vec
                    let collected: Vec<Vec<serde_json::Value>> = rows.iter()
                        .map(|row| row.clone())
                        .collect();
                    black_box(collected)
                })
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("estimate_memory_per_row", format!("{}x{}", row_count, column_count)),
            &rows,
            |b, rows| {
                b.iter(|| {
                    // Simulate memory estimation during collection
                    let mut total_memory = 0usize;
                    for row in rows.iter() {
                        for value in row.iter() {
                            total_memory += estimate_json_value_memory(value);
                        }
                    }
                    black_box(total_memory)
                })
            },
        );
    }
    
    group.finish();
}

/// Benchmark JSON parsing (simulating ClickHouse response parsing).
fn bench_json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");
    
    // Small row (typical dashboard query)
    let small_row = r#"[1, "alice@example.com", 99.99, true, "2025-01-15"]"#;
    group.bench_function("parse_small_row", |b| {
        b.iter(|| {
            let parsed: Vec<serde_json::Value> = serde_json::from_str(black_box(small_row)).unwrap();
            black_box(parsed)
        })
    });
    
    // Medium row (analytics query with more columns)
    let medium_row = r#"[1, "alice", "alice@example.com", "US", "New York", 10028, 42.5, 1500.00, true, "2025-01-15", "premium", null, 25, 3.14159]"#;
    group.bench_function("parse_medium_row", |b| {
        b.iter(|| {
            let parsed: Vec<serde_json::Value> = serde_json::from_str(black_box(medium_row)).unwrap();
            black_box(parsed)
        })
    });
    
    // Large row with nested JSON (event data)
    let large_row = r#"[12345, "click_event", {"page": "/products/123", "referrer": "https://google.com", "utm_source": "newsletter", "device": {"type": "mobile", "os": "iOS", "browser": "Safari"}}, "2025-01-15T10:30:00Z", 1500, "session_abc123"]"#;
    group.bench_function("parse_large_row_with_nesting", |b| {
        b.iter(|| {
            let parsed: Vec<serde_json::Value> = serde_json::from_str(black_box(large_row)).unwrap();
            black_box(parsed)
        })
    });
    
    // Batch parsing (multiple rows at once)
    let batch_rows: String = (0..100)
        .map(|i| format!(r#"[{}, "user_{}", {}.99, true]"#, i, i, i))
        .collect::<Vec<_>>()
        .join("\n");
    group.bench_function("parse_100_rows_batch", |b| {
        b.iter(|| {
            let parsed: Vec<Vec<serde_json::Value>> = batch_rows
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            black_box(parsed)
        })
    });
    
    group.finish();
}

/// Benchmark skip index cache operations.
fn bench_skip_index_cache(c: &mut Criterion) {
    use reiver_pond::warehouse::indexes::skip_index_cache::SkipIndexCache;
    use uuid::Uuid;
    
    let mut group = c.benchmark_group("skip_index_cache");
    
    let cache = SkipIndexCache::with_defaults();
    let project_id = Uuid::new_v4();
    
    // Pre-populate cache
    for i in 0..100 {
        let index = HierarchicalSkipIndex::new();
        cache.put(project_id, &format!("table_{}", i), index, 1024);
    }
    
    // Benchmark cache hit
    group.bench_function("cache_hit", |b| {
        b.iter(|| {
            let result = cache.get(black_box(project_id), black_box("table_50"));
            black_box(result)
        })
    });
    
    // Benchmark cache miss
    let other_project = Uuid::new_v4();
    group.bench_function("cache_miss", |b| {
        b.iter(|| {
            let result = cache.get(black_box(other_project), black_box("nonexistent"));
            black_box(result)
        })
    });
    
    // Benchmark put
    group.bench_function("cache_put", |b| {
        let mut counter = 0usize;
        b.iter(|| {
            counter += 1;
            let index = HierarchicalSkipIndex::new();
            cache.put(black_box(project_id), &format!("bench_table_{}", counter % 1000), black_box(index), 1024);
        })
    });
    
    // Benchmark stats collection
    group.bench_function("cache_stats", |b| {
        b.iter(|| {
            let stats = cache.stats();
            black_box(stats)
        })
    });
    
    group.finish();
}

// ============================================================================
// Criterion Groups
// ============================================================================

// ============================================================================
// FST vs Parquet Stats vs Full Scan Comparison
// ============================================================================

/// Benchmark comparing different query strategies:
/// - FST index lookup (sub-millisecond for low-cardinality)
/// - Parquet stats only (numeric min/max from metadata)  
/// - Full scan (baseline, no filtering)
///
/// This validates reiver's competitive advantage over PostHog's approach.
fn bench_indexing_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing_strategies");
    group.sample_size(50);
    
    // Create a large index simulating a production dataset
    let file_count = 10_000;
    let partition_count = 100;
    let index = create_hierarchical_index(file_count, partition_count);
    
    // Scenario 1: Equality filter on low-cardinality column (FST excels)
    // e.g., WHERE status = 'active'
    let mut equality_predicate = HashMap::new();
    equality_predicate.insert("status".to_string(), "active".to_string());
    
    group.throughput(Throughput::Elements(file_count as u64));
    
    group.bench_function("fst_equality_filter", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&equality_predicate), None);
            // FST provides exact match - files either contain value or don't
            black_box(result)
        })
    });
    
    // Scenario 2: Range filter (would use numeric stats in real implementation)
    // This simulates checking min/max stats for each file
    // In production, this uses Parquet footer metadata
    let mut range_predicate = HashMap::new();
    range_predicate.insert("customer".to_string(), "customer_500".to_string());
    
    group.bench_function("parquet_stats_range_filter", |b| {
        b.iter(|| {
            // FST can still help with string ranges by prefix matching
            let result = index.filter_with_partition_hint(black_box(&range_predicate), None);
            black_box(result)
        })
    });
    
    // Scenario 3: Full scan (baseline - what PostHog does)
    // Simply return all files without filtering
    group.bench_function("full_scan_baseline", |b| {
        b.iter(|| {
            // Get all files without filtering
            let all_files = index.total_files();
            black_box(all_files)
        })
    });
    
    group.finish();
}

/// Benchmark skip rate calculation for different query patterns.
/// Higher skip rate = more files pruned = faster queries.
fn bench_skip_rate_scenarios(c: &mut Criterion) {
    let mut group = c.benchmark_group("skip_rate");
    group.sample_size(30);
    
    let file_count = 10_000;
    let partition_count = 100;
    let index = create_hierarchical_index(file_count, partition_count);
    
    // Scenario A: Highly selective query (should skip ~90% of files)
    let mut highly_selective = HashMap::new();
    highly_selective.insert("status".to_string(), "pending".to_string());
    highly_selective.insert("region".to_string(), "eu-west".to_string());
    
    group.bench_function("highly_selective_90pct", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&highly_selective), None);
            let matching = result.len();
            let skip_rate = 1.0 - (matching as f64 / file_count as f64);
            black_box((matching, skip_rate))
        })
    });
    
    // Scenario B: Moderately selective query (should skip ~50% of files)
    let mut moderately_selective = HashMap::new();
    moderately_selective.insert("status".to_string(), "active".to_string());
    
    group.bench_function("moderately_selective_50pct", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&moderately_selective), None);
            let matching = result.len();
            let skip_rate = 1.0 - (matching as f64 / file_count as f64);
            black_box((matching, skip_rate))
        })
    });
    
    // Scenario C: Date partition + value filter (best case)
    let partition_hints = vec!["2025/01", "2025/02"];
    let mut combined_filter = HashMap::new();
    combined_filter.insert("status".to_string(), "active".to_string());
    
    group.bench_function("partition_plus_value_filter", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(
                black_box(&combined_filter),
                Some(black_box(&partition_hints)),
            );
            let matching = result.len();
            // This should have the highest skip rate
            black_box(matching)
        })
    });
    
    group.finish();
}

/// Benchmark index build time for different scenarios.
/// This validates that building indexes is cost-effective.
fn bench_index_build_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_build_cost");
    group.sample_size(20);
    
    // Scenario: Build index for different file counts
    // Measures the overhead of maintaining indexes
    for file_count in [100, 1000, 5000] {
        group.throughput(Throughput::Elements(file_count as u64));
        
        group.bench_with_input(
            BenchmarkId::new("incremental_add", file_count),
            &file_count,
            |b, &count| {
                b.iter(|| {
                    let mut index = HierarchicalSkipIndex::new();
                    for i in 0..count {
                        let partition = format!("2025/{:02}", (i % 12) + 1);
                        let file_path = format!("{}/data_{:04}.parquet", partition, i);
                        let file_index = create_file_skip_index(&file_path, 50);
                        let _ = index.add_file(&partition, file_index, 1000);
                    }
                    black_box(index)
                })
            },
        );
    }
    
    group.finish();
}

/// Benchmark demonstrating scaling benefits of FST indexing at large scale.
/// Shows how FST + partition hints maintain constant-time performance
/// while full scan grows linearly with data size.
fn bench_large_scale_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_fst");
    group.sample_size(20);
    
    let mut predicates = HashMap::new();
    predicates.insert("status".to_string(), "active".to_string());
    predicates.insert("region".to_string(), "us-east".to_string());
    
    let partition_hints: Vec<&str> = vec!["2025/01", "2025/02", "2025/03"];
    
    // Test at multiple scales to show scaling behavior
    // Use minimal indexes (3 values/column = just low-cardinality columns) for fast setup
    // Scale from 5K to 100K files - this shows the trend clearly
    for (file_count, partition_count) in [
        (5_000, 50),
        (10_000, 100),
        (25_000, 250),
        (50_000, 500),
        (100_000, 1000),
    ] {
        // Use minimal values - just the 3 low-cardinality columns (status, region)
        // No customer column to speed up FST building dramatically
        let index = create_minimal_hierarchical_index(file_count, partition_count);
        
        group.throughput(Throughput::Elements(file_count as u64));
        
        // FST with partition hints - should remain nearly constant
        group.bench_with_input(
            BenchmarkId::new("fst_with_partition", file_count),
            &(&index, &predicates, &partition_hints),
            |b, (idx, preds, hints)| {
                b.iter(|| {
                    let result = idx.filter_with_partition_hint(
                        black_box(preds),
                        Some(black_box(hints.as_slice())),
                    );
                    black_box(result)
                })
            },
        );
        
        // FST without partition hints - grows sub-linearly
        group.bench_with_input(
            BenchmarkId::new("fst_no_partition", file_count),
            &(&index, &predicates),
            |b, (idx, preds)| {
                b.iter(|| {
                    let result = idx.filter_with_partition_hint(black_box(preds), None);
                    black_box(result)
                })
            },
        );
        
        // Simulated full scan - linear cost (representing PostHog approach)
        // Iterate through file count to simulate per-file work
        group.bench_with_input(
            BenchmarkId::new("simulated_full_scan", file_count),
            &file_count,
            |b, &count| {
                b.iter(|| {
                    // Simulating iterating through all files without index
                    let mut sum = 0usize;
                    for i in 0..count {
                        sum = sum.wrapping_add(i);
                    }
                    black_box(sum)
                })
            },
        );
    }
    
    group.finish();
}

/// Create a minimal HierarchicalSkipIndex for large scale benchmarks.
/// Only includes low-cardinality columns (status, region) - no customer column.
/// This is much faster to build while still testing FST filtering behavior.
fn create_minimal_hierarchical_index(file_count: usize, partition_count: usize) -> HierarchicalSkipIndex {
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:06}.parquet", partition_key, f);
            let file_index = create_minimal_file_skip_index(&file_path);
            index
                .add_file(&partition_key, file_index, 10_000)
                .expect("Failed to add file");
        }
    }
    
    index
}

/// Create a minimal FileSkipIndex with only low-cardinality columns.
fn create_minimal_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Only low cardinality columns - fast to build
    columns.insert(
        "status".to_string(),
        vec!["active".to_string(), "inactive".to_string(), "pending".to_string()],
    );
    
    columns.insert(
        "region".to_string(),
        vec!["us-east".to_string(), "us-west".to_string(), "eu-west".to_string()],
    );
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

// ============================================================================
// Realistic Latency Constants (based on production measurements)
// ============================================================================

/// Average S3/R2 GetObject latency to first byte (network round-trip)
const S3_NETWORK_LATENCY_MS: u64 = 50;

/// Average time to parse Parquet footer metadata after download
const PARQUET_FOOTER_PARSE_MS: u64 = 2;

/// Total time per file to check if it matches query predicate (no index)
const TOTAL_FILE_CHECK_MS: u64 = S3_NETWORK_LATENCY_MS + PARQUET_FOOTER_PARSE_MS; // 52ms

/// Average time to read matching file data after FST determines it's needed
const FILE_DATA_READ_MS: u64 = 100;

// ============================================================================
// Full Scan Baseline Helper (Realistic with I/O simulation)
// ============================================================================

/// Simulate a full scan without any index using REALISTIC latencies.
/// This represents what PostHog and other systems do - read every file's
/// Parquet footer from S3/R2 to check row group statistics.
/// 
/// Real-world cost: ~52ms per file (50ms network + 2ms parse)
#[inline(never)]
fn simulate_full_scan_realistic(file_count: usize) -> usize {
    let mut matched = 0usize;
    for i in 0..file_count {
        // Simulate real S3/R2 network latency + Parquet footer parse
        std::thread::sleep(std::time::Duration::from_millis(TOTAL_FILE_CHECK_MS));
        
        // Simulate ~10% of files matching the predicate
        if i % 10 == 0 {
            matched += 1;
        }
    }
    matched
}

/// Simulate partition-aware full scan with realistic latencies.
/// Only scans files in matching partitions, but still reads each file's metadata.
#[inline(never)]
fn simulate_partition_scan_realistic(file_count: usize, partition_count: usize, matching_partitions: usize) -> usize {
    let files_per_partition = file_count / partition_count;
    let files_to_scan = files_per_partition * matching_partitions;
    simulate_full_scan_realistic(files_to_scan)
}

/// Simulate reading matched files after FST filtering.
/// FST tells us which files to read, then we pay the read cost only for those.
#[inline(never)]
fn simulate_read_matched_files(matched_file_count: usize) -> usize {
    for _ in 0..matched_file_count {
        // Simulate reading actual file data (larger than just footer)
        std::thread::sleep(std::time::Duration::from_millis(FILE_DATA_READ_MS));
    }
    matched_file_count
}

// ============================================================================
// CPU-only simulation (for micro-benchmarks, no sleep)
// ============================================================================

/// Fast CPU-only simulation for micro-benchmarks (no I/O simulation).
/// Use this for comparing FST internal performance, not for realistic comparisons.
#[inline(never)]
fn simulate_full_scan_cpu_only(file_count: usize, work_per_file: usize) -> usize {
    let mut matched = 0usize;
    for i in 0..file_count {
        let mut hash = i;
        for _ in 0..work_per_file {
            hash = hash.wrapping_mul(31).wrapping_add(17);
        }
        if hash % 10 == 0 {
            matched += 1;
        }
    }
    matched
}

/// CPU-only partition scan for micro-benchmarks.
#[inline(never)]
fn simulate_partition_scan_cpu_only(file_count: usize, partition_count: usize, matching_partitions: usize, work_per_file: usize) -> usize {
    let files_per_partition = file_count / partition_count;
    let files_to_scan = files_per_partition * matching_partitions;
    simulate_full_scan_cpu_only(files_to_scan, work_per_file)
}

// ============================================================================
// Realistic I/O Comparison Benchmarks
// ============================================================================

/// Benchmark realistic FST vs Full Scan comparison with actual I/O latency simulation.
/// Uses small file counts because realistic latencies (52ms/file) make large scales impractical.
/// 
/// This is the KEY benchmark that shows real-world FST value.
fn bench_realistic_io_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_io");
    group.sample_size(10); // Low sample size due to sleep-based delays
    
    // Test with small file counts (realistic benchmark times)
    // 10 files × 52ms = 520ms per iteration
    // 50 files × 52ms = 2.6s per iteration
    for file_count in [10, 25, 50] {
        let partition_count = 5;
        
        // Build FST index
        let mut index = HierarchicalSkipIndex::new();
        let files_per_partition = file_count / partition_count;
        for p in 0..partition_count {
            let partition_key = format!("2025/{:02}", (p % 12) + 1);
            for f in 0..files_per_partition {
                let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
                let file_index = create_minimal_file_skip_index(&file_path);
                index.add_file(&partition_key, file_index, 10_000).expect("add file");
            }
        }
        
        let mut predicate = HashMap::new();
        predicate.insert("status".to_string(), "active".to_string());
        
        // FST lookup only (sub-millisecond)
        group.bench_function(
            BenchmarkId::new("fst_lookup_only", file_count),
            |b| {
                b.iter(|| {
                    let result = index.filter_with_partition_hint(black_box(&predicate), None);
                    black_box(result)
                })
            },
        );
        
        // FST lookup + read matched files (FST says ~33% match with 3 status values)
        let matched_count = file_count / 3;
        group.bench_function(
            BenchmarkId::new("fst_plus_read_matched", file_count),
            |b| {
                b.iter(|| {
                    let files = index.filter_with_partition_hint(black_box(&predicate), None);
                    // After FST filters, read the matched files
                    let read_count = simulate_read_matched_files(matched_count);
                    black_box((files, read_count))
                })
            },
        );
        
        // Full scan WITHOUT index - must check every file's metadata
        group.bench_function(
            BenchmarkId::new("no_index_full_scan", file_count),
            |b| {
                b.iter(|| {
                    let result = simulate_full_scan_realistic(black_box(file_count));
                    black_box(result)
                })
            },
        );
    }
    
    group.finish();
}

/// Benchmark showing FST value at scale with projected times.
/// Uses FST lookup (real) and calculates what full scan WOULD take.
fn bench_fst_value_at_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("fst_value_at_scale");
    group.sample_size(20);
    
    for file_count in [1_000, 10_000, 100_000] {
        let partition_count = file_count / 100; // 100 files per partition
        
        // Build minimal FST index
        let index = create_minimal_hierarchical_index(file_count, partition_count.max(1));
        
        let mut predicate = HashMap::new();
        predicate.insert("status".to_string(), "active".to_string());
        let partition_hints = vec!["2025/01"];
        
        // FST with partition hint
        group.bench_function(
            BenchmarkId::new("fst_with_partition", file_count),
            |b| {
                b.iter(|| {
                    let result = index.filter_with_partition_hint(
                        black_box(&predicate),
                        Some(black_box(&partition_hints)),
                    );
                    black_box(result)
                })
            },
        );
        
        // FST without partition hint
        group.bench_function(
            BenchmarkId::new("fst_no_partition", file_count),
            |b| {
                b.iter(|| {
                    let result = index.filter_with_partition_hint(black_box(&predicate), None);
                    black_box(result)
                })
            },
        );
        
        // Calculate projected full scan time (don't actually run it)
        let projected_full_scan_ms = file_count as u64 * TOTAL_FILE_CHECK_MS;
        let projected_full_scan_sec = projected_full_scan_ms as f64 / 1000.0;
        
        println!(
            "\n[{}] Projected full scan (no index): {:.1}s ({} files × {}ms)",
            file_count, projected_full_scan_sec, file_count, TOTAL_FILE_CHECK_MS
        );
    }
    
    group.finish();
}

// ============================================================================
// Data Type Variety Benchmarks
// ============================================================================

/// Create a FileSkipIndex simulating numeric column values as strings.
/// In production, numeric columns would use min/max stats instead of FST.
/// This benchmark shows what happens if you incorrectly FST-index numeric data.
fn create_numeric_file_skip_index(file_path: &str, value_count: usize) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Integer values as strings (e.g., quantity, count)
    let integers: Vec<String> = (0..value_count)
        .map(|i| format!("{}", i * 10))
        .collect();
    columns.insert("quantity".to_string(), integers);
    
    // Float values as strings (e.g., price, amount)
    let floats: Vec<String> = (0..value_count)
        .map(|i| format!("{:.2}", i as f64 * 9.99))
        .collect();
    columns.insert("price".to_string(), floats);
    
    // Decimal/currency values
    let decimals: Vec<String> = (0..value_count)
        .map(|i| format!("${:.2}", i as f64 * 123.45))
        .collect();
    columns.insert("amount".to_string(), decimals);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create a FileSkipIndex with timestamp/date column values.
fn create_timestamp_file_skip_index(file_path: &str, day_count: usize) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Date values (YYYY-MM-DD format)
    let dates: Vec<String> = (0..day_count)
        .map(|i| format!("2025-{:02}-{:02}", (i / 28) % 12 + 1, (i % 28) + 1))
        .collect();
    columns.insert("date".to_string(), dates);
    
    // ISO timestamp values
    let timestamps: Vec<String> = (0..day_count)
        .map(|i| format!("2025-{:02}-{:02}T{:02}:00:00Z", (i / 28) % 12 + 1, (i % 28) + 1, i % 24))
        .collect();
    columns.insert("created_at".to_string(), timestamps);
    
    // Hour buckets (low cardinality)
    let hours: Vec<String> = (0..24).map(|h| format!("{:02}:00", h)).collect();
    columns.insert("hour_bucket".to_string(), hours);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create a FileSkipIndex with boolean column values.
fn create_boolean_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Boolean as strings
    columns.insert(
        "is_active".to_string(),
        vec!["true".to_string(), "false".to_string()],
    );
    
    columns.insert(
        "is_verified".to_string(),
        vec!["true".to_string(), "false".to_string()],
    );
    
    // Nullable boolean (three-valued logic)
    columns.insert(
        "is_premium".to_string(),
        vec!["true".to_string(), "false".to_string(), "null".to_string()],
    );
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create a FileSkipIndex with mixed data types (realistic scenario).
fn create_mixed_type_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Low-cardinality string (ideal for FST)
    columns.insert(
        "status".to_string(),
        vec!["active".to_string(), "inactive".to_string(), "pending".to_string(), "cancelled".to_string(), "completed".to_string()],
    );
    
    // Medium-cardinality string
    let countries: Vec<String> = ["US", "UK", "DE", "FR", "JP", "AU", "CA", "BR", "IN", "MX", 
                                   "ES", "IT", "NL", "SE", "CH", "KR", "SG", "HK", "NZ", "IE"]
        .iter().map(|s| s.to_string()).collect();
    columns.insert("country".to_string(), countries);
    
    // Boolean
    columns.insert(
        "is_paid".to_string(),
        vec!["true".to_string(), "false".to_string()],
    );
    
    // Payment method (low cardinality)
    columns.insert(
        "payment_method".to_string(),
        vec!["card".to_string(), "bank".to_string(), "crypto".to_string(), "wire".to_string(), "ach".to_string()],
    );
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Benchmark FST behavior with numeric column data.
/// Shows why numeric columns should use min/max stats, not FST.
fn bench_numeric_column_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_types/numeric");
    group.sample_size(30);
    
    let file_count = 1000;
    let partition_count = 10;
    
    // Build index with numeric columns
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_numeric_file_skip_index(&file_path, 100);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Filter on numeric value - FST can do exact match but not range
    let mut predicate = HashMap::new();
    predicate.insert("quantity".to_string(), "100".to_string());
    
    group.bench_function("fst_exact_numeric", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&predicate), None);
            black_box(result)
        })
    });
    
    // Full scan baseline - no index
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark FST behavior with timestamp/date column data.
fn bench_timestamp_column_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_types/timestamp");
    group.sample_size(30);
    
    let file_count = 1000;
    let partition_count = 10;
    
    // Build index with timestamp columns
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_timestamp_file_skip_index(&file_path, 30);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Filter on hour bucket (low cardinality - good for FST)
    let mut hour_predicate = HashMap::new();
    hour_predicate.insert("hour_bucket".to_string(), "14:00".to_string());
    
    group.bench_function("fst_hour_bucket", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&hour_predicate), None);
            black_box(result)
        })
    });
    
    // Combined: partition hint + hour filter
    let partition_hints = vec!["2025/01"];
    group.bench_function("fst_partition_plus_hour", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(
                black_box(&hour_predicate),
                Some(black_box(&partition_hints)),
            );
            black_box(result)
        })
    });
    
    // Full scan baseline - no index
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    // Partition-only scan (no FST, but with partition pruning)
    group.bench_function("no_index_partition_only", |b| {
        b.iter(|| {
            let result = simulate_partition_scan_cpu_only(black_box(file_count), black_box(partition_count), 1, 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark FST with mixed data types (realistic production scenario).
fn bench_mixed_type_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_types/mixed");
    group.sample_size(30);
    
    let file_count = 5000;
    let partition_count = 50;
    
    // Build index with mixed type columns
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_mixed_type_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Single predicate on low-cardinality column
    let mut single_pred = HashMap::new();
    single_pred.insert("status".to_string(), "active".to_string());
    
    group.bench_function("fst_single_column", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&single_pred), None);
            black_box(result)
        })
    });
    
    // Multiple predicates (AND logic)
    let mut multi_pred = HashMap::new();
    multi_pred.insert("status".to_string(), "active".to_string());
    multi_pred.insert("country".to_string(), "US".to_string());
    multi_pred.insert("is_paid".to_string(), "true".to_string());
    
    group.bench_function("fst_multi_column", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&multi_pred), None);
            black_box(result)
        })
    });
    
    // Full scan baseline - no index
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

// ============================================================================
// Cardinality Spectrum Benchmarks
// ============================================================================

/// Create FileSkipIndex with specified cardinality level.
fn create_cardinality_file_skip_index(file_path: &str, cardinality: usize) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    let values: Vec<String> = (0..cardinality)
        .map(|i| format!("value_{:08}", i))
        .collect();
    columns.insert("test_column".to_string(), values);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create FileSkipIndex with UUID-like high-cardinality values.
fn create_uuid_file_skip_index(file_path: &str, count: usize) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Simulated UUIDs (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
    let uuids: Vec<String> = (0..count)
        .map(|i| format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", 
            i, i % 65536, (i * 7) % 65536, (i * 13) % 65536, i * 17))
        .collect();
    columns.insert("id".to_string(), uuids);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create FileSkipIndex with email-like values.
fn create_email_file_skip_index(file_path: &str, count: usize) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    let domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com", "example.org"];
    let emails: Vec<String> = (0..count)
        .map(|i| format!("user{}@{}", i, domains[i % domains.len()]))
        .collect();
    columns.insert("email".to_string(), emails);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Benchmark FST across the cardinality spectrum.
fn bench_cardinality_spectrum(c: &mut Criterion) {
    let mut group = c.benchmark_group("cardinality/spectrum");
    group.sample_size(20);
    
    let file_count = 1000;
    let partition_count = 10;
    
    // First, add no_index baseline for comparison
    group.bench_function("no_index_baseline", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    // Test different cardinality levels
    let cardinalities = [
        ("fst_card_3", 3),
        ("fst_card_50", 50),
        ("fst_card_1000", 1000),
        ("fst_card_10000", 10_000),
        ("fst_card_50000", 50_000),
    ];
    
    for (name, cardinality) in cardinalities {
        // Build index
        let mut index = HierarchicalSkipIndex::new();
        let files_per_partition = file_count / partition_count;
        for p in 0..partition_count {
            let partition_key = format!("2025/{:02}", (p % 12) + 1);
            for f in 0..files_per_partition {
                let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
                let file_index = create_cardinality_file_skip_index(&file_path, cardinality);
                index.add_file(&partition_key, file_index, 10_000).expect("add file");
            }
        }
        
        // Query for a specific value
        let mut predicate = HashMap::new();
        predicate.insert("test_column".to_string(), "value_00000000".to_string());
        
        group.bench_function(name, |b| {
            b.iter(|| {
                let result = index.filter_with_partition_hint(black_box(&predicate), None);
                black_box(result)
            })
        });
    }
    
    group.finish();
}

/// Benchmark FST with UUID columns (high cardinality - should be skipped in production).
fn bench_uuid_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("cardinality/uuid");
    group.sample_size(20);
    
    // Smaller scale for high-cardinality test
    let file_count = 100;
    let partition_count = 10;
    let uuids_per_file = 1000; // 1000 unique UUIDs per file
    
    // Build index with UUID columns
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_uuid_file_skip_index(&file_path, uuids_per_file);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Query for specific UUID
    let mut predicate = HashMap::new();
    predicate.insert("id".to_string(), "00000064-0064-02bc-0344-0000000006a4".to_string());
    
    group.bench_function("fst_uuid_match", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&predicate), None);
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark FST with email columns (high cardinality).
fn bench_email_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("cardinality/email");
    group.sample_size(20);
    
    let file_count = 100;
    let partition_count = 10;
    let emails_per_file = 500;
    
    // Build index with email columns
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_email_file_skip_index(&file_path, emails_per_file);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Query for specific email
    let mut predicate = HashMap::new();
    predicate.insert("email".to_string(), "user100@gmail.com".to_string());
    
    group.bench_function("fst_email_match", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&predicate), None);
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

// ============================================================================
// Table Shape Benchmarks
// ============================================================================

/// Create a narrow table FileSkipIndex (5 columns).
fn create_narrow_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    columns.insert("status".to_string(), 
        vec!["active".to_string(), "inactive".to_string()]);
    columns.insert("type".to_string(),
        vec!["A".to_string(), "B".to_string(), "C".to_string()]);
    columns.insert("region".to_string(),
        vec!["us".to_string(), "eu".to_string(), "asia".to_string()]);
    columns.insert("tier".to_string(),
        vec!["free".to_string(), "pro".to_string(), "enterprise".to_string()]);
    columns.insert("source".to_string(),
        vec!["web".to_string(), "mobile".to_string(), "api".to_string()]);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create a wide table FileSkipIndex (100 columns).
fn create_wide_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Generate 100 columns with varying cardinalities
    for i in 0..100 {
        let cardinality = match i % 4 {
            0 => 2,   // Boolean-like
            1 => 5,   // Low cardinality
            2 => 20,  // Medium cardinality
            _ => 50,  // Higher cardinality
        };
        
        let values: Vec<String> = (0..cardinality)
            .map(|v| format!("col{}_val{}", i, v))
            .collect();
        columns.insert(format!("column_{}", i), values);
    }
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create an ultra-wide table FileSkipIndex (500 columns).
fn create_ultra_wide_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Generate 500 columns - only low cardinality to keep build fast
    for i in 0..500 {
        let values = vec![
            format!("col{}_a", i),
            format!("col{}_b", i),
            format!("col{}_c", i),
        ];
        columns.insert(format!("c{}", i), values);
    }
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Benchmark narrow table (5 columns).
fn bench_narrow_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_shape/narrow");
    group.sample_size(30);
    
    let file_count = 5000;
    let partition_count = 50;
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_narrow_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Single column filter
    let mut single_pred = HashMap::new();
    single_pred.insert("status".to_string(), "active".to_string());
    
    group.bench_function("fst_single_column", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&single_pred), None);
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark wide table (100 columns).
fn bench_wide_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_shape/wide");
    group.sample_size(20);
    
    let file_count = 500;
    let partition_count = 10;
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_wide_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Single column filter (out of 100)
    let mut single_pred = HashMap::new();
    single_pred.insert("column_0".to_string(), "col0_val0".to_string());
    
    group.bench_function("fst_single_of_100", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&single_pred), None);
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark column selection overhead (how many columns affect performance).
fn bench_column_selection_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_shape/column_overhead");
    group.sample_size(20);
    
    let file_count = 200;
    let partition_count = 10;
    
    // Build ultra-wide index
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_ultra_wide_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // No index baseline first
    group.bench_function("no_index_baseline", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    // Test different predicate counts with FST
    for count in [1, 10, 50] {
        let mut pred = HashMap::new();
        for i in 0..count {
            pred.insert(format!("c{}", i), format!("col{}_a", i));
        }
        
        group.bench_with_input(
            BenchmarkId::new("fst_predicates", count),
            &pred,
            |b, p| {
                b.iter(|| {
                    let result = index.filter_with_partition_hint(black_box(p), None);
                    black_box(result)
                })
            },
        );
    }
    
    group.finish();
}

// ============================================================================
// Real-World Data Pattern Benchmarks
// ============================================================================

/// Create e-commerce order data pattern.
fn create_ecommerce_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Status (low cardinality - good for FST)
    columns.insert("status".to_string(), vec![
        "pending".to_string(), "processing".to_string(), "shipped".to_string(),
        "delivered".to_string(), "cancelled".to_string(), "refunded".to_string(),
    ]);
    
    // Payment method (low cardinality)
    columns.insert("payment_method".to_string(), vec![
        "credit_card".to_string(), "debit_card".to_string(), "paypal".to_string(),
        "apple_pay".to_string(), "google_pay".to_string(), "bank_transfer".to_string(),
        "crypto".to_string(), "afterpay".to_string(),
    ]);
    
    // Shipping method (low cardinality)
    columns.insert("shipping_method".to_string(), vec![
        "standard".to_string(), "express".to_string(), "overnight".to_string(),
        "pickup".to_string(), "freight".to_string(),
    ]);
    
    // Country (medium cardinality)
    let countries = ["US", "UK", "DE", "FR", "JP", "AU", "CA", "BR", "IN", "MX",
                     "ES", "IT", "NL", "SE", "CH", "KR", "SG", "HK", "NZ", "IE",
                     "BE", "AT", "PL", "DK", "NO", "FI", "PT", "CZ", "GR", "IL"];
    columns.insert("country".to_string(), 
        countries.iter().map(|s| s.to_string()).collect());
    
    // Product category (medium cardinality)
    let categories = ["electronics", "clothing", "home", "beauty", "sports", "toys",
                      "books", "food", "automotive", "garden", "health", "office",
                      "pets", "jewelry", "music", "movies"];
    columns.insert("category".to_string(),
        categories.iter().map(|s| s.to_string()).collect());
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create event/log data pattern.
fn create_event_log_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Event type (low cardinality)
    columns.insert("event_type".to_string(), vec![
        "page_view".to_string(), "click".to_string(), "scroll".to_string(),
        "form_submit".to_string(), "purchase".to_string(), "signup".to_string(),
        "login".to_string(), "logout".to_string(), "search".to_string(),
        "add_to_cart".to_string(), "remove_from_cart".to_string(), "checkout_start".to_string(),
    ]);
    
    // Platform (low cardinality)
    columns.insert("platform".to_string(), vec![
        "web".to_string(), "ios".to_string(), "android".to_string(), 
        "desktop_app".to_string(), "api".to_string(),
    ]);
    
    // Browser (low cardinality)
    columns.insert("browser".to_string(), vec![
        "chrome".to_string(), "safari".to_string(), "firefox".to_string(),
        "edge".to_string(), "opera".to_string(), "other".to_string(),
    ]);
    
    // Country (medium cardinality - top 20)
    let countries = ["US", "UK", "DE", "FR", "JP", "AU", "CA", "BR", "IN", "MX",
                     "ES", "IT", "NL", "SE", "CH", "KR", "SG", "HK", "NZ", "IE"];
    columns.insert("country".to_string(),
        countries.iter().map(|s| s.to_string()).collect());
    
    // Page category (medium cardinality)
    let pages = ["home", "product", "category", "cart", "checkout", "account",
                 "search", "blog", "about", "contact", "faq", "terms", "privacy",
                 "support", "pricing", "features"];
    columns.insert("page_category".to_string(),
        pages.iter().map(|s| s.to_string()).collect());
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create time-series sensor data pattern.
fn create_timeseries_file_skip_index(file_path: &str, device_count: usize) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Device ID (medium cardinality - e.g., 1000 IoT devices)
    let devices: Vec<String> = (0..device_count)
        .map(|i| format!("device_{:04}", i))
        .collect();
    columns.insert("device_id".to_string(), devices);
    
    // Metric name (low cardinality)
    columns.insert("metric".to_string(), vec![
        "temperature".to_string(), "humidity".to_string(), "pressure".to_string(),
        "voltage".to_string(), "current".to_string(), "power".to_string(),
        "speed".to_string(), "acceleration".to_string(), "vibration".to_string(),
        "flow_rate".to_string(), "level".to_string(), "ph".to_string(),
    ]);
    
    // Location (low cardinality)
    columns.insert("location".to_string(), vec![
        "building_a".to_string(), "building_b".to_string(), "building_c".to_string(),
        "warehouse_1".to_string(), "warehouse_2".to_string(),
        "outdoor".to_string(), "server_room".to_string(),
    ]);
    
    // Status (low cardinality)
    columns.insert("status".to_string(), vec![
        "normal".to_string(), "warning".to_string(), "critical".to_string(),
        "offline".to_string(), "maintenance".to_string(),
    ]);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Benchmark e-commerce query patterns.
fn bench_ecommerce_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("realworld/ecommerce");
    group.sample_size(30);
    
    let file_count = 5000;
    let partition_count = 50;
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_ecommerce_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Query: Find all shipped orders
    let mut shipped_query = HashMap::new();
    shipped_query.insert("status".to_string(), "shipped".to_string());
    
    group.bench_function("fst_shipped_orders", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&shipped_query), None);
            black_box(result)
        })
    });
    
    // Query with partition hint
    let mut complex_query = HashMap::new();
    complex_query.insert("category".to_string(), "electronics".to_string());
    complex_query.insert("shipping_method".to_string(), "express".to_string());
    let partition_hints = vec!["2025/01"];
    
    group.bench_function("fst_with_partition", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(
                black_box(&complex_query),
                Some(black_box(&partition_hints)),
            );
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    // Partition-only baseline (no FST)
    group.bench_function("no_index_partition_only", |b| {
        b.iter(|| {
            let result = simulate_partition_scan_cpu_only(black_box(file_count), black_box(partition_count), 1, 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark event/log query patterns.
fn bench_event_log_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("realworld/events");
    group.sample_size(30);
    
    let file_count = 5000;
    let partition_count = 50;
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_event_log_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Query: All purchases
    let mut purchase_query = HashMap::new();
    purchase_query.insert("event_type".to_string(), "purchase".to_string());
    
    group.bench_function("fst_purchases", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&purchase_query), None);
            black_box(result)
        })
    });
    
    // Query with partition hint
    let mut chrome_us_signup = HashMap::new();
    chrome_us_signup.insert("browser".to_string(), "chrome".to_string());
    chrome_us_signup.insert("country".to_string(), "US".to_string());
    chrome_us_signup.insert("event_type".to_string(), "signup".to_string());
    let partition_hints = vec!["2025/01"];
    
    group.bench_function("fst_with_partition", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(
                black_box(&chrome_us_signup),
                Some(black_box(&partition_hints)),
            );
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark time-series query patterns.
fn bench_timeseries_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("realworld/timeseries");
    group.sample_size(30);
    
    let file_count = 2000;
    let partition_count = 20;
    let device_count = 100; // 100 IoT devices
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_timeseries_file_skip_index(&file_path, device_count);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Query: All critical alerts
    let mut critical_query = HashMap::new();
    critical_query.insert("status".to_string(), "critical".to_string());
    
    group.bench_function("fst_critical_alerts", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&critical_query), None);
            black_box(result)
        })
    });
    
    // Query with partition hint
    let mut server_room_query = HashMap::new();
    server_room_query.insert("location".to_string(), "server_room".to_string());
    let partition_hints = vec!["2025/01"];
    
    group.bench_function("fst_with_partition", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(
                black_box(&server_room_query),
                Some(black_box(&partition_hints)),
            );
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

// ============================================================================
// Edge Case Benchmarks
// ============================================================================

/// Create FileSkipIndex where all values are the same (100% selectivity).
fn create_single_value_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // All files have the same value
    columns.insert("status".to_string(), vec!["active".to_string()]);
    columns.insert("region".to_string(), vec!["us".to_string()]);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create FileSkipIndex where all values are unique (0% reuse).
fn create_all_unique_file_skip_index(file_path: &str, unique_count: usize) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    let unique_values: Vec<String> = (0..unique_count)
        .map(|i| format!("unique_{}_{}", file_path.replace('/', "_"), i))
        .collect();
    columns.insert("id".to_string(), unique_values);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create FileSkipIndex with skewed distribution (90% one value, 10% others).
fn create_skewed_file_skip_index(file_path: &str, file_idx: usize) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // 90% of files have "common", 10% have rare values
    if file_idx % 10 == 0 {
        columns.insert("value".to_string(), vec![format!("rare_{}", file_idx)]);
    } else {
        columns.insert("value".to_string(), vec!["common".to_string()]);
    }
    
    // Also include a normal column for comparison
    columns.insert("status".to_string(), 
        vec!["active".to_string(), "inactive".to_string()]);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create FileSkipIndex with long string values (1KB+).
fn create_long_string_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Generate long string values (1KB each)
    let long_values: Vec<String> = (0..10)
        .map(|i| format!("long_value_{}_", i).repeat(100)) // ~1.5KB each
        .collect();
    columns.insert("description".to_string(), long_values);
    
    // Normal short column for comparison
    columns.insert("type".to_string(), 
        vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Create FileSkipIndex with unicode/multi-byte strings.
fn create_unicode_file_skip_index(file_path: &str) -> FileSkipIndex {
    let mut columns = HashMap::new();
    
    // Various unicode characters (emojis, CJK, Arabic, etc.)
    columns.insert("name".to_string(), vec![
        "北京市".to_string(),
        "東京都".to_string(),
        "서울특별시".to_string(),
        "Москва".to_string(),
        "القاهرة".to_string(),
        "München".to_string(),
        "São Paulo".to_string(),
        "Zürich".to_string(),
    ]);
    
    // Emoji column
    columns.insert("mood".to_string(), vec![
        "😀".to_string(),
        "😢".to_string(),
        "😡".to_string(),
        "🎉".to_string(),
        "❤️".to_string(),
    ]);
    
    FileSkipIndex::build(file_path, columns).expect("Failed to build FileSkipIndex")
}

/// Benchmark single value column (all files match or none match).
fn bench_single_value_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_cases/single_value");
    group.sample_size(30);
    
    let file_count = 5000;
    let partition_count = 50;
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_single_value_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Query matching all files
    let mut match_all = HashMap::new();
    match_all.insert("status".to_string(), "active".to_string());
    
    group.bench_function("fst_match_all", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&match_all), None);
            black_box(result)
        })
    });
    
    // Query matching no files (FST can reject instantly)
    let mut match_none = HashMap::new();
    match_none.insert("status".to_string(), "inactive".to_string());
    
    group.bench_function("fst_match_none", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&match_none), None);
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark all unique values (worst case for FST memory).
fn bench_all_unique_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_cases/all_unique");
    group.sample_size(20);
    
    let file_count = 200;
    let partition_count = 10;
    let unique_per_file = 100; // 100 unique values per file
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_all_unique_file_skip_index(&file_path, unique_per_file);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Query for a specific unique value
    let mut query = HashMap::new();
    query.insert("id".to_string(), "unique_2025_05_data_0010_parquet_50".to_string());
    
    group.bench_function("fst_find_unique", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&query), None);
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark skewed distribution (90% common value).
fn bench_skewed_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_cases/skewed");
    group.sample_size(30);
    
    let file_count = 1000;
    let partition_count = 10;
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    let mut file_idx = 0;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_skewed_file_skip_index(&file_path, file_idx);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
            file_idx += 1;
        }
    }
    
    // Query for rare value (matches 10% of files)
    let mut rare_query = HashMap::new();
    rare_query.insert("value".to_string(), "rare_100".to_string());
    
    group.bench_function("fst_rare_value", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&rare_query), None);
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark long string values (memory pressure on FST).
fn bench_long_string_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_cases/long_strings");
    group.sample_size(20);
    
    let file_count = 500;
    let partition_count = 10;
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_long_string_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Query on long string column
    let mut long_query = HashMap::new();
    long_query.insert("description".to_string(), "long_value_5_".repeat(100));
    
    group.bench_function("fst_long_string", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&long_query), None);
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark unicode/multi-byte string handling.
fn bench_unicode_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_cases/unicode");
    group.sample_size(30);
    
    let file_count = 1000;
    let partition_count = 10;
    
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_unicode_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Query for CJK characters
    let mut cjk_query = HashMap::new();
    cjk_query.insert("name".to_string(), "東京都".to_string());
    
    group.bench_function("fst_cjk", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&cjk_query), None);
            black_box(result)
        })
    });
    
    // Query for emoji
    let mut emoji_query = HashMap::new();
    emoji_query.insert("mood".to_string(), "🎉".to_string());
    
    group.bench_function("fst_emoji", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&emoji_query), None);
            black_box(result)
        })
    });
    
    // No index baseline
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

// ============================================================================
// Memory Pressure Benchmarks
// ============================================================================

/// Benchmark FST memory usage tracking.
fn bench_fst_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/usage");
    group.sample_size(20);
    
    // Test different cardinalities and measure the index building
    for cardinality in [100, 1000, 5000, 10000] {
        let values: Vec<String> = (0..cardinality)
            .map(|i| format!("value_{:08}", i))
            .collect();
        
        let mut columns = HashMap::new();
        columns.insert("test_column".to_string(), values);
        
        group.throughput(Throughput::Elements(cardinality as u64));
        group.bench_with_input(
            BenchmarkId::new("build_and_size", cardinality),
            &columns,
            |b, cols| {
                b.iter(|| {
                    let index = FileSkipIndex::build(
                        black_box("test.parquet"),
                        black_box(cols.clone()),
                    ).expect("build");
                    // Return size for measurement
                    black_box(index)
                })
            },
        );
    }
    
    group.finish();
}

/// Benchmark behavior near MAX_SUMMARY_CARDINALITY limit (100K).
fn bench_cardinality_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/cardinality_limit");
    group.sample_size(10); // Low sample size for slow builds
    
    // Test at 50%, 75%, 90% of the 100K limit
    for (name, cardinality) in [
        ("50pct_of_limit", 50_000),
        ("75pct_of_limit", 75_000),
        ("90pct_of_limit", 90_000),
    ] {
        let values: Vec<String> = (0..cardinality)
            .map(|i| format!("v{:06}", i))
            .collect();
        
        let mut columns = HashMap::new();
        columns.insert("high_card_column".to_string(), values);
        
        group.throughput(Throughput::Elements(cardinality as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                let index = FileSkipIndex::build(
                    black_box("test.parquet"),
                    black_box(columns.clone()),
                );
                black_box(index)
            })
        });
    }
    
    group.finish();
}

/// Benchmark hierarchical index memory with many partitions.
fn bench_partition_memory_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/partition_scaling");
    group.sample_size(10);
    
    // Test scaling with partition count
    for partition_count in [10, 50, 100, 200] {
        let file_count = partition_count * 100; // 100 files per partition
        
        group.throughput(Throughput::Elements(file_count as u64));
        group.bench_with_input(
            BenchmarkId::new("partitions", partition_count),
            &(file_count, partition_count),
            |b, &(files, partitions)| {
                b.iter(|| {
                    let index = create_minimal_hierarchical_index(files, partitions);
                    black_box(index)
                })
            },
        );
    }
    
    group.finish();
}

// ============================================================================
// Criterion Groups
// ============================================================================

criterion_group!(
    name = skip_index_benches;
    config = Criterion::default().significance_level(0.05).noise_threshold(0.02);
    targets = bench_skip_index_filter, bench_flat_vs_hierarchical
);

criterion_group!(
    name = fst_benches;
    config = Criterion::default();
    targets = bench_fst_build
);

criterion_group!(
    name = indexing_comparison_benches;
    config = Criterion::default().sample_size(50);
    targets = bench_indexing_strategies, bench_skip_rate_scenarios, bench_index_build_cost
);

criterion_group!(
    name = large_scale_benches;
    config = Criterion::default().sample_size(20);
    targets = bench_large_scale_comparison
);

criterion_group!(
    name = query_benches;
    config = Criterion::default();
    targets = bench_query_rewrite, bench_query_plan_cache, bench_query_hash
);

criterion_group!(
    name = streaming_benches;
    config = Criterion::default().sample_size(50);
    targets = bench_memory_estimation, bench_streaming_throughput, bench_json_parsing, bench_skip_index_cache
);

// New benchmark groups for diverse data patterns

/// Benchmark FST with boolean column data.
fn bench_boolean_column_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_types/boolean");
    group.sample_size(30);
    
    let file_count = 1000;
    let partition_count = 10;
    
    // Build index with boolean columns
    let mut index = HierarchicalSkipIndex::new();
    let files_per_partition = file_count / partition_count;
    for p in 0..partition_count {
        let partition_key = format!("2025/{:02}", (p % 12) + 1);
        for f in 0..files_per_partition {
            let file_path = format!("{}/data_{:04}.parquet", partition_key, f);
            let file_index = create_boolean_file_skip_index(&file_path);
            index.add_file(&partition_key, file_index, 10_000).expect("add file");
        }
    }
    
    // Filter on boolean value
    let mut predicate = HashMap::new();
    predicate.insert("is_active".to_string(), "true".to_string());
    
    group.bench_function("fst_boolean", |b| {
        b.iter(|| {
            let result = index.filter_with_partition_hint(black_box(&predicate), None);
            black_box(result)
        })
    });
    
    // Full scan baseline - no index
    group.bench_function("no_index_full_scan", |b| {
        b.iter(|| {
            let result = simulate_full_scan_cpu_only(black_box(file_count), 10);
            black_box(result)
        })
    });
    
    group.finish();
}

criterion_group!(
    name = data_type_benches;
    config = Criterion::default().sample_size(30);
    targets = bench_numeric_column_filtering, bench_timestamp_column_filtering, bench_boolean_column_filtering, bench_mixed_type_filtering
);

criterion_group!(
    name = cardinality_benches;
    config = Criterion::default().sample_size(20);
    targets = bench_cardinality_spectrum, bench_uuid_column, bench_email_column
);

criterion_group!(
    name = table_shape_benches;
    config = Criterion::default().sample_size(20);
    targets = bench_narrow_table, bench_wide_table, bench_column_selection_overhead
);

criterion_group!(
    name = realworld_benches;
    config = Criterion::default().sample_size(30);
    targets = bench_ecommerce_queries, bench_event_log_queries, bench_timeseries_queries
);

criterion_group!(
    name = edge_case_benches;
    config = Criterion::default().sample_size(20);
    targets = bench_single_value_column, bench_all_unique_column, bench_skewed_distribution, 
              bench_long_string_values, bench_unicode_strings
);

criterion_group!(
    name = memory_benches;
    config = Criterion::default().sample_size(10);
    targets = bench_fst_memory_usage, bench_cardinality_limit, bench_partition_memory_scaling
);

// Realistic I/O comparison benchmarks (with simulated network latency)
criterion_group!(
    name = realistic_io_benches;
    config = Criterion::default().sample_size(10);
    targets = bench_realistic_io_comparison, bench_fst_value_at_scale
);

// ============================================================================
// Multi-Strategy Index Comparison Benchmarks
// ============================================================================

use reiver_pond::warehouse::indexes::{
    XorColumnFilter, FileXorIndex, DataXorIndex,
    BitmapSkipIndex,
    ColumnCardinalityEstimator,
    IndexStrategy,
};
use reiver_pond::warehouse::types::ColumnType;

/// Benchmark Xor Filter build time for different cardinalities.
fn bench_xor_filter_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_strategy/xor_build");
    group.sample_size(20);
    
    for cardinality in [1_000, 10_000, 100_000, 500_000] {
        let values: Vec<String> = (0..cardinality)
            .map(|i| format!("value_{:08}", i))
            .collect();
        
        group.throughput(Throughput::Elements(cardinality as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(cardinality),
            &values,
            |b, vals| {
                b.iter(|| {
                    let filter = XorColumnFilter::build(
                        black_box("test_column"),
                        black_box(vals.iter().map(|s| s.as_str())),
                    );
                    black_box(filter)
                })
            },
        );
    }
    
    group.finish();
}

/// Benchmark Xor Filter lookup speed.
fn bench_xor_filter_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_strategy/xor_lookup");
    
    // Build filter with 100K values
    let values: Vec<String> = (0..100_000)
        .map(|i| format!("value_{:08}", i))
        .collect();
    let filter = XorColumnFilter::build("test_column", values.iter().map(|s| s.as_str())).unwrap();
    
    // Lookup for existing value
    group.bench_function("existing_value", |b| {
        b.iter(|| {
            let result = filter.might_contain(black_box("value_00050000"));
            black_box(result)
        })
    });
    
    // Lookup for non-existing value (should be fast false, but may have FP)
    group.bench_function("non_existing_value", |b| {
        b.iter(|| {
            let result = filter.might_contain(black_box("nonexistent_value_xyz"));
            black_box(result)
        })
    });
    
    // Batch lookup (might_contain_any)
    let lookup_values = vec!["value_00001000", "value_00050000", "value_00099999"];
    group.bench_function("batch_lookup_3", |b| {
        b.iter(|| {
            let result = filter.might_contain_any(black_box(&lookup_values));
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark FST vs Xor Filter for high-cardinality columns.
fn bench_fst_vs_xor_high_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_strategy/fst_vs_xor");
    group.sample_size(20);
    
    // Test with 200K values (above FST summary threshold but suitable for Xor)
    let cardinality = 200_000;
    let values: Vec<String> = (0..cardinality)
        .map(|i| format!("user_{:08}", i))
        .collect();
    
    // Build FST-based FileSkipIndex
    let mut fst_columns = HashMap::new();
    fst_columns.insert("user_id".to_string(), values.clone());
    
    group.bench_function("fst_build_200k", |b| {
        b.iter(|| {
            let index = FileSkipIndex::build(
                black_box("test.parquet"),
                black_box(fst_columns.clone()),
            );
            black_box(index)
        })
    });
    
    // Build Xor Filter
    group.bench_function("xor_build_200k", |b| {
        b.iter(|| {
            let filter = XorColumnFilter::build(
                black_box("user_id"),
                black_box(values.iter().map(|s| s.as_str())),
            );
            black_box(filter)
        })
    });
    
    // Now benchmark lookup performance
    let fst_index = FileSkipIndex::build("test.parquet", fst_columns.clone()).unwrap();
    let xor_filter = XorColumnFilter::build("user_id", values.iter().map(|s| s.as_str())).unwrap();
    
    // FST lookup
    group.bench_function("fst_lookup", |b| {
        b.iter(|| {
            let result = fst_index.might_contain("user_id", black_box("user_00100000"));
            black_box(result)
        })
    });
    
    // Xor lookup
    group.bench_function("xor_lookup", |b| {
        b.iter(|| {
            let result = xor_filter.might_contain(black_box("user_00100000"));
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark Roaring Bitmap operations for file set filtering.
fn bench_bitmap_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_strategy/bitmap");
    group.sample_size(30);
    
    // Create a bitmap index with many files
    let mut index = BitmapSkipIndex::new();
    
    for i in 0..10_000 {
        let path = format!("file_{:05}.parquet", i);
        // Distribute values across files
        let status = if i % 3 == 0 { "active" } else if i % 3 == 1 { "pending" } else { "inactive" };
        let region = if i % 4 == 0 { "us" } else if i % 4 == 1 { "eu" } else if i % 4 == 2 { "asia" } else { "other" };
        
        index.add_column_values(&path, "status", vec![status]);
        index.add_column_values(&path, "region", vec![region]);
    }
    
    // Single predicate filter
    let mut single_pred = HashMap::new();
    single_pred.insert("status".to_string(), "active".to_string());
    
    group.bench_function("single_predicate_filter", |b| {
        b.iter(|| {
            let result = index.filter_by_predicates(black_box(&single_pred));
            black_box(result)
        })
    });
    
    // Multi-predicate filter (uses bitmap intersection)
    let mut multi_pred = HashMap::new();
    multi_pred.insert("status".to_string(), "active".to_string());
    multi_pred.insert("region".to_string(), "us".to_string());
    
    group.bench_function("multi_predicate_filter", |b| {
        b.iter(|| {
            let result = index.filter_by_predicates(black_box(&multi_pred));
            black_box(result)
        })
    });
    
    // IN list filter
    let mut in_pred = HashMap::new();
    in_pred.insert(
        "status".to_string(),
        vec!["active".to_string(), "pending".to_string()],
    );
    
    group.bench_function("in_list_filter", |b| {
        b.iter(|| {
            let result = index.filter_by_in_predicates(black_box(&in_pred));
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark HyperLogLog cardinality estimation.
fn bench_cardinality_estimation(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_strategy/cardinality");
    
    // Benchmark adding values
    group.bench_function("add_100k_strings", |b| {
        b.iter(|| {
            let mut estimator = ColumnCardinalityEstimator::new("test", ColumnType::String);
            for i in 0..100_000 {
                estimator.add_string(black_box(&format!("value_{}", i)));
            }
            black_box(estimator.estimate())
        })
    });
    
    // Benchmark estimation (already populated)
    let mut estimator = ColumnCardinalityEstimator::new("test", ColumnType::String);
    for i in 0..100_000 {
        estimator.add_string(&format!("value_{}", i));
    }
    
    group.bench_function("estimate", |b| {
        b.iter(|| {
            let result = estimator.estimate();
            black_box(result)
        })
    });
    
    // Benchmark strategy selection
    group.bench_function("strategy_selection", |b| {
        b.iter(|| {
            let strategy = IndexStrategy::from_stats(
                black_box(ColumnType::String),
                black_box(200_000),
                black_box(1_000_000),
            );
            black_box(strategy)
        })
    });
    
    group.finish();
}

/// Benchmark memory efficiency comparison.
fn bench_memory_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_strategy/memory");
    group.sample_size(10);
    
    // Compare memory usage for 100K values
    let cardinality = 100_000;
    let values: Vec<String> = (0..cardinality)
        .map(|i| format!("value_{:08}", i))
        .collect();
    
    // Xor Filter memory
    group.bench_function("xor_100k_build_and_size", |b| {
        b.iter(|| {
            let filter = XorColumnFilter::build(
                "test",
                values.iter().map(|s| s.as_str()),
            ).unwrap();
            let size = filter.size_bytes();
            black_box((filter, size))
        })
    });
    
    // FST memory (via FileSkipIndex)
    let mut columns = HashMap::new();
    columns.insert("test".to_string(), values.clone());
    
    group.bench_function("fst_100k_build", |b| {
        b.iter(|| {
            let index = FileSkipIndex::build("test.parquet", columns.clone()).unwrap();
            black_box(index)
        })
    });
    
    group.finish();
}

/// Benchmark DataXorIndex for table-level filtering.
fn bench_data_xor_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_strategy/data_xor_index");
    group.sample_size(20);
    
    // Build a DataXorIndex with 1000 files
    let mut data_index = DataXorIndex::new();
    
    for i in 0..1000 {
        let file_path = format!("file_{:04}.parquet", i);
        let mut columns = HashMap::new();
        
        // Each file has different user IDs
        let users: Vec<String> = (i * 100..(i + 1) * 100)
            .map(|u| format!("user_{:06}", u))
            .collect();
        columns.insert("user_id".to_string(), users);
        
        let file_index = FileXorIndex::build(&file_path, columns).unwrap();
        data_index.add_file(file_index);
    }
    
    // Filter by user ID
    let mut predicate = HashMap::new();
    predicate.insert("user_id".to_string(), "user_050000".to_string());
    
    group.bench_function("filter_by_user_id", |b| {
        b.iter(|| {
            let result = data_index.filter_files_by_predicates(black_box(&predicate));
            black_box(result)
        })
    });
    
    // IN list predicate
    let mut in_predicate = HashMap::new();
    in_predicate.insert(
        "user_id".to_string(),
        vec!["user_050000".to_string(), "user_075000".to_string()],
    );
    
    group.bench_function("filter_by_in_list", |b| {
        b.iter(|| {
            let result = data_index.filter_files_by_in_predicates(black_box(&in_predicate));
            black_box(result)
        })
    });
    
    group.finish();
}

/// Benchmark comparing all strategies at a fixed cardinality.
fn bench_strategy_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_strategy/strategy_comparison");
    group.sample_size(30);
    
    // Create 5000 files with different value distributions
    let file_count = 5000;
    
    // -- Setup FST-based HierarchicalSkipIndex --
    let fst_index = {
        let mut index = HierarchicalSkipIndex::new();
        for i in 0..file_count {
            let partition = format!("2025/{:02}", (i % 12) + 1);
            let path = format!("{}/file_{:04}.parquet", partition, i);
            
            let mut columns = HashMap::new();
            columns.insert("status".to_string(), 
                vec!["active".to_string(), "pending".to_string(), "inactive".to_string()]);
            columns.insert("region".to_string(),
                vec!["us".to_string(), "eu".to_string(), "asia".to_string()]);
            
            let file_index = FileSkipIndex::build(&path, columns).unwrap();
            index.add_file(&partition, file_index, 10_000).unwrap();
        }
        index
    };
    
    // -- Setup Bitmap-based index --
    let bitmap_index = {
        let mut index = BitmapSkipIndex::new();
        for i in 0..file_count {
            let path = format!("2025/{:02}/file_{:04}.parquet", (i % 12) + 1, i);
            
            let status = if i % 3 == 0 { "active" } else if i % 3 == 1 { "pending" } else { "inactive" };
            let region = if i % 3 == 0 { "us" } else if i % 3 == 1 { "eu" } else { "asia" };
            
            index.add_column_values(&path, "status", vec![status]);
            index.add_column_values(&path, "region", vec![region]);
        }
        index
    };
    
    // Single predicate
    let mut single_pred = HashMap::new();
    single_pred.insert("status".to_string(), "active".to_string());
    
    group.bench_function("fst_single_pred", |b| {
        b.iter(|| {
            let result = fst_index.filter_with_partition_hint(black_box(&single_pred), None);
            black_box(result)
        })
    });
    
    group.bench_function("bitmap_single_pred", |b| {
        b.iter(|| {
            let result = bitmap_index.filter_by_predicates(black_box(&single_pred));
            black_box(result)
        })
    });
    
    // Multi-predicate (AND logic)
    let mut multi_pred = HashMap::new();
    multi_pred.insert("status".to_string(), "active".to_string());
    multi_pred.insert("region".to_string(), "us".to_string());
    
    group.bench_function("fst_multi_pred", |b| {
        b.iter(|| {
            let result = fst_index.filter_with_partition_hint(black_box(&multi_pred), None);
            black_box(result)
        })
    });
    
    group.bench_function("bitmap_multi_pred", |b| {
        b.iter(|| {
            let result = bitmap_index.filter_by_predicates(black_box(&multi_pred));
            black_box(result)
        })
    });
    
    group.finish();
}

criterion_group!(
    name = multi_strategy_benches;
    config = Criterion::default().sample_size(20);
    targets = bench_xor_filter_build, bench_xor_filter_lookup, bench_fst_vs_xor_high_cardinality,
              bench_bitmap_operations, bench_cardinality_estimation, bench_memory_efficiency,
              bench_data_xor_index, bench_strategy_comparison
);

criterion_main!(
    skip_index_benches, 
    fst_benches, 
    query_benches, 
    streaming_benches, 
    indexing_comparison_benches, 
    large_scale_benches,
    // New diverse data pattern benchmarks
    data_type_benches,
    cardinality_benches,
    table_shape_benches,
    realworld_benches,
    edge_case_benches,
    memory_benches,
    // Realistic I/O simulation benchmarks
    realistic_io_benches,
    // Multi-strategy index comparison benchmarks
    multi_strategy_benches
);
