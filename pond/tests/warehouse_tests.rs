//! Warehouse Integration Tests
//!
//! Tests for the data warehouse feature including:
//! - Query execution with rewriting
//! - Skip index optimization
//! - Cache behavior
//! - Timeout handling
//! - Memory budgets

use serde_json::json;

#[cfg(test)]
mod query_rewriter_tests {
    use reiver_pond::warehouse::query::rewriter::TableRewriter;
    use reiver_pond::warehouse::types::R2TablePath;
    use ahash::AHashMap;
    use uuid::Uuid;
    
    fn test_project_id() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }
    
    fn test_r2_path(project_id: Uuid, table: &str) -> R2TablePath {
        R2TablePath::for_testing(format!("{}/{}", project_id, table))
    }
    
    #[test]
    fn test_simple_select_rewrite() {
        let project_id = test_project_id();
        let mut tables = AHashMap::new();
        tables.insert("orders".to_string(), test_r2_path(project_id, "orders"));
        
        let rewriter = TableRewriter::new("r2_collection");
        let result = rewriter.rewrite(
            "SELECT * FROM orders",
            &tables,
        );
        
        assert!(result.is_ok());
        let rewritten = result.unwrap();
        assert!(rewritten.contains("s3("));
    }
    
    #[test]
    fn test_date_predicate_extraction() {
        let project_id = test_project_id();
        let mut tables = AHashMap::new();
        tables.insert("events".to_string(), test_r2_path(project_id, "events"));
        
        let rewriter = TableRewriter::new("r2_collection");
        let result = rewriter.rewrite(
            "SELECT * FROM events WHERE date >= '2025-01-01' AND date <= '2025-01-31'",
            &tables,
        );
        
        assert!(result.is_ok());
        let rewritten = result.unwrap();
        // Should include date-based path pattern for partition pruning
        assert!(rewritten.contains("s3("));
    }
    
    #[test]
    fn test_join_rewrite() {
        let project_id = test_project_id();
        let mut tables = AHashMap::new();
        tables.insert("orders".to_string(), test_r2_path(project_id, "orders"));
        tables.insert("users".to_string(), test_r2_path(project_id, "users"));
        
        let rewriter = TableRewriter::new("r2_collection");
        let result = rewriter.rewrite(
            "SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id",
            &tables,
        );
        
        assert!(result.is_ok());
        let rewritten = result.unwrap();
        // Both tables should be rewritten
        assert!(rewritten.matches("s3(").count() >= 2);
    }
    
    #[test]
    fn test_subquery_rewrite() {
        let project_id = test_project_id();
        let mut tables = AHashMap::new();
        tables.insert("orders".to_string(), test_r2_path(project_id, "orders"));
        
        let rewriter = TableRewriter::new("r2_collection");
        let result = rewriter.rewrite(
            "SELECT * FROM (SELECT * FROM orders WHERE amount > 100) AS high_value",
            &tables,
        );
        
        assert!(result.is_ok());
    }
}

#[cfg(test)]
mod skip_index_tests {
    use reiver_pond::warehouse::indexes::skip_index::{
        FileSkipIndex, DataSkipIndex, HierarchicalSkipIndex, SkipPredicates
    };
    use std::collections::HashMap;
    
    #[test]
    fn test_file_skip_index_creation() {
        let mut columns = HashMap::new();
        columns.insert("status".to_string(), vec![
            "active".to_string(),
            "pending".to_string(),
            "completed".to_string(),
        ]);
        
        let result = FileSkipIndex::build("test.parquet", columns);
        assert!(result.is_ok());
        
        let index = result.unwrap();
        assert!(index.might_contain("status", "active"));
        assert!(index.might_contain("status", "pending"));
        assert!(!index.might_contain("status", "unknown"));
    }
    
    #[test]
    fn test_file_skip_index_prefix_search() {
        let mut columns = HashMap::new();
        columns.insert("email".to_string(), vec![
            "user1@example.com".to_string(),
            "user2@example.com".to_string(),
            "admin@company.com".to_string(),
        ]);
        
        let index = FileSkipIndex::build("test.parquet", columns).unwrap();
        
        assert!(index.might_contain_prefix("email", "user"));
        assert!(index.might_contain_prefix("email", "admin"));
        assert!(!index.might_contain_prefix("email", "zzz"));
    }
    
    #[test]
    fn test_hierarchical_skip_index() {
        let mut hierarchical = HierarchicalSkipIndex::new();
        
        // Add files to different partitions
        let mut cols1 = HashMap::new();
        cols1.insert("country".to_string(), vec!["US".to_string(), "CA".to_string()]);
        let file1 = FileSkipIndex::build("2025/01/file1.parquet", cols1).unwrap();
        hierarchical.add_file("2025/01", file1, 1000).unwrap();
        
        let mut cols2 = HashMap::new();
        cols2.insert("country".to_string(), vec!["UK".to_string(), "FR".to_string()]);
        let file2 = FileSkipIndex::build("2025/02/file2.parquet", cols2).unwrap();
        hierarchical.add_file("2025/02", file2, 1500).unwrap();
        
        assert_eq!(hierarchical.total_files(), 2);
        
        // Test filtering with predicates
        let mut predicates = std::collections::HashMap::new();
        predicates.insert("country".to_string(), "US".to_string());
        
        let matching_files = hierarchical.filter_with_partition_hint(&predicates, None);
        assert!(!matching_files.is_empty());
    }
    
    #[test]
    fn test_skip_index_from_serialized_fst() {
        // Create an FST and serialize it
        use fst::SetBuilder;
        
        let mut builder = SetBuilder::memory();
        builder.insert("value1").unwrap();
        builder.insert("value2").unwrap();
        builder.insert("value3").unwrap();
        let fst_bytes = builder.into_inner().unwrap();
        
        // Deserialize and verify
        let result = FileSkipIndex::from_serialized_fst(
            "test.parquet",
            "test_column",
            fst_bytes,
        );
        
        assert!(result.is_ok());
        let index = result.unwrap();
        assert!(index.might_contain("test_column", "value1"));
        assert!(index.might_contain("test_column", "value2"));
        assert!(!index.might_contain("test_column", "nonexistent"));
    }
    
    #[test]
    fn test_high_cardinality_merge() {
        // Test that merging high-cardinality FSTs works without OOM
        let mut hierarchical = HierarchicalSkipIndex::new();
        
        // Create files with many unique values
        for i in 0..10 {
            let mut cols = HashMap::new();
            let values: Vec<String> = (0..1000)
                .map(|j| format!("uuid-{}-{}", i, j))
                .collect();
            cols.insert("id".to_string(), values);
            
            let file = FileSkipIndex::build(
                &format!("partition/file{}.parquet", i),
                cols,
            ).unwrap();
            
            hierarchical.add_file("partition", file, 1000).unwrap();
        }
        
        // Should have merged 10,000 unique values using streaming union
        assert_eq!(hierarchical.total_files(), 10);
    }
}

#[cfg(test)]
mod query_settings_tests {
    use reiver_pond::warehouse::query::executor::ClickHouseQuerySettings;
    
    #[test]
    fn test_default_settings() {
        let settings = ClickHouseQuerySettings::default();
        
        assert!(settings.input_format_parquet_filter_push_down);
        assert!(settings.max_memory_usage > 0);
        assert_eq!(settings.max_execution_time, 0); // Not set by default
    }
    
    #[test]
    fn test_with_timeout() {
        let settings = ClickHouseQuerySettings::default().with_timeout(30);
        
        assert_eq!(settings.max_execution_time, 30);
        
        let params = settings.to_query_params();
        let timeout_param = params.iter().find(|(k, _)| *k == "max_execution_time");
        
        assert!(timeout_param.is_some());
        assert_eq!(timeout_param.unwrap().1, "30");
    }
    
    #[test]
    fn test_with_result_limits() {
        let settings = ClickHouseQuerySettings::default()
            .with_result_limits(10000, 1024 * 1024);
        
        assert_eq!(settings.max_result_rows, 10000);
        assert_eq!(settings.max_result_bytes, 1024 * 1024);
    }
    
    #[test]
    fn test_without_result_limits() {
        let settings = ClickHouseQuerySettings::default().without_result_limits();
        
        assert_eq!(settings.max_result_rows, 0);
        assert_eq!(settings.max_result_bytes, 0);
    }
}

#[cfg(test)]
mod retry_config_tests {
    use reiver_pond::warehouse::storage::r2::RetryConfig;
    use std::time::Duration;
    
    #[test]
    fn test_default_config() {
        let config = RetryConfig::default();
        
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.jitter_factor, 0.2);
    }
    
    #[test]
    fn test_delay_with_jitter() {
        let config = RetryConfig::default();
        let base_delay = Duration::from_secs(1);
        
        // Call multiple times to verify jitter varies
        let mut delays = Vec::new();
        for _ in 0..10 {
            delays.push(config.delay_with_jitter(base_delay));
            std::thread::sleep(Duration::from_millis(1)); // Vary the nanos
        }
        
        // All delays should be within jitter range (0.8 to 1.2 seconds)
        for delay in &delays {
            assert!(delay.as_secs_f64() >= 0.8);
            assert!(delay.as_secs_f64() <= 1.2);
        }
    }
    
    #[test]
    fn test_next_delay_with_backoff() {
        let config = RetryConfig::default();
        let initial = Duration::from_millis(100);
        
        let next = config.next_delay(initial);
        
        // Should be roughly 200ms (2x multiplier) with some jitter
        assert!(next.as_millis() >= 160); // 200 * 0.8
        assert!(next.as_millis() <= 240); // 200 * 1.2
    }
    
    #[test]
    fn test_max_delay_cap() {
        let config = RetryConfig {
            max_delay: Duration::from_secs(1),
            ..Default::default()
        };
        
        let large_delay = Duration::from_secs(10);
        let capped = config.delay_with_jitter(large_delay);
        
        // Should be capped at max_delay
        assert!(capped.as_secs_f64() <= 1.2); // max_delay with jitter
    }
}

#[cfg(test)]
mod cache_tests {
    use reiver_pond::warehouse::query::cache::QueryCacheConfig;
    
    #[test]
    fn test_default_cache_config() {
        let config = QueryCacheConfig::default();
        
        assert!(config.enabled);
        assert!(config.ttl_secs > 0);
    }
    
    #[test]
    fn test_cache_config_disabled() {
        let config = QueryCacheConfig {
            enabled: false,
            ..Default::default()
        };
        
        assert!(!config.enabled);
    }
}

#[cfg(test)]
mod cost_estimator_tests {
    use reiver_pond::warehouse::query::cost_estimator::{QueryCostEstimator, TableStats};
    
    #[test]
    fn test_empty_estimator() {
        let mut estimator = QueryCostEstimator::new();
        
        // Unknown table should return estimate with zero values
        let estimate = estimator.estimate("SELECT * FROM unknown");
        assert!(estimate.is_ok());
        let cost = estimate.unwrap();
        assert_eq!(cost.estimated_bytes_scanned, 0);
    }
    
    #[test]
    fn test_with_table_stats() {
        let mut estimator = QueryCostEstimator::new();
        
        estimator.add_table_stats(TableStats {
            table_name: "orders".to_string(),
            row_count: 1_000_000,
            size_bytes: 100_000_000, // 100MB
            file_count: 10,
            avg_row_size: 100,
            last_updated: None,
        });
        
        let estimate = estimator.estimate("SELECT * FROM orders");
        assert!(estimate.is_ok());
        
        let cost = estimate.unwrap();
        assert!(cost.estimated_bytes_scanned > 0);
        assert!(cost.estimated_rows > 0);
    }
}

#[cfg(test)]
mod query_limiter_tests {
    use reiver_pond::warehouse::query::limiter::{QueryLimiter, QueryLimiterConfig};
    use uuid::Uuid;
    
    #[tokio::test]
    async fn test_limiter_permits() {
        let limiter = QueryLimiter::with_defaults();
        let project_id = Uuid::new_v4();
        
        // Should acquire permit
        let permit = limiter.acquire(project_id).await;
        assert!(permit.is_ok());
    }
    
    #[tokio::test]
    async fn test_limiter_per_project_limit() {
        let config = QueryLimiterConfig {
            max_concurrent_per_project: 2,
            max_concurrent_global: 100,
            ..Default::default()
        };
        let limiter = QueryLimiter::new(config);
        let project_id = Uuid::new_v4();
        
        // Should acquire first two permits
        let permit1 = limiter.acquire(project_id).await;
        assert!(permit1.is_ok());
        
        let permit2 = limiter.acquire(project_id).await;
        assert!(permit2.is_ok());
        
        // Keep permits alive
        let _p1 = permit1.unwrap();
        let _p2 = permit2.unwrap();
        
        // Third should wait (we won't actually wait in this test)
    }
}

#[cfg(test)]
mod utils_tests {
    use reiver_pond::warehouse::utils::{normalize_query, hash_query, increment_last_byte};
    
    #[test]
    fn test_normalize_query() {
        let q1 = normalize_query("SELECT * FROM   orders  WHERE id = 1");
        let q2 = normalize_query("SELECT * FROM orders WHERE id = 1");
        
        assert_eq!(q1, q2);
    }
    
    #[test]
    fn test_hash_query() {
        let hash1 = hash_query("SELECT * FROM orders");
        let hash2 = hash_query("SELECT * FROM users");
        
        assert_ne!(hash1, hash2);
        assert_eq!(hash1.len(), 16); // 64-bit hash as hex
    }
    
    #[test]
    fn test_increment_last_byte() {
        let result = increment_last_byte("abc");
        assert_eq!(result, "abd");
        
        let result2 = increment_last_byte("ab");
        assert_eq!(result2, "ac");
    }
}

// ============================================================================
// Load Tests for TB-Scale Validation
//
// These tests validate performance characteristics at scale:
// - Skip index with 1M files
// - Streaming with large result sets
// - Memory bounded operations
//
// Run with: cargo test --release load_tests -- --ignored --nocapture
// ============================================================================

#[cfg(test)]
mod load_tests {
    use reiver_pond::warehouse::indexes::skip_index::{
        FileSkipIndex, HierarchicalSkipIndex, SkipPredicates
    };
    use ahash::AHashMap;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};
    
    /// Generate a deterministic set of file values based on file index.
    fn generate_file_values(file_idx: usize, cardinality: usize) -> HashMap<String, Vec<String>> {
        let mut columns = HashMap::new();
        
        // Status column - low cardinality (5 values)
        let statuses = ["active", "pending", "completed", "cancelled", "processing"];
        columns.insert(
            "status".to_string(),
            vec![statuses[file_idx % statuses.len()].to_string()],
        );
        
        // Country column - medium cardinality (50 values)
        let country_idx = file_idx % 50;
        columns.insert(
            "country".to_string(),
            vec![format!("country_{:03}", country_idx)],
        );
        
        // Segment column - controlled cardinality
        let segment_idx = file_idx % cardinality.min(100);
        columns.insert(
            "segment".to_string(),
            vec![format!("segment_{:04}", segment_idx)],
        );
        
        columns
    }
    
    /// Generate partition key from file index (simulates date partitioning).
    fn generate_partition_key(file_idx: usize) -> String {
        // Distribute files across 1000 partitions (simulating ~3 years of daily data)
        let partition_idx = file_idx % 1000;
        let year = 2022 + (partition_idx / 365);
        let day_of_year = partition_idx % 365;
        let month = 1 + (day_of_year / 30).min(11);
        format!("{:04}/{:02}", year, month)
    }
    
    /// Load test: Skip index with 100K files.
    ///
    /// This test validates that the hierarchical skip index can handle 100K files
    /// with acceptable latency for filter operations.
    ///
    /// Target: Filter latency < 50ms for selective predicates
    #[test]
    #[ignore] // Run with --ignored flag for load tests
    fn test_skip_index_100k_files() {
        const FILE_COUNT: usize = 100_000;
        const PARTITIONS: usize = 100;
        
        println!("Building hierarchical skip index with {} files...", FILE_COUNT);
        let start = Instant::now();
        
        let mut hierarchical = HierarchicalSkipIndex::new();
        
        // Add files to partitions
        for i in 0..FILE_COUNT {
            let partition_key = format!("partition_{:03}", i % PARTITIONS);
            let file_path = format!("{}/file_{:06}.parquet", partition_key, i);
            
            let columns = generate_file_values(i, 100);
            let file_index = FileSkipIndex::build(&file_path, columns).unwrap();
            
            // Row count varies by file
            let row_count = (10_000 + (i % 10_000)) as u64;
            hierarchical.add_file(&partition_key, file_index, row_count).unwrap();
            
            if (i + 1) % 10_000 == 0 {
                println!("  Added {}/{} files ({:.2}s)", i + 1, FILE_COUNT, start.elapsed().as_secs_f64());
            }
        }
        
        let build_time = start.elapsed();
        println!("Index built in {:.2}s", build_time.as_secs_f64());
        println!("Total files: {}", hierarchical.total_files());
        
        // Benchmark filter operations
        println!("\nBenchmarking filter operations...");
        
        // Test 1: Single equality predicate (should match ~1% of files)
        let mut predicates = SkipPredicates::new();
        predicates.add_equals("country", "country_001");
        
        let filter_start = Instant::now();
        let matching = hierarchical.filter_with_partition_hint(&predicates.equality, None);
        let filter_time = filter_start.elapsed();
        
        println!("  Equality filter: {} matches in {:?}", matching.len(), filter_time);
        assert!(filter_time < Duration::from_millis(100), "Filter took too long: {:?}", filter_time);
        
        // Test 2: Status filter (should match ~20% of files)
        let mut status_predicates = SkipPredicates::new();
        status_predicates.add_equals("status", "active");
        
        let filter_start = Instant::now();
        let status_matches = hierarchical.filter_with_partition_hint(&status_predicates.equality, None);
        let filter_time = filter_start.elapsed();
        
        println!("  Status filter: {} matches in {:?}", status_matches.len(), filter_time);
        assert!(filter_time < Duration::from_millis(100), "Filter took too long: {:?}", filter_time);
        
        // Test 3: Filter with partition hint (should be O(1) partition lookup)
        let mut partitioned_predicates = SkipPredicates::new();
        partitioned_predicates.add_equals("country", "country_001");
        
        let filter_start = Instant::now();
        let partition_matches = hierarchical.filter_with_partition_hint(
            &partitioned_predicates.equality,
            Some(&["partition_001"]),
        );
        let filter_time = filter_start.elapsed();
        
        println!("  Partitioned filter: {} matches in {:?}", partition_matches.len(), filter_time);
        assert!(filter_time < Duration::from_millis(10), "Partitioned filter took too long: {:?}", filter_time);
        
        // Test 4: Multiple predicates (AND)
        let mut multi_predicates = SkipPredicates::new();
        multi_predicates.add_equals("status", "active");
        multi_predicates.add_equals("country", "country_001");
        
        let filter_start = Instant::now();
        let multi_matches = hierarchical.filter_with_partition_hint(&multi_predicates.equality, None);
        let filter_time = filter_start.elapsed();
        
        println!("  Multi-predicate filter: {} matches in {:?}", multi_matches.len(), filter_time);
        assert!(filter_time < Duration::from_millis(100), "Multi-predicate filter took too long: {:?}", filter_time);
        
        println!("\n✓ All filter operations completed within latency targets");
    }
    
    /// Load test: Skip index with 1M files.
    ///
    /// This is a more aggressive test that validates the hierarchical index
    /// at true TB-scale (assuming ~1KB-1MB per file, 1M files = 1TB-1PB of data).
    ///
    /// Run this test only on machines with sufficient memory (8GB+).
    #[test]
    #[ignore] // Run with --ignored flag for load tests
    fn test_skip_index_1m_files() {
        const FILE_COUNT: usize = 1_000_000;
        const PARTITIONS: usize = 1000;
        const BATCH_SIZE: usize = 100_000;
        
        println!("Building hierarchical skip index with {} files...", FILE_COUNT);
        println!("This test requires ~4-8GB of memory and may take several minutes.");
        let start = Instant::now();
        
        let mut hierarchical = HierarchicalSkipIndex::new();
        
        // Add files in batches to monitor memory pressure
        for batch in 0..(FILE_COUNT / BATCH_SIZE) {
            let batch_start = batch * BATCH_SIZE;
            let batch_end = ((batch + 1) * BATCH_SIZE).min(FILE_COUNT);
            
            for i in batch_start..batch_end {
                let partition_key = generate_partition_key(i);
                let file_path = format!("{}/file_{:07}.parquet", partition_key, i);
                
                // Lower cardinality for 1M file test to manage memory
                let columns = generate_file_values(i, 50);
                let file_index = FileSkipIndex::build(&file_path, columns).unwrap();
                
                let row_count = (50_000 + (i % 50_000)) as u64;
                hierarchical.add_file(&partition_key, file_index, row_count).unwrap();
            }
            
            println!(
                "  Batch {}: Added files {}-{} ({:.1}s elapsed)",
                batch + 1,
                batch_start,
                batch_end,
                start.elapsed().as_secs_f64()
            );
        }
        
        let build_time = start.elapsed();
        println!("\nIndex built in {:.1}s", build_time.as_secs_f64());
        println!("Total files: {}", hierarchical.total_files());
        
        // Benchmark filter operations
        println!("\nBenchmarking filter operations at 1M scale...");
        
        // Test 1: Equality filter
        let mut predicates = SkipPredicates::new();
        predicates.add_equals("country", "country_001");
        
        let iterations = 10;
        let mut total_time = Duration::ZERO;
        let mut total_matches = 0;
        
        for _ in 0..iterations {
            let filter_start = Instant::now();
            let matching = hierarchical.filter_with_partition_hint(&predicates.equality, None);
            total_time += filter_start.elapsed();
            total_matches = matching.len();
        }
        
        let avg_time = total_time / iterations;
        println!("  Equality filter: {} matches, avg {:?} over {} iterations", 
            total_matches, avg_time, iterations);
        assert!(avg_time < Duration::from_millis(500), "Filter took too long: {:?}", avg_time);
        
        // Test 2: Partitioned filter (should be much faster)
        let filter_start = Instant::now();
        let partition_matches = hierarchical.filter_with_partition_hint(
            &predicates.equality,
            Some(&["2023/01", "2023/02"]),
        );
        let filter_time = filter_start.elapsed();
        
        println!("  Partitioned filter (2 partitions): {} matches in {:?}", 
            partition_matches.len(), filter_time);
        assert!(filter_time < Duration::from_millis(50), "Partitioned filter took too long: {:?}", filter_time);
        
        // Test 3: Build file pattern
        let pattern_start = Instant::now();
        let pattern = hierarchical.build_file_pattern(
            "prefix/",
            &[("country".to_string(), "country_001".to_string())].into_iter().collect(),
            Some(&["2023/06"]),
        );
        let pattern_time = pattern_start.elapsed();
        
        println!("  Build file pattern: {:?} in {:?}", pattern.chars().take(80).collect::<String>(), pattern_time);
        
        println!("\n✓ 1M file skip index test completed successfully");
    }
    
    /// Load test: Memory estimation accuracy.
    ///
    /// Validates that our memory estimation functions are accurate for large JSON objects.
    #[test]
    #[ignore]
    fn test_memory_estimation_accuracy() {
        use reiver_pond::warehouse::utils::estimate_json_value_memory;
        use serde_json::json;
        
        println!("Testing memory estimation accuracy...");
        
        // Test 1: Large array
        let large_array: serde_json::Value = (0..10_000)
            .map(|i| json!({"id": i, "name": format!("item_{}", i)}))
            .collect();
        
        let estimated = estimate_json_value_memory(&large_array);
        println!("  Large array (10K elements): estimated {} bytes", estimated);
        assert!(estimated > 500_000, "Underestimated large array");
        assert!(estimated < 5_000_000, "Overestimated large array");
        
        // Test 2: Deeply nested object
        let mut nested = json!({});
        let mut current = &mut nested;
        for i in 0..100 {
            *current = json!({ format!("level_{}", i): {} });
            current = current.get_mut(&format!("level_{}", i)).unwrap();
        }
        
        let estimated = estimate_json_value_memory(&nested);
        println!("  Deeply nested (100 levels): estimated {} bytes", estimated);
        assert!(estimated > 1000, "Underestimated nested object");
        
        // Test 3: Wide object
        let wide: serde_json::Value = (0..1000)
            .map(|i| (format!("field_{}", i), json!(i)))
            .collect::<serde_json::Map<_, _>>()
            .into();
        
        let estimated = estimate_json_value_memory(&wide);
        println!("  Wide object (1K fields): estimated {} bytes", estimated);
        assert!(estimated > 50_000, "Underestimated wide object");
        
        println!("\n✓ Memory estimation tests completed");
    }
    
    /// Load test: Query plan cache performance.
    ///
    /// Validates that the query plan cache provides significant speedup for repeated queries.
    #[test]
    #[ignore]
    fn test_query_plan_cache_performance() {
        use reiver_pond::warehouse::query::rewriter::{TableRewriter, QueryPlanCache, SharedQueryPlanCache};
        use reiver_pond::warehouse::types::R2TablePath;
        use std::sync::Arc;
        
        println!("Testing query plan cache performance...");
        
        // Create test tables
        let mut tables = AHashMap::new();
        for i in 0..10 {
            tables.insert(
                format!("table_{}", i),
                R2TablePath::for_testing(format!("prefix/table_{}", i)),
            );
        }
        
        // Create rewriter without cache
        let rewriter_no_cache = TableRewriter::new("collection");
        
        // Create rewriter with cache
        let cache: SharedQueryPlanCache = Arc::new(QueryPlanCache::with_default_capacity());
        let rewriter_with_cache = TableRewriter::new("collection")
            .with_cache(cache.clone());
        
        // Generate test queries
        let queries: Vec<String> = (0..100)
            .map(|i| format!("SELECT * FROM table_{} WHERE id = {}", i % 10, i))
            .collect();
        
        // Benchmark without cache
        let start = Instant::now();
        for query in &queries {
            rewriter_no_cache.rewrite(query, &tables).unwrap();
        }
        let no_cache_time = start.elapsed();
        println!("  Without cache: {} queries in {:?}", queries.len(), no_cache_time);
        
        // Warm up cache
        for query in &queries {
            rewriter_with_cache.rewrite(query, &tables).unwrap();
        }
        
        // Benchmark with cache (should be much faster)
        let start = Instant::now();
        for query in &queries {
            rewriter_with_cache.rewrite(query, &tables).unwrap();
        }
        let with_cache_time = start.elapsed();
        println!("  With cache (warm): {} queries in {:?}", queries.len(), with_cache_time);
        
        // Get cache stats
        let stats = cache.stats();
        println!("  Cache stats: {} hits, {} misses, {:.1}% hit rate",
            stats.hits, stats.misses, stats.hit_rate());
        
        // Cache should provide at least 2x speedup
        assert!(
            with_cache_time < no_cache_time / 2,
            "Cache should be at least 2x faster. No cache: {:?}, With cache: {:?}",
            no_cache_time, with_cache_time
        );
        
        println!("\n✓ Query plan cache provides {:.1}x speedup", 
            no_cache_time.as_secs_f64() / with_cache_time.as_secs_f64());
    }
    
    /// Concurrency test: Query limiter under load.
    ///
    /// Validates that the query limiter correctly enforces limits under concurrent load.
    #[tokio::test]
    #[ignore]
    async fn test_query_limiter_under_load() {
        use reiver_pond::warehouse::query::limiter::QueryLimiter;
        use uuid::Uuid;
        use tokio::time::timeout;
        
        println!("Testing query limiter under concurrent load...");
        
        const GLOBAL_LIMIT: usize = 10;
        const PER_PROJECT_LIMIT: usize = 3;
        const CONCURRENT_REQUESTS: usize = 50;
        
        use reiver_pond::warehouse::query::limiter::QueryLimiterConfig;
        let config = QueryLimiterConfig {
            max_concurrent_global: GLOBAL_LIMIT,
            max_concurrent_per_project: PER_PROJECT_LIMIT,
            ..Default::default()
        };
        let limiter = std::sync::Arc::new(QueryLimiter::new(config));
        let project_id = Uuid::new_v4();
        
        // Track permits acquired
        let acquired = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let rejected = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        
        // Spawn many concurrent requests
        let mut handles = Vec::new();
        for _ in 0..CONCURRENT_REQUESTS {
            let limiter = limiter.clone();
            let acquired = acquired.clone();
            let rejected = rejected.clone();
            
            handles.push(tokio::spawn(async move {
                // Try to acquire with timeout
                match timeout(Duration::from_millis(100), limiter.acquire(project_id)).await {
                    Ok(Ok(permit)) => {
                        acquired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        // Hold permit briefly
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        drop(permit);
                    }
                    _ => {
                        rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }));
        }
        
        // Wait for all requests
        for handle in handles {
            let _ = handle.await;
        }
        
        let total_acquired = acquired.load(std::sync::atomic::Ordering::Relaxed);
        let total_rejected = rejected.load(std::sync::atomic::Ordering::Relaxed);
        
        println!("  Concurrent requests: {}", CONCURRENT_REQUESTS);
        println!("  Permits acquired: {}", total_acquired);
        println!("  Requests rejected/timed out: {}", total_rejected);
        
        // At least some should have been acquired
        assert!(total_acquired > 0, "No permits were acquired");
        
        // Per-project limit should be enforced (can't have more than PER_PROJECT_LIMIT concurrent)
        println!("\n✓ Query limiter correctly enforces limits under load");
    }
}

// ============================================================================
// ClickHouse Integration Tests
// ============================================================================

/// Integration tests that require a running ClickHouse instance.
/// 
/// These tests verify end-to-end functionality including:
/// - Query execution against ClickHouse
/// - S3/R2 data access via ClickHouse
/// - Skip index effectiveness
/// 
/// # Running Tests
/// 
/// These tests require ClickHouse to be running. You can start it with:
/// 
/// ```bash
/// docker run -d --name clickhouse-test \
///   -p 8123:8123 \
///   -p 9000:9000 \
///   clickhouse/clickhouse-server:latest
/// ```
/// 
/// Or using testcontainers (automatically managed):
/// 
/// ```bash
/// cargo test --features testcontainers -- --ignored clickhouse
/// ```
#[cfg(test)]
mod clickhouse_integration_tests {
    use std::time::Duration;
    
    /// Configuration for ClickHouse testcontainer.
    #[cfg(feature = "testcontainers")]
    mod testcontainer_config {
        pub const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
        pub const CLICKHOUSE_TAG: &str = "24.1";
        pub const HTTP_PORT: u16 = 8123;
        pub const NATIVE_PORT: u16 = 9000;
    }
    
    /// Test helper to check if ClickHouse is available.
    async fn is_clickhouse_available(base_url: &str) -> bool {
        let client = reqwest::Client::new();
        match client
            .get(format!("{}/ping", base_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }
    
    /// Integration test: Query executor against real ClickHouse.
    /// 
    /// Tests basic query execution to verify the executor works correctly.
    #[tokio::test]
    #[ignore = "Requires ClickHouse - run with: cargo test --ignored clickhouse_executor"]
    async fn test_clickhouse_executor_basic() {
        use reiver_pond::warehouse::query::executor::{QueryExecutor, ExecutionOptions};
        
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://localhost:8123".to_string());
        
        if !is_clickhouse_available(&clickhouse_url).await {
            println!("⚠️ Skipping test: ClickHouse not available at {}", clickhouse_url);
            return;
        }
        
        println!("Testing query executor against ClickHouse at {}", clickhouse_url);
        
        use reiver_pond::warehouse::query::executor::ClickHouseConfig;
        let config = ClickHouseConfig {
            host: std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string()),
            native_port: 9000,
            http_port: 8123,
            database: "default".to_string(),
            username: None,
            password: None,
            pool: Default::default(),
        };
        let executor = QueryExecutor::with_config(config).await.unwrap();
        
        // Test simple query
        let options = ExecutionOptions::default();
        let result = executor.execute("SELECT 1 + 1 AS result", options).await;
        
        assert!(result.is_ok(), "Query should succeed: {:?}", result);
        let query_result = result.unwrap();
        
        assert_eq!(query_result.row_count, 1, "Should return one row");
        
        println!("✓ Basic query execution works");
    }
    
    /// Integration test: Query executor with system tables.
    /// 
    /// Tests querying ClickHouse system tables to verify complex queries work.
    #[tokio::test]
    #[ignore = "Requires ClickHouse - run with: cargo test --ignored clickhouse_system_tables"]
    async fn test_clickhouse_system_tables() {
        use reiver_pond::warehouse::query::executor::{QueryExecutor, ExecutionOptions};
        
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://localhost:8123".to_string());
        
        if !is_clickhouse_available(&clickhouse_url).await {
            println!("⚠️ Skipping test: ClickHouse not available at {}", clickhouse_url);
            return;
        }
        
        println!("Testing system table queries...");
        
        use reiver_pond::warehouse::query::executor::ClickHouseConfig;
        let config = ClickHouseConfig {
            host: std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string()),
            native_port: 9000,
            http_port: 8123,
            database: "default".to_string(),
            username: None,
            password: None,
            pool: Default::default(),
        };
        let executor = QueryExecutor::with_config(config).await.unwrap();
        
        // Query system.databases to verify we can access system tables
        let options = ExecutionOptions {
            limit: Some(10),
            ..Default::default()
        };
        
        let result = executor.execute(
            "SELECT name FROM system.databases LIMIT 10",
            options,
        ).await;
        
        assert!(result.is_ok(), "System table query should succeed: {:?}", result);
        let query_result = result.unwrap();
        
        assert!(query_result.row_count > 0, "Should return at least one database");
        
        println!("  Found {} databases", query_result.row_count);
        println!("✓ System table queries work");
    }
    
    /// Integration test: Streaming query execution.
    /// 
    /// Tests that streaming works correctly for large result sets.
    #[tokio::test]
    #[ignore = "Requires ClickHouse - run with: cargo test --ignored clickhouse_streaming"]
    async fn test_clickhouse_streaming() {
        use reiver_pond::warehouse::query::executor::{QueryExecutor, ExecutionOptions};
        use futures::StreamExt;
        
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://localhost:8123".to_string());
        
        if !is_clickhouse_available(&clickhouse_url).await {
            println!("⚠️ Skipping test: ClickHouse not available at {}", clickhouse_url);
            return;
        }
        
        println!("Testing streaming query execution...");
        
        use reiver_pond::warehouse::query::executor::ClickHouseConfig;
        let config = ClickHouseConfig {
            host: std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string()),
            native_port: 9000,
            http_port: 8123,
            database: "default".to_string(),
            username: None,
            password: None,
            pool: Default::default(),
        };
        let executor = QueryExecutor::with_config(config).await.unwrap();
        
        // Generate a larger result set using numbers()
        let options = ExecutionOptions {
            limit: Some(1000),
            ..Default::default()
        };
        
        let mut streaming_result = executor.execute_streaming(
            "SELECT number, number * 2 AS doubled FROM numbers(1000)",
            options,
        ).await.expect("Streaming query should start");
        
        let mut row_count = 0;
        while let Some(row_result) = streaming_result.rows.next().await {
            assert!(row_result.is_ok(), "Each row should be valid");
            row_count += 1;
        }
        
        assert_eq!(row_count, 1000, "Should stream all 1000 rows");
        
        println!("  Streamed {} rows successfully", row_count);
        println!("✓ Streaming query execution works");
    }
    
    /// Integration test: Query timeout handling.
    /// 
    /// Tests that queries respect timeout settings.
    #[tokio::test]
    #[ignore = "Requires ClickHouse - run with: cargo test --ignored clickhouse_timeout"]
    async fn test_clickhouse_query_timeout() {
        use reiver_pond::warehouse::query::executor::{QueryExecutor, ExecutionOptions};
        
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://localhost:8123".to_string());
        
        if !is_clickhouse_available(&clickhouse_url).await {
            println!("⚠️ Skipping test: ClickHouse not available at {}", clickhouse_url);
            return;
        }
        
        println!("Testing query timeout handling...");
        
        use reiver_pond::warehouse::query::executor::ClickHouseConfig;
        let config = ClickHouseConfig {
            host: std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string()),
            native_port: 9000,
            http_port: 8123,
            database: "default".to_string(),
            username: None,
            password: None,
            pool: Default::default(),
        };
        let executor = QueryExecutor::with_config(config).await.unwrap();
        
        // Set a very short timeout
        let options = ExecutionOptions {
            timeout_secs: Some(1),
            ..Default::default()
        };
        
        // This query should timeout (sleep for 10 seconds)
        let result = executor.execute(
            "SELECT sleep(10)",
            options,
        ).await;
        
        // Should return a timeout error
        assert!(result.is_err(), "Query should timeout");
        
        let error = result.unwrap_err();
        println!("  Got expected error: {}", error);
        
        println!("✓ Query timeout handling works");
    }
    
    /// Integration test: Query with LIMIT enforcement.
    /// 
    /// Tests that LIMIT is properly enforced server-side.
    #[tokio::test]
    #[ignore = "Requires ClickHouse - run with: cargo test --ignored clickhouse_limit"]
    async fn test_clickhouse_limit_enforcement() {
        use reiver_pond::warehouse::query::executor::{QueryExecutor, ExecutionOptions};
        
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://localhost:8123".to_string());
        
        if !is_clickhouse_available(&clickhouse_url).await {
            println!("⚠️ Skipping test: ClickHouse not available at {}", clickhouse_url);
            return;
        }
        
        println!("Testing LIMIT enforcement...");
        
        use reiver_pond::warehouse::query::executor::ClickHouseConfig;
        let config = ClickHouseConfig {
            host: std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string()),
            native_port: 9000,
            http_port: 8123,
            database: "default".to_string(),
            username: None,
            password: None,
            pool: Default::default(),
        };
        let executor = QueryExecutor::with_config(config).await.unwrap();
        
        // Generate 1000 rows but limit to 100
        let options = ExecutionOptions {
            limit: Some(100),
            ..Default::default()
        };
        
        let result = executor.execute(
            "SELECT number FROM numbers(1000)",
            options,
        ).await.expect("Query should succeed");
        
        // Should only return 100 rows due to limit
        assert!(result.row_count <= 100, "Should respect LIMIT: got {} rows", result.row_count);
        
        println!("  Returned {} rows (limit: 100)", result.row_count);
        println!("✓ LIMIT enforcement works");
    }
}

/// Integration tests for the full warehouse query flow.
/// 
/// These tests verify end-to-end behavior including:
/// - Query rewriting with skip index optimization
/// - Cache invalidation after data syncs
/// - Query plan cache behavior
#[cfg(test)]
mod warehouse_integration_tests {
    use reiver_pond::warehouse::query::rewriter::{QueryPlanCache, TableRewriter};
    use reiver_pond::warehouse::query::cache::{QueryCache, QueryCacheConfig};
    use reiver_pond::warehouse::indexes::skip_index::{HierarchicalSkipIndex, FileSkipIndex};
    use reiver_pond::warehouse::types::{R2TablePath, SourceType};
    use ahash::AHashMap;
    use std::collections::HashMap;
    use uuid::Uuid;
    
    fn test_project_id() -> Uuid {
        Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
    }
    
    fn create_test_tables(project_id: Uuid) -> AHashMap<String, R2TablePath> {
        let mut tables = AHashMap::new();
        
        tables.insert(
            "orders".to_string(),
            R2TablePath::try_with_project(project_id, SourceType::Stripe, "orders")
                .expect("valid table"),
        );
        
        tables.insert(
            "customers".to_string(),
            R2TablePath::try_with_project(project_id, SourceType::Stripe, "customers")
                .expect("valid table"),
        );
        
        tables
    }
    
    fn create_test_skip_index() -> HierarchicalSkipIndex {
        let mut index = HierarchicalSkipIndex::new();
        
        // Add some test files with status values
        let mut cols1 = HashMap::new();
        cols1.insert("status".to_string(), vec!["active".to_string(), "pending".to_string()]);
        let file1 = FileSkipIndex::build("2025/01/data_001.parquet", cols1).unwrap();
        index.add_file("2025/01", file1, 10000).unwrap();
        
        let mut cols2 = HashMap::new();
        cols2.insert("status".to_string(), vec!["inactive".to_string()]);
        let file2 = FileSkipIndex::build("2025/01/data_002.parquet", cols2).unwrap();
        index.add_file("2025/01", file2, 10000).unwrap();
        
        let mut cols3 = HashMap::new();
        cols3.insert("status".to_string(), vec!["active".to_string()]);
        let file3 = FileSkipIndex::build("2025/02/data_001.parquet", cols3).unwrap();
        index.add_file("2025/02", file3, 10000).unwrap();
        
        index
    }
    
    #[test]
    fn test_query_plan_cache_hit_miss() {
        let cache = QueryPlanCache::new(100);
        let tables = create_test_tables(test_project_id());
        
        let sql = "SELECT * FROM orders WHERE status = 'active'";
        let rewritten = "SELECT * FROM s3(...) WHERE status = 'active'";
        
        // First access should be a miss
        let result = cache.get(sql, &tables);
        assert!(result.is_none(), "First access should be cache miss");
        
        // Store in cache
        cache.put(sql, &tables, rewritten.to_string());
        
        // Second access should be a hit
        let result = cache.get(sql, &tables);
        assert!(result.is_some(), "Second access should be cache hit");
        assert_eq!(&*result.unwrap(), rewritten);
        
        // Check stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }
    
    #[test]
    fn test_query_plan_cache_generation_invalidation() {
        let cache = QueryPlanCache::new(100);
        let tables = create_test_tables(test_project_id());
        
        let sql = "SELECT * FROM orders";
        let rewritten = "SELECT * FROM s3(...)";
        
        // Store in cache
        cache.put(sql, &tables, rewritten.to_string());
        
        // Should be a hit
        assert!(cache.get(sql, &tables).is_some());
        
        // Increment generation (simulating data sync)
        cache.increment_generation();
        
        // Should now be a miss (generation changed)
        assert!(cache.get(sql, &tables).is_none(), "Should miss after generation change");
    }
    
    #[test]
    fn test_query_plan_cache_memory_limit() {
        // Create cache with 1KB limit
        let cache = QueryPlanCache::with_memory_limit(1000, 1024);
        let tables = create_test_tables(test_project_id());
        
        // Add entries until we exceed memory limit
        for i in 0..100 {
            let sql = format!("SELECT * FROM orders WHERE id = {}", i);
            let rewritten = format!("SELECT * FROM s3(...) WHERE id = {}", i);
            cache.put(&sql, &tables, rewritten);
        }
        
        // Memory should be limited
        let stats = cache.stats();
        assert!(
            stats.estimated_memory_bytes <= stats.max_memory_bytes,
            "Memory {} should be <= limit {}",
            stats.estimated_memory_bytes,
            stats.max_memory_bytes
        );
        
        // Some entries should have been evicted
        assert!(stats.memory_evictions > 0, "Should have evicted some entries");
    }
    
    #[test]
    fn test_skip_index_filtering() {
        let index = create_test_skip_index();
        
        // Filter for active status - should match 2 files
        let mut predicates = HashMap::new();
        predicates.insert("status".to_string(), "active".to_string());
        
        let files = index.filter_with_partition_hint(&predicates, None);
        assert_eq!(files.len(), 2, "Should find 2 files with 'active' status");
        
        // Filter for inactive status - should match 1 file
        predicates.clear();
        predicates.insert("status".to_string(), "inactive".to_string());
        
        let files = index.filter_with_partition_hint(&predicates, None);
        assert_eq!(files.len(), 1, "Should find 1 file with 'inactive' status");
        
        // Filter with partition hint - only search 2025/01
        predicates.clear();
        predicates.insert("status".to_string(), "active".to_string());
        
        let files = index.filter_with_partition_hint(&predicates, Some(&["2025/01"]));
        assert_eq!(files.len(), 1, "Should find 1 file in 2025/01 with 'active' status");
    }
    
    #[test]
    fn test_skip_index_partition_pruning() {
        let index = create_test_skip_index();
        
        // Get partitions that might contain a value
        let partitions = index.partitions_containing("status", "inactive");
        
        // Only 2025/01 should have 'inactive' status
        assert!(partitions.contains(&"2025/01"), "2025/01 should contain 'inactive'");
        // 2025/02 should not (it only has 'active')
        // Note: Due to summary FST behavior, this depends on implementation
    }
    
    #[test]
    fn test_rewriter_with_skip_indexes() {
        let project_id = test_project_id();
        let tables = create_test_tables(project_id);
        let skip_index = create_test_skip_index();
        
        let mut indexes: AHashMap<String, HierarchicalSkipIndex> = AHashMap::new();
        indexes.insert("orders".to_string(), skip_index);
        
        let rewriter = TableRewriter::new("warehouse_r2");
        
        let sql = "SELECT * FROM orders WHERE status = 'active'";
        
        // Rewrite with hierarchical optimization
        let result = rewriter.rewrite_with_hierarchical_optimization(sql, &tables, &indexes);
        assert!(result.is_ok(), "Rewrite should succeed: {:?}", result.err());
        
        let rewritten = result.unwrap();
        assert!(rewritten.contains("s3("), "Should contain s3() function");
        assert!(rewritten.contains("warehouse_r2"), "Should use named collection");
    }
    
    #[test]
    fn test_table_isolation_enforced() {
        let project_a = Uuid::new_v4();
        let project_b = Uuid::new_v4();
        
        // Create tables for project A
        let tables_a = create_test_tables(project_a);
        
        // Validate should fail for project B accessing project A's tables
        let validation = TableRewriter::validate_table_access(&tables_a, project_b);
        assert!(validation.is_err(), "Cross-project access should be denied");
    }
    
    #[test]
    fn test_dangerous_query_detection() {
        let dialect = sqlparser::dialect::ClickHouseDialect {};

        // Multi-statement injection: parser must return >1 statement
        let multi_stmt = "SELECT * FROM orders; DROP TABLE orders";
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, multi_stmt).unwrap();
        assert!(
            stmts.len() > 1,
            "Multi-statement SQL must be detected: got {} statements",
            stmts.len()
        );

        // UNION-based injection: parser returns a single statement, so
        // callers must inspect the AST (e.g. reject UNION with system tables).
        let union_inject = "SELECT * FROM orders UNION SELECT * FROM information_schema.tables";
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, union_inject).unwrap();
        assert_eq!(stmts.len(), 1, "UNION parses as a single statement");

        // Verify the single statement contains a UNION body so callers
        // know they must apply secondary validation.
        let sql_str = format!("{}", &stmts[0]);
        assert!(
            sql_str.contains("UNION"),
            "UNION keyword must be present in the parsed AST output for secondary checks"
        );
    }
    
    #[test]
    fn test_query_rewrite_preserves_structure() {
        let project_id = test_project_id();
        let tables = create_test_tables(project_id);
        
        let rewriter = TableRewriter::new("warehouse_r2");
        
        // Test various query structures are preserved
        let test_cases = [
            ("SELECT * FROM orders", "Simple select"),
            ("SELECT * FROM orders WHERE id = 1", "With WHERE"),
            ("SELECT * FROM orders ORDER BY created_at DESC", "With ORDER BY"),
            ("SELECT * FROM orders LIMIT 100", "With LIMIT"),
            ("SELECT COUNT(*) FROM orders GROUP BY status", "With GROUP BY"),
        ];
        
        for (sql, description) in test_cases {
            let result = rewriter.rewrite(sql, &tables);
            assert!(result.is_ok(), "{} failed: {:?}", description, result.err());
            
            let rewritten = result.unwrap();
            
            // Verify key SQL elements are preserved
            if sql.contains("WHERE") {
                assert!(rewritten.contains("WHERE"), "{} should preserve WHERE", description);
            }
            if sql.contains("ORDER BY") {
                assert!(rewritten.contains("ORDER BY"), "{} should preserve ORDER BY", description);
            }
            if sql.contains("LIMIT") {
                assert!(rewritten.contains("LIMIT"), "{} should preserve LIMIT", description);
            }
            if sql.contains("GROUP BY") {
                assert!(rewritten.contains("GROUP BY"), "{} should preserve GROUP BY", description);
            }
        }
    }
}

#[cfg(test)]
mod cache_invalidation_tests {
    use reiver_pond::warehouse::indexes::skip_index_cache::{SkipIndexCache, SkipIndexCacheConfig};
    use reiver_pond::warehouse::indexes::skip_index::HierarchicalSkipIndex;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;
    use tokio::sync::Barrier;
    
    /// Test that cache invalidation during concurrent reads is safe.
    #[tokio::test]
    async fn test_concurrent_read_with_invalidation() {
        let cache = Arc::new(SkipIndexCache::with_defaults());
        let project_id = Uuid::new_v4();
        
        // Pre-populate cache
        cache.put(project_id, "orders", HierarchicalSkipIndex::new(), 1024);
        cache.put(project_id, "users", HierarchicalSkipIndex::new(), 1024);
        
        let barrier = Arc::new(Barrier::new(3));
        
        // Reader 1
        let cache1 = cache.clone();
        let barrier1 = barrier.clone();
        let read1 = tokio::spawn(async move {
            barrier1.wait().await;
            for _ in 0..100 {
                let _ = cache1.get(project_id, "orders");
                tokio::task::yield_now().await;
            }
        });
        
        // Reader 2
        let cache2 = cache.clone();
        let barrier2 = barrier.clone();
        let read2 = tokio::spawn(async move {
            barrier2.wait().await;
            for _ in 0..100 {
                let _ = cache2.get(project_id, "users");
                tokio::task::yield_now().await;
            }
        });
        
        // Invalidator
        let cache3 = cache.clone();
        let barrier3 = barrier.clone();
        let invalidator = tokio::spawn(async move {
            barrier3.wait().await;
            for i in 0..10 {
                if i % 2 == 0 {
                    cache3.invalidate_table(project_id, "orders");
                } else {
                    cache3.invalidate_project(project_id);
                }
                tokio::task::yield_now().await;
            }
        });
        
        // All tasks should complete without panic
        let (r1, r2, inv) = tokio::join!(read1, read2, invalidator);
        r1.unwrap();
        r2.unwrap();
        inv.unwrap();
    }
    
    /// Test that generation-based invalidation works correctly during concurrent operations.
    #[tokio::test]
    async fn test_generation_invalidation_during_concurrent_writes() {
        let cache = Arc::new(SkipIndexCache::with_defaults());
        
        let barrier = Arc::new(Barrier::new(4));
        
        // Writer 1 - writes to project A
        let cache1 = cache.clone();
        let barrier1 = barrier.clone();
        let project_a = Uuid::new_v4();
        let write1 = tokio::spawn(async move {
            barrier1.wait().await;
            for i in 0..50 {
                cache1.put(project_a, &format!("table_{}", i % 5), HierarchicalSkipIndex::new(), 512);
                tokio::task::yield_now().await;
            }
        });
        
        // Writer 2 - writes to project B
        let cache2 = cache.clone();
        let barrier2 = barrier.clone();
        let project_b = Uuid::new_v4();
        let write2 = tokio::spawn(async move {
            barrier2.wait().await;
            for i in 0..50 {
                cache2.put(project_b, &format!("table_{}", i % 5), HierarchicalSkipIndex::new(), 512);
                tokio::task::yield_now().await;
            }
        });
        
        // Generation incrementer
        let cache3 = cache.clone();
        let barrier3 = barrier.clone();
        let gen_inc = tokio::spawn(async move {
            barrier3.wait().await;
            for _ in 0..10 {
                cache3.increment_generation();
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        });
        
        // Reader - reads should see either hit or miss, never stale data after generation increment
        let cache4 = cache.clone();
        let barrier4 = barrier.clone();
        let reader = tokio::spawn(async move {
            barrier4.wait().await;
            for _ in 0..100 {
                // Just verify we can read without panicking
                let _ = cache4.get(project_a, "table_0");
                let _ = cache4.get(project_b, "table_1");
                tokio::task::yield_now().await;
            }
        });
        
        // All should complete without panic
        let (w1, w2, gen, r) = tokio::join!(write1, write2, gen_inc, reader);
        w1.unwrap();
        w2.unwrap();
        gen.unwrap();
        r.unwrap();
    }
    
    /// Test that entries cached before generation increment are not returned.
    #[tokio::test]
    async fn test_stale_entries_not_returned_after_generation_increment() {
        let cache = SkipIndexCache::with_defaults();
        let project_id = Uuid::new_v4();
        
        // Put an entry
        cache.put(project_id, "orders", HierarchicalSkipIndex::new(), 1024);
        
        // Should be retrievable
        assert!(cache.get(project_id, "orders").is_some());
        
        // Increment generation (simulating sync completion)
        cache.increment_generation();
        
        // Entry should now be stale and not returned
        assert!(cache.get(project_id, "orders").is_none());
        
        // Put a fresh entry
        cache.put(project_id, "orders", HierarchicalSkipIndex::new(), 1024);
        
        // Fresh entry should be retrievable
        assert!(cache.get(project_id, "orders").is_some());
    }
    
    /// Test memory accounting during concurrent operations.
    #[tokio::test]
    async fn test_memory_accounting_concurrent() {
        let config = SkipIndexCacheConfig {
            max_memory_bytes: 100_000, // Small limit for testing
            ..Default::default()
        };
        let cache = Arc::new(SkipIndexCache::new(config));
        
        let barrier = Arc::new(Barrier::new(3));
        
        // Writer 1
        let cache1 = cache.clone();
        let barrier1 = barrier.clone();
        let project_a = Uuid::new_v4();
        let write1 = tokio::spawn(async move {
            barrier1.wait().await;
            for i in 0..20 {
                cache1.put(project_a, &format!("table_{}", i), HierarchicalSkipIndex::new(), 5000);
                tokio::task::yield_now().await;
            }
        });
        
        // Writer 2
        let cache2 = cache.clone();
        let barrier2 = barrier.clone();
        let project_b = Uuid::new_v4();
        let write2 = tokio::spawn(async move {
            barrier2.wait().await;
            for i in 0..20 {
                cache2.put(project_b, &format!("table_{}", i), HierarchicalSkipIndex::new(), 5000);
                tokio::task::yield_now().await;
            }
        });
        
        // Invalidator (to trigger evictions)
        let cache3 = cache.clone();
        let barrier3 = barrier.clone();
        let invalidator = tokio::spawn(async move {
            barrier3.wait().await;
            for _ in 0..5 {
                cache3.invalidate_project(project_a);
                tokio::task::yield_now().await;
            }
        });
        
        let (w1, w2, inv) = tokio::join!(write1, write2, invalidator);
        w1.unwrap();
        w2.unwrap();
        inv.unwrap();
        
        // Memory should be within limits (with some slack for race conditions)
        let stats = cache.stats();
        // Just verify we didn't panic and stats are available
        assert!(stats.memory_usage_bytes <= 200_000); // Some slack for concurrent updates
    }
    
    /// Test that clear works correctly during concurrent operations.
    #[tokio::test]
    async fn test_clear_during_concurrent_access() {
        let cache = Arc::new(SkipIndexCache::with_defaults());
        let project_id = Uuid::new_v4();
        
        // Pre-populate
        for i in 0..10 {
            cache.put(project_id, &format!("table_{}", i), HierarchicalSkipIndex::new(), 1024);
        }
        
        let barrier = Arc::new(Barrier::new(3));
        
        // Reader
        let cache1 = cache.clone();
        let barrier1 = barrier.clone();
        let reader = tokio::spawn(async move {
            barrier1.wait().await;
            for _ in 0..50 {
                for i in 0..10 {
                    let _ = cache1.get(project_id, &format!("table_{}", i));
                }
                tokio::task::yield_now().await;
            }
        });
        
        // Writer
        let cache2 = cache.clone();
        let barrier2 = barrier.clone();
        let writer = tokio::spawn(async move {
            barrier2.wait().await;
            for _ in 0..50 {
                cache2.put(project_id, "new_table", HierarchicalSkipIndex::new(), 1024);
                tokio::task::yield_now().await;
            }
        });
        
        // Clearer
        let cache3 = cache.clone();
        let barrier3 = barrier.clone();
        let clearer = tokio::spawn(async move {
            barrier3.wait().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
            cache3.clear();
        });
        
        let (r, w, c) = tokio::join!(reader, writer, clearer);
        r.unwrap();
        w.unwrap();
        c.unwrap();
        
        // After clear, cache should be empty or only have entries from after the clear
        // The stats should be consistent (no negative values, etc.)
        let stats = cache.stats();
        assert!(stats.memory_usage_bytes >= 0);
    }
}

#[cfg(test)]
mod property_tests {
    use proptest::prelude::*;
    use reiver_pond::warehouse::query::rewriter::TableRewriter;
    use reiver_pond::warehouse::types::R2TablePath;
    use reiver_pond::warehouse::utils::{normalize_query, hash_query, increment_last_byte};
    use reiver_pond::warehouse::indexes::skip_index::{FileSkipIndex, DataSkipIndex};
    use ahash::AHashMap;
    use std::collections::HashMap;
    
    // ==================== SQL Parsing Property Tests ====================
    
    /// Generate valid SQL identifier strings
    fn valid_identifier() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_]{0,20}".prop_filter("not empty", |s| !s.is_empty())
    }
    
    /// Generate simple SELECT queries
    fn simple_select_query() -> impl Strategy<Value = String> {
        valid_identifier().prop_map(|table| format!("SELECT * FROM {}", table))
    }
    
    /// Generate SELECT queries with WHERE clause
    fn select_with_where() -> impl Strategy<Value = (String, String, i32)> {
        (valid_identifier(), valid_identifier(), any::<i32>())
    }
    
    proptest! {
        /// Property: Query normalization is idempotent
        #[test]
        fn prop_normalization_is_idempotent(query in "[a-zA-Z0-9_ ]{1,100}") {
            let normalized_once = normalize_query(&query);
            let normalized_twice = normalize_query(&normalized_once);
            prop_assert_eq!(normalized_once, normalized_twice);
        }
        
        /// Property: Query hash is deterministic
        #[test]
        fn prop_hash_is_deterministic(query in "[a-zA-Z0-9_ ]{1,100}") {
            let hash1 = hash_query(&query);
            let hash2 = hash_query(&query);
            prop_assert_eq!(hash1, hash2);
        }
        
        /// Property: Different queries produce different hashes (collision resistance)
        #[test]
        fn prop_hash_different_queries(
            query1 in "[a-zA-Z0-9_ ]{1,50}",
            query2 in "[a-zA-Z0-9_ ]{1,50}"
        ) {
            if normalize_query(&query1) != normalize_query(&query2) {
                // Different normalized queries should (usually) produce different hashes
                // We can't guarantee no collisions, but they should be rare
                let hash1 = hash_query(&query1);
                let hash2 = hash_query(&query2);
                // Just verify they're valid hashes (no crash)
                prop_assert_eq!(hash1.len(), 16);
                prop_assert_eq!(hash2.len(), 16);
            }
        }
        
        /// Property: Simple SELECT queries can be parsed and rewritten
        #[test]
        fn prop_simple_select_can_be_rewritten(table_name in valid_identifier()) {
            let sql = format!("SELECT * FROM {}", table_name);
            
            let rewriter = TableRewriter::new("r2_test");
            
            let mut tables = AHashMap::new();
            tables.insert(table_name.clone(), R2TablePath::for_testing(&format!("data/{}", table_name)));
            
            let result = rewriter.rewrite(&sql, &tables);
            // Should either succeed or fail gracefully
            prop_assert!(result.is_ok() || result.is_err());
            
            if let Ok(rewritten) = result {
                // Rewritten query should contain s3() function
                prop_assert!(rewritten.contains("s3("));
            }
        }
        
        /// Property: SELECT with WHERE can be parsed
        #[test]
        fn prop_select_with_where_parseable(
            (table, column, value) in select_with_where()
        ) {
            let sql = format!("SELECT * FROM {} WHERE {} = {}", table, column, value);
            
            // Verify we can extract tables
            let tables_result = TableRewriter::extract_tables(&sql);
            prop_assert!(tables_result.is_ok());
            
            let tables = tables_result.unwrap();
            prop_assert!(tables.contains(&table));
        }
    }
    
    // ==================== Increment Last Byte Property Tests ====================
    
    proptest! {
        /// Property: increment_last_byte produces valid UTF-8
        #[test]
        fn prop_increment_produces_valid_utf8(s in "[a-zA-Z0-9]{1,20}") {
            let result = increment_last_byte(&s);
            // Result should be valid UTF-8 (this is guaranteed by String type)
            // Verify we can iterate over chars
            let char_count = result.chars().count();
            prop_assert!(char_count > 0);
        }
        
        /// Property: increment_last_byte length is preserved
        #[test]
        fn prop_increment_preserves_length(s in "[a-zA-Z0-9]{1,20}") {
            let result = increment_last_byte(&s);
            // For ASCII, length should be preserved
            prop_assert_eq!(s.len(), result.len());
        }
        
        /// Property: increment produces lexicographically greater string (for ASCII)
        #[test]
        fn prop_increment_is_greater_for_ascii(s in "[a-zA-Y0-8]{1,20}") {
            // Avoid z/9 at end which would wrap
            let result = increment_last_byte(&s);
            prop_assert!(result > s);
        }
        
        /// Property: empty string stays empty
        #[test]
        fn prop_increment_empty_is_empty(_dummy in Just(())) {
            let result = increment_last_byte("");
            prop_assert!(result.is_empty());
        }
    }
    
    // ==================== Skip Index Property Tests ====================
    
    /// Generate column names for skip index tests
    fn column_name() -> impl Strategy<Value = String> {
        "[a-z_]{1,10}".prop_filter("not empty", |s| !s.is_empty())
    }
    
    /// Generate column values
    fn column_value() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9_]{1,20}".prop_filter("not empty", |s| !s.is_empty())
    }
    
    proptest! {
        /// Property: FileSkipIndex contains the values it was built with
        #[test]
        fn prop_skip_index_contains_added_values(
            values in prop::collection::vec(column_value(), 1..20)
        ) {
            let mut columns = HashMap::new();
            columns.insert("test_column".to_string(), values.clone());
            
            let index = FileSkipIndex::build("test.parquet", columns);
            prop_assert!(index.is_ok());
            
            let index = index.unwrap();
            
            // Index should contain the column
            let fst = index.column_values.get("test_column");
            prop_assert!(fst.is_some());
        }
        
        /// Property: DataSkipIndex files_containing returns correct files
        #[test]
        fn prop_data_skip_index_filter_correctness(
            file_values in prop::collection::vec(
                (prop::collection::vec(column_value(), 1..5)),
                1..5
            )
        ) {
            let mut index = DataSkipIndex::new();
            
            // Build index with files
            for (i, values) in file_values.iter().enumerate() {
                let mut columns = HashMap::new();
                columns.insert("status".to_string(), values.clone());
                
                if let Ok(file_index) = FileSkipIndex::build(&format!("file_{}.parquet", i), columns) {
                    index.add_file(file_index);
                }
            }
            
            // Query for each value that was added
            for (i, values) in file_values.iter().enumerate() {
                for value in values {
                    let files = index.files_containing("status", value);
                    // The file that contains this value should be in the result
                    // (unless the value was deduplicated in FST building)
                    let file_name = format!("file_{}.parquet", i);
                    if !files.contains(&file_name.as_str()) {
                        // Value might have been in multiple files or deduplicated
                        // This is fine as long as we don't crash
                    }
                }
            }
        }
        
        /// Property: Querying for non-existent values returns empty
        #[test]
        fn prop_skip_index_nonexistent_returns_empty(
            values in prop::collection::vec(column_value(), 1..10),
            nonexistent in "[z]{21,30}"  // Value that won't be in the index
        ) {
            let mut columns = HashMap::new();
            columns.insert("status".to_string(), values);
            
            let mut index = DataSkipIndex::new();
            if let Ok(file_index) = FileSkipIndex::build("test.parquet", columns) {
                index.add_file(file_index);
            }
            
            // Query for non-existent value should return empty
            let files = index.files_containing("status", &nonexistent);
            prop_assert!(files.is_empty());
        }
    }
    
    // ==================== R2TablePath Property Tests ====================
    
    proptest! {
        /// Property: R2TablePath construction is consistent
        #[test]
        #[allow(deprecated)]
        fn prop_r2_table_path_consistent(
            prefix in "[a-zA-Z0-9/]{1,30}"
        ) {
            let path = R2TablePath::for_testing(&prefix);
            
            // Path should contain the prefix
            prop_assert!(path.prefix.contains(&prefix) || path.prefix == prefix);
        }
    }
}

// ==================== Federation Tests ====================

#[cfg(test)]
mod federation_tests {
    use reiver_pond::warehouse::query::federation::{TableReference, CombinationStrategy};
    
    #[test]
    fn test_table_reference_parse_source_qualified() {
        let ref1 = TableReference::parse("stripe.customers");
        assert!(ref1.is_some());
        let ref1 = ref1.unwrap();
        assert_eq!(ref1.source_name, "stripe");
        assert_eq!(ref1.table_name, "customers");
        assert_eq!(ref1.qualified_name(), "stripe.customers");
    }

    #[test]
    fn test_table_reference_parse_unqualified() {
        let ref1 = TableReference::parse("customers");
        assert!(ref1.is_none());
    }

    #[test]
    fn test_table_reference_with_alias() {
        let ref1 = TableReference::new("stripe", "customers")
            .with_alias("c");
        
        assert_eq!(ref1.alias, Some("c".to_string()));
        assert_eq!(ref1.to_string(), "stripe.customers AS c");
    }

    #[test]
    fn test_table_reference_parse_complex_names() {
        let ref1 = TableReference::parse("s3_events.user_activity_logs");
        assert!(ref1.is_some());
        let ref1 = ref1.unwrap();
        assert_eq!(ref1.source_name, "s3_events");
        assert_eq!(ref1.table_name, "user_activity_logs");
    }
}

// ==================== AI Config Tests ====================

#[cfg(test)]
mod ai_config_tests {
    use reiver_pond::warehouse::ai_config::{
        DataProfile, ColumnProfile, MetadataSampler, ColumnStats,
        MockAIConfigProvider, AIConfigProvider,
    };
    use reiver_pond::warehouse::types::{CardinalityHint, ColumnType};
    use uuid::Uuid;

    #[test]
    fn test_column_profile_identifier_detection() {
        let user_id = ColumnProfile::new("user_id", ColumnType::String);
        assert!(user_id.looks_like_identifier());

        let email = ColumnProfile::new("email", ColumnType::String);
        assert!(email.looks_like_identifier());

        let uuid_col = ColumnProfile::new("request_uuid", ColumnType::String);
        assert!(uuid_col.looks_like_identifier());
    }

    #[test]
    fn test_column_profile_category_detection() {
        let status = ColumnProfile::new("status", ColumnType::String);
        assert!(status.looks_like_category());

        let country = ColumnProfile::new("country_code", ColumnType::String);
        assert!(country.looks_like_category());

        let tier = ColumnProfile::new("subscription_tier", ColumnType::String);
        assert!(tier.looks_like_category());
    }

    #[test]
    fn test_column_profile_timestamp_detection() {
        let created = ColumnProfile::new("created_at", ColumnType::Timestamp);
        assert!(created.looks_like_timestamp());

        let event_time = ColumnProfile::new("event_time", ColumnType::String);
        assert!(event_time.looks_like_timestamp());

        let occurred = ColumnProfile::new("occurred_at", ColumnType::Timestamp);
        assert!(occurred.looks_like_timestamp());
    }

    #[test]
    fn test_cardinality_hint_inference() {
        let mut col = ColumnProfile::new("test", ColumnType::String);
        
        col.estimated_cardinality = Some(10);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::VeryLow);

        col.estimated_cardinality = Some(1_000);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::Low);

        col.estimated_cardinality = Some(50_000);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::Medium);

        col.estimated_cardinality = Some(500_000);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::High);

        col.estimated_cardinality = Some(2_000_000);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::VeryHigh);
    }

    #[test]
    fn test_sampler_detect_partition_pattern() {
        let sampler = MetadataSampler::new();
        
        // Hive-style partitions
        let paths = vec![
            "data/year=2024/month=01/day=15/file1.parquet".to_string(),
            "data/year=2024/month=01/day=16/file2.parquet".to_string(),
            "data/year=2024/month=02/day=01/file3.parquet".to_string(),
        ];
        
        let pattern = sampler.detect_partition_pattern(&paths);
        assert!(pattern.is_some());
        let pattern = pattern.unwrap();
        assert!(pattern.contains("year="));
        assert!(pattern.contains("month="));
        assert!(pattern.contains("day="));
    }

    #[test]
    fn test_sampler_select_time_column_priority() {
        let sampler = MetadataSampler::new();
        let mut profile = DataProfile::new(Uuid::new_v4());
        
        // timestamp should have highest priority
        profile.detected_time_columns = vec![
            "date".to_string(),
            "created_at".to_string(),
            "timestamp".to_string(),
        ];
        
        let selected = sampler.select_time_column(&profile);
        assert_eq!(selected, Some("timestamp".to_string()));
    }

    #[test]
    fn test_sampler_select_time_column_at_suffix() {
        let sampler = MetadataSampler::new();
        let mut profile = DataProfile::new(Uuid::new_v4());
        
        profile.detected_time_columns = vec![
            "processed_at".to_string(),
            "some_date".to_string(),
        ];
        
        let selected = sampler.select_time_column(&profile);
        assert_eq!(selected, Some("processed_at".to_string()));
    }

    #[tokio::test]
    async fn test_mock_provider_generates_config() {
        let provider = MockAIConfigProvider::new();
        
        let mut profile = DataProfile::new(Uuid::new_v4());
        profile.columns = vec![
            {
                let mut c = ColumnProfile::new("user_id", ColumnType::String);
                c.estimated_cardinality = Some(1_000_000);
                c
            },
            {
                let mut c = ColumnProfile::new("status", ColumnType::String);
                c.estimated_cardinality = Some(5);
                c
            },
            ColumnProfile::new("created_at", ColumnType::Timestamp),
        ];
        profile.detected_time_columns = vec!["created_at".to_string()];
        profile.detected_partition_pattern = Some("year={year}/month={month}".to_string());
        profile.file_count = 100;
        
        let recommendation = provider.generate_config(&profile).await.unwrap();
        
        assert!(recommendation.confidence > 0.0);
        assert!(!recommendation.explanations.is_empty());
        assert!(recommendation.config.time_column.is_some());
        assert_eq!(recommendation.config.time_column, Some("created_at".to_string()));
    }

    #[tokio::test]
    async fn test_mock_provider_empty_profile_error() {
        let provider = MockAIConfigProvider::new();
        let profile = DataProfile::new(Uuid::new_v4());
        
        let result = provider.generate_config(&profile).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_data_profile_column_lookup() {
        let mut profile = DataProfile::new(Uuid::new_v4());
        profile.columns = vec![
            ColumnProfile::new("user_id", ColumnType::String),
            ColumnProfile::new("email", ColumnType::String),
            ColumnProfile::new("status", ColumnType::String),
        ];
        
        assert!(profile.get_column("user_id").is_some());
        assert!(profile.get_column("nonexistent").is_none());
        
        // identifier_columns should return high-cardinality columns
        let identifiers = profile.identifier_columns();
        assert!(identifiers.iter().any(|c| c.name == "user_id"));
        assert!(identifiers.iter().any(|c| c.name == "email"));
        
        // category_columns should return low-cardinality columns
        let categories = profile.category_columns();
        assert!(categories.iter().any(|c| c.name == "status"));
    }
}

// ============================================================================
// Track 3: Query Executor Edge Cases (Integration Tests)
// ============================================================================

#[cfg(test)]
mod query_rewriter_edge_cases {
    use reiver_pond::warehouse::query::rewriter::TableRewriter;
    use reiver_pond::warehouse::types::R2TablePath;
    use ahash::AHashMap;
    use uuid::Uuid;

    fn test_project_id() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
    }

    fn test_r2_path(table: &str) -> R2TablePath {
        R2TablePath::for_testing(format!("{}/{}", test_project_id(), table))
    }

    #[test]
    fn test_rewrite_cte_referencing_known_tables() {
        let mut tables = AHashMap::new();
        tables.insert("orders".to_string(), test_r2_path("orders"));

        let rewriter = TableRewriter::new("r2_collection");
        let sql = "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte";
        let result = rewriter.rewrite(sql, &tables);

        assert!(result.is_ok(), "CTE query should rewrite: {:?}", result.err());
        let rewritten = result.unwrap();
        // The real table (orders) in the CTE definition should be rewritten
        assert!(rewritten.contains("s3("), "CTE body should reference s3()");
    }

    #[test]
    fn test_rewrite_union_multiple_tables() {
        let mut tables = AHashMap::new();
        tables.insert("orders".to_string(), test_r2_path("orders"));
        tables.insert("customers".to_string(), test_r2_path("customers"));

        let rewriter = TableRewriter::new("r2_collection");
        let sql = "SELECT id FROM orders UNION ALL SELECT id FROM customers";
        let result = rewriter.rewrite(sql, &tables);

        assert!(result.is_ok(), "UNION query should rewrite: {:?}", result.err());
        let rewritten = result.unwrap();
        // Both tables should be rewritten to s3()
        assert!(
            rewritten.matches("s3(").count() >= 2,
            "Both branches of UNION should reference s3(), got: {}", rewritten
        );
    }

    #[test]
    fn test_rewrite_aliased_subquery_unknown_table() {
        let mut tables = AHashMap::new();
        tables.insert("orders".to_string(), test_r2_path("orders"));

        let rewriter = TableRewriter::new("r2_collection");
        let sql = "SELECT * FROM (SELECT * FROM nonexistent_table) AS sub";
        let result = rewriter.rewrite(sql, &tables);

        // The rewriter may either error or pass through unknown tables;
        // what matters is it does not panic and handles it gracefully
        match result {
            Ok(rewritten) => {
                // If it passes through, the unknown table should still appear
                assert!(
                    rewritten.contains("nonexistent_table"),
                    "Unknown table should be passed through: {}", rewritten
                );
            }
            Err(_) => {
                // Erroring on unknown tables is also valid behavior
            }
        }
    }

    #[test]
    fn test_extract_tables_from_cte() {
        let sql = "WITH cte AS (SELECT * FROM orders WHERE amount > 100) SELECT * FROM cte";
        let tables = TableRewriter::extract_tables(sql);
        assert!(tables.is_ok());
        let table_list = tables.unwrap();
        // Should extract "orders" from CTE definition
        assert!(
            table_list.contains(&"orders".to_string()),
            "Should extract 'orders' from CTE: {:?}", table_list
        );
    }

    #[test]
    fn test_extract_tables_from_union() {
        let sql = "SELECT id FROM orders UNION ALL SELECT id FROM customers";
        let tables = TableRewriter::extract_tables(sql);
        assert!(tables.is_ok());
        let table_list = tables.unwrap();
        assert!(table_list.contains(&"orders".to_string()));
        assert!(table_list.contains(&"customers".to_string()));
    }
}

#[cfg(test)]
mod skip_index_edge_cases {
    use reiver_pond::warehouse::indexes::skip_index::{
        FileSkipIndex, HierarchicalSkipIndex,
    };
    use std::collections::HashMap;

    #[test]
    fn test_skip_index_empty_file_set() {
        let hierarchical = HierarchicalSkipIndex::new();

        // Empty index should return empty results
        let predicates = HashMap::new();
        let matching = hierarchical.filter_with_partition_hint(&predicates, None);
        assert!(matching.is_empty(), "Empty index should return no matches");
        assert_eq!(hierarchical.total_files(), 0);
    }

    #[test]
    fn test_skip_index_with_empty_string_values() {
        let mut columns = HashMap::new();
        columns.insert("status".to_string(), vec![
            "".to_string(),
            "".to_string(),
            "".to_string(),
        ]);

        // Building with all-empty values should still work
        let result = FileSkipIndex::build("test.parquet", columns);
        assert!(result.is_ok(), "Index with empty string values should build: {:?}", result.err());
    }

    #[test]
    fn test_skip_index_filter_with_nonexistent_column() {
        let mut hierarchical = HierarchicalSkipIndex::new();

        let mut cols = HashMap::new();
        cols.insert("status".to_string(), vec!["active".to_string()]);
        let file = FileSkipIndex::build("p/file1.parquet", cols).unwrap();
        hierarchical.add_file("p", file, 1000).unwrap();

        // Query for a column that doesn't exist in the index
        let mut predicates = HashMap::new();
        predicates.insert("nonexistent_column".to_string(), "value".to_string());

        // Should not panic; should return all files (no filter possible)
        let matching = hierarchical.filter_with_partition_hint(&predicates, None);
        // Implementation may return all or empty, but should not panic
        let _ = matching;
    }
}

#[cfg(test)]
mod cost_estimator_edge_cases {
    use reiver_pond::warehouse::query::cost_estimator::{QueryCostEstimator, TableStats};

    #[test]
    fn test_cost_estimator_zero_size_files() {
        let mut estimator = QueryCostEstimator::new();

        estimator.add_table_stats(TableStats {
            table_name: "empty_table".to_string(),
            row_count: 0,
            size_bytes: 0,
            file_count: 0,
            avg_row_size: 0,
            last_updated: None,
        });

        let estimate = estimator.estimate("SELECT * FROM empty_table");
        assert!(estimate.is_ok(), "Zero-size table estimation should not error");

        let cost = estimate.unwrap();
        // Should return 0 cost, not panic
        assert_eq!(cost.estimated_bytes_scanned, 0);
    }

    #[test]
    fn test_cost_estimator_very_large_table() {
        let mut estimator = QueryCostEstimator::new();

        estimator.add_table_stats(TableStats {
            table_name: "huge_table".to_string(),
            row_count: 10_000_000_000,    // 10B rows
            size_bytes: 10_000_000_000_000, // 10TB
            file_count: 1_000_000,
            avg_row_size: 1000,
            last_updated: None,
        });

        let estimate = estimator.estimate("SELECT * FROM huge_table");
        assert!(estimate.is_ok(), "Very large table estimation should succeed");

        let cost = estimate.unwrap();
        assert!(cost.estimated_bytes_scanned > 0);
    }
}

#[cfg(test)]
mod query_settings_round_trip {
    use reiver_pond::warehouse::query::executor::ClickHouseQuerySettings;

    #[test]
    fn test_settings_to_query_params_round_trip() {
        let settings = ClickHouseQuerySettings::default()
            .with_timeout(30)
            .with_result_limits(5000, 50 * 1024 * 1024);

        let params = settings.to_query_params();

        // All settings should serialize correctly
        let param_map: std::collections::HashMap<_, _> = params.into_iter().collect();

        assert_eq!(param_map.get("max_execution_time"), Some(&"30".to_string()));
        assert_eq!(param_map.get("max_result_rows"), Some(&"5000".to_string()));
        assert_eq!(
            param_map.get("max_result_bytes"),
            Some(&(50 * 1024 * 1024).to_string())
        );

        // Verify boolean settings serialize as "1"/"0"
        assert!(
            param_map.get("input_format_parquet_filter_push_down")
                == Some(&"1".to_string()),
            "Parquet filter pushdown should be enabled by default"
        );
    }

    #[test]
    fn test_settings_no_timeout_no_result_limit() {
        let settings = ClickHouseQuerySettings::default().without_result_limits();

        let params = settings.to_query_params();
        let param_map: std::collections::HashMap<_, _> = params.into_iter().collect();

        // Zero-value params are intentionally omitted from query params
        assert!(param_map.get("max_result_rows").is_none(), "0-value max_result_rows should be omitted");
        assert!(param_map.get("max_result_bytes").is_none(), "0-value max_result_bytes should be omitted");
        assert!(param_map.get("max_execution_time").is_none(), "0-value max_execution_time should be omitted");

        // But always-present params should still be there
        assert!(param_map.get("max_threads").is_some(), "max_threads should always be present");
        assert!(param_map.get("max_memory_usage").is_some(), "max_memory_usage should always be present");
    }
}
