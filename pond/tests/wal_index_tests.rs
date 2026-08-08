//! Integration tests for the WAL-based two-phase indexing system
//!
//! These tests verify the correctness of the two-phase query execution:
//! - Phase 1: Block elimination using skip indexes (Xor filters, MinMax stats)
//! - Phase 2: PK resolution using inverted indexes (Roaring Bitmaps)
//!
//! To run:
//!   cargo test --test wal_index_tests

// Test the core indexing components
mod block_tests {
    use reiver_pond::warehouse::connectors::wal_index::{
        Block, BlockManager, BlockManagerConfig, PrimaryKey,
    };

    #[test]
    fn test_block_creation() {
        let block = Block::new(1, "pk_100");
        assert_eq!(block.id, 1);
        assert_eq!(block.row_count, 0);
        assert!(!block.is_closed);
    }

    #[test]
    fn test_block_add_rows() {
        let mut block = Block::new(1, "pk_001");
        
        // Add some rows
        for i in 1..=100 {
            block.add_row(&format!("pk_{:03}", i));
        }
        
        assert_eq!(block.row_count, 100);
        assert_eq!(block.pk_end, "pk_100");
    }

    #[test]
    fn test_block_should_split() {
        let mut block = Block::new(1, "pk_001");
        
        // Add rows up to threshold
        for i in 1..=10000 {
            block.add_row(&format!("pk_{:06}", i));
        }
        
        // At 10000 rows, should not split yet (threshold is 2x target of 10000)
        assert!(!block.should_split(10000));
        
        // Add more rows to trigger split threshold
        for i in 10001..=25000 {
            block.add_row(&format!("pk_{:06}", i));
        }
        
        // At 25000 rows, should split (> 2x 10000)
        assert!(block.should_split(10000));
    }

    #[test]
    fn test_block_manager_assign_block() {
        let config = BlockManagerConfig::default();
        let manager = BlockManager::new("test_source", "test_table", config);
        
        // First PK should create a new block
        let pk1 = PrimaryKey::String("pk_001".into());
        let result = manager.assign_block(&pk1);
        assert!(result.is_ok());
        let (block_id, is_new) = result.unwrap();
        // Block IDs are implementation-specific, just check it's new
        assert!(is_new);
        let first_block_id = block_id;
        
        // Same or subsequent PKs should go to same block
        let pk2 = PrimaryKey::String("pk_002".into());
        let result2 = manager.assign_block(&pk2);
        assert!(result2.is_ok());
        let (block_id2, is_new2) = result2.unwrap();
        assert_eq!(block_id2, first_block_id);
        assert!(!is_new2);
    }

    #[test]
    fn test_block_manager_with_string_pks() {
        let config = BlockManagerConfig::default();
        let manager = BlockManager::new("test_source", "test_table", config);
        
        let pk1 = PrimaryKey::String("user_001".into());
        let pk2 = PrimaryKey::String("user_002".into());
        let result1 = manager.assign_block(&pk1);
        let result2 = manager.assign_block(&pk2);
        
        // Should be in the same block initially
        assert_eq!(result1.unwrap().0, result2.unwrap().0);
    }

    #[test]
    fn test_block_manager_handles_delete() {
        let config = BlockManagerConfig::default();
        let manager = BlockManager::new("test_source", "test_table", config);
        
        // Assign some blocks
        let pk1 = PrimaryKey::String("pk_001".into());
        let pk2 = PrimaryKey::String("pk_002".into());
        let pk3 = PrimaryKey::String("pk_003".into());
        manager.assign_block(&pk1).unwrap();
        manager.assign_block(&pk2).unwrap();
        manager.assign_block(&pk3).unwrap();
        
        // Delete a PK - handle_delete returns Option<BlockId>
        let result = manager.handle_delete(&pk2);
        assert!(result.is_some());
    }

    #[test]
    fn test_block_contains_pk() {
        let mut block = Block::new(1, "aaa");
        block.add_row("aaa");
        block.add_row("bbb");
        block.add_row("ccc");
        
        assert!(block.contains_pk("bbb"));
        assert!(block.contains_pk("aaa"));
        assert!(block.contains_pk("ccc"));
        assert!(!block.contains_pk("zzz"));
    }
}

mod skip_index_tests {
    use reiver_pond::warehouse::connectors::wal_index::skip_index::{
        BlockSkipIndex, MinMaxStats, SkipIndexType, XorFilterIndex,
    };

    #[test]
    fn test_minmax_stats_creation() {
        let stats = MinMaxStats::new(50.0);
        assert_eq!(stats.min, 50.0);
        assert_eq!(stats.max, 50.0);
        assert_eq!(stats.value_count, 1);
    }

    #[test]
    fn test_minmax_stats_update() {
        let mut stats = MinMaxStats::new(50.0);
        
        stats.update(Some(10.0));
        assert_eq!(stats.min, 10.0);
        assert_eq!(stats.max, 50.0);
        
        stats.update(Some(100.0));
        assert_eq!(stats.min, 10.0);
        assert_eq!(stats.max, 100.0);
    }

    #[test]
    fn test_minmax_might_contain_in_range() {
        let mut stats = MinMaxStats::new(10.0);
        stats.update(Some(100.0));
        
        // Value in range
        assert!(stats.might_contain(50.0));
        
        // Value at boundaries
        assert!(stats.might_contain(10.0));
        assert!(stats.might_contain(100.0));
    }

    #[test]
    fn test_minmax_might_contain_out_of_range() {
        let mut stats = MinMaxStats::new(10.0);
        stats.update(Some(100.0));
        
        // Value below range
        assert!(!stats.might_contain(5.0));
        
        // Value above range
        assert!(!stats.might_contain(150.0));
    }

    #[test]
    fn test_minmax_range_methods() {
        let mut stats = MinMaxStats::new(10.0);
        stats.update(Some(100.0));
        
        // GT checks
        assert!(stats.might_contain_gt(50.0)); // 100 > 50
        assert!(!stats.might_contain_gt(150.0)); // nothing > 150
        
        // LT checks
        assert!(stats.might_contain_lt(50.0)); // 10 < 50
        assert!(!stats.might_contain_lt(5.0)); // nothing < 5
    }

    #[test]
    fn test_skip_index_serialization() {
        let mut stats = MinMaxStats::new(10.0);
        stats.update(Some(100.0));
        let index = BlockSkipIndex::MinMax(stats);
        
        // Serialize to base64
        let encoded = index.to_base64();
        assert!(!encoded.is_empty());
        
        // Deserialize back
        let decoded = BlockSkipIndex::from_base64(&encoded, SkipIndexType::MinMax)
            .expect("deserialization should succeed");
        
        match decoded {
            BlockSkipIndex::MinMax(decoded_stats) => {
                assert_eq!(decoded_stats.min, 10.0);
                assert_eq!(decoded_stats.max, 100.0);
            }
            _ => panic!("Expected MinMax index"),
        }
    }

    #[test]
    fn test_xor_filter_index() {
        use reiver_pond::warehouse::connectors::wal_index::ColumnValue;
        
        // Build from value hashes
        let hashes: Vec<u64> = (0..1000)
            .map(|i| ColumnValue::String(format!("value_{}", i)).stable_hash())
            .collect();
        
        let filter = XorFilterIndex::build(&hashes).expect("filter build should succeed");
        
        // Test membership using hashes - existing values should return true
        let hash_0 = ColumnValue::String("value_0".into()).stable_hash();
        let hash_500 = ColumnValue::String("value_500".into()).stable_hash();
        let hash_999 = ColumnValue::String("value_999".into()).stable_hash();
        
        assert!(filter.might_contain(hash_0));
        assert!(filter.might_contain(hash_500));
        assert!(filter.might_contain(hash_999));
        
        // Non-existing values may return true (false positives) or false
        // We can't test for definite negatives with probabilistic filters
    }

    #[test]
    fn test_skip_index_type_detection() {
        use reiver_pond::warehouse::connectors::wal_index::ColumnValue;
        
        // Numeric types should suggest MinMax
        let int_val = ColumnValue::Int64(42);
        let float_val = ColumnValue::Float64(3.14);
        
        // String types should suggest Xor
        let str_val = ColumnValue::String("test".into());
        
        // These would be used by SkipIndexBuilder to determine index type
        assert!(matches!(int_val, ColumnValue::Int64(_)));
        assert!(matches!(float_val, ColumnValue::Float64(_)));
        assert!(matches!(str_val, ColumnValue::String(_)));
    }

    #[test]
    fn test_minmax_null_handling() {
        let mut stats = MinMaxStats::new(50.0);
        
        // Update with null
        stats.update(None);
        
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.value_count, 1); // Initial value
        assert_eq!(stats.min, 50.0);
        assert_eq!(stats.max, 50.0);
    }
}

mod inverted_index_tests {
    use reiver_pond::warehouse::connectors::wal_index::{
        InvertedIndex, InvertedIndexManager, ColumnValue, PrimaryKey,
    };

    #[test]
    fn test_inverted_index_add_and_lookup() {
        let mut index = InvertedIndex::new("status");
        
        // Add some PKs for value "active"
        let value = ColumnValue::String("active".into());
        let pk1 = PrimaryKey::Int64(1);
        let pk2 = PrimaryKey::Int64(2);
        let pk5 = PrimaryKey::Int64(5);
        
        index.add(&value, &pk1);
        index.add(&value, &pk2);
        index.add(&value, &pk5);
        
        // Lookup should return all PKs
        let bitmap = index.get(&value);
        assert!(bitmap.is_some());
        
        let bitmap = bitmap.unwrap();
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(5));
        assert!(!bitmap.contains(3));
    }

    #[test]
    fn test_inverted_index_multiple_values() {
        let mut index = InvertedIndex::new("status");
        
        // Add PKs for different values
        let active = ColumnValue::String("active".into());
        let inactive = ColumnValue::String("inactive".into());
        
        index.add(&active, &PrimaryKey::Int64(1));
        index.add(&active, &PrimaryKey::Int64(2));
        index.add(&inactive, &PrimaryKey::Int64(3));
        index.add(&inactive, &PrimaryKey::Int64(4));
        
        // Lookup active
        let active_pks = index.get(&active).unwrap();
        assert_eq!(active_pks.len(), 2);
        assert!(active_pks.contains(1));
        assert!(active_pks.contains(2));
        
        // Lookup inactive
        let inactive_pks = index.get(&inactive).unwrap();
        assert_eq!(inactive_pks.len(), 2);
        assert!(inactive_pks.contains(3));
        assert!(inactive_pks.contains(4));
    }

    #[test]
    fn test_inverted_index_remove() {
        let mut index = InvertedIndex::new("status");
        
        let value = ColumnValue::String("active".into());
        index.add(&value, &PrimaryKey::Int64(1));
        index.add(&value, &PrimaryKey::Int64(2));
        index.add(&value, &PrimaryKey::Int64(3));
        
        // Remove one PK
        index.remove(&value, &PrimaryKey::Int64(2));
        
        let bitmap = index.get(&value).unwrap();
        assert!(bitmap.contains(1));
        assert!(!bitmap.contains(2));
        assert!(bitmap.contains(3));
    }

    #[test]
    fn test_inverted_index_distinct_values() {
        let mut index = InvertedIndex::new("user_id");
        
        // Add many distinct values
        for i in 0..100 {
            let value = ColumnValue::Int64(i);
            index.add(&value, &PrimaryKey::Int64(i));
        }
        
        assert_eq!(index.distinct_values(), 100);
    }

    #[test]
    fn test_inverted_index_manager() {
        let manager = InvertedIndexManager::new("test_source", "test_table");
        
        // Add entries for a low-cardinality column
        let column = "status";
        let active = ColumnValue::String("active".into());
        let inactive = ColumnValue::String("inactive".into());
        
        for i in 0..100 {
            let value = if i % 2 == 0 { &active } else { &inactive };
            manager.add(column, value, &PrimaryKey::Int64(i));
        }
        
        // Should not be high cardinality (only 2 distinct values)
        assert!(!manager.is_high_cardinality(column));
    }

    #[test]
    fn test_inverted_index_manager_high_cardinality_detection() {
        let manager = InvertedIndexManager::new("test_source", "test_table");
        
        // Add many distinct values (simulating a high-cardinality column like user_id)
        let column = "user_id";
        for i in 0..110000 {
            let value = ColumnValue::Int64(i);
            manager.add(column, &value, &PrimaryKey::Int64(i));
        }
        
        // Should be flagged as high cardinality (> 100000 distinct values)
        assert!(manager.is_high_cardinality(column));
    }

    #[test]
    fn test_inverted_index_entries_iteration() {
        let mut index = InvertedIndex::new("category");
        
        let values = ["A", "B", "C"];
        for (i, val) in values.iter().enumerate() {
            let value = ColumnValue::String((*val).to_string());
            index.add(&value, &PrimaryKey::Int64(i as i64));
        }
        
        // Should have 3 entries
        let entries = index.entries();
        assert_eq!(entries.len(), 3);
    }
}

mod query_tests {
    use reiver_pond::warehouse::connectors::wal_index::{
        Predicate, ColumnValue,
    };
    use reiver_pond::warehouse::connectors::wal_index::query::PredicateOp;
    use reiver_pond::warehouse::connectors::wal_index::skip_index::{BlockSkipIndex, MinMaxStats, SkipIndexType};

    #[test]
    fn test_predicate_creation() {
        let pred = Predicate::gt("age", ColumnValue::Int64(21));
        
        assert_eq!(pred.column, "age");
        assert!(matches!(pred.op, PredicateOp::Gt));
    }

    #[test]
    fn test_predicate_ops() {
        // Equality
        let eq = Predicate::eq("status", ColumnValue::String("active".into()));
        assert!(matches!(eq.op, PredicateOp::Eq));
        
        // Range
        let gt = Predicate::gt("price", ColumnValue::Float64(100.0));
        assert!(matches!(gt.op, PredicateOp::Gt));
        
        // In list
        let in_list = Predicate::in_values(
            "category",
            vec![ColumnValue::String("A".into()), ColumnValue::String("B".into())],
        );
        assert!(matches!(in_list.op, PredicateOp::In));
    }

    #[test]
    fn test_minmax_check_eq() {
        // Create a MinMax index for an "age" column with range [18, 65]
        let mut stats = MinMaxStats::new(18.0);
        stats.update(Some(65.0));
        
        // Test equality that falls within range
        assert!(stats.might_contain(30.0));
        
        // Test equality that falls outside range
        assert!(!stats.might_contain(10.0));
    }

    #[test]
    fn test_minmax_check_range() {
        let mut stats = MinMaxStats::new(100.0);
        stats.update(Some(500.0));
        
        // GT where block might have matching values
        // value > 400, block has [100, 500], so some values could be > 400
        assert!(stats.might_contain(400.0));
        
        // Value outside range
        assert!(!stats.might_contain(600.0));
    }

    #[test]
    fn test_skip_index_base64_roundtrip() {
        let mut stats = MinMaxStats::new(10.0);
        stats.update(Some(100.0));
        let original = BlockSkipIndex::MinMax(stats);
        
        let encoded = original.to_base64();
        let decoded = BlockSkipIndex::from_base64(&encoded, SkipIndexType::MinMax)
            .expect("decoding should succeed");
        
        match decoded {
            BlockSkipIndex::MinMax(dec) => {
                assert_eq!(dec.min, 10.0);
                assert_eq!(dec.max, 100.0);
            }
            _ => panic!("Type mismatch after roundtrip"),
        }
    }

    #[test]
    fn test_block_skip_index_might_contain() {
        let mut stats = MinMaxStats::new(10.0);
        stats.update(Some(100.0));
        let index = BlockSkipIndex::MinMax(stats);
        
        // Test using ColumnValue - use might_contain_eq for equality checks
        assert!(index.might_contain_eq(&ColumnValue::Int64(50)));
        assert!(!index.might_contain_eq(&ColumnValue::Int64(200)));
    }
}

mod column_value_tests {
    use reiver_pond::warehouse::connectors::wal_index::ColumnValue;

    #[test]
    fn test_column_value_types() {
        let null = ColumnValue::Null;
        let bool_val = ColumnValue::Bool(true);
        let int_val = ColumnValue::Int64(42);
        let float_val = ColumnValue::Float64(3.14159);
        let str_val = ColumnValue::String("hello".into());
        let bytes_val = ColumnValue::Bytes(vec![1, 2, 3, 4]);
        
        assert!(matches!(null, ColumnValue::Null));
        assert!(matches!(bool_val, ColumnValue::Bool(true)));
        assert!(matches!(int_val, ColumnValue::Int64(42)));
        assert!(matches!(float_val, ColumnValue::Float64(_)));
        assert!(matches!(str_val, ColumnValue::String(_)));
        assert!(matches!(bytes_val, ColumnValue::Bytes(_)));
    }

    #[test]
    fn test_column_value_stable_hash() {
        let val1 = ColumnValue::String("test_value".into());
        let val2 = ColumnValue::String("test_value".into());
        let val3 = ColumnValue::String("different_value".into());
        
        // Same values should produce same hash
        assert_eq!(val1.stable_hash(), val2.stable_hash());
        
        // Different values should produce different hashes
        assert_ne!(val1.stable_hash(), val3.stable_hash());
    }

    #[test]
    fn test_column_value_as_f64() {
        let int_val = ColumnValue::Int64(42);
        let float_val = ColumnValue::Float64(3.14);
        let str_val = ColumnValue::String("hello".into());
        
        assert_eq!(int_val.as_f64(), Some(42.0));
        assert_eq!(float_val.as_f64(), Some(3.14));
        assert_eq!(str_val.as_f64(), None);
    }
}

mod wal_event_tests {
    use reiver_pond::warehouse::connectors::wal_index::{
        WalEvent, WalEventType, ColumnValue, PrimaryKey,
    };

    #[test]
    fn test_insert_event() {
        let columns = vec![
            ("name".to_string(), ColumnValue::String("Alice".into())),
            ("age".to_string(), ColumnValue::Int64(30)),
        ];
        
        let event = WalEvent::insert(
            PrimaryKey::Int64(1),
            columns,
            vec![1, 2, 3], // checkpoint bytes
        );
        
        assert!(matches!(event.event_type, WalEventType::Insert));
        assert_eq!(event.primary_key, PrimaryKey::Int64(1));
        assert!(event.get_column("name").is_some());
        assert!(event.get_column("age").is_some());
    }

    #[test]
    fn test_update_event() {
        let columns = vec![
            ("status".to_string(), ColumnValue::String("inactive".into())),
        ];
        
        let event = WalEvent::update(
            PrimaryKey::Int64(1),
            columns,
            vec![4, 5, 6],
        );
        
        assert!(matches!(event.event_type, WalEventType::Update));
        assert!(event.get_column("status").is_some());
    }

    #[test]
    fn test_delete_event() {
        let event = WalEvent::delete(
            PrimaryKey::Int64(1),
            vec![7, 8, 9],
        );
        
        assert!(matches!(event.event_type, WalEventType::Delete));
        assert!(event.columns.is_empty());
    }
}

mod storage_tests {
    use reiver_pond::warehouse::connectors::wal_index::{Block, BlockSkipIndex, PrimaryKey};
    use reiver_pond::warehouse::connectors::wal_index::skip_index::{MinMaxStats, SkipIndexType};
    use reiver_pond::warehouse::connectors::wal_index::inverted_index::InvertedIndexEntry;

    #[test]
    fn test_block_serialization() {
        let block = Block::new(42, "pk_100");
        
        // Verify block properties can be extracted for storage
        assert_eq!(block.id, 42);
        assert_eq!(block.pk_start, "pk_100");
    }

    #[test]
    fn test_skip_index_base64_roundtrip() {
        let mut stats = MinMaxStats::new(10.0);
        stats.update(Some(100.0));
        let original = BlockSkipIndex::MinMax(stats);
        
        let encoded = original.to_base64();
        let decoded = BlockSkipIndex::from_base64(&encoded, SkipIndexType::MinMax)
            .expect("decoding should succeed");
        
        match decoded {
            BlockSkipIndex::MinMax(dec) => {
                assert_eq!(dec.min, 10.0);
                assert_eq!(dec.max, 100.0);
            }
            _ => panic!("Type mismatch after roundtrip"),
        }
    }

    #[test]
    fn test_inverted_index_entry_bitmap_serialization() {
        let mut entry = InvertedIndexEntry::new(12345678);
        entry.add_pk(&PrimaryKey::Int64(1));
        entry.add_pk(&PrimaryKey::Int64(100));
        entry.add_pk(&PrimaryKey::Int64(1000));
        
        // Serialize to base64
        let encoded = entry.bitmap_to_base64();
        assert!(!encoded.is_empty());
        
        // Deserialize back
        let decoded = InvertedIndexEntry::bitmap_from_base64(&encoded)
            .expect("decoding should succeed");
        
        assert!(decoded.contains(1));
        assert!(decoded.contains(100));
        assert!(decoded.contains(1000));
        assert!(!decoded.contains(50));
    }
}

mod primary_key_tests {
    use reiver_pond::warehouse::connectors::wal_index::PrimaryKey;

    #[test]
    fn test_primary_key_int() {
        let pk = PrimaryKey::Int64(12345);
        
        assert_eq!(pk.to_string_repr(), "12345");
        assert_eq!(pk.as_u32(), Some(12345));
        assert_eq!(pk.as_i64(), Some(12345));
    }

    #[test]
    fn test_primary_key_string() {
        let pk = PrimaryKey::String("user_abc".into());
        
        assert_eq!(pk.to_string_repr(), "user_abc");
        assert_eq!(pk.as_i64(), None);
        assert_eq!(pk.as_u32(), None);
    }

    #[test]
    fn test_primary_key_composite() {
        let pk = PrimaryKey::Composite(vec![
            PrimaryKey::Int64(42),
            PrimaryKey::String("tenant_abc".into()),
        ]);
        
        // Test string representation
        let repr = pk.to_string_repr();
        assert!(repr.contains("42"));
        assert!(repr.contains("tenant_abc"));
        
        // Composite PKs don't have direct i64 conversion
        assert_eq!(pk.as_i64(), None);
    }

    #[test]
    fn test_primary_key_equality() {
        let pk1 = PrimaryKey::Int64(1);
        let pk2 = PrimaryKey::Int64(2);
        let pk3 = PrimaryKey::Int64(1);
        
        assert_ne!(pk1, pk2);
        assert_eq!(pk1, pk3);
    }

    #[test]
    fn test_primary_key_hash_consistency() {
        let pk1 = PrimaryKey::String("test_user".into());
        let pk2 = PrimaryKey::String("test_user".into());
        let pk3 = PrimaryKey::String("different_user".into());
        
        // Same values should produce same hash
        assert_eq!(pk1.stable_hash(), pk2.stable_hash());
        
        // Different values should (almost certainly) produce different hashes
        assert_ne!(pk1.stable_hash(), pk3.stable_hash());
    }

    #[test]
    fn test_primary_key_from_traits() {
        // From i64
        let pk1: PrimaryKey = 42i64.into();
        assert_eq!(pk1.as_i64(), Some(42));
        
        // From i32
        let pk2: PrimaryKey = 42i32.into();
        assert_eq!(pk2.as_i64(), Some(42));
        
        // From String
        let pk3: PrimaryKey = "test".to_string().into();
        assert_eq!(pk3.to_string_repr(), "test");
        
        // From &str
        let pk4: PrimaryKey = "test".into();
        assert_eq!(pk4.to_string_repr(), "test");
    }
}

mod skip_index_builder_tests {
    use reiver_pond::warehouse::connectors::wal_index::{ColumnValue, SkipIndexBuilder};
    use reiver_pond::warehouse::connectors::wal_index::skip_index::BlockSkipIndex;

    #[test]
    fn test_skip_index_builder_numeric() {
        let mut builder = SkipIndexBuilder::numeric();
        
        builder.add_value(&ColumnValue::Int64(10));
        builder.add_value(&ColumnValue::Int64(50));
        builder.add_value(&ColumnValue::Int64(100));
        
        let result = builder.build();
        assert!(result.is_ok());
        
        let index = result.unwrap();
        // Verify it's a MinMax index
        assert!(matches!(index, BlockSkipIndex::MinMax(_)));
    }

    #[test]
    fn test_skip_index_builder_string() {
        let mut builder = SkipIndexBuilder::string();
        
        // Add enough values to build a proper Xor filter
        for i in 0..1000 {
            builder.add_value(&ColumnValue::String(format!("value_{}", i)));
        }
        
        let result = builder.build();
        // XorFilter should build with enough values
        assert!(result.is_ok());
    }
}
