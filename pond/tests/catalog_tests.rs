//! Catalog Service Tests
//!
//! Tests for the unified catalog and metadata feature including:
//! - Catalog types and serialization
//! - Type conversions

#[cfg(test)]
mod catalog_types_tests {
    use reiver_pond::warehouse::catalog::types::*;
    use uuid::Uuid;

    fn test_project_id() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    #[test]
    fn test_sync_status_from_str() {
        assert_eq!(SyncStatus::from_str("synced"), SyncStatus::Synced);
        assert_eq!(SyncStatus::from_str("syncing"), SyncStatus::Syncing);
        assert_eq!(SyncStatus::from_str("stale"), SyncStatus::Stale);
        assert_eq!(SyncStatus::from_str("error"), SyncStatus::Error);
        assert_eq!(SyncStatus::from_str("unknown"), SyncStatus::Unknown);
        assert_eq!(SyncStatus::from_str("invalid"), SyncStatus::Unknown);
    }

    #[test]
    fn test_sync_status_to_str() {
        assert_eq!(SyncStatus::Synced.as_str(), "synced");
        assert_eq!(SyncStatus::Syncing.as_str(), "syncing");
        assert_eq!(SyncStatus::Stale.as_str(), "stale");
        assert_eq!(SyncStatus::Error.as_str(), "error");
        assert_eq!(SyncStatus::Unknown.as_str(), "unknown");
    }

    #[test]
    fn test_sync_status_roundtrip() {
        for status in [
            SyncStatus::Synced,
            SyncStatus::Syncing,
            SyncStatus::Stale,
            SyncStatus::Error,
            SyncStatus::Unknown,
        ] {
            let s = status.as_str();
            let parsed = SyncStatus::from_str(s);
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_column_ref_parse() {
        // Valid three-part reference
        let col_ref = ColumnRef::parse("source.table.column");
        assert!(col_ref.is_some());
        let col_ref = col_ref.unwrap();
        assert_eq!(col_ref.source, "source");
        assert_eq!(col_ref.table, "table");
        assert_eq!(col_ref.column, "column");

        // Two-part reference (no column)
        let col_ref = ColumnRef::parse("source.table");
        assert!(col_ref.is_none());

        // Single part
        let col_ref = ColumnRef::parse("source");
        assert!(col_ref.is_none());
    }

    #[test]
    fn test_column_ref_fqn() {
        let col_ref = ColumnRef::new("postgres", "users", "id");
        assert_eq!(col_ref.fqn(), "postgres.users.id");
    }

    #[test]
    fn test_column_ref_table_ref() {
        let col_ref = ColumnRef::new("stripe", "customers", "email");
        let table_ref = col_ref.table_ref();
        assert_eq!(table_ref.source, "stripe");
        assert_eq!(table_ref.table, "customers");
    }

    #[test]
    fn test_table_ref_parse() {
        // Valid two-part reference
        let table_ref = TableRef::parse("source.table");
        assert!(table_ref.is_some());
        let table_ref = table_ref.unwrap();
        assert_eq!(table_ref.source, "source");
        assert_eq!(table_ref.table, "table");

        // Single part should fail
        let table_ref = TableRef::parse("source");
        assert!(table_ref.is_none());
    }

    #[test]
    fn test_table_ref_fqn() {
        let table_ref = TableRef::new("stripe", "customers");
        assert_eq!(table_ref.fqn(), "stripe.customers");
    }

    #[test]
    fn test_freshness_info_is_stale() {
        use chrono::{Duration, Utc};
        
        // Fresh data (recent sync) is not stale
        let mut fresh_info = FreshnessInfo::new();
        fresh_info.last_sync_at = Some(Utc::now() - Duration::minutes(5));
        fresh_info.sync_status = SyncStatus::Synced;
        
        assert!(!fresh_info.is_stale(Duration::hours(1)));
        assert!(fresh_info.is_stale(Duration::minutes(1)));
        
        // Old data is stale
        let mut stale_info = FreshnessInfo::new();
        stale_info.last_sync_at = Some(Utc::now() - Duration::hours(2));
        stale_info.sync_status = SyncStatus::Synced;
        
        assert!(stale_info.is_stale(Duration::hours(1)));
        
        // Never synced is stale
        let never_synced = FreshnessInfo::new();
        assert!(never_synced.is_stale(Duration::hours(1)));
    }

    #[test]
    fn test_freshness_staleness() {
        use chrono::{Duration, Utc};
        
        let mut info = FreshnessInfo::new();
        assert!(info.staleness().is_none(), "No sync should mean no staleness");
        
        info.last_sync_at = Some(Utc::now() - Duration::hours(2));
        let staleness = info.staleness();
        assert!(staleness.is_some());
        // Should be roughly 2 hours
        let hours = staleness.unwrap().num_hours();
        assert!(hours >= 1 && hours <= 3, "Staleness should be around 2 hours");
    }

    #[test]
    fn test_transformation_type_from_str() {
        assert_eq!(TransformationType::from_str("direct"), TransformationType::Direct);
        assert_eq!(TransformationType::from_str("derived"), TransformationType::Derived);
        assert_eq!(TransformationType::from_str("aggregated"), TransformationType::Aggregated);
        assert_eq!(TransformationType::from_str("joined"), TransformationType::Joined);
        assert_eq!(TransformationType::from_str("filtered"), TransformationType::Filtered);
        assert_eq!(TransformationType::from_str("unknown"), TransformationType::Unknown);
        assert_eq!(TransformationType::from_str("invalid"), TransformationType::Unknown);
    }

    #[test]
    fn test_transformation_type_roundtrip() {
        for trans_type in [
            TransformationType::Direct,
            TransformationType::Derived,
            TransformationType::Aggregated,
            TransformationType::Joined,
            TransformationType::Filtered,
            TransformationType::Unknown,
        ] {
            let s = trans_type.as_str();
            let parsed = TransformationType::from_str(s);
            assert_eq!(trans_type, parsed);
        }
    }

    #[test]
    fn test_relationship_type_from_str() {
        assert_eq!(RelationshipType::from_str("foreign_key"), RelationshipType::ForeignKey);
        assert_eq!(RelationshipType::from_str("inferred"), RelationshipType::Inferred);
        assert_eq!(RelationshipType::from_str("manual"), RelationshipType::Manual);
        assert_eq!(RelationshipType::from_str("invalid"), RelationshipType::Manual);
    }

    #[test]
    fn test_cardinality_from_str() {
        assert_eq!(Cardinality::from_str("one_to_one"), Cardinality::OneToOne);
        assert_eq!(Cardinality::from_str("one_to_many"), Cardinality::OneToMany);
        assert_eq!(Cardinality::from_str("many_to_one"), Cardinality::ManyToOne);
        assert_eq!(Cardinality::from_str("many_to_many"), Cardinality::ManyToMany);
        assert_eq!(Cardinality::from_str("unknown"), Cardinality::Unknown);
        assert_eq!(Cardinality::from_str("invalid"), Cardinality::Unknown);
    }

    #[test]
    fn test_lineage_discovery_method_from_str() {
        assert_eq!(LineageDiscoveryMethod::from_str("manual"), LineageDiscoveryMethod::Manual);
        assert_eq!(LineageDiscoveryMethod::from_str("inferred"), LineageDiscoveryMethod::Inferred);
        assert_eq!(LineageDiscoveryMethod::from_str("query_analysis"), LineageDiscoveryMethod::QueryAnalysis);
        assert_eq!(LineageDiscoveryMethod::from_str("sync"), LineageDiscoveryMethod::Sync);
    }

    #[test]
    fn test_cross_source_relationship() {
        let rel = CrossSourceRelationship::foreign_key(
            test_project_id(),
            ColumnRef::new("postgres", "orders", "customer_id"),
            ColumnRef::new("stripe", "customers", "id"),
        );
        
        assert_eq!(rel.from.fqn(), "postgres.orders");
        assert_eq!(rel.to.fqn(), "stripe.customers");
        assert!(rel.is_cross_source());
        assert_eq!(rel.relationship_type, RelationshipType::ForeignKey);
        assert_eq!(rel.cardinality, Cardinality::ManyToOne);
    }

    #[test]
    fn test_cross_source_relationship_same_source() {
        let rel = CrossSourceRelationship::foreign_key(
            test_project_id(),
            ColumnRef::new("postgres", "orders", "user_id"),
            ColumnRef::new("postgres", "users", "id"),
        );
        
        assert!(!rel.is_cross_source());
    }

    #[test]
    fn test_cross_source_relationship_description() {
        let rel = CrossSourceRelationship::foreign_key(
            test_project_id(),
            ColumnRef::new("stripe", "charges", "customer_id"),
            ColumnRef::new("stripe", "customers", "id"),
        );
        
        let desc = rel.description();
        assert!(desc.contains("charges"));
        assert!(desc.contains("customers"));
        assert!(desc.contains("customer_id"));
    }

    #[test]
    fn test_column_lineage() {
        let mut lineage = ColumnLineage::new(ColumnRef::new("warehouse", "fact_orders", "customer_name"));
        
        assert!(!lineage.has_lineage());
        
        lineage.add_source(LineageSource::new(
            ColumnRef::new("postgres", "customers", "name"),
            TransformationType::Direct,
        ));
        
        assert!(lineage.has_lineage());
        assert_eq!(lineage.sources.len(), 1);
        assert_eq!(lineage.sources[0].column.fqn(), "postgres.customers.name");
    }

    #[test]
    fn test_lineage_source_builder() {
        let source = LineageSource::new(
            ColumnRef::new("stripe", "charges", "amount"),
            TransformationType::Derived,
        )
        .with_sql("amount / 100")
        .with_confidence(0.85)
        .with_discovery(LineageDiscoveryMethod::QueryAnalysis);
        
        assert_eq!(source.transformation_type, TransformationType::Derived);
        assert_eq!(source.transformation_sql, Some("amount / 100".to_string()));
        assert_eq!(source.confidence, 0.85);
        assert_eq!(source.discovered_by, LineageDiscoveryMethod::QueryAnalysis);
    }

    #[test]
    fn test_lineage_source_confidence_clamping() {
        // Test that confidence is clamped between 0 and 1
        let source_high = LineageSource::new(
            ColumnRef::new("a", "b", "c"),
            TransformationType::Direct,
        ).with_confidence(1.5);
        assert_eq!(source_high.confidence, 1.0);
        
        let source_low = LineageSource::new(
            ColumnRef::new("a", "b", "c"),
            TransformationType::Direct,
        ).with_confidence(-0.5);
        assert_eq!(source_low.confidence, 0.0);
    }
}

#[cfg(test)]
mod summary_types_tests {
    use reiver_pond::warehouse::catalog::types::*;

    #[test]
    fn test_table_summary_fqn() {
        let summary = TableSummary {
            source_name: "stripe".to_string(),
            table_name: "customers".to_string(),
            column_count: 15,
            row_count_estimate: Some(50_000),
            size_bytes_estimate: Some(25_000_000),
            sync_status: SyncStatus::Synced,
            last_sync_at: Some(chrono::Utc::now()),
            description: None,
        };
        
        assert_eq!(summary.fqn(), "stripe.customers");
    }

    #[test]
    fn test_search_result_types() {
        // Test that all search result types are accessible
        let _table = SearchResultType::Table;
        let _column = SearchResultType::Column;
        let _relationship = SearchResultType::Relationship;
    }
}

#[cfg(test)]
mod serialization_tests {
    use reiver_pond::warehouse::catalog::types::*;
    use uuid::Uuid;

    fn test_project_id() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    #[test]
    fn test_column_ref_json_roundtrip() {
        let col = ColumnRef::new("postgres", "users", "id");
        let json = serde_json::to_string(&col).expect("Should serialize");
        let deserialized: ColumnRef = serde_json::from_str(&json).expect("Should deserialize");
        
        assert_eq!(col, deserialized);
    }

    #[test]
    fn test_table_ref_json_roundtrip() {
        let table = TableRef::new("stripe", "customers");
        let json = serde_json::to_string(&table).expect("Should serialize");
        let deserialized: TableRef = serde_json::from_str(&json).expect("Should deserialize");
        
        assert_eq!(table, deserialized);
    }

    #[test]
    fn test_sync_status_json_roundtrip() {
        for status in [
            SyncStatus::Synced,
            SyncStatus::Syncing,
            SyncStatus::Stale,
            SyncStatus::Error,
            SyncStatus::Unknown,
        ] {
            let json = serde_json::to_string(&status).expect("Should serialize");
            let deserialized: SyncStatus = serde_json::from_str(&json).expect("Should deserialize");
            assert_eq!(status, deserialized);
        }
    }

    #[test]
    fn test_column_lineage_json_roundtrip() {
        let mut lineage = ColumnLineage::new(ColumnRef::new("warehouse", "dim_customers", "full_name"));
        
        lineage.add_source(
            LineageSource::new(
                ColumnRef::new("postgres", "users", "first_name"),
                TransformationType::Derived,
            )
            .with_sql("CONCAT(first_name, ' ', last_name)")
            .with_discovery(LineageDiscoveryMethod::QueryAnalysis)
        );
        
        let json = serde_json::to_string(&lineage).expect("Should serialize");
        let deserialized: ColumnLineage = serde_json::from_str(&json).expect("Should deserialize");
        
        assert_eq!(deserialized.target.fqn(), lineage.target.fqn());
        assert_eq!(deserialized.sources.len(), 1);
        assert_eq!(deserialized.sources[0].transformation_sql, lineage.sources[0].transformation_sql);
    }

    #[test]
    fn test_cross_source_relationship_json_roundtrip() {
        let rel = CrossSourceRelationship::foreign_key(
            test_project_id(),
            ColumnRef::new("postgres", "orders", "customer_id"),
            ColumnRef::new("stripe", "customers", "id"),
        ).with_name("fk_orders_customers")
         .with_confidence(0.85);
        
        let json = serde_json::to_string(&rel).expect("Should serialize");
        let deserialized: CrossSourceRelationship = serde_json::from_str(&json).expect("Should deserialize");
        
        assert_eq!(deserialized.from.fqn(), rel.from.fqn());
        assert_eq!(deserialized.to.fqn(), rel.to.fqn());
        assert_eq!(deserialized.confidence, rel.confidence);
        assert_eq!(deserialized.is_validated, rel.is_validated);
    }

    #[test]
    fn test_freshness_info_json_roundtrip() {
        let mut info = FreshnessInfo::new();
        info.sync_status = SyncStatus::Synced;
        info.last_sync_at = Some(chrono::Utc::now());
        info.row_count_estimate = Some(100_000);
        info.size_bytes_estimate = Some(50_000_000);
        
        let json = serde_json::to_string(&info).expect("Should serialize");
        let deserialized: FreshnessInfo = serde_json::from_str(&json).expect("Should deserialize");
        
        assert_eq!(deserialized.sync_status, info.sync_status);
        assert_eq!(deserialized.row_count_estimate, info.row_count_estimate);
        assert_eq!(deserialized.size_bytes_estimate, info.size_bytes_estimate);
    }
}
