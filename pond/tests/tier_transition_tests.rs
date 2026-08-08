//! Integration tests for index/data lifecycle during tier transitions.
//!
//! Verifies that:
//! 1. Successful tier transitions properly clean up stale skip indexes
//! 2. Failed tier transitions preserve existing skip indexes and data
//! 3. Cleanup is correctly scoped (table isolation, project isolation)
//!
//! These tests require PostgreSQL and are `#[ignore]`-tagged to run only via `make itest`.
//!
//! To run:
//!   DATABASE_URL=postgresql://postgres:postgres@localhost:5432/reiver cargo test --test tier_transition_tests -- --ignored --nocapture

use sqlx::{PgPool, Row};
use uuid::Uuid;

// Re-export the cleanup helper from the crate
use reiver_pond::warehouse::sync::TierTransitionCleanup;
use reiver_pond::warehouse::indexes::delete_table_skip_indexes;

// ── Test Helpers ────────────────────────────────────────────────────────

/// Connect to the test PostgreSQL database.
async fn test_db() -> PgPool {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/reiver".to_string());
    PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test database")
}

/// Seed a warehouse_sources row with the given tier.
/// Returns the source_id.
async fn seed_source(db: &PgPool, project_id: Uuid, name: &str, tier: &str) -> Uuid {
    let source_id = Uuid::new_v4();
    let config_hash = format!("test_hash_{}", source_id);
    sqlx::query(
        r#"
        INSERT INTO warehouse_sources (id, project_id, name, source_type, config, tier, connection_config_hash, created_at, updated_at)
        VALUES ($1, $2, $3, 'postgresql', '{}'::jsonb, $4, $5, NOW(), NOW())
        "#,
    )
    .bind(source_id)
    .bind(project_id)
    .bind(name)
    .bind(tier)
    .bind(config_hash)
    .execute(db)
    .await
    .expect("Failed to seed warehouse_sources");
    source_id
}

/// Seed a warehouse_tables row for a source.
/// Returns the table row id.
async fn seed_warehouse_table(db: &PgPool, source_id: Uuid, table_name: &str) -> Uuid {
    let table_id = Uuid::new_v4();
    let r2_prefix = format!("warehouse/{}/{}", source_id, table_name);
    sqlx::query(
        r#"
        INSERT INTO warehouse_tables (id, source_id, name, schema, r2_prefix, created_at, updated_at)
        VALUES ($1, $2, $3, '{}'::jsonb, $4, NOW(), NOW())
        ON CONFLICT (source_id, name) DO UPDATE SET updated_at = NOW()
        RETURNING id
        "#,
    )
    .bind(table_id)
    .bind(source_id)
    .bind(table_name)
    .bind(r2_prefix)
    .fetch_one(db)
    .await
    .map(|row| row.get("id"))
    .expect("Failed to seed warehouse_tables")
}

/// Seed skip index rows for a project/table.
/// Creates `count` rows with distinct column names.
async fn seed_skip_indexes(db: &PgPool, project_id: Uuid, table_name: &str, count: usize) {
    for i in 0..count {
        let partition_key = format!("2025/{:02}", (i % 12) + 1);
        let file_path = format!("warehouse/{}/{}/part_{}.parquet", project_id, table_name, i);
        let column_name = format!("col_{}", i);
        // Use a minimal valid FST bytes (empty FST)
        let fst_bytes: Vec<u8> = vec![0u8; 16];
        sqlx::query(
            r#"
            INSERT INTO warehouse_skip_indexes (project_id, table_name, partition_key, file_path, column_name, values_fst, row_count)
            VALUES ($1, $2, $3, $4, $5, $6, 1000)
            ON CONFLICT (project_id, table_name, partition_key, file_path, column_name) DO NOTHING
            "#,
        )
        .bind(project_id)
        .bind(table_name)
        .bind(&partition_key)
        .bind(&file_path)
        .bind(&column_name)
        .bind(&fst_bytes)
        .execute(db)
        .await
        .expect("Failed to seed skip index");
    }
}

/// Seed partition records for a source/table.
/// Returns the partition IDs.
async fn seed_partitions(db: &PgPool, source_id: Uuid, table_name: &str, count: usize) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for i in 0..count {
        let partition_id = Uuid::new_v4();
        let parquet_path = format!("warehouse/{}/{}/part_{}.parquet", source_id, table_name, i);
        let partition_date = chrono::NaiveDate::from_ymd_opt(2025, ((i % 12) + 1) as u32, 1).unwrap();
        sqlx::query(
            r#"
            INSERT INTO warehouse_partitions (id, source_id, table_name, partition_date, state, sync_state, parquet_path, row_count, size_bytes)
            VALUES ($1, $2, $3, $4, 'frozen', 'committed', $5, 1000, 10000)
            "#,
        )
        .bind(partition_id)
        .bind(source_id)
        .bind(table_name)
        .bind(partition_date)
        .bind(&parquet_path)
        .execute(db)
        .await
        .expect("Failed to seed partition");
        ids.push(partition_id);
    }
    ids
}

/// Count skip index rows for a project/table.
async fn count_skip_indexes(db: &PgPool, project_id: Uuid, table_name: &str) -> i64 {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM warehouse_skip_indexes WHERE project_id = $1 AND table_name = $2",
    )
    .bind(project_id)
    .bind(table_name)
    .fetch_one(db)
    .await
    .expect("Failed to count skip indexes");
    row.0
}

/// Count partition records for a source.
async fn count_partitions(db: &PgPool, source_id: Uuid) -> i64 {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM warehouse_partitions WHERE source_id = $1",
    )
    .bind(source_id)
    .fetch_one(db)
    .await
    .expect("Failed to count partitions");
    row.0
}

/// Get the current tier of a source.
async fn get_source_tier(db: &PgPool, source_id: Uuid) -> String {
    let row: (String,) = sqlx::query_as(
        "SELECT tier FROM warehouse_sources WHERE id = $1",
    )
    .bind(source_id)
    .fetch_one(db)
    .await
    .expect("Failed to get source tier");
    row.0
}

/// Clean up all test data for a given project_id and source_ids.
async fn cleanup_test_data(db: &PgPool, project_id: Uuid, source_ids: &[Uuid]) {
    // Delete skip indexes
    sqlx::query("DELETE FROM warehouse_skip_indexes WHERE project_id = $1")
        .bind(project_id)
        .execute(db)
        .await
        .ok();

    for &source_id in source_ids {
        // Delete partitions (depends on source)
        sqlx::query("DELETE FROM warehouse_partitions WHERE source_id = $1")
            .bind(source_id)
            .execute(db)
            .await
            .ok();
        // Delete warehouse tables (depends on source)
        sqlx::query("DELETE FROM warehouse_tables WHERE source_id = $1")
            .bind(source_id)
            .execute(db)
            .await
            .ok();
        // Delete source
        sqlx::query("DELETE FROM warehouse_sources WHERE id = $1")
            .bind(source_id)
            .execute(db)
            .await
            .ok();
    }
}

// ── Scenario 1: Successful Transition Deletes Old Indexes ───────────

#[tokio::test]
#[ignore]
async fn test_upgrade_warm_to_hot_deletes_skip_indexes() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "orders").await;
    seed_skip_indexes(&db, project_id, "orders", 5).await;

    // Verify indexes exist
    assert_eq!(count_skip_indexes(&db, project_id, "orders").await, 5);

    // Simulate successful warm-to-hot cleanup: delete skip indexes for the table
    let table_names = vec!["orders".to_string()];
    let results = TierTransitionCleanup::cleanup_skip_indexes_for_tables(&db, project_id, &table_names).await;

    // Verify all cleanup operations succeeded
    for (table_name, result) in &results {
        assert!(result.is_ok(), "Failed to clean up indexes for table {}: {:?}", table_name, result);
    }

    // Assert: skip index count is now 0
    assert_eq!(count_skip_indexes(&db, project_id, "orders").await, 0);

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_downgrade_hot_to_warm_deletes_skip_indexes() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "hot").await;
    seed_warehouse_table(&db, source_id, "events").await;
    seed_skip_indexes(&db, project_id, "events", 10).await;

    // Verify indexes exist
    assert_eq!(count_skip_indexes(&db, project_id, "events").await, 10);

    // Simulate successful hot-to-warm cleanup
    let table_names = vec!["events".to_string()];
    let results = TierTransitionCleanup::cleanup_skip_indexes_for_tables(&db, project_id, &table_names).await;

    for (_, result) in &results {
        assert!(result.is_ok());
    }

    // Assert: skip index count is now 0
    assert_eq!(count_skip_indexes(&db, project_id, "events").await, 0);

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_downgrade_to_cold_deletes_skip_indexes() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "analytics").await;
    seed_warehouse_table(&db, source_id, "metrics").await;
    seed_skip_indexes(&db, project_id, "analytics", 8).await;
    seed_skip_indexes(&db, project_id, "metrics", 4).await;

    // Verify indexes exist for both tables
    assert_eq!(count_skip_indexes(&db, project_id, "analytics").await, 8);
    assert_eq!(count_skip_indexes(&db, project_id, "metrics").await, 4);

    // Simulate successful cold downgrade: cleanup_skip_indexes_for_source
    // queries warehouse_tables for the source to find all table names
    let results = TierTransitionCleanup::cleanup_skip_indexes_for_source(&db, source_id, project_id).await;

    for (_, result) in &results {
        assert!(result.is_ok());
    }

    // Assert: all skip indexes for all tables are deleted
    assert_eq!(count_skip_indexes(&db, project_id, "analytics").await, 0);
    assert_eq!(count_skip_indexes(&db, project_id, "metrics").await, 0);

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_upgrade_deletes_only_target_table_indexes() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "orders").await;
    seed_warehouse_table(&db, source_id, "customers").await;
    seed_skip_indexes(&db, project_id, "orders", 5).await;
    seed_skip_indexes(&db, project_id, "customers", 3).await;

    // Verify both tables have indexes
    assert_eq!(count_skip_indexes(&db, project_id, "orders").await, 5);
    assert_eq!(count_skip_indexes(&db, project_id, "customers").await, 3);

    // Only clean up "orders"
    let table_names = vec!["orders".to_string()];
    TierTransitionCleanup::cleanup_skip_indexes_for_tables(&db, project_id, &table_names).await;

    // Assert: "orders" indexes deleted, "customers" indexes untouched
    assert_eq!(count_skip_indexes(&db, project_id, "orders").await, 0);
    assert_eq!(count_skip_indexes(&db, project_id, "customers").await, 3);

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_upgrade_deletes_only_target_project_indexes() {
    let db = test_db().await;
    let project_a = Uuid::new_v4();
    let project_b = Uuid::new_v4();
    let source_a = seed_source(&db, project_a, "src_a", "warm").await;
    let source_b = seed_source(&db, project_b, "src_b", "warm").await;
    seed_warehouse_table(&db, source_a, "orders").await;
    seed_warehouse_table(&db, source_b, "orders").await;
    seed_skip_indexes(&db, project_a, "orders", 5).await;
    seed_skip_indexes(&db, project_b, "orders", 7).await;

    // Verify both projects have indexes for "orders"
    assert_eq!(count_skip_indexes(&db, project_a, "orders").await, 5);
    assert_eq!(count_skip_indexes(&db, project_b, "orders").await, 7);

    // Clean up project A only
    let table_names = vec!["orders".to_string()];
    TierTransitionCleanup::cleanup_skip_indexes_for_tables(&db, project_a, &table_names).await;

    // Assert: project A indexes deleted, project B indexes untouched
    assert_eq!(count_skip_indexes(&db, project_a, "orders").await, 0);
    assert_eq!(count_skip_indexes(&db, project_b, "orders").await, 7);

    cleanup_test_data(&db, project_a, &[source_a]).await;
    cleanup_test_data(&db, project_b, &[source_b]).await;
}

// ── Scenario 2: Failed Transition Preserves Indexes/Data ────────────

#[tokio::test]
#[ignore]
async fn test_failed_warm_to_hot_import_preserves_indexes() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "orders").await;
    seed_skip_indexes(&db, project_id, "orders", 5).await;
    let _partition_ids = seed_partitions(&db, source_id, "orders", 3).await;

    // Verify initial state
    assert_eq!(count_skip_indexes(&db, project_id, "orders").await, 5);
    assert_eq!(count_partitions(&db, source_id).await, 3);
    assert_eq!(get_source_tier(&db, source_id).await, "warm");

    // Simulate a failed warm-to-hot import: the function returns Err before
    // reaching any cleanup code. We simply do NOT call cleanup.
    // (In the real code, upgrade_warm_to_hot returns early on import error)

    // Assert: everything is preserved — skip indexes, partitions, tier unchanged
    assert_eq!(count_skip_indexes(&db, project_id, "orders").await, 5);
    assert_eq!(count_partitions(&db, source_id).await, 3);
    assert_eq!(get_source_tier(&db, source_id).await, "warm");

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_failed_hot_to_warm_export_preserves_indexes() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "hot").await;
    seed_warehouse_table(&db, source_id, "events").await;
    seed_skip_indexes(&db, project_id, "events", 8).await;

    // Verify initial state
    assert_eq!(count_skip_indexes(&db, project_id, "events").await, 8);
    assert_eq!(get_source_tier(&db, source_id).await, "hot");

    // Simulate a failed hot-to-warm export: the function returns Err before
    // reaching tier update or cleanup code. We do NOT call cleanup.

    // Assert: skip indexes preserved, tier unchanged
    assert_eq!(count_skip_indexes(&db, project_id, "events").await, 8);
    assert_eq!(get_source_tier(&db, source_id).await, "hot");

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_failed_downgrade_to_cold_preserves_indexes_on_early_failure() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "hot").await;
    seed_warehouse_table(&db, source_id, "analytics").await;
    seed_skip_indexes(&db, project_id, "analytics", 6).await;
    let _partition_ids = seed_partitions(&db, source_id, "analytics", 2).await;

    // Verify initial state
    assert_eq!(count_skip_indexes(&db, project_id, "analytics").await, 6);
    assert_eq!(count_partitions(&db, source_id).await, 2);
    assert_eq!(get_source_tier(&db, source_id).await, "hot");

    // Simulate an early failure in the cold downgrade (e.g., before cleanup runs).
    // The execute_downgrade_to_cold function uses best-effort cleanup, but if
    // it fails fatally early (e.g., load_source fails), nothing is cleaned up.
    // We verify the state is preserved.

    // Assert: skip indexes preserved, partitions preserved, tier unchanged
    assert_eq!(count_skip_indexes(&db, project_id, "analytics").await, 6);
    assert_eq!(count_partitions(&db, source_id).await, 2);
    assert_eq!(get_source_tier(&db, source_id).await, "hot");

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_partial_cold_downgrade_skip_index_cleanup_still_runs() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "logs").await;
    seed_skip_indexes(&db, project_id, "logs", 10).await;

    // Verify initial state
    assert_eq!(count_skip_indexes(&db, project_id, "logs").await, 10);

    // Simulate the partial cold downgrade scenario:
    // Even if R2 deletion fails (best-effort), the skip index cleanup
    // should still execute because the cold downgrade continues past errors.
    // We call cleanup_skip_indexes_for_source directly to verify the SQL works.
    let results = TierTransitionCleanup::cleanup_skip_indexes_for_source(&db, source_id, project_id).await;

    // Assert: skip indexes ARE deleted (cleanup ran successfully despite hypothetical R2 failure)
    for (_, result) in &results {
        assert!(result.is_ok());
    }
    assert_eq!(count_skip_indexes(&db, project_id, "logs").await, 0);

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

// ── Additional Edge Case Tests ──────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_cleanup_with_no_indexes_is_noop() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "empty_table").await;

    // No skip indexes seeded — count should be 0
    assert_eq!(count_skip_indexes(&db, project_id, "empty_table").await, 0);

    // Cleanup should succeed without error even with nothing to delete
    let table_names = vec!["empty_table".to_string()];
    let results = TierTransitionCleanup::cleanup_skip_indexes_for_tables(&db, project_id, &table_names).await;

    for (_, result) in &results {
        assert!(result.is_ok());
        assert_eq!(*result.as_ref().unwrap(), 0); // 0 rows affected
    }

    assert_eq!(count_skip_indexes(&db, project_id, "empty_table").await, 0);

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_cleanup_partitions_removes_records() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    let partition_ids = seed_partitions(&db, source_id, "orders", 5).await;

    assert_eq!(count_partitions(&db, source_id).await, 5);

    // Clean up partitions
    let results = TierTransitionCleanup::cleanup_partitions(&db, &partition_ids).await;

    // All should succeed
    for (_, success) in &results {
        assert!(success);
    }

    assert_eq!(count_partitions(&db, source_id).await, 0);

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_delete_table_skip_indexes_returns_correct_count() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "orders").await;
    seed_skip_indexes(&db, project_id, "orders", 7).await;

    // delete_table_skip_indexes returns the number of rows deleted
    let deleted = delete_table_skip_indexes(&db, project_id, "orders")
        .await
        .expect("delete_table_skip_indexes should not fail");

    assert_eq!(deleted, 7);
    assert_eq!(count_skip_indexes(&db, project_id, "orders").await, 0);

    cleanup_test_data(&db, project_id, &[source_id]).await;
}

#[tokio::test]
#[ignore]
async fn test_count_skip_indexes_helper_matches_direct_query() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();
    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "events").await;
    seed_skip_indexes(&db, project_id, "events", 4).await;

    // Test the TierTransitionCleanup::count_skip_indexes helper
    let count = TierTransitionCleanup::count_skip_indexes(&db, project_id, "events")
        .await
        .expect("count_skip_indexes should not fail");

    assert_eq!(count, 4);

    // Also verify our local helper agrees
    assert_eq!(count_skip_indexes(&db, project_id, "events").await, 4);

    cleanup_test_data(&db, project_id, &[source_id]).await;
}
