//! Integration tests for the warehouse table loading and query rewriting pipeline.
//!
//! With copy-on-write sync, deduplication happens at write time, so the query
//! rewriter no longer wraps tables in argMax subqueries. These tests verify:
//!
//! 1. `load_project_tables_with_tier` correctly loads warm and hot table metadata
//! 2. The query rewriter produces plain s3() calls without dedup overhead
//! 3. Merge-on-write utilities correctly merge batches by PK
//!
//! Tests requiring PostgreSQL are `#[ignore]`-tagged to run only via `make itest`.
//!
//! To run:
//!   DATABASE_URL=postgresql://postgres:postgres@localhost:5432/reiver cargo test --test dedup_integration_tests -- --ignored --nocapture

use sqlx::PgPool;
use uuid::Uuid;

use reiver_pond::api::warehouse::load_project_tables_with_tier;
use reiver_pond::warehouse::query::rewriter::TableRewriter;

// ── Test Helpers ────────────────────────────────────────────────────────

async fn test_db() -> PgPool {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/reiver".to_string());
    PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test database")
}

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

async fn seed_warehouse_table(
    db: &PgPool,
    source_id: Uuid,
    table_name: &str,
) -> Uuid {
    let table_id = Uuid::new_v4();
    let r2_prefix = format!("warehouse/{}/{}", source_id, table_name);
    sqlx::query(
        r#"
        INSERT INTO warehouse_tables (id, source_id, name, schema, r2_prefix, primary_key_columns, sync_enabled, sync_state, created_at, updated_at)
        VALUES ($1, $2, $3, '{}'::jsonb, $4, '{}', true, 'committed', NOW(), NOW())
        ON CONFLICT (source_id, name) DO UPDATE SET
            sync_state = 'committed',
            updated_at = NOW()
        RETURNING id
        "#,
    )
    .bind(table_id)
    .bind(source_id)
    .bind(table_name)
    .bind(r2_prefix)
    .fetch_one(db)
    .await
    .map(|row| sqlx::Row::get(&row, "id"))
    .expect("Failed to seed warehouse_tables")
}

async fn cleanup_test_data(db: &PgPool, source_ids: &[Uuid]) {
    for &source_id in source_ids {
        let partition_ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM warehouse_partitions WHERE source_id = $1",
        )
        .bind(source_id)
        .fetch_all(db)
        .await
        .unwrap_or_default();

        for pid in &partition_ids {
            sqlx::query("DELETE FROM warehouse_partition_files WHERE partition_id = $1")
                .bind(pid)
                .execute(db)
                .await
                .ok();
        }

        sqlx::query("DELETE FROM warehouse_partitions WHERE source_id = $1")
            .bind(source_id)
            .execute(db)
            .await
            .ok();

        sqlx::query("DELETE FROM warehouse_tables WHERE source_id = $1")
            .bind(source_id)
            .execute(db)
            .await
            .ok();

        sqlx::query("DELETE FROM warehouse_sources WHERE id = $1")
            .bind(source_id)
            .execute(db)
            .await
            .ok();
    }
}

// ── Test: load_project_tables_with_tier returns warm tables ───────────────

#[tokio::test]
#[ignore]
async fn test_load_tables_returns_warm_tables() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();

    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "users").await;

    let (warm, hot, _backing) = load_project_tables_with_tier(&db, project_id)
        .await
        .expect("load_project_tables_with_tier failed");

    assert!(warm.contains_key("test_src.users"), "Should contain warm table");
    assert!(hot.is_empty(), "Should have no hot tables");

    cleanup_test_data(&db, &[source_id]).await;
}

// ── Test: load_project_tables_with_tier returns hot tables ───────────────

#[tokio::test]
#[ignore]
async fn test_load_tables_returns_hot_tables() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();

    let source_id = seed_source(&db, project_id, "test_src", "hot").await;
    seed_warehouse_table(&db, source_id, "events").await;

    let (warm, hot, _backing) = load_project_tables_with_tier(&db, project_id)
        .await
        .expect("load_project_tables_with_tier failed");

    assert!(warm.is_empty(), "Should have no warm tables");
    assert!(hot.contains_key("test_src.events"), "Should contain hot table");

    cleanup_test_data(&db, &[source_id]).await;
}

// ── Test: query rewrite produces plain s3() without argMax ───────────────

#[tokio::test]
#[ignore]
async fn test_rewrite_produces_plain_s3_calls() {
    let db = test_db().await;
    let project_id = Uuid::new_v4();

    let source_id = seed_source(&db, project_id, "test_src", "warm").await;
    seed_warehouse_table(&db, source_id, "users").await;

    let (warm_tables, _hot, _backing) = load_project_tables_with_tier(&db, project_id)
        .await
        .expect("load_project_tables_with_tier failed");

    let rewriter = TableRewriter::new("test_collection");
    let sql = "SELECT id, name FROM test_src.users WHERE id > 10";
    let result = rewriter
        .rewrite(sql, &warm_tables)
        .expect("rewrite failed");

    assert!(result.contains("s3("), "Should contain s3() call: {}", result);
    assert!(!result.contains("argMax"), "Should NOT contain argMax (no query-time dedup): {}", result);
    assert!(!result.contains("GROUP BY"), "Should NOT contain GROUP BY: {}", result);

    cleanup_test_data(&db, &[source_id]).await;
}

// ── Unit tests: merge_batches_by_pk ──────────────────────────────────────

#[test]
fn test_merge_batches_by_pk_new_rows_replace_existing() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use reiver_pond::warehouse::sync::merge::merge_batches_by_pk;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let existing = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    ).unwrap();

    let new_data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![2, 4])),
            Arc::new(StringArray::from(vec!["Bobby", "Dave"])),
        ],
    ).unwrap();

    let result = merge_batches_by_pk(&[existing], &[new_data], &["id".to_string()]).unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4); // id=1,3 kept from old + id=2,4 from new

    let mut all_rows: Vec<(i64, String)> = Vec::new();
    for batch in &result {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            all_rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    all_rows.sort_by_key(|(id, _)| *id);

    assert_eq!(all_rows[0], (1, "Alice".to_string()));
    assert_eq!(all_rows[1], (2, "Bobby".to_string())); // replaced
    assert_eq!(all_rows[2], (3, "Carol".to_string()));
    assert_eq!(all_rows[3], (4, "Dave".to_string())); // new
}

#[test]
fn test_merge_batches_no_pk_appends() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use reiver_pond::warehouse::sync::merge::merge_batches_by_pk;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let existing = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice"])),
        ],
    ).unwrap();

    let new_data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alicia"])),
        ],
    ).unwrap();

    // No PK = simple append (both rows kept)
    let result = merge_batches_by_pk(&[existing], &[new_data], &[]).unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[test]
fn test_strip_metadata_columns() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use reiver_pond::warehouse::sync::merge::strip_metadata_columns;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("_dh_sync_version", DataType::Int64, false),
        Field::new("_dh_op", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int64Array::from(vec![5, 10])),
            Arc::new(StringArray::from(vec!["I", "U"])),
        ],
    ).unwrap();

    let result = strip_metadata_columns(&[batch]).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_columns(), 2);
    assert_eq!(result[0].schema().field(0).name(), "id");
    assert_eq!(result[0].schema().field(1).name(), "name");
    assert_eq!(result[0].num_rows(), 2);
}
