//! Warehouse PII Scanner
//!
//! Scans Arrow `RecordBatch` data during sync to detect PII in string columns.
//! Uses the same regex patterns as `reiver_core::pii::detect_pii` — only
//! actual data values are checked; no column-name heuristics.
//!
//! Findings are upserted to `warehouse_pii_findings` per (source, table, column).
//!
//! ## Architecture
//!
//! A dedicated background thread (`PiiScanWorker`) owns a rayon thread pool
//! with 4 threads. The sync executor sends batches through a bounded channel
//! (capacity 64); the worker dispatches CPU-bound scanning to rayon and hands
//! off the async DB upsert to the tokio runtime via `Handle::spawn`.

use std::collections::{HashMap, HashSet};
use std::sync::mpsc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use reiver_core::pii::{detect_pii, PiiType};
use sqlx::PgPool;
use tracing::info;
use uuid::Uuid;

/// Result of scanning a single column across all batches.
#[derive(Debug)]
pub struct PiiScanResult {
    pub column_name: String,
    pub pii_types: HashSet<PiiType>,
    pub rows_scanned: u64,
    pub rows_with_pii: u64,
}

/// Scan all string columns in the given `RecordBatch` slice for PII.
///
/// Iterates every row of every Utf8/LargeUtf8 column and runs `detect_pii`
/// on each value. Returns one `PiiScanResult` per column that had at least
/// one PII match.
pub fn scan_batches(batches: &[RecordBatch]) -> Vec<PiiScanResult> {
    if batches.is_empty() {
        return Vec::new();
    }

    let schema = batches[0].schema();

    // Identify string column indices
    let string_columns: Vec<(usize, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8))
        .map(|(i, f)| (i, f.name().clone()))
        .collect();

    if string_columns.is_empty() {
        return Vec::new();
    }

    // Accumulate per-column results
    let mut accumulators: HashMap<usize, PiiScanResult> = string_columns
        .iter()
        .map(|(idx, name)| {
            (
                *idx,
                PiiScanResult {
                    column_name: name.clone(),
                    pii_types: HashSet::new(),
                    rows_scanned: 0,
                    rows_with_pii: 0,
                },
            )
        })
        .collect();

    for batch in batches {
        for &(col_idx, _) in &string_columns {
            let column = batch.column(col_idx);
            let acc = accumulators.get_mut(&col_idx).unwrap();

            match column.data_type() {
                DataType::Utf8 => {
                    let array = column.as_string::<i32>();
                    for row_idx in 0..array.len() {
                        acc.rows_scanned += 1;
                        if array.is_null(row_idx) {
                            continue;
                        }
                        let value = array.value(row_idx);
                        let types = detect_pii(value);
                        if !types.is_empty() {
                            acc.rows_with_pii += 1;
                            acc.pii_types.extend(types);
                        }
                    }
                }
                DataType::LargeUtf8 => {
                    let array = column.as_string::<i64>();
                    for row_idx in 0..array.len() {
                        acc.rows_scanned += 1;
                        if array.is_null(row_idx) {
                            continue;
                        }
                        let value = array.value(row_idx);
                        let types = detect_pii(value);
                        if !types.is_empty() {
                            acc.rows_with_pii += 1;
                            acc.pii_types.extend(types);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Return only columns where PII was found
    accumulators
        .into_values()
        .filter(|r| !r.pii_types.is_empty())
        .collect()
}

/// Upsert PII scan results into `warehouse_pii_findings`.
///
/// Uses `ON CONFLICT (source_id, table_name, column_name)` to merge PII types
/// across syncs (union via JSONB `||` with deduplication).
pub async fn upsert_findings(
    db: &PgPool,
    source_id: Uuid,
    project_id: Uuid,
    source_name: &str,
    table_name: &str,
    results: &[PiiScanResult],
) -> Result<(), sqlx::Error> {
    for result in results {
        let pii_types_json: Vec<String> = result.pii_types.iter().map(|t| t.to_string()).collect();
        let pii_types = serde_json::to_value(&pii_types_json).unwrap_or_default();

        sqlx::query(
            r#"
            INSERT INTO warehouse_pii_findings (
                project_id, source_id, source_name, table_name, column_name,
                pii_types, total_rows_scanned, rows_with_pii
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (source_id, table_name, column_name) DO UPDATE SET
                pii_types = (
                    SELECT jsonb_agg(DISTINCT t)
                    FROM (
                        SELECT jsonb_array_elements_text(warehouse_pii_findings.pii_types) AS t
                        UNION
                        SELECT jsonb_array_elements_text($6::jsonb) AS t
                    ) combined
                ),
                total_rows_scanned = $7,
                rows_with_pii = $8,
                last_scanned_at = NOW()
            "#,
        )
        .bind(project_id)
        .bind(source_id)
        .bind(source_name)
        .bind(table_name)
        .bind(&result.column_name)
        .bind(&pii_types)
        .bind(result.rows_scanned as i64)
        .bind(result.rows_with_pii as i64)
        .execute(db)
        .await?;
    }

    info!(
        source_id = %source_id,
        table = table_name,
        findings = results.len(),
        "PII scan findings upserted"
    );

    Ok(())
}

/// A request to scan a batch of data for PII, sent through the worker channel.
pub struct PiiScanRequest {
    pub batches: Vec<RecordBatch>,
    pub db: PgPool,
    pub source_id: Uuid,
    pub project_id: Uuid,
    pub source_name: String,
    pub table_name: String,
    /// The sync scope that produced these batches (e.g. "full", "time_based(30d)").
    /// Used for logging so operators can see which scope produced the latest findings.
    pub sync_scope: String,
    pub tokio_handle: tokio::runtime::Handle,
}

/// Background worker that processes PII scan requests on a dedicated rayon pool.
///
/// Owns a 4-thread rayon pool running on a separate OS thread. The sync executor
/// sends `PiiScanRequest`s through a bounded channel; this worker dispatches
/// scanning to rayon and hands the async DB upsert back to tokio.
///
/// Call [`PiiScanWorker::shutdown`] for graceful shutdown: it drops the sender,
/// waits for the channel to drain and in-flight rayon tasks to complete, then
/// joins the worker thread.
pub struct PiiScanWorker {
    sender: Option<mpsc::SyncSender<PiiScanRequest>>,
    worker_handle: Option<std::thread::JoinHandle<()>>,
}

impl PiiScanWorker {
    /// Spawn the background worker thread with a dedicated 4-thread rayon pool.
    ///
    /// The channel capacity is 64, which is generous — each request represents
    /// one table's worth of batches. If the scanner falls 64 tables behind, the
    /// sync thread blocks on `send`, providing natural backpressure so memory
    /// usage stays bounded.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::sync_channel::<PiiScanRequest>(64);

        let handle = std::thread::Builder::new()
            .name("pii-scan-worker".to_string())
            .spawn(move || {
                let pool = reiver_sdk::InstrumentedThreadPoolBuilder::new("pii-scanner")
                    .num_threads(4)
                    .thread_name(|i| format!("pii-scan-{i}"))
                    .build()
                    .expect("failed to build PII scan thread pool");

                while let Ok(req) = rx.recv() {
                    pool.spawn(move || {
                        let source_id = req.source_id;
                        let table_name = req.table_name.clone();
                        let row_count: usize = req.batches.iter().map(|b| b.num_rows()).sum();
                        let col_count: usize = req.batches.first()
                            .map(|b| b.schema().fields().iter()
                                .filter(|f| matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8))
                                .count())
                            .unwrap_or(0);

                        let sync_scope = req.sync_scope.clone();
                        let span = tracing::info_span!(
                            "warehouse.pii_scanner.scan_table",
                            %source_id,
                            table = %table_name,
                            rows = row_count,
                            string_columns = col_count,
                            sync_scope = %sync_scope,
                        );
                        let _guard = span.enter();

                        let start = std::time::Instant::now();
                        let pii_results = scan_batches(&req.batches);
                        let elapsed = start.elapsed();

                        if pii_results.is_empty() {
                            info!(
                                duration_ms = elapsed.as_millis() as u64,
                                "PII scan complete, no findings"
                            );
                            return;
                        }

                        let finding_count = pii_results.len();
                        let pii_types: Vec<String> = pii_results.iter()
                            .flat_map(|r| r.pii_types.iter().map(|t| t.to_string()))
                            .collect::<HashSet<_>>()
                            .into_iter()
                            .collect();

                        info!(
                            duration_ms = elapsed.as_millis() as u64,
                            findings = finding_count,
                            pii_types = ?pii_types,
                            "PII scan complete, upserting findings"
                        );

                        req.tokio_handle.spawn(async move {
                            let upsert_start = std::time::Instant::now();
                            if let Err(e) = upsert_findings(
                                &req.db,
                                req.source_id,
                                req.project_id,
                                &req.source_name,
                                &req.table_name,
                                &pii_results,
                            )
                            .await
                            {
                                tracing::warn!(
                                    source_id = %req.source_id,
                                    table = %req.table_name,
                                    error = %e,
                                    "Failed to upsert PII findings"
                                );
                            } else {
                                info!(
                                    source_id = %req.source_id,
                                    table = %req.table_name,
                                    duration_ms = upsert_start.elapsed().as_millis() as u64,
                                    "PII findings upserted"
                                );
                            }
                        });
                    });
                }

                // Channel closed — drain in-flight rayon tasks by running a
                // no-op closure on the pool. `install` blocks until all prior
                // `spawn` closures have finished.
                pool.install(|| {});
                info!("PII scan worker shutting down");
            })
            .expect("failed to spawn PII scan worker thread");

        Self {
            sender: Some(tx),
            worker_handle: Some(handle),
        }
    }

    /// Send a scan request to the background worker.
    ///
    /// Blocks if the channel is full (backpressure). Returns immediately
    /// on success. Logs a warning if the channel is closed.
    pub fn send(&self, request: PiiScanRequest) {
        if let Some(ref sender) = self.sender {
            if sender.send(request).is_err() {
                tracing::warn!("PII scan worker channel closed, dropping scan request");
            }
        } else {
            tracing::warn!("PII scan worker already shut down, dropping scan request");
        }
    }

    /// Gracefully shut down the worker.
    ///
    /// Drops the channel sender so the worker's `recv` loop exits, waits for
    /// all in-flight rayon tasks to complete via `pool.install(|| {})`, then
    /// joins the worker thread. This method blocks until shutdown is complete.
    pub fn shutdown(&mut self) {
        // Drop the sender to close the channel
        self.sender.take();

        // Join the worker thread — it will drain in-flight rayon tasks before exiting
        if let Some(handle) = self.worker_handle.take() {
            if let Err(e) = handle.join() {
                tracing::warn!("PII scan worker thread panicked during shutdown: {:?}", e);
            }
        }
    }
}

impl Drop for PiiScanWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(columns: Vec<(&str, Vec<Option<&str>>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns
            .into_iter()
            .map(|(_, values)| Arc::new(StringArray::from(values)) as Arc<dyn Array>)
            .collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    #[test]
    fn scan_empty_batches() {
        let results = scan_batches(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn scan_no_string_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let results = scan_batches(&[batch]);
        assert!(results.is_empty());
    }

    #[test]
    fn scan_clean_data() {
        let batch = make_batch(vec![
            ("name", vec![Some("Alice"), Some("Bob"), Some("Charlie")]),
            ("city", vec![Some("NYC"), Some("LA"), Some("Chicago")]),
        ]);
        let results = scan_batches(&[batch]);
        assert!(results.is_empty());
    }

    #[test]
    fn scan_detects_email() {
        let batch = make_batch(vec![
            ("contact", vec![Some("user@example.com"), Some("hello"), Some("admin@test.org")]),
        ]);
        let results = scan_batches(&[batch]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].column_name, "contact");
        assert!(results[0].pii_types.contains(&PiiType::Email));
        assert_eq!(results[0].rows_scanned, 3);
        assert_eq!(results[0].rows_with_pii, 2);
    }

    #[test]
    fn scan_detects_ssn() {
        let batch = make_batch(vec![
            ("data", vec![Some("SSN: 123-45-6789"), Some("no pii here")]),
        ]);
        let results = scan_batches(&[batch]);
        assert_eq!(results.len(), 1);
        assert!(results[0].pii_types.contains(&PiiType::Ssn));
        assert_eq!(results[0].rows_with_pii, 1);
    }

    #[test]
    fn scan_multiple_columns_mixed() {
        let batch = make_batch(vec![
            ("email", vec![Some("user@test.com"), Some("clean")]),
            ("notes", vec![Some("nothing here"), Some("all good")]),
        ]);
        let results = scan_batches(&[batch]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].column_name, "email");
    }

    #[test]
    fn scan_handles_nulls() {
        let batch = make_batch(vec![
            ("field", vec![None, Some("user@example.com"), None]),
        ]);
        let results = scan_batches(&[batch]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rows_scanned, 3);
        assert_eq!(results[0].rows_with_pii, 1);
    }

    #[test]
    fn scan_multiple_batches() {
        let batch1 = make_batch(vec![
            ("info", vec![Some("user@a.com")]),
        ]);
        let batch2 = make_batch(vec![
            ("info", vec![Some("clean"), Some("admin@b.com")]),
        ]);
        let results = scan_batches(&[batch1, batch2]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rows_scanned, 3);
        assert_eq!(results[0].rows_with_pii, 2);
    }

    #[test]
    fn scan_detects_credit_card() {
        let batch = make_batch(vec![
            ("payment", vec![Some("Card 4111-1111-1111-1111"), Some("cash")]),
        ]);
        let results = scan_batches(&[batch]);
        assert_eq!(results.len(), 1);
        assert!(results[0].pii_types.contains(&PiiType::CreditCard));
    }
}
