# UDF Transactional Batch Processing

**Status:** Level 1 Implemented | Level 2 & 3 Planned  
**Last Updated:** February 2026

---

## 1. Overview

UDF jobs in Reiver follow a pipeline pattern: read batches from a source connector, transform each batch through a compiled Go-to-Wasm UDF, and write the output to a sink connector. This pipeline has several failure points:

- The UDF may error or exceed its fuel/time budget mid-stream.
- The sink write may fail partway through (network error, constraint violation, disk full).
- The server process may crash at any point during execution.

Without transactional guarantees, these failures can leave partial or duplicate data in the sink, corrupt downstream analytics, and leave run records in a permanently stale state.

This document defines three incremental levels of transactional guarantees for UDF job writes, the connector tiers that determine which guarantees are achievable per sink type, and the migration path from Level 1 through Level 3.

---

## 2. Guarantee Levels

### Level 1 -- All-or-Nothing (Implemented)

**Goal:** Either all output rows from a job run are visible in the sink, or none are.

**How it works:**

- The job executor buffers all transformed output batches in memory.
- If the UDF errors on any batch, execution stops and `write_table()` is never called. No partial output reaches the sink.
- Database connectors (PostgreSQL) wrap the entire `write_table()` call in a SQL transaction (`BEGIN` / `COMMIT`). If any INSERT chunk fails, the transaction rolls back and zero rows are visible.
- On server startup, any `warehouse_udf_runs` rows stuck in `running` status are marked as `crashed` with an explanatory error message.
- When a sink does not support transactional writes, the executor logs a warning so operators are aware of the weaker guarantee.

**Limitations:**

- If the server crashes after the sink transaction commits but before the run status is updated to `succeeded`, the run will be marked `crashed` on restart even though the data was written. Users may re-trigger the job and get duplicate data.
- Non-database sinks (APIs, files) cannot roll back partial writes, so the all-or-nothing guarantee applies only at the executor level (don't call `write_table` if the UDF errored).

**Implementation references:**

- `PostgresConnector::write_table` -- uses `pool.begin()` / `tx.commit()`.
- `Connector::supports_transactional_write()` -- trait method returning `false` by default, overridden to `true` by PostgreSQL.
- `JobExecutor::cleanup_stale_runs()` -- called on startup.

---

### Level 2 -- Exactly-Once / Idempotent Writes (Future)

**Goal:** A job run can be safely retried without producing duplicate data in the sink.

**How it works:**

Each job run already has a unique `run_id` (UUID). Level 2 extends this into the data path:

1. **Run ID stamping:** The executor adds a `_dh_run_id UUID` column to each output batch, populated with the run's ID.
2. **Idempotent write protocol:** Before writing, the sink deletes any existing rows with the same `_dh_run_id`, then inserts the new rows -- all within a single transaction. This is semantically a `DELETE WHERE _dh_run_id = $1` followed by `INSERT`.
3. **Automatic retry:** If a run fails during the write phase, the executor can automatically retry (up to a configurable limit) because the delete-then-insert pattern is safe to repeat.
4. **API sinks:** Connectors that support idempotency keys (e.g., Stripe) pass the `run_id` as the idempotency key. Connectors without idempotency support remain at Level 1 best-effort.

**Requirements:**

- New `Connector` trait method: `write_table_idempotent(table, batches, run_id)`.
- Sink tables must have a `_dh_run_id` column (auto-created by the connector on first write or via explicit setup).
- Retry configuration in `JobConfig` (max retries, backoff).
- `warehouse_udf_runs` schema extension to track retry count.

**Open questions:**

- Should `_dh_run_id` be a regular column or metadata? Regular column is simpler and queryable.
- How to handle sinks where we cannot add columns (e.g., pre-existing tables with strict schemas)?

---

### Level 3 -- Crash-Resilient Resume (Future)

**Goal:** If the server crashes mid-job, the partially completed work is not lost. On restart, the job resumes from where it left off.

**How it works:**

1. **Durable output buffer:** After the UDF transforms each batch, the executor writes the output batch to a durable staging area (the warehouse's own PostgreSQL or local Parquet files) before writing to the external sink.
2. **Checkpoint tracking:** Each buffered batch is assigned a sequence number. The `warehouse_udf_runs` row tracks the last successfully written sequence number.
3. **Two-phase protocol:**
   - Phase 1: Transform batch, write to staging buffer, record sequence number.
   - Phase 2: Read buffered batches, write to sink, advance checkpoint.
4. **Startup recovery:** On restart, `cleanup_stale_runs` is extended to scan for runs in `running` state that have buffered batches. Instead of marking them `crashed`, it resumes Phase 2 (write buffered data to sink, update status).
5. **Buffer cleanup:** After a run completes successfully, the staging buffer is deleted.

**Requirements:**

- New `warehouse_udf_run_batches` table or Parquet staging directory.
- Modified `JobExecutor::run()` to write-through the buffer.
- Recovery loop in `cleanup_stale_runs()`.
- Configuration for buffer storage backend (Postgres BYTEA vs. local Parquet vs. S3).

**Trade-offs:**

- Doubles the write I/O (once to buffer, once to sink). For large datasets, Parquet staging on local disk is more efficient than PostgreSQL BYTEA.
- Adds complexity to the executor. May not be needed until job volumes justify it.
- The recovery loop must handle the case where the sink connector's credentials have changed since the original run started.

---

## 3. Connector Guarantee Tiers

Different sink connector types have fundamentally different capabilities. The maximum achievable guarantee level depends on the sink.

| Tier | Connectors | Level 1 | Level 2 | Level 3 | Notes |
|------|-----------|---------|---------|---------|-------|
| **A -- Full Transactional** | PostgreSQL, MySQL, SQL Server | Native | Native | Full | SQL transactions provide atomicity. UPSERT/delete-insert provides idempotency. |
| **B -- Idempotent** | Stripe, Shopify (POST with idempotency key) | Best-effort | Via idempotency key | Full (via buffer) | Cannot roll back partial API calls, but retries are safe. |
| **C -- Best-effort** | Generic HTTP, webhooks, Salesforce | Best-effort | Not available | Via buffer only | No rollback, no idempotency. The executor prevents writes on UDF error, but a partial write failure cannot be undone. |
| **D -- Append-only** | File sinks (S3/Parquet), message queues (Kafka) | Via atomic rename | Requires consumer dedup | Full (via buffer) | Files can use write-to-temp-then-rename for atomicity. Message queues rely on consumer-side deduplication. |

**Tier A** connectors get the strongest guarantees at every level. They are the primary target for Level 1 and 2 implementation.

**Tier B** connectors can achieve Level 2 idempotency through vendor-specific APIs but cannot roll back partial failures. The durable buffer (Level 3) gives them crash resilience.

**Tier C** connectors have the weakest guarantees. Users writing to Tier C sinks should be informed via the API and UI that partial writes are possible on failure.

**Tier D** connectors are a special case. File-based sinks can achieve atomicity through write-then-rename patterns, but deduplication must happen downstream.

---

## 4. Future Connector Trait Evolution

The `Connector` trait will evolve incrementally as each level is implemented.

### Current (Level 1)

```rust
trait Connector {
    fn supports_write(&self) -> bool { false }
    fn supports_transactional_write(&self) -> bool { false }
    async fn write_table(&self, table: &str, batches: Vec<RecordBatch>) -> ConnectorResult<WriteResult>;
}
```

### Level 2 Additions

```rust
trait Connector {
    // ... existing methods ...

    /// The write guarantee tier this connector provides.
    fn write_guarantee_tier(&self) -> WriteGuaranteeTier {
        WriteGuaranteeTier::BestEffort  // Tier C default
    }

    /// Write with idempotency: safe to retry without creating duplicates.
    /// Database connectors implement as DELETE WHERE _dh_run_id = run_id + INSERT
    /// within a transaction. API connectors pass run_id as an idempotency key.
    async fn write_table_idempotent(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
        run_id: Uuid,
    ) -> ConnectorResult<WriteResult> {
        // Default: fall back to non-idempotent write
        self.write_table(table, batches).await
    }
}

enum WriteGuaranteeTier {
    FullTransactional,  // Tier A
    Idempotent,         // Tier B
    BestEffort,         // Tier C
    AppendOnly,         // Tier D
}
```

### Level 3 Additions

No additional trait changes needed. Level 3 is implemented entirely within the `JobExecutor` using a staging buffer and checkpoint system. The connector interface remains unchanged -- the executor writes from the buffer using the existing `write_table` or `write_table_idempotent` methods.

---

## 5. Migration Path

### From Level 1 to Level 2

1. Add the `WriteGuaranteeTier` enum and `write_guarantee_tier()` method to the `Connector` trait.
2. Implement `write_table_idempotent()` on `PostgresConnector` using `DELETE WHERE _dh_run_id` + `INSERT` in a transaction.
3. Update `JobExecutor::run()` to call `write_table_idempotent()` when the sink supports it.
4. Add retry logic to the executor with configurable max retries and exponential backoff.
5. Expose the guarantee tier in the API response for job configuration so the UI can inform users.

### From Level 2 to Level 3

1. Create the `warehouse_udf_run_batches` staging table (or Parquet staging directory).
2. Modify `JobExecutor::run()` to write each transformed batch to staging before accumulating for the sink write.
3. Extend `cleanup_stale_runs()` to detect runs with buffered batches and resume them instead of marking as `crashed`.
4. Add a configuration option to choose the staging backend (Postgres vs. local disk vs. S3).
5. Add buffer cleanup logic after successful runs and a TTL-based garbage collector for abandoned buffers.

### Backward Compatibility

- Level 2 and 3 are additive. Existing UDFs continue to work at Level 1 without configuration changes.
- The `write_table()` method remains the primary write interface. `write_table_idempotent()` is a separate opt-in method.
- Sinks that don't implement higher-level methods gracefully fall back to the lower level.
