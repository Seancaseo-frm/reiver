# Pond — Remaining Work

## Partially Implemented

### 1. Partition Discovery from Bucket

**Location**: `pond/src/api/warehouse.rs` — `list_partitions` endpoint (line ~2429)

The `list_partitions` API endpoint for `external_parquet` sources validates that the source exists but returns an empty `Vec`. It needs to use the `object_store` crate to list objects under the source's bucket prefix, parse Hive-style partition patterns from file paths (e.g. `key=value/`), group files by partition, and return the partition list with file counts and sizes.

Without this, users cannot browse partitions for external Parquet sources through the UI — they must know exactly which tables exist.

---

### 2. Copy Stats from Shared Estimator

**Location**: `pond/src/api/warehouse.rs` — `explain_query` endpoint (line ~4640)

The `explain_query` handler creates a fresh `QueryCostEstimator` with no table statistics instead of copying accumulated stats (row counts, sizes, etc.) from the application-wide `warehouse_cost_estimator`. The shared estimator is acquired via a read lock and then immediately dropped without transferring data.

Either copy/clone the accumulated `TableStats` into the per-request estimator, or restructure to pass a reference to the shared estimator directly. Without this, the explain endpoint produces cost estimates based on zero or default statistics.

---

### 3. Storage Tier Lifecycle Worker

**Location**: `pond/src/warehouse/sync/lifecycle_worker.rs`

This worker is **code-complete**. It runs hourly, queries sources with `Lifecycle` tier policies, evaluates each partition's age against `after_days` transitions, publishes Kafka jobs for tier transitions, handles access-based promote/demote policies, and cleans up old access logs. There are no remaining TODOs in the file.

**Remaining**: End-to-end integration/manual testing with real Kafka and partitions with actual dates to verify transitions fire correctly.

---

## Future Phases (No Code or Stubs Only)

### 4. Text-to-SQL (Phase 2)

Phase 1 is **fully implemented** in `pond/src/warehouse/nl_query/`:
- LLM client calling the Flow gateway (GPT-4o default)
- `PromptBuilder` that loads the project catalog schema
- `SqlValidator` blocking dangerous operations and validating table references
- Retry loop (up to 3 LLM attempts with error context fed back)
- Security: SQL rewritten for project isolation, capped at 1000 rows, 30s timeout, 50MB memory

Phase 2 would add (no code exists):
- Conversation history / multi-turn queries (follow-ups like "now filter that by region")
- Query suggestions based on schema
- Caching SQL for repeated questions
- Prompt fine-tuning using query history
- Additional model support beyond GPT-4o

---

### 5. Full-Text Search on Data Content

**No code exists.** This would be a feature for searching across actual data stored in the warehouse (not just schema names), likely using ClickHouse full-text indexing or a dedicated search index. Entirely greenfield.

---

### 6. Migration Wizard (Phase 4)

**No code exists.** No files, stubs, or references anywhere in `pond/src`. This would be a guided UI/API flow to help users migrate data between source types or tiers, or to onboard data from external systems into Pond. Entirely greenfield.

---

### 7. AI Config Analyzer Stubs

**Location**: `pond/src/warehouse/ai_config/analyzer.rs`

The framework is built — `ConfigAnalyzer` takes a source, builds a `DataProfile`, and passes it to an `AIConfigProvider` to generate `ConfigRecommendation`s. The `analyze_with_stats` path works (has a passing unit test). The two real-data paths are stubs returning empty profiles:

- **`build_object_storage_profile`** (line ~143): Returns `DataProfile::new(source_id)` instead of listing bucket files and reading Parquet footers for schema/statistics.
- **`build_clickhouse_profile`** (line ~163): Returns `DataProfile::new(source_id)` instead of querying ClickHouse `system.columns` and `system.parts`.

**Remaining**: Implement actual metadata sampling — list files from the bucket (or query ClickHouse system tables), read Parquet footers, extract column-level statistics (cardinality, min/max, null ratios), and populate the `DataProfile`.

---

### 8. FST Schema Index for Autocomplete

**Index implementation**: `pond/src/warehouse/indexes/schema_index.rs` — **fully implemented** with tests. Builds an FST from table/column names and supports prefix-based autocomplete.

**API wiring**: `pond/src/api/warehouse.rs` — `autocomplete` endpoint (line ~4758) returns empty results with a TODO comment.

**Remaining**: Load the project's tables/columns, build a `SchemaIndex` (or keep a cached one in `PondState`), and call `schema_index.autocomplete(&params.prefix)` in the handler. Decide on a caching/refresh strategy so the index is not rebuilt on every request.
