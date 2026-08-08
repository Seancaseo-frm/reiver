-- Reset Bitcoin sync state for the ClickHouse buffer table migration.
--
-- After deploying the new buffer-based sync code, re-enable by setting enabled = true.
--
-- MANUAL STEP REQUIRED: Delete all existing Bitcoin parquet files in R2:
--   Run from a pod or local env with R2 credentials:
--     aws s3 rm s3://$R2_BUCKET/global/bitcoin/ --recursive --endpoint-url https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com
--   Bitcoin RPC URL (for reference): https://bitcoin-rpc.publicnode.com

-- 1. Disable bitcoin sync
UPDATE blockchain_global_sources
SET enabled = false
WHERE chain = 'bitcoin';

-- 2. Reset sync checkpoint so it re-syncs from block 0
UPDATE blockchain_global_sources
SET last_synced_height = 0,
    last_synced_hash = NULL,
    tip_hashes = '{}'::jsonb,
    updated_at = NOW()
WHERE chain = 'bitcoin';

-- 3. Remove per-file skip index rows for bitcoin tables.
--    The source_id (project_id column) in warehouse_skip_indexes comes from
--    blockchain_global_sources.id.
DELETE FROM warehouse_skip_indexes
WHERE project_id IN (
    SELECT id FROM blockchain_global_sources WHERE chain = 'bitcoin'
);

-- 4. Remove skip index manifests for bitcoin tables.
DELETE FROM warehouse_skip_index_manifests
WHERE project_id IN (
    SELECT id FROM blockchain_global_sources WHERE chain = 'bitcoin'
);
