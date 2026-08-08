-- Add is_platform_key to llm_cost_daily so BYOK fees can be computed from
-- ClickHouse instead of per-request Postgres INSERTs into platform_fees.
-- Note: no ON CLUSTER needed — the Replicated database engine replicates DDL automatically.

-- Step 1: Drop the existing materialized view (stops new inserts while we alter)
DROP VIEW IF EXISTS reiver.llm_cost_daily_mv;

-- Step 2: Add the new column to the local and distributed tables
ALTER TABLE reiver.llm_cost_daily_local
    ADD COLUMN IF NOT EXISTS `is_platform_key` UInt8 DEFAULT 0;

ALTER TABLE reiver.llm_cost_daily
    ADD COLUMN IF NOT EXISTS `is_platform_key` UInt8 DEFAULT 0;

-- Step 3: Recreate the materialized view with is_platform_key in GROUP BY
CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.llm_cost_daily_mv
TO reiver.llm_cost_daily_local
(
    `project_id`           String,
    `date`                 Date,
    `gen_ai_system`        String,
    `gen_ai_request_model` String,
    `is_platform_key`      UInt8,
    `request_count`        UInt64,
    `input_tokens`         UInt64,
    `output_tokens`        UInt64,
    `total_cost_usd`       Decimal(38, 8)
)
AS SELECT
    project_id,
    toDate(timestamp) AS date,
    gen_ai_system,
    gen_ai_request_model,
    is_platform_key,
    count() AS request_count,
    sum(input_tokens) AS input_tokens,
    sum(output_tokens) AS output_tokens,
    sum(cost_usd) AS total_cost_usd
FROM reiver.llm_requests_local
GROUP BY project_id, date, gen_ai_system, gen_ai_request_model, is_platform_key;

-- Step 4: Backfill current month from llm_requests so existing data is queryable
INSERT INTO reiver.llm_cost_daily_local
SELECT
    project_id,
    toDate(timestamp) AS date,
    gen_ai_system,
    gen_ai_request_model,
    is_platform_key,
    count() AS request_count,
    sum(input_tokens) AS input_tokens,
    sum(output_tokens) AS output_tokens,
    sum(cost_usd) AS total_cost_usd
FROM reiver.llm_requests_local
WHERE timestamp >= toStartOfMonth(today())
GROUP BY project_id, date, gen_ai_system, gen_ai_request_model, is_platform_key;
