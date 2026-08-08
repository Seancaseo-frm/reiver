-- V4 added is_platform_key as a column but not in the ORDER BY of the
-- SummingMergeTree. Old rows defaulted to is_platform_key=0 (including
-- platform-key traffic), and the backfill duplicated data on top.
-- Fix: drop everything and recreate with is_platform_key in the ORDER BY.

-- Step 1: Drop MV, distributed table, then local table (order matters)
DROP VIEW IF EXISTS reiver.llm_cost_daily_mv;
DROP TABLE IF EXISTS reiver.llm_cost_daily;
DROP TABLE IF EXISTS reiver.llm_cost_daily_local;

-- Step 2: Recreate local table with is_platform_key in ORDER BY
CREATE TABLE IF NOT EXISTS reiver.llm_cost_daily_local
(
    `project_id`           String,
    `date`                 Date,
    `gen_ai_system`        String,
    `gen_ai_request_model` String,
    `is_platform_key`      UInt8                                    DEFAULT 0,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `input_tokens`         SimpleAggregateFunction(sum, UInt64),
    `output_tokens`        SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8))
)
ENGINE = ReplicatedSummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (project_id, date, gen_ai_system, gen_ai_request_model, is_platform_key)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- Step 3: Recreate distributed table
CREATE TABLE IF NOT EXISTS reiver.llm_cost_daily
(
    `project_id`           String,
    `date`                 Date,
    `gen_ai_system`        String,
    `gen_ai_request_model` String,
    `is_platform_key`      UInt8                                    DEFAULT 0,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `input_tokens`         SimpleAggregateFunction(sum, UInt64),
    `output_tokens`        SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8))
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_cost_daily_local', cityHash64(project_id));

-- Step 4: Recreate MV with is_platform_key
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

-- Step 5: Backfill from llm_requests_local (90-day TTL window)
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
GROUP BY project_id, date, gen_ai_system, gen_ai_request_model, is_platform_key;
