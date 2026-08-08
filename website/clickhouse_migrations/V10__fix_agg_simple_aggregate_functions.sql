-- Fix pre-aggregated tables to use SimpleAggregateFunction column types.
--
-- The original tables used plain Float64/UInt64 columns with AggregatingMergeTree.
-- During background merges, AggregatingMergeTree discards all but one row per key
-- for regular columns, silently losing aggregated data. SimpleAggregateFunction
-- tells the engine how to correctly re-aggregate (sum, min, max, anyLast) across
-- rows from different insert batches.
--
-- Also changes argMax(value, unix_milli) -> anyLast(value) in MVs because argMax
-- is not a supported SimpleAggregateFunction.
--
-- Existing data is incorrect and will be dropped; new data will accumulate correctly.

-- Step 1: Drop both MVs (must happen before dropping target tables)
DROP VIEW IF EXISTS reiver.samples_v1_agg_30m_mv;
DROP VIEW IF EXISTS reiver.samples_v1_agg_5m_mv;

-- Step 2: Drop all 4 tables (distributed first, then local)
DROP TABLE IF EXISTS reiver.samples_v1_agg_5m;
DROP TABLE IF EXISTS reiver.samples_v1_agg_30m;
DROP TABLE IF EXISTS reiver.samples_v1_agg_5m_local;
DROP TABLE IF EXISTS reiver.samples_v1_agg_30m_local;

-- Step 3: Recreate local tables with SimpleAggregateFunction columns

CREATE TABLE IF NOT EXISTS reiver.samples_v1_agg_5m_local
(
    `project_id`   UUID,
    `metric_name`  LowCardinality(String),
    `fingerprint`  UInt64,
    `unix_milli`   Int64,
    `sum`          SimpleAggregateFunction(sum, Float64),
    `count`        SimpleAggregateFunction(sum, UInt64),
    `min`          SimpleAggregateFunction(min, Float64),
    `max`          SimpleAggregateFunction(max, Float64),
    `last`         SimpleAggregateFunction(anyLast, Float64),
    `temporality`  LowCardinality(String)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(toDateTime(intDiv(unix_milli, 1000)))
ORDER BY (project_id, metric_name, fingerprint, unix_milli)
TTL toDateTime(intDiv(unix_milli, 1000)) + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.samples_v1_agg_30m_local
(
    `project_id`   UUID,
    `metric_name`  LowCardinality(String),
    `fingerprint`  UInt64,
    `unix_milli`   Int64,
    `sum`          SimpleAggregateFunction(sum, Float64),
    `count`        SimpleAggregateFunction(sum, UInt64),
    `min`          SimpleAggregateFunction(min, Float64),
    `max`          SimpleAggregateFunction(max, Float64),
    `last`         SimpleAggregateFunction(anyLast, Float64),
    `temporality`  LowCardinality(String)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(toDateTime(intDiv(unix_milli, 1000)))
ORDER BY (project_id, metric_name, fingerprint, unix_milli)
TTL toDateTime(intDiv(unix_milli, 1000)) + toIntervalDay(365)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- Step 4: Recreate distributed tables

CREATE TABLE IF NOT EXISTS reiver.samples_v1_agg_5m
(
    `project_id`   UUID,
    `metric_name`  LowCardinality(String),
    `fingerprint`  UInt64,
    `unix_milli`   Int64,
    `sum`          SimpleAggregateFunction(sum, Float64),
    `count`        SimpleAggregateFunction(sum, UInt64),
    `min`          SimpleAggregateFunction(min, Float64),
    `max`          SimpleAggregateFunction(max, Float64),
    `last`         SimpleAggregateFunction(anyLast, Float64),
    `temporality`  LowCardinality(String)
)
ENGINE = Distributed('{cluster}', 'reiver', 'samples_v1_agg_5m_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.samples_v1_agg_30m
(
    `project_id`   UUID,
    `metric_name`  LowCardinality(String),
    `fingerprint`  UInt64,
    `unix_milli`   Int64,
    `sum`          SimpleAggregateFunction(sum, Float64),
    `count`        SimpleAggregateFunction(sum, UInt64),
    `min`          SimpleAggregateFunction(min, Float64),
    `max`          SimpleAggregateFunction(max, Float64),
    `last`         SimpleAggregateFunction(anyLast, Float64),
    `temporality`  LowCardinality(String)
)
ENGINE = Distributed('{cluster}', 'reiver', 'samples_v1_agg_30m_local', cityHash64(project_id));

-- Step 5: Recreate MVs with anyLast instead of argMax

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.samples_v1_agg_5m_mv
TO reiver.samples_v1_agg_5m_local
AS SELECT
    project_id,
    metric_name,
    fingerprint,
    intDiv(unix_milli, 300000) * 300000 AS unix_milli,
    sum(value) AS sum,
    count() AS count,
    min(value) AS min,
    max(value) AS max,
    anyLast(value) AS last,
    anyLast(temporality) AS temporality
FROM reiver.samples_v1_local
WHERE bitAnd(flags, 1) = 0
GROUP BY project_id, metric_name, fingerprint, unix_milli;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.samples_v1_agg_30m_mv
TO reiver.samples_v1_agg_30m_local
AS SELECT
    project_id,
    metric_name,
    fingerprint,
    intDiv(unix_milli, 1800000) * 1800000 AS unix_milli,
    sum(sum) AS sum,
    sum(count) AS count,
    min(min) AS min,
    max(max) AS max,
    anyLast(last) AS last,
    anyLast(temporality) AS temporality
FROM reiver.samples_v1_agg_5m_local
GROUP BY project_id, metric_name, fingerprint, unix_milli;
