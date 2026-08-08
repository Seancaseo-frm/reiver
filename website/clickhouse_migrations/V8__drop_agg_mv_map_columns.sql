-- Drop unused anyLast(resource_attributes) and anyLast(metric_attributes) Map columns
-- from the 5m and 30m aggregation MVs and tables.
-- These Maps are never read from the agg tables and are the dominant source of
-- memory pressure during background merges.

-- Step 1: Drop both MVs (must happen before altering target tables)
DROP VIEW IF EXISTS reiver.samples_v1_agg_5m_mv;
DROP VIEW IF EXISTS reiver.samples_v1_agg_30m_mv;

-- Step 2: Drop the Map columns from local and distributed tables

ALTER TABLE reiver.samples_v1_agg_5m_local    DROP COLUMN IF EXISTS resource_attributes,
    DROP COLUMN IF EXISTS metric_attributes;

ALTER TABLE reiver.samples_v1_agg_5m    DROP COLUMN IF EXISTS resource_attributes,
    DROP COLUMN IF EXISTS metric_attributes;

ALTER TABLE reiver.samples_v1_agg_30m_local    DROP COLUMN IF EXISTS resource_attributes,
    DROP COLUMN IF EXISTS metric_attributes;

ALTER TABLE reiver.samples_v1_agg_30m    DROP COLUMN IF EXISTS resource_attributes,
    DROP COLUMN IF EXISTS metric_attributes;

-- Step 3: Recreate MVs without the Map columns

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.samples_v1_agg_5m_mv
TO reiver.samples_v1_agg_5m_local
(
    `project_id`   UUID,
    `metric_name`  LowCardinality(String),
    `fingerprint`  UInt64,
    `unix_milli`   Int64,
    `sum`          Float64,
    `count`        UInt64,
    `min`          Float64,
    `max`          Float64,
    `last`         Float64,
    `temporality`  String
)
AS SELECT
    project_id,
    metric_name,
    fingerprint,
    intDiv(unix_milli, 300000) * 300000 AS unix_milli,
    sum(value) AS sum,
    count() AS count,
    min(value) AS min,
    max(value) AS max,
    argMax(value, unix_milli) AS last,
    anyLast(temporality) AS temporality
FROM reiver.samples_v1_local
WHERE bitAnd(flags, 1) = 0
GROUP BY project_id, metric_name, fingerprint, unix_milli;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.samples_v1_agg_30m_mv
TO reiver.samples_v1_agg_30m_local
(
    `project_id`   UUID,
    `metric_name`  LowCardinality(String),
    `fingerprint`  UInt64,
    `unix_milli`   Int64,
    `sum`          Float64,
    `count`        UInt64,
    `min`          Float64,
    `max`          Float64,
    `last`         Float64,
    `temporality`  String
)
AS SELECT
    project_id,
    metric_name,
    fingerprint,
    intDiv(unix_milli, 1800000) * 1800000 AS unix_milli,
    sum(sum) AS sum,
    sum(count) AS count,
    min(min) AS min,
    max(max) AS max,
    argMax(last, unix_milli) AS last,
    anyLast(temporality) AS temporality
FROM reiver.samples_v1_agg_5m_local
GROUP BY project_id, metric_name, fingerprint, unix_milli;
