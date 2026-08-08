-- Add 30-day TTL to profiles tables (matching traces/logs retention).
ALTER TABLE reiver.profiles_local
    MODIFY TTL toDateTime(timestamp) + toIntervalDay(30);

ALTER TABLE reiver.profiles_local
    MODIFY SETTING ttl_only_drop_parts = 1;

ALTER TABLE reiver.profiles_local
    MODIFY SETTING merge_with_ttl_timeout = 3600;

-- Update profile_version_stats MV to use the service_version column directly
-- instead of JSONExtractString(profile_data, ...) which breaks with protobuf storage.
DROP VIEW IF EXISTS reiver.profile_version_stats_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.profile_version_stats_mv
TO reiver.profile_version_stats_local
(
    `project_id`         String,
    `service_name`       String,
    `service_version`    String,
    `period_type`        String,
    `hour`               DateTime,
    `profile_count`      UInt64,
    `total_samples`      UInt64,
    `total_duration_nano` UInt64
)
AS SELECT
    project_id,
    service_name,
    service_version,
    period_type,
    toStartOfHour(timestamp) AS hour,
    count() AS profile_count,
    sum(sample_count) AS total_samples,
    sum(duration_nano) AS total_duration_nano
FROM reiver.profiles_local
GROUP BY project_id, service_name, service_version, period_type, hour;

-- Exploded samples table for analytics (top functions, time-series queries).
-- Short 7-day TTL since this is for real-time analytics, not archival.
CREATE TABLE IF NOT EXISTS reiver.profile_samples_local
(
    `project_id`      String,
    `service_name`    String,
    `service_version` String,
    `profile_type`    LowCardinality(String),
    `profile_id`      String,
    `timestamp`       DateTime64(3),
    `function_name`   String,
    `filename`        String DEFAULT '',
    `line_number`     UInt32 DEFAULT 0,
    `value`           Int64
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, service_name, profile_type, function_name, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(7)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.profile_samples
(
    `project_id`      String,
    `service_name`    String,
    `service_version` String,
    `profile_type`    LowCardinality(String),
    `profile_id`      String,
    `timestamp`       DateTime64(3),
    `function_name`   String,
    `filename`        String DEFAULT '',
    `line_number`     UInt32 DEFAULT 0,
    `value`           Int64
)
ENGINE = Distributed('{cluster}', 'reiver', 'profile_samples_local', cityHash64(project_id));
