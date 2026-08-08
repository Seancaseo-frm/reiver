-- ============================================================================
-- V2__attribute_key_views.sql
-- Refreshable materialized views for pre-computing distinct attribute keys
-- from logs and spans tables. Refreshes every 10 minutes, atomically
-- replacing the result set.
-- ============================================================================

-- ── Log attribute keys ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS reiver.log_attribute_keys_local
(
    `project_id` String,
    `key`        String
)
ENGINE = ReplicatedMergeTree()
ORDER BY (project_id, key);

CREATE TABLE IF NOT EXISTS reiver.log_attribute_keys
AS reiver.log_attribute_keys_local
ENGINE = Distributed('{cluster}', 'reiver', 'log_attribute_keys_local', cityHash64(project_id));

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.log_attribute_keys_mv
REFRESH EVERY 10 MINUTE
TO reiver.log_attribute_keys_local AS
SELECT DISTINCT project_id, key FROM (
    SELECT project_id, arrayJoin(mapKeys(log_attributes)) AS key
    FROM reiver.logs
    WHERE timestamp >= now() - INTERVAL 24 HOUR
    UNION ALL
    SELECT project_id, arrayJoin(mapKeys(resource_attributes)) AS key
    FROM reiver.logs
    WHERE timestamp >= now() - INTERVAL 24 HOUR
);

-- ── Span attribute keys ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS reiver.span_attribute_keys_local
(
    `project_id` String,
    `key`        String
)
ENGINE = ReplicatedMergeTree()
ORDER BY (project_id, key);

CREATE TABLE IF NOT EXISTS reiver.span_attribute_keys
AS reiver.span_attribute_keys_local
ENGINE = Distributed('{cluster}', 'reiver', 'span_attribute_keys_local', cityHash64(project_id));

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.span_attribute_keys_mv
REFRESH EVERY 10 MINUTE
TO reiver.span_attribute_keys_local AS
SELECT DISTINCT project_id, key FROM (
    SELECT project_id, arrayJoin(mapKeys(span_attributes)) AS key
    FROM reiver.spans
    WHERE timestamp >= now() - INTERVAL 24 HOUR
    UNION ALL
    SELECT project_id, arrayJoin(mapKeys(resource_attributes)) AS key
    FROM reiver.spans
    WHERE timestamp >= now() - INTERVAL 24 HOUR
);
