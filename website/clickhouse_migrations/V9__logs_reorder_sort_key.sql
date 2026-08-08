-- Reorder logs_local sorting key from (project_id, service_name, timestamp)
-- to (project_id, timestamp, service_name).
--
-- The old key puts service_name before timestamp, which means
-- "latest 100 logs across all services" requires a full scan of every
-- service group. With timestamp before service_name, the query reads
-- backwards from the tail and stops after 100 rows — sub-second instead
-- of 12-15 seconds on a 90M-row 24h window.
--
-- ClickHouse does not support reordering existing ORDER BY columns, so
-- we must recreate the tables. Historical log data is lost; the 30-day
-- TTL means the table self-recovers within one retention cycle.

DROP TABLE IF EXISTS reiver.logs;
DROP TABLE IF EXISTS reiver.logs_local;

CREATE TABLE IF NOT EXISTS reiver.logs_local
(
    `project_id`          String,
    `timestamp`           DateTime64(9),
    `trace_id`            String                  DEFAULT '',
    `span_id`             String                  DEFAULT '',
    `severity_text`       String                  DEFAULT '',
    `severity_number`     UInt8                   DEFAULT 0,
    `service_name`        String                  DEFAULT '',
    `body`                String,
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `log_attributes`      Map(String, String)     DEFAULT map(),
    INDEX idx_severity  severity_text TYPE set(10)            GRANULARITY 1,
    INDEX idx_service   service_name  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_trace_id  trace_id      TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, timestamp, service_name)
TTL toDateTime(timestamp) + toIntervalDay(30)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.logs
(
    `project_id`          String,
    `timestamp`           DateTime64(9),
    `trace_id`            String                  DEFAULT '',
    `span_id`             String                  DEFAULT '',
    `severity_text`       String                  DEFAULT '',
    `severity_number`     UInt8                   DEFAULT 0,
    `service_name`        String                  DEFAULT '',
    `body`                String,
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `log_attributes`      Map(String, String)     DEFAULT map()
)
ENGINE = Distributed('{cluster}', 'reiver', 'logs_local', cityHash64(project_id));
