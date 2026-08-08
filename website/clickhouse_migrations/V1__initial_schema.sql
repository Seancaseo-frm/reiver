-- ============================================================================
-- V1__initial_schema.sql
-- Squashed ClickHouse migration for the reiver platform.
--
-- This file contains ALL DDL for the reiver and catalog databases.
-- Both databases use the Replicated engine so that table-level ZK paths
-- are assigned automatically — ReplicatedMergeTree variants are created
-- WITHOUT explicit ZK path/replica arguments.
--
-- Naming convention:
--   - Local (storage) tables use `_local` suffix (ReplicatedMergeTree engines)
--   - Distributed tables use bare names (application code queries these)
--   - Materialized views source and target `_local` tables
--
-- Table creation order:
--   1. Databases
--   2. Local tables (`_local` suffix)
--   3. Distributed tables (bare names)
--   4. Materialized views
--
-- Kafka-engine tables and their MVs (exceptions_kafka, llm_chunks_kafka,
-- exceptions_mv, llm_chunks_mv) are NOT included here — they are created
-- at runtime by application code.
-- ============================================================================

-- ============================================================================
-- DATABASES
-- ============================================================================

CREATE DATABASE IF NOT EXISTS reiver
    ENGINE = Replicated('/clickhouse/databases/reiver', '{shard}', '{replica}');

CREATE DATABASE IF NOT EXISTS catalog
    ENGINE = Replicated('/clickhouse/databases/catalog', '{shard}', '{replica}');

-- ============================================================================
-- LOCAL TABLES — OpenTelemetry: Spans, Logs, Metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.spans_local
(
    `project_id`          String,
    `trace_id`            String,
    `span_id`             String,
    `parent_span_id`      String                  DEFAULT '',
    `trace_state`         String                  DEFAULT '',
    `span_name`           String,
    `span_kind`           LowCardinality(String)  DEFAULT '',
    `service_name`        String                  DEFAULT '',
    `timestamp`           DateTime64(9),
    `duration`            Int64,
    `status_code`         LowCardinality(String)  DEFAULT 'STATUS_CODE_UNSET',
    `status_message`      String                  DEFAULT '',
    `span_attributes`     Map(String, String)     DEFAULT map(),
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `events`              String                  DEFAULT '[]',
    `links`               String                  DEFAULT '[]',
    INDEX idx_trace_id     trace_id     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_service_name service_name TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_name    span_name    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_status_code  status_code  TYPE set(10) GRANULARITY 1,
    INDEX idx_http_route   mapValues(mapFilter((k, v) -> (k = 'http.route'), span_attributes))
                                        TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, service_name, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(30)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

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
ORDER BY (project_id, service_name, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(30)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.metrics_local
(
    `id`         String,
    `project_id` String,
    `name`       String,
    `value`      Float64,
    `type`       String,
    `tags`       String,
    `timestamp`  DateTime64(3),
    `created_at` DateTime64(3) DEFAULT now64(),
    INDEX idx_metric_name name TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, name, timestamp)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — OTel Metrics v1 (samples / time-series / exemplars)
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.samples_v1_local
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `unix_milli`          Int64,
    `value`               Float64,
    `temporality`         LowCardinality(String),
    `metric_type`         LowCardinality(String),
    `flags`               UInt8                   DEFAULT 0,
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `metric_attributes`   Map(String, String)     DEFAULT map()
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(toDateTime(intDiv(unix_milli, 1000)))
ORDER BY (project_id, metric_name, fingerprint, unix_milli)
TTL toDateTime(intDiv(unix_milli, 1000)) + toIntervalDay(30)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.samples_v1_agg_5m_local
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `unix_milli`          Int64,
    `sum`                 Float64,
    `count`               UInt64,
    `min`                 Float64,
    `max`                 Float64,
    `last`                Float64,
    `temporality`         LowCardinality(String),
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `metric_attributes`   Map(String, String)     DEFAULT map()
)
ENGINE = ReplicatedAggregatingMergeTree()
PARTITION BY toYYYYMM(toDateTime(intDiv(unix_milli, 1000)))
ORDER BY (project_id, metric_name, fingerprint, unix_milli)
TTL toDateTime(intDiv(unix_milli, 1000)) + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.samples_v1_agg_30m_local
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `unix_milli`          Int64,
    `sum`                 Float64,
    `count`               UInt64,
    `min`                 Float64,
    `max`                 Float64,
    `last`                Float64,
    `temporality`         LowCardinality(String),
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `metric_attributes`   Map(String, String)     DEFAULT map()
)
ENGINE = ReplicatedAggregatingMergeTree()
PARTITION BY toYYYYMM(toDateTime(intDiv(unix_milli, 1000)))
ORDER BY (project_id, metric_name, fingerprint, unix_milli)
TTL toDateTime(intDiv(unix_milli, 1000)) + toIntervalDay(365)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.time_series_v1_local
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `labels`              String,
    `temporality`         LowCardinality(String),
    `metric_type`         LowCardinality(String),
    `unix_milli`          Int64,
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `metric_attributes`   Map(String, String)     DEFAULT map()
)
ENGINE = ReplicatedReplacingMergeTree(unix_milli)
PARTITION BY toYYYYMM(toDateTime(intDiv(unix_milli, 1000)))
ORDER BY (project_id, metric_name, fingerprint)
TTL toDateTime(intDiv(unix_milli, 1000)) + toIntervalDay(30)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.metric_exemplars_local
(
    `project_id`              UUID,
    `metric_name`             LowCardinality(String),
    `fingerprint`             UInt64,
    `exemplar_time_unix_nano` Int64,
    `trace_id`                String,
    `span_id`                 String,
    `value`                   Float64,
    `filtered_attributes`     Map(String, String)     DEFAULT map(),
    `inserted_at`             DateTime64(3)           DEFAULT now64()
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(toDateTime(intDiv(exemplar_time_unix_nano, 1000000000)))
ORDER BY (project_id, metric_name, fingerprint, exemplar_time_unix_nano)
TTL toDateTime(intDiv(exemplar_time_unix_nano, 1000000000)) + toIntervalDay(30)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — OTel Attributes
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.otlp_attributes_local
(
    `project_id`      String,
    `attribute_type`  String,
    `attribute_value` String,
    `last_seen`       DateTime64(9)
)
ENGINE = ReplicatedReplacingMergeTree(last_seen)
PRIMARY KEY (project_id, attribute_type, attribute_value)
ORDER BY (project_id, attribute_type, attribute_value)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- LOCAL TABLES — Discovered Services
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.discovered_services_local
(
    `project_id`         String,
    `service_name`       String,
    `first_seen`         DateTime64(9),
    `last_seen`          DateTime64(9),
    `has_http_spans`     UInt8,
    `has_db_spans`       UInt8,
    `has_rpc_spans`      UInt8,
    `has_messaging_spans` UInt8,
    `span_count`         UInt64,
    `error_count`        UInt64
)
ENGINE = ReplicatedReplacingMergeTree(last_seen)
ORDER BY (project_id, service_name)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS reiver.discovered_services_agg_local
(
    `project_id`         String,
    `service_name`       String,
    `first_seen`         SimpleAggregateFunction(min, DateTime64(9)),
    `last_seen`          SimpleAggregateFunction(max, DateTime64(9)),
    `has_http_spans`     SimpleAggregateFunction(max, UInt8),
    `has_db_spans`       SimpleAggregateFunction(max, UInt8),
    `has_rpc_spans`      SimpleAggregateFunction(max, UInt8),
    `has_messaging_spans` SimpleAggregateFunction(max, UInt8),
    `span_count`         SimpleAggregateFunction(sum, UInt64),
    `error_count`        SimpleAggregateFunction(sum, UInt64)
)
ENGINE = ReplicatedAggregatingMergeTree()
ORDER BY (project_id, service_name)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- LOCAL TABLES — Exceptions
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.exceptions_local
(
    `id`               String,
    `project_id`       String,
    `fingerprint`      String,
    `level`            String,
    `message`          String,
    `exception_type`   String       DEFAULT '',
    `exception_value`  String       DEFAULT '',
    `stacktrace`       String       DEFAULT '',
    `context`          String,
    `tags`             String,
    `user_data`        String,
    `service_name`     String       DEFAULT '',
    `trace_id`         String       DEFAULT '',
    `span_id`          String       DEFAULT '',
    `service_version`  String       DEFAULT '',
    `environment`      String       DEFAULT '',
    `repository_url`   String       DEFAULT '',
    `status`           String       DEFAULT 'unresolved',
    `timestamp`        DateTime64(9),
    `created_at`       DateTime64(9) DEFAULT now64(),
    INDEX idx_fingerprint      fingerprint      TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_level            level            TYPE set(100) GRANULARITY 1,
    INDEX idx_service_version  service_version  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_environment      environment      TYPE set(100) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, timestamp)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — LLM Observability
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.llm_requests_local
(
    `project_id`            String,
    `request_id`            String,
    `trace_id`              String,
    `span_id`               String,
    `gen_ai_system`         String,
    `gen_ai_request_model`  String,
    `gen_ai_response_model` String                  DEFAULT '',
    `gen_ai_operation_name` String                  DEFAULT '',
    `input_tokens`          UInt32                  DEFAULT 0,
    `output_tokens`         UInt32                  DEFAULT 0,
    `total_tokens`          UInt32                  DEFAULT 0,
    `cache_read_tokens`     UInt32                  DEFAULT 0,
    `cache_write_tokens`    UInt32                  DEFAULT 0,
    `cost_usd`              Float64                 DEFAULT 0,
    `timestamp`             DateTime64(9),
    `duration_ms`           UInt32                  DEFAULT 0,
    `time_to_first_token_ms` UInt32                 DEFAULT 0,
    `status_code`           LowCardinality(String)  DEFAULT 'ok',
    `error_type`            String                  DEFAULT '',
    `error_message`         String                  DEFAULT '',
    `session_id`            String                  DEFAULT '',
    `session_name`          String                  DEFAULT '',
    `user_id`               String                  DEFAULT '',
    `request_messages`      String                  DEFAULT '' TTL toDateTime(timestamp) + toIntervalHour(2),
    `response_content`      String                  DEFAULT '' TTL toDateTime(timestamp) + toIntervalHour(2),
    `properties`            Map(String, String)     DEFAULT map(),
    `scores`                Map(String, Float64)    DEFAULT map(),
    `service_name`          String                  DEFAULT '',
    `rollout_id`            String                  DEFAULT '',
    `rollout_variant`       String                  DEFAULT '',
    `prompt_config_id`      String                  DEFAULT '',
    `prompt_version_id`     String                  DEFAULT '',
    `request_embedding`     Array(Float32)          DEFAULT [],
    `fallback_used`         UInt8                   DEFAULT 0,
    `original_model`        String                  DEFAULT '',
    `retry_count`           UInt32                  DEFAULT 0,
    `guardrail_violations`  Array(String)           DEFAULT [],
    `temperature`           Float32                 DEFAULT 0,
    `top_p`                 Float32                 DEFAULT 0,
    `max_tokens`            UInt32                  DEFAULT 0,
    `frequency_penalty`     Float32                 DEFAULT 0,
    `presence_penalty`      Float32                 DEFAULT 0,
    `tool_call_count`       UInt32                  DEFAULT 0,
    `tool_names`            Array(String)           DEFAULT [],
    `is_platform_key`       UInt8                   DEFAULT 0,
    INDEX idx_trace_id          trace_id            TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_session_id        session_id          TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_user_id           user_id             TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_model             gen_ai_request_model TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_system            gen_ai_system       TYPE set(50) GRANULARITY 1,
    INDEX idx_status            status_code         TYPE set(10) GRANULARITY 1,
    INDEX idx_rollout_id        rollout_id          TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_prompt_config_id  prompt_config_id    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_rollout_variant   rollout_variant     TYPE set(3) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, gen_ai_system, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.llm_chunks_local
(
    `project_id`    String,
    `request_id`    String,
    `chunk_index`   UInt32,
    `content`       String,
    `model`         String,
    `provider`      String,
    `timestamp`     DateTime64(3),
    `is_final`      UInt8       DEFAULT 0,
    `finish_reason` String      DEFAULT '',
    `input_tokens`  UInt32      DEFAULT 0,
    `output_tokens` UInt32      DEFAULT 0,
    INDEX idx_request_id request_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (project_id, request_id, chunk_index)
TTL toDateTime(timestamp) + toIntervalDay(30)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.llm_message_labels_local
(
    `project_id`    String,
    `session_id`    String,
    `request_id`    String,
    `labels`        Array(String),
    `classified_at` DateTime64(3) DEFAULT now64(3),
    INDEX idx_session_id session_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_labels     labels     TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
ORDER BY (project_id, session_id, request_id)
TTL toDateTime(classified_at) + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.llm_request_embeddings_local
(
    `project_id`           String,
    `request_id`           String,
    `embedding`            Array(Float32),
    `gen_ai_request_model` String,
    `user_id`              String       DEFAULT '',
    `session_id`           String       DEFAULT '',
    `timestamp`            DateTime64(9),
    `content_preview`      String       DEFAULT '',
    `created_at`           DateTime     DEFAULT now(),
    INDEX idx_request_id request_id           TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_model      gen_ai_request_model TYPE set(50) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, timestamp, request_id)
TTL toDateTime(timestamp) + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — LLM Aggregates
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.llm_sessions_agg_local
(
    `project_id`         String,
    `session_id`         String,
    `session_name`       String                                        DEFAULT '',
    `user_id`            String                                        DEFAULT '',
    `first_request_time` SimpleAggregateFunction(min, DateTime64(9)),
    `last_request_time`  SimpleAggregateFunction(max, DateTime64(9)),
    `request_count`      SimpleAggregateFunction(sum, UInt64),
    `total_input_tokens` SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens` SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`     SimpleAggregateFunction(sum, Decimal(38, 8)),
    `total_duration_ms`  SimpleAggregateFunction(sum, UInt64),
    `error_count`        SimpleAggregateFunction(sum, UInt64),
    `models`             AggregateFunction(groupUniqArray, String)
)
ENGINE = ReplicatedAggregatingMergeTree()
ORDER BY (project_id, session_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS reiver.llm_model_metrics_agg_local
(
    `project_id`           String,
    `gen_ai_system`        String,
    `gen_ai_request_model` String,
    `hour`                 DateTime,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `total_input_tokens`   SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens`  SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8)),
    `duration_quantiles`   AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32),
    `ttft_quantiles`       AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32),
    `error_count`          SimpleAggregateFunction(sum, UInt64),
    `total_duration_ms`    SimpleAggregateFunction(sum, UInt64)         DEFAULT 0
)
ENGINE = ReplicatedAggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (project_id, gen_ai_system, gen_ai_request_model, hour)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.llm_cost_daily_local
(
    `project_id`           String,
    `date`                 Date,
    `gen_ai_system`        String,
    `gen_ai_request_model` String,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `input_tokens`         SimpleAggregateFunction(sum, UInt64),
    `output_tokens`        SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8))
)
ENGINE = ReplicatedSummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (project_id, date, gen_ai_system, gen_ai_request_model)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.llm_prompt_metrics_agg_local
(
    `project_id`           String,
    `prompt_config_id`     String,
    `prompt_version_id`    String,
    `date`                 Date,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `error_count`          SimpleAggregateFunction(sum, UInt64),
    `total_duration_ms`    SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8)),
    `total_input_tokens`   SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens`  SimpleAggregateFunction(sum, UInt64)
)
ENGINE = ReplicatedSummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (project_id, prompt_config_id, prompt_version_id, date)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.llm_rollout_metrics_agg_local
(
    `project_id`           String,
    `rollout_id`           String,
    `rollout_variant`      LowCardinality(String),
    `hour`                 DateTime,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `error_count`          SimpleAggregateFunction(sum, UInt64),
    `total_duration_ms`    SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8)),
    `total_input_tokens`   SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens`  SimpleAggregateFunction(sum, UInt64),
    `duration_quantiles`   AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32)
)
ENGINE = ReplicatedAggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (project_id, rollout_id, rollout_variant, hour)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.llm_user_metrics_agg_local
(
    `project_id`           String,
    `user_id`              String,
    `date`                 Date,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `session_count`        AggregateFunction(uniq, String),
    `total_input_tokens`   SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens`  SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8)),
    `error_count`          SimpleAggregateFunction(sum, UInt64),
    `models`               AggregateFunction(groupUniqArray, String)
)
ENGINE = ReplicatedAggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (project_id, user_id, date)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — Profiles
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.profiles_local
(
    `id`              String,
    `project_id`      String,
    `service_name`    String,
    `trace_id`        Nullable(String),
    `span_id`         Nullable(String),
    `profile_id`      String,
    `time_unix_nano`  UInt64,
    `duration_nano`   UInt64,
    `period_type`     String,
    `period`          Int64,
    `sample_count`    UInt64,
    `profile_data`    String,
    `dictionary_data` String,
    `timestamp`       DateTime64(3),
    `created_at`      DateTime64(3) DEFAULT now64(),
    `service_version` String        DEFAULT '',
    INDEX idx_service_version service_version TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, service_name, timestamp)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.profile_version_stats_local
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
ENGINE = ReplicatedSummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (project_id, service_name, service_version, period_type, hour)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — Game Observability
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.game_matches_local
(
    `project_id`              String,
    `match_id`                String,
    `match_mode`              LowCardinality(String)  DEFAULT '',
    `match_type`              LowCardinality(String)  DEFAULT '',
    `match_map`               String                  DEFAULT '',
    `server_id`               String                  DEFAULT '',
    `server_region`           LowCardinality(String)  DEFAULT '',
    `start_time`              DateTime64(9),
    `end_time`                DateTime64(9)           DEFAULT toDateTime64(0, 9),
    `duration_seconds`        Float64                 DEFAULT 0,
    `player_count`            UInt16                  DEFAULT 0,
    `max_player_count`        UInt16                  DEFAULT 0,
    `outcome`                 LowCardinality(String)  DEFAULT '',
    `winning_team`            String                  DEFAULT '',
    `avg_server_tick_rate`    Float64                 DEFAULT 0,
    `avg_player_rtt_seconds`  Float64                 DEFAULT 0,
    `max_player_rtt_seconds`  Float64                 DEFAULT 0,
    `packet_loss_ratio`       Float64                 DEFAULT 0,
    `error_count`             UInt32                  DEFAULT 0,
    `crash_count`             UInt32                  DEFAULT 0,
    `properties`              Map(String, String)     DEFAULT map(),
    INDEX idx_match_mode    match_mode    TYPE set(50)            GRANULARITY 1,
    INDEX idx_match_map     match_map     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_server_region server_region TYPE set(100)           GRANULARITY 1,
    INDEX idx_outcome       outcome       TYPE set(10)            GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree(start_time)
PARTITION BY toYYYYMM(start_time)
ORDER BY (project_id, match_id)
TTL toDateTime(start_time) + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.game_match_players_local
(
    `project_id`        String,
    `match_id`          String,
    `session_id`        String,
    `player_id`         String                  DEFAULT '',
    `team`              String                  DEFAULT '',
    `join_time`         DateTime64(9),
    `leave_time`        DateTime64(9)           DEFAULT toDateTime64(0, 9),
    `duration_seconds`  Float64                 DEFAULT 0,
    `leave_reason`      LowCardinality(String)  DEFAULT '',
    `avg_fps`           Float64                 DEFAULT 0,
    `avg_rtt_seconds`   Float64                 DEFAULT 0,
    `packet_loss_ratio` Float64                 DEFAULT 0,
    `stats`             Map(String, Float64)    DEFAULT map()
)
ENGINE = ReplicatedReplacingMergeTree(join_time)
PARTITION BY toYYYYMM(join_time)
ORDER BY (project_id, match_id, session_id)
TTL toDateTime(join_time) + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.game_player_sessions_local
(
    `project_id`             String,
    `session_id`             String,
    `player_id`              String                  DEFAULT '',
    `device_id`              String                  DEFAULT '',
    `device_manufacturer`    String                  DEFAULT '',
    `device_model`           String                  DEFAULT '',
    `platform`               LowCardinality(String)  DEFAULT '',
    `gpu_vendor`             String                  DEFAULT '',
    `gpu_model`              String                  DEFAULT '',
    `game_version`           String                  DEFAULT '',
    `start_time`             DateTime64(9),
    `end_time`               DateTime64(9)           DEFAULT toDateTime64(0, 9),
    `duration_seconds`       Float64                 DEFAULT 0,
    `end_reason`             LowCardinality(String)  DEFAULT '',
    `avg_fps`                Float64                 DEFAULT 0,
    `min_fps`                Float64                 DEFAULT 0,
    `p95_frame_time_seconds` Float64                 DEFAULT 0,
    `avg_rtt_seconds`        Float64                 DEFAULT 0,
    `avg_jitter_seconds`     Float64                 DEFAULT 0,
    `packet_loss_ratio`      Float64                 DEFAULT 0,
    `peak_memory_bytes`      UInt64                  DEFAULT 0,
    `matches_played`         UInt16                  DEFAULT 0,
    `matches_completed`      UInt16                  DEFAULT 0,
    `quality_score`          Float64                 DEFAULT 0,
    `country_code`           LowCardinality(String)  DEFAULT '',
    `region`                 String                  DEFAULT '',
    `properties`             Map(String, String)     DEFAULT map(),
    INDEX idx_player_id  player_id    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_platform   platform     TYPE set(20) GRANULARITY 1,
    INDEX idx_end_reason end_reason   TYPE set(20) GRANULARITY 1,
    INDEX idx_country    country_code TYPE set(300) GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree(start_time)
PARTITION BY toYYYYMM(start_time)
ORDER BY (project_id, session_id)
TTL toDateTime(start_time) + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.game_metrics_hourly_local
(
    `project_id`          String,
    `hour`                DateTime,
    `match_mode`          LowCardinality(String)  DEFAULT '',
    `server_region`       LowCardinality(String)  DEFAULT '',
    `platform`            LowCardinality(String)  DEFAULT '',
    `match_starts`        SimpleAggregateFunction(sum, UInt64),
    `match_completions`   SimpleAggregateFunction(sum, UInt64),
    `match_abandonments`  SimpleAggregateFunction(sum, UInt64),
    `unique_players`      AggregateFunction(uniq, String),
    `player_sessions`     SimpleAggregateFunction(sum, UInt64),
    `avg_tick_rate`       Float64                 DEFAULT 0,
    `avg_fps`             Float64                 DEFAULT 0,
    `avg_rtt_seconds`     Float64                 DEFAULT 0,
    `avg_packet_loss`     Float64                 DEFAULT 0,
    `crash_count`         SimpleAggregateFunction(sum, UInt64),
    `error_count`         SimpleAggregateFunction(sum, UInt64)
)
ENGINE = ReplicatedAggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (project_id, hour, match_mode, server_region, platform)
TTL toDateTime(hour) + toIntervalDay(365)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — Health Checks
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.health_check_results_local
(
    `id`                    String,
    `check_id`              String,
    `project_id`            String,
    `timestamp`             DateTime64(9),
    `check_type`            String,
    `check_name`            String,
    `target`                String,
    `status`                String,
    `success`               UInt8,
    `response_time_ms`      Float64,
    `dns_time_ms`           Float64,
    `connect_time_ms`       Float64,
    `tls_time_ms`           Float64,
    `first_byte_time_ms`    Float64,
    `http_status_code`      Int32,
    `http_response_size`    Int64,
    `ssl_valid`             UInt8,
    `ssl_days_until_expiry` Int32,
    `ssl_issuer`            String,
    `ssl_subject`           String,
    `ssl_expires_at`        Nullable(DateTime64(9)),
    `error_type`            String,
    `error_message`         String,
    `agent_id`              String,
    `agent_location`        String,
    `response_body`         String
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, check_id, timestamp)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.health_check_uptime_hourly_local
(
    `project_id`           String,
    `check_id`             String,
    `hour`                 DateTime,
    `total_checks`         UInt64,
    `successful_checks`    UInt64,
    `avg_response_time_ms` Float64
)
ENGINE = ReplicatedMergeTree()
ORDER BY (project_id, check_id, hour)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — Audit Events
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.audit_events_local
(
    `event_id`               String,
    `project_id`             String                  DEFAULT '',
    `event_type`             LowCardinality(String),
    `action`                 String                  DEFAULT '',
    `caller_type`            LowCardinality(String),
    `caller_user_id`         String                  DEFAULT '',
    `caller_key_label`       String                  DEFAULT '',
    `caller_key_prefix`      String                  DEFAULT '',
    `service`                LowCardinality(String)  DEFAULT '',
    `http_method`            LowCardinality(String)  DEFAULT '',
    `http_path`              String                  DEFAULT '',
    `http_status`            UInt16                  DEFAULT 0,
    `source_id`              String                  DEFAULT '',
    `prompt_config_name`     String                  DEFAULT '',
    `prompt_config_id`       String                  DEFAULT '',
    `prompt_version_id`      String                  DEFAULT '',
    `prompt_version_number`  UInt32                  DEFAULT 0,
    `rendered_system_prompt` String                  DEFAULT '',
    `prompt_variables`       String                  DEFAULT '',
    `model_used`             String                  DEFAULT '',
    `total_input_tokens`     UInt64                  DEFAULT 0,
    `total_output_tokens`    UInt64                  DEFAULT 0,
    `total_turns`            UInt32                  DEFAULT 0,
    `tool_calls_log`         String                  DEFAULT '[]',
    `mcp_tool_name`          String                  DEFAULT '',
    `mcp_tool_arguments`     String                  DEFAULT '',
    `mcp_tool_success`       UInt8                   DEFAULT 1,
    `mcp_tool_error`         String                  DEFAULT '',
    `timestamp`              DateTime64(3),
    `duration_ms`            UInt64                  DEFAULT 0,
    `organization_id`        String                  DEFAULT '',
    `actor_id`               String                  DEFAULT '',
    `ip_address`             String                  DEFAULT '',
    `user_agent`             String                  DEFAULT '',
    `resource_type`          LowCardinality(String)  DEFAULT '',
    `resource_id`            String                  DEFAULT '',
    `details`                String                  DEFAULT '',
    `success`                UInt8                   DEFAULT 1,
    `error_message`          String                  DEFAULT '',
    `origin_type`            LowCardinality(String)  DEFAULT '',
    `origin_ref`             String                  DEFAULT '',
    `origin_reason`          String                  DEFAULT '',
    INDEX idx_source       source_id        TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_caller_label caller_key_label  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_event_type   event_type        TYPE set(10) GRANULARITY 1,
    INDEX idx_http_path    http_path         TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_mcp_tool     mcp_tool_name     TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, event_type, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — Usage Tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.usage_local
(
    `project_id` String,
    `event_type` LowCardinality(String),
    `date`       Date,
    `value`      UInt64
)
ENGINE = ReplicatedSummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (project_id, event_type, date)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS reiver.usage_hourly_local
(
    `organization_id` String,
    `project_id`      String,
    `event_type`      LowCardinality(String),
    `hour`            DateTime,
    `events_count`    UInt64,
    `ingested_bytes`  UInt64 DEFAULT 0
)
ENGINE = ReplicatedSummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (organization_id, project_id, event_type, hour)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — A2A (Agent-to-Agent)
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.a2a_tasks_local
(
    `task_id`         UUID,
    `context_id`      Nullable(UUID),
    `source_agent_id` UUID,
    `target_agent_id` UUID,
    `source_org_id`   UUID,
    `target_org_id`   UUID,
    `status`          String,
    `metadata`        String        DEFAULT '{}',
    `artifacts`       String        DEFAULT '[]',
    `updated_at`      DateTime64(3),
    `created_at`      DateTime64(3)
)
ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY task_id
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS reiver.a2a_messages_local
(
    `message_id`         UUID,
    `task_id`            UUID,
    `context_id`         Nullable(UUID),
    `role`               String,
    `parts`              String       DEFAULT '[]',
    `reference_task_ids` Array(UUID),
    `metadata`           String       DEFAULT '{}',
    `pipeline_flags`     String       DEFAULT '{}',
    `created_at`         DateTime64(3)
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (task_id, created_at)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS reiver.a2a_request_log_local
(
    `request_id`         UUID,
    `task_id`            UUID,
    `source_agent_id`    UUID,
    `target_agent_id`    UUID,
    `source_org_id`      UUID,
    `target_org_id`      UUID,
    `method`             String,
    `status_code`        UInt16,
    `latency_ms`         UInt32,
    `message_parts_count` UInt16,
    `pii_redacted`       Bool,
    `injection_flagged`  Bool,
    `timestamp`          DateTime64(3)
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, source_org_id, target_org_id)
TTL timestamp + toIntervalDay(90)
SETTINGS index_granularity = 8192, storage_policy = 'tiered';

-- ============================================================================
-- LOCAL TABLES — Gateway Provider Latency
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.provider_latency_samples_local
(
    `provider`    String,
    `ts`          DateTime64(3),
    `duration_ms` UInt64
)
ENGINE = ReplicatedMergeTree()
ORDER BY (provider, ts)
TTL toDateTime(ts) + toIntervalDay(1)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- DISTRIBUTED TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.spans
(
    `project_id`          String,
    `trace_id`            String,
    `span_id`             String,
    `parent_span_id`      String                  DEFAULT '',
    `trace_state`         String                  DEFAULT '',
    `span_name`           String,
    `span_kind`           LowCardinality(String)  DEFAULT '',
    `service_name`        String                  DEFAULT '',
    `timestamp`           DateTime64(9),
    `duration`            Int64,
    `status_code`         LowCardinality(String)  DEFAULT 'STATUS_CODE_UNSET',
    `status_message`      String                  DEFAULT '',
    `span_attributes`     Map(String, String)     DEFAULT map(),
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `events`              String                  DEFAULT '[]',
    `links`               String                  DEFAULT '[]'
)
ENGINE = Distributed('{cluster}', 'reiver', 'spans_local', cityHash64(project_id));

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

CREATE TABLE IF NOT EXISTS reiver.metrics
(
    `id`         String,
    `project_id` String,
    `name`       String,
    `value`      Float64,
    `type`       String,
    `tags`       String,
    `timestamp`  DateTime64(3),
    `created_at` DateTime64(3) DEFAULT now64()
)
ENGINE = Distributed('{cluster}', 'reiver', 'metrics_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.samples_v1
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `unix_milli`          Int64,
    `value`               Float64,
    `temporality`         LowCardinality(String),
    `metric_type`         LowCardinality(String),
    `flags`               UInt8                   DEFAULT 0,
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `metric_attributes`   Map(String, String)     DEFAULT map()
)
ENGINE = Distributed('{cluster}', 'reiver', 'samples_v1_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.samples_v1_agg_5m
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `unix_milli`          Int64,
    `sum`                 Float64,
    `count`               UInt64,
    `min`                 Float64,
    `max`                 Float64,
    `last`                Float64,
    `temporality`         LowCardinality(String),
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `metric_attributes`   Map(String, String)     DEFAULT map()
)
ENGINE = Distributed('{cluster}', 'reiver', 'samples_v1_agg_5m_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.samples_v1_agg_30m
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `unix_milli`          Int64,
    `sum`                 Float64,
    `count`               UInt64,
    `min`                 Float64,
    `max`                 Float64,
    `last`                Float64,
    `temporality`         LowCardinality(String),
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `metric_attributes`   Map(String, String)     DEFAULT map()
)
ENGINE = Distributed('{cluster}', 'reiver', 'samples_v1_agg_30m_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.time_series_v1
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `labels`              String,
    `temporality`         LowCardinality(String),
    `metric_type`         LowCardinality(String),
    `unix_milli`          Int64,
    `resource_attributes` Map(String, String)     DEFAULT map(),
    `metric_attributes`   Map(String, String)     DEFAULT map()
)
ENGINE = Distributed('{cluster}', 'reiver', 'time_series_v1_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.metric_exemplars
(
    `project_id`              UUID,
    `metric_name`             LowCardinality(String),
    `fingerprint`             UInt64,
    `exemplar_time_unix_nano` Int64,
    `trace_id`                String,
    `span_id`                 String,
    `value`                   Float64,
    `filtered_attributes`     Map(String, String)     DEFAULT map(),
    `inserted_at`             DateTime64(3)           DEFAULT now64()
)
ENGINE = Distributed('{cluster}', 'reiver', 'metric_exemplars_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.otlp_attributes
(
    `project_id`      String,
    `attribute_type`  String,
    `attribute_value` String,
    `last_seen`       DateTime64(9)
)
ENGINE = Distributed('{cluster}', 'reiver', 'otlp_attributes_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.discovered_services
(
    `project_id`         String,
    `service_name`       String,
    `first_seen`         DateTime64(9),
    `last_seen`          DateTime64(9),
    `has_http_spans`     UInt8,
    `has_db_spans`       UInt8,
    `has_rpc_spans`      UInt8,
    `has_messaging_spans` UInt8,
    `span_count`         UInt64,
    `error_count`        UInt64
)
ENGINE = Distributed('{cluster}', 'reiver', 'discovered_services_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.discovered_services_agg
(
    `project_id`         String,
    `service_name`       String,
    `first_seen`         SimpleAggregateFunction(min, DateTime64(9)),
    `last_seen`          SimpleAggregateFunction(max, DateTime64(9)),
    `has_http_spans`     SimpleAggregateFunction(max, UInt8),
    `has_db_spans`       SimpleAggregateFunction(max, UInt8),
    `has_rpc_spans`      SimpleAggregateFunction(max, UInt8),
    `has_messaging_spans` SimpleAggregateFunction(max, UInt8),
    `span_count`         SimpleAggregateFunction(sum, UInt64),
    `error_count`        SimpleAggregateFunction(sum, UInt64)
)
ENGINE = Distributed('{cluster}', 'reiver', 'discovered_services_agg_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.exceptions
(
    `id`              String,
    `project_id`      String,
    `fingerprint`     String,
    `level`           String,
    `message`         String,
    `exception_type`  String       DEFAULT '',
    `exception_value` String       DEFAULT '',
    `stacktrace`      String       DEFAULT '',
    `context`         String,
    `tags`            String,
    `user_data`       String,
    `service_name`    String       DEFAULT '',
    `trace_id`        String       DEFAULT '',
    `span_id`         String       DEFAULT '',
    `service_version` String       DEFAULT '',
    `environment`     String       DEFAULT '',
    `repository_url`  String       DEFAULT '',
    `status`          String       DEFAULT 'unresolved',
    `timestamp`       DateTime64(9),
    `created_at`      DateTime64(9) DEFAULT now64()
)
ENGINE = Distributed('{cluster}', 'reiver', 'exceptions_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_requests
(
    `project_id`            String,
    `request_id`            String,
    `trace_id`              String,
    `span_id`               String,
    `gen_ai_system`         String,
    `gen_ai_request_model`  String,
    `gen_ai_response_model` String                  DEFAULT '',
    `gen_ai_operation_name` String                  DEFAULT '',
    `input_tokens`          UInt32                  DEFAULT 0,
    `output_tokens`         UInt32                  DEFAULT 0,
    `total_tokens`          UInt32                  DEFAULT 0,
    `cache_read_tokens`     UInt32                  DEFAULT 0,
    `cache_write_tokens`    UInt32                  DEFAULT 0,
    `cost_usd`              Float64                 DEFAULT 0,
    `timestamp`             DateTime64(9),
    `duration_ms`           UInt32                  DEFAULT 0,
    `time_to_first_token_ms` UInt32                 DEFAULT 0,
    `status_code`           LowCardinality(String)  DEFAULT 'ok',
    `error_type`            String                  DEFAULT '',
    `error_message`         String                  DEFAULT '',
    `session_id`            String                  DEFAULT '',
    `session_name`          String                  DEFAULT '',
    `user_id`               String                  DEFAULT '',
    `request_messages`      String                  DEFAULT '',
    `response_content`      String                  DEFAULT '',
    `properties`            Map(String, String)     DEFAULT map(),
    `scores`                Map(String, Float64)    DEFAULT map(),
    `service_name`          String                  DEFAULT '',
    `rollout_id`            String                  DEFAULT '',
    `rollout_variant`       String                  DEFAULT '',
    `prompt_config_id`      String                  DEFAULT '',
    `prompt_version_id`     String                  DEFAULT '',
    `request_embedding`     Array(Float32)          DEFAULT [],
    `fallback_used`         UInt8                   DEFAULT 0,
    `original_model`        String                  DEFAULT '',
    `retry_count`           UInt32                  DEFAULT 0,
    `guardrail_violations`  Array(String)           DEFAULT [],
    `temperature`           Float32                 DEFAULT 0,
    `top_p`                 Float32                 DEFAULT 0,
    `max_tokens`            UInt32                  DEFAULT 0,
    `frequency_penalty`     Float32                 DEFAULT 0,
    `presence_penalty`      Float32                 DEFAULT 0,
    `tool_call_count`       UInt32                  DEFAULT 0,
    `tool_names`            Array(String)           DEFAULT [],
    `is_platform_key`       UInt8                   DEFAULT 0
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_requests_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_chunks
(
    `project_id`    String,
    `request_id`    String,
    `chunk_index`   UInt32,
    `content`       String,
    `model`         String,
    `provider`      String,
    `timestamp`     DateTime64(3),
    `is_final`      UInt8       DEFAULT 0,
    `finish_reason` String      DEFAULT '',
    `input_tokens`  UInt32      DEFAULT 0,
    `output_tokens` UInt32      DEFAULT 0
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_chunks_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_message_labels
(
    `project_id`    String,
    `session_id`    String,
    `request_id`    String,
    `labels`        Array(String),
    `classified_at` DateTime64(3) DEFAULT now64(3)
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_message_labels_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_request_embeddings
(
    `project_id`           String,
    `request_id`           String,
    `embedding`            Array(Float32),
    `gen_ai_request_model` String,
    `user_id`              String       DEFAULT '',
    `session_id`           String       DEFAULT '',
    `timestamp`            DateTime64(9),
    `content_preview`      String       DEFAULT '',
    `created_at`           DateTime     DEFAULT now()
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_request_embeddings_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_sessions_agg
(
    `project_id`         String,
    `session_id`         String,
    `session_name`       String                                        DEFAULT '',
    `user_id`            String                                        DEFAULT '',
    `first_request_time` SimpleAggregateFunction(min, DateTime64(9)),
    `last_request_time`  SimpleAggregateFunction(max, DateTime64(9)),
    `request_count`      SimpleAggregateFunction(sum, UInt64),
    `total_input_tokens` SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens` SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`     SimpleAggregateFunction(sum, Decimal(38, 8)),
    `total_duration_ms`  SimpleAggregateFunction(sum, UInt64),
    `error_count`        SimpleAggregateFunction(sum, UInt64),
    `models`             AggregateFunction(groupUniqArray, String)
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_sessions_agg_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_model_metrics_agg
(
    `project_id`           String,
    `gen_ai_system`        String,
    `gen_ai_request_model` String,
    `hour`                 DateTime,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `total_input_tokens`   SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens`  SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8)),
    `duration_quantiles`   AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32),
    `ttft_quantiles`       AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32),
    `error_count`          SimpleAggregateFunction(sum, UInt64),
    `total_duration_ms`    SimpleAggregateFunction(sum, UInt64)         DEFAULT 0
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_model_metrics_agg_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_cost_daily
(
    `project_id`           String,
    `date`                 Date,
    `gen_ai_system`        String,
    `gen_ai_request_model` String,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `input_tokens`         SimpleAggregateFunction(sum, UInt64),
    `output_tokens`        SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8))
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_cost_daily_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_prompt_metrics_agg
(
    `project_id`           String,
    `prompt_config_id`     String,
    `prompt_version_id`    String,
    `date`                 Date,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `error_count`          SimpleAggregateFunction(sum, UInt64),
    `total_duration_ms`    SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8)),
    `total_input_tokens`   SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens`  SimpleAggregateFunction(sum, UInt64)
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_prompt_metrics_agg_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_rollout_metrics_agg
(
    `project_id`           String,
    `rollout_id`           String,
    `rollout_variant`      LowCardinality(String),
    `hour`                 DateTime,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `error_count`          SimpleAggregateFunction(sum, UInt64),
    `total_duration_ms`    SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8)),
    `total_input_tokens`   SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens`  SimpleAggregateFunction(sum, UInt64),
    `duration_quantiles`   AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32)
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_rollout_metrics_agg_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.llm_user_metrics_agg
(
    `project_id`           String,
    `user_id`              String,
    `date`                 Date,
    `request_count`        SimpleAggregateFunction(sum, UInt64),
    `session_count`        AggregateFunction(uniq, String),
    `total_input_tokens`   SimpleAggregateFunction(sum, UInt64),
    `total_output_tokens`  SimpleAggregateFunction(sum, UInt64),
    `total_cost_usd`       SimpleAggregateFunction(sum, Decimal(38, 8)),
    `error_count`          SimpleAggregateFunction(sum, UInt64),
    `models`               AggregateFunction(groupUniqArray, String)
)
ENGINE = Distributed('{cluster}', 'reiver', 'llm_user_metrics_agg_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.profiles
(
    `id`              String,
    `project_id`      String,
    `service_name`    String,
    `trace_id`        Nullable(String),
    `span_id`         Nullable(String),
    `profile_id`      String,
    `time_unix_nano`  UInt64,
    `duration_nano`   UInt64,
    `period_type`     String,
    `period`          Int64,
    `sample_count`    UInt64,
    `profile_data`    String,
    `dictionary_data` String,
    `timestamp`       DateTime64(3),
    `created_at`      DateTime64(3) DEFAULT now64(),
    `service_version` String        DEFAULT ''
)
ENGINE = Distributed('{cluster}', 'reiver', 'profiles_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.profile_version_stats
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
ENGINE = Distributed('{cluster}', 'reiver', 'profile_version_stats_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.game_matches
(
    `project_id`              String,
    `match_id`                String,
    `match_mode`              LowCardinality(String)  DEFAULT '',
    `match_type`              LowCardinality(String)  DEFAULT '',
    `match_map`               String                  DEFAULT '',
    `server_id`               String                  DEFAULT '',
    `server_region`           LowCardinality(String)  DEFAULT '',
    `start_time`              DateTime64(9),
    `end_time`                DateTime64(9)           DEFAULT toDateTime64(0, 9),
    `duration_seconds`        Float64                 DEFAULT 0,
    `player_count`            UInt16                  DEFAULT 0,
    `max_player_count`        UInt16                  DEFAULT 0,
    `outcome`                 LowCardinality(String)  DEFAULT '',
    `winning_team`            String                  DEFAULT '',
    `avg_server_tick_rate`    Float64                 DEFAULT 0,
    `avg_player_rtt_seconds`  Float64                 DEFAULT 0,
    `max_player_rtt_seconds`  Float64                 DEFAULT 0,
    `packet_loss_ratio`       Float64                 DEFAULT 0,
    `error_count`             UInt32                  DEFAULT 0,
    `crash_count`             UInt32                  DEFAULT 0,
    `properties`              Map(String, String)     DEFAULT map()
)
ENGINE = Distributed('{cluster}', 'reiver', 'game_matches_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.game_match_players
(
    `project_id`        String,
    `match_id`          String,
    `session_id`        String,
    `player_id`         String                  DEFAULT '',
    `team`              String                  DEFAULT '',
    `join_time`         DateTime64(9),
    `leave_time`        DateTime64(9)           DEFAULT toDateTime64(0, 9),
    `duration_seconds`  Float64                 DEFAULT 0,
    `leave_reason`      LowCardinality(String)  DEFAULT '',
    `avg_fps`           Float64                 DEFAULT 0,
    `avg_rtt_seconds`   Float64                 DEFAULT 0,
    `packet_loss_ratio` Float64                 DEFAULT 0,
    `stats`             Map(String, Float64)    DEFAULT map()
)
ENGINE = Distributed('{cluster}', 'reiver', 'game_match_players_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.game_player_sessions
(
    `project_id`             String,
    `session_id`             String,
    `player_id`              String                  DEFAULT '',
    `device_id`              String                  DEFAULT '',
    `device_manufacturer`    String                  DEFAULT '',
    `device_model`           String                  DEFAULT '',
    `platform`               LowCardinality(String)  DEFAULT '',
    `gpu_vendor`             String                  DEFAULT '',
    `gpu_model`              String                  DEFAULT '',
    `game_version`           String                  DEFAULT '',
    `start_time`             DateTime64(9),
    `end_time`               DateTime64(9)           DEFAULT toDateTime64(0, 9),
    `duration_seconds`       Float64                 DEFAULT 0,
    `end_reason`             LowCardinality(String)  DEFAULT '',
    `avg_fps`                Float64                 DEFAULT 0,
    `min_fps`                Float64                 DEFAULT 0,
    `p95_frame_time_seconds` Float64                 DEFAULT 0,
    `avg_rtt_seconds`        Float64                 DEFAULT 0,
    `avg_jitter_seconds`     Float64                 DEFAULT 0,
    `packet_loss_ratio`      Float64                 DEFAULT 0,
    `peak_memory_bytes`      UInt64                  DEFAULT 0,
    `matches_played`         UInt16                  DEFAULT 0,
    `matches_completed`      UInt16                  DEFAULT 0,
    `quality_score`          Float64                 DEFAULT 0,
    `country_code`           LowCardinality(String)  DEFAULT '',
    `region`                 String                  DEFAULT '',
    `properties`             Map(String, String)     DEFAULT map()
)
ENGINE = Distributed('{cluster}', 'reiver', 'game_player_sessions_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.game_metrics_hourly
(
    `project_id`          String,
    `hour`                DateTime,
    `match_mode`          LowCardinality(String)  DEFAULT '',
    `server_region`       LowCardinality(String)  DEFAULT '',
    `platform`            LowCardinality(String)  DEFAULT '',
    `match_starts`        SimpleAggregateFunction(sum, UInt64),
    `match_completions`   SimpleAggregateFunction(sum, UInt64),
    `match_abandonments`  SimpleAggregateFunction(sum, UInt64),
    `unique_players`      AggregateFunction(uniq, String),
    `player_sessions`     SimpleAggregateFunction(sum, UInt64),
    `avg_tick_rate`       Float64                 DEFAULT 0,
    `avg_fps`             Float64                 DEFAULT 0,
    `avg_rtt_seconds`     Float64                 DEFAULT 0,
    `avg_packet_loss`     Float64                 DEFAULT 0,
    `crash_count`         SimpleAggregateFunction(sum, UInt64),
    `error_count`         SimpleAggregateFunction(sum, UInt64)
)
ENGINE = Distributed('{cluster}', 'reiver', 'game_metrics_hourly_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.health_check_results
(
    `id`                    String,
    `check_id`              String,
    `project_id`            String,
    `timestamp`             DateTime64(9),
    `check_type`            String,
    `check_name`            String,
    `target`                String,
    `status`                String,
    `success`               UInt8,
    `response_time_ms`      Float64,
    `dns_time_ms`           Float64,
    `connect_time_ms`       Float64,
    `tls_time_ms`           Float64,
    `first_byte_time_ms`    Float64,
    `http_status_code`      Int32,
    `http_response_size`    Int64,
    `ssl_valid`             UInt8,
    `ssl_days_until_expiry` Int32,
    `ssl_issuer`            String,
    `ssl_subject`           String,
    `ssl_expires_at`        Nullable(DateTime64(9)),
    `error_type`            String,
    `error_message`         String,
    `agent_id`              String,
    `agent_location`        String,
    `response_body`         String
)
ENGINE = Distributed('{cluster}', 'reiver', 'health_check_results_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.health_check_uptime_hourly
(
    `project_id`           String,
    `check_id`             String,
    `hour`                 DateTime,
    `total_checks`         UInt64,
    `successful_checks`    UInt64,
    `avg_response_time_ms` Float64
)
ENGINE = Distributed('{cluster}', 'reiver', 'health_check_uptime_hourly_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.audit_events
(
    `event_id`               String,
    `project_id`             String                  DEFAULT '',
    `event_type`             LowCardinality(String),
    `action`                 String                  DEFAULT '',
    `caller_type`            LowCardinality(String),
    `caller_user_id`         String                  DEFAULT '',
    `caller_key_label`       String                  DEFAULT '',
    `caller_key_prefix`      String                  DEFAULT '',
    `service`                LowCardinality(String)  DEFAULT '',
    `http_method`            LowCardinality(String)  DEFAULT '',
    `http_path`              String                  DEFAULT '',
    `http_status`            UInt16                  DEFAULT 0,
    `source_id`              String                  DEFAULT '',
    `prompt_config_name`     String                  DEFAULT '',
    `prompt_config_id`       String                  DEFAULT '',
    `prompt_version_id`      String                  DEFAULT '',
    `prompt_version_number`  UInt32                  DEFAULT 0,
    `rendered_system_prompt` String                  DEFAULT '',
    `prompt_variables`       String                  DEFAULT '',
    `model_used`             String                  DEFAULT '',
    `total_input_tokens`     UInt64                  DEFAULT 0,
    `total_output_tokens`    UInt64                  DEFAULT 0,
    `total_turns`            UInt32                  DEFAULT 0,
    `tool_calls_log`         String                  DEFAULT '[]',
    `mcp_tool_name`          String                  DEFAULT '',
    `mcp_tool_arguments`     String                  DEFAULT '',
    `mcp_tool_success`       UInt8                   DEFAULT 1,
    `mcp_tool_error`         String                  DEFAULT '',
    `timestamp`              DateTime64(3),
    `duration_ms`            UInt64                  DEFAULT 0,
    `organization_id`        String                  DEFAULT '',
    `actor_id`               String                  DEFAULT '',
    `ip_address`             String                  DEFAULT '',
    `user_agent`             String                  DEFAULT '',
    `resource_type`          LowCardinality(String)  DEFAULT '',
    `resource_id`            String                  DEFAULT '',
    `details`                String                  DEFAULT '',
    `success`                UInt8                   DEFAULT 1,
    `error_message`          String                  DEFAULT '',
    `origin_type`            LowCardinality(String)  DEFAULT '',
    `origin_ref`             String                  DEFAULT '',
    `origin_reason`          String                  DEFAULT ''
)
ENGINE = Distributed('{cluster}', 'reiver', 'audit_events_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.usage
(
    `project_id` String,
    `event_type` LowCardinality(String),
    `date`       Date,
    `value`      UInt64
)
ENGINE = Distributed('{cluster}', 'reiver', 'usage_local', cityHash64(project_id));

CREATE TABLE IF NOT EXISTS reiver.usage_hourly
(
    `organization_id` String,
    `project_id`      String,
    `event_type`      LowCardinality(String),
    `hour`            DateTime,
    `events_count`    UInt64,
    `ingested_bytes`  UInt64 DEFAULT 0
)
ENGINE = Distributed('{cluster}', 'reiver', 'usage_hourly_local', cityHash64(organization_id));

CREATE TABLE IF NOT EXISTS reiver.a2a_tasks
(
    `task_id`         UUID,
    `context_id`      Nullable(UUID),
    `source_agent_id` UUID,
    `target_agent_id` UUID,
    `source_org_id`   UUID,
    `target_org_id`   UUID,
    `status`          String,
    `metadata`        String        DEFAULT '{}',
    `artifacts`       String        DEFAULT '[]',
    `updated_at`      DateTime64(3),
    `created_at`      DateTime64(3)
)
ENGINE = Distributed('{cluster}', 'reiver', 'a2a_tasks_local', cityHash64(task_id));

CREATE TABLE IF NOT EXISTS reiver.a2a_messages
(
    `message_id`         UUID,
    `task_id`            UUID,
    `context_id`         Nullable(UUID),
    `role`               String,
    `parts`              String       DEFAULT '[]',
    `reference_task_ids` Array(UUID),
    `metadata`           String       DEFAULT '{}',
    `pipeline_flags`     String       DEFAULT '{}',
    `created_at`         DateTime64(3)
)
ENGINE = Distributed('{cluster}', 'reiver', 'a2a_messages_local', cityHash64(task_id));

CREATE TABLE IF NOT EXISTS reiver.a2a_request_log
(
    `request_id`         UUID,
    `task_id`            UUID,
    `source_agent_id`    UUID,
    `target_agent_id`    UUID,
    `source_org_id`      UUID,
    `target_org_id`      UUID,
    `method`             String,
    `status_code`        UInt16,
    `latency_ms`         UInt32,
    `message_parts_count` UInt16,
    `pii_redacted`       Bool,
    `injection_flagged`  Bool,
    `timestamp`          DateTime64(3)
)
ENGINE = Distributed('{cluster}', 'reiver', 'a2a_request_log_local', cityHash64(task_id));

CREATE TABLE IF NOT EXISTS reiver.provider_latency_samples
(
    `provider`    String,
    `ts`          DateTime64(3),
    `duration_ms` UInt64
)
ENGINE = Distributed('{cluster}', 'reiver', 'provider_latency_samples_local', cityHash64(provider));

-- ============================================================================
-- MATERIALIZED VIEWS
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.discovered_services_mv
TO reiver.discovered_services_agg_local
(
    `project_id`         String,
    `service_name`       String,
    `first_seen`         DateTime64(9),
    `last_seen`          DateTime64(9),
    `has_http_spans`     UInt8,
    `has_db_spans`       UInt8,
    `has_rpc_spans`      UInt8,
    `has_messaging_spans` UInt8,
    `span_count`         UInt64,
    `error_count`        UInt64
)
AS SELECT
    project_id,
    service_name,
    min(timestamp) AS first_seen,
    max(timestamp) AS last_seen,
    max(if(((span_attributes['http.route']) != '') OR ((span_attributes['http.method']) != '') OR (span_kind = 'SPAN_KIND_SERVER'), 1, 0)) AS has_http_spans,
    max(if((span_attributes['db.system']) != '', 1, 0)) AS has_db_spans,
    max(if(((span_attributes['rpc.system']) != '') OR (span_kind = 'SPAN_KIND_CLIENT'), 1, 0)) AS has_rpc_spans,
    max(if((span_attributes['messaging.system']) != '', 1, 0)) AS has_messaging_spans,
    count() AS span_count,
    countIf(status_code = 'STATUS_CODE_ERROR') AS error_count
FROM reiver.spans_local
WHERE service_name != ''
GROUP BY project_id, service_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.discovered_metric_services_mv
TO reiver.discovered_services_agg_local
(
    `project_id`         String,
    `service_name`       String,
    `first_seen`         DateTime64(9),
    `last_seen`          DateTime64(9),
    `has_http_spans`     UInt8,
    `has_db_spans`       UInt8,
    `has_rpc_spans`      UInt8,
    `has_messaging_spans` UInt8,
    `span_count`         UInt64,
    `error_count`        UInt64
)
AS SELECT
    toString(project_id) AS project_id,
    resource_attributes['service.name'] AS service_name,
    toDateTime64(intDiv(min(unix_milli), 1000), 9) AS first_seen,
    toDateTime64(intDiv(max(unix_milli), 1000), 9) AS last_seen,
    toUInt8(0) AS has_http_spans,
    toUInt8(0) AS has_db_spans,
    toUInt8(0) AS has_rpc_spans,
    toUInt8(0) AS has_messaging_spans,
    toUInt64(count()) AS span_count,
    toUInt64(0) AS error_count
FROM reiver.samples_v1_local
WHERE (resource_attributes['service.name']) != ''
GROUP BY project_id, service_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.game_services_mv
TO reiver.discovered_services_agg_local
(
    `project_id`         String,
    `service_name`       String,
    `first_seen`         DateTime64(9),
    `last_seen`          DateTime64(9),
    `has_http_spans`     UInt8,
    `has_db_spans`       UInt8,
    `has_rpc_spans`      UInt8,
    `has_messaging_spans` UInt8,
    `span_count`         UInt64,
    `error_count`        UInt64
)
AS SELECT
    project_id,
    service_name,
    min(timestamp) AS first_seen,
    max(timestamp) AS last_seen,
    max(if(((span_attributes['game.match.id']) != '') OR ((span_attributes['game.server.region']) != '') OR (span_name LIKE 'game.%') OR (span_name LIKE 'match.%'), 1, 0)) AS has_http_spans,
    0 AS has_db_spans,
    0 AS has_rpc_spans,
    0 AS has_messaging_spans,
    count() AS span_count,
    countIf(status_code = 'STATUS_CODE_ERROR') AS error_count
FROM reiver.spans_local
WHERE ((span_attributes['game.match.id']) != '')
   OR ((span_attributes['game.server.region']) != '')
   OR (span_name LIKE 'game.%')
   OR (span_name LIKE 'match.%')
GROUP BY project_id, service_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.samples_v1_agg_5m_mv
TO reiver.samples_v1_agg_5m_local
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `unix_milli`          Int64,
    `sum`                 Float64,
    `count`               UInt64,
    `min`                 Float64,
    `max`                 Float64,
    `last`                Float64,
    `temporality`         String,
    `resource_attributes` Map(String, String),
    `metric_attributes`   Map(String, String)
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
    anyLast(temporality) AS temporality,
    anyLast(resource_attributes) AS resource_attributes,
    anyLast(metric_attributes) AS metric_attributes
FROM reiver.samples_v1_local
WHERE bitAnd(flags, 1) = 0
GROUP BY project_id, metric_name, fingerprint, unix_milli;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.samples_v1_agg_30m_mv
TO reiver.samples_v1_agg_30m_local
(
    `project_id`          UUID,
    `metric_name`         LowCardinality(String),
    `fingerprint`         UInt64,
    `unix_milli`          Int64,
    `sum`                 Float64,
    `count`               UInt64,
    `min`                 Float64,
    `max`                 Float64,
    `last`                Float64,
    `temporality`         String,
    `resource_attributes` Map(String, String),
    `metric_attributes`   Map(String, String)
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
    anyLast(temporality) AS temporality,
    anyLast(resource_attributes) AS resource_attributes,
    anyLast(metric_attributes) AS metric_attributes
FROM reiver.samples_v1_agg_5m_local
GROUP BY project_id, metric_name, fingerprint, unix_milli;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.llm_sessions_mv
TO reiver.llm_sessions_agg_local
(
    `project_id`         String,
    `session_id`         String,
    `session_name`       String,
    `user_id`            String,
    `first_request_time` DateTime64(9),
    `last_request_time`  DateTime64(9),
    `request_count`      UInt64,
    `total_input_tokens` UInt64,
    `total_output_tokens` UInt64,
    `total_cost_usd`     Decimal(38, 8),
    `total_duration_ms`  UInt64,
    `error_count`        UInt64,
    `models`             AggregateFunction(groupUniqArray, String)
)
AS SELECT
    project_id,
    session_id,
    anyLast(session_name) AS session_name,
    anyLast(user_id) AS user_id,
    min(timestamp) AS first_request_time,
    max(timestamp) AS last_request_time,
    count() AS request_count,
    sum(input_tokens) AS total_input_tokens,
    sum(output_tokens) AS total_output_tokens,
    sum(cost_usd) AS total_cost_usd,
    sum(duration_ms) AS total_duration_ms,
    countIf(status_code = 'error') AS error_count,
    groupUniqArrayState(gen_ai_request_model) AS models
FROM reiver.llm_requests_local
WHERE session_id != ''
GROUP BY project_id, session_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.llm_model_metrics_mv
TO reiver.llm_model_metrics_agg_local
(
    `project_id`           String,
    `gen_ai_system`        String,
    `gen_ai_request_model` String,
    `hour`                 DateTime,
    `request_count`        UInt64,
    `total_input_tokens`   UInt64,
    `total_output_tokens`  UInt64,
    `total_cost_usd`       Float64,
    `total_duration_ms`    UInt64,
    `duration_quantiles`   AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32),
    `ttft_quantiles`       AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32),
    `error_count`          UInt64
)
AS SELECT
    project_id,
    gen_ai_system,
    gen_ai_request_model,
    toStartOfHour(timestamp) AS hour,
    count() AS request_count,
    sum(input_tokens) AS total_input_tokens,
    sum(output_tokens) AS total_output_tokens,
    sum(cost_usd) AS total_cost_usd,
    sum(duration_ms) AS total_duration_ms,
    quantilesState(0.5, 0.95, 0.99)(duration_ms) AS duration_quantiles,
    quantilesState(0.5, 0.95, 0.99)(time_to_first_token_ms) AS ttft_quantiles,
    countIf(status_code = 'error') AS error_count
FROM reiver.llm_requests_local
GROUP BY project_id, gen_ai_system, gen_ai_request_model, hour;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.llm_cost_daily_mv
TO reiver.llm_cost_daily_local
(
    `project_id`           String,
    `date`                 Date,
    `gen_ai_system`        String,
    `gen_ai_request_model` String,
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
    count() AS request_count,
    sum(input_tokens) AS input_tokens,
    sum(output_tokens) AS output_tokens,
    sum(cost_usd) AS total_cost_usd
FROM reiver.llm_requests_local
GROUP BY project_id, date, gen_ai_system, gen_ai_request_model;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.llm_prompt_metrics_mv
TO reiver.llm_prompt_metrics_agg_local
(
    `project_id`           String,
    `prompt_config_id`     String,
    `prompt_version_id`    String,
    `date`                 Date,
    `request_count`        UInt64,
    `error_count`          UInt64,
    `total_duration_ms`    UInt64,
    `total_cost_usd`       Decimal(38, 8),
    `total_input_tokens`   UInt64,
    `total_output_tokens`  UInt64
)
AS SELECT
    project_id,
    prompt_config_id,
    prompt_version_id,
    toDate(timestamp) AS date,
    count() AS request_count,
    countIf(status_code = 'error') AS error_count,
    sum(duration_ms) AS total_duration_ms,
    sum(cost_usd) AS total_cost_usd,
    sum(input_tokens) AS total_input_tokens,
    sum(output_tokens) AS total_output_tokens
FROM reiver.llm_requests_local
WHERE prompt_config_id != ''
GROUP BY project_id, prompt_config_id, prompt_version_id, date;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.llm_rollout_metrics_mv
TO reiver.llm_rollout_metrics_agg_local
(
    `project_id`           String,
    `rollout_id`           String,
    `rollout_variant`      String,
    `hour`                 DateTime,
    `request_count`        UInt64,
    `error_count`          UInt64,
    `total_duration_ms`    UInt64,
    `total_cost_usd`       Decimal(38, 8),
    `total_input_tokens`   UInt64,
    `total_output_tokens`  UInt64,
    `duration_quantiles`   AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32)
)
AS SELECT
    project_id,
    rollout_id,
    rollout_variant,
    toStartOfHour(timestamp) AS hour,
    count() AS request_count,
    countIf(status_code = 'error') AS error_count,
    sum(duration_ms) AS total_duration_ms,
    sum(cost_usd) AS total_cost_usd,
    sum(input_tokens) AS total_input_tokens,
    sum(output_tokens) AS total_output_tokens,
    quantilesState(0.5, 0.95, 0.99)(duration_ms) AS duration_quantiles
FROM reiver.llm_requests_local
WHERE rollout_id != ''
GROUP BY project_id, rollout_id, rollout_variant, hour;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.llm_user_metrics_mv
TO reiver.llm_user_metrics_agg_local
(
    `project_id`           String,
    `user_id`              String,
    `date`                 Date,
    `request_count`        UInt64,
    `session_count`        AggregateFunction(uniq, String),
    `total_input_tokens`   UInt64,
    `total_output_tokens`  UInt64,
    `total_cost_usd`       Decimal(38, 8),
    `error_count`          UInt64,
    `models`               AggregateFunction(groupUniqArray, String)
)
AS SELECT
    project_id,
    user_id,
    toDate(timestamp) AS date,
    count() AS request_count,
    uniqState(session_id) AS session_count,
    sum(input_tokens) AS total_input_tokens,
    sum(output_tokens) AS total_output_tokens,
    sum(cost_usd) AS total_cost_usd,
    countIf(status_code = 'error') AS error_count,
    groupUniqArrayState(gen_ai_request_model) AS models
FROM reiver.llm_requests_local
WHERE user_id != ''
GROUP BY project_id, user_id, date;

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.health_check_uptime_hourly_mv
TO reiver.health_check_uptime_hourly_local
(
    `project_id`           String,
    `check_id`             String,
    `hour`                 DateTime,
    `total_checks`         UInt64,
    `successful_checks`    UInt64,
    `avg_response_time_ms` Float64
)
AS SELECT
    project_id,
    check_id,
    toStartOfHour(timestamp) AS hour,
    count() AS total_checks,
    countIf(success = 1) AS successful_checks,
    avg(response_time_ms) AS avg_response_time_ms
FROM reiver.health_check_results_local
GROUP BY project_id, check_id, hour;

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
    if(service_version = '', JSONExtractString(profile_data, 'service_version'), service_version) AS service_version,
    period_type,
    toStartOfHour(timestamp) AS hour,
    count() AS profile_count,
    sum(sample_count) AS total_samples,
    sum(duration_nano) AS total_duration_nano
FROM reiver.profiles_local
GROUP BY project_id, service_name, service_version, period_type, hour;
