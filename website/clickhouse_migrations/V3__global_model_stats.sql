-- ============================================================================
-- V3__global_model_stats.sql
-- Global (cross-project) model-level stats for the public pricing page.
-- Refreshable MV runs every 2 minutes as a batch query over 24h of
-- llm_requests — no per-insert overhead on the hot ingestion path.
-- ============================================================================

CREATE TABLE IF NOT EXISTS reiver.llm_global_model_stats_local
(
    `hour`                      DateTime,
    `provider`                  String,
    `model`                     String,
    `request_count`             UInt64,
    `error_count`               UInt64,
    `total_duration_ms`         UInt64,
    `p50_duration_ms`           Float64,
    `p95_duration_ms`           Float64,
    `p99_duration_ms`           Float64,
    `p50_ttft_ms`               Float64,
    `p95_ttft_ms`               Float64,
    `guardrail_triggered_count` UInt64,
    `pii_violation_count`       UInt64,
    `injection_violation_count` UInt64
)
ENGINE = ReplicatedMergeTree()
ORDER BY (provider, model, hour)
TTL hour + INTERVAL 48 HOUR;

CREATE TABLE IF NOT EXISTS reiver.llm_global_model_stats
AS reiver.llm_global_model_stats_local
ENGINE = Distributed('{cluster}', 'reiver', 'llm_global_model_stats_local', cityHash64(provider));

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.llm_global_model_stats_mv
REFRESH EVERY 2 MINUTE
TO reiver.llm_global_model_stats_local AS
SELECT
    toStartOfHour(timestamp)                             AS hour,
    gen_ai_system                                        AS provider,
    gen_ai_request_model                                 AS model,
    count()                                              AS request_count,
    countIf(status_code = 'error')                       AS error_count,
    sum(duration_ms)                                     AS total_duration_ms,
    quantile(0.5)(duration_ms)                           AS p50_duration_ms,
    quantile(0.95)(duration_ms)                          AS p95_duration_ms,
    quantile(0.99)(duration_ms)                          AS p99_duration_ms,
    quantile(0.5)(time_to_first_token_ms)                AS p50_ttft_ms,
    quantile(0.95)(time_to_first_token_ms)               AS p95_ttft_ms,
    countIf(length(guardrail_violations) > 0)            AS guardrail_triggered_count,
    countIf(hasAny(guardrail_violations, ['PiiBlocked', 'PiiRedacted']))
                                                         AS pii_violation_count,
    countIf(has(guardrail_violations, 'PromptInjectionDetected'))
                                                         AS injection_violation_count
FROM reiver.llm_requests
WHERE timestamp >= now() - INTERVAL 24 HOUR
  AND gen_ai_system != ''
  AND gen_ai_request_model != ''
GROUP BY hour, provider, model;
