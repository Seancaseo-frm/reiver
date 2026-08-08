-- Usage daily snapshots are no longer needed: billing now queries
-- ClickHouse reiver.usage directly for Watch costs and
-- reiver.llm_cost_daily for BYOK fees.
DROP TABLE IF EXISTS usage_daily_snapshots;
