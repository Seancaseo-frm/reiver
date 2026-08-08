-- Clean up usage_daily_snapshots: remove columns that are no longer populated
-- now that billing is based on ingested bytes (spans/logs) and data point
-- counts (metrics) from the new ClickHouse reiver.usage table.

ALTER TABLE usage_daily_snapshots DROP COLUMN IF EXISTS spans_count;
ALTER TABLE usage_daily_snapshots DROP COLUMN IF EXISTS logs_count;
ALTER TABLE usage_daily_snapshots DROP COLUMN IF EXISTS total_events;
ALTER TABLE usage_daily_snapshots DROP COLUMN IF EXISTS metrics_ingested_bytes;

ALTER TABLE usage_daily_snapshots RENAME COLUMN estimated_cost_usd TO cost_usd;
