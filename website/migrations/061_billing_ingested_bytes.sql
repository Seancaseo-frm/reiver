-- Add metrics_ingested_bytes to daily snapshots for tracking purposes.
ALTER TABLE usage_daily_snapshots
    ADD COLUMN IF NOT EXISTS metrics_ingested_bytes BIGINT NOT NULL DEFAULT 0;

-- Backfill: delete existing April snapshots so the billing worker
-- recreates them from the now-corrected ClickHouse usage_hourly data.
DELETE FROM usage_daily_snapshots
WHERE date >= '2026-04-01' AND date < '2026-05-01';

-- Delete the incorrect April pending charge so it gets regenerated.
DELETE FROM pending_charges
WHERE billing_period_start = '2026-04-01'
  AND charge_type = 'watch_usage';
