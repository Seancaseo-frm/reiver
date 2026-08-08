-- Add per-tier fee rates (decimal percentages stored as JSONB).
-- Also remove dead features (response_caching, dev_staging_prod_labels) from
-- existing tier definitions.

ALTER TABLE tier_definitions ADD COLUMN rates JSONB NOT NULL DEFAULT '{}';

UPDATE tier_definitions SET
  rates = '{"gateway_fee_percent": 0, "moodeng_fee_percent": 0}',
  features = features - 'response_caching' - 'dev_staging_prod_labels';

ALTER TABLE tier_overrides ADD COLUMN rate_overrides JSONB NOT NULL DEFAULT '{}';
