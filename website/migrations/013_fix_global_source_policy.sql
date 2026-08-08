-- Fix storage_tier_policy for existing global (blockchain) sources.
-- The enable_blockchain endpoint previously omitted the "tier" key,
-- causing the UI to fall back to "cold". Global sources are always warm.
UPDATE warehouse_sources
SET storage_tier_policy = '{"type": "fixed", "tier": "warm"}'::jsonb
WHERE global_source_id IS NOT NULL
  AND (
    storage_tier_policy IS NULL
    OR storage_tier_policy = '{"type": "fixed"}'::jsonb
    OR NOT (storage_tier_policy ? 'tier')
  );
