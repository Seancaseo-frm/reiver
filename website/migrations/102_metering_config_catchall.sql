-- Ensure all tier_definitions have a metering config section.
-- The original 100_metering_config.sql only covered free/starter/scale/enterprise,
-- missing any other tiers (e.g. founders). This catch-all sets unlimited (-1) for
-- any tier that doesn't already have metering configured — the admin panel can then
-- be used to set specific limits.

UPDATE tier_definitions SET config = config || jsonb_build_object(
  'metering', jsonb_build_object(
    'credits_per_month', -1,
    'scans_per_month', -1,
    'observability_gb_included', -1,
    'max_label_types', -1,
    'credit_overage_usd', 0,
    'scan_price_usd', 0,
    'observability_overage_per_gb_usd', 0
  )
) WHERE NOT (config ? 'metering');
