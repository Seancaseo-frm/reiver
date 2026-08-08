-- Add metering section to tier_definitions config JSONB.
-- Seeds allotments and overage rates per tier as specified in the billing spec.

UPDATE tier_definitions SET config = config || jsonb_build_object(
  'metering', jsonb_build_object(
    'credits_per_month', 500,
    'scans_per_month', -1,
    'observability_gb_included', 50,
    'max_label_types', 3,
    'credit_overage_usd', 0,
    'scan_price_usd', 0,
    'observability_overage_per_gb_usd', 0
  )
) WHERE name = 'free';

UPDATE tier_definitions SET config = config || jsonb_build_object(
  'metering', jsonb_build_object(
    'credits_per_month', 5000,
    'scans_per_month', -1,
    'observability_gb_included', 200,
    'max_label_types', 5,
    'credit_overage_usd', 0.20,
    'scan_price_usd', 0.003,
    'observability_overage_per_gb_usd', 0.25
  )
) WHERE name = 'starter';

UPDATE tier_definitions SET config = config || jsonb_build_object(
  'metering', jsonb_build_object(
    'credits_per_month', 30000,
    'scans_per_month', -1,
    'observability_gb_included', 1000,
    'max_label_types', 25,
    'credit_overage_usd', 0.20,
    'scan_price_usd', 0.003,
    'observability_overage_per_gb_usd', 0.25
  )
) WHERE name = 'scale';

-- Enterprise tier (if exists) gets unlimited everything
UPDATE tier_definitions SET config = config || jsonb_build_object(
  'metering', jsonb_build_object(
    'credits_per_month', 100000,
    'scans_per_month', -1,
    'observability_gb_included', -1,
    'max_label_types', -1,
    'credit_overage_usd', 0.20,
    'scan_price_usd', 0.003,
    'observability_overage_per_gb_usd', 0.25
  )
) WHERE name = 'enterprise';
