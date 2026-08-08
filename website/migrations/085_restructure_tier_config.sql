-- Consolidate products/features/quotas/rates into a single `config` JSONB column.
-- This matches the Rust TierConfig struct: nested objects for platform, gateway,
-- prompt_hub, watch, and herd.

ALTER TABLE tier_definitions ADD COLUMN config JSONB NOT NULL DEFAULT '{}';

UPDATE tier_definitions SET config = jsonb_build_object(
  'platform', jsonb_build_object(
    'sso', COALESCE((features->>'sso')::boolean, false),
    'audit_log', COALESCE((features->>'audit_log')::boolean, false),
    'priority_support', COALESCE((features->>'priority_support')::boolean, false),
    'max_projects', COALESCE((quotas->>'max_projects')::bigint, 0)
  ),
  'gateway', jsonb_build_object(
    'requests_per_month', COALESCE((quotas->>'gateway_requests_per_month')::bigint, 0),
    'fee_percent', COALESCE((rates->>'gateway_fee_percent')::numeric, 0),
    'moodeng_fee_percent', COALESCE((rates->>'moodeng_fee_percent')::numeric, 0)
  ),
  'prompt_hub', jsonb_build_object(
    'enabled', COALESCE(products ? 'prompt_hub', false),
    'prompt_versioning', COALESCE((features->>'prompt_versioning')::boolean, false),
    'provider_fallback', COALESCE((features->>'provider_fallback')::boolean, false),
    'staged_rollouts', COALESCE((features->>'staged_rollouts')::boolean, false),
    'max_prompts', COALESCE((quotas->>'max_prompts')::bigint, 0),
    'max_prompt_versions', COALESCE((quotas->>'max_prompt_versions')::bigint, 0),
    'max_fallback_rules', COALESCE((quotas->>'max_fallback_rules')::bigint, 0),
    'max_parallel_rollouts', COALESCE((quotas->>'max_parallel_rollouts')::bigint, 0),
    'max_session_profiles', COALESCE((quotas->>'max_session_profiles')::bigint, 0),
    'max_labels', COALESCE((quotas->>'max_labels')::bigint, 0)
  ),
  'watch', jsonb_build_object(
    'enabled', COALESCE(products ? 'watch', false),
    'webhook_alerts', COALESCE((features->>'webhook_alerts')::boolean, false),
    'slack_alerts', COALESCE((features->>'slack_alerts')::boolean, false),
    'traces_logs_per_gb_usd', 0,
    'metrics_per_million_usd', 0
  ),
  'herd', jsonb_build_object(
    'enabled', COALESCE(products ? 'herd', false)
  )
);

ALTER TABLE tier_definitions
  DROP COLUMN products,
  DROP COLUMN features,
  DROP COLUMN quotas,
  DROP COLUMN rates,
  DROP COLUMN price_cents_per_month;

-- Overrides: merge into single sparse JSONB
ALTER TABLE tier_overrides ADD COLUMN config_overrides JSONB NOT NULL DEFAULT '{}';

UPDATE tier_overrides SET config_overrides = (
  SELECT jsonb_strip_nulls(jsonb_build_object(
    'platform', CASE WHEN
      (feature_overrides ? 'sso' OR feature_overrides ? 'audit_log' OR
       feature_overrides ? 'priority_support' OR quota_overrides ? 'max_projects')
      THEN jsonb_strip_nulls(jsonb_build_object(
        'sso', (feature_overrides->>'sso')::boolean,
        'audit_log', (feature_overrides->>'audit_log')::boolean,
        'priority_support', (feature_overrides->>'priority_support')::boolean,
        'max_projects', (quota_overrides->>'max_projects')::bigint
      ))
    END,
    'gateway', CASE WHEN
      (quota_overrides ? 'gateway_requests_per_month' OR
       rate_overrides ? 'gateway_fee_percent' OR rate_overrides ? 'moodeng_fee_percent')
      THEN jsonb_strip_nulls(jsonb_build_object(
        'requests_per_month', (quota_overrides->>'gateway_requests_per_month')::bigint,
        'fee_percent', (rate_overrides->>'gateway_fee_percent')::numeric,
        'moodeng_fee_percent', (rate_overrides->>'moodeng_fee_percent')::numeric
      ))
    END,
    'prompt_hub', CASE WHEN
      (product_overrides ? 'prompt_hub' OR feature_overrides ? 'prompt_versioning' OR
       feature_overrides ? 'provider_fallback' OR feature_overrides ? 'staged_rollouts' OR
       quota_overrides ? 'max_prompts' OR quota_overrides ? 'max_prompt_versions')
      THEN jsonb_strip_nulls(jsonb_build_object(
        'enabled', CASE WHEN product_overrides ? 'prompt_hub' THEN true END,
        'prompt_versioning', (feature_overrides->>'prompt_versioning')::boolean,
        'provider_fallback', (feature_overrides->>'provider_fallback')::boolean,
        'staged_rollouts', (feature_overrides->>'staged_rollouts')::boolean,
        'max_prompts', (quota_overrides->>'max_prompts')::bigint,
        'max_prompt_versions', (quota_overrides->>'max_prompt_versions')::bigint,
        'max_fallback_rules', (quota_overrides->>'max_fallback_rules')::bigint,
        'max_parallel_rollouts', (quota_overrides->>'max_parallel_rollouts')::bigint,
        'max_session_profiles', (quota_overrides->>'max_session_profiles')::bigint,
        'max_labels', (quota_overrides->>'max_labels')::bigint
      ))
    END,
    'watch', CASE WHEN
      (product_overrides ? 'watch' OR feature_overrides ? 'webhook_alerts' OR
       feature_overrides ? 'slack_alerts')
      THEN jsonb_strip_nulls(jsonb_build_object(
        'enabled', CASE WHEN product_overrides ? 'watch' THEN true END,
        'webhook_alerts', (feature_overrides->>'webhook_alerts')::boolean,
        'slack_alerts', (feature_overrides->>'slack_alerts')::boolean
      ))
    END,
    'herd', CASE WHEN product_overrides ? 'herd'
      THEN jsonb_build_object('enabled', true)
    END
  ))
);

ALTER TABLE tier_overrides
  DROP COLUMN product_overrides,
  DROP COLUMN feature_overrides,
  DROP COLUMN quota_overrides,
  DROP COLUMN rate_overrides;

DROP TABLE IF EXISTS billing_pricing;
