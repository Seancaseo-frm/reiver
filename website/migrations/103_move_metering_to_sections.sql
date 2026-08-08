-- Move billing fields from the standalone 'metering' section into their
-- respective feature sections (gateway, prompt_hub, watch) with correct
-- field names and allotment values per the billing spec.
--
-- After this migration, 'metering' key is removed from all tier configs.

-- Free tier: 100 credits, 500 evals, 50 GB, 3 label types
UPDATE tier_definitions SET config =
  jsonb_set(
    jsonb_set(
      jsonb_set(config,
        '{gateway}', COALESCE(config->'gateway', '{}'::jsonb) || jsonb_build_object(
          'agent_credits_included', 100,
          'agent_credit_overage_usd', 0
        )
      ),
      '{prompt_hub}', COALESCE(config->'prompt_hub', '{}'::jsonb) || jsonb_build_object(
        'max_labels', 3,
        'session_evals_included', 500,
        'session_eval_overage_usd', 0
      )
    ),
    '{watch}', COALESCE(config->'watch', '{}'::jsonb) || jsonb_build_object(
      'ingestion_gb_included', 50,
      'traces_logs_per_gb_usd', 0,
      'metrics_per_million_usd', 0
    )
  ) - 'metering'
WHERE name = 'free';

-- Starter tier: 10k credits, 5k evals, 200 GB, 5 label types
UPDATE tier_definitions SET config =
  jsonb_set(
    jsonb_set(
      jsonb_set(config,
        '{gateway}', COALESCE(config->'gateway', '{}'::jsonb) || jsonb_build_object(
          'agent_credits_included', 10000,
          'agent_credit_overage_usd', 0.20
        )
      ),
      '{prompt_hub}', COALESCE(config->'prompt_hub', '{}'::jsonb) || jsonb_build_object(
        'max_labels', 5,
        'session_evals_included', 5000,
        'session_eval_overage_usd', 0.003
      )
    ),
    '{watch}', COALESCE(config->'watch', '{}'::jsonb) || jsonb_build_object(
      'ingestion_gb_included', 200,
      'traces_logs_per_gb_usd', 0.25,
      'metrics_per_million_usd', 0.10
    )
  ) - 'metering'
WHERE name = 'starter';

-- Scale tier: 100k credits, 30k evals, 1000 GB, 25 label types
UPDATE tier_definitions SET config =
  jsonb_set(
    jsonb_set(
      jsonb_set(config,
        '{gateway}', COALESCE(config->'gateway', '{}'::jsonb) || jsonb_build_object(
          'agent_credits_included', 100000,
          'agent_credit_overage_usd', 0.20
        )
      ),
      '{prompt_hub}', COALESCE(config->'prompt_hub', '{}'::jsonb) || jsonb_build_object(
        'max_labels', 25,
        'session_evals_included', 30000,
        'session_eval_overage_usd', 0.003
      )
    ),
    '{watch}', COALESCE(config->'watch', '{}'::jsonb) || jsonb_build_object(
      'ingestion_gb_included', 1000,
      'traces_logs_per_gb_usd', 0.25,
      'metrics_per_million_usd', 0.10
    )
  ) - 'metering'
WHERE name = 'scale';

-- Enterprise tier: unlimited ingestion, negotiated pricing
UPDATE tier_definitions SET config =
  jsonb_set(
    jsonb_set(
      jsonb_set(config,
        '{gateway}', COALESCE(config->'gateway', '{}'::jsonb) || jsonb_build_object(
          'agent_credits_included', -1,
          'agent_credit_overage_usd', 0.20
        )
      ),
      '{prompt_hub}', COALESCE(config->'prompt_hub', '{}'::jsonb) || jsonb_build_object(
        'max_labels', -1,
        'session_evals_included', -1,
        'session_eval_overage_usd', 0.003
      )
    ),
    '{watch}', COALESCE(config->'watch', '{}'::jsonb) || jsonb_build_object(
      'ingestion_gb_included', -1,
      'traces_logs_per_gb_usd', 0.25,
      'metrics_per_million_usd', 0.10
    )
  ) - 'metering'
WHERE name = 'enterprise';

-- Founders + any other tier: unlimited (safe default)
UPDATE tier_definitions SET config =
  jsonb_set(
    jsonb_set(
      jsonb_set(config,
        '{gateway}', COALESCE(config->'gateway', '{}'::jsonb) || jsonb_build_object(
          'agent_credits_included', -1,
          'agent_credit_overage_usd', 0
        )
      ),
      '{prompt_hub}', COALESCE(config->'prompt_hub', '{}'::jsonb) || jsonb_build_object(
        'max_labels', -1,
        'session_evals_included', -1,
        'session_eval_overage_usd', 0
      )
    ),
    '{watch}', COALESCE(config->'watch', '{}'::jsonb) || jsonb_build_object(
      'ingestion_gb_included', -1,
      'traces_logs_per_gb_usd', 0,
      'metrics_per_million_usd', 0
    )
  ) - 'metering'
WHERE name NOT IN ('free', 'starter', 'scale', 'enterprise');
