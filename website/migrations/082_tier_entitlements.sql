-- Tier definitions: named tiers with products, features, and quotas.
-- Three standard tiers are seeded; admins can create additional custom tiers.
CREATE TABLE tier_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    price_cents_per_month INT NOT NULL DEFAULT 0,
    products JSONB NOT NULL DEFAULT '[]',
    features JSONB NOT NULL DEFAULT '{}',
    quotas JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed the three standard tiers
INSERT INTO tier_definitions (name, display_name, price_cents_per_month, products, features, quotas) VALUES
('free', 'Free', 0,
 '["prompt_hub", "herd"]',
 '{"prompt_versioning": false, "staged_rollouts": false, "dev_staging_prod_labels": false, "provider_fallback": false, "response_caching": false, "webhook_alerts": false, "slack_alerts": false, "sso": false, "audit_log": false, "priority_support": false}',
 '{"max_projects": 1, "max_prompts": 10, "max_prompt_versions": 5, "gateway_requests_per_month": 50000, "max_fallback_rules": 0, "max_parallel_rollouts": 1, "max_session_profiles": 1, "max_labels": 5}'
),
('starter', 'Starter', 0,
 '["prompt_hub", "herd"]',
 '{"prompt_versioning": true, "staged_rollouts": false, "dev_staging_prod_labels": true, "provider_fallback": true, "response_caching": false, "webhook_alerts": false, "slack_alerts": false, "sso": false, "audit_log": true, "priority_support": false}',
 '{"max_projects": 3, "max_prompts": 50, "max_prompt_versions": 50, "gateway_requests_per_month": 250000, "max_fallback_rules": 1, "max_parallel_rollouts": 5, "max_session_profiles": 5, "max_labels": 50}'
),
('scale', 'Scale', 0,
 '["prompt_hub", "herd"]',
 '{"prompt_versioning": true, "staged_rollouts": true, "dev_staging_prod_labels": true, "provider_fallback": true, "response_caching": true, "webhook_alerts": true, "slack_alerts": true, "sso": true, "audit_log": true, "priority_support": true}',
 '{"max_projects": 10, "max_prompts": 500, "max_prompt_versions": 500, "gateway_requests_per_month": 2000000, "max_fallback_rules": -1, "max_parallel_rollouts": 50, "max_session_profiles": 50, "max_labels": 500}'
);

-- Link organizations to their tier
ALTER TABLE organizations
    ADD COLUMN tier_definition_id UUID REFERENCES tier_definitions(id);

-- Default all existing orgs to the 'free' tier
UPDATE organizations SET tier_definition_id = (
    SELECT id FROM tier_definitions WHERE name = 'free'
);
ALTER TABLE organizations
    ALTER COLUMN tier_definition_id SET NOT NULL;

-- Per-org overrides for one-off exceptions on top of the assigned tier
CREATE TABLE tier_overrides (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    product_overrides JSONB NOT NULL DEFAULT '[]',
    feature_overrides JSONB NOT NULL DEFAULT '{}',
    quota_overrides JSONB NOT NULL DEFAULT '{}',
    reason TEXT,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(organization_id)
);
