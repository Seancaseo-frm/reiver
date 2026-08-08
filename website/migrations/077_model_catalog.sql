-- ============================================================================
-- Model Catalog: replaces llm_pricing as the canonical source of model data.
-- Populated from the OpenRouter /api/v1/models API on a daily sync.
-- Stores the API response shape as-is: nested objects as JSONB, top-level
-- scalars with their original API field names.
-- ============================================================================

CREATE TABLE model_catalog (
    -- Top-level API scalars (matching OpenRouter field names)
    id                   VARCHAR(255) PRIMARY KEY,
    name                 VARCHAR(500) NOT NULL,
    created              BIGINT,
    description          TEXT,
    context_length       INT,
    canonical_slug       VARCHAR(500),
    hugging_face_id      VARCHAR(500),
    knowledge_cutoff     VARCHAR(20),
    expiration_date      VARCHAR(20),

    -- Nested API objects stored as raw JSONB
    pricing              JSONB NOT NULL DEFAULT '{}',
    architecture         JSONB NOT NULL DEFAULT '{}',
    top_provider         JSONB NOT NULL DEFAULT '{}',
    default_parameters   JSONB,
    supported_parameters JSONB NOT NULL DEFAULT '[]',

    -- Platform-specific columns (not from the API)
    provider_slug        VARCHAR(100) NOT NULL,
    model_slug           VARCHAR(255) NOT NULL,
    enabled              BOOLEAN NOT NULL DEFAULT FALSE,

    -- Housekeeping
    last_synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_model_catalog_provider ON model_catalog(provider_slug);
CREATE INDEX idx_model_catalog_enabled ON model_catalog(enabled);
CREATE INDEX idx_model_catalog_provider_model ON model_catalog(provider_slug, model_slug);

-- Drop old tables
DROP TABLE IF EXISTS llm_pricing_sync_log;
DROP TABLE IF EXISTS llm_pricing;
