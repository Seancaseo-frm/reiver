-- Per-project key/value configuration store.
--
-- Used by the Flow LLM gateway to store encrypted provider API keys
-- (e.g. gateway_openai_api_key), gateway settings (rate limits, guardrails,
-- session budgets, prompt mode), and any other per-project configuration
-- that services need to read at request time.
--
-- Keys are namespaced by convention: `gateway_*` for Flow, `pond_*` for Pond, etc.
-- Values are plain text unless the service encrypts them (e.g. API keys are
-- AES-256 encrypted by Flow before insert, using ENCRYPTION_KEY).

CREATE TABLE project_settings (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id   UUID        NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key          TEXT        NOT NULL,
    value        TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (project_id, key)
);

CREATE INDEX idx_project_settings_project_id ON project_settings(project_id);
