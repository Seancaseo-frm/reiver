-- Add scoped permissions, labels, expiry, and key type to project_keys
ALTER TABLE project_keys
  ADD COLUMN label VARCHAR(255),
  ADD COLUMN created_by UUID REFERENCES users(id) ON DELETE SET NULL,
  ADD COLUMN scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN expires_at TIMESTAMPTZ,
  ADD COLUMN key_type VARCHAR(20) NOT NULL DEFAULT 'sdk',
  ADD COLUMN key_prefix VARCHAR(8);

-- Backfill existing keys with full access scopes
UPDATE project_keys SET scopes = '["project:read","project:write","llm:read","llm:write","observability:read","observability:write","billing:read"]'::jsonb;

-- Backfill key_prefix for masking (last 4 chars)
UPDATE project_keys SET key_prefix = RIGHT(key, 4);
