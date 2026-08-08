-- Add token prefix and creation timestamp for the SCIM settings UI.
ALTER TABLE sso_configurations
    ADD COLUMN IF NOT EXISTS scim_bearer_token_prefix VARCHAR(16),
    ADD COLUMN IF NOT EXISTS scim_bearer_token_created_at TIMESTAMPTZ;
