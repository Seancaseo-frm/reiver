-- Track whether a prompt version was created by a user, agent, or system,
-- and store the agent key label for attribution.
ALTER TABLE llm_prompt_versions
    ADD COLUMN created_by_type VARCHAR(10) NOT NULL DEFAULT 'user',
    ADD COLUMN created_by_key_label VARCHAR(255);

-- Backfill: versions with created_by = NULL were created before this migration
-- (all by users via the UI), so the default 'user' is correct.
