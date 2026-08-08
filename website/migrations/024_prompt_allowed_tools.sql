-- Add tool whitelist to prompt versions.
-- NULL = no restriction (all tools allowed); empty array = no tools allowed.
ALTER TABLE llm_prompt_versions ADD COLUMN allowed_tools JSONB;
