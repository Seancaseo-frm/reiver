-- Remove orphaned prompt_mode rows now that the feature has been removed.
-- Managed prompts now always require explicit opt-in via prompt_config;
-- the auto/explicit toggle is no longer needed.
DELETE FROM project_settings WHERE key = 'gateway_prompt_mode';
