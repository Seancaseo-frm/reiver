-- Remove orphaned preferred_models rows now that the feature has been removed.
-- The gateway_preferred_models key was used for project-level auto-routing
-- preferences, which have been replaced by per-request `models` arrays and
-- `default_fallback_models`.
DELETE FROM project_settings WHERE key = 'gateway_preferred_models';
