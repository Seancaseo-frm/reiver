-- This migration was used to rebrand internal user emails during development.
-- It is a no-op for fresh deployments and is kept only to preserve migration ordering.
DO $$
BEGIN
    RAISE NOTICE 'Migration 072: no-op for fresh deployments (internal rebrand)';
END;
$$;
