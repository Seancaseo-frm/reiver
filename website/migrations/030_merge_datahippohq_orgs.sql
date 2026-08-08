-- This migration was used to merge two internal orgs during early development.
-- It is a no-op for fresh deployments and is kept only to preserve migration ordering.
DO $$
BEGIN
    RAISE NOTICE 'Migration 030: no-op for fresh deployments (internal org merge)';
END;
$$;
