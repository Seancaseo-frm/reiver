-- Add a persisted mode column to pipelines so list queries can return it
-- without loading all node configs.

ALTER TABLE warehouse_pipelines
    ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'batch';
