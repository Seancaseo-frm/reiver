-- Add locked column to dashboards to prevent accidental edits.
ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS locked BOOLEAN NOT NULL DEFAULT false;
