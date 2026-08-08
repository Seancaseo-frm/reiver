-- Drop all existing dashboards (pre-launch, no backwards compat needed).
-- CASCADE removes dashboard_tabs and dashboard_widgets via FK.
TRUNCATE dashboards CASCADE;

-- Every dashboard must store its original import payload for reconversion.
ALTER TABLE dashboards
  ADD COLUMN import_source JSONB NOT NULL;
