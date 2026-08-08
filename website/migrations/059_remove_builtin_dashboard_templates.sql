-- Remove all built-in dashboard templates.
-- The system now uses Grafana dashboard import instead of built-in templates.
DELETE FROM dashboard_templates;
