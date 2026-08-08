-- Fix "Avg Tokens per Request" widget expressions that used max(count(), 1).
-- ClickHouse's max() is an aggregate function over rows, not a scalar max(a,b).
-- The correct function is greatest(count(), 1) to avoid division by zero.

-- Fix dashboard_templates (affects future dashboard instantiation)
UPDATE dashboard_templates
SET template_config = REPLACE(
    template_config::text,
    'max(count(), 1)',
    'greatest(count(), 1)'
)::jsonb
WHERE template_config::text LIKE '%max(count(), 1)%';

-- Fix already-instantiated dashboard_widgets
UPDATE dashboard_widgets
SET widget_config = REPLACE(
    widget_config::text,
    'max(count(), 1)',
    'greatest(count(), 1)'
)::jsonb
WHERE widget_config::text LIKE '%max(count(), 1)%';
