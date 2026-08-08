-- Fix HTTP Service template: use `.count`/`.sum` (OTel storage form) instead of
-- `_count`/`_sum` (Prometheus convention). Ingestion stores histogram decompositions
-- as `{base}.count` and `{base}.sum`, so the template queries must match.
--
-- The platform now also normalizes `_count`→`.count` and `_sum`→`.sum` at query
-- time for OTel-dotted metrics, but templates should use the canonical form.

UPDATE dashboard_templates
SET template_config = REPLACE(
    REPLACE(
        REPLACE(
            REPLACE(template_config::text,
                'duration_count', 'duration.count'),
            'duration_sum', 'duration.sum'),
        'size_count', 'size.count'),
    'size_sum', 'size.sum'
)::jsonb
WHERE name = 'HTTP Service'
  AND template_config::text LIKE '%\_count%';
