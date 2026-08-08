-- Add host.name variable to Host Metrics template.
-- The OTel collector DaemonSet now adds host.name=${K8S_NODE_NAME} via resource/host processor.

UPDATE dashboard_templates
SET template_config = jsonb_set(
    template_config,
    '{variables}',
    '[{"name": "host", "label": "Host", "type": "query", "query": "label_values(system.cpu.load_average.1m, host.name)"}]'::jsonb
)
WHERE name = 'Host Metrics';
