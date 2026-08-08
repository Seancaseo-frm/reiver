-- Rename deprecated OTel K8s CPU metric names in prebuilt dashboard templates.
-- k8s.pod.cpu.utilization → k8s.pod.cpu.usage
-- k8s.node.cpu.utilization → k8s.node.cpu.usage
UPDATE dashboard_templates
SET template_config = REPLACE(
        REPLACE(template_config::text,
            'k8s.pod.cpu.utilization', 'k8s.pod.cpu.usage'),
        'k8s.node.cpu.utilization', 'k8s.node.cpu.usage')::jsonb
WHERE template_config::text LIKE '%k8s.pod.cpu.utilization%'
   OR template_config::text LIKE '%k8s.node.cpu.utilization%';

-- Also update any user-created dashboards that were cloned from the template.
UPDATE dashboards
SET layout_config = REPLACE(
        REPLACE(layout_config::text,
            'k8s.pod.cpu.utilization', 'k8s.pod.cpu.usage'),
        'k8s.node.cpu.utilization', 'k8s.node.cpu.usage')::jsonb
WHERE layout_config::text LIKE '%k8s.pod.cpu.utilization%'
   OR layout_config::text LIKE '%k8s.node.cpu.utilization%';
