-- Fix: migration 094 used WHERE name = 'Kubernetes' but the template is named 'Kubernetes Cluster'

UPDATE dashboard_templates
SET template_config = '{
    "variables": [
        {"name": "namespace", "label": "Namespace", "type": "query", "query": "label_values(k8s.pod.phase, k8s.namespace.name)"},
        {"name": "node", "label": "Node", "type": "query", "query": "label_values(k8s.node.condition_ready, k8s.node.name)"}
    ],
    "tabs": [
        {
            "name": "Cluster",
            "icon": "cloud",
            "widgets": [
                {"type": "stat", "title": "Ready Nodes", "x": 0, "y": 0, "w": 2, "h": 2, "config": {"query": {"promql": "sum(k8s.node.condition_ready)", "instant": true}}},
                {"type": "stat", "title": "Running Pods", "x": 2, "y": 0, "w": 2, "h": 2, "config": {"query": {"promql": "count(k8s.pod.phase{k8s.namespace.name=~\"$namespace\"} == 2)", "instant": true}}},
                {"type": "stat", "title": "Failed Pods", "x": 4, "y": 0, "w": 2, "h": 2, "config": {"query": {"promql": "count(k8s.pod.phase{k8s.namespace.name=~\"$namespace\"} == 4) or vector(0)", "instant": true}, "thresholds": [{"value": 0, "color": "green"}, {"value": 1, "color": "red"}]}},
                {"type": "stat", "title": "Container Restarts (24h)", "x": 6, "y": 0, "w": 2, "h": 2, "config": {"query": {"promql": "sum(increase(k8s.container.restarts{k8s.namespace.name=~\"$namespace\"}[24h]))", "instant": true}, "thresholds": [{"value": 0, "color": "green"}, {"value": 5, "color": "orange"}, {"value": 20, "color": "red"}]}},
                {"type": "stat", "title": "Available Deployments", "x": 8, "y": 0, "w": 2, "h": 2, "config": {"query": {"promql": "sum(k8s.deployment.available{k8s.namespace.name=~\"$namespace\"})", "instant": true}}},
                {"type": "stat", "title": "Pending Pods", "x": 10, "y": 0, "w": 2, "h": 2, "config": {"query": {"promql": "count(k8s.pod.phase{k8s.namespace.name=~\"$namespace\"} == 1) or vector(0)", "instant": true}, "thresholds": [{"value": 0, "color": "green"}, {"value": 1, "color": "orange"}]}},
                {"type": "timeseries", "title": "Pod Count by Phase", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "count(k8s.pod.phase{k8s.namespace.name=~\"$namespace\"} == 2)", "legend_format": "Running"}, {"promql": "count(k8s.pod.phase{k8s.namespace.name=~\"$namespace\"} == 1) or vector(0)", "legend_format": "Pending"}, {"promql": "count(k8s.pod.phase{k8s.namespace.name=~\"$namespace\"} == 4) or vector(0)", "legend_format": "Failed"}, {"promql": "count(k8s.pod.phase{k8s.namespace.name=~\"$namespace\"} == 3) or vector(0)", "legend_format": "Succeeded"}]}}},
                {"type": "timeseries", "title": "Node CPU Usage", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "k8s.node.cpu.usage{k8s.node.name=~\"$node\"}"}, "unit": "cores"}},
                {"type": "timeseries", "title": "Node Memory", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "k8s.node.memory.usage{k8s.node.name=~\"$node\"}", "legend_format": "used"}, {"promql": "k8s.node.memory.available{k8s.node.name=~\"$node\"}", "legend_format": "available"}]}, "unit": "bytes"}},
                {"type": "timeseries", "title": "Container Restarts Rate", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (k8s.pod.name) (rate(k8s.container.restarts{k8s.namespace.name=~\"$namespace\"}[5m]))"}}}
            ]
        },
        {
            "name": "Workloads",
            "icon": "layers",
            "widgets": [
                {"type": "timeseries", "title": "Deployment Available vs Desired", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(k8s.deployment.available{k8s.namespace.name=~\"$namespace\"})", "legend_format": "available"}, {"promql": "sum(k8s.deployment.desired{k8s.namespace.name=~\"$namespace\"})", "legend_format": "desired"}]}}},
                {"type": "timeseries", "title": "StatefulSet Ready vs Desired", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(k8s.statefulset.ready_pods{k8s.namespace.name=~\"$namespace\"})", "legend_format": "ready"}, {"promql": "sum(k8s.statefulset.desired_pods{k8s.namespace.name=~\"$namespace\"})", "legend_format": "desired"}]}}},
                {"type": "timeseries", "title": "DaemonSet Ready vs Desired", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(k8s.daemonset.ready_nodes{k8s.namespace.name=~\"$namespace\"})", "legend_format": "ready"}, {"promql": "sum(k8s.daemonset.desired_scheduled_nodes{k8s.namespace.name=~\"$namespace\"})", "legend_format": "desired"}]}}},
                {"type": "timeseries", "title": "HPA Current vs Target Replicas", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(k8s.hpa.current_replicas{k8s.namespace.name=~\"$namespace\"})", "legend_format": "current"}, {"promql": "sum(k8s.hpa.desired_replicas{k8s.namespace.name=~\"$namespace\"})", "legend_format": "desired"}, {"promql": "sum(k8s.hpa.max_replicas{k8s.namespace.name=~\"$namespace\"})", "legend_format": "max"}]}}}
            ]
        },
        {
            "name": "Pods",
            "icon": "box",
            "widgets": [
                {"type": "timeseries", "title": "Pod CPU Usage by Namespace", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (k8s.namespace.name) (k8s.pod.cpu.usage{k8s.namespace.name=~\"$namespace\"})"}, "unit": "cores"}},
                {"type": "timeseries", "title": "Pod Memory Usage by Namespace", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (k8s.namespace.name) (k8s.pod.memory.usage{k8s.namespace.name=~\"$namespace\"})"}, "unit": "bytes"}},
                {"type": "timeseries", "title": "Pod Network I/O", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum by (k8s.pod.name) (rate(k8s.pod.network.io{k8s.namespace.name=~\"$namespace\", direction=\"receive\"}[5m]))", "legend_format": "{{k8s.pod.name}} rx"}, {"promql": "sum by (k8s.pod.name) (rate(k8s.pod.network.io{k8s.namespace.name=~\"$namespace\", direction=\"transmit\"}[5m]))", "legend_format": "{{k8s.pod.name}} tx"}]}, "unit": "Bps"}},
                {"type": "timeseries", "title": "Container Restarts by Pod", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (k8s.pod.name) (increase(k8s.container.restarts{k8s.namespace.name=~\"$namespace\"}[1h]))"}}},
                {"type": "timeseries", "title": "Container CPU Usage", "x": 0, "y": 8, "w": 6, "h": 4, "config": {"query": {"promql": "topk(10, k8s.pod.cpu.usage{k8s.namespace.name=~\"$namespace\"})"}, "unit": "cores"}},
                {"type": "timeseries", "title": "Container Memory Usage", "x": 6, "y": 8, "w": 6, "h": 4, "config": {"query": {"promql": "topk(10, k8s.pod.memory.usage{k8s.namespace.name=~\"$namespace\"})"}, "unit": "bytes"}}
            ]
        }
    ]
}'::jsonb
WHERE name = 'Kubernetes Cluster';
