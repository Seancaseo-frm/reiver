-- Fix Host Metrics and Kubernetes dashboard templates to use correct OTel metric names.
--
-- Host Metrics issues:
--   - system.cpu.utilization does not exist; derive from rate(system.cpu.time)
--   - system.filesystem.utilization does not exist; derive from system.filesystem.usage ratio
--   - host.name label does not exist; remove the variable filter
--   - system.paging.usage does not exist; replace with disk I/O weighted time
--
-- Kubernetes issues:
--   - k8s.node.cpu.utilization does not exist; use k8s.node.cpu.usage (in cores)
--   - k8s.statefulset.ready_replicas / desired_replicas do not exist; use ready_pods / desired_pods
--   - k8s.pod.cpu.utilization does not exist; use k8s.pod.cpu.usage (in cores)

UPDATE dashboard_templates
SET template_config = '{
    "variables": [],
    "tabs": [
        {
            "name": "Overview",
            "icon": "server",
            "widgets": [
                {"type": "stat", "title": "CPU Utilization", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "1 - sum(rate(system.cpu.time{state=\"idle\"}[5m])) / sum(rate(system.cpu.time[5m]))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.7, "color": "orange"}, {"value": 0.9, "color": "red"}]}},
                {"type": "stat", "title": "Memory Utilization", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(system.memory.usage{state=\"used\"}) / sum(system.memory.usage)", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.8, "color": "orange"}, {"value": 0.95, "color": "red"}]}},
                {"type": "stat", "title": "Disk Utilization", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "max(sum by (mountpoint, device) (system.filesystem.usage{state=\"used\"}) / (sum by (mountpoint, device) (system.filesystem.usage{state=\"used\"}) + sum by (mountpoint, device) (system.filesystem.usage{state=\"free\"})))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.8, "color": "orange"}, {"value": 0.9, "color": "red"}]}},
                {"type": "stat", "title": "Network Throughput", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(system.network.io[5m]))", "instant": true}, "unit": "Bps"}},
                {"type": "timeseries", "title": "CPU by State", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(system.cpu.time{state=\"user\"}[5m])) / sum(rate(system.cpu.time[5m]))", "legend_format": "user"}, {"promql": "sum(rate(system.cpu.time{state=\"system\"}[5m])) / sum(rate(system.cpu.time[5m]))", "legend_format": "system"}, {"promql": "sum(rate(system.cpu.time{state=\"wait\"}[5m])) / sum(rate(system.cpu.time[5m]))", "legend_format": "iowait"}, {"promql": "sum(rate(system.cpu.time{state=\"steal\"}[5m])) / sum(rate(system.cpu.time[5m]))", "legend_format": "steal"}, {"promql": "sum(rate(system.cpu.time{state=\"idle\"}[5m])) / sum(rate(system.cpu.time[5m]))", "legend_format": "idle"}]}, "unit": "percentunit", "stacking": "normal"}},
                {"type": "timeseries", "title": "Memory Breakdown", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "system.memory.usage{state=\"used\"}", "legend_format": "used"}, {"promql": "system.memory.usage{state=\"cached\"}", "legend_format": "cached"}, {"promql": "system.memory.usage{state=\"buffered\"}", "legend_format": "buffers"}, {"promql": "system.memory.usage{state=\"free\"}", "legend_format": "free"}]}, "unit": "bytes", "stacking": "normal"}},
                {"type": "timeseries", "title": "System Load Average", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "system.cpu.load_average.1m", "legend_format": "1m"}, {"promql": "system.cpu.load_average.5m", "legend_format": "5m"}, {"promql": "system.cpu.load_average.15m", "legend_format": "15m"}]}}},
                {"type": "timeseries", "title": "Disk I/O Wait", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.disk.io_time[5m])", "legend_format": "io_time"}, {"promql": "rate(system.disk.weighted_io_time[5m])", "legend_format": "weighted_io_time"}]}, "unit": "s"}}
            ]
        },
        {
            "name": "Storage",
            "icon": "hard-drive",
            "widgets": [
                {"type": "timeseries", "title": "Disk I/O Throughput", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.disk.io{direction=\"read\"}[5m])", "legend_format": "read"}, {"promql": "rate(system.disk.io{direction=\"write\"}[5m])", "legend_format": "write"}]}, "unit": "Bps"}},
                {"type": "timeseries", "title": "Disk IOPS", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.disk.operations{direction=\"read\"}[5m])", "legend_format": "read ops"}, {"promql": "rate(system.disk.operations{direction=\"write\"}[5m])", "legend_format": "write ops"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Disk Latency", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.disk.operation_time{direction=\"read\"}[5m]) / rate(system.disk.operations{direction=\"read\"}[5m])", "legend_format": "read latency"}, {"promql": "rate(system.disk.operation_time{direction=\"write\"}[5m]) / rate(system.disk.operations{direction=\"write\"}[5m])", "legend_format": "write latency"}]}, "unit": "s"}},
                {"type": "timeseries", "title": "Filesystem Usage by Mountpoint", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (mountpoint, device) (system.filesystem.usage{state=\"used\"}) / (sum by (mountpoint, device) (system.filesystem.usage{state=\"used\"}) + sum by (mountpoint, device) (system.filesystem.usage{state=\"free\"}))"}, "unit": "percentunit"}}
            ]
        },
        {
            "name": "Network",
            "icon": "wifi",
            "widgets": [
                {"type": "timeseries", "title": "Bandwidth", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.network.io{direction=\"receive\"}[5m])", "legend_format": "received"}, {"promql": "rate(system.network.io{direction=\"transmit\"}[5m])", "legend_format": "transmitted"}]}, "unit": "Bps"}},
                {"type": "timeseries", "title": "Packets", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.network.packets{direction=\"receive\"}[5m])", "legend_format": "rx packets"}, {"promql": "rate(system.network.packets{direction=\"transmit\"}[5m])", "legend_format": "tx packets"}]}, "unit": "pps"}},
                {"type": "timeseries", "title": "Errors", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.network.errors{direction=\"receive\"}[5m])", "legend_format": "rx errors"}, {"promql": "rate(system.network.errors{direction=\"transmit\"}[5m])", "legend_format": "tx errors"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Drops", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.network.dropped{direction=\"receive\"}[5m])", "legend_format": "rx drops"}, {"promql": "rate(system.network.dropped{direction=\"transmit\"}[5m])", "legend_format": "tx drops"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "TCP Connections by State", "x": 0, "y": 8, "w": 12, "h": 4, "config": {"query": {"promql": "system.network.connections"}}}
            ]
        }
    ]
}'::jsonb
WHERE name = 'Host Metrics';

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
WHERE name = 'Kubernetes';
