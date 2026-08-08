-- Replace all dashboard templates with improved, industry-standard versions.
-- Follows the same TRUNCATE pattern as migration 059.

TRUNCATE dashboard_templates;

-- ============================================================================
-- 1. Host Metrics (hostmetricsreceiver) — multi-tab
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Host Metrics',
    'Comprehensive host monitoring: CPU by state, memory breakdown, disk IOPS/latency, network errors, swap, and load averages via OTel hostmetricsreceiver',
    'infrastructure',
    true,
    1,
    ARRAY['host', 'infrastructure', 'cpu', 'memory', 'disk', 'network', 'otel'],
    '{
        "variables": [
            {"name": "instance", "label": "Host", "type": "query", "query": "label_values(system.cpu.utilization, host.name)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "server",
                "widgets": [
                    {"type": "stat", "title": "CPU Utilization", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "avg(system.cpu.utilization{host.name=~\"$instance\"})", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.7, "color": "orange"}, {"value": 0.9, "color": "red"}]}},
                    {"type": "stat", "title": "Memory Utilization", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "system.memory.usage{host.name=~\"$instance\", state=\"used\"} / (system.memory.usage{host.name=~\"$instance\", state=\"used\"} + system.memory.usage{host.name=~\"$instance\", state=\"free\"} + system.memory.usage{host.name=~\"$instance\", state=\"cached\"} + system.memory.usage{host.name=~\"$instance\", state=\"buffered\"})", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.8, "color": "orange"}, {"value": 0.95, "color": "red"}]}},
                    {"type": "stat", "title": "Disk Utilization", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "max(system.filesystem.utilization{host.name=~\"$instance\"})", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.8, "color": "orange"}, {"value": 0.9, "color": "red"}]}},
                    {"type": "stat", "title": "Network Throughput", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(system.network.io{host.name=~\"$instance\"}[5m]))", "instant": true}, "unit": "Bps"}},
                    {"type": "timeseries", "title": "CPU by State", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "avg(system.cpu.utilization{host.name=~\"$instance\", state=\"user\"})", "legend_format": "user"}, {"promql": "avg(system.cpu.utilization{host.name=~\"$instance\", state=\"system\"})", "legend_format": "system"}, {"promql": "avg(system.cpu.utilization{host.name=~\"$instance\", state=\"wait\"})", "legend_format": "iowait"}, {"promql": "avg(system.cpu.utilization{host.name=~\"$instance\", state=\"steal\"})", "legend_format": "steal"}, {"promql": "avg(system.cpu.utilization{host.name=~\"$instance\", state=\"idle\"})", "legend_format": "idle"}]}, "unit": "percentunit", "stacking": "normal"}},
                    {"type": "timeseries", "title": "Memory Breakdown", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "system.memory.usage{host.name=~\"$instance\", state=\"used\"}", "legend_format": "used"}, {"promql": "system.memory.usage{host.name=~\"$instance\", state=\"cached\"}", "legend_format": "cached"}, {"promql": "system.memory.usage{host.name=~\"$instance\", state=\"buffered\"}", "legend_format": "buffers"}, {"promql": "system.memory.usage{host.name=~\"$instance\", state=\"free\"}", "legend_format": "free"}]}, "unit": "bytes", "stacking": "normal"}},
                    {"type": "timeseries", "title": "System Load Average", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "system.cpu.load_average.1m{host.name=~\"$instance\"}", "legend_format": "1m"}, {"promql": "system.cpu.load_average.5m{host.name=~\"$instance\"}", "legend_format": "5m"}, {"promql": "system.cpu.load_average.15m{host.name=~\"$instance\"}", "legend_format": "15m"}]}}},
                    {"type": "timeseries", "title": "Swap Usage", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "system.paging.usage{host.name=~\"$instance\", state=\"used\"}", "legend_format": "used"}, {"promql": "system.paging.usage{host.name=~\"$instance\", state=\"free\"}", "legend_format": "free"}]}, "unit": "bytes"}}
                ]
            },
            {
                "name": "Storage",
                "icon": "hard-drive",
                "widgets": [
                    {"type": "timeseries", "title": "Disk I/O Throughput", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.disk.io{host.name=~\"$instance\", direction=\"read\"}[5m])", "legend_format": "read"}, {"promql": "rate(system.disk.io{host.name=~\"$instance\", direction=\"write\"}[5m])", "legend_format": "write"}]}, "unit": "Bps"}},
                    {"type": "timeseries", "title": "Disk IOPS", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.disk.operations{host.name=~\"$instance\", direction=\"read\"}[5m])", "legend_format": "read ops"}, {"promql": "rate(system.disk.operations{host.name=~\"$instance\", direction=\"write\"}[5m])", "legend_format": "write ops"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Disk Latency", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.disk.operation_time{host.name=~\"$instance\", direction=\"read\"}[5m]) / rate(system.disk.operations{host.name=~\"$instance\", direction=\"read\"}[5m])", "legend_format": "read latency"}, {"promql": "rate(system.disk.operation_time{host.name=~\"$instance\", direction=\"write\"}[5m]) / rate(system.disk.operations{host.name=~\"$instance\", direction=\"write\"}[5m])", "legend_format": "write latency"}]}, "unit": "s"}},
                    {"type": "timeseries", "title": "Filesystem Usage by Mountpoint", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "system.filesystem.utilization{host.name=~\"$instance\"}"}, "unit": "percentunit"}}
                ]
            },
            {
                "name": "Network",
                "icon": "wifi",
                "widgets": [
                    {"type": "timeseries", "title": "Bandwidth", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.network.io{host.name=~\"$instance\", direction=\"receive\"}[5m])", "legend_format": "received"}, {"promql": "rate(system.network.io{host.name=~\"$instance\", direction=\"transmit\"}[5m])", "legend_format": "transmitted"}]}, "unit": "Bps"}},
                    {"type": "timeseries", "title": "Packets", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.network.packets{host.name=~\"$instance\", direction=\"receive\"}[5m])", "legend_format": "rx packets"}, {"promql": "rate(system.network.packets{host.name=~\"$instance\", direction=\"transmit\"}[5m])", "legend_format": "tx packets"}]}, "unit": "pps"}},
                    {"type": "timeseries", "title": "Errors", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.network.errors{host.name=~\"$instance\", direction=\"receive\"}[5m])", "legend_format": "rx errors"}, {"promql": "rate(system.network.errors{host.name=~\"$instance\", direction=\"transmit\"}[5m])", "legend_format": "tx errors"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Drops", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(system.network.dropped{host.name=~\"$instance\", direction=\"receive\"}[5m])", "legend_format": "rx drops"}, {"promql": "rate(system.network.dropped{host.name=~\"$instance\", direction=\"transmit\"}[5m])", "legend_format": "tx drops"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "TCP Connections by State", "x": 0, "y": 8, "w": 12, "h": 4, "config": {"query": {"promql": "system.network.connections{host.name=~\"$instance\"}"}}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 2. Kubernetes Cluster (k8sclusterreceiver + kubeletstatsreceiver) — multi-tab
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Kubernetes Cluster',
    'Comprehensive K8s monitoring: cluster health, workload status, pod resources vs limits, container restarts, OOMKills, and storage via OTel k8sclusterreceiver',
    'infrastructure',
    true,
    2,
    ARRAY['kubernetes', 'k8s', 'cluster', 'infrastructure', 'otel'],
    '{
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
                    {"type": "timeseries", "title": "Node CPU Utilization", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "k8s.node.cpu.utilization{k8s.node.name=~\"$node\"}"}, "unit": "percentunit"}},
                    {"type": "timeseries", "title": "Node Memory Utilization", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "k8s.node.memory.usage{k8s.node.name=~\"$node\"}", "legend_format": "used"}, {"promql": "k8s.node.memory.available{k8s.node.name=~\"$node\"}", "legend_format": "available"}]}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "Container Restarts Rate", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (k8s.pod.name) (rate(k8s.container.restarts{k8s.namespace.name=~\"$namespace\"}[5m]))"}}}
                ]
            },
            {
                "name": "Workloads",
                "icon": "layers",
                "widgets": [
                    {"type": "timeseries", "title": "Deployment Available vs Desired", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(k8s.deployment.available{k8s.namespace.name=~\"$namespace\"})", "legend_format": "available"}, {"promql": "sum(k8s.deployment.desired{k8s.namespace.name=~\"$namespace\"})", "legend_format": "desired"}]}}},
                    {"type": "timeseries", "title": "StatefulSet Ready vs Desired", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(k8s.statefulset.ready_replicas{k8s.namespace.name=~\"$namespace\"})", "legend_format": "ready"}, {"promql": "sum(k8s.statefulset.desired_replicas{k8s.namespace.name=~\"$namespace\"})", "legend_format": "desired"}]}}},
                    {"type": "timeseries", "title": "DaemonSet Ready vs Desired", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(k8s.daemonset.ready_nodes{k8s.namespace.name=~\"$namespace\"})", "legend_format": "ready"}, {"promql": "sum(k8s.daemonset.desired_scheduled_nodes{k8s.namespace.name=~\"$namespace\"})", "legend_format": "desired"}]}}},
                    {"type": "timeseries", "title": "HPA Current vs Target Replicas", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(k8s.hpa.current_replicas{k8s.namespace.name=~\"$namespace\"})", "legend_format": "current"}, {"promql": "sum(k8s.hpa.desired_replicas{k8s.namespace.name=~\"$namespace\"})", "legend_format": "desired"}, {"promql": "sum(k8s.hpa.max_replicas{k8s.namespace.name=~\"$namespace\"})", "legend_format": "max"}]}}}
                ]
            },
            {
                "name": "Pods",
                "icon": "box",
                "widgets": [
                    {"type": "timeseries", "title": "Pod CPU Usage by Namespace", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (k8s.namespace.name) (k8s.pod.cpu.utilization{k8s.namespace.name=~\"$namespace\"})"}, "unit": "percentunit"}},
                    {"type": "timeseries", "title": "Pod Memory Usage by Namespace", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (k8s.namespace.name) (k8s.pod.memory.usage{k8s.namespace.name=~\"$namespace\"})"}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "Pod Network I/O", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum by (k8s.pod.name) (rate(k8s.pod.network.io{k8s.namespace.name=~\"$namespace\", direction=\"receive\"}[5m]))", "legend_format": "{{k8s.pod.name}} rx"}, {"promql": "sum by (k8s.pod.name) (rate(k8s.pod.network.io{k8s.namespace.name=~\"$namespace\", direction=\"transmit\"}[5m]))", "legend_format": "{{k8s.pod.name}} tx"}]}, "unit": "Bps"}},
                    {"type": "timeseries", "title": "Container Restarts by Pod", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (k8s.pod.name) (increase(k8s.container.restarts{k8s.namespace.name=~\"$namespace\"}[1h]))"}}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 3. PostgreSQL (postgresqlreceiver) — multi-tab
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'PostgreSQL',
    'Complete PostgreSQL monitoring: cache hit ratio, connections, transactions, replication lag, vacuum stats, WAL size, and lock analysis via OTel postgresqlreceiver',
    'database',
    true,
    3,
    ARRAY['postgresql', 'postgres', 'database', 'otel'],
    '{
        "variables": [
            {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(postgresql.backends, server.address)"},
            {"name": "database", "label": "Database", "type": "query", "query": "label_values(postgresql.backends, postgresql.db.name)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "database",
                "widgets": [
                    {"type": "stat", "title": "Active Connections", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(postgresql.backends{server.address=~\"$instance\"})", "instant": true}}},
                    {"type": "stat", "title": "Cache Hit Ratio", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m])) / (sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m])) + sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap\"}[5m])))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "red"}, {"value": 0.9, "color": "orange"}, {"value": 0.99, "color": "green"}]}},
                    {"type": "stat", "title": "Commits/s", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(postgresql.commits{server.address=~\"$instance\"}[5m]))", "instant": true}, "unit": "ops"}},
                    {"type": "stat", "title": "Database Size", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(postgresql.db_size{server.address=~\"$instance\"})", "instant": true}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "Connections Over Time", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (postgresql.db.name) (postgresql.backends{server.address=~\"$instance\"})"}}},
                    {"type": "timeseries", "title": "Transaction Rate", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(postgresql.commits{server.address=~\"$instance\"}[5m]))", "legend_format": "commits"}, {"promql": "sum(rate(postgresql.rollbacks{server.address=~\"$instance\"}[5m]))", "legend_format": "rollbacks"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Cache Hit Ratio Over Time", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m])) / (sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m])) + sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap\"}[5m])))"}, "unit": "percentunit"}},
                    {"type": "timeseries", "title": "Database Size Growth", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (postgresql.db.name) (postgresql.db_size{server.address=~\"$instance\"})"}, "unit": "bytes"}}
                ]
            },
            {
                "name": "Performance",
                "icon": "zap",
                "widgets": [
                    {"type": "timeseries", "title": "Rows Operated", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(postgresql.rows{server.address=~\"$instance\", state=\"inserted\"}[5m]))", "legend_format": "inserted"}, {"promql": "sum(rate(postgresql.rows{server.address=~\"$instance\", state=\"updated\"}[5m]))", "legend_format": "updated"}, {"promql": "sum(rate(postgresql.rows{server.address=~\"$instance\", state=\"deleted\"}[5m]))", "legend_format": "deleted"}, {"promql": "sum(rate(postgresql.rows{server.address=~\"$instance\", state=\"fetched\"}[5m]))", "legend_format": "fetched"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Scans: Sequential vs Index", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(postgresql.sequential_scans{server.address=~\"$instance\"}[5m]))", "legend_format": "sequential"}, {"promql": "sum(rate(postgresql.index_scans{server.address=~\"$instance\"}[5m]))", "legend_format": "index"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Block I/O (Heap Read vs Cache Hit)", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap\"}[5m]))", "legend_format": "disk reads"}, {"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m]))", "legend_format": "cache hits"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Temp Bytes Written", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(postgresql.temp_bytes{server.address=~\"$instance\"}[5m]))"}, "unit": "Bps"}}
                ]
            },
            {
                "name": "Health",
                "icon": "heart",
                "widgets": [
                    {"type": "timeseries", "title": "Replication Lag", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "postgresql.replication.data_delay{server.address=~\"$instance\"}"}, "unit": "s"}},
                    {"type": "timeseries", "title": "Dead Tuples (Vacuum Needed)", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (postgresql.db.name) (postgresql.dead_tuples{server.address=~\"$instance\"})"}}},
                    {"type": "timeseries", "title": "Locks by Mode", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (mode) (postgresql.locks{server.address=~\"$instance\"})"}}},
                    {"type": "timeseries", "title": "WAL Size", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "postgresql.wal.age{server.address=~\"$instance\"}"}, "unit": "bytes"}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 4. Redis (redisreceiver) — multi-tab
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Redis',
    'Complete Redis monitoring: cache hit rate, command latency, memory fragmentation, persistence status, replication, and key evictions via OTel redisreceiver',
    'database',
    true,
    4,
    ARRAY['redis', 'cache', 'database', 'otel'],
    '{
        "variables": [
            {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(redis.uptime, server.address)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "database",
                "widgets": [
                    {"type": "stat", "title": "Connected Clients", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "redis.clients.connected{server.address=~\"$instance\"}", "instant": true}}},
                    {"type": "stat", "title": "Memory Used", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "redis.memory.used{server.address=~\"$instance\"}", "instant": true}, "unit": "bytes"}},
                    {"type": "stat", "title": "Cache Hit Rate", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "rate(redis.keyspace.hits{server.address=~\"$instance\"}[5m]) / (rate(redis.keyspace.hits{server.address=~\"$instance\"}[5m]) + rate(redis.keyspace.misses{server.address=~\"$instance\"}[5m]))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "red"}, {"value": 0.8, "color": "orange"}, {"value": 0.95, "color": "green"}]}},
                    {"type": "stat", "title": "Commands/s", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "rate(redis.commands.processed{server.address=~\"$instance\"}[5m])", "instant": true}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Commands/s Over Time", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "rate(redis.commands.processed{server.address=~\"$instance\"}[5m])"}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Hit / Miss Rate", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(redis.keyspace.hits{server.address=~\"$instance\"}[5m])", "legend_format": "hits"}, {"promql": "rate(redis.keyspace.misses{server.address=~\"$instance\"}[5m])", "legend_format": "misses"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Connected Clients", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "redis.clients.connected{server.address=~\"$instance\"}", "legend_format": "connected"}, {"promql": "redis.clients.blocked{server.address=~\"$instance\"}", "legend_format": "blocked"}]}}},
                    {"type": "timeseries", "title": "Memory Usage", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "redis.memory.used{server.address=~\"$instance\"}", "legend_format": "used"}, {"promql": "redis.memory.peak{server.address=~\"$instance\"}", "legend_format": "peak"}, {"promql": "redis.memory.rss{server.address=~\"$instance\"}", "legend_format": "rss"}]}, "unit": "bytes"}}
                ]
            },
            {
                "name": "Performance",
                "icon": "zap",
                "widgets": [
                    {"type": "timeseries", "title": "Command Latency", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "rate(redis.commands.duration{server.address=~\"$instance\"}[5m]) / rate(redis.commands.calls{server.address=~\"$instance\"}[5m])"}, "unit": "us"}},
                    {"type": "timeseries", "title": "Key Evictions & Expirations", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(redis.keys.evicted{server.address=~\"$instance\"}[5m])", "legend_format": "evicted"}, {"promql": "rate(redis.keys.expired{server.address=~\"$instance\"}[5m])", "legend_format": "expired"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Network I/O", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(redis.net.input{server.address=~\"$instance\"}[5m])", "legend_format": "input"}, {"promql": "rate(redis.net.output{server.address=~\"$instance\"}[5m])", "legend_format": "output"}]}, "unit": "Bps"}},
                    {"type": "timeseries", "title": "Slow Log Count", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "redis.slowlog.count{server.address=~\"$instance\"}"}}}
                ]
            },
            {
                "name": "Reliability",
                "icon": "shield",
                "widgets": [
                    {"type": "stat", "title": "Connected Replicas", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "redis.slaves.connected{server.address=~\"$instance\"}", "instant": true}}},
                    {"type": "stat", "title": "Memory Fragmentation", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "redis.memory.fragmentation_ratio{server.address=~\"$instance\"}", "instant": true}, "thresholds": [{"value": 1, "color": "green"}, {"value": 1.5, "color": "orange"}, {"value": 2, "color": "red"}]}},
                    {"type": "stat", "title": "Uptime", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "redis.uptime{server.address=~\"$instance\"}", "instant": true}, "unit": "s"}},
                    {"type": "stat", "title": "Rejected Connections", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "rate(redis.clients.rejected{server.address=~\"$instance\"}[5m])", "instant": true}, "unit": "ops", "thresholds": [{"value": 0, "color": "green"}, {"value": 1, "color": "red"}]}},
                    {"type": "timeseries", "title": "Replication Offset Lag", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "redis.replication.offset_diff{server.address=~\"$instance\"}"}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "RDB Last Save Time", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "redis.rdb.changes_since_last_save{server.address=~\"$instance\"}"}}},
                    {"type": "timeseries", "title": "CPU Usage", "x": 0, "y": 6, "w": 12, "h": 4, "config": {"query": {"queries": [{"promql": "rate(redis.cpu.time{server.address=~\"$instance\", state=\"user\"}[5m])", "legend_format": "user"}, {"promql": "rate(redis.cpu.time{server.address=~\"$instance\", state=\"sys\"}[5m])", "legend_format": "system"}]}, "unit": "s"}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 5. HTTP Service (OTel HTTP semantic conventions) — multi-tab RED method
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'HTTP Service',
    'Full RED method monitoring: request rate, error rate, latency percentiles, slowest routes, availability SLI, and throughput using OTel HTTP semantic conventions',
    'apm',
    true,
    5,
    ARRAY['http', 'service', 'apm', 'latency', 'red', 'otel'],
    '{
        "variables": [
            {"name": "service", "label": "Service", "type": "service_select", "default": ""},
            {"name": "http_route", "label": "Route", "type": "query", "query": "label_values(http.server.request.duration.count, http.route)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "globe",
                "widgets": [
                    {"type": "stat", "title": "Request Rate", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))", "instant": true}, "unit": "reqps"}},
                    {"type": "stat", "title": "Error Rate", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "(sum(rate(http.server.request.duration.count{http.response.status_code=~\"5..\", http.route=~\"$http_route\"}[5m])) or vector(0)) / sum(rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.01, "color": "orange"}, {"value": 0.05, "color": "red"}]}},
                    {"type": "stat", "title": "P95 Latency", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "histogram_quantile(0.95, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "instant": true}, "unit": "s"}},
                    {"type": "stat", "title": "P50 Latency", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "histogram_quantile(0.5, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "instant": true}, "unit": "s"}},
                    {"type": "timeseries", "title": "Request Rate by Status Code", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (http.response.status_code) (rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))"}, "unit": "reqps"}},
                    {"type": "timeseries", "title": "Latency Percentiles", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "histogram_quantile(0.5, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "legend_format": "p50"}, {"promql": "histogram_quantile(0.9, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "legend_format": "p90"}, {"promql": "histogram_quantile(0.95, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "legend_format": "p95"}, {"promql": "histogram_quantile(0.99, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "legend_format": "p99"}]}, "unit": "s"}},
                    {"type": "timeseries", "title": "Error Rate Over Time", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "(sum(rate(http.server.request.duration.count{http.response.status_code=~\"5..\", http.route=~\"$http_route\"}[5m])) or vector(0)) / sum(rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))"}, "unit": "percentunit"}},
                    {"type": "timeseries", "title": "Request Rate by Service", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (service.name) (rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))"}, "unit": "reqps"}}
                ]
            },
            {
                "name": "Routes",
                "icon": "map",
                "widgets": [
                    {"type": "top_list", "title": "Slowest Routes (P95)", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "histogram_quantile(0.95, sum by (http.route, le) (rate(http.server.request.duration_bucket[5m])))", "instant": true}, "unit": "s"}},
                    {"type": "top_list", "title": "Highest Error Rate Routes", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "(sum by (http.route) (rate(http.server.request.duration.count{http.response.status_code=~\"5..\"}[5m])) or vector(0)) / sum by (http.route) (rate(http.server.request.duration.count[5m]))", "instant": true}, "unit": "percentunit"}},
                    {"type": "timeseries", "title": "Request Rate by Route", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (http.route) (rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))"}, "unit": "reqps"}},
                    {"type": "timeseries", "title": "Latency by Route (P95)", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "histogram_quantile(0.95, sum by (http.route, le) (rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])))"}, "unit": "s"}}
                ]
            },
            {
                "name": "Throughput",
                "icon": "bar-chart",
                "widgets": [
                    {"type": "timeseries", "title": "Request Body Size (avg)", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "rate(http.server.request.body.size.sum{http.route=~\"$http_route\"}[5m]) / rate(http.server.request.body.size.count{http.route=~\"$http_route\"}[5m])"}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "Response Body Size (avg)", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "rate(http.server.response.body.size.sum{http.route=~\"$http_route\"}[5m]) / rate(http.server.response.body.size.count{http.route=~\"$http_route\"}[5m])"}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "Availability SLI", "x": 0, "y": 4, "w": 12, "h": 4, "config": {"query": {"promql": "1 - (sum(rate(http.server.request.duration.count{http.response.status_code=~\"5..\", http.route=~\"$http_route\"}[5m])) or vector(0)) / sum(rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))"}, "unit": "percentunit"}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 6. GenAI / LLM (OTel GenAI semantic conventions) — NEW
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'GenAI / LLM',
    'LLM application monitoring: token usage, operation latency, error rates, cost tracking, and model comparison using OTel GenAI semantic conventions (gen_ai.*)',
    'ai',
    true,
    6,
    ARRAY['genai', 'llm', 'ai', 'tokens', 'openai', 'anthropic', 'otel'],
    '{
        "variables": [
            {"name": "model", "label": "Model", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.request.model)"},
            {"name": "provider", "label": "Provider", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.provider.name)"},
            {"name": "operation", "label": "Operation", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.operation.name)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "cpu",
                "widgets": [
                    {"type": "stat", "title": "Request Rate", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[5m]))", "instant": true}, "unit": "reqps"}},
                    {"type": "stat", "title": "Total Tokens/s", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(gen_ai.client.token.usage.sum{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[5m]))", "instant": true}, "unit": "ops"}},
                    {"type": "stat", "title": "Avg Latency", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(gen_ai.client.operation.duration.sum{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[5m])) / sum(rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[5m]))", "instant": true}, "unit": "s"}},
                    {"type": "stat", "title": "Error Rate", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "(sum(rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\", error.type!=\"\"}[5m])) or vector(0)) / sum(rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[5m]))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.01, "color": "orange"}, {"value": 0.05, "color": "red"}]}},
                    {"type": "timeseries", "title": "Token Usage (Input vs Output)", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(gen_ai.client.token.usage.sum{gen_ai.request.model=~\"$model\", gen_ai.token.type=\"input\"}[5m]))", "legend_format": "input tokens"}, {"promql": "sum(rate(gen_ai.client.token.usage.sum{gen_ai.request.model=~\"$model\", gen_ai.token.type=\"output\"}[5m]))", "legend_format": "output tokens"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Operation Duration Percentiles", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "histogram_quantile(0.5, sum(rate(gen_ai.client.operation.duration_bucket{gen_ai.request.model=~\"$model\"}[5m])) by (le))", "legend_format": "p50"}, {"promql": "histogram_quantile(0.95, sum(rate(gen_ai.client.operation.duration_bucket{gen_ai.request.model=~\"$model\"}[5m])) by (le))", "legend_format": "p95"}, {"promql": "histogram_quantile(0.99, sum(rate(gen_ai.client.operation.duration_bucket{gen_ai.request.model=~\"$model\"}[5m])) by (le))", "legend_format": "p99"}]}, "unit": "s"}},
                    {"type": "timeseries", "title": "Requests by Model", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{gen_ai.provider.name=~\"$provider\"}[5m]))"}, "unit": "reqps"}},
                    {"type": "timeseries", "title": "Error Rate by Model", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{error.type!=\"\", gen_ai.provider.name=~\"$provider\"}[5m])) / sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{gen_ai.provider.name=~\"$provider\"}[5m]))"}, "unit": "percentunit"}}
                ]
            },
            {
                "name": "Cost & Usage",
                "icon": "dollar-sign",
                "widgets": [
                    {"type": "timeseries", "title": "Token Consumption by Model", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.token.usage.sum{gen_ai.provider.name=~\"$provider\"}[5m]))"}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Requests by Operation Type", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.operation.name) (rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\"}[5m]))"}, "unit": "reqps"}},
                    {"type": "timeseries", "title": "Token Efficiency (Output/Input Ratio)", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(gen_ai.client.token.usage.sum{gen_ai.token.type=\"output\", gen_ai.request.model=~\"$model\"}[5m])) / sum(rate(gen_ai.client.token.usage.sum{gen_ai.token.type=\"input\", gen_ai.request.model=~\"$model\"}[5m]))"}}},
                    {"type": "timeseries", "title": "Avg Tokens per Request by Model", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.token.usage.sum{gen_ai.provider.name=~\"$provider\"}[5m])) / sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{gen_ai.provider.name=~\"$provider\"}[5m]))"}}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 7. Go Runtime (process.runtime.go.*) — NEW
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Go Runtime',
    'Go application runtime metrics: goroutines, heap allocation, GC pauses, CPU time, file descriptors, and threads via OTel Go runtime instrumentation',
    'runtime',
    true,
    7,
    ARRAY['go', 'golang', 'runtime', 'gc', 'goroutines', 'otel'],
    '{
        "variables": [
            {"name": "service", "label": "Service", "type": "query", "query": "label_values(runtime.go.goroutines, service.name)"},
            {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(runtime.go.goroutines, service.instance.id)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "activity",
                "widgets": [
                    {"type": "stat", "title": "Goroutines", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(runtime.go.goroutines{service.name=~\"$service\", service.instance.id=~\"$instance\"})", "instant": true}}},
                    {"type": "stat", "title": "Heap Alloc", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(runtime.go.mem.heap_alloc{service.name=~\"$service\", service.instance.id=~\"$instance\"})", "instant": true}, "unit": "bytes"}},
                    {"type": "stat", "title": "GC Pause (avg)", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "rate(runtime.go.gc.pause_ns.sum{service.name=~\"$service\"}[5m]) / rate(runtime.go.gc.pause_ns.count{service.name=~\"$service\"}[5m])", "instant": true}, "unit": "ns"}},
                    {"type": "stat", "title": "Threads", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(runtime.go.threads{service.name=~\"$service\", service.instance.id=~\"$instance\"})", "instant": true}}},
                    {"type": "timeseries", "title": "Goroutines Over Time", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "runtime.go.goroutines{service.name=~\"$service\", service.instance.id=~\"$instance\"}"}}},
                    {"type": "timeseries", "title": "Heap Memory", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "runtime.go.mem.heap_alloc{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "alloc"}, {"promql": "runtime.go.mem.heap_sys{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "sys"}, {"promql": "runtime.go.mem.heap_idle{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "idle"}]}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "GC Pause Duration", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "rate(runtime.go.gc.pause_ns.sum{service.name=~\"$service\", service.instance.id=~\"$instance\"}[5m]) / rate(runtime.go.gc.pause_ns.count{service.name=~\"$service\", service.instance.id=~\"$instance\"}[5m])"}, "unit": "ns"}},
                    {"type": "timeseries", "title": "GC Frequency", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "rate(runtime.go.gc.count{service.name=~\"$service\", service.instance.id=~\"$instance\"}[5m])"}, "unit": "ops"}},
                    {"type": "timeseries", "title": "CPU Time", "x": 0, "y": 10, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(process.cpu.time{service.name=~\"$service\", state=\"user\"}[5m])", "legend_format": "user"}, {"promql": "rate(process.cpu.time{service.name=~\"$service\", state=\"system\"}[5m])", "legend_format": "system"}]}, "unit": "s"}},
                    {"type": "timeseries", "title": "Open File Descriptors", "x": 6, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "process.open_file_descriptors{service.name=~\"$service\", service.instance.id=~\"$instance\"}"}}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 8. NGINX (nginxreceiver) — NEW
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'NGINX',
    'NGINX web server monitoring: active connections, request rate, connection states, and dropped connections via OTel nginxreceiver',
    'web',
    true,
    8,
    ARRAY['nginx', 'web', 'reverse-proxy', 'load-balancer', 'otel'],
    '{
        "variables": [
            {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(nginx.connections_current, server.address)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "globe",
                "widgets": [
                    {"type": "stat", "title": "Active Connections", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(nginx.connections_current{server.address=~\"$instance\", state=\"active\"})", "instant": true}}},
                    {"type": "stat", "title": "Requests/s", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(nginx.requests{server.address=~\"$instance\"}[5m]))", "instant": true}, "unit": "reqps"}},
                    {"type": "stat", "title": "Accepted/s", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(nginx.connections_accepted{server.address=~\"$instance\"}[5m]))", "instant": true}, "unit": "ops"}},
                    {"type": "stat", "title": "Dropped Connections", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(nginx.connections_accepted{server.address=~\"$instance\"}[5m])) - sum(rate(nginx.connections_handled{server.address=~\"$instance\"}[5m]))", "instant": true}, "unit": "ops", "thresholds": [{"value": 0, "color": "green"}, {"value": 1, "color": "red"}]}},
                    {"type": "timeseries", "title": "Connections by State", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "nginx.connections_current{server.address=~\"$instance\", state=\"active\"}", "legend_format": "active"}, {"promql": "nginx.connections_current{server.address=~\"$instance\", state=\"reading\"}", "legend_format": "reading"}, {"promql": "nginx.connections_current{server.address=~\"$instance\", state=\"writing\"}", "legend_format": "writing"}, {"promql": "nginx.connections_current{server.address=~\"$instance\", state=\"waiting\"}", "legend_format": "waiting"}]}}},
                    {"type": "timeseries", "title": "Request Rate", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(nginx.requests{server.address=~\"$instance\"}[5m]))"}, "unit": "reqps"}},
                    {"type": "timeseries", "title": "Connections Accepted vs Handled", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(nginx.connections_accepted{server.address=~\"$instance\"}[5m]))", "legend_format": "accepted"}, {"promql": "sum(rate(nginx.connections_handled{server.address=~\"$instance\"}[5m]))", "legend_format": "handled"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Dropped Connections Over Time", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(nginx.connections_accepted{server.address=~\"$instance\"}[5m])) - sum(rate(nginx.connections_handled{server.address=~\"$instance\"}[5m]))"}, "unit": "ops"}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 9. MySQL (mysqlreceiver) — NEW
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'MySQL',
    'MySQL database monitoring: connections, queries, buffer pool hit rate, InnoDB row operations, replication lag, and table locks via OTel mysqlreceiver',
    'database',
    true,
    9,
    ARRAY['mysql', 'mariadb', 'database', 'otel'],
    '{
        "variables": [
            {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(mysql.connections, server.address)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "database",
                "widgets": [
                    {"type": "stat", "title": "Connections", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "mysql.connections{server.address=~\"$instance\"}", "instant": true}}},
                    {"type": "stat", "title": "Queries/s", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "rate(mysql.queries{server.address=~\"$instance\"}[5m])", "instant": true}, "unit": "ops"}},
                    {"type": "stat", "title": "Slow Queries/s", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "rate(mysql.slow_queries{server.address=~\"$instance\"}[5m])", "instant": true}, "unit": "ops", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.1, "color": "orange"}, {"value": 1, "color": "red"}]}},
                    {"type": "stat", "title": "Buffer Pool Hit Rate", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "1 - (rate(mysql.buffer_pool.disk_reads{server.address=~\"$instance\"}[5m]) / rate(mysql.buffer_pool.read_requests{server.address=~\"$instance\"}[5m]))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "red"}, {"value": 0.9, "color": "orange"}, {"value": 0.99, "color": "green"}]}},
                    {"type": "timeseries", "title": "Connections", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "mysql.connections{server.address=~\"$instance\"}", "legend_format": "current"}, {"promql": "mysql.connections.max{server.address=~\"$instance\"}", "legend_format": "max"}]}}},
                    {"type": "timeseries", "title": "Queries/s", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "rate(mysql.queries{server.address=~\"$instance\"}[5m])"}, "unit": "ops"}},
                    {"type": "timeseries", "title": "InnoDB Row Operations", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(mysql.row_operations{server.address=~\"$instance\", operation=\"read\"}[5m])", "legend_format": "reads"}, {"promql": "rate(mysql.row_operations{server.address=~\"$instance\", operation=\"insert\"}[5m])", "legend_format": "inserts"}, {"promql": "rate(mysql.row_operations{server.address=~\"$instance\", operation=\"update\"}[5m])", "legend_format": "updates"}, {"promql": "rate(mysql.row_operations{server.address=~\"$instance\", operation=\"delete\"}[5m])", "legend_format": "deletes"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Buffer Pool Utilization", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "mysql.buffer_pool.usage{server.address=~\"$instance\"}", "legend_format": "used"}, {"promql": "mysql.buffer_pool.limit{server.address=~\"$instance\"}", "legend_format": "limit"}]}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "Replication Lag", "x": 0, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "mysql.replica.time_behind_source{server.address=~\"$instance\"}"}, "unit": "s"}},
                    {"type": "timeseries", "title": "Table Locks", "x": 6, "y": 10, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(mysql.locks{server.address=~\"$instance\", kind=\"waited\"}[5m])", "legend_format": "waited"}, {"promql": "rate(mysql.locks{server.address=~\"$instance\", kind=\"immediate\"}[5m])", "legend_format": "immediate"}]}, "unit": "ops"}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 10. MongoDB (mongodbreceiver) — NEW
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'MongoDB',
    'MongoDB monitoring: operations by type, connections, memory usage, page faults, replication lag, and global lock metrics via OTel mongodbreceiver',
    'database',
    true,
    10,
    ARRAY['mongodb', 'mongo', 'nosql', 'database', 'otel'],
    '{
        "variables": [
            {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(mongodb.connection.count, server.address)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "database",
                "widgets": [
                    {"type": "stat", "title": "Current Connections", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(mongodb.connection.count{server.address=~\"$instance\", state=\"current\"})", "instant": true}}},
                    {"type": "stat", "title": "Operations/s", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(mongodb.operation.count{server.address=~\"$instance\"}[5m]))", "instant": true}, "unit": "ops"}},
                    {"type": "stat", "title": "Resident Memory", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "mongodb.memory.usage{server.address=~\"$instance\", type=\"resident\"}", "instant": true}, "unit": "bytes"}},
                    {"type": "stat", "title": "Page Faults/s", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "rate(mongodb.global_lock.time{server.address=~\"$instance\"}[5m])", "instant": true}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Operations by Type", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(mongodb.operation.count{server.address=~\"$instance\", operation=\"insert\"}[5m])", "legend_format": "insert"}, {"promql": "rate(mongodb.operation.count{server.address=~\"$instance\", operation=\"query\"}[5m])", "legend_format": "query"}, {"promql": "rate(mongodb.operation.count{server.address=~\"$instance\", operation=\"update\"}[5m])", "legend_format": "update"}, {"promql": "rate(mongodb.operation.count{server.address=~\"$instance\", operation=\"delete\"}[5m])", "legend_format": "delete"}, {"promql": "rate(mongodb.operation.count{server.address=~\"$instance\", operation=\"command\"}[5m])", "legend_format": "command"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Connections", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "mongodb.connection.count{server.address=~\"$instance\", state=\"current\"}", "legend_format": "current"}, {"promql": "mongodb.connection.count{server.address=~\"$instance\", state=\"available\"}", "legend_format": "available"}]}}},
                    {"type": "timeseries", "title": "Memory Usage", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "mongodb.memory.usage{server.address=~\"$instance\", type=\"resident\"}", "legend_format": "resident"}, {"promql": "mongodb.memory.usage{server.address=~\"$instance\", type=\"virtual\"}", "legend_format": "virtual"}]}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "Document Operations", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(mongodb.document.operation.count{server.address=~\"$instance\", operation=\"inserted\"}[5m])", "legend_format": "inserted"}, {"promql": "rate(mongodb.document.operation.count{server.address=~\"$instance\", operation=\"updated\"}[5m])", "legend_format": "updated"}, {"promql": "rate(mongodb.document.operation.count{server.address=~\"$instance\", operation=\"deleted\"}[5m])", "legend_format": "deleted"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Replication Lag", "x": 0, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "mongodb.replication.lag{server.address=~\"$instance\"}"}, "unit": "s"}},
                    {"type": "timeseries", "title": "Global Lock Queue", "x": 6, "y": 10, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "mongodb.global_lock.active_clients{server.address=~\"$instance\", type=\"readers\"}", "legend_format": "readers"}, {"promql": "mongodb.global_lock.active_clients{server.address=~\"$instance\", type=\"writers\"}", "legend_format": "writers"}]}}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 11. Kafka (kafkametricsreceiver) — NEW
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Kafka',
    'Apache Kafka monitoring: broker count, partition health, consumer group lag, message throughput, under-replicated partitions via OTel kafkametricsreceiver',
    'messaging',
    true,
    11,
    ARRAY['kafka', 'messaging', 'streaming', 'queue', 'otel'],
    '{
        "variables": [
            {"name": "topic", "label": "Topic", "type": "query", "query": "label_values(kafka.topic.partitions, topic)"},
            {"name": "consumer_group", "label": "Consumer Group", "type": "query", "query": "label_values(kafka.consumer_group.lag, group)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "send",
                "widgets": [
                    {"type": "stat", "title": "Active Brokers", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "kafka.brokers", "instant": true}}},
                    {"type": "stat", "title": "Total Partitions", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(kafka.topic.partitions{topic=~\"$topic\"})", "instant": true}}},
                    {"type": "stat", "title": "Total Consumer Lag", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(kafka.consumer_group.lag{group=~\"$consumer_group\"})", "instant": true}, "thresholds": [{"value": 0, "color": "green"}, {"value": 1000, "color": "orange"}, {"value": 10000, "color": "red"}]}},
                    {"type": "stat", "title": "Messages In/s", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(kafka.topic.messages_in{topic=~\"$topic\"}[5m]))", "instant": true}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Messages In/s by Topic", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (topic) (rate(kafka.topic.messages_in{topic=~\"$topic\"}[5m]))"}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Consumer Group Lag", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (group) (kafka.consumer_group.lag{group=~\"$consumer_group\"})"}}},
                    {"type": "timeseries", "title": "Bytes In/Out per Second", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(kafka.topic.bytes_in{topic=~\"$topic\"}[5m]))", "legend_format": "bytes in"}, {"promql": "sum(rate(kafka.topic.bytes_out{topic=~\"$topic\"}[5m]))", "legend_format": "bytes out"}]}, "unit": "Bps"}},
                    {"type": "timeseries", "title": "Under-Replicated Partitions", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum(kafka.partition.under_replicated{topic=~\"$topic\"})"}, "thresholds": [{"value": 0, "color": "green"}, {"value": 1, "color": "red"}]}},
                    {"type": "timeseries", "title": "Consumer Group Offset", "x": 0, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (group) (rate(kafka.consumer_group.offset{group=~\"$consumer_group\"}[5m]))"}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Partition Count by Topic", "x": 6, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "kafka.topic.partitions{topic=~\"$topic\"}"}}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 12. gRPC Service (OTel RPC semantic conventions) — NEW
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'gRPC Service',
    'gRPC service monitoring using RED method: RPC rate, error rate by status code, latency percentiles, and message sizes via OTel RPC semantic conventions',
    'apm',
    true,
    12,
    ARRAY['grpc', 'rpc', 'service', 'apm', 'otel'],
    '{
        "variables": [
            {"name": "service", "label": "Service", "type": "query", "query": "label_values(rpc.server.duration.count, service.name)"},
            {"name": "method", "label": "Method", "type": "query", "query": "label_values(rpc.server.duration.count, rpc.method)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "zap",
                "widgets": [
                    {"type": "stat", "title": "RPC Rate", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(rpc.server.duration.count{service.name=~\"$service\", rpc.method=~\"$method\"}[5m]))", "instant": true}, "unit": "reqps"}},
                    {"type": "stat", "title": "Error Rate", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "(sum(rate(rpc.server.duration.count{service.name=~\"$service\", rpc.grpc.status_code!=\"0\", rpc.method=~\"$method\"}[5m])) or vector(0)) / sum(rate(rpc.server.duration.count{service.name=~\"$service\", rpc.method=~\"$method\"}[5m]))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.01, "color": "orange"}, {"value": 0.05, "color": "red"}]}},
                    {"type": "stat", "title": "P95 Duration", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "histogram_quantile(0.95, sum(rate(rpc.server.duration_bucket{service.name=~\"$service\", rpc.method=~\"$method\"}[5m])) by (le))", "instant": true}, "unit": "ms"}},
                    {"type": "stat", "title": "P50 Duration", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "histogram_quantile(0.5, sum(rate(rpc.server.duration_bucket{service.name=~\"$service\", rpc.method=~\"$method\"}[5m])) by (le))", "instant": true}, "unit": "ms"}},
                    {"type": "timeseries", "title": "RPC Rate by Method", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (rpc.method) (rate(rpc.server.duration.count{service.name=~\"$service\", rpc.method=~\"$method\"}[5m]))"}, "unit": "reqps"}},
                    {"type": "timeseries", "title": "Duration Percentiles", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "histogram_quantile(0.5, sum(rate(rpc.server.duration_bucket{service.name=~\"$service\", rpc.method=~\"$method\"}[5m])) by (le))", "legend_format": "p50"}, {"promql": "histogram_quantile(0.9, sum(rate(rpc.server.duration_bucket{service.name=~\"$service\", rpc.method=~\"$method\"}[5m])) by (le))", "legend_format": "p90"}, {"promql": "histogram_quantile(0.95, sum(rate(rpc.server.duration_bucket{service.name=~\"$service\", rpc.method=~\"$method\"}[5m])) by (le))", "legend_format": "p95"}, {"promql": "histogram_quantile(0.99, sum(rate(rpc.server.duration_bucket{service.name=~\"$service\", rpc.method=~\"$method\"}[5m])) by (le))", "legend_format": "p99"}]}, "unit": "ms"}},
                    {"type": "timeseries", "title": "Error Rate by Status Code", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (rpc.grpc.status_code) (rate(rpc.server.duration.count{service.name=~\"$service\", rpc.grpc.status_code!=\"0\", rpc.method=~\"$method\"}[5m]))"}, "unit": "reqps"}},
                    {"type": "timeseries", "title": "Message Size", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(rpc.server.request.size.sum{service.name=~\"$service\", rpc.method=~\"$method\"}[5m]) / rate(rpc.server.request.size.count{service.name=~\"$service\", rpc.method=~\"$method\"}[5m])", "legend_format": "request avg"}, {"promql": "rate(rpc.server.response.size.sum{service.name=~\"$service\", rpc.method=~\"$method\"}[5m]) / rate(rpc.server.response.size.count{service.name=~\"$service\", rpc.method=~\"$method\"}[5m])", "legend_format": "response avg"}]}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "RPC Rate by Service", "x": 0, "y": 10, "w": 12, "h": 4, "config": {"query": {"promql": "sum by (service.name) (rate(rpc.server.duration.count{rpc.method=~\"$method\"}[5m]))"}, "unit": "reqps"}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- 13. Node.js Runtime (process.runtime.nodejs.*) — NEW
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Node.js Runtime',
    'Node.js application runtime metrics: event loop lag, heap usage, GC duration, active handles/requests, and CPU time via OTel Node.js runtime instrumentation',
    'runtime',
    true,
    13,
    ARRAY['nodejs', 'node', 'javascript', 'runtime', 'event-loop', 'otel'],
    '{
        "variables": [
            {"name": "service", "label": "Service", "type": "query", "query": "label_values(nodejs.eventloop.delay.mean, service.name)"},
            {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(nodejs.eventloop.delay.mean, service.instance.id)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "activity",
                "widgets": [
                    {"type": "stat", "title": "Event Loop Lag (p99)", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "nodejs.eventloop.delay.p99{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "instant": true}, "unit": "ms", "thresholds": [{"value": 0, "color": "green"}, {"value": 50, "color": "orange"}, {"value": 200, "color": "red"}]}},
                    {"type": "stat", "title": "Heap Used", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "nodejs.memory.heap.used{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "instant": true}, "unit": "bytes"}},
                    {"type": "stat", "title": "Active Handles", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "nodejs.active_handles.total{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "instant": true}}},
                    {"type": "stat", "title": "Process Uptime", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "process.uptime{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "instant": true}, "unit": "s"}},
                    {"type": "timeseries", "title": "Event Loop Lag", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "nodejs.eventloop.delay.mean{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "mean"}, {"promql": "nodejs.eventloop.delay.p99{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "p99"}]}, "unit": "ms"}},
                    {"type": "timeseries", "title": "Heap Memory", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "nodejs.memory.heap.used{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "used"}, {"promql": "nodejs.memory.heap.total{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "total"}, {"promql": "process.memory.rss{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "rss"}]}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "GC Duration", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(nodejs.gc.duration.sum{service.name=~\"$service\", gc.type=\"minor\"}[5m])", "legend_format": "minor GC"}, {"promql": "rate(nodejs.gc.duration.sum{service.name=~\"$service\", gc.type=\"major\"}[5m])", "legend_format": "major GC"}]}, "unit": "s"}},
                    {"type": "timeseries", "title": "Active Handles & Requests", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "nodejs.active_handles.total{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "handles"}, {"promql": "nodejs.active_requests.total{service.name=~\"$service\", service.instance.id=~\"$instance\"}", "legend_format": "requests"}]}}},
                    {"type": "timeseries", "title": "CPU Time", "x": 0, "y": 10, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(process.cpu.time{service.name=~\"$service\", state=\"user\"}[5m])", "legend_format": "user"}, {"promql": "rate(process.cpu.time{service.name=~\"$service\", state=\"system\"}[5m])", "legend_format": "system"}]}, "unit": "s"}},
                    {"type": "timeseries", "title": "Event Loop Utilization", "x": 6, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "nodejs.eventloop.utilization{service.name=~\"$service\", service.instance.id=~\"$instance\"}"}, "unit": "percentunit"}}
                ]
            }
        ]
    }'::jsonb
);
