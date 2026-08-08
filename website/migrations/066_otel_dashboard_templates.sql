-- Built-in dashboard templates for common OpenTelemetry Collector setups.
-- Each uses PromQL queries against well-known OTel metric names.

-- ============================================================================
-- Host Metrics (hostmetricsreceiver)
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Host Metrics',
    'CPU, memory, disk, and network monitoring via OpenTelemetry hostmetricsreceiver',
    'infrastructure',
    true,
    10,
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
                    {
                        "type": "stat",
                        "title": "CPU Utilization",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "avg(system.cpu.utilization{host.name=~\"$instance\"})", "instant": true},
                            "unit": "percentunit",
                            "format": "percentage"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Memory Used",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "system.memory.usage{host.name=~\"$instance\", state=\"used\"}", "instant": true},
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Disk Used",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "system.filesystem.usage{host.name=~\"$instance\", state=\"used\"}", "instant": true},
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Network Received",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "rate(system.network.io{host.name=~\"$instance\", direction=\"receive\"}[5m])", "instant": true},
                            "unit": "Bps"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "CPU Utilization Over Time",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "avg by (cpu) (system.cpu.utilization{host.name=~\"$instance\"})"},
                            "unit": "percentunit"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "system.memory.usage{host.name=~\"$instance\"}"},
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Disk I/O",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "rate(system.disk.io{host.name=~\"$instance\", direction=\"read\"}[5m])", "legend_format": "read"},
                                    {"promql": "rate(system.disk.io{host.name=~\"$instance\", direction=\"write\"}[5m])", "legend_format": "write"}
                                ]
                            },
                            "unit": "Bps"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Network I/O",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "rate(system.network.io{host.name=~\"$instance\", direction=\"receive\"}[5m])", "legend_format": "received"},
                                    {"promql": "rate(system.network.io{host.name=~\"$instance\", direction=\"transmit\"}[5m])", "legend_format": "transmitted"}
                                ]
                            },
                            "unit": "Bps"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Filesystem Usage",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "system.filesystem.utilization{host.name=~\"$instance\"}"},
                            "unit": "percentunit"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "System Load Average",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "system.cpu.load_average.1m{host.name=~\"$instance\"}", "legend_format": "1m"},
                                    {"promql": "system.cpu.load_average.5m{host.name=~\"$instance\"}", "legend_format": "5m"},
                                    {"promql": "system.cpu.load_average.15m{host.name=~\"$instance\"}", "legend_format": "15m"}
                                ]
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Kubernetes Cluster (k8sclusterreceiver + kubeletstatsreceiver)
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Kubernetes Cluster',
    'Cluster-level monitoring: node status, pod resources, container restarts, and deployment health via OTel k8sclusterreceiver',
    'infrastructure',
    true,
    11,
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
                    {
                        "type": "stat",
                        "title": "Ready Nodes",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "sum(k8s.node.condition_ready)", "instant": true}
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Running Pods",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "count(k8s.pod.phase{k8s.namespace.name=~\"$namespace\"} == 1)", "instant": true}
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Container Restarts",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "sum(k8s.container.restarts{k8s.namespace.name=~\"$namespace\"})", "instant": true}
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Available Deployments",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "sum(k8s.deployment.available{k8s.namespace.name=~\"$namespace\"})", "instant": true}
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Pod CPU Usage by Namespace",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "sum by (k8s.namespace.name) (k8s.pod.cpu.utilization{k8s.namespace.name=~\"$namespace\"})"},
                            "unit": "percentunit"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Pod Memory Usage by Namespace",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "sum by (k8s.namespace.name) (k8s.pod.memory.usage{k8s.namespace.name=~\"$namespace\"})"},
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Container Restarts Over Time",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "sum by (k8s.pod.name) (rate(k8s.container.restarts{k8s.namespace.name=~\"$namespace\"}[5m]))"}
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Node CPU Utilization",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "k8s.node.cpu.utilization{k8s.node.name=~\"$node\"}"},
                            "unit": "percentunit"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Node Memory Usage",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "k8s.node.memory.usage{k8s.node.name=~\"$node\"}", "legend_format": "used"},
                                    {"promql": "k8s.node.memory.available{k8s.node.name=~\"$node\"}", "legend_format": "available"}
                                ]
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Network I/O per Pod",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "sum by (k8s.pod.name) (rate(k8s.pod.network.io{k8s.namespace.name=~\"$namespace\", direction=\"receive\"}[5m]))", "legend_format": "{{k8s.pod.name}} rx"},
                                    {"promql": "sum by (k8s.pod.name) (rate(k8s.pod.network.io{k8s.namespace.name=~\"$namespace\", direction=\"transmit\"}[5m]))", "legend_format": "{{k8s.pod.name}} tx"}
                                ]
                            },
                            "unit": "Bps"
                        }
                    }
                ]
            }
        ]
    }'::jsonb
)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- PostgreSQL (postgresqlreceiver)
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'PostgreSQL',
    'Database monitoring: connections, transactions, locks, replication lag, and query performance via OTel postgresqlreceiver',
    'database',
    true,
    12,
    ARRAY['postgresql', 'postgres', 'database', 'otel'],
    '{
        "variables": [
            {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(postgresql.backends, server.address)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "database",
                "widgets": [
                    {
                        "type": "stat",
                        "title": "Active Connections",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "sum(postgresql.backends{server.address=~\"$instance\"})", "instant": true}
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Commits/s",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "sum(rate(postgresql.commits{server.address=~\"$instance\"}[5m]))", "instant": true},
                            "unit": "ops"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Rollbacks/s",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "sum(rate(postgresql.rollbacks{server.address=~\"$instance\"}[5m]))", "instant": true},
                            "unit": "ops"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Database Size",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "sum(postgresql.db_size{server.address=~\"$instance\"})", "instant": true},
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Active Connections",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "sum by (postgresql.db.name) (postgresql.backends{server.address=~\"$instance\"})"}
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Transactions/s",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "sum(rate(postgresql.commits{server.address=~\"$instance\"}[5m]))", "legend_format": "commits"},
                                    {"promql": "sum(rate(postgresql.rollbacks{server.address=~\"$instance\"}[5m]))", "legend_format": "rollbacks"}
                                ]
                            },
                            "unit": "ops"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Rows Operated",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "sum(rate(postgresql.rows{server.address=~\"$instance\", state=\"inserted\"}[5m]))", "legend_format": "inserted"},
                                    {"promql": "sum(rate(postgresql.rows{server.address=~\"$instance\", state=\"updated\"}[5m]))", "legend_format": "updated"},
                                    {"promql": "sum(rate(postgresql.rows{server.address=~\"$instance\", state=\"deleted\"}[5m]))", "legend_format": "deleted"}
                                ]
                            },
                            "unit": "ops"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Block I/O",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap\"}[5m]))", "legend_format": "heap reads"},
                                    {"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m]))", "legend_format": "cache hits"}
                                ]
                            },
                            "unit": "ops"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Replication Lag",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "postgresql.replication.data_delay{server.address=~\"$instance\"}"},
                            "unit": "s"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Locks by Type",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "sum by (lock_type) (postgresql.deadlocks{server.address=~\"$instance\"})"}
                        }
                    }
                ]
            }
        ]
    }'::jsonb
)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Redis (redisreceiver)
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Redis',
    'Cache monitoring: hit/miss rate, memory usage, connections, latency, and key expiry via OTel redisreceiver',
    'database',
    true,
    13,
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
                    {
                        "type": "stat",
                        "title": "Connected Clients",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "redis.clients.connected{server.address=~\"$instance\"}", "instant": true}
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Memory Used",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "redis.memory.used{server.address=~\"$instance\"}", "instant": true},
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Cache Hit Rate",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "redis.keyspace.hits{server.address=~\"$instance\"} / (redis.keyspace.hits{server.address=~\"$instance\"} + redis.keyspace.misses{server.address=~\"$instance\"})", "instant": true},
                            "unit": "percentunit",
                            "format": "percentage"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Uptime",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "redis.uptime{server.address=~\"$instance\"}", "instant": true},
                            "unit": "s"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Commands/s",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "rate(redis.commands.processed{server.address=~\"$instance\"}[5m])"},
                            "unit": "ops"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Hit / Miss Rate",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "rate(redis.keyspace.hits{server.address=~\"$instance\"}[5m])", "legend_format": "hits"},
                                    {"promql": "rate(redis.keyspace.misses{server.address=~\"$instance\"}[5m])", "legend_format": "misses"}
                                ]
                            },
                            "unit": "ops"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "redis.memory.used{server.address=~\"$instance\"}", "legend_format": "used"},
                                    {"promql": "redis.memory.peak{server.address=~\"$instance\"}", "legend_format": "peak"},
                                    {"promql": "redis.memory.rss{server.address=~\"$instance\"}", "legend_format": "rss"}
                                ]
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Connected Clients",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "redis.clients.connected{server.address=~\"$instance\"}", "legend_format": "connected"},
                                    {"promql": "redis.clients.blocked{server.address=~\"$instance\"}", "legend_format": "blocked"}
                                ]
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Network I/O",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "rate(redis.net.input{server.address=~\"$instance\"}[5m])", "legend_format": "input"},
                                    {"promql": "rate(redis.net.output{server.address=~\"$instance\"}[5m])", "legend_format": "output"}
                                ]
                            },
                            "unit": "Bps"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Key Evictions & Expirations",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "rate(redis.keys.evicted{server.address=~\"$instance\"}[5m])", "legend_format": "evicted"},
                                    {"promql": "rate(redis.keys.expired{server.address=~\"$instance\"}[5m])", "legend_format": "expired"}
                                ]
                            },
                            "unit": "ops"
                        }
                    }
                ]
            }
        ]
    }'::jsonb
)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- HTTP Service (standard OTel HTTP server metrics)
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'HTTP Service',
    'Request rate, error rate, latency percentiles, and throughput for HTTP services using standard OTel semantic conventions',
    'apm',
    true,
    14,
    ARRAY['http', 'service', 'apm', 'latency', 'otel'],
    '{
        "variables": [
            {"name": "service", "label": "Service", "type": "service_select", "default": ""},
            {"name": "http_route", "label": "Route", "type": "query", "query": "label_values(http.server.request.duration_count, http.route)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "globe",
                "widgets": [
                    {
                        "type": "stat",
                        "title": "Request Rate",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "sum(rate(http.server.request.duration_count{http.route=~\"$http_route\"}[5m]))", "instant": true},
                            "unit": "reqps"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Error Rate",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "sum(rate(http.server.request.duration_count{http.response.status_code=~\"5..\", http.route=~\"$http_route\"}[5m])) / sum(rate(http.server.request.duration_count{http.route=~\"$http_route\"}[5m]))", "instant": true},
                            "unit": "percentunit",
                            "format": "percentage",
                            "thresholds": [
                                {"value": 0, "color": "green"},
                                {"value": 0.01, "color": "orange"},
                                {"value": 0.05, "color": "red"}
                            ]
                        }
                    },
                    {
                        "type": "stat",
                        "title": "P95 Latency",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "histogram_quantile(0.95, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "instant": true},
                            "unit": "s"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "P50 Latency",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {"promql": "histogram_quantile(0.5, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "instant": true},
                            "unit": "s"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Request Rate by Status",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "sum by (http.response.status_code) (rate(http.server.request.duration_count{http.route=~\"$http_route\"}[5m]))"},
                            "unit": "reqps"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Latency Percentiles",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "queries": [
                                    {"promql": "histogram_quantile(0.5, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "legend_format": "p50"},
                                    {"promql": "histogram_quantile(0.9, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "legend_format": "p90"},
                                    {"promql": "histogram_quantile(0.95, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "legend_format": "p95"},
                                    {"promql": "histogram_quantile(0.99, sum(rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])) by (le))", "legend_format": "p99"}
                                ]
                            },
                            "unit": "s"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Request Rate by Route",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "sum by (http.route) (rate(http.server.request.duration_count{http.route=~\"$http_route\"}[5m]))"},
                            "unit": "reqps"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Error Rate Over Time",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "sum(rate(http.server.request.duration_count{http.response.status_code=~\"5..\", http.route=~\"$http_route\"}[5m])) / sum(rate(http.server.request.duration_count{http.route=~\"$http_route\"}[5m]))"},
                            "unit": "percentunit"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Request Size",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "avg(rate(http.server.request.body.size_sum{http.route=~\"$http_route\"}[5m]) / rate(http.server.request.body.size_count{http.route=~\"$http_route\"}[5m]))"},
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Response Size",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {"promql": "avg(rate(http.server.response.body.size_sum{http.route=~\"$http_route\"}[5m]) / rate(http.server.response.body.size_count{http.route=~\"$http_route\"}[5m]))"},
                            "unit": "bytes"
                        }
                    }
                ]
            }
        ]
    }'::jsonb
)
ON CONFLICT DO NOTHING;
