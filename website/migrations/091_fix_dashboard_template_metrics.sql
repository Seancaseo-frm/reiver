-- Fix dashboard template metric names to align with actual OTel receiver output
-- and the platform-side Prometheus→OTel mappings in metric_names.rs.

-- ============================================================================
-- PostgreSQL: fix metric names to match postgresqlreceiver conventions
-- ============================================================================

UPDATE dashboard_templates
SET template_config = '{
    "variables": [
        {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(postgresql.backends, server.address)"},
        {"name": "database", "label": "Database", "type": "query", "query": "label_values(postgresql.backends, postgresql.database.name)"}
    ],
    "tabs": [
        {
            "name": "Overview",
            "icon": "database",
            "widgets": [
                {"type": "stat", "title": "Active Connections", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(postgresql.backends{server.address=~\"$instance\"})", "instant": true}}},
                {"type": "stat", "title": "Cache Hit Ratio", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m])) / (sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m])) + sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_read\"}[5m])))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "red"}, {"value": 0.9, "color": "orange"}, {"value": 0.99, "color": "green"}]}},
                {"type": "stat", "title": "Commits/s", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(postgresql.commits{server.address=~\"$instance\"}[5m]))", "instant": true}, "unit": "ops"}},
                {"type": "stat", "title": "Database Size", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(postgresql.db_size{server.address=~\"$instance\"})", "instant": true}, "unit": "bytes"}},
                {"type": "timeseries", "title": "Connections Over Time", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (postgresql.database.name) (postgresql.backends{server.address=~\"$instance\"})"}}},
                {"type": "timeseries", "title": "Transaction Rate", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(postgresql.commits{server.address=~\"$instance\"}[5m]))", "legend_format": "commits"}, {"promql": "sum(rate(postgresql.rollbacks{server.address=~\"$instance\"}[5m]))", "legend_format": "rollbacks"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Cache Hit Ratio Over Time", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m])) / (sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m])) + sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_read\"}[5m])))"}, "unit": "percentunit"}},
                {"type": "timeseries", "title": "Database Size Growth", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (postgresql.database.name) (postgresql.db_size{server.address=~\"$instance\"})"}, "unit": "bytes"}}
            ]
        },
        {
            "name": "Performance",
            "icon": "zap",
            "widgets": [
                {"type": "timeseries", "title": "Rows Operated", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(postgresql.operations{server.address=~\"$instance\", operation=\"ins\"}[5m]))", "legend_format": "inserted"}, {"promql": "sum(rate(postgresql.operations{server.address=~\"$instance\", operation=\"upd\"}[5m]))", "legend_format": "updated"}, {"promql": "sum(rate(postgresql.operations{server.address=~\"$instance\", operation=\"del\"}[5m]))", "legend_format": "deleted"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Scans: Sequential vs Index", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(postgresql.sequential_scans{server.address=~\"$instance\"}[5m]))", "legend_format": "sequential"}, {"promql": "sum(rate(postgresql.index.scans{server.address=~\"$instance\"}[5m]))", "legend_format": "index"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Block I/O (Heap Read vs Cache Hit)", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_read\"}[5m]))", "legend_format": "disk reads"}, {"promql": "sum(rate(postgresql.blocks_read{server.address=~\"$instance\", source=\"heap_hit\"}[5m]))", "legend_format": "cache hits"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Temp Bytes Written", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(postgresql.temp.io{server.address=~\"$instance\"}[5m]))"}, "unit": "Bps"}}
            ]
        },
        {
            "name": "Health",
            "icon": "heart",
            "widgets": [
                {"type": "timeseries", "title": "Replication Data Delay", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "postgresql.replication.data_delay{server.address=~\"$instance\"}"}, "unit": "bytes"}},
                {"type": "timeseries", "title": "Dead Tuples (Vacuum Needed)", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (postgresql.table.name) (postgresql.rows{server.address=~\"$instance\", state=\"dead\"})"}}},
                {"type": "timeseries", "title": "Locks by Mode", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (mode) (postgresql.database.locks{server.address=~\"$instance\"})"}}},
                {"type": "timeseries", "title": "WAL Replication Lag", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "postgresql.wal.lag{server.address=~\"$instance\"}"}, "unit": "s"}}
            ]
        }
    ]
}'::jsonb
WHERE name = 'PostgreSQL';

-- ============================================================================
-- Redis: fix metric names to match redisreceiver conventions
-- ============================================================================

UPDATE dashboard_templates
SET template_config = '{
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
                {"type": "timeseries", "title": "Commands Processed/s", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "rate(redis.commands.processed{server.address=~\"$instance\"}[5m])"}, "unit": "ops"}},
                {"type": "timeseries", "title": "Key Evictions & Expirations", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(redis.keys.evicted{server.address=~\"$instance\"}[5m])", "legend_format": "evicted"}, {"promql": "rate(redis.keys.expired{server.address=~\"$instance\"}[5m])", "legend_format": "expired"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Network I/O", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(redis.net.input{server.address=~\"$instance\"}[5m])", "legend_format": "input"}, {"promql": "rate(redis.net.output{server.address=~\"$instance\"}[5m])", "legend_format": "output"}]}, "unit": "Bps"}},
                {"type": "timeseries", "title": "DB Keys", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "redis.db.keys{server.address=~\"$instance\"}"}}}
            ]
        },
        {
            "name": "Reliability",
            "icon": "shield",
            "widgets": [
                {"type": "stat", "title": "Connected Replicas", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "redis.slaves.connected{server.address=~\"$instance\"}", "instant": true}}},
                {"type": "stat", "title": "Memory Fragmentation", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "redis.memory.fragmentation_ratio{server.address=~\"$instance\"}", "instant": true}, "thresholds": [{"value": 1, "color": "green"}, {"value": 1.5, "color": "orange"}, {"value": 2, "color": "red"}]}},
                {"type": "stat", "title": "Uptime", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "redis.uptime{server.address=~\"$instance\"}", "instant": true}, "unit": "s"}},
                {"type": "stat", "title": "Rejected Connections", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "rate(redis.connections.rejected{server.address=~\"$instance\"}[5m])", "instant": true}, "unit": "ops", "thresholds": [{"value": 0, "color": "green"}, {"value": 1, "color": "red"}]}},
                {"type": "timeseries", "title": "Replication Offset", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "redis.replication.offset{server.address=~\"$instance\"}"}, "unit": "bytes"}},
                {"type": "timeseries", "title": "RDB Changes Since Last Save", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "redis.rdb.changes_since_last_save{server.address=~\"$instance\"}"}}},
                {"type": "timeseries", "title": "CPU Usage", "x": 0, "y": 6, "w": 12, "h": 4, "config": {"query": {"queries": [{"promql": "rate(redis.cpu.time{server.address=~\"$instance\", state=\"user\"}[5m])", "legend_format": "user"}, {"promql": "rate(redis.cpu.time{server.address=~\"$instance\", state=\"sys\"}[5m])", "legend_format": "system"}]}, "unit": "s"}}
            ]
        }
    ]
}'::jsonb
WHERE name = 'Redis';

-- ============================================================================
-- Kafka: use only kafkametricsreceiver metrics, remove invented ones
-- ============================================================================

UPDATE dashboard_templates
SET template_config = '{
    "variables": [
        {"name": "topic", "label": "Topic", "type": "query", "query": "label_values(kafka.topic.partitions, kafka.topic)"},
        {"name": "consumer_group", "label": "Consumer Group", "type": "query", "query": "label_values(kafka.consumer_group.lag, kafka.consumer_group)"}
    ],
    "tabs": [
        {
            "name": "Overview",
            "icon": "send",
            "widgets": [
                {"type": "stat", "title": "Active Brokers", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "kafka.brokers", "instant": true}}},
                {"type": "stat", "title": "Total Partitions", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(kafka.topic.partitions{kafka.topic=~\"$topic\"})", "instant": true}}},
                {"type": "stat", "title": "Total Consumer Lag", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(kafka.consumer_group.lag{kafka.consumer_group=~\"$consumer_group\"})", "instant": true}, "thresholds": [{"value": 0, "color": "green"}, {"value": 1000, "color": "orange"}, {"value": 10000, "color": "red"}]}},
                {"type": "stat", "title": "Consumer Groups", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "count(kafka.consumer_group.members{kafka.consumer_group=~\"$consumer_group\"})", "instant": true}}},
                {"type": "timeseries", "title": "Partition Offset Rate (approx msgs/s)", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (kafka.topic) (rate(kafka.partition.current_offset{kafka.topic=~\"$topic\"}[5m]))"}, "unit": "ops"}},
                {"type": "timeseries", "title": "Consumer Group Lag", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (kafka.consumer_group) (kafka.consumer_group.lag{kafka.consumer_group=~\"$consumer_group\"})"}}},
                {"type": "timeseries", "title": "Under-Replicated Partitions", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (kafka.topic) (kafka.partition.replicas{kafka.topic=~\"$topic\"} - kafka.partition.replicas_in_sync{kafka.topic=~\"$topic\"})"}, "thresholds": [{"value": 0, "color": "green"}, {"value": 1, "color": "red"}]}},
                {"type": "timeseries", "title": "Consumer Group Offset Rate", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (kafka.consumer_group) (rate(kafka.consumer_group.offset{kafka.consumer_group=~\"$consumer_group\"}[5m]))"}, "unit": "ops"}},
                {"type": "timeseries", "title": "Partition Count by Topic", "x": 0, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "kafka.topic.partitions{kafka.topic=~\"$topic\"}"}}},
                {"type": "timeseries", "title": "Consumer Group Members", "x": 6, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "kafka.consumer_group.members{kafka.consumer_group=~\"$consumer_group\"}"}}}
            ]
        }
    ]
}'::jsonb
WHERE name = 'Kafka';

-- ============================================================================
-- ClickHouse: new template using native ClickHouse Prometheus metrics
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'ClickHouse',
    'ClickHouse database monitoring: queries, inserts, merges, memory, replication, and parts via native ClickHouse Prometheus metrics endpoint',
    'database',
    true,
    14,
    ARRAY['clickhouse', 'database', 'olap', 'analytics'],
    '{
        "variables": [
            {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(ClickHouseMetrics_TCPConnection, instance)"}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "database",
                "widgets": [
                    {"type": "stat", "title": "Running Queries", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "ClickHouseMetrics_Query{instance=~\"$instance\"}", "instant": true}}},
                    {"type": "stat", "title": "Active Merges", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "ClickHouseMetrics_Merge{instance=~\"$instance\"}", "instant": true}}},
                    {"type": "stat", "title": "Memory Tracked", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "ClickHouseMetrics_MemoryTracking{instance=~\"$instance\"}", "instant": true}, "unit": "bytes"}},
                    {"type": "stat", "title": "TCP Connections", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "ClickHouseMetrics_TCPConnection{instance=~\"$instance\"}", "instant": true}}},
                    {"type": "timeseries", "title": "Queries/s", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "rate(ClickHouseProfileEvents_Query{instance=~\"$instance\"}[5m])", "legend_format": "all"}, {"promql": "rate(ClickHouseProfileEvents_SelectQuery{instance=~\"$instance\"}[5m])", "legend_format": "select"}, {"promql": "rate(ClickHouseProfileEvents_InsertQuery{instance=~\"$instance\"}[5m])", "legend_format": "insert"}]}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Failed Queries/s", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "rate(ClickHouseProfileEvents_FailedQuery{instance=~\"$instance\"}[5m])"}, "unit": "ops", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.1, "color": "red"}]}},
                    {"type": "timeseries", "title": "Inserted Rows/s", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "rate(ClickHouseProfileEvents_InsertedRows{instance=~\"$instance\"}[5m])"}, "unit": "ops"}},
                    {"type": "timeseries", "title": "Inserted Bytes/s", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "rate(ClickHouseProfileEvents_InsertedBytes{instance=~\"$instance\"}[5m])"}, "unit": "Bps"}}
                ]
            },
            {
                "name": "Performance",
                "icon": "zap",
                "widgets": [
                    {"type": "timeseries", "title": "Merges", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "ClickHouseMetrics_Merge{instance=~\"$instance\"}", "legend_format": "active merges"}, {"promql": "rate(ClickHouseProfileEvents_MergedRows{instance=~\"$instance\"}[5m])", "legend_format": "rows merged/s"}]}}},
                    {"type": "timeseries", "title": "Max Parts per Partition", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "ClickHouseMetrics_MaxPartCountForPartition{instance=~\"$instance\"}"}, "thresholds": [{"value": 0, "color": "green"}, {"value": 150, "color": "orange"}, {"value": 300, "color": "red"}]}},
                    {"type": "timeseries", "title": "Read Throughput", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "rate(ClickHouseProfileEvents_ReadCompressedBytes{instance=~\"$instance\"}[5m])"}, "unit": "Bps"}},
                    {"type": "timeseries", "title": "Merge Time", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "rate(ClickHouseProfileEvents_MergesTimeMilliseconds{instance=~\"$instance\"}[5m]) / 1000"}, "unit": "s"}}
                ]
            },
            {
                "name": "Resources",
                "icon": "cpu",
                "widgets": [
                    {"type": "timeseries", "title": "Memory Usage", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "ClickHouseMetrics_MemoryTracking{instance=~\"$instance\"}"}, "unit": "bytes"}},
                    {"type": "timeseries", "title": "Connections", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "ClickHouseMetrics_TCPConnection{instance=~\"$instance\"}", "legend_format": "TCP"}, {"promql": "ClickHouseMetrics_HTTPConnection{instance=~\"$instance\"}", "legend_format": "HTTP"}]}}},
                    {"type": "timeseries", "title": "Background Pool Tasks", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "ClickHouseMetrics_BackgroundMergesAndMutationsPoolTask{instance=~\"$instance\"}"}}},
                    {"type": "timeseries", "title": "Replication Queue", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "ClickHouseMetrics_ReplicatedFetch{instance=~\"$instance\"}", "legend_format": "fetches"}, {"promql": "ClickHouseMetrics_ReplicatedSend{instance=~\"$instance\"}", "legend_format": "sends"}]}}}
                ]
            }
        ]
    }'::jsonb
);

-- ============================================================================
-- GenAI: update to match actual Flow service metric names (gen_ai.client.*)
-- ============================================================================

UPDATE dashboard_templates
SET template_config = '{
    "variables": [
        {"name": "model", "label": "Model", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.request.model)"},
        {"name": "system", "label": "System", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.system)"},
        {"name": "operation", "label": "Operation", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.operation.name)"}
    ],
    "tabs": [
        {
            "name": "Overview",
            "icon": "cpu",
            "widgets": [
                {"type": "stat", "title": "Request Rate", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[5m]))", "instant": true}, "unit": "reqps"}},
                {"type": "stat", "title": "Total Tokens/s", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[5m]))", "instant": true}, "unit": "ops"}},
                {"type": "stat", "title": "Avg Latency", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(gen_ai.client.operation.duration.sum{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[5m])) / sum(rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[5m]))", "instant": true}, "unit": "s"}},
                {"type": "stat", "title": "Error Rate", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "(sum(rate(gen_ai.client.error{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[5m])) or vector(0)) / sum(rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[5m]))", "instant": true}, "unit": "percentunit", "format": "percentage", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.01, "color": "orange"}, {"value": 0.05, "color": "red"}]}},
                {"type": "timeseries", "title": "Token Usage (Input vs Output)", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\", gen_ai.token.type=\"input\"}[5m]))", "legend_format": "input tokens"}, {"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\", gen_ai.token.type=\"output\"}[5m]))", "legend_format": "output tokens"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Operation Duration Percentiles", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "histogram_quantile(0.5, sum(rate(gen_ai.client.operation.duration_bucket{gen_ai.request.model=~\"$model\"}[5m])) by (le))", "legend_format": "p50"}, {"promql": "histogram_quantile(0.95, sum(rate(gen_ai.client.operation.duration_bucket{gen_ai.request.model=~\"$model\"}[5m])) by (le))", "legend_format": "p95"}, {"promql": "histogram_quantile(0.99, sum(rate(gen_ai.client.operation.duration_bucket{gen_ai.request.model=~\"$model\"}[5m])) by (le))", "legend_format": "p99"}]}, "unit": "s"}},
                {"type": "timeseries", "title": "Requests by Model", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{gen_ai.system=~\"$system\"}[5m]))"}, "unit": "reqps"}},
                {"type": "timeseries", "title": "Requests by Operation", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.operation.name) (rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\"}[5m]))"}, "unit": "reqps"}}
            ]
        },
        {
            "name": "Cost & Usage",
            "icon": "dollar-sign",
            "widgets": [
                {"type": "timeseries", "title": "Token Consumption by Model", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.token.usage{gen_ai.system=~\"$system\"}[5m]))"}, "unit": "ops"}},
                {"type": "timeseries", "title": "Requests by Operation Type", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.operation.name) (rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\"}[5m]))"}, "unit": "reqps"}},
                {"type": "timeseries", "title": "Token Efficiency (Output/Input Ratio)", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.token.type=\"output\", gen_ai.request.model=~\"$model\"}[5m])) / sum(rate(gen_ai.client.token.usage{gen_ai.token.type=\"input\", gen_ai.request.model=~\"$model\"}[5m]))"}}},
                {"type": "timeseries", "title": "Avg Tokens per Request by Model", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.token.usage{gen_ai.system=~\"$system\"}[5m])) / sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{gen_ai.system=~\"$system\"}[5m]))"}}}
            ]
        }
    ]
}'::jsonb
WHERE name = 'GenAI / LLM';
