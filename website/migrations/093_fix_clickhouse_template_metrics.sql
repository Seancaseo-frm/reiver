-- Fix ClickHouse dashboard template metric names:
-- 1. MaxPartCountForPartition is under ClickHouseAsyncMetrics_ (not ClickHouseMetrics_)
-- 2. Merge time metric is MergeTotalMilliseconds (not MergesTimeMilliseconds)

UPDATE dashboard_templates
SET template_config = '{
    "variables": [
        {"name": "instance", "label": "Instance", "type": "query", "query": "label_values(ClickHouseMetrics_TCPConnection, instance)"}
    ],
    "tabs": [
        {
            "name": "Overview",
            "icon": "database",
            "widgets": [
                {"type": "stat", "title": "Running Queries", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(ClickHouseMetrics_Query{instance=~\"$instance\"})", "instant": true}}},
                {"type": "stat", "title": "Active Merges", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(ClickHouseMetrics_Merge{instance=~\"$instance\"})", "instant": true}}},
                {"type": "stat", "title": "Memory Tracked", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(ClickHouseMetrics_MemoryTracking{instance=~\"$instance\"})", "instant": true}, "unit": "bytes"}},
                {"type": "stat", "title": "TCP Connections", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(ClickHouseMetrics_TCPConnection{instance=~\"$instance\"})", "instant": true}}},
                {"type": "timeseries", "title": "Queries/s", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(ClickHouseProfileEvents_Query{instance=~\"$instance\"}[5m]))", "legend_format": "all"}, {"promql": "sum(rate(ClickHouseProfileEvents_SelectQuery{instance=~\"$instance\"}[5m]))", "legend_format": "select"}, {"promql": "sum(rate(ClickHouseProfileEvents_InsertQuery{instance=~\"$instance\"}[5m]))", "legend_format": "insert"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Failed Queries/s", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(ClickHouseProfileEvents_FailedQuery{instance=~\"$instance\"}[5m]))"}, "unit": "ops", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.1, "color": "red"}]}},
                {"type": "timeseries", "title": "Inserted Rows/s", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(ClickHouseProfileEvents_InsertedRows{instance=~\"$instance\"}[5m]))"}, "unit": "ops"}},
                {"type": "timeseries", "title": "Inserted Bytes/s", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(ClickHouseProfileEvents_InsertedBytes{instance=~\"$instance\"}[5m]))"}, "unit": "Bps"}}
            ]
        },
        {
            "name": "Performance",
            "icon": "zap",
            "widgets": [
                {"type": "timeseries", "title": "Merges", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(ClickHouseMetrics_Merge{instance=~\"$instance\"})", "legend_format": "active merges"}, {"promql": "sum(rate(ClickHouseProfileEvents_MergedRows{instance=~\"$instance\"}[5m]))", "legend_format": "rows merged/s"}]}}},
                {"type": "timeseries", "title": "Max Parts per Partition", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "max(ClickHouseAsyncMetrics_MaxPartCountForPartition{instance=~\"$instance\"})"}, "thresholds": [{"value": 0, "color": "green"}, {"value": 150, "color": "orange"}, {"value": 300, "color": "red"}]}},
                {"type": "timeseries", "title": "Read Throughput", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(ClickHouseProfileEvents_ReadCompressedBytes{instance=~\"$instance\"}[5m]))"}, "unit": "Bps"}},
                {"type": "timeseries", "title": "Merge Time", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(ClickHouseProfileEvents_MergeTotalMilliseconds{instance=~\"$instance\"}[5m])) / 1000"}, "unit": "s"}}
            ]
        },
        {
            "name": "Resources",
            "icon": "cpu",
            "widgets": [
                {"type": "timeseries", "title": "Memory Usage", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "ClickHouseMetrics_MemoryTracking{instance=~\"$instance\"}"}, "unit": "bytes"}},
                {"type": "timeseries", "title": "Connections", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(ClickHouseMetrics_TCPConnection{instance=~\"$instance\"})", "legend_format": "TCP"}, {"promql": "sum(ClickHouseMetrics_HTTPConnection{instance=~\"$instance\"})", "legend_format": "HTTP"}]}}},
                {"type": "timeseries", "title": "Background Pool Tasks", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum(ClickHouseMetrics_BackgroundMergesAndMutationsPoolTask{instance=~\"$instance\"})"}}},
                {"type": "timeseries", "title": "Replication Queue", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(ClickHouseMetrics_ReplicatedFetch{instance=~\"$instance\"})", "legend_format": "fetches"}, {"promql": "sum(ClickHouseMetrics_ReplicatedSend{instance=~\"$instance\"})", "legend_format": "sends"}]}}}
            ]
        }
    ]
}'::jsonb
WHERE name = 'ClickHouse';
