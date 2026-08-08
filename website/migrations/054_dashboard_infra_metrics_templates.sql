-- Services Dashboard: Database tab uses ClickHouse infra metrics from samples_v1.
-- Database Monitoring: prebuilt ClickHouse + Redpanda template.
-- Split from 001_initial_schema.sql so the initial migration checksum stays stable.

UPDATE dashboard_templates SET
    template_config = $json$
{
        "variables": [
            {"name": "service", "label": "Service", "type": "service_select", "default": ""}
        ],
        "tabs": [
            {
                "name": "HTTP",
                "icon": "globe",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "Request Error Rate",
                        "x": 0, "y": 0, "w": 6, "h": 3,
                        "config": {
                            "displayMode": "stacked",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "countIf", "args": ["status_code = 'STATUS_CODE_ERROR'"], "alias": "errors"},
                                    {"fn": "count", "alias": "total"},
                                    {"expr": "errors / total * 100", "alias": "error_rate"}
                                ],
                                "where": "span_kind = 'SPAN_KIND_SERVER' AND span_attributes['http.route'] != ''",
                                "groupBy": ["span_attributes['http.route']"],
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Request Throughput",
                        "x": 6, "y": 0, "w": 6, "h": 3,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "requests"}],
                                "where": "span_kind = 'SPAN_KIND_SERVER'",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Request Latency",
                        "x": 0, "y": 3, "w": 8, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "quantile", "args": [0.5], "field": "duration", "alias": "p50"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95"},
                                    {"fn": "quantile", "args": [0.99], "field": "duration", "alias": "p99"}
                                ],
                                "where": "span_kind = 'SPAN_KIND_SERVER'",
                                "interval": "1m"
                            },
                            "unit": "ns"
                        }
                    },
                    {
                        "type": "histogram",
                        "title": "Latency Distribution",
                        "x": 8, "y": 3, "w": 4, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "histogram", "field": "duration", "buckets": 20}],
                                "where": "span_kind = 'SPAN_KIND_SERVER'"
                            },
                            "unit": "ns"
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Top 20 Most Time Consuming Endpoints",
                        "x": 0, "y": 7, "w": 12, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "endpoint",
                            "valueField": "total_time",
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes['http.route']", "alias": "endpoint"},
                                    {"fn": "sum", "field": "duration", "alias": "total_time"},
                                    {"fn": "count", "alias": "requests"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95"}
                                ],
                                "where": "span_kind = 'SPAN_KIND_SERVER' AND span_attributes['http.route'] != ''",
                                "groupBy": ["span_attributes['http.route']"],
                                "orderBy": "total_time DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Endpoints",
                        "x": 0, "y": 11, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["endpoint", "req_per_min", "p95_ns", "median_ns", "total_ns", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes['http.route']", "alias": "endpoint"},
                                    {"fn": "count", "alias": "requests"},
                                    {"expr": "requests / 60", "alias": "req_per_min"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95_ns"},
                                    {"fn": "quantile", "args": [0.5], "field": "duration", "alias": "median_ns"},
                                    {"fn": "sum", "field": "duration", "alias": "total_ns"},
                                    {"fn": "countIf", "args": ["status_code = 'STATUS_CODE_ERROR'"], "alias": "errors"}
                                ],
                                "where": "span_kind = 'SPAN_KIND_SERVER' AND span_attributes['http.route'] != ''",
                                "groupBy": ["span_attributes['http.route']"],
                                "orderBy": "total_ns DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            },
            {
                "name": "Database",
                "icon": "database",
                "widgets": [
                    {
                        "type": "stat",
                        "title": "Active Queries",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "queries"}],
                                "where": "metric_name = 'ClickHouseMetrics_Query'"
                            },
                            "description": "Currently executing queries"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Active Connections",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "connections"}],
                                "where": "metric_name = 'ClickHouseMetrics_TCPConnection'"
                            },
                            "description": "Open TCP connections"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Memory Usage",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "memory"}],
                                "where": "metric_name = 'ClickHouseMetrics_MemoryTracking'"
                            },
                            "unit": "bytes",
                            "description": "Allocated memory tracked by ClickHouse"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Background Merges",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "merges"}],
                                "where": "metric_name = 'ClickHouseMetrics_Merge'"
                            },
                            "description": "Running background merge operations"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Active Queries",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "queries"}],
                                "where": "metric_name = 'ClickHouseMetrics_Query'",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Active Connections",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "connections"}],
                                "where": "metric_name = 'ClickHouseMetrics_TCPConnection'",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "memory"}],
                                "where": "metric_name = 'ClickHouseMetrics_MemoryTracking'",
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Inserted Rows",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "rows"}],
                                "where": "metric_name = 'ClickHouseProfileEvents_InsertedRows'",
                                "interval": "1m"
                            }
                        }
                    }
                ]
            },
            {
                "name": "Errors",
                "icon": "alert-triangle",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "Error Events per Service",
                        "x": 0, "y": 0, "w": 12, "h": 5,
                        "config": {
                            "displayMode": "stacked",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "errors"}],
                                "where": "status_code = 'STATUS_CODE_ERROR'",
                                "groupBy": ["service_name"],
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Recent Errors",
                        "x": 0, "y": 5, "w": 12, "h": 5,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "timestamp", "alias": "time"},
                                    {"field": "service_name", "alias": "service"},
                                    {"field": "span_name", "alias": "operation"},
                                    {"field": "status_message", "alias": "message"}
                                ],
                                "where": "status_code = 'STATUS_CODE_ERROR'",
                                "orderBy": "timestamp DESC",
                                "limit": 100
                            }
                        }
                    }
                ]
            }
        ]
    }
$json$::jsonb,
    updated_at = NOW()
WHERE name = 'Services Dashboard';

-- ============================================================================
-- Database Monitoring dashboard template (ClickHouse + Redpanda)
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Database Monitoring',
    'Infrastructure metrics for ClickHouse and Redpanda: query throughput, connections, memory, disk, consumer lag, and partition health',
    'infrastructure',
    true,
    5,
    ARRAY['database', 'clickhouse', 'redpanda', 'infrastructure', 'kafka'],
    '{
        "variables": [
            {"name": "service", "label": "Service", "type": "service_select", "default": ""}
        ],
        "tabs": [
            {
                "name": "ClickHouse",
                "icon": "database",
                "widgets": [
                    {
                        "type": "stat",
                        "title": "Active Queries",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "queries"}],
                                "where": "metric_name = ''ClickHouseMetrics_Query''"
                            },
                            "description": "Currently executing queries"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Active Connections",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "connections"}],
                                "where": "metric_name = ''ClickHouseMetrics_TCPConnection''"
                            },
                            "description": "Open TCP connections to ClickHouse"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Memory Tracked",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "memory"}],
                                "where": "metric_name = ''ClickHouseMetrics_MemoryTracking''"
                            },
                            "unit": "bytes",
                            "description": "Memory allocated by ClickHouse"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Background Merges",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "merges"}],
                                "where": "metric_name = ''ClickHouseMetrics_Merge''"
                            },
                            "description": "Running background merge operations"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Active Queries",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "queries"}],
                                "where": "metric_name = ''ClickHouseMetrics_Query''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Active Connections",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "connections"}],
                                "where": "metric_name = ''ClickHouseMetrics_TCPConnection''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "memory"}],
                                "where": "metric_name = ''ClickHouseMetrics_MemoryTracking''",
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Background Merges",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "merges"}],
                                "where": "metric_name = ''ClickHouseMetrics_Merge''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Inserted Rows",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "rows"}],
                                "where": "metric_name = ''ClickHouseProfileEvents_InsertedRows''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Read Compressed Bytes",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "bytes"}],
                                "where": "metric_name = ''ClickHouseProfileEvents_ReadCompressedBytes''",
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Resident Memory (RSS)",
                        "x": 0, "y": 14, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "rss"}],
                                "where": "metric_name = ''ClickHouseAsyncMetrics_MemoryResident''",
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Queries Started",
                        "x": 6, "y": 14, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "queries"}],
                                "where": "metric_name = ''ClickHouseProfileEvents_Query''",
                                "interval": "1m"
                            }
                        }
                    }
                ]
            },
            {
                "name": "Redpanda",
                "icon": "radio",
                "widgets": [
                    {
                        "type": "stat",
                        "title": "Topics",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "topics"}],
                                "where": "metric_name = ''redpanda_cluster_topics''"
                            }
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Partitions",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "partitions"}],
                                "where": "metric_name = ''redpanda_cluster_partitions''"
                            }
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Brokers",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "brokers"}],
                                "where": "metric_name = ''redpanda_cluster_brokers''"
                            }
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Unavailable Partitions",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "unavailable"}],
                                "where": "metric_name = ''redpanda_cluster_unavailable_partitions''"
                            },
                            "color": "red",
                            "description": "Partitions without a quorum. Should be 0"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Records Produced",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "records"}],
                                "where": "metric_name = ''redpanda_kafka_records_produced_total''",
                                "groupBy": ["metric_attributes[''redpanda_topic'']"],
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Records Fetched",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "records"}],
                                "where": "metric_name = ''redpanda_kafka_records_fetched_total''",
                                "groupBy": ["metric_attributes[''redpanda_topic'']"],
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Consumer Group Lag",
                        "x": 0, "y": 6, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "lag"}],
                                "where": "metric_name = ''redpanda_kafka_consumer_group_lag_sum''",
                                "groupBy": ["metric_attributes[''redpanda_group'']"],
                                "interval": "1m"
                            },
                            "description": "Total offset lag per consumer group. Rising lag means consumers are falling behind"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Disk Free Space",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "free"}],
                                "where": "metric_name = ''redpanda_storage_disk_free_bytes''",
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "allocated"}],
                                "where": "metric_name = ''redpanda_memory_allocated_memory''",
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Request Bytes (In/Out)",
                        "x": 0, "y": 14, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "bytes"}],
                                "where": "metric_name = ''redpanda_kafka_request_bytes_total''",
                                "groupBy": ["metric_attributes[''redpanda_request'']"],
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Under-replicated Replicas",
                        "x": 6, "y": 14, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "under_replicated"}],
                                "where": "metric_name = ''redpanda_kafka_under_replicated_replicas''",
                                "interval": "1m"
                            },
                            "description": "Replicas behind the leader. Should be 0 in steady state"
                        }
                    }
                ]
            }
        ]
    }'::jsonb
)
ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    category = EXCLUDED.category,
    is_featured = EXCLUDED.is_featured,
    display_order = EXCLUDED.display_order,
    tags = EXCLUDED.tags,
    template_config = EXCLUDED.template_config,
    updated_at = NOW();
