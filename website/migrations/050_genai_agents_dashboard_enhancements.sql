-- Enhance GenAI Dashboard and Agents Dashboard with full OTel GenAI semconv coverage.
--
-- GenAI Dashboard: add Caching, Token Usage, Providers tabs; enhance Models/Sessions/Overview.
-- Agents Dashboard: make generic (remove service_name hardcode), add Agents/Conversations tabs.

-- ============================================================================
-- 1. GenAI Dashboard
-- ============================================================================
UPDATE dashboard_templates
SET
    description = 'Comprehensive GenAI observability: models, providers, token usage, caching, sessions, and errors — powered by OTel GenAI semantic conventions',
    template_config = '{
        "variables": [],
        "tabs": [
            {
                "name": "Overview",
                "icon": "brain",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "Total GenAI Spans",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "total_spans"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Duration",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "avg", "field": "duration", "alias": "avg_duration"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Error Rate %",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "percentage",
                            "color": "red",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"},
                                    {"fn": "count", "alias": "total"},
                                    {"expr": "if(total > 0, errors / total * 100, 0)", "alias": "error_rate"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Unique Models",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "uniqExact", "field": "span_attributes[''gen_ai.request.model'']", "alias": "unique_models"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "GenAI Spans Over Time",
                        "x": 0, "y": 2, "w": 8, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "spans"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Latency Percentiles",
                        "x": 8, "y": 2, "w": 4, "h": 4,
                        "config": {
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "quantile", "args": [0.5], "field": "duration", "alias": "p50"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95"},
                                    {"fn": "quantile", "args": [0.99], "field": "duration", "alias": "p99"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Finish Reasons",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "finish_reason",
                            "valueField": "count",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.response.finish_reasons'']", "alias": "finish_reason"},
                                    {"fn": "count", "alias": "count"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.response.finish_reasons''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.response.finish_reasons'']"],
                                "orderBy": "count DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Requests by Provider",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "provider",
                            "valueField": "calls",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.provider.name'']", "alias": "provider"},
                                    {"fn": "count", "alias": "calls"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.provider.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 20
                            }
                        }
                    }
                ]
            },
            {
                "name": "Models",
                "icon": "cpu",
                "widgets": [
                    {
                        "type": "bar",
                        "title": "Requests by Model",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "model",
                            "valueField": "calls",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.request.model'']", "alias": "model"},
                                    {"fn": "count", "alias": "calls"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.request.model''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.request.model'']"],
                                "orderBy": "calls DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Avg Latency by Model",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "model",
                            "valueField": "avg_latency",
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.request.model'']", "alias": "model"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.request.model''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.request.model'']"],
                                "orderBy": "avg_latency DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Model Performance",
                        "x": 0, "y": 4, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["model", "provider", "calls", "avg_latency", "p95_latency", "input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.request.model'']", "alias": "model"},
                                    {"field": "span_attributes[''gen_ai.provider.name'']", "alias": "provider"},
                                    {"fn": "count", "alias": "calls"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95_latency"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])", "alias": "input_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens''])", "alias": "output_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens''])", "alias": "cache_read_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_creation.input_tokens''])", "alias": "cache_creation_tokens"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.request.model''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.request.model'']", "span_attributes[''gen_ai.provider.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 50
                            }
                        }
                    },
                    {
                        "type": "histogram",
                        "title": "Latency Distribution",
                        "x": 0, "y": 9, "w": 12, "h": 4,
                        "config": {
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "histogram", "field": "duration", "buckets": 20}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    }
                ]
            },
            {
                "name": "Token Usage",
                "icon": "bar-chart-2",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "Total Input Tokens",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])", "alias": "total_input"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Total Output Tokens",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens''])", "alias": "total_output"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Tokens per Request",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"expr": "(sum(toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])) + sum(toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens'']))) / max(count(), 1)", "alias": "avg_tokens"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Total Tokens",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"expr": "sum(toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])) + sum(toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens'']))", "alias": "total_tokens"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Token Consumption Over Time",
                        "x": 0, "y": 2, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])", "alias": "input_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens''])", "alias": "output_tokens"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "interval": "1h"
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Top Models by Token Usage",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "model",
                            "valueField": "total_tokens",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.request.model'']", "alias": "model"},
                                    {"expr": "sum(toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])) + sum(toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens'']))", "alias": "total_tokens"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.request.model''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.request.model'']"],
                                "orderBy": "total_tokens DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Token Usage by Provider",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "sortable": true,
                            "columns": ["provider", "input_tokens", "output_tokens", "total_tokens", "requests", "avg_per_request"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.provider.name'']", "alias": "provider"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])", "alias": "input_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens''])", "alias": "output_tokens"},
                                    {"expr": "sum(toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])) + sum(toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens'']))", "alias": "total_tokens"},
                                    {"fn": "count", "alias": "requests"},
                                    {"expr": "(sum(toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])) + sum(toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens'']))) / max(count(), 1)", "alias": "avg_per_request"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.provider.name'']"],
                                "orderBy": "total_tokens DESC",
                                "limit": 20
                            }
                        }
                    }
                ]
            },
            {
                "name": "Caching",
                "icon": "database",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "Cache Read Tokens",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens''])", "alias": "cache_read_tokens"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Cache Creation Tokens",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_creation.input_tokens''])", "alias": "cache_creation_tokens"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Cache Hit Rate %",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "percentage",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "countIf", "args": ["toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens'']) > 0"], "alias": "cache_hits"},
                                    {"fn": "count", "alias": "total"},
                                    {"expr": "if(total > 0, cache_hits / total * 100, 0)", "alias": "cache_hit_rate"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Requests with Cache Hits",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "countIf", "args": ["toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens'']) > 0"], "alias": "cache_hit_requests"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Cache Tokens Over Time",
                        "x": 0, "y": 2, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens''])", "alias": "cache_read"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_creation.input_tokens''])", "alias": "cache_creation"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "interval": "1h"
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Cache by Model",
                        "x": 0, "y": 6, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["model", "requests", "cache_hits", "cache_read_tokens", "cache_creation_tokens", "cache_hit_rate"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.request.model'']", "alias": "model"},
                                    {"fn": "count", "alias": "requests"},
                                    {"fn": "countIf", "args": ["toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens'']) > 0"], "alias": "cache_hits"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens''])", "alias": "cache_read_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_creation.input_tokens''])", "alias": "cache_creation_tokens"},
                                    {"expr": "if(requests > 0, cache_hits / requests * 100, 0)", "alias": "cache_hit_rate"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.request.model''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.request.model'']"],
                                "orderBy": "cache_read_tokens DESC",
                                "limit": 50
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Cache by Session",
                        "x": 0, "y": 11, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["session_id", "requests", "cache_hits", "cache_read_tokens", "cache_creation_tokens", "cache_hit_rate"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.conversation.id'']", "alias": "session_id"},
                                    {"fn": "count", "alias": "requests"},
                                    {"fn": "countIf", "args": ["toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens'']) > 0"], "alias": "cache_hits"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens''])", "alias": "cache_read_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_creation.input_tokens''])", "alias": "cache_creation_tokens"},
                                    {"expr": "if(requests > 0, cache_hits / requests * 100, 0)", "alias": "cache_hit_rate"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.conversation.id''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.conversation.id'']"],
                                "orderBy": "cache_read_tokens DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            },
            {
                "name": "Providers",
                "icon": "layers",
                "widgets": [
                    {
                        "type": "bar",
                        "title": "Requests by Provider",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "provider",
                            "valueField": "calls",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.provider.name'']", "alias": "provider"},
                                    {"fn": "count", "alias": "calls"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.provider.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Avg Latency by Provider",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "provider",
                            "valueField": "avg_latency",
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.provider.name'']", "alias": "provider"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.provider.name'']"],
                                "orderBy": "avg_latency DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Provider Comparison",
                        "x": 0, "y": 4, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["provider", "calls", "avg_latency", "p95_latency", "error_rate", "input_tokens", "output_tokens", "cache_read_tokens"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.provider.name'']", "alias": "provider"},
                                    {"fn": "count", "alias": "calls"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95_latency"},
                                    {"expr": "if(count() > 0, countIf(status_code = ''STATUS_CODE_ERROR'') / count() * 100, 0)", "alias": "error_rate"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])", "alias": "input_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens''])", "alias": "output_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens''])", "alias": "cache_read_tokens"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.provider.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 20
                            }
                        }
                    }
                ]
            },
            {
                "name": "Sessions",
                "icon": "message-square",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "Unique Sessions",
                        "x": 0, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "uniqExact", "field": "span_attributes[''gen_ai.conversation.id'']", "alias": "unique_sessions"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.conversation.id''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Spans per Session",
                        "x": 4, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "count", "alias": "total_spans"},
                                    {"fn": "uniqExact", "field": "span_attributes[''gen_ai.conversation.id'']", "alias": "sessions"},
                                    {"expr": "if(sessions > 0, total_spans / sessions, 0)", "alias": "avg_per_session"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.conversation.id''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Total Session Tokens",
                        "x": 8, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"expr": "sum(toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])) + sum(toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens'']))", "alias": "total_tokens"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.conversation.id''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Sessions Over Time",
                        "x": 0, "y": 2, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "uniqExact", "field": "span_attributes[''gen_ai.conversation.id'']", "alias": "sessions"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.conversation.id''] != ''''",
                                "interval": "1h"
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Sessions",
                        "x": 0, "y": 6, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["session_id", "spans", "avg_duration", "input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.conversation.id'']", "alias": "session_id"},
                                    {"fn": "count", "alias": "spans"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_duration"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])", "alias": "input_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens''])", "alias": "output_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_read.input_tokens''])", "alias": "cache_read_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.cache_creation.input_tokens''])", "alias": "cache_creation_tokens"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.conversation.id''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.conversation.id'']"],
                                "orderBy": "spans DESC",
                                "limit": 50
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
                        "type": "metric",
                        "title": "Total Errors",
                        "x": 0, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "color": "red",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Error Rate %",
                        "x": 4, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "percentage",
                            "color": "red",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"},
                                    {"fn": "count", "alias": "total"},
                                    {"expr": "if(total > 0, errors / total * 100, 0)", "alias": "error_rate"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Affected Models",
                        "x": 8, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "color": "red",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "uniqExact", "field": "span_attributes[''gen_ai.request.model'']", "alias": "affected_models"}],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND status_code = ''STATUS_CODE_ERROR''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Errors Over Time",
                        "x": 0, "y": 2, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"},
                                    {"fn": "count", "alias": "total"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != ''''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Errors by Model",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "model",
                            "valueField": "errors",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.request.model'']", "alias": "model"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND status_code = ''STATUS_CODE_ERROR'' AND span_attributes[''gen_ai.request.model''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.request.model'']"],
                                "orderBy": "errors DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Errors by Provider",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "provider",
                            "valueField": "errors",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.provider.name'']", "alias": "provider"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' AND status_code = ''STATUS_CODE_ERROR''",
                                "groupBy": ["span_attributes[''gen_ai.provider.name'']"],
                                "orderBy": "errors DESC",
                                "limit": 20
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
WHERE name = 'GenAI Dashboard';


-- ============================================================================
-- 2. Agents Dashboard — generic (no service_name hardcode) + new tabs
-- ============================================================================
UPDATE dashboard_templates
SET
    description = 'Monitor AI agent activity: tool calls, agent performance, conversations, latency, and errors — powered by OTel GenAI semantic conventions',
    template_config = '{
        "variables": [],
        "tabs": [
            {
                "name": "Overview",
                "icon": "activity",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "Total Tool Calls",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "total_calls"}],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'')"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Latency",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "avg", "field": "duration", "alias": "avg_latency"}],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'')"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Error Rate %",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "percentage",
                            "color": "red",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"},
                                    {"fn": "count", "alias": "total"},
                                    {"expr": "if(total > 0, errors / total * 100, 0)", "alias": "error_rate"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'')"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Unique Tools",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "uniqExact", "field": "span_attributes[''gen_ai.tool.name'']", "alias": "unique_tools"}],
                                "where": "span_attributes[''gen_ai.operation.name''] = ''execute_tool'' AND span_attributes[''gen_ai.tool.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Operations Over Time",
                        "x": 0, "y": 2, "w": 8, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "calls"}],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'')",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Error Rate Over Time",
                        "x": 8, "y": 2, "w": 4, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"},
                                    {"fn": "count", "alias": "total"},
                                    {"expr": "if(total > 0, errors / total * 100, 0)", "alias": "error_rate"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'')",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Operations Breakdown",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "operation",
                            "valueField": "count",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.operation.name'']", "alias": "operation"},
                                    {"fn": "count", "alias": "count"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'')",
                                "groupBy": ["span_attributes[''gen_ai.operation.name'']"],
                                "orderBy": "count DESC",
                                "limit": 10
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Calls by Service",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "service",
                            "valueField": "calls",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "service_name", "alias": "service"},
                                    {"fn": "count", "alias": "calls"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'')",
                                "groupBy": ["service_name"],
                                "orderBy": "calls DESC",
                                "limit": 20
                            }
                        }
                    }
                ]
            },
            {
                "name": "Agents",
                "icon": "users",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "Unique Agents",
                        "x": 0, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "uniqExact", "field": "span_attributes[''gen_ai.agent.name'']", "alias": "unique_agents"}],
                                "where": "span_attributes[''gen_ai.agent.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Total Agent Invocations",
                        "x": 4, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "invocations"}],
                                "where": "span_attributes[''gen_ai.agent.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Agent Latency",
                        "x": 8, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "avg", "field": "duration", "alias": "avg_latency"}],
                                "where": "span_attributes[''gen_ai.agent.name''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Calls by Agent",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "agent_name",
                            "valueField": "calls",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.agent.name'']", "alias": "agent_name"},
                                    {"fn": "count", "alias": "calls"}
                                ],
                                "where": "span_attributes[''gen_ai.agent.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.agent.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Avg Latency by Agent",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "agent_name",
                            "valueField": "avg_latency",
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.agent.name'']", "alias": "agent_name"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"}
                                ],
                                "where": "span_attributes[''gen_ai.agent.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.agent.name'']"],
                                "orderBy": "avg_latency DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Agent Performance",
                        "x": 0, "y": 6, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["agent_name", "total_calls", "unique_tools_used", "avg_latency", "p95_latency", "error_rate"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.agent.name'']", "alias": "agent_name"},
                                    {"fn": "count", "alias": "total_calls"},
                                    {"fn": "uniqExact", "field": "span_attributes[''gen_ai.tool.name'']", "alias": "unique_tools_used"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95_latency"},
                                    {"expr": "if(count() > 0, countIf(status_code = ''STATUS_CODE_ERROR'') / count() * 100, 0)", "alias": "error_rate"}
                                ],
                                "where": "span_attributes[''gen_ai.agent.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.agent.name'']"],
                                "orderBy": "total_calls DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            },
            {
                "name": "Tools",
                "icon": "wrench",
                "widgets": [
                    {
                        "type": "bar",
                        "title": "Calls by Tool",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "tool_name",
                            "valueField": "calls",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.tool.name'']", "alias": "tool_name"},
                                    {"fn": "count", "alias": "calls"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] = ''execute_tool'' AND span_attributes[''gen_ai.tool.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.tool.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Avg Latency by Tool",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "tool_name",
                            "valueField": "avg_latency",
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.tool.name'']", "alias": "tool_name"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] = ''execute_tool'' AND span_attributes[''gen_ai.tool.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.tool.name'']"],
                                "orderBy": "avg_latency DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Tool Performance",
                        "x": 0, "y": 4, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["tool_name", "calls", "avg_latency", "p95_latency", "success_rate", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.tool.name'']", "alias": "tool_name"},
                                    {"fn": "count", "alias": "calls"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95_latency"},
                                    {"expr": "if(count() > 0, (count() - countIf(status_code = ''STATUS_CODE_ERROR'')) / count() * 100, 0)", "alias": "success_rate"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] = ''execute_tool'' AND span_attributes[''gen_ai.tool.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.tool.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 50
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Tool Usage by Agent",
                        "x": 0, "y": 9, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["agent_name", "tool_name", "calls", "avg_latency", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.agent.name'']", "alias": "agent_name"},
                                    {"field": "span_attributes[''gen_ai.tool.name'']", "alias": "tool_name"},
                                    {"fn": "count", "alias": "calls"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] = ''execute_tool'' AND span_attributes[''gen_ai.tool.name''] != '''' AND span_attributes[''gen_ai.agent.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.agent.name'']", "span_attributes[''gen_ai.tool.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 50
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Recent Tool Calls",
                        "x": 0, "y": 14, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["tool_name", "agent_name", "duration", "status_code", "timestamp"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.tool.name'']", "alias": "tool_name"},
                                    {"field": "span_attributes[''gen_ai.agent.name'']", "alias": "agent_name"},
                                    {"field": "duration", "alias": "duration"},
                                    {"field": "status_code", "alias": "status_code"},
                                    {"field": "timestamp", "alias": "timestamp"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] = ''execute_tool''",
                                "orderBy": "timestamp DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            },
            {
                "name": "Conversations",
                "icon": "message-square",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "Unique Conversations",
                        "x": 0, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "uniqExact", "field": "span_attributes[''gen_ai.conversation.id'']", "alias": "unique_conversations"}],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'') AND span_attributes[''gen_ai.conversation.id''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Calls per Conversation",
                        "x": 4, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "count", "alias": "total"},
                                    {"fn": "uniqExact", "field": "span_attributes[''gen_ai.conversation.id'']", "alias": "conversations"},
                                    {"expr": "if(conversations > 0, total / conversations, 0)", "alias": "avg_per_conversation"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'') AND span_attributes[''gen_ai.conversation.id''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Conversation Duration",
                        "x": 8, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "avg", "field": "duration", "alias": "avg_duration"}],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'') AND span_attributes[''gen_ai.conversation.id''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Conversations Over Time",
                        "x": 0, "y": 2, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "uniqExact", "field": "span_attributes[''gen_ai.conversation.id'']", "alias": "conversations"}],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'') AND span_attributes[''gen_ai.conversation.id''] != ''''",
                                "interval": "1h"
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Conversations",
                        "x": 0, "y": 6, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["conversation_id", "tool_calls", "unique_tools", "avg_latency", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.conversation.id'']", "alias": "conversation_id"},
                                    {"fn": "count", "alias": "tool_calls"},
                                    {"fn": "uniqExact", "field": "span_attributes[''gen_ai.tool.name'']", "alias": "unique_tools"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "span_attributes[''gen_ai.operation.name''] IN (''execute_tool'', ''invoke_agent'', ''create_agent'') AND span_attributes[''gen_ai.conversation.id''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.conversation.id'']"],
                                "orderBy": "tool_calls DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
WHERE name = 'Agents Dashboard';
