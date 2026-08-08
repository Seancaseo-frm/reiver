-- ============================================================================
-- Prebuilt dashboard templates: Agents Dashboard + GenAI Dashboard
-- Data source: OTel spans (reiver.spans) sent to Watch via OTLP
-- ============================================================================

-- ============================================================================
-- AGENTS DASHBOARD
-- Monitors MCP tool calls from the reiver-mcp service
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Agents Dashboard',
    'Monitor AI agent activity including MCP tool calls, latency, error rates, and usage breakdown by tool and token',
    'agents',
    true,
    20,
    ARRAY['agents', 'mcp', 'tools', 'ai'],
    '{
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
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call''"
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
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call''"
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
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call''"
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
                                "select": [{"fn": "uniqExact", "field": "span_attributes[''mcp.tool.name'']", "alias": "unique_tools"}],
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Tool Calls Over Time",
                        "x": 0, "y": 2, "w": 8, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "calls"}],
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call''",
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
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call''",
                                "interval": "1m"
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
                                    {"field": "span_attributes[''mcp.tool.name'']", "alias": "tool_name"},
                                    {"fn": "count", "alias": "calls"}
                                ],
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call'' AND span_attributes[''mcp.tool.name''] != ''''",
                                "groupBy": ["span_attributes[''mcp.tool.name'']"],
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
                                    {"field": "span_attributes[''mcp.tool.name'']", "alias": "tool_name"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"}
                                ],
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call'' AND span_attributes[''mcp.tool.name''] != ''''",
                                "groupBy": ["span_attributes[''mcp.tool.name'']"],
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
                            "columns": ["tool_name", "calls", "avg_latency", "p95_latency", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''mcp.tool.name'']", "alias": "tool_name"},
                                    {"fn": "count", "alias": "calls"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95_latency"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call'' AND span_attributes[''mcp.tool.name''] != ''''",
                                "groupBy": ["span_attributes[''mcp.tool.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            },
            {
                "name": "Tokens",
                "icon": "key",
                "widgets": [
                    {
                        "type": "bar",
                        "title": "Calls by Agent Token",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "token_name",
                            "valueField": "calls",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''mcp.token.name'']", "alias": "token_name"},
                                    {"fn": "count", "alias": "calls"}
                                ],
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call'' AND span_attributes[''mcp.token.name''] != ''''",
                                "groupBy": ["span_attributes[''mcp.token.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Errors by Agent Token",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "token_name",
                            "valueField": "errors",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''mcp.token.name'']", "alias": "token_name"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call'' AND span_attributes[''mcp.token.name''] != ''''",
                                "groupBy": ["span_attributes[''mcp.token.name'']"],
                                "orderBy": "errors DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Recent Tool Calls",
                        "x": 0, "y": 4, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["tool_name", "token_name", "duration", "status_code", "timestamp"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''mcp.tool.name'']", "alias": "tool_name"},
                                    {"field": "span_attributes[''mcp.token.name'']", "alias": "token_name"},
                                    {"field": "duration", "alias": "duration"},
                                    {"field": "status_code", "alias": "status_code"},
                                    {"field": "timestamp", "alias": "timestamp"}
                                ],
                                "where": "service_name = ''reiver-mcp'' AND span_name = ''mcp.tool.call''",
                                "orderBy": "timestamp DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            }
        ]
    }'
);

-- ============================================================================
-- GENAI DASHBOARD
-- Monitors GenAI/LLM spans following OTel GenAI semantic conventions
-- ============================================================================

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'GenAI Dashboard',
    'Monitor AI and LLM operations with OpenTelemetry GenAI semantic conventions. Track model usage, latency, sessions, and errors across providers.',
    'llm',
    true,
    21,
    ARRAY['llm', 'genai', 'opentelemetry', 'sessions', 'ai'],
    '{
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''"
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''"
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''"
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''"
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''",
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''",
                                "interval": "1m"
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
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND span_attributes[''gen_ai.request.model''] != ''''",
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
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND span_attributes[''gen_ai.request.model''] != ''''",
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
                            "columns": ["model", "provider", "calls", "avg_latency", "p95_latency", "input_tokens", "output_tokens", "errors"],
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
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND span_attributes[''gen_ai.request.model''] != ''''",
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''"
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
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND span_attributes[''gen_ai.conversation.id''] != ''''"
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
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND span_attributes[''gen_ai.conversation.id''] != ''''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Total Tokens",
                        "x": 8, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "format": "number",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"expr": "sum(toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])) + sum(toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens'']))", "alias": "total_tokens"}
                                ],
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''"
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
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND span_attributes[''gen_ai.conversation.id''] != ''''",
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
                            "columns": ["session_id", "spans", "avg_duration", "input_tokens", "output_tokens", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.conversation.id'']", "alias": "session_id"},
                                    {"fn": "count", "alias": "spans"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_duration"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.input_tokens''])", "alias": "input_tokens"},
                                    {"fn": "sum", "field": "toUInt32OrZero(span_attributes[''gen_ai.usage.output_tokens''])", "alias": "output_tokens"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND span_attributes[''gen_ai.conversation.id''] != ''''",
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''"
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''"
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
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND status_code = ''STATUS_CODE_ERROR''"
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
                                "where": "span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != ''''",
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
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND status_code = ''STATUS_CODE_ERROR'' AND span_attributes[''gen_ai.request.model''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.request.model'']"],
                                "orderBy": "errors DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Errors by Operation",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "operation",
                            "valueField": "errors",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.operation.name'']", "alias": "operation"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "(span_attributes[''gen_ai.provider.name''] != '''' OR span_attributes[''gen_ai.system''] != '''') AND status_code = ''STATUS_CODE_ERROR'' AND span_attributes[''gen_ai.operation.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.operation.name'']"],
                                "orderBy": "errors DESC",
                                "limit": 20
                            }
                        }
                    }
                ]
            }
        ]
    }'
);
