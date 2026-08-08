-- Align Agents Dashboard with OTel GenAI semantic conventions:
-- 1. Remove the "Tokens" tab (mcp.token.name does not exist)
-- 2. Replace mcp.tool.name -> gen_ai.tool.name
-- 3. Replace span_name = 'mcp.tool.call' -> span_attributes['gen_ai.operation.name'] = 'execute_tool'
-- 4. Add "Recent Tool Calls" table to the Tools tab

UPDATE dashboard_templates
SET
    description = 'Monitor AI agent activity including MCP tool calls, latency, error rates, and usage breakdown by tool',
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
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool''"
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
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool''"
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
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool''"
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
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool''"
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
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool''",
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
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool''",
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
                                    {"field": "span_attributes[''gen_ai.tool.name'']", "alias": "tool_name"},
                                    {"fn": "count", "alias": "calls"}
                                ],
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool'' AND span_attributes[''gen_ai.tool.name''] != ''''",
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
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool'' AND span_attributes[''gen_ai.tool.name''] != ''''",
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
                            "columns": ["tool_name", "calls", "avg_latency", "p95_latency", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.tool.name'']", "alias": "tool_name"},
                                    {"fn": "count", "alias": "calls"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_latency"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95_latency"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool'' AND span_attributes[''gen_ai.tool.name''] != ''''",
                                "groupBy": ["span_attributes[''gen_ai.tool.name'']"],
                                "orderBy": "calls DESC",
                                "limit": 50
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Recent Tool Calls",
                        "x": 0, "y": 9, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["tool_name", "duration", "status_code", "timestamp"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''gen_ai.tool.name'']", "alias": "tool_name"},
                                    {"field": "duration", "alias": "duration"},
                                    {"field": "status_code", "alias": "status_code"},
                                    {"field": "timestamp", "alias": "timestamp"}
                                ],
                                "where": "service_name = ''reiver-mcp'' AND span_attributes[''gen_ai.operation.name''] = ''execute_tool''",
                                "orderBy": "timestamp DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
WHERE name = 'Agents Dashboard';
