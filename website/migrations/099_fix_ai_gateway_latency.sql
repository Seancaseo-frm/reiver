-- Fix AI Gateway latency metrics:
-- 1. GenAI / LLM (PromQL): change gen_ai.system -> gen_ai.provider.name to match
--    updated gateway emission (OTel semconv alignment).
-- 2. GenAI Dashboard (span SQL): exclude error spans from latency widgets so
--    guardrail-blocked spans (duration_ns=0) don't pollute latency aggregates.

-- ============================================================================
-- 1. GenAI / LLM — PromQL template (replaces migration 098)
-- ============================================================================
UPDATE dashboard_templates
SET template_config = '{
    "variables": [
        {"name": "model", "label": "Model", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.request.model)"},
        {"name": "provider", "label": "Provider", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.provider.name)"},
        {"name": "operation", "label": "Operation", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.operation.name)"}
    ],
    "tabs": [
        {
            "name": "Overview",
            "widgets": [
                {"type": "stat", "title": "Requests", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[$__range]))", "instant": true}}},
                {"type": "stat", "title": "Total Tokens", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[$__range]))", "instant": true}}},
                {"type": "stat", "title": "Avg Latency", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.operation.duration.sum{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[$__range])) / sum(increase(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[$__range]))", "instant": true}, "unit": "s"}},
                {"type": "stat", "title": "Error Rate", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "(sum(increase(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\", error.type!=\"\"}[$__range])) or vector(0)) / sum(increase(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.provider.name=~\"$provider\"}[$__range]))", "instant": true}, "unit": "percentunit", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.01, "color": "orange"}, {"value": 0.05, "color": "red"}]}},
                {"type": "timeseries", "title": "Token Usage (Input vs Output)", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\", gen_ai.token.type=\"input\"}[$__rate_interval]))", "legend_format": "input tokens"}, {"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\", gen_ai.token.type=\"output\"}[$__rate_interval]))", "legend_format": "output tokens"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Avg Operation Duration", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(gen_ai.client.operation.duration.sum{gen_ai.request.model=~\"$model\"}[$__rate_interval])) / sum(rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\"}[$__rate_interval]))"}, "unit": "s"}},
                {"type": "timeseries", "title": "Requests by Model", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{gen_ai.provider.name=~\"$provider\"}[$__rate_interval]))"}, "unit": "reqps"}},
                {"type": "timeseries", "title": "Requests by Operation", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.operation.name) (rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\"}[$__rate_interval]))"}, "unit": "reqps"}}
            ]
        },
        {
            "name": "Tokens & Efficiency",
            "widgets": [
                {"type": "stat", "title": "Input Tokens", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.token.usage{gen_ai.token.type=\"input\", gen_ai.request.model=~\"$model\"}[$__range]))", "instant": true}}},
                {"type": "stat", "title": "Output Tokens", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.token.usage{gen_ai.token.type=\"output\", gen_ai.request.model=~\"$model\"}[$__range]))", "instant": true}}},
                {"type": "stat", "title": "Tokens per Request", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\"}[$__range])) / sum(increase(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\"}[$__range]))", "instant": true}}},
                {"type": "stat", "title": "Output/Input Ratio", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.token.usage{gen_ai.token.type=\"output\", gen_ai.request.model=~\"$model\"}[$__range])) / sum(increase(gen_ai.client.token.usage{gen_ai.token.type=\"input\", gen_ai.request.model=~\"$model\"}[$__range]))", "instant": true}}},
                {"type": "timeseries", "title": "Token Consumption by Model", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.token.usage{gen_ai.provider.name=~\"$provider\"}[$__rate_interval]))"}, "unit": "ops"}},
                {"type": "timeseries", "title": "Token Efficiency (Output/Input Ratio)", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.token.type=\"output\", gen_ai.request.model=~\"$model\"}[$__rate_interval])) / sum(rate(gen_ai.client.token.usage{gen_ai.token.type=\"input\", gen_ai.request.model=~\"$model\"}[$__rate_interval]))"}}},
                {"type": "timeseries", "title": "Avg Tokens per Request by Model", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.token.usage{gen_ai.provider.name=~\"$provider\"}[$__rate_interval])) / sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{gen_ai.provider.name=~\"$provider\"}[$__rate_interval]))"}}},
                {"type": "timeseries", "title": "Duration by Operation", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.operation.name) (rate(gen_ai.client.operation.duration.sum{gen_ai.request.model=~\"$model\"}[$__rate_interval])) / sum by (gen_ai.operation.name) (rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\"}[$__rate_interval]))"}, "unit": "s"}}
            ]
        }
    ]
}'
WHERE name = 'GenAI / LLM';


-- ============================================================================
-- 2. GenAI Dashboard — add error exclusion to latency widgets
-- ============================================================================
-- Only latency-focused widgets need the exclusion; count/token/error widgets
-- should still include error spans in their totals.
UPDATE dashboard_templates
SET template_config = jsonb_set(
    template_config,
    '{tabs,0,widgets,1,config,query,where}',
    '"span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''error.type''] = ''''"'::jsonb
)
WHERE name = 'GenAI Dashboard';

UPDATE dashboard_templates
SET template_config = jsonb_set(
    template_config,
    '{tabs,0,widgets,5,config,query,where}',
    '"span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''error.type''] = ''''"'::jsonb
)
WHERE name = 'GenAI Dashboard';

UPDATE dashboard_templates
SET template_config = jsonb_set(
    template_config,
    '{tabs,1,widgets,1,config,query,where}',
    '"span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.request.model''] != '''' AND span_attributes[''error.type''] = ''''"'::jsonb
)
WHERE name = 'GenAI Dashboard';

UPDATE dashboard_templates
SET template_config = jsonb_set(
    template_config,
    '{tabs,1,widgets,2,config,query,where}',
    '"span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''gen_ai.request.model''] != '''' AND span_attributes[''error.type''] = ''''"'::jsonb
)
WHERE name = 'GenAI Dashboard';

UPDATE dashboard_templates
SET template_config = jsonb_set(
    template_config,
    '{tabs,1,widgets,3,config,query,where}',
    '"span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''error.type''] = ''''"'::jsonb
)
WHERE name = 'GenAI Dashboard';

UPDATE dashboard_templates
SET template_config = jsonb_set(
    template_config,
    '{tabs,4,widgets,1,config,query,where}',
    '"span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''error.type''] = ''''"'::jsonb
)
WHERE name = 'GenAI Dashboard';

UPDATE dashboard_templates
SET template_config = jsonb_set(
    template_config,
    '{tabs,4,widgets,2,config,query,where}',
    '"span_attributes[''gen_ai.provider.name''] != '''' AND span_attributes[''error.type''] = ''''"'::jsonb
)
WHERE name = 'GenAI Dashboard';
