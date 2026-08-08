-- Fix GenAI / LLM dashboard template:
-- 1. Use $__range for stat panels (shows totals over dashboard time range)
-- 2. Use $__rate_interval for time series panels
-- 3. Fix provider label: gen_ai.provider.name -> gen_ai.system (matches actual data)
-- 4. Fix token metric: gen_ai.client.token.usage.sum -> gen_ai.client.token.usage (our counter)
-- 5. Replace histogram_quantile with avg duration (delta temporality compat)

UPDATE dashboard_templates
SET template_config = '{
    "variables": [
        {"name": "model", "label": "Model", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.request.model)"},
        {"name": "system", "label": "Provider", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.system)"},
        {"name": "operation", "label": "Operation", "type": "query", "query": "label_values(gen_ai.client.operation.duration.count, gen_ai.operation.name)"}
    ],
    "tabs": [
        {
            "name": "Overview",
            "widgets": [
                {"type": "stat", "title": "Requests", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[$__range]))", "instant": true}}},
                {"type": "stat", "title": "Total Tokens", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[$__range]))", "instant": true}}},
                {"type": "stat", "title": "Avg Latency", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(increase(gen_ai.client.operation.duration.sum{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[$__range])) / sum(increase(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[$__range]))", "instant": true}, "unit": "s"}},
                {"type": "stat", "title": "Error Rate", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "(sum(increase(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\", error.type!=\"\"}[$__range])) or vector(0)) / sum(increase(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\", gen_ai.system=~\"$system\"}[$__range]))", "instant": true}, "unit": "percentunit", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.01, "color": "orange"}, {"value": 0.05, "color": "red"}]}},
                {"type": "timeseries", "title": "Token Usage (Input vs Output)", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"queries": [{"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\", gen_ai.token.type=\"input\"}[$__rate_interval]))", "legend_format": "input tokens"}, {"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.request.model=~\"$model\", gen_ai.token.type=\"output\"}[$__rate_interval]))", "legend_format": "output tokens"}]}, "unit": "ops"}},
                {"type": "timeseries", "title": "Avg Operation Duration", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(gen_ai.client.operation.duration.sum{gen_ai.request.model=~\"$model\"}[$__rate_interval])) / sum(rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\"}[$__rate_interval]))"}, "unit": "s"}},
                {"type": "timeseries", "title": "Requests by Model", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{gen_ai.system=~\"$system\"}[$__rate_interval]))"}, "unit": "reqps"}},
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
                {"type": "timeseries", "title": "Token Consumption by Model", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.token.usage{gen_ai.system=~\"$system\"}[$__rate_interval]))"}, "unit": "ops"}},
                {"type": "timeseries", "title": "Token Efficiency (Output/Input Ratio)", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum(rate(gen_ai.client.token.usage{gen_ai.token.type=\"output\", gen_ai.request.model=~\"$model\"}[$__rate_interval])) / sum(rate(gen_ai.client.token.usage{gen_ai.token.type=\"input\", gen_ai.request.model=~\"$model\"}[$__rate_interval]))"}}},
                {"type": "timeseries", "title": "Avg Tokens per Request by Model", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.request.model) (rate(gen_ai.client.token.usage{gen_ai.system=~\"$system\"}[$__rate_interval])) / sum by (gen_ai.request.model) (rate(gen_ai.client.operation.duration.count{gen_ai.system=~\"$system\"}[$__rate_interval]))"}}},
                {"type": "timeseries", "title": "Duration by Operation", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (gen_ai.operation.name) (rate(gen_ai.client.operation.duration.sum{gen_ai.request.model=~\"$model\"}[$__rate_interval])) / sum by (gen_ai.operation.name) (rate(gen_ai.client.operation.duration.count{gen_ai.request.model=~\"$model\"}[$__rate_interval]))"}, "unit": "s"}}
            ]
        }
    ]
}'
WHERE name = 'GenAI / LLM';
