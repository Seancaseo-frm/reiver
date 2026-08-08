-- Fix HTTP Service template:
-- 1. Replace non-existent "Response Body Size (avg)" with "Avg Request Duration"
-- 2. Add $http_route filter to "Slowest Routes (P95)" top_list widget

UPDATE dashboard_templates
SET template_config = '{
    "variables": [
        {"name": "service", "label": "Service", "type": "service_select"},
        {"name": "http_route", "label": "Route", "type": "query", "query": "label_values(http.server.request.duration.count, http.route)"}
    ],
    "tabs": [
        {
            "name": "Overview",
            "icon": "activity",
            "widgets": [
                {"type": "stat", "title": "Request Rate", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))", "instant": true}, "unit": "reqps"}},
                {"type": "stat", "title": "Error Rate", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "(sum(rate(http.server.request.duration.count{http.response.status_code=~\"5..\", http.route=~\"$http_route\"}[5m])) or vector(0)) / sum(rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))", "instant": true}, "format": "percentage", "unit": "percentunit", "thresholds": [{"value": 0, "color": "green"}, {"value": 0.01, "color": "orange"}, {"value": 0.05, "color": "red"}]}},
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
            "icon": "git-branch",
            "widgets": [
                {"type": "top_list", "title": "Slowest Routes (P95)", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "histogram_quantile(0.95, sum by (http.route, le) (rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])))", "instant": true}, "unit": "s"}},
                {"type": "top_list", "title": "Highest Error Rate Routes", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "(sum by (http.route) (rate(http.server.request.duration.count{http.response.status_code=~\"5..\"}[5m])) or vector(0)) / sum by (http.route) (rate(http.server.request.duration.count[5m]))", "instant": true}, "unit": "percentunit"}},
                {"type": "timeseries", "title": "Request Rate by Route", "x": 0, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (http.route) (rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))"}, "unit": "reqps"}},
                {"type": "timeseries", "title": "Latency by Route (P95)", "x": 6, "y": 4, "w": 6, "h": 4, "config": {"query": {"promql": "histogram_quantile(0.95, sum by (http.route, le) (rate(http.server.request.duration_bucket{http.route=~\"$http_route\"}[5m])))"}, "unit": "s"}}
            ]
        },
        {
            "name": "Throughput",
            "icon": "bar-chart-2",
            "widgets": [
                {"type": "timeseries", "title": "Request Body Size (avg)", "x": 0, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "rate(http.server.request.body.size.sum{http.route=~\"$http_route\"}[5m]) / rate(http.server.request.body.size.count{http.route=~\"$http_route\"}[5m])"}, "unit": "bytes"}},
                {"type": "timeseries", "title": "Avg Request Duration", "x": 6, "y": 0, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (http.route) (rate(http.server.request.duration.sum{http.route=~\"$http_route\"}[5m])) / sum by (http.route) (rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))"}, "unit": "s"}},
                {"type": "timeseries", "title": "Availability SLI", "x": 0, "y": 4, "w": 12, "h": 4, "config": {"query": {"promql": "1 - (sum(rate(http.server.request.duration.count{http.response.status_code=~\"5..\", http.route=~\"$http_route\"}[5m])) or vector(0)) / sum(rate(http.server.request.duration.count{http.route=~\"$http_route\"}[5m]))"}, "unit": "percentunit"}}
            ]
        }
    ]
}'::jsonb
WHERE name = 'HTTP Service';
