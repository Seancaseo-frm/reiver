# Available Tools

The MCP server provides 5 tools that cover the entire Reiver platform. Each tool uses a discriminator field to route to the correct operation.

## search

Search across different data sources. Set `source` to select the backend.

- `llm_requests` — text search over LLM prompts and completions. Filters: `model`, `user_id`, `session_id`, `start_time`, `end_time`.
- `logs` — search ingested logs. Filters: `query`, `level`, `service`, `trace_id`, `time_range`, `start_time`, `end_time`, `attributes`.
- `web` — web search for real-time information.

Example: `{"source": "logs", "query": "timeout", "level": "error", "time_range": "24h"}`

## get

Retrieve a specific resource by type and ID. Set `resource` to select the type.

Resources: `trace`, `session`, `session_requests`, `log`, `log_context`, `exception`, `incident`, `incident_errors`, `alert_rule`, `dashboard`, `health_check`, `health_check_results`, `maintenance_window`, `prompt_config`, `prompt_version`, `rollout`, `rollout_metrics`, `profile`, `project`, `request_scores`, `gateway_settings`, `session_profile_filter_fields`, `attachment`, `a2a_task`.

Example: `{"resource": "trace", "trace_id": "abc123"}`
Example (A2A): `{"resource": "a2a_task", "task_id": "<task-id>"}`

## list

Browse and list resources with optional filters. Set `resource` to select what to list.

Resources: `traces`, `services`, `service_versions`, `sessions`, `session_profiles`, `exceptions`, `incidents`, `api_endpoints`, `api_endpoint_errors`, `alert_rules`, `alerts`, `notification_channels`, `dashboards`, `dashboard_templates`, `widgets`, `health_checks`, `maintenance_windows`, `integrations`, `prompt_configs`, `prompt_versions`, `rollouts`, `profiles`, `service_profiles`, `projects`, `api_keys`, `llm_scores`, `llm_pricing`, `metric_names`, `trace_attribute_keys`, `trace_attribute_values`, `log_attribute_keys`, `log_attribute_values`, `a2a_agents`, `a2a_tasks`.

Example: `{"resource": "traces", "status": "error", "limit": 10}`
Example (A2A): `{"resource": "a2a_agents", "query": "billing"}`

## analyze

Run analytics, queries, comparisons, and diagnostics. Set `analysis` to select the type.

### LLM metrics
- `llm_overview` — gateway overview (requests, latency, errors, costs)
- `llm_model_metrics` — per-model breakdown
- `llm_cost_daily` — daily cost breakdown
- `llm_user_metrics` — per-user usage

### Observability data queries
- `widget_query` — execute a PromQL query against the project's metrics. Both Prometheus-style names (e.g. `http_request_duration_seconds`) and OpenTelemetry-style names (e.g. `http.server.request.duration`) are supported — always discover available names with `list` resource `metric_names` first. Use this to test queries before creating dashboard widgets, or for ad-hoc analysis. Params: `query` (with `promql`, optional `legend_format`, `instant`, `queries`), `time_range`.
- `dashboard_snapshot` — execute all widgets on a dashboard and return their data as structured JSON, equivalent to viewing the dashboard in the UI. Start here when the user asks about application health or monitoring status. Params: `dashboard_id`, `time_range`, optional `variables`.
- `otel_metrics` — query OpenTelemetry metrics by name (use `list metric_names` first)

### LLM testing
- `playground` — run a prompt in the LLM playground
- `compare_models` — compare up to 5 models side-by-side

### Service diagnostics
- `compare_versions` — compare two deployment versions
- `compare_profiles` — compare performance profiles
- `detect_faults` — detect faulty deployments via anomaly detection
- `root_cause` — AI-powered root cause analysis

### Project & billing
- `project_stats` — project-level statistics
- `endpoint_summary` — API endpoint summary metrics
- `usage` — overall platform usage
- `usage_by_project` — per-project breakdown
- `usage_forecast` — forecast future usage/costs
- `budget_status` — budget status and spend tracking

## execute

Create, update, configure, deploy, test, and run operations. Provide `resource`, `action`, and `params`.

Resources and actions:
- `project` — create, update, create_api_key
- `prompt` — create_config, update_config, create_version, deploy, promote, complete, pause, rollback
- `dashboard` — create, create_from_template, update, create_widget, update_widget
- `alert_rule` — create, update
- `integration` — configure, update, test, create_secret_slot
- `health_check` — create, update
- `maintenance_window` — create, update
- `notification_channel` — configure, update, test, configure_servicenow
- `cloud_integration` — configure_aws, configure_gcp, configure_azure, configure_oci
- `auth_provider` — configure
- `github` — list_installations, list_commits, get_commit, list_directory, read_file, search_code, link_repo, unlink_repo
- `exception` — update_status
- `llm_score` — submit
- `session` — feedback, end
- `session_profile` — create, update, delete
- `a2a` — register_agent
- `gateway` — update_settings

Example: `{"resource": "alert_rule", "action": "create", "params": {"name": "High Error Rate", ...}}`
Example (A2A): `{"resource": "a2a", "action": "register_agent", "params": {"name": "my-agent", "endpoint_url": "https://example.com/a2a"}}`
