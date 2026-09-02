# Available Tools

The MCP server exposes **6 tools** that cover the entire Reiver platform. Each tool accepts a discriminator field that routes to the right operation, keeping the tool surface small while supporting 100+ distinct operations.

All tools operate within the context of the authenticated project.

## Overview

| Tool | Discriminator | Purpose | Operations |
|------|---------------|---------|------------|
| `search` | `source` | Find resources by text query | 3 |
| `get` | `resource` | Retrieve a specific resource by type + ID | 21 |
| `list` | `resource` | Browse/list resources with optional filters | 27 |
| `analyze` | `analysis` | Metrics, analytics, comparisons, diagnostics | 18 |
| `execute` | `resource` + `action` | Create, update, configure, deploy, test, run | 39 |
| `delete` | `resource` | Remove a resource permanently | 5 |

---

## `search`

Search across different data sources. Set `source` to select the search backend.

```json
{ "source": "llm_requests", "query": "error handling", "model": "<observed-model-id>" }
```

| Source | Description | Key Parameters |
|--------|-------------|----------------|
| `llm_requests` | Text search over LLM prompts and completions | `query`, `limit?`, `model?`, `user_id?`, `session_id?`, `start_time?`, `end_time?` |
| `logs` | Search logs with cross-signal correlation | `query?`, `level?`, `service?`, `trace_id?`, `time_range?`, `start_time?`, `end_time?`, `limit?` |
| `web` | Web search for real-time information | `query`, `max_results?` |

---

## `get`

Retrieve a specific resource by type and ID. Set `resource` to select the resource type.

```json
{ "resource": "trace", "trace_id": "abc123" }
```

| Resource | Description |
|----------|-------------|
| `trace` | Distributed trace by ID |
| `session` | LLM session with conversation metadata |
| `session_requests` | All requests within an LLM session |
| `log` | Single log entry by ID |
| `log_context` | Surrounding log lines for context |
| `exception` | Exception details with stack trace |
| `incident` | Incident details and timeline |
| `incident_errors` | Error events for a specific incident |
| `alert_rule` | Alert rule configuration |
| `dashboard` | Dashboard layout and widgets |
| `health_check` | Health check configuration |
| `health_check_results` | Recent health check probe results |
| `maintenance_window` | Maintenance window details |
| `prompt_config` | Prompt configuration |
| `prompt_version` | Specific prompt version |
| `rollout` | Rollout details |
| `rollout_metrics` | Rollout performance metrics |
| `profile` | Performance profile |
| `project` | Project details |
| `request_scores` | LLM request quality scores |
| `gateway_settings` | LLM gateway settings |

---

## `list`

Browse and list resources with optional filters. Set `resource` to select what to list.

```json
{ "resource": "traces", "status": "error", "sort_by": "start_time", "limit": 10 }
```

| Resource | Description |
|----------|-------------|
| `traces` | Distributed traces. Filters: `status` (error/ok), `service`, `environment`, `service_version`, `http_method`, `http_route`, `search`, `start_time`, `end_time`, `sort_by` (start_time/duration), `sort_order` (asc/desc), `limit` |
| `services` | Monitored services |
| `service_versions` | Service deployment versions |
| `sessions` | LLM sessions |
| `exceptions` | Exception groups |
| `incidents` | Incidents |
| `api_endpoints` | Auto-discovered API endpoints |
| `api_endpoint_errors` | Errors for a specific API endpoint |
| `alert_rules` | Alert rules |
| `alerts` | Triggered alerts |
| `notification_channels` | Notification channels |
| `dashboards` | Dashboards |
| `dashboard_templates` | Dashboard templates |
| `widgets` | Widgets on a dashboard |
| `health_checks` | Health checks |
| `maintenance_windows` | Maintenance windows |
| `integrations` | LLM provider integrations |
| `prompt_configs` | Prompt configurations |
| `prompt_versions` | Prompt versions for a config |
| `rollouts` | Prompt rollouts |
| `profiles` | Performance profiles |
| `service_profiles` | Profiles for a specific service |
| `projects` | Projects |
| `api_keys` | API keys |
| `llm_scores` | LLM quality scores |
| `llm_pricing` | LLM model pricing (internal) |
| `model_catalog` | Current interactive model IDs filtered to the project's enabled provider integrations. Use this live catalogue before configuring routing or explicitly pinning a model. |
| `metric_names` | Available OpenTelemetry metric names. Filters: `prefix?`, `limit?`. Returns name, type, unit, label keys. Use before querying otel_metrics. |

---

## `analyze`

Run analytics, queries, comparisons, and diagnostics. Set `analysis` to select the type.

```json
{ "analysis": "llm_overview", "start_date": "2026-04-01", "end_date": "2026-04-07" }
```

### LLM Metrics

| Analysis | Description |
|----------|-------------|
| `llm_overview` | Gateway overview metrics (requests, latency, errors, costs) |
| `llm_model_metrics` | Per-model LLM metrics breakdown |
| `llm_cost_daily` | Daily LLM cost breakdown |
| `llm_user_metrics` | Per-user LLM usage metrics |

### Custom Data Queries

| Analysis | Description |
|----------|-------------|
| `widget_query` | Custom ClickHouse query on observability data |
| `otel_metrics` | Query OpenTelemetry metrics by name. Params: `metric_name` (required), `from?`, `to?`, `step?`, `time_aggregation?`, `space_aggregation?`, `filters?`, `group_by?`. Use list metric_names first. |

### LLM Testing

| Analysis | Description |
|----------|-------------|
| `playground` | Run a prompt in the LLM playground |
| `compare_models` | Compare up to 5 LLM models side-by-side |

### Service Diagnostics

| Analysis | Description |
|----------|-------------|
| `compare_versions` | Compare two service deployment versions |
| `compare_profiles` | Compare performance profiles |
| `detect_faults` | Detect faulty deployments via anomaly detection |
| `root_cause` | AI-powered root cause analysis for an exception |

### Project & Billing Analytics

| Analysis | Description |
|----------|-------------|
| `project_stats` | Project-level statistics |
| `endpoint_summary` | API endpoint summary metrics |
| `usage` | Overall platform usage |
| `usage_by_project` | Usage broken down by project |
| `usage_forecast` | Forecast future usage and costs |
| `budget_status` | Budget status and spend tracking |

---

## `execute`

Create, update, configure, deploy, test, and run operations. Provide `resource`, `action`, and `params`.

```json
{
  "resource": "alert_rule",
  "action": "create",
  "params": {
    "rule": {
      "name": "High Error Rate",
      "query_config": { "query_type": "metrics", "metric_name": "http.server.errors", "time_aggregation": "count" },
      "threshold": 5.0,
      "threshold_type": "above",
      "notification_channels": ["<channel-uuid>"]
    }
  }
}
```

### Alert Rule Query Types

Alert rules support four query types via `query_config.query_type` (required discriminator):

| Query Type | Use Case | Key Fields |
|------------|----------|------------|
| `metrics` | Infrastructure/APM metrics | `metric_name`, `filters`, `time_aggregation` |
| `log_pattern` | Log-based alerting | `patterns` (array of strings), `log_source` |
| `promql` | Raw PromQL expression | `promql` (string) |
| `llm` | LLM gateway metrics | `metric_name` (with `llm.` prefix: `llm.error_rate`, `llm.latency_p95`, `llm.cost_daily`, `llm.token_usage`, `llm.request_count`), `filters` |

**LLM alert example:**
```json
{
  "resource": "alert_rule",
  "action": "create",
  "params": {
    "rule": {
      "name": "Gateway P95 latency high",
      "query_config": { "query_type": "llm", "metric_name": "llm.latency_p95" },
      "threshold": 30000,
      "threshold_type": "above",
      "notification_channels": ["<channel-uuid>"],
      "eval_window_seconds": 300
    }
  }
}
```

**PromQL alert example:**
```json
{
  "resource": "alert_rule",
  "action": "create",
  "params": {
    "rule": {
      "name": "Service down",
      "query_config": { "query_type": "promql", "promql": "up == 0" },
      "threshold": 1,
      "threshold_type": "above"
    }
  }
}
```

### Resources and Actions

| Resource | Actions |
|----------|---------|
| `project` | `create`, `update`, `create_api_key` |
| `prompt` | `create_config`, `update_config`, `create_version`, `deploy`, `promote`, `complete`, `pause`, `rollback` |
| `gateway` | `update_settings` |
| `integration` | `configure`, `update`, `test`, `create_secret_slot` |
| `dashboard` | `create`, `create_from_template`, `update`, `create_widget`, `update_widget` |
| `alert_rule` | `create`, `update` |
| `health_check` | `create`, `update` |
| `maintenance_window` | `create`, `update` |
| `notification_channel` | `configure`, `update`, `test`, `configure_servicenow` |
| `cloud_integration` | `configure_aws`, `configure_gcp`, `configure_azure`, `configure_oci` |
| `auth_provider` | `configure` |
| `github` | `list_installations`, `list_commits`, `get_commit`, `list_directory`, `read_file`, `search_code`, `link_repo`, `unlink_repo` |
| `exception` | `update_status` |
| `llm_score` | `submit` |
| `session` | `feedback`, `end` |
| `session_profile` | `create`, `update`, `delete` |

### GitHub

The `github` resource delegates to a multi-action handler. The `action` field selects the operation, and `params` contains action-specific fields:

| Action | Parameters | Description |
|--------|-----------|-------------|
| `list_installations` | — | List GitHub App installations available to the organization |
| `list_commits` | `branch?`, `limit?` | List recent commits (default 10, max 30) |
| `get_commit` | `sha` | Get details for a specific commit |
| `list_directory` | `path?`, `ref?` | List files and subdirectories at a path |
| `read_file` | `path`, `ref?` | Read the contents of a file |
| `search_code` | `query` | Search for code by keyword or symbol |
| `link_repo` | `repository_url` | Link the project to a GitHub repository |
| `unlink_repo` | — | Unlink the project from its repository |

---

## `delete`

Remove a resource permanently. Set `resource` to select what to delete. Each resource type has its own ID field.

```json
{ "resource": "alert_rule", "rule_id": "abc123", "confirm": true }
```

| Resource | Description |
|----------|-------------|
| `integration` | Delete an LLM provider integration |
| `alert_rule` | Delete an alert rule |
| `health_check` | Delete a health check |
| `maintenance_window` | Delete a maintenance window |
| `widget` | Delete a dashboard widget |
