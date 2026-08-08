//! Linear connector for the data warehouse.
//!
//! Syncs Linear data (issues, projects, teams, etc.) via the GraphQL API.

use super::builders::ColumnBuilders;
use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::crypto::SecretString;
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

const LINEAR_API_URL: &str = "https://api.linear.app/graphql";
const PAGE_SIZE: u32 = 50;
const BATCH_THRESHOLD: usize = 1_000;
const MAX_TOTAL_ROWS: usize = 1_000_000;

#[derive(Clone)]
pub struct LinearConfig {
    pub api_key: SecretString,
}

impl std::fmt::Debug for LinearConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinearConfig")
            .field("api_key", &"***REDACTED***")
            .finish()
    }
}

impl LinearConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: SecretString::new(api_key),
        }
    }
}

pub struct LinearConnector {
    config: LinearConfig,
    client: reqwest::Client,
}

impl LinearConnector {
    const TABLES: &'static [&'static str] = &[
        "issues",
        "projects",
        "teams",
        "users",
        "workflow_states",
        "labels",
        "cycles",
        "comments",
        "project_updates",
        "initiatives",
        "documents",
    ];

    pub fn new(config: LinearConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    // ========================================================================
    // Schema Definitions
    // ========================================================================

    fn get_table_schema(table: &str) -> Option<TableSchema> {
        let columns = match table {
            "issues" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("identifier", ColumnType::String, false)
                    .with_description("Issue identifier (e.g. LIN-123)"),
                ColumnSchema::new("title", ColumnType::String, false)
                    .with_description("Issue title"),
                ColumnSchema::new("description", ColumnType::String, true)
                    .with_description("Issue description in markdown"),
                ColumnSchema::new("priority", ColumnType::Int32, false)
                    .with_description("Priority (0=none, 1=urgent, 2=high, 3=medium, 4=low)"),
                ColumnSchema::new("estimate", ColumnType::Float32, true)
                    .with_description("Story point estimate"),
                ColumnSchema::new("due_date", ColumnType::String, true)
                    .with_description("Due date (YYYY-MM-DD)"),
                ColumnSchema::new("state_id", ColumnType::String, true)
                    .with_description("Workflow state ID"),
                ColumnSchema::new("state_name", ColumnType::String, true)
                    .with_description("Workflow state name"),
                ColumnSchema::new("assignee_id", ColumnType::String, true)
                    .with_description("Assignee user ID"),
                ColumnSchema::new("team_id", ColumnType::String, true)
                    .with_description("Team ID"),
                ColumnSchema::new("project_id", ColumnType::String, true)
                    .with_description("Project ID"),
                ColumnSchema::new("label_ids", ColumnType::String, true)
                    .with_description("Comma-separated label IDs"),
                ColumnSchema::new("cycle_id", ColumnType::String, true)
                    .with_description("Cycle ID"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
                ColumnSchema::new("archived_at", ColumnType::Timestamp, true)
                    .with_description("Archive time")
                    .with_timezone("UTC"),
            ],
            "projects" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("name", ColumnType::String, false)
                    .with_description("Project name"),
                ColumnSchema::new("description", ColumnType::String, true)
                    .with_description("Project description"),
                ColumnSchema::new("state", ColumnType::String, false)
                    .with_description("Project state (planned, started, paused, completed, canceled)"),
                ColumnSchema::new("progress", ColumnType::Float64, false)
                    .with_description("Completion progress (0.0 to 1.0)"),
                ColumnSchema::new("start_date", ColumnType::String, true)
                    .with_description("Start date (YYYY-MM-DD)"),
                ColumnSchema::new("target_date", ColumnType::String, true)
                    .with_description("Target completion date (YYYY-MM-DD)"),
                ColumnSchema::new("lead_id", ColumnType::String, true)
                    .with_description("Project lead user ID"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            "teams" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("name", ColumnType::String, false)
                    .with_description("Team name"),
                ColumnSchema::new("key", ColumnType::String, false)
                    .with_description("Team key (prefix for issue identifiers)"),
                ColumnSchema::new("description", ColumnType::String, true)
                    .with_description("Team description"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            "users" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("name", ColumnType::String, false)
                    .with_description("User full name"),
                ColumnSchema::new("email", ColumnType::String, true)
                    .with_description("User email address"),
                ColumnSchema::new("display_name", ColumnType::String, false)
                    .with_description("Display name"),
                ColumnSchema::new("active", ColumnType::Boolean, false)
                    .with_description("Whether the user is active"),
                ColumnSchema::new("admin", ColumnType::Boolean, false)
                    .with_description("Whether the user is an admin"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            "workflow_states" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("name", ColumnType::String, false)
                    .with_description("State name"),
                ColumnSchema::new("type", ColumnType::String, false)
                    .with_description("State type (triage, backlog, unstarted, started, completed, canceled)"),
                ColumnSchema::new("color", ColumnType::String, false)
                    .with_description("State color hex"),
                ColumnSchema::new("position", ColumnType::Float64, false)
                    .with_description("Display position"),
                ColumnSchema::new("team_id", ColumnType::String, false)
                    .with_description("Team ID"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            "labels" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("name", ColumnType::String, false)
                    .with_description("Label name"),
                ColumnSchema::new("color", ColumnType::String, false)
                    .with_description("Label color hex"),
                ColumnSchema::new("description", ColumnType::String, true)
                    .with_description("Label description"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            "cycles" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Cycle name"),
                ColumnSchema::new("number", ColumnType::Int32, false)
                    .with_description("Cycle number"),
                ColumnSchema::new("starts_at", ColumnType::Timestamp, false)
                    .with_description("Cycle start time")
                    .with_timezone("UTC"),
                ColumnSchema::new("ends_at", ColumnType::Timestamp, false)
                    .with_description("Cycle end time")
                    .with_timezone("UTC"),
                ColumnSchema::new("completed_at", ColumnType::Timestamp, true)
                    .with_description("Completion time")
                    .with_timezone("UTC"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            "comments" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("body", ColumnType::String, true)
                    .with_description("Comment body in markdown"),
                ColumnSchema::new("issue_id", ColumnType::String, true)
                    .with_description("Parent issue ID"),
                ColumnSchema::new("user_id", ColumnType::String, true)
                    .with_description("Author user ID"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            "project_updates" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("body", ColumnType::String, true)
                    .with_description("Update body in markdown"),
                ColumnSchema::new("health", ColumnType::String, true)
                    .with_description("Project health (onTrack, atRisk, offTrack)"),
                ColumnSchema::new("user_id", ColumnType::String, true)
                    .with_description("Author user ID"),
                ColumnSchema::new("project_id", ColumnType::String, true)
                    .with_description("Parent project ID"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            "initiatives" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("name", ColumnType::String, false)
                    .with_description("Initiative name"),
                ColumnSchema::new("description", ColumnType::String, true)
                    .with_description("Initiative description"),
                ColumnSchema::new("status", ColumnType::String, false)
                    .with_description("Initiative status"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            "documents" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier"),
                ColumnSchema::new("title", ColumnType::String, false)
                    .with_description("Document title"),
                ColumnSchema::new("content", ColumnType::String, true)
                    .with_description("Document content in markdown"),
                ColumnSchema::new("project_id", ColumnType::String, true)
                    .with_description("Associated project ID"),
                ColumnSchema::new("creator_id", ColumnType::String, true)
                    .with_description("Creator user ID"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_description("Creation time")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false)
                    .with_description("Last update time")
                    .with_timezone("UTC"),
            ],
            _ => return None,
        };

        Some(TableSchema { columns })
    }

    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), col.nullable))
            .collect();
        Schema::new(fields)
    }

    // ========================================================================
    // GraphQL Queries
    // ========================================================================

    /// Returns the GraphQL root field and node field list for a given table.
    fn graphql_fields(table: &str) -> (&'static str, &'static str) {
        match table {
            "issues" => ("issues", "id identifier title description priority estimate dueDate state { id name } assignee { id } team { id } project { id } labels { nodes { id } } cycle { id } createdAt updatedAt archivedAt"),
            "projects" => ("projects", "id name description state progress startDate targetDate lead { id } createdAt updatedAt"),
            "teams" => ("teams", "id name key description createdAt updatedAt"),
            "users" => ("users", "id name email displayName active admin createdAt updatedAt"),
            "workflow_states" => ("workflowStates", "id name type color position team { id } createdAt updatedAt"),
            "labels" => ("issueLabels", "id name color description createdAt updatedAt"),
            "cycles" => ("cycles", "id name number startsAt endsAt completedAt createdAt updatedAt"),
            "comments" => ("comments", "id body issue { id } user { id } createdAt updatedAt"),
            "project_updates" => ("projectUpdates", "id body health user { id } project { id } createdAt updatedAt"),
            "initiatives" => ("initiatives", "id name description status createdAt updatedAt"),
            "documents" => ("documents", "id title content project { id } creator { id } createdAt updatedAt"),
            _ => ("", ""),
        }
    }

    /// Maps table names to the GraphQL filter argument name.
    fn graphql_filter_type(table: &str) -> &'static str {
        match table {
            "issues" => "IssueFilter",
            "projects" => "ProjectFilter",
            "teams" => "TeamFilter",
            "users" => "UserFilter",
            "workflow_states" => "WorkflowStateFilter",
            "labels" => "IssueLabelFilter",
            "cycles" => "CycleFilter",
            "comments" => "CommentFilter",
            "project_updates" => "ProjectUpdateFilter",
            "initiatives" => "InitiativeFilter",
            "documents" => "DocumentFilter",
            _ => "IssueFilter",
        }
    }

    fn build_query(table: &str, has_filter: bool) -> String {
        let (root_field, fields) = Self::graphql_fields(table);
        let filter_type = Self::graphql_filter_type(table);

        let var_decl = if has_filter {
            format!("$first: Int!, $after: String, $filter: {filter_type}")
        } else {
            "$first: Int!, $after: String".to_string()
        };

        let args = if has_filter {
            "first: $first, after: $after, filter: $filter, orderBy: updatedAt, includeArchived: true"
        } else {
            "first: $first, after: $after, includeArchived: true"
        };

        format!(
            "query Fetch({var_decl}) {{ {root_field}({args}) {{ nodes {{ {fields} }} pageInfo {{ hasNextPage endCursor }} }} }}"
        )
    }

    // ========================================================================
    // HTTP / GraphQL Request
    // ========================================================================

    async fn graphql_request(
        &self,
        query: &str,
        variables: serde_json::Value,
    ) -> ConnectorResult<serde_json::Value> {
        let body = serde_json::json!({
            "query": query,
            "variables": variables,
        });

        let response = self
            .client
            .post(LINEAR_API_URL)
            .header("Content-Type", "application/json")
            .header("Authorization", self.config.api_key.expose())
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                ConnectorError::Network(format!("Failed to connect to Linear API: {}", e))
            })?;

        if response.status() == 401 {
            return Err(ConnectorError::Authentication(
                "Invalid Linear API key".to_string(),
            ));
        }

        if response.status() == 429 {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(60);
            return Err(ConnectorError::RateLimited {
                retry_after_secs: retry_after,
            });
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ConnectorError::Internal(format!(
                "Linear API error ({}): {}",
                status, body
            )));
        }

        let json: serde_json::Value = response.json().await.map_err(|e| {
            ConnectorError::Internal(format!("Failed to parse Linear response: {}", e))
        })?;

        if let Some(errors) = json.get("errors") {
            if let Some(arr) = errors.as_array() {
                if !arr.is_empty() {
                    let msg = arr
                        .iter()
                        .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(ConnectorError::Internal(format!(
                        "Linear GraphQL errors: {}",
                        msg
                    )));
                }
            }
        }

        json.get("data")
            .cloned()
            .ok_or_else(|| ConnectorError::Internal("Linear response missing 'data' field".to_string()))
    }

    // ========================================================================
    // Timestamp Parsing
    // ========================================================================

    fn parse_linear_timestamp(val: &serde_json::Value) -> Option<i64> {
        val.as_str().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.timestamp_micros())
        })
    }

    // ========================================================================
    // Object -> Arrow Builder
    // ========================================================================

    fn append_linear_object(
        obj: &serde_json::Value,
        schema: &TableSchema,
        table: &str,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            if table == "issues" && col.name == "label_ids" {
                let label_ids = Self::extract_label_ids(obj);
                builders.builder(i).append_string(label_ids.as_deref());
                continue;
            }
            match col.data_type {
                ColumnType::Timestamp => {
                    let ts = Self::resolve_field(obj, &col.name, table)
                        .and_then(|v| Self::parse_linear_timestamp(v));
                    builders.builder(i).append_timestamp(ts);
                }
                _ => {
                    let val = Self::resolve_field(obj, &col.name, table);
                    builders.builder(i).append_json_value(val);
                }
            }
        }
        builders.row_complete();
    }

    /// Resolves a schema column name to the corresponding value in a GraphQL
    /// response node, handling nested objects and naming conventions.
    fn resolve_field<'a>(
        obj: &'a serde_json::Value,
        column: &str,
        table: &str,
    ) -> Option<&'a serde_json::Value> {
        match (table, column) {
            // Nested object -> ID extractions
            ("issues", "state_id") => obj.get("state").and_then(|s| s.get("id")),
            ("issues", "state_name") => obj.get("state").and_then(|s| s.get("name")),
            ("issues", "assignee_id") => obj.get("assignee").and_then(|a| a.get("id")),
            ("issues", "team_id") => obj.get("team").and_then(|t| t.get("id")),
            ("issues", "project_id") => obj.get("project").and_then(|p| p.get("id")),
            ("issues", "cycle_id") => obj.get("cycle").and_then(|c| c.get("id")),
            ("issues", "label_ids") => {
                // Serialized as comma-separated string from labels.nodes[].id
                return None; // handled specially below
            }
            ("issues", "due_date") => obj.get("dueDate"),
            ("projects", "start_date") => obj.get("startDate"),
            ("projects", "target_date") => obj.get("targetDate"),
            ("projects", "lead_id") => obj.get("lead").and_then(|l| l.get("id")),
            ("workflow_states", "team_id") => obj.get("team").and_then(|t| t.get("id")),
            ("comments", "issue_id") => obj.get("issue").and_then(|i| i.get("id")),
            ("comments", "user_id") => obj.get("user").and_then(|u| u.get("id")),
            ("project_updates", "user_id") => obj.get("user").and_then(|u| u.get("id")),
            ("project_updates", "project_id") => obj.get("project").and_then(|p| p.get("id")),
            ("documents", "project_id") => obj.get("project").and_then(|p| p.get("id")),
            ("documents", "creator_id") => obj.get("creator").and_then(|c| c.get("id")),
            // snake_case -> camelCase mapping for timestamps
            (_, "created_at") => obj.get("createdAt"),
            (_, "updated_at") => obj.get("updatedAt"),
            (_, "archived_at") => obj.get("archivedAt"),
            (_, "starts_at") => obj.get("startsAt"),
            (_, "ends_at") => obj.get("endsAt"),
            (_, "completed_at") => obj.get("completedAt"),
            (_, "display_name") => obj.get("displayName"),
            // Direct field access
            (_, name) => obj.get(name),
        }
    }

    /// Builds the comma-separated label IDs string for an issue node.
    fn extract_label_ids(obj: &serde_json::Value) -> Option<String> {
        let nodes = obj.get("labels")?.get("nodes")?.as_array()?;
        if nodes.is_empty() {
            return None;
        }
        let ids: Vec<&str> = nodes
            .iter()
            .filter_map(|n| n.get("id").and_then(|id| id.as_str()))
            .collect();
        if ids.is_empty() {
            None
        } else {
            Some(ids.join(","))
        }
    }

    // ========================================================================
    // Predicate Pushdown
    // ========================================================================

    fn predicates_to_graphql_filter(
        predicates: &[crate::warehouse::query::predicate_pushdown::Predicate],
    ) -> Option<serde_json::Value> {
        use crate::warehouse::query::predicate_pushdown::Predicate;

        let mut filter = serde_json::Map::new();

        for pred in predicates {
            match pred {
                Predicate::Equals { column, value } => {
                    let gql_field = Self::column_to_graphql_field(column);
                    let mut cmp = serde_json::Map::new();
                    cmp.insert("eq".to_string(), serde_json::Value::String(value.to_string()));
                    Self::merge_filter_field(&mut filter, gql_field, cmp);
                }
                Predicate::In { column, values } => {
                    let gql_field = Self::column_to_graphql_field(column);
                    let arr: Vec<serde_json::Value> = values
                        .iter()
                        .map(|v| serde_json::Value::String(v.to_string()))
                        .collect();
                    let mut cmp = serde_json::Map::new();
                    cmp.insert("in".to_string(), serde_json::Value::Array(arr));
                    Self::merge_filter_field(&mut filter, gql_field, cmp);
                }
                Predicate::GreaterThan {
                    column,
                    value,
                    inclusive,
                } => {
                    let gql_field = Self::column_to_graphql_field(column);
                    let op = if *inclusive { "gte" } else { "gt" };
                    let mut cmp = serde_json::Map::new();
                    cmp.insert(op.to_string(), serde_json::Value::String(value.to_string()));
                    Self::merge_filter_field(&mut filter, gql_field, cmp);
                }
                Predicate::LessThan {
                    column,
                    value,
                    inclusive,
                } => {
                    let gql_field = Self::column_to_graphql_field(column);
                    let op = if *inclusive { "lte" } else { "lt" };
                    let mut cmp = serde_json::Map::new();
                    cmp.insert(op.to_string(), serde_json::Value::String(value.to_string()));
                    Self::merge_filter_field(&mut filter, gql_field, cmp);
                }
                Predicate::Between { column, low, high } => {
                    let gql_field = Self::column_to_graphql_field(column);
                    let mut cmp = serde_json::Map::new();
                    cmp.insert("gte".to_string(), serde_json::Value::String(low.to_string()));
                    cmp.insert("lte".to_string(), serde_json::Value::String(high.to_string()));
                    Self::merge_filter_field(&mut filter, gql_field, cmp);
                }
                _ => {}
            }
        }

        if filter.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(filter))
        }
    }

    fn column_to_graphql_field(column: &str) -> String {
        match column {
            "created_at" => "createdAt".to_string(),
            "updated_at" => "updatedAt".to_string(),
            "archived_at" => "archivedAt".to_string(),
            "starts_at" => "startsAt".to_string(),
            "ends_at" => "endsAt".to_string(),
            "completed_at" => "completedAt".to_string(),
            "due_date" => "dueDate".to_string(),
            "start_date" => "startDate".to_string(),
            "target_date" => "targetDate".to_string(),
            "display_name" => "displayName".to_string(),
            "state_id" => "state".to_string(),
            "assignee_id" => "assignee".to_string(),
            "team_id" => "team".to_string(),
            "project_id" => "project".to_string(),
            "cycle_id" => "cycle".to_string(),
            other => other.to_string(),
        }
    }

    fn is_date_field(field: &str) -> bool {
        matches!(
            field,
            "createdAt"
                | "updatedAt"
                | "archivedAt"
                | "startsAt"
                | "endsAt"
                | "completedAt"
                | "dueDate"
                | "startDate"
                | "targetDate"
        )
    }

    /// Merge comparator entries into a filter field, preserving existing
    /// comparators when multiple predicates target the same column
    /// (e.g. `createdAt >= X AND createdAt < Y`).
    fn merge_filter_field(
        filter: &mut serde_json::Map<String, serde_json::Value>,
        field: String,
        comparators: serde_json::Map<String, serde_json::Value>,
    ) {
        if let Some(serde_json::Value::Object(existing)) = filter.get_mut(&field) {
            existing.extend(comparators);
        } else {
            filter.insert(field, serde_json::Value::Object(comparators));
        }
    }

    // ========================================================================
    // Paginated Fetch
    // ========================================================================

    async fn fetch_paginated(
        &self,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        filter: Option<serde_json::Value>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let (root_field, _) = Self::graphql_fields(table);
        let query = Self::build_query(table, filter.is_some());

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut cursor: Option<String> = None;
        let mut total_rows: usize = 0;

        loop {
            let mut variables = serde_json::json!({
                "first": PAGE_SIZE,
            });
            if let Some(ref c) = cursor {
                variables["after"] = serde_json::Value::String(c.clone());
            }
            if let Some(ref f) = filter {
                variables["filter"] = f.clone();
            }

            let data = self.graphql_request(&query, variables).await?;

            let connection = data.get(root_field).ok_or_else(|| {
                ConnectorError::Internal(format!(
                    "Linear response missing '{}' field",
                    root_field
                ))
            })?;

            let nodes = connection
                .get("nodes")
                .and_then(|n| n.as_array())
                .ok_or_else(|| {
                    ConnectorError::Internal("Linear response missing 'nodes' array".to_string())
                })?;

            if nodes.is_empty() {
                break;
            }

            for node in nodes {
                Self::append_linear_object(node, schema, table, &mut builders);
            }
            total_rows += nodes.len();

            if builders.len() >= BATCH_THRESHOLD {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            let has_next = connection
                .get("pageInfo")
                .and_then(|pi| pi.get("hasNextPage"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if !has_next {
                break;
            }

            cursor = connection
                .get("pageInfo")
                .and_then(|pi| pi.get("endCursor"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if total_rows >= MAX_TOTAL_ROWS {
                tracing::warn!(
                    table = table,
                    count = total_rows,
                    "Linear sync reached safety limit"
                );
                break;
            }
        }

        if builders.len() > 0 {
            let batch = builders.finish(arrow_schema.clone())?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }

        Ok(batches)
    }
}

// ============================================================================
// Connector Trait
// ============================================================================

#[async_trait]
impl Connector for LinearConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Linear
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        Ok(Self::TABLES
            .iter()
            .filter_map(|&table| {
                Self::get_table_schema(table).map(|schema| TableInfo {
                    name: table.to_string(),
                    schema,
                    supports_incremental: true,
                    incremental_key: Some("updated_at".to_string()),
                    estimated_rows: None,
                    primary_key_columns: vec!["id".to_string()],
                })
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        Self::get_table_schema(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))
    }

    async fn fetch_table(
        &self,
        table: &str,
        _incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        if !Self::TABLES.contains(&table) {
            return Err(ConnectorError::TableNotFound(table.to_string()));
        }

        let schema = self.get_schema(table).await?;
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        let filter = last_value.and_then(|ts| {
            let mut cmp = serde_json::Map::new();
            cmp.insert("gt".to_string(), serde_json::Value::String(ts.to_string()));
            let mut filter = serde_json::Map::new();
            filter.insert("updatedAt".to_string(), serde_json::Value::Object(cmp));
            Some(serde_json::Value::Object(filter))
        });

        self.fetch_paginated(table, &schema, arrow_schema, filter)
            .await
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            if !Self::TABLES.contains(&table) {
                return Err(ConnectorError::TableNotFound(table.to_string()));
            }

            let schema = self.get_schema(table).await?;
            let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

            let mut filter = if !options.predicates.is_empty() {
                Self::predicates_to_graphql_filter(&options.predicates)
            } else {
                None
            };

            // Merge incremental filter
            if let Some(ts) = options.last_value.as_deref() {
                let mut cmp = serde_json::Map::new();
                cmp.insert("gt".to_string(), serde_json::Value::String(ts.to_string()));
                let incremental = serde_json::json!({ "updatedAt": cmp });

                filter = Some(match filter {
                    Some(serde_json::Value::Object(mut existing)) => {
                        existing.insert("updatedAt".to_string(), incremental["updatedAt"].clone());
                        serde_json::Value::Object(existing)
                    }
                    _ => incremental,
                });
            }

            let batches = self
                .fetch_paginated(table, &schema, arrow_schema, filter)
                .await?;

            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        let query = "query { viewer { id } }";
        self.graphql_request(query, serde_json::json!({})).await?;
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(api_key: &str) -> LinearConfig {
        LinearConfig::new(api_key)
    }

    struct TestableLinearConnector {
        config: LinearConfig,
        client: reqwest::Client,
        base_url: String,
    }

    impl TestableLinearConnector {
        async fn graphql_request(
            &self,
            query: &str,
            variables: serde_json::Value,
        ) -> ConnectorResult<serde_json::Value> {
            let body = serde_json::json!({
                "query": query,
                "variables": variables,
            });

            let response = self
                .client
                .post(&self.base_url)
                .header("Content-Type", "application/json")
                .header("Authorization", self.config.api_key.expose())
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    ConnectorError::Network(format!("Failed to connect to Linear API: {}", e))
                })?;

            if response.status() == 401 {
                return Err(ConnectorError::Authentication(
                    "Invalid Linear API key".to_string(),
                ));
            }

            if response.status() == 429 {
                let retry_after = response
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60);
                return Err(ConnectorError::RateLimited {
                    retry_after_secs: retry_after,
                });
            }

            if !response.status().is_success() {
                let status = response.status();
                let body_text = response.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "Linear API error ({}): {}",
                    status, body_text
                )));
            }

            let json: serde_json::Value = response.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Linear response: {}", e))
            })?;

            if let Some(errors) = json.get("errors") {
                if let Some(arr) = errors.as_array() {
                    if !arr.is_empty() {
                        let msg = arr
                            .iter()
                            .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                            .collect::<Vec<_>>()
                            .join("; ");
                        return Err(ConnectorError::Internal(format!(
                            "Linear GraphQL errors: {}",
                            msg
                        )));
                    }
                }
            }

            json.get("data")
                .cloned()
                .ok_or_else(|| {
                    ConnectorError::Internal(
                        "Linear response missing 'data' field".to_string(),
                    )
                })
        }
    }

    fn test_connector_with_base_url(
        config: LinearConfig,
        base_url: String,
    ) -> TestableLinearConnector {
        TestableLinearConnector {
            config,
            client: reqwest::Client::new(),
            base_url,
        }
    }

    // ── Config tests ──────────────────────────────────────────────────

    #[test]
    fn test_linear_config_creation() {
        let config = LinearConfig::new("lin_api_test123");
        assert_eq!(config.api_key.expose(), "lin_api_test123");
    }

    #[test]
    fn test_linear_config_debug_redacts_api_key() {
        let config = LinearConfig::new("lin_api_secret_key");
        let debug_output = format!("{:?}", config);
        assert!(!debug_output.contains("lin_api_secret_key"));
        assert!(debug_output.contains("REDACTED"));
    }

    // ── Schema tests ──────────────────────────────────────────────────

    #[test]
    fn test_schema_all_tables() {
        for table in LinearConnector::TABLES {
            let schema = LinearConnector::get_table_schema(table);
            assert!(schema.is_some(), "Missing schema for table: {}", table);
            let schema = schema.unwrap();
            assert!(
                !schema.columns.is_empty(),
                "Empty schema for table: {}",
                table
            );

            let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
            assert!(
                col_names.contains(&"id"),
                "Table {} missing 'id' column",
                table
            );
            assert!(
                col_names.contains(&"created_at"),
                "Table {} missing 'created_at' column",
                table
            );
            assert!(
                col_names.contains(&"updated_at"),
                "Table {} missing 'updated_at' column",
                table
            );
        }
    }

    #[test]
    fn test_schema_unknown_table() {
        assert!(LinearConnector::get_table_schema("nonexistent").is_none());
    }

    #[test]
    fn test_schema_issues_columns() {
        let schema = LinearConnector::get_table_schema("issues").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"identifier"));
        assert!(names.contains(&"title"));
        assert!(names.contains(&"priority"));
        assert!(names.contains(&"state_id"));
        assert!(names.contains(&"assignee_id"));
        assert!(names.contains(&"team_id"));
        assert!(names.contains(&"label_ids"));
    }

    #[test]
    fn test_to_arrow_schema() {
        let table_schema = LinearConnector::get_table_schema("issues").unwrap();
        let arrow_schema = LinearConnector::to_arrow_schema(&table_schema);
        assert_eq!(arrow_schema.fields().len(), table_schema.columns.len());
        assert!(arrow_schema.field_with_name("id").is_ok());
        assert!(arrow_schema.field_with_name("title").is_ok());
    }

    // ── Builder / append tests ────────────────────────────────────────

    #[test]
    fn test_append_linear_object_project() {
        let schema = LinearConnector::get_table_schema("projects").unwrap();
        let arrow_schema = Arc::new(LinearConnector::to_arrow_schema(&schema));

        let obj = serde_json::json!({
            "id": "proj_001",
            "name": "My Project",
            "description": "A test project",
            "state": "started",
            "progress": 0.42,
            "startDate": "2024-01-15",
            "targetDate": "2024-06-30",
            "lead": { "id": "usr_lead_1" },
            "createdAt": "2024-01-15T10:30:00.000Z",
            "updatedAt": "2024-02-20T14:00:00.000Z"
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        LinearConnector::append_linear_object(&obj, &schema, "projects", &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), schema.columns.len());
    }

    #[test]
    fn test_append_linear_object_user() {
        let schema = LinearConnector::get_table_schema("users").unwrap();
        let arrow_schema = Arc::new(LinearConnector::to_arrow_schema(&schema));

        let obj = serde_json::json!({
            "id": "usr_001",
            "name": "Alice Smith",
            "email": "alice@example.com",
            "displayName": "Alice",
            "active": true,
            "admin": false,
            "createdAt": "2023-06-01T09:00:00.000Z",
            "updatedAt": "2024-01-01T12:00:00.000Z"
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        LinearConnector::append_linear_object(&obj, &schema, "users", &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), schema.columns.len());
    }

    #[test]
    fn test_builders_issues_batch() {
        let schema = LinearConnector::get_table_schema("issues").unwrap();
        let arrow_schema = Arc::new(LinearConnector::to_arrow_schema(&schema));

        let nodes = vec![
            serde_json::json!({
                "id": "iss_001",
                "identifier": "LIN-1",
                "title": "Fix bug",
                "description": "A critical bug",
                "priority": 1,
                "estimate": 3.0,
                "dueDate": "2024-03-15",
                "state": { "id": "st_1", "name": "In Progress" },
                "assignee": { "id": "usr_1" },
                "team": { "id": "team_1" },
                "project": { "id": "proj_1" },
                "labels": { "nodes": [{ "id": "lbl_1" }, { "id": "lbl_2" }] },
                "cycle": { "id": "cyc_1" },
                "createdAt": "2024-01-10T08:00:00.000Z",
                "updatedAt": "2024-01-12T10:30:00.000Z",
                "archivedAt": null
            }),
            serde_json::json!({
                "id": "iss_002",
                "identifier": "LIN-2",
                "title": "Add feature",
                "description": null,
                "priority": 3,
                "estimate": null,
                "dueDate": null,
                "state": { "id": "st_2", "name": "Backlog" },
                "assignee": null,
                "team": { "id": "team_1" },
                "project": null,
                "labels": { "nodes": [] },
                "cycle": null,
                "createdAt": "2024-01-11T09:00:00.000Z",
                "updatedAt": "2024-01-11T09:00:00.000Z",
                "archivedAt": null
            }),
        ];

        let mut builders = ColumnBuilders::new(&schema, 4);
        for node in &nodes {
            LinearConnector::append_linear_object(node, &schema, "issues", &mut builders);
        }

        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), schema.columns.len());
    }

    #[test]
    fn test_extract_label_ids() {
        let obj = serde_json::json!({
            "labels": { "nodes": [{ "id": "lbl_a" }, { "id": "lbl_b" }] }
        });
        assert_eq!(
            LinearConnector::extract_label_ids(&obj),
            Some("lbl_a,lbl_b".to_string())
        );

        let empty = serde_json::json!({
            "labels": { "nodes": [] }
        });
        assert_eq!(LinearConnector::extract_label_ids(&empty), None);

        let missing = serde_json::json!({});
        assert_eq!(LinearConnector::extract_label_ids(&missing), None);
    }

    #[test]
    fn test_parse_linear_timestamp() {
        let val = serde_json::json!("2024-01-15T10:30:00.000Z");
        let ts = LinearConnector::parse_linear_timestamp(&val);
        assert!(ts.is_some());
        let micros = ts.unwrap();
        assert!(micros > 0);

        let null_val = serde_json::json!(null);
        assert!(LinearConnector::parse_linear_timestamp(&null_val).is_none());

        let bad_val = serde_json::json!("not-a-date");
        assert!(LinearConnector::parse_linear_timestamp(&bad_val).is_none());
    }

    // ── GraphQL query tests ───────────────────────────────────────────

    #[test]
    fn test_build_query_without_filter() {
        let query = LinearConnector::build_query("issues", false);
        assert!(query.contains("$first: Int!"));
        assert!(query.contains("$after: String"));
        assert!(!query.contains("$filter"));
        assert!(query.contains("issues("));
        assert!(query.contains("nodes"));
        assert!(query.contains("pageInfo"));
    }

    #[test]
    fn test_build_query_with_filter() {
        let query = LinearConnector::build_query("issues", true);
        assert!(query.contains("$filter: IssueFilter"));
        assert!(query.contains("filter: $filter"));
    }

    #[test]
    fn test_graphql_fields_all_tables() {
        for table in LinearConnector::TABLES {
            let (root, fields) = LinearConnector::graphql_fields(table);
            assert!(
                !root.is_empty(),
                "Empty root field for table: {}",
                table
            );
            assert!(
                !fields.is_empty(),
                "Empty fields for table: {}",
                table
            );
        }
    }

    // ── Predicate pushdown tests ──────────────────────────────────────

    #[test]
    fn test_predicates_to_graphql_filter_equals() {
        use crate::warehouse::query::predicate_pushdown::Predicate;
        use compact_str::CompactString;

        let predicates = vec![Predicate::Equals {
            column: CompactString::new("title"),
            value: CompactString::new("Bug fix"),
        }];

        let filter = LinearConnector::predicates_to_graphql_filter(&predicates);
        assert!(filter.is_some());
        let f = filter.unwrap();
        assert_eq!(f["title"]["eq"], "Bug fix");
    }

    #[test]
    fn test_predicates_to_graphql_filter_date_range() {
        use crate::warehouse::query::predicate_pushdown::Predicate;
        use compact_str::CompactString;

        let predicates = vec![
            Predicate::GreaterThan {
                column: CompactString::new("created_at"),
                value: CompactString::new("2024-01-01T00:00:00.000Z"),
                inclusive: true,
            },
            Predicate::LessThan {
                column: CompactString::new("created_at"),
                value: CompactString::new("2024-12-31T23:59:59.999Z"),
                inclusive: false,
            },
        ];

        let filter = LinearConnector::predicates_to_graphql_filter(&predicates);
        assert!(filter.is_some());
        let f = filter.unwrap();
        let created = f.get("createdAt").unwrap().as_object().unwrap();
        assert_eq!(created.get("gte").unwrap(), "2024-01-01T00:00:00.000Z");
        assert_eq!(created.get("lt").unwrap(), "2024-12-31T23:59:59.999Z");
    }

    #[test]
    fn test_predicates_to_graphql_filter_in() {
        use crate::warehouse::query::predicate_pushdown::Predicate;
        use compact_str::CompactString;

        let predicates = vec![Predicate::In {
            column: CompactString::new("priority"),
            values: vec![
                CompactString::new("1"),
                CompactString::new("2"),
            ],
        }];

        let filter = LinearConnector::predicates_to_graphql_filter(&predicates);
        assert!(filter.is_some());
        let f = filter.unwrap();
        let arr = f["priority"]["in"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_predicates_empty() {
        let filter = LinearConnector::predicates_to_graphql_filter(&[]);
        assert!(filter.is_none());
    }

    // ── Connector trait tests ─────────────────────────────────────────

    #[tokio::test]
    async fn test_list_tables() {
        let connector = LinearConnector::new(test_config("lin_test_key"));
        let tables = connector.list_tables().await.unwrap();
        assert_eq!(tables.len(), LinearConnector::TABLES.len());

        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"issues"));
        assert!(names.contains(&"projects"));
        assert!(names.contains(&"teams"));
        assert!(names.contains(&"documents"));

        for t in &tables {
            assert!(t.supports_incremental);
            assert_eq!(t.incremental_key, Some("updated_at".to_string()));
            assert_eq!(t.primary_key_columns, vec!["id".to_string()]);
        }
    }

    #[tokio::test]
    async fn test_get_schema() {
        let connector = LinearConnector::new(test_config("lin_test_key"));
        let schema = connector.get_schema("issues").await.unwrap();
        assert!(!schema.columns.is_empty());
    }

    #[tokio::test]
    async fn test_get_schema_not_found() {
        let connector = LinearConnector::new(test_config("lin_test_key"));
        let result = connector.get_schema("nonexistent").await;
        assert!(matches!(result, Err(ConnectorError::TableNotFound(_))));
    }

    #[test]
    fn test_source_type() {
        let connector = LinearConnector::new(test_config("lin_test_key"));
        assert_eq!(connector.source_type(), SourceType::Linear);
    }

    // ── HTTP / wiremock tests ─────────────────────────────────────────

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(header("Content-Type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": { "viewer": { "id": "usr_123" } }
            })))
            .mount(&mock_server)
            .await;

        let config = test_config("lin_test_key");
        let connector = test_connector_with_base_url(config, mock_server.uri());

        let result = connector
            .graphql_request("query { viewer { id } }", serde_json::json!({}))
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["viewer"]["id"], "usr_123");
    }

    #[tokio::test]
    async fn test_authentication_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&mock_server)
            .await;

        let config = test_config("invalid_key");
        let connector = test_connector_with_base_url(config, mock_server.uri());

        let result = connector
            .graphql_request("query { viewer { id } }", serde_json::json!({}))
            .await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_rate_limited() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(429).insert_header("Retry-After", "30"),
            )
            .mount(&mock_server)
            .await;

        let config = test_config("lin_test_key");
        let connector = test_connector_with_base_url(config, mock_server.uri());

        let result = connector
            .graphql_request("query { viewer { id } }", serde_json::json!({}))
            .await;
        match result {
            Err(ConnectorError::RateLimited { retry_after_secs }) => {
                assert_eq!(retry_after_secs, 30);
            }
            _ => panic!("Expected RateLimited error"),
        }
    }

    #[tokio::test]
    async fn test_graphql_error_response() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "errors": [
                    { "message": "Field 'foo' not found" }
                ]
            })))
            .mount(&mock_server)
            .await;

        let config = test_config("lin_test_key");
        let connector = test_connector_with_base_url(config, mock_server.uri());

        let result = connector
            .graphql_request("query { foo }", serde_json::json!({}))
            .await;
        assert!(matches!(result, Err(ConnectorError::Internal(_))));
        if let Err(ConnectorError::Internal(msg)) = result {
            assert!(msg.contains("Field 'foo' not found"));
        }
    }
}
