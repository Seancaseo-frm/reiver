//! Jira connector for the data warehouse.
//!
//! Syncs Jira data (issues, projects, sprints, boards, etc.) via the REST API.
//! Uses the `gouqi` crate (async mode) for typed entity access and JQL search,
//! supplemented with direct `reqwest` calls for lookup endpoints not exposed
//! by gouqi. Supports 19 tables, JQL predicate pushdown, and incremental sync
//! on the issues table via `updated >=` JQL filters.

use super::builders::ColumnBuilders;
use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::crypto::SecretString;
use crate::warehouse::query::predicate_pushdown::Predicate;
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

const BATCH_THRESHOLD: usize = 500;
const MAX_TOTAL_ROWS: usize = 1_000_000;
const PAGE_SIZE: u64 = 50;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 100;

const TABLES: &[&str] = &[
    "issues",
    "projects",
    "boards",
    "sprints",
    "users",
    "statuses",
    "priorities",
    "issue_types",
    "resolutions",
    "comments",
    "worklogs",
    "changelogs",
    "versions",
    "components",
    "labels",
    "fields",
    "filters",
    "roles",
    "attachments",
];

// ═══════════════════════════════════════════════════════════════════════════
// Config
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone)]
pub struct JiraConfig {
    pub host: String,
    pub email: Option<String>,
    pub api_token: Option<SecretString>,
    pub personal_access_token: Option<SecretString>,
}

impl std::fmt::Debug for JiraConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JiraConfig")
            .field("host", &self.host)
            .field("email", &self.email)
            .field("api_token", &"***REDACTED***")
            .field("personal_access_token", &"***REDACTED***")
            .finish()
    }
}

impl JiraConfig {
    pub fn with_basic_auth(
        host: impl Into<String>,
        email: impl Into<String>,
        api_token: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            email: Some(email.into()),
            api_token: Some(SecretString::new(api_token)),
            personal_access_token: None,
        }
    }

    pub fn with_pat(host: impl Into<String>, pat: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            email: None,
            api_token: None,
            personal_access_token: Some(SecretString::new(pat)),
        }
    }

    fn credentials(&self) -> ConnectorResult<gouqi::Credentials> {
        if let Some(ref pat) = self.personal_access_token {
            return Ok(gouqi::Credentials::Bearer(pat.expose().to_string()));
        }
        if let (Some(ref email), Some(ref token)) = (&self.email, &self.api_token) {
            return Ok(gouqi::Credentials::Basic(
                email.clone(),
                token.expose().to_string(),
            ));
        }
        Err(ConnectorError::Config(
            "Jira config requires either (email + api_token) or personal_access_token".to_string(),
        ))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Connector
// ═══════════════════════════════════════════════════════════════════════════

pub struct JiraConnector {
    config: JiraConfig,
    http: reqwest::Client,
}

impl JiraConnector {
    pub fn new(config: JiraConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    fn build_gouqi_client(&self) -> ConnectorResult<gouqi::r#async::Jira> {
        let creds = self.config.credentials()?;
        gouqi::r#async::Jira::new(&self.config.host, creds)
            .map_err(|e| ConnectorError::Config(format!("Failed to create Jira client: {}", e)))
    }

    // ════════════════════════════════════════════════════════════════════
    // Arrow schema conversion
    // ════════════════════════════════════════════════════════════════════

    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), col.nullable))
            .collect();
        Schema::new(fields)
    }

    // ════════════════════════════════════════════════════════════════════
    // Static table schemas (19 tables)
    // ════════════════════════════════════════════════════════════════════

    fn get_table_schema(table: &str) -> Option<TableSchema> {
        let columns = match table {
            "issues" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("key", ColumnType::String, false),
                ColumnSchema::new("summary", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("status_name", ColumnType::String, true),
                ColumnSchema::new("status_category", ColumnType::String, true),
                ColumnSchema::new("priority_name", ColumnType::String, true),
                ColumnSchema::new("issue_type_name", ColumnType::String, true),
                ColumnSchema::new("project_key", ColumnType::String, true),
                ColumnSchema::new("assignee_id", ColumnType::String, true),
                ColumnSchema::new("assignee_name", ColumnType::String, true),
                ColumnSchema::new("reporter_id", ColumnType::String, true),
                ColumnSchema::new("reporter_name", ColumnType::String, true),
                ColumnSchema::new("creator_id", ColumnType::String, true),
                ColumnSchema::new("creator_name", ColumnType::String, true),
                ColumnSchema::new("resolution", ColumnType::String, true),
                ColumnSchema::new("labels", ColumnType::String, true)
                    .with_description("Comma-separated labels"),
                ColumnSchema::new("components", ColumnType::String, true)
                    .with_description("Comma-separated component names"),
                ColumnSchema::new("fix_versions", ColumnType::String, true)
                    .with_description("Comma-separated fix version names"),
                ColumnSchema::new("parent_key", ColumnType::String, true),
                ColumnSchema::new("created", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("resolved", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("due_date", ColumnType::String, true),
                ColumnSchema::new("original_estimate_secs", ColumnType::Float64, true),
                ColumnSchema::new("time_spent_secs", ColumnType::Float64, true),
                ColumnSchema::new("remaining_estimate_secs", ColumnType::Float64, true),
                ColumnSchema::new("url", ColumnType::String, true),
            ],
            "projects" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("key", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("lead_id", ColumnType::String, true),
                ColumnSchema::new("lead_name", ColumnType::String, true),
                ColumnSchema::new("project_type", ColumnType::String, true),
                ColumnSchema::new("url", ColumnType::String, true),
            ],
            "boards" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("board_type", ColumnType::String, true),
                ColumnSchema::new("self_link", ColumnType::String, true),
            ],
            "sprints" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("state", ColumnType::String, true),
                ColumnSchema::new("start_date", ColumnType::String, true),
                ColumnSchema::new("end_date", ColumnType::String, true),
                ColumnSchema::new("complete_date", ColumnType::String, true),
                ColumnSchema::new("board_id", ColumnType::Float64, true),
            ],
            "users" => vec![
                ColumnSchema::new("account_id", ColumnType::String, false),
                ColumnSchema::new("display_name", ColumnType::String, true),
                ColumnSchema::new("email", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, true),
                ColumnSchema::new("account_type", ColumnType::String, true),
            ],
            "statuses" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("category_key", ColumnType::String, true),
                ColumnSchema::new("category_name", ColumnType::String, true),
            ],
            "priorities" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("icon_url", ColumnType::String, true),
            ],
            "issue_types" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("subtask", ColumnType::Boolean, true),
                ColumnSchema::new("icon_url", ColumnType::String, true),
            ],
            "resolutions" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
            ],
            "comments" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("issue_key", ColumnType::String, false),
                ColumnSchema::new("author_id", ColumnType::String, true),
                ColumnSchema::new("author_name", ColumnType::String, true),
                ColumnSchema::new("body", ColumnType::String, true),
                ColumnSchema::new("created", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "worklogs" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("issue_key", ColumnType::String, false),
                ColumnSchema::new("author_id", ColumnType::String, true),
                ColumnSchema::new("author_name", ColumnType::String, true),
                ColumnSchema::new("time_spent_seconds", ColumnType::Float64, true),
                ColumnSchema::new("started", ColumnType::String, true),
                ColumnSchema::new("created", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("comment", ColumnType::String, true),
            ],
            "changelogs" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("issue_key", ColumnType::String, false),
                ColumnSchema::new("author_id", ColumnType::String, true),
                ColumnSchema::new("author_name", ColumnType::String, true),
                ColumnSchema::new("created", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("field", ColumnType::String, true),
                ColumnSchema::new("from_value", ColumnType::String, true),
                ColumnSchema::new("to_value", ColumnType::String, true),
            ],
            "versions" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("project_key", ColumnType::String, true),
                ColumnSchema::new("released", ColumnType::Boolean, true),
                ColumnSchema::new("archived", ColumnType::Boolean, true),
                ColumnSchema::new("release_date", ColumnType::String, true),
                ColumnSchema::new("start_date", ColumnType::String, true),
            ],
            "components" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("project_key", ColumnType::String, true),
                ColumnSchema::new("lead_id", ColumnType::String, true),
                ColumnSchema::new("lead_name", ColumnType::String, true),
            ],
            "labels" => vec![
                ColumnSchema::new("name", ColumnType::String, false),
            ],
            "fields" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("custom", ColumnType::Boolean, true),
                ColumnSchema::new("schema_type", ColumnType::String, true),
            ],
            "filters" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("jql", ColumnType::String, true),
                ColumnSchema::new("owner_name", ColumnType::String, true),
                ColumnSchema::new("favourite", ColumnType::Boolean, true),
            ],
            "roles" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
            ],
            "attachments" => vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("issue_key", ColumnType::String, false),
                ColumnSchema::new("filename", ColumnType::String, true),
                ColumnSchema::new("author_id", ColumnType::String, true),
                ColumnSchema::new("author_name", ColumnType::String, true),
                ColumnSchema::new("created", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("size", ColumnType::Float64, true),
                ColumnSchema::new("mime_type", ColumnType::String, true),
                ColumnSchema::new("content_url", ColumnType::String, true),
            ],
            _ => return None,
        };
        Some(TableSchema { columns })
    }

    // ════════════════════════════════════════════════════════════════════
    // Timestamp parsing
    // ════════════════════════════════════════════════════════════════════

    fn parse_timestamp_str(s: &str) -> Option<i64> {
        chrono::DateTime::parse_from_rfc3339(s)
            .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.3f%z"))
            .ok()
            .map(|dt| dt.timestamp_micros())
    }

    fn parse_timestamp_json(val: &serde_json::Value) -> Option<i64> {
        val.as_str().and_then(Self::parse_timestamp_str)
    }

    fn offset_datetime_to_micros(dt: &time::OffsetDateTime) -> i64 {
        let secs = dt.unix_timestamp();
        let nanos = dt.nanosecond() as i64;
        secs * 1_000_000 + nanos / 1_000
    }

    // ════════════════════════════════════════════════════════════════════
    // HTTP helper with retry
    // ════════════════════════════════════════════════════════════════════

    fn auth_header_value(&self) -> ConnectorResult<String> {
        if let Some(ref pat) = self.config.personal_access_token {
            return Ok(format!("Bearer {}", pat.expose()));
        }
        if let (Some(ref email), Some(ref token)) = (&self.config.email, &self.config.api_token) {
            let encoded = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{}:{}", email, token.expose()),
            );
            return Ok(format!("Basic {}", encoded));
        }
        Err(ConnectorError::Config("Missing Jira credentials".to_string()))
    }

    async fn api_get(&self, path: &str) -> ConnectorResult<serde_json::Value> {
        let url = format!("{}{}", self.config.host.trim_end_matches('/'), path);
        let auth = self.auth_header_value()?;
        let mut attempts = 0u32;

        loop {
            let resp = self
                .http
                .get(&url)
                .header("Authorization", &auth)
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("Jira request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Jira credentials".to_string(),
                ));
            }

            if resp.status() == 429 {
                if attempts < MAX_RETRIES {
                    attempts += 1;
                    let delay = INITIAL_RETRY_DELAY_MS * 2u64.pow(attempts - 1);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    continue;
                }
                return Err(ConnectorError::RateLimited { retry_after_secs: 30 });
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "Jira API error ({}): {}",
                    status, body
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Jira response: {}", e))
            })?;

            return Ok(json);
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // Predicate -> JQL translation
    // ════════════════════════════════════════════════════════════════════

    fn column_to_jql_field(col: &str) -> &str {
        match col {
            "status_name" => "status",
            "priority_name" => "priority",
            "issue_type_name" => "issuetype",
            "project_key" => "project",
            "assignee_id" | "assignee_name" => "assignee",
            "reporter_id" | "reporter_name" => "reporter",
            "creator_id" | "creator_name" => "creator",
            "resolution" => "resolution",
            "due_date" => "duedate",
            "parent_key" => "parent",
            "key" => "key",
            "summary" => "summary",
            "description" => "description",
            "created" => "created",
            "updated" => "updated",
            "resolved" => "resolved",
            other => other,
        }
    }

    fn escape_jql_value(val: &str) -> String {
        val.replace('\\', "\\\\").replace('"', "\\\"")
    }

    fn predicates_to_jql(predicates: &[Predicate]) -> Option<String> {
        if predicates.is_empty() {
            return None;
        }
        let parts: Vec<String> = predicates
            .iter()
            .filter_map(Self::predicate_to_jql_clause)
            .collect();
        if parts.is_empty() {
            return None;
        }
        Some(parts.join(" AND "))
    }

    fn predicate_to_jql_clause(pred: &Predicate) -> Option<String> {
        match pred {
            Predicate::Equals { column, value } => {
                let field = Self::column_to_jql_field(column);
                Some(format!("{} = \"{}\"", field, Self::escape_jql_value(value)))
            }
            Predicate::In { column, values } => {
                let field = Self::column_to_jql_field(column);
                let vals: Vec<String> = values
                    .iter()
                    .map(|v| format!("\"{}\"", Self::escape_jql_value(v)))
                    .collect();
                Some(format!("{} IN ({})", field, vals.join(",")))
            }
            Predicate::Contains { column, substring } => {
                let field = Self::column_to_jql_field(column);
                Some(format!("{} ~ \"{}\"", field, Self::escape_jql_value(substring)))
            }
            Predicate::GreaterThan { column, value, inclusive } => {
                let field = Self::column_to_jql_field(column);
                let op = if *inclusive { ">=" } else { ">" };
                Some(format!("{} {} \"{}\"", field, op, Self::escape_jql_value(value)))
            }
            Predicate::LessThan { column, value, inclusive } => {
                let field = Self::column_to_jql_field(column);
                let op = if *inclusive { "<=" } else { "<" };
                Some(format!("{} {} \"{}\"", field, op, Self::escape_jql_value(value)))
            }
            Predicate::IsNull { column, is_null } => {
                let field = Self::column_to_jql_field(column);
                if *is_null {
                    Some(format!("{} IS EMPTY", field))
                } else {
                    Some(format!("{} IS NOT EMPTY", field))
                }
            }
            Predicate::Not(inner) => {
                match inner.as_ref() {
                    Predicate::Equals { column, value } => {
                        let field = Self::column_to_jql_field(column);
                        Some(format!("{} != \"{}\"", field, Self::escape_jql_value(value)))
                    }
                    other => {
                        let clause = Self::predicate_to_jql_clause(other)?;
                        Some(format!("NOT ({})", clause))
                    }
                }
            }
            Predicate::And(preds) => {
                let parts: Vec<String> = preds
                    .iter()
                    .filter_map(Self::predicate_to_jql_clause)
                    .collect();
                if parts.is_empty() { None }
                else if parts.len() == 1 { Some(parts.into_iter().next().unwrap()) }
                else { Some(format!("({})", parts.join(" AND "))) }
            }
            Predicate::Or(preds) => {
                let parts: Vec<String> = preds
                    .iter()
                    .filter_map(Self::predicate_to_jql_clause)
                    .collect();
                if parts.is_empty() { None }
                else if parts.len() == 1 { Some(parts.into_iter().next().unwrap()) }
                else { Some(format!("({})", parts.join(" OR "))) }
            }
            Predicate::Like { column, pattern } => {
                let field = Self::column_to_jql_field(column);
                Some(format!("{} ~ \"{}\"", field, Self::escape_jql_value(pattern)))
            }
            Predicate::Between { column, low, high } => {
                let field = Self::column_to_jql_field(column);
                Some(format!(
                    "{} >= \"{}\" AND {} <= \"{}\"",
                    field, Self::escape_jql_value(low),
                    field, Self::escape_jql_value(high),
                ))
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // Issue fetching (JQL search with pagination)
    // ════════════════════════════════════════════════════════════════════

    async fn fetch_issues(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let client = self.build_gouqi_client()?;

        let mut jql_parts: Vec<String> = Vec::new();
        if let Some(pred_jql) = Self::predicates_to_jql(&options.predicates) {
            jql_parts.push(pred_jql);
        }
        if let Some(ref last_value) = options.last_value {
            jql_parts.push(format!("updated >= \"{}\"", Self::escape_jql_value(last_value)));
        }

        let jql = if jql_parts.is_empty() {
            "created IS NOT EMPTY ORDER BY updated ASC".to_string()
        } else {
            format!("{} ORDER BY updated ASC", jql_parts.join(" AND "))
        };

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut start_at: u64 = 0;
        let mut total_rows: usize = 0;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        loop {
            if total_rows >= max_rows {
                break;
            }

            let search_opts = gouqi::SearchOptions::builder()
                .all_fields()
                .max_results(PAGE_SIZE)
                .start_at(start_at)
                .build();

            let results = client
                .search()
                .list(&jql, &search_opts)
                .await
                .map_err(|e| {
                    let msg = e.to_string();
                    if msg.contains("401") || msg.contains("403") {
                        ConnectorError::Authentication("Invalid Jira credentials".to_string())
                    } else if msg.contains("429") {
                        ConnectorError::RateLimited { retry_after_secs: 30 }
                    } else {
                        ConnectorError::Internal(format!("Jira search failed: {}", msg))
                    }
                })?;

            if results.issues.is_empty() {
                break;
            }

            for issue in &results.issues {
                Self::append_issue(issue, schema, &mut builders);
            }

            total_rows += results.issues.len();

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            start_at += results.issues.len() as u64;
            if start_at >= results.total {
                break;
            }

            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    fn append_issue(
        issue: &gouqi::Issue,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_string(Some(&issue.id)),
                "key" => builders.builder(i).append_string(Some(&issue.key)),
                "summary" => builders.builder(i).append_string(issue.summary().as_deref()),
                "description" => builders.builder(i).append_string(issue.description().as_deref()),
                "status_name" => {
                    let v = issue.status().map(|s| s.name);
                    builders.builder(i).append_string(v.as_deref());
                }
                "status_category" => {
                    let v = issue.field::<serde_json::Value>("status")
                        .and_then(|r| r.ok())
                        .and_then(|s| s.get("statusCategory").and_then(|c| c.get("name").and_then(|n| n.as_str()).map(String::from)));
                    builders.builder(i).append_string(v.as_deref());
                }
                "priority_name" => {
                    let v = issue.priority().map(|p| p.name);
                    builders.builder(i).append_string(v.as_deref());
                }
                "issue_type_name" => {
                    let v = issue.issue_type().map(|t| t.name);
                    builders.builder(i).append_string(v.as_deref());
                }
                "project_key" => {
                    let v = issue.project().map(|p| p.key);
                    builders.builder(i).append_string(v.as_deref());
                }
                "assignee_id" => {
                    let v = issue.assignee().and_then(|u| u.account_id);
                    builders.builder(i).append_string(v.as_deref());
                }
                "assignee_name" => {
                    let v = issue.assignee().map(|u| u.display_name);
                    builders.builder(i).append_string(v.as_deref());
                }
                "reporter_id" => {
                    let v = issue.reporter().and_then(|u| u.account_id);
                    builders.builder(i).append_string(v.as_deref());
                }
                "reporter_name" => {
                    let v = issue.reporter().map(|u| u.display_name);
                    builders.builder(i).append_string(v.as_deref());
                }
                "creator_id" => {
                    let v = issue.creator().and_then(|u| u.account_id);
                    builders.builder(i).append_string(v.as_deref());
                }
                "creator_name" => {
                    let v = issue.creator().map(|u| u.display_name);
                    builders.builder(i).append_string(v.as_deref());
                }
                "resolution" => {
                    let v = issue.field::<serde_json::Value>("resolution")
                        .and_then(|r| r.ok())
                        .and_then(|r| r.get("name").and_then(|n| n.as_str()).map(String::from));
                    builders.builder(i).append_string(v.as_deref());
                }
                "labels" => {
                    let labels = issue.labels();
                    let v = if labels.is_empty() { None } else { Some(labels.join(",")) };
                    builders.builder(i).append_string(v.as_deref());
                }
                "components" => {
                    let v = issue.field::<Vec<serde_json::Value>>("components")
                        .and_then(|r| r.ok())
                        .map(|cs| cs.iter()
                            .filter_map(|c| c.get("name").and_then(|n| n.as_str()))
                            .collect::<Vec<_>>()
                            .join(","));
                    builders.builder(i).append_string(v.as_deref());
                }
                "fix_versions" => {
                    let vs = issue.fix_versions();
                    let v = if vs.is_empty() { None } else {
                        Some(vs.iter().map(|v| v.name.as_str()).collect::<Vec<_>>().join(","))
                    };
                    builders.builder(i).append_string(v.as_deref());
                }
                "parent_key" => {
                    let v = issue.field::<serde_json::Value>("parent")
                        .and_then(|r| r.ok())
                        .and_then(|p| p.get("key").and_then(|k| k.as_str()).map(String::from));
                    builders.builder(i).append_string(v.as_deref());
                }
                "created" => {
                    let ts = issue.created().map(|dt| Self::offset_datetime_to_micros(&dt));
                    builders.builder(i).append_timestamp(ts);
                }
                "updated" => {
                    let ts = issue.updated().map(|dt| Self::offset_datetime_to_micros(&dt));
                    builders.builder(i).append_timestamp(ts);
                }
                "resolved" => {
                    let v = issue.field::<String>("resolutiondate").and_then(|r| r.ok());
                    let ts = v.as_deref().and_then(Self::parse_timestamp_str);
                    builders.builder(i).append_timestamp(ts);
                }
                "due_date" => {
                    let v = issue.field::<String>("duedate").and_then(|r| r.ok());
                    builders.builder(i).append_string(v.as_deref());
                }
                "original_estimate_secs" => {
                    let v = issue.field::<serde_json::Value>("timetracking")
                        .and_then(|r| r.ok())
                        .and_then(|t| t.get("originalEstimateSeconds").and_then(|n| n.as_f64()));
                    builders.builder(i).append_f64(v);
                }
                "time_spent_secs" => {
                    let v = issue.field::<serde_json::Value>("timetracking")
                        .and_then(|r| r.ok())
                        .and_then(|t| t.get("timeSpentSeconds").and_then(|n| n.as_f64()));
                    builders.builder(i).append_f64(v);
                }
                "remaining_estimate_secs" => {
                    let v = issue.field::<serde_json::Value>("timetracking")
                        .and_then(|r| r.ok())
                        .and_then(|t| t.get("remainingEstimateSeconds").and_then(|n| n.as_f64()));
                    builders.builder(i).append_f64(v);
                }
                "url" => {
                    builders.builder(i).append_string(Some(&issue.self_link));
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    // ════════════════════════════════════════════════════════════════════
    // Typed entity table fetching (via gouqi)
    // ════════════════════════════════════════════════════════════════════

    async fn fetch_projects(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let data = self.api_get("/rest/api/3/project?expand=lead,description").await?;
        let projects = data.as_array().ok_or_else(|| {
            ConnectorError::Internal("Jira projects response is not an array".to_string())
        })?;

        let mut builders = ColumnBuilders::new(schema, projects.len().max(1));
        for p in projects {
            for (i, col) in schema.columns.iter().enumerate() {
                match col.name.as_str() {
                    "id" => builders.builder(i).append_string(p.get("id").and_then(|v| v.as_str())),
                    "key" => builders.builder(i).append_string(p.get("key").and_then(|v| v.as_str())),
                    "name" => builders.builder(i).append_string(p.get("name").and_then(|v| v.as_str())),
                    "description" => builders.builder(i).append_string(p.get("description").and_then(|v| v.as_str())),
                    "lead_id" => {
                        let v = p.get("lead").and_then(|l| l.get("accountId")).and_then(|v| v.as_str());
                        builders.builder(i).append_string(v);
                    }
                    "lead_name" => {
                        let v = p.get("lead").and_then(|l| l.get("displayName")).and_then(|v| v.as_str());
                        builders.builder(i).append_string(v);
                    }
                    "project_type" => builders.builder(i).append_string(p.get("projectTypeKey").and_then(|v| v.as_str())),
                    "url" => builders.builder(i).append_string(p.get("self").and_then(|v| v.as_str())),
                    _ => builders.builder(i).append_null(),
                }
            }
            builders.row_complete();
        }
        Self::finish_batches(builders, arrow_schema)
    }

    async fn fetch_boards(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut start_at = 0u64;

        loop {
            let data = self.api_get(&format!(
                "/rest/agile/1.0/board?startAt={}&maxResults=50", start_at
            )).await?;
            let boards = data.get("values").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let total = data.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
            if boards.is_empty() { break; }
            start_at += boards.len() as u64;

            for b in &boards {
                for (i, col) in schema.columns.iter().enumerate() {
                    match col.name.as_str() {
                        "id" => builders.builder(i).append_f64(b.get("id").and_then(|v| v.as_f64())),
                        "name" => builders.builder(i).append_string(b.get("name").and_then(|v| v.as_str())),
                        "board_type" => builders.builder(i).append_string(b.get("type").and_then(|v| v.as_str())),
                        "self_link" => builders.builder(i).append_string(b.get("self").and_then(|v| v.as_str())),
                        _ => builders.builder(i).append_null(),
                    }
                }
                builders.row_complete();
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            if start_at >= total { break; }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    async fn fetch_sprints(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        // First get all board IDs
        let mut board_ids: Vec<u64> = Vec::new();
        let mut start_at = 0u64;
        loop {
            let data = self.api_get(&format!(
                "/rest/agile/1.0/board?startAt={}&maxResults=50", start_at
            )).await?;
            let boards = data.get("values").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let total = data.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
            for b in &boards {
                if let Some(id) = b.get("id").and_then(|v| v.as_u64()) {
                    board_ids.push(id);
                }
            }
            if boards.is_empty() { break; }
            start_at += boards.len() as u64;
            if start_at >= total { break; }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();

        for board_id in &board_ids {
            let mut sprint_start = 0u64;
            loop {
                let result = self.api_get(&format!(
                    "/rest/agile/1.0/board/{}/sprint?startAt={}&maxResults=50",
                    board_id, sprint_start
                )).await;

                let data = match result {
                    Ok(d) => d,
                    Err(ConnectorError::Internal(ref msg)) if msg.contains("404") => break,
                    Err(e) => return Err(e),
                };

                let sprints = data.get("values").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                let total = data.get("total").and_then(|v| v.as_u64()).unwrap_or(0);

                for s in &sprints {
                    for (i, col) in schema.columns.iter().enumerate() {
                        match col.name.as_str() {
                            "id" => builders.builder(i).append_f64(s.get("id").and_then(|v| v.as_f64())),
                            "name" => builders.builder(i).append_string(s.get("name").and_then(|v| v.as_str())),
                            "state" => builders.builder(i).append_string(s.get("state").and_then(|v| v.as_str())),
                            "start_date" => builders.builder(i).append_string(s.get("startDate").and_then(|v| v.as_str())),
                            "end_date" => builders.builder(i).append_string(s.get("endDate").and_then(|v| v.as_str())),
                            "complete_date" => builders.builder(i).append_string(s.get("completeDate").and_then(|v| v.as_str())),
                            "board_id" => builders.builder(i).append_f64(Some(*board_id as f64)),
                            _ => builders.builder(i).append_null(),
                        }
                    }
                    builders.row_complete();
                }

                if builders.len() >= BATCH_THRESHOLD {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
                }

                if sprints.is_empty() { break; }
                sprint_start += sprints.len() as u64;
                if sprint_start >= total { break; }
                tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
            }
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    async fn fetch_users(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut start_at = 0u64;

        loop {
            let data = self.api_get(&format!(
                "/rest/api/3/users/search?startAt={}&maxResults=50", start_at
            )).await?;
            let users = data.as_array().cloned().unwrap_or_default();
            if users.is_empty() { break; }
            start_at += users.len() as u64;

            for u in &users {
                for (i, col) in schema.columns.iter().enumerate() {
                    match col.name.as_str() {
                        "account_id" => builders.builder(i).append_string(u.get("accountId").and_then(|v| v.as_str())),
                        "display_name" => builders.builder(i).append_string(u.get("displayName").and_then(|v| v.as_str())),
                        "email" => builders.builder(i).append_string(u.get("emailAddress").and_then(|v| v.as_str())),
                        "active" => builders.builder(i).append_bool(u.get("active").and_then(|v| v.as_bool())),
                        "account_type" => builders.builder(i).append_string(u.get("accountType").and_then(|v| v.as_str())),
                        _ => builders.builder(i).append_null(),
                    }
                }
                builders.row_complete();
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    async fn fetch_versions(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let projects_data = self.api_get("/rest/api/3/project").await?;
        let projects = projects_data.as_array().cloned().unwrap_or_default();

        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();

        for p in &projects {
            let project_key = p.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let project_id = p.get("id").and_then(|v| v.as_str()).unwrap_or("");
            if project_id.is_empty() { continue; }

            let mut start_at = 0u64;
            loop {
                let data = self.api_get(&format!(
                    "/rest/api/3/project/{}/version?startAt={}&maxResults=50",
                    project_id, start_at
                )).await?;

                let versions = data.get("values").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                let total = data.get("total").and_then(|v| v.as_u64()).unwrap_or(0);

                for v in &versions {
                    for (i, col) in schema.columns.iter().enumerate() {
                        match col.name.as_str() {
                            "id" => builders.builder(i).append_string(v.get("id").and_then(|x| x.as_str())),
                            "name" => builders.builder(i).append_string(v.get("name").and_then(|x| x.as_str())),
                            "description" => builders.builder(i).append_string(v.get("description").and_then(|x| x.as_str())),
                            "project_key" => builders.builder(i).append_string(Some(project_key)),
                            "released" => builders.builder(i).append_bool(v.get("released").and_then(|x| x.as_bool())),
                            "archived" => builders.builder(i).append_bool(v.get("archived").and_then(|x| x.as_bool())),
                            "release_date" => builders.builder(i).append_string(v.get("releaseDate").and_then(|x| x.as_str())),
                            "start_date" => builders.builder(i).append_string(v.get("startDate").and_then(|x| x.as_str())),
                            _ => builders.builder(i).append_null(),
                        }
                    }
                    builders.row_complete();
                }

                if builders.len() >= BATCH_THRESHOLD {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
                }

                if versions.is_empty() { break; }
                start_at += versions.len() as u64;
                if start_at >= total { break; }
                tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
            }
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    async fn fetch_components(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let projects_data = self.api_get("/rest/api/3/project").await?;
        let projects = projects_data.as_array().cloned().unwrap_or_default();

        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();

        for p in &projects {
            let project_key = p.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let project_id = p.get("id").and_then(|v| v.as_str()).unwrap_or("");
            if project_id.is_empty() { continue; }

            let data = self.api_get(&format!(
                "/rest/api/3/project/{}/components", project_id
            )).await?;
            let components = data.as_array().cloned().unwrap_or_default();

            for c in &components {
                for (i, col) in schema.columns.iter().enumerate() {
                    match col.name.as_str() {
                        "id" => builders.builder(i).append_string(c.get("id").and_then(|v| v.as_str())),
                        "name" => builders.builder(i).append_string(c.get("name").and_then(|v| v.as_str())),
                        "description" => builders.builder(i).append_string(c.get("description").and_then(|v| v.as_str())),
                        "project_key" => builders.builder(i).append_string(Some(project_key)),
                        "lead_id" => {
                            let v = c.get("lead").and_then(|l| l.get("accountId")).and_then(|v| v.as_str());
                            builders.builder(i).append_string(v);
                        }
                        "lead_name" => {
                            let v = c.get("lead").and_then(|l| l.get("displayName")).and_then(|v| v.as_str());
                            builders.builder(i).append_string(v);
                        }
                        _ => builders.builder(i).append_null(),
                    }
                }
                builders.row_complete();
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    // ════════════════════════════════════════════════════════════════════
    // Derived tables (comments, worklogs, changelogs, attachments)
    // ════════════════════════════════════════════════════════════════════

    async fn fetch_issue_keys_for_derived(
        &self,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<String>> {
        let jql = if let Some(ref last_value) = options.last_value {
            format!("updated >= \"{}\" ORDER BY updated ASC", Self::escape_jql_value(last_value))
        } else {
            "created IS NOT EMPTY ORDER BY updated ASC".to_string()
        };

        let client = self.build_gouqi_client()?;
        let mut keys: Vec<String> = Vec::new();
        let mut start_at: u64 = 0;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        loop {
            if keys.len() >= max_rows { break; }

            let search_opts = gouqi::SearchOptions::builder()
                .essential_fields()
                .max_results(PAGE_SIZE)
                .start_at(start_at)
                .build();

            let results = client
                .search()
                .list(&jql, &search_opts)
                .await
                .map_err(|e| ConnectorError::Internal(format!("Jira search failed: {}", e)))?;

            if results.issues.is_empty() { break; }
            for issue in &results.issues {
                keys.push(issue.key.clone());
            }
            start_at += results.issues.len() as u64;
            if start_at >= results.total { break; }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        Ok(keys)
    }

    async fn fetch_comments(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let issue_keys = self.fetch_issue_keys_for_derived(options).await?;
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();

        for key in &issue_keys {
            let data = self.api_get(&format!("/rest/api/3/issue/{}/comment", key)).await?;
            let comments = data.get("comments").and_then(|c| c.as_array()).cloned().unwrap_or_default();

            for c in &comments {
                for (i, col) in schema.columns.iter().enumerate() {
                    match col.name.as_str() {
                        "id" => builders.builder(i).append_string(c.get("id").and_then(|v| v.as_str())),
                        "issue_key" => builders.builder(i).append_string(Some(key.as_str())),
                        "author_id" => {
                            let v = c.get("author").and_then(|a| a.get("accountId")).and_then(|v| v.as_str());
                            builders.builder(i).append_string(v);
                        }
                        "author_name" => {
                            let v = c.get("author").and_then(|a| a.get("displayName")).and_then(|v| v.as_str());
                            builders.builder(i).append_string(v);
                        }
                        "body" => {
                            let v = c.get("body").map(|b| {
                                if let Some(s) = b.as_str() { s.to_string() }
                                else { b.to_string() }
                            });
                            builders.builder(i).append_string(v.as_deref());
                        }
                        "created" => {
                            let ts = c.get("created").and_then(Self::parse_timestamp_json);
                            builders.builder(i).append_timestamp(ts);
                        }
                        "updated" => {
                            let ts = c.get("updated").and_then(Self::parse_timestamp_json);
                            builders.builder(i).append_timestamp(ts);
                        }
                        _ => builders.builder(i).append_null(),
                    }
                }
                builders.row_complete();
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }
        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    async fn fetch_worklogs(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let issue_keys = self.fetch_issue_keys_for_derived(options).await?;
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();

        for key in &issue_keys {
            let data = self.api_get(&format!("/rest/api/3/issue/{}/worklog", key)).await?;
            let worklogs = data.get("worklogs").and_then(|w| w.as_array()).cloned().unwrap_or_default();

            for w in &worklogs {
                for (i, col) in schema.columns.iter().enumerate() {
                    match col.name.as_str() {
                        "id" => builders.builder(i).append_string(w.get("id").and_then(|v| v.as_str())),
                        "issue_key" => builders.builder(i).append_string(Some(key.as_str())),
                        "author_id" => {
                            let v = w.get("author").and_then(|a| a.get("accountId")).and_then(|v| v.as_str());
                            builders.builder(i).append_string(v);
                        }
                        "author_name" => {
                            let v = w.get("author").and_then(|a| a.get("displayName")).and_then(|v| v.as_str());
                            builders.builder(i).append_string(v);
                        }
                        "time_spent_seconds" => builders.builder(i).append_f64(w.get("timeSpentSeconds").and_then(|v| v.as_f64())),
                        "started" => builders.builder(i).append_string(w.get("started").and_then(|v| v.as_str())),
                        "created" => {
                            let ts = w.get("created").and_then(Self::parse_timestamp_json);
                            builders.builder(i).append_timestamp(ts);
                        }
                        "updated" => {
                            let ts = w.get("updated").and_then(Self::parse_timestamp_json);
                            builders.builder(i).append_timestamp(ts);
                        }
                        "comment" => {
                            let v = w.get("comment").map(|b| {
                                if let Some(s) = b.as_str() { s.to_string() }
                                else { b.to_string() }
                            });
                            builders.builder(i).append_string(v.as_deref());
                        }
                        _ => builders.builder(i).append_null(),
                    }
                }
                builders.row_complete();
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }
        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    async fn fetch_changelogs(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let issue_keys = self.fetch_issue_keys_for_derived(options).await?;
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();

        for key in &issue_keys {
            let data = self.api_get(&format!(
                "/rest/api/3/issue/{}?expand=changelog&fields=key", key
            )).await?;

            let histories = data
                .get("changelog")
                .and_then(|c| c.get("histories"))
                .and_then(|h| h.as_array())
                .cloned()
                .unwrap_or_default();

            for h in &histories {
                let history_id = h.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let author_id = h.get("author").and_then(|a| a.get("accountId")).and_then(|v| v.as_str());
                let author_name = h.get("author").and_then(|a| a.get("displayName")).and_then(|v| v.as_str());
                let created_ts = h.get("created").and_then(Self::parse_timestamp_json);

                let items = h.get("items").and_then(|i| i.as_array()).cloned().unwrap_or_default();
                for item in &items {
                    for (i, col) in schema.columns.iter().enumerate() {
                        match col.name.as_str() {
                            "id" => builders.builder(i).append_string(Some(history_id)),
                            "issue_key" => builders.builder(i).append_string(Some(key.as_str())),
                            "author_id" => builders.builder(i).append_string(author_id),
                            "author_name" => builders.builder(i).append_string(author_name),
                            "created" => builders.builder(i).append_timestamp(created_ts),
                            "field" => builders.builder(i).append_string(item.get("field").and_then(|v| v.as_str())),
                            "from_value" => builders.builder(i).append_string(item.get("fromString").and_then(|v| v.as_str())),
                            "to_value" => builders.builder(i).append_string(item.get("toString").and_then(|v| v.as_str())),
                            _ => builders.builder(i).append_null(),
                        }
                    }
                    builders.row_complete();
                }
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }
        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    async fn fetch_attachments(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let issue_keys = self.fetch_issue_keys_for_derived(options).await?;
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();

        for key in &issue_keys {
            let data = self.api_get(&format!(
                "/rest/api/3/issue/{}?fields=attachment", key
            )).await?;

            let attachments = data
                .get("fields")
                .and_then(|f| f.get("attachment"))
                .and_then(|a| a.as_array())
                .cloned()
                .unwrap_or_default();

            for a in &attachments {
                for (i, col) in schema.columns.iter().enumerate() {
                    match col.name.as_str() {
                        "id" => builders.builder(i).append_string(a.get("id").and_then(|v| v.as_str())),
                        "issue_key" => builders.builder(i).append_string(Some(key.as_str())),
                        "filename" => builders.builder(i).append_string(a.get("filename").and_then(|v| v.as_str())),
                        "author_id" => {
                            let v = a.get("author").and_then(|au| au.get("accountId")).and_then(|v| v.as_str());
                            builders.builder(i).append_string(v);
                        }
                        "author_name" => {
                            let v = a.get("author").and_then(|au| au.get("displayName")).and_then(|v| v.as_str());
                            builders.builder(i).append_string(v);
                        }
                        "created" => {
                            let ts = a.get("created").and_then(Self::parse_timestamp_json);
                            builders.builder(i).append_timestamp(ts);
                        }
                        "size" => builders.builder(i).append_f64(a.get("size").and_then(|v| v.as_f64())),
                        "mime_type" => builders.builder(i).append_string(a.get("mimeType").and_then(|v| v.as_str())),
                        "content_url" => builders.builder(i).append_string(a.get("content").and_then(|v| v.as_str())),
                        _ => builders.builder(i).append_null(),
                    }
                }
                builders.row_complete();
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }
        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    // ════════════════════════════════════════════════════════════════════
    // Lookup tables (reqwest direct)
    // ════════════════════════════════════════════════════════════════════

    async fn fetch_lookup_table(
        &self,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        match table {
            "statuses" => self.fetch_statuses(schema, arrow_schema).await,
            "priorities" => self.fetch_simple_list("/rest/api/3/priority", schema, arrow_schema, Self::append_priority_row).await,
            "issue_types" => self.fetch_simple_list("/rest/api/3/issuetype", schema, arrow_schema, Self::append_issue_type_row).await,
            "resolutions" => self.fetch_simple_list("/rest/api/3/resolution/search", schema, arrow_schema, Self::append_resolution_row).await,
            "labels" => self.fetch_labels(schema, arrow_schema).await,
            "fields" => self.fetch_fields(schema, arrow_schema).await,
            "filters" => self.fetch_filters(schema, arrow_schema).await,
            "roles" => self.fetch_roles(schema, arrow_schema).await,
            _ => Err(ConnectorError::TableNotFound(table.to_string())),
        }
    }

    async fn fetch_statuses(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let data = self.api_get("/rest/api/3/status").await?;
        let statuses = data.as_array().cloned().unwrap_or_default();

        let mut builders = ColumnBuilders::new(schema, statuses.len().max(1));
        for s in &statuses {
            for (i, col) in schema.columns.iter().enumerate() {
                match col.name.as_str() {
                    "id" => builders.builder(i).append_string(s.get("id").and_then(|v| v.as_str())),
                    "name" => builders.builder(i).append_string(s.get("name").and_then(|v| v.as_str())),
                    "description" => builders.builder(i).append_string(s.get("description").and_then(|v| v.as_str())),
                    "category_key" => {
                        let v = s.get("statusCategory").and_then(|c| c.get("key")).and_then(|v| v.as_str());
                        builders.builder(i).append_string(v);
                    }
                    "category_name" => {
                        let v = s.get("statusCategory").and_then(|c| c.get("name")).and_then(|v| v.as_str());
                        builders.builder(i).append_string(v);
                    }
                    _ => builders.builder(i).append_null(),
                }
            }
            builders.row_complete();
        }
        Self::finish_batches(builders, arrow_schema)
    }

    async fn fetch_simple_list<F>(
        &self,
        path: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        append_fn: F,
    ) -> ConnectorResult<Vec<RecordBatch>>
    where
        F: Fn(&serde_json::Value, &TableSchema, &mut ColumnBuilders),
    {
        let data = self.api_get(path).await?;
        let items = if let Some(arr) = data.as_array() {
            arr.clone()
        } else if let Some(arr) = data.get("values").and_then(|v| v.as_array()) {
            arr.clone()
        } else {
            Vec::new()
        };

        let mut builders = ColumnBuilders::new(schema, items.len().max(1));
        for item in &items {
            append_fn(item, schema, &mut builders);
        }
        Self::finish_batches(builders, arrow_schema)
    }

    fn append_priority_row(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_string(item.get("id").and_then(|v| v.as_str())),
                "name" => builders.builder(i).append_string(item.get("name").and_then(|v| v.as_str())),
                "description" => builders.builder(i).append_string(item.get("description").and_then(|v| v.as_str())),
                "icon_url" => builders.builder(i).append_string(item.get("iconUrl").and_then(|v| v.as_str())),
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_issue_type_row(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_string(item.get("id").and_then(|v| v.as_str())),
                "name" => builders.builder(i).append_string(item.get("name").and_then(|v| v.as_str())),
                "description" => builders.builder(i).append_string(item.get("description").and_then(|v| v.as_str())),
                "subtask" => builders.builder(i).append_bool(item.get("subtask").and_then(|v| v.as_bool())),
                "icon_url" => builders.builder(i).append_string(item.get("iconUrl").and_then(|v| v.as_str())),
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_resolution_row(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_string(item.get("id").and_then(|v| v.as_str())),
                "name" => builders.builder(i).append_string(item.get("name").and_then(|v| v.as_str())),
                "description" => builders.builder(i).append_string(item.get("description").and_then(|v| v.as_str())),
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    async fn fetch_labels(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let page_size = 1000;
        let mut start_at: u64 = 0;
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();

        loop {
            let data = self.api_get(&format!(
                "/rest/api/3/label?maxResults={}&startAt={}", page_size, start_at
            )).await?;
            let labels = data.get("values").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let total = data.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
            let count = labels.len() as u64;

            for label in &labels {
                if let Some(name) = label.as_str() {
                    builders.builder(0).append_string(Some(name));
                    builders.row_complete();
                }
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            start_at += count;
            if count == 0 || start_at >= total { break; }
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    async fn fetch_fields(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let data = self.api_get("/rest/api/3/field").await?;
        let fields = data.as_array().cloned().unwrap_or_default();

        let mut builders = ColumnBuilders::new(schema, fields.len().max(1));
        for f in &fields {
            for (i, col) in schema.columns.iter().enumerate() {
                match col.name.as_str() {
                    "id" => builders.builder(i).append_string(f.get("id").and_then(|v| v.as_str())),
                    "name" => builders.builder(i).append_string(f.get("name").and_then(|v| v.as_str())),
                    "custom" => builders.builder(i).append_bool(f.get("custom").and_then(|v| v.as_bool())),
                    "schema_type" => {
                        let v = f.get("schema").and_then(|s| s.get("type")).and_then(|v| v.as_str());
                        builders.builder(i).append_string(v);
                    }
                    _ => builders.builder(i).append_null(),
                }
            }
            builders.row_complete();
        }
        Self::finish_batches(builders, arrow_schema)
    }

    async fn fetch_filters(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let page_size = 100;
        let mut start_at: u64 = 0;
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();

        loop {
            let data = self.api_get(&format!(
                "/rest/api/3/filter/search?maxResults={}&startAt={}&expand=jql", page_size, start_at
            )).await?;
            let filters = data.get("values").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let total = data.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
            let count = filters.len() as u64;

            for f in &filters {
                for (i, col) in schema.columns.iter().enumerate() {
                    match col.name.as_str() {
                        "id" => builders.builder(i).append_string(f.get("id").and_then(|v| v.as_str())),
                        "name" => builders.builder(i).append_string(f.get("name").and_then(|v| v.as_str())),
                        "jql" => builders.builder(i).append_string(f.get("jql").and_then(|v| v.as_str())),
                        "owner_name" => {
                            let v = f.get("owner").and_then(|o| o.get("displayName")).and_then(|v| v.as_str());
                            builders.builder(i).append_string(v);
                        }
                        "favourite" => builders.builder(i).append_bool(f.get("favourite").and_then(|v| v.as_bool())),
                        _ => builders.builder(i).append_null(),
                    }
                }
                builders.row_complete();
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            start_at += count;
            if count == 0 || start_at >= total { break; }
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }
        Ok(batches)
    }

    async fn fetch_roles(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let data = self.api_get("/rest/api/3/role").await?;
        let roles = data.as_array().cloned().unwrap_or_default();

        let mut builders = ColumnBuilders::new(schema, roles.len().max(1));
        for r in &roles {
            for (i, col) in schema.columns.iter().enumerate() {
                match col.name.as_str() {
                    "id" => builders.builder(i).append_f64(r.get("id").and_then(|v| v.as_f64())),
                    "name" => builders.builder(i).append_string(r.get("name").and_then(|v| v.as_str())),
                    "description" => builders.builder(i).append_string(r.get("description").and_then(|v| v.as_str())),
                    _ => builders.builder(i).append_null(),
                }
            }
            builders.row_complete();
        }
        Self::finish_batches(builders, arrow_schema)
    }

    // ════════════════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════════════════

    fn finish_batches(
        builders: ColumnBuilders,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        if builders.len() > 0 {
            let batch = builders.finish(arrow_schema)?;
            Ok(vec![batch])
        } else {
            Ok(vec![RecordBatch::new_empty(arrow_schema)])
        }
    }

    async fn do_fetch(
        &self,
        table: &str,
        options: FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let schema = Self::get_table_schema(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        match table {
            "issues" => self.fetch_issues(&schema, arrow_schema, &options).await,
            "projects" => self.fetch_projects(&schema, arrow_schema).await,
            "boards" => self.fetch_boards(&schema, arrow_schema).await,
            "sprints" => self.fetch_sprints(&schema, arrow_schema).await,
            "users" => self.fetch_users(&schema, arrow_schema).await,
            "versions" => self.fetch_versions(&schema, arrow_schema).await,
            "components" => self.fetch_components(&schema, arrow_schema).await,
            "comments" => self.fetch_comments(&schema, arrow_schema, &options).await,
            "worklogs" => self.fetch_worklogs(&schema, arrow_schema, &options).await,
            "changelogs" => self.fetch_changelogs(&schema, arrow_schema, &options).await,
            "attachments" => self.fetch_attachments(&schema, arrow_schema, &options).await,
            "statuses" | "priorities" | "issue_types" | "resolutions"
            | "labels" | "fields" | "filters" | "roles" => {
                self.fetch_lookup_table(table, &schema, arrow_schema).await
            }
            _ => Err(ConnectorError::TableNotFound(table.to_string())),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Connector trait
// ═══════════════════════════════════════════════════════════════════════════

#[async_trait]
impl Connector for JiraConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Jira
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        Ok(TABLES
            .iter()
            .filter_map(|&table| {
                let schema = Self::get_table_schema(table)?;
                let (supports_incremental, incremental_key) = match table {
                    "issues" => (true, Some("updated".to_string())),
                    _ => (false, None),
                };
                let pk = match table {
                    "labels" => vec!["name".to_string()],
                    "changelogs" => vec!["id".to_string(), "field".to_string()],
                    "issues" => vec!["key".to_string()],
                    _ => vec!["id".to_string()],
                };
                Some(TableInfo {
                    name: table.to_string(),
                    schema,
                    supports_incremental,
                    incremental_key,
                    estimated_rows: None,
                    primary_key_columns: pk,
                })
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        Self::get_table_schema(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            let batches = self.do_fetch(table, options).await?;
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        self.api_get("/rest/api/3/myself").await?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(base_url: &str) -> JiraConfig {
        JiraConfig::with_basic_auth(base_url, "user@test.com", "test_token")
    }

    fn test_connector(base_url: &str) -> JiraConnector {
        JiraConnector::new(test_config(base_url))
    }

    // ── Schema tests ─────────────────────────────────────────────────

    #[test]
    fn test_all_tables_have_schemas() {
        for table in TABLES {
            assert!(
                JiraConnector::get_table_schema(table).is_some(),
                "Missing schema for table: {}",
                table
            );
        }
    }

    #[test]
    fn test_issues_schema_columns() {
        let schema = JiraConnector::get_table_schema("issues").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"key"));
        assert!(names.contains(&"summary"));
        assert!(names.contains(&"status_name"));
        assert!(names.contains(&"created"));
        assert!(names.contains(&"updated"));
    }

    #[test]
    fn test_unknown_table_returns_none() {
        assert!(JiraConnector::get_table_schema("nonexistent").is_none());
    }

    // ── Config tests ─────────────────────────────────────────────────

    #[test]
    fn test_config_debug_redacts() {
        let config = JiraConfig::with_basic_auth("https://jira.example.com", "user@test.com", "secret");
        let debug = format!("{:?}", config);
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn test_basic_auth_credentials() {
        let config = JiraConfig::with_basic_auth("https://j.com", "a@b.com", "tok");
        let creds = config.credentials().unwrap();
        match creds {
            gouqi::Credentials::Basic(email, token) => {
                assert_eq!(email, "a@b.com");
                assert_eq!(token, "tok");
            }
            _ => panic!("Expected Basic credentials"),
        }
    }

    #[test]
    fn test_pat_credentials() {
        let config = JiraConfig::with_pat("https://j.com", "my_pat");
        let creds = config.credentials().unwrap();
        match creds {
            gouqi::Credentials::Bearer(token) => assert_eq!(token, "my_pat"),
            _ => panic!("Expected Bearer credentials"),
        }
    }

    // ── JQL translation tests ────────────────────────────────────────

    #[test]
    fn test_jql_equals() {
        let preds = vec![Predicate::Equals {
            column: "status_name".into(),
            value: "Done".into(),
        }];
        let jql = JiraConnector::predicates_to_jql(&preds);
        assert_eq!(jql, Some("status = \"Done\"".to_string()));
    }

    #[test]
    fn test_jql_in() {
        let preds = vec![Predicate::In {
            column: "project_key".into(),
            values: vec!["PROJ".into(), "DEMO".into()],
        }];
        let jql = JiraConnector::predicates_to_jql(&preds);
        assert_eq!(jql, Some("project IN (\"PROJ\",\"DEMO\")".to_string()));
    }

    #[test]
    fn test_jql_contains() {
        let preds = vec![Predicate::Contains {
            column: "summary".into(),
            substring: "bug".into(),
        }];
        let jql = JiraConnector::predicates_to_jql(&preds);
        assert_eq!(jql, Some("summary ~ \"bug\"".to_string()));
    }

    #[test]
    fn test_jql_is_null() {
        let preds = vec![Predicate::IsNull {
            column: "resolution".into(),
            is_null: true,
        }];
        let jql = JiraConnector::predicates_to_jql(&preds);
        assert_eq!(jql, Some("resolution IS EMPTY".to_string()));
    }

    #[test]
    fn test_jql_not_equals() {
        let preds = vec![Predicate::Not(Box::new(Predicate::Equals {
            column: "status_name".into(),
            value: "Closed".into(),
        }))];
        let jql = JiraConnector::predicates_to_jql(&preds);
        assert_eq!(jql, Some("status != \"Closed\"".to_string()));
    }

    #[test]
    fn test_jql_compound_and() {
        let preds = vec![
            Predicate::Equals { column: "project_key".into(), value: "PROJ".into() },
            Predicate::Equals { column: "status_name".into(), value: "Open".into() },
        ];
        let jql = JiraConnector::predicates_to_jql(&preds);
        assert_eq!(
            jql,
            Some("project = \"PROJ\" AND status = \"Open\"".to_string())
        );
    }

    #[test]
    fn test_jql_greater_less_than() {
        let preds = vec![
            Predicate::GreaterThan { column: "created".into(), value: "2024-01-01".into(), inclusive: true },
            Predicate::LessThan { column: "created".into(), value: "2024-12-31".into(), inclusive: false },
        ];
        let jql = JiraConnector::predicates_to_jql(&preds);
        assert_eq!(
            jql,
            Some("created >= \"2024-01-01\" AND created < \"2024-12-31\"".to_string())
        );
    }

    #[test]
    fn test_jql_empty_predicates() {
        let jql = JiraConnector::predicates_to_jql(&[]);
        assert_eq!(jql, None);
    }

    #[test]
    fn test_escape_jql_value() {
        assert_eq!(JiraConnector::escape_jql_value("hello"), "hello");
        assert_eq!(JiraConnector::escape_jql_value("say \"hi\""), "say \\\"hi\\\"");
        assert_eq!(JiraConnector::escape_jql_value("a\\b"), "a\\\\b");
    }

    // ── API tests with wiremock ───────────────────────────────────────

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path("/rest/api/3/myself"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "accountId": "abc123",
                "displayName": "Test User"
            })))
            .mount(&server)
            .await;

        let result = connector.validate_credentials().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_credentials_auth_failure() {
        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path("/rest/api/3/myself"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_fetch_statuses() {
        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path("/rest/api/3/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "id": "1", "name": "Open", "description": "Issue is open",
                    "statusCategory": {"key": "new", "name": "To Do"}
                },
                {
                    "id": "3", "name": "Done", "description": "Issue is done",
                    "statusCategory": {"key": "done", "name": "Done"}
                }
            ])))
            .mount(&server)
            .await;

        let schema = JiraConnector::get_table_schema("statuses").unwrap();
        let arrow_schema = Arc::new(JiraConnector::to_arrow_schema(&schema));
        let batches = connector.fetch_statuses(&schema, arrow_schema).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_fetch_projects() {
        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path("/rest/api/3/project"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "id": "10001", "key": "PROJ", "name": "Project One",
                    "description": "A project",
                    "lead": {"accountId": "user1", "displayName": "Lead User"},
                    "projectTypeKey": "software",
                    "self": "https://jira.example.com/rest/api/3/project/10001"
                }
            ])))
            .mount(&server)
            .await;

        let schema = JiraConnector::get_table_schema("projects").unwrap();
        let arrow_schema = Arc::new(JiraConnector::to_arrow_schema(&schema));
        let batches = connector.fetch_projects(&schema, arrow_schema).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_fetch_labels() {
        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path("/rest/api/3/label"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "values": ["bug", "feature", "urgent"],
                "total": 3
            })))
            .mount(&server)
            .await;

        let schema = JiraConnector::get_table_schema("labels").unwrap();
        let arrow_schema = Arc::new(JiraConnector::to_arrow_schema(&schema));
        let batches = connector.fetch_labels(&schema, arrow_schema).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_rate_limit_retry() {
        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path("/rest/api/3/myself"))
            .respond_with(ResponseTemplate::new(429))
            .expect(4)
            .mount(&server)
            .await;

        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::RateLimited { .. })));
    }

    #[test]
    fn test_timestamp_parsing() {
        let ts = JiraConnector::parse_timestamp_str("2024-06-15T10:30:00.000+0000");
        assert!(ts.is_some());

        let ts = JiraConnector::parse_timestamp_str("2024-06-15T10:30:00.000Z");
        assert!(ts.is_some());

        assert!(JiraConnector::parse_timestamp_str("not-a-date").is_none());
    }

    #[test]
    fn test_column_to_jql_field_mapping() {
        assert_eq!(JiraConnector::column_to_jql_field("status_name"), "status");
        assert_eq!(JiraConnector::column_to_jql_field("priority_name"), "priority");
        assert_eq!(JiraConnector::column_to_jql_field("issue_type_name"), "issuetype");
        assert_eq!(JiraConnector::column_to_jql_field("project_key"), "project");
        assert_eq!(JiraConnector::column_to_jql_field("assignee_id"), "assignee");
        assert_eq!(JiraConnector::column_to_jql_field("due_date"), "duedate");
        assert_eq!(JiraConnector::column_to_jql_field("unknown_col"), "unknown_col");
    }

    // ── T1: append_issue tests ────────────────────────────────────────

    #[test]
    fn test_append_issue_basic_fields() {
        use arrow::array::Array;
        use std::collections::BTreeMap;

        let schema = JiraConnector::get_table_schema("issues").unwrap();
        let mut builders = ColumnBuilders::new(&schema, 10);

        let mut fields = BTreeMap::new();
        fields.insert("summary".to_string(), serde_json::json!("Fix login bug"));
        fields.insert("description".to_string(), serde_json::json!("Users cannot log in"));
        fields.insert("status".to_string(), serde_json::json!({
            "id": "3", "name": "In Progress", "description": "Work in progress",
            "iconUrl": "https://jira.example.com/status.png", "self": "https://jira.example.com/rest/api/3/status/3",
            "statusCategory": {"name": "In Progress", "key": "indeterminate", "id": 4}
        }));
        fields.insert("priority".to_string(), serde_json::json!({
            "id": "2", "name": "High", "description": "High priority",
            "iconUrl": "https://jira.example.com/priority.png", "self": "https://jira.example.com/rest/api/3/priority/2"
        }));
        fields.insert("issuetype".to_string(), serde_json::json!({
            "id": "1", "name": "Bug", "description": "A bug", "subtask": false,
            "iconUrl": "https://jira.example.com/issuetype.png", "self": "https://jira.example.com/rest/api/3/issuetype/1"
        }));
        fields.insert("project".to_string(), serde_json::json!({
            "id": "10001", "key": "PROJ", "name": "Project", "projectTypeKey": "software",
            "self": "https://jira.example.com/rest/api/3/project/10001"
        }));
        fields.insert("assignee".to_string(), serde_json::json!({
            "accountId": "user-1", "displayName": "Alice", "active": true,
            "self": "https://jira.example.com/rest/api/3/user?accountId=user-1"
        }));
        fields.insert("reporter".to_string(), serde_json::json!({
            "accountId": "user-2", "displayName": "Bob", "active": true,
            "self": "https://jira.example.com/rest/api/3/user?accountId=user-2"
        }));
        fields.insert("creator".to_string(), serde_json::json!({
            "accountId": "user-3", "displayName": "Charlie", "active": true,
            "self": "https://jira.example.com/rest/api/3/user?accountId=user-3"
        }));
        fields.insert("labels".to_string(), serde_json::json!(["bug", "urgent"]));
        fields.insert("resolution".to_string(), serde_json::json!({"name": "Done"}));
        fields.insert("parent".to_string(), serde_json::json!({"key": "PROJ-1"}));
        fields.insert("duedate".to_string(), serde_json::json!("2024-12-31"));

        let issue = gouqi::Issue {
            self_link: "https://jira.example.com/rest/api/3/issue/10001".to_string(),
            key: "PROJ-42".to_string(),
            id: "10001".to_string(),
            fields,
        };

        JiraConnector::append_issue(&issue, &schema, &mut builders);

        let arrow_schema = Arc::new(JiraConnector::to_arrow_schema(&schema));
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let col_idx = |name: &str| schema.columns.iter().position(|c| c.name == name).unwrap();
        let str_val = |name: &str| -> Option<String> {
            let arr = batch.column(col_idx(name));
            let sa = arr.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            sa.is_valid(0).then(|| sa.value(0).to_string())
        };

        assert_eq!(str_val("id"), Some("10001".to_string()));
        assert_eq!(str_val("key"), Some("PROJ-42".to_string()));
        assert_eq!(str_val("summary"), Some("Fix login bug".to_string()));
        assert_eq!(str_val("status_name"), Some("In Progress".to_string()));
        assert_eq!(str_val("status_category"), Some("In Progress".to_string()));
        assert_eq!(str_val("priority_name"), Some("High".to_string()));
        assert_eq!(str_val("issue_type_name"), Some("Bug".to_string()));
        assert_eq!(str_val("project_key"), Some("PROJ".to_string()));
        assert_eq!(str_val("assignee_id"), Some("user-1".to_string()));
        assert_eq!(str_val("assignee_name"), Some("Alice".to_string()));
        assert_eq!(str_val("reporter_id"), Some("user-2".to_string()));
        assert_eq!(str_val("reporter_name"), Some("Bob".to_string()));
        assert_eq!(str_val("creator_id"), Some("user-3".to_string()));
        assert_eq!(str_val("creator_name"), Some("Charlie".to_string()));
        assert_eq!(str_val("labels"), Some("bug,urgent".to_string()));
        assert_eq!(str_val("resolution"), Some("Done".to_string()));
        assert_eq!(str_val("parent_key"), Some("PROJ-1".to_string()));
        assert_eq!(str_val("due_date"), Some("2024-12-31".to_string()));
        assert_eq!(str_val("url"), Some("https://jira.example.com/rest/api/3/issue/10001".to_string()));
    }

    #[test]
    fn test_append_issue_nullable_fields() {
        use arrow::array::Array;
        use std::collections::BTreeMap;

        let schema = JiraConnector::get_table_schema("issues").unwrap();
        let mut builders = ColumnBuilders::new(&schema, 10);

        let issue = gouqi::Issue {
            self_link: "https://jira.example.com/rest/api/3/issue/10002".to_string(),
            key: "PROJ-99".to_string(),
            id: "10002".to_string(),
            fields: BTreeMap::new(),
        };

        JiraConnector::append_issue(&issue, &schema, &mut builders);

        let arrow_schema = Arc::new(JiraConnector::to_arrow_schema(&schema));
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let col_idx = |name: &str| schema.columns.iter().position(|c| c.name == name).unwrap();
        let arr = batch.column(col_idx("summary"));
        let sa = arr.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert!(sa.is_null(0));

        let arr = batch.column(col_idx("assignee_id"));
        let sa = arr.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert!(sa.is_null(0));
    }

    // ── T2: Derived table (comments) tests ────────────────────────────

    #[tokio::test]
    async fn test_fetch_comments() {
        use arrow::array::Array;
        use wiremock::matchers::path_regex;

        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path_regex("/rest/api/latest/search.*"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "total": 1,
                "startAt": 0,
                "maxResults": 50,
                "issues": [
                    {
                        "self": "https://jira.example.com/rest/api/3/issue/10001",
                        "id": "10001",
                        "key": "PROJ-1",
                        "fields": {}
                    }
                ]
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/rest/api/3/issue/PROJ-1/comment"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "comments": [
                    {
                        "id": "100",
                        "author": {"accountId": "user-1", "displayName": "Alice"},
                        "body": "This is a comment",
                        "created": "2024-06-15T10:30:00.000+0000",
                        "updated": "2024-06-15T11:00:00.000+0000"
                    },
                    {
                        "id": "101",
                        "author": {"accountId": "user-2", "displayName": "Bob"},
                        "body": "Another comment",
                        "created": "2024-06-16T08:00:00.000+0000",
                        "updated": "2024-06-16T08:00:00.000+0000"
                    }
                ]
            })))
            .mount(&server)
            .await;

        let schema = JiraConnector::get_table_schema("comments").unwrap();
        let arrow_schema = Arc::new(JiraConnector::to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();
        let batches = connector.fetch_comments(&schema, arrow_schema, &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let batch = &batches[0];
        let col_idx = |name: &str| schema.columns.iter().position(|c| c.name == name).unwrap();
        let str_val = |name: &str, row: usize| -> Option<String> {
            let arr = batch.column(col_idx(name));
            let sa = arr.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            sa.is_valid(row).then(|| sa.value(row).to_string())
        };

        assert_eq!(str_val("id", 0), Some("100".to_string()));
        assert_eq!(str_val("issue_key", 0), Some("PROJ-1".to_string()));
        assert_eq!(str_val("author_id", 0), Some("user-1".to_string()));
        assert_eq!(str_val("author_name", 0), Some("Alice".to_string()));
        assert_eq!(str_val("id", 1), Some("101".to_string()));
        assert_eq!(str_val("issue_key", 1), Some("PROJ-1".to_string()));
    }

    // ── T3: Boards and Sprints tests ──────────────────────────────────

    #[tokio::test]
    async fn test_fetch_boards() {
        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path("/rest/agile/1.0/board"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "values": [
                    {"id": 1, "name": "Scrum Board", "type": "scrum", "self": "https://jira.example.com/board/1"},
                    {"id": 2, "name": "Kanban Board", "type": "kanban", "self": "https://jira.example.com/board/2"}
                ],
                "total": 2
            })))
            .mount(&server)
            .await;

        let schema = JiraConnector::get_table_schema("boards").unwrap();
        let arrow_schema = Arc::new(JiraConnector::to_arrow_schema(&schema));
        let batches = connector.fetch_boards(&schema, arrow_schema).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        let col_idx = |name: &str| schema.columns.iter().position(|c| c.name == name).unwrap();
        let batch = &batches[0];
        let names = batch.column(col_idx("name"))
            .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(names.value(0), "Scrum Board");
        assert_eq!(names.value(1), "Kanban Board");

        let types = batch.column(col_idx("board_type"))
            .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(types.value(0), "scrum");
        assert_eq!(types.value(1), "kanban");
    }

    #[tokio::test]
    async fn test_fetch_sprints() {
        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path("/rest/agile/1.0/board"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "values": [{"id": 1, "name": "Board 1", "type": "scrum"}],
                "total": 1
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/rest/agile/1.0/board/1/sprint"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "values": [
                    {
                        "id": 10, "name": "Sprint 1", "state": "active",
                        "startDate": "2024-01-01T00:00:00.000Z",
                        "endDate": "2024-01-14T00:00:00.000Z"
                    },
                    {
                        "id": 11, "name": "Sprint 2", "state": "closed",
                        "startDate": "2024-01-15T00:00:00.000Z",
                        "endDate": "2024-01-28T00:00:00.000Z",
                        "completeDate": "2024-01-27T00:00:00.000Z"
                    }
                ],
                "total": 2
            })))
            .mount(&server)
            .await;

        let schema = JiraConnector::get_table_schema("sprints").unwrap();
        let arrow_schema = Arc::new(JiraConnector::to_arrow_schema(&schema));
        let batches = connector.fetch_sprints(&schema, arrow_schema).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let batch = &batches[0];
        let col_idx = |name: &str| schema.columns.iter().position(|c| c.name == name).unwrap();
        let names = batch.column(col_idx("name"))
            .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(names.value(0), "Sprint 1");
        assert_eq!(names.value(1), "Sprint 2");

        let states = batch.column(col_idx("state"))
            .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(states.value(0), "active");
        assert_eq!(states.value(1), "closed");
    }

    // ── T4: Incremental sync tests ────────────────────────────────────

    #[tokio::test]
    async fn test_fetch_issues_incremental() {
        use wiremock::matchers::path_regex;

        let server = MockServer::start().await;
        let connector = test_connector(&server.uri());

        Mock::given(method("GET"))
            .and(path_regex("/rest/api/latest/search.*"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "total": 1,
                "startAt": 0,
                "maxResults": 50,
                "issues": [
                    {
                        "self": "https://jira.example.com/rest/api/3/issue/10001",
                        "id": "10001",
                        "key": "PROJ-1",
                        "fields": {
                            "summary": "Updated issue",
                            "status": {"name": "Done", "statusCategory": {"name": "Done"}},
                            "issuetype": {"name": "Task", "id": "10002", "description": "", "iconUrl": "", "self": "", "subtask": false}
                        }
                    }
                ]
            })))
            .mount(&server)
            .await;

        let schema = JiraConnector::get_table_schema("issues").unwrap();
        let arrow_schema = Arc::new(JiraConnector::to_arrow_schema(&schema));
        let options = FetchOptions::incremental("updated", "2024-06-01");
        let batches = connector.fetch_issues(&schema, arrow_schema, &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);

        let batch = &batches[0];
        let col_idx = |name: &str| schema.columns.iter().position(|c| c.name == name).unwrap();
        let summary = batch.column(col_idx("summary"))
            .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(summary.value(0), "Updated issue");
    }

    #[test]
    fn test_full_sync_jql_has_predicate() {
        let preds: Vec<Predicate> = vec![];
        let jql = JiraConnector::predicates_to_jql(&preds);
        assert_eq!(jql, None);
    }

    #[tokio::test]
    async fn test_list_tables_incremental_only_on_issues() {
        let connector = test_connector("https://jira.example.com");
        let tables = connector.list_tables().await.unwrap();

        for table in &tables {
            if table.name == "issues" {
                assert!(table.supports_incremental);
                assert_eq!(table.incremental_key, Some("updated".to_string()));
            } else {
                assert!(!table.supports_incremental, "Table {} should not support incremental", table.name);
                assert_eq!(table.incremental_key, None, "Table {} should not have incremental key", table.name);
            }
        }
    }
}
