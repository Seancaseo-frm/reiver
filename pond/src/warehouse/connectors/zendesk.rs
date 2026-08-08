//! Zendesk Support connector for the data warehouse.
//!
//! Syncs Zendesk Support data (tickets, users, organizations, groups, etc.)
//! via the REST API v2. Uses direct `reqwest` calls with cursor-based
//! pagination and the Incremental Exports API for efficient syncing of
//! core tables. Supports 14 tables, API token and OAuth authentication,
//! and incremental sync on tickets, users, and organizations.

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
const PAGE_SIZE: u64 = 100;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 100;

const TABLES: &[&str] = &[
    "tickets",
    "users",
    "organizations",
    "groups",
    "brands",
    "ticket_fields",
    "ticket_forms",
    "ticket_metrics",
    "satisfaction_ratings",
    "tags",
    "macros",
    "views",
    "sla_policies",
    "automations",
];

// ═══════════════════════════════════════════════════════════════════════════
// Config
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone)]
pub struct ZendeskConfig {
    pub subdomain: String,
    pub email: Option<String>,
    pub api_token: Option<SecretString>,
    pub oauth_token: Option<SecretString>,
}

impl std::fmt::Debug for ZendeskConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZendeskConfig")
            .field("subdomain", &self.subdomain)
            .field("email", &self.email)
            .field("api_token", &"***REDACTED***")
            .field("oauth_token", &"***REDACTED***")
            .finish()
    }
}

impl ZendeskConfig {
    pub fn with_api_token(
        subdomain: impl Into<String>,
        email: impl Into<String>,
        api_token: impl Into<String>,
    ) -> Self {
        Self {
            subdomain: subdomain.into(),
            email: Some(email.into()),
            api_token: Some(SecretString::new(api_token)),
            oauth_token: None,
        }
    }

    pub fn with_oauth(subdomain: impl Into<String>, oauth_token: impl Into<String>) -> Self {
        Self {
            subdomain: subdomain.into(),
            email: None,
            api_token: None,
            oauth_token: Some(SecretString::new(oauth_token)),
        }
    }

    fn base_url(&self) -> String {
        format!("https://{}.zendesk.com", self.subdomain)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Connector
// ═══════════════════════════════════════════════════════════════════════════

pub struct ZendeskConnector {
    config: ZendeskConfig,
    http: reqwest::Client,
    #[cfg(test)]
    base_url_override: Option<String>,
}

impl ZendeskConnector {
    pub fn new(config: ZendeskConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
            #[cfg(test)]
            base_url_override: None,
        }
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
    // Static table schemas (14 tables)
    // ════════════════════════════════════════════════════════════════════

    fn get_table_schema(table: &str) -> Option<TableSchema> {
        let columns = match table {
            "tickets" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("subject", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("status", ColumnType::String, true),
                ColumnSchema::new("priority", ColumnType::String, true),
                ColumnSchema::new("ticket_type", ColumnType::String, true),
                ColumnSchema::new("requester_id", ColumnType::Float64, true),
                ColumnSchema::new("submitter_id", ColumnType::Float64, true),
                ColumnSchema::new("assignee_id", ColumnType::Float64, true),
                ColumnSchema::new("group_id", ColumnType::Float64, true),
                ColumnSchema::new("organization_id", ColumnType::Float64, true),
                ColumnSchema::new("brand_id", ColumnType::Float64, true),
                ColumnSchema::new("tags", ColumnType::String, true)
                    .with_description("Comma-separated tags"),
                ColumnSchema::new("channel", ColumnType::String, true),
                ColumnSchema::new("satisfaction_rating", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("due_at", ColumnType::String, true),
                ColumnSchema::new("url", ColumnType::String, true),
            ],
            "users" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("email", ColumnType::String, true),
                ColumnSchema::new("phone", ColumnType::String, true),
                ColumnSchema::new("role", ColumnType::String, true),
                ColumnSchema::new("organization_id", ColumnType::Float64, true),
                ColumnSchema::new("time_zone", ColumnType::String, true),
                ColumnSchema::new("locale", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, true),
                ColumnSchema::new("suspended", ColumnType::Boolean, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "organizations" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("details", ColumnType::String, true),
                ColumnSchema::new("notes", ColumnType::String, true),
                ColumnSchema::new("group_id", ColumnType::Float64, true),
                ColumnSchema::new("domain_names", ColumnType::String, true)
                    .with_description("Comma-separated domain names"),
                ColumnSchema::new("tags", ColumnType::String, true)
                    .with_description("Comma-separated tags"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "groups" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("default", ColumnType::Boolean, true),
                ColumnSchema::new("deleted", ColumnType::Boolean, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "brands" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("subdomain", ColumnType::String, true),
                ColumnSchema::new("url", ColumnType::String, true),
                ColumnSchema::new("brand_url", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, true),
                ColumnSchema::new("default", ColumnType::Boolean, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "ticket_fields" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("title", ColumnType::String, true),
                ColumnSchema::new("field_type", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, true),
                ColumnSchema::new("required", ColumnType::Boolean, true),
                ColumnSchema::new("position", ColumnType::Float64, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "ticket_forms" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("display_name", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, true),
                ColumnSchema::new("default", ColumnType::Boolean, true),
                ColumnSchema::new("position", ColumnType::Float64, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "ticket_metrics" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("ticket_id", ColumnType::Float64, true),
                ColumnSchema::new("group_stations", ColumnType::Float64, true),
                ColumnSchema::new("assignee_stations", ColumnType::Float64, true),
                ColumnSchema::new("reopens", ColumnType::Float64, true),
                ColumnSchema::new("replies", ColumnType::Float64, true),
                ColumnSchema::new("reply_time_in_minutes_calendar", ColumnType::Float64, true),
                ColumnSchema::new("reply_time_in_minutes_business", ColumnType::Float64, true),
                ColumnSchema::new("full_resolution_time_in_minutes_calendar", ColumnType::Float64, true),
                ColumnSchema::new("full_resolution_time_in_minutes_business", ColumnType::Float64, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "satisfaction_ratings" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("ticket_id", ColumnType::Float64, true),
                ColumnSchema::new("requester_id", ColumnType::Float64, true),
                ColumnSchema::new("assignee_id", ColumnType::Float64, true),
                ColumnSchema::new("group_id", ColumnType::Float64, true),
                ColumnSchema::new("score", ColumnType::String, true),
                ColumnSchema::new("comment", ColumnType::String, true),
                ColumnSchema::new("reason", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "tags" => vec![
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("count", ColumnType::Float64, true),
            ],
            "macros" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("title", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, true),
                ColumnSchema::new("position", ColumnType::Float64, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "views" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("title", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, true),
                ColumnSchema::new("position", ColumnType::Float64, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "sla_policies" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("title", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("position", ColumnType::Float64, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
            ],
            "automations" => vec![
                ColumnSchema::new("id", ColumnType::Float64, false),
                ColumnSchema::new("title", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, true),
                ColumnSchema::new("position", ColumnType::Float64, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true).with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true).with_timezone("UTC"),
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
            .ok()
            .map(|dt| dt.timestamp_micros())
    }

    fn parse_timestamp_json(val: &serde_json::Value) -> Option<i64> {
        val.as_str().and_then(Self::parse_timestamp_str)
    }

    // ════════════════════════════════════════════════════════════════════
    // HTTP helper with retry
    // ════════════════════════════════════════════════════════════════════

    fn auth_header_value(&self) -> ConnectorResult<String> {
        if let Some(ref oauth) = self.config.oauth_token {
            return Ok(format!("Bearer {}", oauth.expose()));
        }
        if let (Some(ref email), Some(ref token)) = (&self.config.email, &self.config.api_token) {
            let encoded = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{}/token:{}", email, token.expose()),
            );
            return Ok(format!("Basic {}", encoded));
        }
        Err(ConnectorError::Config("Missing Zendesk credentials".to_string()))
    }

    fn resolve_url(&self, path: &str) -> String {
        if path.starts_with("https://") || path.starts_with("http://") {
            return path.to_string();
        }
        #[cfg(test)]
        if let Some(ref base) = self.base_url_override {
            return format!("{}{}", base, path);
        }
        format!("{}{}", self.config.base_url(), path)
    }

    async fn api_get(&self, path: &str) -> ConnectorResult<serde_json::Value> {
        let url = self.resolve_url(path);
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
                .map_err(|e| ConnectorError::Network(format!("Zendesk request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Zendesk credentials".to_string(),
                ));
            }

            if resp.status() == 429 {
                if attempts < MAX_RETRIES {
                    attempts += 1;
                    let retry_after = resp
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(INITIAL_RETRY_DELAY_MS / 1000);
                    let delay_ms = retry_after * 1000;
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                return Err(ConnectorError::RateLimited { retry_after_secs: 30 });
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "Zendesk API error ({}): {}",
                    status, body
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Zendesk response: {}", e))
            })?;

            return Ok(json);
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // Predicate -> Zendesk search query translation
    // ════════════════════════════════════════════════════════════════════

    fn column_to_search_field(col: &str) -> &str {
        match col {
            "ticket_type" => "type",
            "assignee_id" => "assignee",
            "requester_id" => "requester",
            "submitter_id" => "submitter",
            "organization_id" => "organization",
            "group_id" => "group",
            "brand_id" => "brand",
            "created_at" => "created",
            "updated_at" => "updated",
            other => other,
        }
    }

    fn predicates_to_query(predicates: &[Predicate]) -> Option<String> {
        if predicates.is_empty() {
            return None;
        }
        let parts: Vec<String> = predicates
            .iter()
            .filter_map(Self::predicate_to_query_clause)
            .collect();
        if parts.is_empty() {
            return None;
        }
        Some(parts.join(" "))
    }

    fn predicate_to_query_clause(pred: &Predicate) -> Option<String> {
        match pred {
            Predicate::Equals { column, value } => {
                let field = Self::column_to_search_field(column);
                Some(format!("{}:{}", field, value))
            }
            Predicate::GreaterThan { column, value, inclusive } => {
                let field = Self::column_to_search_field(column);
                let op = if *inclusive { ">=" } else { ">" };
                Some(format!("{}{}{}", field, op, value))
            }
            Predicate::LessThan { column, value, inclusive } => {
                let field = Self::column_to_search_field(column);
                let op = if *inclusive { "<=" } else { "<" };
                Some(format!("{}{}{}", field, op, value))
            }
            _ => None,
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // Incremental Export fetching (tickets, users, organizations)
    // ════════════════════════════════════════════════════════════════════

    async fn fetch_incremental(
        &self,
        resource: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
        append_fn: fn(&serde_json::Value, &TableSchema, &mut ColumnBuilders),
    ) -> ConnectorResult<Vec<RecordBatch>> {
        if !options.predicates.is_empty() && resource == "tickets" {
            if let Some(query) = Self::predicates_to_query(&options.predicates) {
                return self.fetch_search(
                    &format!("type:ticket {}", query),
                    schema, arrow_schema, options, append_fn,
                ).await;
            }
        }

        let start_time = options
            .last_value
            .as_deref()
            .and_then(|v| Self::parse_timestamp_str(v))
            .map(|micros| micros / 1_000_000)
            .unwrap_or(0);

        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows: usize = 0;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        let mut next_url = format!(
            "/api/v2/incremental/{}/cursor.json?start_time={}",
            resource, start_time
        );

        loop {
            if total_rows >= max_rows { break; }

            let data = self.api_get(&next_url).await?;
            let items = data.get(resource).and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let end_of_stream = data.get("end_of_stream").and_then(|v| v.as_bool()).unwrap_or(true);

            for item in &items {
                if total_rows >= max_rows { break; }
                append_fn(item, schema, &mut builders);
                total_rows += 1;
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            if end_of_stream { break; }

            let cursor = data.get("after_url").and_then(|v| v.as_str());
            match cursor {
                Some(url) => next_url = url.to_string(),
                None => break,
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

    async fn fetch_search(
        &self,
        query: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
        append_fn: fn(&serde_json::Value, &TableSchema, &mut ColumnBuilders),
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows: usize = 0;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        let encoded_query = urlencoding::encode(query);
        let mut next_url = format!(
            "/api/v2/search.json?query={}&per_page={}",
            encoded_query, PAGE_SIZE
        );

        loop {
            if total_rows >= max_rows { break; }

            let data = self.api_get(&next_url).await?;
            let items = data.get("results").and_then(|v| v.as_array()).cloned().unwrap_or_default();

            for item in &items {
                if total_rows >= max_rows { break; }
                append_fn(item, schema, &mut builders);
                total_rows += 1;
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            let next_page = data.get("next_page").and_then(|v| v.as_str());
            match next_page {
                Some(url) => next_url = url.to_string(),
                None => break,
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

    fn append_ticket(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "subject" => builders.builder(i).append_string(item.get("subject").and_then(|v| v.as_str())),
                "description" => builders.builder(i).append_string(item.get("description").and_then(|v| v.as_str())),
                "status" => builders.builder(i).append_string(item.get("status").and_then(|v| v.as_str())),
                "priority" => builders.builder(i).append_string(item.get("priority").and_then(|v| v.as_str())),
                "ticket_type" => builders.builder(i).append_string(item.get("type").and_then(|v| v.as_str())),
                "requester_id" => builders.builder(i).append_f64(item.get("requester_id").and_then(|v| v.as_f64())),
                "submitter_id" => builders.builder(i).append_f64(item.get("submitter_id").and_then(|v| v.as_f64())),
                "assignee_id" => builders.builder(i).append_f64(item.get("assignee_id").and_then(|v| v.as_f64())),
                "group_id" => builders.builder(i).append_f64(item.get("group_id").and_then(|v| v.as_f64())),
                "organization_id" => builders.builder(i).append_f64(item.get("organization_id").and_then(|v| v.as_f64())),
                "brand_id" => builders.builder(i).append_f64(item.get("brand_id").and_then(|v| v.as_f64())),
                "tags" => {
                    let tags = item.get("tags")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|t| t.as_str()).collect::<Vec<_>>().join(","));
                    builders.builder(i).append_string(tags.as_deref());
                }
                "channel" => builders.builder(i).append_string(
                    item.get("via").and_then(|v| v.get("channel")).and_then(|v| v.as_str())
                ),
                "satisfaction_rating" => {
                    let v = item.get("satisfaction_rating")
                        .and_then(|r| r.get("score"))
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "due_at" => builders.builder(i).append_string(item.get("due_at").and_then(|v| v.as_str())),
                "url" => builders.builder(i).append_string(item.get("url").and_then(|v| v.as_str())),
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_user(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "name" => builders.builder(i).append_string(item.get("name").and_then(|v| v.as_str())),
                "email" => builders.builder(i).append_string(item.get("email").and_then(|v| v.as_str())),
                "phone" => builders.builder(i).append_string(item.get("phone").and_then(|v| v.as_str())),
                "role" => builders.builder(i).append_string(item.get("role").and_then(|v| v.as_str())),
                "organization_id" => builders.builder(i).append_f64(item.get("organization_id").and_then(|v| v.as_f64())),
                "time_zone" => builders.builder(i).append_string(item.get("time_zone").and_then(|v| v.as_str())),
                "locale" => builders.builder(i).append_string(item.get("locale").and_then(|v| v.as_str())),
                "active" => builders.builder(i).append_bool(item.get("active").and_then(|v| v.as_bool())),
                "suspended" => builders.builder(i).append_bool(item.get("suspended").and_then(|v| v.as_bool())),
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_organization(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "name" => builders.builder(i).append_string(item.get("name").and_then(|v| v.as_str())),
                "details" => builders.builder(i).append_string(item.get("details").and_then(|v| v.as_str())),
                "notes" => builders.builder(i).append_string(item.get("notes").and_then(|v| v.as_str())),
                "group_id" => builders.builder(i).append_f64(item.get("group_id").and_then(|v| v.as_f64())),
                "domain_names" => {
                    let domains = item.get("domain_names")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|d| d.as_str()).collect::<Vec<_>>().join(","));
                    builders.builder(i).append_string(domains.as_deref());
                }
                "tags" => {
                    let tags = item.get("tags")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|t| t.as_str()).collect::<Vec<_>>().join(","));
                    builders.builder(i).append_string(tags.as_deref());
                }
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    // ════════════════════════════════════════════════════════════════════
    // Cursor-paginated fetching (lookup/config tables)
    // ════════════════════════════════════════════════════════════════════

    async fn fetch_cursor_paginated(
        &self,
        initial_url: &str,
        items_key: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
        append_fn: fn(&serde_json::Value, &TableSchema, &mut ColumnBuilders),
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows: usize = 0;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        let mut next_url = initial_url.to_string();

        loop {
            if total_rows >= max_rows { break; }

            let data = self.api_get(&next_url).await?;
            let items = data.get(items_key).and_then(|v| v.as_array()).cloned().unwrap_or_default();

            for item in &items {
                if total_rows >= max_rows { break; }
                append_fn(item, schema, &mut builders);
                total_rows += 1;
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            let has_more = data.get("meta")
                .and_then(|m| m.get("has_more"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !has_more { break; }

            let next_link = data.get("links")
                .and_then(|l| l.get("next"))
                .and_then(|v| v.as_str());
            match next_link {
                Some(url) => next_url = url.to_string(),
                None => break,
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
    // Non-paginated fetching (small lookup tables)
    // ════════════════════════════════════════════════════════════════════

    async fn fetch_simple(
        &self,
        url: &str,
        items_key: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        append_fn: fn(&serde_json::Value, &TableSchema, &mut ColumnBuilders),
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let data = self.api_get(url).await?;
        let items = data.get(items_key).and_then(|v| v.as_array()).cloned().unwrap_or_default();

        let mut builders = ColumnBuilders::new(schema, items.len().max(1));
        for item in &items {
            append_fn(item, schema, &mut builders);
        }
        Self::finish_batches(builders, arrow_schema)
    }

    // ════════════════════════════════════════════════════════════════════
    // Append functions for paginated tables
    // ════════════════════════════════════════════════════════════════════

    fn append_group(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "name" => builders.builder(i).append_string(item.get("name").and_then(|v| v.as_str())),
                "description" => builders.builder(i).append_string(item.get("description").and_then(|v| v.as_str())),
                "default" => builders.builder(i).append_bool(item.get("default").and_then(|v| v.as_bool())),
                "deleted" => builders.builder(i).append_bool(item.get("deleted").and_then(|v| v.as_bool())),
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_brand(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "name" => builders.builder(i).append_string(item.get("name").and_then(|v| v.as_str())),
                "subdomain" => builders.builder(i).append_string(item.get("subdomain").and_then(|v| v.as_str())),
                "url" => builders.builder(i).append_string(item.get("url").and_then(|v| v.as_str())),
                "brand_url" => builders.builder(i).append_string(item.get("brand_url").and_then(|v| v.as_str())),
                "active" => builders.builder(i).append_bool(item.get("active").and_then(|v| v.as_bool())),
                "default" => builders.builder(i).append_bool(item.get("default").and_then(|v| v.as_bool())),
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_ticket_field(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "title" => builders.builder(i).append_string(item.get("title").and_then(|v| v.as_str())),
                "field_type" => builders.builder(i).append_string(item.get("type").and_then(|v| v.as_str())),
                "description" => builders.builder(i).append_string(item.get("description").and_then(|v| v.as_str())),
                "active" => builders.builder(i).append_bool(item.get("active").and_then(|v| v.as_bool())),
                "required" => builders.builder(i).append_bool(item.get("required").and_then(|v| v.as_bool())),
                "position" => builders.builder(i).append_f64(item.get("position").and_then(|v| v.as_f64())),
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_ticket_form(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "name" => builders.builder(i).append_string(item.get("name").and_then(|v| v.as_str())),
                "display_name" => builders.builder(i).append_string(item.get("display_name").and_then(|v| v.as_str())),
                "active" => builders.builder(i).append_bool(item.get("active").and_then(|v| v.as_bool())),
                "default" => builders.builder(i).append_bool(item.get("default").and_then(|v| v.as_bool())),
                "position" => builders.builder(i).append_f64(item.get("position").and_then(|v| v.as_f64())),
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_ticket_metric(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "ticket_id" => builders.builder(i).append_f64(item.get("ticket_id").and_then(|v| v.as_f64())),
                "group_stations" => builders.builder(i).append_f64(item.get("group_stations").and_then(|v| v.as_f64())),
                "assignee_stations" => builders.builder(i).append_f64(item.get("assignee_stations").and_then(|v| v.as_f64())),
                "reopens" => builders.builder(i).append_f64(item.get("reopens").and_then(|v| v.as_f64())),
                "replies" => builders.builder(i).append_f64(item.get("replies").and_then(|v| v.as_f64())),
                "reply_time_in_minutes_calendar" => {
                    let v = item.get("reply_time_in_minutes").and_then(|r| r.get("calendar")).and_then(|v| v.as_f64());
                    builders.builder(i).append_f64(v);
                }
                "reply_time_in_minutes_business" => {
                    let v = item.get("reply_time_in_minutes").and_then(|r| r.get("business")).and_then(|v| v.as_f64());
                    builders.builder(i).append_f64(v);
                }
                "full_resolution_time_in_minutes_calendar" => {
                    let v = item.get("full_resolution_time_in_minutes").and_then(|r| r.get("calendar")).and_then(|v| v.as_f64());
                    builders.builder(i).append_f64(v);
                }
                "full_resolution_time_in_minutes_business" => {
                    let v = item.get("full_resolution_time_in_minutes").and_then(|r| r.get("business")).and_then(|v| v.as_f64());
                    builders.builder(i).append_f64(v);
                }
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_satisfaction_rating(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "ticket_id" => builders.builder(i).append_f64(item.get("ticket_id").and_then(|v| v.as_f64())),
                "requester_id" => builders.builder(i).append_f64(item.get("requester_id").and_then(|v| v.as_f64())),
                "assignee_id" => builders.builder(i).append_f64(item.get("assignee_id").and_then(|v| v.as_f64())),
                "group_id" => builders.builder(i).append_f64(item.get("group_id").and_then(|v| v.as_f64())),
                "score" => builders.builder(i).append_string(item.get("score").and_then(|v| v.as_str())),
                "comment" => builders.builder(i).append_string(item.get("comment").and_then(|v| v.as_str())),
                "reason" => builders.builder(i).append_string(item.get("reason").and_then(|v| v.as_str())),
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_tag(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "name" => builders.builder(i).append_string(item.get("name").and_then(|v| v.as_str())),
                "count" => builders.builder(i).append_f64(item.get("count").and_then(|v| v.as_f64())),
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_generic_rule(item: &serde_json::Value, schema: &TableSchema, builders: &mut ColumnBuilders) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_f64(item.get("id").and_then(|v| v.as_f64())),
                "title" => builders.builder(i).append_string(item.get("title").and_then(|v| v.as_str())),
                "description" => builders.builder(i).append_string(item.get("description").and_then(|v| v.as_str())),
                "active" => builders.builder(i).append_bool(item.get("active").and_then(|v| v.as_bool())),
                "position" => builders.builder(i).append_f64(item.get("position").and_then(|v| v.as_f64())),
                "created_at" => {
                    let ts = item.get("created_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                "updated_at" => {
                    let ts = item.get("updated_at").and_then(Self::parse_timestamp_json);
                    builders.builder(i).append_timestamp(ts);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
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
            "tickets" => self.fetch_incremental("tickets", &schema, arrow_schema, &options, Self::append_ticket).await,
            "users" => self.fetch_incremental("users", &schema, arrow_schema, &options, Self::append_user).await,
            "organizations" => self.fetch_incremental("organizations", &schema, arrow_schema, &options, Self::append_organization).await,
            "groups" => self.fetch_cursor_paginated(&format!("/api/v2/groups?page[size]={}", PAGE_SIZE), "groups", &schema, arrow_schema, &options, Self::append_group).await,
            "brands" => self.fetch_cursor_paginated(&format!("/api/v2/brands?page[size]={}", PAGE_SIZE), "brands", &schema, arrow_schema, &options, Self::append_brand).await,
            "ticket_fields" => self.fetch_cursor_paginated(&format!("/api/v2/ticket_fields?page[size]={}", PAGE_SIZE), "ticket_fields", &schema, arrow_schema, &options, Self::append_ticket_field).await,
            "ticket_forms" => self.fetch_simple("/api/v2/ticket_forms", "ticket_forms", &schema, arrow_schema, Self::append_ticket_form).await,
            "ticket_metrics" => self.fetch_cursor_paginated(&format!("/api/v2/ticket_metrics?page[size]={}", PAGE_SIZE), "ticket_metrics", &schema, arrow_schema, &options, Self::append_ticket_metric).await,
            "satisfaction_ratings" => self.fetch_cursor_paginated(&format!("/api/v2/satisfaction_ratings?page[size]={}", PAGE_SIZE), "satisfaction_ratings", &schema, arrow_schema, &options, Self::append_satisfaction_rating).await,
            "tags" => self.fetch_simple("/api/v2/tags", "tags", &schema, arrow_schema, Self::append_tag).await,
            "macros" => self.fetch_cursor_paginated(&format!("/api/v2/macros?page[size]={}", PAGE_SIZE), "macros", &schema, arrow_schema, &options, Self::append_generic_rule).await,
            "views" => self.fetch_cursor_paginated(&format!("/api/v2/views?page[size]={}", PAGE_SIZE), "views", &schema, arrow_schema, &options, Self::append_generic_rule).await,
            "sla_policies" => self.fetch_simple("/api/v2/slas/policies", "sla_policies", &schema, arrow_schema, Self::append_generic_rule).await,
            "automations" => self.fetch_cursor_paginated(&format!("/api/v2/automations?page[size]={}", PAGE_SIZE), "automations", &schema, arrow_schema, &options, Self::append_generic_rule).await,
            _ => Err(ConnectorError::TableNotFound(table.to_string())),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Connector trait
// ═══════════════════════════════════════════════════════════════════════════

#[async_trait]
impl Connector for ZendeskConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Zendesk
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        Ok(TABLES
            .iter()
            .filter_map(|&table| {
                let schema = Self::get_table_schema(table)?;
                let (supports_incremental, incremental_key) = match table {
                    "tickets" | "users" | "organizations" => {
                        (true, Some("updated_at".to_string()))
                    }
                    _ => (false, None),
                };
                let pk = match table {
                    "tags" => vec!["name".to_string()],
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
        self.api_get("/api/v2/users/me").await?;
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

    fn mock_connector() -> ZendeskConnector {
        let config = ZendeskConfig {
            subdomain: "test".to_string(),
            email: Some("user@test.com".to_string()),
            api_token: Some(SecretString::new("test_token")),
            oauth_token: None,
        };
        ZendeskConnector {
            config,
            http: reqwest::Client::new(),
            base_url_override: None,
        }
    }

    fn mock_connector_with_base(server_uri: &str) -> ZendeskConnector {
        let config = ZendeskConfig {
            subdomain: "test".to_string(),
            email: Some("user@test.com".to_string()),
            api_token: Some(SecretString::new("test_token")),
            oauth_token: None,
        };
        ZendeskConnector {
            config,
            http: reqwest::Client::new(),
            base_url_override: Some(server_uri.to_string()),
        }
    }

    // ── Schema tests ─────────────────────────────────────────────────

    #[test]
    fn test_all_tables_have_schemas() {
        for table in TABLES {
            assert!(
                ZendeskConnector::get_table_schema(table).is_some(),
                "Missing schema for table: {}",
                table
            );
        }
    }

    #[test]
    fn test_tickets_schema_columns() {
        let schema = ZendeskConnector::get_table_schema("tickets").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"subject"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"priority"));
        assert!(names.contains(&"requester_id"));
        assert!(names.contains(&"assignee_id"));
        assert!(names.contains(&"tags"));
        assert!(names.contains(&"created_at"));
        assert!(names.contains(&"updated_at"));
    }

    #[test]
    fn test_users_schema_columns() {
        let schema = ZendeskConnector::get_table_schema("users").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"email"));
        assert!(names.contains(&"role"));
        assert!(names.contains(&"active"));
    }

    #[test]
    fn test_unknown_table_returns_none() {
        assert!(ZendeskConnector::get_table_schema("nonexistent").is_none());
    }

    // ── Config tests ─────────────────────────────────────────────────

    #[test]
    fn test_config_debug_redacts() {
        let config = ZendeskConfig::with_api_token("mycompany", "user@test.com", "secret_token");
        let debug = format!("{:?}", config);
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("secret_token"));
    }

    #[test]
    fn test_api_token_auth_header() {
        let config = ZendeskConfig::with_api_token("mycompany", "user@test.com", "tok123");
        let connector = ZendeskConnector::new(config);
        let header = connector.auth_header_value().unwrap();
        assert!(header.starts_with("Basic "));
        let decoded = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            header.trim_start_matches("Basic "),
        ).unwrap();
        let decoded_str = String::from_utf8(decoded).unwrap();
        assert_eq!(decoded_str, "user@test.com/token:tok123");
    }

    #[test]
    fn test_oauth_auth_header() {
        let config = ZendeskConfig::with_oauth("mycompany", "oauth_token_123");
        let connector = ZendeskConnector::new(config);
        let header = connector.auth_header_value().unwrap();
        assert_eq!(header, "Bearer oauth_token_123");
    }

    #[test]
    fn test_base_url() {
        let config = ZendeskConfig::with_api_token("mycompany", "u@t.com", "tok");
        assert_eq!(config.base_url(), "https://mycompany.zendesk.com");
    }

    // ── Predicate translation tests ──────────────────────────────────

    #[test]
    fn test_predicate_equals() {
        let preds = vec![Predicate::Equals {
            column: "status".into(),
            value: "open".into(),
        }];
        let query = ZendeskConnector::predicates_to_query(&preds);
        assert_eq!(query, Some("status:open".to_string()));
    }

    #[test]
    fn test_predicate_greater_than() {
        let preds = vec![Predicate::GreaterThan {
            column: "updated_at".into(),
            value: "2024-01-01".into(),
            inclusive: true,
        }];
        let query = ZendeskConnector::predicates_to_query(&preds);
        assert_eq!(query, Some("updated>=2024-01-01".to_string()));
    }

    #[test]
    fn test_predicate_empty() {
        let query = ZendeskConnector::predicates_to_query(&[]);
        assert_eq!(query, None);
    }

    #[test]
    fn test_predicate_field_mapping() {
        assert_eq!(ZendeskConnector::column_to_search_field("ticket_type"), "type");
        assert_eq!(ZendeskConnector::column_to_search_field("assignee_id"), "assignee");
        assert_eq!(ZendeskConnector::column_to_search_field("created_at"), "created");
        assert_eq!(ZendeskConnector::column_to_search_field("status"), "status");
    }

    // ── API tests with wiremock ───────────────────────────────────────

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let server = MockServer::start().await;
        let connector = mock_connector();

        Mock::given(method("GET"))
            .and(path("/api/v2/users/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "user": {"id": 123, "name": "Test User", "email": "user@test.com"}
            })))
            .mount(&server)
            .await;

        let url = format!("{}/api/v2/users/me", server.uri());
        let result = connector.api_get(&url).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_auth_failure() {
        let server = MockServer::start().await;
        let connector = mock_connector();

        Mock::given(method("GET"))
            .and(path("/api/v2/users/me"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let url = format!("{}/api/v2/users/me", server.uri());
        let result = connector.api_get(&url).await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_rate_limit_retry() {
        let server = MockServer::start().await;
        let connector = mock_connector();

        Mock::given(method("GET"))
            .and(path("/api/v2/users/me"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "0")
            )
            .expect(4)
            .mount(&server)
            .await;

        let url = format!("{}/api/v2/users/me", server.uri());
        let result = connector.api_get(&url).await;
        assert!(matches!(result, Err(ConnectorError::RateLimited { .. })));
    }

    #[tokio::test]
    async fn test_fetch_groups() {
        let server = MockServer::start().await;
        let connector = mock_connector();

        Mock::given(method("GET"))
            .and(path("/api/v2/groups"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "groups": [
                    {
                        "id": 1, "name": "Support", "description": "Support team",
                        "default": true, "deleted": false,
                        "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-06-01T00:00:00Z"
                    },
                    {
                        "id": 2, "name": "Sales", "description": "Sales team",
                        "default": false, "deleted": false,
                        "created_at": "2024-02-01T00:00:00Z", "updated_at": "2024-06-01T00:00:00Z"
                    }
                ],
                "meta": {"has_more": false},
                "links": {}
            })))
            .mount(&server)
            .await;

        let url = format!("{}/api/v2/groups", server.uri());
        let data = connector.api_get(&url).await.unwrap();
        let items = data.get("groups").unwrap().as_array().unwrap();
        assert_eq!(items.len(), 2);

        let schema = ZendeskConnector::get_table_schema("groups").unwrap();
        let arrow_schema = Arc::new(ZendeskConnector::to_arrow_schema(&schema));
        let mut builders = ColumnBuilders::new(&schema, 10);
        for item in items {
            ZendeskConnector::append_group(item, &schema, &mut builders);
        }
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_fetch_tickets_incremental() {
        let server = MockServer::start().await;
        let connector = mock_connector();

        Mock::given(method("GET"))
            .and(path("/api/v2/incremental/tickets/cursor.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "tickets": [
                    {
                        "id": 1, "subject": "Help needed", "description": "I need help",
                        "status": "open", "priority": "high", "type": "incident",
                        "requester_id": 100, "submitter_id": 100, "assignee_id": 200,
                        "group_id": 1, "organization_id": 10, "brand_id": 5,
                        "tags": ["urgent", "billing"],
                        "via": {"channel": "email"},
                        "satisfaction_rating": {"score": "good"},
                        "created_at": "2024-01-15T10:00:00Z",
                        "updated_at": "2024-06-15T14:00:00Z",
                        "due_at": null,
                        "url": "https://test.zendesk.com/api/v2/tickets/1.json"
                    }
                ],
                "end_of_stream": true,
                "after_url": null
            })))
            .mount(&server)
            .await;

        let url = format!("{}/api/v2/incremental/tickets/cursor.json?start_time=0", server.uri());
        let data = connector.api_get(&url).await.unwrap();
        let items = data.get("tickets").unwrap().as_array().unwrap();

        let schema = ZendeskConnector::get_table_schema("tickets").unwrap();
        let arrow_schema = Arc::new(ZendeskConnector::to_arrow_schema(&schema));
        let mut builders = ColumnBuilders::new(&schema, 10);
        for item in items {
            ZendeskConnector::append_ticket(item, &schema, &mut builders);
        }
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let col_idx = |name: &str| schema.columns.iter().position(|c| c.name == name).unwrap();
        let str_val = |name: &str| -> Option<String> {
            use arrow::array::Array;
            let arr = batch.column(col_idx(name));
            let sa = arr.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            sa.is_valid(0).then(|| sa.value(0).to_string())
        };
        let f64_val = |name: &str| -> Option<f64> {
            use arrow::array::Array;
            let arr = batch.column(col_idx(name));
            let fa = arr.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
            fa.is_valid(0).then(|| fa.value(0))
        };

        assert_eq!(f64_val("id"), Some(1.0));
        assert_eq!(str_val("subject"), Some("Help needed".to_string()));
        assert_eq!(str_val("status"), Some("open".to_string()));
        assert_eq!(str_val("priority"), Some("high".to_string()));
        assert_eq!(str_val("tags"), Some("urgent,billing".to_string()));
        assert_eq!(str_val("channel"), Some("email".to_string()));
        assert_eq!(str_val("satisfaction_rating"), Some("good".to_string()));
        assert_eq!(f64_val("requester_id"), Some(100.0));
        assert_eq!(f64_val("assignee_id"), Some(200.0));
    }

    #[tokio::test]
    async fn test_fetch_users_incremental() {
        let server = MockServer::start().await;
        let connector = mock_connector();

        Mock::given(method("GET"))
            .and(path("/api/v2/incremental/users/cursor.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "users": [
                    {
                        "id": 100, "name": "Alice", "email": "alice@test.com",
                        "phone": null, "role": "end-user",
                        "organization_id": 10, "time_zone": "UTC", "locale": "en-US",
                        "active": true, "suspended": false,
                        "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-06-01T00:00:00Z"
                    }
                ],
                "end_of_stream": true,
                "after_url": null
            })))
            .mount(&server)
            .await;

        let url = format!("{}/api/v2/incremental/users/cursor.json?start_time=0", server.uri());
        let data = connector.api_get(&url).await.unwrap();
        let items = data.get("users").unwrap().as_array().unwrap();

        let schema = ZendeskConnector::get_table_schema("users").unwrap();
        let arrow_schema = Arc::new(ZendeskConnector::to_arrow_schema(&schema));
        let mut builders = ColumnBuilders::new(&schema, 10);
        for item in items {
            ZendeskConnector::append_user(item, &schema, &mut builders);
        }
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_list_tables_incremental() {
        let config = ZendeskConfig::with_api_token("test", "u@t.com", "tok");
        let connector = ZendeskConnector::new(config);
        let tables = connector.list_tables().await.unwrap();

        for table in &tables {
            match table.name.as_str() {
                "tickets" | "users" | "organizations" => {
                    assert!(table.supports_incremental, "{} should support incremental", table.name);
                    assert_eq!(table.incremental_key, Some("updated_at".to_string()));
                }
                _ => {
                    assert!(!table.supports_incremental, "{} should not support incremental", table.name);
                    assert_eq!(table.incremental_key, None);
                }
            }
        }
    }

    #[test]
    fn test_timestamp_parsing() {
        let ts = ZendeskConnector::parse_timestamp_str("2024-06-15T10:30:00Z");
        assert!(ts.is_some());

        let ts = ZendeskConnector::parse_timestamp_str("2024-06-15T10:30:00+00:00");
        assert!(ts.is_some());

        assert!(ZendeskConnector::parse_timestamp_str("not-a-date").is_none());
    }

    #[tokio::test]
    async fn test_cursor_pagination_multi_page() {
        let server = MockServer::start().await;
        let connector = mock_connector();

        let page2_url = format!("{}/api/v2/groups?page[size]=100&page[after]=cursor123", server.uri());

        Mock::given(method("GET"))
            .and(path("/api/v2/groups"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "groups": [
                    {"id": 1, "name": "Support", "description": null, "default": true, "deleted": false,
                     "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z"}
                ],
                "meta": {"has_more": true, "after_cursor": "cursor123"},
                "links": {"next": page2_url}
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/v2/groups"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "groups": [
                    {"id": 2, "name": "Sales", "description": null, "default": false, "deleted": false,
                     "created_at": "2024-02-01T00:00:00Z", "updated_at": "2024-02-01T00:00:00Z"}
                ],
                "meta": {"has_more": false},
                "links": {}
            })))
            .mount(&server)
            .await;

        let schema = ZendeskConnector::get_table_schema("groups").unwrap();
        let arrow_schema = Arc::new(ZendeskConnector::to_arrow_schema(&schema));

        let initial_url = format!("{}/api/v2/groups?page[size]={}", server.uri(), PAGE_SIZE);
        let options = FetchOptions::full_sync();
        let batches = connector.fetch_cursor_paginated(
            &initial_url, "groups", &schema, arrow_schema, &options, ZendeskConnector::append_group,
        ).await.unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_internal_server_error() {
        let server = MockServer::start().await;
        let connector = mock_connector();

        Mock::given(method("GET"))
            .and(path("/api/v2/test"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .mount(&server)
            .await;

        let url = format!("{}/api/v2/test", server.uri());
        let result = connector.api_get(&url).await;
        assert!(matches!(result, Err(ConnectorError::Internal(_))));
    }

    #[test]
    fn test_append_ticket_nullable_fields() {
        let schema = ZendeskConnector::get_table_schema("tickets").unwrap();
        let arrow_schema = Arc::new(ZendeskConnector::to_arrow_schema(&schema));

        let item = serde_json::json!({
            "id": 99,
            "subject": null,
            "description": null,
            "status": null,
            "priority": null,
            "type": null,
            "requester_id": null,
            "submitter_id": null,
            "assignee_id": null,
            "group_id": null,
            "organization_id": null,
            "brand_id": null,
            "tags": [],
            "via": null,
            "satisfaction_rating": null,
            "created_at": null,
            "updated_at": null,
            "due_at": null,
            "url": null
        });

        let mut builders = ColumnBuilders::new(&schema, 10);
        ZendeskConnector::append_ticket(&item, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_append_organization() {
        let schema = ZendeskConnector::get_table_schema("organizations").unwrap();
        let arrow_schema = Arc::new(ZendeskConnector::to_arrow_schema(&schema));

        let item = serde_json::json!({
            "id": 42, "name": "Acme Corp", "details": "Enterprise customer",
            "notes": "Important", "group_id": 1,
            "domain_names": ["acme.com", "acme.org"],
            "tags": ["enterprise", "vip"],
            "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-06-01T00:00:00Z"
        });

        let mut builders = ColumnBuilders::new(&schema, 10);
        ZendeskConnector::append_organization(&item, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        use arrow::array::Array;
        let col_idx = |name: &str| schema.columns.iter().position(|c| c.name == name).unwrap();
        let arr = batch.column(col_idx("domain_names"));
        let sa = arr.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(sa.value(0), "acme.com,acme.org");

        let arr = batch.column(col_idx("tags"));
        let sa = arr.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(sa.value(0), "enterprise,vip");
    }

    #[tokio::test]
    async fn test_do_fetch_groups_integration() {
        let server = MockServer::start().await;
        let connector = mock_connector_with_base(&server.uri());

        Mock::given(method("GET"))
            .and(path("/api/v2/groups"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "groups": [
                    {
                        "id": 1, "name": "Support", "description": "Support team",
                        "default": true, "deleted": false,
                        "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-06-01T00:00:00Z"
                    },
                    {
                        "id": 2, "name": "Sales", "description": null,
                        "default": false, "deleted": false,
                        "created_at": "2024-02-01T00:00:00Z", "updated_at": "2024-06-01T00:00:00Z"
                    }
                ],
                "meta": {"has_more": false},
                "links": {}
            })))
            .mount(&server)
            .await;

        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("groups", options).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_validate_credentials_trait() {
        let server = MockServer::start().await;
        let connector = mock_connector_with_base(&server.uri());

        Mock::given(method("GET"))
            .and(path("/api/v2/users/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "user": {"id": 123, "name": "Test User", "email": "user@test.com"}
            })))
            .mount(&server)
            .await;

        let result = connector.validate_credentials().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_credentials_trait_auth_failure() {
        let server = MockServer::start().await;
        let connector = mock_connector_with_base(&server.uri());

        Mock::given(method("GET"))
            .and(path("/api/v2/users/me"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_do_fetch_tags_simple() {
        let server = MockServer::start().await;
        let connector = mock_connector_with_base(&server.uri());

        Mock::given(method("GET"))
            .and(path("/api/v2/tags"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "tags": [
                    {"name": "billing", "count": 42},
                    {"name": "urgent", "count": 7}
                ]
            })))
            .mount(&server)
            .await;

        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("tags", options).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_fetch_tickets_with_predicates() {
        let server = MockServer::start().await;
        let connector = mock_connector_with_base(&server.uri());

        Mock::given(method("GET"))
            .and(path("/api/v2/search.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [
                    {
                        "id": 10, "subject": "Filtered ticket", "description": "Matched",
                        "status": "open", "priority": null, "type": null,
                        "requester_id": 100, "submitter_id": 100, "assignee_id": null,
                        "group_id": null, "organization_id": null, "brand_id": null,
                        "tags": [], "via": {"channel": "web"},
                        "satisfaction_rating": null,
                        "created_at": "2024-03-01T00:00:00Z",
                        "updated_at": "2024-06-01T00:00:00Z",
                        "due_at": null, "url": null
                    }
                ],
                "next_page": null,
                "count": 1
            })))
            .mount(&server)
            .await;

        let options = FetchOptions {
            predicates: vec![Predicate::Equals {
                column: "status".into(),
                value: "open".into(),
            }],
            ..FetchOptions::full_sync()
        };
        let batches = connector.do_fetch("tickets", options).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }
}
