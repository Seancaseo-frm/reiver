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
use std::time::Duration;

const API_BASE: &str = "https://app.posthog.com";
const BATCH_CAPACITY: usize = 4096;
const PAGE_LIMIT: u64 = 100;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 50;
const MAX_TOTAL_ROWS: usize = 1_000_000;

const ALL_TABLES: &[&str] = &[
    "events",
    "persons",
    "feature_flags",
    "cohorts",
    "insights",
    "annotations",
];

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PostHogConfig {
    pub personal_api_key: SecretString,
    pub project_id: String,
    pub api_base: Option<String>,
}

impl std::fmt::Debug for PostHogConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostHogConfig")
            .field("personal_api_key", &"[REDACTED]")
            .field("project_id", &self.project_id)
            .field("api_base", &self.api_base)
            .finish()
    }
}

impl PostHogConfig {
    pub fn new(api_key: impl Into<String>, project_id: impl Into<String>) -> Self {
        Self {
            personal_api_key: SecretString::new(api_key.into()),
            project_id: project_id.into(),
            api_base: None,
        }
    }

    pub fn with_api_base(mut self, base: impl Into<String>) -> Self {
        self.api_base = Some(base.into());
        self
    }

    fn base_url(&self) -> &str {
        self.api_base.as_deref().unwrap_or(API_BASE)
    }
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct PostHogConnector {
    config: PostHogConfig,
    http: reqwest::Client,
    #[cfg(test)]
    base_url_override: Option<String>,
}

impl PostHogConnector {
    pub fn new(config: PostHogConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
            #[cfg(test)]
            base_url_override: None,
        }
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

    // -----------------------------------------------------------------------
    // HTTP helpers
    // -----------------------------------------------------------------------

    async fn api_get(&self, path: &str) -> ConnectorResult<serde_json::Value> {
        let url = self.resolve_url(path);
        let mut attempts = 0u32;

        loop {
            let resp = self
                .http
                .get(&url)
                .header(
                    "Authorization",
                    format!("Bearer {}", self.config.personal_api_key.expose()),
                )
                .header("Accept", "application/json")
                .header("User-Agent", "reiver-connector")
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("PostHog request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid PostHog API key".to_string(),
                ));
            }

            if resp.status() == 429 {
                if attempts < MAX_RETRIES {
                    attempts += 1;
                    let delay = INITIAL_RETRY_DELAY_MS * 2u64.pow(attempts - 1);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                return Err(ConnectorError::RateLimited { retry_after_secs: 60 });
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "PostHog API error ({}): {}",
                    status, body
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse PostHog response: {}", e))
            })?;

            return Ok(json);
        }
    }

    // -----------------------------------------------------------------------
    // Fetch strategies
    // -----------------------------------------------------------------------

    async fn fetch_paginated(
        &self,
        initial_path: &str,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);
        let mut path = initial_path.to_string();

        loop {
            if total_rows >= max_rows {
                break;
            }

            let body = self.api_get(&path).await?;

            let items = body
                .get("results")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            for item in &items {
                if total_rows >= max_rows {
                    break;
                }

                append_row(item, table, schema, &mut builders);
                total_rows += 1;

                if builders.len() >= BATCH_CAPACITY {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                }
            }

            let next_url = body
                .get("next")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            match next_url {
                Some(url) => {
                    path = url;
                    tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
                }
                None => break,
            }
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(arrow_schema));
        }
        Ok(batches)
    }

    async fn do_fetch(
        &self,
        table: &str,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let schema = get_table_schema(table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("Unknown table: {}", table))
        })?;
        let arrow_schema = Arc::new(to_arrow_schema(&schema));

        let mut query = format!(
            "/api/projects/{}/{}/?limit={}",
            self.config.project_id, table, PAGE_LIMIT
        );

        apply_predicate_params(&mut query, &options.predicates, table);

        if let (Some(key), Some(val)) = (&options.incremental_key, &options.last_value) {
            match (table, key.as_str()) {
                ("events", "timestamp") => {
                    let sep = if query.contains('?') { '&' } else { '?' };
                    query.push_str(&format!("{}after={}", sep, val));
                }
                _ => {}
            }
        }

        self.fetch_paginated(&query, table, &schema, arrow_schema, options)
            .await
    }
}

// ---------------------------------------------------------------------------
// Predicate pushdown -> query params
// ---------------------------------------------------------------------------

fn apply_predicate_params(query: &mut String, predicates: &[Predicate], table: &str) {
    for pred in predicates {
        match pred {
            Predicate::GreaterThan {
                column,
                value,
                inclusive: _,
            } if column == "timestamp" && table == "events" => {
                let sep = if query.contains('?') { '&' } else { '?' };
                query.push_str(&format!("{}after={}", sep, value));
            }
            Predicate::LessThan {
                column,
                value,
                inclusive: _,
            } if column == "timestamp" && table == "events" => {
                let sep = if query.contains('?') { '&' } else { '?' };
                query.push_str(&format!("{}before={}", sep, value));
            }
            Predicate::Equals { column, value }
                if column == "event" && table == "events" =>
            {
                let sep = if query.contains('?') { '&' } else { '?' };
                query.push_str(&format!("{}event={}", sep, value));
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Table schemas
// ---------------------------------------------------------------------------

fn get_table_schema(table: &str) -> Option<TableSchema> {
    match table {
        "events" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("uuid", ColumnType::String, false),
                ColumnSchema::new("event", ColumnType::String, false),
                ColumnSchema::new("distinct_id", ColumnType::String, false),
                ColumnSchema::new("timestamp", ColumnType::Timestamp, false),
                ColumnSchema::new("properties", ColumnType::String, true),
                ColumnSchema::new("elements_chain", ColumnType::String, true),
            ],
        }),

        "persons" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("uuid", ColumnType::String, false),
                ColumnSchema::new("distinct_ids", ColumnType::String, true),
                ColumnSchema::new("properties", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("is_identified", ColumnType::Boolean, true),
            ],
        }),

        "feature_flags" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("key", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, false),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("created_by", ColumnType::String, true),
                ColumnSchema::new("filters", ColumnType::String, true),
                ColumnSchema::new("rollout_percentage", ColumnType::Int64, true),
                ColumnSchema::new("ensure_experience_continuity", ColumnType::Boolean, true),
            ],
        }),

        "cohorts" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("created_by", ColumnType::String, true),
                ColumnSchema::new("count", ColumnType::Int64, true),
                ColumnSchema::new("is_calculating", ColumnType::Boolean, true),
                ColumnSchema::new("is_static", ColumnType::Boolean, true),
            ],
        }),

        "insights" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("created_by", ColumnType::String, true),
                ColumnSchema::new("filters", ColumnType::String, true),
                ColumnSchema::new("last_refresh", ColumnType::Timestamp, true),
                ColumnSchema::new("result", ColumnType::String, true),
            ],
        }),

        "annotations" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("content", ColumnType::String, true),
                ColumnSchema::new("date_marker", ColumnType::Timestamp, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("created_by", ColumnType::String, true),
                ColumnSchema::new("scope", ColumnType::String, true),
            ],
        }),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Field mappings
// ---------------------------------------------------------------------------

struct FieldMapping {
    fields: &'static [(&'static str, FieldPath)],
}

#[derive(Clone, Copy)]
enum FieldPath {
    Direct(&'static str),
    Nested(&'static str, &'static str),
    JsonObject(&'static str),
    JsonStringArray(&'static str),
}

fn field_mapping(table: &str) -> Option<FieldMapping> {
    match table {
        "events" => Some(FieldMapping {
            fields: &[
                ("uuid", FieldPath::Direct("uuid")),
                ("event", FieldPath::Direct("event")),
                ("distinct_id", FieldPath::Direct("distinct_id")),
                ("timestamp", FieldPath::Direct("timestamp")),
                ("properties", FieldPath::JsonObject("properties")),
                ("elements_chain", FieldPath::Direct("elements_chain")),
            ],
        }),

        "persons" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("uuid", FieldPath::Direct("uuid")),
                ("distinct_ids", FieldPath::JsonStringArray("distinct_ids")),
                ("properties", FieldPath::JsonObject("properties")),
                ("created_at", FieldPath::Direct("created_at")),
                ("is_identified", FieldPath::Direct("is_identified")),
            ],
        }),

        "feature_flags" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("key", FieldPath::Direct("key")),
                ("name", FieldPath::Direct("name")),
                ("active", FieldPath::Direct("active")),
                ("created_at", FieldPath::Direct("created_at")),
                ("created_by", FieldPath::Nested("created_by", "email")),
                ("filters", FieldPath::JsonObject("filters")),
                ("rollout_percentage", FieldPath::Direct("rollout_percentage")),
                (
                    "ensure_experience_continuity",
                    FieldPath::Direct("ensure_experience_continuity"),
                ),
            ],
        }),

        "cohorts" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("description", FieldPath::Direct("description")),
                ("created_at", FieldPath::Direct("created_at")),
                ("created_by", FieldPath::Nested("created_by", "email")),
                ("count", FieldPath::Direct("count")),
                ("is_calculating", FieldPath::Direct("is_calculating")),
                ("is_static", FieldPath::Direct("is_static")),
            ],
        }),

        "insights" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("description", FieldPath::Direct("description")),
                ("created_at", FieldPath::Direct("created_at")),
                ("created_by", FieldPath::Nested("created_by", "email")),
                ("filters", FieldPath::JsonObject("filters")),
                ("last_refresh", FieldPath::Direct("last_refresh")),
                ("result", FieldPath::JsonObject("result")),
            ],
        }),

        "annotations" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("content", FieldPath::Direct("content")),
                ("date_marker", FieldPath::Direct("date_marker")),
                ("created_at", FieldPath::Direct("created_at")),
                ("created_by", FieldPath::Nested("created_by", "email")),
                ("scope", FieldPath::Direct("scope")),
            ],
        }),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Data parsing
// ---------------------------------------------------------------------------

fn parse_timestamp_str(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .map(|ndt| ndt.and_utc().timestamp_micros())
}

fn resolve_field(
    item: &serde_json::Value,
    path: &FieldPath,
) -> Option<serde_json::Value> {
    match path {
        FieldPath::Direct(key) => item.get(key).filter(|v| !v.is_null()).cloned(),
        FieldPath::Nested(parent, child) => item
            .get(parent)
            .and_then(|p| p.get(child))
            .filter(|v| !v.is_null())
            .cloned(),
        FieldPath::JsonObject(key) | FieldPath::JsonStringArray(key) => {
            item.get(key).filter(|v| !v.is_null()).map(|v| {
                serde_json::Value::String(serde_json::to_string(v).unwrap_or_default())
            })
        }
    }
}

fn append_row(
    item: &serde_json::Value,
    table: &str,
    schema: &TableSchema,
    builders: &mut ColumnBuilders,
) {
    let mapping = match field_mapping(table) {
        Some(m) => m,
        None => return,
    };

    for (idx, ((col_name, field_path), col_schema)) in mapping
        .fields
        .iter()
        .zip(schema.columns.iter())
        .enumerate()
    {
        debug_assert_eq!(*col_name, col_schema.name.as_str());

        let raw_val = resolve_field(item, field_path);

        match col_schema.data_type {
            ColumnType::Int64 => {
                let parsed = raw_val.as_ref().and_then(|v| v.as_i64());
                builders.builder(idx).append_i64(parsed);
            }
            ColumnType::Boolean => {
                let parsed = raw_val.as_ref().and_then(|v| v.as_bool());
                builders.builder(idx).append_bool(parsed);
            }
            ColumnType::Timestamp => {
                let parsed = raw_val
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .and_then(parse_timestamp_str);
                builders.builder(idx).append_timestamp(parsed);
            }
            _ => {
                let str_val = raw_val.as_ref().and_then(|v| {
                    if v.is_string() {
                        v.as_str().map(|s| s.to_string())
                    } else if v.is_null() {
                        None
                    } else {
                        Some(v.to_string())
                    }
                });
                builders.builder(idx).append_string(str_val.as_deref());
            }
        }
    }
    builders.row_complete();
}

fn to_arrow_schema(schema: &TableSchema) -> Schema {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), col.nullable))
        .collect();
    Schema::new(fields)
}

// ---------------------------------------------------------------------------
// Connector trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Connector for PostHogConnector {
    fn source_type(&self) -> SourceType {
        SourceType::PostHog
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            let (supports_incremental, incremental_key) = match name {
                "events" => (true, Some("timestamp".to_string())),
                "persons" => (true, Some("created_at".to_string())),
                _ => (false, None),
            };

            let pk = match name {
                "events" => vec!["uuid".to_string()],
                _ => vec!["id".to_string()],
            };

            tables.push(TableInfo {
                name: name.to_string(),
                schema,
                supports_incremental,
                incremental_key,
                estimated_rows: None,
                primary_key_columns: pk,
            });
        }

        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        get_table_schema(table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("Unknown table: {}", table))
        })
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            let batches = self.do_fetch(table, &options).await?;
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        self.api_get(&format!("/api/projects/{}/", self.config.project_id))
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use compact_str::CompactString;

    fn test_config() -> PostHogConfig {
        PostHogConfig::new("test-posthog-key", "123")
    }

    fn test_connector_with_base(base_url: &str) -> PostHogConnector {
        let config = test_config();
        PostHogConnector {
            config,
            http: reqwest::Client::new(),
            base_url_override: Some(base_url.to_string()),
        }
    }

    #[test]
    fn test_all_tables_have_schemas() {
        for &table in ALL_TABLES {
            let schema = get_table_schema(table);
            assert!(schema.is_some(), "Missing schema for table: {}", table);
            assert!(
                !schema.unwrap().columns.is_empty(),
                "Empty schema for table: {}",
                table
            );
        }
    }

    #[test]
    fn test_schema_unknown_table_returns_none() {
        assert!(get_table_schema("nonexistent").is_none());
    }

    #[test]
    fn test_all_tables_have_field_mappings() {
        for &table in ALL_TABLES {
            assert!(
                field_mapping(table).is_some(),
                "Missing field mapping for table: {}",
                table
            );
        }
    }

    #[test]
    fn test_schema_and_mapping_column_count_match() {
        for &table in ALL_TABLES {
            let schema = get_table_schema(table).unwrap();
            let mapping = field_mapping(table).unwrap();
            assert_eq!(
                schema.columns.len(),
                mapping.fields.len(),
                "Column count mismatch for table '{}'",
                table
            );
        }
    }

    #[test]
    fn test_schema_and_mapping_column_names_aligned() {
        for &table in ALL_TABLES {
            let schema = get_table_schema(table).unwrap();
            let mapping = field_mapping(table).unwrap();
            for (i, (col, (mapping_name, _))) in
                schema.columns.iter().zip(mapping.fields.iter()).enumerate()
            {
                assert_eq!(
                    col.name, *mapping_name,
                    "Name mismatch at idx {} for table '{}': schema='{}', mapping='{}'",
                    i, table, col.name, mapping_name
                );
            }
        }
    }

    #[test]
    fn test_config_debug_redacts_secrets() {
        let config = test_config();
        let debug = format!("{:?}", config);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("test-posthog-key"));
        assert!(debug.contains("123"));
    }

    #[test]
    fn test_config_with_api_base() {
        let config = PostHogConfig::new("key", "456")
            .with_api_base("https://posthog.example.com");
        assert_eq!(config.base_url(), "https://posthog.example.com");
    }

    #[test]
    fn test_config_default_base() {
        let config = PostHogConfig::new("key", "456");
        assert_eq!(config.base_url(), "https://app.posthog.com");
    }

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/projects/123/")
            .match_header("Authorization", "Bearer test-posthog-key")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"id":123,"name":"My Project"}"#)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        connector.validate_credentials().await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_validate_credentials_unauthorized() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("GET", "/api/projects/123/")
            .with_status(401)
            .with_body(r#"{"detail":"Authentication credentials were not provided."}"#)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_rate_limit_retry() {
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("GET", "/api/projects/123/")
            .with_status(429)
            .with_header("content-type", "application/json")
            .with_body(r#"{"detail":"Request was throttled."}"#)
            .expect(4)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let result = connector
            .api_get("/api/projects/123/")
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectorError::RateLimited { retry_after_secs } => {
                assert_eq!(retry_after_secs, 60);
            }
            other => panic!("Expected RateLimited error, got: {:?}", other),
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_pagination_two_pages() {
        let mut server = mockito::Server::new_async().await;

        let page2_url = format!("{}/api/projects/123/events/?limit=100&offset=100", server.url());

        let _mock1 = server
            .mock(
                "GET",
                mockito::Matcher::Regex(r"/api/projects/123/events/\?limit=100".to_string()),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "results": [
                        {
                            "uuid": "evt-1",
                            "event": "pageview",
                            "distinct_id": "user-1",
                            "timestamp": "2024-01-01T00:00:00Z",
                            "properties": {"url": "/home"},
                            "elements_chain": null
                        }
                    ],
                    "next": page2_url
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let _mock2 = server
            .mock(
                "GET",
                mockito::Matcher::Regex(
                    r"/api/projects/123/events/\?limit=100&offset=100".to_string(),
                ),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "results": [
                        {
                            "uuid": "evt-2",
                            "event": "click",
                            "distinct_id": "user-2",
                            "timestamp": "2024-01-02T00:00:00Z",
                            "properties": {"button": "signup"},
                            "elements_chain": null
                        }
                    ],
                    "next": null
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("events").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_paginated(
                &format!("/api/projects/123/events/?limit={}", PAGE_LIMIT),
                "events",
                &schema,
                arrow_schema,
                &options,
            )
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        _mock1.assert_async().await;
        _mock2.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_persons() {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock(
                "GET",
                mockito::Matcher::Regex(r"/api/projects/123/persons/\?limit=100".to_string()),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "results": [
                        {
                            "id": 1,
                            "uuid": "person-uuid-1",
                            "distinct_ids": ["user-1", "anon-1"],
                            "properties": {"email": "alice@example.com"},
                            "created_at": "2024-01-15T10:30:00Z",
                            "is_identified": true
                        }
                    ],
                    "next": null
                })
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("persons", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn test_predicate_after_events() {
        let mut query = "/api/projects/123/events/?limit=100".to_string();
        let predicates = vec![Predicate::GreaterThan {
            column: CompactString::from("timestamp"),
            value: CompactString::from("2024-01-01T00:00:00Z"),
            inclusive: false,
        }];
        apply_predicate_params(&mut query, &predicates, "events");
        assert!(query.contains("after=2024-01-01T00:00:00Z"));
    }

    #[test]
    fn test_predicate_event_type() {
        let mut query = "/api/projects/123/events/?limit=100".to_string();
        let predicates = vec![Predicate::Equals {
            column: CompactString::from("event"),
            value: CompactString::from("pageview"),
        }];
        apply_predicate_params(&mut query, &predicates, "events");
        assert!(query.contains("event=pageview"));
    }

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = PostHogConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), ALL_TABLES.len());

        let incremental: Vec<_> = tables.iter().filter(|t| t.supports_incremental).collect();
        assert_eq!(incremental.len(), 2);

        let events_table = tables.iter().find(|t| t.name == "events").unwrap();
        assert_eq!(events_table.incremental_key.as_deref(), Some("timestamp"));
        assert_eq!(events_table.primary_key_columns, vec!["uuid"]);

        let persons_table = tables.iter().find(|t| t.name == "persons").unwrap();
        assert_eq!(persons_table.incremental_key.as_deref(), Some("created_at"));
        assert_eq!(persons_table.primary_key_columns, vec!["id"]);
    }
}
