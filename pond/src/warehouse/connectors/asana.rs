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

const API_BASE: &str = "https://app.asana.com/api/1.0";
const BATCH_CAPACITY: usize = 4096;
const PAGE_LIMIT: u64 = 100;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 50;
const MAX_TOTAL_ROWS: usize = 1_000_000;

const ALL_TABLES: &[&str] = &["tasks", "projects", "sections", "users", "teams", "tags"];

#[derive(Clone)]
pub struct AsanaConfig {
    pub personal_access_token: SecretString,
    pub workspace_gid: String,
}

impl std::fmt::Debug for AsanaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsanaConfig")
            .field("personal_access_token", &"[REDACTED]")
            .field("workspace_gid", &self.workspace_gid)
            .finish()
    }
}

impl AsanaConfig {
    pub fn new(token: impl Into<String>, workspace_gid: impl Into<String>) -> Self {
        Self {
            personal_access_token: SecretString::new(token.into()),
            workspace_gid: workspace_gid.into(),
        }
    }
}

pub struct AsanaConnector {
    config: AsanaConfig,
    http: reqwest::Client,
    #[cfg(test)]
    base_url_override: Option<String>,
}

impl AsanaConnector {
    pub fn new(config: AsanaConfig) -> Self {
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
        format!("{}{}", API_BASE, path)
    }

    async fn api_get(&self, path: &str) -> ConnectorResult<serde_json::Value> {
        let url = self.resolve_url(path);
        let mut attempts = 0u32;

        loop {
            let resp = self
                .http
                .get(&url)
                .header(
                    "Authorization",
                    format!("Bearer {}", self.config.personal_access_token.expose()),
                )
                .header("Accept", "application/json")
                .header("User-Agent", "reiver-connector")
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("Asana request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Asana personal access token".to_string(),
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
                    "Asana API error ({}): {}",
                    status, body
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Asana response: {}", e))
            })?;

            return Ok(json);
        }
    }

    async fn discover_project_gids(&self) -> ConnectorResult<Vec<(String, String)>> {
        let mut projects = Vec::new();
        let mut path = format!(
            "/projects?workspace={}&opt_fields=gid,name&limit={}",
            self.config.workspace_gid, PAGE_LIMIT
        );

        loop {
            let body = self.api_get(&path).await?;
            let items = body
                .get("data")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            for item in &items {
                if let (Some(gid), Some(name)) = (
                    item.get("gid").and_then(|v| v.as_str()),
                    item.get("name").and_then(|v| v.as_str()),
                ) {
                    projects.push((gid.to_string(), name.to_string()));
                }
            }

            let offset = body
                .get("next_page")
                .filter(|v| !v.is_null())
                .and_then(|np| np.get("offset"))
                .and_then(|v| v.as_str())
                .map(String::from);

            match offset {
                Some(token) => {
                    path = format!(
                        "/projects?workspace={}&opt_fields=gid,name&limit={}&offset={}",
                        self.config.workspace_gid, PAGE_LIMIT, token
                    );
                    tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
                }
                None => break,
            }
        }

        Ok(projects)
    }

    async fn fetch_paginated(
        &self,
        initial_path: &str,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
        context: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);
        let base_path = initial_path.to_string();
        let mut current_offset: Option<String> = None;

        loop {
            if total_rows >= max_rows {
                break;
            }

            let path = match &current_offset {
                Some(token) => {
                    let sep = if base_path.contains('?') { '&' } else { '?' };
                    format!("{}{}offset={}", base_path, sep, token)
                }
                None => base_path.clone(),
            };

            let body = self.api_get(&path).await?;

            let items = body
                .get("data")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            for item in &items {
                if total_rows >= max_rows {
                    break;
                }

                append_row(item, table, schema, &mut builders, context);
                total_rows += 1;

                if builders.len() >= BATCH_CAPACITY {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                }
            }

            current_offset = body
                .get("next_page")
                .filter(|v| !v.is_null())
                .and_then(|np| np.get("offset"))
                .and_then(|v| v.as_str())
                .map(String::from);

            match &current_offset {
                Some(_) => {
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

    async fn fetch_sections(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let projects = self.discover_project_gids().await?;
        let mut all_batches = Vec::new();

        for (project_gid, project_name) in &projects {
            let path = format!(
                "/projects/{}/sections?opt_fields=gid,name,created_at&limit={}",
                project_gid, PAGE_LIMIT
            );
            let batches = self
                .fetch_paginated(
                    &path,
                    "sections",
                    schema,
                    arrow_schema.clone(),
                    options,
                    Some(project_name),
                )
                .await?;
            all_batches.extend(batches.into_iter().filter(|b| b.num_rows() > 0));
        }

        if all_batches.is_empty() {
            all_batches.push(RecordBatch::new_empty(arrow_schema));
        }
        Ok(all_batches)
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

        match table {
            "tasks" => {
                let mut query = format!(
                    "/workspaces/{}/tasks?opt_fields=gid,name,assignee.name,completed,completed_at,created_at,modified_at,due_on,notes,num_subtasks,projects.name,tags.name,resource_type&limit={}",
                    self.config.workspace_gid, PAGE_LIMIT
                );
                apply_predicate_params(&mut query, &options.predicates, table);
                if let (Some(key), Some(val)) = (&options.incremental_key, &options.last_value) {
                    if key == "modified_at" {
                        let sep = if query.contains('?') { '&' } else { '?' };
                        query.push_str(&format!("{}modified_since={}", sep, val));
                    }
                }
                self.fetch_paginated(&query, table, &schema, arrow_schema, options, None)
                    .await
            }
            "projects" => {
                let query = format!(
                    "/projects?workspace={}&opt_fields=gid,name,owner.name,created_at,modified_at,due_on,start_on,color,archived,public,notes&limit={}",
                    self.config.workspace_gid, PAGE_LIMIT
                );
                self.fetch_paginated(&query, table, &schema, arrow_schema, options, None)
                    .await
            }
            "sections" => {
                self.fetch_sections(&schema, arrow_schema, options).await
            }
            "users" => {
                let query = format!(
                    "/workspaces/{}/users?opt_fields=gid,name,email,resource_type&limit={}",
                    self.config.workspace_gid, PAGE_LIMIT
                );
                self.fetch_paginated(&query, table, &schema, arrow_schema, options, None)
                    .await
            }
            "teams" => {
                let query = format!(
                    "/organizations/{}/teams?opt_fields=gid,name,description&limit={}",
                    self.config.workspace_gid, PAGE_LIMIT
                );
                self.fetch_paginated(&query, table, &schema, arrow_schema, options, None)
                    .await
            }
            "tags" => {
                let query = format!(
                    "/workspaces/{}/tags?opt_fields=gid,name,color,created_at,notes&limit={}",
                    self.config.workspace_gid, PAGE_LIMIT
                );
                self.fetch_paginated(&query, table, &schema, arrow_schema, options, None)
                    .await
            }
            _ => Err(ConnectorError::TableNotFound(table.to_string())),
        }
    }
}

fn apply_predicate_params(query: &mut String, predicates: &[Predicate], table: &str) {
    for pred in predicates {
        match pred {
            Predicate::GreaterThan {
                column,
                value,
                inclusive: _,
            } if column == "modified_at" && table == "tasks" => {
                let sep = if query.contains('?') { '&' } else { '?' };
                query.push_str(&format!("{}modified_since={}", sep, value));
            }
            _ => {}
        }
    }
}

fn get_table_schema(table: &str) -> Option<TableSchema> {
    match table {
        "tasks" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("gid", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("assignee", ColumnType::String, true),
                ColumnSchema::new("completed", ColumnType::Boolean, false),
                ColumnSchema::new("completed_at", ColumnType::Timestamp, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("modified_at", ColumnType::Timestamp, false),
                ColumnSchema::new("due_on", ColumnType::String, true),
                ColumnSchema::new("notes", ColumnType::String, true),
                ColumnSchema::new("num_subtasks", ColumnType::Int64, false),
                ColumnSchema::new("projects", ColumnType::String, true),
                ColumnSchema::new("tags", ColumnType::String, true),
                ColumnSchema::new("resource_type", ColumnType::String, false),
            ],
        }),

        "projects" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("gid", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("owner", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("modified_at", ColumnType::Timestamp, false),
                ColumnSchema::new("due_on", ColumnType::String, true),
                ColumnSchema::new("start_on", ColumnType::String, true),
                ColumnSchema::new("color", ColumnType::String, true),
                ColumnSchema::new("archived", ColumnType::Boolean, false),
                ColumnSchema::new("public", ColumnType::Boolean, false),
                ColumnSchema::new("notes", ColumnType::String, true),
            ],
        }),

        "sections" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("gid", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("project", ColumnType::String, true),
            ],
        }),

        "users" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("gid", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("email", ColumnType::String, true),
                ColumnSchema::new("resource_type", ColumnType::String, false),
            ],
        }),

        "teams" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("gid", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
            ],
        }),

        "tags" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("gid", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("color", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true),
                ColumnSchema::new("notes", ColumnType::String, true),
            ],
        }),

        _ => None,
    }
}

struct FieldMapping {
    fields: &'static [(&'static str, FieldPath)],
}

#[derive(Clone, Copy)]
enum FieldPath {
    Direct(&'static str),
    Nested(&'static str, &'static str),
    JsonArray(&'static str),
    Context,
}

fn field_mapping(table: &str) -> Option<FieldMapping> {
    match table {
        "tasks" => Some(FieldMapping {
            fields: &[
                ("gid", FieldPath::Direct("gid")),
                ("name", FieldPath::Direct("name")),
                ("assignee", FieldPath::Nested("assignee", "name")),
                ("completed", FieldPath::Direct("completed")),
                ("completed_at", FieldPath::Direct("completed_at")),
                ("created_at", FieldPath::Direct("created_at")),
                ("modified_at", FieldPath::Direct("modified_at")),
                ("due_on", FieldPath::Direct("due_on")),
                ("notes", FieldPath::Direct("notes")),
                ("num_subtasks", FieldPath::Direct("num_subtasks")),
                ("projects", FieldPath::JsonArray("projects")),
                ("tags", FieldPath::JsonArray("tags")),
                ("resource_type", FieldPath::Direct("resource_type")),
            ],
        }),

        "projects" => Some(FieldMapping {
            fields: &[
                ("gid", FieldPath::Direct("gid")),
                ("name", FieldPath::Direct("name")),
                ("owner", FieldPath::Nested("owner", "name")),
                ("created_at", FieldPath::Direct("created_at")),
                ("modified_at", FieldPath::Direct("modified_at")),
                ("due_on", FieldPath::Direct("due_on")),
                ("start_on", FieldPath::Direct("start_on")),
                ("color", FieldPath::Direct("color")),
                ("archived", FieldPath::Direct("archived")),
                ("public", FieldPath::Direct("public")),
                ("notes", FieldPath::Direct("notes")),
            ],
        }),

        "sections" => Some(FieldMapping {
            fields: &[
                ("gid", FieldPath::Direct("gid")),
                ("name", FieldPath::Direct("name")),
                ("created_at", FieldPath::Direct("created_at")),
                ("project", FieldPath::Context),
            ],
        }),

        "users" => Some(FieldMapping {
            fields: &[
                ("gid", FieldPath::Direct("gid")),
                ("name", FieldPath::Direct("name")),
                ("email", FieldPath::Direct("email")),
                ("resource_type", FieldPath::Direct("resource_type")),
            ],
        }),

        "teams" => Some(FieldMapping {
            fields: &[
                ("gid", FieldPath::Direct("gid")),
                ("name", FieldPath::Direct("name")),
                ("description", FieldPath::Direct("description")),
            ],
        }),

        "tags" => Some(FieldMapping {
            fields: &[
                ("gid", FieldPath::Direct("gid")),
                ("name", FieldPath::Direct("name")),
                ("color", FieldPath::Direct("color")),
                ("created_at", FieldPath::Direct("created_at")),
                ("notes", FieldPath::Direct("notes")),
            ],
        }),

        _ => None,
    }
}

fn parse_timestamp_str(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
}

fn resolve_field<'a>(
    item: &'a serde_json::Value,
    path: &FieldPath,
    context: Option<&str>,
) -> Option<serde_json::Value> {
    match path {
        FieldPath::Direct(key) => item.get(key).filter(|v| !v.is_null()).cloned(),
        FieldPath::Nested(parent, child) => item
            .get(parent)
            .and_then(|p| p.get(child))
            .filter(|v| !v.is_null())
            .cloned(),
        FieldPath::JsonArray(key) => {
            item.get(key).filter(|v| !v.is_null()).map(|v| {
                if v.is_array() {
                    let names: Vec<String> = v
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter_map(|el| el.get("name").and_then(|n| n.as_str()).map(String::from))
                        .collect();
                    serde_json::Value::String(serde_json::to_string(&names).unwrap_or_default())
                } else {
                    v.clone()
                }
            })
        }
        FieldPath::Context => context.map(|s| serde_json::Value::String(s.to_string())),
    }
}

fn append_row(
    item: &serde_json::Value,
    table: &str,
    schema: &TableSchema,
    builders: &mut ColumnBuilders,
    context: Option<&str>,
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

        let raw_val = resolve_field(item, field_path, context);

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

#[async_trait]
impl Connector for AsanaConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Asana
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            let (supports_incremental, incremental_key) = match name {
                "tasks" | "projects" => (true, Some("modified_at".to_string())),
                _ => (false, None),
            };

            tables.push(TableInfo {
                name: name.to_string(),
                schema,
                supports_incremental,
                incremental_key,
                estimated_rows: None,
                primary_key_columns: vec!["gid".to_string()],
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
        self.api_get("/users/me").await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compact_str::CompactString;

    fn test_config() -> AsanaConfig {
        AsanaConfig::new("test-asana-token", "workspace-123")
    }

    fn test_connector_with_base(base_url: &str) -> AsanaConnector {
        let config = test_config();
        AsanaConnector {
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
        assert!(!debug.contains("test-asana-token"));
        assert!(debug.contains("workspace-123"));
    }

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/users/me")
            .match_header("Authorization", "Bearer test-asana-token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"data":{"gid":"12345","name":"Test User"}}"#)
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
            .mock("GET", "/users/me")
            .with_status(401)
            .with_body(r#"{"errors":[{"message":"Not Authorized"}]}"#)
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
            .mock("GET", "/users/me")
            .with_status(429)
            .with_header("content-type", "application/json")
            .with_body(r#"{"errors":[{"message":"rate limit exceeded"}]}"#)
            .expect(4)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let result = connector.api_get("/users/me").await;

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

        let _mock1 = server
            .mock(
                "GET",
                mockito::Matcher::Regex(
                    r"/workspaces/workspace-123/tasks\?.*limit=100.*".to_string(),
                ),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": [
                        {
                            "gid": "1001",
                            "name": "Task 1",
                            "completed": false,
                            "created_at": "2024-01-01T00:00:00Z",
                            "modified_at": "2024-06-01T00:00:00Z",
                            "num_subtasks": 0,
                            "resource_type": "task"
                        }
                    ],
                    "next_page": {
                        "offset": "eyJ0eXAiOiJKV1Qi",
                        "uri": "/tasks?limit=100&offset=eyJ0eXAiOiJKV1Qi"
                    }
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
                    r"/workspaces/workspace-123/tasks\?.*offset=eyJ0eXAiOiJKV1Qi.*".to_string(),
                ),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": [
                        {
                            "gid": "1002",
                            "name": "Task 2",
                            "completed": true,
                            "completed_at": "2024-05-01T00:00:00Z",
                            "created_at": "2024-02-01T00:00:00Z",
                            "modified_at": "2024-07-01T00:00:00Z",
                            "num_subtasks": 2,
                            "resource_type": "task"
                        }
                    ],
                    "next_page": null
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("tasks").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_paginated(
                &format!(
                    "/workspaces/workspace-123/tasks?opt_fields=gid,name,assignee.name,completed,completed_at,created_at,modified_at,due_on,notes,num_subtasks,projects.name,tags.name,resource_type&limit={}",
                    PAGE_LIMIT
                ),
                "tasks",
                &schema,
                arrow_schema,
                &options,
                None,
            )
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        _mock1.assert_async().await;
        _mock2.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_projects() {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock(
                "GET",
                mockito::Matcher::Regex(r"/projects\?workspace=workspace-123.*".to_string()),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": [
                        {
                            "gid": "2001",
                            "name": "Project Alpha",
                            "owner": {"name": "Alice"},
                            "created_at": "2024-01-01T00:00:00Z",
                            "modified_at": "2024-06-01T00:00:00Z",
                            "archived": false,
                            "public": true
                        }
                    ],
                    "next_page": null
                })
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("projects", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn test_predicate_modified_at() {
        let mut query = "/workspaces/123/tasks?opt_fields=gid&limit=100".to_string();
        let predicates = vec![Predicate::GreaterThan {
            column: CompactString::from("modified_at"),
            value: CompactString::from("2024-01-01T00:00:00Z"),
            inclusive: false,
        }];
        apply_predicate_params(&mut query, &predicates, "tasks");
        assert!(query.contains("modified_since=2024-01-01T00:00:00Z"));
    }

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = AsanaConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), ALL_TABLES.len());

        let incremental: Vec<_> = tables.iter().filter(|t| t.supports_incremental).collect();
        assert_eq!(incremental.len(), 2);

        let tasks_table = tables.iter().find(|t| t.name == "tasks").unwrap();
        assert_eq!(tasks_table.incremental_key.as_deref(), Some("modified_at"));
        assert_eq!(tasks_table.primary_key_columns, vec!["gid"]);

        let users_table = tables.iter().find(|t| t.name == "users").unwrap();
        assert!(!users_table.supports_incremental);
        assert!(users_table.incremental_key.is_none());
    }
}
