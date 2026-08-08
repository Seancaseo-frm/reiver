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

const API_BASE: &str = "https://api.monday.com/v2";
const BATCH_CAPACITY: usize = 4096;
const PAGE_LIMIT: usize = 100;
const ITEMS_PAGE_LIMIT: usize = 500;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 50;
const MAX_TOTAL_ROWS: usize = 1_000_000;

const ALL_TABLES: &[&str] = &["boards", "items", "users", "teams", "workspaces", "updates"];

#[derive(Clone)]
pub struct MondayConfig {
    pub api_token: SecretString,
    pub board_ids: Option<Vec<u64>>,
}

impl std::fmt::Debug for MondayConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MondayConfig")
            .field("api_token", &"[REDACTED]")
            .field("board_ids", &self.board_ids)
            .finish()
    }
}

impl MondayConfig {
    pub fn new(api_token: impl Into<String>) -> Self {
        Self {
            api_token: SecretString::new(api_token.into()),
            board_ids: None,
        }
    }

    pub fn with_board_ids(mut self, ids: Vec<u64>) -> Self {
        self.board_ids = Some(ids);
        self
    }
}

pub struct MondayConnector {
    config: MondayConfig,
    http: reqwest::Client,
    #[cfg(test)]
    base_url_override: Option<String>,
}

impl MondayConnector {
    pub fn new(config: MondayConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
            #[cfg(test)]
            base_url_override: None,
        }
    }

    fn resolve_url(&self) -> String {
        #[cfg(test)]
        if let Some(ref base) = self.base_url_override {
            return base.clone();
        }
        API_BASE.to_string()
    }

    async fn graphql_query(&self, query: &str) -> ConnectorResult<serde_json::Value> {
        let url = self.resolve_url();
        let mut attempts = 0u32;

        loop {
            let body = serde_json::json!({ "query": query });

            let resp = self
                .http
                .post(&url)
                .header("Authorization", self.config.api_token.expose())
                .header("Content-Type", "application/json")
                .header("API-Version", "2024-10")
                .header("User-Agent", "reiver-connector")
                .json(&body)
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("Monday.com request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Monday.com API token".to_string(),
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
                    "Monday.com API error ({}): {}",
                    status, body
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Monday.com response: {}", e))
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
                            "Monday.com GraphQL errors: {}",
                            msg
                        )));
                    }
                }
            }

            return json
                .get("data")
                .cloned()
                .ok_or_else(|| ConnectorError::Internal("Missing 'data' in response".to_string()));
        }
    }

    async fn discover_board_ids(&self) -> ConnectorResult<Vec<u64>> {
        if let Some(ref ids) = self.config.board_ids {
            return Ok(ids.clone());
        }

        let mut all_ids = Vec::new();
        let mut page = 1usize;

        loop {
            let query = format!(
                "{{ boards(limit: {}, page: {}) {{ id }} }}",
                PAGE_LIMIT, page
            );
            let data = self.graphql_query(&query).await?;
            let boards = data
                .get("boards")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            if boards.is_empty() {
                break;
            }

            for board in &boards {
                if let Some(id) = board
                    .get("id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    all_ids.push(id);
                } else if let Some(id) = board.get("id").and_then(|v| v.as_u64()) {
                    all_ids.push(id);
                }
            }

            page += 1;
            tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        Ok(all_ids)
    }

    async fn fetch_page_numbered(
        &self,
        build_query: impl Fn(usize) -> String,
        data_path: &[&str],
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
        let mut page = 1usize;

        loop {
            if total_rows >= max_rows {
                break;
            }

            let query = build_query(page);
            let data = self.graphql_query(&query).await?;

            let resolved = navigate_path(&data, data_path);
            let items = resolved
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            if items.is_empty() {
                break;
            }

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

            page += 1;
            tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(arrow_schema));
        }
        Ok(batches)
    }

    async fn fetch_boards(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let board_ids = apply_board_filter(&options.predicates);
        self.fetch_page_numbered(
            |page| {
                let ids_arg = match &board_ids {
                    Some(ids) => {
                        let ids_str: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
                        format!("ids: [{}], ", ids_str.join(", "))
                    }
                    None => String::new(),
                };
                format!(
                    "{{ boards({}limit: {}, page: {}) {{ id name description state board_kind created_at updated_at permissions item_terminology workspace_id }} }}",
                    ids_arg, PAGE_LIMIT, page
                )
            },
            &["boards"],
            "boards",
            schema,
            arrow_schema,
            options,
            None,
        )
        .await
    }

    async fn fetch_users(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        self.fetch_page_numbered(
            |page| {
                format!(
                    "{{ users(limit: {}, page: {}) {{ id name email enabled is_admin is_guest created_at title birthday location }} }}",
                    PAGE_LIMIT, page
                )
            },
            &["users"],
            "users",
            schema,
            arrow_schema,
            options,
            None,
        )
        .await
    }

    async fn fetch_teams(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        self.fetch_page_numbered(
            |page| format!("{{ teams(page: {}) {{ id name picture_url }} }}", page),
            &["teams"],
            "teams",
            schema,
            arrow_schema,
            options,
            None,
        )
        .await
    }

    async fn fetch_workspaces(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        self.fetch_page_numbered(
            |page| {
                format!(
                    "{{ workspaces(limit: {}, page: {}) {{ id name kind description created_at }} }}",
                    PAGE_LIMIT, page
                )
            },
            &["workspaces"],
            "workspaces",
            schema,
            arrow_schema,
            options,
            None,
        )
        .await
    }

    async fn fetch_updates(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let board_ids = match apply_board_filter(&options.predicates) {
            Some(ids) => ids,
            None => self.discover_board_ids().await?,
        };

        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for board_id in &board_ids {
            let bid = *board_id;
            let batches = self
                .fetch_page_numbered(
                    move |page| {
                        format!(
                            "{{ boards(ids: [{}]) {{ updates(limit: {}, page: {}) {{ id body text_body created_at updated_at creator_id item_id }} }} }}",
                            bid, PAGE_LIMIT, page
                        )
                    },
                    &["boards", "0", "updates"],
                    "updates",
                    schema,
                    arrow_schema.clone(),
                    options,
                    None,
                )
                .await?;
            all_batches.extend(batches.into_iter().filter(|b| b.num_rows() > 0));
        }

        if all_batches.is_empty() {
            all_batches.push(RecordBatch::new_empty(arrow_schema));
        }
        Ok(all_batches)
    }

    async fn fetch_items(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let board_ids = match apply_board_filter(&options.predicates) {
            Some(ids) => ids,
            None => self.discover_board_ids().await?,
        };

        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        for board_id in &board_ids {
            if total_rows >= max_rows {
                break;
            }

            let board_id_str = board_id.to_string();

            let first_query = format!(
                "{{ boards(ids: [{}]) {{ items_page(limit: {}) {{ cursor items {{ id name group {{ id }} created_at updated_at state column_values {{ id text value }} }} }} }} }}",
                board_id, ITEMS_PAGE_LIMIT
            );

            let data = self.graphql_query(&first_query).await?;

            let boards = data
                .get("boards")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if boards.is_empty() {
                continue;
            }

            let items_page = match boards[0].get("items_page") {
                Some(ip) => ip,
                None => continue,
            };

            let mut cursor = items_page
                .get("cursor")
                .and_then(|v| v.as_str())
                .map(String::from);
            let items = items_page
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            for item in &items {
                if total_rows >= max_rows {
                    break;
                }
                append_row(item, "items", schema, &mut builders, Some(&board_id_str));
                total_rows += 1;

                if builders.len() >= BATCH_CAPACITY {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                }
            }

            while let Some(ref cur) = cursor {
                if total_rows >= max_rows {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;

                let next_query = format!(
                    "{{ next_items_page(cursor: \"{}\", limit: {}) {{ cursor items {{ id name group {{ id }} created_at updated_at state column_values {{ id text value }} }} }} }}",
                    cur, ITEMS_PAGE_LIMIT
                );

                let data = self.graphql_query(&next_query).await?;

                let next_page = match data.get("next_items_page") {
                    Some(np) => np,
                    None => break,
                };

                cursor = next_page
                    .get("cursor")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                let items = next_page
                    .get("items")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();

                if items.is_empty() {
                    break;
                }

                for item in &items {
                    if total_rows >= max_rows {
                        break;
                    }
                    append_row(item, "items", schema, &mut builders, Some(&board_id_str));
                    total_rows += 1;

                    if builders.len() >= BATCH_CAPACITY {
                        batches.push(builders.finish(arrow_schema.clone())?);
                        builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                    }
                }
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

        match table {
            "boards" => self.fetch_boards(&schema, arrow_schema, options).await,
            "items" => self.fetch_items(&schema, arrow_schema, options).await,
            "users" => self.fetch_users(&schema, arrow_schema, options).await,
            "teams" => self.fetch_teams(&schema, arrow_schema, options).await,
            "workspaces" => self.fetch_workspaces(&schema, arrow_schema, options).await,
            "updates" => self.fetch_updates(&schema, arrow_schema, options).await,
            _ => Err(ConnectorError::TableNotFound(table.to_string())),
        }
    }
}

fn navigate_path<'a>(data: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = data;
    for &segment in path {
        current = if let Ok(idx) = segment.parse::<usize>() {
            current.as_array().and_then(|arr| arr.get(idx))?
        } else {
            current.get(segment)?
        };
    }
    Some(current)
}

fn apply_board_filter(predicates: &[Predicate]) -> Option<Vec<u64>> {
    for pred in predicates {
        match pred {
            Predicate::Equals { column, value } if column == "board_id" => {
                if let Ok(id) = value.parse::<u64>() {
                    return Some(vec![id]);
                }
            }
            Predicate::In { column, values } if column == "board_id" => {
                let ids: Vec<u64> = values
                    .iter()
                    .filter_map(|v| v.parse::<u64>().ok())
                    .collect();
                if !ids.is_empty() {
                    return Some(ids);
                }
            }
            _ => {}
        }
    }
    None
}

fn get_table_schema(table: &str) -> Option<TableSchema> {
    match table {
        "boards" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("state", ColumnType::String, false),
                ColumnSchema::new("board_kind", ColumnType::String, false),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true),
                ColumnSchema::new("permissions", ColumnType::String, true),
                ColumnSchema::new("item_terminology", ColumnType::String, true),
                ColumnSchema::new("workspace_id", ColumnType::String, true),
            ],
        }),

        "items" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("board_id", ColumnType::String, false),
                ColumnSchema::new("group_id", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true),
                ColumnSchema::new("state", ColumnType::String, true),
                ColumnSchema::new("column_values", ColumnType::String, true),
            ],
        }),

        "users" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("email", ColumnType::String, true),
                ColumnSchema::new("enabled", ColumnType::Boolean, false),
                ColumnSchema::new("is_admin", ColumnType::Boolean, true),
                ColumnSchema::new("is_guest", ColumnType::Boolean, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true),
                ColumnSchema::new("title", ColumnType::String, true),
                ColumnSchema::new("birthday", ColumnType::String, true),
                ColumnSchema::new("location", ColumnType::String, true),
            ],
        }),

        "teams" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("picture_url", ColumnType::String, true),
            ],
        }),

        "workspaces" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("kind", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true),
            ],
        }),

        "updates" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("body", ColumnType::String, true),
                ColumnSchema::new("text_body", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true),
                ColumnSchema::new("creator_id", ColumnType::String, true),
                ColumnSchema::new("item_id", ColumnType::String, true),
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
    JsonSerialize(&'static str),
    Context,
}

fn field_mapping(table: &str) -> Option<FieldMapping> {
    match table {
        "boards" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("description", FieldPath::Direct("description")),
                ("state", FieldPath::Direct("state")),
                ("board_kind", FieldPath::Direct("board_kind")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
                ("permissions", FieldPath::Direct("permissions")),
                ("item_terminology", FieldPath::Direct("item_terminology")),
                ("workspace_id", FieldPath::Direct("workspace_id")),
            ],
        }),

        "items" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("board_id", FieldPath::Context),
                ("group_id", FieldPath::Nested("group", "id")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
                ("state", FieldPath::Direct("state")),
                ("column_values", FieldPath::JsonSerialize("column_values")),
            ],
        }),

        "users" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("email", FieldPath::Direct("email")),
                ("enabled", FieldPath::Direct("enabled")),
                ("is_admin", FieldPath::Direct("is_admin")),
                ("is_guest", FieldPath::Direct("is_guest")),
                ("created_at", FieldPath::Direct("created_at")),
                ("title", FieldPath::Direct("title")),
                ("birthday", FieldPath::Direct("birthday")),
                ("location", FieldPath::Direct("location")),
            ],
        }),

        "teams" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("picture_url", FieldPath::Direct("picture_url")),
            ],
        }),

        "workspaces" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("kind", FieldPath::Direct("kind")),
                ("description", FieldPath::Direct("description")),
                ("created_at", FieldPath::Direct("created_at")),
            ],
        }),

        "updates" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("body", FieldPath::Direct("body")),
                ("text_body", FieldPath::Direct("text_body")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
                ("creator_id", FieldPath::Direct("creator_id")),
                ("item_id", FieldPath::Direct("item_id")),
            ],
        }),

        _ => None,
    }
}

fn parse_timestamp_str(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
                .ok()
                .map(|dt| dt.and_utc().timestamp_micros())
        })
}

fn resolve_field(
    item: &serde_json::Value,
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
        FieldPath::JsonSerialize(key) => {
            item.get(key).filter(|v| !v.is_null()).map(|v| {
                serde_json::Value::String(serde_json::to_string(v).unwrap_or_default())
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
                let parsed = raw_val.as_ref().and_then(|v| {
                    v.as_i64().or_else(|| {
                        v.as_str().and_then(|s| s.parse::<i64>().ok())
                    })
                });
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
impl Connector for MondayConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Monday
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            let (supports_incremental, incremental_key) = match name {
                "boards" | "items" | "updates" => (true, Some("updated_at".to_string())),
                _ => (false, None),
            };

            tables.push(TableInfo {
                name: name.to_string(),
                schema,
                supports_incremental,
                incremental_key,
                estimated_rows: None,
                primary_key_columns: vec!["id".to_string()],
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
        self.graphql_query("{ me { id name } }").await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compact_str::CompactString;

    fn test_config() -> MondayConfig {
        MondayConfig::new("test-monday-token")
    }

    fn test_connector_with_base(base_url: &str) -> MondayConnector {
        MondayConnector {
            config: test_config(),
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
        assert!(!debug.contains("test-monday-token"));
    }

    #[test]
    fn test_config_with_board_ids() {
        let config = MondayConfig::new("token").with_board_ids(vec![111, 222]);
        assert_eq!(config.board_ids.as_ref().unwrap(), &vec![111u64, 222]);
    }

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_header("Authorization", "test-monday-token")
            .match_header("API-Version", "2024-10")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": { "me": { "id": 12345, "name": "Test User" } }
                })
                .to_string(),
            )
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
            .mock("POST", "/")
            .with_status(401)
            .with_body(r#"{"error":"Not Authenticated"}"#)
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
            .mock("POST", "/")
            .with_status(429)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"rate limit exceeded"}"#)
            .expect(4)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let result = connector.graphql_query("{ me { id } }").await;

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
    async fn test_fetch_boards_pagination() {
        let mut server = mockito::Server::new_async().await;

        let _mock1 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r"page: 1\)".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": {
                        "boards": [
                            {
                                "id": "1001",
                                "name": "Project Alpha",
                                "description": "First board",
                                "state": "active",
                                "board_kind": "public",
                                "created_at": "2024-01-15T10:30:00Z",
                                "updated_at": "2024-06-01T12:00:00Z",
                                "permissions": "everyone",
                                "item_terminology": "items",
                                "workspace_id": "100"
                            }
                        ]
                    }
                })
                .to_string(),
            )
            .create_async()
            .await;

        let _mock2 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r"page: 2\)".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": {
                        "boards": [
                            {
                                "id": "1002",
                                "name": "Project Beta",
                                "description": null,
                                "state": "active",
                                "board_kind": "private",
                                "created_at": "2024-02-01T08:00:00Z",
                                "updated_at": "2024-07-01T09:00:00Z",
                                "permissions": "owners",
                                "item_terminology": null,
                                "workspace_id": null
                            }
                        ]
                    }
                })
                .to_string(),
            )
            .create_async()
            .await;

        let _mock3 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r"page: 3\)".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({ "data": { "boards": [] } }).to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("boards").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_boards(&schema, arrow_schema, &options)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_fetch_users() {
        let mut server = mockito::Server::new_async().await;

        let _mock1 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r"users\(limit: 100, page: 1\)".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": {
                        "users": [
                            {
                                "id": 42,
                                "name": "Alice",
                                "email": "alice@example.com",
                                "enabled": true,
                                "is_admin": true,
                                "is_guest": false,
                                "created_at": "2024-01-10T09:00:00Z",
                                "title": "Engineer",
                                "birthday": "1990-05-20",
                                "location": "NYC"
                            }
                        ]
                    }
                })
                .to_string(),
            )
            .create_async()
            .await;

        let _mock2 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r"users\(limit: 100, page: 2\)".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({ "data": { "users": [] } }).to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("users").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_users(&schema, arrow_schema, &options)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_fetch_items_cursor_pagination() {
        let mut server = mockito::Server::new_async().await;

        let _mock_first = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r"boards\(ids: \[999\]\)".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": {
                        "boards": [{
                            "items_page": {
                                "cursor": "next_cursor_abc",
                                "items": [
                                    {
                                        "id": "5001",
                                        "name": "Task A",
                                        "group": { "id": "group_1" },
                                        "created_at": "2024-03-01T10:00:00Z",
                                        "updated_at": "2024-06-01T11:00:00Z",
                                        "state": "active",
                                        "column_values": [
                                            { "id": "status", "text": "Done", "value": null }
                                        ]
                                    }
                                ]
                            }
                        }]
                    }
                })
                .to_string(),
            )
            .create_async()
            .await;

        let _mock_next = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r"next_items_page".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": {
                        "next_items_page": {
                            "cursor": null,
                            "items": [
                                {
                                    "id": "5002",
                                    "name": "Task B",
                                    "group": { "id": "group_2" },
                                    "created_at": "2024-04-01T10:00:00Z",
                                    "updated_at": "2024-07-01T11:00:00Z",
                                    "state": "active",
                                    "column_values": []
                                }
                            ]
                        }
                    }
                })
                .to_string(),
            )
            .create_async()
            .await;

        let config = MondayConfig::new("test-monday-token").with_board_ids(vec![999]);
        let connector = MondayConnector {
            config,
            http: reqwest::Client::new(),
            base_url_override: Some(server.url()),
        };

        let schema = get_table_schema("items").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_items(&schema, arrow_schema, &options)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_board_filter_predicate() {
        let predicates = vec![Predicate::Equals {
            column: CompactString::from("board_id"),
            value: CompactString::from("12345"),
        }];
        let result = apply_board_filter(&predicates);
        assert_eq!(result, Some(vec![12345u64]));

        let predicates = vec![Predicate::In {
            column: CompactString::from("board_id"),
            values: vec![
                CompactString::from("100"),
                CompactString::from("200"),
                CompactString::from("300"),
            ],
        }];
        let result = apply_board_filter(&predicates);
        assert_eq!(result, Some(vec![100u64, 200, 300]));

        let predicates = vec![Predicate::Equals {
            column: CompactString::from("name"),
            value: CompactString::from("test"),
        }];
        let result = apply_board_filter(&predicates);
        assert!(result.is_none());
    }

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = MondayConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), ALL_TABLES.len());

        let incremental: Vec<_> = tables.iter().filter(|t| t.supports_incremental).collect();
        assert_eq!(incremental.len(), 3);

        let boards_table = tables.iter().find(|t| t.name == "boards").unwrap();
        assert_eq!(boards_table.incremental_key.as_deref(), Some("updated_at"));
        assert_eq!(boards_table.primary_key_columns, vec!["id"]);

        let users_table = tables.iter().find(|t| t.name == "users").unwrap();
        assert!(!users_table.supports_incremental);
        assert!(users_table.incremental_key.is_none());
    }
}
