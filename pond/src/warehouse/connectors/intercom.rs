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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const API_BASE: &str = "https://api.intercom.io";
const API_VERSION: &str = "2.11";
const BATCH_CAPACITY: usize = 4096;
const PAGE_LIMIT: u64 = 150;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 100;
const MAX_TOTAL_ROWS: usize = 1_000_000;

const ALL_TABLES: &[&str] = &[
    "conversations",
    "contacts",
    "companies",
    "tags",
    "teams",
];

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct IntercomConfig {
    pub access_token: SecretString,
    pub api_base: Option<String>,
}

impl std::fmt::Debug for IntercomConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IntercomConfig")
            .field("access_token", &"[REDACTED]")
            .field("api_base", &self.api_base)
            .finish()
    }
}

impl IntercomConfig {
    pub fn new(access_token: impl Into<String>) -> Self {
        Self {
            access_token: SecretString::new(access_token.into()),
            api_base: None,
        }
    }

    pub fn with_region(mut self, region: &str) -> Self {
        self.api_base = Some(match region {
            "eu" | "EU" => "https://api.eu.intercom.io".to_string(),
            "au" | "AU" => "https://api.au.intercom.io".to_string(),
            _ => API_BASE.to_string(),
        });
        self
    }

    fn base_url(&self) -> &str {
        self.api_base.as_deref().unwrap_or(API_BASE)
    }
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct IntercomConnector {
    config: IntercomConfig,
    http: reqwest::Client,
    #[cfg(test)]
    base_url_override: Option<String>,
}

impl IntercomConnector {
    pub fn new(config: IntercomConfig) -> Self {
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
                .header("Authorization", format!("Bearer {}", self.config.access_token.expose()))
                .header("Accept", "application/json")
                .header("Intercom-Version", API_VERSION)
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("Intercom request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Intercom access token".to_string(),
                ));
            }

            if resp.status() == 429 {
                if attempts < MAX_RETRIES {
                    attempts += 1;
                    let delay = self.retry_delay_from_response(&resp, attempts);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                return Err(ConnectorError::RateLimited { retry_after_secs: 60 });
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "Intercom API error ({}): {}",
                    status, body
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Intercom response: {}", e))
            })?;

            return Ok(json);
        }
    }

    async fn api_post(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> ConnectorResult<serde_json::Value> {
        let url = self.resolve_url(path);
        let mut attempts = 0u32;

        loop {
            let resp = self
                .http
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.config.access_token.expose()))
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .header("Intercom-Version", API_VERSION)
                .json(body)
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("Intercom request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Intercom access token".to_string(),
                ));
            }

            if resp.status() == 429 {
                if attempts < MAX_RETRIES {
                    attempts += 1;
                    let delay = self.retry_delay_from_response(&resp, attempts);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                return Err(ConnectorError::RateLimited { retry_after_secs: 60 });
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body_text = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "Intercom API error ({}): {}",
                    status, body_text
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Intercom response: {}", e))
            })?;

            return Ok(json);
        }
    }

    fn retry_delay_from_response(&self, resp: &reqwest::Response, attempt: u32) -> u64 {
        if let Some(reset) = resp
            .headers()
            .get("X-RateLimit-Reset")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
        {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let wait = reset.saturating_sub(now);
            if wait > 0 && wait < 120 {
                return wait * 1000;
            }
        }
        INITIAL_RETRY_DELAY_MS * 2u64.pow(attempt - 1)
    }

    // -----------------------------------------------------------------------
    // Fetch strategies
    // -----------------------------------------------------------------------

    async fn fetch_cursor_paginated(
        &self,
        path: &str,
        items_key: &str,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        let mut url = format!("{}?per_page={}", path, PAGE_LIMIT);

        loop {
            if total_rows >= max_rows {
                break;
            }

            let data = self.api_get(&url).await?;
            let items = data
                .get(items_key)
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

            let next_cursor = data
                .get("pages")
                .and_then(|p| p.get("next"))
                .and_then(|n| n.get("starting_after"))
                .and_then(|v| v.as_str());

            match next_cursor {
                Some(cursor) => {
                    url = format!(
                        "{}?per_page={}&starting_after={}",
                        path, PAGE_LIMIT, urlencoding::encode(cursor),
                    );
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

    async fn fetch_companies_paginated(
        &self,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);
        let mut page = 1u64;

        loop {
            if total_rows >= max_rows {
                break;
            }

            let body = serde_json::json!({
                "per_page": PAGE_LIMIT,
                "page": page,
                "order": "asc",
            });

            let data = self.api_post("/companies/list", &body).await?;
            let items = data
                .get("data")
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
                append_row(item, table, schema, &mut builders);
                total_rows += 1;

                if builders.len() >= BATCH_CAPACITY {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                }
            }

            let total_pages = data
                .get("pages")
                .and_then(|p| p.get("total_pages"))
                .and_then(|v| v.as_u64())
                .unwrap_or(1);

            if page >= total_pages {
                break;
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

    async fn fetch_simple(
        &self,
        path: &str,
        items_key: &str,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let data = self.api_get(path).await?;
        let items = data
            .get(items_key)
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut builders = ColumnBuilders::new(schema, items.len().max(1));

        for item in &items {
            append_row(item, table, schema, &mut builders);
        }

        if builders.len() > 0 {
            Ok(vec![builders.finish(arrow_schema)?])
        } else {
            Ok(vec![RecordBatch::new_empty(arrow_schema)])
        }
    }

    async fn fetch_search(
        &self,
        search_path: &str,
        items_key: &str,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        query: serde_json::Value,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);
        let mut starting_after: Option<String> = None;

        loop {
            if total_rows >= max_rows {
                break;
            }

            let mut body = serde_json::json!({
                "query": query,
                "pagination": {
                    "per_page": PAGE_LIMIT,
                },
            });

            if let Some(ref cursor) = starting_after {
                body["pagination"]["starting_after"] = serde_json::Value::String(cursor.clone());
            }

            let data = self.api_post(search_path, &body).await?;
            let items = data
                .get(items_key)
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

            let next_cursor = data
                .get("pages")
                .and_then(|p| p.get("next"))
                .and_then(|n| n.get("starting_after"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            match next_cursor {
                Some(cursor) => {
                    starting_after = Some(cursor);
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

        match table {
            "conversations" => {
                if let Some(query) = build_search_query(&options.predicates) {
                    return self
                        .fetch_search(
                            "/conversations/search",
                            "conversations",
                            table,
                            &schema,
                            arrow_schema,
                            query,
                            options,
                        )
                        .await;
                }
                self.fetch_cursor_paginated(
                    "/conversations",
                    "conversations",
                    table,
                    &schema,
                    arrow_schema,
                    options,
                )
                .await
            }
            "contacts" => {
                if let Some(query) = build_search_query(&options.predicates) {
                    return self
                        .fetch_search(
                            "/contacts/search",
                            "data",
                            table,
                            &schema,
                            arrow_schema,
                            query,
                            options,
                        )
                        .await;
                }
                self.fetch_cursor_paginated(
                    "/contacts",
                    "data",
                    table,
                    &schema,
                    arrow_schema,
                    options,
                )
                .await
            }
            "companies" => {
                self.fetch_companies_paginated(table, &schema, arrow_schema, options)
                    .await
            }
            "tags" => {
                self.fetch_simple("/tags", "data", table, &schema, arrow_schema)
                    .await
            }
            "teams" => {
                self.fetch_simple("/teams", "teams", table, &schema, arrow_schema)
                    .await
            }
            _ => Err(ConnectorError::TableNotFound(table.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Table schemas
// ---------------------------------------------------------------------------

fn get_table_schema(table: &str) -> Option<TableSchema> {
    match table {
        "conversations" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("title", ColumnType::String, true),
                ColumnSchema::new("state", ColumnType::String, false),
                ColumnSchema::new("open", ColumnType::Boolean, false),
                ColumnSchema::new("read", ColumnType::Boolean, false),
                ColumnSchema::new("priority", ColumnType::String, true),
                ColumnSchema::new("admin_assignee_id", ColumnType::Int64, true),
                ColumnSchema::new("team_assignee_id", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false),
                ColumnSchema::new("waiting_since", ColumnType::Timestamp, true),
            ],
        }),

        "contacts" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("email", ColumnType::String, true),
                ColumnSchema::new("role", ColumnType::String, false),
                ColumnSchema::new("phone", ColumnType::String, true),
                ColumnSchema::new("external_id", ColumnType::String, true),
                ColumnSchema::new("owner_id", ColumnType::Int64, true),
                ColumnSchema::new("city", ColumnType::String, true),
                ColumnSchema::new("country", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false),
                ColumnSchema::new("signed_up_at", ColumnType::Timestamp, true),
                ColumnSchema::new("last_seen_at", ColumnType::Timestamp, true),
                ColumnSchema::new("last_contacted_at", ColumnType::Timestamp, true),
            ],
        }),

        "companies" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("company_id", ColumnType::String, true),
                ColumnSchema::new("plan_name", ColumnType::String, true),
                ColumnSchema::new("industry", ColumnType::String, true),
                ColumnSchema::new("website", ColumnType::String, true),
                ColumnSchema::new("user_count", ColumnType::Int64, true),
                ColumnSchema::new("monthly_spend", ColumnType::Int64, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false),
                ColumnSchema::new("size", ColumnType::Int64, true),
            ],
        }),

        "tags" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
            ],
        }),

        "teams" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("admin_ids", ColumnType::String, true),
            ],
        }),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Field mappings: schema column name -> API JSON path
// ---------------------------------------------------------------------------

struct FieldMapping {
    fields: &'static [(&'static str, FieldPath)],
}

#[derive(Clone, Copy)]
enum FieldPath {
    Direct(&'static str),
    Nested(&'static str, &'static str),
    JsonArray(&'static str),
}

fn field_mapping(table: &str) -> Option<FieldMapping> {
    match table {
        "conversations" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("title", FieldPath::Direct("title")),
                ("state", FieldPath::Direct("state")),
                ("open", FieldPath::Direct("open")),
                ("read", FieldPath::Direct("read")),
                ("priority", FieldPath::Direct("priority")),
                ("admin_assignee_id", FieldPath::Direct("admin_assignee_id")),
                ("team_assignee_id", FieldPath::Direct("team_assignee_id")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
                ("waiting_since", FieldPath::Direct("waiting_since")),
            ],
        }),

        "contacts" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("email", FieldPath::Direct("email")),
                ("role", FieldPath::Direct("role")),
                ("phone", FieldPath::Direct("phone")),
                ("external_id", FieldPath::Direct("external_id")),
                ("owner_id", FieldPath::Direct("owner_id")),
                ("city", FieldPath::Nested("location", "city")),
                ("country", FieldPath::Nested("location", "country")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
                ("signed_up_at", FieldPath::Direct("signed_up_at")),
                ("last_seen_at", FieldPath::Direct("last_seen_at")),
                ("last_contacted_at", FieldPath::Direct("last_contacted_at")),
            ],
        }),

        "companies" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("company_id", FieldPath::Direct("company_id")),
                ("plan_name", FieldPath::Nested("plan", "name")),
                ("industry", FieldPath::Direct("industry")),
                ("website", FieldPath::Direct("website")),
                ("user_count", FieldPath::Direct("user_count")),
                ("monthly_spend", FieldPath::Direct("monthly_spend")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
                ("size", FieldPath::Direct("size")),
            ],
        }),

        "tags" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
            ],
        }),

        "teams" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("admin_ids", FieldPath::JsonArray("admin_ids")),
            ],
        }),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Data parsing
// ---------------------------------------------------------------------------

fn unix_ts_to_micros(val: &serde_json::Value) -> Option<i64> {
    val.as_i64().map(|secs| secs * 1_000_000)
}

fn resolve_field<'a>(item: &'a serde_json::Value, path: &FieldPath) -> Option<&'a serde_json::Value> {
    match path {
        FieldPath::Direct(key) => item.get(key).filter(|v| !v.is_null()),
        FieldPath::Nested(parent, child) => {
            item.get(parent)
                .and_then(|p| p.get(child))
                .filter(|v| !v.is_null())
        }
        FieldPath::JsonArray(key) => item.get(key).filter(|v| !v.is_null()),
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
                let parsed = raw_val.and_then(|v| v.as_i64());
                builders.builder(idx).append_i64(parsed);
            }
            ColumnType::Float64 => {
                let parsed = raw_val.and_then(|v| {
                    v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))
                });
                builders.builder(idx).append_f64(parsed);
            }
            ColumnType::Boolean => {
                let parsed = raw_val.and_then(|v| v.as_bool());
                builders.builder(idx).append_bool(parsed);
            }
            ColumnType::Timestamp => {
                let parsed = raw_val.and_then(unix_ts_to_micros);
                builders.builder(idx).append_timestamp(parsed);
            }
            _ => {
                let str_val = raw_val.and_then(|v| match field_path {
                    FieldPath::JsonArray(_) => {
                        if v.is_array() {
                            Some(v.to_string())
                        } else {
                            None
                        }
                    }
                    _ => {
                        if v.is_string() {
                            v.as_str().map(|s| s.to_string())
                        } else if v.is_null() {
                            None
                        } else {
                            Some(v.to_string())
                        }
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
// Predicate pushdown -> Intercom search query
// ---------------------------------------------------------------------------

fn build_search_query(predicates: &[Predicate]) -> Option<serde_json::Value> {
    if predicates.is_empty() {
        return None;
    }

    let filters: Vec<serde_json::Value> = predicates
        .iter()
        .filter_map(predicate_to_filter)
        .collect();

    if filters.is_empty() {
        return None;
    }

    if filters.len() == 1 {
        return Some(filters.into_iter().next().unwrap());
    }

    Some(serde_json::json!({
        "operator": "AND",
        "value": filters,
    }))
}

fn predicate_to_filter(pred: &Predicate) -> Option<serde_json::Value> {
    match pred {
        Predicate::Equals { column, value } => Some(serde_json::json!({
            "field": column.as_str(),
            "operator": "=",
            "value": value.as_str(),
        })),
        Predicate::GreaterThan {
            column,
            value,
            inclusive: _,
        } => Some(serde_json::json!({
            "field": column.as_str(),
            "operator": ">",
            "value": value.as_str(),
        })),
        Predicate::LessThan {
            column,
            value,
            inclusive: _,
        } => Some(serde_json::json!({
            "field": column.as_str(),
            "operator": "<",
            "value": value.as_str(),
        })),
        Predicate::In { column, values } => {
            let vals: Vec<&str> = values.iter().map(|v| v.as_str()).collect();
            Some(serde_json::json!({
                "field": column.as_str(),
                "operator": "IN",
                "value": vals,
            }))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Connector trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Connector for IntercomConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Intercom
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            let (supports_incremental, incremental_key) = match name {
                "conversations" | "contacts" | "companies" => {
                    (true, Some("updated_at".to_string()))
                }
                _ => (false, None),
            };

            let pk = vec!["id".to_string()];

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
        self.api_get("/me").await?;
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

    fn test_config() -> IntercomConfig {
        IntercomConfig::new("test-access-token")
    }

    fn test_connector_with_base(base_url: &str) -> IntercomConnector {
        let config = test_config();
        IntercomConnector {
            config,
            http: reqwest::Client::new(),
            base_url_override: Some(base_url.to_string()),
        }
    }

    // -- Schema tests --

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
    fn test_conversations_schema() {
        let schema = get_table_schema("conversations").unwrap();
        assert_eq!(schema.columns.len(), 11);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].data_type, ColumnType::String);
        assert!(!schema.columns[0].nullable);
    }

    #[test]
    fn test_contacts_schema() {
        let schema = get_table_schema("contacts").unwrap();
        assert_eq!(schema.columns.len(), 14);
        let city = schema.columns.iter().find(|c| c.name == "city").unwrap();
        assert_eq!(city.data_type, ColumnType::String);
        assert!(city.nullable);
    }

    #[test]
    fn test_companies_schema() {
        let schema = get_table_schema("companies").unwrap();
        assert_eq!(schema.columns.len(), 11);
        let plan = schema.columns.iter().find(|c| c.name == "plan_name").unwrap();
        assert_eq!(plan.data_type, ColumnType::String);
        assert!(plan.nullable);
    }

    // -- Field mapping tests --

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

    // -- Config tests --

    #[test]
    fn test_config_debug_redacts_secrets() {
        let config = test_config();
        let debug = format!("{:?}", config);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("test-access-token"));
    }

    #[test]
    fn test_config_region_eu() {
        let config = IntercomConfig::new("token").with_region("eu");
        assert_eq!(config.base_url(), "https://api.eu.intercom.io");
    }

    #[test]
    fn test_config_region_au() {
        let config = IntercomConfig::new("token").with_region("au");
        assert_eq!(config.base_url(), "https://api.au.intercom.io");
    }

    #[test]
    fn test_config_region_default() {
        let config = IntercomConfig::new("token");
        assert_eq!(config.base_url(), "https://api.intercom.io");
    }

    // -- Timestamp parsing --

    #[test]
    fn test_unix_ts_to_micros() {
        let val = serde_json::json!(1700000000);
        assert_eq!(unix_ts_to_micros(&val), Some(1_700_000_000_000_000));
    }

    #[test]
    fn test_unix_ts_to_micros_null() {
        let val = serde_json::json!(null);
        assert!(unix_ts_to_micros(&val).is_none());
    }

    #[test]
    fn test_unix_ts_to_micros_string() {
        let val = serde_json::json!("not-a-number");
        assert!(unix_ts_to_micros(&val).is_none());
    }

    // -- Predicate pushdown tests --

    #[test]
    fn test_build_search_query_equals() {
        let predicates = vec![Predicate::Equals {
            column: CompactString::from("email"),
            value: CompactString::from("user@test.com"),
        }];
        let query = build_search_query(&predicates).unwrap();
        assert_eq!(query["field"].as_str().unwrap(), "email");
        assert_eq!(query["operator"].as_str().unwrap(), "=");
        assert_eq!(query["value"].as_str().unwrap(), "user@test.com");
    }

    #[test]
    fn test_build_search_query_multiple_and() {
        let predicates = vec![
            Predicate::Equals {
                column: CompactString::from("role"),
                value: CompactString::from("user"),
            },
            Predicate::GreaterThan {
                column: CompactString::from("created_at"),
                value: CompactString::from("1700000000"),
                inclusive: false,
            },
        ];
        let query = build_search_query(&predicates).unwrap();
        assert_eq!(query["operator"].as_str().unwrap(), "AND");
        assert_eq!(query["value"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_build_search_query_in() {
        let predicates = vec![Predicate::In {
            column: CompactString::from("state"),
            values: vec![
                CompactString::from("open"),
                CompactString::from("closed"),
            ],
        }];
        let query = build_search_query(&predicates).unwrap();
        assert_eq!(query["operator"].as_str().unwrap(), "IN");
        let vals = query["value"].as_array().unwrap();
        assert_eq!(vals.len(), 2);
    }

    #[test]
    fn test_build_search_query_empty() {
        assert!(build_search_query(&[]).is_none());
    }

    #[test]
    fn test_build_search_query_unsupported_predicate() {
        let predicates = vec![Predicate::Like {
            column: CompactString::from("name"),
            pattern: CompactString::from("%test%"),
        }];
        assert!(build_search_query(&predicates).is_none());
    }

    // -- list_tables --

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = IntercomConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), ALL_TABLES.len());

        let incremental: Vec<_> = tables
            .iter()
            .filter(|t| t.supports_incremental)
            .collect();
        assert_eq!(incremental.len(), 3);

        let non_incremental: Vec<_> = tables
            .iter()
            .filter(|t| !t.supports_incremental)
            .collect();
        assert_eq!(non_incremental.len(), 2);
    }

    // -- Mock tests --

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/me")
            .match_header("Authorization", "Bearer test-access-token")
            .match_header("Intercom-Version", API_VERSION)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"type":"admin","id":"12345","name":"Test Admin"}"#)
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
            .mock("GET", "/me")
            .with_status(401)
            .with_body(r#"{"type":"error.list","errors":[{"code":"token_unauthorized"}]}"#)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_cursor_pagination_two_pages() {
        let mut server = mockito::Server::new_async().await;

        let _mock1 = server
            .mock("GET", mockito::Matcher::Regex(r"/tags\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "type": "list",
                    "data": [
                        {"type": "tag", "id": "1", "name": "VIP"},
                        {"type": "tag", "id": "2", "name": "Bug"},
                    ],
                    "pages": {
                        "next": {
                            "starting_after": "cursor_abc"
                        }
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
                mockito::Matcher::Regex(r"/tags\?.*starting_after=cursor_abc.*".to_string()),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "type": "list",
                    "data": [
                        {"type": "tag", "id": "3", "name": "Feature"},
                    ],
                    "pages": {}
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("tags").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_cursor_paginated("/tags", "data", "tags", &schema, arrow_schema, &options)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        _mock1.assert_async().await;
        _mock2.assert_async().await;
    }

    #[tokio::test]
    async fn test_rate_limit_retry() {
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("GET", "/me")
            .with_status(429)
            .with_header("content-type", "application/json")
            .with_body(r#"{"type":"error.list","errors":[{"code":"rate_limit"}]}"#)
            .expect(4)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let result = connector.api_get("/me").await;

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
    async fn test_search_endpoint() {
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("POST", "/contacts/search")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "type": "list",
                    "data": [
                        {
                            "type": "contact",
                            "id": "c1",
                            "name": "Alice",
                            "email": "alice@test.com",
                            "role": "user",
                            "created_at": 1700000000,
                            "updated_at": 1700100000,
                        }
                    ],
                    "pages": {}
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("contacts").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));

        let options = FetchOptions {
            predicates: vec![Predicate::Equals {
                column: CompactString::from("email"),
                value: CompactString::from("alice@test.com"),
            }],
            ..Default::default()
        };

        let query = build_search_query(&options.predicates).unwrap();
        let batches = connector
            .fetch_search("/contacts/search", "data", "contacts", &schema, arrow_schema, query, &options)
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_companies_paginated() {
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("POST", "/companies/list")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "type": "list",
                    "data": [
                        {
                            "type": "company",
                            "id": "co1",
                            "name": "Acme Inc",
                            "company_id": "acme",
                            "plan": {"name": "Pro"},
                            "user_count": 42,
                            "created_at": 1700000000,
                            "updated_at": 1700100000,
                        }
                    ],
                    "pages": {
                        "total_pages": 1,
                        "page": 1,
                    }
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("companies", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_simple_fetch_teams() {
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("GET", "/teams")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "type": "team.list",
                    "teams": [
                        {
                            "type": "team",
                            "id": "t1",
                            "name": "Support",
                            "admin_ids": [1, 2, 3],
                        },
                        {
                            "type": "team",
                            "id": "t2",
                            "name": "Sales",
                            "admin_ids": [4],
                        }
                    ]
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("teams", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        mock.assert_async().await;
    }
}
