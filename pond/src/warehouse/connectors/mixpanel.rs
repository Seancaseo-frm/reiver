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

const EXPORT_API_BASE: &str = "https://data.mixpanel.com/api/2.0";
const QUERY_API_BASE: &str = "https://mixpanel.com/api/2.0";
const BATCH_CAPACITY: usize = 4096;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 100;
const MAX_TOTAL_ROWS: usize = 1_000_000;

const ALL_TABLES: &[&str] = &["events", "people", "funnels", "cohorts"];

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct MixpanelConfig {
    pub api_secret: SecretString,
    pub project_id: String,
    pub region: Option<String>,
}

impl std::fmt::Debug for MixpanelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MixpanelConfig")
            .field("api_secret", &"[REDACTED]")
            .field("project_id", &self.project_id)
            .field("region", &self.region)
            .finish()
    }
}

impl MixpanelConfig {
    pub fn new(api_secret: impl Into<String>, project_id: impl Into<String>) -> Self {
        Self {
            api_secret: SecretString::new(api_secret.into()),
            project_id: project_id.into(),
            region: None,
        }
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    fn export_base_url(&self) -> &str {
        match self.region.as_deref() {
            Some("eu") => "https://data-eu.mixpanel.com/api/2.0",
            Some("in") => "https://data-in.mixpanel.com/api/2.0",
            _ => EXPORT_API_BASE,
        }
    }

    fn query_base_url(&self) -> &str {
        match self.region.as_deref() {
            Some("eu") => "https://eu.mixpanel.com/api/2.0",
            Some("in") => "https://in.mixpanel.com/api/2.0",
            _ => QUERY_API_BASE,
        }
    }
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct MixpanelConnector {
    config: MixpanelConfig,
    http: reqwest::Client,
    #[cfg(test)]
    export_base_override: Option<String>,
    #[cfg(test)]
    query_base_override: Option<String>,
}

impl MixpanelConnector {
    pub fn new(config: MixpanelConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
            #[cfg(test)]
            export_base_override: None,
            #[cfg(test)]
            query_base_override: None,
        }
    }

    fn resolve_export_url(&self, path: &str) -> String {
        if path.starts_with("https://") || path.starts_with("http://") {
            return path.to_string();
        }
        #[cfg(test)]
        if let Some(ref base) = self.export_base_override {
            return format!("{}{}", base, path);
        }
        format!("{}{}", self.config.export_base_url(), path)
    }

    fn resolve_query_url(&self, path: &str) -> String {
        if path.starts_with("https://") || path.starts_with("http://") {
            return path.to_string();
        }
        #[cfg(test)]
        if let Some(ref base) = self.query_base_override {
            return format!("{}{}", base, path);
        }
        format!("{}{}", self.config.query_base_url(), path)
    }

    // -----------------------------------------------------------------------
    // HTTP helpers
    // -----------------------------------------------------------------------

    async fn api_get(&self, url: &str) -> ConnectorResult<serde_json::Value> {
        let mut attempts = 0u32;

        loop {
            let resp = self
                .http
                .get(url)
                .basic_auth("", Some(self.config.api_secret.expose()))
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("Mixpanel request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Mixpanel API secret".to_string(),
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
                    "Mixpanel API error ({}): {}",
                    status, body
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Mixpanel response: {}", e))
            })?;

            return Ok(json);
        }
    }

    async fn api_get_text(&self, url: &str) -> ConnectorResult<String> {
        let mut attempts = 0u32;

        loop {
            let resp = self
                .http
                .get(url)
                .basic_auth("", Some(self.config.api_secret.expose()))
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("Mixpanel request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Mixpanel API secret".to_string(),
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
                    "Mixpanel API error ({}): {}",
                    status, body
                )));
            }

            let text = resp.text().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to read Mixpanel response: {}", e))
            })?;

            return Ok(text);
        }
    }

    // -----------------------------------------------------------------------
    // Fetch strategies
    // -----------------------------------------------------------------------

    async fn fetch_events(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let thirty_days_ago = (chrono::Utc::now() - chrono::Duration::days(30))
            .format("%Y-%m-%d")
            .to_string();

        let mut from_date = thirty_days_ago;
        let mut to_date = today;

        if let (Some(key), Some(val)) = (&options.incremental_key, &options.last_value) {
            if key == "time" {
                if let Some(d) = parse_date_param(val) {
                    from_date = d;
                }
            }
        }

        for pred in &options.predicates {
            match pred {
                Predicate::GreaterThan { column, value, .. } if column == "time" => {
                    if let Some(d) = parse_date_param(value.as_str()) {
                        from_date = d;
                    }
                }
                Predicate::LessThan { column, value, .. } if column == "time" => {
                    if let Some(d) = parse_date_param(value.as_str()) {
                        to_date = d;
                    }
                }
                _ => {}
            }
        }

        let path = format!(
            "/export?from_date={}&to_date={}&project_id={}",
            from_date, to_date, self.config.project_id
        );
        let url = self.resolve_export_url(&path);
        let text = self.api_get_text(&url).await?;

        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if total_rows >= max_rows {
                break;
            }

            let item: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse event JSONL line: {}", e))
            })?;

            append_event_row(&item, schema, &mut builders);
            total_rows += 1;

            if builders.len() >= BATCH_CAPACITY {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
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

    async fn fetch_people(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        let mut session_id: Option<String> = None;
        let mut page = 0u64;

        loop {
            if total_rows >= max_rows {
                break;
            }

            let url = self.resolve_query_url(&format!(
                "/engage?project_id={}",
                self.config.project_id
            ));

            let mut body = serde_json::json!({"page": page});
            if let Some(ref sid) = session_id {
                body["session_id"] = serde_json::Value::String(sid.clone());
            }

            let mut attempts = 0u32;
            let resp_json: serde_json::Value = loop {
                let resp = self
                    .http
                    .post(&url)
                    .basic_auth("", Some(self.config.api_secret.expose()))
                    .header("Accept", "application/json")
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| {
                        ConnectorError::Network(format!("Mixpanel request failed: {}", e))
                    })?;

                if resp.status() == 401 || resp.status() == 403 {
                    return Err(ConnectorError::Authentication(
                        "Invalid Mixpanel API secret".to_string(),
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
                    let resp_body = resp.text().await.unwrap_or_default();
                    return Err(ConnectorError::Internal(format!(
                        "Mixpanel API error ({}): {}",
                        status, resp_body
                    )));
                }

                let json: serde_json::Value = resp.json().await.map_err(|e| {
                    ConnectorError::Internal(format!("Failed to parse Mixpanel response: {}", e))
                })?;
                break json;
            };

            if session_id.is_none() {
                session_id = resp_json
                    .get("session_id")
                    .and_then(|v| v.as_str())
                    .map(String::from);
            }

            let results = resp_json
                .get("results")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            if results.is_empty() {
                break;
            }

            for person in &results {
                if total_rows >= max_rows {
                    break;
                }

                append_person_row(person, schema, &mut builders);
                total_rows += 1;

                if builders.len() >= BATCH_CAPACITY {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                }
            }

            let total = resp_json
                .get("total")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let page_size = resp_json
                .get("page_size")
                .and_then(|v| v.as_u64())
                .unwrap_or(1000);

            page += 1;
            if page * page_size >= total {
                break;
            }

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

    async fn fetch_simple_list(
        &self,
        endpoint: &str,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let url = self.resolve_query_url(&format!(
            "{}?project_id={}",
            endpoint, self.config.project_id
        ));
        let body = self.api_get(&url).await?;

        let items = body.as_array().cloned().unwrap_or_default();

        let mapping = field_mapping(table).ok_or_else(|| {
            ConnectorError::Internal(format!("No field mapping for table: {}", table))
        })?;

        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        for item in &items {
            if total_rows >= max_rows {
                break;
            }

            append_mapped_row(item, &mapping, schema, &mut builders);
            total_rows += 1;

            if builders.len() >= BATCH_CAPACITY {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
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
            "events" => self.fetch_events(&schema, arrow_schema, options).await,
            "people" => self.fetch_people(&schema, arrow_schema, options).await,
            "funnels" => {
                self.fetch_simple_list("/funnels/list", table, &schema, arrow_schema, options)
                    .await
            }
            "cohorts" => {
                self.fetch_simple_list("/cohorts/list", table, &schema, arrow_schema, options)
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
        "events" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("event", ColumnType::String, false),
                ColumnSchema::new("distinct_id", ColumnType::String, false),
                ColumnSchema::new("time", ColumnType::Timestamp, false),
                ColumnSchema::new("insert_id", ColumnType::String, true),
                ColumnSchema::new("properties", ColumnType::String, false),
            ],
        }),

        "people" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("distinct_id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("email", ColumnType::String, true),
                ColumnSchema::new("created", ColumnType::Timestamp, true),
                ColumnSchema::new("last_seen", ColumnType::Timestamp, true),
                ColumnSchema::new("properties", ColumnType::String, false),
            ],
        }),

        "funnels" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("funnel_id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
            ],
        }),

        "cohorts" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("created", ColumnType::Timestamp, false),
                ColumnSchema::new("count", ColumnType::Int64, false),
            ],
        }),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Field mappings (for funnels and cohorts)
// ---------------------------------------------------------------------------

struct FieldMapping {
    fields: &'static [(&'static str, &'static str)],
}

fn field_mapping(table: &str) -> Option<FieldMapping> {
    match table {
        "funnels" => Some(FieldMapping {
            fields: &[
                ("funnel_id", "funnel_id"),
                ("name", "name"),
                ("description", "description"),
            ],
        }),

        "cohorts" => Some(FieldMapping {
            fields: &[
                ("id", "id"),
                ("name", "name"),
                ("description", "description"),
                ("created", "created"),
                ("count", "count"),
            ],
        }),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Data parsing
// ---------------------------------------------------------------------------

fn parse_iso_timestamp(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
}

fn parse_date_param(s: &str) -> Option<String> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.format("%Y-%m-%d").to_string());
    }
    if let Ok(epoch) = s.parse::<i64>() {
        let dt = chrono::DateTime::from_timestamp(epoch, 0)?;
        return Some(dt.format("%Y-%m-%d").to_string());
    }
    if s.len() == 10 && s.chars().filter(|c| *c == '-').count() == 2 {
        return Some(s.to_string());
    }
    None
}

fn append_event_row(
    item: &serde_json::Value,
    schema: &TableSchema,
    builders: &mut ColumnBuilders,
) {
    let event_name = item
        .get("event")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let props = item.get("properties");

    for (idx, col) in schema.columns.iter().enumerate() {
        match col.name.as_str() {
            "event" => {
                builders.builder(idx).append_string(Some(event_name));
            }
            "distinct_id" => {
                let val = props
                    .and_then(|p| p.get("distinct_id"))
                    .and_then(|v| v.as_str());
                builders.builder(idx).append_string(val.or(Some("")));
            }
            "time" => {
                let ts = props
                    .and_then(|p| p.get("time"))
                    .and_then(|v| v.as_i64())
                    .map(|secs| secs * 1_000_000);
                builders.builder(idx).append_timestamp(ts);
            }
            "insert_id" => {
                let val = props
                    .and_then(|p| p.get("$insert_id"))
                    .and_then(|v| v.as_str());
                builders.builder(idx).append_string(val);
            }
            "properties" => {
                let serialized = props
                    .map(|p| serde_json::to_string(p).unwrap_or_default())
                    .unwrap_or_else(|| "{}".to_string());
                builders.builder(idx).append_string(Some(&serialized));
            }
            _ => {
                builders.builder(idx).append_string(None);
            }
        }
    }
    builders.row_complete();
}

fn append_person_row(
    item: &serde_json::Value,
    schema: &TableSchema,
    builders: &mut ColumnBuilders,
) {
    let distinct_id = item
        .get("$distinct_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let props = item.get("$properties");

    for (idx, col) in schema.columns.iter().enumerate() {
        match col.name.as_str() {
            "distinct_id" => {
                builders.builder(idx).append_string(Some(distinct_id));
            }
            "name" => {
                let val = props
                    .and_then(|p| p.get("$name"))
                    .and_then(|v| v.as_str());
                builders.builder(idx).append_string(val);
            }
            "email" => {
                let val = props
                    .and_then(|p| p.get("$email"))
                    .and_then(|v| v.as_str());
                builders.builder(idx).append_string(val);
            }
            "created" => {
                let ts = props
                    .and_then(|p| p.get("$created"))
                    .and_then(|v| v.as_str())
                    .and_then(parse_iso_timestamp);
                builders.builder(idx).append_timestamp(ts);
            }
            "last_seen" => {
                let ts = props
                    .and_then(|p| p.get("$last_seen"))
                    .and_then(|v| v.as_str())
                    .and_then(parse_iso_timestamp);
                builders.builder(idx).append_timestamp(ts);
            }
            "properties" => {
                let serialized = props
                    .map(|p| serde_json::to_string(p).unwrap_or_default())
                    .unwrap_or_else(|| "{}".to_string());
                builders.builder(idx).append_string(Some(&serialized));
            }
            _ => {
                builders.builder(idx).append_string(None);
            }
        }
    }
    builders.row_complete();
}

fn append_mapped_row(
    item: &serde_json::Value,
    mapping: &FieldMapping,
    schema: &TableSchema,
    builders: &mut ColumnBuilders,
) {
    for (idx, ((_, json_key), col_schema)) in mapping
        .fields
        .iter()
        .zip(schema.columns.iter())
        .enumerate()
    {
        let raw_val = item.get(*json_key).filter(|v| !v.is_null());

        match col_schema.data_type {
            ColumnType::Int64 => {
                let parsed = raw_val.and_then(|v| v.as_i64());
                builders.builder(idx).append_i64(parsed);
            }
            ColumnType::Timestamp => {
                let parsed = raw_val
                    .and_then(|v| v.as_str())
                    .and_then(parse_iso_timestamp);
                builders.builder(idx).append_timestamp(parsed);
            }
            _ => {
                let str_val = raw_val.and_then(|v| {
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
impl Connector for MixpanelConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Mixpanel
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            let (supports_incremental, incremental_key) = match name {
                "events" => (true, Some("time".to_string())),
                "people" => (true, Some("last_seen".to_string())),
                _ => (false, None),
            };

            let pk = match name {
                "events" => vec![],
                "people" => vec!["distinct_id".to_string()],
                "funnels" => vec!["funnel_id".to_string()],
                "cohorts" => vec!["id".to_string()],
                _ => vec![],
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
        let url = self.resolve_query_url(&format!(
            "/funnels/list?project_id={}",
            self.config.project_id
        ));
        self.api_get(&url).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MixpanelConfig {
        MixpanelConfig::new("test-mixpanel-secret", "12345")
    }

    fn test_connector_with_base(export_base: &str, query_base: &str) -> MixpanelConnector {
        let config = test_config();
        MixpanelConnector {
            config,
            http: reqwest::Client::new(),
            export_base_override: Some(export_base.to_string()),
            query_base_override: Some(query_base.to_string()),
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
    fn test_events_schema() {
        let schema = get_table_schema("events").unwrap();
        assert_eq!(schema.columns.len(), 5);
        assert_eq!(schema.columns[0].name, "event");
        assert_eq!(schema.columns[0].data_type, ColumnType::String);
        assert_eq!(schema.columns[2].name, "time");
        assert_eq!(schema.columns[2].data_type, ColumnType::Timestamp);
    }

    #[test]
    fn test_people_schema() {
        let schema = get_table_schema("people").unwrap();
        assert_eq!(schema.columns.len(), 6);
        assert_eq!(schema.columns[0].name, "distinct_id");
        assert_eq!(schema.columns[0].data_type, ColumnType::String);
        assert_eq!(schema.columns[3].name, "created");
        assert_eq!(schema.columns[3].data_type, ColumnType::Timestamp);
    }

    // -- Config tests --

    #[test]
    fn test_config_debug_redacts_secrets() {
        let config = test_config();
        let debug = format!("{:?}", config);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("test-mixpanel-secret"));
        assert!(debug.contains("12345"));
    }

    #[test]
    fn test_config_region_eu() {
        let config = MixpanelConfig::new("secret", "123").with_region("eu");
        assert!(config.export_base_url().contains("data-eu"));
        assert!(config.query_base_url().contains("eu.mixpanel"));
    }

    #[test]
    fn test_config_default_region() {
        let config = MixpanelConfig::new("secret", "123");
        assert_eq!(config.export_base_url(), EXPORT_API_BASE);
        assert_eq!(config.query_base_url(), QUERY_API_BASE);
    }

    // -- list_tables --

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = MixpanelConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), ALL_TABLES.len());

        let incremental: Vec<_> = tables.iter().filter(|t| t.supports_incremental).collect();
        assert_eq!(incremental.len(), 2);

        let events_table = tables.iter().find(|t| t.name == "events").unwrap();
        assert_eq!(events_table.incremental_key.as_deref(), Some("time"));
        assert!(events_table.primary_key_columns.is_empty());

        let people_table = tables.iter().find(|t| t.name == "people").unwrap();
        assert_eq!(people_table.incremental_key.as_deref(), Some("last_seen"));
        assert_eq!(people_table.primary_key_columns, vec!["distinct_id"]);

        let funnels_table = tables.iter().find(|t| t.name == "funnels").unwrap();
        assert_eq!(funnels_table.primary_key_columns, vec!["funnel_id"]);

        let cohorts_table = tables.iter().find(|t| t.name == "cohorts").unwrap();
        assert_eq!(cohorts_table.primary_key_columns, vec!["id"]);
    }

    // -- Mock tests --

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"/funnels/list\?project_id=12345".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("[]")
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url(), &server.url());
        connector.validate_credentials().await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_validate_credentials_unauthorized() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("GET", mockito::Matcher::Regex(r"/funnels/list\?.*".to_string()))
            .with_status(401)
            .with_body(r#"{"error":"Invalid credentials"}"#)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url(), &server.url());
        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_rate_limit_retry() {
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"/funnels/list\?.*".to_string()))
            .with_status(429)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"rate limit exceeded"}"#)
            .expect(4)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url(), &server.url());
        let url = connector.resolve_query_url(&format!(
            "/funnels/list?project_id={}",
            connector.config.project_id
        ));
        let result = connector.api_get(&url).await;

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
    async fn test_fetch_events_jsonl() {
        let mut server = mockito::Server::new_async().await;

        let jsonl = [
            r#"{"event":"page_view","properties":{"distinct_id":"user1","time":1700000000,"$insert_id":"abc123"}}"#,
            r#"{"event":"signup","properties":{"distinct_id":"user2","time":1700000060,"$insert_id":"def456"}}"#,
        ]
        .join("\n");

        let _mock = server
            .mock("GET", mockito::Matcher::Regex(r"/export\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body(&jsonl)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url(), &server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("events", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_fetch_funnels() {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock("GET", mockito::Matcher::Regex(r"/funnels/list\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!([
                    {"funnel_id": 1, "name": "Signup Funnel", "description": "Tracks signups"},
                    {"funnel_id": 2, "name": "Purchase Funnel", "description": null},
                ])
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url(), &server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("funnels", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_fetch_cohorts() {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock("GET", mockito::Matcher::Regex(r"/cohorts/list\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!([
                    {
                        "id": 10,
                        "name": "Power Users",
                        "description": "Users with 10+ sessions",
                        "created": "2024-01-15T10:30:00+00:00",
                        "count": 456
                    },
                    {
                        "id": 20,
                        "name": "New Users",
                        "description": "Signed up last 30 days",
                        "created": "2024-02-01T08:00:00+00:00",
                        "count": 1200
                    },
                ])
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url(), &server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("cohorts", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_fetch_people() {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock("POST", mockito::Matcher::Regex(r"/engage\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "results": [
                        {
                            "$distinct_id": "user1",
                            "$properties": {
                                "$name": "Alice",
                                "$email": "alice@example.com",
                                "$created": "2024-01-01T00:00:00+00:00",
                                "$last_seen": "2024-06-15T10:00:00+00:00"
                            }
                        }
                    ],
                    "session_id": "sess-abc",
                    "total": 1,
                    "page": 0,
                    "page_size": 1000
                })
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url(), &server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("people", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }
}
