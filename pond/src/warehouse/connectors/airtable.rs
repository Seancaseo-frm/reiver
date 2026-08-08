//! Airtable connector for the data warehouse.
//!
//! Syncs Airtable base data via the REST API. Uses the `airtable-api` crate for
//! record listing and direct `reqwest` calls for the Metadata API (schema
//! discovery) and filtered queries (incremental sync, predicate pushdown).
//! Schema is dynamic -- tables and fields are user-defined per base.

use super::builders::ColumnBuilders;
use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::crypto::SecretString;
use crate::warehouse::query::predicate_pushdown::Predicate;
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use airtable_api::Airtable;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

const AIRTABLE_META_URL: &str = "https://api.airtable.com/v0/meta/bases";
const AIRTABLE_API_URL: &str = "https://api.airtable.com/v0";
const BATCH_THRESHOLD: usize = 500;
const MAX_TOTAL_ROWS: usize = 100_000;
const PAGE_SIZE: u32 = 100;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 220;

#[derive(Clone)]
pub struct AirtableConfig {
    pub api_key: SecretString,
    pub base_id: String,
}

impl std::fmt::Debug for AirtableConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AirtableConfig")
            .field("api_key", &"***REDACTED***")
            .field("base_id", &self.base_id)
            .finish()
    }
}

impl AirtableConfig {
    pub fn new(api_key: impl Into<String>, base_id: impl Into<String>) -> Self {
        Self {
            api_key: SecretString::new(api_key),
            base_id: base_id.into(),
        }
    }
}

/// Metadata from the Airtable Metadata API for a single table.
#[derive(Debug, Clone)]
struct AirtableTableMeta {
    id: String,
    name: String,
    fields: Vec<AirtableFieldMeta>,
}

/// Metadata for a single field within an Airtable table.
#[derive(Debug, Clone)]
struct AirtableFieldMeta {
    name: String,
    field_type: String,
}

pub struct AirtableConnector {
    config: AirtableConfig,
    client: Airtable,
    http: reqwest::Client,
    #[cfg(test)]
    base_url: Option<String>,
}

impl AirtableConnector {
    pub fn new(config: AirtableConfig) -> Self {
        let client = Airtable::new(
            config.api_key.expose(),
            &config.base_id,
            "",
        );
        Self {
            config,
            client,
            http: reqwest::Client::new(),
            #[cfg(test)]
            base_url: None,
        }
    }

    // ========================================================================
    // Arrow schema conversion
    // ========================================================================

    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), col.nullable))
            .collect();
        Schema::new(fields)
    }

    // ========================================================================
    // Field type mapping
    // ========================================================================

    fn map_field_type(airtable_type: &str) -> Option<ColumnType> {
        match airtable_type {
            "singleLineText" | "multilineText" | "richText" | "email" | "url"
            | "phoneNumber" | "singleSelect" | "barcode" | "externalSyncSource"
            | "aiText" => Some(ColumnType::String),

            "multipleSelects" | "multipleRecordLinks" | "multipleAttachments"
            | "multipleLookupValues" | "multipleCollaborators" => Some(ColumnType::String),

            "number" | "currency" | "percent" | "count" | "autoNumber" | "rating"
            | "duration" => Some(ColumnType::Float64),

            "checkbox" => Some(ColumnType::Boolean),

            "dateTime" | "createdTime" | "lastModifiedTime" => Some(ColumnType::Timestamp),

            "date" => Some(ColumnType::String),

            "formula" | "rollup" => Some(ColumnType::String),

            "button" => None,

            _ => Some(ColumnType::String),
        }
    }

    // ========================================================================
    // Field name sanitization
    // ========================================================================

    fn sanitize_field_name(name: &str) -> String {
        let s: String = name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
            .collect();
        let s = s.trim_matches('_').to_lowercase();
        if s.is_empty() {
            "unnamed_field".to_string()
        } else if s.chars().next().map_or(false, |c| c.is_ascii_digit()) {
            format!("col_{}", s)
        } else {
            s
        }
    }

    // ========================================================================
    // Schema construction from metadata
    // ========================================================================

    fn build_schema(
        table_meta: &AirtableTableMeta,
    ) -> (TableSchema, HashMap<String, String>) {
        let mut columns = vec![
            ColumnSchema::new("record_id", ColumnType::String, false)
                .with_description("Airtable record ID"),
            ColumnSchema::new("created_time", ColumnType::Timestamp, true)
                .with_description("Record creation timestamp")
                .with_timezone("UTC"),
        ];

        let mut field_map: HashMap<String, String> = HashMap::new();
        let base_columns = ["record_id", "created_time"];
        let mut seen_names: HashMap<String, usize> = HashMap::new();

        for field in &table_meta.fields {
            let col_type = match Self::map_field_type(&field.field_type) {
                Some(ct) => ct,
                None => continue,
            };

            let mut sanitized = Self::sanitize_field_name(&field.name);
            if base_columns.contains(&sanitized.as_str()) {
                sanitized = format!("field_{}", sanitized);
            }

            let count = seen_names.entry(sanitized.clone()).or_insert(0);
            let final_name = if *count > 0 {
                format!("{}_{}", sanitized, count)
            } else {
                sanitized.clone()
            };
            *count += 1;

            let mut col = ColumnSchema::new(&final_name, col_type, true)
                .with_description(&field.name);
            if col_type == ColumnType::Timestamp {
                col = col.with_timezone("UTC");
            }
            columns.push(col);
            field_map.insert(final_name, field.name.clone());
        }

        (TableSchema { columns }, field_map)
    }

    // ========================================================================
    // Metadata API
    // ========================================================================

    fn metadata_url(&self) -> String {
        #[cfg(test)]
        if let Some(ref base) = self.base_url {
            return format!("{}/meta/bases/{}/tables", base, self.config.base_id);
        }
        format!("{}/{}/tables", AIRTABLE_META_URL, self.config.base_id)
    }

    fn records_url(&self, table_id_or_name: &str) -> String {
        #[cfg(test)]
        if let Some(ref base) = self.base_url {
            return format!(
                "{}/{}/{}",
                base,
                self.config.base_id,
                urlencoding::encode(table_id_or_name)
            );
        }
        format!(
            "{}/{}/{}",
            AIRTABLE_API_URL,
            self.config.base_id,
            urlencoding::encode(table_id_or_name)
        )
    }

    async fn fetch_metadata(&self) -> ConnectorResult<Vec<AirtableTableMeta>> {
        let url = self.metadata_url();
        let response = self.http_get_with_retry(&url).await?;

        let json: serde_json::Value = response.json().await.map_err(|e| {
            ConnectorError::Internal(format!("Failed to parse metadata response: {}", e))
        })?;

        let tables = json
            .get("tables")
            .and_then(|t| t.as_array())
            .ok_or_else(|| {
                ConnectorError::Internal("Metadata response missing 'tables' array".to_string())
            })?;

        let mut result = Vec::with_capacity(tables.len());
        for table in tables {
            let id = table
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = table
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let fields = table
                .get("fields")
                .and_then(|f| f.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|f| {
                            let fname = f.get("name")?.as_str()?.to_string();
                            let ftype = f.get("type")?.as_str()?.to_string();
                            Some(AirtableFieldMeta {
                                name: fname,
                                field_type: ftype,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            if !id.is_empty() && !name.is_empty() {
                result.push(AirtableTableMeta { id, name, fields });
            }
        }

        Ok(result)
    }

    // ========================================================================
    // HTTP helpers with retry
    // ========================================================================

    async fn http_get_with_retry(
        &self,
        url: &str,
    ) -> ConnectorResult<reqwest::Response> {
        let mut attempts = 0u32;
        loop {
            let resp = self
                .http
                .get(url)
                .header("Authorization", format!("Bearer {}", self.config.api_key.expose()))
                .send()
                .await
                .map_err(|e| {
                    ConnectorError::Network(format!("Airtable request failed: {}", e))
                })?;

            if resp.status() == 401 {
                return Err(ConnectorError::Authentication(
                    "Invalid Airtable API key".to_string(),
                ));
            }

            if resp.status() == 429 {
                if attempts < MAX_RETRIES {
                    attempts += 1;
                    let delay = INITIAL_RETRY_DELAY_MS * 2u64.pow(attempts - 1);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    continue;
                }
                return Err(ConnectorError::RateLimited {
                    retry_after_secs: 30,
                });
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "Airtable API error ({}): {}",
                    status, body
                )));
            }

            return Ok(resp);
        }
    }

    // ========================================================================
    // Timestamp parsing
    // ========================================================================

    fn parse_timestamp(val: &serde_json::Value) -> Option<i64> {
        val.as_str().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.timestamp_micros())
        })
    }

    fn parse_timestamp_str(s: &str) -> Option<i64> {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.timestamp_micros())
    }

    // ========================================================================
    // Record to Arrow builders
    // ========================================================================

    fn append_record(
        fields_obj: &serde_json::Value,
        record_id: &str,
        created_time: Option<&str>,
        schema: &TableSchema,
        field_map: &HashMap<String, String>,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            if col.name == "record_id" {
                builders.builder(i).append_string(Some(record_id));
                continue;
            }
            if col.name == "created_time" {
                let ts = created_time.and_then(Self::parse_timestamp_str);
                builders.builder(i).append_timestamp(ts);
                continue;
            }

            let original_name = match field_map.get(&col.name) {
                Some(n) => n,
                None => {
                    builders.builder(i).append_null();
                    continue;
                }
            };

            let val = fields_obj.get(original_name.as_str());
            match col.data_type {
                ColumnType::Timestamp => {
                    let ts = val.and_then(Self::parse_timestamp);
                    builders.builder(i).append_timestamp(ts);
                }
                ColumnType::Float64 => {
                    let f = val.and_then(|v| v.as_f64());
                    builders.builder(i).append_f64(f);
                }
                ColumnType::Boolean => {
                    let b = val.and_then(|v| v.as_bool());
                    builders.builder(i).append_bool(b);
                }
                _ => {
                    let s = val.and_then(|v| Self::extract_string_value(v));
                    builders.builder(i).append_string(s.as_deref());
                }
            }
        }
        builders.row_complete();
    }

    fn extract_string_value(val: &serde_json::Value) -> Option<String> {
        if val.is_null() {
            return None;
        }
        if let Some(s) = val.as_str() {
            return Some(s.to_string());
        }
        if val.is_array() || val.is_object() {
            return Some(val.to_string());
        }
        if let Some(n) = val.as_f64() {
            return Some(n.to_string());
        }
        if let Some(b) = val.as_bool() {
            return Some(b.to_string());
        }
        None
    }

    // ========================================================================
    // Predicate pushdown -> filterByFormula
    // ========================================================================

    fn predicates_to_formula(
        predicates: &[Predicate],
        field_map: &HashMap<String, String>,
    ) -> Option<String> {
        if predicates.is_empty() {
            return None;
        }

        let parts: Vec<String> = predicates
            .iter()
            .filter_map(|p| Self::predicate_to_formula(p, field_map))
            .collect();

        if parts.is_empty() {
            return None;
        }
        if parts.len() == 1 {
            return Some(parts.into_iter().next().unwrap());
        }
        Some(format!("AND({})", parts.join(",")))
    }

    fn predicate_to_formula(
        pred: &Predicate,
        field_map: &HashMap<String, String>,
    ) -> Option<String> {
        match pred {
            Predicate::Equals { column, value } => {
                let field = Self::resolve_field_name(column, field_map)?;
                Some(format!("{{{}}}='{}'", field, Self::escape_formula_value(value)))
            }
            Predicate::Contains { column, substring } => {
                let field = Self::resolve_field_name(column, field_map)?;
                Some(format!(
                    "FIND(\"{}\",{{{}}})>0",
                    Self::escape_formula_value(substring),
                    field
                ))
            }
            Predicate::Not(inner) => {
                let inner_formula = Self::predicate_to_formula(inner, field_map)?;
                Some(format!("NOT({})", inner_formula))
            }
            Predicate::And(preds) => {
                let parts: Vec<String> = preds
                    .iter()
                    .filter_map(|p| Self::predicate_to_formula(p, field_map))
                    .collect();
                if parts.is_empty() {
                    None
                } else if parts.len() == 1 {
                    Some(parts.into_iter().next().unwrap())
                } else {
                    Some(format!("AND({})", parts.join(",")))
                }
            }
            Predicate::Or(preds) => {
                let parts: Vec<String> = preds
                    .iter()
                    .filter_map(|p| Self::predicate_to_formula(p, field_map))
                    .collect();
                if parts.is_empty() {
                    None
                } else if parts.len() == 1 {
                    Some(parts.into_iter().next().unwrap())
                } else {
                    Some(format!("OR({})", parts.join(",")))
                }
            }
            Predicate::GreaterThan { column, value, inclusive } => {
                let field = Self::resolve_field_name(column, field_map)?;
                let op = if *inclusive { ">=" } else { ">" };
                Some(format!("{{{}}}{}'{}'", field, op, Self::escape_formula_value(value)))
            }
            Predicate::LessThan { column, value, inclusive } => {
                let field = Self::resolve_field_name(column, field_map)?;
                let op = if *inclusive { "<=" } else { "<" };
                Some(format!("{{{}}}{}'{}'", field, op, Self::escape_formula_value(value)))
            }
            _ => None,
        }
    }

    fn resolve_field_name(
        column: &str,
        field_map: &HashMap<String, String>,
    ) -> Option<String> {
        field_map.get(column).cloned()
    }

    fn escape_formula_value(val: &str) -> String {
        val.replace('\\', "\\\\").replace('\'', "\\'")
    }

    // ========================================================================
    // Record fetching (raw HTTP with offset pagination)
    // ========================================================================

    async fn fetch_records_raw(
        &self,
        table_id: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        field_map: &HashMap<String, String>,
        filter_formula: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut offset: Option<String> = None;
        let mut total_rows: usize = 0;

        loop {
            let url = self.records_url(table_id);
            let mut request = self
                .http
                .get(&url)
                .header("Authorization", format!("Bearer {}", self.config.api_key.expose()))
                .query(&[("pageSize", PAGE_SIZE.to_string())]);

            if let Some(ref off) = offset {
                request = request.query(&[("offset", off.as_str())]);
            }
            if let Some(formula) = filter_formula {
                request = request.query(&[("filterByFormula", formula)]);
            }

            let response = self.send_with_retry(request).await?;
            let json: serde_json::Value = response.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse records response: {}", e))
            })?;

            let records = json
                .get("records")
                .and_then(|r| r.as_array())
                .ok_or_else(|| {
                    ConnectorError::Internal(
                        "Airtable response missing 'records' array".to_string(),
                    )
                })?;

            if records.is_empty() {
                break;
            }

            for record in records {
                let record_id = record
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let created_time = record
                    .get("createdTime")
                    .and_then(|v| v.as_str());
                let empty_obj = serde_json::Value::Object(serde_json::Map::new());
                let fields_obj = record.get("fields").unwrap_or(&empty_obj);

                Self::append_record(
                    fields_obj,
                    record_id,
                    created_time,
                    schema,
                    field_map,
                    &mut builders,
                );
            }

            total_rows += records.len();

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            offset = json
                .get("offset")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if offset.is_none() {
                break;
            }

            if total_rows >= MAX_TOTAL_ROWS {
                tracing::warn!(
                    table = table_id,
                    count = total_rows,
                    "Airtable sync reached safety limit"
                );
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

    async fn send_with_retry(
        &self,
        request: reqwest::RequestBuilder,
    ) -> ConnectorResult<reqwest::Response> {
        let built = request.try_clone().ok_or_else(|| {
            ConnectorError::Internal("Failed to clone request for retry".to_string())
        })?;

        let mut attempts = 0u32;
        let mut current = built;

        loop {
            let resp = current.send().await.map_err(|e| {
                ConnectorError::Network(format!("Airtable request failed: {}", e))
            })?;

            if resp.status() == 401 {
                return Err(ConnectorError::Authentication(
                    "Invalid Airtable API key".to_string(),
                ));
            }

            if resp.status() == 429 {
                if attempts < MAX_RETRIES {
                    attempts += 1;
                    let delay = INITIAL_RETRY_DELAY_MS * 2u64.pow(attempts - 1);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    current = request.try_clone().ok_or_else(|| {
                        ConnectorError::Internal("Failed to clone request for retry".to_string())
                    })?;
                    continue;
                }
                return Err(ConnectorError::RateLimited {
                    retry_after_secs: 30,
                });
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "Airtable API error ({}): {}",
                    status, body
                )));
            }

            return Ok(resp);
        }
    }

    // ========================================================================
    // Full sync using the airtable-api crate
    // ========================================================================

    async fn fetch_with_crate(
        &self,
        table_name: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        field_map: &HashMap<String, String>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let records: Vec<airtable_api::Record<serde_json::Value>> = self
            .client
            .list_records(table_name, "", vec![])
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("401") || msg.contains("AUTHENTICATION_REQUIRED") {
                    ConnectorError::Authentication("Invalid Airtable API key".to_string())
                } else if msg.contains("429") {
                    ConnectorError::RateLimited { retry_after_secs: 30 }
                } else {
                    ConnectorError::Internal(format!("Airtable list_records failed: {}", msg))
                }
            })?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);

        for record in &records {
            let fields_val = serde_json::to_value(&record.fields).unwrap_or_default();
            let created_str = record
                .created_time
                .as_ref()
                .map(|dt| dt.to_rfc3339());

            Self::append_record(
                &fields_val,
                &record.id,
                created_str.as_deref(),
                schema,
                field_map,
                &mut builders,
            );

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
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
}

#[async_trait]
impl Connector for AirtableConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Airtable
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let tables_meta = self.fetch_metadata().await?;
        let mut tables = Vec::with_capacity(tables_meta.len());

        for meta in &tables_meta {
            let (schema, _) = Self::build_schema(meta);
            let has_last_modified = meta.fields.iter().any(|f| f.field_type == "lastModifiedTime");
            tables.push(TableInfo {
                name: meta.name.clone(),
                schema,
                supports_incremental: has_last_modified,
                incremental_key: if has_last_modified {
                    Some("last_modified_time".to_string())
                } else {
                    None
                },
                estimated_rows: None,
                primary_key_columns: vec!["record_id".to_string()],
            });
        }

        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        let tables_meta = self.fetch_metadata().await?;
        let meta = tables_meta
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;
        let (schema, _) = Self::build_schema(meta);
        Ok(schema)
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            let tables_meta = self.fetch_metadata().await?;
            let meta = tables_meta
                .iter()
                .find(|t| t.name == table)
                .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;

            let (schema, field_map) = Self::build_schema(meta);
            let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

            let mut formula_parts: Vec<String> = Vec::new();

            if let Some(pred_formula) =
                Self::predicates_to_formula(&options.predicates, &field_map)
            {
                formula_parts.push(pred_formula);
            }

            if let Some(ref last_value) = options.last_value {
                formula_parts.push(format!(
                    "LAST_MODIFIED_TIME()>'{}'",
                    Self::escape_formula_value(last_value)
                ));
            }

            let use_raw = !formula_parts.is_empty();
            let batches = if use_raw {
                let combined = if formula_parts.len() == 1 {
                    formula_parts.into_iter().next().unwrap()
                } else {
                    format!("AND({})", formula_parts.join(","))
                };
                self.fetch_records_raw(
                    &meta.id,
                    &schema,
                    arrow_schema,
                    &field_map,
                    Some(&combined),
                )
                .await?
            } else {
                self.fetch_with_crate(&meta.name, &schema, arrow_schema, &field_map)
                    .await?
            };

            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        let url = self.metadata_url();
        self.http_get_with_retry(&url).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config() -> AirtableConfig {
        AirtableConfig::new("test_api_key", "appTEST123")
    }

    fn test_connector(base_url: String) -> AirtableConnector {
        let config = test_config();
        let client = Airtable::new(config.api_key.expose(), &config.base_id, "");
        AirtableConnector {
            config,
            client,
            http: reqwest::Client::new(),
            base_url: Some(base_url),
        }
    }

    fn sample_metadata_response() -> serde_json::Value {
        serde_json::json!({
            "tables": [
                {
                    "id": "tblABC123",
                    "name": "Tasks",
                    "fields": [
                        {"id": "fld1", "name": "Name", "type": "singleLineText"},
                        {"id": "fld2", "name": "Status", "type": "singleSelect"},
                        {"id": "fld3", "name": "Due Date", "type": "dateTime"},
                        {"id": "fld4", "name": "Priority", "type": "number"},
                        {"id": "fld5", "name": "Done", "type": "checkbox"},
                        {"id": "fld6", "name": "Last Modified", "type": "lastModifiedTime"},
                        {"id": "fld7", "name": "Tags", "type": "multipleSelects"},
                        {"id": "fld8", "name": "Action", "type": "button"}
                    ]
                },
                {
                    "id": "tblDEF456",
                    "name": "Contacts",
                    "fields": [
                        {"id": "fld10", "name": "Full Name", "type": "singleLineText"},
                        {"id": "fld11", "name": "Email", "type": "email"},
                        {"id": "fld12", "name": "Phone", "type": "phoneNumber"}
                    ]
                }
            ]
        })
    }

    fn sample_records_response(offset: Option<&str>) -> serde_json::Value {
        let mut resp = serde_json::json!({
            "records": [
                {
                    "id": "rec001",
                    "createdTime": "2024-06-15T10:30:00.000Z",
                    "fields": {
                        "Name": "Buy groceries",
                        "Status": "Todo",
                        "Due Date": "2024-07-01T09:00:00.000Z",
                        "Priority": 2.0,
                        "Done": false,
                        "Tags": ["urgent", "personal"]
                    }
                },
                {
                    "id": "rec002",
                    "createdTime": "2024-06-16T14:00:00.000Z",
                    "fields": {
                        "Name": "Write report",
                        "Status": "In Progress",
                        "Due Date": "2024-07-05T17:00:00.000Z",
                        "Priority": 1.0,
                        "Done": true,
                        "Tags": ["work"]
                    }
                }
            ]
        });
        if let Some(off) = offset {
            resp["offset"] = serde_json::Value::String(off.to_string());
        }
        resp
    }

    // ── Config / Schema tests ────────────────────────────────────────

    #[test]
    fn test_config_debug_redacts_key() {
        let config = test_config();
        let debug = format!("{:?}", config);
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("test_api_key"));
    }

    #[test]
    fn test_field_type_mapping() {
        assert_eq!(AirtableConnector::map_field_type("singleLineText"), Some(ColumnType::String));
        assert_eq!(AirtableConnector::map_field_type("number"), Some(ColumnType::Float64));
        assert_eq!(AirtableConnector::map_field_type("checkbox"), Some(ColumnType::Boolean));
        assert_eq!(AirtableConnector::map_field_type("dateTime"), Some(ColumnType::Timestamp));
        assert_eq!(AirtableConnector::map_field_type("lastModifiedTime"), Some(ColumnType::Timestamp));
        assert_eq!(AirtableConnector::map_field_type("button"), None);
        assert_eq!(AirtableConnector::map_field_type("multipleSelects"), Some(ColumnType::String));
        assert_eq!(AirtableConnector::map_field_type("formula"), Some(ColumnType::String));
        assert_eq!(AirtableConnector::map_field_type("unknownNewType"), Some(ColumnType::String));
    }

    #[test]
    fn test_sanitize_field_name() {
        assert_eq!(AirtableConnector::sanitize_field_name("My Field"), "my_field");
        assert_eq!(AirtableConnector::sanitize_field_name("Due Date"), "due_date");
        assert_eq!(AirtableConnector::sanitize_field_name("123start"), "col_123start");
        assert_eq!(AirtableConnector::sanitize_field_name("___"), "unnamed_field");
        assert_eq!(AirtableConnector::sanitize_field_name("hello-world!"), "hello_world");
    }

    #[test]
    fn test_build_schema_from_metadata() {
        let meta = AirtableTableMeta {
            id: "tblABC".to_string(),
            name: "Tasks".to_string(),
            fields: vec![
                AirtableFieldMeta { name: "Name".to_string(), field_type: "singleLineText".to_string() },
                AirtableFieldMeta { name: "Count".to_string(), field_type: "number".to_string() },
                AirtableFieldMeta { name: "Done".to_string(), field_type: "checkbox".to_string() },
                AirtableFieldMeta { name: "Created".to_string(), field_type: "createdTime".to_string() },
                AirtableFieldMeta { name: "Button".to_string(), field_type: "button".to_string() },
            ],
        };

        let (schema, field_map) = AirtableConnector::build_schema(&meta);

        assert_eq!(schema.columns.len(), 6);
        assert_eq!(schema.columns[0].name, "record_id");
        assert_eq!(schema.columns[0].data_type, ColumnType::String);
        assert_eq!(schema.columns[1].name, "created_time");
        assert_eq!(schema.columns[1].data_type, ColumnType::Timestamp);
        assert_eq!(schema.columns[2].name, "name");
        assert_eq!(schema.columns[2].data_type, ColumnType::String);
        assert_eq!(schema.columns[3].name, "count");
        assert_eq!(schema.columns[3].data_type, ColumnType::Float64);
        assert_eq!(schema.columns[4].name, "done");
        assert_eq!(schema.columns[4].data_type, ColumnType::Boolean);
        assert_eq!(schema.columns[5].name, "created");
        assert_eq!(schema.columns[5].data_type, ColumnType::Timestamp);

        assert_eq!(field_map.get("name"), Some(&"Name".to_string()));
        assert_eq!(field_map.get("count"), Some(&"Count".to_string()));
        assert!(!field_map.contains_key("button"));
    }

    #[test]
    fn test_build_schema_deduplicates_names() {
        let meta = AirtableTableMeta {
            id: "tbl1".to_string(),
            name: "T".to_string(),
            fields: vec![
                AirtableFieldMeta { name: "status".to_string(), field_type: "singleLineText".to_string() },
                AirtableFieldMeta { name: "Status".to_string(), field_type: "singleSelect".to_string() },
            ],
        };

        let (schema, _) = AirtableConnector::build_schema(&meta);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"status"));
        assert!(names.contains(&"status_1"));
    }

    #[test]
    fn test_build_schema_reserved_name_collision() {
        let meta = AirtableTableMeta {
            id: "tbl1".to_string(),
            name: "T".to_string(),
            fields: vec![
                AirtableFieldMeta { name: "record_id".to_string(), field_type: "singleLineText".to_string() },
                AirtableFieldMeta { name: "created_time".to_string(), field_type: "singleLineText".to_string() },
            ],
        };

        let (schema, _) = AirtableConnector::build_schema(&meta);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"field_record_id"));
        assert!(names.contains(&"field_created_time"));
    }

    // ── Record conversion tests ──────────────────────────────────────

    #[test]
    fn test_append_record_to_builders() {
        let meta = AirtableTableMeta {
            id: "tbl1".to_string(),
            name: "Tasks".to_string(),
            fields: vec![
                AirtableFieldMeta { name: "Name".to_string(), field_type: "singleLineText".to_string() },
                AirtableFieldMeta { name: "Priority".to_string(), field_type: "number".to_string() },
                AirtableFieldMeta { name: "Done".to_string(), field_type: "checkbox".to_string() },
            ],
        };

        let (schema, field_map) = AirtableConnector::build_schema(&meta);
        let arrow_schema = Arc::new(AirtableConnector::to_arrow_schema(&schema));
        let mut builders = ColumnBuilders::new(&schema, 100);

        let fields = serde_json::json!({
            "Name": "Test task",
            "Priority": 3.0,
            "Done": true
        });

        AirtableConnector::append_record(
            &fields,
            "recXYZ",
            Some("2024-06-15T10:30:00.000Z"),
            &schema,
            &field_map,
            &mut builders,
        );

        assert_eq!(builders.len(), 1);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_append_record_with_null_fields() {
        let meta = AirtableTableMeta {
            id: "tbl1".to_string(),
            name: "T".to_string(),
            fields: vec![
                AirtableFieldMeta { name: "Name".to_string(), field_type: "singleLineText".to_string() },
                AirtableFieldMeta { name: "Score".to_string(), field_type: "number".to_string() },
            ],
        };

        let (schema, field_map) = AirtableConnector::build_schema(&meta);
        let arrow_schema = Arc::new(AirtableConnector::to_arrow_schema(&schema));
        let mut builders = ColumnBuilders::new(&schema, 100);

        let fields = serde_json::json!({"Name": "Partial"});

        AirtableConnector::append_record(
            &fields,
            "rec1",
            None,
            &schema,
            &field_map,
            &mut builders,
        );

        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_timestamp_parsing() {
        let val = serde_json::json!("2024-06-15T10:30:00.000Z");
        let ts = AirtableConnector::parse_timestamp(&val);
        assert!(ts.is_some());

        let val = serde_json::json!("not-a-date");
        assert!(AirtableConnector::parse_timestamp(&val).is_none());

        let val = serde_json::json!(null);
        assert!(AirtableConnector::parse_timestamp(&val).is_none());
    }

    #[test]
    fn test_extract_string_value() {
        assert_eq!(
            AirtableConnector::extract_string_value(&serde_json::json!("hello")),
            Some("hello".to_string())
        );
        assert_eq!(
            AirtableConnector::extract_string_value(&serde_json::json!(42.5)),
            Some("42.5".to_string())
        );
        assert_eq!(
            AirtableConnector::extract_string_value(&serde_json::json!(true)),
            Some("true".to_string())
        );
        assert_eq!(
            AirtableConnector::extract_string_value(&serde_json::json!(null)),
            None
        );
        let arr = serde_json::json!(["a", "b"]);
        let result = AirtableConnector::extract_string_value(&arr);
        assert!(result.is_some());
        assert!(result.unwrap().contains("\"a\""));
    }

    // ── Predicate pushdown tests ─────────────────────────────────────

    #[test]
    fn test_predicate_equals() {
        let mut field_map = HashMap::new();
        field_map.insert("status".to_string(), "Status".to_string());

        let preds = vec![Predicate::Equals {
            column: "status".into(),
            value: "Done".into(),
        }];

        let formula = AirtableConnector::predicates_to_formula(&preds, &field_map);
        assert_eq!(formula, Some("{Status}='Done'".to_string()));
    }

    #[test]
    fn test_predicate_contains() {
        let mut field_map = HashMap::new();
        field_map.insert("name".to_string(), "Name".to_string());

        let preds = vec![Predicate::Contains {
            column: "name".into(),
            substring: "report".into(),
        }];

        let formula = AirtableConnector::predicates_to_formula(&preds, &field_map);
        assert_eq!(formula, Some("FIND(\"report\",{Name})>0".to_string()));
    }

    #[test]
    fn test_predicate_not_equals() {
        let mut field_map = HashMap::new();
        field_map.insert("status".to_string(), "Status".to_string());

        let preds = vec![Predicate::Not(Box::new(Predicate::Equals {
            column: "status".into(),
            value: "Cancelled".into(),
        }))];

        let formula = AirtableConnector::predicates_to_formula(&preds, &field_map);
        assert_eq!(formula, Some("NOT({Status}='Cancelled')".to_string()));
    }

    #[test]
    fn test_predicate_compound_and() {
        let mut field_map = HashMap::new();
        field_map.insert("status".to_string(), "Status".to_string());
        field_map.insert("name".to_string(), "Name".to_string());

        let preds = vec![
            Predicate::Equals { column: "status".into(), value: "Done".into() },
            Predicate::Contains { column: "name".into(), substring: "test".into() },
        ];

        let formula = AirtableConnector::predicates_to_formula(&preds, &field_map);
        assert_eq!(
            formula,
            Some("AND({Status}='Done',FIND(\"test\",{Name})>0)".to_string())
        );
    }

    #[test]
    fn test_predicate_unknown_column_skipped() {
        let field_map = HashMap::new();

        let preds = vec![Predicate::Equals {
            column: "nonexistent".into(),
            value: "val".into(),
        }];

        let formula = AirtableConnector::predicates_to_formula(&preds, &field_map);
        assert_eq!(formula, None);
    }

    #[test]
    fn test_predicate_greater_less_than() {
        let mut field_map = HashMap::new();
        field_map.insert("priority".to_string(), "Priority".to_string());

        let preds = vec![
            Predicate::GreaterThan { column: "priority".into(), value: "2".into(), inclusive: true },
            Predicate::LessThan { column: "priority".into(), value: "5".into(), inclusive: false },
        ];

        let formula = AirtableConnector::predicates_to_formula(&preds, &field_map);
        assert_eq!(
            formula,
            Some("AND({Priority}>='2',{Priority}<'5')".to_string())
        );
    }

    // ── Metadata API tests ───────────────────────────────────────────

    #[tokio::test]
    async fn test_fetch_metadata_success() {
        let server = MockServer::start().await;
        let connector = test_connector(server.uri());

        Mock::given(method("GET"))
            .and(path(format!("/meta/bases/{}/tables", connector.config.base_id)))
            .and(header("Authorization", "Bearer test_api_key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_metadata_response()))
            .mount(&server)
            .await;

        let tables = connector.fetch_metadata().await.unwrap();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "Tasks");
        assert_eq!(tables[0].fields.len(), 8);
        assert_eq!(tables[1].name, "Contacts");
        assert_eq!(tables[1].fields.len(), 3);
    }

    #[tokio::test]
    async fn test_fetch_metadata_auth_failure() {
        let server = MockServer::start().await;
        let connector = test_connector(server.uri());

        Mock::given(method("GET"))
            .and(path(format!("/meta/bases/{}/tables", connector.config.base_id)))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": {"type": "AUTHENTICATION_REQUIRED", "message": "Invalid API key"}
            })))
            .mount(&server)
            .await;

        let result = connector.fetch_metadata().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_fetch_metadata_rate_limit() {
        let server = MockServer::start().await;
        let connector = test_connector(server.uri());

        Mock::given(method("GET"))
            .and(path(format!("/meta/bases/{}/tables", connector.config.base_id)))
            .respond_with(ResponseTemplate::new(429))
            .expect(4)
            .mount(&server)
            .await;

        let result = connector.fetch_metadata().await;
        assert!(matches!(result, Err(ConnectorError::RateLimited { .. })));
    }

    // ── Record fetching tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_fetch_records_raw_single_page() {
        let server = MockServer::start().await;
        let connector = test_connector(server.uri());

        let meta = AirtableTableMeta {
            id: "tblABC123".to_string(),
            name: "Tasks".to_string(),
            fields: vec![
                AirtableFieldMeta { name: "Name".to_string(), field_type: "singleLineText".to_string() },
                AirtableFieldMeta { name: "Priority".to_string(), field_type: "number".to_string() },
                AirtableFieldMeta { name: "Done".to_string(), field_type: "checkbox".to_string() },
            ],
        };

        let (schema, field_map) = AirtableConnector::build_schema(&meta);
        let arrow_schema = Arc::new(AirtableConnector::to_arrow_schema(&schema));

        Mock::given(method("GET"))
            .and(path(format!("/{}/tblABC123", connector.config.base_id)))
            .and(query_param("pageSize", "100"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(sample_records_response(None)),
            )
            .mount(&server)
            .await;

        let batches = connector
            .fetch_records_raw("tblABC123", &schema, arrow_schema, &field_map, None)
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_fetch_records_raw_with_pagination() {
        let server = MockServer::start().await;
        let connector = test_connector(server.uri());

        let meta = AirtableTableMeta {
            id: "tblP".to_string(),
            name: "P".to_string(),
            fields: vec![
                AirtableFieldMeta { name: "Name".to_string(), field_type: "singleLineText".to_string() },
            ],
        };

        let (schema, field_map) = AirtableConnector::build_schema(&meta);
        let arrow_schema = Arc::new(AirtableConnector::to_arrow_schema(&schema));

        Mock::given(method("GET"))
            .and(path(format!("/{}/tblP", connector.config.base_id)))
            .and(query_param("pageSize", "100"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "records": [
                    {"id": "rec1", "createdTime": "2024-01-01T00:00:00.000Z", "fields": {"Name": "First"}}
                ],
                "offset": "page2cursor"
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/{}/tblP", connector.config.base_id)))
            .and(query_param("offset", "page2cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "records": [
                    {"id": "rec2", "createdTime": "2024-01-02T00:00:00.000Z", "fields": {"Name": "Second"}}
                ]
            })))
            .mount(&server)
            .await;

        let batches = connector
            .fetch_records_raw("tblP", &schema, arrow_schema, &field_map, None)
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_fetch_records_raw_with_filter() {
        let server = MockServer::start().await;
        let connector = test_connector(server.uri());

        let meta = AirtableTableMeta {
            id: "tblF".to_string(),
            name: "F".to_string(),
            fields: vec![
                AirtableFieldMeta { name: "Name".to_string(), field_type: "singleLineText".to_string() },
            ],
        };

        let (schema, field_map) = AirtableConnector::build_schema(&meta);
        let arrow_schema = Arc::new(AirtableConnector::to_arrow_schema(&schema));

        Mock::given(method("GET"))
            .and(path(format!("/{}/tblF", connector.config.base_id)))
            .and(query_param("filterByFormula", "{Name}='test'"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "records": [
                    {"id": "rec1", "createdTime": "2024-01-01T00:00:00.000Z", "fields": {"Name": "test"}}
                ]
            })))
            .mount(&server)
            .await;

        let batches = connector
            .fetch_records_raw("tblF", &schema, arrow_schema, &field_map, Some("{Name}='test'"))
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    // ── Validate credentials test ────────────────────────────────────

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let server = MockServer::start().await;
        let connector = test_connector(server.uri());

        Mock::given(method("GET"))
            .and(path(format!("/meta/bases/{}/tables", connector.config.base_id)))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(sample_metadata_response()),
            )
            .mount(&server)
            .await;

        let result = connector.validate_credentials().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_credentials_failure() {
        let server = MockServer::start().await;
        let connector = test_connector(server.uri());

        Mock::given(method("GET"))
            .and(path(format!("/meta/bases/{}/tables", connector.config.base_id)))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[test]
    fn test_escape_formula_value() {
        assert_eq!(AirtableConnector::escape_formula_value("hello"), "hello");
        assert_eq!(AirtableConnector::escape_formula_value("it's"), "it\\'s");
        assert_eq!(AirtableConnector::escape_formula_value("a\\b"), "a\\\\b");
    }
}
