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

const BATCH_CAPACITY: usize = 4096;
const PAGE_LIMIT: u64 = 100;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 100;
const MAX_TOTAL_ROWS: usize = 1_000_000;

const ALL_TABLES: &[&str] = &[
    "orders",
    "products",
    "customers",
    "coupons",
    "refunds",
];

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct WooCommerceConfig {
    pub consumer_key: SecretString,
    pub consumer_secret: SecretString,
    pub store_url: String,
}

impl std::fmt::Debug for WooCommerceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WooCommerceConfig")
            .field("consumer_key", &"[REDACTED]")
            .field("consumer_secret", &"[REDACTED]")
            .field("store_url", &self.store_url)
            .finish()
    }
}

impl WooCommerceConfig {
    pub fn new(
        consumer_key: impl Into<String>,
        consumer_secret: impl Into<String>,
        store_url: impl Into<String>,
    ) -> Self {
        Self {
            consumer_key: SecretString::new(consumer_key.into()),
            consumer_secret: SecretString::new(consumer_secret.into()),
            store_url: store_url.into(),
        }
    }

    fn base_url(&self) -> String {
        format!("{}/wp-json/wc/v3", self.store_url.trim_end_matches('/'))
    }
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct WooCommerceConnector {
    config: WooCommerceConfig,
    http: reqwest::Client,
    #[cfg(test)]
    base_url_override: Option<String>,
}

impl WooCommerceConnector {
    pub fn new(config: WooCommerceConfig) -> Self {
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

    async fn api_get(
        &self,
        path: &str,
    ) -> ConnectorResult<(serde_json::Value, Option<u64>)> {
        let url = self.resolve_url(path);
        let mut attempts = 0u32;

        loop {
            let resp = self
                .http
                .get(&url)
                .basic_auth(
                    self.config.consumer_key.expose(),
                    Some(self.config.consumer_secret.expose()),
                )
                .header("User-Agent", "reiver-connector")
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("WooCommerce request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid WooCommerce consumer key or secret".to_string(),
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

            if resp.status() == 404 {
                return Err(ConnectorError::Internal(
                    "WooCommerce API: not found (404)".to_string(),
                ));
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "WooCommerce API error ({}): {}",
                    status, body
                )));
            }

            let total_pages = resp
                .headers()
                .get("X-WP-TotalPages")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse WooCommerce response: {}", e))
            })?;

            return Ok((json, total_pages));
        }
    }

    // -----------------------------------------------------------------------
    // Order discovery (for refunds)
    // -----------------------------------------------------------------------

    async fn discover_order_ids(&self) -> ConnectorResult<Vec<i64>> {
        let mut order_ids = Vec::new();
        let mut page = 1u64;

        loop {
            let path = format!("/orders?per_page={}&page={}", PAGE_LIMIT, page);
            let (body, total_pages) = self.api_get(&path).await?;

            let items = body.as_array().cloned().unwrap_or_default();
            if items.is_empty() {
                break;
            }

            for item in &items {
                if let Some(id) = item.get("id").and_then(|v| v.as_i64()) {
                    order_ids.push(id);
                }
            }

            let max_pages = total_pages.unwrap_or(1);
            if page >= max_pages {
                break;
            }
            page += 1;
            tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        Ok(order_ids)
    }

    // -----------------------------------------------------------------------
    // Fetch strategies
    // -----------------------------------------------------------------------

    async fn fetch_paginated(
        &self,
        path: &str,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
        query_params: &str,
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

            let full_path = if query_params.is_empty() {
                format!("{}?page={}&per_page={}", path, page, PAGE_LIMIT)
            } else {
                format!("{}?page={}&per_page={}{}", path, page, PAGE_LIMIT, query_params)
            };

            let (body, total_pages) = self.api_get(&full_path).await?;

            let items = body.as_array().cloned().unwrap_or_default();
            if items.is_empty() {
                break;
            }

            for item in &items {
                if total_rows >= max_rows {
                    break;
                }

                append_row(item, table, schema, &mut builders, None);
                total_rows += 1;

                if builders.len() >= BATCH_CAPACITY {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                }
            }

            let max_pages = total_pages.unwrap_or(1);
            if page >= max_pages {
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

    async fn fetch_refunds(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let order_ids = self.discover_order_ids().await?;
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);
        let mut total_rows = 0usize;

        for order_id in &order_ids {
            if total_rows >= max_rows {
                break;
            }

            let path = format!("/orders/{}/refunds", order_id);
            let (body, _) = self.api_get(&path).await?;

            let items = body.as_array().cloned().unwrap_or_default();
            for item in &items {
                if total_rows >= max_rows {
                    break;
                }

                append_row(item, "refunds", schema, &mut builders, Some(*order_id));
                total_rows += 1;

                if builders.len() >= BATCH_CAPACITY {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                }
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

    async fn do_fetch(
        &self,
        table: &str,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let schema = get_table_schema(table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("Unknown table: {}", table))
        })?;
        let arrow_schema = Arc::new(to_arrow_schema(&schema));

        let mut query_params = String::new();
        apply_predicate_params(&mut query_params, &options.predicates, table);

        if let (Some(key), Some(val)) = (&options.incremental_key, &options.last_value) {
            if key == "date_modified" {
                query_params.push_str(&format!("&modified_after={}", val));
            }
        }

        match table {
            "orders" => {
                let mut params = "&orderby=date&order=desc".to_string();
                params.push_str(&query_params);
                self.fetch_paginated("/orders", table, &schema, arrow_schema, options, &params)
                    .await
            }
            "products" => {
                self.fetch_paginated("/products", table, &schema, arrow_schema, options, &query_params)
                    .await
            }
            "customers" => {
                let mut params = "&orderby=registered_date&order=desc".to_string();
                params.push_str(&query_params);
                self.fetch_paginated("/customers", table, &schema, arrow_schema, options, &params)
                    .await
            }
            "coupons" => {
                self.fetch_paginated("/coupons", table, &schema, arrow_schema, options, &query_params)
                    .await
            }
            "refunds" => self.fetch_refunds(&schema, arrow_schema, options).await,
            _ => Err(ConnectorError::TableNotFound(table.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Predicate pushdown -> query params
// ---------------------------------------------------------------------------

fn apply_predicate_params(query: &mut String, predicates: &[Predicate], table: &str) {
    for pred in predicates {
        match pred {
            Predicate::Equals { column, value }
                if column == "status"
                    && matches!(table, "orders" | "products") =>
            {
                query.push_str(&format!("&status={}", value));
            }
            Predicate::GreaterThan {
                column,
                value,
                inclusive: _,
            } if column == "date_created" => {
                query.push_str(&format!("&after={}", value));
            }
            Predicate::GreaterThan {
                column,
                value,
                inclusive: _,
            } if column == "date_modified" => {
                query.push_str(&format!("&modified_after={}", value));
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
        "orders" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("number", ColumnType::String, false),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("currency", ColumnType::String, false),
                ColumnSchema::new("total", ColumnType::String, false),
                ColumnSchema::new("discount_total", ColumnType::String, false),
                ColumnSchema::new("shipping_total", ColumnType::String, false),
                ColumnSchema::new("customer_id", ColumnType::Int64, false),
                ColumnSchema::new("billing_email", ColumnType::String, true),
                ColumnSchema::new("payment_method", ColumnType::String, true),
                ColumnSchema::new("date_created", ColumnType::Timestamp, false),
                ColumnSchema::new("date_modified", ColumnType::Timestamp, false),
            ],
        }),

        "products" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("slug", ColumnType::String, false),
                ColumnSchema::new("type", ColumnType::String, false),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("sku", ColumnType::String, true),
                ColumnSchema::new("price", ColumnType::String, false),
                ColumnSchema::new("regular_price", ColumnType::String, true),
                ColumnSchema::new("sale_price", ColumnType::String, true),
                ColumnSchema::new("stock_quantity", ColumnType::Int64, true),
                ColumnSchema::new("stock_status", ColumnType::String, false),
                ColumnSchema::new("categories", ColumnType::String, true),
                ColumnSchema::new("date_created", ColumnType::Timestamp, false),
                ColumnSchema::new("date_modified", ColumnType::Timestamp, false),
            ],
        }),

        "customers" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("email", ColumnType::String, false),
                ColumnSchema::new("first_name", ColumnType::String, true),
                ColumnSchema::new("last_name", ColumnType::String, true),
                ColumnSchema::new("username", ColumnType::String, false),
                ColumnSchema::new("role", ColumnType::String, false),
                ColumnSchema::new("orders_count", ColumnType::Int64, false),
                ColumnSchema::new("total_spent", ColumnType::String, false),
                ColumnSchema::new("date_created", ColumnType::Timestamp, false),
                ColumnSchema::new("date_modified", ColumnType::Timestamp, false),
            ],
        }),

        "coupons" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("code", ColumnType::String, false),
                ColumnSchema::new("amount", ColumnType::String, false),
                ColumnSchema::new("discount_type", ColumnType::String, false),
                ColumnSchema::new("usage_count", ColumnType::Int64, false),
                ColumnSchema::new("usage_limit", ColumnType::Int64, true),
                ColumnSchema::new("date_created", ColumnType::Timestamp, false),
                ColumnSchema::new("date_modified", ColumnType::Timestamp, false),
            ],
        }),

        "refunds" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("order_id", ColumnType::Int64, false),
                ColumnSchema::new("amount", ColumnType::String, false),
                ColumnSchema::new("reason", ColumnType::String, true),
                ColumnSchema::new("date_created", ColumnType::Timestamp, false),
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
    JsonArrayNames(&'static str),
    OrderContext,
}

fn field_mapping(table: &str) -> Option<FieldMapping> {
    match table {
        "orders" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("number", FieldPath::Direct("number")),
                ("status", FieldPath::Direct("status")),
                ("currency", FieldPath::Direct("currency")),
                ("total", FieldPath::Direct("total")),
                ("discount_total", FieldPath::Direct("discount_total")),
                ("shipping_total", FieldPath::Direct("shipping_total")),
                ("customer_id", FieldPath::Direct("customer_id")),
                ("billing_email", FieldPath::Nested("billing", "email")),
                ("payment_method", FieldPath::Direct("payment_method")),
                ("date_created", FieldPath::Direct("date_created")),
                ("date_modified", FieldPath::Direct("date_modified")),
            ],
        }),

        "products" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("slug", FieldPath::Direct("slug")),
                ("type", FieldPath::Direct("type")),
                ("status", FieldPath::Direct("status")),
                ("sku", FieldPath::Direct("sku")),
                ("price", FieldPath::Direct("price")),
                ("regular_price", FieldPath::Direct("regular_price")),
                ("sale_price", FieldPath::Direct("sale_price")),
                ("stock_quantity", FieldPath::Direct("stock_quantity")),
                ("stock_status", FieldPath::Direct("stock_status")),
                ("categories", FieldPath::JsonArrayNames("categories")),
                ("date_created", FieldPath::Direct("date_created")),
                ("date_modified", FieldPath::Direct("date_modified")),
            ],
        }),

        "customers" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("email", FieldPath::Direct("email")),
                ("first_name", FieldPath::Direct("first_name")),
                ("last_name", FieldPath::Direct("last_name")),
                ("username", FieldPath::Direct("username")),
                ("role", FieldPath::Direct("role")),
                ("orders_count", FieldPath::Direct("orders_count")),
                ("total_spent", FieldPath::Direct("total_spent")),
                ("date_created", FieldPath::Direct("date_created")),
                ("date_modified", FieldPath::Direct("date_modified")),
            ],
        }),

        "coupons" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("code", FieldPath::Direct("code")),
                ("amount", FieldPath::Direct("amount")),
                ("discount_type", FieldPath::Direct("discount_type")),
                ("usage_count", FieldPath::Direct("usage_count")),
                ("usage_limit", FieldPath::Direct("usage_limit")),
                ("date_created", FieldPath::Direct("date_created")),
                ("date_modified", FieldPath::Direct("date_modified")),
            ],
        }),

        "refunds" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("order_id", FieldPath::OrderContext),
                ("amount", FieldPath::Direct("amount")),
                ("reason", FieldPath::Direct("reason")),
                ("date_created", FieldPath::Direct("date_created")),
            ],
        }),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Data parsing
// ---------------------------------------------------------------------------

fn parse_wc_timestamp(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().timestamp_micros());
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
}

fn resolve_field(
    item: &serde_json::Value,
    path: &FieldPath,
    order_id: Option<i64>,
) -> Option<serde_json::Value> {
    match path {
        FieldPath::Direct(key) => item.get(key).filter(|v| !v.is_null()).cloned(),
        FieldPath::Nested(parent, child) => item
            .get(parent)
            .and_then(|p| p.get(child))
            .filter(|v| !v.is_null())
            .cloned(),
        FieldPath::JsonArrayNames(key) => {
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
        FieldPath::OrderContext => {
            order_id.map(|id| serde_json::Value::Number(serde_json::Number::from(id)))
        }
    }
}

fn append_row(
    item: &serde_json::Value,
    table: &str,
    schema: &TableSchema,
    builders: &mut ColumnBuilders,
    order_id: Option<i64>,
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

        let raw_val = resolve_field(item, field_path, order_id);

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
                    .and_then(parse_wc_timestamp);
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
impl Connector for WooCommerceConnector {
    fn source_type(&self) -> SourceType {
        SourceType::WooCommerce
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            let (supports_incremental, incremental_key) = match name {
                "orders" | "products" | "customers" | "coupons" => {
                    (true, Some("date_modified".to_string()))
                }
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
        self.api_get("").await?;
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

    fn test_config() -> WooCommerceConfig {
        WooCommerceConfig::new("test-consumer-key", "test-consumer-secret", "https://test-store.com")
    }

    fn test_connector_with_base(base_url: &str) -> WooCommerceConnector {
        let config = test_config();
        WooCommerceConnector {
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
    fn test_orders_schema() {
        let schema = get_table_schema("orders").unwrap();
        assert_eq!(schema.columns.len(), 12);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].data_type, ColumnType::Int64);
    }

    #[test]
    fn test_products_schema() {
        let schema = get_table_schema("products").unwrap();
        assert_eq!(schema.columns.len(), 14);
        let categories = schema.columns.iter().find(|c| c.name == "categories").unwrap();
        assert_eq!(categories.data_type, ColumnType::String);
        assert!(categories.nullable);
    }

    #[test]
    fn test_refunds_schema() {
        let schema = get_table_schema("refunds").unwrap();
        assert_eq!(schema.columns.len(), 5);
        let order_id = schema.columns.iter().find(|c| c.name == "order_id").unwrap();
        assert_eq!(order_id.data_type, ColumnType::Int64);
        assert!(!order_id.nullable);
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
        assert!(!debug.contains("test-consumer-key"));
        assert!(!debug.contains("test-consumer-secret"));
        assert!(debug.contains("test-store.com"));
    }

    // -- Timestamp parsing --

    #[test]
    fn test_parse_wc_timestamp() {
        let ts = parse_wc_timestamp("2024-01-15T10:30:00");
        assert!(ts.is_some());
    }

    #[test]
    fn test_parse_wc_timestamp_with_tz() {
        let ts = parse_wc_timestamp("2024-01-15T10:30:00+00:00");
        assert!(ts.is_some());
    }

    #[test]
    fn test_parse_wc_timestamp_invalid() {
        assert!(parse_wc_timestamp("not-a-date").is_none());
    }

    // -- Predicate pushdown tests --

    #[test]
    fn test_predicate_status_equals() {
        let mut query = String::new();
        let predicates = vec![Predicate::Equals {
            column: CompactString::from("status"),
            value: CompactString::from("processing"),
        }];
        apply_predicate_params(&mut query, &predicates, "orders");
        assert!(query.contains("&status=processing"));
    }

    #[test]
    fn test_predicate_date_after() {
        let mut query = String::new();
        let predicates = vec![Predicate::GreaterThan {
            column: CompactString::from("date_created"),
            value: CompactString::from("2024-01-01T00:00:00"),
            inclusive: false,
        }];
        apply_predicate_params(&mut query, &predicates, "orders");
        assert!(query.contains("&after=2024-01-01T00:00:00"));
    }

    // -- list_tables --

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = WooCommerceConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), 5);

        let incremental: Vec<_> = tables.iter().filter(|t| t.supports_incremental).collect();
        assert_eq!(incremental.len(), 4);

        let refunds_table = tables.iter().find(|t| t.name == "refunds").unwrap();
        assert!(!refunds_table.supports_incremental);
        assert!(refunds_table.incremental_key.is_none());
    }

    // -- Mock tests --

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"store":{"name":"Test Store"}}"#)
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
            .mock("GET", "/")
            .with_status(401)
            .with_body(r#"{"message":"Consumer key is invalid"}"#)
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
            .mock("GET", "/test")
            .with_status(429)
            .with_header("content-type", "application/json")
            .with_body(r#"{"message":"rate limit exceeded"}"#)
            .expect(4)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let result = connector.api_get("/test").await;

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
            .mock("GET", mockito::Matcher::Regex(r"/orders\?.*page=1.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_header("X-WP-TotalPages", "2")
            .with_body(
                serde_json::json!([
                    {
                        "id": 1, "number": "1001", "status": "processing",
                        "currency": "USD", "total": "99.99", "discount_total": "0.00",
                        "shipping_total": "5.00", "customer_id": 10,
                        "billing": {"email": "test@example.com"},
                        "payment_method": "stripe",
                        "date_created": "2024-01-15T10:30:00",
                        "date_modified": "2024-01-15T11:00:00"
                    }
                ])
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let _mock2 = server
            .mock("GET", mockito::Matcher::Regex(r"/orders\?.*page=2.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_header("X-WP-TotalPages", "2")
            .with_body(
                serde_json::json!([
                    {
                        "id": 2, "number": "1002", "status": "completed",
                        "currency": "USD", "total": "49.99", "discount_total": "10.00",
                        "shipping_total": "0.00", "customer_id": 11,
                        "billing": {"email": "user@example.com"},
                        "payment_method": "paypal",
                        "date_created": "2024-01-16T09:00:00",
                        "date_modified": "2024-01-16T10:00:00"
                    }
                ])
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("orders").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_paginated(
                "/orders",
                "orders",
                &schema,
                arrow_schema,
                &options,
                "&orderby=date&order=desc",
            )
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        _mock1.assert_async().await;
        _mock2.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_products() {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock("GET", mockito::Matcher::Regex(r"/products\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_header("X-WP-TotalPages", "1")
            .with_body(
                serde_json::json!([
                    {
                        "id": 100, "name": "T-Shirt", "slug": "t-shirt",
                        "type": "simple", "status": "publish",
                        "sku": "TSH-001", "price": "29.99",
                        "regular_price": "39.99", "sale_price": "29.99",
                        "stock_quantity": 50, "stock_status": "instock",
                        "categories": [
                            {"id": 1, "name": "Clothing"},
                            {"id": 2, "name": "T-Shirts"}
                        ],
                        "date_created": "2024-01-10T08:00:00",
                        "date_modified": "2024-01-12T14:30:00"
                    }
                ])
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("products").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_paginated(
                "/products",
                "products",
                &schema,
                arrow_schema,
                &options,
                "",
            )
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);

        let batch = &batches[0];
        let categories_col = batch
            .column_by_name("categories")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let cat_val = categories_col.value(0);
        assert!(cat_val.contains("Clothing"));
        assert!(cat_val.contains("T-Shirts"));
    }
}
