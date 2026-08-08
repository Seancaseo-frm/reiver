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

const API_BASE: &str = "https://api.xero.com/api.xro/2.0";
const BATCH_CAPACITY: usize = 4096;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 100;
const MAX_TOTAL_ROWS: usize = 1_000_000;

const ALL_TABLES: &[&str] = &[
    "invoices",
    "contacts",
    "accounts",
    "payments",
    "bank_transactions",
    "items",
];

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct XeroConfig {
    pub access_token: SecretString,
    pub tenant_id: String,
    pub api_base: Option<String>,
}

impl std::fmt::Debug for XeroConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XeroConfig")
            .field("access_token", &"[REDACTED]")
            .field("tenant_id", &self.tenant_id)
            .field("api_base", &self.api_base)
            .finish()
    }
}

impl XeroConfig {
    pub fn new(access_token: impl Into<String>, tenant_id: impl Into<String>) -> Self {
        Self {
            access_token: SecretString::new(access_token.into()),
            tenant_id: tenant_id.into(),
            api_base: None,
        }
    }

    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Self {
        self.api_base = Some(api_base.into());
        self
    }

    fn base_url(&self) -> &str {
        self.api_base.as_deref().unwrap_or(API_BASE)
    }
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct XeroConnector {
    config: XeroConfig,
    http: reqwest::Client,
    #[cfg(test)]
    base_url_override: Option<String>,
}

impl XeroConnector {
    pub fn new(config: XeroConfig) -> Self {
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
        if_modified_since: Option<&str>,
    ) -> ConnectorResult<serde_json::Value> {
        let url = self.resolve_url(path);
        let mut attempts = 0u32;

        loop {
            let mut req = self
                .http
                .get(&url)
                .header("Authorization", format!("Bearer {}", self.config.access_token.expose()))
                .header("Xero-Tenant-Id", &self.config.tenant_id)
                .header("Accept", "application/json");

            if let Some(since) = if_modified_since {
                req = req.header("If-Modified-Since", since);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("Xero request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Xero access token".to_string(),
                ));
            }

            if resp.status() == 429 {
                if attempts < MAX_RETRIES {
                    attempts += 1;
                    let delay = retry_delay_from_headers(resp.headers(), attempts);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                return Err(ConnectorError::RateLimited { retry_after_secs: 60 });
            }

            if resp.status() == 404 {
                return Err(ConnectorError::Internal("Xero API: not found (404)".to_string()));
            }

            if resp.status() == 304 {
                return Ok(serde_json::Value::Object(serde_json::Map::new()));
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "Xero API error ({}): {}",
                    status, body
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Xero response: {}", e))
            })?;

            return Ok(json);
        }
    }

    // -----------------------------------------------------------------------
    // Fetch strategies
    // -----------------------------------------------------------------------

    async fn fetch_paginated(
        &self,
        base_path: &str,
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
        let mut page = 1u64;

        let if_modified_since = resolve_if_modified_since(options);

        loop {
            if total_rows >= max_rows {
                break;
            }

            let sep = if base_path.contains('?') { '&' } else { '?' };
            let path = format!("{}{}page={}", base_path, sep, page);

            let body = self.api_get(&path, if_modified_since.as_deref()).await?;

            let items = body
                .get(items_key)
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

    async fn do_fetch(
        &self,
        table: &str,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let schema = get_table_schema(table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("Unknown table: {}", table))
        })?;
        let arrow_schema = Arc::new(to_arrow_schema(&schema));

        let (endpoint, items_key) = match table {
            "invoices" => ("/Invoices", "Invoices"),
            "contacts" => ("/Contacts", "Contacts"),
            "accounts" => ("/Accounts", "Accounts"),
            "payments" => ("/Payments", "Payments"),
            "bank_transactions" => ("/BankTransactions", "BankTransactions"),
            "items" => ("/Items", "Items"),
            _ => return Err(ConnectorError::TableNotFound(table.to_string())),
        };

        self.fetch_paginated(endpoint, items_key, table, &schema, arrow_schema, options)
            .await
    }
}

// ---------------------------------------------------------------------------
// Retry helpers
// ---------------------------------------------------------------------------

fn retry_delay_from_headers(headers: &reqwest::header::HeaderMap, attempt: u32) -> u64 {
    if let Some(retry_after) = headers
        .get("Retry-After")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
    {
        if retry_after > 0 && retry_after < 120 {
            return retry_after * 1000;
        }
    }
    INITIAL_RETRY_DELAY_MS * 2u64.pow(attempt - 1)
}

// ---------------------------------------------------------------------------
// Incremental sync helpers
// ---------------------------------------------------------------------------

fn resolve_if_modified_since(options: &FetchOptions) -> Option<String> {
    if let (Some(key), Some(val)) = (&options.incremental_key, &options.last_value) {
        if key == "updated_date_utc" {
            return Some(val.clone());
        }
    }

    for pred in &options.predicates {
        if let Predicate::GreaterThan { column, value, .. } = pred {
            if column == "updated_date_utc" {
                return Some(value.to_string());
            }
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Xero timestamp parsing
// ---------------------------------------------------------------------------

fn parse_xero_timestamp(s: &str) -> Option<i64> {
    if s.starts_with("/Date(") {
        let inner = s.trim_start_matches("/Date(").trim_end_matches(")/");
        let millis_str = if let Some(pos) = inner.rfind('+') {
            &inner[..pos]
        } else if let Some(pos) = inner.rfind('-') {
            if pos > 0 { &inner[..pos] } else { inner }
        } else {
            inner
        };
        let millis: i64 = millis_str.parse().ok()?;
        return Some(millis * 1000);
    }

    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
}

// ---------------------------------------------------------------------------
// Table schemas
// ---------------------------------------------------------------------------

fn get_table_schema(table: &str) -> Option<TableSchema> {
    match table {
        "invoices" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("type", ColumnType::String, false),
                ColumnSchema::new("contact_id", ColumnType::String, false),
                ColumnSchema::new("contact_name", ColumnType::String, false),
                ColumnSchema::new("date", ColumnType::Timestamp, false),
                ColumnSchema::new("due_date", ColumnType::Timestamp, true),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("line_amount_types", ColumnType::String, false),
                ColumnSchema::new("sub_total", ColumnType::Float64, false),
                ColumnSchema::new("total_tax", ColumnType::Float64, false),
                ColumnSchema::new("total", ColumnType::Float64, false),
                ColumnSchema::new("amount_due", ColumnType::Float64, false),
                ColumnSchema::new("amount_paid", ColumnType::Float64, false),
                ColumnSchema::new("amount_credited", ColumnType::Float64, false),
                ColumnSchema::new("currency_code", ColumnType::String, false),
                ColumnSchema::new("updated_date_utc", ColumnType::Timestamp, false),
            ],
        }),

        "contacts" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("first_name", ColumnType::String, true),
                ColumnSchema::new("last_name", ColumnType::String, true),
                ColumnSchema::new("email", ColumnType::String, true),
                ColumnSchema::new("is_supplier", ColumnType::Boolean, false),
                ColumnSchema::new("is_customer", ColumnType::Boolean, false),
                ColumnSchema::new("account_number", ColumnType::String, true),
                ColumnSchema::new("tax_number", ColumnType::String, true),
                ColumnSchema::new("phone", ColumnType::String, true),
                ColumnSchema::new("updated_date_utc", ColumnType::Timestamp, false),
            ],
        }),

        "accounts" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("code", ColumnType::String, true),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("type", ColumnType::String, false),
                ColumnSchema::new("tax_type", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("class", ColumnType::String, true),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("updated_date_utc", ColumnType::Timestamp, false),
            ],
        }),

        "payments" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("date", ColumnType::Timestamp, false),
                ColumnSchema::new("amount", ColumnType::Float64, false),
                ColumnSchema::new("reference", ColumnType::String, true),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("invoice_id", ColumnType::String, true),
                ColumnSchema::new("account_id", ColumnType::String, true),
                ColumnSchema::new("currency_code", ColumnType::String, true),
                ColumnSchema::new("updated_date_utc", ColumnType::Timestamp, false),
            ],
        }),

        "bank_transactions" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("type", ColumnType::String, false),
                ColumnSchema::new("contact_id", ColumnType::String, true),
                ColumnSchema::new("date", ColumnType::Timestamp, false),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("line_amount_types", ColumnType::String, false),
                ColumnSchema::new("sub_total", ColumnType::Float64, false),
                ColumnSchema::new("total_tax", ColumnType::Float64, false),
                ColumnSchema::new("total", ColumnType::Float64, false),
                ColumnSchema::new("updated_date_utc", ColumnType::Timestamp, false),
            ],
        }),

        "items" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("code", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("purchase_description", ColumnType::String, true),
                ColumnSchema::new("is_sold", ColumnType::Boolean, false),
                ColumnSchema::new("is_purchased", ColumnType::Boolean, false),
                ColumnSchema::new("sale_unit_price", ColumnType::Float64, true),
                ColumnSchema::new("purchase_unit_price", ColumnType::Float64, true),
                ColumnSchema::new("updated_date_utc", ColumnType::Timestamp, false),
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
    ArrayFirst(&'static str, &'static str),
}

fn field_mapping(table: &str) -> Option<FieldMapping> {
    match table {
        "invoices" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("InvoiceID")),
                ("type", FieldPath::Direct("Type")),
                ("contact_id", FieldPath::Nested("Contact", "ContactID")),
                ("contact_name", FieldPath::Nested("Contact", "Name")),
                ("date", FieldPath::Direct("Date")),
                ("due_date", FieldPath::Direct("DueDate")),
                ("status", FieldPath::Direct("Status")),
                ("line_amount_types", FieldPath::Direct("LineAmountTypes")),
                ("sub_total", FieldPath::Direct("SubTotal")),
                ("total_tax", FieldPath::Direct("TotalTax")),
                ("total", FieldPath::Direct("Total")),
                ("amount_due", FieldPath::Direct("AmountDue")),
                ("amount_paid", FieldPath::Direct("AmountPaid")),
                ("amount_credited", FieldPath::Direct("AmountCredited")),
                ("currency_code", FieldPath::Direct("CurrencyCode")),
                ("updated_date_utc", FieldPath::Direct("UpdatedDateUTC")),
            ],
        }),

        "contacts" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("ContactID")),
                ("name", FieldPath::Direct("Name")),
                ("first_name", FieldPath::Direct("FirstName")),
                ("last_name", FieldPath::Direct("LastName")),
                ("email", FieldPath::Direct("EmailAddress")),
                ("is_supplier", FieldPath::Direct("IsSupplier")),
                ("is_customer", FieldPath::Direct("IsCustomer")),
                ("account_number", FieldPath::Direct("AccountNumber")),
                ("tax_number", FieldPath::Direct("TaxNumber")),
                ("phone", FieldPath::ArrayFirst("Phones", "PhoneNumber")),
                ("updated_date_utc", FieldPath::Direct("UpdatedDateUTC")),
            ],
        }),

        "accounts" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("AccountID")),
                ("code", FieldPath::Direct("Code")),
                ("name", FieldPath::Direct("Name")),
                ("type", FieldPath::Direct("Type")),
                ("tax_type", FieldPath::Direct("TaxType")),
                ("description", FieldPath::Direct("Description")),
                ("class", FieldPath::Direct("Class")),
                ("status", FieldPath::Direct("Status")),
                ("updated_date_utc", FieldPath::Direct("UpdatedDateUTC")),
            ],
        }),

        "payments" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("PaymentID")),
                ("date", FieldPath::Direct("Date")),
                ("amount", FieldPath::Direct("Amount")),
                ("reference", FieldPath::Direct("Reference")),
                ("status", FieldPath::Direct("Status")),
                ("invoice_id", FieldPath::Nested("Invoice", "InvoiceID")),
                ("account_id", FieldPath::Nested("Account", "AccountID")),
                ("currency_code", FieldPath::Direct("CurrencyCode")),
                ("updated_date_utc", FieldPath::Direct("UpdatedDateUTC")),
            ],
        }),

        "bank_transactions" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("BankTransactionID")),
                ("type", FieldPath::Direct("Type")),
                ("contact_id", FieldPath::Nested("Contact", "ContactID")),
                ("date", FieldPath::Direct("Date")),
                ("status", FieldPath::Direct("Status")),
                ("line_amount_types", FieldPath::Direct("LineAmountTypes")),
                ("sub_total", FieldPath::Direct("SubTotal")),
                ("total_tax", FieldPath::Direct("TotalTax")),
                ("total", FieldPath::Direct("Total")),
                ("updated_date_utc", FieldPath::Direct("UpdatedDateUTC")),
            ],
        }),

        "items" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("ItemID")),
                ("code", FieldPath::Direct("Code")),
                ("name", FieldPath::Direct("Name")),
                ("description", FieldPath::Direct("Description")),
                ("purchase_description", FieldPath::Direct("PurchaseDescription")),
                ("is_sold", FieldPath::Direct("IsSold")),
                ("is_purchased", FieldPath::Direct("IsPurchased")),
                ("sale_unit_price", FieldPath::Nested("SalesDetails", "UnitPrice")),
                ("purchase_unit_price", FieldPath::Nested("PurchaseDetails", "UnitPrice")),
                ("updated_date_utc", FieldPath::Direct("UpdatedDateUTC")),
            ],
        }),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Data parsing
// ---------------------------------------------------------------------------

fn resolve_field(item: &serde_json::Value, path: &FieldPath) -> Option<serde_json::Value> {
    match path {
        FieldPath::Direct(key) => item.get(key).filter(|v| !v.is_null()).cloned(),
        FieldPath::Nested(parent, child) => item
            .get(parent)
            .and_then(|p| p.get(child))
            .filter(|v| !v.is_null())
            .cloned(),
        FieldPath::ArrayFirst(array_key, field_key) => item
            .get(array_key)
            .and_then(|arr| arr.as_array())
            .and_then(|arr| arr.first())
            .and_then(|el| el.get(field_key))
            .filter(|v| !v.is_null())
            .cloned(),
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
            ColumnType::Float64 => {
                let parsed = raw_val.as_ref().and_then(|v| v.as_f64());
                builders.builder(idx).append_f64(parsed);
            }
            ColumnType::Boolean => {
                let parsed = raw_val.as_ref().and_then(|v| v.as_bool());
                builders.builder(idx).append_bool(parsed);
            }
            ColumnType::Timestamp => {
                let parsed = raw_val
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .and_then(parse_xero_timestamp);
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
impl Connector for XeroConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Xero
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            tables.push(TableInfo {
                name: name.to_string(),
                schema,
                supports_incremental: true,
                incremental_key: Some("updated_date_utc".to_string()),
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
        self.api_get("/Organisation", None).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> XeroConfig {
        XeroConfig::new("test-xero-token", "test-tenant-id")
    }

    fn test_connector_with_base(base_url: &str) -> XeroConnector {
        let config = test_config();
        XeroConnector {
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
    fn test_invoices_schema() {
        let schema = get_table_schema("invoices").unwrap();
        assert_eq!(schema.columns.len(), 16);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].data_type, ColumnType::String);
    }

    #[test]
    fn test_contacts_schema() {
        let schema = get_table_schema("contacts").unwrap();
        assert_eq!(schema.columns.len(), 11);
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
        assert!(!debug.contains("test-xero-token"));
        assert!(debug.contains("test-tenant-id"));
    }

    #[test]
    fn test_config_api_base() {
        let config = XeroConfig::new("token", "tenant")
            .with_api_base("https://xero.example.com/api");
        assert_eq!(config.base_url(), "https://xero.example.com/api");
    }

    #[test]
    fn test_config_default_base() {
        let config = XeroConfig::new("token", "tenant");
        assert_eq!(config.base_url(), "https://api.xero.com/api.xro/2.0");
    }

    // -- Timestamp parsing --

    #[test]
    fn test_parse_xero_timestamp() {
        let ts = parse_xero_timestamp("/Date(1439434356790+0000)/");
        assert!(ts.is_some());
        assert_eq!(ts.unwrap(), 1439434356790 * 1000);
    }

    #[test]
    fn test_parse_xero_timestamp_negative() {
        let ts = parse_xero_timestamp("/Date(-62135596800000+0000)/");
        assert!(ts.is_some());
        assert_eq!(ts.unwrap(), -62135596800000 * 1000);
    }

    #[test]
    fn test_parse_xero_timestamp_negative_tz() {
        let ts = parse_xero_timestamp("/Date(1439434356790-0500)/");
        assert!(ts.is_some());
        assert_eq!(ts.unwrap(), 1439434356790 * 1000);
    }

    #[test]
    fn test_parse_xero_timestamp_invalid() {
        assert!(parse_xero_timestamp("not-a-date").is_none());
    }

    // -- list_tables --

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = XeroConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), ALL_TABLES.len());

        let incremental: Vec<_> = tables.iter().filter(|t| t.supports_incremental).collect();
        assert_eq!(incremental.len(), 6);

        for table in &tables {
            assert_eq!(table.incremental_key.as_deref(), Some("updated_date_utc"));
            assert_eq!(table.primary_key_columns, vec!["id"]);
        }
    }

    // -- Mock tests --

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/Organisation")
            .match_header("Authorization", "Bearer test-xero-token")
            .match_header("Xero-Tenant-Id", "test-tenant-id")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"Organisations":[{"Name":"Test Org"}]}"#)
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
            .mock("GET", "/Organisation")
            .with_status(401)
            .with_body(r#"{"message":"Unauthorized"}"#)
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
            .mock("GET", "/Organisation")
            .with_status(429)
            .with_header("content-type", "application/json")
            .with_body(r#"{"message":"rate limit exceeded"}"#)
            .expect(4)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let result = connector.api_get("/Organisation", None).await;

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
            .mock("GET", mockito::Matcher::Regex(r"/Invoices\?page=1".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "Invoices": [{
                        "InvoiceID": "inv-001",
                        "Type": "ACCREC",
                        "Contact": {"ContactID": "c-001", "Name": "Acme Corp"},
                        "Date": "/Date(1439434356790+0000)/",
                        "Status": "AUTHORISED",
                        "LineAmountTypes": "Exclusive",
                        "SubTotal": 100.0,
                        "TotalTax": 15.0,
                        "Total": 115.0,
                        "AmountDue": 115.0,
                        "AmountPaid": 0.0,
                        "AmountCredited": 0.0,
                        "CurrencyCode": "USD",
                        "UpdatedDateUTC": "/Date(1439434356790+0000)/"
                    }]
                })
                .to_string(),
            )
            .create_async()
            .await;

        let _mock2 = server
            .mock("GET", mockito::Matcher::Regex(r"/Invoices\?page=2".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "Invoices": [{
                        "InvoiceID": "inv-002",
                        "Type": "ACCPAY",
                        "Contact": {"ContactID": "c-002", "Name": "Widget Co"},
                        "Date": "/Date(1439434356790+0000)/",
                        "Status": "PAID",
                        "LineAmountTypes": "Inclusive",
                        "SubTotal": 200.0,
                        "TotalTax": 30.0,
                        "Total": 230.0,
                        "AmountDue": 0.0,
                        "AmountPaid": 230.0,
                        "AmountCredited": 0.0,
                        "CurrencyCode": "NZD",
                        "UpdatedDateUTC": "/Date(1439434356790+0000)/"
                    }]
                })
                .to_string(),
            )
            .create_async()
            .await;

        let _mock3 = server
            .mock("GET", mockito::Matcher::Regex(r"/Invoices\?page=3".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::json!({"Invoices": []}).to_string())
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("invoices").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_paginated("/Invoices", "Invoices", "invoices", &schema, arrow_schema, &options)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_fetch_contacts() {
        let mut server = mockito::Server::new_async().await;

        let _mock1 = server
            .mock("GET", mockito::Matcher::Regex(r"/Contacts\?page=1".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "Contacts": [{
                        "ContactID": "c-001",
                        "Name": "Acme Corp",
                        "FirstName": "John",
                        "LastName": "Doe",
                        "EmailAddress": "john@acme.com",
                        "IsSupplier": false,
                        "IsCustomer": true,
                        "Phones": [{"PhoneNumber": "555-1234"}],
                        "UpdatedDateUTC": "/Date(1439434356790+0000)/"
                    }]
                })
                .to_string(),
            )
            .create_async()
            .await;

        let _mock2 = server
            .mock("GET", mockito::Matcher::Regex(r"/Contacts\?page=2".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::json!({"Contacts": []}).to_string())
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("contacts", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }
}
