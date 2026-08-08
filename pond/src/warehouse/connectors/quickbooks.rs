//! QuickBooks Online connector for the data warehouse.
//!
//! Syncs QuickBooks accounting data (invoices, customers, payments, bills,
//! accounts, journal entries) to the warehouse. Uses OAuth 2.0 for
//! authentication and the QuickBooks Online Accounting API v3.

use super::builders::ColumnBuilders;
use super::http_api::{AuthConfig, HttpApiClient};
use super::oauth::OAuthConfig;
use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

const QB_API_BASE: &str = "https://quickbooks.api.intuit.com";
const QB_SANDBOX_API_BASE: &str = "https://sandbox-quickbooks.api.intuit.com";
const QB_MINOR_VERSION: &str = "75";
const PAGE_SIZE: usize = 1000;
const MAX_TOTAL_ROWS: usize = 1_000_000;
const BATCH_THRESHOLD: usize = 1_000;

const TABLES: &[&str] = &[
    "invoices",
    "customers",
    "payments",
    "bills",
    "accounts",
    "journal_entries",
];

/// QuickBooks connector configuration.
#[derive(Clone)]
pub struct QuickBooksConfig {
    pub oauth: Arc<OAuthConfig>,
    pub realm_id: String,
    pub sandbox: bool,
}

impl std::fmt::Debug for QuickBooksConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickBooksConfig")
            .field("oauth", &"***REDACTED***")
            .field("realm_id", &self.realm_id)
            .field("sandbox", &self.sandbox)
            .finish()
    }
}

impl QuickBooksConfig {
    pub fn new(oauth: OAuthConfig, realm_id: impl Into<String>) -> Self {
        Self {
            oauth: Arc::new(oauth),
            realm_id: realm_id.into(),
            sandbox: false,
        }
    }

    pub fn with_sandbox(mut self, sandbox: bool) -> Self {
        self.sandbox = sandbox;
        self
    }
}

/// QuickBooks Online data source connector.
pub struct QuickBooksConnector {
    #[allow(dead_code)]
    config: QuickBooksConfig,
    client: HttpApiClient,
    base_path: String,
}

impl QuickBooksConnector {
    pub fn new(config: QuickBooksConfig) -> Self {
        let api_base = if config.sandbox {
            QB_SANDBOX_API_BASE
        } else {
            QB_API_BASE
        };

        let client = HttpApiClient::new(api_base)
            .with_auth(AuthConfig::OAuth(config.oauth.clone()))
            .with_header("Accept", "application/json")
            .with_rate_limit(500, std::time::Duration::from_secs(60));

        let base_path = format!("/v3/company/{}", config.realm_id);

        Self {
            config,
            client,
            base_path,
        }
    }

    fn get_table_schema(table: &str) -> Option<TableSchema> {
        let columns = match table {
            "invoices" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("QuickBooks invoice ID"),
                ColumnSchema::new("doc_number", ColumnType::String, true)
                    .with_description("Reference number for the transaction"),
                ColumnSchema::new("txn_date", ColumnType::Date, true)
                    .with_description("Transaction date"),
                ColumnSchema::new("due_date", ColumnType::Date, true)
                    .with_description("Date when the payment is due"),
                ColumnSchema::new("total_amt", ColumnType::Float64, true)
                    .with_description("Total amount of the invoice"),
                ColumnSchema::new("balance", ColumnType::Float64, true)
                    .with_description("Outstanding balance on the invoice"),
                ColumnSchema::new("customer_ref_value", ColumnType::String, true)
                    .with_description("Customer ID reference"),
                ColumnSchema::new("customer_ref_name", ColumnType::String, true)
                    .with_description("Customer display name"),
                ColumnSchema::new("email_status", ColumnType::String, true)
                    .with_description("Email delivery status"),
                ColumnSchema::new("print_status", ColumnType::String, true)
                    .with_description("Print status of the invoice"),
                ColumnSchema::new("line_items", ColumnType::Json, true)
                    .with_description("Invoice line items as JSON array"),
                ColumnSchema::new("currency_ref", ColumnType::String, true)
                    .with_description("Currency code"),
                ColumnSchema::new("metadata_create_time", ColumnType::Timestamp, true)
                    .with_description("Time the invoice was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("metadata_last_updated_time", ColumnType::Timestamp, true)
                    .with_description("Time the invoice was last modified")
                    .with_timezone("UTC"),
            ],
            "customers" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("QuickBooks customer ID"),
                ColumnSchema::new("display_name", ColumnType::String, true)
                    .with_description("Full display name of the customer"),
                ColumnSchema::new("given_name", ColumnType::String, true)
                    .with_description("Customer first/given name"),
                ColumnSchema::new("family_name", ColumnType::String, true)
                    .with_description("Customer last/family name"),
                ColumnSchema::new("company_name", ColumnType::String, true)
                    .with_description("Company name"),
                ColumnSchema::new("primary_email", ColumnType::String, true)
                    .with_description("Primary email address"),
                ColumnSchema::new("primary_phone", ColumnType::String, true)
                    .with_description("Primary phone number"),
                ColumnSchema::new("balance", ColumnType::Float64, true)
                    .with_description("Open balance for the customer"),
                ColumnSchema::new("active", ColumnType::Boolean, true)
                    .with_description("Whether the customer is active"),
                ColumnSchema::new("currency_ref", ColumnType::String, true)
                    .with_description("Currency code"),
                ColumnSchema::new("metadata_create_time", ColumnType::Timestamp, true)
                    .with_description("Time the customer was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("metadata_last_updated_time", ColumnType::Timestamp, true)
                    .with_description("Time the customer was last modified")
                    .with_timezone("UTC"),
            ],
            "payments" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("QuickBooks payment ID"),
                ColumnSchema::new("total_amt", ColumnType::Float64, true)
                    .with_description("Total amount of the payment"),
                ColumnSchema::new("txn_date", ColumnType::Date, true)
                    .with_description("Transaction date"),
                ColumnSchema::new("customer_ref_value", ColumnType::String, true)
                    .with_description("Customer ID reference"),
                ColumnSchema::new("customer_ref_name", ColumnType::String, true)
                    .with_description("Customer display name"),
                ColumnSchema::new("payment_method_ref", ColumnType::String, true)
                    .with_description("Payment method name"),
                ColumnSchema::new("deposit_to_account_ref", ColumnType::String, true)
                    .with_description("Account deposited to"),
                ColumnSchema::new("currency_ref", ColumnType::String, true)
                    .with_description("Currency code"),
                ColumnSchema::new("metadata_create_time", ColumnType::Timestamp, true)
                    .with_description("Time the payment was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("metadata_last_updated_time", ColumnType::Timestamp, true)
                    .with_description("Time the payment was last modified")
                    .with_timezone("UTC"),
            ],
            "bills" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("QuickBooks bill ID"),
                ColumnSchema::new("doc_number", ColumnType::String, true)
                    .with_description("Reference number for the bill"),
                ColumnSchema::new("txn_date", ColumnType::Date, true)
                    .with_description("Transaction date"),
                ColumnSchema::new("due_date", ColumnType::Date, true)
                    .with_description("Date when the payment is due"),
                ColumnSchema::new("total_amt", ColumnType::Float64, true)
                    .with_description("Total amount of the bill"),
                ColumnSchema::new("balance", ColumnType::Float64, true)
                    .with_description("Outstanding balance on the bill"),
                ColumnSchema::new("vendor_ref_value", ColumnType::String, true)
                    .with_description("Vendor ID reference"),
                ColumnSchema::new("vendor_ref_name", ColumnType::String, true)
                    .with_description("Vendor display name"),
                ColumnSchema::new("line_items", ColumnType::Json, true)
                    .with_description("Bill line items as JSON array"),
                ColumnSchema::new("currency_ref", ColumnType::String, true)
                    .with_description("Currency code"),
                ColumnSchema::new("metadata_create_time", ColumnType::Timestamp, true)
                    .with_description("Time the bill was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("metadata_last_updated_time", ColumnType::Timestamp, true)
                    .with_description("Time the bill was last modified")
                    .with_timezone("UTC"),
            ],
            "accounts" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("QuickBooks account ID"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Account name"),
                ColumnSchema::new("account_type", ColumnType::String, true)
                    .with_description("Account type (e.g., Bank, Expense, Income)"),
                ColumnSchema::new("account_sub_type", ColumnType::String, true)
                    .with_description("Account sub-type"),
                ColumnSchema::new("current_balance", ColumnType::Float64, true)
                    .with_description("Current balance of the account"),
                ColumnSchema::new("active", ColumnType::Boolean, true)
                    .with_description("Whether the account is active"),
                ColumnSchema::new("classification", ColumnType::String, true)
                    .with_description("Classification (Asset, Liability, Equity, Revenue, Expense)"),
                ColumnSchema::new("currency_ref", ColumnType::String, true)
                    .with_description("Currency code"),
                ColumnSchema::new("metadata_create_time", ColumnType::Timestamp, true)
                    .with_description("Time the account was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("metadata_last_updated_time", ColumnType::Timestamp, true)
                    .with_description("Time the account was last modified")
                    .with_timezone("UTC"),
            ],
            "journal_entries" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("QuickBooks journal entry ID"),
                ColumnSchema::new("doc_number", ColumnType::String, true)
                    .with_description("Reference number for the journal entry"),
                ColumnSchema::new("txn_date", ColumnType::Date, true)
                    .with_description("Transaction date"),
                ColumnSchema::new("total_amt", ColumnType::Float64, true)
                    .with_description("Total amount of the journal entry"),
                ColumnSchema::new("adjustment", ColumnType::Boolean, true)
                    .with_description("Whether this is an adjustment entry"),
                ColumnSchema::new("private_note", ColumnType::String, true)
                    .with_description("Private memo/note"),
                ColumnSchema::new("line_items", ColumnType::Json, true)
                    .with_description("Journal entry line items as JSON array"),
                ColumnSchema::new("currency_ref", ColumnType::String, true)
                    .with_description("Currency code"),
                ColumnSchema::new("metadata_create_time", ColumnType::Timestamp, true)
                    .with_description("Time the journal entry was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("metadata_last_updated_time", ColumnType::Timestamp, true)
                    .with_description("Time the journal entry was last modified")
                    .with_timezone("UTC"),
            ],
            _ => return None,
        };

        Some(TableSchema { columns })
    }

    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), col.nullable))
            .collect();
        Schema::new(fields)
    }

    /// Map a warehouse table name to the QuickBooks entity name used in queries.
    fn entity_name(table: &str) -> Option<&'static str> {
        match table {
            "invoices" => Some("Invoice"),
            "customers" => Some("Customer"),
            "payments" => Some("Payment"),
            "bills" => Some("Bill"),
            "accounts" => Some("Account"),
            "journal_entries" => Some("JournalEntry"),
            _ => None,
        }
    }

    /// The response JSON key under `QueryResponse` that holds the array of
    /// results. QuickBooks returns the entity name as the key (e.g. "Invoice").
    fn response_key(table: &str) -> Option<&'static str> {
        Self::entity_name(table)
    }

    /// Build a QuickBooks query string for the given entity with optional
    /// incremental filtering and pagination.
    fn build_query(
        entity: &str,
        start_position: usize,
        last_value: Option<&str>,
    ) -> String {
        let mut query = format!("SELECT * FROM {}", entity);
        if let Some(lv) = last_value {
            query.push_str(&format!(
                " WHERE MetaData.LastUpdatedTime > '{}' ORDER BY MetaData.LastUpdatedTime ASC",
                lv
            ));
        }
        query.push_str(&format!(
            " STARTPOSITION {} MAXRESULTS {}",
            start_position, PAGE_SIZE
        ));
        query
    }

    /// Parse a QuickBooks timestamp (ISO-8601 format) to epoch microseconds.
    fn parse_qb_timestamp(value: &str) -> Option<i64> {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
            return Some(dt.timestamp_micros());
        }
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
            return Some(dt.and_utc().timestamp_micros());
        }
        None
    }

    /// Parse a QuickBooks date string (YYYY-MM-DD) to days since epoch.
    fn parse_qb_date(value: &str) -> Option<i32> {
        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .ok()
            .map(|d| {
                (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
            })
    }

    /// Fetch a table with paginated queries.
    async fn fetch_table_data(
        &self,
        table: &str,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let entity = Self::entity_name(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;
        let response_key = Self::response_key(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;

        let table_schema = Self::get_table_schema(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;
        let arrow_schema = Arc::new(Self::to_arrow_schema(&table_schema));

        let last_value = match (&options.incremental_key, &options.last_value) {
            (Some(_), Some(lv)) => Some(lv.as_str()),
            _ => None,
        };
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(&table_schema, BATCH_THRESHOLD);
        let mut total_rows: usize = 0;
        let mut start_position: usize = 1;
        let query_path = format!("{}/query", self.base_path);

        loop {
            if total_rows >= max_rows {
                break;
            }

            let query = Self::build_query(entity, start_position, last_value);
            let params = vec![
                ("query".to_string(), query),
                ("minorversion".to_string(), QB_MINOR_VERSION.to_string()),
            ];

            let response: serde_json::Value =
                self.client.get_with_params(&query_path, &params).await?;

            let query_response = response.get("QueryResponse").ok_or_else(|| {
                ConnectorError::Internal(
                    "Invalid QuickBooks response: missing 'QueryResponse'".to_string(),
                )
            })?;

            let items = match query_response.get(response_key).and_then(|v| v.as_array()) {
                Some(arr) => arr,
                None => break,
            };

            if items.is_empty() {
                break;
            }

            let page_count = items.len();

            for obj in items {
                Self::append_entity(table, obj, &table_schema, &mut builders);
                total_rows += 1;
                if total_rows >= max_rows {
                    break;
                }
            }

            if builders.len() >= BATCH_THRESHOLD {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
                builders = ColumnBuilders::new(&table_schema, BATCH_THRESHOLD);
            }

            if page_count < PAGE_SIZE {
                break;
            }
            start_position += page_count;
        }

        if builders.len() > 0 {
            let batch = builders.finish(arrow_schema.clone())?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }

        Ok(batches)
    }

    /// Append a single QuickBooks entity (JSON object) to the column builders.
    fn append_entity(
        table: &str,
        obj: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        match table {
            "invoices" => Self::append_invoice(obj, schema, builders),
            "customers" => Self::append_customer(obj, schema, builders),
            "payments" => Self::append_payment(obj, schema, builders),
            "bills" => Self::append_bill(obj, schema, builders),
            "accounts" => Self::append_account(obj, schema, builders),
            "journal_entries" => Self::append_journal_entry(obj, schema, builders),
            _ => {}
        }
    }

    fn append_invoice(
        obj: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => {
                    let v = obj.get("Id").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "doc_number" => {
                    let v = obj.get("DocNumber").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "txn_date" => {
                    let days = obj
                        .get("TxnDate")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_date);
                    builders.builder(i).append_date32(days);
                }
                "due_date" => {
                    let days = obj
                        .get("DueDate")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_date);
                    builders.builder(i).append_date32(days);
                }
                "total_amt" => {
                    builders.builder(i).append_json_value(obj.get("TotalAmt"));
                }
                "balance" => {
                    builders.builder(i).append_json_value(obj.get("Balance"));
                }
                "customer_ref_value" => {
                    let v = obj
                        .pointer("/CustomerRef/value")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "customer_ref_name" => {
                    let v = obj
                        .pointer("/CustomerRef/name")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "email_status" => {
                    let v = obj.get("EmailStatus").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "print_status" => {
                    let v = obj.get("PrintStatus").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "line_items" => {
                    let v = obj.get("Line").map(|v| v.to_string());
                    builders.builder(i).append_string(v.as_deref());
                }
                "currency_ref" => {
                    let v = obj
                        .pointer("/CurrencyRef/value")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "metadata_create_time" => {
                    let micros = obj
                        .pointer("/MetaData/CreateTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                "metadata_last_updated_time" => {
                    let micros = obj
                        .pointer("/MetaData/LastUpdatedTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_customer(
        obj: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => {
                    let v = obj.get("Id").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "display_name" => {
                    let v = obj.get("DisplayName").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "given_name" => {
                    let v = obj.get("GivenName").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "family_name" => {
                    let v = obj.get("FamilyName").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "company_name" => {
                    let v = obj.get("CompanyName").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "primary_email" => {
                    let v = obj
                        .pointer("/PrimaryEmailAddr/Address")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "primary_phone" => {
                    let v = obj
                        .pointer("/PrimaryPhone/FreeFormNumber")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "balance" => {
                    builders.builder(i).append_json_value(obj.get("Balance"));
                }
                "active" => {
                    builders.builder(i).append_json_value(obj.get("Active"));
                }
                "currency_ref" => {
                    let v = obj
                        .pointer("/CurrencyRef/value")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "metadata_create_time" => {
                    let micros = obj
                        .pointer("/MetaData/CreateTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                "metadata_last_updated_time" => {
                    let micros = obj
                        .pointer("/MetaData/LastUpdatedTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_payment(
        obj: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => {
                    let v = obj.get("Id").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "total_amt" => {
                    builders.builder(i).append_json_value(obj.get("TotalAmt"));
                }
                "txn_date" => {
                    let days = obj
                        .get("TxnDate")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_date);
                    builders.builder(i).append_date32(days);
                }
                "customer_ref_value" => {
                    let v = obj
                        .pointer("/CustomerRef/value")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "customer_ref_name" => {
                    let v = obj
                        .pointer("/CustomerRef/name")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "payment_method_ref" => {
                    let v = obj
                        .pointer("/PaymentMethodRef/name")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "deposit_to_account_ref" => {
                    let v = obj
                        .pointer("/DepositToAccountRef/name")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "currency_ref" => {
                    let v = obj
                        .pointer("/CurrencyRef/value")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "metadata_create_time" => {
                    let micros = obj
                        .pointer("/MetaData/CreateTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                "metadata_last_updated_time" => {
                    let micros = obj
                        .pointer("/MetaData/LastUpdatedTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_bill(
        obj: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => {
                    let v = obj.get("Id").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "doc_number" => {
                    let v = obj.get("DocNumber").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "txn_date" => {
                    let days = obj
                        .get("TxnDate")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_date);
                    builders.builder(i).append_date32(days);
                }
                "due_date" => {
                    let days = obj
                        .get("DueDate")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_date);
                    builders.builder(i).append_date32(days);
                }
                "total_amt" => {
                    builders.builder(i).append_json_value(obj.get("TotalAmt"));
                }
                "balance" => {
                    builders.builder(i).append_json_value(obj.get("Balance"));
                }
                "vendor_ref_value" => {
                    let v = obj
                        .pointer("/VendorRef/value")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "vendor_ref_name" => {
                    let v = obj
                        .pointer("/VendorRef/name")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "line_items" => {
                    let v = obj.get("Line").map(|v| v.to_string());
                    builders.builder(i).append_string(v.as_deref());
                }
                "currency_ref" => {
                    let v = obj
                        .pointer("/CurrencyRef/value")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "metadata_create_time" => {
                    let micros = obj
                        .pointer("/MetaData/CreateTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                "metadata_last_updated_time" => {
                    let micros = obj
                        .pointer("/MetaData/LastUpdatedTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_account(
        obj: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => {
                    let v = obj.get("Id").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "name" => {
                    let v = obj.get("Name").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "account_type" => {
                    let v = obj.get("AccountType").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "account_sub_type" => {
                    let v = obj.get("AccountSubType").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "current_balance" => {
                    builders
                        .builder(i)
                        .append_json_value(obj.get("CurrentBalance"));
                }
                "active" => {
                    builders.builder(i).append_json_value(obj.get("Active"));
                }
                "classification" => {
                    let v = obj.get("Classification").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "currency_ref" => {
                    let v = obj
                        .pointer("/CurrencyRef/value")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "metadata_create_time" => {
                    let micros = obj
                        .pointer("/MetaData/CreateTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                "metadata_last_updated_time" => {
                    let micros = obj
                        .pointer("/MetaData/LastUpdatedTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    fn append_journal_entry(
        obj: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => {
                    let v = obj.get("Id").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "doc_number" => {
                    let v = obj.get("DocNumber").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "txn_date" => {
                    let days = obj
                        .get("TxnDate")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_date);
                    builders.builder(i).append_date32(days);
                }
                "total_amt" => {
                    builders.builder(i).append_json_value(obj.get("TotalAmt"));
                }
                "adjustment" => {
                    builders
                        .builder(i)
                        .append_json_value(obj.get("Adjustment"));
                }
                "private_note" => {
                    let v = obj.get("PrivateNote").and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "line_items" => {
                    let v = obj.get("Line").map(|v| v.to_string());
                    builders.builder(i).append_string(v.as_deref());
                }
                "currency_ref" => {
                    let v = obj
                        .pointer("/CurrencyRef/value")
                        .and_then(|v| v.as_str());
                    builders.builder(i).append_string(v);
                }
                "metadata_create_time" => {
                    let micros = obj
                        .pointer("/MetaData/CreateTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                "metadata_last_updated_time" => {
                    let micros = obj
                        .pointer("/MetaData/LastUpdatedTime")
                        .and_then(|v| v.as_str())
                        .and_then(Self::parse_qb_timestamp);
                    builders.builder(i).append_timestamp(micros);
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }
}

#[async_trait]
impl Connector for QuickBooksConnector {
    fn source_type(&self) -> SourceType {
        SourceType::QuickBooks
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        Ok(TABLES
            .iter()
            .filter_map(|&table| {
                Self::get_table_schema(table).map(|schema| TableInfo {
                    name: table.to_string(),
                    schema,
                    supports_incremental: true,
                    incremental_key: Some("metadata_last_updated_time".to_string()),
                    estimated_rows: None,
                    primary_key_columns: vec!["id".to_string()],
                })
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        Self::get_table_schema(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        if !TABLES.contains(&table) {
            return Err(ConnectorError::TableNotFound(table.to_string()));
        }

        let options = FetchOptions {
            incremental_key: incremental_key.map(String::from),
            last_value: last_value.map(String::from),
            ..Default::default()
        };

        self.fetch_table_data(table, &options).await
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            let batches = self.fetch_table_data(table, &options).await?;
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        let path = format!("{}/companyinfo/{}", self.base_path, self.config.realm_id);
        let params = vec![
            ("minorversion".to_string(), QB_MINOR_VERSION.to_string()),
        ];
        let _: serde_json::Value = self.client.get_with_params(&path, &params).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_oauth() -> OAuthConfig {
        let future_expiry = chrono::Utc::now() + chrono::Duration::hours(1);
        OAuthConfig::new(
            "test_client_id",
            "test_client_secret",
            "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        )
        .with_access_token("test_access_token", Some(future_expiry))
        .with_refresh_token("test_refresh_token")
    }

    fn test_config() -> QuickBooksConfig {
        QuickBooksConfig::new(test_oauth(), "1234567890")
    }

    fn test_connector_with_base_url(base_url: &str) -> QuickBooksConnector {
        let config = test_config();
        let client = HttpApiClient::new(base_url)
            .with_auth(AuthConfig::OAuth(config.oauth.clone()))
            .with_header("Accept", "application/json")
            .with_rate_limit(500, std::time::Duration::from_secs(60));
        let base_path = format!("/v3/company/{}", config.realm_id);
        QuickBooksConnector {
            config,
            client,
            base_path,
        }
    }

    #[test]
    fn test_quickbooks_config_creation() {
        let config = test_config();
        assert_eq!(config.realm_id, "1234567890");
        assert!(!config.sandbox);
    }

    #[test]
    fn test_quickbooks_config_debug_redacts() {
        let config = test_config();
        let debug_output = format!("{:?}", config);
        assert!(!debug_output.contains("test_client_secret"));
        assert!(!debug_output.contains("test_access_token"));
        assert!(debug_output.contains("REDACTED"));
        assert!(debug_output.contains("1234567890"));
    }

    #[test]
    fn test_quickbooks_config_sandbox() {
        let config = test_config().with_sandbox(true);
        assert!(config.sandbox);
    }

    #[test]
    fn test_entity_name_mapping() {
        assert_eq!(QuickBooksConnector::entity_name("invoices"), Some("Invoice"));
        assert_eq!(QuickBooksConnector::entity_name("customers"), Some("Customer"));
        assert_eq!(QuickBooksConnector::entity_name("payments"), Some("Payment"));
        assert_eq!(QuickBooksConnector::entity_name("bills"), Some("Bill"));
        assert_eq!(QuickBooksConnector::entity_name("accounts"), Some("Account"));
        assert_eq!(
            QuickBooksConnector::entity_name("journal_entries"),
            Some("JournalEntry")
        );
        assert_eq!(QuickBooksConnector::entity_name("nonexistent"), None);
    }

    #[test]
    fn test_build_query_full_sync() {
        let query = QuickBooksConnector::build_query("Invoice", 1, None);
        assert_eq!(
            query,
            "SELECT * FROM Invoice STARTPOSITION 1 MAXRESULTS 1000"
        );
    }

    #[test]
    fn test_build_query_incremental() {
        let query =
            QuickBooksConnector::build_query("Invoice", 1, Some("2024-06-01T00:00:00-07:00"));
        assert!(query.contains("WHERE MetaData.LastUpdatedTime > '2024-06-01T00:00:00-07:00'"));
        assert!(query.contains("ORDER BY MetaData.LastUpdatedTime ASC"));
        assert!(query.contains("STARTPOSITION 1"));
    }

    #[test]
    fn test_build_query_pagination() {
        let query = QuickBooksConnector::build_query("Customer", 501, None);
        assert!(query.contains("STARTPOSITION 501"));
    }

    #[test]
    fn test_parse_qb_timestamp_rfc3339() {
        let micros = QuickBooksConnector::parse_qb_timestamp("2024-06-15T12:30:00-07:00");
        assert!(micros.is_some());
    }

    #[test]
    fn test_parse_qb_timestamp_invalid() {
        assert!(QuickBooksConnector::parse_qb_timestamp("not-a-date").is_none());
    }

    #[test]
    fn test_parse_qb_date() {
        let days = QuickBooksConnector::parse_qb_date("2024-01-01");
        assert!(days.is_some());
        assert!(days.unwrap() > 0);
    }

    #[test]
    fn test_parse_qb_date_invalid() {
        assert!(QuickBooksConnector::parse_qb_date("not-a-date").is_none());
    }

    #[test]
    fn test_get_table_schema_invoices() {
        let schema = QuickBooksConnector::get_table_schema("invoices");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"doc_number"));
        assert!(names.contains(&"total_amt"));
        assert!(names.contains(&"balance"));
        assert!(names.contains(&"customer_ref_value"));
        assert!(names.contains(&"line_items"));
        assert!(names.contains(&"metadata_create_time"));
        assert!(names.contains(&"metadata_last_updated_time"));
    }

    #[test]
    fn test_get_table_schema_customers() {
        let schema = QuickBooksConnector::get_table_schema("customers");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"display_name"));
        assert!(names.contains(&"primary_email"));
        assert!(names.contains(&"balance"));
        assert!(names.contains(&"active"));
    }

    #[test]
    fn test_get_table_schema_payments() {
        let schema = QuickBooksConnector::get_table_schema("payments");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"total_amt"));
        assert!(names.contains(&"customer_ref_value"));
        assert!(names.contains(&"payment_method_ref"));
    }

    #[test]
    fn test_get_table_schema_bills() {
        let schema = QuickBooksConnector::get_table_schema("bills");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"doc_number"));
        assert!(names.contains(&"vendor_ref_value"));
        assert!(names.contains(&"line_items"));
    }

    #[test]
    fn test_get_table_schema_accounts() {
        let schema = QuickBooksConnector::get_table_schema("accounts");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"account_type"));
        assert!(names.contains(&"current_balance"));
        assert!(names.contains(&"classification"));
    }

    #[test]
    fn test_get_table_schema_journal_entries() {
        let schema = QuickBooksConnector::get_table_schema("journal_entries");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"doc_number"));
        assert!(names.contains(&"adjustment"));
        assert!(names.contains(&"line_items"));
    }

    #[test]
    fn test_get_table_schema_unknown() {
        assert!(QuickBooksConnector::get_table_schema("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_list_tables() {
        let connector = QuickBooksConnector::new(test_config());
        let tables = connector.list_tables().await.unwrap();
        assert_eq!(tables.len(), 6);

        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"invoices"));
        assert!(names.contains(&"customers"));
        assert!(names.contains(&"payments"));
        assert!(names.contains(&"bills"));
        assert!(names.contains(&"accounts"));
        assert!(names.contains(&"journal_entries"));

        for t in &tables {
            assert!(t.supports_incremental);
            assert_eq!(
                t.incremental_key.as_deref(),
                Some("metadata_last_updated_time")
            );
            assert_eq!(t.primary_key_columns, vec!["id".to_string()]);
        }
    }

    #[tokio::test]
    async fn test_get_schema() {
        let connector = QuickBooksConnector::new(test_config());
        let schema = connector.get_schema("invoices").await.unwrap();
        assert!(!schema.columns.is_empty());
    }

    #[tokio::test]
    async fn test_get_schema_not_found() {
        let connector = QuickBooksConnector::new(test_config());
        let result = connector.get_schema("nonexistent").await;
        assert!(matches!(result, Err(ConnectorError::TableNotFound(_))));
    }

    #[test]
    fn test_source_type() {
        let connector = QuickBooksConnector::new(test_config());
        assert_eq!(connector.source_type(), SourceType::QuickBooks);
    }

    #[test]
    fn test_builders_invoice_batch() {
        let schema = QuickBooksConnector::get_table_schema("invoices").unwrap();
        let arrow_schema = Arc::new(QuickBooksConnector::to_arrow_schema(&schema));

        let obj = serde_json::json!({
            "Id": "1",
            "DocNumber": "INV-001",
            "TxnDate": "2024-06-15",
            "DueDate": "2024-07-15",
            "TotalAmt": 1500.00,
            "Balance": 500.00,
            "CustomerRef": {"value": "42", "name": "Acme Corp"},
            "EmailStatus": "EmailSent",
            "PrintStatus": "NeedToPrint",
            "Line": [{"Amount": 1500.00, "Description": "Consulting"}],
            "CurrencyRef": {"value": "USD"},
            "MetaData": {
                "CreateTime": "2024-06-15T10:00:00-07:00",
                "LastUpdatedTime": "2024-06-16T14:30:00-07:00"
            }
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        QuickBooksConnector::append_invoice(&obj, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 14);

        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "1");

        let total_col = batch
            .column_by_name("total_amt")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((total_col.value(0) - 1500.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_builders_customer_batch() {
        let schema = QuickBooksConnector::get_table_schema("customers").unwrap();
        let arrow_schema = Arc::new(QuickBooksConnector::to_arrow_schema(&schema));

        let obj = serde_json::json!({
            "Id": "42",
            "DisplayName": "Acme Corp",
            "GivenName": "John",
            "FamilyName": "Doe",
            "CompanyName": "Acme Corp",
            "PrimaryEmailAddr": {"Address": "john@acme.com"},
            "PrimaryPhone": {"FreeFormNumber": "+1-555-0100"},
            "Balance": 2500.00,
            "Active": true,
            "CurrencyRef": {"value": "USD"},
            "MetaData": {
                "CreateTime": "2024-01-01T00:00:00-08:00",
                "LastUpdatedTime": "2024-06-15T12:00:00-07:00"
            }
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        QuickBooksConnector::append_customer(&obj, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let name_col = batch
            .column_by_name("display_name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Acme Corp");

        let email_col = batch
            .column_by_name("primary_email")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(email_col.value(0), "john@acme.com");
    }

    #[test]
    fn test_builders_account_batch() {
        let schema = QuickBooksConnector::get_table_schema("accounts").unwrap();
        let arrow_schema = Arc::new(QuickBooksConnector::to_arrow_schema(&schema));

        let obj = serde_json::json!({
            "Id": "10",
            "Name": "Checking",
            "AccountType": "Bank",
            "AccountSubType": "Checking",
            "CurrentBalance": 50000.00,
            "Active": true,
            "Classification": "Asset",
            "CurrencyRef": {"value": "USD"},
            "MetaData": {
                "CreateTime": "2023-01-01T00:00:00-08:00",
                "LastUpdatedTime": "2024-06-01T00:00:00-07:00"
            }
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        QuickBooksConnector::append_account(&obj, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let active_col = batch
            .column_by_name("active")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(active_col.value(0));
    }

    #[tokio::test]
    async fn test_validate_credentials_auth_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v3/company/1234567890/companyinfo/1234567890"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mock_server = MockServer::start().await;

        let body = serde_json::json!({
            "CompanyInfo": {
                "CompanyName": "Test Company",
                "LegalName": "Test Company LLC"
            }
        });

        Mock::given(method("GET"))
            .and(path("/v3/company/1234567890/companyinfo/1234567890"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&body))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.validate_credentials().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_invoices_full_sync() {
        let mock_server = MockServer::start().await;

        let body = serde_json::json!({
            "QueryResponse": {
                "Invoice": [
                    {
                        "Id": "1",
                        "DocNumber": "INV-001",
                        "TxnDate": "2024-06-15",
                        "DueDate": "2024-07-15",
                        "TotalAmt": 1500.00,
                        "Balance": 0.00,
                        "CustomerRef": {"value": "42", "name": "Acme Corp"},
                        "EmailStatus": "EmailSent",
                        "PrintStatus": "NeedToPrint",
                        "Line": [],
                        "CurrencyRef": {"value": "USD"},
                        "MetaData": {
                            "CreateTime": "2024-06-15T10:00:00-07:00",
                            "LastUpdatedTime": "2024-06-16T14:30:00-07:00"
                        }
                    },
                    {
                        "Id": "2",
                        "DocNumber": "INV-002",
                        "TxnDate": "2024-07-01",
                        "TotalAmt": 3000.00,
                        "Balance": 3000.00,
                        "CustomerRef": {"value": "43", "name": "Beta Inc"},
                        "Line": [],
                        "CurrencyRef": {"value": "USD"},
                        "MetaData": {
                            "CreateTime": "2024-07-01T08:00:00-07:00",
                            "LastUpdatedTime": "2024-07-01T08:00:00-07:00"
                        }
                    }
                ],
                "startPosition": 1,
                "maxResults": 2
            }
        });

        Mock::given(method("GET"))
            .and(path("/v3/company/1234567890/query"))
            .and(query_param("minorversion", "75"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&body))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let batches = connector
            .fetch_table("invoices", None, None)
            .await
            .unwrap();
        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_fetch_table_not_found() {
        let connector = QuickBooksConnector::new(test_config());
        let result = connector.fetch_table("nonexistent", None, None).await;
        assert!(matches!(result, Err(ConnectorError::TableNotFound(_))));
    }

    #[tokio::test]
    async fn test_fetch_rate_limited() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v3/company/1234567890/query"))
            .respond_with(
                ResponseTemplate::new(429).insert_header("Retry-After", "60"),
            )
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.fetch_table("invoices", None, None).await;
        assert!(matches!(result, Err(ConnectorError::RateLimited { .. })));
    }
}
