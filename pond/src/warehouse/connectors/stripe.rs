//! Stripe connector for the data warehouse.
//!
//! Syncs Stripe data (customers, charges, invoices, etc.) to the warehouse.

use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::crypto::SecretString;
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use super::builders::ColumnBuilders;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

/// Stripe connector configuration.
///
/// The API key is wrapped in `SecretString` to prevent accidental logging.
#[derive(Clone)]
pub struct StripeConfig {
    /// Stripe API key (protected from accidental logging)
    pub api_key: SecretString,
    /// Optional Stripe account ID (for Connect)
    pub account_id: Option<String>,
}

impl std::fmt::Debug for StripeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StripeConfig")
            .field("api_key", &"***REDACTED***")
            .field("account_id", &self.account_id)
            .finish()
    }
}

impl StripeConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: SecretString::new(api_key),
            account_id: None,
        }
    }

    pub fn with_account_id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }
}

/// Stripe data source connector.
pub struct StripeConnector {
    config: StripeConfig,
    client: reqwest::Client,
}

impl StripeConnector {
    /// Available tables in Stripe.
    const TABLES: &'static [&'static str] = &[
        "customers",
        "charges",
        "invoices",
        "subscriptions",
        "products",
        "prices",
        "payment_intents",
        "refunds",
    ];

    /// Create a new Stripe connector.
    pub fn new(config: StripeConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Get the schema for a specific Stripe table.
    fn get_table_schema(table: &str) -> Option<TableSchema> {
        let columns = match table {
            "customers" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the customer"),
                ColumnSchema::new("email", ColumnType::String, true)
                    .with_description("Customer's email address"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Customer's full name"),
                ColumnSchema::new("created", ColumnType::Timestamp, false)
                    .with_description("Time at which the customer was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("currency", ColumnType::String, true)
                    .with_description("Customer's default currency"),
                ColumnSchema::new("delinquent", ColumnType::Boolean, false)
                    .with_description("Whether the customer has unpaid invoices"),
                ColumnSchema::new("metadata", ColumnType::Json, true)
                    .with_description("Custom metadata attached to the customer"),
            ],
            "charges" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the charge"),
                ColumnSchema::new("amount", ColumnType::Int64, false)
                    .with_description("Amount in cents"),
                ColumnSchema::new("currency", ColumnType::String, false)
                    .with_description("Three-letter ISO currency code"),
                ColumnSchema::new("customer", ColumnType::String, true)
                    .with_description("Customer ID"),
                ColumnSchema::new("status", ColumnType::String, false)
                    .with_description("Charge status"),
                ColumnSchema::new("created", ColumnType::Timestamp, false)
                    .with_description("Time at which the charge was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("paid", ColumnType::Boolean, false)
                    .with_description("Whether the charge was successful"),
                ColumnSchema::new("refunded", ColumnType::Boolean, false)
                    .with_description("Whether the charge has been refunded"),
            ],
            "invoices" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the invoice"),
                ColumnSchema::new("customer", ColumnType::String, false)
                    .with_description("Customer ID"),
                ColumnSchema::new("subscription", ColumnType::String, true)
                    .with_description("Subscription ID"),
                ColumnSchema::new("status", ColumnType::String, false)
                    .with_description("Invoice status"),
                ColumnSchema::new("total", ColumnType::Int64, false)
                    .with_description("Total in cents"),
                ColumnSchema::new("currency", ColumnType::String, false)
                    .with_description("Three-letter ISO currency code"),
                ColumnSchema::new("created", ColumnType::Timestamp, false)
                    .with_description("Time at which the invoice was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("due_date", ColumnType::Timestamp, true)
                    .with_description("Invoice due date")
                    .with_timezone("UTC"),
            ],
            "subscriptions" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the subscription"),
                ColumnSchema::new("customer", ColumnType::String, false)
                    .with_description("Customer ID"),
                ColumnSchema::new("status", ColumnType::String, false)
                    .with_description("Subscription status"),
                ColumnSchema::new("current_period_start", ColumnType::Timestamp, false)
                    .with_description("Start of the current period")
                    .with_timezone("UTC"),
                ColumnSchema::new("current_period_end", ColumnType::Timestamp, false)
                    .with_description("End of the current period")
                    .with_timezone("UTC"),
                ColumnSchema::new("created", ColumnType::Timestamp, false)
                    .with_description("Time at which the subscription was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("cancel_at_period_end", ColumnType::Boolean, false)
                    .with_description("Whether subscription will be canceled at period end"),
            ],
            "products" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the product"),
                ColumnSchema::new("name", ColumnType::String, false)
                    .with_description("Product name"),
                ColumnSchema::new("description", ColumnType::String, true)
                    .with_description("Product description"),
                ColumnSchema::new("active", ColumnType::Boolean, false)
                    .with_description("Whether the product is available for purchase"),
                ColumnSchema::new("created", ColumnType::Timestamp, false)
                    .with_description("Time at which the product was created")
                    .with_timezone("UTC"),
            ],
            "prices" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the price"),
                ColumnSchema::new("product", ColumnType::String, false)
                    .with_description("Product ID"),
                ColumnSchema::new("unit_amount", ColumnType::Int64, true)
                    .with_description("Unit amount in cents"),
                ColumnSchema::new("currency", ColumnType::String, false)
                    .with_description("Three-letter ISO currency code"),
                ColumnSchema::new("active", ColumnType::Boolean, false)
                    .with_description("Whether the price is active"),
                ColumnSchema::new("type", ColumnType::String, false)
                    .with_description("Price type (one_time or recurring)"),
                ColumnSchema::new("created", ColumnType::Timestamp, false)
                    .with_description("Time at which the price was created")
                    .with_timezone("UTC"),
            ],
            "payment_intents" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the payment intent"),
                ColumnSchema::new("amount", ColumnType::Int64, false)
                    .with_description("Amount intended to be collected in cents"),
                ColumnSchema::new("currency", ColumnType::String, false)
                    .with_description("Three-letter ISO currency code"),
                ColumnSchema::new("customer", ColumnType::String, true)
                    .with_description("Customer ID"),
                ColumnSchema::new("status", ColumnType::String, false)
                    .with_description("Payment intent status"),
                ColumnSchema::new("created", ColumnType::Timestamp, false)
                    .with_description("Time at which the payment intent was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("payment_method", ColumnType::String, true)
                    .with_description("Payment method ID"),
                ColumnSchema::new("metadata", ColumnType::Json, true)
                    .with_description("Custom metadata"),
            ],
            "refunds" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the refund"),
                ColumnSchema::new("amount", ColumnType::Int64, false)
                    .with_description("Amount refunded in cents"),
                ColumnSchema::new("currency", ColumnType::String, false)
                    .with_description("Three-letter ISO currency code"),
                ColumnSchema::new("charge", ColumnType::String, false)
                    .with_description("Charge ID that was refunded"),
                ColumnSchema::new("status", ColumnType::String, false)
                    .with_description("Refund status"),
                ColumnSchema::new("created", ColumnType::Timestamp, false)
                    .with_description("Time at which the refund was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("reason", ColumnType::String, true)
                    .with_description("Reason for the refund"),
            ],
            _ => return None,
        };

        Some(TableSchema { columns })
    }

    /// Convert table schema to Arrow schema.
    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), col.nullable))
            .collect();
        Schema::new(fields)
    }

    /// Make an authenticated request to the Stripe API.
    async fn stripe_request(&self, endpoint: &str) -> ConnectorResult<serde_json::Value> {
        let url = format!("https://api.stripe.com/v1/{}", endpoint);

        let mut request = self
            .client
            .get(&url)
            .bearer_auth(self.config.api_key.expose());

        if let Some(account_id) = &self.config.account_id {
            request = request.header("Stripe-Account", account_id);
        }

        let response = request.send().await.map_err(|e| {
            ConnectorError::Network(format!("Failed to connect to Stripe API: {}", e))
        })?;

        if response.status() == 401 {
            return Err(ConnectorError::Authentication(
                "Invalid Stripe API key".to_string(),
            ));
        }

        if response.status() == 429 {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(60);
            return Err(ConnectorError::RateLimited {
                retry_after_secs: retry_after,
            });
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ConnectorError::Internal(format!(
                "Stripe API error ({}): {}",
                status, body
            )));
        }

        response.json().await.map_err(|e| {
            ConnectorError::Internal(format!("Failed to parse Stripe response: {}", e))
        })
    }

    // ========================================================================
    // Predicate Translation for API Pushdown
    // ========================================================================

    /// Translate predicates to Stripe API parameters.
    ///
    /// This converts SQL-style predicates into Stripe API query parameters,
    /// enabling predicate pushdown for supported filters.
    ///
    /// # Supported Filters by Table
    ///
    /// | Table | Column | Supported Operations |
    /// |-------|--------|---------------------|
    /// | All | created | `>=`, `<=`, `>`, `<` (as epoch timestamps) |
    /// | charges | customer | `=` |
    /// | charges | status | `=` (succeeded, pending, failed) |
    /// | invoices | customer | `=` |
    /// | invoices | subscription | `=` |
    /// | invoices | status | `=` |
    /// | subscriptions | customer | `=` |
    /// | subscriptions | status | `=` |
    /// | customers | email | `=` |
    ///
    /// # Example
    ///
    /// ```ignore
    /// use crate::warehouse::query::predicate_pushdown::{Predicate, TranslatedPredicate};
    ///
    /// let predicates = vec![
    ///     TranslatedPredicate::new(
    ///         Predicate::Equals { column: "customer".into(), value: "cus_123".into() },
    ///         PredicateTranslation::api_param("customer", "cus_123"),
    ///     ),
    /// ];
    /// let params = connector.translate_predicates(&predicates, "charges");
    /// // params = { "customer": "cus_123" }
    /// ```
    pub fn translate_predicates(
        &self,
        predicates: &[crate::warehouse::query::predicate_pushdown::TranslatedPredicate],
        table: &str,
    ) -> std::collections::HashMap<String, String> {
        use crate::warehouse::query::predicate_pushdown::{Predicate, PredicateTranslation};
        
        let mut params = std::collections::HashMap::new();

        // Collect API params from translations
        for pred in predicates {
            if let PredicateTranslation::ApiParams(p) = &pred.translated {
                params.extend(p.clone());
                continue;
            }

            // Fall back to manual translation based on predicate type
            match &*pred.original {
                Predicate::Equals { column, value } => {
                    if let Some(param_name) = self.get_api_param_name(table, column) {
                        params.insert(param_name, value.to_string());
                    }
                }
                Predicate::GreaterThan { column, value, inclusive } => {
                    if column == "created" {
                        if let Some(epoch) = self.timestamp_to_epoch(value) {
                            let suffix = if *inclusive { "[gte]" } else { "[gt]" };
                            params.insert(format!("created{}", suffix), epoch.to_string());
                        }
                    }
                }
                Predicate::LessThan { column, value, inclusive } => {
                    if column == "created" {
                        if let Some(epoch) = self.timestamp_to_epoch(value) {
                            let suffix = if *inclusive { "[lte]" } else { "[lt]" };
                            params.insert(format!("created{}", suffix), epoch.to_string());
                        }
                    }
                }
                Predicate::Between { column, low, high } => {
                    if column == "created" {
                        if let Some(low_epoch) = self.timestamp_to_epoch(low) {
                            params.insert("created[gte]".to_string(), low_epoch.to_string());
                        }
                        if let Some(high_epoch) = self.timestamp_to_epoch(high) {
                            params.insert("created[lte]".to_string(), high_epoch.to_string());
                        }
                    }
                }
                _ => {}
            }
        }

        params
    }

    /// Get the Stripe API parameter name for a column.
    fn get_api_param_name(&self, table: &str, column: &str) -> Option<String> {
        match (table, column) {
            // Universal columns
            (_, "created") => Some("created".to_string()),
            
            // Customer ID
            ("charges", "customer") => Some("customer".to_string()),
            ("invoices", "customer") => Some("customer".to_string()),
            ("subscriptions", "customer") => Some("customer".to_string()),
            
            // Subscription ID
            ("invoices", "subscription") => Some("subscription".to_string()),
            
            // Status
            ("charges", "status") => Some("status".to_string()),
            ("invoices", "status") => Some("status".to_string()),
            ("subscriptions", "status") => Some("status".to_string()),
            
            // Customer email (for customers table)
            ("customers", "email") => Some("email".to_string()),
            
            _ => None,
        }
    }

    /// Convert a timestamp string to Unix epoch seconds.
    ///
    /// Supports various formats:
    /// - Unix epoch (already a number)
    /// - ISO 8601 / RFC 3339
    /// - YYYY-MM-DD
    /// - YYYY-MM-DD HH:MM:SS
    fn timestamp_to_epoch(&self, value: &str) -> Option<i64> {
        // Already an epoch timestamp?
        if let Ok(epoch) = value.parse::<i64>() {
            return Some(epoch);
        }

        // Try RFC 3339 / ISO 8601
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
            return Some(dt.timestamp());
        }

        // Try date only (YYYY-MM-DD)
        if let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            return date
                .and_hms_opt(0, 0, 0)
                .map(|dt| dt.and_utc().timestamp());
        }

        // Try datetime without timezone
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
            return Some(dt.and_utc().timestamp());
        }

        None
    }

    /// Convert raw `Predicate`s into Stripe API query parameters.
    ///
    /// Reuses the same column-to-param mapping as `translate_predicates` but
    /// works directly on `Predicate` values (no `TranslatedPredicate` wrapper).
    fn predicates_to_api_params(
        &self,
        predicates: &[crate::warehouse::query::predicate_pushdown::Predicate],
        table: &str,
    ) -> std::collections::HashMap<String, String> {
        use crate::warehouse::query::predicate_pushdown::Predicate;

        let mut params = std::collections::HashMap::new();
        for pred in predicates {
            match pred {
                Predicate::Equals { column, value } => {
                    if let Some(param_name) = self.get_api_param_name(table, column) {
                        params.insert(param_name, value.to_string());
                    }
                }
                Predicate::GreaterThan { column, value, inclusive } => {
                    if column == "created" {
                        if let Some(epoch) = self.timestamp_to_epoch(value) {
                            let suffix = if *inclusive { "[gte]" } else { "[gt]" };
                            params.insert(format!("created{}", suffix), epoch.to_string());
                        }
                    }
                }
                Predicate::LessThan { column, value, inclusive } => {
                    if column == "created" {
                        if let Some(epoch) = self.timestamp_to_epoch(value) {
                            let suffix = if *inclusive { "[lte]" } else { "[lt]" };
                            params.insert(format!("created{}", suffix), epoch.to_string());
                        }
                    }
                }
                Predicate::Between { column, low, high } => {
                    if column == "created" {
                        if let Some(low_epoch) = self.timestamp_to_epoch(low) {
                            params.insert("created[gte]".to_string(), low_epoch.to_string());
                        }
                        if let Some(high_epoch) = self.timestamp_to_epoch(high) {
                            params.insert("created[lte]".to_string(), high_epoch.to_string());
                        }
                    }
                }
                Predicate::In { column, values } => {
                    if values.len() == 1 {
                        if let Some(param_name) = self.get_api_param_name(table, column) {
                            params.insert(param_name, values[0].to_string());
                        }
                    }
                    // When len > 1 we intentionally skip pushdown so the
                    // caller applies post-filtering on all values.
                }
                _ => {}
            }
        }
        params
    }

    /// Build a Stripe API endpoint with query parameters.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut params = HashMap::new();
    /// params.insert("customer".to_string(), "cus_123".to_string());
    /// params.insert("created[gte]".to_string(), "1704067200".to_string());
    /// 
    /// let endpoint = connector.build_endpoint_with_params("charges", &params);
    /// // endpoint = "charges?customer=cus_123&created[gte]=1704067200&limit=100"
    /// ```
    pub fn build_endpoint_with_params(
        &self,
        table: &str,
        params: &std::collections::HashMap<String, String>,
        limit: Option<u32>,
    ) -> String {
        let mut query_parts: Vec<String> = params
            .iter()
            .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
            .collect();

        query_parts.push(format!("limit={}", limit.unwrap_or(100)));

        format!("{}?{}", table, query_parts.join("&"))
    }

    /// Validate that a predicate value is acceptable for Stripe.
    ///
    /// Returns an error message if the value is invalid.
    pub fn validate_predicate_value(&self, column: &str, value: &str) -> Result<(), String> {
        match column {
            "status" => {
                let valid_statuses = ["succeeded", "pending", "failed", "open", "paid", 
                    "uncollectible", "void", "draft", "active", "canceled", 
                    "incomplete", "incomplete_expired", "trialing", "past_due", "unpaid"];
                if !valid_statuses.contains(&value) {
                    return Err(format!(
                        "Invalid status '{}'. Valid values: {}",
                        value,
                        valid_statuses.join(", ")
                    ));
                }
            }
            "customer" => {
                if !value.starts_with("cus_") {
                    return Err("Customer ID must start with 'cus_'".to_string());
                }
            }
            "subscription" => {
                if !value.starts_with("sub_") {
                    return Err("Subscription ID must start with 'sub_'".to_string());
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl Connector for StripeConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Stripe
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        Ok(Self::TABLES
            .iter()
            .filter_map(|&table| {
                Self::get_table_schema(table).map(|schema| TableInfo {
                    name: table.to_string(),
                    schema,
                    supports_incremental: true,
                    incremental_key: Some("created".to_string()),
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
        // Validate table name against known tables for security
        if !Self::TABLES.contains(&table) {
            return Err(ConnectorError::TableNotFound(table.to_string()));
        }
        
        let schema = self.get_schema(table).await?;
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        // Process pages incrementally to avoid memory issues
        // Instead of collecting all data, we convert each page to a batch immediately
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(&schema, 1_000);
        let mut starting_after: Option<String> = None;
        let mut total_rows: usize = 0;

        const BATCH_THRESHOLD: usize = 1_000;
        const MAX_TOTAL_ROWS: usize = 1_000_000;

        let mut base_params = vec![("limit", "100".to_string())];

        if let (Some("created"), Some(timestamp)) = (incremental_key, last_value) {
            if let Some(ts) = self.timestamp_to_epoch(timestamp) {
                base_params.push(("created[gt]", ts.to_string()));
            }
        }

        loop {
            let mut params = base_params.clone();
            if let Some(ref cursor) = starting_after {
                params.push(("starting_after", cursor.clone()));
            }

            let query_string = params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");

            let endpoint = format!("{}?{}", table, query_string);
            let response = self.stripe_request(&endpoint).await?;

            let data = response["data"]
                .as_array()
                .ok_or_else(|| ConnectorError::Internal("Invalid Stripe response format".to_string()))?;

            if data.is_empty() {
                break;
            }

            if let Some(last_obj) = data.last() {
                starting_after = last_obj.get("id").and_then(|v| v.as_str()).map(|s| s.to_string());
            }

            for obj in data {
                Self::append_stripe_object(obj, &schema, &mut builders);
            }
            total_rows += data.len();

            if builders.len() >= BATCH_THRESHOLD {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
                builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
            }

            let has_more = response["has_more"].as_bool().unwrap_or(false);
            if !has_more {
                break;
            }

            if total_rows >= MAX_TOTAL_ROWS {
                tracing::warn!(
                    table = table,
                    count = total_rows,
                    "Stripe sync reached safety limit of 1M records"
                );
                break;
            }
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

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>> {
        Box::pin(async move {
            if options.predicates.is_empty() {
                let batches = self.fetch_table(
                    table,
                    options.incremental_key.as_deref(),
                    options.last_value.as_deref(),
                ).await?;
                let stream = futures::stream::iter(batches.into_iter().map(Ok));
                return Ok(Box::pin(stream) as RecordBatchStream);
            }

            let extra_params = self.predicates_to_api_params(&options.predicates, table);

            if !Self::TABLES.contains(&table) {
                return Err(ConnectorError::TableNotFound(table.to_string()));
            }

            let schema = self.get_schema(table).await?;
            let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

            let mut batches: Vec<RecordBatch> = Vec::new();
            let mut builders = ColumnBuilders::new(&schema, 1_000);
            let mut starting_after: Option<String> = None;
            let mut total_rows: usize = 0;
            const BATCH_THRESHOLD: usize = 1_000;
            const MAX_TOTAL_ROWS: usize = 1_000_000;

            let mut base_params: Vec<(String, String)> = vec![("limit".to_string(), "100".to_string())];
            if let (Some("created"), Some(timestamp)) = (
                options.incremental_key.as_deref(),
                options.last_value.as_deref(),
            ) {
                if let Some(ts) = self.timestamp_to_epoch(timestamp) {
                    base_params.push(("created[gt]".to_string(), ts.to_string()));
                }
            }

            for (k, v) in &extra_params {
                base_params.push((k.clone(), v.clone()));
            }

            loop {
                let mut params = base_params.clone();
                if let Some(ref cursor) = starting_after {
                    params.push(("starting_after".to_string(), cursor.clone()));
                }

                let query_string = params
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("&");

                let endpoint = format!("{}?{}", table, query_string);
                let response = self.stripe_request(&endpoint).await?;

                let data = response["data"]
                    .as_array()
                    .ok_or_else(|| ConnectorError::Internal("Invalid Stripe response format".to_string()))?;

                if data.is_empty() { break; }

                if let Some(last_obj) = data.last() {
                    starting_after = last_obj.get("id").and_then(|v| v.as_str()).map(|s| s.to_string());
                }

                for obj in data {
                    Self::append_stripe_object(obj, &schema, &mut builders);
                }
                total_rows += data.len();

                if builders.len() >= BATCH_THRESHOLD {
                    let batch = builders.finish(arrow_schema.clone())?;
                    batches.push(batch);
                    builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
                }

                let has_more = response["has_more"].as_bool().unwrap_or(false);
                if !has_more { break; }

                if total_rows >= MAX_TOTAL_ROWS {
                    tracing::warn!(table = table, count = total_rows, "Stripe sync reached safety limit");
                    break;
                }
            }

            if builders.len() > 0 {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
            }

            if batches.is_empty() {
                batches.push(RecordBatch::new_empty(arrow_schema));
            }

            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        // Make a simple API call to validate the key
        self.stripe_request("balance").await?;
        Ok(())
    }
}

impl StripeConnector {
    /// Push a single Stripe object directly into columnar builders.
    /// Stripe timestamps are Unix epoch seconds; dates are epoch seconds
    /// converted to days.
    fn append_stripe_object(
        obj: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            let val = obj.get(&col.name);
            match col.data_type {
                ColumnType::Timestamp => {
                    builders.builder(i).append_timestamp(
                        val.and_then(|v| v.as_i64()).map(|ts| ts * 1_000_000),
                    );
                }
                ColumnType::Date => {
                    const SECONDS_PER_DAY: i64 = 86400;
                    builders.builder(i).append_date32(
                        val.and_then(|v| v.as_i64()).map(|ts| (ts / SECONDS_PER_DAY) as i32),
                    );
                }
                _ => {
                    builders.builder(i).append_json_value(val);
                }
            }
        }
        builders.row_complete();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{bearer_token, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Create a test config pointing to mock server.
    fn test_config(api_key: &str) -> StripeConfig {
        StripeConfig::new(api_key)
    }

    /// Create a connector with a custom client pointing to mock server.
    fn test_connector_with_base_url(config: StripeConfig, base_url: String) -> TestableStripeConnector {
        TestableStripeConnector {
            config,
            client: reqwest::Client::new(),
            base_url,
        }
    }

    /// Testable version of StripeConnector that allows setting base URL.
    struct TestableStripeConnector {
        config: StripeConfig,
        client: reqwest::Client,
        base_url: String,
    }

    impl TestableStripeConnector {
        async fn stripe_request(&self, endpoint: &str) -> ConnectorResult<serde_json::Value> {
            let url = format!("{}/{}", self.base_url, endpoint);

            let mut request = self.client.get(&url).bearer_auth(self.config.api_key.expose());

            if let Some(account_id) = &self.config.account_id {
                request = request.header("Stripe-Account", account_id);
            }

            let response = request.send().await.map_err(|e| {
                ConnectorError::Network(format!("Failed to connect to Stripe API: {}", e))
            })?;

            if response.status() == 401 {
                return Err(ConnectorError::Authentication(
                    "Invalid Stripe API key".to_string(),
                ));
            }

            if response.status() == 429 {
                let retry_after = response
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60);
                return Err(ConnectorError::RateLimited {
                    retry_after_secs: retry_after,
                });
            }

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "Stripe API error ({}): {}",
                    status, body
                )));
            }

            response.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Stripe response: {}", e))
            })
        }
    }

    #[test]
    fn test_stripe_config_creation() {
        let config = StripeConfig::new("sk_test_123");
        assert_eq!(config.api_key.expose(), "sk_test_123");
        assert!(config.account_id.is_none());
    }

    #[test]
    fn test_stripe_config_with_account_id() {
        let config = StripeConfig::new("sk_test_123").with_account_id("acct_123");
        assert_eq!(config.api_key.expose(), "sk_test_123");
        assert_eq!(config.account_id, Some("acct_123".to_string()));
    }

    #[test]
    fn test_stripe_config_debug_redacts_api_key() {
        let config = StripeConfig::new("sk_test_secret_key");
        let debug_output = format!("{:?}", config);
        assert!(!debug_output.contains("sk_test_secret_key"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn test_get_table_schema_customers() {
        let schema = StripeConnector::get_table_schema("customers");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        
        let column_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(column_names.contains(&"id"));
        assert!(column_names.contains(&"email"));
        assert!(column_names.contains(&"name"));
        assert!(column_names.contains(&"created"));
    }

    #[test]
    fn test_get_table_schema_charges() {
        let schema = StripeConnector::get_table_schema("charges");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        
        let column_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(column_names.contains(&"id"));
        assert!(column_names.contains(&"amount"));
        assert!(column_names.contains(&"currency"));
        assert!(column_names.contains(&"status"));
    }

    #[test]
    fn test_get_table_schema_unknown() {
        let schema = StripeConnector::get_table_schema("unknown_table");
        assert!(schema.is_none());
    }

    #[test]
    fn test_available_tables() {
        let tables = StripeConnector::TABLES;
        assert!(tables.contains(&"customers"));
        assert!(tables.contains(&"charges"));
        assert!(tables.contains(&"invoices"));
        assert!(tables.contains(&"subscriptions"));
        assert!(tables.contains(&"products"));
        assert!(tables.contains(&"prices"));
    }

    #[test]
    fn test_to_arrow_schema() {
        let table_schema = StripeConnector::get_table_schema("customers").unwrap();
        let arrow_schema = StripeConnector::to_arrow_schema(&table_schema);
        
        assert_eq!(arrow_schema.fields().len(), table_schema.columns.len());
        
        // Check first field
        let id_field = arrow_schema.field_with_name("id");
        assert!(id_field.is_ok());
    }

    #[tokio::test]
    async fn test_list_tables() {
        let config = test_config("sk_test_123");
        let connector = StripeConnector::new(config);
        
        let tables = connector.list_tables().await.unwrap();
        assert!(!tables.is_empty());
        
        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"customers"));
        assert!(table_names.contains(&"charges"));
    }

    #[tokio::test]
    async fn test_get_schema() {
        let config = test_config("sk_test_123");
        let connector = StripeConnector::new(config);
        
        let schema = connector.get_schema("customers").await.unwrap();
        assert!(!schema.columns.is_empty());
    }

    #[tokio::test]
    async fn test_get_schema_not_found() {
        let config = test_config("sk_test_123");
        let connector = StripeConnector::new(config);
        
        let result = connector.get_schema("nonexistent").await;
        assert!(matches!(result, Err(ConnectorError::TableNotFound(_))));
    }

    #[tokio::test]
    async fn test_stripe_request_authentication_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/balance"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&mock_server)
            .await;

        let config = test_config("invalid_key");
        let connector = test_connector_with_base_url(config, mock_server.uri());

        let result = connector.stripe_request("balance").await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_stripe_request_rate_limited() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("Retry-After", "120"),
            )
            .mount(&mock_server)
            .await;

        let config = test_config("sk_test_123");
        let connector = test_connector_with_base_url(config, mock_server.uri());

        let result = connector.stripe_request("customers").await;
        match result {
            Err(ConnectorError::RateLimited { retry_after_secs }) => {
                assert_eq!(retry_after_secs, 120);
            }
            _ => panic!("Expected RateLimited error"),
        }
    }

    #[tokio::test]
    async fn test_stripe_request_success() {
        let mock_server = MockServer::start().await;

        let response_body = serde_json::json!({
            "object": "balance",
            "available": [{"amount": 1000, "currency": "usd"}]
        });

        Mock::given(method("GET"))
            .and(path("/balance"))
            .and(bearer_token("sk_test_123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .mount(&mock_server)
            .await;

        let config = test_config("sk_test_123");
        let connector = test_connector_with_base_url(config, mock_server.uri());

        let result = connector.stripe_request("balance").await;
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data["object"], "balance");
    }

    #[tokio::test]
    async fn test_stripe_request_with_account_header() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/balance"))
            .and(header("Stripe-Account", "acct_123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&mock_server)
            .await;

        let config = StripeConfig::new("sk_test_123").with_account_id("acct_123");
        let connector = test_connector_with_base_url(config, mock_server.uri());

        let result = connector.stripe_request("balance").await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_builders_customers_batch() {
        let schema = StripeConnector::get_table_schema("customers").unwrap();
        let arrow_schema = Arc::new(StripeConnector::to_arrow_schema(&schema));

        let objects = vec![
            serde_json::json!({
                "id": "cus_123",
                "email": "test@example.com",
                "name": "Test User",
                "created": 1704067200,
                "currency": "usd",
                "delinquent": false,
                "metadata": {}
            }),
            serde_json::json!({
                "id": "cus_456",
                "email": null,
                "name": "Another User",
                "created": 1704153600,
                "currency": null,
                "delinquent": true,
                "metadata": {"key": "value"}
            }),
        ];

        let mut builders = ColumnBuilders::new(&schema, 4);
        for obj in &objects {
            StripeConnector::append_stripe_object(obj, &schema, &mut builders);
        }
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 7);
    }

    #[test]
    fn test_source_type() {
        let config = test_config("sk_test_123");
        let connector = StripeConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::Stripe);
    }

    #[tokio::test]
    async fn test_list_tables_has_primary_key() {
        let config = test_config("sk_test_123");
        let connector = StripeConnector::new(config);
        let tables = connector.list_tables().await.unwrap();
        for table in &tables {
            assert_eq!(
                table.primary_key_columns,
                vec!["id".to_string()],
                "Stripe table '{}' must declare 'id' as primary key to enable dedup",
                table.name
            );
        }
    }

    #[test]
    fn test_timestamp_to_epoch_handles_iso8601() {
        let config = test_config("sk_test_123");
        let connector = StripeConnector::new(config);

        let iso = "2024-06-15T10:30:00.000000Z";
        let result = connector.timestamp_to_epoch(iso);
        assert!(
            result.is_some(),
            "timestamp_to_epoch must parse ISO-8601 strings from extract_last_incremental_value"
        );
        let epoch = result.unwrap();
        assert_eq!(epoch, 1718447400, "2024-06-15T10:30:00Z == 1718447400 epoch");
    }

    #[test]
    fn test_timestamp_to_epoch_handles_raw_epoch() {
        let config = test_config("sk_test_123");
        let connector = StripeConnector::new(config);

        let raw = "1718447400";
        let result = connector.timestamp_to_epoch(raw);
        assert_eq!(result, Some(1718447400));
    }

    #[test]
    fn test_in_predicate_multi_value_not_pushed_down() {
        use crate::warehouse::query::predicate_pushdown::Predicate;
        use compact_str::CompactString;

        let config = test_config("sk_test_123");
        let connector = StripeConnector::new(config);

        let predicates = vec![
            Predicate::In {
                column: CompactString::from("status"),
                values: vec![CompactString::from("succeeded"), CompactString::from("failed")],
            },
        ];

        let params = connector.predicates_to_api_params(&predicates, "charges");
        assert!(
            params.is_empty(),
            "IN predicate with multiple values must NOT be pushed down (would silently drop values); got: {:?}",
            params
        );
    }

    #[test]
    fn test_in_predicate_single_value_pushed_down() {
        use crate::warehouse::query::predicate_pushdown::Predicate;
        use compact_str::CompactString;

        let config = test_config("sk_test_123");
        let connector = StripeConnector::new(config);

        let predicates = vec![
            Predicate::In {
                column: CompactString::from("status"),
                values: vec![CompactString::from("succeeded")],
            },
        ];

        let params = connector.predicates_to_api_params(&predicates, "charges");
        assert_eq!(
            params.get("status"),
            Some(&"succeeded".to_string()),
            "IN predicate with a single value should be pushed down"
        );
    }
}
