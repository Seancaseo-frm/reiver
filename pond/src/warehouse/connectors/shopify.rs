//! Shopify connector for the data warehouse.
//!
//! Syncs Shopify e-commerce data (products, orders, customers, etc.) to the warehouse.
//! Uses the `shopify_api` crate with API access token authentication and GraphQL
//! bulk operations for efficient data sync.

use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::query::predicate_pushdown::Predicate;
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use super::builders::ColumnBuilders;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use crate::crypto::SecretString;
use shopify_api::Shopify;
use shopify_api::utils::ReadJsonTreeSteps;
use std::pin::Pin;
use std::sync::Arc;

const DEFAULT_API_VERSION: &str = "2025-01";
const MAX_TOTAL_ROWS: usize = 1_000_000;
const BATCH_THRESHOLD: usize = 1_000;
const BULK_POLL_MAX_DURATION_SECS: u64 = 1800;

const TABLES: &[&str] = &[
    "products",
    "orders",
    "customers",
    "collections",
    "inventory_items",
];

/// Shopify connector configuration.
#[derive(Clone)]
pub struct ShopifyConfig {
    pub shop_name: String,
    pub api_key: SecretString,
    pub api_version: String,
}

impl std::fmt::Debug for ShopifyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShopifyConfig")
            .field("shop_name", &self.shop_name)
            .field("api_key", &"***REDACTED***")
            .field("api_version", &self.api_version)
            .finish()
    }
}

impl ShopifyConfig {
    pub fn new(shop_name: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            shop_name: shop_name.into(),
            api_key: SecretString::new(api_key.into()),
            api_version: DEFAULT_API_VERSION.to_string(),
        }
    }

    pub fn with_api_version(mut self, version: impl Into<String>) -> Self {
        self.api_version = version.into();
        self
    }
}

/// Shopify e-commerce data source connector.
pub struct ShopifyConnector {
    #[allow(dead_code)]
    config: ShopifyConfig,
    client: Shopify,
}

impl ShopifyConnector {
    pub fn new(config: ShopifyConfig) -> Self {
        let client = Shopify::new(
            &config.shop_name,
            config.api_key.expose(),
            config.api_version.clone(),
            None,
        );
        Self { config, client }
    }

    fn get_table_schema(table: &str) -> Option<TableSchema> {
        let columns = match table {
            "products" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Shopify product GID"),
                ColumnSchema::new("title", ColumnType::String, true)
                    .with_description("Product title"),
                ColumnSchema::new("body_html", ColumnType::String, true)
                    .with_description("Product description in HTML"),
                ColumnSchema::new("vendor", ColumnType::String, true)
                    .with_description("Product vendor"),
                ColumnSchema::new("product_type", ColumnType::String, true)
                    .with_description("Product type classification"),
                ColumnSchema::new("handle", ColumnType::String, true)
                    .with_description("URL-friendly product handle"),
                ColumnSchema::new("status", ColumnType::String, true)
                    .with_description("Product status (active, archived, draft)"),
                ColumnSchema::new("tags", ColumnType::String, true)
                    .with_description("Comma-separated product tags"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true)
                    .with_description("Product creation timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true)
                    .with_description("Product last update timestamp")
                    .with_timezone("UTC"),
            ],
            "orders" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Shopify order GID"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Order name/number (e.g. #1001)"),
                ColumnSchema::new("email", ColumnType::String, true)
                    .with_description("Customer email for this order"),
                ColumnSchema::new("financial_status", ColumnType::String, true)
                    .with_description("Payment status (paid, pending, refunded, etc.)"),
                ColumnSchema::new("fulfillment_status", ColumnType::String, true)
                    .with_description("Fulfillment status (fulfilled, partial, null)"),
                ColumnSchema::new("total_price", ColumnType::Float64, true)
                    .with_description("Total order price"),
                ColumnSchema::new("subtotal_price", ColumnType::Float64, true)
                    .with_description("Subtotal before tax and shipping"),
                ColumnSchema::new("total_tax", ColumnType::Float64, true)
                    .with_description("Total tax amount"),
                ColumnSchema::new("total_discounts", ColumnType::Float64, true)
                    .with_description("Total discount amount"),
                ColumnSchema::new("currency", ColumnType::String, true)
                    .with_description("Order currency code (e.g. USD)"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true)
                    .with_description("Order creation timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true)
                    .with_description("Order last update timestamp")
                    .with_timezone("UTC"),
            ],
            "customers" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Shopify customer GID"),
                ColumnSchema::new("email", ColumnType::String, true)
                    .with_description("Customer email"),
                ColumnSchema::new("first_name", ColumnType::String, true)
                    .with_description("Customer first name"),
                ColumnSchema::new("last_name", ColumnType::String, true)
                    .with_description("Customer last name"),
                ColumnSchema::new("phone", ColumnType::String, true)
                    .with_description("Customer phone number"),
                ColumnSchema::new("orders_count", ColumnType::Int64, true)
                    .with_description("Number of orders placed"),
                ColumnSchema::new("total_spent", ColumnType::Float64, true)
                    .with_description("Total amount spent"),
                ColumnSchema::new("state", ColumnType::String, true)
                    .with_description("Customer account state (enabled, disabled, invited, declined)"),
                ColumnSchema::new("tags", ColumnType::String, true)
                    .with_description("Comma-separated customer tags"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true)
                    .with_description("Customer creation timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true)
                    .with_description("Customer last update timestamp")
                    .with_timezone("UTC"),
            ],
            "collections" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Shopify collection GID"),
                ColumnSchema::new("title", ColumnType::String, true)
                    .with_description("Collection title"),
                ColumnSchema::new("handle", ColumnType::String, true)
                    .with_description("URL-friendly collection handle"),
                ColumnSchema::new("body_html", ColumnType::String, true)
                    .with_description("Collection description in HTML"),
                ColumnSchema::new("sort_order", ColumnType::String, true)
                    .with_description("Product sort order in collection"),
                ColumnSchema::new("published_at", ColumnType::Timestamp, true)
                    .with_description("Collection publish timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true)
                    .with_description("Collection last update timestamp")
                    .with_timezone("UTC"),
            ],
            "inventory_items" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Shopify inventory item GID"),
                ColumnSchema::new("sku", ColumnType::String, true)
                    .with_description("Stock keeping unit"),
                ColumnSchema::new("cost", ColumnType::Float64, true)
                    .with_description("Unit cost of the inventory item"),
                ColumnSchema::new("tracked", ColumnType::Boolean, true)
                    .with_description("Whether inventory is tracked"),
                ColumnSchema::new("created_at", ColumnType::Timestamp, true)
                    .with_description("Inventory item creation timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, true)
                    .with_description("Inventory item last update timestamp")
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

    /// Map our snake_case column names to Shopify GraphQL camelCase field names.
    fn graphql_field_name(column: &str) -> &str {
        match column {
            "body_html" => "descriptionHtml",
            "product_type" => "productType",
            "created_at" => "createdAt",
            "updated_at" => "updatedAt",
            "financial_status" => "displayFinancialStatus",
            "fulfillment_status" => "displayFulfillmentStatus",
            "total_price" => "totalPriceSet",
            "subtotal_price" => "subtotalPriceSet",
            "total_tax" => "totalTaxSet",
            "total_discounts" => "totalDiscountsSet",
            "first_name" => "firstName",
            "last_name" => "lastName",
            "orders_count" => "numberOfOrders",
            "total_spent" => "amountSpent",
            "sort_order" => "sortOrder",
            "published_at" => "publishedAt",
            "order_number" => "name",
            "currency" => "currencyCode",
            "cost" => "unitCost",
            other => other,
        }
    }

    /// Map Shopify GraphQL camelCase field names back to our snake_case names.
    fn column_name_from_graphql(gql_field: &str) -> &str {
        match gql_field {
            "descriptionHtml" => "body_html",
            "productType" => "product_type",
            "createdAt" => "created_at",
            "updatedAt" => "updated_at",
            "displayFinancialStatus" => "financial_status",
            "displayFulfillmentStatus" => "fulfillment_status",
            "totalPriceSet" => "total_price",
            "subtotalPriceSet" => "subtotal_price",
            "totalTaxSet" => "total_tax",
            "totalDiscountsSet" => "total_discounts",
            "firstName" => "first_name",
            "lastName" => "last_name",
            "numberOfOrders" => "orders_count",
            "amountSpent" => "total_spent",
            "sortOrder" => "sort_order",
            "publishedAt" => "published_at",
            "currencyCode" => "currency",
            "unitCost" => "cost",
            other => other,
        }
    }

    /// Get the default GraphQL fields for a table.
    fn graphql_fields_for(table: &str) -> Vec<&'static str> {
        match table {
            "products" => vec![
                "id", "title", "descriptionHtml", "vendor", "productType",
                "handle", "status", "tags", "createdAt", "updatedAt",
            ],
            "orders" => vec![
                "id", "name", "email",
                "displayFinancialStatus", "displayFulfillmentStatus",
                "totalPriceSet", "subtotalPriceSet", "totalTaxSet",
                "totalDiscountsSet", "currencyCode",
                "createdAt", "updatedAt",
            ],
            "customers" => vec![
                "id", "email", "firstName", "lastName", "phone",
                "numberOfOrders", "amountSpent",
                "state", "tags", "createdAt", "updatedAt",
            ],
            "collections" => vec![
                "id", "title", "handle", "descriptionHtml",
                "sortOrder", "publishedAt", "updatedAt",
            ],
            "inventory_items" => vec![
                "id", "sku", "unitCost", "tracked",
                "createdAt", "updatedAt",
            ],
            _ => vec![],
        }
    }

    /// Get the GraphQL resource name for bulk queries.
    fn graphql_resource_name(table: &str) -> &str {
        match table {
            "products" => "products",
            "orders" => "orders",
            "customers" => "customers",
            "collections" => "collections",
            "inventory_items" => "inventoryItems",
            _ => table,
        }
    }

    /// Build a GraphQL node body from the list of fields, handling money set
    /// fields and tags (which is an array in GraphQL).
    fn build_graphql_node_body(table: &str, fields: &[&str]) -> String {
        let mut parts = Vec::new();
        for &field in fields {
            match field {
                "totalPriceSet" | "subtotalPriceSet" | "totalTaxSet" | "totalDiscountsSet" => {
                    parts.push(format!("{} {{ shopMoney {{ amount }} }}", field));
                }
                "amountSpent" => {
                    parts.push("amountSpent { amount currencyCode }".to_string());
                }
                "unitCost" => {
                    parts.push("unitCost { amount }".to_string());
                }
                "tags" if table == "products" || table == "customers" => {
                    parts.push("tags".to_string());
                }
                "currencyCode" => {
                    parts.push("currencyCode".to_string());
                }
                _ => {
                    parts.push(field.to_string());
                }
            }
        }
        parts.join("\n            ")
    }

    /// Build a GraphQL bulk query string for a table.
    fn build_bulk_query(
        table: &str,
        fields: &[&str],
        query_filter: Option<&str>,
    ) -> String {
        let resource = Self::graphql_resource_name(table);
        let node_body = Self::build_graphql_node_body(table, fields);

        let filter_arg = match query_filter {
            Some(q) => format!("(query: \"{}\")", q),
            None => String::new(),
        };

        format!(
            r#"{{
  {resource}{filter_arg} {{
    edges {{
      node {{
            {node_body}
      }}
    }}
  }}
}}"#
        )
    }

    /// Build a regular (non-bulk) GraphQL query with `first` limit.
    fn build_limited_query(
        table: &str,
        fields: &[&str],
        query_filter: Option<&str>,
        first: usize,
        after: Option<&str>,
    ) -> String {
        let resource = Self::graphql_resource_name(table);
        let node_body = Self::build_graphql_node_body(table, fields);

        let mut args = vec![format!("first: {}", first)];
        if let Some(q) = query_filter {
            args.push(format!("query: \"{}\"", q));
        }
        if let Some(cursor) = after {
            args.push(format!("after: \"{}\"", cursor));
        }
        let args_str = args.join(", ");

        format!(
            r#"query {{
  {resource}({args_str}) {{
    edges {{
      node {{
            {node_body}
      }}
      cursor
    }}
    pageInfo {{
      hasNextPage
    }}
  }}
}}"#
        )
    }

    /// Apply projection to filter fields and schema.
    fn apply_projection<'a>(
        schema: TableSchema,
        fields: Vec<&'a str>,
        projection: &[String],
    ) -> (TableSchema, Vec<&'a str>) {
        if projection.is_empty() {
            return (schema, fields);
        }

        let filtered_schema = TableSchema {
            columns: schema
                .columns
                .into_iter()
                .filter(|c| c.name == "id" || projection.iter().any(|p| p == &c.name))
                .collect(),
        };

        let filtered_fields: Vec<&str> = fields
            .into_iter()
            .filter(|f| {
                let col = Self::column_name_from_graphql(f);
                col == "id" || projection.iter().any(|p| p.as_str() == col)
            })
            .collect();

        (filtered_schema, filtered_fields)
    }

    /// Build a Shopify search query filter string from predicates.
    fn build_search_filter(predicates: &[Predicate]) -> Option<String> {
        let parts: Vec<String> = predicates
            .iter()
            .filter_map(|p| Self::predicate_to_shopify_filter(p))
            .collect();

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" AND "))
        }
    }

    /// Convert a single predicate to a Shopify search query fragment.
    fn predicate_to_shopify_filter(predicate: &Predicate) -> Option<String> {
        match predicate {
            Predicate::Equals { column, value } => {
                let field = Self::graphql_field_name(column);
                Some(format!("{}:'{}'", field, escape_shopify_search(value)))
            }
            Predicate::GreaterThan { column, value, inclusive } => {
                let field = Self::graphql_field_name(column);
                let op = if *inclusive { ">=" } else { ">" };
                Some(format!("{}:{}'{}'", field, op, escape_shopify_search(value)))
            }
            Predicate::LessThan { column, value, inclusive } => {
                let field = Self::graphql_field_name(column);
                let op = if *inclusive { "<=" } else { "<" };
                Some(format!("{}:{}'{}'", field, op, escape_shopify_search(value)))
            }
            Predicate::Contains { column, substring } => {
                let field = Self::graphql_field_name(column);
                Some(format!("{}:*{}*", field, escape_shopify_search(substring)))
            }
            Predicate::And(inner) => {
                let parts: Vec<String> = inner
                    .iter()
                    .filter_map(|p| Self::predicate_to_shopify_filter(p))
                    .collect();
                if parts.is_empty() {
                    None
                } else {
                    Some(parts.join(" AND "))
                }
            }
            Predicate::Or(inner) => {
                let parts: Vec<String> = inner
                    .iter()
                    .filter_map(|p| Self::predicate_to_shopify_filter(p))
                    .collect();
                if parts.is_empty() {
                    None
                } else {
                    Some(format!("({})", parts.join(" OR ")))
                }
            }
            _ => None,
        }
    }

    /// Push a single Shopify object directly into columnar builders.
    fn append_shopify_object(
        obj: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            if col.name == "id" {
                builders.builder(i).append_string(obj.get("id").and_then(|v| v.as_str()));
                continue;
            }
            let gql_field = Self::graphql_field_name(&col.name);
            let value = Self::extract_field_value(obj, gql_field, &col.name);
            if value.is_null() {
                builders.builder(i).append_null();
                continue;
            }
            match col.data_type {
                ColumnType::Timestamp => {
                    builders.builder(i).append_timestamp(
                        value.as_str().and_then(Self::parse_shopify_timestamp),
                    );
                }
                _ => {
                    builders.builder(i).append_json_value(Some(&value));
                }
            }
        }
        builders.row_complete();
    }

    /// Extract a field value from a GraphQL response, handling money sets and tags.
    fn extract_field_value(
        obj: &serde_json::Value,
        gql_field: &str,
        col_name: &str,
    ) -> serde_json::Value {
        match gql_field {
            "totalPriceSet" | "subtotalPriceSet" | "totalTaxSet" | "totalDiscountsSet" => {
                obj.get(gql_field)
                    .and_then(|v| v.get("shopMoney"))
                    .and_then(|v| v.get("amount"))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            }
            "amountSpent" => {
                obj.get("amountSpent")
                    .and_then(|v| v.get("amount"))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            }
            "unitCost" => {
                obj.get("unitCost")
                    .and_then(|v| v.get("amount"))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            }
            "tags" => {
                match obj.get("tags") {
                    Some(serde_json::Value::Array(arr)) => {
                        let joined: Vec<String> = arr
                            .iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect();
                        serde_json::Value::String(joined.join(", "))
                    }
                    Some(serde_json::Value::String(s)) => {
                        serde_json::Value::String(s.clone())
                    }
                    _ => serde_json::Value::Null,
                }
            }
            "currencyCode" => {
                obj.get("currencyCode")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            }
            _ => {
                if let Some(val) = obj.get(gql_field) {
                    val.clone()
                } else if gql_field != col_name {
                    obj.get(col_name).cloned().unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                }
            }
        }
    }

    /// Parse a Shopify ISO-8601 timestamp to epoch microseconds.
    fn parse_shopify_timestamp(value: &str) -> Option<i64> {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
            return Some(dt.timestamp_micros());
        }
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%SZ") {
            return Some(dt.and_utc().timestamp_micros());
        }
        None
    }

    /// Perform the actual data fetch for a table using the shopify_api crate.
    async fn do_fetch(
        &self,
        table: &str,
        options: FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let schema = Self::get_table_schema(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;

        let default_fields = Self::graphql_fields_for(table);
        if default_fields.is_empty() {
            return Err(ConnectorError::TableNotFound(table.to_string()));
        }

        let (schema, fields) = if let Some(ref proj) = options.projection {
            Self::apply_projection(schema, default_fields, proj)
        } else {
            (schema, default_fields)
        };

        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        // Build the search filter from predicates + incremental key
        let mut filter_parts: Vec<String> = Vec::new();

        if let (Some(ref _key), Some(ref last_value)) =
            (&options.incremental_key, &options.last_value)
        {
            filter_parts.push(format!("updated_at:>'{}'", escape_shopify_search(last_value)));
        }

        if let Some(pred_filter) = Self::build_search_filter(&options.predicates) {
            filter_parts.push(pred_filter);
        }

        let query_filter = if filter_parts.is_empty() {
            None
        } else {
            Some(filter_parts.join(" AND "))
        };

        // If max_rows is set, use paginated GraphQL query instead of bulk
        if let Some(max_rows) = options.max_rows {
            return self
                .fetch_with_pagination(table, &fields, query_filter.as_deref(), max_rows, &schema, arrow_schema)
                .await;
        }

        // Use bulk query for full/incremental sync
        self.fetch_with_bulk(table, &fields, query_filter.as_deref(), &schema, arrow_schema)
            .await
    }

    /// Fetch data using GraphQL bulk operations.
    async fn fetch_with_bulk(
        &self,
        table: &str,
        fields: &[&str],
        query_filter: Option<&str>,
        table_schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let query = Self::build_bulk_query(table, fields, query_filter);

        let bulk_result = self
            .client
            .make_bulk_query(&query)
            .await
            .map_err(|e| ConnectorError::Internal(format!("Bulk query submission failed: {}", e)))?;

        let bulk_op = bulk_result.bulk_operation.as_ref()
            .ok_or_else(|| ConnectorError::Internal("No bulk operation returned".to_string()))?;
        let bulk_id = bulk_op.id.as_ref()
            .ok_or_else(|| ConnectorError::Internal("No bulk operation ID returned".to_string()))?;

        let timeout_duration = std::time::Duration::from_secs(BULK_POLL_MAX_DURATION_SECS);
        let completed = tokio::time::timeout(timeout_duration, self.client.wait_for_bulk(bulk_id))
            .await
            .map_err(|_| {
                ConnectorError::Internal(format!(
                    "Bulk query timed out after {}s",
                    BULK_POLL_MAX_DURATION_SECS
                ))
            })?
            .map_err(|e| ConnectorError::Internal(format!("Bulk query failed: {}", e)))?;

        if completed.error_code.is_some() {
            return Err(ConnectorError::Internal(
                "Bulk query completed with error".to_string(),
            ));
        }

        let download_url = match completed.url {
            Some(ref url) if !url.is_empty() => url.clone(),
            _ => {
                return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
            }
        };

        let objects = Shopify::download_bulk(&download_url)
            .await
            .map_err(|e| ConnectorError::Internal(format!("Bulk download failed: {}", e)))?;

        if objects.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(table_schema, BATCH_THRESHOLD);

        for obj in &objects {
            if obj.get("__parentId").is_some() {
                continue;
            }
            Self::append_shopify_object(obj, table_schema, &mut builders);

            if builders.len() >= BATCH_THRESHOLD {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
                builders = ColumnBuilders::new(table_schema, BATCH_THRESHOLD);
            }

            if batches.iter().map(|b| b.num_rows()).sum::<usize>() + builders.len() >= MAX_TOTAL_ROWS
            {
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

    /// Fetch data using paginated GraphQL queries (when max_rows is set).
    async fn fetch_with_pagination(
        &self,
        table: &str,
        fields: &[&str],
        query_filter: Option<&str>,
        max_rows: usize,
        table_schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let page_size = max_rows.min(250);
        let resource = Self::graphql_resource_name(table);

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(table_schema, BATCH_THRESHOLD);
        let mut total_rows: usize = 0;
        let mut after: Option<String> = None;

        loop {
            let remaining = max_rows - total_rows;
            if remaining == 0 {
                break;
            }
            let fetch_count = remaining.min(page_size);

            let query = Self::build_limited_query(
                table,
                fields,
                query_filter,
                fetch_count,
                after.as_deref(),
            );

            let json_finder = vec![
                ReadJsonTreeSteps::Key("data"),
                ReadJsonTreeSteps::Key(resource),
            ];

            let response: serde_json::Value = self
                .client
                .graphql_query(&query, &serde_json::json!({}), &json_finder)
                .await
                .map_err(|e| {
                    ConnectorError::Internal(format!("GraphQL query failed: {}", e))
                })?;

            let edges = response
                .get("edges")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    ConnectorError::Internal("Missing 'edges' in GraphQL response".to_string())
                })?;

            if edges.is_empty() {
                break;
            }

            for edge in edges {
                if let Some(node) = edge.get("node") {
                    Self::append_shopify_object(node, table_schema, &mut builders);
                    total_rows += 1;
                }
            }

            if builders.len() >= BATCH_THRESHOLD {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
                builders = ColumnBuilders::new(table_schema, BATCH_THRESHOLD);
            }

            let has_next = response
                .pointer("/pageInfo/hasNextPage")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if !has_next || total_rows >= max_rows {
                break;
            }

            after = edges
                .last()
                .and_then(|e| e.get("cursor"))
                .and_then(|v| v.as_str())
                .map(String::from);
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
}

/// Escape special characters for Shopify search query strings.
fn escape_shopify_search(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('"', "\\\"")
}

#[async_trait]
impl Connector for ShopifyConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Shopify
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        Ok(TABLES
            .iter()
            .filter_map(|&table| {
                Self::get_table_schema(table).map(|schema| TableInfo {
                    name: table.to_string(),
                    schema,
                    supports_incremental: true,
                    incremental_key: Some("updated_at".to_string()),
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

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            let batches = self.do_fetch(table, options).await?;
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        let json_finder = vec![
            ReadJsonTreeSteps::Key("data"),
            ReadJsonTreeSteps::Key("shop"),
        ];

        let _: serde_json::Value = self
            .client
            .graphql_query(
                "query { shop { name } }",
                &serde_json::json!({}),
                &json_finder,
            )
            .await
            .map_err(|e| ConnectorError::Authentication(format!("Shopify auth failed: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compact_str::CompactString;

    #[test]
    fn test_shopify_config_creation() {
        let config = ShopifyConfig::new("my-store", "shpat_test123");
        assert_eq!(config.shop_name, "my-store");
        assert_eq!(config.api_version, DEFAULT_API_VERSION);
    }

    #[test]
    fn test_shopify_config_debug_redacts() {
        let config = ShopifyConfig::new("my-store", "shpat_secret_key");
        let debug_output = format!("{:?}", config);
        assert!(!debug_output.contains("shpat_secret_key"));
        assert!(debug_output.contains("REDACTED"));
        assert!(debug_output.contains("my-store"));
    }

    #[test]
    fn test_shopify_config_with_api_version() {
        let config = ShopifyConfig::new("my-store", "key")
            .with_api_version("2024-04");
        assert_eq!(config.api_version, "2024-04");
    }

    #[test]
    fn test_get_table_schema_products() {
        let schema = ShopifyConnector::get_table_schema("products");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.columns.len(), 10);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"title"));
        assert!(names.contains(&"vendor"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"created_at"));
        assert!(names.contains(&"updated_at"));
    }

    #[test]
    fn test_get_table_schema_orders() {
        let schema = ShopifyConnector::get_table_schema("orders");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.columns.len(), 12);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"total_price"));
        assert!(names.contains(&"financial_status"));
        assert!(names.contains(&"currency"));
    }

    #[test]
    fn test_get_table_schema_customers() {
        let schema = ShopifyConnector::get_table_schema("customers");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.columns.len(), 11);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"orders_count"));
        assert!(names.contains(&"total_spent"));
        assert!(names.contains(&"state"));
    }

    #[test]
    fn test_get_table_schema_collections() {
        let schema = ShopifyConnector::get_table_schema("collections");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.columns.len(), 7);
    }

    #[test]
    fn test_get_table_schema_inventory_items() {
        let schema = ShopifyConnector::get_table_schema("inventory_items");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.columns.len(), 6);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"sku"));
        assert!(names.contains(&"cost"));
        assert!(names.contains(&"tracked"));
    }

    #[test]
    fn test_get_table_schema_unknown() {
        assert!(ShopifyConnector::get_table_schema("nonexistent").is_none());
    }

    #[test]
    fn test_graphql_field_name_mapping() {
        assert_eq!(ShopifyConnector::graphql_field_name("created_at"), "createdAt");
        assert_eq!(ShopifyConnector::graphql_field_name("updated_at"), "updatedAt");
        assert_eq!(ShopifyConnector::graphql_field_name("product_type"), "productType");
        assert_eq!(ShopifyConnector::graphql_field_name("body_html"), "descriptionHtml");
        assert_eq!(ShopifyConnector::graphql_field_name("total_price"), "totalPriceSet");
        assert_eq!(ShopifyConnector::graphql_field_name("first_name"), "firstName");
        assert_eq!(ShopifyConnector::graphql_field_name("title"), "title");
    }

    #[test]
    fn test_column_name_from_graphql() {
        assert_eq!(ShopifyConnector::column_name_from_graphql("createdAt"), "created_at");
        assert_eq!(ShopifyConnector::column_name_from_graphql("totalPriceSet"), "total_price");
        assert_eq!(ShopifyConnector::column_name_from_graphql("firstName"), "first_name");
        assert_eq!(ShopifyConnector::column_name_from_graphql("title"), "title");
    }

    #[test]
    fn test_build_bulk_query_products() {
        let fields = ShopifyConnector::graphql_fields_for("products");
        let query = ShopifyConnector::build_bulk_query("products", &fields, None);
        assert!(query.contains("products"));
        assert!(query.contains("title"));
        assert!(query.contains("createdAt"));
        assert!(query.contains("edges"));
        assert!(query.contains("node"));
    }

    #[test]
    fn test_build_bulk_query_with_filter() {
        let fields = ShopifyConnector::graphql_fields_for("products");
        let query =
            ShopifyConnector::build_bulk_query("products", &fields, Some("updated_at:>'2024-01-01'"));
        assert!(query.contains("query: \"updated_at:>'2024-01-01'\""));
    }

    #[test]
    fn test_build_bulk_query_orders_money_fields() {
        let fields = ShopifyConnector::graphql_fields_for("orders");
        let query = ShopifyConnector::build_bulk_query("orders", &fields, None);
        assert!(query.contains("totalPriceSet { shopMoney { amount } }"));
        assert!(query.contains("subtotalPriceSet { shopMoney { amount } }"));
        assert!(query.contains("currencyCode"));
    }

    #[test]
    fn test_build_limited_query() {
        let fields = ShopifyConnector::graphql_fields_for("products");
        let query = ShopifyConnector::build_limited_query("products", &fields, None, 10, None);
        assert!(query.contains("first: 10"));
        assert!(query.contains("pageInfo"));
        assert!(query.contains("hasNextPage"));
        assert!(query.contains("cursor"));
    }

    #[test]
    fn test_build_limited_query_with_after() {
        let fields = ShopifyConnector::graphql_fields_for("products");
        let query = ShopifyConnector::build_limited_query(
            "products",
            &fields,
            None,
            10,
            Some("abc123"),
        );
        assert!(query.contains("after: \"abc123\""));
    }

    #[test]
    fn test_predicate_equals() {
        let pred = Predicate::Equals {
            column: CompactString::from("status"),
            value: CompactString::from("active"),
        };
        let result = ShopifyConnector::predicate_to_shopify_filter(&pred);
        assert_eq!(result, Some("status:'active'".to_string()));
    }

    #[test]
    fn test_predicate_greater_than() {
        let pred = Predicate::GreaterThan {
            column: CompactString::from("created_at"),
            value: CompactString::from("2024-01-01"),
            inclusive: false,
        };
        let result = ShopifyConnector::predicate_to_shopify_filter(&pred);
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.contains("createdAt"));
        assert!(r.contains(">"));
    }

    #[test]
    fn test_predicate_contains() {
        let pred = Predicate::Contains {
            column: CompactString::from("title"),
            substring: CompactString::from("shoes"),
        };
        let result = ShopifyConnector::predicate_to_shopify_filter(&pred);
        assert_eq!(result, Some("title:*shoes*".to_string()));
    }

    #[test]
    fn test_predicate_and() {
        let pred = Predicate::And(vec![
            Predicate::Equals {
                column: CompactString::from("status"),
                value: CompactString::from("active"),
            },
            Predicate::Contains {
                column: CompactString::from("title"),
                substring: CompactString::from("shirt"),
            },
        ]);
        let result = ShopifyConnector::predicate_to_shopify_filter(&pred);
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.contains("AND"));
        assert!(r.contains("status:'active'"));
        assert!(r.contains("title:*shirt*"));
    }

    #[test]
    fn test_predicate_unsupported_returns_none() {
        let pred = Predicate::IsNull {
            column: CompactString::from("email"),
            is_null: true,
        };
        assert!(ShopifyConnector::predicate_to_shopify_filter(&pred).is_none());
    }

    #[test]
    fn test_escape_shopify_search() {
        assert_eq!(escape_shopify_search("hello"), "hello");
        assert_eq!(escape_shopify_search("it's"), "it\\'s");
        assert_eq!(escape_shopify_search("a\\b"), "a\\\\b");
    }

    #[test]
    fn test_apply_projection() {
        let schema = ShopifyConnector::get_table_schema("products").unwrap();
        let fields = ShopifyConnector::graphql_fields_for("products");
        let projection = vec!["title".to_string(), "vendor".to_string(), "updated_at".to_string()];

        let (proj_schema, proj_fields) =
            ShopifyConnector::apply_projection(schema, fields, &projection);

        let col_names: Vec<&str> = proj_schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"title"));
        assert!(col_names.contains(&"vendor"));
        assert!(col_names.contains(&"updated_at"));
        assert!(!col_names.contains(&"status"));
        assert!(!col_names.contains(&"body_html"));

        assert!(proj_fields.contains(&"id"));
        assert!(proj_fields.contains(&"title"));
        assert!(proj_fields.contains(&"vendor"));
        assert!(proj_fields.contains(&"updatedAt"));
        assert!(!proj_fields.contains(&"status"));
    }

    #[test]
    fn test_apply_projection_empty() {
        let schema = ShopifyConnector::get_table_schema("products").unwrap();
        let fields = ShopifyConnector::graphql_fields_for("products");
        let (proj_schema, proj_fields) =
            ShopifyConnector::apply_projection(schema.clone(), fields.clone(), &[]);
        assert_eq!(proj_schema.columns.len(), schema.columns.len());
        assert_eq!(proj_fields.len(), fields.len());
    }

    #[test]
    fn test_append_shopify_object_product() {
        let schema = ShopifyConnector::get_table_schema("products").unwrap();
        let arrow_schema = Arc::new(ShopifyConnector::to_arrow_schema(&schema));
        let obj = serde_json::json!({
            "id": "gid://shopify/Product/123",
            "title": "Test Product",
            "descriptionHtml": "<p>Desc</p>",
            "vendor": "TestVendor",
            "productType": "Shoes",
            "handle": "test-product",
            "status": "ACTIVE",
            "tags": ["sale", "new"],
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-06-01T00:00:00Z"
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        ShopifyConnector::append_shopify_object(&obj, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let id_col = batch.column_by_name("id").unwrap().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(id_col.value(0), "gid://shopify/Product/123");
        let title_col = batch.column_by_name("title").unwrap().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(title_col.value(0), "Test Product");
        let tags_col = batch.column_by_name("tags").unwrap().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(tags_col.value(0), "sale, new");
    }

    #[test]
    fn test_append_shopify_object_order_money_fields() {
        let schema = ShopifyConnector::get_table_schema("orders").unwrap();
        let arrow_schema = Arc::new(ShopifyConnector::to_arrow_schema(&schema));
        let obj = serde_json::json!({
            "id": "gid://shopify/Order/456",
            "name": "#1001",
            "email": "test@example.com",
            "displayFinancialStatus": "PAID",
            "displayFulfillmentStatus": "FULFILLED",
            "totalPriceSet": { "shopMoney": { "amount": "99.99" } },
            "subtotalPriceSet": { "shopMoney": { "amount": "89.99" } },
            "totalTaxSet": { "shopMoney": { "amount": "10.00" } },
            "totalDiscountsSet": { "shopMoney": { "amount": "0.00" } },
            "currencyCode": "USD",
            "createdAt": "2024-01-15T10:30:00Z",
            "updatedAt": "2024-01-15T12:00:00Z"
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        ShopifyConnector::append_shopify_object(&obj, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();

        let total_price = batch.column_by_name("total_price").unwrap().as_any()
            .downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert!((total_price.value(0) - 99.99).abs() < f64::EPSILON);
        let currency = batch.column_by_name("currency").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(currency.value(0), "USD");
    }

    #[test]
    fn test_append_shopify_object_customer() {
        let schema = ShopifyConnector::get_table_schema("customers").unwrap();
        let arrow_schema = Arc::new(ShopifyConnector::to_arrow_schema(&schema));
        let obj = serde_json::json!({
            "id": "gid://shopify/Customer/789",
            "email": "jane@example.com",
            "firstName": "Jane",
            "lastName": "Doe",
            "phone": "+15551234",
            "numberOfOrders": "5",
            "amountSpent": { "amount": "500.00", "currencyCode": "USD" },
            "state": "ENABLED",
            "tags": ["vip"],
            "createdAt": "2023-06-01T00:00:00Z",
            "updatedAt": "2024-03-15T00:00:00Z"
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        ShopifyConnector::append_shopify_object(&obj, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();

        let first_name = batch.column_by_name("first_name").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(first_name.value(0), "Jane");
        let tags = batch.column_by_name("tags").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(tags.value(0), "vip");
    }

    #[test]
    fn test_parse_shopify_timestamp() {
        let micros = ShopifyConnector::parse_shopify_timestamp("2024-01-01T00:00:00Z");
        assert!(micros.is_some());
        assert_eq!(micros.unwrap(), 1704067200000000);
    }

    #[test]
    fn test_parse_shopify_timestamp_with_offset() {
        let micros = ShopifyConnector::parse_shopify_timestamp("2024-01-01T00:00:00+00:00");
        assert!(micros.is_some());
        assert_eq!(micros.unwrap(), 1704067200000000);
    }

    #[test]
    fn test_parse_shopify_timestamp_invalid() {
        assert!(ShopifyConnector::parse_shopify_timestamp("not-a-date").is_none());
    }

    #[test]
    fn test_builders_products_batch() {
        let schema = ShopifyConnector::get_table_schema("products").unwrap();
        let arrow_schema = Arc::new(ShopifyConnector::to_arrow_schema(&schema));

        let objects = vec![
            serde_json::json!({
                "id": "gid://shopify/Product/1",
                "title": "T-Shirt",
                "descriptionHtml": "<p>Nice shirt</p>",
                "vendor": "Acme",
                "productType": "Apparel",
                "handle": "t-shirt",
                "status": "ACTIVE",
                "tags": ["sale", "summer"],
                "createdAt": "2024-01-01T00:00:00Z",
                "updatedAt": "2024-06-01T00:00:00Z"
            }),
            serde_json::json!({
                "id": "gid://shopify/Product/2",
                "title": "Jeans",
                "descriptionHtml": null,
                "vendor": "Acme",
                "productType": "Apparel",
                "handle": "jeans",
                "status": "DRAFT",
                "tags": [],
                "createdAt": "2024-02-01T00:00:00Z",
                "updatedAt": "2024-07-01T00:00:00Z"
            }),
        ];

        let mut builders = ColumnBuilders::new(&schema, 4);
        for obj in &objects {
            ShopifyConnector::append_shopify_object(obj, &schema, &mut builders);
        }
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 10);
    }

    #[test]
    fn test_builders_inventory_items_boolean() {
        let schema = ShopifyConnector::get_table_schema("inventory_items").unwrap();
        let arrow_schema = Arc::new(ShopifyConnector::to_arrow_schema(&schema));

        let obj = serde_json::json!({
            "id": "gid://shopify/InventoryItem/1",
            "sku": "SKU-001",
            "unitCost": { "amount": "12.50" },
            "tracked": true,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        ShopifyConnector::append_shopify_object(&obj, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let tracked = batch.column_by_name("tracked").unwrap().as_any()
            .downcast_ref::<arrow::array::BooleanArray>().unwrap();
        assert!(tracked.value(0));
    }

    #[tokio::test]
    async fn test_list_tables() {
        let config = ShopifyConfig::new("test-store", "test-key");
        let connector = ShopifyConnector::new(config);
        let tables = connector.list_tables().await.unwrap();

        assert_eq!(tables.len(), 5);
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"products"));
        assert!(names.contains(&"orders"));
        assert!(names.contains(&"customers"));
        assert!(names.contains(&"collections"));
        assert!(names.contains(&"inventory_items"));

        for t in &tables {
            assert!(t.supports_incremental);
            assert_eq!(t.incremental_key.as_deref(), Some("updated_at"));
            assert_eq!(t.primary_key_columns, vec!["id".to_string()]);
        }
    }

    #[tokio::test]
    async fn test_get_schema() {
        let config = ShopifyConfig::new("test-store", "test-key");
        let connector = ShopifyConnector::new(config);
        let schema = connector.get_schema("products").await.unwrap();
        assert_eq!(schema.columns.len(), 10);
    }

    #[tokio::test]
    async fn test_get_schema_not_found() {
        let config = ShopifyConfig::new("test-store", "test-key");
        let connector = ShopifyConnector::new(config);
        let result = connector.get_schema("nonexistent").await;
        assert!(matches!(result, Err(ConnectorError::TableNotFound(_))));
    }

    #[test]
    fn test_source_type() {
        let config = ShopifyConfig::new("test-store", "test-key");
        let connector = ShopifyConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::Shopify);
    }

    #[test]
    fn test_graphql_resource_name() {
        assert_eq!(ShopifyConnector::graphql_resource_name("products"), "products");
        assert_eq!(ShopifyConnector::graphql_resource_name("orders"), "orders");
        assert_eq!(
            ShopifyConnector::graphql_resource_name("inventory_items"),
            "inventoryItems"
        );
    }

    #[test]
    fn test_build_search_filter_empty() {
        assert!(ShopifyConnector::build_search_filter(&[]).is_none());
    }

    #[test]
    fn test_graphql_field_name_cost_mapping() {
        assert_eq!(ShopifyConnector::graphql_field_name("cost"), "unitCost");
    }

    #[test]
    fn test_column_name_from_graphql_unitcost_mapping() {
        assert_eq!(ShopifyConnector::column_name_from_graphql("unitCost"), "cost");
    }

    #[test]
    fn test_predicate_greater_than_no_trailing_space() {
        let pred = Predicate::GreaterThan {
            column: CompactString::from("created_at"),
            value: CompactString::from("2024-01-01"),
            inclusive: false,
        };
        let result = ShopifyConnector::predicate_to_shopify_filter(&pred).unwrap();
        assert_eq!(result, "createdAt:>'2024-01-01'");
        assert!(!result.ends_with(' '));
    }

    #[test]
    fn test_predicate_less_than_no_trailing_space() {
        let pred = Predicate::LessThan {
            column: CompactString::from("updated_at"),
            value: CompactString::from("2024-12-31"),
            inclusive: true,
        };
        let result = ShopifyConnector::predicate_to_shopify_filter(&pred).unwrap();
        assert_eq!(result, "updatedAt:<='2024-12-31'");
        assert!(!result.ends_with(' '));
    }

    #[test]
    fn test_escape_shopify_search_double_quotes() {
        assert_eq!(escape_shopify_search(r#"say "hello""#), r#"say \"hello\""#);
    }

    #[test]
    fn test_builders_with_projected_schema() {
        let full_schema = ShopifyConnector::get_table_schema("products").unwrap();
        let default_fields = ShopifyConnector::graphql_fields_for("products");

        let projection = vec!["title".to_string(), "vendor".to_string()];
        let (proj_schema, _proj_fields) =
            ShopifyConnector::apply_projection(full_schema, default_fields, &projection);

        let arrow_schema = Arc::new(ShopifyConnector::to_arrow_schema(&proj_schema));

        let obj = serde_json::json!({
            "id": "gid://shopify/Product/1",
            "title": "T-Shirt",
            "vendor": "Acme"
        });

        let mut builders = ColumnBuilders::new(&proj_schema, 4);
        ShopifyConnector::append_shopify_object(&obj, &proj_schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3); // id + title + vendor
    }

    #[test]
    fn test_build_search_filter_multiple() {
        let predicates = vec![
            Predicate::Equals {
                column: CompactString::from("status"),
                value: CompactString::from("active"),
            },
            Predicate::Contains {
                column: CompactString::from("title"),
                substring: CompactString::from("shoe"),
            },
        ];
        let result = ShopifyConnector::build_search_filter(&predicates);
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.contains("status:'active'"));
        assert!(r.contains("title:*shoe*"));
        assert!(r.contains(" AND "));
    }
}
