//! HubSpot CRM connector for the data warehouse.
//!
//! Syncs HubSpot CRM data (contacts, companies, deals, etc.) to the warehouse.
//! Uses OAuth 2.0 for authentication and the HubSpot CRM v3 API.

use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use super::http_api::{HttpApiClient, AuthConfig};
use super::oauth::OAuthConfig;
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use super::builders::ColumnBuilders;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

const HUBSPOT_API_BASE: &str = "https://api.hubapi.com";
const PAGE_LIMIT: u32 = 100;
const MAX_TOTAL_ROWS: usize = 1_000_000;
const BATCH_THRESHOLD: usize = 1_000;

/// HubSpot connector configuration.
#[derive(Clone)]
pub struct HubSpotConfig {
    pub oauth: Arc<OAuthConfig>,
}

impl std::fmt::Debug for HubSpotConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubSpotConfig")
            .field("oauth", &"***REDACTED***")
            .finish()
    }
}

impl HubSpotConfig {
    pub fn new(oauth: OAuthConfig) -> Self {
        Self {
            oauth: Arc::new(oauth),
        }
    }
}

/// HubSpot CRM data source connector.
pub struct HubSpotConnector {
    #[allow(dead_code)]
    config: HubSpotConfig,
    client: HttpApiClient,
}

impl HubSpotConnector {
    const TABLES: &'static [&'static str] = &[
        "contacts",
        "companies",
        "deals",
        "tickets",
        "products",
        "line_items",
        "owners",
    ];

    pub fn new(config: HubSpotConfig) -> Self {
        let client = HttpApiClient::new(HUBSPOT_API_BASE)
            .with_auth(AuthConfig::OAuth(config.oauth.clone()))
            .with_rate_limit(100, std::time::Duration::from_secs(10));

        Self { config, client }
    }

    fn get_table_schema(table: &str) -> Option<TableSchema> {
        let columns = match table {
            "contacts" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the contact"),
                ColumnSchema::new("email", ColumnType::String, true)
                    .with_description("Contact email address"),
                ColumnSchema::new("firstname", ColumnType::String, true)
                    .with_description("Contact first name"),
                ColumnSchema::new("lastname", ColumnType::String, true)
                    .with_description("Contact last name"),
                ColumnSchema::new("phone", ColumnType::String, true)
                    .with_description("Contact phone number"),
                ColumnSchema::new("company", ColumnType::String, true)
                    .with_description("Associated company name"),
                ColumnSchema::new("lifecyclestage", ColumnType::String, true)
                    .with_description("Contact lifecycle stage"),
                ColumnSchema::new("createdate", ColumnType::Timestamp, true)
                    .with_description("Time at which the contact was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Time at which the contact was last modified")
                    .with_timezone("UTC"),
                ColumnSchema::new("hs_object_id", ColumnType::String, true)
                    .with_description("HubSpot internal object ID"),
            ],
            "companies" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the company"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Company name"),
                ColumnSchema::new("domain", ColumnType::String, true)
                    .with_description("Company domain"),
                ColumnSchema::new("industry", ColumnType::String, true)
                    .with_description("Company industry"),
                ColumnSchema::new("city", ColumnType::String, true)
                    .with_description("Company city"),
                ColumnSchema::new("state", ColumnType::String, true)
                    .with_description("Company state/region"),
                ColumnSchema::new("country", ColumnType::String, true)
                    .with_description("Company country"),
                ColumnSchema::new("phone", ColumnType::String, true)
                    .with_description("Company phone number"),
                ColumnSchema::new("createdate", ColumnType::Timestamp, true)
                    .with_description("Time at which the company was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Time at which the company was last modified")
                    .with_timezone("UTC"),
            ],
            "deals" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the deal"),
                ColumnSchema::new("dealname", ColumnType::String, true)
                    .with_description("Deal name"),
                ColumnSchema::new("amount", ColumnType::Float64, true)
                    .with_description("Deal amount"),
                ColumnSchema::new("dealstage", ColumnType::String, true)
                    .with_description("Deal stage"),
                ColumnSchema::new("pipeline", ColumnType::String, true)
                    .with_description("Deal pipeline"),
                ColumnSchema::new("closedate", ColumnType::Timestamp, true)
                    .with_description("Expected close date")
                    .with_timezone("UTC"),
                ColumnSchema::new("createdate", ColumnType::Timestamp, true)
                    .with_description("Time at which the deal was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Time at which the deal was last modified")
                    .with_timezone("UTC"),
                ColumnSchema::new("hs_object_id", ColumnType::String, true)
                    .with_description("HubSpot internal object ID"),
            ],
            "tickets" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the ticket"),
                ColumnSchema::new("subject", ColumnType::String, true)
                    .with_description("Ticket subject"),
                ColumnSchema::new("content", ColumnType::String, true)
                    .with_description("Ticket content/description"),
                ColumnSchema::new("hs_pipeline", ColumnType::String, true)
                    .with_description("Ticket pipeline"),
                ColumnSchema::new("hs_pipeline_stage", ColumnType::String, true)
                    .with_description("Ticket pipeline stage"),
                ColumnSchema::new("createdate", ColumnType::Timestamp, true)
                    .with_description("Time at which the ticket was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Time at which the ticket was last modified")
                    .with_timezone("UTC"),
            ],
            "products" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the product"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Product name"),
                ColumnSchema::new("description", ColumnType::String, true)
                    .with_description("Product description"),
                ColumnSchema::new("price", ColumnType::Float64, true)
                    .with_description("Product price"),
                ColumnSchema::new("hs_sku", ColumnType::String, true)
                    .with_description("Product SKU"),
                ColumnSchema::new("createdate", ColumnType::Timestamp, true)
                    .with_description("Time at which the product was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Time at which the product was last modified")
                    .with_timezone("UTC"),
            ],
            "line_items" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the line item"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Line item name"),
                ColumnSchema::new("quantity", ColumnType::Float64, true)
                    .with_description("Line item quantity"),
                ColumnSchema::new("price", ColumnType::Float64, true)
                    .with_description("Line item unit price"),
                ColumnSchema::new("amount", ColumnType::Float64, true)
                    .with_description("Line item total amount"),
                ColumnSchema::new("hs_product_id", ColumnType::String, true)
                    .with_description("Associated product ID"),
                ColumnSchema::new("createdate", ColumnType::Timestamp, true)
                    .with_description("Time at which the line item was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Time at which the line item was last modified")
                    .with_timezone("UTC"),
            ],
            "owners" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Unique identifier for the owner"),
                ColumnSchema::new("email", ColumnType::String, true)
                    .with_description("Owner email address"),
                ColumnSchema::new("firstname", ColumnType::String, true)
                    .with_description("Owner first name"),
                ColumnSchema::new("lastname", ColumnType::String, true)
                    .with_description("Owner last name"),
                ColumnSchema::new("userid", ColumnType::String, true)
                    .with_description("HubSpot user ID"),
                ColumnSchema::new("createdate", ColumnType::Timestamp, true)
                    .with_description("Time at which the owner was created")
                    .with_timezone("UTC"),
                ColumnSchema::new("updatedat", ColumnType::Timestamp, true)
                    .with_description("Time at which the owner was last updated")
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

    /// Get the list of property names to request for a table (excluding "id").
    fn properties_for_table(table: &str) -> Vec<&'static str> {
        match table {
            "contacts" => vec![
                "email", "firstname", "lastname", "phone", "company",
                "lifecyclestage", "createdate", "lastmodifieddate", "hs_object_id",
            ],
            "companies" => vec![
                "name", "domain", "industry", "city", "state", "country",
                "phone", "createdate", "lastmodifieddate",
            ],
            "deals" => vec![
                "dealname", "amount", "dealstage", "pipeline", "closedate",
                "createdate", "lastmodifieddate", "hs_object_id",
            ],
            "tickets" => vec![
                "subject", "content", "hs_pipeline", "hs_pipeline_stage",
                "createdate", "lastmodifieddate",
            ],
            "products" => vec![
                "name", "description", "price", "hs_sku",
                "createdate", "lastmodifieddate",
            ],
            "line_items" => vec![
                "name", "quantity", "price", "amount", "hs_product_id",
                "createdate", "lastmodifieddate",
            ],
            "owners" => vec![
                "email", "firstname", "lastname", "userid",
                "createdate", "updatedat",
            ],
            _ => vec![],
        }
    }

    /// Map table name to the HubSpot CRM v3 object type path segment.
    fn api_object_type(table: &str) -> &str {
        match table {
            "line_items" => "line_items",
            "owners" => "owners",
            _ => table,
        }
    }

    /// Build the properties query parameter string.
    fn properties_param(table: &str) -> String {
        Self::properties_for_table(table).join(",")
    }

    /// Fetch a table page-by-page using the list endpoint (full sync).
    async fn fetch_list(
        &self,
        table: &str,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let object_type = Self::api_object_type(table);
        let properties = Self::properties_param(table);

        let is_owners = table == "owners";
        let base_path = if is_owners {
            "/crm/v3/owners".to_string()
        } else {
            format!("/crm/v3/objects/{}", object_type)
        };

        let table_schema = Self::get_table_schema(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(&table_schema, BATCH_THRESHOLD);
        let mut after: Option<String> = None;
        let mut total_rows: usize = 0;

        loop {
            let mut params: Vec<(String, String)> = vec![
                ("limit".to_string(), PAGE_LIMIT.to_string()),
            ];
            if !is_owners {
                params.push(("properties".to_string(), properties.clone()));
            }
            if let Some(ref cursor) = after {
                params.push(("after".to_string(), cursor.clone()));
            }

            let response: serde_json::Value = self.client.get_with_params(&base_path, &params).await?;

            let results = response
                .get("results")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    ConnectorError::Internal("Invalid HubSpot response: missing 'results'".to_string())
                })?;

            if results.is_empty() {
                break;
            }

            for obj in results {
                Self::append_hubspot_object(obj, is_owners, &table_schema, &mut builders);
            }
            total_rows += results.len();

            if builders.len() >= BATCH_THRESHOLD {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
                builders = ColumnBuilders::new(&table_schema, BATCH_THRESHOLD);
            }

            after = response
                .pointer("/paging/next/after")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if after.is_none() || total_rows >= MAX_TOTAL_ROWS {
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

    /// Fetch a table incrementally using the search endpoint.
    async fn fetch_search(
        &self,
        table: &str,
        last_value: &str,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let object_type = Self::api_object_type(table);
        let properties: Vec<String> = Self::properties_for_table(table)
            .iter()
            .map(|s| s.to_string())
            .collect();

        if table == "owners" {
            return self.fetch_owners_incremental(last_value, arrow_schema).await;
        }

        let search_path = format!("/crm/v3/objects/{}/search", object_type);

        let table_schema = Self::get_table_schema(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(&table_schema, BATCH_THRESHOLD);
        let mut after: Option<String> = None;
        let mut total_rows: usize = 0;

        loop {
            let mut body = serde_json::json!({
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "lastmodifieddate",
                        "operator": "GTE",
                        "value": last_value
                    }]
                }],
                "properties": properties,
                "limit": PAGE_LIMIT
            });

            if let Some(ref cursor) = after {
                body["after"] = serde_json::json!(cursor);
            }

            let response: serde_json::Value = self.client.post(&search_path, &body).await?;

            let results = response
                .get("results")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    ConnectorError::Internal(
                        "Invalid HubSpot search response: missing 'results'".to_string(),
                    )
                })?;

            if results.is_empty() {
                break;
            }

            for obj in results {
                Self::append_hubspot_object(obj, false, &table_schema, &mut builders);
            }
            total_rows += results.len();

            if builders.len() >= BATCH_THRESHOLD {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
                builders = ColumnBuilders::new(&table_schema, BATCH_THRESHOLD);
            }

            after = response
                .pointer("/paging/next/after")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if after.is_none() || total_rows >= MAX_TOTAL_ROWS {
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

    /// Owners don't support search, so incremental sync filters client-side
    /// using the `updatedAt` field returned by the list endpoint.
    async fn fetch_owners_incremental(
        &self,
        last_value: &str,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let threshold_ms: i64 = chrono::DateTime::parse_from_rfc3339(last_value)
            .ok()
            .map(|dt| dt.timestamp_millis())
            .unwrap_or_else(|| last_value.parse().unwrap_or(0));

        let table_schema = Self::get_table_schema("owners")
            .ok_or_else(|| ConnectorError::TableNotFound("owners".to_string()))?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(&table_schema, BATCH_THRESHOLD);
        let mut after: Option<String> = None;
        let mut total_rows: usize = 0;

        loop {
            let mut params: Vec<(String, String)> =
                vec![("limit".to_string(), PAGE_LIMIT.to_string())];
            if let Some(ref cursor) = after {
                params.push(("after".to_string(), cursor.clone()));
            }

            let response: serde_json::Value =
                self.client.get_with_params("/crm/v3/owners", &params).await?;

            let results = response
                .get("results")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    ConnectorError::Internal(
                        "Invalid HubSpot owners response: missing 'results'".to_string(),
                    )
                })?;

            if results.is_empty() {
                break;
            }

            for obj in results {
                let updated = obj
                    .get("updatedAt")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Self::parse_hubspot_timestamp(s))
                    .unwrap_or(0);
                if updated >= threshold_ms {
                    Self::append_hubspot_object(obj, true, &table_schema, &mut builders);
                    total_rows += 1;
                }
            }

            if builders.len() >= BATCH_THRESHOLD {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
                builders = ColumnBuilders::new(&table_schema, BATCH_THRESHOLD);
            }

            after = response
                .pointer("/paging/next/after")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if after.is_none() || total_rows >= MAX_TOTAL_ROWS {
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

    /// Parse a HubSpot timestamp string (ISO-8601 or epoch ms) to epoch milliseconds.
    fn parse_hubspot_timestamp(value: &str) -> Option<i64> {
        if let Ok(ms) = value.parse::<i64>() {
            return Some(ms);
        }
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
            return Some(dt.timestamp_millis());
        }
        if let Ok(dt) =
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.fZ")
        {
            return Some(dt.and_utc().timestamp_millis());
        }
        None
    }

    /// Push a single HubSpot object directly into columnar builders.
    /// Handles both CRM objects (nested `properties`) and owner objects (flat).
    fn append_hubspot_object(
        obj: &serde_json::Value,
        is_owner: bool,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            let val = if col.name == "id" {
                obj.get("id")
            } else if is_owner {
                let api_key = match col.name.as_str() {
                    "firstname" => "firstName",
                    "lastname" => "lastName",
                    "userid" => "userId",
                    "createdate" => "createdAt",
                    "updatedat" => "updatedAt",
                    other => other,
                };
                obj.get(api_key)
            } else {
                obj.get("properties").and_then(|p| p.get(&col.name))
            };

            match col.data_type {
                ColumnType::Timestamp => {
                    let micros = val.and_then(|v| {
                        if let Some(ms) = v.as_i64() {
                            Some(ms * 1_000)
                        } else {
                            v.as_str().and_then(|s| {
                                Self::parse_hubspot_timestamp(s).map(|ms| ms * 1_000)
                            })
                        }
                    });
                    builders.builder(i).append_timestamp(micros);
                }
                ColumnType::Date => {
                    const MS_PER_DAY: i64 = 86_400_000;
                    let days = val.and_then(|v| {
                        if let Some(ms) = v.as_i64() {
                            Some((ms / MS_PER_DAY) as i32)
                        } else {
                            v.as_str().and_then(|s| {
                                Self::parse_hubspot_timestamp(s)
                                    .map(|ms| (ms / MS_PER_DAY) as i32)
                            })
                        }
                    });
                    builders.builder(i).append_date32(days);
                }
                _ => {
                    builders.builder(i).append_json_value(val);
                }
            }
        }
        builders.row_complete();
    }

    /// Get the incremental key for a table.
    fn incremental_key_for(table: &str) -> &'static str {
        match table {
            "owners" => "updatedat",
            _ => "lastmodifieddate",
        }
    }
}

#[async_trait]
impl Connector for HubSpotConnector {
    fn source_type(&self) -> SourceType {
        SourceType::HubSpot
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        Ok(Self::TABLES
            .iter()
            .filter_map(|&table| {
                Self::get_table_schema(table).map(|schema| TableInfo {
                    name: table.to_string(),
                    schema,
                    supports_incremental: true,
                    incremental_key: Some(Self::incremental_key_for(table).to_string()),
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
        if !Self::TABLES.contains(&table) {
            return Err(ConnectorError::TableNotFound(table.to_string()));
        }

        let schema = self.get_schema(table).await?;
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        match (incremental_key, last_value) {
            (Some(_key), Some(value)) => {
                self.fetch_search(table, value, arrow_schema).await
            }
            _ => self.fetch_list(table, arrow_schema).await,
        }
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            let batches = self
                .fetch_table(
                    table,
                    options.incremental_key.as_deref(),
                    options.last_value.as_deref(),
                )
                .await?;
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        let params: Vec<(String, String)> = vec![("limit".to_string(), "1".to_string())];
        let _: serde_json::Value = self
            .client
            .get_with_params("/crm/v3/objects/contacts", &params)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_oauth() -> OAuthConfig {
        let future_expiry = chrono::Utc::now() + chrono::Duration::hours(1);
        OAuthConfig::new("test_client_id", "test_client_secret", "https://api.hubapi.com/oauth/v1/token")
            .with_access_token("test_access_token", Some(future_expiry))
            .with_refresh_token("test_refresh_token")
    }

    fn test_config() -> HubSpotConfig {
        HubSpotConfig::new(test_oauth())
    }

    fn test_connector_with_base_url(base_url: &str) -> HubSpotConnector {
        let config = test_config();
        let client = HttpApiClient::new(base_url)
            .with_auth(AuthConfig::OAuth(config.oauth.clone()))
            .with_rate_limit(100, std::time::Duration::from_secs(10));
        HubSpotConnector { config, client }
    }

    #[test]
    fn test_hubspot_config_creation() {
        let config = test_config();
        assert!(format!("{:?}", config).contains("REDACTED"));
    }

    #[test]
    fn test_hubspot_config_debug_redacts() {
        let config = test_config();
        let debug_output = format!("{:?}", config);
        assert!(!debug_output.contains("test_client_secret"));
        assert!(!debug_output.contains("test_access_token"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn test_get_table_schema_contacts() {
        let schema = HubSpotConnector::get_table_schema("contacts");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"email"));
        assert!(names.contains(&"firstname"));
        assert!(names.contains(&"lastname"));
        assert!(names.contains(&"createdate"));
        assert!(names.contains(&"lastmodifieddate"));
    }

    #[test]
    fn test_get_table_schema_companies() {
        let schema = HubSpotConnector::get_table_schema("companies");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"domain"));
        assert!(names.contains(&"industry"));
    }

    #[test]
    fn test_get_table_schema_deals() {
        let schema = HubSpotConnector::get_table_schema("deals");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"dealname"));
        assert!(names.contains(&"amount"));
        assert!(names.contains(&"dealstage"));
    }

    #[test]
    fn test_get_table_schema_tickets() {
        let schema = HubSpotConnector::get_table_schema("tickets");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"subject"));
        assert!(names.contains(&"hs_pipeline"));
    }

    #[test]
    fn test_get_table_schema_products() {
        let schema = HubSpotConnector::get_table_schema("products");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"price"));
        assert!(names.contains(&"hs_sku"));
    }

    #[test]
    fn test_get_table_schema_line_items() {
        let schema = HubSpotConnector::get_table_schema("line_items");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"quantity"));
        assert!(names.contains(&"hs_product_id"));
    }

    #[test]
    fn test_get_table_schema_owners() {
        let schema = HubSpotConnector::get_table_schema("owners");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"email"));
        assert!(names.contains(&"userid"));
        assert!(names.contains(&"updatedat"));
    }

    #[test]
    fn test_get_table_schema_unknown() {
        assert!(HubSpotConnector::get_table_schema("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_list_tables() {
        let connector = HubSpotConnector::new(test_config());
        let tables = connector.list_tables().await.unwrap();
        assert_eq!(tables.len(), 7);

        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"contacts"));
        assert!(names.contains(&"companies"));
        assert!(names.contains(&"deals"));
        assert!(names.contains(&"tickets"));
        assert!(names.contains(&"products"));
        assert!(names.contains(&"line_items"));
        assert!(names.contains(&"owners"));

        for t in &tables {
            assert!(t.supports_incremental);
            assert!(t.incremental_key.is_some());
            assert_eq!(t.primary_key_columns, vec!["id".to_string()]);
        }
    }

    #[tokio::test]
    async fn test_get_schema() {
        let connector = HubSpotConnector::new(test_config());
        let schema = connector.get_schema("contacts").await.unwrap();
        assert!(!schema.columns.is_empty());
    }

    #[tokio::test]
    async fn test_get_schema_not_found() {
        let connector = HubSpotConnector::new(test_config());
        let result = connector.get_schema("nonexistent").await;
        assert!(matches!(result, Err(ConnectorError::TableNotFound(_))));
    }

    #[test]
    fn test_source_type() {
        let connector = HubSpotConnector::new(test_config());
        assert_eq!(connector.source_type(), SourceType::HubSpot);
    }

    #[test]
    fn test_builders_contacts_batch() {
        let schema = HubSpotConnector::get_table_schema("contacts").unwrap();
        let arrow_schema = Arc::new(HubSpotConnector::to_arrow_schema(&schema));

        let objects = vec![
            serde_json::json!({
                "id": "101",
                "properties": {
                    "email": "alice@example.com",
                    "firstname": "Alice",
                    "lastname": "Smith",
                    "phone": "+1234567890",
                    "company": "Acme Inc",
                    "lifecyclestage": "lead",
                    "createdate": "2024-01-01T00:00:00Z",
                    "lastmodifieddate": "2024-06-15T12:30:00Z",
                    "hs_object_id": "101"
                }
            }),
            serde_json::json!({
                "id": "102",
                "properties": {
                    "email": null,
                    "firstname": "Bob",
                    "lastname": null,
                    "phone": null,
                    "company": null,
                    "lifecyclestage": null,
                    "createdate": "2024-02-01T10:00:00Z",
                    "lastmodifieddate": "2024-07-01T08:00:00Z",
                    "hs_object_id": "102"
                }
            }),
        ];

        let mut builders = ColumnBuilders::new(&schema, 4);
        for obj in &objects {
            HubSpotConnector::append_hubspot_object(obj, false, &schema, &mut builders);
        }
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 10);
    }

    #[test]
    fn test_builders_deals_with_float_amount() {
        let schema = HubSpotConnector::get_table_schema("deals").unwrap();
        let arrow_schema = Arc::new(HubSpotConnector::to_arrow_schema(&schema));

        let obj = serde_json::json!({
            "id": "501",
            "properties": {
                "dealname": "Big deal",
                "amount": "15000.50",
                "dealstage": "closedwon",
                "pipeline": "default",
                "closedate": "2024-03-01T00:00:00Z",
                "createdate": "2024-01-15T00:00:00Z",
                "lastmodifieddate": "2024-03-01T00:00:00Z",
                "hs_object_id": "501"
            }
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        HubSpotConnector::append_hubspot_object(&obj, false, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let amount_col = batch.column_by_name("amount").unwrap().as_any()
            .downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert!((amount_col.value(0) - 15000.50).abs() < f64::EPSILON);
    }

    #[test]
    fn test_append_hubspot_object_crm() {
        let schema = HubSpotConnector::get_table_schema("contacts").unwrap();
        let arrow_schema = Arc::new(HubSpotConnector::to_arrow_schema(&schema));

        let obj = serde_json::json!({
            "id": "123",
            "properties": {
                "email": "test@example.com",
                "firstname": "Test"
            },
            "createdAt": "2024-01-01T00:00:00Z"
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        HubSpotConnector::append_hubspot_object(&obj, false, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();

        let id_col = batch.column_by_name("id").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(id_col.value(0), "123");
        let email_col = batch.column_by_name("email").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(email_col.value(0), "test@example.com");
    }

    #[test]
    fn test_append_hubspot_object_owner() {
        let schema = HubSpotConnector::get_table_schema("owners").unwrap();
        let arrow_schema = Arc::new(HubSpotConnector::to_arrow_schema(&schema));

        let obj = serde_json::json!({
            "id": "456",
            "email": "owner@example.com",
            "firstName": "Jane",
            "lastName": "Doe",
            "userId": "789",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-06-01T00:00:00Z"
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        HubSpotConnector::append_hubspot_object(&obj, true, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();

        let id_col = batch.column_by_name("id").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(id_col.value(0), "456");
        let firstname_col = batch.column_by_name("firstname").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(firstname_col.value(0), "Jane");
        let lastname_col = batch.column_by_name("lastname").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(lastname_col.value(0), "Doe");
    }

    #[test]
    fn test_parse_hubspot_timestamp_epoch_ms() {
        let ms = HubSpotConnector::parse_hubspot_timestamp("1704067200000");
        assert_eq!(ms, Some(1704067200000));
    }

    #[test]
    fn test_parse_hubspot_timestamp_iso8601() {
        let ms = HubSpotConnector::parse_hubspot_timestamp("2024-01-01T00:00:00Z");
        assert_eq!(ms, Some(1704067200000));
    }

    #[test]
    fn test_parse_hubspot_timestamp_invalid() {
        assert!(HubSpotConnector::parse_hubspot_timestamp("not-a-date").is_none());
    }

    #[test]
    fn test_incremental_search_body() {
        let last_value = "1704067200000";
        let properties = vec!["email".to_string(), "firstname".to_string()];

        let body = serde_json::json!({
            "filterGroups": [{
                "filters": [{
                    "propertyName": "lastmodifieddate",
                    "operator": "GTE",
                    "value": last_value
                }]
            }],
            "properties": properties,
            "limit": PAGE_LIMIT
        });

        let filters = &body["filterGroups"][0]["filters"][0];
        assert_eq!(filters["propertyName"], "lastmodifieddate");
        assert_eq!(filters["operator"], "GTE");
        assert_eq!(filters["value"], last_value);
        assert_eq!(body["limit"], PAGE_LIMIT);
    }

    #[tokio::test]
    async fn test_hubspot_request_authentication_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/crm/v3/objects/contacts"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_hubspot_request_rate_limited() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/crm/v3/objects/contacts"))
            .respond_with(
                ResponseTemplate::new(429).insert_header("Retry-After", "120"),
            )
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.validate_credentials().await;
        assert!(matches!(
            result,
            Err(ConnectorError::RateLimited { .. })
        ));
    }

    #[tokio::test]
    async fn test_hubspot_request_success() {
        let mock_server = MockServer::start().await;

        let body = serde_json::json!({
            "results": [{
                "id": "1",
                "properties": {
                    "email": "a@b.com",
                    "firstname": "A",
                    "lastname": "B",
                    "phone": null,
                    "company": null,
                    "lifecyclestage": "lead",
                    "createdate": "2024-01-01T00:00:00Z",
                    "lastmodifieddate": "2024-06-01T00:00:00Z",
                    "hs_object_id": "1"
                }
            }],
            "paging": {}
        });

        Mock::given(method("GET"))
            .and(path("/crm/v3/objects/contacts"))
            .and(query_param("limit", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&body))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.validate_credentials().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_hubspot_fetch_table_full_sync() {
        let mock_server = MockServer::start().await;

        let body = serde_json::json!({
            "results": [
                {
                    "id": "1",
                    "properties": {
                        "email": "alice@example.com",
                        "firstname": "Alice",
                        "lastname": "Smith",
                        "phone": null,
                        "company": "Acme",
                        "lifecyclestage": "customer",
                        "createdate": "2024-01-01T00:00:00Z",
                        "lastmodifieddate": "2024-06-01T00:00:00Z",
                        "hs_object_id": "1"
                    }
                },
                {
                    "id": "2",
                    "properties": {
                        "email": "bob@example.com",
                        "firstname": "Bob",
                        "lastname": "Jones",
                        "phone": "+1555000",
                        "company": null,
                        "lifecyclestage": "lead",
                        "createdate": "2024-02-01T00:00:00Z",
                        "lastmodifieddate": "2024-07-01T00:00:00Z",
                        "hs_object_id": "2"
                    }
                }
            ],
            "paging": {}
        });

        Mock::given(method("GET"))
            .and(path("/crm/v3/objects/contacts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&body))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let batches = connector.fetch_table("contacts", None, None).await.unwrap();
        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_iso8601_owners_threshold_parsing() {
        let iso = "2024-06-15T10:30:00.000000Z";
        let threshold_ms: i64 = chrono::DateTime::parse_from_rfc3339(iso)
            .ok()
            .map(|dt| dt.timestamp_millis())
            .unwrap_or_else(|| iso.parse().unwrap_or(0));

        assert_ne!(
            threshold_ms, 0,
            "ISO-8601 checkpoint must parse to non-zero millis, got 0"
        );
        assert_eq!(
            threshold_ms, 1718447400000,
            "2024-06-15T10:30:00Z = 1718447400000 ms"
        );
    }

    #[test]
    fn test_raw_epoch_ms_owners_threshold_parsing() {
        let raw = "1718447400000";
        let threshold_ms: i64 = chrono::DateTime::parse_from_rfc3339(raw)
            .ok()
            .map(|dt| dt.timestamp_millis())
            .unwrap_or_else(|| raw.parse().unwrap_or(0));

        assert_eq!(threshold_ms, 1718447400000);
    }
}
