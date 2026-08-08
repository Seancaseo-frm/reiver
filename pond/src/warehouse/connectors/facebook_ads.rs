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
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const API_BASE: &str = "https://graph.facebook.com/v22.0";
const BATCH_CAPACITY: usize = 4096;
const PAGE_LIMIT: u64 = 250;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 100;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct FacebookAdsConfig {
    pub access_token: SecretString,
    pub app_id: Option<String>,
    pub app_secret: Option<SecretString>,
    pub ad_account_id: String,
}

impl std::fmt::Debug for FacebookAdsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FacebookAdsConfig")
            .field("ad_account_id", &self.ad_account_id)
            .field("app_id", &self.app_id)
            .field("access_token", &"[REDACTED]")
            .field("app_secret", &"[REDACTED]")
            .finish()
    }
}

impl FacebookAdsConfig {
    pub fn new(
        access_token: impl Into<String>,
        ad_account_id: impl Into<String>,
    ) -> Self {
        let mut account_id = ad_account_id.into();
        if !account_id.starts_with("act_") {
            account_id = format!("act_{}", account_id);
        }
        Self {
            access_token: SecretString::new(access_token.into()),
            app_id: None,
            app_secret: None,
            ad_account_id: account_id,
        }
    }

    pub fn with_app_credentials(
        mut self,
        app_id: impl Into<String>,
        app_secret: impl Into<String>,
    ) -> Self {
        self.app_id = Some(app_id.into());
        self.app_secret = Some(SecretString::new(app_secret.into()));
        self
    }
}

// ---------------------------------------------------------------------------
// Token management
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Option<Instant>,
}

struct TokenManager {
    http: reqwest::Client,
    config: FacebookAdsConfig,
    cached: RwLock<Option<CachedToken>>,
    #[cfg(test)]
    api_base_override: Option<String>,
}

impl TokenManager {
    fn new(config: &FacebookAdsConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config: config.clone(),
            cached: RwLock::new(None),
            #[cfg(test)]
            api_base_override: None,
        }
    }

    fn api_base(&self) -> &str {
        #[cfg(test)]
        if let Some(ref url) = self.api_base_override {
            return url.as_str();
        }
        API_BASE
    }

    async fn get_access_token(&self) -> ConnectorResult<String> {
        {
            let guard = self.cached.read().await;
            if let Some(ref t) = *guard {
                if let Some(expires_at) = t.expires_at {
                    if expires_at > Instant::now() + Duration::from_secs(300) {
                        return Ok(t.access_token.clone());
                    }
                } else {
                    return Ok(t.access_token.clone());
                }
            }
        }
        self.refresh_or_init().await
    }

    async fn refresh_or_init(&self) -> ConnectorResult<String> {
        let mut guard = self.cached.write().await;

        if let Some(ref t) = *guard {
            if let Some(expires_at) = t.expires_at {
                if expires_at > Instant::now() + Duration::from_secs(300) {
                    return Ok(t.access_token.clone());
                }
            } else {
                return Ok(t.access_token.clone());
            }
        }

        let (token, expires_at) = if self.config.app_id.is_some()
            && self.config.app_secret.is_some()
        {
            self.exchange_token_http().await?
        } else {
            (self.config.access_token.expose().to_string(), None)
        };

        *guard = Some(CachedToken {
            access_token: token.clone(),
            expires_at,
        });
        Ok(token)
    }

    /// Perform the HTTP token exchange. Returns (access_token, optional expiry).
    /// Does NOT touch the cache -- the caller must update it.
    async fn exchange_token_http(&self) -> ConnectorResult<(String, Option<Instant>)> {
        let app_id = self.config.app_id.as_deref().unwrap();
        let app_secret = self.config.app_secret.as_ref().unwrap();

        let url = format!(
            "{}/oauth/access_token?grant_type=fb_exchange_token&client_id={}&client_secret={}&fb_exchange_token={}",
            self.api_base(),
            app_id,
            app_secret.expose(),
            self.config.access_token.expose(),
        );

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| ConnectorError::Network(format!("Token exchange failed: {}", e)))?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.map_err(|e| {
            ConnectorError::Network(format!("Failed to parse token response: {}", e))
        })?;

        if !status.is_success() {
            let err_msg = body["error"]["message"]
                .as_str()
                .unwrap_or("unknown error");
            return Err(ConnectorError::Authentication(format!(
                "Token exchange failed ({}): {}",
                status, err_msg
            )));
        }

        let access_token = body["access_token"]
            .as_str()
            .ok_or_else(|| {
                ConnectorError::Authentication("Response missing access_token".to_string())
            })?
            .to_string();

        let expires_in = body["expires_in"].as_u64();
        let expires_at = expires_in.map(|secs| Instant::now() + Duration::from_secs(secs));

        Ok((access_token, expires_at))
    }
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct FacebookAdsConnector {
    config: FacebookAdsConfig,
    http: reqwest::Client,
    token_manager: Arc<TokenManager>,
    #[cfg(test)]
    base_url_override: Option<String>,
}

impl FacebookAdsConnector {
    pub fn new(config: FacebookAdsConfig) -> Self {
        let token_manager = Arc::new(TokenManager::new(&config));
        Self {
            config,
            http: reqwest::Client::new(),
            token_manager,
            #[cfg(test)]
            base_url_override: None,
        }
    }

    fn api_base(&self) -> &str {
        #[cfg(test)]
        if let Some(ref url) = self.base_url_override {
            return url.as_str();
        }
        API_BASE
    }

    async fn api_get(&self, url: &str) -> ConnectorResult<serde_json::Value> {
        let token = self.token_manager.get_access_token().await?;
        let mut attempts = 0u32;

        loop {
            let resp = self
                .http
                .get(url)
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("Facebook API request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                return Err(ConnectorError::Authentication(
                    "Invalid Facebook access token".to_string(),
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
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                let error_msg = body["error"]["message"]
                    .as_str()
                    .unwrap_or("unknown error");
                let error_code = body["error"]["code"].as_u64().unwrap_or(0);

                if error_code == 17 || error_code == 32 {
                    if attempts < MAX_RETRIES {
                        attempts += 1;
                        let delay = INITIAL_RETRY_DELAY_MS * 2u64.pow(attempts - 1);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        continue;
                    }
                    return Err(ConnectorError::RateLimited { retry_after_secs: 60 });
                }

                return Err(ConnectorError::Internal(format!(
                    "Facebook API error ({}, code {}): {}",
                    status, error_code, error_msg
                )));
            }

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse Facebook response: {}", e))
            })?;

            return Ok(json);
        }
    }

    fn resolve_next_url(&self, next_url: &str) -> String {
        #[cfg(test)]
        if let Some(ref base) = self.base_url_override {
            if next_url.starts_with("https://graph.facebook.com") {
                let path = next_url
                    .strip_prefix("https://graph.facebook.com/v22.0")
                    .unwrap_or(next_url);
                return format!("{}{}", base, path);
            }
        }
        next_url.to_string()
    }

    // -----------------------------------------------------------------------
    // Fetch helpers -- page-by-page conversion into RecordBatches
    // -----------------------------------------------------------------------

    async fn fetch_entities(
        &self,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mapping = field_mapping(table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("Unknown table: {}", table))
        })?;

        let fields: Vec<&str> = mapping
            .api_fields
            .iter()
            .map(|(_, api_field)| *api_field)
            .collect();

        let mut url = format!(
            "{}/{}/{}?fields={}&limit={}",
            self.api_base(),
            self.config.ad_account_id,
            mapping.endpoint,
            fields.join(","),
            PAGE_LIMIT,
        );

        if let (Some(key), Some(val)) = (&options.incremental_key, &options.last_value) {
            if key == "updated_time" {
                let filter = format!(
                    "[{{\"field\":\"updated_time\",\"operator\":\"GREATER_THAN\",\"value\":\"{}\"}}]",
                    val
                );
                url.push_str(&format!("&filtering={}", urlencoding::encode(&filter)));
            }
        }

        let status_filter = extract_status_predicate(&options.predicates);
        if let Some(statuses) = status_filter {
            let encoded = format!("[{}]", statuses.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(","));
            url.push_str(&format!("&effective_status={}", urlencoding::encode(&encoded)));
        }

        self.fetch_paginated_batches(&url, table, schema, arrow_schema, options.max_rows).await
    }

    async fn fetch_insights(
        &self,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mapping = field_mapping(table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("Unknown table: {}", table))
        })?;

        let fields: Vec<&str> = mapping
            .api_fields
            .iter()
            .map(|(_, api_field)| *api_field)
            .collect();

        let level = mapping.endpoint;
        let time_range = build_time_range(options);

        let mut url = format!(
            "{}/{}/insights?fields={}&level={}&time_increment=1&limit={}",
            self.api_base(),
            self.config.ad_account_id,
            fields.join(","),
            level,
            PAGE_LIMIT,
        );

        if let Some(ref tr) = time_range {
            url.push_str(&format!("&time_range={}", urlencoding::encode(tr)));
        }

        self.fetch_paginated_batches(&url, table, schema, arrow_schema, options.max_rows).await
    }

    async fn fetch_paginated_batches(
        &self,
        initial_url: &str,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        max_rows: Option<usize>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let limit = max_rows.unwrap_or(usize::MAX);
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let mut url = initial_url.to_string();

        loop {
            if total_rows >= limit {
                break;
            }

            let json = self.api_get(&url).await?;

            if let Some(data) = json["data"].as_array() {
                for item in data {
                    if total_rows >= limit {
                        break;
                    }
                    append_row(item, table, schema, &mut builders);
                    total_rows += 1;

                    if builders.len() >= BATCH_CAPACITY {
                        batches.push(builders.finish(arrow_schema.clone())?);
                        builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                    }
                }
            }

            match json["paging"]["next"].as_str() {
                Some(next_url) => {
                    url = self.resolve_next_url(next_url);
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

        if is_insights_table(table) {
            self.fetch_insights(table, &schema, arrow_schema, options).await
        } else {
            self.fetch_entities(table, &schema, arrow_schema, options).await
        }
    }
}

// ---------------------------------------------------------------------------
// Table schemas
// ---------------------------------------------------------------------------

fn get_table_schema(table: &str) -> Option<TableSchema> {
    match table {
        "campaigns" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("campaign_id", ColumnType::String, false),
                ColumnSchema::new("campaign_name", ColumnType::String, false),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("objective", ColumnType::String, true),
                ColumnSchema::new("bid_strategy", ColumnType::String, true),
                ColumnSchema::new("daily_budget", ColumnType::Float64, true),
                ColumnSchema::new("lifetime_budget", ColumnType::Float64, true),
                ColumnSchema::new("created_time", ColumnType::Timestamp, true),
                ColumnSchema::new("updated_time", ColumnType::Timestamp, true),
            ],
        }),

        "ad_sets" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("ad_set_id", ColumnType::String, false),
                ColumnSchema::new("ad_set_name", ColumnType::String, false),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("campaign_id", ColumnType::String, false),
                ColumnSchema::new("optimization_goal", ColumnType::String, true),
                ColumnSchema::new("billing_event", ColumnType::String, true),
                ColumnSchema::new("daily_budget", ColumnType::Float64, true),
                ColumnSchema::new("lifetime_budget", ColumnType::Float64, true),
                ColumnSchema::new("start_time", ColumnType::Timestamp, true),
                ColumnSchema::new("end_time", ColumnType::Timestamp, true),
                ColumnSchema::new("created_time", ColumnType::Timestamp, true),
                ColumnSchema::new("updated_time", ColumnType::Timestamp, true),
            ],
        }),

        "ads" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("ad_id", ColumnType::String, false),
                ColumnSchema::new("ad_name", ColumnType::String, false),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("ad_set_id", ColumnType::String, true),
                ColumnSchema::new("campaign_id", ColumnType::String, true),
                ColumnSchema::new("creative_id", ColumnType::String, true),
                ColumnSchema::new("created_time", ColumnType::Timestamp, true),
                ColumnSchema::new("updated_time", ColumnType::Timestamp, true),
            ],
        }),

        "account_insights" => Some(insights_schema(&[])),
        "campaign_insights" => Some(insights_schema(&[
            ("campaign_id", ColumnType::String, false),
            ("campaign_name", ColumnType::String, false),
        ])),
        "ad_set_insights" => Some(insights_schema(&[
            ("ad_set_id", ColumnType::String, false),
            ("ad_set_name", ColumnType::String, false),
            ("campaign_id", ColumnType::String, false),
        ])),
        "ad_insights" => Some(insights_schema(&[
            ("ad_id", ColumnType::String, false),
            ("ad_name", ColumnType::String, false),
            ("ad_set_id", ColumnType::String, false),
            ("campaign_id", ColumnType::String, false),
        ])),

        _ => None,
    }
}

fn insights_schema(prefix_cols: &[(&str, ColumnType, bool)]) -> TableSchema {
    let mut columns: Vec<ColumnSchema> = prefix_cols
        .iter()
        .map(|(name, dt, nullable)| ColumnSchema::new(*name, *dt, *nullable))
        .collect();

    columns.extend(vec![
        ColumnSchema::new("date", ColumnType::Date, false),
        ColumnSchema::new("impressions", ColumnType::Int64, true),
        ColumnSchema::new("clicks", ColumnType::Int64, true),
        ColumnSchema::new("spend", ColumnType::Float64, true),
        ColumnSchema::new("cpc", ColumnType::Float64, true),
        ColumnSchema::new("cpm", ColumnType::Float64, true),
        ColumnSchema::new("ctr", ColumnType::Float64, true),
        ColumnSchema::new("reach", ColumnType::Int64, true),
        ColumnSchema::new("frequency", ColumnType::Float64, true),
        ColumnSchema::new("conversions", ColumnType::String, true),
        ColumnSchema::new("actions", ColumnType::String, true),
        ColumnSchema::new("cost_per_action_type", ColumnType::String, true),
    ]);

    TableSchema { columns }
}

const ALL_TABLES: &[&str] = &[
    "campaigns",
    "ad_sets",
    "ads",
    "account_insights",
    "campaign_insights",
    "ad_set_insights",
    "ad_insights",
];

fn is_insights_table(table: &str) -> bool {
    matches!(
        table,
        "account_insights" | "campaign_insights" | "ad_set_insights" | "ad_insights"
    )
}

// ---------------------------------------------------------------------------
// Field mappings: column name -> API field name
// ---------------------------------------------------------------------------

struct FieldMapping {
    endpoint: &'static str,
    api_fields: &'static [(&'static str, &'static str)],
}

fn field_mapping(table: &str) -> Option<FieldMapping> {
    match table {
        "campaigns" => Some(FieldMapping {
            endpoint: "campaigns",
            api_fields: &[
                ("campaign_id", "id"),
                ("campaign_name", "name"),
                ("status", "effective_status"),
                ("objective", "objective"),
                ("bid_strategy", "bid_strategy"),
                ("daily_budget", "daily_budget"),
                ("lifetime_budget", "lifetime_budget"),
                ("created_time", "created_time"),
                ("updated_time", "updated_time"),
            ],
        }),

        "ad_sets" => Some(FieldMapping {
            endpoint: "adsets",
            api_fields: &[
                ("ad_set_id", "id"),
                ("ad_set_name", "name"),
                ("status", "effective_status"),
                ("campaign_id", "campaign_id"),
                ("optimization_goal", "optimization_goal"),
                ("billing_event", "billing_event"),
                ("daily_budget", "daily_budget"),
                ("lifetime_budget", "lifetime_budget"),
                ("start_time", "start_time"),
                ("end_time", "end_time"),
                ("created_time", "created_time"),
                ("updated_time", "updated_time"),
            ],
        }),

        "ads" => Some(FieldMapping {
            endpoint: "ads",
            api_fields: &[
                ("ad_id", "id"),
                ("ad_name", "name"),
                ("status", "effective_status"),
                ("ad_set_id", "adset_id"),
                ("campaign_id", "campaign_id"),
                ("creative_id", "creative{id}"),
                ("created_time", "created_time"),
                ("updated_time", "updated_time"),
            ],
        }),

        "account_insights" => Some(FieldMapping {
            endpoint: "account",
            api_fields: &[
                ("date", "date_start"),
                ("impressions", "impressions"),
                ("clicks", "clicks"),
                ("spend", "spend"),
                ("cpc", "cpc"),
                ("cpm", "cpm"),
                ("ctr", "ctr"),
                ("reach", "reach"),
                ("frequency", "frequency"),
                ("conversions", "conversions"),
                ("actions", "actions"),
                ("cost_per_action_type", "cost_per_action_type"),
            ],
        }),

        "campaign_insights" => Some(FieldMapping {
            endpoint: "campaign",
            api_fields: &[
                ("campaign_id", "campaign_id"),
                ("campaign_name", "campaign_name"),
                ("date", "date_start"),
                ("impressions", "impressions"),
                ("clicks", "clicks"),
                ("spend", "spend"),
                ("cpc", "cpc"),
                ("cpm", "cpm"),
                ("ctr", "ctr"),
                ("reach", "reach"),
                ("frequency", "frequency"),
                ("conversions", "conversions"),
                ("actions", "actions"),
                ("cost_per_action_type", "cost_per_action_type"),
            ],
        }),

        "ad_set_insights" => Some(FieldMapping {
            endpoint: "adset",
            api_fields: &[
                ("ad_set_id", "adset_id"),
                ("ad_set_name", "adset_name"),
                ("campaign_id", "campaign_id"),
                ("date", "date_start"),
                ("impressions", "impressions"),
                ("clicks", "clicks"),
                ("spend", "spend"),
                ("cpc", "cpc"),
                ("cpm", "cpm"),
                ("ctr", "ctr"),
                ("reach", "reach"),
                ("frequency", "frequency"),
                ("conversions", "conversions"),
                ("actions", "actions"),
                ("cost_per_action_type", "cost_per_action_type"),
            ],
        }),

        "ad_insights" => Some(FieldMapping {
            endpoint: "ad",
            api_fields: &[
                ("ad_id", "ad_id"),
                ("ad_name", "ad_name"),
                ("ad_set_id", "adset_id"),
                ("campaign_id", "campaign_id"),
                ("date", "date_start"),
                ("impressions", "impressions"),
                ("clicks", "clicks"),
                ("spend", "spend"),
                ("cpc", "cpc"),
                ("cpm", "cpm"),
                ("ctr", "ctr"),
                ("reach", "reach"),
                ("frequency", "frequency"),
                ("conversions", "conversions"),
                ("actions", "actions"),
                ("cost_per_action_type", "cost_per_action_type"),
            ],
        }),

        _ => None,
    }
}

#[cfg(test)]
fn column_to_api_field(table: &str, column: &str) -> Option<&'static str> {
    let mapping = field_mapping(table)?;
    mapping
        .api_fields
        .iter()
        .find(|(col, _)| *col == column)
        .map(|(_, api)| *api)
}

// ---------------------------------------------------------------------------
// Predicate pushdown
// ---------------------------------------------------------------------------

fn build_time_range(options: &FetchOptions) -> Option<String> {
    let mut since: Option<String> = None;
    let mut until: Option<String> = None;

    if let (Some(key), Some(val)) = (&options.incremental_key, &options.last_value) {
        if key == "date" {
            since = Some(val.clone());
        }
    }

    for pred in &options.predicates {
        match pred {
            Predicate::Equals { column, value } if column == "date" => {
                since = Some(value.to_string());
                until = Some(value.to_string());
            }
            Predicate::Between { column, low, high } if column == "date" => {
                since = Some(low.to_string());
                until = Some(high.to_string());
            }
            Predicate::GreaterThan {
                column,
                value,
                ..
            } if column == "date" => {
                since = Some(value.to_string());
            }
            Predicate::LessThan {
                column,
                value,
                ..
            } if column == "date" => {
                until = Some(value.to_string());
            }
            _ => {}
        }
    }

    if since.is_none() && until.is_none() {
        return None;
    }

    let s = since.unwrap_or_else(|| "2020-01-01".to_string());
    let u = until.unwrap_or_else(|| {
        chrono::Utc::now().format("%Y-%m-%d").to_string()
    });
    Some(format!("{{\"since\":\"{}\",\"until\":\"{}\"}}", s, u))
}

fn extract_status_predicate(predicates: &[Predicate]) -> Option<Vec<String>> {
    for pred in predicates {
        match pred {
            Predicate::Equals { column, value } if column == "status" => {
                return Some(vec![value.to_string()]);
            }
            Predicate::In { column, values } if column == "status" => {
                return Some(values.iter().map(|v| v.to_string()).collect());
            }
            _ => {}
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Data parsing: JSON -> ColumnBuilders
// ---------------------------------------------------------------------------

fn parse_timestamp_str(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
        .or_else(|| {
            chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%z")
                .ok()
                .map(|dt| dt.timestamp_micros())
        })
}

fn parse_date_to_days(s: &str) -> Option<i32> {
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    Some(
        (date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?).num_days() as i32,
    )
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

    for (idx, ((col_name, api_field), col_schema)) in mapping
        .api_fields
        .iter()
        .zip(schema.columns.iter())
        .enumerate()
    {
        debug_assert_eq!(*col_name, col_schema.name.as_str());

        let raw_val = if *api_field == "creative{id}" {
            item.get("creative").and_then(|c| c.get("id"))
        } else {
            item.get(api_field)
        };

        match col_schema.data_type {
            ColumnType::Int64 => {
                let parsed = raw_val.and_then(|v| {
                    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                });
                builders.builder(idx).append_i64(parsed);
            }
            ColumnType::Float64 => {
                let parsed = raw_val.and_then(|v| {
                    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                });
                builders.builder(idx).append_f64(parsed);
            }
            ColumnType::Date => {
                let parsed = raw_val
                    .and_then(|v| v.as_str())
                    .and_then(parse_date_to_days);
                builders.builder(idx).append_date32(parsed);
            }
            ColumnType::Timestamp => {
                let parsed = raw_val
                    .and_then(|v| v.as_str())
                    .and_then(parse_timestamp_str);
                builders.builder(idx).append_timestamp(parsed);
            }
            ColumnType::Boolean => {
                let parsed = raw_val.and_then(|v| v.as_bool());
                builders.builder(idx).append_bool(parsed);
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
impl Connector for FacebookAdsConnector {
    fn source_type(&self) -> SourceType {
        SourceType::FacebookAds
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            let (supports_incremental, incremental_key) = if is_insights_table(name) {
                (true, Some("date".to_string()))
            } else {
                (true, Some("updated_time".to_string()))
            };

            tables.push(TableInfo {
                name: name.to_string(),
                schema,
                supports_incremental,
                incremental_key,
                estimated_rows: None,
                primary_key_columns: vec![],
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
        let url = format!(
            "{}/{}?fields=name,account_status",
            self.api_base(),
            self.config.ad_account_id,
        );
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
    use compact_str::CompactString;

    fn test_config() -> FacebookAdsConfig {
        FacebookAdsConfig::new("test-access-token", "1234567890")
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
    fn test_campaigns_schema() {
        let schema = get_table_schema("campaigns").unwrap();
        assert_eq!(schema.columns.len(), 9);
        assert_eq!(schema.columns[0].name, "campaign_id");
        assert_eq!(schema.columns[0].data_type, ColumnType::String);
        assert!(!schema.columns[0].nullable);
    }

    #[test]
    fn test_insights_schema_has_metrics() {
        let schema = get_table_schema("campaign_insights").unwrap();
        let date_col = schema.columns.iter().find(|c| c.name == "date").unwrap();
        assert_eq!(date_col.data_type, ColumnType::Date);

        let spend_col = schema.columns.iter().find(|c| c.name == "spend").unwrap();
        assert_eq!(spend_col.data_type, ColumnType::Float64);

        let actions_col = schema.columns.iter().find(|c| c.name == "actions").unwrap();
        assert_eq!(actions_col.data_type, ColumnType::String);
        assert!(actions_col.nullable);
    }

    #[test]
    fn test_is_insights_table() {
        assert!(is_insights_table("account_insights"));
        assert!(is_insights_table("campaign_insights"));
        assert!(is_insights_table("ad_set_insights"));
        assert!(is_insights_table("ad_insights"));
        assert!(!is_insights_table("campaigns"));
        assert!(!is_insights_table("ad_sets"));
        assert!(!is_insights_table("ads"));
    }

    // -- Config tests --

    #[test]
    fn test_config_debug_redacts_secrets() {
        let config = test_config();
        let debug = format!("{:?}", config);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("test-access-token"));
        assert!(debug.contains("act_1234567890"));
    }

    #[test]
    fn test_config_prepends_act_prefix() {
        let config = FacebookAdsConfig::new("token", "1234567890");
        assert_eq!(config.ad_account_id, "act_1234567890");
    }

    #[test]
    fn test_config_does_not_double_prefix() {
        let config = FacebookAdsConfig::new("token", "act_1234567890");
        assert_eq!(config.ad_account_id, "act_1234567890");
    }

    #[test]
    fn test_config_with_app_credentials() {
        let config = test_config().with_app_credentials("app-123", "app-secret");
        assert_eq!(config.app_id.as_deref(), Some("app-123"));
        assert!(config.app_secret.is_some());
    }

    // -- Field mapping tests --

    #[test]
    fn test_all_tables_have_field_mappings() {
        for &table in ALL_TABLES {
            let mapping = field_mapping(table);
            assert!(
                mapping.is_some(),
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
                mapping.api_fields.len(),
                "Column count mismatch for table '{}': schema has {}, mapping has {}",
                table,
                schema.columns.len(),
                mapping.api_fields.len()
            );
        }
    }

    #[test]
    fn test_schema_and_mapping_column_names_aligned() {
        for &table in ALL_TABLES {
            let schema = get_table_schema(table).unwrap();
            let mapping = field_mapping(table).unwrap();
            for (i, (col, (mapping_name, _))) in
                schema.columns.iter().zip(mapping.api_fields.iter()).enumerate()
            {
                assert_eq!(
                    col.name, *mapping_name,
                    "Column name mismatch at index {} for table '{}': schema='{}', mapping='{}'",
                    i, table, col.name, mapping_name
                );
            }
        }
    }

    #[test]
    fn test_column_to_api_field() {
        assert_eq!(column_to_api_field("campaigns", "campaign_id"), Some("id"));
        assert_eq!(column_to_api_field("campaigns", "status"), Some("effective_status"));
        assert_eq!(column_to_api_field("ad_sets", "campaign_id"), Some("campaign_id"));
        assert_eq!(column_to_api_field("campaign_insights", "date"), Some("date_start"));
        assert_eq!(column_to_api_field("campaigns", "nonexistent"), None);
        assert_eq!(column_to_api_field("unknown_table", "id"), None);
    }

    // -- Time range / predicate tests --

    #[test]
    fn test_build_time_range_from_incremental() {
        let opts = FetchOptions::incremental("date", "2024-01-15");
        let tr = build_time_range(&opts).unwrap();
        assert!(tr.contains("\"since\":\"2024-01-15\""));
    }

    #[test]
    fn test_build_time_range_from_equals_predicate() {
        let opts = FetchOptions {
            predicates: vec![Predicate::Equals {
                column: CompactString::from("date"),
                value: CompactString::from("2024-03-01"),
            }],
            ..Default::default()
        };
        let tr = build_time_range(&opts).unwrap();
        assert!(tr.contains("\"since\":\"2024-03-01\""));
        assert!(tr.contains("\"until\":\"2024-03-01\""));
    }

    #[test]
    fn test_build_time_range_from_between_predicate() {
        let opts = FetchOptions {
            predicates: vec![Predicate::Between {
                column: CompactString::from("date"),
                low: CompactString::from("2024-01-01"),
                high: CompactString::from("2024-01-31"),
            }],
            ..Default::default()
        };
        let tr = build_time_range(&opts).unwrap();
        assert!(tr.contains("\"since\":\"2024-01-01\""));
        assert!(tr.contains("\"until\":\"2024-01-31\""));
    }

    #[test]
    fn test_build_time_range_none_when_no_date() {
        let opts = FetchOptions::default();
        assert!(build_time_range(&opts).is_none());
    }

    #[test]
    fn test_extract_status_predicate_equals() {
        let predicates = vec![Predicate::Equals {
            column: CompactString::from("status"),
            value: CompactString::from("ACTIVE"),
        }];
        let result = extract_status_predicate(&predicates).unwrap();
        assert_eq!(result, vec!["ACTIVE"]);
    }

    #[test]
    fn test_extract_status_predicate_in() {
        let predicates = vec![Predicate::In {
            column: CompactString::from("status"),
            values: vec![
                CompactString::from("ACTIVE"),
                CompactString::from("PAUSED"),
            ],
        }];
        let result = extract_status_predicate(&predicates).unwrap();
        assert_eq!(result, vec!["ACTIVE", "PAUSED"]);
    }

    #[test]
    fn test_extract_status_predicate_none() {
        let predicates = vec![Predicate::Equals {
            column: CompactString::from("objective"),
            value: CompactString::from("CONVERSIONS"),
        }];
        assert!(extract_status_predicate(&predicates).is_none());
    }

    // -- Date/timestamp parsing tests --

    #[test]
    fn test_parse_date_to_days() {
        let days = parse_date_to_days("2024-01-15").unwrap();
        let expected =
            (chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
                - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        assert_eq!(days, expected);
    }

    #[test]
    fn test_parse_date_invalid() {
        assert!(parse_date_to_days("not-a-date").is_none());
        assert!(parse_date_to_days("").is_none());
    }

    #[test]
    fn test_parse_timestamp_rfc3339() {
        let ts = parse_timestamp_str("2024-01-15T10:30:00+0000");
        assert!(ts.is_some());
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        assert!(parse_timestamp_str("not-a-timestamp").is_none());
        assert!(parse_timestamp_str("").is_none());
    }

    // -- list_tables tests --

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = FacebookAdsConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), ALL_TABLES.len());

        let insights_tables: Vec<_> = tables
            .iter()
            .filter(|t| t.supports_incremental && t.incremental_key.as_deref() == Some("date"))
            .collect();
        assert_eq!(insights_tables.len(), 4);

        let entity_tables: Vec<_> = tables
            .iter()
            .filter(|t| t.incremental_key.as_deref() == Some("updated_time"))
            .collect();
        assert_eq!(entity_tables.len(), 3);
    }

    // -- OAuth token exchange tests --

    #[tokio::test]
    async fn test_token_exchange_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"/oauth/access_token.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"access_token":"long-lived-token-abc","token_type":"bearer","expires_in":5184000}"#)
            .create_async()
            .await;

        let config = test_config().with_app_credentials("app-id", "app-secret");
        let tm = TokenManager {
            http: reqwest::Client::new(),
            config,
            cached: RwLock::new(None),
            api_base_override: Some(server.url()),
        };

        let (token, expires_at) = tm.exchange_token_http().await.unwrap();
        assert_eq!(token, "long-lived-token-abc");
        assert!(expires_at.is_some());

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_token_exchange_failure() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"/oauth/access_token.*".to_string()))
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":{"message":"Invalid OAuth access token.","type":"OAuthException","code":190}}"#)
            .create_async()
            .await;

        let config = test_config().with_app_credentials("app-id", "app-secret");
        let tm = TokenManager {
            http: reqwest::Client::new(),
            config,
            cached: RwLock::new(None),
            api_base_override: Some(server.url()),
        };

        let result = tm.exchange_token_http().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectorError::Authentication(msg) => {
                assert!(msg.contains("Invalid OAuth access token"));
            }
            other => panic!("Expected Authentication error, got: {:?}", other),
        }

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_access_token_returns_cached() {
        let config = test_config();
        let tm = TokenManager::new(&config);

        *tm.cached.write().await = Some(CachedToken {
            access_token: "cached-token".to_string(),
            expires_at: None,
        });

        let token = tm.get_access_token().await.unwrap();
        assert_eq!(token, "cached-token");
    }

    // -- Validate credentials test --

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"/act_1234567890\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"name":"Test Account","account_status":1,"id":"act_1234567890"}"#)
            .create_async()
            .await;

        let config = test_config();
        let tm = Arc::new(TokenManager::new(&config));
        *tm.cached.write().await = Some(CachedToken {
            access_token: "test-token".to_string(),
            expires_at: None,
        });

        let connector = FacebookAdsConnector {
            config,
            http: reqwest::Client::new(),
            token_manager: tm,
            base_url_override: Some(server.url()),
        };

        connector.validate_credentials().await.unwrap();
        mock.assert_async().await;
    }

    // -- Pagination test --

    #[tokio::test]
    async fn test_fetch_paginated_two_pages() {
        let mut server = mockito::Server::new_async().await;

        let page2_url = format!("{}/page2", server.url());

        let _mock1 = server
            .mock("GET", mockito::Matcher::Regex(r"/act_\d+/campaigns\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::json!({
                "data": [
                    {"id": "1", "name": "Campaign 1", "effective_status": "ACTIVE"},
                    {"id": "2", "name": "Campaign 2", "effective_status": "PAUSED"},
                ],
                "paging": {
                    "cursors": {"after": "cursor1"},
                    "next": page2_url,
                }
            }).to_string())
            .create_async()
            .await;

        let _mock2 = server
            .mock("GET", mockito::Matcher::Regex(r"/page2.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::json!({
                "data": [
                    {"id": "3", "name": "Campaign 3", "effective_status": "ACTIVE"},
                ],
                "paging": {
                    "cursors": {"after": "cursor2"},
                }
            }).to_string())
            .create_async()
            .await;

        let config = test_config();
        let tm = Arc::new(TokenManager::new(&config));
        *tm.cached.write().await = Some(CachedToken {
            access_token: "test-token".to_string(),
            expires_at: None,
        });

        let connector = FacebookAdsConnector {
            config,
            http: reqwest::Client::new(),
            token_manager: tm,
            base_url_override: Some(server.url()),
        };

        let schema = get_table_schema("campaigns").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let url = format!(
            "{}/act_1234567890/campaigns?fields=id,name&limit=250",
            server.url(),
        );
        let batches = connector
            .fetch_paginated_batches(&url, "campaigns", &schema, arrow_schema, None)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    // -- Rate limit test --

    #[tokio::test]
    async fn test_api_get_retries_on_429() {
        let mut server = mockito::Server::new_async().await;

        let mock_429 = server
            .mock("GET", mockito::Matcher::Regex(r"/test-endpoint.*".to_string()))
            .with_status(429)
            .expect(4)
            .create_async()
            .await;

        let config = test_config();
        let tm = Arc::new(TokenManager::new(&config));
        *tm.cached.write().await = Some(CachedToken {
            access_token: "test-token".to_string(),
            expires_at: None,
        });

        let connector = FacebookAdsConnector {
            config,
            http: reqwest::Client::new(),
            token_manager: tm,
            base_url_override: Some(server.url()),
        };

        let result = connector
            .api_get(&format!("{}/test-endpoint", server.url()))
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectorError::RateLimited { retry_after_secs } => {
                assert_eq!(retry_after_secs, 60);
            }
            other => panic!("Expected RateLimited error, got: {:?}", other),
        }

        mock_429.assert_async().await;
    }
}
