use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::{OnceCell, RwLock};
use tokio_stream::StreamExt;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::codegen::InterceptedService;

use googleads_rs::google::ads::googleads::v23::services::{
    google_ads_service_client::GoogleAdsServiceClient,
    GoogleAdsRow, SearchGoogleAdsStreamRequest,
};

use crate::crypto::SecretString;
use crate::warehouse::connectors::builders::ColumnBuilders;
use crate::warehouse::connectors::{
    ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::query::predicate_pushdown::Predicate;
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const GOOGLE_ADS_ENDPOINT: &str = "https://googleads.googleapis.com:443";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";
const BATCH_CAPACITY: usize = 4096;

#[derive(Clone)]
pub struct GoogleAdsConfig {
    pub developer_token: SecretString,
    pub client_id: String,
    pub client_secret: SecretString,
    pub refresh_token: SecretString,
    pub customer_id: String,
    pub login_customer_id: Option<String>,
}

impl std::fmt::Debug for GoogleAdsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleAdsConfig")
            .field("client_id", &self.client_id)
            .field("customer_id", &self.customer_id)
            .field("login_customer_id", &self.login_customer_id)
            .field("developer_token", &"[REDACTED]")
            .field("client_secret", &"[REDACTED]")
            .field("refresh_token", &"[REDACTED]")
            .finish()
    }
}

impl GoogleAdsConfig {
    pub fn new(
        developer_token: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        refresh_token: impl Into<String>,
        customer_id: impl Into<String>,
    ) -> Self {
        Self {
            developer_token: SecretString::new(developer_token.into()),
            client_id: client_id.into(),
            client_secret: SecretString::new(client_secret.into()),
            refresh_token: SecretString::new(refresh_token.into()),
            customer_id: customer_id.into(),
            login_customer_id: None,
        }
    }

    pub fn with_login_customer_id(mut self, id: impl Into<String>) -> Self {
        self.login_customer_id = Some(id.into());
        self
    }
}

// ---------------------------------------------------------------------------
// OAuth2 token management
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

struct TokenManager {
    http: reqwest::Client,
    client_id: String,
    client_secret: SecretString,
    refresh_token: SecretString,
    cached: RwLock<Option<CachedToken>>,
    #[cfg(test)]
    token_endpoint_override: Option<String>,
}

impl TokenManager {
    fn new(config: &GoogleAdsConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            client_id: config.client_id.clone(),
            client_secret: config.client_secret.clone(),
            refresh_token: config.refresh_token.clone(),
            cached: RwLock::new(None),
            #[cfg(test)]
            token_endpoint_override: None,
        }
    }

    fn token_endpoint(&self) -> &str {
        #[cfg(test)]
        if let Some(ref url) = self.token_endpoint_override {
            return url.as_str();
        }
        TOKEN_ENDPOINT
    }

    async fn get_access_token(&self) -> ConnectorResult<String> {
        {
            let guard = self.cached.read().await;
            if let Some(ref t) = *guard {
                if t.expires_at > Instant::now() + Duration::from_secs(60) {
                    return Ok(t.access_token.clone());
                }
            }
        }
        self.refresh().await
    }

    async fn refresh(&self) -> ConnectorResult<String> {
        let mut guard = self.cached.write().await;

        if let Some(ref t) = *guard {
            if t.expires_at > Instant::now() + Duration::from_secs(60) {
                return Ok(t.access_token.clone());
            }
        }

        let resp = self
            .http
            .post(self.token_endpoint())
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", &self.client_id),
                ("client_secret", self.client_secret.expose()),
                ("refresh_token", self.refresh_token.expose()),
            ])
            .send()
            .await
            .map_err(|e| ConnectorError::Network(format!("OAuth2 token refresh failed: {}", e)))?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.map_err(|e| {
            ConnectorError::Network(format!("Failed to parse OAuth2 response: {}", e))
        })?;

        if !status.is_success() {
            let err_desc = body["error_description"]
                .as_str()
                .or_else(|| body["error"].as_str())
                .unwrap_or("unknown error");
            return Err(ConnectorError::Authentication(format!(
                "OAuth2 token refresh failed ({}): {}",
                status, err_desc
            )));
        }

        let access_token = body["access_token"]
            .as_str()
            .ok_or_else(|| {
                ConnectorError::Authentication(
                    "OAuth2 response missing access_token".to_string(),
                )
            })?
            .to_string();

        let expires_in = body["expires_in"].as_u64().unwrap_or(3600);

        *guard = Some(CachedToken {
            access_token: access_token.clone(),
            expires_at: Instant::now() + Duration::from_secs(expires_in),
        });

        Ok(access_token)
    }
}

// ---------------------------------------------------------------------------
// gRPC interceptor
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct GoogleAdsInterceptor {
    auth_token: MetadataValue<Ascii>,
    dev_token: MetadataValue<Ascii>,
    login_customer_id: Option<MetadataValue<Ascii>>,
}

impl Interceptor for GoogleAdsInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.auth_token.clone());
        request
            .metadata_mut()
            .insert("developer-token", self.dev_token.clone());
        if let Some(ref id) = self.login_customer_id {
            request
                .metadata_mut()
                .insert("login-customer-id", id.clone());
        }
        Ok(request)
    }
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct GoogleAdsConnector {
    config: GoogleAdsConfig,
    token_manager: Arc<TokenManager>,
    channel: OnceCell<Channel>,
}

impl GoogleAdsConnector {
    pub fn new(config: GoogleAdsConfig) -> Self {
        let token_manager = Arc::new(TokenManager::new(&config));
        Self {
            config,
            token_manager,
            channel: OnceCell::new(),
        }
    }

    async fn get_channel(&self) -> ConnectorResult<Channel> {
        self.channel
            .get_or_try_init(|| async {
                let tls_config = tonic::transport::ClientTlsConfig::new().with_native_roots();
                Channel::from_static(GOOGLE_ADS_ENDPOINT)
                    .tls_config(tls_config)
                    .map_err(|e| ConnectorError::Config(format!("TLS config error: {}", e)))?
                    .connect()
                    .await
                    .map_err(|e| {
                        ConnectorError::Network(format!(
                            "Failed to connect to Google Ads API: {}",
                            e
                        ))
                    })
            })
            .await
            .cloned()
    }

    async fn create_client(
        &self,
    ) -> ConnectorResult<
        GoogleAdsServiceClient<InterceptedService<Channel, GoogleAdsInterceptor>>,
    > {
        let access_token = self.token_manager.get_access_token().await?;
        let channel = self.get_channel().await?;

        let bearer = format!("Bearer {}", access_token);
        let interceptor = GoogleAdsInterceptor {
            auth_token: MetadataValue::try_from(&bearer).map_err(|e| {
                ConnectorError::Authentication(format!("Invalid auth token: {}", e))
            })?,
            dev_token: MetadataValue::try_from(self.config.developer_token.expose())
                .map_err(|e| {
                    ConnectorError::Authentication(format!("Invalid developer token: {}", e))
                })?,
            login_customer_id: self
                .config
                .login_customer_id
                .as_ref()
                .map(|id| MetadataValue::try_from(id.as_str()))
                .transpose()
                .map_err(|e| {
                    ConnectorError::Config(format!("Invalid login-customer-id: {}", e))
                })?,
        };

        Ok(GoogleAdsServiceClient::with_interceptor(
            channel,
            interceptor,
        ))
    }

    async fn execute_gaql(
        &self,
        query: &str,
    ) -> ConnectorResult<Vec<GoogleAdsRow>> {
        let mut client = self.create_client().await?;

        let response = client
            .search_stream(SearchGoogleAdsStreamRequest {
                customer_id: self.config.customer_id.clone(),
                query: query.to_string(),
                summary_row_setting: 0,
            })
            .await
            .map_err(map_grpc_error)?;

        let mut stream = response.into_inner();
        let mut all_rows = Vec::new();

        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    all_rows.extend(chunk.results);
                }
                Err(status) => {
                    return Err(ConnectorError::Internal(format!(
                        "Stream error: {}",
                        status.message()
                    )));
                }
            }
        }

        Ok(all_rows)
    }

    async fn execute_gaql_batched(
        &self,
        query: &str,
        table: &str,
        schema: &TableSchema,
        max_rows: Option<usize>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut client = self.create_client().await?;

        let response = client
            .search_stream(SearchGoogleAdsStreamRequest {
                customer_id: self.config.customer_id.clone(),
                query: query.to_string(),
                summary_row_setting: 0,
            })
            .await
            .map_err(map_grpc_error)?;

        let mut stream = response.into_inner();
        let arrow_schema = Arc::new(to_arrow_schema(schema));
        let limit = max_rows.unwrap_or(usize::MAX);
        let mut batches = Vec::new();
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut total = 0usize;

        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    for row in &chunk.results {
                        if total >= limit {
                            break;
                        }
                        append_row(row, table, schema, &mut builders);
                        total += 1;

                        if builders.len() >= BATCH_CAPACITY {
                            batches.push(builders.finish(arrow_schema.clone())?);
                            builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                        }
                    }
                    if total >= limit {
                        break;
                    }
                }
                Err(status) => {
                    return Err(ConnectorError::Internal(format!(
                        "Stream error: {}",
                        status.message()
                    )));
                }
            }
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema)?);
        }

        Ok(batches)
    }
}

fn map_grpc_error(status: tonic::Status) -> ConnectorError {
    let msg = status.message().to_string();
    if msg.contains("UNAUTHENTICATED") || msg.contains("PERMISSION_DENIED") {
        ConnectorError::Authentication(format!("Google Ads API auth error: {}", msg))
    } else if msg.contains("RESOURCE_EXHAUSTED") {
        ConnectorError::RateLimited {
            retry_after_secs: 60,
        }
    } else {
        ConnectorError::Internal(format!("Google Ads API error: {}", msg))
    }
}

// ---------------------------------------------------------------------------
// Table schemas
// ---------------------------------------------------------------------------

fn get_table_schema(table: &str) -> Option<TableSchema> {
    match table {
        "campaigns" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("campaign_id", ColumnType::Int64, false),
                ColumnSchema::new("campaign_name", ColumnType::String, false),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("advertising_channel_type", ColumnType::String, true),
                ColumnSchema::new("bidding_strategy_type", ColumnType::String, true),
                ColumnSchema::new("budget_amount_micros", ColumnType::Int64, true),
                ColumnSchema::new("start_date", ColumnType::String, true),
                ColumnSchema::new("end_date", ColumnType::String, true),
                ColumnSchema::new("serving_status", ColumnType::String, true),
            ],
        }),

        "ad_groups" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("ad_group_id", ColumnType::Int64, false),
                ColumnSchema::new("ad_group_name", ColumnType::String, false),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("campaign_id", ColumnType::Int64, false),
                ColumnSchema::new("ad_group_type", ColumnType::String, true),
                ColumnSchema::new("cpc_bid_micros", ColumnType::Int64, true),
            ],
        }),

        "ads" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("ad_id", ColumnType::Int64, false),
                ColumnSchema::new("ad_group_id", ColumnType::Int64, false),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("ad_type", ColumnType::String, true),
                ColumnSchema::new("ad_name", ColumnType::String, true),
                ColumnSchema::new("headlines", ColumnType::String, true),
                ColumnSchema::new("descriptions", ColumnType::String, true),
            ],
        }),

        "keywords" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("criterion_id", ColumnType::Int64, false),
                ColumnSchema::new("ad_group_id", ColumnType::Int64, false),
                ColumnSchema::new("keyword_text", ColumnType::String, true),
                ColumnSchema::new("match_type", ColumnType::String, true),
                ColumnSchema::new("status", ColumnType::String, false),
                ColumnSchema::new("cpc_bid_micros", ColumnType::Int64, true),
            ],
        }),

        "campaign_metrics" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("campaign_id", ColumnType::Int64, false),
                ColumnSchema::new("campaign_name", ColumnType::String, false),
                ColumnSchema::new("date", ColumnType::Date, false),
                ColumnSchema::new("impressions", ColumnType::Int64, false),
                ColumnSchema::new("clicks", ColumnType::Int64, false),
                ColumnSchema::new("cost_micros", ColumnType::Int64, false),
                ColumnSchema::new("conversions", ColumnType::Float64, true),
                ColumnSchema::new("conversions_value", ColumnType::Float64, true),
                ColumnSchema::new("ctr", ColumnType::Float64, true),
                ColumnSchema::new("average_cpc", ColumnType::Float64, true),
                ColumnSchema::new("average_cpm", ColumnType::Float64, true),
                ColumnSchema::new("interactions", ColumnType::Int64, true),
            ],
        }),

        "ad_group_metrics" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("ad_group_id", ColumnType::Int64, false),
                ColumnSchema::new("ad_group_name", ColumnType::String, false),
                ColumnSchema::new("campaign_id", ColumnType::Int64, false),
                ColumnSchema::new("date", ColumnType::Date, false),
                ColumnSchema::new("impressions", ColumnType::Int64, false),
                ColumnSchema::new("clicks", ColumnType::Int64, false),
                ColumnSchema::new("cost_micros", ColumnType::Int64, false),
                ColumnSchema::new("conversions", ColumnType::Float64, true),
                ColumnSchema::new("conversions_value", ColumnType::Float64, true),
                ColumnSchema::new("ctr", ColumnType::Float64, true),
                ColumnSchema::new("average_cpc", ColumnType::Float64, true),
                ColumnSchema::new("interactions", ColumnType::Int64, true),
            ],
        }),

        "ad_metrics" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("ad_id", ColumnType::Int64, false),
                ColumnSchema::new("ad_group_id", ColumnType::Int64, false),
                ColumnSchema::new("campaign_id", ColumnType::Int64, false),
                ColumnSchema::new("date", ColumnType::Date, false),
                ColumnSchema::new("impressions", ColumnType::Int64, false),
                ColumnSchema::new("clicks", ColumnType::Int64, false),
                ColumnSchema::new("cost_micros", ColumnType::Int64, false),
                ColumnSchema::new("conversions", ColumnType::Float64, true),
                ColumnSchema::new("conversions_value", ColumnType::Float64, true),
                ColumnSchema::new("ctr", ColumnType::Float64, true),
                ColumnSchema::new("average_cpc", ColumnType::Float64, true),
                ColumnSchema::new("interactions", ColumnType::Int64, true),
            ],
        }),

        "keyword_metrics" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("criterion_id", ColumnType::Int64, false),
                ColumnSchema::new("ad_group_id", ColumnType::Int64, false),
                ColumnSchema::new("campaign_id", ColumnType::Int64, false),
                ColumnSchema::new("keyword_text", ColumnType::String, true),
                ColumnSchema::new("match_type", ColumnType::String, true),
                ColumnSchema::new("date", ColumnType::Date, false),
                ColumnSchema::new("impressions", ColumnType::Int64, false),
                ColumnSchema::new("clicks", ColumnType::Int64, false),
                ColumnSchema::new("cost_micros", ColumnType::Int64, false),
                ColumnSchema::new("conversions", ColumnType::Float64, true),
                ColumnSchema::new("conversions_value", ColumnType::Float64, true),
                ColumnSchema::new("ctr", ColumnType::Float64, true),
                ColumnSchema::new("average_cpc", ColumnType::Float64, true),
                ColumnSchema::new("interactions", ColumnType::Int64, true),
            ],
        }),

        _ => None,
    }
}

const ALL_TABLES: &[&str] = &[
    "campaigns",
    "ad_groups",
    "ads",
    "keywords",
    "campaign_metrics",
    "ad_group_metrics",
    "ad_metrics",
    "keyword_metrics",
];

#[cfg(test)]
fn is_metrics_table(table: &str) -> bool {
    matches!(
        table,
        "campaign_metrics" | "ad_group_metrics" | "ad_metrics" | "keyword_metrics"
    )
}

// ---------------------------------------------------------------------------
// GAQL query building
// ---------------------------------------------------------------------------

struct GaqlFieldMapping {
    gaql_resource: &'static str,
    field_mappings: &'static [(&'static str, &'static str)],
}

fn gaql_mapping(table: &str) -> Option<GaqlFieldMapping> {
    match table {
        "campaigns" => Some(GaqlFieldMapping {
            gaql_resource: "campaign",
            field_mappings: &[
                ("campaign_id", "campaign.id"),
                ("campaign_name", "campaign.name"),
                ("status", "campaign.status"),
                ("advertising_channel_type", "campaign.advertising_channel_type"),
                ("bidding_strategy_type", "campaign.bidding_strategy_type"),
                ("budget_amount_micros", "campaign_budget.amount_micros"),
                ("start_date", "campaign.start_date_time"),
                ("end_date", "campaign.end_date_time"),
                ("serving_status", "campaign.serving_status"),
            ],
        }),

        "ad_groups" => Some(GaqlFieldMapping {
            gaql_resource: "ad_group",
            field_mappings: &[
                ("ad_group_id", "ad_group.id"),
                ("ad_group_name", "ad_group.name"),
                ("status", "ad_group.status"),
                ("campaign_id", "campaign.id"),
                ("ad_group_type", "ad_group.type"),
                ("cpc_bid_micros", "ad_group.cpc_bid_micros"),
            ],
        }),

        "ads" => Some(GaqlFieldMapping {
            gaql_resource: "ad_group_ad",
            field_mappings: &[
                ("ad_id", "ad_group_ad.ad.id"),
                ("ad_group_id", "ad_group.id"),
                ("status", "ad_group_ad.status"),
                ("ad_type", "ad_group_ad.ad.type"),
                ("ad_name", "ad_group_ad.ad.name"),
                ("headlines", "ad_group_ad.ad.responsive_search_ad.headlines"),
                ("descriptions", "ad_group_ad.ad.responsive_search_ad.descriptions"),
            ],
        }),

        "keywords" => Some(GaqlFieldMapping {
            gaql_resource: "ad_group_criterion",
            field_mappings: &[
                ("criterion_id", "ad_group_criterion.criterion_id"),
                ("ad_group_id", "ad_group.id"),
                ("keyword_text", "ad_group_criterion.keyword.text"),
                ("match_type", "ad_group_criterion.keyword.match_type"),
                ("status", "ad_group_criterion.status"),
                ("cpc_bid_micros", "ad_group_criterion.cpc_bid_micros"),
            ],
        }),

        "campaign_metrics" => Some(GaqlFieldMapping {
            gaql_resource: "campaign",
            field_mappings: &[
                ("campaign_id", "campaign.id"),
                ("campaign_name", "campaign.name"),
                ("date", "segments.date"),
                ("impressions", "metrics.impressions"),
                ("clicks", "metrics.clicks"),
                ("cost_micros", "metrics.cost_micros"),
                ("conversions", "metrics.conversions"),
                ("conversions_value", "metrics.conversions_value"),
                ("ctr", "metrics.ctr"),
                ("average_cpc", "metrics.average_cpc"),
                ("average_cpm", "metrics.average_cpm"),
                ("interactions", "metrics.interactions"),
            ],
        }),

        "ad_group_metrics" => Some(GaqlFieldMapping {
            gaql_resource: "ad_group",
            field_mappings: &[
                ("ad_group_id", "ad_group.id"),
                ("ad_group_name", "ad_group.name"),
                ("campaign_id", "campaign.id"),
                ("date", "segments.date"),
                ("impressions", "metrics.impressions"),
                ("clicks", "metrics.clicks"),
                ("cost_micros", "metrics.cost_micros"),
                ("conversions", "metrics.conversions"),
                ("conversions_value", "metrics.conversions_value"),
                ("ctr", "metrics.ctr"),
                ("average_cpc", "metrics.average_cpc"),
                ("interactions", "metrics.interactions"),
            ],
        }),

        "ad_metrics" => Some(GaqlFieldMapping {
            gaql_resource: "ad_group_ad",
            field_mappings: &[
                ("ad_id", "ad_group_ad.ad.id"),
                ("ad_group_id", "ad_group.id"),
                ("campaign_id", "campaign.id"),
                ("date", "segments.date"),
                ("impressions", "metrics.impressions"),
                ("clicks", "metrics.clicks"),
                ("cost_micros", "metrics.cost_micros"),
                ("conversions", "metrics.conversions"),
                ("conversions_value", "metrics.conversions_value"),
                ("ctr", "metrics.ctr"),
                ("average_cpc", "metrics.average_cpc"),
                ("interactions", "metrics.interactions"),
            ],
        }),

        "keyword_metrics" => Some(GaqlFieldMapping {
            gaql_resource: "keyword_view",
            field_mappings: &[
                ("criterion_id", "ad_group_criterion.criterion_id"),
                ("ad_group_id", "ad_group.id"),
                ("campaign_id", "campaign.id"),
                ("keyword_text", "ad_group_criterion.keyword.text"),
                ("match_type", "ad_group_criterion.keyword.match_type"),
                ("date", "segments.date"),
                ("impressions", "metrics.impressions"),
                ("clicks", "metrics.clicks"),
                ("cost_micros", "metrics.cost_micros"),
                ("conversions", "metrics.conversions"),
                ("conversions_value", "metrics.conversions_value"),
                ("ctr", "metrics.ctr"),
                ("average_cpc", "metrics.average_cpc"),
                ("interactions", "metrics.interactions"),
            ],
        }),

        _ => None,
    }
}

/// Map our column name to the GAQL field path used in predicates.
fn column_to_gaql_field(table: &str, column: &str) -> Option<&'static str> {
    let mapping = gaql_mapping(table)?;
    mapping
        .field_mappings
        .iter()
        .find(|(col, _)| *col == column)
        .map(|(_, gaql)| *gaql)
}

fn build_gaql_query(table: &str, options: &FetchOptions) -> ConnectorResult<String> {
    let mapping = gaql_mapping(table).ok_or_else(|| {
        ConnectorError::TableNotFound(format!("Unknown table: {}", table))
    })?;

    let select_fields: Vec<&str> = mapping
        .field_mappings
        .iter()
        .map(|(_, gaql)| *gaql)
        .collect();

    let mut query = format!(
        "SELECT {} FROM {}",
        select_fields.join(", "),
        mapping.gaql_resource
    );

    let mut where_clauses = Vec::new();

    if let (Some(key), Some(val)) = (&options.incremental_key, &options.last_value) {
        if let Some(gaql_field) = column_to_gaql_field(table, key) {
            where_clauses.push(format!("{} >= '{}'", gaql_field, escape_gaql_value(val)));
        }
    }

    let predicate_clauses = predicates_to_gaql(table, &options.predicates);
    where_clauses.extend(predicate_clauses);

    if table == "keywords" {
        where_clauses.push(
            "ad_group_criterion.type = 'KEYWORD'".to_string(),
        );
    }

    if !where_clauses.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&where_clauses.join(" AND "));
    }

    if let Some(max) = options.max_rows {
        query.push_str(&format!(" LIMIT {}", max));
    }

    Ok(query)
}

fn predicates_to_gaql(table: &str, predicates: &[Predicate]) -> Vec<String> {
    let mut clauses = Vec::new();
    for pred in predicates {
        if let Some(clause) = predicate_to_gaql_clause(table, pred) {
            clauses.push(clause);
        }
    }
    clauses
}

fn escape_gaql_value(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

fn predicate_to_gaql_clause(table: &str, pred: &Predicate) -> Option<String> {
    match pred {
        Predicate::Equals { column, value } => {
            let field = column_to_gaql_field(table, column)?;
            Some(format!("{} = '{}'", field, escape_gaql_value(value)))
        }
        Predicate::In { column, values } => {
            let field = column_to_gaql_field(table, column)?;
            let vals: Vec<String> = values
                .iter()
                .map(|v| format!("'{}'", escape_gaql_value(v)))
                .collect();
            Some(format!("{} IN ({})", field, vals.join(", ")))
        }
        Predicate::GreaterThan {
            column,
            value,
            inclusive,
        } => {
            let field = column_to_gaql_field(table, column)?;
            let op = if *inclusive { ">=" } else { ">" };
            Some(format!("{} {} '{}'", field, op, escape_gaql_value(value)))
        }
        Predicate::LessThan {
            column,
            value,
            inclusive,
        } => {
            let field = column_to_gaql_field(table, column)?;
            let op = if *inclusive { "<=" } else { "<" };
            Some(format!("{} {} '{}'", field, op, escape_gaql_value(value)))
        }
        Predicate::Between {
            column,
            low,
            high,
        } => {
            let field = column_to_gaql_field(table, column)?;
            Some(format!(
                "{} BETWEEN '{}' AND '{}'",
                field,
                escape_gaql_value(low),
                escape_gaql_value(high)
            ))
        }
        Predicate::IsNull { column, is_null } => {
            let field = column_to_gaql_field(table, column)?;
            if *is_null {
                Some(format!("{} IS NULL", field))
            } else {
                Some(format!("{} IS NOT NULL", field))
            }
        }
        Predicate::And(preds) => {
            let subs: Vec<String> = preds
                .iter()
                .filter_map(|p| predicate_to_gaql_clause(table, p))
                .collect();
            if subs.is_empty() {
                None
            } else {
                Some(subs.join(" AND "))
            }
        }
        Predicate::Not(inner) => {
            let clause = predicate_to_gaql_clause(table, inner)?;
            Some(format!("NOT ({})", clause))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Row parsing: GoogleAdsRow -> ColumnBuilders
// ---------------------------------------------------------------------------

fn parse_date_to_days(s: &str) -> Option<i32> {
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    Some(
        (date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?).num_days() as i32,
    )
}

fn parse_timestamp_str(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .or_else(|| {
            chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%z").ok()
        })
        .map(|dt| dt.timestamp_micros())
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|ndt| ndt.and_utc().timestamp_micros())
        })
}

fn append_row(
    row: &GoogleAdsRow,
    table: &str,
    schema: &TableSchema,
    builders: &mut ColumnBuilders,
) {
    let mapping = match gaql_mapping(table) {
        Some(m) => m,
        None => return,
    };

    for (idx, ((_col_name, gaql_path), col_schema)) in mapping
        .field_mappings
        .iter()
        .zip(schema.columns.iter())
        .enumerate()
    {
        let raw = row.get(gaql_path);
        let raw = raw.trim().trim_matches('"');
        let is_empty =
            raw.is_empty() || raw == "--" || raw == "N/A" || raw == "not implemented by googleads-rs";

        match col_schema.data_type {
            ColumnType::Int64 => {
                if is_empty {
                    builders.builder(idx).append_null();
                } else {
                    builders
                        .builder(idx)
                        .append_i64(raw.parse::<i64>().ok());
                }
            }
            ColumnType::Float64 => {
                if is_empty {
                    builders.builder(idx).append_null();
                } else {
                    builders
                        .builder(idx)
                        .append_f64(raw.parse::<f64>().ok());
                }
            }
            ColumnType::Date => {
                if is_empty {
                    builders.builder(idx).append_null();
                } else {
                    builders
                        .builder(idx)
                        .append_date32(parse_date_to_days(raw));
                }
            }
            ColumnType::Timestamp => {
                if is_empty {
                    builders.builder(idx).append_null();
                } else {
                    builders
                        .builder(idx)
                        .append_timestamp(parse_timestamp_str(raw));
                }
            }
            ColumnType::Boolean => {
                if is_empty {
                    builders.builder(idx).append_null();
                } else {
                    builders.builder(idx).append_bool(Some(
                        raw.eq_ignore_ascii_case("true")
                            || raw.eq_ignore_ascii_case("enabled"),
                    ));
                }
            }
            _ => {
                if is_empty && col_schema.nullable {
                    builders.builder(idx).append_null();
                } else {
                    builders.builder(idx).append_string(Some(raw));
                }
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
impl crate::warehouse::connectors::Connector for GoogleAdsConnector {
    fn source_type(&self) -> SourceType {
        SourceType::GoogleAds
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            let (supports_incremental, incremental_key) = match name {
                "campaign_metrics" | "ad_group_metrics" | "ad_metrics" | "keyword_metrics" => {
                    (true, Some("date".to_string()))
                }
                _ => (false, None),
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
            let schema = get_table_schema(table).ok_or_else(|| {
                ConnectorError::TableNotFound(format!("Unknown table: {}", table))
            })?;

            let query = build_gaql_query(table, &options)?;
            let batches =
                self.execute_gaql_batched(&query, table, &schema, options.max_rows).await?;

            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        let query = "SELECT customer.id FROM customer LIMIT 1";
        self.execute_gaql(query).await?;
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
    use crate::warehouse::connectors::Connector;

    fn test_config() -> GoogleAdsConfig {
        GoogleAdsConfig::new(
            "test-dev-token",
            "test-client-id",
            "test-client-secret",
            "test-refresh-token",
            "1234567890",
        )
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
        assert_eq!(schema.columns[0].data_type, ColumnType::Int64);
        assert!(!schema.columns[0].nullable);
        let budget_col = schema
            .columns
            .iter()
            .find(|c| c.name == "budget_amount_micros")
            .unwrap();
        assert_eq!(budget_col.data_type, ColumnType::Int64);
        assert!(budget_col.nullable);
    }

    #[test]
    fn test_campaign_metrics_schema_has_date() {
        let schema = get_table_schema("campaign_metrics").unwrap();
        let date_col = schema.columns.iter().find(|c| c.name == "date").unwrap();
        assert_eq!(date_col.data_type, ColumnType::Date);
        assert!(!date_col.nullable);
    }

    #[test]
    fn test_is_metrics_table() {
        assert!(is_metrics_table("campaign_metrics"));
        assert!(is_metrics_table("ad_group_metrics"));
        assert!(is_metrics_table("ad_metrics"));
        assert!(is_metrics_table("keyword_metrics"));
        assert!(!is_metrics_table("campaigns"));
        assert!(!is_metrics_table("ads"));
    }

    // -- Config tests --

    #[test]
    fn test_config_debug_redacts_secrets() {
        let config = test_config();
        let debug = format!("{:?}", config);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("test-dev-token"));
        assert!(!debug.contains("test-client-secret"));
        assert!(!debug.contains("test-refresh-token"));
        assert!(debug.contains("test-client-id"));
        assert!(debug.contains("1234567890"));
    }

    #[test]
    fn test_config_with_login_customer_id() {
        let config = test_config().with_login_customer_id("9876543210");
        assert_eq!(config.login_customer_id.as_deref(), Some("9876543210"));
    }

    // -- GAQL query building tests --

    #[test]
    fn test_build_gaql_campaigns() {
        let opts = FetchOptions::default();
        let q = build_gaql_query("campaigns", &opts).unwrap();
        assert!(q.starts_with("SELECT"));
        assert!(q.contains("campaign.id"));
        assert!(q.contains("campaign.name"));
        assert!(q.contains("campaign_budget.amount_micros"));
        assert!(q.contains("FROM campaign"));
        assert!(!q.contains("WHERE"));
    }

    #[test]
    fn test_build_gaql_campaign_metrics() {
        let opts = FetchOptions::default();
        let q = build_gaql_query("campaign_metrics", &opts).unwrap();
        assert!(q.contains("metrics.impressions"));
        assert!(q.contains("metrics.clicks"));
        assert!(q.contains("segments.date"));
        assert!(q.contains("FROM campaign"));
    }

    #[test]
    fn test_build_gaql_keyword_view() {
        let opts = FetchOptions::default();
        let q = build_gaql_query("keyword_metrics", &opts).unwrap();
        assert!(q.contains("FROM keyword_view"));
        assert!(q.contains("metrics.impressions"));
    }

    #[test]
    fn test_build_gaql_keywords_filter() {
        let opts = FetchOptions::default();
        let q = build_gaql_query("keywords", &opts).unwrap();
        assert!(q.contains("ad_group_criterion.type = 'KEYWORD'"));
    }

    #[test]
    fn test_build_gaql_incremental() {
        let opts = FetchOptions::incremental("date", "2024-01-15");
        let q = build_gaql_query("campaign_metrics", &opts).unwrap();
        assert!(q.contains("segments.date >= '2024-01-15'"));
    }

    #[test]
    fn test_build_gaql_with_max_rows() {
        let opts = FetchOptions {
            max_rows: Some(100),
            ..Default::default()
        };
        let q = build_gaql_query("campaigns", &opts).unwrap();
        assert!(q.contains("LIMIT 100"));
    }

    #[test]
    fn test_build_gaql_unknown_table() {
        let opts = FetchOptions::default();
        let result = build_gaql_query("nonexistent", &opts);
        assert!(result.is_err());
    }

    // -- Predicate pushdown tests --

    #[test]
    fn test_predicate_equals() {
        let preds = vec![Predicate::Equals {
            column: CompactString::from("status"),
            value: CompactString::from("ENABLED"),
        }];
        let clauses = predicates_to_gaql("campaigns", &preds);
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0], "campaign.status = 'ENABLED'");
    }

    #[test]
    fn test_predicate_in() {
        let preds = vec![Predicate::In {
            column: CompactString::from("status"),
            values: vec![
                CompactString::from("ENABLED"),
                CompactString::from("PAUSED"),
            ],
        }];
        let clauses = predicates_to_gaql("campaigns", &preds);
        assert_eq!(clauses.len(), 1);
        assert_eq!(
            clauses[0],
            "campaign.status IN ('ENABLED', 'PAUSED')"
        );
    }

    #[test]
    fn test_predicate_greater_than_inclusive() {
        let preds = vec![Predicate::GreaterThan {
            column: CompactString::from("date"),
            value: CompactString::from("2024-01-01"),
            inclusive: true,
        }];
        let clauses = predicates_to_gaql("campaign_metrics", &preds);
        assert_eq!(clauses[0], "segments.date >= '2024-01-01'");
    }

    #[test]
    fn test_predicate_between() {
        let preds = vec![Predicate::Between {
            column: CompactString::from("date"),
            low: CompactString::from("2024-01-01"),
            high: CompactString::from("2024-01-31"),
        }];
        let clauses = predicates_to_gaql("campaign_metrics", &preds);
        assert_eq!(
            clauses[0],
            "segments.date BETWEEN '2024-01-01' AND '2024-01-31'"
        );
    }

    #[test]
    fn test_predicate_unknown_column_ignored() {
        let preds = vec![Predicate::Equals {
            column: CompactString::from("nonexistent_col"),
            value: CompactString::from("foo"),
        }];
        let clauses = predicates_to_gaql("campaigns", &preds);
        assert!(clauses.is_empty());
    }

    #[test]
    fn test_predicate_not() {
        let preds = vec![Predicate::Not(Box::new(Predicate::Equals {
            column: CompactString::from("status"),
            value: CompactString::from("REMOVED"),
        }))];
        let clauses = predicates_to_gaql("campaigns", &preds);
        assert_eq!(clauses[0], "NOT (campaign.status = 'REMOVED')");
    }

    #[test]
    fn test_predicate_is_null() {
        let preds = vec![Predicate::IsNull {
            column: CompactString::from("end_date"),
            is_null: true,
        }];
        let clauses = predicates_to_gaql("campaigns", &preds);
        assert_eq!(clauses[0], "campaign.end_date_time IS NULL");
    }

    #[test]
    fn test_multiple_predicates_combined() {
        let preds = vec![
            Predicate::Equals {
                column: CompactString::from("campaign_name"),
                value: CompactString::from("My Campaign"),
            },
            Predicate::GreaterThan {
                column: CompactString::from("date"),
                value: CompactString::from("2024-01-01"),
                inclusive: true,
            },
        ];
        let opts = FetchOptions {
            predicates: preds,
            ..Default::default()
        };
        let q = build_gaql_query("campaign_metrics", &opts).unwrap();
        assert!(q.contains("campaign.name = 'My Campaign'"));
        assert!(q.contains("segments.date >= '2024-01-01'"));
        assert!(q.contains(" AND "));
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
        let ts = parse_timestamp_str("2024-01-15T10:30:00+00:00");
        assert!(ts.is_some());
    }

    #[test]
    fn test_parse_timestamp_naive() {
        let ts = parse_timestamp_str("2024-01-15 10:30:00");
        assert!(ts.is_some());
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        assert!(parse_timestamp_str("not-a-timestamp").is_none());
        assert!(parse_timestamp_str("").is_none());
    }

    // -- GAQL field mapping tests --

    #[test]
    fn test_column_to_gaql_field() {
        assert_eq!(
            column_to_gaql_field("campaigns", "campaign_id"),
            Some("campaign.id")
        );
        assert_eq!(
            column_to_gaql_field("campaigns", "status"),
            Some("campaign.status")
        );
        assert_eq!(
            column_to_gaql_field("campaign_metrics", "date"),
            Some("segments.date")
        );
        assert_eq!(
            column_to_gaql_field("campaign_metrics", "impressions"),
            Some("metrics.impressions")
        );
        assert_eq!(
            column_to_gaql_field("keywords", "keyword_text"),
            Some("ad_group_criterion.keyword.text")
        );
        assert_eq!(column_to_gaql_field("campaigns", "nonexistent"), None);
        assert_eq!(column_to_gaql_field("unknown_table", "status"), None);
    }

    // -- Mapping completeness tests --

    #[test]
    fn test_all_tables_have_gaql_mappings() {
        for &table in ALL_TABLES {
            let mapping = gaql_mapping(table);
            assert!(
                mapping.is_some(),
                "Missing GAQL mapping for table: {}",
                table
            );
        }
    }

    #[test]
    fn test_schema_and_mapping_column_count_match() {
        for &table in ALL_TABLES {
            let schema = get_table_schema(table).unwrap();
            let mapping = gaql_mapping(table).unwrap();
            assert_eq!(
                schema.columns.len(),
                mapping.field_mappings.len(),
                "Column count mismatch for table '{}': schema has {}, mapping has {}",
                table,
                schema.columns.len(),
                mapping.field_mappings.len()
            );
        }
    }

    #[test]
    fn test_schema_and_mapping_column_names_aligned() {
        for &table in ALL_TABLES {
            let schema = get_table_schema(table).unwrap();
            let mapping = gaql_mapping(table).unwrap();
            for (i, (col, (mapping_name, _))) in
                schema.columns.iter().zip(mapping.field_mappings.iter()).enumerate()
            {
                assert_eq!(
                    col.name, *mapping_name,
                    "Column name mismatch at index {} for table '{}': schema='{}', mapping='{}'",
                    i, table, col.name, mapping_name
                );
            }
        }
    }

    // -- list_tables tests --

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = GoogleAdsConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), ALL_TABLES.len());

        let metrics_tables: Vec<_> = tables
            .iter()
            .filter(|t| t.supports_incremental && t.incremental_key.as_deref() == Some("date"))
            .collect();
        assert_eq!(metrics_tables.len(), 4);

        assert!(tables.iter().find(|t| t.name == "change_status").is_none());
        assert!(tables.iter().find(|t| t.name == "campaign_budgets").is_none());

        let campaigns = tables.iter().find(|t| t.name == "campaigns").unwrap();
        assert!(!campaigns.supports_incremental);
    }

    // -- OAuth token refresh tests --

    fn test_token_manager(endpoint: &str) -> TokenManager {
        let config = test_config();
        TokenManager {
            http: reqwest::Client::new(),
            client_id: config.client_id.clone(),
            client_secret: config.client_secret.clone(),
            refresh_token: config.refresh_token.clone(),
            cached: RwLock::new(None),
            token_endpoint_override: Some(endpoint.to_string()),
        }
    }

    #[tokio::test]
    async fn test_token_refresh_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"access_token":"ya29.test-token","expires_in":3600,"token_type":"Bearer"}"#,
            )
            .create_async()
            .await;

        let tm = test_token_manager(&format!("{}/token", server.url()));
        let token = tm.refresh().await.unwrap();
        assert_eq!(token, "ya29.test-token");

        let cached = tm.cached.read().await;
        assert!(cached.is_some());
        assert_eq!(cached.as_ref().unwrap().access_token, "ya29.test-token");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_token_refresh_auth_failure() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/token")
            .with_status(401)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"invalid_grant","error_description":"Token has been revoked."}"#)
            .create_async()
            .await;

        let tm = test_token_manager(&format!("{}/token", server.url()));
        let result = tm.refresh().await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            ConnectorError::Authentication(msg) => {
                assert!(msg.contains("Token has been revoked."));
            }
            other => panic!("Expected Authentication error, got: {:?}", other),
        }

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_access_token_refreshes_when_expired() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"access_token":"ya29.fresh-token","expires_in":3600,"token_type":"Bearer"}"#,
            )
            .create_async()
            .await;

        let tm = test_token_manager(&format!("{}/token", server.url()));

        let token = tm.get_access_token().await.unwrap();
        assert_eq!(token, "ya29.fresh-token");

        mock.assert_async().await;
    }

    // -- Token caching tests --

    #[tokio::test]
    async fn test_token_cache_returns_cached() {
        let config = test_config();
        let tm = TokenManager::new(&config);

        let cached = CachedToken {
            access_token: "cached-token-abc".to_string(),
            expires_at: Instant::now() + Duration::from_secs(3600),
        };
        *tm.cached.write().await = Some(cached);

        let token = tm.get_access_token().await.unwrap();
        assert_eq!(token, "cached-token-abc");
    }

    // -- Ad group and ads GAQL tests --

    #[test]
    fn test_build_gaql_ad_groups() {
        let opts = FetchOptions::default();
        let q = build_gaql_query("ad_groups", &opts).unwrap();
        assert!(q.contains("ad_group.id"));
        assert!(q.contains("ad_group.name"));
        assert!(q.contains("campaign.id"));
        assert!(q.contains("FROM ad_group"));
    }

    #[test]
    fn test_build_gaql_ads() {
        let opts = FetchOptions::default();
        let q = build_gaql_query("ads", &opts).unwrap();
        assert!(q.contains("ad_group_ad.ad.id"));
        assert!(q.contains("ad_group_ad.ad.type"));
        assert!(q.contains("FROM ad_group_ad"));
    }

    #[test]
    fn test_gaql_escape_value() {
        assert_eq!(escape_gaql_value("O'Brien"), "O\\'Brien");
        assert_eq!(escape_gaql_value("normal"), "normal");
        assert_eq!(escape_gaql_value("it's a 'test'"), "it\\'s a \\'test\\'");
        assert_eq!(escape_gaql_value("back\\slash"), "back\\\\slash");
    }

    #[test]
    fn test_predicate_with_quote_escaped() {
        let preds = vec![Predicate::Equals {
            column: CompactString::from("campaign_name"),
            value: CompactString::from("O'Brien's Campaign"),
        }];
        let clauses = predicates_to_gaql("campaigns", &preds);
        assert_eq!(clauses.len(), 1);
        assert_eq!(
            clauses[0],
            "campaign.name = 'O\\'Brien\\'s Campaign'"
        );
    }
}
