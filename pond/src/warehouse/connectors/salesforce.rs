//! Salesforce CRM connector for the data warehouse.
//!
//! Syncs Salesforce CRM data (accounts, contacts, leads, opportunities, etc.)
//! to the warehouse. Supports three API strategies:
//!
//! - **REST API + SOQL**: Synchronous queries returning JSON, used for
//!   incremental sync, cold-tier queries, and small full syncs.
//! - **Bulk API 2.0**: Asynchronous jobs returning CSV, used for full sync
//!   of large objects (>10K rows).
//! - **Composite Batch API**: Batches up to 25 subrequests in one HTTP call,
//!   used for schema discovery and count queries.
//!
//! Uses OAuth 2.0 for authentication via the existing `OAuthConfig` infrastructure.

use super::http_api::{AuthConfig, HttpApiClient};
use super::oauth::OAuthConfig;
use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::query::predicate_pushdown::Predicate;
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use super::builders::ColumnBuilders;
use arrow::csv::ReaderBuilder as CsvReaderBuilder;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_util::io::StreamReader;
use tracing::{debug, info, warn};

const DEFAULT_API_VERSION: &str = "v62.0";
const MAX_TOTAL_ROWS: usize = 5_000_000;
const BATCH_THRESHOLD: usize = 1_000;
const BULK_ROW_THRESHOLD: usize = 10_000;
const BULK_POLL_INITIAL_MS: u64 = 1_000;
const BULK_POLL_MAX_MS: u64 = 30_000;
const BULK_POLL_MAX_DURATION_SECS: u64 = 1_800;
const BULK_CSV_BATCH_SIZE: usize = 8_192;
const DESCRIBE_CACHE_TTL_SECS: u64 = 300;
const COMPOSITE_BATCH_LIMIT: usize = 25;

// ═══════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Salesforce connector configuration.
#[derive(Clone)]
pub struct SalesforceConfig {
    pub oauth: Arc<OAuthConfig>,
    pub instance_url: String,
    pub api_version: String,
}

impl std::fmt::Debug for SalesforceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SalesforceConfig")
            .field("oauth", &"***REDACTED***")
            .field("instance_url", &self.instance_url)
            .field("api_version", &self.api_version)
            .finish()
    }
}

impl SalesforceConfig {
    pub fn new(oauth: OAuthConfig, instance_url: impl Into<String>) -> Self {
        Self {
            oauth: Arc::new(oauth),
            instance_url: instance_url.into().trim_end_matches('/').to_string(),
            api_version: DEFAULT_API_VERSION.to_string(),
        }
    }

    pub fn with_api_version(mut self, version: impl Into<String>) -> Self {
        self.api_version = version.into();
        self
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Describe cache
// ═══════════════════════════════════════════════════════════════════════════

struct CachedDescribe {
    schema: TableSchema,
    fields: Vec<String>,
    has_system_modstamp: bool,
    fetched_at: Instant,
}

impl CachedDescribe {
    fn is_expired(&self) -> bool {
        self.fetched_at.elapsed().as_secs() > DESCRIBE_CACHE_TTL_SECS
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Connector
// ═══════════════════════════════════════════════════════════════════════════

/// Salesforce CRM data source connector.
pub struct SalesforceConnector {
    config: SalesforceConfig,
    client: HttpApiClient,
    describe_cache: Arc<RwLock<HashMap<String, CachedDescribe>>>,
}

impl SalesforceConnector {
    const STANDARD_TABLES: &'static [&'static str] = &[
        "accounts",
        "contacts",
        "leads",
        "opportunities",
        "cases",
        "tasks",
        "events",
        "users",
        "campaigns",
    ];

    pub fn new(config: SalesforceConfig) -> Self {
        let client = HttpApiClient::new(&config.instance_url)
            .with_auth(AuthConfig::OAuth(config.oauth.clone()))
            .with_rate_limit(100, std::time::Duration::from_secs(20));

        Self {
            config,
            client,
            describe_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Construct the services path prefix: `/services/data/v62.0`
    fn api_path(&self) -> String {
        format!("/services/data/{}", self.config.api_version)
    }

    // ───────────────────────────────────────────────────────────────────
    // Table name <-> SObject mapping
    // ───────────────────────────────────────────────────────────────────

    /// Map our lowercase table name to the Salesforce SObject API name.
    fn table_to_sobject(table: &str) -> &str {
        match table {
            "accounts" => "Account",
            "contacts" => "Contact",
            "leads" => "Lead",
            "opportunities" => "Opportunity",
            "cases" => "Case",
            "tasks" => "Task",
            "events" => "Event",
            "users" => "User",
            "campaigns" => "Campaign",
            _ => table,
        }
    }

    /// Map a Salesforce SObject API name to our lowercase table name.
    fn sobject_to_table(sobject: &str) -> String {
        match sobject {
            "Account" => "accounts".to_string(),
            "Contact" => "contacts".to_string(),
            "Lead" => "leads".to_string(),
            "Opportunity" => "opportunities".to_string(),
            "Case" => "cases".to_string(),
            "Task" => "tasks".to_string(),
            "Event" => "events".to_string(),
            "User" => "users".to_string(),
            "Campaign" => "campaigns".to_string(),
            other => other.to_lowercase(),
        }
    }

    // ───────────────────────────────────────────────────────────────────
    // Static schemas for 9 standard objects
    // ───────────────────────────────────────────────────────────────────

    fn get_static_schema(table: &str) -> Option<TableSchema> {
        let columns = match table {
            "accounts" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Account ID"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Account name"),
                ColumnSchema::new("industry", ColumnType::String, true)
                    .with_description("Industry sector"),
                ColumnSchema::new("type", ColumnType::String, true)
                    .with_description("Account type"),
                ColumnSchema::new("phone", ColumnType::String, true)
                    .with_description("Phone number"),
                ColumnSchema::new("website", ColumnType::String, true)
                    .with_description("Website URL"),
                ColumnSchema::new("billingcity", ColumnType::String, true)
                    .with_description("Billing city"),
                ColumnSchema::new("billingstate", ColumnType::String, true)
                    .with_description("Billing state"),
                ColumnSchema::new("billingcountry", ColumnType::String, true)
                    .with_description("Billing country"),
                ColumnSchema::new("annualrevenue", ColumnType::Float64, true)
                    .with_description("Annual revenue"),
                ColumnSchema::new("numberofemployees", ColumnType::Int64, true)
                    .with_description("Number of employees"),
                ColumnSchema::new("ownerid", ColumnType::String, true)
                    .with_description("Owner user ID"),
                ColumnSchema::new("createddate", ColumnType::Timestamp, true)
                    .with_description("Created timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Last modified timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("systemmodstamp", ColumnType::Timestamp, true)
                    .with_description("System modification timestamp")
                    .with_timezone("UTC"),
            ],
            "contacts" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Contact ID"),
                ColumnSchema::new("firstname", ColumnType::String, true)
                    .with_description("First name"),
                ColumnSchema::new("lastname", ColumnType::String, true)
                    .with_description("Last name"),
                ColumnSchema::new("email", ColumnType::String, true)
                    .with_description("Email address"),
                ColumnSchema::new("phone", ColumnType::String, true)
                    .with_description("Phone number"),
                ColumnSchema::new("accountid", ColumnType::String, true)
                    .with_description("Parent account ID"),
                ColumnSchema::new("title", ColumnType::String, true)
                    .with_description("Job title"),
                ColumnSchema::new("department", ColumnType::String, true)
                    .with_description("Department"),
                ColumnSchema::new("mailingcity", ColumnType::String, true)
                    .with_description("Mailing city"),
                ColumnSchema::new("mailingstate", ColumnType::String, true)
                    .with_description("Mailing state"),
                ColumnSchema::new("mailingcountry", ColumnType::String, true)
                    .with_description("Mailing country"),
                ColumnSchema::new("ownerid", ColumnType::String, true)
                    .with_description("Owner user ID"),
                ColumnSchema::new("createddate", ColumnType::Timestamp, true)
                    .with_description("Created timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Last modified timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("systemmodstamp", ColumnType::Timestamp, true)
                    .with_description("System modification timestamp")
                    .with_timezone("UTC"),
            ],
            "leads" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Lead ID"),
                ColumnSchema::new("firstname", ColumnType::String, true)
                    .with_description("First name"),
                ColumnSchema::new("lastname", ColumnType::String, true)
                    .with_description("Last name"),
                ColumnSchema::new("email", ColumnType::String, true)
                    .with_description("Email address"),
                ColumnSchema::new("phone", ColumnType::String, true)
                    .with_description("Phone number"),
                ColumnSchema::new("company", ColumnType::String, true)
                    .with_description("Company name"),
                ColumnSchema::new("title", ColumnType::String, true)
                    .with_description("Job title"),
                ColumnSchema::new("status", ColumnType::String, true)
                    .with_description("Lead status"),
                ColumnSchema::new("leadsource", ColumnType::String, true)
                    .with_description("Lead source"),
                ColumnSchema::new("industry", ColumnType::String, true)
                    .with_description("Industry"),
                ColumnSchema::new("ownerid", ColumnType::String, true)
                    .with_description("Owner user ID"),
                ColumnSchema::new("createddate", ColumnType::Timestamp, true)
                    .with_description("Created timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Last modified timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("systemmodstamp", ColumnType::Timestamp, true)
                    .with_description("System modification timestamp")
                    .with_timezone("UTC"),
            ],
            "opportunities" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Opportunity ID"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Opportunity name"),
                ColumnSchema::new("amount", ColumnType::Float64, true)
                    .with_description("Deal amount"),
                ColumnSchema::new("stagename", ColumnType::String, true)
                    .with_description("Stage name"),
                ColumnSchema::new("probability", ColumnType::Float64, true)
                    .with_description("Win probability"),
                ColumnSchema::new("closedate", ColumnType::Date, true)
                    .with_description("Expected close date"),
                ColumnSchema::new("accountid", ColumnType::String, true)
                    .with_description("Parent account ID"),
                ColumnSchema::new("ownerid", ColumnType::String, true)
                    .with_description("Owner user ID"),
                ColumnSchema::new("type", ColumnType::String, true)
                    .with_description("Opportunity type"),
                ColumnSchema::new("leadsource", ColumnType::String, true)
                    .with_description("Lead source"),
                ColumnSchema::new("createddate", ColumnType::Timestamp, true)
                    .with_description("Created timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Last modified timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("systemmodstamp", ColumnType::Timestamp, true)
                    .with_description("System modification timestamp")
                    .with_timezone("UTC"),
            ],
            "cases" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Case ID"),
                ColumnSchema::new("subject", ColumnType::String, true)
                    .with_description("Subject"),
                ColumnSchema::new("description", ColumnType::String, true)
                    .with_description("Description"),
                ColumnSchema::new("status", ColumnType::String, true)
                    .with_description("Case status"),
                ColumnSchema::new("priority", ColumnType::String, true)
                    .with_description("Priority"),
                ColumnSchema::new("origin", ColumnType::String, true)
                    .with_description("Case origin"),
                ColumnSchema::new("accountid", ColumnType::String, true)
                    .with_description("Parent account ID"),
                ColumnSchema::new("contactid", ColumnType::String, true)
                    .with_description("Contact ID"),
                ColumnSchema::new("ownerid", ColumnType::String, true)
                    .with_description("Owner user ID"),
                ColumnSchema::new("createddate", ColumnType::Timestamp, true)
                    .with_description("Created timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Last modified timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("systemmodstamp", ColumnType::Timestamp, true)
                    .with_description("System modification timestamp")
                    .with_timezone("UTC"),
            ],
            "tasks" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Task ID"),
                ColumnSchema::new("subject", ColumnType::String, true)
                    .with_description("Subject"),
                ColumnSchema::new("status", ColumnType::String, true)
                    .with_description("Task status"),
                ColumnSchema::new("priority", ColumnType::String, true)
                    .with_description("Priority"),
                ColumnSchema::new("activitydate", ColumnType::Date, true)
                    .with_description("Activity date"),
                ColumnSchema::new("whoid", ColumnType::String, true)
                    .with_description("Related contact/lead ID"),
                ColumnSchema::new("whatid", ColumnType::String, true)
                    .with_description("Related object ID"),
                ColumnSchema::new("ownerid", ColumnType::String, true)
                    .with_description("Owner user ID"),
                ColumnSchema::new("createddate", ColumnType::Timestamp, true)
                    .with_description("Created timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Last modified timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("systemmodstamp", ColumnType::Timestamp, true)
                    .with_description("System modification timestamp")
                    .with_timezone("UTC"),
            ],
            "events" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Event ID"),
                ColumnSchema::new("subject", ColumnType::String, true)
                    .with_description("Subject"),
                ColumnSchema::new("startdatetime", ColumnType::Timestamp, true)
                    .with_description("Start date/time")
                    .with_timezone("UTC"),
                ColumnSchema::new("enddatetime", ColumnType::Timestamp, true)
                    .with_description("End date/time")
                    .with_timezone("UTC"),
                ColumnSchema::new("whoid", ColumnType::String, true)
                    .with_description("Related contact/lead ID"),
                ColumnSchema::new("whatid", ColumnType::String, true)
                    .with_description("Related object ID"),
                ColumnSchema::new("ownerid", ColumnType::String, true)
                    .with_description("Owner user ID"),
                ColumnSchema::new("createddate", ColumnType::Timestamp, true)
                    .with_description("Created timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Last modified timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("systemmodstamp", ColumnType::Timestamp, true)
                    .with_description("System modification timestamp")
                    .with_timezone("UTC"),
            ],
            "users" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("User ID"),
                ColumnSchema::new("username", ColumnType::String, true)
                    .with_description("Username"),
                ColumnSchema::new("email", ColumnType::String, true)
                    .with_description("Email address"),
                ColumnSchema::new("firstname", ColumnType::String, true)
                    .with_description("First name"),
                ColumnSchema::new("lastname", ColumnType::String, true)
                    .with_description("Last name"),
                ColumnSchema::new("isactive", ColumnType::Boolean, true)
                    .with_description("Is active"),
                ColumnSchema::new("createddate", ColumnType::Timestamp, true)
                    .with_description("Created timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Last modified timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("systemmodstamp", ColumnType::Timestamp, true)
                    .with_description("System modification timestamp")
                    .with_timezone("UTC"),
            ],
            "campaigns" => vec![
                ColumnSchema::new("id", ColumnType::String, false)
                    .with_description("Campaign ID"),
                ColumnSchema::new("name", ColumnType::String, true)
                    .with_description("Campaign name"),
                ColumnSchema::new("type", ColumnType::String, true)
                    .with_description("Campaign type"),
                ColumnSchema::new("status", ColumnType::String, true)
                    .with_description("Campaign status"),
                ColumnSchema::new("startdate", ColumnType::Date, true)
                    .with_description("Start date"),
                ColumnSchema::new("enddate", ColumnType::Date, true)
                    .with_description("End date"),
                ColumnSchema::new("budgetedcost", ColumnType::Float64, true)
                    .with_description("Budgeted cost"),
                ColumnSchema::new("actualcost", ColumnType::Float64, true)
                    .with_description("Actual cost"),
                ColumnSchema::new("numberofleads", ColumnType::Int64, true)
                    .with_description("Number of leads"),
                ColumnSchema::new("numberofcontacts", ColumnType::Int64, true)
                    .with_description("Number of contacts"),
                ColumnSchema::new("createddate", ColumnType::Timestamp, true)
                    .with_description("Created timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("lastmodifieddate", ColumnType::Timestamp, true)
                    .with_description("Last modified timestamp")
                    .with_timezone("UTC"),
                ColumnSchema::new("systemmodstamp", ColumnType::Timestamp, true)
                    .with_description("System modification timestamp")
                    .with_timezone("UTC"),
            ],
            _ => return None,
        };
        Some(TableSchema { columns })
    }

    /// Get the SOQL field names for a static table schema.
    fn static_fields_for(table: &str) -> Option<Vec<&'static str>> {
        match table {
            "accounts" => Some(vec![
                "Id", "Name", "Industry", "Type", "Phone", "Website",
                "BillingCity", "BillingState", "BillingCountry",
                "AnnualRevenue", "NumberOfEmployees", "OwnerId",
                "CreatedDate", "LastModifiedDate", "SystemModstamp",
            ]),
            "contacts" => Some(vec![
                "Id", "FirstName", "LastName", "Email", "Phone",
                "AccountId", "Title", "Department",
                "MailingCity", "MailingState", "MailingCountry", "OwnerId",
                "CreatedDate", "LastModifiedDate", "SystemModstamp",
            ]),
            "leads" => Some(vec![
                "Id", "FirstName", "LastName", "Email", "Phone",
                "Company", "Title", "Status", "LeadSource", "Industry", "OwnerId",
                "CreatedDate", "LastModifiedDate", "SystemModstamp",
            ]),
            "opportunities" => Some(vec![
                "Id", "Name", "Amount", "StageName", "Probability",
                "CloseDate", "AccountId", "OwnerId", "Type", "LeadSource",
                "CreatedDate", "LastModifiedDate", "SystemModstamp",
            ]),
            "cases" => Some(vec![
                "Id", "Subject", "Description", "Status", "Priority",
                "Origin", "AccountId", "ContactId", "OwnerId",
                "CreatedDate", "LastModifiedDate", "SystemModstamp",
            ]),
            "tasks" => Some(vec![
                "Id", "Subject", "Status", "Priority", "ActivityDate",
                "WhoId", "WhatId", "OwnerId",
                "CreatedDate", "LastModifiedDate", "SystemModstamp",
            ]),
            "events" => Some(vec![
                "Id", "Subject", "StartDateTime", "EndDateTime",
                "WhoId", "WhatId", "OwnerId",
                "CreatedDate", "LastModifiedDate", "SystemModstamp",
            ]),
            "users" => Some(vec![
                "Id", "Username", "Email", "FirstName", "LastName", "IsActive",
                "CreatedDate", "LastModifiedDate", "SystemModstamp",
            ]),
            "campaigns" => Some(vec![
                "Id", "Name", "Type", "Status", "StartDate", "EndDate",
                "BudgetedCost", "ActualCost", "NumberOfLeads", "NumberOfContacts",
                "CreatedDate", "LastModifiedDate", "SystemModstamp",
            ]),
            _ => None,
        }
    }

    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), col.nullable))
            .collect();
        Schema::new(fields)
    }

    // ───────────────────────────────────────────────────────────────────
    // REST API SOQL execution
    // ───────────────────────────────────────────────────────────────────

    /// Execute a SOQL query via the REST API and return all records as JSON.
    /// Handles pagination via `nextRecordsUrl`.
    async fn fetch_soql(
        &self,
        soql: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let api = self.api_path();
        let encoded = urlencoding::encode(soql);
        let first_url = format!("{}/query/?q={}", api, encoded);

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
        let mut total_rows: usize = 0;
        let mut next_url: Option<String> = Some(first_url);

        while let Some(url) = next_url.take() {
            let response: serde_json::Value = self.client.get(&url).await?;

            let records = response
                .get("records")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    ConnectorError::Internal(
                        "Invalid Salesforce query response: missing 'records'".to_string(),
                    )
                })?;

            if records.is_empty() {
                break;
            }

            for record in records {
                Self::append_sf_record(record, schema, &mut builders);
            }
            total_rows += records.len();

            if builders.len() >= BATCH_THRESHOLD {
                let batch = builders.finish(arrow_schema.clone())?;
                batches.push(batch);
                builders = ColumnBuilders::new(schema, BATCH_THRESHOLD);
            }

            let done = response
                .get("done")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            if done || total_rows >= MAX_TOTAL_ROWS {
                break;
            }

            next_url = response
                .get("nextRecordsUrl")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
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

    /// Execute `SELECT COUNT() FROM {SObject}` and return the count.
    async fn fetch_soql_count(&self, sobject: &str) -> ConnectorResult<usize> {
        let soql = format!("SELECT COUNT() FROM {}", sobject);
        let api = self.api_path();
        let encoded = urlencoding::encode(&soql);
        let url = format!("{}/query/?q={}", api, encoded);

        let response: serde_json::Value = self.client.get(&url).await?;
        let count = response
            .get("totalSize")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        Ok(count)
    }

    // ───────────────────────────────────────────────────────────────────
    // Bulk API 2.0
    // ───────────────────────────────────────────────────────────────────

    /// Full sync via Bulk API 2.0: create job -> poll -> download CSV -> parse.
    async fn fetch_bulk(
        &self,
        soql: &str,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let job_id = self.create_bulk_query_job(soql).await?;
        self.poll_bulk_job(&job_id).await?;
        self.download_bulk_results(&job_id, arrow_schema).await
    }

    async fn create_bulk_query_job(&self, soql: &str) -> ConnectorResult<String> {
        let api = self.api_path();
        let body = serde_json::json!({
            "operation": "query",
            "query": soql
        });

        let response: serde_json::Value =
            self.client.post(&format!("{}/jobs/query", api), &body).await?;

        response
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                ConnectorError::Internal("Bulk API: missing job id in response".to_string())
            })
    }

    async fn poll_bulk_job(&self, job_id: &str) -> ConnectorResult<()> {
        let api = self.api_path();
        let url = format!("{}/jobs/query/{}", api, job_id);
        let mut delay_ms = BULK_POLL_INITIAL_MS;
        let started = Instant::now();
        let max_duration = Duration::from_secs(BULK_POLL_MAX_DURATION_SECS);

        loop {
            if started.elapsed() > max_duration {
                return Err(ConnectorError::Internal(format!(
                    "Bulk query job {} timed out after {} minutes",
                    job_id,
                    BULK_POLL_MAX_DURATION_SECS / 60
                )));
            }

            let response: serde_json::Value = self.client.get(&url).await?;
            let state = response
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");

            match state {
                "JobComplete" => return Ok(()),
                "Failed" => {
                    let msg = response
                        .get("errorMessage")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown error");
                    return Err(ConnectorError::Internal(format!(
                        "Bulk query job failed: {}",
                        msg
                    )));
                }
                "Aborted" => {
                    return Err(ConnectorError::Internal(
                        "Bulk query job was aborted".to_string(),
                    ));
                }
                _ => {
                    debug!(state, delay_ms, "Bulk job still in progress, waiting");
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(BULK_POLL_MAX_MS);
                }
            }
        }
    }

    /// Download and stream-parse Bulk API CSV results page by page.
    /// Uses `Sforce-Locator` response header for pagination.
    async fn download_bulk_results(
        &self,
        job_id: &str,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let api = self.api_path();
        let base_url = format!("{}/jobs/query/{}/results", api, job_id);
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut locator: Option<String> = None;

        loop {
            let mut params: Vec<(String, String)> = Vec::new();
            if let Some(ref loc) = locator {
                params.push(("locator".to_string(), loc.clone()));
            }

            let response = self.client.get_streaming(&base_url, &params).await?;

            let next_locator = response
                .headers()
                .get("Sforce-Locator")
                .and_then(|v| v.to_str().ok())
                .filter(|v| !v.is_empty() && *v != "null")
                .map(|s| s.to_string());

            let byte_stream = response
                .bytes_stream()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
            let async_reader = StreamReader::new(byte_stream);

            let batches = tokio::task::spawn_blocking({
                let schema = arrow_schema.clone();
                move || -> ConnectorResult<Vec<RecordBatch>> {
                    let sync_reader = tokio_util::io::SyncIoBridge::new(async_reader);
                    let csv_reader = CsvReaderBuilder::new(schema.clone())
                        .with_header(true)
                        .with_batch_size(BULK_CSV_BATCH_SIZE)
                        .build(sync_reader)
                        .map_err(|e| {
                            ConnectorError::Internal(format!(
                                "Failed to create CSV reader: {}",
                                e
                            ))
                        })?;

                    let mut batches = Vec::new();
                    for result in csv_reader {
                        let batch: RecordBatch = result.map_err(|e| {
                            ConnectorError::Internal(format!(
                                "Failed to parse CSV batch: {}",
                                e
                            ))
                        })?;
                        if batch.num_rows() > 0 {
                            batches.push(batch);
                        }
                    }
                    Ok(batches)
                }
            })
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!("CSV parsing task panicked: {}", e))
            })??;

            all_batches.extend(batches);

            match next_locator {
                Some(loc) => locator = Some(loc),
                None => break,
            }
        }

        if all_batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }

        Ok(all_batches)
    }

    // ───────────────────────────────────────────────────────────────────
    // Dynamic schema discovery
    // ───────────────────────────────────────────────────────────────────

    /// List all queryable SObjects via describe global.
    async fn describe_global(&self) -> ConnectorResult<Vec<SObjectInfo>> {
        let api = self.api_path();
        let response: serde_json::Value =
            self.client.get(&format!("{}/sobjects/", api)).await?;

        let sobjects = response
            .get("sobjects")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                ConnectorError::Internal("Describe global: missing 'sobjects'".to_string())
            })?;

        let mut results = Vec::new();
        for obj in sobjects {
            let queryable = obj.get("queryable").and_then(|v| v.as_bool()).unwrap_or(false);
            if !queryable {
                continue;
            }
            let name = match obj.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let label = obj
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            results.push(SObjectInfo { name, label });
        }
        Ok(results)
    }

    /// Parse field metadata from a describe response JSON array.
    fn parse_describe_fields(fields_json: &[serde_json::Value]) -> DescribeResult {
        let mut fields = Vec::new();
        for f in fields_json {
            let name = match f.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let sf_type = f
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("string")
                .to_string();
            let nillable = f.get("nillable").and_then(|v| v.as_bool()).unwrap_or(true);
            let label = f
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            fields.push(SObjectField {
                name,
                sf_type,
                nillable,
                label,
            });
        }
        let has_system_modstamp = fields.iter().any(|f| f.name == "SystemModstamp");
        DescribeResult {
            fields,
            has_system_modstamp,
        }
    }

    /// Describe a single SObject and return field metadata.
    async fn describe_object(&self, sobject: &str) -> ConnectorResult<DescribeResult> {
        let api = self.api_path();
        let response: serde_json::Value = self
            .client
            .get(&format!("{}/sobjects/{}/describe", api, sobject))
            .await?;

        let fields_json = response
            .get("fields")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                ConnectorError::Internal(format!("Describe {}: missing 'fields'", sobject))
            })?;

        Ok(Self::parse_describe_fields(fields_json))
    }

    /// Batch describe using Composite Batch API: up to 25 per HTTP call.
    async fn batch_describe(
        &self,
        object_names: &[String],
    ) -> ConnectorResult<Vec<(String, DescribeResult)>> {
        let api = self.api_path();
        let mut all_results = Vec::new();

        for chunk in object_names.chunks(COMPOSITE_BATCH_LIMIT) {
            let batch_requests: Vec<serde_json::Value> = chunk
                .iter()
                .map(|name| {
                    serde_json::json!({
                        "method": "GET",
                        "url": format!("{}/sobjects/{}/describe", api, name)
                    })
                })
                .collect();

            let body = serde_json::json!({ "batchRequests": batch_requests });
            let response: serde_json::Value = self
                .client
                .post(&format!("{}/composite/batch", api), &body)
                .await?;

            let results = response
                .get("results")
                .and_then(|v| v.as_array())
                .unwrap_or(&Vec::new())
                .clone();

            for (i, result) in results.iter().enumerate() {
                let status = result
                    .get("statusCode")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(500);
                if status != 200 {
                    if let Some(name) = chunk.get(i) {
                        warn!(sobject = %name, status, "Describe failed in composite batch");
                    }
                    continue;
                }

                let body = match result.get("result") {
                    Some(b) => b,
                    None => continue,
                };

                let fields_json = match body.get("fields").and_then(|v| v.as_array()) {
                    Some(f) => f,
                    None => continue,
                };

                let describe = Self::parse_describe_fields(fields_json);

                if let Some(obj_name) = chunk.get(i) {
                    all_results.push((obj_name.clone(), describe));
                }
            }
        }

        Ok(all_results)
    }

    /// Map a Salesforce field type to our ColumnType.
    fn sf_type_to_column_type(sf_type: &str) -> ColumnType {
        match sf_type {
            "double" | "currency" | "percent" => ColumnType::Float64,
            "int" | "long" => ColumnType::Int64,
            "boolean" => ColumnType::Boolean,
            "datetime" => ColumnType::Timestamp,
            "date" => ColumnType::Date,
            _ => ColumnType::String,
        }
    }

    /// Build a `TableSchema` from a describe result.
    fn build_dynamic_schema(describe: &DescribeResult) -> TableSchema {
        let columns: Vec<ColumnSchema> = describe
            .fields
            .iter()
            .map(|f| {
                let col_type = Self::sf_type_to_column_type(&f.sf_type);
                let mut col = ColumnSchema::new(f.name.to_lowercase(), col_type, f.nillable);
                if !f.label.is_empty() {
                    col = col.with_description(&f.label);
                }
                if matches!(col_type, ColumnType::Timestamp) {
                    col = col.with_timezone("UTC");
                }
                col
            })
            .collect();
        TableSchema { columns }
    }

    /// Get or populate the describe cache for a given SObject.
    async fn get_cached_describe(
        &self,
        sobject: &str,
    ) -> ConnectorResult<(TableSchema, Vec<String>, bool)> {
        {
            let cache = self.describe_cache.read().await;
            if let Some(cached) = cache.get(sobject) {
                if !cached.is_expired() {
                    return Ok((
                        cached.schema.clone(),
                        cached.fields.clone(),
                        cached.has_system_modstamp,
                    ));
                }
            }
        }

        let describe = self.describe_object(sobject).await?;
        let schema = Self::build_dynamic_schema(&describe);
        let fields: Vec<String> = describe.fields.iter().map(|f| f.name.clone()).collect();
        let has_sms = describe.has_system_modstamp;

        let mut cache = self.describe_cache.write().await;
        cache.insert(
            sobject.to_string(),
            CachedDescribe {
                schema: schema.clone(),
                fields: fields.clone(),
                has_system_modstamp: has_sms,
                fetched_at: Instant::now(),
            },
        );

        Ok((schema, fields, has_sms))
    }

    // ───────────────────────────────────────────────────────────────────
    // SOQL building and predicate translation
    // ───────────────────────────────────────────────────────────────────

    /// Build a SELECT SOQL string from fields and SObject name.
    #[cfg(test)]
    fn build_select_soql(fields: &[&str], sobject: &str) -> String {
        format!("SELECT {} FROM {}", fields.join(", "), sobject)
    }

    /// Build a SELECT SOQL string from owned field names.
    fn build_select_soql_owned(fields: &[String], sobject: &str) -> String {
        format!("SELECT {} FROM {}", fields.join(", "), sobject)
    }

    /// Translate predicates into a SOQL WHERE clause.
    pub fn predicates_to_soql(predicates: &[Predicate]) -> String {
        let parts: Vec<String> = predicates.iter().filter_map(Self::predicate_to_soql_fragment).collect();
        if parts.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", parts.join(" AND "))
        }
    }

    fn predicate_to_soql_fragment(pred: &Predicate) -> Option<String> {
        match pred {
            Predicate::Equals { column, value } => {
                Some(format!("{} = {}", column, Self::format_soql_value(value)))
            }
            Predicate::In { column, values } => {
                let vals: Vec<String> = values
                    .iter()
                    .map(|v| Self::format_soql_value(v))
                    .collect();
                Some(format!("{} IN ({})", column, vals.join(", ")))
            }
            Predicate::GreaterThan {
                column,
                value,
                inclusive,
            } => {
                let op = if *inclusive { ">=" } else { ">" };
                Some(format!("{} {} {}", column, op, Self::format_soql_value(value)))
            }
            Predicate::LessThan {
                column,
                value,
                inclusive,
            } => {
                let op = if *inclusive { "<=" } else { "<" };
                Some(format!("{} {} {}", column, op, Self::format_soql_value(value)))
            }
            Predicate::Between { column, low, high } => Some(format!(
                "{} >= {} AND {} <= {}",
                column,
                Self::format_soql_value(low),
                column,
                Self::format_soql_value(high)
            )),
            Predicate::Like { column, pattern } => {
                Some(format!("{} LIKE '{}'", column, Self::escape_soql(pattern)))
            }
            Predicate::Contains { column, substring } => Some(format!(
                "{} LIKE '%{}%'",
                column,
                Self::escape_soql_like(substring)
            )),
            Predicate::IsNull { column, is_null } => {
                if *is_null {
                    Some(format!("{} = null", column))
                } else {
                    Some(format!("{} != null", column))
                }
            }
            Predicate::And(preds) => {
                let parts: Vec<String> =
                    preds.iter().filter_map(Self::predicate_to_soql_fragment).collect();
                if parts.is_empty() {
                    None
                } else {
                    Some(format!("({})", parts.join(" AND ")))
                }
            }
            Predicate::Or(preds) => {
                let parts: Vec<String> =
                    preds.iter().filter_map(Self::predicate_to_soql_fragment).collect();
                if parts.is_empty() {
                    None
                } else {
                    Some(format!("({})", parts.join(" OR ")))
                }
            }
            Predicate::Not(pred) => {
                Self::predicate_to_soql_fragment(pred).map(|s| format!("NOT ({})", s))
            }
        }
    }

    fn escape_soql(value: &str) -> String {
        value.replace('\\', "\\\\").replace('\'', "\\'")
    }

    /// Escape a value for use inside a SOQL LIKE pattern. In addition to
    /// standard SOQL escaping, `%` and `_` are escaped so they match
    /// literally instead of acting as wildcards.
    fn escape_soql_like(value: &str) -> String {
        Self::escape_soql(value)
            .replace('%', "\\%")
            .replace('_', "\\_")
    }

    /// Format a value for SOQL: timestamps, numbers, booleans, and null go
    /// unquoted; everything else gets single-quoted.
    fn format_soql_value(value: &str) -> String {
        if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
            return value.to_lowercase();
        }
        if value.eq_ignore_ascii_case("null") {
            return "null".to_string();
        }
        if Self::is_soql_datetime(value) {
            return value.to_string();
        }
        if value.parse::<f64>().is_ok() {
            return value.to_string();
        }
        format!("'{}'", Self::escape_soql(value))
    }

    /// Validate that a string is a well-formed ISO-8601 datetime for SOQL.
    fn is_soql_datetime(value: &str) -> bool {
        chrono::DateTime::parse_from_rfc3339(value).is_ok()
            || chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.fZ").is_ok()
            || chrono::DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f%z").is_ok()
            || chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
    }

    // ───────────────────────────────────────────────────────────────────
    // JSON -> RecordBatch conversion
    // ───────────────────────────────────────────────────────────────────

    /// Parse a Salesforce ISO-8601 timestamp to epoch microseconds.
    fn parse_sf_timestamp(value: &str) -> Option<i64> {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
            return Some(dt.timestamp_micros());
        }
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.fZ") {
            return Some(dt.and_utc().timestamp_micros());
        }
        if let Ok(dt) = chrono::DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f%z") {
            return Some(dt.timestamp_micros());
        }
        None
    }

    /// Parse a Salesforce date string (YYYY-MM-DD) to days since epoch.
    fn parse_sf_date(value: &str) -> Option<i32> {
        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .ok()
            .map(|d| {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                (d - epoch).num_days() as i32
            })
    }

    /// Push a single Salesforce record directly into columnar builders.
    /// Lowercases record keys and skips the `attributes` metadata field.
    fn append_sf_record(
        record: &serde_json::Value,
        schema: &TableSchema,
        builders: &mut ColumnBuilders,
    ) {
        let lookup: HashMap<String, &serde_json::Value> = record
            .as_object()
            .map(|obj| {
                obj.iter()
                    .filter(|(k, _)| k.as_str() != "attributes")
                    .map(|(k, v)| (k.to_lowercase(), v))
                    .collect()
            })
            .unwrap_or_default();

        for (i, col) in schema.columns.iter().enumerate() {
            let val = lookup.get(&col.name).copied();
            match col.data_type {
                ColumnType::Timestamp => {
                    builders.builder(i).append_timestamp(
                        val.and_then(|v| v.as_str()).and_then(Self::parse_sf_timestamp),
                    );
                }
                ColumnType::Date => {
                    builders.builder(i).append_date32(
                        val.and_then(|v| v.as_str()).and_then(Self::parse_sf_date),
                    );
                }
                _ => {
                    builders.builder(i).append_json_value(val);
                }
            }
        }
        builders.row_complete();
    }

    // ───────────────────────────────────────────────────────────────────
    // High-level fetch orchestration
    // ───────────────────────────────────────────────────────────────────

    /// Resolve schema and fields for a table, using static schemas for standard
    /// objects and dynamic describe for custom objects.
    async fn resolve_schema(
        &self,
        table: &str,
    ) -> ConnectorResult<(TableSchema, Vec<String>, String, bool)> {
        let sobject = Self::table_to_sobject(table);

        if let Some(static_schema) = Self::get_static_schema(table) {
            if let Some(fields) = Self::static_fields_for(table) {
                let field_strings: Vec<String> = fields.iter().map(|s| s.to_string()).collect();
                let has_sms = static_schema
                    .columns
                    .iter()
                    .any(|c| c.name == "systemmodstamp");
                return Ok((static_schema, field_strings, sobject.to_string(), has_sms));
            }
        }

        let (schema, fields, has_sms) = self.get_cached_describe(sobject).await?;
        Ok((schema, fields, sobject.to_string(), has_sms))
    }

    /// Apply column projection: keep only the requested columns from
    /// `fields` and `schema`, preserving original order.
    fn apply_projection(
        schema: TableSchema,
        fields: Vec<String>,
        projection: &[String],
    ) -> (TableSchema, Vec<String>) {
        let proj_lower: std::collections::HashSet<String> =
            projection.iter().map(|s| s.to_lowercase()).collect();

        let columns: Vec<ColumnSchema> = schema
            .columns
            .into_iter()
            .filter(|c| proj_lower.contains(&c.name))
            .collect();

        let filtered_fields: Vec<String> = fields
            .into_iter()
            .filter(|f| proj_lower.contains(&f.to_lowercase()))
            .collect();

        (TableSchema { columns }, filtered_fields)
    }

    /// Append `LIMIT N` to a SOQL query string.
    fn append_soql_limit(soql: &str, limit: usize) -> String {
        format!("{} LIMIT {}", soql, limit)
    }

    /// Core fetch: decides between REST API and Bulk API based on context.
    async fn do_fetch(
        &self,
        table: &str,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let (schema, fields, sobject, _has_sms) = self.resolve_schema(table).await?;

        let (schema, fields) = if let Some(ref proj) = options.projection {
            if !proj.is_empty() {
                Self::apply_projection(schema, fields, proj)
            } else {
                (schema, fields)
            }
        } else {
            (schema, fields)
        };

        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        let is_incremental = options.last_value.is_some();
        let has_predicates = !options.predicates.is_empty();

        if is_incremental {
            let last_value = options.last_value.as_deref().unwrap_or("");
            let inc_key = options
                .incremental_key
                .as_deref()
                .unwrap_or("SystemModstamp");
            let mut soql = format!(
                "{} WHERE {} >= {}",
                Self::build_select_soql_owned(&fields, &sobject),
                inc_key,
                Self::format_soql_value(last_value)
            );
            if let Some(max) = options.max_rows {
                soql = Self::append_soql_limit(&soql, max);
            }
            return self.fetch_soql(&soql, &schema, arrow_schema).await;
        }

        if has_predicates {
            let base = Self::build_select_soql_owned(&fields, &sobject);
            let where_clause = Self::predicates_to_soql(&options.predicates);
            let mut soql = format!("{}{}", base, where_clause);
            if let Some(max) = options.max_rows {
                soql = Self::append_soql_limit(&soql, max);
            }
            return self.fetch_soql(&soql, &schema, arrow_schema).await;
        }

        // Full sync: check row count to decide REST vs Bulk
        let count = self.fetch_soql_count(&sobject).await.unwrap_or(0);
        let mut base_soql = Self::build_select_soql_owned(&fields, &sobject);

        if let Some(max) = options.max_rows {
            base_soql = Self::append_soql_limit(&base_soql, max);
        }

        if count > BULK_ROW_THRESHOLD && options.max_rows.is_none() {
            info!(
                table,
                count, "Using Bulk API 2.0 for full sync (>{} rows)", BULK_ROW_THRESHOLD
            );
            self.fetch_bulk(&base_soql, arrow_schema).await
        } else {
            self.fetch_soql(&base_soql, &schema, arrow_schema).await
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Supporting types for describe
// ═══════════════════════════════════════════════════════════════════════════

struct SObjectInfo {
    name: String,
    #[allow(dead_code)]
    label: String,
}

struct DescribeResult {
    fields: Vec<SObjectField>,
    has_system_modstamp: bool,
}

struct SObjectField {
    name: String,
    sf_type: String,
    nillable: bool,
    label: String,
}

// ═══════════════════════════════════════════════════════════════════════════
// Connector trait implementation
// ═══════════════════════════════════════════════════════════════════════════

#[async_trait]
impl Connector for SalesforceConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Salesforce
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables: Vec<TableInfo> = Self::STANDARD_TABLES
            .iter()
            .filter_map(|&table| {
                Self::get_static_schema(table).map(|schema| {
                    let has_sms = schema
                        .columns
                        .iter()
                        .any(|c| c.name == "systemmodstamp");
                    TableInfo {
                        name: table.to_string(),
                        schema,
                        supports_incremental: has_sms,
                        incremental_key: if has_sms {
                            Some("systemmodstamp".to_string())
                        } else {
                            None
                        },
                        estimated_rows: None,
                        primary_key_columns: vec!["id".to_string()],
                    }
                })
            })
            .collect();

        let static_names: std::collections::HashSet<&str> =
            Self::STANDARD_TABLES.iter().copied().collect();

        match self.describe_global().await {
            Ok(all_objects) => {
                let extra_names: Vec<String> = all_objects
                    .into_iter()
                    .filter(|obj| {
                        let table_name = Self::sobject_to_table(&obj.name);
                        !static_names.contains(table_name.as_str())
                    })
                    .map(|obj| obj.name)
                    .collect();

                if !extra_names.is_empty() {
                    info!(
                        count = extra_names.len(),
                        "Discovered additional queryable Salesforce objects"
                    );

                    match self.batch_describe(&extra_names).await {
                        Ok(described) => {
                            for (sobject_name, describe) in described {
                                let table_name = Self::sobject_to_table(&sobject_name);
                                let schema = Self::build_dynamic_schema(&describe);
                                tables.push(TableInfo {
                                    name: table_name,
                                    schema,
                                    supports_incremental: describe.has_system_modstamp,
                                    incremental_key: if describe.has_system_modstamp {
                                        Some("systemmodstamp".to_string())
                                    } else {
                                        None
                                    },
                                    estimated_rows: None,
                                    primary_key_columns: vec!["id".to_string()],
                                });
                            }
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                "Failed to describe objects, returning standard tables only"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Failed to list Salesforce objects, returning standard tables only"
                );
            }
        }

        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        if let Some(schema) = Self::get_static_schema(table) {
            return Ok(schema);
        }

        let sobject = Self::table_to_sobject(table);
        let (schema, _, _) = self.get_cached_describe(sobject).await?;
        Ok(schema)
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let options = FetchOptions {
            incremental_key: incremental_key.map(String::from),
            last_value: last_value.map(String::from),
            ..Default::default()
        };
        self.do_fetch(table, &options).await
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
        let api = self.api_path();
        let _: serde_json::Value = self.client.get(&format!("{}/limits", api)).await?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

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
            "https://login.salesforce.com/services/oauth2/token",
        )
        .with_access_token("test_access_token", Some(future_expiry))
        .with_refresh_token("test_refresh_token")
    }

    fn test_config() -> SalesforceConfig {
        SalesforceConfig::new(test_oauth(), "https://na1.salesforce.com")
    }

    fn test_connector_with_base_url(base_url: &str) -> SalesforceConnector {
        let config = SalesforceConfig::new(test_oauth(), base_url);
        let client = HttpApiClient::new(base_url)
            .with_auth(AuthConfig::OAuth(config.oauth.clone()))
            .with_rate_limit(100, std::time::Duration::from_secs(20))
            .with_max_retries(0);
        SalesforceConnector {
            config,
            client,
            describe_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ── Config tests ─────────────────────────────────────────────────

    #[test]
    fn test_salesforce_config_creation() {
        let config = test_config();
        assert!(format!("{:?}", config).contains("REDACTED"));
        assert!(format!("{:?}", config).contains("na1.salesforce.com"));
    }

    #[test]
    fn test_salesforce_config_debug_redacts() {
        let config = test_config();
        let debug = format!("{:?}", config);
        assert!(!debug.contains("test_client_secret"));
        assert!(!debug.contains("test_access_token"));
        assert!(debug.contains("REDACTED"));
    }

    #[test]
    fn test_salesforce_config_api_version() {
        let config = test_config().with_api_version("v61.0");
        assert_eq!(config.api_version, "v61.0");
    }

    // ── Schema tests ─────────────────────────────────────────────────

    #[test]
    fn test_get_static_schema_accounts() {
        let schema = SalesforceConnector::get_static_schema("accounts").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"industry"));
        assert!(names.contains(&"annualrevenue"));
        assert!(names.contains(&"systemmodstamp"));
    }

    #[test]
    fn test_get_static_schema_contacts() {
        let schema = SalesforceConnector::get_static_schema("contacts").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"firstname"));
        assert!(names.contains(&"lastname"));
        assert!(names.contains(&"email"));
        assert!(names.contains(&"accountid"));
    }

    #[test]
    fn test_get_static_schema_leads() {
        let schema = SalesforceConnector::get_static_schema("leads").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"company"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"leadsource"));
    }

    #[test]
    fn test_get_static_schema_opportunities() {
        let schema = SalesforceConnector::get_static_schema("opportunities").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"amount"));
        assert!(names.contains(&"stagename"));
        assert!(names.contains(&"closedate"));
    }

    #[test]
    fn test_get_static_schema_cases() {
        let schema = SalesforceConnector::get_static_schema("cases").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"subject"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"priority"));
    }

    #[test]
    fn test_get_static_schema_tasks() {
        let schema = SalesforceConnector::get_static_schema("tasks").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"subject"));
        assert!(names.contains(&"activitydate"));
    }

    #[test]
    fn test_get_static_schema_events() {
        let schema = SalesforceConnector::get_static_schema("events").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"startdatetime"));
        assert!(names.contains(&"enddatetime"));
    }

    #[test]
    fn test_get_static_schema_users() {
        let schema = SalesforceConnector::get_static_schema("users").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"username"));
        assert!(names.contains(&"isactive"));
    }

    #[test]
    fn test_get_static_schema_campaigns() {
        let schema = SalesforceConnector::get_static_schema("campaigns").unwrap();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"budgetedcost"));
        assert!(names.contains(&"numberofleads"));
        assert!(names.contains(&"systemmodstamp"));
    }

    #[test]
    fn test_get_static_schema_unknown() {
        assert!(SalesforceConnector::get_static_schema("nonexistent").is_none());
    }

    // ── Connector trait tests ────────────────────────────────────────

    #[test]
    fn test_source_type() {
        let connector = SalesforceConnector::new(test_config());
        assert_eq!(connector.source_type(), SourceType::Salesforce);
    }

    #[tokio::test]
    async fn test_get_schema() {
        let connector = SalesforceConnector::new(test_config());
        let schema = connector.get_schema("accounts").await.unwrap();
        assert!(!schema.columns.is_empty());
    }

    #[tokio::test]
    async fn test_get_schema_not_found_falls_to_describe() {
        let connector = SalesforceConnector::new(test_config());
        let result = connector.get_schema("nonexistent").await;
        // Will fail because describe_object hits a fake URL, but it attempts dynamic discovery
        assert!(result.is_err());
    }

    // ── Table / SObject mapping ──────────────────────────────────────

    #[test]
    fn test_table_to_sobject() {
        assert_eq!(SalesforceConnector::table_to_sobject("accounts"), "Account");
        assert_eq!(SalesforceConnector::table_to_sobject("contacts"), "Contact");
        assert_eq!(
            SalesforceConnector::table_to_sobject("opportunities"),
            "Opportunity"
        );
        assert_eq!(
            SalesforceConnector::table_to_sobject("custom_object__c"),
            "custom_object__c"
        );
    }

    #[test]
    fn test_sobject_to_table() {
        assert_eq!(SalesforceConnector::sobject_to_table("Account"), "accounts");
        assert_eq!(
            SalesforceConnector::sobject_to_table("Custom__c"),
            "custom__c"
        );
    }

    // ── SOQL and predicate tests ─────────────────────────────────────

    #[test]
    fn test_build_select_soql() {
        let soql =
            SalesforceConnector::build_select_soql(&["Id", "Name", "Email"], "Contact");
        assert_eq!(soql, "SELECT Id, Name, Email FROM Contact");
    }

    #[test]
    fn test_apply_projection() {
        let schema = SalesforceConnector::get_static_schema("accounts").unwrap();
        let fields: Vec<String> = SalesforceConnector::static_fields_for("accounts")
            .unwrap()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let projection = vec!["id".to_string(), "name".to_string(), "industry".to_string()];
        let (proj_schema, proj_fields) =
            SalesforceConnector::apply_projection(schema, fields, &projection);

        assert_eq!(proj_schema.columns.len(), 3);
        assert_eq!(proj_schema.columns[0].name, "id");
        assert_eq!(proj_schema.columns[1].name, "name");
        assert_eq!(proj_schema.columns[2].name, "industry");
        assert_eq!(proj_fields.len(), 3);
        assert_eq!(proj_fields[0], "Id");
        assert_eq!(proj_fields[1], "Name");
        assert_eq!(proj_fields[2], "Industry");
    }

    #[test]
    fn test_append_soql_limit() {
        let soql = "SELECT Id, Name FROM Account";
        assert_eq!(
            SalesforceConnector::append_soql_limit(soql, 100),
            "SELECT Id, Name FROM Account LIMIT 100"
        );
    }

    #[test]
    fn test_predicates_to_soql_equals_string() {
        use compact_str::CompactString;
        let preds = vec![Predicate::Equals {
            column: CompactString::from("Status"),
            value: CompactString::from("Active"),
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE Status = 'Active'");
    }

    #[test]
    fn test_predicates_to_soql_equals_number() {
        use compact_str::CompactString;
        let preds = vec![Predicate::Equals {
            column: CompactString::from("Amount"),
            value: CompactString::from("1000"),
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE Amount = 1000");
    }

    #[test]
    fn test_predicates_to_soql_equals_boolean() {
        use compact_str::CompactString;
        let preds = vec![Predicate::Equals {
            column: CompactString::from("IsActive"),
            value: CompactString::from("true"),
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE IsActive = true");
    }

    #[test]
    fn test_predicates_to_soql_in() {
        use compact_str::CompactString;
        let preds = vec![Predicate::In {
            column: CompactString::from("Status"),
            values: vec![
                CompactString::from("Active"),
                CompactString::from("Closed"),
            ],
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE Status IN ('Active', 'Closed')");
    }

    #[test]
    fn test_predicates_to_soql_in_numbers() {
        use compact_str::CompactString;
        let preds = vec![Predicate::In {
            column: CompactString::from("Amount"),
            values: vec![
                CompactString::from("100"),
                CompactString::from("200"),
            ],
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE Amount IN (100, 200)");
    }

    #[test]
    fn test_predicates_to_soql_greater_than() {
        use compact_str::CompactString;
        let preds = vec![Predicate::GreaterThan {
            column: CompactString::from("CreatedDate"),
            value: CompactString::from("2024-01-01T00:00:00Z"),
            inclusive: true,
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE CreatedDate >= 2024-01-01T00:00:00Z");
    }

    #[test]
    fn test_predicates_to_soql_is_null() {
        use compact_str::CompactString;
        let preds = vec![Predicate::IsNull {
            column: CompactString::from("Email"),
            is_null: true,
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE Email = null");
    }

    #[test]
    fn test_predicates_to_soql_like() {
        use compact_str::CompactString;
        let preds = vec![Predicate::Like {
            column: CompactString::from("Name"),
            pattern: CompactString::from("Acme%"),
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE Name LIKE 'Acme%'");
    }

    #[test]
    fn test_predicates_to_soql_and_compound() {
        use compact_str::CompactString;
        let preds = vec![
            Predicate::Equals {
                column: CompactString::from("Status"),
                value: CompactString::from("Active"),
            },
            Predicate::GreaterThan {
                column: CompactString::from("Amount"),
                value: CompactString::from("1000"),
                inclusive: false,
            },
        ];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE Status = 'Active' AND Amount > 1000");
    }

    #[test]
    fn test_predicates_to_soql_not() {
        use compact_str::CompactString;
        let preds = vec![Predicate::Not(Box::new(Predicate::Equals {
            column: CompactString::from("Status"),
            value: CompactString::from("Closed"),
        }))];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE NOT (Status = 'Closed')");
    }

    #[test]
    fn test_predicates_to_soql_between() {
        use compact_str::CompactString;
        let preds = vec![Predicate::Between {
            column: CompactString::from("Amount"),
            low: CompactString::from("100"),
            high: CompactString::from("500"),
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE Amount >= 100 AND Amount <= 500");
    }

    #[test]
    fn test_predicates_to_soql_empty() {
        let preds: Vec<Predicate> = vec![];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, "");
    }

    #[test]
    fn test_escape_soql() {
        assert_eq!(SalesforceConnector::escape_soql("O'Brien"), "O\\'Brien");
        assert_eq!(SalesforceConnector::escape_soql("a\\b"), "a\\\\b");
    }

    #[test]
    fn test_escape_soql_like() {
        assert_eq!(SalesforceConnector::escape_soql_like("100%"), "100\\%");
        assert_eq!(SalesforceConnector::escape_soql_like("a_b"), "a\\_b");
        assert_eq!(
            SalesforceConnector::escape_soql_like("50% off O'Brien"),
            "50\\% off O\\'Brien"
        );
    }

    #[test]
    fn test_predicates_to_soql_contains_escapes_wildcards() {
        use compact_str::CompactString;
        let preds = vec![Predicate::Contains {
            column: CompactString::from("Name"),
            substring: CompactString::from("100%"),
        }];
        let clause = SalesforceConnector::predicates_to_soql(&preds);
        assert_eq!(clause, " WHERE Name LIKE '%100\\%%'");
    }

    #[test]
    fn test_format_soql_value() {
        assert_eq!(SalesforceConnector::format_soql_value("Active"), "'Active'");
        assert_eq!(SalesforceConnector::format_soql_value("1000"), "1000");
        assert_eq!(SalesforceConnector::format_soql_value("3.14"), "3.14");
        assert_eq!(SalesforceConnector::format_soql_value("true"), "true");
        assert_eq!(SalesforceConnector::format_soql_value("FALSE"), "false");
        assert_eq!(SalesforceConnector::format_soql_value("null"), "null");
        assert_eq!(
            SalesforceConnector::format_soql_value("2024-01-01T00:00:00Z"),
            "2024-01-01T00:00:00Z"
        );
    }

    // ── JSON -> RecordBatch tests ────────────────────────────────────

    #[test]
    fn test_builders_accounts_batch() {
        let schema = SalesforceConnector::get_static_schema("accounts").unwrap();
        let arrow_schema = Arc::new(SalesforceConnector::to_arrow_schema(&schema));

        let records = vec![
            serde_json::json!({
                "attributes": {"type": "Account"},
                "Id": "001xx000003DGbX",
                "Name": "Acme Corp",
                "Industry": "Technology",
                "Type": "Customer",
                "Phone": "+1234567890",
                "Website": "https://acme.com",
                "BillingCity": "San Francisco",
                "BillingState": "CA",
                "BillingCountry": "US",
                "AnnualRevenue": 5000000.0,
                "NumberOfEmployees": 250,
                "OwnerId": "005xx000001Svb",
                "CreatedDate": "2024-01-15T10:30:00.000+0000",
                "LastModifiedDate": "2024-06-01T14:00:00.000+0000",
                "SystemModstamp": "2024-06-01T14:00:00.000+0000"
            }),
            serde_json::json!({
                "attributes": {"type": "Account"},
                "Id": "001xx000003DGbY",
                "Name": "Globex",
                "Industry": null,
                "Type": null,
                "Phone": null,
                "Website": null,
                "BillingCity": null,
                "BillingState": null,
                "BillingCountry": null,
                "AnnualRevenue": null,
                "NumberOfEmployees": null,
                "OwnerId": "005xx000001Svb",
                "CreatedDate": "2024-02-01T08:00:00.000+0000",
                "LastModifiedDate": "2024-07-01T09:00:00.000+0000",
                "SystemModstamp": "2024-07-01T09:00:00.000+0000"
            }),
        ];

        let mut builders = ColumnBuilders::new(&schema, 4);
        for record in &records {
            SalesforceConnector::append_sf_record(record, &schema, &mut builders);
        }
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 15);
    }

    #[test]
    fn test_builders_opportunities_with_float() {
        let schema = SalesforceConnector::get_static_schema("opportunities").unwrap();
        let arrow_schema = Arc::new(SalesforceConnector::to_arrow_schema(&schema));

        let record = serde_json::json!({
            "attributes": {"type": "Opportunity"},
            "Id": "006xx000003abcD",
            "Name": "Big Deal",
            "Amount": 150000.50,
            "StageName": "Closed Won",
            "Probability": 100.0,
            "CloseDate": "2024-03-15",
            "AccountId": "001xx000003DGbX",
            "OwnerId": "005xx000001Svb",
            "Type": "New Business",
            "LeadSource": "Web",
            "CreatedDate": "2024-01-01T00:00:00.000+0000",
            "LastModifiedDate": "2024-03-15T12:00:00.000+0000",
            "SystemModstamp": "2024-03-15T12:00:00.000+0000"
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        SalesforceConnector::append_sf_record(&record, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let amount_col = batch.column_by_name("amount").unwrap().as_any()
            .downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert!((amount_col.value(0) - 150000.50).abs() < f64::EPSILON);
    }

    #[test]
    fn test_append_sf_record_skips_attributes() {
        let schema = SalesforceConnector::get_static_schema("accounts").unwrap();
        let arrow_schema = Arc::new(SalesforceConnector::to_arrow_schema(&schema));

        let record = serde_json::json!({
            "attributes": {"type": "Account", "url": "/services/data/v62.0/sobjects/Account/001"},
            "Id": "001xx",
            "Name": "Acme",
            "AnnualRevenue": 5000000.0
        });

        let mut builders = ColumnBuilders::new(&schema, 4);
        SalesforceConnector::append_sf_record(&record, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();

        let id_col = batch.column_by_name("id").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(id_col.value(0), "001xx");
        let name_col = batch.column_by_name("name").unwrap().as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(name_col.value(0), "Acme");
    }

    // ── Timestamp / date parsing tests ───────────────────────────────

    #[test]
    fn test_parse_sf_timestamp_rfc3339() {
        let micros = SalesforceConnector::parse_sf_timestamp("2024-01-01T00:00:00Z").unwrap();
        assert_eq!(micros, 1704067200_000_000);
    }

    #[test]
    fn test_parse_sf_timestamp_with_offset() {
        let micros =
            SalesforceConnector::parse_sf_timestamp("2024-01-01T00:00:00.000+0000");
        assert!(micros.is_some());
    }

    #[test]
    fn test_parse_sf_timestamp_invalid() {
        assert!(SalesforceConnector::parse_sf_timestamp("not-a-date").is_none());
    }

    #[test]
    fn test_parse_sf_date() {
        let days = SalesforceConnector::parse_sf_date("2024-01-01").unwrap();
        assert_eq!(days, 19723); // 2024-01-01 is 19723 days from 1970-01-01
    }

    #[test]
    fn test_parse_sf_date_invalid() {
        assert!(SalesforceConnector::parse_sf_date("not-a-date").is_none());
    }

    // ── Dynamic schema tests ─────────────────────────────────────────

    #[test]
    fn test_sf_type_to_column_type() {
        assert!(matches!(
            SalesforceConnector::sf_type_to_column_type("double"),
            ColumnType::Float64
        ));
        assert!(matches!(
            SalesforceConnector::sf_type_to_column_type("currency"),
            ColumnType::Float64
        ));
        assert!(matches!(
            SalesforceConnector::sf_type_to_column_type("int"),
            ColumnType::Int64
        ));
        assert!(matches!(
            SalesforceConnector::sf_type_to_column_type("boolean"),
            ColumnType::Boolean
        ));
        assert!(matches!(
            SalesforceConnector::sf_type_to_column_type("datetime"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            SalesforceConnector::sf_type_to_column_type("date"),
            ColumnType::Date
        ));
        assert!(matches!(
            SalesforceConnector::sf_type_to_column_type("string"),
            ColumnType::String
        ));
        assert!(matches!(
            SalesforceConnector::sf_type_to_column_type("reference"),
            ColumnType::String
        ));
        assert!(matches!(
            SalesforceConnector::sf_type_to_column_type("picklist"),
            ColumnType::String
        ));
    }

    #[test]
    fn test_build_dynamic_schema() {
        let describe = DescribeResult {
            fields: vec![
                SObjectField {
                    name: "Id".to_string(),
                    sf_type: "id".to_string(),
                    nillable: false,
                    label: "Record ID".to_string(),
                },
                SObjectField {
                    name: "Revenue__c".to_string(),
                    sf_type: "currency".to_string(),
                    nillable: true,
                    label: "Revenue".to_string(),
                },
                SObjectField {
                    name: "IsActive__c".to_string(),
                    sf_type: "boolean".to_string(),
                    nillable: true,
                    label: "Is Active".to_string(),
                },
            ],
            has_system_modstamp: false,
        };

        let schema = SalesforceConnector::build_dynamic_schema(&describe);
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "id");
        assert!(!schema.columns[0].nullable);
        assert_eq!(schema.columns[1].name, "revenue__c");
        assert!(matches!(schema.columns[1].data_type, ColumnType::Float64));
        assert_eq!(schema.columns[2].name, "isactive__c");
        assert!(matches!(schema.columns[2].data_type, ColumnType::Boolean));
    }

    // ── Wiremock API tests ───────────────────────────────────────────

    #[tokio::test]
    async fn test_salesforce_auth_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/limits"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.validate_credentials().await;
        assert!(matches!(result, Err(ConnectorError::Authentication(_))));
    }

    #[tokio::test]
    async fn test_salesforce_rate_limited() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/limits"))
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
    async fn test_salesforce_validate_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/limits"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"DailyApiRequests": {"Max": 100000}})),
            )
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.validate_credentials().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_salesforce_soql_query() {
        let mock_server = MockServer::start().await;

        let body = serde_json::json!({
            "totalSize": 2,
            "done": true,
            "records": [
                {
                    "attributes": {"type": "Account"},
                    "Id": "001xx1",
                    "Name": "Acme",
                    "Industry": "Technology",
                    "Type": null,
                    "Phone": null,
                    "Website": null,
                    "BillingCity": "SF",
                    "BillingState": "CA",
                    "BillingCountry": "US",
                    "AnnualRevenue": 1000000.0,
                    "NumberOfEmployees": 50,
                    "OwnerId": "005xx1",
                    "CreatedDate": "2024-01-01T00:00:00.000+0000",
                    "LastModifiedDate": "2024-06-01T00:00:00.000+0000",
                    "SystemModstamp": "2024-06-01T00:00:00.000+0000"
                },
                {
                    "attributes": {"type": "Account"},
                    "Id": "001xx2",
                    "Name": "Globex",
                    "Industry": null,
                    "Type": null,
                    "Phone": null,
                    "Website": null,
                    "BillingCity": null,
                    "BillingState": null,
                    "BillingCountry": null,
                    "AnnualRevenue": null,
                    "NumberOfEmployees": null,
                    "OwnerId": "005xx2",
                    "CreatedDate": "2024-02-01T00:00:00.000+0000",
                    "LastModifiedDate": "2024-07-01T00:00:00.000+0000",
                    "SystemModstamp": "2024-07-01T00:00:00.000+0000"
                }
            ]
        });

        // Mock for the count query
        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/query/"))
            .and(query_param("q", "SELECT COUNT() FROM Account"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"totalSize": 2, "done": true, "records": []})),
            )
            .mount(&mock_server)
            .await;

        // Mock for the actual query
        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/query/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&body))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let batches = connector
            .fetch_table("accounts", None, None)
            .await
            .unwrap();
        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_salesforce_describe_global() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/sobjects/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "sobjects": [
                    {"name": "Account", "queryable": true, "custom": false, "label": "Account"},
                    {"name": "Custom__c", "queryable": true, "custom": true, "label": "Custom"},
                    {"name": "ApexLog", "queryable": false, "custom": false, "label": "Apex Log"}
                ]
            })))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let objects = connector.describe_global().await.unwrap();
        assert_eq!(objects.len(), 2); // Only queryable objects
        assert_eq!(objects[0].name, "Account");
        assert_eq!(objects[1].name, "Custom__c");
    }

    #[tokio::test]
    async fn test_salesforce_describe_object() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/sobjects/Custom__c/describe"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "fields": [
                    {"name": "Id", "type": "id", "nillable": false, "label": "Record ID"},
                    {"name": "Name", "type": "string", "nillable": false, "label": "Name"},
                    {"name": "Revenue__c", "type": "currency", "nillable": true, "label": "Revenue"},
                    {"name": "SystemModstamp", "type": "datetime", "nillable": false, "label": "System Modstamp"}
                ]
            })))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.describe_object("Custom__c").await.unwrap();
        assert_eq!(result.fields.len(), 4);
        assert!(result.has_system_modstamp);
    }

    #[tokio::test]
    async fn test_salesforce_composite_batch_describe() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/services/data/v62.0/composite/batch"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "hasErrors": false,
                "results": [
                    {
                        "statusCode": 200,
                        "result": {
                            "fields": [
                                {"name": "Id", "type": "id", "nillable": false, "label": "ID"},
                                {"name": "Name", "type": "string", "nillable": false, "label": "Name"}
                            ]
                        }
                    },
                    {
                        "statusCode": 200,
                        "result": {
                            "fields": [
                                {"name": "Id", "type": "id", "nillable": false, "label": "ID"},
                                {"name": "Value__c", "type": "double", "nillable": true, "label": "Value"}
                            ]
                        }
                    }
                ]
            })))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let results = connector
            .batch_describe(&["Obj1__c".to_string(), "Obj2__c".to_string()])
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "Obj1__c");
        assert_eq!(results[0].1.fields.len(), 2);
        assert_eq!(results[1].0, "Obj2__c");
        assert_eq!(results[1].1.fields.len(), 2);
    }

    #[tokio::test]
    async fn test_salesforce_bulk_job_create() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/services/data/v62.0/jobs/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "750xx000003abc",
                "operation": "query",
                "state": "UploadComplete"
            })))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let job_id = connector
            .create_bulk_query_job("SELECT Id FROM Account")
            .await
            .unwrap();
        assert_eq!(job_id, "750xx000003abc");
    }

    #[tokio::test]
    async fn test_salesforce_bulk_job_poll_complete() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/jobs/query/750xx"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "750xx",
                "state": "JobComplete"
            })))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.poll_bulk_job("750xx").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_salesforce_bulk_job_poll_failed() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/jobs/query/750xx"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "750xx",
                "state": "Failed",
                "errorMessage": "SOQL syntax error"
            })))
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());
        let result = connector.poll_bulk_job("750xx").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("SOQL syntax error"));
    }

    #[tokio::test]
    async fn test_salesforce_bulk_download_streaming() {
        let mock_server = MockServer::start().await;

        let csv_body = "Id,Name,Amount\n001,Acme,50000.0\n002,Globex,75000.5\n";

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/jobs/query/job123/results"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(csv_body)
                    .insert_header("Content-Type", "text/csv"),
            )
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());

        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("Id", ColumnType::String, false),
                ColumnSchema::new("Name", ColumnType::String, true),
                ColumnSchema::new("Amount", ColumnType::Float64, true),
            ],
        };
        let arrow_schema = Arc::new(SalesforceConnector::to_arrow_schema(&schema));

        let batches = connector
            .download_bulk_results("job123", arrow_schema)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_salesforce_bulk_download_pagination() {
        let mock_server = MockServer::start().await;

        let page1_csv = "Id,Name\n001,Acme\n";
        let page2_csv = "Id,Name\n002,Globex\n";

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/jobs/query/job456/results"))
            .and(wiremock::matchers::query_param_is_missing("locator"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(page1_csv)
                    .insert_header("Content-Type", "text/csv")
                    .insert_header("Sforce-Locator", "page2loc"),
            )
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/jobs/query/job456/results"))
            .and(wiremock::matchers::query_param("locator", "page2loc"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(page2_csv)
                    .insert_header("Content-Type", "text/csv"),
            )
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());

        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("Id", ColumnType::String, false),
                ColumnSchema::new("Name", ColumnType::String, true),
            ],
        };
        let arrow_schema = Arc::new(SalesforceConnector::to_arrow_schema(&schema));

        let batches = connector
            .download_bulk_results("job456", arrow_schema)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_describe_cache_ttl() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/data/v62.0/sobjects/Test__c/describe"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "fields": [
                    {"name": "Id", "type": "id", "nillable": false, "label": "ID"}
                ]
            })))
            .expect(1) // Should only be called once due to caching
            .mount(&mock_server)
            .await;

        let connector = test_connector_with_base_url(&mock_server.uri());

        // First call populates cache
        let (schema1, _, _) = connector.get_cached_describe("Test__c").await.unwrap();
        // Second call uses cache
        let (schema2, _, _) = connector.get_cached_describe("Test__c").await.unwrap();

        assert_eq!(schema1.columns.len(), schema2.columns.len());
    }

    #[test]
    fn test_parse_sf_timestamp_with_timezone_offset() {
        // RFC3339 with Z suffix
        let utc = SalesforceConnector::parse_sf_timestamp("2024-01-15T10:30:00.000Z").unwrap();

        // Same instant expressed with +05:30 offset (local time is 16:00)
        let with_offset =
            SalesforceConnector::parse_sf_timestamp("2024-01-15T16:00:00.000+0530").unwrap();
        assert_eq!(utc, with_offset, "timezone offset must be applied, not discarded");

        // Negative offset: UTC-05:00 (local time is 05:30)
        let neg_offset =
            SalesforceConnector::parse_sf_timestamp("2024-01-15T05:30:00.000-0500").unwrap();
        assert_eq!(utc, neg_offset, "negative timezone offset must be applied");

        // Plain Z suffix via the NaiveDateTime fallback
        let z_suffix =
            SalesforceConnector::parse_sf_timestamp("2024-01-15T10:30:00.000Z").unwrap();
        assert_eq!(utc, z_suffix);
    }

    #[test]
    fn test_format_soql_value_rejects_injection() {
        // Malicious string containing T and : should be quoted, not passed through raw
        let malicious = "T:) OR Status='Closed' OR (";
        let result = SalesforceConnector::format_soql_value(malicious);
        assert!(
            result.starts_with('\'') && result.ends_with('\''),
            "non-datetime string with T and : must be quoted, got: {}",
            result
        );

        // Valid ISO-8601 timestamps should still be unquoted
        let valid_ts = "2024-01-15T10:30:00.000Z";
        let result = SalesforceConnector::format_soql_value(valid_ts);
        assert_eq!(result, valid_ts, "valid timestamp must remain unquoted");

        // Valid RFC3339
        let rfc3339 = "2024-01-15T10:30:00+05:30";
        let result = SalesforceConnector::format_soql_value(rfc3339);
        assert_eq!(result, rfc3339, "valid RFC3339 must remain unquoted");
    }
}
