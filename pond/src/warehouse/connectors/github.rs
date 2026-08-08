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

const API_BASE: &str = "https://api.github.com";
const API_VERSION: &str = "2022-11-28";
const BATCH_CAPACITY: usize = 4096;
const PAGE_LIMIT: u64 = 100;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;
const RATE_LIMIT_DELAY_MS: u64 = 50;
const MAX_TOTAL_ROWS: usize = 1_000_000;

const ALL_TABLES: &[&str] = &[
    "repositories",
    "pull_requests",
    "issues",
    "commits",
    "workflows",
    "deployments",
];

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct GitHubConfig {
    pub access_token: SecretString,
    pub owner: String,
    pub repos: Option<Vec<String>>,
    pub api_base: Option<String>,
}

impl std::fmt::Debug for GitHubConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubConfig")
            .field("access_token", &"[REDACTED]")
            .field("owner", &self.owner)
            .field("repos", &self.repos)
            .field("api_base", &self.api_base)
            .finish()
    }
}

impl GitHubConfig {
    pub fn new(access_token: impl Into<String>, owner: impl Into<String>) -> Self {
        Self {
            access_token: SecretString::new(access_token.into()),
            owner: owner.into(),
            repos: None,
            api_base: None,
        }
    }

    pub fn with_repos(mut self, repos: Vec<String>) -> Self {
        self.repos = Some(repos);
        self
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

pub struct GitHubConnector {
    config: GitHubConfig,
    http: reqwest::Client,
    #[cfg(test)]
    base_url_override: Option<String>,
}

impl GitHubConnector {
    pub fn new(config: GitHubConfig) -> Self {
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

    async fn api_get_paged(
        &self,
        path: &str,
    ) -> ConnectorResult<(serde_json::Value, Option<String>)> {
        let url = self.resolve_url(path);
        let mut attempts = 0u32;

        loop {
            let resp = self
                .http
                .get(&url)
                .header("Authorization", format!("Bearer {}", self.config.access_token.expose()))
                .header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", API_VERSION)
                .header("User-Agent", "reiver-connector")
                .send()
                .await
                .map_err(|e| ConnectorError::Network(format!("GitHub request failed: {}", e)))?;

            if resp.status() == 401 || resp.status() == 403 {
                let remaining = resp
                    .headers()
                    .get("x-ratelimit-remaining")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());

                if remaining == Some(0) {
                    if attempts < MAX_RETRIES {
                        attempts += 1;
                        let delay = retry_delay_from_headers(resp.headers(), attempts);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        continue;
                    }
                    return Err(ConnectorError::RateLimited { retry_after_secs: 60 });
                }

                return Err(ConnectorError::Authentication(
                    "Invalid GitHub access token".to_string(),
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
                return Err(ConnectorError::Internal("GitHub API: not found (404)".to_string()));
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ConnectorError::Internal(format!(
                    "GitHub API error ({}): {}",
                    status, body
                )));
            }

            let next_url = parse_next_link(resp.headers());

            let json: serde_json::Value = resp.json().await.map_err(|e| {
                ConnectorError::Internal(format!("Failed to parse GitHub response: {}", e))
            })?;

            return Ok((json, next_url));
        }
    }

    // -----------------------------------------------------------------------
    // Repo discovery
    // -----------------------------------------------------------------------

    async fn discover_repos(&self) -> ConnectorResult<Vec<String>> {
        if let Some(ref repos) = self.config.repos {
            return Ok(repos
                .iter()
                .map(|r| {
                    if r.contains('/') {
                        r.clone()
                    } else {
                        format!("{}/{}", self.config.owner, r)
                    }
                })
                .collect());
        }

        let path = format!(
            "/orgs/{}/repos?per_page={}&sort=updated",
            self.config.owner, PAGE_LIMIT
        );
        match self.paginate_repo_names(&path).await {
            Ok(repos) => Ok(repos),
            Err(ConnectorError::Internal(msg)) if msg.contains("404") => {
                let path = format!(
                    "/users/{}/repos?per_page={}&sort=updated",
                    self.config.owner, PAGE_LIMIT
                );
                self.paginate_repo_names(&path).await
            }
            Err(e) => Err(e),
        }
    }

    async fn paginate_repo_names(&self, initial_path: &str) -> ConnectorResult<Vec<String>> {
        let mut names = Vec::new();
        let mut path = initial_path.to_string();

        loop {
            let (body, next_url) = self.api_get_paged(&path).await?;
            let items = body.as_array().cloned().unwrap_or_default();

            for item in &items {
                if let Some(full_name) = item.get("full_name").and_then(|v| v.as_str()) {
                    names.push(full_name.to_string());
                }
            }

            match next_url {
                Some(url) => {
                    path = url;
                    tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
                }
                None => break,
            }
        }

        Ok(names)
    }

    // -----------------------------------------------------------------------
    // Fetch strategies
    // -----------------------------------------------------------------------

    async fn fetch_paginated(
        &self,
        initial_path: &str,
        items_key: Option<&str>,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
        repo_full_name: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);
        let mut path = initial_path.to_string();

        loop {
            if total_rows >= max_rows {
                break;
            }

            let (body, next_url) = self.api_get_paged(&path).await?;

            let items = match items_key {
                Some(key) => body
                    .get(key)
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default(),
                None => body.as_array().cloned().unwrap_or_default(),
            };

            for item in &items {
                if total_rows >= max_rows {
                    break;
                }

                if table == "issues"
                    && item.get("pull_request").map_or(false, |v| !v.is_null())
                {
                    continue;
                }

                append_row(item, table, schema, &mut builders, repo_full_name);
                total_rows += 1;

                if builders.len() >= BATCH_CAPACITY {
                    batches.push(builders.finish(arrow_schema.clone())?);
                    builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
                }
            }

            match next_url {
                Some(url) => {
                    path = url;
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

    async fn fetch_repos(
        &self,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        if let Some(ref repos) = self.config.repos {
            return self.fetch_repos_individually(repos, schema, arrow_schema, options).await;
        }

        let mut query = format!("?per_page={}&sort=updated", PAGE_LIMIT);
        apply_predicate_params(&mut query, &options.predicates, "repositories");

        let path = format!("/orgs/{}/repos{}", self.config.owner, query);
        let result = self
            .fetch_paginated(&path, None, "repositories", schema, arrow_schema.clone(), options, None)
            .await;

        match result {
            Ok(batches) => Ok(batches),
            Err(ConnectorError::Internal(msg)) if msg.contains("404") => {
                let path = format!("/users/{}/repos{}", self.config.owner, query);
                self.fetch_paginated(
                    &path,
                    None,
                    "repositories",
                    schema,
                    arrow_schema,
                    options,
                    None,
                )
                .await
            }
            Err(e) => Err(e),
        }
    }

    async fn fetch_repos_individually(
        &self,
        repos: &[String],
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
        let mut batches: Vec<RecordBatch> = Vec::new();
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);
        let mut total_rows = 0usize;

        for repo_name in repos {
            if total_rows >= max_rows {
                break;
            }

            let full_name = if repo_name.contains('/') {
                repo_name.clone()
            } else {
                format!("{}/{}", self.config.owner, repo_name)
            };

            let path = format!("/repos/{}", full_name);
            let (item, _) = self.api_get_paged(&path).await?;

            append_row(&item, "repositories", schema, &mut builders, None);
            total_rows += 1;

            if builders.len() >= BATCH_CAPACITY {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(schema, BATCH_CAPACITY);
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

    async fn fetch_per_repo(
        &self,
        endpoint_suffix: &str,
        items_key: Option<&str>,
        table: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
        options: &FetchOptions,
        base_query: &str,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let repos = self.discover_repos().await?;
        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for repo in &repos {
            let mut query = base_query.to_string();
            apply_predicate_params(&mut query, &options.predicates, table);

            if let (Some(key), Some(val)) = (&options.incremental_key, &options.last_value) {
                match (table, key.as_str()) {
                    ("issues" | "commits", "updated_at" | "date") => {
                        let sep = if query.contains('?') { '&' } else { '?' };
                        query.push_str(&format!("{}since={}", sep, val));
                    }
                    ("pull_requests", "updated_at") => {
                        let sep = if query.contains('?') { '&' } else { '?' };
                        query.push_str(&format!("{}sort=updated&direction=desc", sep));
                    }
                    _ => {}
                }
            }

            let path = format!("/repos/{}{}{}", repo, endpoint_suffix, query);
            let batches = self
                .fetch_paginated(
                    &path,
                    items_key,
                    table,
                    schema,
                    arrow_schema.clone(),
                    options,
                    Some(repo.as_str()),
                )
                .await?;
            all_batches.extend(batches.into_iter().filter(|b| b.num_rows() > 0));
        }

        if all_batches.is_empty() {
            all_batches.push(RecordBatch::new_empty(arrow_schema));
        }
        Ok(all_batches)
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

        match table {
            "repositories" => self.fetch_repos(&schema, arrow_schema, options).await,
            "pull_requests" => {
                self.fetch_per_repo(
                    "/pulls",
                    None,
                    table,
                    &schema,
                    arrow_schema,
                    options,
                    "?state=all&per_page=100",
                )
                .await
            }
            "issues" => {
                self.fetch_per_repo(
                    "/issues",
                    None,
                    table,
                    &schema,
                    arrow_schema,
                    options,
                    "?state=all&per_page=100&filter=all",
                )
                .await
            }
            "commits" => {
                self.fetch_per_repo(
                    "/commits",
                    None,
                    table,
                    &schema,
                    arrow_schema,
                    options,
                    &format!("?per_page={}", PAGE_LIMIT),
                )
                .await
            }
            "workflows" => {
                self.fetch_per_repo(
                    "/actions/workflows",
                    Some("workflows"),
                    table,
                    &schema,
                    arrow_schema,
                    options,
                    "",
                )
                .await
            }
            "deployments" => {
                self.fetch_per_repo(
                    "/deployments",
                    None,
                    table,
                    &schema,
                    arrow_schema,
                    options,
                    &format!("?per_page={}", PAGE_LIMIT),
                )
                .await
            }
            _ => Err(ConnectorError::TableNotFound(table.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Link header pagination
// ---------------------------------------------------------------------------

fn parse_next_link(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let link = headers.get("link")?.to_str().ok()?;
    for part in link.split(',') {
        let part = part.trim();
        if part.ends_with("rel=\"next\"") {
            let url = part
                .split(';')
                .next()?
                .trim()
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_string();
            return Some(url);
        }
    }
    None
}

fn retry_delay_from_headers(headers: &reqwest::header::HeaderMap, attempt: u32) -> u64 {
    if let Some(reset) = headers
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
    {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let wait = reset.saturating_sub(now);
        if wait > 0 && wait < 120 {
            return wait * 1000;
        }
    }
    INITIAL_RETRY_DELAY_MS * 2u64.pow(attempt - 1)
}

// ---------------------------------------------------------------------------
// Predicate pushdown -> query params
// ---------------------------------------------------------------------------

fn apply_predicate_params(query: &mut String, predicates: &[Predicate], table: &str) {
    for pred in predicates {
        match pred {
            Predicate::Equals { column, value }
                if column == "state"
                    && matches!(table, "pull_requests" | "issues") =>
            {
                let sep = if query.contains('?') { '&' } else { '?' };
                *query = query.replace("state=all", &format!("state={}", value));
                if !query.contains("state=") {
                    query.push_str(&format!("{}state={}", sep, value));
                }
            }
            Predicate::GreaterThan {
                column,
                value,
                inclusive: _,
            } if column == "updated_at" && matches!(table, "issues" | "commits") => {
                let sep = if query.contains('?') { '&' } else { '?' };
                query.push_str(&format!("{}since={}", sep, value));
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
        "repositories" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("full_name", ColumnType::String, false),
                ColumnSchema::new("owner_login", ColumnType::String, false),
                ColumnSchema::new("private", ColumnType::Boolean, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("language", ColumnType::String, true),
                ColumnSchema::new("default_branch", ColumnType::String, true),
                ColumnSchema::new("stargazers_count", ColumnType::Int64, false),
                ColumnSchema::new("forks_count", ColumnType::Int64, false),
                ColumnSchema::new("open_issues_count", ColumnType::Int64, false),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false),
                ColumnSchema::new("pushed_at", ColumnType::Timestamp, true),
            ],
        }),

        "pull_requests" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("number", ColumnType::Int64, false),
                ColumnSchema::new("title", ColumnType::String, false),
                ColumnSchema::new("state", ColumnType::String, false),
                ColumnSchema::new("user_login", ColumnType::String, false),
                ColumnSchema::new("repository", ColumnType::String, false),
                ColumnSchema::new("draft", ColumnType::Boolean, false),
                ColumnSchema::new("head_ref", ColumnType::String, true),
                ColumnSchema::new("base_ref", ColumnType::String, true),
                ColumnSchema::new("merged_at", ColumnType::Timestamp, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false),
                ColumnSchema::new("closed_at", ColumnType::Timestamp, true),
            ],
        }),

        "issues" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("number", ColumnType::Int64, false),
                ColumnSchema::new("title", ColumnType::String, false),
                ColumnSchema::new("state", ColumnType::String, false),
                ColumnSchema::new("user_login", ColumnType::String, false),
                ColumnSchema::new("repository", ColumnType::String, false),
                ColumnSchema::new("labels", ColumnType::String, true),
                ColumnSchema::new("assignee_login", ColumnType::String, true),
                ColumnSchema::new("milestone_title", ColumnType::String, true),
                ColumnSchema::new("comments", ColumnType::Int64, false),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false),
                ColumnSchema::new("closed_at", ColumnType::Timestamp, true),
            ],
        }),

        "commits" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("sha", ColumnType::String, false),
                ColumnSchema::new("message", ColumnType::String, false),
                ColumnSchema::new("author_login", ColumnType::String, true),
                ColumnSchema::new("author_name", ColumnType::String, false),
                ColumnSchema::new("author_email", ColumnType::String, false),
                ColumnSchema::new("committer_login", ColumnType::String, true),
                ColumnSchema::new("repository", ColumnType::String, false),
                ColumnSchema::new("date", ColumnType::Timestamp, false),
            ],
        }),

        "workflows" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("path", ColumnType::String, false),
                ColumnSchema::new("state", ColumnType::String, false),
                ColumnSchema::new("repository", ColumnType::String, false),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false),
            ],
        }),

        "deployments" => Some(TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("sha", ColumnType::String, false),
                ColumnSchema::new("ref_name", ColumnType::String, false),
                ColumnSchema::new("task", ColumnType::String, false),
                ColumnSchema::new("environment", ColumnType::String, false),
                ColumnSchema::new("creator_login", ColumnType::String, true),
                ColumnSchema::new("repository", ColumnType::String, false),
                ColumnSchema::new("description", ColumnType::String, true),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false),
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
    DeepNested(&'static str, &'static str, &'static str),
    JsonArray(&'static str),
    RepoContext,
}

fn field_mapping(table: &str) -> Option<FieldMapping> {
    match table {
        "repositories" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("full_name", FieldPath::Direct("full_name")),
                ("owner_login", FieldPath::Nested("owner", "login")),
                ("private", FieldPath::Direct("private")),
                ("description", FieldPath::Direct("description")),
                ("language", FieldPath::Direct("language")),
                ("default_branch", FieldPath::Direct("default_branch")),
                ("stargazers_count", FieldPath::Direct("stargazers_count")),
                ("forks_count", FieldPath::Direct("forks_count")),
                ("open_issues_count", FieldPath::Direct("open_issues_count")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
                ("pushed_at", FieldPath::Direct("pushed_at")),
            ],
        }),

        "pull_requests" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("number", FieldPath::Direct("number")),
                ("title", FieldPath::Direct("title")),
                ("state", FieldPath::Direct("state")),
                ("user_login", FieldPath::Nested("user", "login")),
                ("repository", FieldPath::RepoContext),
                ("draft", FieldPath::Direct("draft")),
                ("head_ref", FieldPath::Nested("head", "ref")),
                ("base_ref", FieldPath::Nested("base", "ref")),
                ("merged_at", FieldPath::Direct("merged_at")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
                ("closed_at", FieldPath::Direct("closed_at")),
            ],
        }),

        "issues" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("number", FieldPath::Direct("number")),
                ("title", FieldPath::Direct("title")),
                ("state", FieldPath::Direct("state")),
                ("user_login", FieldPath::Nested("user", "login")),
                ("repository", FieldPath::RepoContext),
                ("labels", FieldPath::JsonArray("labels")),
                ("assignee_login", FieldPath::Nested("assignee", "login")),
                ("milestone_title", FieldPath::Nested("milestone", "title")),
                ("comments", FieldPath::Direct("comments")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
                ("closed_at", FieldPath::Direct("closed_at")),
            ],
        }),

        "commits" => Some(FieldMapping {
            fields: &[
                ("sha", FieldPath::Direct("sha")),
                ("message", FieldPath::Nested("commit", "message")),
                ("author_login", FieldPath::Nested("author", "login")),
                ("author_name", FieldPath::DeepNested("commit", "author", "name")),
                ("author_email", FieldPath::DeepNested("commit", "author", "email")),
                ("committer_login", FieldPath::Nested("committer", "login")),
                ("repository", FieldPath::RepoContext),
                ("date", FieldPath::DeepNested("commit", "author", "date")),
            ],
        }),

        "workflows" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("name", FieldPath::Direct("name")),
                ("path", FieldPath::Direct("path")),
                ("state", FieldPath::Direct("state")),
                ("repository", FieldPath::RepoContext),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
            ],
        }),

        "deployments" => Some(FieldMapping {
            fields: &[
                ("id", FieldPath::Direct("id")),
                ("sha", FieldPath::Direct("sha")),
                ("ref_name", FieldPath::Direct("ref")),
                ("task", FieldPath::Direct("task")),
                ("environment", FieldPath::Direct("environment")),
                ("creator_login", FieldPath::Nested("creator", "login")),
                ("repository", FieldPath::RepoContext),
                ("description", FieldPath::Direct("description")),
                ("created_at", FieldPath::Direct("created_at")),
                ("updated_at", FieldPath::Direct("updated_at")),
            ],
        }),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Data parsing
// ---------------------------------------------------------------------------

fn parse_timestamp_str(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
}

fn resolve_field<'a>(
    item: &'a serde_json::Value,
    path: &FieldPath,
    repo_full_name: Option<&str>,
) -> Option<serde_json::Value> {
    match path {
        FieldPath::Direct(key) => item.get(key).filter(|v| !v.is_null()).cloned(),
        FieldPath::Nested(parent, child) => item
            .get(parent)
            .and_then(|p| p.get(child))
            .filter(|v| !v.is_null())
            .cloned(),
        FieldPath::DeepNested(a, b, c) => item
            .get(a)
            .and_then(|x| x.get(b))
            .and_then(|x| x.get(c))
            .filter(|v| !v.is_null())
            .cloned(),
        FieldPath::JsonArray(key) => {
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
        FieldPath::RepoContext => {
            repo_full_name.map(|s| serde_json::Value::String(s.to_string()))
        }
    }
}

fn append_row(
    item: &serde_json::Value,
    table: &str,
    schema: &TableSchema,
    builders: &mut ColumnBuilders,
    repo_full_name: Option<&str>,
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

        let raw_val = resolve_field(item, field_path, repo_full_name);

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
                    .and_then(parse_timestamp_str);
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
impl Connector for GitHubConnector {
    fn source_type(&self) -> SourceType {
        SourceType::GitHub
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables = Vec::with_capacity(ALL_TABLES.len());

        for &name in ALL_TABLES {
            let schema = get_table_schema(name).unwrap();

            let (supports_incremental, incremental_key) = match name {
                "repositories" | "pull_requests" | "issues" | "deployments" => {
                    (true, Some("updated_at".to_string()))
                }
                "commits" => (true, Some("date".to_string())),
                _ => (false, None),
            };

            let pk = match name {
                "commits" => vec!["sha".to_string()],
                _ => vec!["id".to_string()],
            };

            tables.push(TableInfo {
                name: name.to_string(),
                schema,
                supports_incremental,
                incremental_key,
                estimated_rows: None,
                primary_key_columns: pk,
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
        self.api_get_paged("/user").await?;
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

    fn test_config() -> GitHubConfig {
        GitHubConfig::new("test-github-token", "test-org")
    }

    fn test_connector_with_base(base_url: &str) -> GitHubConnector {
        let config = test_config();
        GitHubConnector {
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
    fn test_repositories_schema() {
        let schema = get_table_schema("repositories").unwrap();
        assert_eq!(schema.columns.len(), 14);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].data_type, ColumnType::Int64);
    }

    #[test]
    fn test_pull_requests_schema() {
        let schema = get_table_schema("pull_requests").unwrap();
        assert_eq!(schema.columns.len(), 13);
        let repo = schema.columns.iter().find(|c| c.name == "repository").unwrap();
        assert_eq!(repo.data_type, ColumnType::String);
        assert!(!repo.nullable);
    }

    #[test]
    fn test_commits_schema() {
        let schema = get_table_schema("commits").unwrap();
        assert_eq!(schema.columns.len(), 8);
        assert_eq!(schema.columns[0].name, "sha");
        assert_eq!(schema.columns[0].data_type, ColumnType::String);
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
        assert!(!debug.contains("test-github-token"));
        assert!(debug.contains("test-org"));
    }

    #[test]
    fn test_config_with_repos() {
        let config = GitHubConfig::new("token", "myorg").with_repos(vec!["repo1".into(), "repo2".into()]);
        assert_eq!(config.repos.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_config_api_base() {
        let config = GitHubConfig::new("token", "myorg")
            .with_api_base("https://github.example.com/api/v3");
        assert_eq!(config.base_url(), "https://github.example.com/api/v3");
    }

    #[test]
    fn test_config_default_base() {
        let config = GitHubConfig::new("token", "myorg");
        assert_eq!(config.base_url(), "https://api.github.com");
    }

    // -- Timestamp parsing --

    #[test]
    fn test_parse_timestamp_rfc3339() {
        let ts = parse_timestamp_str("2024-01-15T10:30:00Z");
        assert!(ts.is_some());
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        assert!(parse_timestamp_str("not-a-date").is_none());
    }

    // -- Link header parsing --

    #[test]
    fn test_parse_next_link_basic() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "link",
            reqwest::header::HeaderValue::from_static(
                r#"<https://api.github.com/repos?page=2>; rel="next", <https://api.github.com/repos?page=5>; rel="last""#,
            ),
        );
        let next = parse_next_link(&headers);
        assert_eq!(next, Some("https://api.github.com/repos?page=2".to_string()));
    }

    #[test]
    fn test_parse_next_link_no_next() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "link",
            reqwest::header::HeaderValue::from_static(
                r#"<https://api.github.com/repos?page=1>; rel="prev""#,
            ),
        );
        assert!(parse_next_link(&headers).is_none());
    }

    #[test]
    fn test_parse_next_link_missing_header() {
        let headers = reqwest::header::HeaderMap::new();
        assert!(parse_next_link(&headers).is_none());
    }

    // -- Predicate pushdown tests --

    #[test]
    fn test_predicate_state_equals() {
        let mut query = "?state=all&per_page=100".to_string();
        let predicates = vec![Predicate::Equals {
            column: CompactString::from("state"),
            value: CompactString::from("open"),
        }];
        apply_predicate_params(&mut query, &predicates, "pull_requests");
        assert!(query.contains("state=open"));
        assert!(!query.contains("state=all"));
    }

    #[test]
    fn test_predicate_since() {
        let mut query = "?per_page=100".to_string();
        let predicates = vec![Predicate::GreaterThan {
            column: CompactString::from("updated_at"),
            value: CompactString::from("2024-01-01T00:00:00Z"),
            inclusive: false,
        }];
        apply_predicate_params(&mut query, &predicates, "issues");
        assert!(query.contains("since=2024-01-01T00:00:00Z"));
    }

    #[test]
    fn test_predicate_ignored_for_wrong_table() {
        let mut query = "?per_page=100".to_string();
        let predicates = vec![Predicate::Equals {
            column: CompactString::from("state"),
            value: CompactString::from("open"),
        }];
        apply_predicate_params(&mut query, &predicates, "commits");
        assert!(!query.contains("state="));
    }

    // -- list_tables --

    #[test]
    fn test_list_tables_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connector = GitHubConnector::new(test_config());
        let tables = rt.block_on(connector.list_tables()).unwrap();

        assert_eq!(tables.len(), ALL_TABLES.len());

        let incremental: Vec<_> = tables.iter().filter(|t| t.supports_incremental).collect();
        assert_eq!(incremental.len(), 5);

        let commits_table = tables.iter().find(|t| t.name == "commits").unwrap();
        assert_eq!(commits_table.incremental_key.as_deref(), Some("date"));
        assert_eq!(commits_table.primary_key_columns, vec!["sha"]);
    }

    // -- Mock tests --

    #[tokio::test]
    async fn test_validate_credentials_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/user")
            .match_header("Authorization", "Bearer test-github-token")
            .match_header("X-GitHub-Api-Version", API_VERSION)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"login":"testuser","id":12345}"#)
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
            .mock("GET", "/user")
            .with_status(401)
            .with_body(r#"{"message":"Bad credentials"}"#)
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
            .mock("GET", "/user")
            .with_status(429)
            .with_header("content-type", "application/json")
            .with_body(r#"{"message":"rate limit exceeded"}"#)
            .expect(4)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let result = connector.api_get_paged("/user").await;

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
    async fn test_link_header_pagination_two_pages() {
        let mut server = mockito::Server::new_async().await;

        let page2_url = format!("{}/repos?page=2", server.url());

        let _mock1 = server
            .mock("GET", mockito::Matcher::Regex(r"/orgs/test-org/repos\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_header(
                "link",
                &format!(r#"<{}>; rel="next""#, page2_url),
            )
            .with_body(
                serde_json::json!([
                    {"id": 1, "name": "repo1", "full_name": "test-org/repo1",
                     "owner": {"login": "test-org"}, "private": false,
                     "stargazers_count": 10, "forks_count": 2, "open_issues_count": 5,
                     "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-06-01T00:00:00Z"},
                ])
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let _mock2 = server
            .mock("GET", mockito::Matcher::Regex(r"/repos\?page=2".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!([
                    {"id": 2, "name": "repo2", "full_name": "test-org/repo2",
                     "owner": {"login": "test-org"}, "private": true,
                     "stargazers_count": 5, "forks_count": 1, "open_issues_count": 0,
                     "created_at": "2024-02-01T00:00:00Z", "updated_at": "2024-07-01T00:00:00Z"},
                ])
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("repositories").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_paginated(
                &format!("/orgs/test-org/repos?per_page={}&sort=updated", PAGE_LIMIT),
                None,
                "repositories",
                &schema,
                arrow_schema,
                &options,
                None,
            )
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        _mock1.assert_async().await;
        _mock2.assert_async().await;
    }

    #[tokio::test]
    async fn test_repo_discovery_org() {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock("GET", mockito::Matcher::Regex(r"/orgs/test-org/repos\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!([
                    {"full_name": "test-org/repo1"},
                    {"full_name": "test-org/repo2"},
                ])
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let repos = connector.discover_repos().await.unwrap();
        assert_eq!(repos, vec!["test-org/repo1", "test-org/repo2"]);
    }

    #[tokio::test]
    async fn test_repo_discovery_explicit_repos() {
        let config = GitHubConfig::new("token", "myorg").with_repos(vec!["frontend".into(), "backend".into()]);
        let connector = GitHubConnector::new(config);
        let repos = connector.discover_repos().await.unwrap();
        assert_eq!(repos, vec!["myorg/frontend", "myorg/backend"]);
    }

    #[tokio::test]
    async fn test_issues_filters_out_prs() {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock("GET", mockito::Matcher::Regex(r"/repos/test-org/repo1/issues\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!([
                    {
                        "id": 1, "number": 1, "title": "Bug report",
                        "state": "open", "user": {"login": "alice"},
                        "labels": [{"name": "bug"}],
                        "comments": 3,
                        "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-06-01T00:00:00Z",
                    },
                    {
                        "id": 2, "number": 2, "title": "Fix stuff (PR)",
                        "state": "closed", "user": {"login": "bob"},
                        "labels": [],
                        "comments": 1,
                        "pull_request": {"url": "https://api.github.com/repos/test-org/repo1/pulls/2"},
                        "created_at": "2024-02-01T00:00:00Z",
                        "updated_at": "2024-07-01T00:00:00Z",
                    },
                ])
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let schema = get_table_schema("issues").unwrap();
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        let options = FetchOptions::full_sync();

        let batches = connector
            .fetch_paginated(
                "/repos/test-org/repo1/issues?state=all&per_page=100&filter=all",
                None,
                "issues",
                &schema,
                arrow_schema,
                &options,
                Some("test-org/repo1"),
            )
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_fetch_pull_requests_per_repo() {
        let mut server = mockito::Server::new_async().await;

        let _repos_mock = server
            .mock("GET", mockito::Matcher::Regex(r"/orgs/test-org/repos\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!([
                    {"full_name": "test-org/repo1"},
                ])
                .to_string(),
            )
            .create_async()
            .await;

        let _prs_mock = server
            .mock("GET", mockito::Matcher::Regex(r"/repos/test-org/repo1/pulls\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!([
                    {
                        "id": 100, "number": 1, "title": "Add feature",
                        "state": "open", "user": {"login": "alice"},
                        "draft": false,
                        "head": {"ref": "feature-branch"},
                        "base": {"ref": "main"},
                        "created_at": "2024-03-01T00:00:00Z",
                        "updated_at": "2024-06-15T00:00:00Z",
                    },
                ])
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("pull_requests", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_fetch_workflows() {
        let mut server = mockito::Server::new_async().await;

        let _repos_mock = server
            .mock("GET", mockito::Matcher::Regex(r"/orgs/test-org/repos\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!([
                    {"full_name": "test-org/repo1"},
                ])
                .to_string(),
            )
            .create_async()
            .await;

        let _wf_mock = server
            .mock("GET", mockito::Matcher::Regex(r"/repos/test-org/repo1/actions/workflows".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "total_count": 2,
                    "workflows": [
                        {
                            "id": 1, "name": "CI", "path": ".github/workflows/ci.yml",
                            "state": "active",
                            "created_at": "2024-01-01T00:00:00Z",
                            "updated_at": "2024-06-01T00:00:00Z",
                        },
                        {
                            "id": 2, "name": "Deploy", "path": ".github/workflows/deploy.yml",
                            "state": "active",
                            "created_at": "2024-02-01T00:00:00Z",
                            "updated_at": "2024-07-01T00:00:00Z",
                        },
                    ]
                })
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("workflows", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_fetch_commits() {
        let mut server = mockito::Server::new_async().await;

        let _repos_mock = server
            .mock("GET", mockito::Matcher::Regex(r"/orgs/test-org/repos\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::json!([{"full_name": "test-org/repo1"}]).to_string())
            .create_async()
            .await;

        let _commits_mock = server
            .mock("GET", mockito::Matcher::Regex(r"/repos/test-org/repo1/commits\?.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!([
                    {
                        "sha": "abc123",
                        "commit": {
                            "message": "Initial commit",
                            "author": {
                                "name": "Alice",
                                "email": "alice@example.com",
                                "date": "2024-06-15T10:00:00Z",
                            },
                        },
                        "author": {"login": "alice"},
                        "committer": {"login": "alice"},
                    },
                ])
                .to_string(),
            )
            .create_async()
            .await;

        let connector = test_connector_with_base(&server.url());
        let options = FetchOptions::full_sync();
        let batches = connector.do_fetch("commits", &options).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }
}
