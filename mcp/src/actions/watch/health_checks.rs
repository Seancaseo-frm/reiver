use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List Health Checks ──────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListHealthChecksInput {}

#[derive(Serialize)]
pub struct ListHealthChecksOutput {
    pub health_checks: serde_json::Value,
}

pub struct ListHealthChecks;

#[async_trait]
impl PlatformAction for ListHealthChecks {
    type Input = ListHealthChecksInput;
    type Output = ListHealthChecksOutput;

    fn name(&self) -> &'static str {
        "list_health_checks"
    }
    fn description(&self) -> &'static str {
        "List configured health check endpoints for the current project. Returns each check's \
         URL, current status (up/down), response latency, and check frequency."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!("/api/health-checks/checks?project_id={}", ctx.project_id);
        let resp = ctx.http.watch_get(&path).await?;
        let health_checks = resp.json().await?;
        Ok(ListHealthChecksOutput { health_checks })
    }
}

// ── Get Health Check ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetHealthCheckInput {
    /// Health check ID
    pub check_id: String,
}

#[derive(Serialize)]
pub struct GetHealthCheckOutput {
    pub health_check: serde_json::Value,
}

pub struct GetHealthCheck;

#[async_trait]
impl PlatformAction for GetHealthCheck {
    type Input = GetHealthCheckInput;
    type Output = GetHealthCheckOutput;

    fn name(&self) -> &'static str {
        "get_health_check"
    }
    fn description(&self) -> &'static str {
        "Get details of a specific health check including its configuration, \
         current status, and recent results."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!("/api/health-checks/checks/{}", input.check_id);
        let resp = ctx.http.watch_get(&path).await?;
        let health_check = resp.json().await?;
        Ok(GetHealthCheckOutput { health_check })
    }
}

// ── Create Health Check ─────────────────────────────────────────────

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum HealthCheckType {
    Http,
    Tcp,
    Udp,
    Ssl,
}

#[derive(Deserialize, JsonSchema)]
pub struct CreateHealthCheckInput {
    /// Human-readable name for this health check
    pub name: String,
    /// Check type
    pub check_type: HealthCheckType,
    /// Target URL (required for http/ssl checks, e.g. "https://api.example.com/health")
    pub target_url: Option<String>,
    /// Target host (for tcp/udp checks)
    pub target_host: Option<String>,
    /// Target port (for tcp/udp checks)
    pub target_port: Option<i32>,
    /// HTTP method: "GET", "POST", "HEAD" (default: "GET")
    pub http_method: Option<String>,
    /// Custom HTTP headers as key-value pairs
    pub http_headers: Option<serde_json::Value>,
    /// Expected HTTP status codes (e.g. [200, 201]). Default: [200]
    pub http_expected_status: Option<Vec<i32>>,
    /// Timeout for the HTTP request in milliseconds (default: 10000)
    pub http_timeout_ms: Option<i32>,
    /// Check interval in seconds (default: 60)
    #[schemars(range(min = 10, max = 86400))]
    pub check_interval_seconds: Option<i32>,
    /// Overall timeout in seconds (default: 30)
    #[schemars(range(min = 1, max = 120))]
    pub timeout_seconds: Option<i32>,
    /// Regions to run checks from (e.g. ["us-east-1", "eu-west-1"])
    #[serde(default)]
    pub locations: Option<Vec<String>>,
    /// Whether the check is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct CreateHealthCheckOutput {
    pub health_check: serde_json::Value,
}

pub struct CreateHealthCheck;

#[async_trait]
impl PlatformAction for CreateHealthCheck {
    type Input = CreateHealthCheckInput;
    type Output = CreateHealthCheckOutput;

    fn name(&self) -> &'static str {
        "create_health_check"
    }
    fn description(&self) -> &'static str {
        "Create a new health check that periodically probes an endpoint and alerts \
         on failures. Supports HTTP, TCP, UDP, and SSL check types."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let check_type_val = serde_json::to_value(&input.check_type)?;
        let mut body = serde_json::json!({
            "project_id": ctx.project_id,
            "name": input.name,
            "check_type": check_type_val,
        });
        let obj = body.as_object_mut().unwrap();
        if let Some(v) = input.target_url {
            obj.insert("target_url".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.target_host {
            obj.insert("target_host".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.target_port {
            obj.insert("target_port".into(), serde_json::json!(v));
        }
        if let Some(v) = input.http_method {
            obj.insert("http_method".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.http_headers {
            obj.insert("http_headers".into(), v);
        }
        if let Some(v) = input.http_expected_status {
            obj.insert("http_expected_status".into(), serde_json::to_value(v)?);
        }
        if let Some(v) = input.http_timeout_ms {
            obj.insert("http_timeout_ms".into(), serde_json::json!(v));
        }
        if let Some(v) = input.check_interval_seconds {
            obj.insert("check_interval_seconds".into(), serde_json::json!(v));
        }
        if let Some(v) = input.timeout_seconds {
            obj.insert("timeout_seconds".into(), serde_json::json!(v));
        }
        if let Some(v) = input.locations {
            obj.insert("locations".into(), serde_json::to_value(v)?);
        }
        if let Some(v) = input.enabled {
            obj.insert("enabled".into(), serde_json::Value::Bool(v));
        }
        let resp = ctx
            .http
            .watch_post("/api/health-checks/checks", &body)
            .await?;
        let health_check = resp.json().await?;
        Ok(CreateHealthCheckOutput { health_check })
    }
}

// ── Update Health Check ─────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateHealthCheckInput {
    /// Health check ID to update
    pub check_id: String,
    /// Updated name
    pub name: Option<String>,
    /// Updated target URL
    pub target_url: Option<String>,
    /// Updated target host
    pub target_host: Option<String>,
    /// Updated target port
    pub target_port: Option<i32>,
    /// Updated HTTP method
    pub http_method: Option<String>,
    /// Updated HTTP headers
    pub http_headers: Option<serde_json::Value>,
    /// Updated expected HTTP status codes
    pub http_expected_status: Option<Vec<i32>>,
    /// Updated HTTP timeout in milliseconds
    pub http_timeout_ms: Option<i32>,
    /// Updated check interval in seconds
    #[schemars(range(min = 10, max = 86400))]
    pub check_interval_seconds: Option<i32>,
    /// Updated timeout in seconds
    #[schemars(range(min = 1, max = 120))]
    pub timeout_seconds: Option<i32>,
    /// Updated check locations
    pub locations: Option<Vec<String>>,
    /// Whether the check is enabled
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct UpdateHealthCheckOutput {
    pub health_check: serde_json::Value,
}

pub struct UpdateHealthCheck;

#[async_trait]
impl PlatformAction for UpdateHealthCheck {
    type Input = UpdateHealthCheckInput;
    type Output = UpdateHealthCheckOutput;

    fn name(&self) -> &'static str {
        "update_health_check"
    }
    fn description(&self) -> &'static str {
        "Update an existing health check's configuration (URL, interval, expected status, etc.)."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut body = serde_json::Map::new();
        if let Some(n) = input.name {
            body.insert("name".into(), serde_json::Value::String(n));
        }
        if let Some(v) = input.target_url {
            body.insert("target_url".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.target_host {
            body.insert("target_host".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.target_port {
            body.insert("target_port".into(), serde_json::json!(v));
        }
        if let Some(v) = input.http_method {
            body.insert("http_method".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.http_headers {
            body.insert("http_headers".into(), v);
        }
        if let Some(v) = input.http_expected_status {
            body.insert("http_expected_status".into(), serde_json::to_value(v)?);
        }
        if let Some(v) = input.http_timeout_ms {
            body.insert("http_timeout_ms".into(), serde_json::json!(v));
        }
        if let Some(v) = input.check_interval_seconds {
            body.insert("check_interval_seconds".into(), serde_json::json!(v));
        }
        if let Some(v) = input.timeout_seconds {
            body.insert("timeout_seconds".into(), serde_json::json!(v));
        }
        if let Some(v) = input.locations {
            body.insert("locations".into(), serde_json::to_value(v)?);
        }
        if let Some(v) = input.enabled {
            body.insert("enabled".into(), serde_json::Value::Bool(v));
        }
        let resp = ctx
            .http
            .watch_put(
                &format!("/api/health-checks/checks/{}", input.check_id),
                &serde_json::Value::Object(body),
            )
            .await?;
        let health_check = resp.json().await?;
        Ok(UpdateHealthCheckOutput { health_check })
    }
}

// ── Delete Health Check ─────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DeleteHealthCheckInput {
    /// Health check ID to delete
    pub check_id: String,
}

#[derive(Serialize)]
pub struct DeleteHealthCheckOutput {
    pub success: bool,
}

pub struct DeleteHealthCheck;

#[async_trait]
impl PlatformAction for DeleteHealthCheck {
    type Input = DeleteHealthCheckInput;
    type Output = DeleteHealthCheckOutput;

    fn name(&self) -> &'static str {
        "delete_health_check"
    }
    fn description(&self) -> &'static str {
        "Delete a health check. Stops all monitoring for the endpoint."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        ctx.http
            .watch_delete(&format!("/api/health-checks/checks/{}", input.check_id))
            .await?;
        Ok(DeleteHealthCheckOutput { success: true })
    }
}

// ── Get Health Check Results ────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetHealthCheckResultsInput {
    /// Health check ID
    pub check_id: String,
    /// Maximum number of results to return (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct GetHealthCheckResultsOutput {
    pub results: serde_json::Value,
}

pub struct GetHealthCheckResults;

#[async_trait]
impl PlatformAction for GetHealthCheckResults {
    type Input = GetHealthCheckResultsInput;
    type Output = GetHealthCheckResultsOutput;

    fn name(&self) -> &'static str {
        "get_health_check_results"
    }
    fn description(&self) -> &'static str {
        "Get recent check results for a health check endpoint. Returns timestamp, \
         status code, response time, and success/failure for each probe."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/health-checks/checks/{}/results", input.check_id);
        if let Some(l) = input.limit {
            path.push_str(&format!("?limit={l}"));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let results = resp.json().await?;
        Ok(GetHealthCheckResultsOutput { results })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListHealthChecks);
    registry.register(GetHealthCheck);
    registry.register(CreateHealthCheck);
    registry.register(UpdateHealthCheck);
    registry.register(DeleteHealthCheck);
    registry.register(GetHealthCheckResults);
}
