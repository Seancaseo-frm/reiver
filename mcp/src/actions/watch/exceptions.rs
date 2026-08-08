use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::types::ExceptionStatus;
use crate::registry::ActionRegistry;

// ── List Exceptions ─────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListExceptionsInput {
    /// Filter by resolution status
    pub status: Option<ExceptionStatus>,
    /// Maximum number of results to return (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListExceptionsOutput {
    pub exceptions: serde_json::Value,
}

pub struct ListExceptions;

#[async_trait]
impl PlatformAction for ListExceptions {
    type Input = ListExceptionsInput;
    type Output = ListExceptionsOutput;

    fn name(&self) -> &'static str {
        "list_exceptions"
    }
    fn description(&self) -> &'static str {
        "List exceptions (error groups) for the current project. Returns each exception's type, \
         message, occurrence count, affected services, and resolution status. \
         Use get_exception for full stack traces and occurrence history."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/projects/{}/exceptions", ctx.project_id);
        let mut params = vec![];
        if let Some(ref s) = input.status {
            let status_str = serde_json::to_value(s)?;
            if let Some(sv) = status_str.as_str() {
                params.push(format!("status={}", urlencoding::encode(sv)));
            }
        }
        if let Some(l) = input.limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            path.push_str(&format!("?{}", params.join("&")));
        }

        let resp = ctx.http.watch_get(&path).await?;
        let exceptions = resp.json().await?;
        Ok(ListExceptionsOutput { exceptions })
    }
}

// ── Get Exception ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetExceptionInput {
    /// The exception ID to retrieve
    pub exception_id: String,
}

#[derive(Serialize)]
pub struct GetExceptionOutput {
    pub exception: serde_json::Value,
}

pub struct GetException;

#[async_trait]
impl PlatformAction for GetException {
    type Input = GetExceptionInput;
    type Output = GetExceptionOutput;

    fn name(&self) -> &'static str {
        "get_exception"
    }
    fn description(&self) -> &'static str {
        "Get full details for a specific exception including stack trace, occurrence history, \
         and affected services. Use get_root_cause with this exception_id for AI-powered \
         root cause analysis."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!(
            "/api/projects/{}/exceptions/{}",
            ctx.project_id, input.exception_id
        );
        let resp = ctx.http.watch_get(&path).await?;
        let exception = resp.json().await?;
        Ok(GetExceptionOutput { exception })
    }
}

// ── Update Exception Status ─────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateExceptionStatusInput {
    /// The exception group ID to update
    pub exception_id: String,
    /// New status for the exception group
    pub status: ExceptionStatus,
}

#[derive(Serialize)]
pub struct UpdateExceptionStatusOutput {
    pub exception: serde_json::Value,
}

pub struct UpdateExceptionStatus;

#[async_trait]
impl PlatformAction for UpdateExceptionStatus {
    type Input = UpdateExceptionStatusInput;
    type Output = UpdateExceptionStatusOutput;

    fn name(&self) -> &'static str {
        "update_exception_status"
    }
    fn description(&self) -> &'static str {
        "Update the resolution status of an exception group. Set to 'resolved' when the \
         issue is fixed, 'ignored' to suppress it from active views, or 'unresolved' to reopen."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!(
            "/api/projects/{}/exceptions/{}",
            ctx.project_id, input.exception_id
        );
        let status_val = serde_json::to_value(&input.status)?;
        let body = serde_json::json!({ "status": status_val });
        let resp = ctx.http.watch_patch(&path, &body).await?;
        let exception = resp.json().await?;
        Ok(UpdateExceptionStatusOutput { exception })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListExceptions);
    registry.register(GetException);
    registry.register(UpdateExceptionStatus);
}
