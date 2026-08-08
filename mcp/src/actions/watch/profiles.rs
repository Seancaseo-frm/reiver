use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List Profiles ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListProfilesInput {
    /// Filter by service name
    pub service: Option<String>,
    /// Maximum number of results (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListProfilesOutput {
    pub profiles: serde_json::Value,
}

pub struct ListProfiles;

#[async_trait]
impl PlatformAction for ListProfiles {
    type Input = ListProfilesInput;
    type Output = ListProfilesOutput;

    fn name(&self) -> &'static str {
        "list_profiles"
    }
    fn description(&self) -> &'static str {
        "List continuous profiling snapshots for the current project. Optionally filter \
         by service name. Returns profile IDs, timestamps, and profile types (cpu, heap, etc.)."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/profiles/projects/{}/profiles", ctx.project_id);
        let mut params = vec![];
        if let Some(ref s) = input.service {
            params.push(format!("service={}", urlencoding::encode(s)));
        }
        if let Some(l) = input.limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            path.push_str(&format!("?{}", params.join("&")));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let profiles = resp.json().await?;
        Ok(ListProfilesOutput { profiles })
    }
}

// ── Get Profile ─────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetProfileInput {
    /// Profile snapshot ID
    pub profile_id: String,
}

#[derive(Serialize)]
pub struct GetProfileOutput {
    pub profile: serde_json::Value,
}

pub struct GetProfile;

#[async_trait]
impl PlatformAction for GetProfile {
    type Input = GetProfileInput;
    type Output = GetProfileOutput;

    fn name(&self) -> &'static str {
        "get_profile"
    }
    fn description(&self) -> &'static str {
        "Get a specific profiling snapshot with its flamegraph data, top functions, \
         and resource consumption breakdown."
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
            "/api/profiles/projects/{}/profiles/{}",
            ctx.project_id, input.profile_id
        );
        let resp = ctx.http.watch_get(&path).await?;
        let profile = resp.json().await?;
        Ok(GetProfileOutput { profile })
    }
}

// ── List Service Profiles ───────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListServiceProfilesInput {
    /// Service name to list profiles for
    pub service: String,
    /// Profile type filter (e.g. "cpu", "heap", "goroutine")
    pub profile_type: Option<String>,
    /// Maximum number of results (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListServiceProfilesOutput {
    pub profiles: serde_json::Value,
}

pub struct ListServiceProfiles;

#[async_trait]
impl PlatformAction for ListServiceProfiles {
    type Input = ListServiceProfilesInput;
    type Output = ListServiceProfilesOutput;

    fn name(&self) -> &'static str {
        "list_service_profiles"
    }
    fn description(&self) -> &'static str {
        "List profiling snapshots for a specific service, optionally filtered by profile type."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let svc = urlencoding::encode(&input.service);
        let mut path = format!(
            "/api/profiles/projects/{}/services/{}/profiles",
            ctx.project_id, svc
        );
        let mut params = vec![];
        if let Some(ref pt) = input.profile_type {
            params.push(format!("profile_type={}", urlencoding::encode(pt)));
        }
        if let Some(l) = input.limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            path.push_str(&format!("?{}", params.join("&")));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let profiles = resp.json().await?;
        Ok(ListServiceProfilesOutput { profiles })
    }
}

// ── Compare Profiles ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CompareProfilesInput {
    /// Service name
    pub service: String,
    /// First version identifier (e.g. a deployment version string like "v1.2.3")
    pub version1: String,
    /// Second version identifier to compare against
    pub version2: String,
    /// Start of time range (ISO 8601 timestamp). Defaults to last 7 days.
    pub start_time: Option<String>,
    /// End of time range (ISO 8601 timestamp). Defaults to now.
    pub end_time: Option<String>,
}

#[derive(Serialize)]
pub struct CompareProfilesOutput {
    pub comparison: serde_json::Value,
}

pub struct CompareProfiles;

#[async_trait]
impl PlatformAction for CompareProfiles {
    type Input = CompareProfilesInput;
    type Output = CompareProfilesOutput;

    fn name(&self) -> &'static str {
        "compare_profiles"
    }
    fn description(&self) -> &'static str {
        "Compare profiling data between two deployed versions of a service to identify \
         performance regressions or improvements. Returns CPU, memory, and latency diffs."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let svc = urlencoding::encode(&input.service);
        let mut path = format!(
            "/api/profiles/projects/{}/services/{}/profiles/comparison?version1={}&version2={}",
            ctx.project_id,
            svc,
            urlencoding::encode(&input.version1),
            urlencoding::encode(&input.version2),
        );
        if let Some(ref st) = input.start_time {
            path.push_str(&format!("&start_time={}", urlencoding::encode(st)));
        }
        if let Some(ref et) = input.end_time {
            path.push_str(&format!("&end_time={}", urlencoding::encode(et)));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let comparison = resp.json().await?;
        Ok(CompareProfilesOutput { comparison })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListProfiles);
    registry.register(GetProfile);
    registry.register(ListServiceProfiles);
    registry.register(CompareProfiles);
}
