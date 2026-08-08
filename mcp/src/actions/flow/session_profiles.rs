use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::types::{SessionFilterInput, SessionProfileInput};
use crate::registry::ActionRegistry;

// ── List Session Profiles ──────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListSessionProfilesInput {}

#[derive(Serialize)]
pub struct ListSessionProfilesOutput {
    pub session_profiles: serde_json::Value,
}

pub struct ListSessionProfiles;

#[async_trait]
impl PlatformAction for ListSessionProfiles {
    type Input = ListSessionProfilesInput;
    type Output = ListSessionProfilesOutput;

    fn name(&self) -> &'static str {
        "list_session_profiles"
    }
    fn description(&self) -> &'static str {
        "List all session profiles for the current project. Session profiles are named filter \
         sets that determine which LLM sessions get their content preserved for replay. \
         Returns profile IDs, names, logic (AND/OR), and filter conditions."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!("/api/llm/settings?project_id={pid}"))
            .await?;
        let settings: serde_json::Value = resp.json().await?;
        let profiles = settings
            .get("session_profiles")
            .cloned()
            .unwrap_or(serde_json::Value::Array(vec![]));
        Ok(ListSessionProfilesOutput {
            session_profiles: profiles,
        })
    }
}

// ── Get Session Profile Filter Fields ──────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetSessionProfileFilterFieldsInput {}

#[derive(Serialize)]
pub struct GetSessionProfileFilterFieldsOutput {
    pub fields: serde_json::Value,
}

pub struct GetSessionProfileFilterFields;

#[async_trait]
impl PlatformAction for GetSessionProfileFilterFields {
    type Input = GetSessionProfileFilterFieldsInput;
    type Output = GetSessionProfileFilterFieldsOutput;

    fn name(&self) -> &'static str {
        "get_session_profile_filter_fields"
    }
    fn description(&self) -> &'static str {
        "Get the available virtual fields for session profile filter conditions. Returns each \
         field's path (e.g. \"errors.count\", \"latency.avg_ms\", \"tools.names\"), kind \
         (numeric or set), namespace, label, and unit. Use these field paths when creating \
         session profile filters."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!("/api/llm/settings/filter-fields?project_id={pid}"))
            .await?;
        let fields = resp.json().await?;
        Ok(GetSessionProfileFilterFieldsOutput { fields })
    }
}

// ── Create Session Profile ─────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreateSessionProfileInput {
    /// Human-readable profile name
    pub name: String,
    /// How filters are combined: "AND" or "OR" (default: "AND")
    pub logic: Option<String>,
    /// Filter conditions for matching sessions. Use get resource
    /// "session_profile_filter_fields" to discover available field paths.
    pub filters: Vec<SessionFilterInput>,
}

#[derive(Serialize)]
pub struct CreateSessionProfileOutput {
    pub profile: serde_json::Value,
    pub all_profiles: serde_json::Value,
}

pub struct CreateSessionProfile;

#[async_trait]
impl PlatformAction for CreateSessionProfile {
    type Input = CreateSessionProfileInput;
    type Output = CreateSessionProfileOutput;

    fn name(&self) -> &'static str {
        "create_session_profile"
    }
    fn description(&self) -> &'static str {
        "Create a new session profile. A session profile is a named set of filter conditions \
         that determines which LLM sessions get their content preserved for replay. Provide \
         a name, optional logic (AND/OR, default AND), and one or more filter conditions. \
         Each filter needs a field path (e.g. \"errors.count\"), an optional comparison \
         operator (lt/lte/gt/gte for numeric fields), and a value. \
         To filter by session labels, use field \"labels.names\" with a string value matching \
         a label name (e.g. {\"field\": \"labels.names\", \"value\": \"billing-issue\"}). \
         Labels must first be defined via gateway update_settings session_labels."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let new_profile = SessionProfileInput {
            id: uuid::Uuid::new_v4().to_string(),
            name: input.name,
            logic: input.logic,
            filters: input.filters,
        };
        let profile_json = serde_json::to_value(&new_profile)?;
        let pj = profile_json.clone();

        let all_profiles = mutate_profiles(ctx, |profiles| {
            profiles.push(pj);
            Ok(())
        })
        .await?;

        Ok(CreateSessionProfileOutput {
            profile: profile_json,
            all_profiles,
        })
    }
}

// ── Registration ─────────────────────────────────────────────────────

/// Read-modify-write helper: fetches current settings, applies `mutate` to
/// the session_profiles array, and PUTs the result back. Returns the final
/// profiles array from the server response.
async fn mutate_profiles<F>(ctx: &ActionContext, mutate: F) -> anyhow::Result<serde_json::Value>
where
    F: FnOnce(&mut Vec<serde_json::Value>) -> anyhow::Result<()>,
{
    let pid = ctx.project_id;
    let current_resp = ctx
        .http
        .flow_get(&format!("/api/llm/settings?project_id={pid}"))
        .await?;
    let mut settings: serde_json::Value = current_resp.json().await?;

    let arr = settings
        .as_object_mut()
        .and_then(|m| m.get_mut("session_profiles"))
        .and_then(|v| v.as_array_mut());

    match arr {
        Some(profiles) => mutate(profiles)?,
        None => {
            let mut empty = vec![];
            mutate(&mut empty)?;
            if let Some(map) = settings.as_object_mut() {
                map.insert(
                    "session_profiles".to_string(),
                    serde_json::Value::Array(empty),
                );
            }
        }
    }

    if let Some(map) = settings.as_object_mut() {
        map.insert("project_id".to_string(), serde_json::json!(pid));
    }

    let resp = ctx.http.flow_put("/api/llm/settings", &settings).await?;
    let updated: serde_json::Value = resp.json().await?;
    Ok(updated
        .get("session_profiles")
        .cloned()
        .unwrap_or(serde_json::Value::Array(vec![])))
}

// ── Update Session Profile ─────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateSessionProfileInput {
    /// ID of the session profile to update
    pub id: String,
    /// New name (optional, keeps current if omitted)
    pub name: Option<String>,
    /// New filter logic: "AND" or "OR" (optional, keeps current if omitted)
    pub logic: Option<String>,
    /// Replacement filter conditions (optional, keeps current if omitted).
    /// Use get resource "session_profile_filter_fields" to discover available field paths.
    pub filters: Option<Vec<SessionFilterInput>>,
}

#[derive(Serialize)]
pub struct UpdateSessionProfileOutput {
    pub profile: serde_json::Value,
    pub all_profiles: serde_json::Value,
}

pub struct UpdateSessionProfile;

#[async_trait]
impl PlatformAction for UpdateSessionProfile {
    type Input = UpdateSessionProfileInput;
    type Output = UpdateSessionProfileOutput;

    fn name(&self) -> &'static str {
        "update_session_profile"
    }
    fn description(&self) -> &'static str {
        "Update an existing session profile by ID. Only provided fields are changed — omit \
         a field to keep its current value. Use list resource \"session_profiles\" to find \
         profile IDs."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let target_id = input.id.clone();
        let mut updated_profile = serde_json::Value::Null;

        let all_profiles = mutate_profiles(ctx, |profiles| {
            let profile = profiles
                .iter_mut()
                .find(|p| p.get("id").and_then(|v| v.as_str()) == Some(&target_id))
                .ok_or_else(|| anyhow::anyhow!("Session profile '{target_id}' not found"))?;

            if let Some(name) = &input.name {
                profile["name"] = serde_json::json!(name);
            }
            if let Some(logic) = &input.logic {
                profile["logic"] = serde_json::json!(logic);
            }
            if let Some(filters) = &input.filters {
                profile["filters"] = serde_json::to_value(filters)?;
            }
            updated_profile = profile.clone();
            Ok(())
        })
        .await?;

        Ok(UpdateSessionProfileOutput {
            profile: updated_profile,
            all_profiles,
        })
    }
}

// ── Delete Session Profile ─────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DeleteSessionProfileInput {
    /// ID of the session profile to delete
    pub id: String,
}

#[derive(Serialize)]
pub struct DeleteSessionProfileOutput {
    pub deleted_id: String,
    pub all_profiles: serde_json::Value,
}

pub struct DeleteSessionProfile;

#[async_trait]
impl PlatformAction for DeleteSessionProfile {
    type Input = DeleteSessionProfileInput;
    type Output = DeleteSessionProfileOutput;

    fn name(&self) -> &'static str {
        "delete_session_profile"
    }
    fn description(&self) -> &'static str {
        "Delete a session profile by ID. Use list resource \"session_profiles\" to find \
         profile IDs."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let target_id = input.id.clone();
        let mut found = false;

        let all_profiles = mutate_profiles(ctx, |profiles| {
            let before = profiles.len();
            profiles.retain(|p| p.get("id").and_then(|v| v.as_str()) != Some(&target_id));
            found = profiles.len() < before;
            if !found {
                anyhow::bail!("Session profile '{target_id}' not found");
            }
            Ok(())
        })
        .await?;

        Ok(DeleteSessionProfileOutput {
            deleted_id: target_id,
            all_profiles,
        })
    }
}

// ── Get Session Profile (Prompt Compiler) ──────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetSessionProfileInput {
    /// ID of the session profile to retrieve. Omit to get all profiles with
    /// their filter conditions (useful for understanding optimization goals).
    pub profile_id: Option<String>,
}

#[derive(Serialize)]
pub struct GetSessionProfileOutput {
    pub profiles: serde_json::Value,
}

pub struct GetSessionProfile;

#[async_trait]
impl PlatformAction for GetSessionProfile {
    type Input = GetSessionProfileInput;
    type Output = GetSessionProfileOutput;

    fn name(&self) -> &'static str {
        "get_session_profile"
    }
    fn description(&self) -> &'static str {
        "Get session profile conditions. Returns filter conditions so you can understand \
         what the project optimizes for (e.g. errors.count → fewer errors, cost.total → \
         lower cost, latency.avg_ms → lower latency). If profile_id is provided, returns \
         only that profile; otherwise returns all profiles."
    }
    fn required_scope(&self) -> String {
        "internal:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!("/api/llm/settings?project_id={pid}"))
            .await?;
        let settings: serde_json::Value = resp.json().await?;
        let all_profiles = settings
            .get("session_profiles")
            .cloned()
            .unwrap_or(serde_json::Value::Array(vec![]));

        if let Some(profile_id) = &input.profile_id {
            let matching = all_profiles
                .as_array()
                .and_then(|arr| {
                    arr.iter()
                        .find(|p| p.get("id").and_then(|v| v.as_str()) == Some(profile_id.as_str()))
                })
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            if matching.is_null() {
                anyhow::bail!("Session profile '{}' not found", profile_id);
            }
            Ok(GetSessionProfileOutput {
                profiles: serde_json::json!([matching]),
            })
        } else {
            Ok(GetSessionProfileOutput {
                profiles: all_profiles,
            })
        }
    }
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListSessionProfiles);
    registry.register(GetSessionProfileFilterFields);
    registry.register(CreateSessionProfile);
    registry.register(UpdateSessionProfile);
    registry.register(DeleteSessionProfile);
    registry.register(GetSessionProfile);
}
