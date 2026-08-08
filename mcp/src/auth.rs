use anyhow::{anyhow, Context};
use reqwest::Client;
use serde::Deserialize;
use uuid::Uuid;

use crate::action::{ActionContext, Caller};
use crate::client::InternalClient;

#[derive(Debug, Deserialize)]
struct ValidateKeyResponse {
    project_id: Uuid,
    key_id: Uuid,
    organization_id: Uuid,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    key_type: String,
    #[serde(default)]
    key_prefix: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    created_by: Option<Uuid>,
}

/// Validates a project API key against the website's `/api/auth/validate-key`
/// endpoint and builds an [`ActionContext`] with the resolved `project_id`
/// and `key_id`.
pub async fn authenticate(
    api_key: &str,
    website_url: &str,
    flow_url: &str,
    watch_url: &str,
) -> anyhow::Result<ActionContext> {
    let http = Client::new();

    let resp = http
        .get(format!("{website_url}/api/auth/validate-key"))
        .bearer_auth(api_key)
        .send()
        .await
        .context("failed to reach website for key validation")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "API key validation failed (status {status}): {body}"
        ));
    }

    let key_info: ValidateKeyResponse = resp
        .json()
        .await
        .context("failed to parse key validation response")?;

    if key_info.key_type != "agent" {
        return Err(anyhow!(
            "MCP requires an agent token. SDK keys are not accepted. \
             Create an agent token in project settings."
        ));
    }

    let mut client = InternalClient::new(
        website_url.to_string(),
        flow_url.to_string(),
        watch_url.to_string(),
        key_info.project_id,
        api_key.to_string(),
    )
    .with_creator("agent", &key_info.label, &key_info.key_prefix);
    if let Some(uid) = key_info.created_by {
        client = client.with_user_id(uid);
    }

    Ok(ActionContext {
        project_id: key_info.project_id,
        caller: Caller::ApiKey {
            key_id: key_info.key_id,
        },
        scopes: key_info.scopes,
        http: client,
        db: None,
        clickhouse: None,
        encryptor: None,
        asset_storage: None,
        kb_embedder: None,
        meter_service: None,
        organization_id: Some(key_info.organization_id),
        entitlements: std::sync::Arc::new(reiver_core::entitlements::UnlimitedEntitlements),
        key_prefix: key_info.key_prefix,
        key_label: key_info.label,
    })
}
