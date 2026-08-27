//! LLM Provider Integrations API
//!
//! Manage API keys and configurations for AI providers (OpenAI, Anthropic, Google, Bedrock).

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    routing::{delete, get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::gateway::domain_types::TestStatus;

use crate::api::{extract_organization_id, extract_project_id, extract_user_id};
use crate::app_state::FlowState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::error::{AppError, Result};
use crate::gateway::provider_types::Provider;

fn validate_provider(name: &str) -> Result<()> {
    if Provider::from_str(name).is_err() {
        return Err(AppError::BadRequest(format!(
            "Unsupported provider: {name}"
        )));
    }
    Ok(())
}

/// LLM provider integration response
#[derive(Debug, Serialize, FromRow)]
pub struct LlmIntegration {
    pub provider: String,
    pub enabled: bool,
    pub last_tested_at: Option<DateTime<Utc>>,
    pub last_test_status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request to create/update an LLM integration
#[derive(Debug, Deserialize)]
pub struct CreateLlmIntegration {
    pub provider: String,
    #[serde(default)]
    pub api_key: Option<String>,
    /// Opaque slot ID from create_secret_slot (agent flow — mutually exclusive with api_key)
    #[serde(default)]
    pub secret_slot: Option<String>,
    // AWS Bedrock specific fields
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Opaque slot IDs for Bedrock credentials (agent flow)
    #[serde(default)]
    pub access_key_slot: Option<String>,
    #[serde(default)]
    pub secret_key_slot: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    /// Base URL for providers with per-project endpoints (e.g. Theta Dedicated)
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// Request to update an LLM integration
#[derive(Debug, Deserialize)]
pub struct UpdateLlmIntegration {
    #[serde(default)]
    pub api_key: Option<String>,
    /// Opaque slot ID from create_secret_slot (agent flow — mutually exclusive with api_key)
    #[serde(default)]
    pub secret_slot: Option<String>,
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Opaque slot IDs for Bedrock credentials (agent flow)
    #[serde(default)]
    pub access_key_slot: Option<String>,
    #[serde(default)]
    pub secret_key_slot: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    /// Base URL for providers with per-project endpoints (e.g. Theta Dedicated)
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

/// Request to test a provider connection
#[derive(Debug, Deserialize)]
pub struct TestConnectionRequest {
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
}

/// Create the LLM integrations router
pub fn create_llm_integrations_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/", get(list_integrations))
        .route("/", post(create_integration))
        .route("/{provider}", put(update_integration))
        .route("/{provider}", delete(delete_integration))
        .route("/{provider}/test", post(test_connection))
}

/// List all configured LLM provider integrations for a project
async fn list_integrations(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<LlmIntegration>>> {
    let project_id = extract_project_id(&headers)?;

    // Fetch all LLM integrations for this project
    let integrations: Vec<LlmIntegration> = sqlx::query_as(
        r#"
        SELECT 
            provider,
            enabled,
            last_tested_at,
            COALESCE(last_test_status, 'never') as last_test_status,
            created_at,
            updated_at
        FROM llm_provider_integrations
        WHERE project_id = $1
        ORDER BY provider
        "#,
    )
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await?;

    Ok(Json(integrations))
}

/// Create a new LLM provider integration
async fn create_integration(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Json(req): Json<CreateLlmIntegration>,
) -> Result<Json<LlmIntegration>> {
    let project_id = extract_project_id(&headers)?;
    validate_provider(&req.provider)?;

    let user_id = extract_user_id(&headers)?;
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);

    // Resolve secret slots (agent flow)
    let resolved_api_key = if let Some(slot_id_str) = &req.secret_slot {
        let slot_id = uuid::Uuid::parse_str(slot_id_str)
            .map_err(|_| AppError::BadRequest("Invalid secret_slot ID".to_string()))?;
        let secret = crate::api::secret_slots::resolve_secret_slot(
            &state.db,
            &state.clickhouse,
            &state.encryptor,
            slot_id,
            project_id,
            Some(&audit_origin),
        )
        .await?;
        Some(secret.expose().to_string())
    } else {
        None
    };

    let resolved_access_key = if let Some(slot_id_str) = &req.access_key_slot {
        let slot_id = uuid::Uuid::parse_str(slot_id_str)
            .map_err(|_| AppError::BadRequest("Invalid access_key_slot ID".to_string()))?;
        Some(
            crate::api::secret_slots::resolve_secret_slot(
                &state.db,
                &state.clickhouse,
                &state.encryptor,
                slot_id,
                project_id,
                Some(&audit_origin),
            )
            .await?
            .expose()
            .to_string(),
        )
    } else {
        None
    };

    let resolved_secret_key = if let Some(slot_id_str) = &req.secret_key_slot {
        let slot_id = uuid::Uuid::parse_str(slot_id_str)
            .map_err(|_| AppError::BadRequest("Invalid secret_key_slot ID".to_string()))?;
        Some(
            crate::api::secret_slots::resolve_secret_slot(
                &state.db,
                &state.clickhouse,
                &state.encryptor,
                slot_id,
                project_id,
                Some(&audit_origin),
            )
            .await?
            .expose()
            .to_string(),
        )
    } else {
        None
    };

    let setting_key = format!("gateway_{}_api_key", req.provider);

    let encrypted_value = if req.provider == "bedrock" {
        let access_key_id = resolved_access_key.or(req.access_key_id).ok_or_else(|| {
            AppError::BadRequest(
                "access_key_id or access_key_slot is required for Bedrock".to_string(),
            )
        })?;
        let secret_access_key = resolved_secret_key
            .or(req.secret_access_key)
            .ok_or_else(|| {
                AppError::BadRequest(
                    "secret_access_key or secret_key_slot is required for Bedrock".to_string(),
                )
            })?;
        let creds = serde_json::json!({
            "access_key_id": access_key_id,
            "secret_access_key": secret_access_key,
            "region": req.region.unwrap_or_else(|| "us-east-1".to_string()),
        });
        state
            .encryptor
            .encrypt(&creds.to_string())
            .map_err(|e| AppError::External(e.to_string()))?
    } else if req.provider == "theta-dedicated" {
        // API key is optional for Theta Dedicated; encrypt if provided, else empty string
        let api_key = resolved_api_key.or(req.api_key).unwrap_or_default();
        state
            .encryptor
            .encrypt(&api_key)
            .map_err(|e| AppError::External(e.to_string()))?
    } else {
        let api_key = resolved_api_key.or(req.api_key).ok_or_else(|| {
            AppError::BadRequest("api_key or secret_slot is required".to_string())
        })?;
        state
            .encryptor
            .encrypt(&api_key)
            .map_err(|e| AppError::External(e.to_string()))?
    };

    // Use transaction to ensure atomicity of both writes
    let mut tx = state.db.begin().await?;

    // For providers with per-project endpoints, store the base URL as a separate project_setting
    if req.provider == "theta-dedicated"
        || req.provider == "cloudflare"
        || req.provider == "azure-openai"
    {
        let base_url = req.base_url.ok_or_else(|| {
            AppError::BadRequest(format!("base_url is required for {}", req.provider))
        })?;
        let url_key = format!("gateway_{}_base_url", req.provider);
        sqlx::query(
            r#"
            INSERT INTO project_settings (project_id, key, value)
            VALUES ($1, $2, $3)
            ON CONFLICT (project_id, key) DO UPDATE SET value = $3
            "#,
        )
        .bind(project_id)
        .bind(&url_key)
        .bind(&base_url)
        .execute(&mut *tx)
        .await?;
    }

    // Upsert the setting
    sqlx::query(
        r#"
        INSERT INTO project_settings (project_id, key, value)
        VALUES ($1, $2, $3)
        ON CONFLICT (project_id, key) DO UPDATE SET value = $3
        "#,
    )
    .bind(project_id)
    .bind(&setting_key)
    .bind(&encrypted_value)
    .execute(&mut *tx)
    .await?;

    // Create the integration record
    let now = Utc::now();
    let integration_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO llm_provider_integrations (id, project_id, provider, enabled, last_test_status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, 'never', $5, $5)
        ON CONFLICT (project_id, provider) DO UPDATE SET enabled = $4, updated_at = $5
        "#
    )
    .bind(integration_id)
    .bind(project_id)
    .bind(&req.provider)
    .bind(req.enabled)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    // Commit the transaction
    tx.commit().await?;

    // Evict any cached key for this provider so the new key is used immediately
    state
        .provider_key_cache
        .remove(&(project_id, req.provider.clone()));

    let org_id = extract_organization_id(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::LlmIntegrationCreated)
        .user(user_id)
        .project(&project_id.to_string())
        .details(serde_json::json!({
            "created": {
                "provider": &req.provider,
                "enabled": req.enabled,
            }
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        );
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(LlmIntegration {
        provider: req.provider,
        enabled: req.enabled,
        last_tested_at: None,
        last_test_status: "never".to_string(),
        created_at: now,
        updated_at: now,
    }))
}

/// Update an LLM provider integration
async fn update_integration(
    State(state): State<Arc<FlowState>>,
    Path(provider): Path<String>,
    headers: HeaderMap,
    Json(req): Json<UpdateLlmIntegration>,
) -> Result<Json<LlmIntegration>> {
    let project_id = extract_project_id(&headers)?;
    validate_provider(&provider)?;

    let user_id = extract_user_id(&headers)?;
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);

    let before_integration: Option<LlmIntegration> = sqlx::query_as(
        r#"
        SELECT provider, enabled, last_tested_at,
               COALESCE(last_test_status, 'never') as last_test_status,
               created_at, updated_at
        FROM llm_provider_integrations
        WHERE project_id = $1 AND provider = $2
        "#,
    )
    .bind(project_id)
    .bind(&provider)
    .fetch_optional(state.db.as_ref())
    .await?;

    // Resolve secret slots if provided (agent flow)
    let resolved_api_key = if let Some(slot_id_str) = &req.secret_slot {
        let slot_id = uuid::Uuid::parse_str(slot_id_str)
            .map_err(|_| AppError::BadRequest("Invalid secret_slot ID".to_string()))?;
        Some(
            crate::api::secret_slots::resolve_secret_slot(
                &state.db,
                &state.clickhouse,
                &state.encryptor,
                slot_id,
                project_id,
                Some(&audit_origin),
            )
            .await?
            .expose()
            .to_string(),
        )
    } else {
        None
    };

    let resolved_access_key = if let Some(slot_id_str) = &req.access_key_slot {
        let slot_id = uuid::Uuid::parse_str(slot_id_str)
            .map_err(|_| AppError::BadRequest("Invalid access_key_slot ID".to_string()))?;
        Some(
            crate::api::secret_slots::resolve_secret_slot(
                &state.db,
                &state.clickhouse,
                &state.encryptor,
                slot_id,
                project_id,
                Some(&audit_origin),
            )
            .await?
            .expose()
            .to_string(),
        )
    } else {
        None
    };

    let resolved_secret_key = if let Some(slot_id_str) = &req.secret_key_slot {
        let slot_id = uuid::Uuid::parse_str(slot_id_str)
            .map_err(|_| AppError::BadRequest("Invalid secret_key_slot ID".to_string()))?;
        Some(
            crate::api::secret_slots::resolve_secret_slot(
                &state.db,
                &state.clickhouse,
                &state.encryptor,
                slot_id,
                project_id,
                Some(&audit_origin),
            )
            .await?
            .expose()
            .to_string(),
        )
    } else {
        None
    };

    let has_api_key = resolved_api_key.is_some() || req.api_key.is_some();
    let has_bedrock_keys = resolved_access_key.is_some() || req.access_key_id.is_some();
    let has_base_url = req.base_url.is_some();
    let credentials_updated = has_api_key || has_bedrock_keys || has_base_url;

    if credentials_updated {
        // Update API key if provided
        if has_api_key || has_bedrock_keys {
            let setting_key = format!("gateway_{}_api_key", provider);

            let encrypted_value = if provider == "bedrock" {
                let access_key_id = resolved_access_key.or(req.access_key_id).ok_or_else(|| {
                    AppError::BadRequest(
                        "access_key_id or access_key_slot is required for Bedrock".to_string(),
                    )
                })?;
                let secret_access_key =
                    resolved_secret_key
                        .or(req.secret_access_key)
                        .ok_or_else(|| {
                            AppError::BadRequest(
                                "secret_access_key or secret_key_slot is required for Bedrock"
                                    .to_string(),
                            )
                        })?;
                let creds = serde_json::json!({
                    "access_key_id": access_key_id,
                    "secret_access_key": secret_access_key,
                    "region": req.region.unwrap_or_else(|| "us-east-1".to_string()),
                });
                state
                    .encryptor
                    .encrypt(&creds.to_string())
                    .map_err(|e| AppError::External(e.to_string()))?
            } else if let Some(api_key) = resolved_api_key.or(req.api_key) {
                state
                    .encryptor
                    .encrypt(&api_key)
                    .map_err(|e| AppError::External(e.to_string()))?
            } else {
                return Err(AppError::BadRequest(
                    "api_key or secret_slot is required".to_string(),
                ));
            };

            sqlx::query(
                r#"
                INSERT INTO project_settings (project_id, key, value)
                VALUES ($1, $2, $3)
                ON CONFLICT (project_id, key) DO UPDATE SET value = $3
                "#,
            )
            .bind(project_id)
            .bind(&setting_key)
            .bind(&encrypted_value)
            .execute(state.db.as_ref())
            .await?;
        }

        // Update base URL if provided (for Theta Dedicated)
        if let Some(ref base_url) = req.base_url {
            let url_key = format!("gateway_{}_base_url", provider);
            sqlx::query(
                r#"
                INSERT INTO project_settings (project_id, key, value)
                VALUES ($1, $2, $3)
                ON CONFLICT (project_id, key) DO UPDATE SET value = $3
                "#,
            )
            .bind(project_id)
            .bind(&url_key)
            .bind(base_url)
            .execute(state.db.as_ref())
            .await?;
        }

        // Evict cached key so the updated key is used immediately
        state
            .provider_key_cache
            .remove(&(project_id, provider.clone()));
    }

    // Update enabled status if provided
    let now = Utc::now();
    if let Some(enabled) = req.enabled {
        sqlx::query(
            r#"
            UPDATE llm_provider_integrations
            SET enabled = $1, updated_at = $2
            WHERE project_id = $3 AND provider = $4
            "#,
        )
        .bind(enabled)
        .bind(now)
        .bind(project_id)
        .bind(&provider)
        .execute(state.db.as_ref())
        .await?;
    }

    // Fetch updated integration
    let integration: Option<LlmIntegration> = sqlx::query_as(
        r#"
        SELECT 
            provider,
            enabled,
            last_tested_at,
            COALESCE(last_test_status, 'never') as last_test_status,
            created_at,
            updated_at
        FROM llm_provider_integrations
        WHERE project_id = $1 AND provider = $2
        "#,
    )
    .bind(project_id)
    .bind(&provider)
    .fetch_optional(state.db.as_ref())
    .await?;

    if integration.is_some() {
        let org_id = extract_organization_id(&headers);
        let before_enabled = before_integration.as_ref().map(|i| i.enabled);
        let after_enabled = integration.as_ref().map(|i| i.enabled);
        let mut audit = AuditEventBuilder::new(AuditEventType::LlmIntegrationUpdated)
            .user(user_id)
            .project(&project_id.to_string())
            .details(serde_json::json!({
                "provider": &provider,
                "credentials_updated": credentials_updated,
                "before": { "enabled": before_enabled },
                "after": { "enabled": after_enabled },
            }))
            .origin(
                &audit_origin.origin_type,
                &audit_origin.origin_ref,
                &audit_origin.origin_reason,
            )
            .caller(
                &audit_caller.caller_type,
                &audit_caller.key_label,
                &audit_caller.key_prefix,
            );
        if let Some(oid) = org_id {
            audit = audit.organization(oid);
        }
        audit.log(&state.clickhouse).await;
    }

    integration
        .map(Json)
        .ok_or_else(|| AppError::NotFound("Integration not found".to_string()))
}

/// Delete an LLM provider integration
async fn delete_integration(
    State(state): State<Arc<FlowState>>,
    Path(provider): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    let project_id = extract_project_id(&headers)?;
    validate_provider(&provider)?;

    let user_id = extract_user_id(&headers)?;
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);

    // Use transaction to ensure atomicity of both deletes
    let mut tx = state.db.begin().await?;

    // Delete the API key from settings
    let setting_key = format!("gateway_{}_api_key", provider);
    sqlx::query("DELETE FROM project_settings WHERE project_id = $1 AND key = $2")
        .bind(project_id)
        .bind(&setting_key)
        .execute(&mut *tx)
        .await?;

    // Delete the integration record
    sqlx::query("DELETE FROM llm_provider_integrations WHERE project_id = $1 AND provider = $2")
        .bind(project_id)
        .bind(&provider)
        .execute(&mut *tx)
        .await?;

    // Commit the transaction
    tx.commit().await?;

    // Evict cached key so deleted credentials aren't served
    state
        .provider_key_cache
        .remove(&(project_id, provider.clone()));

    let org_id = extract_organization_id(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::LlmIntegrationDeleted)
        .user(user_id)
        .project(&project_id.to_string())
        .details(serde_json::json!({
            "deleted": {
                "provider": &provider,
            }
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        );
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(serde_json::json!({ "deleted": true })))
}

/// Test connection to an LLM provider
async fn test_connection(
    State(state): State<Arc<FlowState>>,
    Path(provider): Path<String>,
    headers: HeaderMap,
    Json(req): Json<TestConnectionRequest>,
) -> Result<Json<serde_json::Value>> {
    let project_id = extract_project_id(&headers)?;

    // Get API key - use provided or fetch from settings
    // For theta-dedicated the API key is optional; avoid erroring on missing.
    let api_key = if let Some(key) = req.api_key {
        key
    } else if provider == "theta-dedicated" {
        // Try to fetch stored key, default to empty if not configured
        let setting_key = format!("gateway_{}_api_key", provider);
        #[derive(FromRow)]
        struct SettingValue {
            value: String,
        }
        let setting: Option<SettingValue> =
            sqlx::query_as("SELECT value FROM project_settings WHERE project_id = $1 AND key = $2")
                .bind(project_id)
                .bind(&setting_key)
                .fetch_optional(state.db.as_ref())
                .await?;
        match setting {
            Some(s) => state.encryptor.decrypt(&s.value).unwrap_or_default(),
            None => String::new(),
        }
    } else {
        let setting_key = format!("gateway_{}_api_key", provider);

        #[derive(FromRow)]
        struct SettingValue {
            value: String,
        }

        let setting: Option<SettingValue> =
            sqlx::query_as("SELECT value FROM project_settings WHERE project_id = $1 AND key = $2")
                .bind(project_id)
                .bind(&setting_key)
                .fetch_optional(state.db.as_ref())
                .await?;

        match setting {
            Some(s) => state
                .encryptor
                .decrypt(&s.value)
                .map_err(|e| AppError::External(e.to_string()))?,
            None => return Err(AppError::NotFound("Provider not configured".to_string())),
        }
    };

    // Test the connection based on provider
    let test_result = match provider.as_str() {
        "openai" => test_openai_connection(&api_key).await,
        "anthropic" => test_anthropic_connection(&api_key).await,
        "google" => test_google_connection(&api_key).await,
        "bedrock" => {
            // For Bedrock, parse credentials from JSON or request
            if let Some(access_key) = req.access_key_id {
                let secret = req.secret_access_key.unwrap_or_default();
                let region = req.region.unwrap_or_else(|| "us-east-1".to_string());
                test_bedrock_connection(&access_key, &secret, &region).await
            } else {
                // Parse from stored JSON
                let creds: serde_json::Value = serde_json::from_str(&api_key)
                    .map_err(|_| AppError::External("Invalid credentials format".to_string()))?;
                let access_key = creds["access_key_id"].as_str().unwrap_or("");
                let secret = creds["secret_access_key"].as_str().unwrap_or("");
                let region = creds["region"].as_str().unwrap_or("us-east-1");
                test_bedrock_connection(access_key, secret, region).await
            }
        }
        "theta" => test_theta_connection(&api_key).await,
        "theta-dedicated" => {
            let base_url = if let Some(url) = req.base_url {
                url
            } else {
                // Fetch stored base URL
                let url_key = format!("gateway_{}_base_url", provider);
                #[derive(FromRow)]
                struct SettingValue {
                    value: String,
                }
                let setting: Option<SettingValue> = sqlx::query_as(
                    "SELECT value FROM project_settings WHERE project_id = $1 AND key = $2",
                )
                .bind(project_id)
                .bind(&url_key)
                .fetch_optional(state.db.as_ref())
                .await?;
                match setting {
                    Some(s) => s.value,
                    None => {
                        return Err(AppError::BadRequest(
                            "Base URL not configured for Theta Dedicated".to_string(),
                        ))
                    }
                }
            };
            test_theta_dedicated_connection(&api_key, &base_url).await
        }
        "deepseek" => test_deepseek_connection(&api_key).await,
        "sambanova" => {
            test_openai_compat_connection(&api_key, "https://api.sambanova.ai/v1/models").await
        }
        "lambda" => {
            test_openai_compat_connection(&api_key, "https://api.lambdalabs.com/v1/models").await
        }
        "lepton" => {
            test_openai_compat_connection(&api_key, "https://api.lepton.ai/v1/models").await
        }
        "hyperbolic" => {
            test_openai_compat_connection(&api_key, "https://api.hyperbolic.xyz/v1/models").await
        }
        "ovhcloud" => {
            test_openai_compat_connection(&api_key, "https://ovh.hf.space/v1/models").await
        }
        "novita" => {
            test_openai_compat_connection(&api_key, "https://api.novita.ai/v3/openai/models").await
        }
        "huggingface" => {
            test_openai_compat_connection(&api_key, "https://router.huggingface.co/v1/models").await
        }
        "cloudflare" => {
            let base_url = if let Some(url) = req.base_url {
                url
            } else {
                let url_key = format!("gateway_{}_base_url", provider);
                #[derive(FromRow)]
                struct SettingValue {
                    value: String,
                }
                let setting: Option<SettingValue> = sqlx::query_as(
                    "SELECT value FROM project_settings WHERE project_id = $1 AND key = $2",
                )
                .bind(project_id)
                .bind(&url_key)
                .fetch_optional(state.db.as_ref())
                .await?;
                match setting {
                    Some(s) => s.value,
                    None => {
                        return Err(AppError::BadRequest(
                            "Account ID not configured for Cloudflare Workers AI".to_string(),
                        ))
                    }
                }
            };
            test_openai_compat_connection(
                &api_key,
                &format!("{}/models", base_url.trim_end_matches('/')),
            )
            .await
        }
        "azure-openai" => {
            let base_url = if let Some(url) = req.base_url {
                url
            } else {
                let url_key = format!("gateway_{}_base_url", provider);
                #[derive(FromRow)]
                struct SettingValue {
                    value: String,
                }
                let setting: Option<SettingValue> = sqlx::query_as(
                    "SELECT value FROM project_settings WHERE project_id = $1 AND key = $2",
                )
                .bind(project_id)
                .bind(&url_key)
                .fetch_optional(state.db.as_ref())
                .await?;
                match setting {
                    Some(s) => s.value,
                    None => {
                        return Err(AppError::BadRequest(
                            "Resource URL not configured for Azure OpenAI".to_string(),
                        ))
                    }
                }
            };
            test_azure_openai_connection(&api_key, &base_url).await
        }
        "vertex-ai" => {
            test_openai_compat_connection(
                &api_key,
                "https://us-central1-aiplatform.googleapis.com/v1beta1/openai/models",
            )
            .await
        }
        "x-ai" => test_openai_compat_connection(&api_key, "https://api.x.ai/v1/models").await,
        "mistralai" => {
            test_openai_compat_connection(&api_key, "https://api.mistral.ai/v1/models").await
        }
        "qwen" => {
            test_openai_compat_connection(
                &api_key,
                "https://dashscope-intl.aliyuncs.com/compatible-mode/v1/models",
            )
            .await
        }
        "ai21" => {
            test_openai_compat_connection(&api_key, "https://api.ai21.com/studio/v1/models").await
        }
        "cerebras" => {
            test_openai_compat_connection(&api_key, "https://api.cerebras.ai/v1/models").await
        }
        "cohere" => {
            test_openai_compat_connection(&api_key, "https://api.cohere.com/v2/models").await
        }
        "deepinfra" => {
            test_openai_compat_connection(&api_key, "https://api.deepinfra.com/v1/openai/models")
                .await
        }
        "fireworks" => {
            test_openai_compat_connection(&api_key, "https://api.fireworks.ai/inference/v1/models")
                .await
        }
        "groq" => {
            test_openai_compat_connection(&api_key, "https://api.groq.com/openai/v1/models").await
        }
        "nvidia" => {
            test_openai_compat_connection(&api_key, "https://integrate.api.nvidia.com/v1/models")
                .await
        }
        "openrouter" => {
            test_openai_compat_connection(&api_key, "https://openrouter.ai/api/v1/models").await
        }
        "perplexity" => {
            test_openai_compat_connection(&api_key, "https://api.perplexity.ai/models").await
        }
        "together" => {
            test_openai_compat_connection(&api_key, "https://api.together.xyz/v1/models").await
        }
        _ => Err(AppError::BadRequest("Unsupported provider".to_string())),
    };

    // Update test status
    let now = Utc::now();
    let status = if test_result.is_ok() {
        TestStatus::Success
    } else {
        TestStatus::Failed
    };

    sqlx::query(
        r#"
        UPDATE llm_provider_integrations
        SET last_tested_at = $1, last_test_status = $2, updated_at = $1
        WHERE project_id = $3 AND provider = $4
        "#,
    )
    .bind(now)
    .bind(status.as_str())
    .bind(project_id)
    .bind(&provider)
    .execute(state.db.as_ref())
    .await?;

    match test_result {
        Ok(_) => Ok(Json(serde_json::json!({ "status": "success" }))),
        Err(e) => Err(e),
    }
}

/// Test OpenAI connection
async fn test_openai_connection(api_key: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get("https://api.openai.com/v1/models")
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| AppError::External(format!("Connection failed: {}", e)))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(AppError::BadRequest(
            "Invalid API key or connection failed".to_string(),
        ))
    }
}

/// Test Anthropic connection
async fn test_anthropic_connection(api_key: &str) -> Result<()> {
    let client = reqwest::Client::new();
    // Authentication check only. Model inference is proved separately by the
    // explicit Flow/Playground smoke test in onboarding.
    let response = client
        .get("https://api.anthropic.com/v1/models?limit=1")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| AppError::External(format!("Connection failed: {}", e)))?;

    if response.status().is_success() {
        Ok(())
    } else if response.status().as_u16() == 401 {
        Err(AppError::BadRequest("Invalid API key".to_string()))
    } else if response.status().as_u16() == 403 {
        Err(AppError::BadRequest(
            "Anthropic API key does not have permission to list models".to_string(),
        ))
    } else {
        Err(AppError::BadRequest(format!(
            "Anthropic authentication test failed (HTTP {})",
            response.status().as_u16()
        )))
    }
}

/// Test Google (Gemini) connection
async fn test_google_connection(api_key: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get("https://generativelanguage.googleapis.com/v1/models")
        .header("x-goog-api-key", api_key)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| AppError::External(format!("Connection failed: {}", e)))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(AppError::BadRequest(
            "Invalid API key or connection failed".to_string(),
        ))
    }
}

/// Test AWS Bedrock connection by listing available foundation models
///
/// Uses the Bedrock management API (not BedrockRuntime) to call list_foundation_models,
/// which is a read-only operation that doesn't incur any model usage costs.
async fn test_bedrock_connection(access_key: &str, secret_key: &str, region: &str) -> Result<()> {
    use aws_sdk_bedrock::config::{Credentials, Region};
    use aws_sdk_bedrock::Client as BedrockClient;

    // Validate credentials format
    if access_key.is_empty() || secret_key.is_empty() {
        return Err(AppError::BadRequest("Missing AWS credentials".to_string()));
    }

    // Create AWS credentials
    let credentials = Credentials::new(
        access_key,
        secret_key,
        None, // No session token
        None, // No expiration
        "reiver-connection-test",
    );

    // Build the Bedrock management client config
    let config = aws_sdk_bedrock::Config::builder()
        .region(Region::new(region.to_string()))
        .credentials_provider(credentials)
        .build();

    let client = BedrockClient::from_conf(config);

    // Use list_foundation_models - a read-only API that doesn't incur costs
    // This validates credentials without making any model inference calls
    let result = client.list_foundation_models().send().await;

    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            let error_str = e.to_string();
            // Check for authentication errors
            if error_str.contains("UnrecognizedClientException")
                || error_str.contains("InvalidSignatureException")
                || error_str.contains("IncompleteSignature")
                || error_str.contains("AccessDenied")
                || error_str.contains("InvalidAccessKeyId")
                || error_str.contains("ExpiredToken")
            {
                Err(AppError::BadRequest(format!(
                    "AWS authentication failed: {}",
                    error_str
                )))
            } else if error_str.contains("ThrottlingException") {
                // Rate limited but credentials are valid
                Ok(())
            } else {
                Err(AppError::External(format!(
                    "Bedrock connection test failed: {}",
                    error_str
                )))
            }
        }
    }
}

/// Test Theta EdgeCloud on-demand API connection by listing available services.
async fn test_theta_connection(api_key: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get("https://ondemand.thetaedgecloud.com/service/list")
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| AppError::External(format!("Connection failed: {}", e)))?;

    if response.status().is_success() {
        Ok(())
    } else if response.status().as_u16() == 401 {
        Err(AppError::BadRequest("Invalid Theta API key".to_string()))
    } else {
        Err(AppError::BadRequest(format!(
            "Theta connection test failed (HTTP {})",
            response.status().as_u16()
        )))
    }
}

/// Test Theta Dedicated deployment connection by hitting the OpenAI-compatible models endpoint.
async fn test_theta_dedicated_connection(api_key: &str, base_url: &str) -> Result<()> {
    let url = format!("{}/models", base_url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let mut request = client.get(&url).timeout(std::time::Duration::from_secs(10));
    if !api_key.is_empty() {
        request = request.header("Authorization", format!("Bearer {}", api_key));
    }
    let response = request
        .send()
        .await
        .map_err(|e| AppError::External(format!("Connection failed: {}", e)))?;

    if response.status().is_success() {
        Ok(())
    } else if response.status().as_u16() == 401 || response.status().as_u16() == 403 {
        Err(AppError::BadRequest(
            "Authentication failed for Theta Dedicated deployment".to_string(),
        ))
    } else {
        Err(AppError::BadRequest(format!(
            "Theta Dedicated connection test failed (HTTP {})",
            response.status().as_u16()
        )))
    }
}

/// Generic connection test for OpenAI-compatible providers.
///
/// Hits the `/models` endpoint with a bearer token and checks for a successful response.
async fn test_openai_compat_connection(api_key: &str, models_url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get(models_url)
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| AppError::External(format!("Connection failed: {}", e)))?;

    if response.status().is_success() {
        Ok(())
    } else if response.status().as_u16() == 401 || response.status().as_u16() == 403 {
        Err(AppError::BadRequest("Invalid API key".to_string()))
    } else {
        Err(AppError::BadRequest(format!(
            "Connection test failed (HTTP {})",
            response.status().as_u16()
        )))
    }
}

/// Test Azure OpenAI connection using the `api-key` header.
async fn test_azure_openai_connection(api_key: &str, base_url: &str) -> Result<()> {
    let url = format!("{}/models", base_url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("api-key", api_key)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| AppError::External(format!("Connection failed: {}", e)))?;

    if response.status().is_success() {
        Ok(())
    } else if response.status().as_u16() == 401 || response.status().as_u16() == 403 {
        Err(AppError::BadRequest(
            "Invalid API key or resource URL".to_string(),
        ))
    } else {
        Err(AppError::BadRequest(format!(
            "Azure OpenAI connection test failed (HTTP {})",
            response.status().as_u16()
        )))
    }
}

/// Test DeepSeek connection by hitting the models endpoint.
async fn test_deepseek_connection(api_key: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get("https://api.deepseek.com/models")
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| AppError::External(format!("Connection failed: {}", e)))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(AppError::BadRequest(
            "Invalid API key or connection failed".to_string(),
        ))
    }
}
