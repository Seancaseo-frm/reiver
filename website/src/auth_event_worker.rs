//! Auth Event Worker
//!
//! Polls identity provider APIs (Okta, Auth0, Entra ID, OneLogin, Ping Identity)
//! to ingest authentication events for correlation with application traces/errors.

use anyhow::Result;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

use crate::clickhouse_db::ClickHousePool;

/// Start the auth event polling worker
pub fn start_auth_event_worker(
    db: Arc<PgPool>,
    clickhouse: Arc<ClickHousePool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> JoinHandle<()> {
    info!("Starting auth event worker");

    tokio::spawn(async move {
        let mut poll_interval = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    if let Err(e) = poll_all_integrations(&db, &clickhouse).await {
                        error!("Auth event worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Auth event worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("Auth event worker stopped");
    })
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct IntegrationConfig {
    id: uuid::Uuid,
    project_id: uuid::Uuid,
    provider: String,
    domain: Option<String>,
    tenant_id: Option<String>,
    environment_id: Option<String>,
    region: Option<String>,
    api_token_encrypted: Option<String>,
    client_id: Option<String>,
    client_secret_encrypted: Option<String>,
    poll_interval_seconds: i32,
    event_types: Vec<String>,
    last_poll_at: Option<chrono::DateTime<chrono::Utc>>,
    last_event_id: Option<String>,
    last_event_timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

async fn poll_all_integrations(db: &PgPool, clickhouse: &ClickHousePool) -> Result<()> {
    // Get integrations that need polling
    let integrations = sqlx::query_as::<_, IntegrationConfig>(
        r#"
        SELECT id, project_id, provider, domain, tenant_id, environment_id, region,
               api_token_encrypted, client_id, client_secret_encrypted,
               poll_interval_seconds, event_types, last_poll_at, last_event_id, last_event_timestamp
        FROM auth_event_integration_configs
        WHERE enabled = true
          AND (last_poll_at IS NULL OR last_poll_at < NOW() - (poll_interval_seconds || ' seconds')::INTERVAL)
          AND consecutive_errors < 10
        ORDER BY last_poll_at NULLS FIRST
        LIMIT 10
        "#
    )
    .fetch_all(db)
    .await?;

    if integrations.is_empty() {
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    for config in integrations {
        let result = match config.provider.as_str() {
            "okta" => poll_okta(&client, &config, clickhouse).await,
            "auth0" => poll_auth0(&client, &config, clickhouse).await,
            "entra_id" => poll_entra(&client, &config, clickhouse).await,
            "onelogin" => poll_onelogin(&client, &config, clickhouse).await,
            "ping_identity" => poll_ping(&client, &config, clickhouse).await,
            "keycloak" => poll_keycloak(&client, &config, clickhouse).await,
            _ => {
                warn!("Unknown provider: {}", config.provider);
                continue;
            }
        };

        match result {
            Ok((event_count, last_id, last_ts)) => {
                info!(
                    "Polled {} events from {} for project {}",
                    event_count, config.provider, config.project_id
                );

                sqlx::query(
                    r#"
                    UPDATE auth_event_integration_configs
                    SET last_poll_at = NOW(),
                        last_event_id = COALESCE($1, last_event_id),
                        last_event_timestamp = COALESCE($2, last_event_timestamp),
                        error_message = NULL,
                        consecutive_errors = 0
                    WHERE id = $3
                    "#,
                )
                .bind(last_id)
                .bind(last_ts)
                .bind(config.id)
                .execute(db)
                .await?;
            }
            Err(e) => {
                error!(
                    "Failed to poll {} for {}: {}",
                    config.provider, config.project_id, e
                );

                sqlx::query(
                    r#"
                    UPDATE auth_event_integration_configs
                    SET last_poll_at = NOW(),
                        error_message = $1,
                        consecutive_errors = consecutive_errors + 1
                    WHERE id = $2
                    "#,
                )
                .bind(e.to_string())
                .bind(config.id)
                .execute(db)
                .await?;
            }
        }
    }

    Ok(())
}

// ============================================================================
// Auth Event Structure for ClickHouse
// ============================================================================

use clickhouse::Row;
use serde::Serialize;

#[derive(Debug, Clone, Row, Serialize)]
struct AuthEventInsert {
    event_id: String,
    project_id: uuid::Uuid,
    provider: String,
    timestamp: i64, // milliseconds
    event_type: String,
    event_category: String,
    outcome: String,
    actor_id: String,
    actor_email: String,
    actor_display_name: String,
    actor_type: String,
    target_id: String,
    target_name: String,
    target_type: String,
    client_ip: String,
    client_user_agent: String,
    client_device_type: String,
    client_os: String,
    client_browser: String,
    geo_country: String,
    geo_region: String,
    geo_city: String,
    auth_method: String,
    mfa_type: String,
    session_id: String,
    risk_level: String,
    risk_reasons: Vec<String>,
    is_suspicious: u8,
    error_code: String,
    error_message: String,
    application_id: String,
    application_name: String,
    raw_event: String,
    provider_data: String,
}

async fn insert_events(clickhouse: &ClickHousePool, events: Vec<AuthEventInsert>) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<AuthEventInsert>("auth_events")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(10_000);

    for event in events {
        inserter.write(&event).await?;
    }
    inserter.commit().await?;

    Ok(())
}

// ============================================================================
// Okta System Log
// ============================================================================

async fn poll_okta(
    client: &reqwest::Client,
    config: &IntegrationConfig,
    clickhouse: &ClickHousePool,
) -> Result<(usize, Option<String>, Option<chrono::DateTime<chrono::Utc>>)> {
    let domain = config
        .domain
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Domain not configured"))?;
    let api_token = config
        .api_token_encrypted
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("API token not configured"))?;

    let mut url = format!("https://{}/api/v1/logs?limit=100", domain);

    // Use since filter if we have a last event timestamp
    if let Some(ts) = config.last_event_timestamp {
        url.push_str(&format!("&since={}", ts.format("%Y-%m-%dT%H:%M:%S%.3fZ")));
    }

    let response = client
        .get(&url)
        .header("Authorization", format!("SSWS {}", api_token))
        .header("Accept", "application/json")
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Okta API error ({}): {}", status, body));
    }

    let events: Vec<serde_json::Value> = response.json().await?;
    let event_count = events.len();

    let mut last_id = None;
    let mut last_ts = None;

    let mut inserts = Vec::new();

    for event in events {
        let event_id = event["uuid"].as_str().unwrap_or_default().to_string();
        let published = event["published"].as_str().unwrap_or_default();
        let timestamp = chrono::DateTime::parse_from_rfc3339(published)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .ok();

        if let Some(ts) = &timestamp {
            last_ts = Some(*ts);
        }
        last_id = Some(event_id.clone());

        let event_type = map_okta_event_type(event["eventType"].as_str().unwrap_or_default());
        let outcome = if event["outcome"]["result"].as_str() == Some("SUCCESS") {
            "success"
        } else if event["outcome"]["result"].as_str() == Some("FAILURE") {
            "failure"
        } else {
            "unknown"
        };

        let actor = &event["actor"];
        let client_info = &event["client"];
        let geo = &client_info["geographicalContext"];

        inserts.push(AuthEventInsert {
            event_id,
            project_id: config.project_id,
            provider: "okta".to_string(),
            timestamp: timestamp.map(|t| t.timestamp_millis()).unwrap_or(0),
            event_type: event_type.0.to_string(),
            event_category: event_type.1.to_string(),
            outcome: outcome.to_string(),
            actor_id: actor["id"].as_str().unwrap_or_default().to_string(),
            actor_email: actor["alternateId"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            actor_display_name: actor["displayName"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            actor_type: actor["type"].as_str().unwrap_or("user").to_string(),
            target_id: event["target"]
                .get(0)
                .and_then(|t| t["id"].as_str())
                .unwrap_or_default()
                .to_string(),
            target_name: event["target"]
                .get(0)
                .and_then(|t| t["displayName"].as_str())
                .unwrap_or_default()
                .to_string(),
            target_type: event["target"]
                .get(0)
                .and_then(|t| t["type"].as_str())
                .unwrap_or_default()
                .to_string(),
            client_ip: client_info["ipAddress"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            client_user_agent: client_info["userAgent"]["rawUserAgent"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            client_device_type: client_info["device"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            client_os: client_info["userAgent"]["os"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            client_browser: client_info["userAgent"]["browser"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            geo_country: geo["country"].as_str().unwrap_or_default().to_string(),
            geo_region: geo["state"].as_str().unwrap_or_default().to_string(),
            geo_city: geo["city"].as_str().unwrap_or_default().to_string(),
            auth_method: event["authenticationContext"]["authenticationProvider"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            mfa_type: event["authenticationContext"]["credentialType"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            session_id: event["authenticationContext"]["externalSessionId"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            risk_level: event["securityContext"]["asOrg"]
                .as_str()
                .unwrap_or("low")
                .to_string(),
            risk_reasons: vec![],
            is_suspicious: if event["securityContext"]["isSuspicious"].as_bool() == Some(true) {
                1
            } else {
                0
            },
            error_code: event["outcome"]["reason"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            error_message: event["displayMessage"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            application_id: event["target"]
                .get(0)
                .and_then(|t| t["id"].as_str())
                .unwrap_or_default()
                .to_string(),
            application_name: event["target"]
                .get(0)
                .and_then(|t| t["displayName"].as_str())
                .unwrap_or_default()
                .to_string(),
            raw_event: serde_json::to_string(&event).unwrap_or_default(),
            provider_data: "{}".to_string(),
        });
    }

    insert_events(clickhouse, inserts).await?;

    Ok((event_count, last_id, last_ts))
}

fn map_okta_event_type(okta_type: &str) -> (&'static str, &'static str) {
    match okta_type {
        t if t.starts_with("user.session.start") => ("login", "authentication"),
        t if t.starts_with("user.session.end") => ("logout", "authentication"),
        t if t.starts_with("user.authentication") => ("login", "authentication"),
        t if t.contains("mfa") => ("mfa", "authentication"),
        t if t.starts_with("user.account.lock") => ("account_locked", "user_lifecycle"),
        t if t.starts_with("user.account.unlock") => ("account_unlocked", "user_lifecycle"),
        t if t.contains("password") => ("password_change", "user_lifecycle"),
        t if t.starts_with("user.lifecycle") => ("user_lifecycle", "user_lifecycle"),
        t if t.starts_with("application") => ("app_access", "authorization"),
        t if t.starts_with("group") => ("group_change", "authorization"),
        t if t.starts_with("policy") => ("policy_change", "admin"),
        _ => ("other", "other"),
    }
}

// ============================================================================
// Auth0 Logs
// ============================================================================

async fn poll_auth0(
    client: &reqwest::Client,
    config: &IntegrationConfig,
    clickhouse: &ClickHousePool,
) -> Result<(usize, Option<String>, Option<chrono::DateTime<chrono::Utc>>)> {
    let domain = config
        .domain
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Domain not configured"))?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client ID not configured"))?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client secret not configured"))?;

    // Get access token
    let token_url = format!("https://{}/oauth/token", domain);
    let token_response = client
        .post(&token_url)
        .json(&serde_json::json!({
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": format!("https://{}/api/v2/", domain),
            "grant_type": "client_credentials"
        }))
        .send()
        .await?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Auth0 token error: {}", body));
    }

    let token_data: serde_json::Value = token_response.json().await?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No access token in response"))?;

    // Fetch logs
    let mut logs_url = format!("https://{}/api/v2/logs?per_page=100&sort=date:1", domain);

    if let Some(ref last_id) = config.last_event_id {
        logs_url.push_str(&format!("&from={}", last_id));
    }

    let response = client
        .get(&logs_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Auth0 logs error: {}", body));
    }

    let events: Vec<serde_json::Value> = response.json().await?;
    let event_count = events.len();

    let mut last_id = None;
    let mut last_ts = None;
    let mut inserts = Vec::new();

    for event in events {
        let event_id = event["_id"].as_str().unwrap_or_default().to_string();
        let date = event["date"].as_str().unwrap_or_default();
        let timestamp = chrono::DateTime::parse_from_rfc3339(date)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .ok();

        if let Some(ts) = &timestamp {
            last_ts = Some(*ts);
        }
        last_id = Some(event_id.clone());

        let event_type = map_auth0_event_type(event["type"].as_str().unwrap_or_default());
        let outcome = if event["type"].as_str().unwrap_or_default().starts_with("s") {
            "success"
        } else if event["type"].as_str().unwrap_or_default().starts_with("f") {
            "failure"
        } else {
            "unknown"
        };

        inserts.push(AuthEventInsert {
            event_id,
            project_id: config.project_id,
            provider: "auth0".to_string(),
            timestamp: timestamp.map(|t| t.timestamp_millis()).unwrap_or(0),
            event_type: event_type.0.to_string(),
            event_category: event_type.1.to_string(),
            outcome: outcome.to_string(),
            actor_id: event["user_id"].as_str().unwrap_or_default().to_string(),
            actor_email: event["user_name"].as_str().unwrap_or_default().to_string(),
            actor_display_name: event["user_name"].as_str().unwrap_or_default().to_string(),
            actor_type: "user".to_string(),
            target_id: event["client_id"].as_str().unwrap_or_default().to_string(),
            target_name: event["client_name"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            target_type: "application".to_string(),
            client_ip: event["ip"].as_str().unwrap_or_default().to_string(),
            client_user_agent: event["user_agent"].as_str().unwrap_or_default().to_string(),
            client_device_type: "".to_string(),
            client_os: "".to_string(),
            client_browser: "".to_string(),
            geo_country: event["location_info"]["country_code"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            geo_region: event["location_info"]["region_name"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            geo_city: event["location_info"]["city_name"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            auth_method: event["connection"].as_str().unwrap_or_default().to_string(),
            mfa_type: "".to_string(),
            session_id: event["session_id"].as_str().unwrap_or_default().to_string(),
            risk_level: "low".to_string(),
            risk_reasons: vec![],
            is_suspicious: 0,
            error_code: event["type"].as_str().unwrap_or_default().to_string(),
            error_message: event["description"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            application_id: event["client_id"].as_str().unwrap_or_default().to_string(),
            application_name: event["client_name"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            raw_event: serde_json::to_string(&event).unwrap_or_default(),
            provider_data: "{}".to_string(),
        });
    }

    insert_events(clickhouse, inserts).await?;

    Ok((event_count, last_id, last_ts))
}

fn map_auth0_event_type(auth0_type: &str) -> (&'static str, &'static str) {
    match auth0_type {
        "s" | "ss" | "ssa" => ("login", "authentication"),
        "f" | "fp" | "fu" => ("failed_login", "authentication"),
        "slo" => ("logout", "authentication"),
        t if t.contains("mfa") => ("mfa", "authentication"),
        "scp" | "fcp" => ("password_change", "user_lifecycle"),
        "fs" => ("signup", "user_lifecycle"),
        "sapi" | "fapi" => ("api_access", "authorization"),
        _ => ("other", "other"),
    }
}

// ============================================================================
// Entra ID (Azure AD) Sign-in Logs
// ============================================================================

async fn poll_entra(
    client: &reqwest::Client,
    config: &IntegrationConfig,
    clickhouse: &ClickHousePool,
) -> Result<(usize, Option<String>, Option<chrono::DateTime<chrono::Utc>>)> {
    let tenant_id = config
        .tenant_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Tenant ID not configured"))?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client ID not configured"))?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client secret not configured"))?;

    // Get access token
    let token_url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );
    let token_response = client
        .post(&token_url)
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("scope", "https://graph.microsoft.com/.default"),
            ("grant_type", "client_credentials"),
        ])
        .send()
        .await?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Entra token error: {}", body));
    }

    let token_data: serde_json::Value = token_response.json().await?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No access token in response"))?;

    // Fetch sign-in logs
    let mut logs_url =
        "https://graph.microsoft.com/v1.0/auditLogs/signIns?$top=100&$orderby=createdDateTime"
            .to_string();

    if let Some(ts) = config.last_event_timestamp {
        logs_url.push_str(&format!(
            "&$filter=createdDateTime gt {}",
            ts.format("%Y-%m-%dT%H:%M:%SZ")
        ));
    }

    let response = client
        .get(&logs_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Entra logs error ({}): {}", status, body));
    }

    let data: serde_json::Value = response.json().await?;
    let events = data["value"].as_array().cloned().unwrap_or_default();
    let event_count = events.len();

    let mut last_id = None;
    let mut last_ts = None;
    let mut inserts = Vec::new();

    for event in events {
        let event_id = event["id"].as_str().unwrap_or_default().to_string();
        let created = event["createdDateTime"].as_str().unwrap_or_default();
        let timestamp = chrono::DateTime::parse_from_rfc3339(created)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .ok();

        if let Some(ts) = &timestamp {
            last_ts = Some(*ts);
        }
        last_id = Some(event_id.clone());

        let status_code = event["status"]["errorCode"].as_i64().unwrap_or(0);
        let outcome = if status_code == 0 {
            "success"
        } else {
            "failure"
        };

        let location = &event["location"];
        let risk = event["riskLevelDuringSignIn"].as_str().unwrap_or("none");

        inserts.push(AuthEventInsert {
            event_id,
            project_id: config.project_id,
            provider: "entra_id".to_string(),
            timestamp: timestamp.map(|t| t.timestamp_millis()).unwrap_or(0),
            event_type: "login".to_string(),
            event_category: "authentication".to_string(),
            outcome: outcome.to_string(),
            actor_id: event["userId"].as_str().unwrap_or_default().to_string(),
            actor_email: event["userPrincipalName"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            actor_display_name: event["userDisplayName"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            actor_type: "user".to_string(),
            target_id: event["appId"].as_str().unwrap_or_default().to_string(),
            target_name: event["appDisplayName"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            target_type: "application".to_string(),
            client_ip: event["ipAddress"].as_str().unwrap_or_default().to_string(),
            client_user_agent: event["deviceDetail"]["browser"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            client_device_type: event["deviceDetail"]["deviceId"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            client_os: event["deviceDetail"]["operatingSystem"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            client_browser: event["deviceDetail"]["browser"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            geo_country: location["countryOrRegion"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            geo_region: location["state"].as_str().unwrap_or_default().to_string(),
            geo_city: location["city"].as_str().unwrap_or_default().to_string(),
            auth_method: event["authenticationMethodsUsed"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            mfa_type: "".to_string(),
            session_id: event["correlationId"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            risk_level: match risk {
                "high" => "high",
                "medium" => "medium",
                "low" => "low",
                _ => "low",
            }
            .to_string(),
            risk_reasons: vec![],
            is_suspicious: if risk == "high" { 1 } else { 0 },
            error_code: status_code.to_string(),
            error_message: event["status"]["failureReason"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            application_id: event["appId"].as_str().unwrap_or_default().to_string(),
            application_name: event["appDisplayName"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            raw_event: serde_json::to_string(&event).unwrap_or_default(),
            provider_data: "{}".to_string(),
        });
    }

    insert_events(clickhouse, inserts).await?;

    Ok((event_count, last_id, last_ts))
}

// ============================================================================
// OneLogin Events
// ============================================================================

async fn poll_onelogin(
    client: &reqwest::Client,
    config: &IntegrationConfig,
    clickhouse: &ClickHousePool,
) -> Result<(usize, Option<String>, Option<chrono::DateTime<chrono::Utc>>)> {
    let region = config
        .region
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Region not configured"))?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client ID not configured"))?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client secret not configured"))?;

    let api_base = match region.to_lowercase().as_str() {
        "us" => "https://api.us.onelogin.com",
        "eu" => "https://api.eu.onelogin.com",
        _ => return Err(anyhow::anyhow!("Invalid region: {}", region)),
    };

    // Get access token
    let token_url = format!("{}/auth/oauth2/v2/token", api_base);
    let token_response = client
        .post(&token_url)
        .header(
            "Authorization",
            format!("client_id:{}, client_secret:{}", client_id, client_secret),
        )
        .json(&serde_json::json!({"grant_type": "client_credentials"}))
        .send()
        .await?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("OneLogin token error: {}", body));
    }

    let token_data: serde_json::Value = token_response.json().await?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No access token in response"))?;

    // Fetch events
    let mut events_url = format!("{}/api/1/events?limit=100", api_base);

    if let Some(ref last_id) = config.last_event_id {
        events_url.push_str(&format!("&since={}", last_id));
    }

    let response = client
        .get(&events_url)
        .header("Authorization", format!("bearer:{}", access_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("OneLogin events error: {}", body));
    }

    let data: serde_json::Value = response.json().await?;
    let events = data["data"].as_array().cloned().unwrap_or_default();
    let event_count = events.len();

    let mut last_id = None;
    let mut last_ts = None;
    let mut inserts = Vec::new();

    for event in events {
        let event_id = event["id"].to_string();
        let created = event["created_at"].as_str().unwrap_or_default();
        let timestamp = chrono::DateTime::parse_from_rfc3339(created)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .ok();

        if let Some(ts) = &timestamp {
            last_ts = Some(*ts);
        }
        last_id = Some(event_id.clone());

        let event_type_id = event["event_type_id"].as_i64().unwrap_or(0);
        let (event_type, event_category) = map_onelogin_event_type(event_type_id);

        inserts.push(AuthEventInsert {
            event_id,
            project_id: config.project_id,
            provider: "onelogin".to_string(),
            timestamp: timestamp.map(|t| t.timestamp_millis()).unwrap_or(0),
            event_type: event_type.to_string(),
            event_category: event_category.to_string(),
            outcome: "success".to_string(), // OneLogin doesn't have clear success/failure
            actor_id: event["user_id"].to_string(),
            actor_email: event["user_name"].as_str().unwrap_or_default().to_string(),
            actor_display_name: event["user_name"].as_str().unwrap_or_default().to_string(),
            actor_type: "user".to_string(),
            target_id: event["app_id"].to_string(),
            target_name: event["app_name"].as_str().unwrap_or_default().to_string(),
            target_type: "application".to_string(),
            client_ip: event["ipaddr"].as_str().unwrap_or_default().to_string(),
            client_user_agent: event["user_agent"].as_str().unwrap_or_default().to_string(),
            client_device_type: "".to_string(),
            client_os: "".to_string(),
            client_browser: "".to_string(),
            geo_country: "".to_string(),
            geo_region: "".to_string(),
            geo_city: "".to_string(),
            auth_method: "".to_string(),
            mfa_type: "".to_string(),
            session_id: "".to_string(),
            risk_level: "low".to_string(),
            risk_reasons: vec![],
            is_suspicious: 0,
            error_code: "".to_string(),
            error_message: event["notes"].as_str().unwrap_or_default().to_string(),
            application_id: event["app_id"].to_string(),
            application_name: event["app_name"].as_str().unwrap_or_default().to_string(),
            raw_event: serde_json::to_string(&event).unwrap_or_default(),
            provider_data: "{}".to_string(),
        });
    }

    insert_events(clickhouse, inserts).await?;

    Ok((event_count, last_id, last_ts))
}

fn map_onelogin_event_type(event_type_id: i64) -> (&'static str, &'static str) {
    match event_type_id {
        5 | 6 | 8 => ("login", "authentication"),
        7 | 9 => ("failed_login", "authentication"),
        10 => ("logout", "authentication"),
        11..=13 => ("mfa", "authentication"),
        14 | 15 => ("password_change", "user_lifecycle"),
        _ => ("other", "other"),
    }
}

// ============================================================================
// Ping Identity Activities
// ============================================================================

async fn poll_ping(
    client: &reqwest::Client,
    config: &IntegrationConfig,
    clickhouse: &ClickHousePool,
) -> Result<(usize, Option<String>, Option<chrono::DateTime<chrono::Utc>>)> {
    let env_id = config
        .environment_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Environment ID not configured"))?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client ID not configured"))?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client secret not configured"))?;

    // Get access token
    let token_url = format!("https://auth.pingone.com/{}/as/token", env_id);
    let token_response = client
        .post(&token_url)
        .basic_auth(client_id, Some(client_secret))
        .form(&[("grant_type", "client_credentials")])
        .send()
        .await?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("PingOne token error: {}", body));
    }

    let token_data: serde_json::Value = token_response.json().await?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No access token in response"))?;

    // Fetch audit events
    let mut events_url = format!(
        "https://api.pingone.com/v1/environments/{}/activities?limit=100&order=-recordedAt",
        env_id
    );

    if let Some(ts) = config.last_event_timestamp {
        events_url.push_str(&format!(
            "&filter=recordedAt gt \"{}\"",
            ts.format("%Y-%m-%dT%H:%M:%SZ")
        ));
    }

    let response = client
        .get(&events_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("PingOne activities error: {}", body));
    }

    let data: serde_json::Value = response.json().await?;
    let events = data["_embedded"]["activities"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let event_count = events.len();

    let mut last_id = None;
    let mut last_ts = None;
    let mut inserts = Vec::new();

    for event in events {
        let event_id = event["id"].as_str().unwrap_or_default().to_string();
        let recorded = event["recordedAt"].as_str().unwrap_or_default();
        let timestamp = chrono::DateTime::parse_from_rfc3339(recorded)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .ok();

        if let Some(ts) = &timestamp {
            last_ts = Some(*ts);
        }
        last_id = Some(event_id.clone());

        let action_type = event["action"]["type"].as_str().unwrap_or_default();
        let (event_type, event_category) = map_ping_event_type(action_type);

        let result = event["result"]["status"].as_str().unwrap_or_default();
        let outcome = if result == "SUCCESS" {
            "success"
        } else {
            "failure"
        };

        let actors = event["actors"].as_array();
        let actor = actors.and_then(|a| a.first()).cloned().unwrap_or_default();

        inserts.push(AuthEventInsert {
            event_id,
            project_id: config.project_id,
            provider: "ping_identity".to_string(),
            timestamp: timestamp.map(|t| t.timestamp_millis()).unwrap_or(0),
            event_type: event_type.to_string(),
            event_category: event_category.to_string(),
            outcome: outcome.to_string(),
            actor_id: actor["id"].as_str().unwrap_or_default().to_string(),
            actor_email: actor["name"].as_str().unwrap_or_default().to_string(),
            actor_display_name: actor["name"].as_str().unwrap_or_default().to_string(),
            actor_type: actor["type"].as_str().unwrap_or("USER").to_lowercase(),
            target_id: event["resources"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|r| r["id"].as_str())
                .unwrap_or_default()
                .to_string(),
            target_name: event["resources"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|r| r["name"].as_str())
                .unwrap_or_default()
                .to_string(),
            target_type: event["resources"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|r| r["type"].as_str())
                .unwrap_or_default()
                .to_string(),
            client_ip: "".to_string(),
            client_user_agent: "".to_string(),
            client_device_type: "".to_string(),
            client_os: "".to_string(),
            client_browser: "".to_string(),
            geo_country: "".to_string(),
            geo_region: "".to_string(),
            geo_city: "".to_string(),
            auth_method: "".to_string(),
            mfa_type: "".to_string(),
            session_id: event["correlationId"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            risk_level: "low".to_string(),
            risk_reasons: vec![],
            is_suspicious: 0,
            error_code: "".to_string(),
            error_message: event["result"]["description"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            application_id: "".to_string(),
            application_name: "".to_string(),
            raw_event: serde_json::to_string(&event).unwrap_or_default(),
            provider_data: "{}".to_string(),
        });
    }

    insert_events(clickhouse, inserts).await?;

    Ok((event_count, last_id, last_ts))
}

fn map_ping_event_type(action_type: &str) -> (&'static str, &'static str) {
    match action_type {
        "AUTHENTICATION" | "LOGIN" => ("login", "authentication"),
        "LOGOUT" => ("logout", "authentication"),
        "MFA" | "MFA.CREATE" | "MFA.CHECK" => ("mfa", "authentication"),
        "PASSWORD.RESET" | "PASSWORD.CHANGE" => ("password_change", "user_lifecycle"),
        "USER.CREATE" | "USER.UPDATE" | "USER.DELETE" => ("user_lifecycle", "user_lifecycle"),
        t if t.starts_with("APPLICATION") => ("app_access", "authorization"),
        t if t.starts_with("GROUP") => ("group_change", "authorization"),
        t if t.starts_with("POLICY") => ("policy_change", "admin"),
        _ => ("other", "other"),
    }
}

// ============================================================================
// Keycloak Events
// ============================================================================

async fn poll_keycloak(
    client: &reqwest::Client,
    config: &IntegrationConfig,
    clickhouse: &ClickHousePool,
) -> Result<(usize, Option<String>, Option<chrono::DateTime<chrono::Utc>>)> {
    let domain = config
        .domain
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Domain (Keycloak URL) not configured"))?;
    let realm = config
        .tenant_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Realm not configured"))?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client ID not configured"))?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Client secret not configured"))?;

    // Get access token from Keycloak
    let token_url = format!("{}/realms/{}/protocol/openid-connect/token", domain, realm);
    let token_response = client
        .post(&token_url)
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("grant_type", "client_credentials"),
        ])
        .send()
        .await?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Keycloak token error: {}", body));
    }

    let token_data: serde_json::Value = token_response.json().await?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No access token in response"))?;

    // Fetch events from Admin API
    let mut events_url = format!("{}/admin/realms/{}/events?max=100", domain, realm);

    // Use dateFrom filter if we have a last event timestamp
    if let Some(ts) = config.last_event_timestamp {
        // Keycloak uses Unix timestamp in milliseconds
        events_url.push_str(&format!("&dateFrom={}", ts.timestamp_millis()));
    }

    let response = client
        .get(&events_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "Keycloak events error ({}): {}",
            status,
            body
        ));
    }

    let events: Vec<serde_json::Value> = response.json().await?;
    let event_count = events.len();

    let mut last_id = None;
    let mut last_ts = None;
    let mut inserts = Vec::new();

    for event in events {
        // Keycloak event structure:
        // {
        //   "time": 1234567890000,  // Unix timestamp in ms
        //   "type": "LOGIN",
        //   "realmId": "...",
        //   "clientId": "...",
        //   "userId": "...",
        //   "sessionId": "...",
        //   "ipAddress": "...",
        //   "error": "...",  // Only on error events
        //   "details": { ... }
        // }

        let time_ms = event["time"].as_i64().unwrap_or(0);
        let timestamp =
            chrono::DateTime::from_timestamp_millis(time_ms).unwrap_or_else(|| chrono::Utc::now());

        last_ts = Some(timestamp);
        let event_id = format!("{}-{}", event["sessionId"].as_str().unwrap_or(""), time_ms);
        last_id = Some(event_id.clone());

        let event_type_str = event["type"].as_str().unwrap_or_default();
        let (event_type, event_category) = map_keycloak_event_type(event_type_str);

        let has_error = event["error"].as_str().is_some();
        let outcome = if has_error { "failure" } else { "success" };

        let details = &event["details"];

        inserts.push(AuthEventInsert {
            event_id,
            project_id: config.project_id,
            provider: "keycloak".to_string(),
            timestamp: time_ms,
            event_type: event_type.to_string(),
            event_category: event_category.to_string(),
            outcome: outcome.to_string(),
            actor_id: event["userId"].as_str().unwrap_or_default().to_string(),
            actor_email: details["username"]
                .as_str()
                .or_else(|| details["email"].as_str())
                .unwrap_or_default()
                .to_string(),
            actor_display_name: details["username"].as_str().unwrap_or_default().to_string(),
            actor_type: "user".to_string(),
            target_id: event["clientId"].as_str().unwrap_or_default().to_string(),
            target_name: event["clientId"].as_str().unwrap_or_default().to_string(),
            target_type: "application".to_string(),
            client_ip: event["ipAddress"].as_str().unwrap_or_default().to_string(),
            client_user_agent: details["user_agent"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            client_device_type: "".to_string(),
            client_os: "".to_string(),
            client_browser: "".to_string(),
            geo_country: "".to_string(),
            geo_region: "".to_string(),
            geo_city: "".to_string(),
            auth_method: details["auth_method"]
                .as_str()
                .or_else(|| details["identity_provider"].as_str())
                .unwrap_or_default()
                .to_string(),
            mfa_type: details["auth_type"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            session_id: event["sessionId"].as_str().unwrap_or_default().to_string(),
            risk_level: "low".to_string(),
            risk_reasons: vec![],
            is_suspicious: 0,
            error_code: event["error"].as_str().unwrap_or_default().to_string(),
            error_message: details["reason"].as_str().unwrap_or_default().to_string(),
            application_id: event["clientId"].as_str().unwrap_or_default().to_string(),
            application_name: event["clientId"].as_str().unwrap_or_default().to_string(),
            raw_event: serde_json::to_string(&event).unwrap_or_default(),
            provider_data: serde_json::json!({
                "realm_id": event["realmId"].as_str().unwrap_or_default(),
            })
            .to_string(),
        });
    }

    insert_events(clickhouse, inserts).await?;

    Ok((event_count, last_id, last_ts))
}

fn map_keycloak_event_type(keycloak_type: &str) -> (&'static str, &'static str) {
    match keycloak_type {
        "LOGIN" | "LOGIN_ERROR" => ("login", "authentication"),
        "LOGOUT" | "LOGOUT_ERROR" => ("logout", "authentication"),
        "REGISTER" | "REGISTER_ERROR" => ("signup", "user_lifecycle"),
        "CODE_TO_TOKEN" | "CODE_TO_TOKEN_ERROR" => ("token_exchange", "authentication"),
        "REFRESH_TOKEN" | "REFRESH_TOKEN_ERROR" => ("token_refresh", "authentication"),
        "INTROSPECT_TOKEN" | "INTROSPECT_TOKEN_ERROR" => ("token_introspect", "authentication"),
        "FEDERATED_IDENTITY_LINK" | "FEDERATED_IDENTITY_LINK_ERROR" => {
            ("identity_link", "user_lifecycle")
        }
        "REMOVE_FEDERATED_IDENTITY" | "REMOVE_FEDERATED_IDENTITY_ERROR" => {
            ("identity_unlink", "user_lifecycle")
        }
        "UPDATE_EMAIL" | "UPDATE_EMAIL_ERROR" => ("email_change", "user_lifecycle"),
        "UPDATE_PROFILE" | "UPDATE_PROFILE_ERROR" => ("profile_update", "user_lifecycle"),
        "RESET_PASSWORD" | "RESET_PASSWORD_ERROR" => ("password_reset", "user_lifecycle"),
        "UPDATE_PASSWORD" | "UPDATE_PASSWORD_ERROR" => ("password_change", "user_lifecycle"),
        "UPDATE_TOTP" | "UPDATE_TOTP_ERROR" => ("mfa_update", "authentication"),
        "REMOVE_TOTP" | "REMOVE_TOTP_ERROR" => ("mfa_remove", "authentication"),
        "VERIFY_EMAIL" | "VERIFY_EMAIL_ERROR" => ("email_verify", "user_lifecycle"),
        "CUSTOM_REQUIRED_ACTION" | "CUSTOM_REQUIRED_ACTION_ERROR" => {
            ("required_action", "authentication")
        }
        "GRANT_CONSENT" | "GRANT_CONSENT_ERROR" => ("consent_grant", "authorization"),
        "UPDATE_CONSENT" | "UPDATE_CONSENT_ERROR" => ("consent_update", "authorization"),
        "REVOKE_GRANT" | "REVOKE_GRANT_ERROR" => ("consent_revoke", "authorization"),
        "CLIENT_LOGIN" | "CLIENT_LOGIN_ERROR" => ("client_login", "authentication"),
        "IMPERSONATE" | "IMPERSONATE_ERROR" => ("impersonate", "admin"),
        _ => ("other", "other"),
    }
}
